package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

// ExternalOperationResult is one durable reconciliation outcome. A non-nil
// error may accompany a terminal result or a post-commit finalization warning;
// callers must treat the returned snapshots as authoritative.
type ExternalOperationResult struct {
	Operation ExternalOperationSnapshot
	Node      NodeInvocationSnapshot
	Attempt   AttemptSnapshot
	Result    *stepkind.StepResult
	Outputs   *values.ValueSetRef
	Warnings  []DispatchWarning
}

// ExternalOperationCoordinator reconciles adapter-owned work without a worker
// claim. Suspension has already released the execution lease; W03-T06 may
// schedule and retry these idempotent observation calls, while durable refs and
// store CAS generations fence competing observers.
type ExternalOperationCoordinator struct {
	store       StateStore
	registry    stepkind.Registry
	now         func() time.Time
	disposition FailureDisposition
	retention   RetentionHook
	redactor    *values.Redactor
}

// ExternalOperationOptions supplies extraction-safe reconciliation
// collaborators. A nil failure disposition closes failed observations
// terminally; W03 retry policy may inject NodeReady.
type ExternalOperationOptions struct {
	Store              StateStore
	Registry           stepkind.Registry
	Now                func() time.Time
	FailureDisposition FailureDisposition
	RetentionHook      RetentionHook
	Redactor           *values.Redactor
}

// NewExternalOperationCoordinator constructs recovery-aware external work
// reconciliation. Adapter calls are never made inside StateStore transactions.
func NewExternalOperationCoordinator(options ExternalOperationOptions) (*ExternalOperationCoordinator, error) {
	if nilStateStore(options.Store) {
		return nil, fmt.Errorf("%w: state store is required", ErrInvalidDispatch)
	}
	if nilStepKindRegistry(options.Registry) {
		return nil, fmt.Errorf("%w: step-kind registry is required", ErrInvalidDispatch)
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &ExternalOperationCoordinator{
		store: options.Store, registry: options.Registry, now: now,
		disposition: options.FailureDisposition, retention: options.RetentionHook,
		redactor: options.Redactor,
	}, nil
}

// Recover returns pending durable operations in store order. It deliberately
// performs no adapter I/O or scheduling policy.
func (c *ExternalOperationCoordinator) Recover(ctx context.Context, query ExternalOperationQuery) ([]ExternalOperationSnapshot, error) {
	if err := c.validate(ctx); err != nil {
		return nil, err
	}
	return c.store.RecoverExternalOperations(ctx, query)
}

// Reconcile heartbeats and observes one pending durable operation. Transient
// heartbeat or observation I/O leaves the durable operation pending for
// recovery. Adapter-reported terminal states and irrecoverable contract
// mismatches close failed by default, or ready when disposition selects retry.
func (c *ExternalOperationCoordinator) Reconcile(ctx context.Context, id AttemptID) (ExternalOperationResult, error) {
	if err := c.validate(ctx); err != nil {
		return ExternalOperationResult{}, err
	}
	if err := id.Validate(); err != nil {
		return ExternalOperationResult{}, fmt.Errorf("%w: %w", ErrInvalidDispatch, err)
	}
	durableCtx := context.WithoutCancel(ctx)
	operation, node, attempt, err := c.load(durableCtx, id)
	if err != nil {
		return ExternalOperationResult{}, err
	}
	result := ExternalOperationResult{Operation: operation, Node: node, Attempt: attempt}
	kind, spec, err := stepkind.Resolve(c.registry, attempt.Executor.Kind, attempt.Executor.Version)
	if err != nil {
		return c.fail(durableCtx, result, nil, stepkind.StepKindSpec{Name: attempt.Executor.Kind, Version: attempt.Executor.Version}, DispatchResolve, err)
	}
	observer, ok := kind.(stepkind.Observer)
	if !ok || spec.Observation.Mode != stepkind.ObservationPoll {
		return c.fail(durableCtx, result, kind, spec, DispatchResolve, errors.New("registered kind cannot observe its durable external operation"))
	}
	heartbeatAt := time.Time{}
	if spec.Observation.Heartbeat {
		heartbeater, ok := kind.(stepkind.Heartbeater)
		if !ok {
			return c.fail(durableCtx, result, kind, spec, DispatchHeartbeat, errors.New("registered kind is missing its advertised heartbeat hook"))
		}
		if heartbeatErr := heartbeater.Heartbeat(ctx, cloneExternalRef(operation.Ref)); heartbeatErr != nil {
			return result, c.externalDispatchError(DispatchHeartbeat, operation, attempt, heartbeatErr)
		}
		heartbeatAt = c.atOrAfter(operation.UpdatedAt, node.UpdatedAt, attempt.UpdatedAt)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return result, c.externalDispatchError(DispatchObserve, operation, attempt, contextErr)
	}
	observation, err := observer.Observe(ctx, cloneExternalRef(operation.Ref))
	observedAt := c.atOrAfter(operation.UpdatedAt, node.UpdatedAt, attempt.UpdatedAt, heartbeatAt)
	if err != nil {
		return c.persistPendingObservationError(durableCtx, result, nil, heartbeatAt, observedAt, DispatchObserve, err)
	}
	if err := observation.Validate(); err != nil {
		validationErr := errors.Join(ErrStepValidation, err)
		if observation.State != stepkind.ObservationSucceeded && observation.State != stepkind.ObservationFailed && observation.State != stepkind.ObservationCanceled {
			return c.persistPendingObservationError(durableCtx, result, observation.Progress, heartbeatAt, observedAt, DispatchObserve, validationErr)
		}
		return c.failWithResult(durableCtx, result, kind, spec, stepkind.StepResult{}, stepkind.ObservationFailed, heartbeatAt, observedAt, DispatchObserve, failureInvalidResult, validationErr)
	}
	at := observedAt
	switch observation.State {
	case stepkind.ObservationPending:
		applied, err := c.store.ApplyExternalOperation(durableCtx, ApplyExternalOperationRequest{
			Attempt: id, ExpectedOperationGeneration: operation.Generation,
			ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation,
			Status: stepkind.ObservationPending, Progress: maskExternalProgress(observation.Progress, c.redactor), ObservedAt: observedAt, HeartbeatAt: heartbeatAt, At: at,
		})
		if err != nil {
			return result, c.externalDispatchError(DispatchObserve, operation, attempt, err)
		}
		return externalResult(applied, nil), nil
	case stepkind.ObservationSucceeded:
		stepResult := cloneStepResult(*observation.Result)
		if err := values.ValidateValueSetSchema(spec.OutputSchema, stepResult.Outputs); err != nil {
			return c.failWithResult(durableCtx, result, kind, spec, stepResult, stepkind.ObservationFailed, heartbeatAt, observedAt, DispatchValidateOutput, failureInvalidResult, errors.Join(ErrStepValidation, err))
		}
		outputRef, err := SaveValuesWithRetention(durableCtx, c.store, c.retention, SaveValuesRequest{
			Owner:  ValueOwner{Kind: "external-operation-outputs", RunID: id.Invocation.RunID, Invocation: &id.Invocation, Attempt: &id},
			Values: stepResult.Outputs,
		})
		if err != nil {
			return c.persistPendingObservationError(durableCtx, result, observation.Progress, heartbeatAt, observedAt, DispatchPersistOutput, err)
		}
		applied, err := c.store.ApplyExternalOperation(durableCtx, ApplyExternalOperationRequest{
			Attempt: id, ExpectedOperationGeneration: operation.Generation,
			ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation,
			Status: stepkind.ObservationSucceeded, Progress: maskExternalProgress(observation.Progress, c.redactor),
			Outputs: &outputRef, NextNodeStatus: NodeSucceeded, ObservedAt: observedAt, HeartbeatAt: heartbeatAt, At: at,
		})
		if err != nil {
			return result, c.externalDispatchError(DispatchFinishAttempt, operation, attempt, err)
		}
		terminal := externalResult(applied, &stepResult)
		terminal.Outputs = cloneValueSetRef(&outputRef)
		c.finalize(durableCtx, kind, spec, operation.Invocation, stepResult, nil, &terminal)
		return terminal, nil
	case stepkind.ObservationFailed, stepkind.ObservationCanceled:
		return c.failWithResult(durableCtx, result, kind, spec, stepkind.StepResult{}, observation.State, heartbeatAt, observedAt, DispatchObserve, observation.Failure.Code, observation.Failure)
	default:
		panic("validated observation has unsupported state")
	}
}

// RequestCancel persists cancellation intent before calling the adapter, then
// reconciles the operation. A crash between those steps leaves discoverable
// intent for recovery to retry.
func (c *ExternalOperationCoordinator) RequestCancel(ctx context.Context, id AttemptID) (ExternalOperationResult, error) {
	if err := c.validate(ctx); err != nil {
		return ExternalOperationResult{}, err
	}
	durableCtx := context.WithoutCancel(ctx)
	operation, node, attempt, err := c.load(durableCtx, id)
	if err != nil {
		return ExternalOperationResult{}, err
	}
	result := ExternalOperationResult{Operation: operation, Node: node, Attempt: attempt}
	requested, err := c.store.RequestExternalOperationCancel(durableCtx, RequestExternalOperationCancelRequest{
		Attempt: id, ExpectedOperationGeneration: operation.Generation,
		At: c.atOrAfter(operation.UpdatedAt, node.UpdatedAt, attempt.UpdatedAt),
	})
	if err != nil {
		return result, c.externalDispatchError(DispatchCancel, operation, attempt, err)
	}
	result.Operation = requested.Operation
	kind, spec, err := stepkind.Resolve(c.registry, attempt.Executor.Kind, attempt.Executor.Version)
	if err != nil {
		return c.fail(durableCtx, result, nil, stepkind.StepKindSpec{Name: attempt.Executor.Kind, Version: attempt.Executor.Version}, DispatchResolve, err)
	}
	canceler, ok := kind.(stepkind.Canceler)
	if !ok || spec.Cancellation.Mode != stepkind.CancellationExplicit {
		return c.fail(durableCtx, result, kind, spec, DispatchCancel, errors.New("registered kind cannot cancel its durable external operation"))
	}
	if err := canceler.Cancel(ctx, cloneExternalRef(requested.Operation.Ref)); err != nil {
		return result, c.externalDispatchError(DispatchCancel, requested.Operation, attempt, err)
	}
	return c.Reconcile(ctx, id)
}

func (c *ExternalOperationCoordinator) load(ctx context.Context, id AttemptID) (ExternalOperationSnapshot, NodeInvocationSnapshot, AttemptSnapshot, error) {
	operation, err := c.store.LoadExternalOperation(ctx, id)
	if err != nil {
		return ExternalOperationSnapshot{}, NodeInvocationSnapshot{}, AttemptSnapshot{}, err
	}
	node, err := c.store.LoadNodeInvocation(ctx, id.Invocation)
	if err != nil {
		return ExternalOperationSnapshot{}, NodeInvocationSnapshot{}, AttemptSnapshot{}, err
	}
	attempt, err := c.store.LoadAttempt(ctx, id)
	if err != nil {
		return ExternalOperationSnapshot{}, NodeInvocationSnapshot{}, AttemptSnapshot{}, err
	}
	return operation, node, attempt, nil
}

func (c *ExternalOperationCoordinator) persistPendingObservationError(
	ctx context.Context,
	result ExternalOperationResult,
	progress map[string]string,
	heartbeatAt time.Time,
	observedAt time.Time,
	stage DispatchStage,
	cause error,
) (ExternalOperationResult, error) {
	if progress == nil {
		progress = result.Operation.Progress
	}
	applied, err := c.store.ApplyExternalOperation(ctx, ApplyExternalOperationRequest{
		Attempt: result.Attempt.ID, ExpectedOperationGeneration: result.Operation.Generation,
		ExpectedNodeGeneration: result.Node.Generation, ExpectedAttemptGeneration: result.Attempt.Generation,
		Status: stepkind.ObservationPending, Progress: maskExternalProgress(progress, c.redactor),
		ObservedAt: observedAt, HeartbeatAt: heartbeatAt, At: observedAt,
	})
	if err != nil {
		return result, c.externalDispatchError(stage, result.Operation, result.Attempt, errors.Join(cause, err))
	}
	pending := externalResult(applied, nil)
	return pending, c.externalDispatchError(stage, pending.Operation, pending.Attempt, cause)
}

func (c *ExternalOperationCoordinator) fail(
	ctx context.Context,
	result ExternalOperationResult,
	kind stepkind.StepKind,
	spec stepkind.StepKindSpec,
	stage DispatchStage,
	cause error,
) (ExternalOperationResult, error) {
	return c.failWithResult(ctx, result, kind, spec, stepkind.StepResult{}, stepkind.ObservationFailed, time.Time{}, time.Time{}, stage, failureExecute, cause)
}

func (c *ExternalOperationCoordinator) failWithResult(
	ctx context.Context,
	result ExternalOperationResult,
	kind stepkind.StepKind,
	spec stepkind.StepKindSpec,
	produced stepkind.StepResult,
	observedState stepkind.ObservationState,
	heartbeatAt time.Time,
	observedAt time.Time,
	stage DispatchStage,
	code string,
	cause error,
) (ExternalOperationResult, error) {
	failure, attemptStatus := executionFailure(code, cause)
	if observedState == stepkind.ObservationCanceled {
		attemptStatus = NodeCanceled
	}
	next := attemptStatus
	var policyErr error
	if c.disposition != nil && (attemptStatus == NodeFailed || attemptStatus == NodeTimedOut || attemptStatus == NodeCrashed) {
		candidate, err := c.disposition.NextNodeStatus(ctx, FailureDispositionRequest{Spec: spec, Attempt: result.Attempt, Failure: failure, Status: attemptStatus})
		if err != nil {
			policyErr = err
			failure.Details = cloneDispatchStringMap(failure.Details)
			if failure.Details == nil {
				failure.Details = make(map[string]string)
			}
			failure.Details["disposition"] = failurePolicy
		} else if candidate == attemptStatus || candidate == NodeReady {
			next = candidate
		} else {
			policyErr = fmt.Errorf("invalid failure disposition %q for attempt status %q", candidate, attemptStatus)
		}
	}
	persisted := maskDispatchFailure(failure, c.redactor)
	status := stepkind.ObservationFailed
	if observedState == stepkind.ObservationCanceled || attemptStatus == NodeCanceled {
		status = stepkind.ObservationCanceled
	}
	at := c.atOrAfter(result.Operation.UpdatedAt, result.Node.UpdatedAt, result.Attempt.UpdatedAt)
	applied, applyErr := c.store.ApplyExternalOperation(ctx, ApplyExternalOperationRequest{
		Attempt: result.Attempt.ID, ExpectedOperationGeneration: result.Operation.Generation,
		ExpectedNodeGeneration: result.Node.Generation, ExpectedAttemptGeneration: result.Attempt.Generation,
		Status: status, Progress: cloneDispatchStringMap(result.Operation.Progress), Failure: &persisted,
		NextNodeStatus: next, ObservedAt: observedAt, HeartbeatAt: heartbeatAt, At: at,
	})
	if applyErr != nil {
		return result, c.externalDispatchError(DispatchFinishAttempt, result.Operation, result.Attempt, errors.Join(cause, policyErr, applyErr))
	}
	terminal := externalResult(applied, nil)
	if !isNilStepKindValue(kind) {
		c.finalize(ctx, kind, spec, result.Operation.Invocation, produced, cause, &terminal)
	}
	return terminal, c.externalDispatchError(stage, result.Operation, result.Attempt, errors.Join(cause, policyErr))
}

func (c *ExternalOperationCoordinator) finalize(ctx context.Context, kind stepkind.StepKind, spec stepkind.StepKindSpec, invocation stepkind.Invocation, result stepkind.StepResult, executionErr error, terminal *ExternalOperationResult) {
	dispatcher := StepDispatcher{store: c.store, registry: c.registry, now: c.now, disposition: c.disposition, retention: c.retention, redactor: c.redactor}
	dispatchResult := DispatchResult{Node: terminal.Node, Attempt: terminal.Attempt, Result: terminal.Result, Outputs: terminal.Outputs, Warnings: terminal.Warnings}
	dispatcher.finalize(ctx, DispatchRequest{
		Claim: ReadyClaim{Candidate: ReadyCandidate{InvocationID: terminal.Attempt.ID.Invocation}},
		Node:  graph.Node{ID: terminal.Attempt.ID.Invocation.NodeID, Kind: spec.Name, KindVersion: spec.Version},
	}, kind, stepkind.PreparedInvocation{Invocation: cloneStepInvocation(invocation)}, result, executionErr, &dispatchResult)
	terminal.Warnings = dispatchResult.Warnings
}

func (c *ExternalOperationCoordinator) externalDispatchError(stage DispatchStage, operation ExternalOperationSnapshot, attempt AttemptSnapshot, cause error) error {
	return &DispatchError{Stage: stage, Invocation: operation.Attempt.Invocation, Kind: attempt.Executor.Kind, Version: attempt.Executor.Version, Cause: cause}
}

func (c *ExternalOperationCoordinator) atOrAfter(floors ...time.Time) time.Time {
	at := c.now().UTC()
	for _, floor := range floors {
		if at.IsZero() || at.Before(floor) {
			at = floor
		}
	}
	return at
}

func (c *ExternalOperationCoordinator) validate(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidDispatch)
	}
	if c == nil || nilStateStore(c.store) || nilStepKindRegistry(c.registry) || c.now == nil {
		return fmt.Errorf("%w: external operation coordinator is not initialized", ErrInvalidDispatch)
	}
	return nil
}

func externalResult(applied ApplyExternalOperationResult, result *stepkind.StepResult) ExternalOperationResult {
	return ExternalOperationResult{Operation: applied.Operation, Node: applied.Node, Attempt: applied.Attempt, Result: result, Outputs: cloneValueSetRef(applied.Operation.Outputs)}
}

func cloneExternalRef(ref stepkind.ExternalOperationRef) stepkind.ExternalOperationRef {
	ref.Metadata = cloneDispatchStringMap(ref.Metadata)
	return ref
}

func maskExternalProgress(progress map[string]string, redactor *values.Redactor) map[string]string {
	masked := cloneDispatchStringMap(progress)
	keys := make([]string, 0, len(masked))
	for key := range masked {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if redactor != nil {
			masked[key] = redactor.MaskString(masked[key])
		}
	}
	return masked
}

func isNilStepKindValue(kind stepkind.StepKind) bool {
	if kind == nil {
		return true
	}
	value := reflect.ValueOf(kind)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
