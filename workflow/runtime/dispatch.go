package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	EventNodeFinalizeWarning = "node.finalize_warning"

	failurePrepare       = "step_prepare_failed"
	failureExecute       = "step_execute_failed"
	failureInvalidResult = "step_result_invalid"
	failurePersistOutput = "step_output_persistence_failed"
	failureSuspend       = "step_suspension_failed"
	failurePolicy        = "step_failure_policy_failed"
	failureFinalize      = "step_finalize_failed"
)

var (
	ErrInvalidDispatch = errors.New("invalid step dispatch request")
	ErrStepValidation  = errors.New("step invocation validation failed")
	ErrStepExecution   = errors.New("step invocation execution failed")
)

// DispatchStage identifies the lifecycle boundary at which dispatch failed.
type DispatchStage string

const (
	DispatchResolve        DispatchStage = "resolve"
	DispatchValidateConfig DispatchStage = "validate_config"
	DispatchValidateInput  DispatchStage = "validate_input"
	DispatchStartAttempt   DispatchStage = "start_attempt"
	DispatchPrepare        DispatchStage = "prepare"
	DispatchExecute        DispatchStage = "execute"
	DispatchValidateOutput DispatchStage = "validate_output"
	DispatchPersistOutput  DispatchStage = "persist_output"
	DispatchSuspend        DispatchStage = "suspend"
	DispatchHeartbeat      DispatchStage = "heartbeat"
	DispatchObserve        DispatchStage = "observe"
	DispatchCancel         DispatchStage = "cancel"
	DispatchFinishAttempt  DispatchStage = "finish_attempt"
	DispatchFinalize       DispatchStage = "finalize"
)

// DispatchError preserves the typed lifecycle stage and underlying cause.
type DispatchError struct {
	Stage      DispatchStage
	Invocation NodeInvocationID
	Kind       string
	Version    string
	Cause      error
}

// Error implements error.
func (e *DispatchError) Error() string {
	return fmt.Sprintf("%s: %s %s@%s for %v: %v", ErrStepExecution, e.Stage, e.Kind, e.Version, e.Invocation, e.Cause)
}

// Unwrap preserves both the stable dispatch category and the underlying cause.
func (e *DispatchError) Unwrap() []error { return []error{ErrStepExecution, e.Cause} }

// FailureDispositionRequest supplies a finished execution failure to injected
// retry policy. W03-T04 may return NodeReady; no policy defaults to the
// terminal attempt status.
type FailureDispositionRequest struct {
	Spec    stepkind.StepKindSpec
	Attempt AttemptSnapshot
	Failure Failure
	Status  NodeStatus
}

// FailureDisposition selects the aggregate node state after an unsuccessful
// attempt without implementing retry limits or backoff in the dispatcher.
type FailureDisposition interface {
	NextNodeStatus(context.Context, FailureDispositionRequest) (NodeStatus, error)
}

// FailureDispositionFunc adapts a function to FailureDisposition.
type FailureDispositionFunc func(context.Context, FailureDispositionRequest) (NodeStatus, error)

// NextNodeStatus implements FailureDisposition.
func (f FailureDispositionFunc) NextNodeStatus(ctx context.Context, request FailureDispositionRequest) (NodeStatus, error) {
	return f(ctx, request)
}

// DispatchWarning is a non-reversing lifecycle warning produced after a
// durable terminal attempt, currently by Finalizer or warning-event storage.
type DispatchWarning struct {
	Stage   DispatchStage
	Failure Failure
	Event   *Event
	Cause   error
}

// DispatchRequest binds one acquired ready claim to an exact graph node and
// adapter invocation. Target and ExecutorAttributes describe the selected host
// execution target without exposing host types.
type DispatchRequest struct {
	Claim              ReadyClaim
	Node               graph.Node
	CallLineage        []graph.DefinitionRef
	IdempotencyKey     string
	Target             string
	ExecutorAttributes map[string]string
}

// DispatchResult contains durable attempt state even when Error is returned
// after a failed invocation. Result and Outputs are populated only on success.
type DispatchResult struct {
	Node        NodeInvocationSnapshot
	Attempt     AttemptSnapshot
	Result      *stepkind.StepResult
	Outputs     *values.ValueSetRef
	Wait        *WaitSnapshot
	External    *ExternalOperationSnapshot
	Diagnostics []diagnostic.Diagnostic
	Warnings    []DispatchWarning
}

// StepDispatcher executes claimed nodes exclusively through a step-kind
// registry and the atomic StateStore attempt lifecycle.
type StepDispatcher struct {
	store       StateStore
	registry    stepkind.Registry
	now         func() time.Time
	disposition FailureDisposition
	retention   RetentionHook
	redactor    *values.Redactor
	waits       *WaitCoordinator
	retry       *RetryCoordinator
}

// DispatcherOptions supplies extraction-safe dispatcher collaborators.
type DispatcherOptions struct {
	Store              StateStore
	Registry           stepkind.Registry
	Now                func() time.Time
	FailureDisposition FailureDisposition
	RetentionHook      RetentionHook
	Redactor           *values.Redactor
	WaitCoordinator    *WaitCoordinator
	RetryCoordinator   *RetryCoordinator
}

// NewStepDispatcher constructs a registry-driven dispatcher. A nil Now uses
// the UTC wall clock. A nil disposition terminates failed attempts.
func NewStepDispatcher(options DispatcherOptions) (*StepDispatcher, error) {
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
	dispatcher := &StepDispatcher{
		store: options.Store, registry: options.Registry, now: now,
		disposition: options.FailureDisposition, retention: options.RetentionHook,
		redactor: options.Redactor,
	}
	if options.RetryCoordinator != nil {
		retry := *options.RetryCoordinator
		retry.Store = options.Store
		dispatcher.retry = &retry
	}
	if waitStore, ok := options.Store.(WaitStore); ok {
		coordinator := WaitCoordinator{Store: waitStore}
		if options.WaitCoordinator != nil {
			coordinator.Scheduler = options.WaitCoordinator.Scheduler
			coordinator.Materializer = options.WaitCoordinator.Materializer
			coordinator.Authorizer = options.WaitCoordinator.Authorizer
		}
		dispatcher.waits = &coordinator
	}
	return dispatcher, nil
}

// Dispatch resolves, validates, starts, executes, and durably finishes one
// claimed invocation. Every adapter-side failure after StartNodeAttempt is
// converted into an unsuccessful FinishNodeAttempt before Dispatch returns.
func (d *StepDispatcher) Dispatch(ctx context.Context, request DispatchRequest) (DispatchResult, error) {
	if ctx == nil {
		return DispatchResult{}, fmt.Errorf("%w: context is required", ErrInvalidDispatch)
	}
	if d == nil || nilStateStore(d.store) || nilStepKindRegistry(d.registry) || d.now == nil {
		return DispatchResult{}, fmt.Errorf("%w: dispatcher is not initialized", ErrInvalidDispatch)
	}
	if err := validateDispatchClaim(request.Claim); err != nil {
		return DispatchResult{}, fmt.Errorf("%w: %w", ErrInvalidDispatch, err)
	}
	durableCtx := context.WithoutCancel(ctx)
	node, loadErr := d.store.LoadNodeInvocation(durableCtx, request.Claim.Candidate.InvocationID)
	if loadErr != nil {
		return DispatchResult{}, dispatchError(DispatchStartAttempt, request, loadErr)
	}
	prestart := DispatchResult{Node: node}
	if contextErr := ctx.Err(); contextErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, contextErr)
	}
	if requestErr := validateDispatchRequest(request); requestErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: %w", ErrInvalidDispatch, requestErr))
	}

	kind, spec, err := stepkind.Resolve(d.registry, request.Node.Kind, request.Node.KindVersion)
	if err != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchError(DispatchResolve, request, err))
	}
	config, err := cloneGraphConfig(request.Node.Config)
	if err != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchError(DispatchValidateConfig, request, err))
	}
	if schemaErr := validateRuntimeObjectSchema(spec.ConfigSchema, config); schemaErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchValidationError(DispatchValidateConfig, request, schemaErr))
	}
	configForValidation, err := cloneGraphConfig(config)
	if err != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchError(DispatchValidateConfig, request, err))
	}
	diagnostics := kind.ValidateConfig(ctx, configForValidation)
	prestart.Diagnostics = cloneDiagnostics(diagnostics)
	if diagnosticsErr := validateAdapterDiagnostics(diagnostics); diagnosticsErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchValidationError(DispatchValidateConfig, request, diagnosticsErr))
	}

	inputs := values.ValueSet{}
	if node.Inputs != nil {
		inputs, err = d.store.LoadValues(ctx, *node.Inputs)
		if err != nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, dispatchError(DispatchValidateInput, request, err))
		}
	}
	if inputsErr := inputs.Validate(); inputsErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchValidationError(DispatchValidateInput, request, inputsErr))
	}
	if schemaErr := values.ValidateValueSetSchema(spec.InputSchema, inputs); schemaErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchValidationError(DispatchValidateInput, request, schemaErr))
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, contextErr)
	}

	claim := ClaimProof{
		Owner: request.Claim.Lease.Owner, Token: request.Claim.Lease.Token,
		Generation: request.Claim.Lease.Generation,
	}
	executor := ExecutorMetadata{
		Kind: request.Node.Kind, Version: spec.Version, Target: request.Target,
		Attributes: cloneDispatchStringMap(request.ExecutorAttributes),
	}
	started, resumed, err := d.startOrResumeAttempt(ctx, node, claim, executor)
	if err != nil {
		return DispatchResult{Node: node, Diagnostics: cloneDiagnostics(diagnostics)}, dispatchError(DispatchStartAttempt, request, err)
	}
	result := DispatchResult{Node: started.Node, Attempt: started.Attempt, Diagnostics: cloneDiagnostics(diagnostics)}

	invocation := stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{
			RunID: string(started.Attempt.ID.Invocation.RunID), NodeID: started.Attempt.ID.Invocation.NodeID,
			Iteration: started.Attempt.ID.Invocation.Iteration, Attempt: started.Attempt.ID.Number,
		},
		Config: config, Inputs: cloneValueSet(inputs), IdempotencyKey: request.IdempotencyKey,
	}
	if request.Node.Call != nil {
		call, callErr := cloneCallInvocation(request.Node.Call, request.CallLineage)
		if callErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, callErr)
		}
		invocation.Call = call
	}
	if resumed {
		if d.waits == nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, errors.New("resumed attempt requires durable wait continuation support"))
		}
		continuation, continuationErr := d.loadWaitContinuation(durableCtx, started.Attempt.ID)
		if continuationErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, continuationErr)
		}
		invocation.Continuation = continuation
	}
	if deadline, ok := ctx.Deadline(); ok {
		invocation.Deadline = deadline
	}
	if invocationErr := invocation.Validate(); invocationErr != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, invocationErr)
	}

	prepared := stepkind.PreparedInvocation{Invocation: invocation}
	if preparer, ok := kind.(stepkind.Preparer); ok {
		prepared, err = preparer.Prepare(ctx, cloneStepInvocation(invocation))
		if err != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchPrepare, failurePrepare, err)
		}
		if preparedErr := validatePreparedInvocation(invocation, prepared); preparedErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchPrepare, failurePrepare, preparedErr)
		}
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchExecute, failureExecute, contextErr)
	}

	stepResult, err := kind.Execute(ctx, prepared)
	if err != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchExecute, failureExecute, err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchExecute, failureExecute, contextErr)
	}
	if resultErr := stepResult.Validate(); resultErr != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchValidateOutput, failureInvalidResult, resultErr)
	}
	clonedResult := cloneStepResult(stepResult)
	switch stepResult.Outcome {
	case stepkind.StepWaiting:
		if !spec.CanSuspend || d.waits == nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, errors.New("step kind or runtime does not support generic suspension"))
		}
		suspended, suspendErr := d.waits.Suspend(durableCtx, SuspendCommand{
			Request: SuspendNodeWaitRequest{
				Wait:                   WaitSnapshot{Ref: WaitRef{ID: WaitID(stepResult.Wait.ID)}, Invocation: started.Node.ID, Record: stepResult.Wait.Record},
				ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
				Claim: claim, At: d.atOrAfter(started.Node.UpdatedAt),
			},
			ResumeToken: stepResult.Wait.ResumeToken,
		})
		if suspended.Wait.Generation > 0 {
			result.Node, result.Attempt = suspended.Node, suspended.Attempt
			result.Result, result.Wait = &clonedResult, cloneWaitSnapshot(&suspended.Wait)
			if suspendErr != nil {
				return result, dispatchError(DispatchSuspend, request, suspendErr)
			}
			return result, nil
		}
		if suspendErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, suspendErr)
		}
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, errors.New("wait suspension returned no durable wait"))
	case stepkind.StepExternal:
		if spec.Observation.Mode != stepkind.ObservationPoll {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, errors.New("step kind does not advertise external observation"))
		}
		suspended, suspendErr := d.store.SuspendExternalOperation(durableCtx, SuspendExternalOperationRequest{
			Operation:              ExternalOperationSnapshot{Attempt: started.Attempt.ID, Ref: *stepResult.External, Invocation: invocation, Status: stepkind.ObservationPending},
			ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
			Claim: claim, At: d.atOrAfter(started.Node.UpdatedAt),
		})
		if suspendErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, suspendErr)
		}
		result.Node, result.Attempt = suspended.Node, suspended.Attempt
		operation := suspended.Operation
		result.Result, result.External = &clonedResult, &operation
		return result, nil
	case stepkind.StepCompleted:
		// Continue below through typed output validation and terminal persistence.
	}
	if schemaErr := values.ValidateValueSetSchema(spec.OutputSchema, stepResult.Outputs); schemaErr != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchValidateOutput, failureInvalidResult, schemaErr)
	}
	if schemaErr := validateDeclaredNodeOutputs(request.Node.Outputs, stepResult.Outputs); schemaErr != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchValidateOutput, failureInvalidResult, schemaErr)
	}

	outputRef, err := SaveValuesWithRetention(durableCtx, d.store, d.retention, SaveValuesRequest{
		Owner: ValueOwner{
			Kind: "node-attempt-outputs", RunID: started.Attempt.ID.Invocation.RunID,
			Invocation: &started.Attempt.ID.Invocation, Attempt: &started.Attempt.ID,
		},
		Values: stepResult.Outputs,
	})
	if err != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchPersistOutput, failurePersistOutput, err)
	}
	finished, err := d.store.FinishNodeAttempt(durableCtx, FinishNodeAttemptRequest{
		InvocationID: started.Attempt.ID.Invocation, AttemptNumber: started.Attempt.ID.Number,
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: claim, AttemptStatus: NodeSucceeded, NextNodeStatus: NodeSucceeded,
		Outputs: &outputRef, At: d.atOrAfter(started.Attempt.StartedAt),
	})
	if err != nil {
		return result, dispatchError(DispatchFinishAttempt, request, err)
	}
	result.Node, result.Attempt = finished.Node, finished.Attempt
	result.Result, result.Outputs = &clonedResult, cloneValueSetRef(&outputRef)
	d.finalize(durableCtx, request, kind, prepared, stepResult, nil, &result)
	return result, nil
}

func (d *StepDispatcher) startOrResumeAttempt(
	ctx context.Context,
	node NodeInvocationSnapshot,
	claim ClaimProof,
	executor ExecutorMetadata,
) (StartNodeAttemptResult, bool, error) {
	at := d.atOrAfter(node.UpdatedAt)
	if node.LatestAttempt == 0 {
		started, err := d.store.StartNodeAttempt(ctx, StartNodeAttemptRequest{
			InvocationID: node.ID, ExpectedNodeGeneration: node.Generation,
			Claim: claim, Executor: executor, Inputs: node.Inputs, At: at,
		})
		return started, false, err
	}
	attempt, err := d.store.LoadAttempt(ctx, AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
	if err != nil {
		return StartNodeAttemptResult{}, false, err
	}
	if attempt.Status != NodeRunning || !attempt.FinishedAt.IsZero() {
		started, startErr := d.store.StartNodeAttempt(ctx, StartNodeAttemptRequest{
			InvocationID: node.ID, ExpectedNodeGeneration: node.Generation,
			Claim: claim, Executor: executor, Inputs: node.Inputs, At: at,
		})
		return started, false, startErr
	}
	if attempt.Executor.Kind != executor.Kind || attempt.Executor.Version != executor.Version ||
		attempt.Executor.Target != executor.Target || !reflect.DeepEqual(attempt.Executor.Attributes, executor.Attributes) {
		return StartNodeAttemptResult{}, false, &AttemptConflictError{Invocation: node.ID, Attempt: attempt.ID.Number, Reason: "resumed attempt executor metadata differs from its immutable start metadata"}
	}
	transitioned, err := d.store.TransitionNode(ctx, NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: node.Generation, To: NodeRunning, Claim: &claim, At: at,
	})
	if err != nil {
		return StartNodeAttemptResult{}, false, err
	}
	return StartNodeAttemptResult{Node: transitioned.Snapshot, Attempt: attempt}, true, nil
}

func (d *StepDispatcher) loadWaitContinuation(ctx context.Context, id AttemptID) (*stepkind.WaitContinuation, error) {
	snapshot, err := d.waits.Store.LoadWaitContinuation(ctx, id)
	if err != nil {
		return nil, err
	}
	if snapshot.Status != WaitResumed || snapshot.ResumeValues == nil {
		return nil, fmt.Errorf("resumed attempt wait is not a resolved payload-bearing continuation")
	}
	resumed, err := d.store.LoadValues(ctx, *snapshot.ResumeValues)
	if err != nil {
		return nil, err
	}
	continuation := &stepkind.WaitContinuation{ID: string(snapshot.Ref.ID), Record: snapshot.Record, Values: cloneValueSet(resumed)}
	if err := continuation.Validate(); err != nil {
		return nil, err
	}
	return continuation, nil
}

func nilStepKindRegistry(registry stepkind.Registry) bool {
	if registry == nil {
		return true
	}
	value := reflect.ValueOf(registry)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (d *StepDispatcher) releasePrestartClaim(
	ctx context.Context,
	request DispatchRequest,
	result DispatchResult,
	dispatchErr error,
) (DispatchResult, error) {
	releaseAt := d.atOrAfter(result.Node.UpdatedAt)
	releaseErr := d.store.ReleaseNodeClaim(ctx, ReleaseClaimRequest{
		InvocationID: request.Claim.Candidate.InvocationID,
		Owner:        request.Claim.Lease.Owner,
		Token:        request.Claim.Lease.Token,
		Generation:   request.Claim.Lease.Generation,
		Now:          releaseAt,
	})
	if releaseErr != nil {
		return result, errors.Join(dispatchErr, fmt.Errorf("release pre-start claim: %w", releaseErr))
	}
	if released, err := d.store.LoadNodeInvocation(ctx, request.Claim.Candidate.InvocationID); err == nil {
		result.Node = released
	}
	return result, dispatchErr
}

func (d *StepDispatcher) finishFailure(
	ctx context.Context,
	request DispatchRequest,
	spec stepkind.StepKindSpec,
	kind stepkind.StepKind,
	result DispatchResult,
	claim ClaimProof,
	prepared stepkind.PreparedInvocation,
	produced stepkind.StepResult,
	stage DispatchStage,
	code string,
	cause error,
) (DispatchResult, error) {
	failure, attemptStatus := executionFailure(code, cause)
	next := attemptStatus
	var policyErr error
	retryHandled := d.retry != nil && request.Node.Retry != nil && (attemptStatus == NodeFailed || attemptStatus == NodeTimedOut || attemptStatus == NodeCrashed)
	if !retryHandled && d.disposition != nil && (attemptStatus == NodeFailed || attemptStatus == NodeTimedOut || attemptStatus == NodeCrashed) {
		candidate, err := d.disposition.NextNodeStatus(ctx, FailureDispositionRequest{
			Spec: spec, Attempt: result.Attempt, Failure: failure, Status: attemptStatus,
		})
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
	persistedFailure := maskDispatchFailure(failure, d.redactor)
	if retryHandled {
		timeoutKind := TimeoutKind(persistedFailure.Details["timeout_kind"])
		if !timeoutKind.Valid() {
			timeoutKind = ""
		}
		at := d.atOrAfter(result.Attempt.StartedAt)
		if at.Before(result.Node.UpdatedAt) {
			at = result.Node.UpdatedAt
		}
		scheduled, decision, retryErr := d.retry.Schedule(ctx, ScheduleRetryCommand{
			Node: request.Node, Spec: spec, NodeSnapshot: result.Node, Attempt: result.Attempt,
			Claim: claim, Failure: persistedFailure, AttemptStatus: attemptStatus,
			Timeout: timeoutKind, IdempotencyKey: request.IdempotencyKey, At: at,
		})
		if decision.Retry && scheduled.Activation.ID != "" {
			result.Node, result.Attempt = scheduled.Node, scheduled.Attempt
			d.finalize(ctx, request, kind, prepared, produced, cause, &result)
			return result, dispatchError(stage, request, errors.Join(cause, retryErr))
		}
		if retryErr != nil {
			policyErr = errors.Join(policyErr, retryErr)
		}
	}
	finished, finishErr := d.store.FinishNodeAttempt(ctx, FinishNodeAttemptRequest{
		InvocationID: result.Attempt.ID.Invocation, AttemptNumber: result.Attempt.ID.Number,
		ExpectedNodeGeneration: result.Node.Generation, ExpectedAttemptGeneration: result.Attempt.Generation,
		Claim: claim, AttemptStatus: attemptStatus, NextNodeStatus: next,
		Failure: &persistedFailure, At: d.atOrAfter(result.Attempt.StartedAt),
	})
	if finishErr != nil {
		return result, dispatchError(DispatchFinishAttempt, request, errors.Join(cause, policyErr, finishErr))
	}
	result.Node, result.Attempt = finished.Node, finished.Attempt
	d.finalize(ctx, request, kind, prepared, produced, cause, &result)
	returnCause := errors.Join(cause, policyErr)
	if stage == DispatchValidateOutput {
		returnCause = errors.Join(ErrStepValidation, returnCause)
	}
	return result, dispatchError(stage, request, returnCause)
}

// NodeOutputValidationError preserves the graph source of one declared output
// while unwrapping the canonical values-layer schema or shape error.
type NodeOutputValidationError struct {
	Output string
	Source *graph.SourceRef
	Cause  error
}

func (e *NodeOutputValidationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	location := ""
	if e.Source != nil {
		location = fmt.Sprintf(" at %s:%d", e.Source.Locator, e.Source.StartLine)
	}
	return fmt.Sprintf("declared node output %q%s: %v", e.Output, location, e.Cause)
}

func (e *NodeOutputValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func validateDeclaredNodeOutputs(declarations []graph.OutputSpec, output values.ValueSet) error {
	if len(declarations) == 0 {
		return nil
	}
	declared := make(map[string]graph.OutputSpec, len(declarations))
	for _, declaration := range declarations {
		if declaration.Name == "" {
			return &NodeOutputValidationError{Cause: errors.New("output declaration name is required")}
		}
		if _, duplicate := declared[declaration.Name]; duplicate {
			return &NodeOutputValidationError{Output: declaration.Name, Source: cloneDispatchSource(declaration.Source), Cause: errors.New("duplicate output declaration")}
		}
		declared[declaration.Name] = declaration
	}
	expected := make([]string, 0, len(declared))
	for name := range declared {
		expected = append(expected, name)
	}
	sort.Strings(expected)
	for _, name := range expected {
		declaration := declared[name]
		value, ok := output[name]
		if !ok {
			return &NodeOutputValidationError{Output: name, Source: cloneDispatchSource(declaration.Source), Cause: errors.New("required declared output is missing")}
		}
		if err := values.ValidateValueSchema(declaration.Schema, value); err != nil {
			return &NodeOutputValidationError{Output: name, Source: cloneDispatchSource(declaration.Source), Cause: err}
		}
	}
	actual := make([]string, 0, len(output))
	for name := range output {
		actual = append(actual, name)
	}
	sort.Strings(actual)
	for _, name := range actual {
		if _, ok := declared[name]; !ok {
			return &NodeOutputValidationError{Output: name, Cause: errors.New("executor produced an undeclared output")}
		}
	}
	return nil
}

func cloneDispatchSource(source *graph.SourceRef) *graph.SourceRef {
	if source == nil {
		return nil
	}
	copySource := *source
	copySource.Path = append([]string(nil), source.Path...)
	return &copySource
}

func (d *StepDispatcher) finalize(
	ctx context.Context,
	request DispatchRequest,
	kind stepkind.StepKind,
	prepared stepkind.PreparedInvocation,
	result stepkind.StepResult,
	executionErr error,
	dispatch *DispatchResult,
) {
	finalizer, ok := kind.(stepkind.Finalizer)
	if !ok {
		return
	}
	if err := finalizer.Finalize(ctx, stepkind.Finalization{
		Invocation: prepared, Result: result, ExecutionError: executionErr,
	}); err != nil {
		failure := maskDispatchFailure(Failure{
			Code: failureFinalize, Message: dispatchFailureMessage(failureFinalize), Details: map[string]string{"stage": string(DispatchFinalize)},
		}, d.redactor)
		warning := DispatchWarning{Stage: DispatchFinalize, Failure: failure, Cause: err}
		event, eventErr := AppendMaskedEvent(ctx, d.store, AppendEventRequest{
			RunID:      dispatch.Attempt.ID.Invocation.RunID,
			Invocation: &dispatch.Attempt.ID.Invocation,
			Attempt:    &dispatch.Attempt.ID,
			Type:       EventNodeFinalizeWarning, OccurredAt: d.atOrAfter(dispatch.Attempt.UpdatedAt),
			Attributes: map[string]string{
				"code": failure.Code, "message": failure.Message,
				"kind": request.Node.Kind, "version": dispatch.Attempt.Executor.Version,
			},
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		}, d.redactor)
		if eventErr == nil {
			warning.Event = &event
		} else {
			warning.Cause = errors.Join(err, fmt.Errorf("append finalize warning: %w", eventErr))
		}
		dispatch.Warnings = append(dispatch.Warnings, warning)
	}
}

func (d *StepDispatcher) atOrAfter(floor time.Time) time.Time {
	now := d.now().UTC()
	if now.IsZero() || now.Before(floor) {
		return floor
	}
	return now
}

func dispatchError(stage DispatchStage, request DispatchRequest, cause error) error {
	return &DispatchError{
		Stage: stage, Invocation: request.Claim.Candidate.InvocationID,
		Kind: request.Node.Kind, Version: request.Node.KindVersion, Cause: cause,
	}
}

func dispatchValidationError(stage DispatchStage, request DispatchRequest, cause error) error {
	return dispatchError(stage, request, fmt.Errorf("%w: %w", ErrStepValidation, cause))
}

func executionFailure(code string, err error) (Failure, NodeStatus) {
	status := NodeFailed
	if errors.Is(err, context.Canceled) {
		status = NodeCanceled
	} else if errors.Is(err, context.DeadlineExceeded) {
		status = NodeTimedOut
	}
	failure := Failure{
		Code: code, Message: dispatchFailureMessage(code), Retryable: stepkind.ClassifyError(err) == stepkind.Retryable,
		Details: map[string]string{"retry_classification": string(stepkind.ClassifyError(err))},
	}
	var structured *stepkind.ExecutionError
	if errors.As(err, &structured) && structured != nil && structured.Validate() == nil {
		failure.Code = structured.Code
		failure.Message = structured.Message
		failure.Retryable = structured.Classification == stepkind.Retryable
		failure.Details = cloneDispatchStringMap(structured.Details)
		if failure.Details == nil {
			failure.Details = make(map[string]string)
		}
		failure.Details["retry_classification"] = string(structured.Classification)
	}
	return failure, status
}

func maskDispatchFailure(failure Failure, redactor *values.Redactor) Failure {
	result := failure
	if redactor != nil {
		result.Message = redactor.MaskString(result.Message)
	}
	result.Details = cloneDispatchStringMap(failure.Details)
	for key, value := range result.Details {
		if redactor != nil {
			value = redactor.MaskString(value)
		}
		result.Details[key] = value
	}
	return result
}

func dispatchFailureMessage(code string) string {
	switch code {
	case failurePrepare:
		return "step preparation failed"
	case failureInvalidResult:
		return "step result validation failed"
	case failurePersistOutput:
		return "step output persistence failed"
	case failureFinalize:
		return "step finalization failed"
	default:
		return "step execution failed"
	}
}

func validateDispatchRequest(request DispatchRequest) error {
	if err := validateDispatchClaim(request.Claim); err != nil {
		return err
	}
	if request.Node.ID != request.Claim.Candidate.InvocationID.NodeID {
		return fmt.Errorf("graph node id %q does not match claimed node %q", request.Node.ID, request.Claim.Candidate.InvocationID.NodeID)
	}
	if strings.TrimSpace(request.Node.Kind) == "" || request.Node.Kind != strings.TrimSpace(request.Node.Kind) {
		return fmt.Errorf("graph node kind is required without surrounding whitespace")
	}
	if strings.TrimSpace(request.Node.KindVersion) == "" || request.Node.KindVersion != strings.TrimSpace(request.Node.KindVersion) {
		return fmt.Errorf("graph node kind_version is required without surrounding whitespace")
	}
	if request.Target != "" && request.Target != strings.TrimSpace(request.Target) {
		return fmt.Errorf("executor target must not contain surrounding whitespace")
	}
	if request.Node.Call == nil && len(request.CallLineage) != 0 {
		return fmt.Errorf("call lineage is valid only for a call node")
	}
	if request.Node.Call != nil && len(request.CallLineage) == 0 {
		return fmt.Errorf("call node requires authoritative definition lineage")
	}
	return validateDispatchStringMap("executor attributes", request.ExecutorAttributes)
}

func validateDispatchClaim(claim ReadyClaim) error {
	if err := claim.Candidate.InvocationID.Validate(); err != nil {
		return err
	}
	return claim.Lease.Validate()
}

func validateAdapterDiagnostics(findings []diagnostic.Diagnostic) error {
	var failures []error
	for i, finding := range findings {
		if err := finding.Validate(); err != nil {
			failures = append(failures, fmt.Errorf("adapter diagnostic[%d]: %w", i, err))
			continue
		}
		if finding.Severity == diagnostic.SeverityError {
			failures = append(failures, fmt.Errorf("%s: %s", finding.Code, finding.Message))
		}
	}
	return errors.Join(failures...)
}

func validatePreparedInvocation(want stepkind.Invocation, prepared stepkind.PreparedInvocation) error {
	if err := prepared.Invocation.Validate(); err != nil {
		return err
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		return err
	}
	gotJSON, err := json.Marshal(prepared.Invocation)
	if err != nil {
		return err
	}
	if !bytes.Equal(wantJSON, gotJSON) {
		return fmt.Errorf("prepared invocation changed immutable invocation fields")
	}
	return nil
}

func cloneGraphConfig(config graph.Config) (graph.Config, error) {
	if config == nil {
		return graph.Config{}, nil
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("config must be JSON-compatible: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var cloned graph.Config
	if err := decoder.Decode(&cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

func cloneValueSet(set values.ValueSet) values.ValueSet {
	encoded, err := json.Marshal(set)
	if err != nil {
		return nil
	}
	var cloned values.ValueSet
	if err := decodeDispatchJSONUseNumber(encoded, &cloned); err != nil {
		return nil
	}
	return cloned
}

func cloneStepResult(result stepkind.StepResult) stepkind.StepResult {
	cloned := stepkind.StepResult{Outcome: result.Outcome, Outputs: cloneValueSet(result.Outputs)}
	if result.Wait != nil {
		waitResult := *result.Wait
		encoded, err := json.Marshal(result.Wait.Record)
		if err == nil {
			_ = decodeDispatchJSONUseNumber(encoded, &waitResult.Record)
		}
		cloned.Wait = &waitResult
	}
	if result.External != nil {
		external := *result.External
		external.Metadata = cloneDispatchStringMap(result.External.Metadata)
		cloned.External = &external
	}
	return cloned
}

func cloneWaitSnapshot(snapshot *WaitSnapshot) *WaitSnapshot {
	if snapshot == nil {
		return nil
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return nil
	}
	var cloned WaitSnapshot
	if err := decodeDispatchJSONUseNumber(encoded, &cloned); err != nil {
		return nil
	}
	return &cloned
}

func cloneStepInvocation(invocation stepkind.Invocation) stepkind.Invocation {
	config, _ := cloneGraphConfig(invocation.Config)
	cloned := stepkind.Invocation{
		Identity: invocation.Identity, Config: config, Inputs: cloneValueSet(invocation.Inputs),
		IdempotencyKey: invocation.IdempotencyKey, Deadline: invocation.Deadline,
	}
	if invocation.Call != nil {
		cloned.Call, _ = cloneCallInvocation(&invocation.Call.Spec, invocation.Call.Lineage)
	}
	if invocation.Continuation != nil {
		continuation := *invocation.Continuation
		encoded, err := json.Marshal(invocation.Continuation.Record)
		if err == nil {
			_ = decodeDispatchJSONUseNumber(encoded, &continuation.Record)
		}
		continuation.Values = cloneValueSet(invocation.Continuation.Values)
		cloned.Continuation = &continuation
	}
	return cloned
}

func cloneCallInvocation(spec *graph.CallSpec, lineage []graph.DefinitionRef) (*stepkind.CallInvocation, error) {
	if spec == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(struct {
		Spec    graph.CallSpec        `json:"spec"`
		Lineage []graph.DefinitionRef `json:"lineage"`
	}{Spec: *spec, Lineage: lineage})
	if err != nil {
		return nil, fmt.Errorf("clone call invocation: %w", err)
	}
	var cloned stepkind.CallInvocation
	if err := decodeDispatchJSONUseNumber(encoded, &cloned); err != nil {
		return nil, fmt.Errorf("clone call invocation: %w", err)
	}
	return &cloned, nil
}

func decodeDispatchJSONUseNumber(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func cloneDiagnostics(findings []diagnostic.Diagnostic) []diagnostic.Diagnostic {
	if findings == nil {
		return nil
	}
	encoded, err := json.Marshal(findings)
	if err != nil {
		return nil
	}
	var cloned []diagnostic.Diagnostic
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return nil
	}
	return cloned
}

func cloneValueSetRef(ref *values.ValueSetRef) *values.ValueSetRef {
	if ref == nil {
		return nil
	}
	cloned := *ref
	return &cloned
}

func cloneDispatchStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func validateDispatchStringMap(name string, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.TrimSpace(key) == "" || key != strings.TrimSpace(key) {
			return fmt.Errorf("%s contains an empty or untrimmed key", name)
		}
		if !utf8.ValidString(key) || !utf8.ValidString(values[key]) {
			return fmt.Errorf("%s[%q] contains invalid UTF-8", name, key)
		}
	}
	return nil
}

func validateRuntimeObjectSchema(schema graph.Schema, object map[string]any) error {
	value, err := values.NewInline(object, values.Metadata{
		Producer:  values.Producer{Kind: "runtime-validation", Reference: "step-dispatch"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionNone,
	})
	if err != nil {
		return fmt.Errorf("runtime object is not JSON-compatible: %w", err)
	}
	return values.ValidateValueSchema(schema, value)
}
