package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
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
	DispatchVerify         DispatchStage = "verify"
	DispatchPersistVerify  DispatchStage = "persist_verification"
	DispatchPersistOutput  DispatchStage = "persist_output"
	DispatchReuseOutput    DispatchStage = "reuse_output"
	DispatchPublishMemo    DispatchStage = "publish_memo"
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
// durable terminal attempt, including finalization and memo publication.
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
	Node         NodeInvocationSnapshot
	Attempt      AttemptSnapshot
	Result       *stepkind.StepResult
	Outputs      *values.ValueSetRef
	Wait         *WaitSnapshot
	External     *ExternalOperationSnapshot
	Service      *ServiceSnapshot
	Verification *VerificationRecord
	Diagnostics  []diagnostic.Diagnostic
	Warnings     []DispatchWarning
	compensation *dispatchCompensation
}

type dispatchCompensation struct {
	planDigest string
	handler    string
	evidence   stepkind.ReversibilityEvidence
}

// StepDispatcher executes claimed nodes exclusively through a step-kind
// registry and the atomic StateStore attempt lifecycle.
type StepDispatcher struct {
	store        StateStore
	registry     stepkind.Registry
	now          func() time.Time
	disposition  FailureDisposition
	retention    RetentionHook
	redactor     *values.Redactor
	waits        *WaitCoordinator
	retry        *RetryCoordinator
	verifiers    verification.Registry
	memo         MemoStore
	pins         PinStore
	reuse        OutputReuseStore
	reusePolicy  ReuseAuthorizer
	services     ServiceStore
	compensation CompensationStore
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
	// Verifiers defaults to the deterministic core registry when nil. A
	// supplied typed-nil registry is rejected rather than silently defaulted.
	Verifiers verification.Registry
	// ReuseAuthorizer is required for pins, private cache entries, and every
	// materialize-effect memoization decision.
	ReuseAuthorizer ReuseAuthorizer
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
	if options.Verifiers == nil {
		options.Verifiers = verification.NewDefaultRegistry()
	} else if nilVerificationRegistry(options.Verifiers) {
		return nil, fmt.Errorf("%w: verification registry is typed nil", ErrInvalidDispatch)
	}
	frozenVerifiers, err := verification.SnapshotRegistry(options.Verifiers)
	if err != nil {
		return nil, fmt.Errorf("%w: snapshot verification registry: %w", ErrInvalidDispatch, err)
	}
	dispatcher.verifiers = frozenVerifiers
	if memo, ok := options.Store.(MemoStore); ok {
		dispatcher.memo = memo
	}
	if pins, ok := options.Store.(PinStore); ok {
		dispatcher.pins = pins
	}
	if reuse, ok := options.Store.(OutputReuseStore); ok {
		dispatcher.reuse = reuse
	}
	if options.ReuseAuthorizer != nil && nilReflect(options.ReuseAuthorizer) {
		return nil, fmt.Errorf("%w: reuse authorizer is typed nil", ErrInvalidDispatch)
	}
	dispatcher.reusePolicy = options.ReuseAuthorizer
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
	if serviceStore, ok := options.Store.(ServiceStore); ok {
		dispatcher.services = serviceStore
	}
	if compensation, ok := options.Store.(CompensationStore); ok {
		dispatcher.compensation = compensation
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
	if node.Phase == InvocationCompensation {
		if d.compensation == nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: compensation store is required for handler invocation", ErrInvalidCompensation))
		}
		entry, entryErr := d.compensation.LoadCompensationEntryByHandler(durableCtx, node.ID)
		if entryErr != nil || entry.Handler != node.ID || graph.NormalizeID(request.Node.ID) != graph.NormalizeID(entry.Handler.NodeID) {
			return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: compensation handler identity: %w", ErrInvalidCompensation, entryErr))
		}
		request.IdempotencyKey = "compensation:" + entry.ID
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, contextErr)
	}
	if requestErr := validateDispatchRequest(request); requestErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: %w", ErrInvalidDispatch, requestErr))
	}

	kind, spec, err := stepkind.Resolve(d.registry, request.Node.Kind, request.Node.KindVersion)
	if err != nil {
		return d.failCompensationPrestart(durableCtx, request, prestart, DispatchResolve, err)
	}
	if node.Phase == InvocationCompensation && request.Node.Call != nil && request.Node.Call.Mode != graph.CallInline {
		return d.failCompensationPrestart(durableCtx, request, prestart, DispatchValidateConfig, errors.New("compensation call handler must complete inline before the ledger can succeed"))
	}
	config, err := cloneGraphConfig(request.Node.Config)
	if err != nil {
		return d.failCompensationPrestart(durableCtx, request, prestart, DispatchValidateConfig, err)
	}
	if schemaErr := validateRuntimeObjectSchema(spec.ConfigSchema, config); schemaErr != nil {
		return d.failCompensationPrestart(durableCtx, request, prestart, DispatchValidateConfig, schemaErr)
	}
	configForValidation, err := cloneGraphConfig(config)
	if err != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchError(DispatchValidateConfig, request, err))
	}
	diagnostics := kind.ValidateConfig(ctx, configForValidation)
	if diagnosticsErr := validateAdapterDiagnostics(diagnostics); diagnosticsErr != nil {
		prestart.Diagnostics = cloneDiagnostics(diagnostics)
		return d.failCompensationPrestart(durableCtx, request, prestart, DispatchValidateConfig, diagnosticsErr)
	}
	verificationDiagnostics := verification.ValidateSpec(ctx, d.verifiers, request.Node.Verification)
	diagnostics = append(diagnostics, verificationDiagnostics...)
	prestart.Diagnostics = cloneDiagnostics(diagnostics)
	if diagnosticsErr := verificationDiagnosticsError(verificationDiagnostics); diagnosticsErr != nil {
		return d.failCompensationPrestart(durableCtx, request, prestart, DispatchValidateConfig, diagnosticsErr)
	}
	if request.Node.Compensation != nil {
		if node.Phase != InvocationForward || d.compensation == nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: compensable forward execution requires durable compensation", ErrInvalidCompensation))
		}
		if spec.CanSuspend || spec.Observation.Mode != stepkind.ObservationNone || spec.Lifecycle.Service || request.Node.Service != nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: suspension, external observation, or service lifecycle has no atomic compensation receipt boundary", ErrInvalidCompensation))
		}
		provider, ok := kind.(stepkind.ReversibilityProvider)
		if !ok || spec.Compensation != stepkind.CompensationReceiptRequired {
			return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: kind does not provide reversibility evidence", ErrInvalidCompensation))
		}
		configForEvidence, cloneErr := cloneGraphConfig(config)
		if cloneErr != nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: isolate reversibility config: %w", ErrInvalidCompensation, cloneErr))
		}
		evidence, evidenceErr := stepkind.ResolveReversibility(ctx, provider, stepkind.ReversibilityRequest{Config: configForEvidence, Call: request.Node.Call})
		if evidenceErr != nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, fmt.Errorf("%w: describe reversibility: %w", ErrInvalidCompensation, evidenceErr))
		}
		if _, evidenceErr = CompensationEvidenceDigest(evidence); evidenceErr != nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, evidenceErr)
		}
		run, runErr := d.store.LoadRun(durableCtx, node.ID.RunID)
		if runErr != nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, runErr)
		}
		prestart.compensation = &dispatchCompensation{planDigest: run.Plan.Digest, handler: request.Node.Compensation.Handler, evidence: evidence}
	}

	inputs := values.ValueSet{}
	if node.Inputs != nil {
		inputs, err = d.store.LoadValues(ctx, *node.Inputs)
		if err != nil {
			return d.releasePrestartClaim(durableCtx, request, prestart, dispatchError(DispatchValidateInput, request, err))
		}
	}
	if inputsErr := inputs.Validate(); inputsErr != nil {
		return d.failCompensationPrestart(durableCtx, request, prestart, DispatchValidateInput, inputsErr)
	}
	if schemaErr := values.ValidateValueSetSchema(spec.InputSchema, inputs); schemaErr != nil {
		return d.failCompensationPrestart(durableCtx, request, prestart, DispatchValidateInput, schemaErr)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return d.releasePrestartClaim(durableCtx, request, prestart, contextErr)
	}

	claim := ClaimProof{
		Owner: request.Claim.Lease.Owner, Token: request.Claim.Lease.Token,
		Generation: request.Claim.Lease.Generation,
	}
	var reused DispatchResult
	var handled bool
	var reuseDiagnostics []diagnostic.Diagnostic
	var reuseErr error
	if request.Node.Compensation == nil && node.Phase != InvocationCompensation {
		reused, handled, reuseDiagnostics, reuseErr = d.tryReuseOutputs(ctx, durableCtx, request, node, spec, claim)
	}
	diagnostics = append(diagnostics, reuseDiagnostics...)
	if reuseErr != nil {
		prestart.Diagnostics = cloneDiagnostics(diagnostics)
		return d.releasePrestartClaim(durableCtx, request, prestart, dispatchError(DispatchReuseOutput, request, reuseErr))
	}
	if handled {
		reused.Diagnostics = cloneDiagnostics(diagnostics)
		return reused, nil
	}
	executor := ExecutorMetadata{
		Kind: request.Node.Kind, Version: spec.Version, Target: request.Target,
		Attributes: cloneDispatchStringMap(request.ExecutorAttributes),
	}
	started, resumed, err := d.startOrResumeAttempt(ctx, node, claim, executor)
	if err != nil {
		return DispatchResult{Node: node, Diagnostics: cloneDiagnostics(diagnostics)}, dispatchError(DispatchStartAttempt, request, err)
	}
	result := DispatchResult{Node: started.Node, Attempt: started.Attempt, Diagnostics: cloneDiagnostics(diagnostics), compensation: prestart.compensation}

	invocation := stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{
			RunID: string(started.Attempt.ID.Invocation.RunID), NodeID: started.Attempt.ID.Invocation.NodeID,
			Iteration: started.Attempt.ID.Invocation.Iteration, Attempt: started.Attempt.ID.Number,
		},
		Config: config, Inputs: cloneValueSet(inputs), IdempotencyKey: request.IdempotencyKey,
	}
	if result.compensation != nil {
		evidence := result.compensation.evidence
		invocation.Compensation = &evidence
	}
	if request.Node.Verification != nil {
		invocation.Verification, err = cloneVerificationSpec(request.Node.Verification)
		if err != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, err)
		}
		invocation.Activity = verification.NewActivityRecorder()
	}
	if request.Node.Call != nil {
		callSpec, resolveErr := resolveDynamicCallSpec(request.Node.Call, inputs)
		if resolveErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, resolveErr)
		}
		call, callErr := cloneCallInvocation(callSpec, request.CallLineage)
		if callErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, callErr)
		}
		invocation.Call = call
	}
	if request.Node.Service != nil && request.Node.Service.TeardownOf != "" {
		if d.services == nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, errors.New("service teardown requires durable service support"))
		}
		startID := NodeInvocationID{RunID: started.Node.ID.RunID, NodeID: request.Node.Service.TeardownOf, Iteration: started.Node.ID.Iteration}
		service, serviceErr := d.services.LoadService(durableCtx, startID)
		if errors.Is(serviceErr, ErrNotFound) {
			startNode, startErr := d.store.LoadNodeInvocation(durableCtx, startID)
			if startErr != nil {
				serviceErr = startErr
			} else if startNode.LatestAttempt == 0 {
				// Absence is a safe no-op only when durable node history proves
				// provider Start was never issued. A missing service record after
				// any attempt is an integrity failure, not evidence of absence.
				invocation.Service = &stepkind.ServiceBinding{Phase: stepkind.ServiceTeardown, Absent: true}
				serviceErr = nil
			} else {
				serviceErr = fmt.Errorf("%w: service start has attempt history but no durable launch intent", ErrInvalidService)
			}
		}
		if serviceErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, stepkind.PreparedInvocation{Invocation: invocation}, stepkind.StepResult{}, DispatchPrepare, failurePrepare, serviceErr)
		}
		if invocation.Service == nil {
			invocation.Service = &stepkind.ServiceBinding{
				Phase:           stepkind.ServiceTeardown,
				StartInvocation: service.Invocation.Identity,
				Ref:             cloneExternalRef(service.Ref),
			}
		}
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
		// Preparation may normalize adapter-owned config but is not an activity
		// boundary. Withhold the runtime-issued recorder so a preparer cannot
		// inject literal evidence or freeze recording before Execute starts.
		prepareInput := cloneStepInvocation(invocation)
		prepareInput.Activity = nil
		prepared, err = preparer.Prepare(ctx, prepareInput)
		if err != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchPrepare, failurePrepare, err)
		}
		if preparedErr := validatePreparedInvocation(prepareInput, prepared); preparedErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchPrepare, failurePrepare, preparedErr)
		}
		prepared.Invocation.Activity = invocation.Activity
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchExecute, failureExecute, contextErr)
	}
	serviceLaunchPending := request.Node.Service != nil && request.Node.Service.TeardownOf == ""
	if serviceLaunchPending {
		if d.services == nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchPrepare, failurePrepare, errors.New("service start requires durable service support"))
		}
		readyCheck, cloneErr := cloneVerificationSpec(request.Node.Service.ReadyCheck)
		if cloneErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchPrepare, failurePrepare, cloneErr)
		}
		_, prepareErr := d.services.PrepareServiceStart(durableCtx, PrepareServiceStartRequest{
			Service:                ServiceSnapshot{Start: started.Attempt.ID, Invocation: cloneStepInvocation(prepared.Invocation), Status: ServiceLaunching, HeartbeatTimeout: request.Node.Service.HeartbeatTimeout, ReadyCheck: readyCheck},
			ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, At: d.atOrAfter(started.Node.UpdatedAt),
		})
		if prepareErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepkind.StepResult{}, DispatchPrepare, failurePrepare, prepareErr)
		}
	}

	stepResult, err := kind.Execute(ctx, prepared)
	if err != nil {
		if result.compensation == nil && stepResult.Compensation != nil {
			return d.finishIndeterminateCompensable(durableCtx, request, kind, result, claim, prepared, stepResult, "unrequested_receipt", errors.Join(err, errors.New("step returned an unrequested compensation receipt")))
		}
		if serviceLaunchPending {
			return result, dispatchError(DispatchExecute, request, err)
		}
		if result.compensation != nil && stepResult.Outcome == stepkind.StepCompleted && stepResult.Compensation == nil {
			return d.finishIndeterminateCompensable(durableCtx, request, kind, result, claim, prepared, stepResult, "missing_receipt", errors.Join(err, errors.New("compensable effect reported completion without a compensation receipt")))
		}
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchExecute, failureExecute, err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		if result.compensation == nil && stepResult.Compensation != nil {
			return d.finishIndeterminateCompensable(durableCtx, request, kind, result, claim, prepared, stepResult, "unrequested_receipt", errors.Join(contextErr, errors.New("step returned an unrequested compensation receipt")))
		}
		if serviceLaunchPending {
			return result, dispatchError(DispatchExecute, request, contextErr)
		}
		if result.compensation != nil && stepResult.Outcome == stepkind.StepCompleted && stepResult.Compensation == nil {
			return d.finishIndeterminateCompensable(durableCtx, request, kind, result, claim, prepared, stepResult, "missing_receipt", errors.Join(contextErr, errors.New("compensable effect reported completion without a compensation receipt")))
		}
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchExecute, failureExecute, contextErr)
	}
	if resultErr := stepResult.Validate(); resultErr != nil {
		if result.compensation == nil && stepResult.Compensation != nil {
			return d.finishIndeterminateCompensable(durableCtx, request, kind, result, claim, prepared, stepResult, "unrequested_receipt", errors.Join(resultErr, errors.New("step returned an unrequested compensation receipt")))
		}
		if serviceLaunchPending {
			return result, dispatchError(DispatchValidateOutput, request, resultErr)
		}
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchValidateOutput, failureInvalidResult, resultErr)
	}
	if result.compensation == nil && stepResult.Compensation != nil {
		return d.finishIndeterminateCompensable(durableCtx, request, kind, result, claim, prepared, stepResult, "unrequested_receipt", errors.New("step returned an unrequested compensation receipt"))
	}
	if result.compensation != nil && stepResult.Compensation == nil {
		return d.finishIndeterminateCompensable(durableCtx, request, kind, result, claim, prepared, stepResult, "missing_receipt", errors.New("compensable effect completed without a compensation receipt"))
	}
	if result.compensation != nil {
		if receiptErr := validateAppliedCompensationReceipt(result.compensation.evidence, *stepResult.Compensation); receiptErr != nil {
			return d.finishIndeterminateCompensable(durableCtx, request, kind, result, claim, prepared, stepResult, "unverified_receipt", errors.Join(receiptErr, errors.New("compensable effect returned an unverified receipt")))
		}
		if persistableErr := values.ValidatePersistableSet(stepResult.Outputs); persistableErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchValidateOutput, failureInvalidResult, fmt.Errorf("applied compensable outputs are not persistable: %w", persistableErr))
		}
	}
	if invocation.Verification != nil && (stepResult.Outcome == stepkind.StepWaiting || stepResult.Outcome == stepkind.StepExternal) {
		// Activity evidence is intentionally process-local. A suspension may
		// continue this attempt in another process, so accepting activity before
		// the durable boundary would silently discard evidence. Empty segments
		// may suspend; the resumed/observed terminal segment is verified later.
		evidence, freezeErr := invocation.Activity.Freeze()
		if freezeErr != nil || len(evidence) != 0 {
			cause := freezeErr
			if cause == nil {
				cause = errors.New("verified suspension cannot carry pre-suspension activity evidence")
			}
			invalid := &stepkind.ExecutionError{
				Code: "verification_evidence_not_durable", Message: verificationFailureMessage("verification_evidence_not_durable"),
				Classification: stepkind.RetryPermanent, Cause: cause,
			}
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchVerify, failureInvalidResult, invalid)
		}
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
		if request.Node.Service != nil {
			if d.services == nil {
				return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, errors.New("service node requires durable service support"))
			}
			if request.Node.Service.TeardownOf == "" {
				readyCheck, cloneErr := cloneVerificationSpec(request.Node.Service.ReadyCheck)
				if cloneErr != nil {
					return result, dispatchError(DispatchSuspend, request, cloneErr)
				}
				suspended, suspendErr := d.services.SuspendServiceStart(durableCtx, SuspendServiceStartRequest{
					Service:                ServiceSnapshot{Start: started.Attempt.ID, Invocation: cloneStepInvocation(prepared.Invocation), Ref: cloneExternalRef(*stepResult.External), Status: ServiceStarting, HeartbeatTimeout: request.Node.Service.HeartbeatTimeout, ReadyCheck: readyCheck},
					ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, At: d.atOrAfter(started.Node.UpdatedAt),
				})
				if suspendErr != nil {
					// Provider Start has already returned successfully and durable
					// launch intent predates that call. Preserve the running attempt
					// for same-key recovery; terminalizing it here could leak the
					// provider resource behind an ambiguous persistence failure.
					return result, dispatchError(DispatchSuspend, request, suspendErr)
				}
				result.Node, result.Attempt = suspended.Node, suspended.Attempt
				service := suspended.Service
				result.Result, result.Service = &clonedResult, &service
				return result, nil
			}
			if invocation.Service == nil || !reflect.DeepEqual(cloneExternalRef(*stepResult.External), invocation.Service.Ref) {
				return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, errors.New("service teardown returned a reference different from the durable start"))
			}
			startID := NodeInvocationID{RunID: started.Node.ID.RunID, NodeID: request.Node.Service.TeardownOf, Iteration: started.Node.ID.Iteration}
			service, loadErr := d.services.LoadService(durableCtx, startID)
			if loadErr != nil {
				return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, loadErr)
			}
			suspended, suspendErr := d.services.SuspendServiceTeardown(durableCtx, SuspendServiceTeardownRequest{
				Start: startID, Teardown: started.Attempt.ID, Invocation: cloneStepInvocation(prepared.Invocation), ExpectedServiceGeneration: service.Generation,
				ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
				Claim: claim, At: d.atOrAfter(serviceTimeFloor(started.Node.UpdatedAt, service.UpdatedAt)),
			})
			if suspendErr != nil {
				return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchSuspend, failureSuspend, suspendErr)
			}
			result.Node, result.Attempt = suspended.Node, suspended.Attempt
			service = suspended.Service
			result.Result, result.Service = &clonedResult, &service
			return result, nil
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
	if invocation.Verification != nil {
		evidence, freezeErr := invocation.Activity.Freeze()
		if freezeErr != nil {
			invalid := &stepkind.ExecutionError{
				Code: "verification_result_invalid", Message: verificationFailureMessage("verification_result_invalid"),
				Classification: stepkind.RetryPermanent, Cause: freezeErr,
			}
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchVerify, failureInvalidResult, invalid)
		}
		report, verificationErr := executeVerification(ctx, d.verifiers, *invocation.Verification, spec.OutputSchema, stepResult.Outputs, evidence)
		recorded, persistErr := persistVerification(
			durableCtx, d.store, d.retention, d.redactor, started.Attempt.ID, report,
			d.atOrAfter(started.Attempt.StartedAt),
		)
		if persistErr != nil {
			failure := &stepkind.ExecutionError{
				Code: "verification_persistence_failed", Message: verificationFailureMessage("verification_persistence_failed"),
				Classification: stepkind.RetryUnspecified, Cause: persistErr,
			}
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchPersistVerify, failurePersistOutput, failure)
		}
		result.Verification = &recorded
		if verificationErr != nil {
			return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchVerify, failureExecute, verificationExecutionError(verificationErr))
		}
	}
	publicOutputs, projectionErr := projectDeclaredNodeOutputs(request.Node, invocation, stepResult.Outputs)
	if projectionErr != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchValidateOutput, failureInvalidResult, projectionErr)
	}

	outputRef, err := SaveValuesWithRetention(durableCtx, d.store, d.retention, SaveValuesRequest{
		Owner: ValueOwner{
			Kind: "node-attempt-outputs", RunID: started.Attempt.ID.Invocation.RunID,
			Invocation: &started.Attempt.ID.Invocation, Attempt: &started.Attempt.ID,
		},
		Values: publicOutputs,
	})
	if err != nil {
		return d.finishFailure(durableCtx, request, spec, kind, result, claim, prepared, stepResult, DispatchPersistOutput, failurePersistOutput, err)
	}
	finishRequest := FinishNodeAttemptRequest{
		InvocationID: started.Attempt.ID.Invocation, AttemptNumber: started.Attempt.ID.Number,
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: claim, AttemptStatus: NodeSucceeded, NextNodeStatus: NodeSucceeded,
		Outputs: &outputRef, At: d.atOrAfter(started.Attempt.StartedAt),
	}
	var finished FinishNodeAttemptResult
	if result.compensation != nil {
		compensable, finishErr := d.compensation.FinishCompensableAttempt(durableCtx, FinishCompensableAttemptRequest{
			Finish: finishRequest,
			Eligibility: CompensationEligibility{PlanDigest: result.compensation.planDigest, HandlerNodeID: result.compensation.handler,
				Evidence: result.compensation.evidence, Receipt: *stepResult.Compensation, OriginalOutputs: stepResult.Outputs, ChildRunID: RunID(stepResult.Compensation.ChildRunID)},
		})
		finished, err = compensable.Finish, finishErr
	} else {
		finished, err = d.store.FinishNodeAttempt(durableCtx, finishRequest)
	}
	if err != nil {
		return result, dispatchError(DispatchFinishAttempt, request, err)
	}
	result.Node, result.Attempt = finished.Node, finished.Attempt
	result.Result, result.Outputs = &clonedResult, cloneValueSetRef(&outputRef)
	if fanOutErr := d.reconcileFanOutTerminal(durableCtx, finished.Node.ID, finished.Node.UpdatedAt); fanOutErr != nil {
		return result, dispatchError(DispatchFinishAttempt, request, &PostCommitError{Operation: "reconcile fan-out completion", Err: fanOutErr})
	}
	if request.Node.Memoization != nil && finished.Node.Phase != InvocationCompensation {
		if warning := d.publishMemoResult(durableCtx, request, spec, inputs, finished, outputRef); warning != nil {
			result.Warnings = append(result.Warnings, *warning)
		}
	}
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

// failCompensationPrestart converts an immutable handler contract failure into
// an ordinary terminal attempt. Releasing the claim would leave the active
// saga entry permanently Ready because the pinned graph cannot become valid on
// retry. Forward invocations retain the existing release-before-attempt path.
func (d *StepDispatcher) failCompensationPrestart(ctx context.Context, request DispatchRequest, result DispatchResult, stage DispatchStage, cause error) (DispatchResult, error) {
	dispatchErr := dispatchValidationError(stage, request, cause)
	if result.Node.Phase != InvocationCompensation {
		return d.releasePrestartClaim(ctx, request, result, dispatchErr)
	}
	claim := ClaimProof{Owner: request.Claim.Lease.Owner, Token: request.Claim.Lease.Token, Generation: request.Claim.Lease.Generation}
	started, _, startErr := d.startOrResumeAttempt(ctx, result.Node, claim, ExecutorMetadata{Kind: request.Node.Kind, Version: request.Node.KindVersion, Target: request.Target, Attributes: cloneDispatchStringMap(request.ExecutorAttributes)})
	if startErr != nil {
		return d.releasePrestartClaim(ctx, request, result, errors.Join(dispatchErr, startErr))
	}
	failure := Failure{Code: "compensation_handler_contract_invalid", Message: "compensation handler contract validation failed", Retryable: false, Details: map[string]string{"stage": string(stage), "retry_classification": string(stepkind.RetryPermanent)}}
	failure = maskDispatchFailure(failure, d.redactor)
	finished, finishErr := d.store.FinishNodeAttempt(ctx, FinishNodeAttemptRequest{InvocationID: started.Attempt.ID.Invocation, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, AttemptStatus: NodeFailed, NextNodeStatus: NodeFailed, Failure: &failure, At: d.atOrAfter(started.Attempt.StartedAt)})
	if finishErr != nil {
		return DispatchResult{Node: started.Node, Attempt: started.Attempt, Diagnostics: result.Diagnostics}, dispatchError(DispatchFinishAttempt, request, errors.Join(cause, finishErr))
	}
	return DispatchResult{Node: finished.Node, Attempt: finished.Attempt, Diagnostics: result.Diagnostics}, dispatchErr
}

func (d *StepDispatcher) finishIndeterminateCompensable(ctx context.Context, request DispatchRequest, kind stepkind.StepKind, result DispatchResult, claim ClaimProof, prepared stepkind.PreparedInvocation, produced stepkind.StepResult, marker string, cause error) (DispatchResult, error) {
	failure := Failure{Code: failureInvalidResult, Message: dispatchFailureMessage(failureInvalidResult), Retryable: false, Details: map[string]string{"retry_classification": string(stepkind.RetryPermanent), "effect_applied": marker}}
	failure = maskDispatchFailure(failure, d.redactor)
	finished, finishErr := d.store.FinishNodeAttempt(ctx, FinishNodeAttemptRequest{InvocationID: result.Attempt.ID.Invocation, AttemptNumber: result.Attempt.ID.Number, ExpectedNodeGeneration: result.Node.Generation, ExpectedAttemptGeneration: result.Attempt.Generation, Claim: claim, AttemptStatus: NodeFailed, NextNodeStatus: NodeFailed, Failure: &failure, At: d.atOrAfter(result.Attempt.StartedAt)})
	if finishErr != nil {
		return result, dispatchError(DispatchFinishAttempt, request, errors.Join(cause, finishErr))
	}
	result.Node, result.Attempt = finished.Node, finished.Attempt
	returnCause := cause
	if fanOutErr := d.reconcileFanOutTerminal(ctx, finished.Node.ID, finished.Node.UpdatedAt); fanOutErr != nil {
		returnCause = errors.Join(returnCause, &PostCommitError{Operation: "reconcile fan-out completion", Err: fanOutErr})
	}
	d.finalize(ctx, request, kind, prepared, produced, returnCause, &result)
	return result, dispatchError(DispatchValidateOutput, request, errors.Join(ErrStepValidation, returnCause))
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
	if result.compensation != nil && produced.Compensation != nil {
		return d.finishAppliedCompensationFailure(ctx, request, spec, kind, result, claim, prepared, produced, stage, code, cause)
	}
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
	if fanOutErr := d.reconcileFanOutTerminal(ctx, finished.Node.ID, finished.Node.UpdatedAt); fanOutErr != nil {
		policyErr = errors.Join(policyErr, &PostCommitError{Operation: "reconcile fan-out completion", Err: fanOutErr})
	}
	d.finalize(ctx, request, kind, prepared, produced, cause, &result)
	returnCause := errors.Join(cause, policyErr)
	if stage == DispatchValidateOutput {
		returnCause = errors.Join(ErrStepValidation, returnCause)
	}
	return result, dispatchError(stage, request, returnCause)
}

// finishAppliedCompensationFailure is the point-of-no-return fence for an
// adapter that reports a compensation receipt together with an execution
// error. The receipt means the forward effect may already exist: validation
// failures can never return the attempt to authored retry or leave it running.
func (d *StepDispatcher) finishAppliedCompensationFailure(
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
	receiptErr := validateAppliedCompensationReceipt(result.compensation.evidence, *produced.Compensation)
	resultErr := validateAppliedCompensationResult(spec.OutputSchema, produced)
	if receiptErr != nil {
		permanentCause := errors.Join(cause, fmt.Errorf("reported compensation receipt is invalid: %w", receiptErr))
		failure, _ := executionFailure(failureInvalidResult, permanentCause)
		failure.Retryable = false
		failure.Details["retry_classification"] = string(stepkind.RetryPermanent)
		failure.Details["effect_applied"] = "unverified_receipt"
		persisted := maskDispatchFailure(failure, d.redactor)
		finished, finishErr := d.store.FinishNodeAttempt(ctx, FinishNodeAttemptRequest{
			InvocationID: result.Attempt.ID.Invocation, AttemptNumber: result.Attempt.ID.Number,
			ExpectedNodeGeneration: result.Node.Generation, ExpectedAttemptGeneration: result.Attempt.Generation,
			Claim: claim, AttemptStatus: NodeFailed, NextNodeStatus: NodeFailed,
			Failure: &persisted, At: d.atOrAfter(result.Attempt.StartedAt),
		})
		if finishErr != nil {
			return result, dispatchError(DispatchFinishAttempt, request, errors.Join(permanentCause, finishErr))
		}
		result.Node, result.Attempt = finished.Node, finished.Attempt
		if fanOutErr := d.reconcileFanOutTerminal(ctx, finished.Node.ID, finished.Node.UpdatedAt); fanOutErr != nil {
			permanentCause = errors.Join(permanentCause, &PostCommitError{Operation: "reconcile fan-out completion", Err: fanOutErr})
		}
		d.finalize(ctx, request, kind, prepared, produced, permanentCause, &result)
		return result, dispatchError(DispatchValidateOutput, request, errors.Join(ErrStepValidation, permanentCause))
	}

	persistStage, persistCode, persistCause := stage, code, cause
	if resultErr != nil {
		persistStage, persistCode = DispatchValidateOutput, failureInvalidResult
		persistCause = errors.Join(cause, fmt.Errorf("applied compensable result is invalid: %w", resultErr))
	}
	failure, attemptStatus := executionFailure(persistCode, persistCause)
	if resultErr != nil {
		failure.Code = failureInvalidResult
		failure.Message = dispatchFailureMessage(failureInvalidResult)
		attemptStatus = NodeFailed
	}
	failure.Retryable = false
	if failure.Details == nil {
		failure.Details = make(map[string]string)
	}
	failure.Details["retry_classification"] = string(stepkind.RetryPermanent)
	failure.Details["effect_applied"] = "true"
	persisted := maskDispatchFailure(failure, d.redactor)
	timeoutKind := TimeoutKind(persisted.Details["timeout_kind"])
	if !timeoutKind.Valid() {
		timeoutKind = ""
	}
	attemptID := result.Attempt.ID
	typed, typedErr := NewFailureValue(attemptID.Invocation, &attemptID, attemptStatus, timeoutKind, persisted)
	if typedErr != nil {
		return result, dispatchError(DispatchFinishAttempt, request, errors.Join(persistCause, typedErr))
	}
	originalOutputs := produced.Outputs
	if values.ValidatePersistableSet(originalOutputs) != nil {
		// The receipt remains the truthful effect-applied boundary even when the
		// adapter's public output envelope itself cannot cross persistence.
		originalOutputs = nil
	}
	finished, finishErr := d.compensation.FinishCompensableAttempt(ctx, FinishCompensableAttemptRequest{
		Finish: FinishNodeAttemptRequest{
			InvocationID: attemptID.Invocation, AttemptNumber: attemptID.Number,
			ExpectedNodeGeneration: result.Node.Generation, ExpectedAttemptGeneration: result.Attempt.Generation,
			Claim: claim, AttemptStatus: attemptStatus, NextNodeStatus: attemptStatus,
			Failure: &persisted, At: d.atOrAfter(result.Attempt.StartedAt),
		},
		Eligibility: CompensationEligibility{PlanDigest: result.compensation.planDigest, HandlerNodeID: result.compensation.handler,
			Evidence: result.compensation.evidence, Receipt: *produced.Compensation, OriginalOutputs: originalOutputs,
			OriginalError: values.ValueSet{"error": typed}, ChildRunID: RunID(produced.Compensation.ChildRunID)},
	})
	if finishErr != nil {
		return result, dispatchError(DispatchFinishAttempt, request, errors.Join(persistCause, finishErr))
	}
	result.Node, result.Attempt = finished.Finish.Node, finished.Finish.Attempt
	if fanOutErr := d.reconcileFanOutTerminal(ctx, finished.Finish.Node.ID, finished.Finish.Node.UpdatedAt); fanOutErr != nil {
		persistCause = errors.Join(persistCause, &PostCommitError{Operation: "reconcile fan-out completion", Err: fanOutErr})
	}
	d.finalize(ctx, request, kind, prepared, produced, persistCause, &result)
	if persistStage == DispatchValidateOutput {
		persistCause = errors.Join(ErrStepValidation, persistCause)
	}
	return result, dispatchError(persistStage, request, persistCause)
}

func validateAppliedCompensationReceipt(evidence stepkind.ReversibilityEvidence, receipt stepkind.CompensationReceipt) error {
	probe := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}, Compensation: &receipt}
	if err := probe.Validate(); err != nil {
		return err
	}
	if receipt.Operation != evidence.Operation {
		return errors.New("receipt operation differs from admitted reversibility evidence")
	}
	if err := values.ValidateValueSetSchema(evidence.ReceiptSchema, receipt.Values); err != nil {
		return fmt.Errorf("receipt schema: %w", err)
	}
	return nil
}

func validateAppliedCompensationResult(schema graph.Schema, produced stepkind.StepResult) error {
	probe := produced
	probe.Compensation = nil
	if err := probe.Validate(); err != nil {
		return err
	}
	if err := values.ValidateValueSetSchema(schema, produced.Outputs); err != nil {
		return err
	}
	if err := values.ValidatePersistableSet(produced.Outputs); err != nil {
		return err
	}
	return nil
}

func (d *StepDispatcher) reconcileFanOutTerminal(ctx context.Context, child NodeInvocationID, at time.Time) error {
	if child.Iteration == "" {
		return nil
	}
	parent := NodeInvocationID{RunID: child.RunID, NodeID: child.NodeID}
	coordinator := FanOutCoordinator{Store: d.store, Retention: d.retention}
	if _, err := coordinator.ReconcileFailFast(ctx, parent, at); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	if _, _, _, err := coordinator.Collect(ctx, parent, at); err != nil && !errors.Is(err, ErrFanOutIncomplete) && !errors.Is(err, ErrCASMismatch) {
		return err
	}
	return nil
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

func projectDeclaredNodeOutputs(node graph.Node, invocation stepkind.Invocation, raw values.ValueSet) (values.ValueSet, error) {
	declarations := node.Outputs
	if len(declarations) == 0 {
		return cloneValueSet(raw), nil
	}
	context, err := nodeOutputExpressionContext(node, invocation, raw)
	if err != nil {
		return nil, err
	}
	declared := make(map[string]struct{}, len(declarations))
	projected := make(values.ValueSet, len(declarations))
	engine := values.NewExpressionEngine()
	reference := fmt.Sprintf("%s/attempt-%d", controlIdentity(NodeInvocationID{
		RunID: RunID(invocation.Identity.RunID), NodeID: invocation.Identity.NodeID, Iteration: invocation.Identity.Iteration,
	}), invocation.Identity.Attempt)
	for _, declaration := range declarations {
		if declaration.Name == "" {
			return nil, &NodeOutputValidationError{Source: cloneDispatchSource(declaration.Source), Cause: errors.New("output declaration name is required")}
		}
		if _, duplicate := declared[declaration.Name]; duplicate {
			return nil, &NodeOutputValidationError{Output: declaration.Name, Source: cloneDispatchSource(declaration.Source), Cause: errors.New("duplicate output declaration")}
		}
		declared[declaration.Name] = struct{}{}
		var value values.Value
		if declaration.Value == nil {
			rawValue, ok := raw[declaration.Name]
			if !ok {
				return nil, &NodeOutputValidationError{Output: declaration.Name, Source: cloneDispatchSource(declaration.Source), Cause: errors.New("same-name raw adapter output is missing")}
			}
			cloned := cloneValueSet(values.ValueSet{declaration.Name: rawValue})
			value, ok = cloned[declaration.Name]
			if !ok {
				return nil, &NodeOutputValidationError{Output: declaration.Name, Source: cloneDispatchSource(declaration.Source), Cause: errors.New("raw adapter output could not be cloned")}
			}
		} else {
			value, err = engine.EvaluateBinding(*declaration.Value, context, values.ExpressionOptions{VisibleSteps: []string{}}, bindingMetadata("node_output", reference, declaration.Name))
			if err != nil {
				return nil, &NodeOutputValidationError{Output: declaration.Name, Source: cloneDispatchSource(declaration.Source), Cause: err}
			}
		}
		if err := values.ValidateValueSchema(declaration.Schema, value); err != nil {
			return nil, &NodeOutputValidationError{Output: declaration.Name, Source: cloneDispatchSource(declaration.Source), Cause: err}
		}
		projected[declaration.Name] = value
	}
	if err := values.ValidatePersistableSet(projected); err != nil {
		return nil, err
	}
	return projected, nil
}

func nodeOutputExpressionContext(node graph.Node, invocation stepkind.Invocation, raw values.ValueSet) (values.ExpressionContext, error) {
	context := values.ExpressionContext{Inputs: invocation.Inputs, Outputs: raw}
	if node.ForEach == nil || invocation.Identity.Iteration == "" {
		return context, nil
	}
	itemName, indexName := node.ForEach.ItemName, node.ForEach.IndexName
	if itemName == "" {
		itemName = "item"
	}
	if indexName == "" {
		indexName = "index"
	}
	item, ok := invocation.Inputs[itemName]
	if !ok {
		return values.ExpressionContext{}, fmt.Errorf("%w: fan-out item input %q is missing during output projection", ErrInvalidDispatch, itemName)
	}
	indexValue, ok := invocation.Inputs[indexName]
	if !ok || indexValue.Type != values.TypeNumber {
		return values.ExpressionContext{}, fmt.Errorf("%w: fan-out index input %q is missing or not numeric during output projection", ErrInvalidDispatch, indexName)
	}
	number, ok := indexValue.Inline.(json.Number)
	if !ok {
		return values.ExpressionContext{}, fmt.Errorf("%w: fan-out index input %q has a noncanonical representation", ErrInvalidDispatch, indexName)
	}
	index, err := strconv.Atoi(number.String())
	if err != nil || index < 0 || FanOutIteration(index) != invocation.Identity.Iteration {
		return values.ExpressionContext{}, fmt.Errorf("%w: fan-out index input %q conflicts with invocation iteration", ErrInvalidDispatch, indexName)
	}
	context.Item, context.Index = &item, &index
	return context, nil
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
	if prepared.Invocation.Activity != want.Activity {
		return fmt.Errorf("prepared invocation changed the runtime-issued activity recorder")
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
	if result.Compensation != nil {
		cloned.Compensation = &stepkind.CompensationReceipt{Operation: result.Compensation.Operation, Values: cloneValueSet(result.Compensation.Values), ChildRunID: result.Compensation.ChildRunID}
	}
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
		Activity: invocation.Activity, IdempotencyKey: invocation.IdempotencyKey, Deadline: invocation.Deadline,
	}
	if invocation.Service != nil {
		service := *invocation.Service
		service.Ref = cloneExternalRef(invocation.Service.Ref)
		cloned.Service = &service
	}
	if invocation.Call != nil {
		cloned.Call, _ = cloneCallInvocation(&invocation.Call.Spec, invocation.Call.Lineage)
	}
	if invocation.Compensation != nil {
		evidence := *invocation.Compensation
		encoded, err := json.Marshal(invocation.Compensation.ReceiptSchema)
		if err == nil {
			_ = decodeDispatchJSONUseNumber(encoded, &evidence.ReceiptSchema)
		}
		cloned.Compensation = &evidence
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
	if invocation.Verification != nil {
		cloned.Verification, _ = cloneVerificationSpec(invocation.Verification)
	}
	return cloned
}

func cloneVerificationSpec(spec *graph.VerificationSpec) (*graph.VerificationSpec, error) {
	if spec == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("clone verification spec: %w", err)
	}
	var cloned graph.VerificationSpec
	if err := decodeDispatchJSONUseNumber(encoded, &cloned); err != nil {
		return nil, fmt.Errorf("clone verification spec: %w", err)
	}
	return &cloned, nil
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
