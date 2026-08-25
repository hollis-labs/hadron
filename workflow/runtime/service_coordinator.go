package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

// ServiceReconcileResult reports the durable record after one provider call.
type ServiceReconcileResult struct {
	Service  ServiceSnapshot
	Node     *NodeInvocationSnapshot
	Attempt  *AttemptSnapshot
	Outputs  *values.ValueSetRef
	Warnings []DispatchWarning
}

// ServiceCoordinatorOptions supplies only extraction-safe collaborators.
type ServiceCoordinatorOptions struct {
	Store         ServiceStore
	State         StateStore
	Registry      stepkind.Registry
	Plans         RecoveryPlanSource
	Control       ControlFlowStore
	Verifiers     verification.Registry
	RetentionHook RetentionHook
	Now           func() time.Time
}

// ServiceCoordinator observes readiness and liveness and executes durable
// stop intent. It never retains a process handle; recovery reconstructs every
// call from ServiceSnapshot and the frozen registry.
type ServiceCoordinator struct {
	store     ServiceStore
	state     StateStore
	registry  stepkind.Registry
	retention RetentionHook
	plans     RecoveryPlanSource
	control   ControlFlowStore
	verifiers verification.Registry
	now       func() time.Time
}

func NewServiceCoordinator(options ServiceCoordinatorOptions) (*ServiceCoordinator, error) {
	if options.Store == nil || nilReflect(options.Store) || nilStateStore(options.State) || nilStepKindRegistry(options.Registry) {
		return nil, fmt.Errorf("%w: service store, state store, and registry are required", ErrInvalidService)
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	if options.Plans == nil || nilReflect(options.Plans) || options.Control == nil || nilReflect(options.Control) {
		return nil, fmt.Errorf("%w: recovery plan source and control store are required", ErrInvalidService)
	}
	if options.Verifiers == nil {
		options.Verifiers = verification.NewDefaultRegistry()
	} else if nilVerificationRegistry(options.Verifiers) {
		return nil, fmt.Errorf("%w: verification registry is typed nil", ErrInvalidService)
	}
	frozen, err := verification.SnapshotRegistry(options.Verifiers)
	if err != nil {
		return nil, fmt.Errorf("%w: snapshot verification registry: %w", ErrInvalidService, err)
	}
	return &ServiceCoordinator{store: options.Store, state: options.State, registry: options.Registry, retention: options.RetentionHook, plans: options.Plans, control: options.Control, verifiers: frozen, now: now}, nil
}

// Recover reconciles active services in deterministic store order.
func (c *ServiceCoordinator) Recover(ctx context.Context, query ServiceQuery) ([]ServiceReconcileResult, error) {
	if ctx == nil || c == nil {
		return nil, fmt.Errorf("%w: initialized coordinator and context are required", ErrInvalidService)
	}
	services, err := c.store.RecoverServices(ctx, query)
	if err != nil {
		return nil, err
	}
	results := make([]ServiceReconcileResult, 0, len(services))
	var reconcileErrors []error
	for _, service := range services {
		result, reconcileErr := c.Reconcile(ctx, service.Start.Invocation)
		if result.Service.Generation > 0 {
			results = append(results, result)
		}
		if reconcileErr != nil {
			reconcileErrors = append(reconcileErrors, reconcileErr)
		}
	}
	return results, errors.Join(reconcileErrors...)
}

// Reconcile advances one durable service. Host stop is invoked only after the
// generated teardown attempt has atomically recorded ServiceStopping.
func (c *ServiceCoordinator) Reconcile(ctx context.Context, start NodeInvocationID) (ServiceReconcileResult, error) {
	if ctx == nil || c == nil {
		return ServiceReconcileResult{}, fmt.Errorf("%w: initialized coordinator and context are required", ErrInvalidService)
	}
	service, loadErr := c.store.LoadService(context.WithoutCancel(ctx), start)
	if loadErr != nil {
		return ServiceReconcileResult{}, loadErr
	}
	result := ServiceReconcileResult{Service: service}
	// Executor metadata is authoritative because service references never
	// duplicate the registered step-kind identity.
	attempt, loadErr := c.state.LoadAttempt(context.WithoutCancel(ctx), service.Start)
	if loadErr != nil {
		return result, loadErr
	}
	kind, spec, resolveErr := stepkind.Resolve(c.registry, attempt.Executor.Kind, attempt.Executor.Version)
	if resolveErr != nil {
		return result, resolveErr
	}
	controller, ok := kind.(stepkind.ServiceController)
	if !ok {
		return result, fmt.Errorf("%w: registered kind does not implement service lifecycle", ErrInvalidService)
	}
	if service.Status == ServiceLaunching {
		return c.recoverLaunch(ctx, service, kind)
	}
	if service.Status == ServiceStopping {
		if err := controller.RequestStop(ctx, cloneExternalRef(service.Ref), serviceStopKey(service.Start.Invocation)); err != nil {
			return result, safeServiceError("service stop request failed", err)
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
	}
	observation, observeErr := controller.ObserveService(ctx, cloneExternalRef(service.Ref))
	if observeErr != nil {
		return result, safeServiceError("service observation failed", observeErr)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := observation.Validate(); err != nil {
		return c.applyServiceFailure(context.WithoutCancel(ctx), service, errors.Join(ErrInvalidService, err))
	}
	now := c.now().UTC()
	if now.IsZero() {
		return result, fmt.Errorf("%w: service clock returned zero", ErrInvalidService)
	}
	heartbeatAt := time.Time{}
	if observation.Heartbeat {
		heartbeatAt = now
	}
	if service.Status == ServiceReady && service.HeartbeatTimeout != "" {
		timeout, _ := time.ParseDuration(string(service.HeartbeatTimeout))
		anchor := service.LastHeartbeatAt
		if now.Sub(anchor) > timeout && !observation.Heartbeat {
			return c.applyServiceFailure(context.WithoutCancel(ctx), service, errors.New("service heartbeat deadline exceeded"))
		}
	}
	switch service.Status {
	case ServiceStarting:
		return c.applyStarting(context.WithoutCancel(ctx), service, kind, spec, observation, heartbeatAt, now)
	case ServiceReady:
		if observation.State == stepkind.ServiceObservationFailed || observation.State == stepkind.ServiceObservationStopped {
			cause := errors.New("ready service became unavailable")
			if observation.Failure != nil {
				cause = observation.Failure
			}
			return c.applyServiceFailure(context.WithoutCancel(ctx), service, cause)
		}
		updated, applyErr := c.store.ApplyServiceHeartbeat(context.WithoutCancel(ctx), ApplyServiceHeartbeatRequest{Start: start, ExpectedServiceGeneration: service.Generation, ObservedAt: now, HeartbeatAt: heartbeatAt, At: now})
		result.Service = updated
		return result, applyErr
	case ServiceStopping:
		return c.applyStopping(context.WithoutCancel(ctx), service, kind, spec, observation, heartbeatAt, now)
	default:
		return result, nil
	}
}

func (c *ServiceCoordinator) recoverLaunch(ctx context.Context, service ServiceSnapshot, kind stepkind.StepKind) (ServiceReconcileResult, error) {
	node, nodeErr := c.state.LoadNodeInvocation(context.WithoutCancel(ctx), service.Start.Invocation)
	if nodeErr != nil {
		return ServiceReconcileResult{Service: service}, nodeErr
	}
	attempt, attemptErr := c.state.LoadAttempt(context.WithoutCancel(ctx), service.Start)
	if attemptErr != nil {
		return ServiceReconcileResult{Service: service}, attemptErr
	}
	invocation := cloneStepInvocation(service.Invocation)
	invocation.Activity = nil
	produced, executeErr := kind.Execute(ctx, stepkind.PreparedInvocation{Invocation: invocation})
	if executeErr != nil {
		return ServiceReconcileResult{Service: service}, safeServiceError("service launch recovery failed", executeErr)
	}
	if err := ctx.Err(); err != nil {
		return ServiceReconcileResult{Service: service}, err
	}
	if validationErr := produced.Validate(); validationErr != nil || produced.Outcome != stepkind.StepExternal || produced.External == nil {
		if validationErr == nil {
			validationErr = errors.New("service launch recovery did not return an external reference")
		}
		return ServiceReconcileResult{Service: service}, fmt.Errorf("%w: %w", ErrInvalidService, validationErr)
	}
	applied, err := c.store.RecoverServiceStart(context.WithoutCancel(ctx), RecoverServiceStartRequest{
		Start: service.Start, Ref: cloneExternalRef(*produced.External),
		ExpectedServiceGeneration: service.Generation,
		ExpectedNodeGeneration:    node.Generation, ExpectedAttemptGeneration: attempt.Generation,
		At: c.atOrAfter(service.UpdatedAt, node.UpdatedAt, attempt.UpdatedAt),
	})
	return ServiceReconcileResult{Service: applied.Service, Node: &applied.Node, Attempt: &applied.Attempt}, err
}

func (c *ServiceCoordinator) applyStarting(ctx context.Context, service ServiceSnapshot, kind stepkind.StepKind, spec stepkind.StepKindSpec, observation stepkind.ServiceObservation, heartbeatAt, at time.Time) (ServiceReconcileResult, error) {
	node, err := c.state.LoadNodeInvocation(ctx, service.Start.Invocation)
	if err != nil {
		return ServiceReconcileResult{Service: service}, err
	}
	attempt, err := c.state.LoadAttempt(ctx, service.Start)
	if err != nil {
		return ServiceReconcileResult{Service: service}, err
	}
	request := ApplyServiceReadyRequest{Start: service.Start, ExpectedServiceGeneration: service.Generation, ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation, ObservedAt: at, HeartbeatAt: heartbeatAt, At: at}
	switch observation.State {
	case stepkind.ServiceObservationStarting:
	case stepkind.ServiceObservationReady:
		if service.ReadyCheck != nil {
			_, verifyErr := executeVerification(ctx, c.verifiers, *service.ReadyCheck, spec.OutputSchema, observation.Outputs, nil)
			if verifyErr != nil {
				failure, _ := executionFailure("service_readiness_check_failed", verificationExecutionError(verifyErr))
				request.Failure = &failure
				break
			}
		}
		ref, saveErr := SaveValuesWithRetention(ctx, c.state, c.retention, SaveValuesRequest{Owner: ValueOwner{Kind: "service-readiness-outputs", RunID: service.Start.Invocation.RunID, Invocation: &service.Start.Invocation, Attempt: &service.Start}, Values: observation.Outputs})
		if saveErr != nil {
			return ServiceReconcileResult{Service: service}, saveErr
		}
		request.Ready, request.Outputs, request.HeartbeatAt = true, &ref, at
	case stepkind.ServiceObservationFailed, stepkind.ServiceObservationStopped:
		failure, _ := executionFailure("service_start_failed", observation.Failure)
		request.Failure = &failure
	}
	applied, err := c.store.ApplyServiceReady(ctx, request)
	result := ServiceReconcileResult{Service: applied.Service, Node: &applied.Node, Attempt: &applied.Attempt, Outputs: cloneValueSetRef(applied.Service.Outputs)}
	if err == nil && (request.Ready || request.Failure != nil) {
		produced := stepkind.StepResult{}
		var executionErr error
		if request.Ready {
			produced = stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: cloneValueSet(observation.Outputs)}
		} else {
			executionErr = observation.Failure
		}
		c.finalize(ctx, kind, spec, service.Invocation, produced, executionErr, &result)
	}
	return result, err
}

func (c *ServiceCoordinator) applyStopping(ctx context.Context, service ServiceSnapshot, kind stepkind.StepKind, spec stepkind.StepKindSpec, observation stepkind.ServiceObservation, heartbeatAt, at time.Time) (ServiceReconcileResult, error) {
	if service.Teardown == nil {
		return ServiceReconcileResult{Service: service}, fmt.Errorf("%w: stopping service has no teardown attempt", ErrInvalidService)
	}
	node, err := c.state.LoadNodeInvocation(ctx, service.Teardown.Invocation)
	if err != nil {
		return ServiceReconcileResult{Service: service}, err
	}
	attempt, err := c.state.LoadAttempt(ctx, *service.Teardown)
	if err != nil {
		return ServiceReconcileResult{Service: service}, err
	}
	request := ApplyServiceStopRequest{Start: service.Start.Invocation, ExpectedServiceGeneration: service.Generation, ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation, ObservedAt: at, HeartbeatAt: heartbeatAt, At: at}
	switch observation.State {
	case stepkind.ServiceObservationStarting, stepkind.ServiceObservationReady:
	case stepkind.ServiceObservationStopped:
		request.Stopped = true
	case stepkind.ServiceObservationFailed:
		failure, _ := executionFailure("service_teardown_failed", observation.Failure)
		request.Failure = &failure
	}
	applied, err := c.store.ApplyServiceStop(ctx, request)
	result := ServiceReconcileResult{Service: applied.Service, Node: &applied.Node, Attempt: &applied.Attempt}
	if err == nil && (request.Stopped || request.Failure != nil) && service.TeardownInvocation != nil {
		produced := stepkind.StepResult{}
		var executionErr error
		if request.Stopped {
			produced = stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}
		} else {
			executionErr = observation.Failure
		}
		c.finalize(ctx, kind, spec, *service.TeardownInvocation, produced, executionErr, &result)
	}
	return result, err
}

func (c *ServiceCoordinator) applyServiceFailure(ctx context.Context, service ServiceSnapshot, cause error) (ServiceReconcileResult, error) {
	failure, _ := executionFailure("service_observation_failed", cause)
	if service.Status == ServiceStarting {
		attempt, loadErr := c.state.LoadAttempt(ctx, service.Start)
		if loadErr != nil {
			return ServiceReconcileResult{Service: service}, loadErr
		}
		_, spec, resolveErr := stepkind.Resolve(c.registry, attempt.Executor.Kind, attempt.Executor.Version)
		if resolveErr != nil {
			return ServiceReconcileResult{Service: service}, resolveErr
		}
		kind, _, resolveErr := stepkind.Resolve(c.registry, attempt.Executor.Kind, attempt.Executor.Version)
		if resolveErr != nil {
			return ServiceReconcileResult{Service: service}, resolveErr
		}
		return c.applyStarting(ctx, service, kind, spec, stepkind.ServiceObservation{State: stepkind.ServiceObservationFailed, Failure: &stepkind.ExecutionError{Code: failure.Code, Message: failure.Message, Classification: stepkind.RetryPermanent}}, time.Time{}, c.now().UTC())
	}
	if service.Status == ServiceReady {
		at := c.now().UTC()
		updated, err := c.store.ApplyServiceHeartbeat(ctx, ApplyServiceHeartbeatRequest{Start: service.Start.Invocation, ExpectedServiceGeneration: service.Generation, Failure: &failure, ObservedAt: at, At: at})
		if err != nil {
			return ServiceReconcileResult{Service: updated}, err
		}
		if fenceErr := c.fenceRunFailure(ctx, updated, failure, at); fenceErr != nil {
			return ServiceReconcileResult{Service: updated}, fenceErr
		}
		return ServiceReconcileResult{Service: updated}, nil
	}
	return ServiceReconcileResult{Service: service}, cause
}

func (c *ServiceCoordinator) fenceRunFailure(ctx context.Context, service ServiceSnapshot, failure Failure, at time.Time) error {
	run, err := c.state.LoadRun(ctx, service.Start.Invocation.RunID)
	if err != nil {
		return err
	}
	plan, err := c.plans.LoadRecoveryPlan(ctx, run)
	if err != nil {
		return err
	}
	finalizers, err := PlanFinalizerScopes(plan.Plan.Graph, run.ID)
	if err != nil {
		return err
	}
	typed, err := NewRunFailureValue(run.ID, RunFailed, failure)
	if err != nil {
		return err
	}
	_, compensationRequired := compensationTriggerForStatus(plan.Plan.Graph.Compensation, RunFailed)
	if compensationRequired {
		if compensation, ok := c.state.(CompensationStore); !ok || compensation == nil {
			return fmt.Errorf("%w: compensation store is required before service failure intent", ErrInvalidService)
		}
	}
	_, err = c.control.BeginTerminalIntent(ctx, BeginTerminalIntentRequest{
		RunID: run.ID, ExpectedRunGeneration: run.Generation, IntendedStatus: RunFailed,
		Reason: &failure, ErrorValues: values.ValueSet{"error": typed},
		CompensationRequired: compensationRequired, IdempotencyKey: "service-failure:" + serviceStopKey(service.Start.Invocation), Finalizers: finalizers, At: at,
	})
	if errors.Is(err, ErrAlreadyExists) || errors.Is(err, ErrIdempotencyConflict) {
		existing, loadErr := c.control.LoadTerminalIntent(ctx, run.ID)
		if loadErr == nil && existing.IntendedStatus != RunSucceeded {
			return nil
		}
	}
	return err
}

func serviceStopKey(start NodeInvocationID) string {
	encoded, _ := json.Marshal(start)
	digest := sha256.Sum256(encoded)
	return "service-stop:sha256:" + hex.EncodeToString(digest[:])
}

func safeServiceError(message string, cause error) error {
	return &stepkind.ExecutionError{Code: "service_host_failed", Message: message, Classification: stepkind.RetryUnspecified, Cause: cause}
}

func (c *ServiceCoordinator) atOrAfter(floors ...time.Time) time.Time {
	at := c.now().UTC()
	for _, floor := range floors {
		if at.IsZero() || at.Before(floor) {
			at = floor
		}
	}
	return at
}

func (c *ServiceCoordinator) finalize(ctx context.Context, kind stepkind.StepKind, spec stepkind.StepKindSpec, invocation stepkind.Invocation, produced stepkind.StepResult, executionErr error, result *ServiceReconcileResult) {
	if result == nil || result.Node == nil || result.Attempt == nil {
		return
	}
	dispatcher := StepDispatcher{store: c.state, registry: c.registry, now: c.now}
	dispatch := DispatchResult{Node: *result.Node, Attempt: *result.Attempt, Outputs: result.Outputs}
	dispatcher.finalize(ctx, DispatchRequest{
		Claim: ReadyClaim{Candidate: ReadyCandidate{InvocationID: result.Attempt.ID.Invocation}},
		Node:  graph.Node{ID: result.Attempt.ID.Invocation.NodeID, Kind: spec.Name, KindVersion: spec.Version},
	}, kind, stepkind.PreparedInvocation{Invocation: cloneStepInvocation(invocation)}, produced, executionErr, &dispatch)
	result.Warnings = append([]DispatchWarning(nil), dispatch.Warnings...)
}
