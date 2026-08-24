package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const EventCrashReconciled = "node.crash_reconciled"

var (
	ErrInvalidRecovery  = errors.New("invalid workflow recovery request")
	ErrRecoveryConflict = errors.New("workflow recovery conflict")
	ErrRecoveryUnsafe   = errors.New("workflow recovery repetition is unsafe")
)

// RecoveryPlan is the exact immutable graph bound to a persisted PlanRef.
// Implementations must load pinned source/plan material, never re-resolve a
// movable definition.
type RecoveryPlan struct {
	Ref        PlanRef                     `json:"ref"`
	Plan       compile.ExecutionPlan       `json:"plan"`
	Visibility compile.ValueVisibilityPlan `json:"visibility"`
}

func (p RecoveryPlan) Validate() error {
	if err := p.Ref.Validate(); err != nil {
		return err
	}
	if p.Plan.ID != p.Ref.ID || p.Plan.Graph.ID != p.Ref.ID || p.Plan.Graph.Version != p.Ref.Version || p.Plan.Digest != p.Ref.Digest {
		return fmt.Errorf("recovery graph identity differs from plan reference")
	}
	if len(p.Plan.Graph.Nodes) == 0 || len(p.Visibility.Nodes) != len(p.Plan.Graph.Nodes) {
		return fmt.Errorf("recovery graph has no nodes")
	}
	for _, node := range p.Plan.Graph.Nodes {
		if _, exists := p.Visibility.Nodes[node.ID]; !exists {
			return fmt.Errorf("recovery visibility is missing node %q", node.ID)
		}
	}
	return nil
}

// RecoveryPlanSource loads exact, restart-safe plan material.
type RecoveryPlanSource interface {
	LoadRecoveryPlan(context.Context, RunSnapshot) (RecoveryPlan, error)
}

// RepeatOperation distinguishes crash uncertainty from an explicit replay of
// a completed durable invocation.
type RepeatOperation string

const (
	RepeatCrashRecovery RepeatOperation = "crash_recovery"
	RepeatReplay        RepeatOperation = "replay"
)

func (o RepeatOperation) valid() bool { return o == RepeatCrashRecovery || o == RepeatReplay }

// RepeatCandidate binds every policy decision to the pinned graph node, the
// frozen registered kind spec, and the authoritative durable attempt.
type RepeatCandidate struct {
	Operation  RepeatOperation        `json:"operation"`
	Run        RunSnapshot            `json:"run"`
	Node       NodeInvocationSnapshot `json:"node"`
	Attempt    *AttemptSnapshot       `json:"attempt,omitempty"`
	Definition graph.Node             `json:"definition"`
	Spec       stepkind.StepKindSpec  `json:"spec"`
	Effects    graph.EffectSet        `json:"effects"`
	// IdempotencyKey is the exact compiler-scoped durable evaluation of a
	// keyed node declaration. It is empty for intrinsic/unsupported modes.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// RepeatPolicyDecision is a persistence-safe policy fact. Code, Reason, and
// Attributes must not contain resolved secrets.
type RepeatPolicyDecision struct {
	Allow      bool              `json:"allow"`
	Code       string            `json:"code"`
	Reason     string            `json:"reason"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (d RepeatPolicyDecision) Validate() error {
	if err := validateRequiredText("repeat policy code", d.Code); err != nil {
		return err
	}
	if err := validateRequiredText("repeat policy reason", d.Reason); err != nil {
		return err
	}
	return validateStringMap("repeat policy attributes", d.Attributes)
}

// RepeatPolicy is the host authorization seam for consequential recovery and
// replay. Core retry-safety and idempotency floors remain non-overridable.
type RepeatPolicy interface {
	EvaluateRepeat(context.Context, RepeatCandidate) (RepeatPolicyDecision, error)
}

// RepeatPolicyFunc adapts a function to RepeatPolicy.
type RepeatPolicyFunc func(context.Context, RepeatCandidate) (RepeatPolicyDecision, error)

func (f RepeatPolicyFunc) EvaluateRepeat(ctx context.Context, candidate RepeatCandidate) (RepeatPolicyDecision, error) {
	return f(ctx, candidate)
}

// CrashRecoveryAction chooses the aggregate node state after atomically
// closing an interrupted attempt as crashed.
type CrashRecoveryAction string

const (
	CrashRetry    CrashRecoveryAction = "retry_waiting"
	CrashTerminal CrashRecoveryAction = "crashed"
)

func (a CrashRecoveryAction) valid() bool { return a == CrashRetry || a == CrashTerminal }

type CrashRecoveryDecision struct {
	Action CrashRecoveryAction  `json:"action"`
	Policy RepeatPolicyDecision `json:"policy"`
	Retry  *RetryDecision       `json:"retry,omitempty"`
}

func (d CrashRecoveryDecision) Validate() error {
	if !d.Action.valid() {
		return fmt.Errorf("unsupported crash recovery action %q", d.Action)
	}
	if d.Action == CrashRetry {
		if d.Retry == nil || !d.Retry.Retry || d.Retry.FireAt.IsZero() {
			return fmt.Errorf("crash retry requires an eligible timed retry decision")
		}
	} else if d.Retry != nil {
		return fmt.Errorf("terminal crash cannot carry retry decision")
	}
	return d.Policy.Validate()
}

type ReconcileCrashedAttemptRequest struct {
	Attempt                   AttemptID             `json:"attempt"`
	ExpectedNodeGeneration    uint64                `json:"expected_node_generation"`
	ExpectedAttemptGeneration uint64                `json:"expected_attempt_generation"`
	IdempotencyKey            string                `json:"idempotency_key"`
	Decision                  CrashRecoveryDecision `json:"decision"`
	At                        time.Time             `json:"at"`
}

func (r ReconcileCrashedAttemptRequest) Validate() error {
	if err := r.Attempt.Validate(); err != nil {
		return err
	}
	if r.ExpectedNodeGeneration == 0 || r.ExpectedAttemptGeneration == 0 || r.At.IsZero() {
		return fmt.Errorf("crash recovery requires generations and timestamp")
	}
	if err := validateRequiredText("crash recovery idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	return r.Decision.Validate()
}

type ReconcileCrashedAttemptResult struct {
	Outcome    IdempotencyOutcome       `json:"outcome"`
	Node       NodeInvocationSnapshot   `json:"node"`
	Attempt    AttemptSnapshot          `json:"attempt"`
	Event      Event                    `json:"event"`
	Activation *RetryActivationSnapshot `json:"activation,omitempty"`
}

// RecoveryStore owns the atomic crash and replay persistence operations added
// by W03-T06 without widening the foundational StateStore interface.
type RecoveryStore interface {
	ReconcileCrashedAttempt(context.Context, ReconcileCrashedAttemptRequest) (ReconcileCrashedAttemptResult, error)
	ListRunInvocations(context.Context, RunID) ([]NodeInvocationSnapshot, error)
	ReplayStore
}

// RecoveryRequest supplies one deterministic recovery pass. Limit is applied
// independently to persisted query categories. ExpressionOptions retain the
// same evaluator semantics used during ordinary progression.
type RecoveryRequest struct {
	Now               time.Time
	Limit             int
	ExpressionOptions values.ExpressionOptions
}

type RecoveryResult struct {
	Snapshot         RecoverySnapshot
	Services         []ServiceReconcileResult
	Waits            WaitRecoveryResults
	RetryActivations []ActivateNodeRetryResult
	Crashes          []ReconcileCrashedAttemptResult
	ControlDecisions []ControlDecisionSnapshot
	Progressed       []ProgressNodeResult
	FanOuts          []CompleteFanOutResult
	FanOutCanceled   []NodeInvocationSnapshot
}

// RecoveryCoordinator reconstructs work exclusively from durable facts. Its
// order is: immutable control facts, interrupted attempts and their run-policy
// fences, waits/timers, due retries, then ordinary readiness. A host must run
// pinned child materialization and recursive cancellation-tree recovery before
// invoking it; Hadron's RecoveryHook boundary provides that ordering.
type RecoveryCoordinator struct {
	Store           StateStore
	Recovery        RecoveryStore
	Inputs          NodeInputStore
	Control         ControlFlowStore
	Plans           RecoveryPlanSource
	Registry        stepkind.Registry
	Policy          RepeatPolicy
	RetryAuthorizer RetryAuthorizer
	Policies        RunPolicyStore
	Waits           *WaitCoordinator
	Evaluator       PredicateEvaluator
}

func (c *RecoveryCoordinator) Recover(ctx context.Context, request RecoveryRequest) (RecoveryResult, error) {
	if err := c.validate(ctx, request); err != nil {
		return RecoveryResult{}, err
	}
	result := RecoveryResult{}
	// Service launch intent is persisted before a provider call. Reacquire it
	// before generic crashed-attempt handling so a conservative repeat policy
	// cannot terminalize the attempt and strand a provider resource between
	// Start and reference persistence.
	if serviceStore, ok := c.Store.(ServiceStore); ok {
		services, serviceErr := NewServiceCoordinator(ServiceCoordinatorOptions{
			Store: serviceStore, State: c.Store, Registry: c.Registry,
			Plans: c.Plans, Control: c.Control, Now: func() time.Time { return request.Now.UTC() },
		})
		if serviceErr != nil {
			return result, serviceErr
		}
		result.Services, serviceErr = services.Recover(ctx, ServiceQuery{Limit: request.Limit})
		if serviceErr != nil {
			return result, serviceErr
		}
	}
	snapshot, err := c.Store.Recovery(ctx, RecoveryQuery{Now: request.Now, Limit: request.Limit})
	if err != nil {
		return result, err
	}
	result.Snapshot = snapshot
	plans := make(map[RunID]RecoveryPlan, len(snapshot.ActiveRuns))
	for _, run := range snapshot.ActiveRuns {
		if run.Status == RunPending {
			continue
		}
		plan, loadErr := c.loadPlan(ctx, run)
		if loadErr != nil {
			return result, loadErr
		}
		plans[run.ID] = plan
		decisions, progress, restoreErr := c.restoreControl(ctx, run, plan, request)
		if restoreErr != nil && !recoveryConcurrentProgress(restoreErr) {
			return result, restoreErr
		}
		result.ControlDecisions = append(result.ControlDecisions, decisions...)
		result.Progressed = append(result.Progressed, progress...)
	}

	for _, node := range snapshot.Running {
		if node.Lease != nil && node.Lease.ExpiresAt.After(request.Now) {
			continue
		}
		plan, ok := plans[node.ID.RunID]
		if !ok {
			run, loadErr := c.Store.LoadRun(ctx, node.ID.RunID)
			if loadErr != nil {
				return result, loadErr
			}
			plan, loadErr = c.loadPlan(ctx, run)
			if loadErr != nil {
				return result, loadErr
			}
			plans[node.ID.RunID] = plan
		}
		reconciled, reconcileErr := c.reconcileCrash(ctx, plan, node)
		if errors.Is(reconcileErr, ErrCASMismatch) || errors.Is(reconcileErr, ErrAttemptConflict) {
			continue
		}
		if reconcileErr != nil {
			return result, reconcileErr
		}
		result.Crashes = append(result.Crashes, reconciled)
	}

	// A crash can establish a catch decision or fail-fast terminal intent. Make
	// those admission facts durable before a due wait or retry is allowed to
	// publish Ready work; this is the cross-process fence, not host health.
	afterCrashes, err := c.Store.Recovery(ctx, RecoveryQuery{Now: request.Now, Limit: request.Limit})
	if err != nil {
		return result, err
	}
	for _, run := range afterCrashes.ActiveRuns {
		if run.Status == RunPending {
			continue
		}
		plan, exists := plans[run.ID]
		if !exists {
			plan, err = c.loadPlan(ctx, run)
			if err != nil {
				return result, err
			}
			plans[run.ID] = plan
		}
		decisions, finalizers, restoreErr := c.restoreControl(ctx, run, plan, request)
		if restoreErr != nil && !recoveryConcurrentProgress(restoreErr) {
			return result, restoreErr
		}
		result.ControlDecisions = append(result.ControlDecisions, decisions...)
		result.Progressed = append(result.Progressed, finalizers...)
		if policyErr := c.restoreRunPolicy(ctx, run, plan); policyErr != nil && !recoveryConcurrentProgress(policyErr) {
			return result, policyErr
		}
	}

	// Reconcile fan-out aggregates after crash outcomes are durable. Fail-fast
	// cancellation uses only the stored expansion and child states; collection
	// then converges the parent whenever no started child remains active.
	for _, parent := range afterCrashes.Waiting {
		if parent.ID.Iteration != "" {
			continue
		}
		fanOut, loadErr := c.Store.LoadFanOut(ctx, parent.ID)
		if errors.Is(loadErr, ErrNotFound) {
			continue
		}
		if loadErr != nil {
			return result, loadErr
		}
		coordinator := FanOutCoordinator{Store: c.Store}
		if fanOut.FailFast {
			canceled, reconcileErr := coordinator.ReconcileFailFast(context.WithoutCancel(ctx), parent.ID, maxRecoveryTime(request.Now, fanOut.UpdatedAt))
			if reconcileErr != nil && !recoveryConcurrentProgress(reconcileErr) {
				return result, reconcileErr
			}
			result.FanOutCanceled = append(result.FanOutCanceled, canceled...)
		}
		completed, _, _, collectErr := coordinator.Collect(context.WithoutCancel(ctx), parent.ID, maxRecoveryTime(request.Now, fanOut.UpdatedAt))
		if collectErr == nil {
			result.FanOuts = append(result.FanOuts, completed)
		} else if !errors.Is(collectErr, ErrFanOutIncomplete) && !recoveryConcurrentProgress(collectErr) {
			return result, collectErr
		}
	}

	if c.Waits != nil {
		waits, waitErr := c.Waits.RecoverWaits(ctx, OpenWaitQuery{Limit: request.Limit}, request.Now)
		if waitErr != nil {
			return result, waitErr
		}
		result.Waits = waits
	}

	dueRetries, err := c.Store.RecoverRetryActivations(ctx, RetryActivationQuery{DueBefore: request.Now, Limit: request.Limit})
	if err != nil {
		return result, err
	}
	for _, activation := range dueRetries {
		node, loadErr := c.Store.LoadNodeInvocation(ctx, activation.Attempt.Invocation)
		if loadErr != nil {
			return result, loadErr
		}
		activated, activateErr := c.Store.ActivateNodeRetry(context.WithoutCancel(ctx), ActivateNodeRetryRequest{
			ActivationID: activation.ID, ExpectedActivationGeneration: activation.Generation,
			ExpectedNodeGeneration: node.Generation, IdempotencyKey: "recover:" + activation.ID, Now: activation.FireAt,
		})
		if errors.Is(activateErr, ErrCASMismatch) || errors.Is(activateErr, ErrRetryNotDue) {
			continue
		}
		if activateErr != nil {
			return result, activateErr
		}
		result.RetryActivations = append(result.RetryActivations, activated)
	}

	// Re-read after crash/timer transitions so ready rebuilding never acts on a
	// stale category snapshot.
	current, err := c.Store.Recovery(ctx, RecoveryQuery{Now: request.Now, Limit: request.Limit})
	if err != nil {
		return result, err
	}
	for _, run := range current.ActiveRuns {
		if run.Status == RunPending {
			continue
		}
		plan, exists := plans[run.ID]
		if !exists {
			plan, err = c.loadPlan(ctx, run)
			if err != nil {
				return result, err
			}
			plans[run.ID] = plan
		}
		decisions, finalizers, restoreErr := c.restoreControl(ctx, run, plan, request)
		if restoreErr != nil && !recoveryConcurrentProgress(restoreErr) {
			return result, restoreErr
		}
		result.ControlDecisions = append(result.ControlDecisions, decisions...)
		result.Progressed = append(result.Progressed, finalizers...)
		if policyErr := c.restoreRunPolicy(ctx, run, plan); policyErr != nil && !recoveryConcurrentProgress(policyErr) {
			return result, policyErr
		}
		progress, progressErr := c.rebuildReady(ctx, run, plan, request)
		if progressErr != nil && !recoveryConcurrentProgress(progressErr) {
			return result, progressErr
		}
		result.Progressed = append(result.Progressed, progress...)
		completionAt, timeErr := c.recoveryCompletionAt(ctx, run)
		if timeErr != nil {
			return result, timeErr
		}
		completedRun, intent, completionErr := NewControlFlowCoordinator(c.Store, c.Control, c.Evaluator).ReconcileRunCompletion(ctx, plan.Plan.Graph, run.ID, "recover-complete:"+string(run.ID), completionAt)
		if errors.Is(completionErr, ErrRunOutputsPending) {
			expression, expressionErr := BuildExpressionContext(ctx, c.Store, c.Control, plan.Plan.Graph, run.ID)
			if expressionErr != nil {
				return result, fmt.Errorf("%w: rebuild workflow output context: %w", ErrInvalidRecovery, expressionErr)
			}
			currentRun, loadErr := c.Store.LoadRun(ctx, run.ID)
			if loadErr != nil {
				return result, loadErr
			}
			if currentRun.Inputs == nil {
				return result, fmt.Errorf("%w: recovering run is missing bound inputs", ErrInvalidRecovery)
			}
			finalized, finalizeErr := FinalizeRunOutputs(ctx, c.Store, FinalizeRunRequest{
				BoundRun: BoundRun{ID: run.ID, Plan: plan.Ref, InputsRef: *currentRun.Inputs, CreatedAt: currentRun.CreatedAt, Provenance: plan.Plan.Provenance},
				Run:      currentRun, Plan: &plan.Plan, Context: expression, BaseOptions: request.ExpressionOptions,
				Control: c.Control, At: completionAt,
			})
			if finalizeErr != nil {
				return result, finalizeErr
			}
			if len(finalized.Diagnostics) != 0 {
				return result, fmt.Errorf("%w: workflow output binding failed: %v", ErrInvalidRecovery, finalized.Diagnostics)
			}
			completedRun, completionErr = finalized.Run, nil
			if intent != nil {
				updatedIntent, loadIntentErr := c.Control.LoadTerminalIntent(ctx, run.ID)
				if loadIntentErr != nil {
					return result, loadIntentErr
				}
				intent = &updatedIntent
			}
		}
		if completionErr != nil && !errors.Is(completionErr, ErrControlFlowPending) && !errors.Is(completionErr, ErrCASMismatch) && !errors.Is(completionErr, ErrTransitionConflict) {
			return result, completionErr
		}
		if intent != nil && intent.Status == TerminalIntentPending && (completionErr == nil || errors.Is(completionErr, ErrControlFlowPending)) {
			_, finalizers, restoreErr := c.restoreControl(ctx, completedRun, plan, request)
			if restoreErr != nil && !errors.Is(restoreErr, ErrControlFlowPending) && !errors.Is(restoreErr, ErrCASMismatch) && !errors.Is(restoreErr, ErrTransitionConflict) {
				return result, restoreErr
			}
			result.Progressed = append(result.Progressed, finalizers...)
		}
	}
	return result, nil
}

func (c *RecoveryCoordinator) restoreRunPolicy(ctx context.Context, run RunSnapshot, plan RecoveryPlan) error {
	if nilRunPolicyStore(c.Policies) {
		return nil
	}
	invocations, err := c.Recovery.ListRunInvocations(ctx, run.ID)
	if err != nil {
		return err
	}
	for _, invocation := range invocations {
		if invocation.ID.Iteration != "" || !hardFailure(invocation.Status) {
			continue
		}
		definition, exists := graphNode(plan.Plan.Graph, invocation.ID.NodeID)
		if !exists {
			return fmt.Errorf("%w: invocation %v is absent from pinned graph", ErrInvalidRecovery, invocation.ID)
		}
		// Cleanup failure is owned by terminal-intent completion, which
		// deterministically promotes the intended terminal status to failed.
		if definition.Finally != nil {
			continue
		}
		policyResult, policyErr := NewRunPolicyCoordinator(c.Store, c.Control, c.Policies).HandleRunFailure(ctx, HandleRunFailureRequest{Workflow: plan.Plan.Graph, Source: invocation.ID, IdempotencyKey: "recover-policy:" + controlIdentity(invocation.ID), At: maxRecoveryTime(run.UpdatedAt, invocation.UpdatedAt)})
		if errors.Is(policyErr, ErrControlFlowPending) || errors.Is(policyErr, ErrCASMismatch) {
			continue
		}
		if policyErr != nil {
			return policyErr
		}
		if policyResult.Disposition == RunFailureFailFast || policyResult.Disposition == RunFailureAlreadyDecided {
			break
		}
	}
	return nil
}

func recoveryConcurrentProgress(err error) bool {
	return errors.Is(err, ErrCASMismatch) || errors.Is(err, ErrTransitionConflict) || errors.Is(err, ErrAttemptConflict) || errors.Is(err, ErrAlreadyExists)
}

func (c *RecoveryCoordinator) recoveryCompletionAt(ctx context.Context, run RunSnapshot) (time.Time, error) {
	at := run.UpdatedAt
	invocations, err := c.Recovery.ListRunInvocations(ctx, run.ID)
	if err != nil {
		return time.Time{}, err
	}
	for _, invocation := range invocations {
		at = maxRecoveryTime(at, invocation.UpdatedAt)
	}
	return at, nil
}

func (c *RecoveryCoordinator) validate(ctx context.Context, request RecoveryRequest) error {
	if ctx == nil || c == nil || nilStateStore(c.Store) || nilRecoveryStore(c.Recovery) || nilNodeInputStore(c.Inputs) ||
		nilRecoveryPlanSource(c.Plans) || nilStepKindRegistry(c.Registry) {
		return fmt.Errorf("%w: context, stores, plan source, and registry are required", ErrInvalidRecovery)
	}
	if request.Now.IsZero() || request.Limit < 0 {
		return fmt.Errorf("%w: recovery requires now and non-negative limit", ErrInvalidRecovery)
	}
	if c.Control == nil || nilControlFlowStore(c.Control) {
		return fmt.Errorf("%w: control-flow store is required", ErrInvalidRecovery)
	}
	return ctx.Err()
}

func (c *RecoveryCoordinator) loadPlan(ctx context.Context, run RunSnapshot) (RecoveryPlan, error) {
	plan, err := c.Plans.LoadRecoveryPlan(ctx, run)
	if err != nil {
		return RecoveryPlan{}, err
	}
	if err := plan.Validate(); err != nil {
		return RecoveryPlan{}, fmt.Errorf("%w: invalid pinned recovery plan: %w", ErrInvalidRecovery, err)
	}
	if plan.Ref != run.Plan {
		return RecoveryPlan{}, fmt.Errorf("%w: pinned recovery plan does not match run", ErrInvalidRecovery)
	}
	return plan, nil
}

func (c *RecoveryCoordinator) reconcileCrash(ctx context.Context, plan RecoveryPlan, node NodeInvocationSnapshot) (ReconcileCrashedAttemptResult, error) {
	definition, ok := graphNode(plan.Plan.Graph, node.ID.NodeID)
	if !ok {
		return ReconcileCrashedAttemptResult{}, fmt.Errorf("%w: running invocation is absent from pinned graph", ErrInvalidRecovery)
	}
	attempt, err := c.Store.LoadAttempt(ctx, AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
	if err != nil {
		return ReconcileCrashedAttemptResult{}, err
	}
	_, spec, err := stepkind.Resolve(c.Registry, attempt.Executor.Kind, attempt.Executor.Version)
	if err != nil {
		return ReconcileCrashedAttemptResult{}, err
	}
	run, err := c.Store.LoadRun(ctx, node.ID.RunID)
	if err != nil {
		return ReconcileCrashedAttemptResult{}, err
	}
	// The durable interruption instant must converge across processes. The
	// recovery pass's wall clock establishes that a lease is expired, but it is
	// not part of the immutable crash fact or retry backoff anchor.
	crashAt := maxRecoveryTime(node.UpdatedAt, attempt.UpdatedAt)
	if node.Lease != nil {
		crashAt = maxRecoveryTime(crashAt, node.Lease.ExpiresAt)
	}
	idempotencyKey, err := c.recoveryIdempotencyKey(ctx, plan, node, definition)
	if err != nil {
		return ReconcileCrashedAttemptResult{}, err
	}
	candidate := RepeatCandidate{Operation: RepeatCrashRecovery, Run: run, Node: node, Attempt: &attempt, Definition: definition, Spec: spec, Effects: effectiveEffects(definition.Effects, spec.Effects), IdempotencyKey: idempotencyKey}
	decision, safe := c.repeatDecision(ctx, candidate)
	action := CrashTerminal
	var retryDecision *RetryDecision
	if safe && decision.Allow {
		failure := Failure{Code: "HADR-PERSIST-001", Message: "executor interrupted before durable completion", Retryable: true, Details: map[string]string{"class": "crashed"}}
		retry, retryErr := (RetryEvaluator{Authorizer: c.RetryAuthorizer}).Evaluate(ctx, RetryEvaluationRequest{Node: definition, Spec: spec, AttemptNumber: attempt.ID.Number, Failure: failure, AttemptStatus: NodeCrashed, IdempotencyKey: idempotencyKey, FailedAt: crashAt})
		if retryErr != nil {
			return ReconcileCrashedAttemptResult{}, retryErr
		}
		if retry.Retry {
			action, retryDecision = CrashRetry, &retry
		}
	}
	return c.Recovery.ReconcileCrashedAttempt(context.WithoutCancel(ctx), ReconcileCrashedAttemptRequest{
		Attempt: attempt.ID, ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation,
		IdempotencyKey: crashRecoveryKey(attempt.ID, attempt.Generation),
		Decision:       CrashRecoveryDecision{Action: action, Policy: decision, Retry: retryDecision}, At: crashAt,
	})
}

func (c *RecoveryCoordinator) repeatDecision(ctx context.Context, candidate RepeatCandidate) (RepeatPolicyDecision, bool) {
	eligible, reason := repeatEligible(candidate)
	if !eligible {
		return RepeatPolicyDecision{Code: reason, Reason: "executor metadata does not permit repeated execution"}, false
	}
	if c.Policy == nil {
		allow := lowEffectOnly(candidate.Effects)
		code := "policy_required"
		if allow {
			code = "safe_default"
		}
		return RepeatPolicyDecision{Allow: allow, Code: code, Reason: "default recovery policy"}, allow
	}
	decision, err := c.Policy.EvaluateRepeat(ctx, candidate)
	if err != nil || decision.Validate() != nil {
		return RepeatPolicyDecision{Code: "policy_error", Reason: "repeat policy failed closed"}, false
	}
	return decision, decision.Allow
}

func repeatEligible(candidate RepeatCandidate) (bool, string) {
	if !candidate.Operation.valid() || candidate.Definition.Kind != candidate.Spec.Name || candidate.Definition.KindVersion != candidate.Spec.Version {
		return false, "metadata_mismatch"
	}
	if candidate.Attempt != nil && (candidate.Attempt.ID.Invocation != candidate.Node.ID ||
		candidate.Attempt.Executor.Kind != candidate.Spec.Name || candidate.Attempt.Executor.Version != candidate.Spec.Version) {
		return false, "metadata_mismatch"
	}
	if candidate.Spec.RetrySafety == stepkind.RetryUnsupported {
		return false, "kind_unsupported"
	}
	if candidate.Operation == RepeatReplay && candidate.Attempt != nil && candidate.Attempt.Status == NodeSucceeded &&
		candidate.Spec.RetrySafety != stepkind.RetrySafe && !effectiveIdempotency(candidate.Definition, candidate.Spec, candidate.IdempotencyKey) {
		return false, "completed_non_idempotent"
	}
	if candidate.Spec.RetrySafety == stepkind.RetryRequiresIdempotency && !effectiveIdempotency(candidate.Definition, candidate.Spec, candidate.IdempotencyKey) {
		return false, "idempotency_missing"
	}
	if !lowEffectOnly(candidate.Effects) && !effectiveIdempotency(candidate.Definition, candidate.Spec, candidate.IdempotencyKey) {
		return false, "effect_idempotency_missing"
	}
	return true, "eligible"
}

func effectiveEffects(declared, trusted graph.EffectSet) graph.EffectSet {
	seen := make(map[graph.Effect]struct{}, len(declared)+len(trusted))
	for _, effect := range trusted {
		seen[effect] = struct{}{}
	}
	for _, effect := range declared {
		seen[effect] = struct{}{}
	}
	result := make(graph.EffectSet, 0, len(seen))
	for effect := range seen {
		result = append(result, effect)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func effectiveIdempotency(node graph.Node, spec stepkind.StepKindSpec, key string) bool {
	switch spec.Idempotency {
	case graph.IdempotencyIntrinsic:
		return true
	case graph.IdempotencyKeyed:
		return node.Idempotency != nil && node.Idempotency.Mode == graph.IdempotencyKeyed && node.Idempotency.Key != nil && key != ""
	default:
		return false
	}
}

func (c *RecoveryCoordinator) recoveryIdempotencyKey(ctx context.Context, plan RecoveryPlan, node NodeInvocationSnapshot, definition graph.Node) (string, error) {
	if definition.Idempotency == nil || definition.Idempotency.Mode != graph.IdempotencyKeyed {
		return "", nil
	}
	if definition.Idempotency.Key == nil {
		return "", fmt.Errorf("%w: keyed node %q has no idempotency expression", ErrRecoveryUnsafe, definition.ID)
	}
	available, err := BuildExpressionContext(ctx, c.Store, c.Control, plan.Plan.Graph, node.ID.RunID)
	if err != nil {
		return "", err
	}
	if node.ID.Iteration != "" {
		fanOut, loadErr := c.Store.LoadFanOut(ctx, NodeInvocationID{RunID: node.ID.RunID, NodeID: node.ID.NodeID})
		if loadErr != nil {
			return "", loadErr
		}
		matched := false
		for _, binding := range fanOut.Items {
			if binding.Invocation != node.ID {
				continue
			}
			set, valuesErr := c.Store.LoadValues(ctx, binding.Inputs)
			if valuesErr != nil {
				return "", valuesErr
			}
			item, exists := set[fanOut.ItemName]
			if !exists {
				return "", fmt.Errorf("%w: fan-out item value is missing", ErrRecoveryConflict)
			}
			index := binding.Index
			available.Item, available.Index, matched = &item, &index, true
			break
		}
		if !matched {
			return "", fmt.Errorf("%w: fan-out iteration is absent from durable expansion", ErrRecoveryConflict)
		}
	}
	scoped, options, err := plan.Visibility.ScopeNodeContext(definition.ID, available, values.ExpressionOptions{})
	if err != nil {
		return "", fmt.Errorf("%w: compiler visibility: %w", ErrInvalidRecovery, err)
	}
	raw, err := values.NewExpressionEngine().EvaluateRaw(*definition.Idempotency.Key, scoped, options)
	if err != nil {
		return "", err
	}
	key, ok := raw.(string)
	if !ok || strings.TrimSpace(key) != key || key == "" {
		return "", fmt.Errorf("%w: idempotency expression for %q must produce canonical non-empty text", ErrRecoveryUnsafe, definition.ID)
	}
	if err := validateRequiredText("recovery idempotency key", key); err != nil {
		return "", fmt.Errorf("%w: %w", ErrRecoveryUnsafe, err)
	}
	return key, nil
}

func lowEffectOnly(effects graph.EffectSet) bool {
	if len(effects) == 0 {
		return false
	}
	for _, effect := range effects {
		if effect != graph.EffectRead && effect != graph.EffectCompute {
			return false
		}
	}
	return true
}

func (c *RecoveryCoordinator) restoreControl(ctx context.Context, run RunSnapshot, plan RecoveryPlan, request RecoveryRequest) ([]ControlDecisionSnapshot, []ProgressNodeResult, error) {
	workflow := plan.Plan.Graph
	coordinator := NewControlFlowCoordinator(c.Store, c.Control, c.Evaluator)
	expression, err := BuildExpressionContext(ctx, c.Store, c.Control, workflow, run.ID)
	if err != nil {
		return nil, nil, err
	}
	nodes := append([]graph.Node(nil), workflow.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	var decisions []ControlDecisionSnapshot
	for _, definition := range nodes {
		id := NodeInvocationID{RunID: run.ID, NodeID: definition.ID}
		snapshot, loadErr := c.Store.LoadNodeInvocation(ctx, id)
		if loadErr != nil {
			return nil, nil, loadErr
		}
		if definition.Switch != nil && snapshot.Status == NodeSucceeded {
			if _, loadErr := c.Control.LoadControlDecision(ctx, ControlDecisionID{Source: id, Kind: ControlSwitch}); errors.Is(loadErr, ErrNotFound) {
				result, decideErr := coordinator.DecideSwitch(ctx, DecideSwitchRequest{Source: id, Node: definition, ExpressionContext: expression, ExpressionOptions: request.ExpressionOptions, At: maxRecoveryTime(request.Now, snapshot.UpdatedAt)})
				if decideErr != nil {
					return nil, nil, decideErr
				}
				decisions = append(decisions, result.Decision)
			} else if loadErr != nil {
				return nil, nil, loadErr
			}
		}
		if len(definition.Catch) != 0 && hardFailure(snapshot.Status) && snapshot.LatestAttempt > 0 {
			if _, loadErr := c.Control.LoadControlDecision(ctx, ControlDecisionID{Source: id, Kind: ControlCatch}); errors.Is(loadErr, ErrNotFound) {
				result, decideErr := coordinator.DecideCatch(ctx, DecideCatchRequest{Source: id, Node: definition, ExpressionContext: expression, ExpressionOptions: request.ExpressionOptions, At: maxRecoveryTime(request.Now, snapshot.UpdatedAt)})
				if decideErr != nil {
					return nil, nil, decideErr
				}
				decisions = append(decisions, result.Decision)
			} else if loadErr != nil {
				return nil, nil, loadErr
			}
		}
	}
	intent, intentErr := c.Control.LoadTerminalIntent(ctx, run.ID)
	if errors.Is(intentErr, ErrNotFound) {
		return decisions, nil, nil
	}
	if intentErr != nil {
		return nil, nil, intentErr
	}
	if intent.Status != TerminalIntentPending {
		return decisions, nil, nil
	}
	var progressed []ProgressNodeResult
	driver := NodeDriver{Store: c.Store, Inputs: c.Inputs, Control: c.Control, Registry: c.Registry, Evaluator: c.Evaluator}
	for _, finalizer := range intent.Finalizers {
		definition, exists := graphNode(workflow, finalizer.Invocation.NodeID)
		if !exists {
			return nil, nil, fmt.Errorf("%w: terminal-intent finalizer is absent from pinned graph", ErrInvalidRecovery)
		}
		snapshot, loadErr := c.Store.LoadNodeInvocation(ctx, finalizer.Invocation)
		if loadErr != nil {
			return nil, nil, loadErr
		}
		// Recovery only drives finalizers that still need readiness work. A ready
		// finalizer is already claimable, a running finalizer must first pass
		// through crash reconciliation, and a waiting finalizer is owned by wait
		// recovery. Terminal snapshots are durable evidence for later scopes.
		if snapshot.Status != NodePending && snapshot.Status != NodeBlocked {
			continue
		}
		result, progressErr := driver.DriveFinally(ctx, DriveNodeRequest{Run: run, Plan: plan, InvocationID: finalizer.Invocation, Node: definition, ExpressionContext: expression, ExpressionOptions: request.ExpressionOptions, At: maxRecoveryTime(request.Now, intent.UpdatedAt)})
		if errors.Is(progressErr, ErrControlFlowPending) {
			continue
		}
		if progressErr != nil {
			if recoveryConcurrentProgress(progressErr) {
				continue
			}
			return nil, nil, progressErr
		}
		progressed = append(progressed, result.Progressed)
	}
	return decisions, progressed, nil
}

func (c *RecoveryCoordinator) rebuildReady(ctx context.Context, run RunSnapshot, plan RecoveryPlan, request RecoveryRequest) ([]ProgressNodeResult, error) {
	if intent, err := c.Control.LoadTerminalIntent(ctx, run.ID); err == nil && intent.Status == TerminalIntentPending {
		return nil, nil
	} else if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	driver := NodeDriver{Store: c.Store, Inputs: c.Inputs, Control: c.Control, Registry: c.Registry, Evaluator: c.Evaluator}
	nodes := append([]graph.Node(nil), plan.Plan.Graph.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	var result []ProgressNodeResult
	for pass := 0; pass < len(nodes); pass++ {
		expression, err := BuildExpressionContext(ctx, c.Store, c.Control, plan.Plan.Graph, run.ID)
		if err != nil {
			return nil, err
		}
		changed := false
		for _, definition := range nodes {
			if definition.Finally != nil {
				continue
			}
			id := NodeInvocationID{RunID: run.ID, NodeID: definition.ID}
			snapshot, loadErr := c.Store.LoadNodeInvocation(ctx, id)
			if loadErr != nil {
				return nil, loadErr
			}
			if snapshot.Status != NodePending && snapshot.Status != NodeBlocked {
				continue
			}
			driven, driveErr := driver.Drive(ctx, DriveNodeRequest{Run: run, Plan: plan, InvocationID: id, Node: definition, ExpressionContext: expression, ExpressionOptions: request.ExpressionOptions, At: maxRecoveryTime(request.Now, snapshot.UpdatedAt)})
			if driveErr != nil {
				if recoveryConcurrentProgress(driveErr) {
					changed = true
					continue
				}
				return nil, driveErr
			}
			if driven.Progressed.Snapshot.Generation != snapshot.Generation || driven.Progressed.Snapshot.Status != snapshot.Status {
				changed = true
				result = append(result, driven.Progressed)
			}
		}
		if !changed {
			break
		}
	}
	return result, nil
}

func graphNode(workflow graph.Graph, id string) (graph.Node, bool) {
	for _, node := range workflow.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return graph.Node{}, false
}

func graphDependencies(workflow graph.Graph, runID RunID, nodeID string) []DependencyRef {
	set := make(map[string]struct{})
	if node, ok := graphNode(workflow, nodeID); ok {
		for _, need := range node.Needs {
			set[need.Node] = struct{}{}
		}
	}
	for _, edge := range workflow.Edges {
		if edge.To == nodeID {
			set[edge.From] = struct{}{}
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]DependencyRef, len(ids))
	for i, id := range ids {
		result[i] = DependencyRef{InvocationID: NodeInvocationID{RunID: runID, NodeID: id}}
	}
	return result
}

func graphRouteTarget(workflow graph.Graph, target string) bool {
	for _, source := range workflow.Nodes {
		for _, rule := range source.Catch {
			for _, id := range rule.Targets {
				if id == target {
					return true
				}
			}
		}
		if source.Switch != nil {
			for _, id := range source.Switch.Default {
				if id == target {
					return true
				}
			}
			for _, arm := range source.Switch.Arms {
				for _, id := range arm.Targets {
					if id == target {
						return true
					}
				}
			}
		}
	}
	return false
}

func crashRecoveryKey(id AttemptID, generation uint64) string {
	encoded, _ := EncodeAttemptIdentity(id)
	return fmt.Sprintf("crash:%s:%d", encoded, generation)
}

func maxRecoveryTime(values ...time.Time) time.Time {
	var result time.Time
	for _, value := range values {
		if value.After(result) {
			result = value
		}
	}
	return result.UTC()
}

func nilRecoveryStore(store RecoveryStore) bool            { return nilReflect(store) }
func nilRecoveryPlanSource(source RecoveryPlanSource) bool { return nilReflect(source) }
func nilReflect(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
