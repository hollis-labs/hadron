package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const EventRunFailFastTriggered = "run.fail_fast_triggered"

var ErrInvalidRunPolicy = errors.New("invalid workflow run policy")

type RunFailureDisposition string

const (
	RunFailureHandled        RunFailureDisposition = "handled"
	RunFailureContinue       RunFailureDisposition = "continue"
	RunFailureFailFast       RunFailureDisposition = "fail_fast"
	RunFailureAlreadyDecided RunFailureDisposition = "already_decided"
)

// RunPolicyDecisionSnapshot is the immutable winning fail-fast trigger. The
// existing TerminalIntentSnapshot is the only run fence and cleanup owner;
// this record exists for inspection and normal concurrent-failure convergence.
type RunPolicyDecisionSnapshot struct {
	RunID            RunID                   `json:"run_id"`
	Mode             graph.RunCompletionMode `json:"mode"`
	Trigger          NodeInvocationID        `json:"trigger"`
	SourceGeneration uint64                  `json:"source_generation"`
	IntendedStatus   RunStatus               `json:"intended_status"`
	IdempotencyKey   string                  `json:"idempotency_key"`
	Generation       uint64                  `json:"generation"`
	CreatedAt        time.Time               `json:"created_at"`
}

func (s RunPolicyDecisionSnapshot) Validate() error {
	if err := validateOpaqueID("run policy run id", string(s.RunID)); err != nil {
		return err
	}
	if s.Mode != graph.CompletionFailFast || s.Trigger.RunID != s.RunID || s.Trigger.Iteration != "" {
		return fmt.Errorf("%w: decision requires a base fail-fast trigger in its run", ErrInvalidRunPolicy)
	}
	if err := s.Trigger.Validate(); err != nil {
		return err
	}
	if !s.IntendedStatus.Terminal() || s.IntendedStatus == RunSucceeded || s.SourceGeneration == 0 || s.Generation == 0 || s.CreatedAt.IsZero() {
		return fmt.Errorf("%w: decision requires failure status, generations, and timestamp", ErrInvalidRunPolicy)
	}
	return validateRequiredText("run policy idempotency key", s.IdempotencyKey)
}

type ApplyRunFailurePolicyRequest struct {
	RunID                    RunID            `json:"run_id"`
	ExpectedRunGeneration    uint64           `json:"expected_run_generation"`
	Trigger                  NodeInvocationID `json:"trigger"`
	ExpectedSourceGeneration uint64           `json:"expected_source_generation"`
	IntendedStatus           RunStatus        `json:"intended_status"`
	Reason                   Failure          `json:"reason"`
	ErrorValues              values.ValueSet  `json:"error_values"`
	IdempotencyKey           string           `json:"idempotency_key"`
	Finalizers               []FinalizerScope `json:"finalizers"`
	At                       time.Time        `json:"at"`
}

func (r ApplyRunFailurePolicyRequest) Validate() error {
	decision := RunPolicyDecisionSnapshot{RunID: r.RunID, Mode: graph.CompletionFailFast, Trigger: r.Trigger, SourceGeneration: r.ExpectedSourceGeneration, IntendedStatus: r.IntendedStatus, IdempotencyKey: r.IdempotencyKey, Generation: 1, CreatedAt: r.At}
	if err := decision.Validate(); err != nil {
		return err
	}
	if r.ExpectedRunGeneration == 0 {
		return fmt.Errorf("%w: expected run generation is required", ErrInvalidRunPolicy)
	}
	if err := r.Reason.Validate(); err != nil {
		return err
	}
	if err := ValidateControlErrorValues(r.ErrorValues); err != nil {
		return err
	}
	for _, finalizer := range r.Finalizers {
		if err := finalizer.Validate(r.RunID); err != nil {
			return err
		}
	}
	return nil
}

type ApplyRunFailurePolicyResult struct {
	Disposition RunFailureDisposition        `json:"disposition"`
	Decision    RunPolicyDecisionSnapshot    `json:"decision"`
	Run         RunSnapshot                  `json:"run"`
	Intent      TerminalIntentSnapshot       `json:"intent"`
	Nodes       []NodeInvocationSnapshot     `json:"nodes,omitempty"`
	Intents     []CancellationIntentSnapshot `json:"cancellation_intents,omitempty"`
	Events      []Event                      `json:"events,omitempty"`
}

type RunPolicyStore interface {
	LoadRunPolicyDecision(context.Context, RunID) (RunPolicyDecisionSnapshot, error)
	ApplyRunFailurePolicy(context.Context, ApplyRunFailurePolicyRequest) (ApplyRunFailurePolicyResult, error)
}

// RunPolicyCoordinator applies explicit graph completion policy after an
// unhandled aggregate node failure. Fan-out children are intentionally ignored
// until their durable aggregate applies the configured tolerance. Locally
// hosted request-cancel children reuse durable cancellation intents; exact
// propagation to separately hosted direct-cancel children remains a host
// recovery concern because this extraction-safe store has no child graph port.
type RunPolicyCoordinator struct {
	Store    StateStore
	Control  ControlFlowStore
	Policies RunPolicyStore
}

// HandleRunFailureRequest carries the exact durable failure when a node
// reaches a hard terminal state before an attempt exists (for example, a
// queue timeout). Attempt-backed failures are always loaded from StateStore;
// supplying a divergent Failure is rejected by the control-flow contract.
type HandleRunFailureRequest struct {
	Workflow       graph.Graph
	Source         NodeInvocationID
	Failure        *Failure
	IdempotencyKey string
	At             time.Time
}

func NewRunPolicyCoordinator(store StateStore, control ControlFlowStore, policies RunPolicyStore) *RunPolicyCoordinator {
	return &RunPolicyCoordinator{Store: store, Control: control, Policies: policies}
}

func (c *RunPolicyCoordinator) HandleFailure(ctx context.Context, workflow graph.Graph, source NodeInvocationID, idempotencyKey string, at time.Time) (ApplyRunFailurePolicyResult, error) {
	return c.HandleRunFailure(ctx, HandleRunFailureRequest{Workflow: workflow, Source: source, IdempotencyKey: idempotencyKey, At: at})
}

// HandleRunFailure applies graph completion policy to one durable aggregate
// failure. Fan-out item invocations are rejected until their aggregate has
// applied its tolerance policy.
func (c *RunPolicyCoordinator) HandleRunFailure(ctx context.Context, request HandleRunFailureRequest) (ApplyRunFailurePolicyResult, error) {
	if ctx == nil || c == nil || nilStateStore(c.Store) || nilControlFlowStore(c.Control) || nilRunPolicyStore(c.Policies) {
		return ApplyRunFailurePolicyResult{}, fmt.Errorf("%w: coordinator requires context and stores", ErrInvalidRunPolicy)
	}
	if err := ctx.Err(); err != nil {
		return ApplyRunFailurePolicyResult{}, err
	}
	if request.Source.Iteration != "" || request.At.IsZero() {
		return ApplyRunFailurePolicyResult{}, fmt.Errorf("%w: policy requires aggregate source and timestamp", ErrInvalidRunPolicy)
	}
	run, err := c.Store.LoadRun(ctx, request.Source.RunID)
	if err != nil {
		return ApplyRunFailurePolicyResult{}, err
	}
	if bindingErr := validateCancellationGraphBinding(run, request.Workflow); bindingErr != nil {
		return ApplyRunFailurePolicyResult{}, bindingErr
	}
	var definition *graph.Node
	for index := range request.Workflow.Nodes {
		if request.Workflow.Nodes[index].ID == request.Source.NodeID {
			definition = &request.Workflow.Nodes[index]
			break
		}
	}
	if definition == nil || definition.Finally != nil {
		return ApplyRunFailurePolicyResult{}, fmt.Errorf("%w: source is not ordinary graph node", ErrInvalidRunPolicy)
	}
	node, err := c.Store.LoadNodeInvocation(ctx, request.Source)
	if err != nil {
		return ApplyRunFailurePolicyResult{}, err
	}
	if !hardFailure(node.Status) {
		return ApplyRunFailurePolicyResult{}, fmt.Errorf("%w: source is not a hard failure", ErrInvalidRunPolicy)
	}
	if len(definition.Catch) != 0 {
		decision, decisionErr := c.Control.LoadControlDecision(ctx, ControlDecisionID{Source: request.Source, Kind: ControlCatch})
		if errors.Is(decisionErr, ErrNotFound) {
			return ApplyRunFailurePolicyResult{}, ErrControlFlowPending
		}
		if decisionErr != nil {
			return ApplyRunFailurePolicyResult{}, decisionErr
		}
		if decision.Outcome == ControlSelected || decision.Outcome == ControlContinued {
			return ApplyRunFailurePolicyResult{Disposition: RunFailureHandled, Run: run}, nil
		}
	}
	mode := graph.CompletionRunToCompletion
	if request.Workflow.Completion != nil {
		mode = request.Workflow.Completion.Mode
	}
	if mode == graph.CompletionRunToCompletion {
		return ApplyRunFailurePolicyResult{Disposition: RunFailureContinue, Run: run}, nil
	}
	if mode != graph.CompletionFailFast {
		return ApplyRunFailurePolicyResult{}, fmt.Errorf("%w: unsupported completion mode %q", ErrInvalidRunPolicy, mode)
	}
	failure, attempt, err := (&ControlFlowCoordinator{Store: c.Store}).originatingFailure(ctx, node, request.Failure)
	if err != nil {
		return ApplyRunFailurePolicyResult{}, err
	}
	timeout, err := durableFailureTimeout(node.Status, failure)
	if err != nil {
		return ApplyRunFailurePolicyResult{}, err
	}
	typed, err := NewFailureValue(node.ID, attempt, node.Status, timeout, failure)
	if err != nil {
		return ApplyRunFailurePolicyResult{}, err
	}
	status, err := runStatusForNodeFailure(node.Status)
	if err != nil {
		return ApplyRunFailurePolicyResult{}, err
	}
	finalizers, err := PlanFinalizerScopes(request.Workflow, request.Source.RunID)
	if err != nil {
		return ApplyRunFailurePolicyResult{}, err
	}
	return c.Policies.ApplyRunFailurePolicy(context.WithoutCancel(ctx), ApplyRunFailurePolicyRequest{
		RunID: request.Source.RunID, ExpectedRunGeneration: run.Generation, Trigger: request.Source, ExpectedSourceGeneration: node.Generation,
		IntendedStatus: status, Reason: failure, ErrorValues: values.ValueSet{"error": typed}, IdempotencyKey: request.IdempotencyKey,
		Finalizers: finalizers, At: request.At,
	})
}

func runStatusForNodeFailure(status NodeStatus) (RunStatus, error) {
	switch status {
	case NodeFailed:
		return RunFailed, nil
	case NodeTimedOut:
		return RunTimedOut, nil
	case NodeCrashed:
		return RunCrashed, nil
	case NodeCanceled:
		return RunCanceled, nil
	default:
		return "", fmt.Errorf("%w: node status %q has no run failure outcome", ErrInvalidRunPolicy, status)
	}
}

func nilRunPolicyStore(store RunPolicyStore) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
