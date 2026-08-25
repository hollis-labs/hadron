package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/values"
)

type controlFlowFixtureInput struct {
	Scenario           string                                 `json:"scenario"`
	Status             workflowruntime.NodeStatus             `json:"status"`
	CleanupStatus      workflowruntime.NodeStatus             `json:"cleanup_status"`
	FailureCode        string                                 `json:"failure_code"`
	Timeout            workflowruntime.TimeoutKind            `json:"timeout"`
	Catch              []graph.CatchRule                      `json:"catch"`
	Switch             *graph.SwitchSpec                      `json:"switch"`
	Graph              graph.Graph                            `json:"graph"`
	WantOutcome        workflowruntime.ControlDecisionOutcome `json:"want_outcome"`
	WantTargets        []string                               `json:"want_targets"`
	WantFinalizers     []string                               `json:"want_finalizers"`
	WantOrders         []int                                  `json:"want_orders"`
	WantIntendedStatus workflowruntime.RunStatus              `json:"want_intended_status"`
	WantRunStatus      workflowruntime.RunStatus              `json:"want_run_status"`
}

func TestControlFlowConformanceFixturesReferenceRuntime(t *testing.T) {
	conformance.ControlFlowSuite(t, conformance.EmbeddedFixtures(), func() (conformance.Runner, error) {
		return conformance.RunnerFunc(runControlFlowFixture), nil
	})
}

func runControlFlowFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input controlFlowFixtureInput
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode control-flow fixture: %w", err)
	}
	switch input.Scenario {
	case "switch":
		return runSwitchFixture(ctx, input)
	case "catch":
		return runCatchFixture(ctx, input)
	case "finally":
		return runFinallyFixture(ctx, input)
	case "completion":
		return runCompletionFixture(ctx, input)
	default:
		return fmt.Errorf("unsupported control-flow scenario %q", input.Scenario)
	}
}

func runSwitchFixture(ctx context.Context, input controlFlowFixtureInput) error {
	store, runID, base, fixtureErr := newControlFlowFixtureStore(ctx, "switch")
	if fixtureErr != nil {
		return fixtureErr
	}
	targetSet := make(map[string]struct{})
	for _, arm := range input.Switch.Arms {
		for _, target := range arm.Targets {
			targetSet[target] = struct{}{}
		}
	}
	for _, target := range input.Switch.Default {
		targetSet[target] = struct{}{}
	}
	if err := createControlNode(ctx, store, runID, "source", base); err != nil {
		return err
	}
	for target := range targetSet {
		if err := createControlNode(ctx, store, runID, target, base); err != nil {
			return err
		}
	}
	if err := finishControlNode(ctx, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, workflowruntime.NodeSucceeded, "", base.Add(time.Second)); err != nil {
		return err
	}
	source := graph.Node{ID: "source", Switch: input.Switch}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	result, err := coordinator.DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{
		Source: workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, Node: source, At: base.Add(2 * time.Second),
	})
	if err != nil {
		return err
	}
	if result.Decision.Outcome != input.WantOutcome || !slices.Equal(controlTargetNames(result.Decision.Targets), input.WantTargets) {
		return fmt.Errorf("switch decision = %#v, want outcome %q targets %v", result.Decision, input.WantOutcome, input.WantTargets)
	}
	targets := make([]string, 0, len(targetSet))
	for target := range targetSet {
		targets = append(targets, target)
	}
	slices.Sort(targets)
	nodes := []graph.Node{source}
	for _, target := range targets {
		nodes = append(nodes, graph.Node{ID: target})
	}
	for _, target := range targets {
		progress, progressErr := coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{Graph: graph.Graph{Nodes: nodes}, InvocationID: workflowruntime.NodeInvocationID{RunID: runID, NodeID: target}, At: base.Add(3 * time.Second)})
		selected := slices.Contains(input.WantTargets, target)
		want := workflowruntime.NodeSkipped
		if selected {
			want = workflowruntime.NodeReady
		}
		if progressErr != nil || progress.Snapshot.Status != want {
			return fmt.Errorf("switch target %q = %#v, %w", target, progress, progressErr)
		}
	}
	return nil
}

func runCatchFixture(ctx context.Context, input controlFlowFixtureInput) error {
	store, runID, base, fixtureErr := newControlFlowFixtureStore(ctx, "catch-"+string(input.Status))
	if fixtureErr != nil {
		return fixtureErr
	}
	if err := createControlNode(ctx, store, runID, "source", base); err != nil {
		return err
	}
	targetSet := make(map[string]struct{})
	for _, rule := range input.Catch {
		for _, target := range rule.Targets {
			targetSet[target] = struct{}{}
			if err := createControlNode(ctx, store, runID, target, base); err != nil {
				return err
			}
		}
	}
	if err := finishControlNode(ctx, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, input.Status, input.FailureCode, base.Add(time.Second)); err != nil {
		return err
	}
	source := graph.Node{ID: "source", Catch: input.Catch}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	result, err := coordinator.DecideCatch(ctx, workflowruntime.DecideCatchRequest{
		Source: workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, Node: source, Timeout: input.Timeout, At: base.Add(2 * time.Second),
	})
	if err != nil {
		return err
	}
	if result.Decision.Outcome != input.WantOutcome || !slices.Equal(controlTargetNames(result.Decision.Targets), input.WantTargets) {
		return fmt.Errorf("catch decision = %#v, want outcome %q targets %v", result.Decision, input.WantOutcome, input.WantTargets)
	}
	if result.Decision.Error == nil {
		return fmt.Errorf("catch decision omitted typed error")
	}
	set, err := store.LoadValues(ctx, *result.Decision.Error)
	if err != nil {
		return err
	}
	if input.Timeout != "" {
		payload, ok := set["error"].Inline.(map[string]any)
		if !ok || payload["timeout_kind"] != string(input.Timeout) {
			return fmt.Errorf("timeout error payload = %#v", set["error"].Inline)
		}
	}
	if result.Decision.Outcome == workflowruntime.ControlSelected {
		targets := make([]graph.Node, 0, len(targetSet)+1)
		targets = append(targets, source)
		for target := range targetSet {
			targetNode := graph.Node{ID: target}
			if result.Decision.BindAs != "" {
				targetNode.If = &graph.Expression{Text: result.Decision.BindAs + `.code == "` + input.FailureCode + `"`}
			}
			targets = append(targets, targetNode)
		}
		progress, progressErr := coordinator.ProgressControlNode(ctx, workflowruntime.ProgressControlNodeRequest{
			Graph: graph.Graph{Nodes: targets}, InvocationID: workflowruntime.NodeInvocationID{RunID: runID, NodeID: input.WantTargets[0]}, At: base.Add(3 * time.Second),
		})
		if progressErr != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
			return fmt.Errorf("catch handler progression = %#v, %w", progress, progressErr)
		}
		if result.Decision.BindAs != "" {
			name, binding, bindErr := workflowruntime.CatchBinding(ctx, store, store, result.Decision.ID)
			if bindErr != nil || name != result.Decision.BindAs || binding[name].Type != values.TypeObject {
				return fmt.Errorf("catch binding = %q %#v, %w", name, binding, bindErr)
			}
		}
	}
	if result.Decision.Outcome == workflowruntime.ControlContinued {
		run, intent, reconcileErr := coordinator.ReconcileRunCompletion(ctx, graph.Graph{Nodes: []graph.Node{source}}, runID, "continue-completion", base.Add(3*time.Second))
		if reconcileErr != nil || intent != nil || run.Status != workflowruntime.RunSucceeded {
			return fmt.Errorf("continued completion = run %#v intent %#v, %w", run, intent, reconcileErr)
		}
	}
	return nil
}

func runFinallyFixture(ctx context.Context, input controlFlowFixtureInput) error {
	scopes, planErr := workflowruntime.PlanFinalizerScopes(input.Graph, "fixture-run-finally")
	if planErr != nil {
		return planErr
	}
	ids := make([]string, len(scopes))
	orders := make([]int, len(scopes))
	for index, scope := range scopes {
		ids[index], orders[index] = scope.Invocation.NodeID, scope.Order
	}
	if !slices.Equal(ids, input.WantFinalizers) || !slices.Equal(orders, input.WantOrders) {
		return fmt.Errorf("finalizer plan = ids %v orders %v", ids, orders)
	}
	store, runID, base, fixtureErr := newControlFlowFixtureStore(ctx, "finally")
	if fixtureErr != nil {
		return fixtureErr
	}
	for _, node := range input.Graph.Nodes {
		if err := createControlNode(ctx, store, runID, node.ID, base); err != nil {
			return err
		}
		if node.Finally == nil {
			if err := finishControlNode(ctx, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: node.ID}, workflowruntime.NodeSucceeded, "", base.Add(time.Second)); err != nil {
				return err
			}
		}
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	if _, _, err := coordinator.ReconcileRunCompletion(ctx, input.Graph, runID, "finally-completion", base.Add(2*time.Second)); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
		return fmt.Errorf("begin finalizer intent = %w", err)
	}
	if len(input.WantFinalizers) > 1 {
		outer := input.WantFinalizers[len(input.WantFinalizers)-1]
		if _, err := coordinator.ProgressFinally(ctx, input.Graph, workflowruntime.NodeInvocationID{RunID: runID, NodeID: outer}, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(2500*time.Millisecond)); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
			return fmt.Errorf("outer finalizer before inner completion = %w", err)
		}
	}
	for index, id := range input.WantFinalizers {
		progress, progressErr := coordinator.ProgressFinally(ctx, input.Graph, workflowruntime.NodeInvocationID{RunID: runID, NodeID: id}, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(time.Duration(3+index*2)*time.Second))
		if progressErr != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
			return fmt.Errorf("progress finalizer %q = %#v, %w", id, progress, progressErr)
		}
		if err := finishControlNode(ctx, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: id}, workflowruntime.NodeSucceeded, "", base.Add(time.Duration(4+index*2)*time.Second)); err != nil {
			return err
		}
	}
	completed, intent, err := coordinator.ReconcileRunCompletion(ctx, input.Graph, runID, "finally-completion", base.Add(time.Duration(4+len(input.WantFinalizers)*2)*time.Second))
	if err != nil || completed.Status != workflowruntime.RunSucceeded || intent == nil || intent.Status != workflowruntime.TerminalIntentCompleted {
		return fmt.Errorf("finalizer completion = run %#v intent %#v, %w", completed, intent, err)
	}
	return nil
}

func runCompletionFixture(ctx context.Context, input controlFlowFixtureInput) error {
	store, runID, base, fixtureErr := newControlFlowFixtureStore(ctx, "completion")
	if fixtureErr != nil {
		return fixtureErr
	}
	workflow := graph.Graph{Nodes: []graph.Node{{ID: "work"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
	for _, node := range workflow.Nodes {
		if err := createControlNode(ctx, store, runID, node.ID, base); err != nil {
			return err
		}
	}
	if err := finishControlNode(ctx, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "work"}, input.Status, "work_failure", base.Add(time.Second)); err != nil {
		return err
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	run, intent, reconcileErr := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "fixture-completion", base.Add(2*time.Second))
	if !errors.Is(reconcileErr, workflowruntime.ErrControlFlowPending) || intent == nil || intent.IntendedStatus != input.WantIntendedStatus || !run.Status.Active() {
		return fmt.Errorf("pending completion = run %q intent %#v, %w", run.Status, intent, reconcileErr)
	}
	progress, progressErr := coordinator.ProgressFinally(ctx, workflow, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "cleanup"}, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(3*time.Second))
	if progressErr != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		return fmt.Errorf("cleanup progression = %#v, %w", progress, progressErr)
	}
	if err := finishControlNode(ctx, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "cleanup"}, input.CleanupStatus, "cleanup_failure", base.Add(4*time.Second)); err != nil {
		return err
	}
	run, intent, reconcileErr = coordinator.ReconcileRunCompletion(ctx, workflow, runID, "fixture-completion", base.Add(5*time.Second))
	if reconcileErr != nil || intent == nil || run.Status != input.WantRunStatus {
		return fmt.Errorf("completion = run %q intent %#v", run.Status, intent)
	}
	return nil
}

func newControlFlowFixtureStore(ctx context.Context, suffix string) (*inmemory.Store, workflowruntime.RunID, time.Time, error) {
	store := inmemory.NewStore()
	base := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("fixture-control-" + suffix)
	_, _, err := store.CreateRun(ctx, workflowruntime.CreateRunRequest{
		ID: runID, Plan: workflowruntime.PlanRef{ID: "fixture-plan", Version: "v1", Digest: values.SHA256Digest([]byte("fixture-control-plan")), SchemaVersion: "workflow.execution-plan/v1"},
		Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + string(runID), CreatedAt: base,
	})
	if err != nil {
		return nil, "", time.Time{}, err
	}
	run, err := store.LoadRun(ctx, runID)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	if _, err = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		return nil, "", time.Time{}, err
	}
	return store, runID, base, nil
}

func createControlNode(ctx context.Context, store *inmemory.Store, runID workflowruntime.RunID, nodeID string, at time.Time) error {
	_, err := store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: workflowruntime.NodeInvocationID{RunID: runID, NodeID: nodeID}, Status: workflowruntime.NodePending, CreatedAt: at, UpdatedAt: at,
	}})
	return err
}

func finishControlNode(ctx context.Context, store *inmemory.Store, id workflowruntime.NodeInvocationID, status workflowruntime.NodeStatus, failureCode string, at time.Time) error {
	node, err := store.LoadNodeInvocation(ctx, id)
	if err != nil {
		return err
	}
	if node.Status == workflowruntime.NodePending {
		ready, transitionErr := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: at})
		if transitionErr != nil {
			return transitionErr
		}
		node = ready.Snapshot
	}
	claim, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: id, ExpectedClaimGeneration: node.ClaimGeneration, Owner: "fixture", Token: "token-" + id.NodeID, IdempotencyKey: "claim-" + id.NodeID, Now: at, LeaseUntil: at.Add(time.Hour)})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		return fmt.Errorf("claim %q: %#v, %w", id.NodeID, claim, err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	claimed, err := store.LoadNodeInvocation(ctx, id)
	if err != nil {
		return err
	}
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "fixture", Version: "v1", Target: "local"}, At: at})
	if err != nil {
		return err
	}
	var failure *workflowruntime.Failure
	if status != workflowruntime.NodeSucceeded {
		failure = &workflowruntime.Failure{Code: failureCode, Message: "fixture failure"}
		if status == workflowruntime.NodeTimedOut {
			failure.Details = map[string]string{"timeout_kind": string(workflowruntime.TimeoutExecution)}
		}
	}
	_, err = store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: id, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation,
		ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: status, NextNodeStatus: status, Failure: failure, At: at,
	})
	return err
}

func controlTargetNames(ids []workflowruntime.NodeInvocationID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = id.NodeID
	}
	return result
}
