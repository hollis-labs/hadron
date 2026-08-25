package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/conformance"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/values"
)

type schedulerFixtureInput struct {
	Scenario     string                       `json:"scenario"`
	Rule         graph.ReadyRule              `json:"rule"`
	Dependencies []workflowruntime.NodeStatus `json:"dependencies"`
	Completion   graph.RunCompletionMode      `json:"completion"`
	Want         string                       `json:"want"`
}

func TestSchedulerPolicyConformanceFixturesReferenceDurableRuntime(t *testing.T) {
	conformance.SchedulerSuite(t, conformance.EmbeddedFixtures(), func() (conformance.Runner, error) {
		return conformance.RunnerFunc(runSchedulerFixture), nil
	})
}

func runSchedulerFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input schedulerFixtureInput
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode scheduler fixture: %w", err)
	}
	switch input.Scenario {
	case "resource":
		return runSchedulerResourceFixture(ctx, input)
	case "run_policy":
		return runSchedulerPolicyFixture(ctx, input)
	case "":
		dependencies := make([]workflowruntime.DependencyState, len(input.Dependencies))
		for index, status := range input.Dependencies {
			dependencies[index] = workflowruntime.DependencyState{InvocationID: workflowruntime.NodeInvocationID{RunID: "fixture-run", NodeID: fmt.Sprintf("dependency-%d", index)}, Status: status}
		}
		evaluation, err := workflowruntime.EvaluateReadiness(input.Rule, dependencies)
		if err != nil {
			return err
		}
		if string(evaluation.Disposition) != input.Want {
			return fmt.Errorf("readiness disposition = %q, want %q", evaluation.Disposition, input.Want)
		}
		return nil
	default:
		return fmt.Errorf("unsupported scheduler scenario %q", input.Scenario)
	}
}

func runSchedulerResourceFixture(ctx context.Context, input schedulerFixtureInput) error {
	store := inmemory.NewStore()
	base := time.Date(2026, 8, 24, 22, 0, 0, 0, time.UTC)
	ids := []workflowruntime.NodeInvocationID{{RunID: "fixture-resource-a", NodeID: "work"}, {RunID: "fixture-resource-b", NodeID: "work"}}
	for _, id := range ids {
		if err := createSchedulerRunNode(ctx, store, id, base); err != nil {
			return err
		}
	}
	limits := workflowruntime.SchedulerLimits{Workers: 4, Named: map[string]int{"fixture-key": 1}}
	results := make([]workflowruntime.AdmitNodeResult, len(ids))
	for index, id := range ids {
		requirements, err := workflowruntime.BuildSchedulerRequirements(id.RunID, limits, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "fixture-key"}}})
		if err != nil {
			return err
		}
		results[index], err = store.AdmitNode(ctx, workflowruntime.AdmitNodeRequest{Claim: workflowruntime.ClaimNodeRequest{InvocationID: id, Owner: fmt.Sprintf("worker-%d", index), Token: fmt.Sprintf("token-%d", index), IdempotencyKey: fmt.Sprintf("fixture-admit-%d", index), Now: base.Add(time.Second), LeaseUntil: base.Add(time.Minute)}, Requirements: requirements, EnqueuedAt: base})
		if err != nil {
			return err
		}
	}
	if input.Want != "blocked" || !results[0].Claim.Acquired || results[1].Claim.Acquired || len(results[1].Blocked) != 1 || results[1].Blocked[0].Kind != workflowruntime.SchedulerResourceKey {
		return fmt.Errorf("cross-run resource outcomes = %#v", results)
	}
	state, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(time.Second)})
	if err != nil || len(state.Holders) != 2 || len(state.Waiters) != 1 {
		return fmt.Errorf("cross-run resource diagnostics = %#v, %w", state, err)
	}
	return nil
}

func runSchedulerPolicyFixture(ctx context.Context, input schedulerFixtureInput) error {
	store := inmemory.NewStore()
	base := time.Date(2026, 8, 24, 22, 30, 0, 0, time.UTC)
	runID := workflowruntime.RunID("fixture-policy-" + string(input.Completion))
	plan := schedulerFixturePlan()
	run, _, err := store.CreateRun(ctx, workflowruntime.CreateRunRequest{ID: runID, Plan: plan, Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + string(runID), CreatedAt: base})
	if err != nil {
		return err
	}
	runningRun, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base})
	if err != nil {
		return err
	}
	source := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}
	independent := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "independent"}
	for _, id := range []workflowruntime.NodeInvocationID{source, independent} {
		if nodeErr := createSchedulerNode(ctx, store, id, base); nodeErr != nil {
			return nodeErr
		}
	}
	claim, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: source, Owner: "source", Token: "source", IdempotencyKey: "source-claim", Now: base.Add(time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || !claim.Acquired {
		return fmt.Errorf("claim source: %#v, %w", claim, err)
	}
	claimed, err := store.LoadNodeInvocation(ctx, source)
	if err != nil {
		return err
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: source, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "fixture", Version: "v1", Target: "local"}, At: base.Add(time.Second)})
	if err != nil {
		return err
	}
	failure := workflowruntime.Failure{Code: "fixture_failed", Message: "fixture failure"}
	if _, finishErr := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: source, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeFailed, NextNodeStatus: workflowruntime.NodeFailed, Failure: &failure, At: base.Add(2 * time.Second)}); finishErr != nil {
		return finishErr
	}
	workflow := graph.Graph{ID: plan.ID, Version: plan.Version, Completion: &graph.RunCompletionPolicy{Mode: input.Completion}, Nodes: []graph.Node{{ID: "source"}, {ID: "independent"}}}
	result, err := workflowruntime.NewRunPolicyCoordinator(store, store, store).HandleFailure(ctx, workflow, source, "fixture-policy", base.Add(3*time.Second))
	if err != nil {
		return err
	}
	if string(result.Disposition) != input.Want {
		return fmt.Errorf("run policy disposition = %q, want %q", result.Disposition, input.Want)
	}
	independentNode, err := store.LoadNodeInvocation(ctx, independent)
	if err != nil {
		return err
	}
	if input.Completion == graph.CompletionFailFast {
		if result.Intent.Status != workflowruntime.TerminalIntentPending || independentNode.Status != workflowruntime.NodeCanceled || result.Run.Generation != runningRun.Snapshot.Generation+1 {
			return fmt.Errorf("fail-fast state = result %#v independent %#v", result, independentNode)
		}
		completed, err := store.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: runID, ExpectedRunGeneration: result.Run.Generation, ExpectedIntentGeneration: result.Intent.Generation, At: base.Add(4 * time.Second)})
		if err != nil || completed.Run.Status != workflowruntime.RunFailed {
			return fmt.Errorf("fail-fast completion = %#v, %w", completed, err)
		}
		return nil
	}
	if independentNode.Status != workflowruntime.NodeReady {
		return fmt.Errorf("run-to-completion independent node = %q", independentNode.Status)
	}
	if _, decisionErr := store.LoadRunPolicyDecision(ctx, runID); !errors.Is(decisionErr, workflowruntime.ErrNotFound) {
		return fmt.Errorf("run-to-completion policy decision: %w", decisionErr)
	}
	return nil
}

func createSchedulerRunNode(ctx context.Context, store workflowruntime.StateStore, id workflowruntime.NodeInvocationID, at time.Time) error {
	plan := schedulerFixturePlan()
	if _, _, err := store.CreateRun(ctx, workflowruntime.CreateRunRequest{ID: id.RunID, Plan: plan, Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + string(id.RunID), CreatedAt: at}); err != nil {
		return err
	}
	return createSchedulerNode(ctx, store, id, at)
}

func createSchedulerNode(ctx context.Context, store workflowruntime.StateStore, id workflowruntime.NodeInvocationID, at time.Time) error {
	node, err := store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: id, Status: workflowruntime.NodePending, CreatedAt: at, UpdatedAt: at}})
	if err != nil {
		return err
	}
	_, err = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: at})
	return err
}

func schedulerFixturePlan() workflowruntime.PlanRef {
	return workflowruntime.PlanRef{ID: "fixture-plan", Version: "v1", Digest: values.SHA256Digest([]byte("fixture-plan")), SchemaVersion: "workflow.execution-plan/v1"}
}
