package appworkflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/hollis-labs/go-workflow/conformance"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

type adoptionSchedulerInput struct {
	Scenario     string                       `json:"scenario"`
	Rule         graph.ReadyRule              `json:"rule"`
	Dependencies []workflowruntime.NodeStatus `json:"dependencies"`
	Completion   graph.RunCompletionMode      `json:"completion"`
	Want         string                       `json:"want"`
}

func (r *hadronConformanceRunner) runSchedulerFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input adoptionSchedulerInput
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode scheduler fixture: %w", err)
	}
	switch input.Scenario {
	case "resource":
		return r.runSchedulerResourceFixture(ctx, input)
	case "run_policy":
		return r.runSchedulerPolicyFixture(ctx, input)
	case "":
		dependencies := make([]workflowruntime.DependencyState, len(input.Dependencies))
		for index, status := range input.Dependencies {
			dependencies[index] = workflowruntime.DependencyState{
				InvocationID: workflowruntime.NodeInvocationID{RunID: "fixture-run", NodeID: fmt.Sprintf("dependency-%d", index)},
				Status:       status,
			}
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

func (r *hadronConformanceRunner) runSchedulerResourceFixture(ctx context.Context, input adoptionSchedulerInput) error {
	base := time.Date(2026, 8, 24, 22, 0, 0, 0, time.UTC)
	ids := []workflowruntime.NodeInvocationID{{RunID: "fixture-resource-a", NodeID: "work"}, {RunID: "fixture-resource-b", NodeID: "work"}}
	for _, id := range ids {
		if err := r.createSchedulerRunNode(ctx, id, base); err != nil {
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
		results[index], err = r.state.AdmitNode(ctx, workflowruntime.AdmitNodeRequest{
			Claim: workflowruntime.ClaimNodeRequest{
				InvocationID: id, Owner: fmt.Sprintf("worker-%d", index), Token: fmt.Sprintf("token-%d", index),
				IdempotencyKey: fmt.Sprintf("fixture-admit-%d", index), Now: base.Add(time.Second), LeaseUntil: base.Add(time.Minute),
			},
			Requirements: requirements, EnqueuedAt: base,
		})
		if err != nil {
			return err
		}
	}
	if input.Want != "blocked" || !results[0].Claim.Acquired || results[1].Claim.Acquired ||
		len(results[1].Blocked) != 1 || results[1].Blocked[0].Kind != workflowruntime.SchedulerResourceKey {
		return fmt.Errorf("cross-run resource outcomes = %#v", results)
	}
	state, err := r.state.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(time.Second)})
	if err != nil || len(state.Holders) != 2 || len(state.Waiters) != 1 {
		return fmt.Errorf("cross-run resource diagnostics = %#v: %w", state, err)
	}
	return nil
}

func (r *hadronConformanceRunner) runSchedulerPolicyFixture(ctx context.Context, input adoptionSchedulerInput) error {
	base := time.Date(2026, 8, 24, 22, 30, 0, 0, time.UTC)
	runID := workflowruntime.RunID("fixture-policy-" + string(input.Completion))
	plan := adoptionPlanRef("scheduler-policy")
	run, _, err := r.state.CreateRun(ctx, workflowruntime.CreateRunRequest{
		ID: runID, Plan: plan, Status: workflowruntime.RunPending,
		StartIdempotencyKey: "start-" + string(runID), CreatedAt: base,
	})
	if err != nil {
		return err
	}
	runningRun, err := r.state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base})
	if err != nil {
		return err
	}
	source := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}
	independent := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "independent"}
	for _, id := range []workflowruntime.NodeInvocationID{source, independent} {
		if nodeErr := r.createReadyNode(ctx, id, base); nodeErr != nil {
			return nodeErr
		}
	}
	if finishErr := r.finishNode(ctx, source, workflowruntime.NodeFailed, "fixture_failed", base.Add(time.Second)); finishErr != nil {
		return finishErr
	}
	workflow := graph.Graph{
		ID: plan.ID, Version: plan.Version, Completion: &graph.RunCompletionPolicy{Mode: input.Completion},
		Nodes: []graph.Node{{ID: "source"}, {ID: "independent"}},
	}
	result, err := workflowruntime.NewRunPolicyCoordinator(r.state, r.state, r.state).HandleFailure(ctx, workflow, source, "fixture-policy", base.Add(3*time.Second))
	if err != nil {
		return err
	}
	if string(result.Disposition) != input.Want {
		return fmt.Errorf("run policy disposition = %q, want %q", result.Disposition, input.Want)
	}
	independentNode, err := r.state.LoadNodeInvocation(ctx, independent)
	if err != nil {
		return err
	}
	if input.Completion == graph.CompletionFailFast {
		if result.Intent.Status != workflowruntime.TerminalIntentPending || independentNode.Status != workflowruntime.NodeCanceled || result.Run.Generation != runningRun.Snapshot.Generation+1 {
			return fmt.Errorf("fail-fast state = result %#v independent %#v", result, independentNode)
		}
		completed, err := r.state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{
			RunID: runID, ExpectedRunGeneration: result.Run.Generation, ExpectedIntentGeneration: result.Intent.Generation, At: base.Add(4 * time.Second),
		})
		if err != nil || completed.Run.Status != workflowruntime.RunFailed {
			return fmt.Errorf("fail-fast completion = %#v: %w", completed, err)
		}
		return nil
	}
	if independentNode.Status != workflowruntime.NodeReady {
		return fmt.Errorf("run-to-completion independent node = %q", independentNode.Status)
	}
	if _, decisionErr := r.state.LoadRunPolicyDecision(ctx, runID); !errors.Is(decisionErr, workflowruntime.ErrNotFound) {
		return fmt.Errorf("run-to-completion policy decision: %w", decisionErr)
	}
	return nil
}

func (r *hadronConformanceRunner) createSchedulerRunNode(ctx context.Context, id workflowruntime.NodeInvocationID, at time.Time) error {
	if _, _, err := r.state.CreateRun(ctx, workflowruntime.CreateRunRequest{
		ID: id.RunID, Plan: adoptionPlanRef(string(id.RunID)), Status: workflowruntime.RunPending,
		StartIdempotencyKey: "start-" + string(id.RunID), CreatedAt: at,
	}); err != nil {
		return err
	}
	return r.createReadyNode(ctx, id, at)
}

func (r *hadronConformanceRunner) createReadyNode(ctx context.Context, id workflowruntime.NodeInvocationID, at time.Time) error {
	node, err := r.state.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodePending, CreatedAt: at, UpdatedAt: at,
	}})
	if err != nil {
		return err
	}
	_, err = r.state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: at})
	return err
}

type adoptionControlInput struct {
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

func (r *hadronConformanceRunner) runControlFlowFixture(ctx context.Context, fixture conformance.Fixture) error {
	var input adoptionControlInput
	if err := json.Unmarshal(fixture.Input, &input); err != nil {
		return fmt.Errorf("decode control-flow fixture: %w", err)
	}
	switch input.Scenario {
	case "switch":
		return r.runSwitchFixture(ctx, input)
	case "catch":
		return r.runCatchFixture(ctx, input)
	case "finally":
		return r.runFinallyFixture(ctx, input)
	case "completion":
		return r.runCompletionFixture(ctx, input)
	default:
		return fmt.Errorf("unsupported control-flow scenario %q", input.Scenario)
	}
}

func (r *hadronConformanceRunner) runSwitchFixture(ctx context.Context, input adoptionControlInput) error {
	runID, base, err := r.createRunningRun(ctx, "switch")
	if err != nil {
		return err
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
	if nodeErr := r.createPendingNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, base); nodeErr != nil {
		return nodeErr
	}
	for target := range targetSet {
		if nodeErr := r.createPendingNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: target}, base); nodeErr != nil {
			return nodeErr
		}
	}
	if finishErr := r.finishNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, workflowruntime.NodeSucceeded, "", base.Add(time.Second)); finishErr != nil {
		return finishErr
	}
	result, err := workflowruntime.NewControlFlowCoordinator(r.state, r.state, nil).DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{
		Source: workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, Node: graph.Node{ID: "source", Switch: input.Switch}, At: base.Add(2 * time.Second),
	})
	if err != nil {
		return err
	}
	if result.Decision.Outcome != input.WantOutcome || !slices.Equal(adoptionTargetNames(result.Decision.Targets), input.WantTargets) {
		return fmt.Errorf("switch decision = %#v", result.Decision)
	}
	return nil
}

func (r *hadronConformanceRunner) runCatchFixture(ctx context.Context, input adoptionControlInput) error {
	runID, base, err := r.createRunningRun(ctx, "catch-"+string(input.Status))
	if err != nil {
		return err
	}
	if nodeErr := r.createPendingNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, base); nodeErr != nil {
		return nodeErr
	}
	for _, rule := range input.Catch {
		for _, target := range rule.Targets {
			if nodeErr := r.createPendingNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: target}, base); nodeErr != nil {
				return nodeErr
			}
		}
	}
	if finishErr := r.finishNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, input.Status, input.FailureCode, base.Add(time.Second)); finishErr != nil {
		return finishErr
	}
	result, err := workflowruntime.NewControlFlowCoordinator(r.state, r.state, nil).DecideCatch(ctx, workflowruntime.DecideCatchRequest{
		Source: workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}, Node: graph.Node{ID: "source", Catch: input.Catch},
		Timeout: input.Timeout, At: base.Add(2 * time.Second),
	})
	if err != nil {
		return err
	}
	if result.Decision.Outcome != input.WantOutcome || !slices.Equal(adoptionTargetNames(result.Decision.Targets), input.WantTargets) || result.Decision.Error == nil {
		return fmt.Errorf("catch decision = %#v", result.Decision)
	}
	if _, err := r.state.LoadValues(ctx, *result.Decision.Error); err != nil {
		return fmt.Errorf("load durable catch error: %w", err)
	}
	return nil
}

func (r *hadronConformanceRunner) runFinallyFixture(ctx context.Context, input adoptionControlInput) error {
	runID, base, err := r.createRunningRun(ctx, "finally")
	if err != nil {
		return err
	}
	scopes, err := workflowruntime.PlanFinalizerScopes(input.Graph, runID)
	if err != nil {
		return err
	}
	ids := make([]string, len(scopes))
	orders := make([]int, len(scopes))
	for index, scope := range scopes {
		ids[index], orders[index] = scope.Invocation.NodeID, scope.Order
	}
	if !slices.Equal(ids, input.WantFinalizers) || !slices.Equal(orders, input.WantOrders) {
		return fmt.Errorf("finalizer plan = ids %v orders %v", ids, orders)
	}
	for _, node := range input.Graph.Nodes {
		id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: node.ID}
		if nodeErr := r.createPendingNode(ctx, id, base); nodeErr != nil {
			return nodeErr
		}
		if node.Finally == nil {
			if finishErr := r.finishNode(ctx, id, workflowruntime.NodeSucceeded, "", base.Add(time.Second)); finishErr != nil {
				return finishErr
			}
		}
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(r.state, r.state, nil)
	if _, _, reconcileErr := coordinator.ReconcileRunCompletion(ctx, input.Graph, runID, "finally-completion", base.Add(2*time.Second)); !errors.Is(reconcileErr, workflowruntime.ErrControlFlowPending) {
		return fmt.Errorf("begin finalizer intent: %w", reconcileErr)
	}
	for index, id := range input.WantFinalizers {
		progress, progressErr := coordinator.ProgressFinally(ctx, input.Graph, workflowruntime.NodeInvocationID{RunID: runID, NodeID: id}, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(time.Duration(3+index*2)*time.Second))
		if progressErr != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
			return fmt.Errorf("progress finalizer %q = %#v: %w", id, progress, progressErr)
		}
		if finishErr := r.finishNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: id}, workflowruntime.NodeSucceeded, "", base.Add(time.Duration(4+index*2)*time.Second)); finishErr != nil {
			return finishErr
		}
	}
	completed, intent, err := coordinator.ReconcileRunCompletion(ctx, input.Graph, runID, "finally-completion", base.Add(time.Duration(4+len(input.WantFinalizers)*2)*time.Second))
	if err != nil || intent == nil || intent.Status != workflowruntime.TerminalIntentCompleted || completed.Status != workflowruntime.RunSucceeded {
		return fmt.Errorf("finalizer completion = run %#v intent %#v: %w", completed, intent, err)
	}
	return nil
}

func (r *hadronConformanceRunner) runCompletionFixture(ctx context.Context, input adoptionControlInput) error {
	runID, base, err := r.createRunningRun(ctx, "completion")
	if err != nil {
		return err
	}
	workflow := graph.Graph{Nodes: []graph.Node{{ID: "work"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
	for _, node := range workflow.Nodes {
		if nodeErr := r.createPendingNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: node.ID}, base); nodeErr != nil {
			return nodeErr
		}
	}
	if finishErr := r.finishNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "work"}, input.Status, "work_failure", base.Add(time.Second)); finishErr != nil {
		return finishErr
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(r.state, r.state, nil)
	run, intent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "fixture-completion", base.Add(2*time.Second))
	if !errors.Is(err, workflowruntime.ErrControlFlowPending) || intent == nil || intent.IntendedStatus != input.WantIntendedStatus || !run.Status.Active() {
		return fmt.Errorf("pending completion = run %q intent %#v: %w", run.Status, intent, err)
	}
	progress, err := coordinator.ProgressFinally(ctx, workflow, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "cleanup"}, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(3*time.Second))
	if err != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		return fmt.Errorf("cleanup progression = %#v: %w", progress, err)
	}
	if finishErr := r.finishNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "cleanup"}, input.CleanupStatus, "cleanup_failure", base.Add(4*time.Second)); finishErr != nil {
		return finishErr
	}
	run, intent, err = coordinator.ReconcileRunCompletion(ctx, workflow, runID, "fixture-completion", base.Add(5*time.Second))
	if err != nil || intent == nil || run.Status != input.WantRunStatus {
		return fmt.Errorf("completion = run %q intent %#v: %w", run.Status, intent, err)
	}
	return nil
}

func (r *hadronConformanceRunner) runVerificationCatchFixture(ctx context.Context) error {
	runID, base, err := r.createRunningRun(ctx, "verification-catch")
	if err != nil {
		return err
	}
	for _, nodeID := range []string{"source", "handler"} {
		if nodeErr := r.createPendingNode(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: nodeID}, base); nodeErr != nil {
			return nodeErr
		}
	}
	source := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "source"}
	if finishErr := r.finishNode(ctx, source, workflowruntime.NodeFailed, "verification_failed", base.Add(time.Second)); finishErr != nil {
		return finishErr
	}
	result, err := workflowruntime.NewControlFlowCoordinator(r.state, r.state, nil).DecideCatch(ctx, workflowruntime.DecideCatchRequest{
		Source: source,
		Node:   graph.Node{ID: "source", Catch: []graph.CatchRule{{Errors: []string{"verification_failed"}, Targets: []string{"handler"}}}},
		At:     base.Add(2 * time.Second),
	})
	if err != nil {
		return err
	}
	if result.Decision.Outcome != workflowruntime.ControlSelected || len(result.Decision.Targets) != 1 || result.Decision.Targets[0].NodeID != "handler" {
		return fmt.Errorf("verification catch decision = %#v", result.Decision)
	}
	return nil
}

func (r *hadronConformanceRunner) createRunningRun(ctx context.Context, suffix string) (workflowruntime.RunID, time.Time, error) {
	base := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("fixture-control-" + suffix)
	run, _, err := r.state.CreateRun(ctx, workflowruntime.CreateRunRequest{
		ID: runID, Plan: adoptionPlanRef(suffix), Status: workflowruntime.RunPending,
		StartIdempotencyKey: "start-" + string(runID), CreatedAt: base,
	})
	if err != nil {
		return "", time.Time{}, err
	}
	if _, err := r.state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		return "", time.Time{}, err
	}
	return runID, base, nil
}

func (r *hadronConformanceRunner) createPendingNode(ctx context.Context, id workflowruntime.NodeInvocationID, at time.Time) error {
	_, err := r.state.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodePending, CreatedAt: at, UpdatedAt: at,
	}})
	return err
}

func (r *hadronConformanceRunner) finishNode(ctx context.Context, id workflowruntime.NodeInvocationID, status workflowruntime.NodeStatus, failureCode string, at time.Time) error {
	node, err := r.state.LoadNodeInvocation(ctx, id)
	if err != nil {
		return err
	}
	if node.Status == workflowruntime.NodePending {
		ready, transitionErr := r.state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: at})
		if transitionErr != nil {
			return transitionErr
		}
		node = ready.Snapshot
	}
	claim, err := r.state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: node.ClaimGeneration, Owner: "fixture", Token: "token-" + id.NodeID,
		IdempotencyKey: "claim-" + id.NodeID, Now: at, LeaseUntil: at.Add(time.Hour),
	})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		return fmt.Errorf("claim %q = %#v: %w", id.NodeID, claim, err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	claimed, err := r.state.LoadNodeInvocation(ctx, id)
	if err != nil {
		return err
	}
	started, err := r.state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: proof,
		Executor: workflowruntime.ExecutorMetadata{Kind: "fixture", Version: "v1", Target: "local"}, At: at,
	})
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
	_, err = r.state.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: id, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation,
		ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof,
		AttemptStatus: status, NextNodeStatus: status, Failure: failure, At: at,
	})
	return err
}

func adoptionPlanRef(suffix string) workflowruntime.PlanRef {
	suffix = strings.ReplaceAll(suffix, "_", "-")
	return workflowruntime.PlanRef{
		ID: "fixture-plan-" + suffix, Version: "v1", Digest: values.SHA256Digest([]byte("fixture-plan-" + suffix)),
		SchemaVersion: workflowcompileSchemaVersion,
	}
}

const workflowcompileSchemaVersion = "workflow.execution-plan/v1"

func adoptionTargetNames(ids []workflowruntime.NodeInvocationID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = id.NodeID
	}
	return result
}
