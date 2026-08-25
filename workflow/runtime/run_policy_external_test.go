package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestRunPolicyFailFastFencesOrdinaryWorkAndPreservesCleanup(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "run-policy-fail-fast")
	source := invocationID(runID, "source")
	ready := invocationID(runID, "ready")
	running := invocationID(runID, "running")
	cleanup := invocationID(runID, "cleanup")
	for _, id := range []workflowruntime.NodeInvocationID{source, ready, running, cleanup} {
		createNode(t, store, id, workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, source, workflowruntime.NodeFailed, base.Add(2*time.Second))
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: ready, ExpectedGeneration: 1, To: workflowruntime.NodeReady, At: base.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: running, ExpectedGeneration: 1, To: workflowruntime.NodeReady, At: base.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	requirements := resourceRequirements(t, runID, workflowruntime.SchedulerLimits{Workers: 4, Effects: map[graph.Effect]int{graph.EffectMutate: 1}}, workflowruntime.SchedulerDemand{Effects: graph.EffectSet{graph.EffectMutate}})
	admitted, err := store.AdmitNode(ctx, schedulerAdmission(running, 0, "fail-fast-running", requirements, base.Add(3*time.Second), base.Add(time.Minute)))
	if err != nil || !admitted.Claim.Acquired {
		t.Fatalf("running admission = %#v, %v", admitted, err)
	}
	runningNode, _ := store.LoadNodeInvocation(ctx, running)
	runningProof := proofFromLease(*admitted.Claim.Lease)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: running, ExpectedNodeGeneration: runningNode.Generation, Claim: runningProof, Executor: testExecutor(), At: base.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	workflow := graph.Graph{
		ID: "plan", Version: "v1", Completion: &graph.RunCompletionPolicy{Mode: graph.CompletionFailFast},
		Nodes: []graph.Node{{ID: "source"}, {ID: "ready"}, {ID: "running"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}},
	}
	coordinator := workflowruntime.NewRunPolicyCoordinator(store, store, store)
	result, err := coordinator.HandleFailure(ctx, workflow, source, "fail-fast-source", base.Add(5*time.Second))
	if err != nil || result.Disposition != workflowruntime.RunFailureFailFast || result.Intent.Status != workflowruntime.TerminalIntentPending || !result.Run.Status.Active() || result.Decision.Trigger != source {
		t.Fatalf("fail-fast result = %#v, %v", result, err)
	}
	readyNode, _ := store.LoadNodeInvocation(ctx, ready)
	cleanupNode, _ := store.LoadNodeInvocation(ctx, cleanup)
	runningNode, _ = store.LoadNodeInvocation(ctx, running)
	if readyNode.Status != workflowruntime.NodeCanceled || cleanupNode.Status != workflowruntime.NodePending || runningNode.Status != workflowruntime.NodeRunning || len(result.Intents) != 1 || result.Intents[0].Kind != workflowruntime.CancellationRunningAttempt {
		t.Fatalf("fail-fast nodes ready=%#v running=%#v cleanup=%#v intents=%#v", readyNode, runningNode, cleanupNode, result.Intents)
	}
	lateRequirements := resourceRequirements(t, runID, workflowruntime.SchedulerLimits{Workers: 4}, workflowruntime.SchedulerDemand{})
	late, err := store.AdmitNode(ctx, schedulerAdmission(ready, readyNode.ClaimGeneration, "late-after-fail-fast", lateRequirements, base.Add(6*time.Second), base.Add(time.Minute)))
	if err != nil || late.Claim.Acquired {
		t.Fatalf("late ordinary admission = %#v, %v", late, err)
	}
	resourceState, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: runID, Now: base.Add(6 * time.Second)})
	if err != nil || len(resourceState.Holders) != len(requirements) {
		t.Fatalf("running cancellation holders = %#v, %v", resourceState, err)
	}
	control := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	if _, progressErr := control.ProgressFinally(ctx, workflow, cleanup, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(6*time.Second)); !errors.Is(progressErr, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("cleanup before cancellation resolution = %v", progressErr)
	}
	resolved, err := store.ResolveCancellationIntent(ctx, workflowruntime.ResolveCancellationIntentRequest{IntentID: result.Intents[0].ID, ExpectedGeneration: result.Intents[0].Generation, At: base.Add(7 * time.Second)})
	if err != nil || resolved.Node == nil || resolved.Node.Status != workflowruntime.NodeCanceled || resolved.Attempt == nil || resolved.Attempt.ID != started.Attempt.ID {
		t.Fatalf("resolve fail-fast cancellation = %#v, %v", resolved, err)
	}
	resourceState, err = store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: runID, Now: base.Add(7 * time.Second)})
	if err != nil || len(resourceState.Holders) != 0 {
		t.Fatalf("resolved cancellation leaked resources = %#v, %v", resourceState, err)
	}
	progressed, err := control.ProgressFinally(ctx, workflow, cleanup, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(8*time.Second))
	if err != nil || progressed.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("cleanup admission = %#v, %v", progressed, err)
	}
	cleanupAdmitted, err := store.AdmitNode(ctx, schedulerAdmission(cleanup, progressed.Snapshot.ClaimGeneration, "fail-fast-cleanup", lateRequirements, base.Add(9*time.Second), base.Add(time.Minute)))
	if err != nil || !cleanupAdmitted.Claim.Acquired {
		t.Fatalf("bounded cleanup admission = %#v, %v", cleanupAdmitted, err)
	}
	cleanupClaimed, _ := store.LoadNodeInvocation(ctx, cleanup)
	cleanupProof := proofFromLease(*cleanupAdmitted.Claim.Lease)
	cleanupStarted, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: cleanup, ExpectedNodeGeneration: cleanupClaimed.Generation, Claim: cleanupProof, Executor: testExecutor(), At: base.Add(9 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, finishErr := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: cleanup, AttemptNumber: cleanupStarted.Attempt.ID.Number, ExpectedNodeGeneration: cleanupStarted.Node.Generation, ExpectedAttemptGeneration: cleanupStarted.Attempt.Generation, Claim: cleanupProof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: base.Add(10 * time.Second)}); finishErr != nil {
		t.Fatal(finishErr)
	}
	run, _ := store.LoadRun(ctx, runID)
	intent, _ := store.LoadTerminalIntent(ctx, runID)
	completed, err := store.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: runID, ExpectedRunGeneration: run.Generation, ExpectedIntentGeneration: intent.Generation, At: base.Add(11 * time.Second)})
	if err != nil || completed.Run.Status != workflowruntime.RunFailed || completed.Intent.Status != workflowruntime.TerminalIntentCompleted {
		t.Fatalf("completed fail-fast run = %#v, %v", completed, err)
	}
	events, err := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	failFastEvents := 0
	for _, event := range events {
		if event.Type == workflowruntime.EventRunFailFastTriggered {
			failFastEvents++
			if event.Values == nil || event.Attributes["trigger_node"] != source.NodeID {
				t.Fatalf("fail-fast event = %#v", event)
			}
		}
	}
	if failFastEvents != 1 {
		t.Fatalf("fail-fast event count = %d", failFastEvents)
	}
}

func TestRunPolicyRunToCompletionAndToleratedFanOutContinue(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "run-policy-continue")
	source := invocationID(runID, "source")
	independent := invocationID(runID, "independent")
	for _, id := range []workflowruntime.NodeInvocationID{source, independent} {
		createNode(t, store, id, workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, source, workflowruntime.NodeFailed, base.Add(2*time.Second))
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: independent, ExpectedGeneration: 1, To: workflowruntime.NodeReady, At: base.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	workflow := graph.Graph{ID: "plan", Version: "v1", Completion: &graph.RunCompletionPolicy{Mode: graph.CompletionRunToCompletion}, Nodes: []graph.Node{{ID: "source"}, {ID: "independent"}}}
	result, err := workflowruntime.NewRunPolicyCoordinator(store, store, store).HandleFailure(ctx, workflow, source, "continue-source", base.Add(3*time.Second))
	if err != nil || result.Disposition != workflowruntime.RunFailureContinue {
		t.Fatalf("run-to-completion = %#v, %v", result, err)
	}
	claim, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: independent, Owner: "continue", Token: "continue", IdempotencyKey: "continue-claim", Now: base.Add(4 * time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || !claim.Acquired {
		t.Fatalf("independent branch claim = %#v, %v", claim, err)
	}
	if _, err := store.LoadRunPolicyDecision(ctx, runID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("run-to-completion persisted fail-fast decision = %v", err)
	}

	// Item failures do not trigger run policy directly. Only the durable
	// aggregate participates after tolerance is applied.
	child := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "bulk", Iteration: "item-a"}
	if _, err := workflowruntime.NewRunPolicyCoordinator(store, store, store).HandleFailure(ctx, graph.Graph{ID: "plan", Version: "v1"}, child, "child-policy", base.Add(5*time.Second)); !errors.Is(err, workflowruntime.ErrInvalidRunPolicy) {
		t.Fatalf("fan-out child policy = %v", err)
	}
}

func TestRunPolicyZeroFinalizerIntentWaitsCancellationAndPublicBeginRejectsIt(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "run-policy-zero-finalizer")
	source := invocationID(runID, "source")
	running := invocationID(runID, "running")
	for _, id := range []workflowruntime.NodeInvocationID{source, running} {
		createNode(t, store, id, workflowruntime.NodePending, 0, base)
	}
	makeTerminalNode(t, store, source, workflowruntime.NodeFailed, base.Add(2*time.Second))
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: running, ExpectedGeneration: 1, To: workflowruntime.NodeReady, At: base.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	claim := claimNode(t, store, running, 0, "zero-worker", "zero-token", "zero-claim", base.Add(3*time.Second), base.Add(time.Minute))
	runningNode, _ := store.LoadNodeInvocation(ctx, running)
	if _, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: running, ExpectedNodeGeneration: runningNode.Generation, Claim: claim, Executor: testExecutor(), At: base.Add(4 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	workflow := graph.Graph{ID: "plan", Version: "v1", Completion: &graph.RunCompletionPolicy{Mode: graph.CompletionFailFast}, Nodes: []graph.Node{{ID: "source"}, {ID: "running"}}}
	result, err := workflowruntime.NewRunPolicyCoordinator(store, store, store).HandleFailure(ctx, workflow, source, "zero-finalizer-policy", base.Add(5*time.Second))
	if err != nil || result.Disposition != workflowruntime.RunFailureFailFast || len(result.Intent.Finalizers) != 0 || len(result.Intents) != 1 {
		t.Fatalf("zero-finalizer fail-fast = %#v, %v", result, err)
	}
	if _, completionErr := store.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: runID, ExpectedRunGeneration: result.Run.Generation, ExpectedIntentGeneration: result.Intent.Generation, At: base.Add(6 * time.Second)}); !errors.Is(completionErr, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("completion before cancellation resolution = %v", completionErr)
	}
	if _, resolutionErr := store.ResolveCancellationIntent(ctx, workflowruntime.ResolveCancellationIntentRequest{IntentID: result.Intents[0].ID, ExpectedGeneration: result.Intents[0].Generation, At: base.Add(7 * time.Second)}); resolutionErr != nil {
		t.Fatal(resolutionErr)
	}
	run, _ := store.LoadRun(ctx, runID)
	intent, _ := store.LoadTerminalIntent(ctx, runID)
	completed, err := store.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: runID, ExpectedRunGeneration: run.Generation, ExpectedIntentGeneration: intent.Generation, At: base.Add(8 * time.Second)})
	if err != nil || completed.Run.Status != workflowruntime.RunFailed {
		t.Fatalf("zero-finalizer completion = %#v, %v", completed, err)
	}

	otherCtx, otherStore, otherBase, otherRun := controlFixture(t, "public-empty-intent")
	emptyReason := workflowruntime.Failure{Code: "empty", Message: "empty finalizer request"}
	errorValue, err := workflowruntime.NewRunFailureValue(otherRun, workflowruntime.RunFailed, emptyReason)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := otherStore.LoadRun(otherCtx, otherRun)
	if _, err := otherStore.BeginTerminalIntent(otherCtx, workflowruntime.BeginTerminalIntentRequest{RunID: otherRun, ExpectedRunGeneration: other.Generation, IntendedStatus: workflowruntime.RunFailed, Reason: &emptyReason, ErrorValues: values.ValueSet{"error": errorValue}, IdempotencyKey: "public-empty", At: otherBase.Add(2 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("public empty terminal intent = %v", err)
	}
}

func TestRunPolicyAcceptsExactPreAttemptQueueTimeout(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "run-policy-queue-timeout")
	source := invocationID(runID, "source")
	createNode(t, store, source, workflowruntime.NodePending, 0, base)
	timedOut, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: source, ExpectedGeneration: 1, To: workflowruntime.NodeTimedOut, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	failure := workflowruntime.TimeoutFailure(workflowruntime.TimeoutQueue, base.Add(time.Second))
	workflow := graph.Graph{ID: "plan", Version: "v1", Completion: &graph.RunCompletionPolicy{Mode: graph.CompletionFailFast}, Nodes: []graph.Node{{ID: "source"}}}
	result, err := workflowruntime.NewRunPolicyCoordinator(store, store, store).HandleRunFailure(ctx, workflowruntime.HandleRunFailureRequest{Workflow: workflow, Source: source, Failure: &failure, IdempotencyKey: "queue-timeout-policy", At: base.Add(2 * time.Second)})
	if err != nil || result.Decision.IntendedStatus != workflowruntime.RunTimedOut || result.Decision.SourceGeneration != timedOut.Snapshot.Generation || result.Intent.Error == nil {
		t.Fatalf("pre-attempt fail-fast = %#v, %v", result, err)
	}
	set, err := store.LoadValues(ctx, *result.Intent.Error)
	if err != nil {
		t.Fatal(err)
	}
	payload := set["error"].Inline.(map[string]any)
	if payload["timeout_kind"] != string(workflowruntime.TimeoutQueue) || payload["attempt"] != nil {
		t.Fatalf("pre-attempt timeout payload = %#v", payload)
	}
}

func TestRunPolicyConcurrentFailuresConvergeOnWinningTrigger(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "run-policy-race")
	first := invocationID(runID, "first")
	second := invocationID(runID, "second")
	cleanup := invocationID(runID, "cleanup")
	for _, id := range []workflowruntime.NodeInvocationID{first, second} {
		createNode(t, store, id, workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, id, workflowruntime.NodeFailed, base.Add(2*time.Second))
	}
	createNode(t, store, cleanup, workflowruntime.NodePending, 0, base)
	workflow := graph.Graph{ID: "plan", Version: "v1", Completion: &graph.RunCompletionPolicy{Mode: graph.CompletionFailFast}, Nodes: []graph.Node{{ID: "first"}, {ID: "second"}, {ID: "cleanup", Finally: &graph.FinallySpec{}}}}
	coordinator := workflowruntime.NewRunPolicyCoordinator(store, store, store)
	type outcome struct {
		result workflowruntime.ApplyRunFailurePolicyResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	for index, source := range []workflowruntime.NodeInvocationID{first, second} {
		wg.Add(1)
		go func(index int, source workflowruntime.NodeInvocationID) {
			defer wg.Done()
			result, err := coordinator.HandleFailure(ctx, workflow, source, "race-trigger-"+source.NodeID, base.Add(time.Duration(3+index)*time.Second))
			outcomes <- outcome{result: result, err: err}
		}(index, source)
	}
	wg.Wait()
	close(outcomes)
	counts := map[workflowruntime.RunFailureDisposition]int{}
	var winner workflowruntime.ApplyRunFailurePolicyResult
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("concurrent failure = %v", outcome.err)
		}
		counts[outcome.result.Disposition]++
		if outcome.result.Disposition == workflowruntime.RunFailureFailFast {
			winner = outcome.result
		}
	}
	if counts[workflowruntime.RunFailureFailFast] != 1 || counts[workflowruntime.RunFailureAlreadyDecided] != 1 {
		t.Fatalf("failure dispositions = %#v", counts)
	}
	decision, err := store.LoadRunPolicyDecision(ctx, runID)
	if err != nil || decision.Trigger != winner.Decision.Trigger || decision.IdempotencyKey != winner.Decision.IdempotencyKey {
		t.Fatalf("winning decision = %#v, %v", decision, err)
	}
	replayed, err := coordinator.HandleFailure(ctx, workflow, decision.Trigger, decision.IdempotencyKey, decision.CreatedAt)
	if err != nil || replayed.Disposition != workflowruntime.RunFailureFailFast || replayed.Decision != decision {
		t.Fatalf("winning replay = %#v, %v", replayed, err)
	}
	if _, err := coordinator.HandleFailure(ctx, workflow, decision.Trigger, decision.IdempotencyKey, decision.CreatedAt.Add(time.Second)); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed same-key policy request = %v", err)
	}
	valuesBefore := 0
	if winner.Intent.Error != nil {
		if _, err := store.LoadValues(ctx, *winner.Intent.Error); err == nil {
			valuesBefore = 1
		}
	}
	if valuesBefore != 1 {
		t.Fatal("winning trigger omitted durable typed error")
	}
}

func TestRunPolicyTypedNilStoresFailClosed(t *testing.T) {
	var nilStore *inmemory.Store
	coordinator := workflowruntime.NewRunPolicyCoordinator(nilStore, nilStore, nilStore)
	_, err := coordinator.HandleFailure(context.Background(), graph.Graph{}, invocationID("nil-run", "node"), "nil-policy", time.Now())
	if !errors.Is(err, workflowruntime.ErrInvalidRunPolicy) {
		t.Fatalf("typed-nil policy stores = %v", err)
	}
}
