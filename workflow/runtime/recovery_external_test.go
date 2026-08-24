package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestRecoveryReconcilesExpiredCrashAndRebuildsControlBeforeReady(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "recover", base)
	run, _ := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "recover", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	_ = run
	crashedID := invocationID("recover", "crashed")
	switchID := invocationID("recover", "switch")
	targetID := invocationID("recover", "target")
	for _, id := range []workflowruntime.NodeInvocationID{crashedID, switchID} {
		createNode(t, store, id, workflowruntime.NodeReady, 0, base.Add(time.Second))
	}
	createNode(t, store, targetID, workflowruntime.NodePending, 0, base.Add(time.Second))
	crashed := startAttemptForRecovery(t, store, crashedID, "safe", "v1", base.Add(2*time.Second), base.Add(3*time.Second))
	finishedSwitch := startAttemptForRecovery(t, store, switchID, "safe", "v1", base.Add(2*time.Second), base.Add(time.Minute))
	if _, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: switchID, AttemptNumber: 1, ExpectedNodeGeneration: finishedSwitch.Node.Generation, ExpectedAttemptGeneration: finishedSwitch.Attempt.Generation, Claim: workflowruntime.ClaimProof{Owner: finishedSwitch.Node.Lease.Owner, Token: finishedSwitch.Node.Lease.Token, Generation: finishedSwitch.Node.Lease.Generation}, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: base.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "crashed", Kind: "safe", KindVersion: "v1", Retry: &graph.RetryPolicy{Attempts: 2, Backoff: graph.BackoffPolicy{Strategy: graph.BackoffFixed, InitialDelay: "1m"}}},
		{ID: "switch", Kind: "safe", KindVersion: "v1", Switch: &graph.SwitchSpec{Default: []string{"target"}}},
		{ID: "target", Kind: "safe", KindVersion: "v1", Needs: []graph.Need{{Node: "switch"}}},
	}}
	registry := recoveryRegistry(t, graph.EffectSet{graph.EffectRead}, graph.IdempotencyIntrinsic, stepkind.RetrySafe)
	coordinator := workflowruntime.RecoveryCoordinator{Store: store, Recovery: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: registry}
	result, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(10 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Crashes) != 1 || result.Crashes[0].Node.Status != workflowruntime.NodeWaiting || result.Crashes[0].Attempt.Status != workflowruntime.NodeCrashed || result.Crashes[0].Activation == nil {
		t.Fatalf("crash recovery = %#v", result.Crashes)
	}
	if result.Crashes[0].Attempt.ID != crashed.Attempt.ID {
		t.Fatalf("crash attempt identity changed: %#v", result.Crashes[0])
	}
	target, _ := store.LoadNodeInvocation(ctx, targetID)
	if target.Status != workflowruntime.NodeReady {
		t.Fatalf("target status = %s", target.Status)
	}
	decision, err := store.LoadControlDecision(ctx, workflowruntime.ControlDecisionID{Source: switchID, Kind: workflowruntime.ControlSwitch})
	if err != nil || decision.Outcome != workflowruntime.ControlDefault {
		t.Fatalf("switch decision = %#v, %v", decision, err)
	}
	events, _ := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: "recover"})
	decisionSequence, targetSequence := uint64(0), uint64(0)
	for _, event := range events {
		if event.Type == workflowruntime.EventSwitchDecided {
			decisionSequence = event.Sequence
		}
		if event.Type == workflowruntime.EventNodeStatusChanged && event.Invocation != nil && *event.Invocation == targetID {
			targetSequence = event.Sequence
		}
	}
	if decisionSequence == 0 || targetSequence <= decisionSequence {
		t.Fatalf("control/ready event order = %d then %d", decisionSequence, targetSequence)
	}
	second, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(11 * time.Second)})
	if err != nil || len(second.Crashes) != 0 {
		t.Fatalf("repeated recovery = %#v, %v", second.Crashes, err)
	}
}

func TestRecoveryPublishesDeclaredOutputsAfterCompletedFinalizer(t *testing.T) {
	base := bindingTime()
	plan := bindingPlan(nil, []graph.OutputSpec{
		bindingOutput("cleanup", graph.Schema{"type": "string"}, "steps.cleanup.outputs.result", 30),
	}, []graph.Node{{ID: "work", Kind: "safe", KindVersion: "v1"}, {ID: "cleanup", Kind: "safe", KindVersion: "v1", Finally: &graph.FinallySpec{}}})
	store := runtimetest.NewStore()
	bound, _ := startedBindingRun(t, store, plan, "recover-finalizer-outputs")
	for _, node := range plan.Graph.Nodes {
		createNode(t, store, workflowruntime.NodeInvocationID{RunID: bound.ID, NodeID: node.ID}, workflowruntime.NodePending, 0, base.Add(time.Minute))
	}
	finishSucceededNodeWithOutput(t, store, workflowruntime.NodeInvocationID{RunID: bound.ID, NodeID: "work"}, "done", base.Add(2*time.Minute))
	control := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	if _, _, err := control.ReconcileRunCompletion(t.Context(), plan.Graph, bound.ID, "recover-complete:"+string(bound.ID), base.Add(3*time.Minute)); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("begin terminal intent = %v", err)
	}
	finishSucceededNodeWithOutput(t, store, workflowruntime.NodeInvocationID{RunID: bound.ID, NodeID: "cleanup"}, "recovered", base.Add(4*time.Minute))
	coordinator := workflowruntime.RecoveryCoordinator{
		Store: store, Recovery: store, Inputs: store, Control: store,
		Plans: exactRecoveryPlanSource{plan: *plan}, Registry: recoveryRegistry(t, graph.EffectSet{graph.EffectRead}, graph.IdempotencyIntrinsic, stepkind.RetrySafe),
	}
	if _, err := coordinator.Recover(t.Context(), workflowruntime.RecoveryRequest{Now: base.Add(5 * time.Minute)}); err != nil {
		t.Fatalf("Recover output-bearing terminal intent: %v", err)
	}
	completed, err := store.LoadRun(t.Context(), bound.ID)
	if err != nil || completed.Status != workflowruntime.RunSucceeded || completed.Outputs == nil {
		t.Fatalf("recovered run outputs = %#v, %v", completed, err)
	}
	outputs, err := store.LoadValues(t.Context(), *completed.Outputs)
	if err != nil || outputs["cleanup"].Inline != "recovered" {
		t.Fatalf("recovered output values = %#v, %v", outputs, err)
	}
	if _, err := coordinator.Recover(t.Context(), workflowruntime.RecoveryRequest{Now: base.Add(6 * time.Minute)}); err != nil {
		t.Fatalf("replayed recovery: %v", err)
	}
}

func TestCrashRecoveryUsesPinnedKindAndEffectiveEffectUnion(t *testing.T) {
	base := time.Date(2026, 8, 24, 11, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "effect", base)
	_, _ = store.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: "effect", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	id := invocationID("effect", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, base.Add(time.Second))
	startAttemptForRecovery(t, store, id, "safe", "v1", base.Add(2*time.Second), base.Add(3*time.Second))
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{ID: "work", Kind: "safe", KindVersion: "v1", Effects: graph.EffectSet{graph.EffectDestructive}}}}
	registry := recoveryRegistry(t, graph.EffectSet{graph.EffectRead}, graph.IdempotencyNone, stepkind.RetrySafe)
	var calls atomic.Int32
	policy := workflowruntime.RepeatPolicyFunc(func(_ context.Context, candidate workflowruntime.RepeatCandidate) (workflowruntime.RepeatPolicyDecision, error) {
		calls.Add(1)
		if len(candidate.Effects) != 2 || candidate.Effects[0] != graph.EffectDestructive || candidate.Effects[1] != graph.EffectRead {
			t.Fatalf("effective effects = %#v", candidate.Effects)
		}
		return workflowruntime.RepeatPolicyDecision{Allow: true, Code: "approved", Reason: "operator approved"}, nil
	})
	coordinator := workflowruntime.RecoveryCoordinator{Store: store, Recovery: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: registry, Policy: policy}
	result, err := coordinator.Recover(context.Background(), workflowruntime.RecoveryRequest{Now: base.Add(10 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 || len(result.Crashes) != 1 || result.Crashes[0].Node.Status != workflowruntime.NodeCrashed {
		t.Fatalf("unsafe union bypassed floor: calls=%d result=%#v", calls.Load(), result.Crashes)
	}

	badStore := runtimetest.NewStore()
	createRun(t, badStore, "kind", base)
	_, _ = badStore.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: "kind", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	badID := invocationID("kind", "work")
	createNode(t, badStore, badID, workflowruntime.NodeReady, 0, base.Add(time.Second))
	startAttemptForRecovery(t, badStore, badID, "safe", "v1", base.Add(2*time.Second), base.Add(3*time.Second))
	badWorkflow := workflow
	badWorkflow.Nodes[0].Kind = "different"
	bad := workflowruntime.RecoveryCoordinator{Store: badStore, Recovery: badStore, Inputs: badStore, Control: badStore, Plans: staticRecoveryPlans{graph: badWorkflow}, Registry: registry, Policy: policy}
	badResult, err := bad.Recover(context.Background(), workflowruntime.RecoveryRequest{Now: base.Add(10 * time.Second)})
	if err != nil || len(badResult.Crashes) != 1 || badResult.Crashes[0].Node.Status != workflowruntime.NodeCrashed {
		t.Fatalf("kind mismatch did not fail closed: %#v, %v", badResult, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("policy called for kind mismatch: %d", calls.Load())
	}
}

func TestReplayReusesUpstreamValuesCreatesFreshHistoryAndConverges(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "source", base)
	_, _ = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "source", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	upstreamID, downstreamID := invocationID("source", "upstream"), invocationID("source", "downstream")
	createNode(t, store, upstreamID, workflowruntime.NodeReady, 0, base.Add(time.Second))
	createNode(t, store, downstreamID, workflowruntime.NodeReady, 0, base.Add(time.Second))
	upstreamOutputs := persistedValues(t, store, "source", "upstream-output", map[string]any{"exact": json.Number("9007199254740993")})
	finishSucceededForRecovery(t, store, startAttemptForRecovery(t, store, upstreamID, "safe", "v1", base.Add(2*time.Second), base.Add(time.Minute)), &upstreamOutputs, base.Add(3*time.Second))
	finishSucceededForRecovery(t, store, startAttemptForRecovery(t, store, downstreamID, "safe", "v1", base.Add(4*time.Second), base.Add(time.Minute)), nil, base.Add(5*time.Second))
	run, _ := store.LoadRun(ctx, "source")
	_, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "source", ExpectedGeneration: run.Generation, To: workflowruntime.RunSucceeded, At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{ID: "upstream", Kind: "safe", KindVersion: "v1"}, {ID: "downstream", Kind: "safe", KindVersion: "v1", Needs: []graph.Need{{Node: "upstream"}}}}}
	registry := recoveryRegistry(t, graph.EffectSet{graph.EffectCompute}, graph.IdempotencyIntrinsic, stepkind.RetrySafe)
	service := workflowruntime.ReplayService{Store: store, Replay: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: registry}
	request := workflowruntime.ReplayRequest{SourceRunID: "source", RunID: "replay", FromNodeID: "downstream", IdempotencyKey: "replay-key", At: base.Add(10 * time.Second)}
	result, err := service.Rerun(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != workflowruntime.IdempotencyApplied || result.Provenance.SourceRunID != "source" || result.Provenance.PlanDigest != testPlan().Digest || len(result.Provenance.Policy) != 1 {
		t.Fatalf("replay result = %#v", result)
	}
	reused, _ := store.LoadNodeInvocation(ctx, invocationID("replay", "upstream"))
	fresh, _ := store.LoadNodeInvocation(ctx, invocationID("replay", "downstream"))
	if reused.Status != workflowruntime.NodeSucceeded || reused.Origin != workflowruntime.OriginReplayed || reused.Outputs == nil || *reused.Outputs != upstreamOutputs || reused.LatestAttempt != 1 {
		t.Fatalf("reused upstream = %#v", reused)
	}
	reusedAttempts, _ := store.ListAttempts(ctx, reused.ID)
	if len(reusedAttempts) != 1 || reusedAttempts[0].Status != workflowruntime.NodeSucceeded || reusedAttempts[0].ID.Invocation != reused.ID {
		t.Fatalf("reused attempts = %#v", reusedAttempts)
	}
	if fresh.Status != workflowruntime.NodeReady || fresh.LatestAttempt != 0 {
		t.Fatalf("fresh downstream = %#v", fresh)
	}
	loaded, err := store.LoadValues(ctx, *reused.Outputs)
	if err != nil || loaded["payload"].Inline.(map[string]any)["exact"] != json.Number("9007199254740993") {
		t.Fatalf("reused exact values = %#v, %v", loaded, err)
	}
	replayed, err := service.Rerun(ctx, request)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("replay convergence = %#v, %v", replayed, err)
	}
	invocations, _ := store.ListRunInvocations(ctx, "replay")
	if len(invocations) != 2 {
		t.Fatalf("replay duplicated nodes: %d", len(invocations))
	}
}

func TestCrashStoreConcurrentExactReplayProducesOneEvent(t *testing.T) {
	base := time.Date(2026, 8, 24, 13, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "race", base)
	_, _ = store.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: "race", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	id := invocationID("race", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, base.Add(time.Second))
	started := startAttemptForRecovery(t, store, id, "safe", "v1", base.Add(2*time.Second), base.Add(3*time.Second))
	fireAt := base.Add(time.Minute)
	request := workflowruntime.ReconcileCrashedAttemptRequest{Attempt: started.Attempt.ID, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, IdempotencyKey: "crash-race", Decision: workflowruntime.CrashRecoveryDecision{Action: workflowruntime.CrashRetry, Policy: workflowruntime.RepeatPolicyDecision{Allow: true, Code: "safe", Reason: "safe"}, Retry: &workflowruntime.RetryDecision{Retry: true, Reason: workflowruntime.RetryReasonEligible, FireAt: fireAt, Delay: fireAt.Sub(base.Add(10 * time.Second))}}, At: base.Add(10 * time.Second)}
	var wg sync.WaitGroup
	outcomes := make(chan workflowruntime.IdempotencyOutcome, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.ReconcileCrashedAttempt(context.Background(), request)
			if err != nil {
				t.Errorf("Reconcile: %v", err)
				return
			}
			outcomes <- result.Outcome
		}()
	}
	wg.Wait()
	close(outcomes)
	applied, replayed := 0, 0
	for outcome := range outcomes {
		switch outcome {
		case workflowruntime.IdempotencyApplied:
			applied++
		case workflowruntime.IdempotencyReplayed:
			replayed++
		}
	}
	events, _ := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: "race"})
	crashes := 0
	for _, event := range events {
		if event.Type == workflowruntime.EventCrashReconciled {
			crashes++
		}
	}
	if applied != 1 || replayed != 15 || crashes != 1 {
		t.Fatalf("outcomes applied=%d replayed=%d crash events=%d", applied, replayed, crashes)
	}
	offset := time.FixedZone("crash-offset", 5*60*60)
	offsetRequest := request
	offsetRequest.At = request.At.In(offset)
	retry := *request.Decision.Retry
	retry.FireAt = retry.FireAt.In(offset)
	offsetRequest.Decision.Retry = &retry
	offsetReplay, err := store.ReconcileCrashedAttempt(context.Background(), offsetRequest)
	if err != nil || offsetReplay.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("same-instant crash replay = %#v, %v", offsetReplay, err)
	}
}

func TestRecoveryCrashConvergesAcrossDifferentWorkerClocks(t *testing.T) {
	base := time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "clock-race", base)
	_, _ = store.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: "clock-race", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	id := invocationID("clock-race", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, base.Add(time.Second))
	startAttemptForRecovery(t, store, id, "safe", "v1", base.Add(2*time.Second), base.Add(3*time.Second))
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{
		ID: "work", Kind: "safe", KindVersion: "v1",
		Retry: &graph.RetryPolicy{Attempts: 2, Backoff: graph.BackoffPolicy{Strategy: graph.BackoffFixed, InitialDelay: "1m"}},
	}}}
	barrier := &barrierRecoveryStore{RecoveryStore: store, ready: make(chan struct{}), release: make(chan struct{})}
	registry := recoveryRegistry(t, graph.EffectSet{graph.EffectRead}, graph.IdempotencyIntrinsic, stepkind.RetrySafe)
	coordinator := workflowruntime.RecoveryCoordinator{Store: store, Recovery: barrier, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: registry}
	results := make(chan workflowruntime.RecoveryResult, 2)
	errs := make(chan error, 2)
	for _, now := range []time.Time{base.Add(10 * time.Second), base.Add(30 * time.Second)} {
		go func(now time.Time) {
			result, err := coordinator.Recover(context.Background(), workflowruntime.RecoveryRequest{Now: now})
			results <- result
			errs <- err
		}(now)
	}
	<-barrier.ready
	<-barrier.ready
	close(barrier.release)
	applied, replayed := 0, 0
	for range 2 {
		result, err := <-results, <-errs
		if err != nil || len(result.Crashes) != 1 {
			t.Fatalf("clock-race recovery = %#v, %v", result, err)
		}
		switch result.Crashes[0].Outcome {
		case workflowruntime.IdempotencyApplied:
			applied++
		case workflowruntime.IdempotencyReplayed:
			replayed++
		}
	}
	if applied != 1 || replayed != 1 {
		t.Fatalf("clock-race outcomes applied=%d replayed=%d", applied, replayed)
	}
}

func TestRecoveryReadyRebuildConvergesReverseIDDependencyChain(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "reverse", base)
	run, _ := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "reverse", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base})
	createNode(t, store, invocationID("reverse", "a"), workflowruntime.NodePending, 0, base)
	createNode(t, store, invocationID("reverse", "z"), workflowruntime.NodePending, 0, base)
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "a", Kind: "safe", KindVersion: "v1", ReadyWhen: graph.ReadyAllDone, Needs: []graph.Need{{Node: "z"}}},
		{ID: "z", Kind: "safe", KindVersion: "v1", If: &graph.Expression{Text: "false"}},
	}}
	coordinator := workflowruntime.RecoveryCoordinator{Store: store, Recovery: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: recoveryRegistry(t, graph.EffectSet{graph.EffectRead}, graph.IdempotencyIntrinsic, stepkind.RetrySafe)}
	if _, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	first, _ := store.LoadNodeInvocation(ctx, invocationID(run.Snapshot.ID, "a"))
	predecessor, _ := store.LoadNodeInvocation(ctx, invocationID(run.Snapshot.ID, "z"))
	if predecessor.Status != workflowruntime.NodeSkipped || first.Status != workflowruntime.NodeReady {
		t.Fatalf("reverse dependency recovery a=%s z=%s", first.Status, predecessor.Status)
	}
}

func TestRecoveryFailsClosedWhenTerminalIntentCannotBeLoaded(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 15, 30, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "intent-load", base)
	if _, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "intent-load", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	createNode(t, store, invocationID("intent-load", "work"), workflowruntime.NodePending, 0, base)
	sentinel := errors.New("terminal-intent storage unavailable")
	control := terminalIntentErrorStore{ControlFlowStore: store, err: sentinel}
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{ID: "work", Kind: "safe", KindVersion: "v1"}}}
	coordinator := workflowruntime.RecoveryCoordinator{Store: store, Recovery: store, Inputs: store, Control: control, Plans: staticRecoveryPlans{graph: workflow}, Registry: recoveryRegistry(t, graph.EffectSet{graph.EffectRead}, graph.IdempotencyIntrinsic, stepkind.RetrySafe)}
	if _, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(time.Second)}); !errors.Is(err, sentinel) {
		t.Fatalf("terminal-intent load error = %v", err)
	}
}

func TestRecoveryTerminalCrashCreatesAndDrivesFinalizerSamePass(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "cleanup", base)
	_, _ = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "cleanup", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base})
	workID, cleanupID := invocationID("cleanup", "work"), invocationID("cleanup", "cleanup")
	createNode(t, store, workID, workflowruntime.NodeReady, 0, base)
	createNode(t, store, cleanupID, workflowruntime.NodePending, 0, base)
	startAttemptForRecovery(t, store, workID, "safe", "v1", base, base.Add(time.Second))
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "work", Kind: "safe", KindVersion: "v1"},
		{ID: "cleanup", Kind: "safe", KindVersion: "v1", Finally: &graph.FinallySpec{}, InputBindings: map[string]graph.Binding{"status": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "run.status"}}}},
	}}
	kind := stepkindtest.NewNoopKind("safe", "v1")
	kind.SpecValue.InputSchema = graph.Schema{"type": "object", "additionalProperties": false, "required": []any{"status"}, "properties": map[string]any{"status": map[string]any{"type": "string"}}}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	coordinator := workflowruntime.RecoveryCoordinator{Store: store, Recovery: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: registry}
	result, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(10 * time.Second)})
	if err != nil || len(result.Crashes) != 1 {
		t.Fatalf("cleanup recovery = %#v, %v", result, err)
	}
	intent, err := store.LoadTerminalIntent(ctx, "cleanup")
	cleanup, loadErr := store.LoadNodeInvocation(ctx, cleanupID)
	if err != nil || loadErr != nil || intent.Status != workflowruntime.TerminalIntentPending || cleanup.Status != workflowruntime.NodeReady || cleanup.Inputs == nil {
		t.Fatalf("driven cleanup intent=%#v node=%#v errors=%v/%v", intent, cleanup, err, loadErr)
	}
	boundInputs, err := store.LoadValues(ctx, *cleanup.Inputs)
	if err != nil || boundInputs["status"].Inline != string(intent.IntendedStatus) {
		t.Fatalf("finalizer scoped status inputs = %#v, %v", boundInputs, err)
	}
	startedCleanup := startAttemptForRecovery(t, store, cleanupID, "safe", "v1", base.Add(11*time.Second), base.Add(12*time.Second))
	second, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(20 * time.Second)})
	if err != nil || len(second.Crashes) != 1 || second.Crashes[0].Attempt.ID != startedCleanup.Attempt.ID || second.Crashes[0].Attempt.Status != workflowruntime.NodeCrashed {
		t.Fatalf("expired running finalizer recovery = %#v, %v", second, err)
	}
}

func TestRecoverySkipsSucceededInnerFinalizerAndDrivesPendingOuter(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 17, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "nested-cleanup", base)
	_, _ = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "nested-cleanup", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base})
	workID := invocationID("nested-cleanup", "work")
	innerID := invocationID("nested-cleanup", "inner")
	outerID := invocationID("nested-cleanup", "outer")
	createNode(t, store, workID, workflowruntime.NodeReady, 0, base)
	createNode(t, store, innerID, workflowruntime.NodePending, 0, base)
	createNode(t, store, outerID, workflowruntime.NodePending, 0, base)
	startAttemptForRecovery(t, store, workID, "safe", "v1", base, base.Add(time.Second))
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "work", Kind: "safe", KindVersion: "v1"},
		{ID: "inner", Kind: "safe", KindVersion: "v1", Finally: &graph.FinallySpec{Scope: []string{"work"}}},
		{ID: "outer", Kind: "safe", KindVersion: "v1", Finally: &graph.FinallySpec{Scope: []string{"work", "inner"}}},
	}}
	coordinator := workflowruntime.RecoveryCoordinator{Store: store, Recovery: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: recoveryRegistry(t, graph.EffectSet{graph.EffectRead}, graph.IdempotencyIntrinsic, stepkind.RetrySafe)}
	if _, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	inner, _ := store.LoadNodeInvocation(ctx, innerID)
	outer, _ := store.LoadNodeInvocation(ctx, outerID)
	if inner.Status != workflowruntime.NodeReady || outer.Status != workflowruntime.NodePending {
		t.Fatalf("initial nested finalizers inner=%s outer=%s", inner.Status, outer.Status)
	}
	finishSucceededForRecovery(t, store, startAttemptForRecovery(t, store, innerID, "safe", "v1", base.Add(11*time.Second), base.Add(time.Minute)), nil, base.Add(12*time.Second))
	if _, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(13 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	inner, _ = store.LoadNodeInvocation(ctx, innerID)
	outer, _ = store.LoadNodeInvocation(ctx, outerID)
	if inner.Status != workflowruntime.NodeSucceeded || outer.Status != workflowruntime.NodeReady {
		t.Fatalf("resumed nested finalizers inner=%s outer=%s", inner.Status, outer.Status)
	}
}

func TestReplayRequestRejectsFreshControlAndForgedFanOutBindings(t *testing.T) {
	base := time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)
	inputRef := values.ValueSetRef{ID: "fan-input", Digest: values.SHA256Digest([]byte("fan-input"))}
	outputRef := values.ValueSetRef{ID: "fan-output", Digest: values.SHA256Digest([]byte("fan-output"))}
	sourceParent := invocationID("source", "fan")
	sourceItem := sourceParent
	sourceItem.Iteration = workflowruntime.FanOutIteration(0)
	targetParent := invocationID("target", "fan")
	targetItem := targetParent
	targetItem.Iteration = workflowruntime.FanOutIteration(0)
	sourceFanOut := workflowruntime.FanOutSnapshot{Parent: sourceParent, ItemName: "item", IndexName: "index", Status: workflowruntime.FanOutSucceeded, Items: []workflowruntime.FanOutItemBinding{{Index: 0, Iteration: sourceItem.Iteration, Invocation: sourceItem, Inputs: inputRef}}, Outputs: &outputRef, Generation: 1, CreatedAt: base, UpdatedAt: base}
	targetFanOut := sourceFanOut
	targetFanOut.Parent = targetParent
	targetFanOut.Items = append([]workflowruntime.FanOutItemBinding(nil), sourceFanOut.Items...)
	targetFanOut.Items[0].Invocation = targetItem
	nodes := []workflowruntime.ReplayNodeBinding{
		{Source: workflowruntime.NodeInvocationSnapshot{ID: sourceParent, Status: workflowruntime.NodeSucceeded, Priority: 7, Generation: 1, CreatedAt: base, UpdatedAt: base}, Target: targetParent, Reuse: true},
		{Source: workflowruntime.NodeInvocationSnapshot{ID: sourceItem, Status: workflowruntime.NodeSucceeded, Priority: 7, Generation: 1, CreatedAt: base, UpdatedAt: base}, Target: targetItem, Reuse: true},
	}
	request := workflowruntime.BeginReplayRequest{Provenance: workflowruntime.ReplayProvenance{RunID: "target", SourceRunID: "source", FromNodeID: "fan", PlanDigest: testPlan().Digest, IdempotencyKey: "fan-replay", CreatedAt: base}, Plan: testPlan(), ExpectedSourceGeneration: 1, Nodes: nodes, FanOuts: []workflowruntime.ReplayFanOutBinding{{Source: sourceFanOut, Target: targetFanOut, Results: []workflowruntime.FanOutItemResult{{Index: 0, Iteration: sourceItem.Iteration, Invocation: sourceItem, Status: workflowruntime.NodeSucceeded}}}}}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid fan-out replay = %v", err)
	}
	forged := request
	forged.FanOuts = append([]workflowruntime.ReplayFanOutBinding(nil), request.FanOuts...)
	forged.FanOuts[0].Target.MaxConcurrency = 9
	if err := forged.Validate(); err == nil {
		t.Fatal("forged fan-out target was accepted")
	}
	orphan := request
	orphan.Nodes = orphan.Nodes[:1]
	if err := orphan.Validate(); err == nil {
		t.Fatal("orphan fan-out item was accepted")
	}
	freshControl := nodes[0]
	freshControl.Reuse = false
	freshControl.Control = []workflowruntime.ControlDecisionSnapshot{{}}
	if err := freshControl.Validate("source", "target"); err == nil {
		t.Fatal("fresh replay control was accepted")
	}
}

func TestReplayRebindsFanOutAggregateItemsAndTypedFailureContext(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "fan", Kind: "safe", KindVersion: "v1", ForEach: &graph.ForEachSpec{Items: graph.Expression{Text: `["one", "two"]`}, Tolerate: &graph.ToleratedFailurePolicy{Count: 1}}, Switch: &graph.SwitchSpec{Default: []string{"after"}}},
		{ID: "after", Kind: "safe", KindVersion: "v1", Needs: []graph.Need{{Node: "fan"}}},
		{ID: "cleanup", Kind: "safe", KindVersion: "v1", Finally: &graph.FinallySpec{}},
	}}
	store := runtimetest.NewStore()
	createRun(t, store, "fan-source", base)
	_, _ = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "fan-source", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base})
	parent, downstream, cleanup := invocationID("fan-source", "fan"), invocationID("fan-source", "after"), invocationID("fan-source", "cleanup")
	createNode(t, store, parent, workflowruntime.NodePending, 4, base)
	createNode(t, store, downstream, workflowruntime.NodeReady, 2, base)
	createNode(t, store, cleanup, workflowruntime.NodeReady, 1, base)
	coordinator := workflowruntime.FanOutCoordinator{Store: store}
	expanded, expandErr := coordinator.Expand(ctx, workflowruntime.FanOutExpandCommand{Parent: parent, ExpectedParentGeneration: 1, Spec: graph.ForEachSpec{Items: graph.Expression{Text: `["one", "two"]`}, Tolerate: &graph.ToleratedFailurePolicy{Count: 1}}, Priority: 4, At: base.Add(time.Second)})
	if expandErr != nil || len(expanded.Children) != 2 {
		t.Fatalf("Expand = %#v, %v", expanded, expandErr)
	}
	first := startAttemptForRecovery(t, store, expanded.Children[0].ID, "safe", "v1", base.Add(2*time.Second), base.Add(time.Minute))
	finishSucceededForRecovery(t, store, first, nil, base.Add(3*time.Second))
	second := startAttemptForRecovery(t, store, expanded.Children[1].ID, "safe", "v1", base.Add(4*time.Second), base.Add(time.Minute))
	secondProof := workflowruntime.ClaimProof{Owner: second.Node.Lease.Owner, Token: second.Node.Lease.Token, Generation: second.Node.Lease.Generation}
	failure := workflowruntime.Failure{Code: "item_failed", Message: "item failed", Retryable: false}
	if _, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: second.Node.ID, AttemptNumber: second.Attempt.ID.Number, ExpectedNodeGeneration: second.Node.Generation, ExpectedAttemptGeneration: second.Attempt.Generation, Claim: secondProof, AttemptStatus: workflowruntime.NodeFailed, NextNodeStatus: workflowruntime.NodeFailed, Failure: &failure, At: base.Add(5 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := coordinator.Collect(ctx, parent, base.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{Source: parent, Node: workflow.Nodes[0], At: base.Add(7 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	finishSucceededForRecovery(t, store, startAttemptForRecovery(t, store, downstream, "safe", "v1", base.Add(8*time.Second), base.Add(time.Minute)), nil, base.Add(9*time.Second))
	finishSucceededForRecovery(t, store, startAttemptForRecovery(t, store, cleanup, "safe", "v1", base.Add(10*time.Second), base.Add(time.Minute)), nil, base.Add(11*time.Second))
	run, _ := store.LoadRun(ctx, "fan-source")
	if _, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunSucceeded, At: base.Add(12 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	registry := recoveryRegistry(t, graph.EffectSet{graph.EffectCompute}, graph.IdempotencyIntrinsic, stepkind.RetrySafe)
	service := workflowruntime.ReplayService{Store: store, Replay: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: registry}
	result, replayErr := service.Rerun(ctx, workflowruntime.ReplayRequest{SourceRunID: "fan-source", RunID: "fan-replay", FromNodeID: "after", IdempotencyKey: "fan-replay", At: base.Add(13 * time.Second)})
	if replayErr != nil || result.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("fan-out replay = %#v, %v", result, replayErr)
	}
	rebound, fanOutErr := store.LoadFanOut(ctx, invocationID("fan-replay", "fan"))
	items, itemsErr := store.LoadFanOutItemResults(ctx, rebound.Parent)
	expression, expressionErr := workflowruntime.BuildExpressionContext(ctx, store, store, workflow, "fan-replay")
	if fanOutErr != nil || itemsErr != nil || expressionErr != nil || len(rebound.Items) != 2 || len(items) != 2 || items[1].Failure == nil || expression.Steps["fan"].Items[1].Error == nil {
		t.Fatalf("replayed fan-out=%#v items=%#v context=%#v errors=%v/%v/%v", rebound, items, expression.Steps["fan"], fanOutErr, itemsErr, expressionErr)
	}
	parentNode, _ := store.LoadNodeInvocation(ctx, rebound.Parent)
	if parentNode.Priority != 4 || parentNode.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("replayed fan-out parent = %#v", parentNode)
	}
	replayedAfter, loadErr := store.LoadNodeInvocation(ctx, invocationID("fan-replay", "after"))
	if loadErr != nil || replayedAfter.Status != workflowruntime.NodeReady {
		t.Fatalf("replayed selected route = %#v, %v", replayedAfter, loadErr)
	}
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: replayedAfter.ID, ExpectedGeneration: replayedAfter.Generation, To: workflowruntime.NodeSkipped, At: base.Add(14 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	finalizers, finalizerErr := workflowruntime.PlanFinalizerScopes(workflow, "fan-replay")
	if finalizerErr != nil {
		t.Fatal(finalizerErr)
	}
	replayedRun, _ := store.LoadRun(ctx, "fan-replay")
	intentResult, intentErr := store.BeginTerminalIntent(ctx, workflowruntime.BeginTerminalIntentRequest{RunID: replayedRun.ID, ExpectedRunGeneration: replayedRun.Generation, IntendedStatus: workflowruntime.RunSucceeded, IdempotencyKey: "explain-terminal", Finalizers: finalizers, At: base.Add(15 * time.Second)})
	if intentErr != nil || intentResult.Intent.Status != workflowruntime.TerminalIntentPending {
		t.Fatalf("explain terminal intent = %#v, %v", intentResult, intentErr)
	}
	explained, explainErr := (workflowruntime.ExplainService{Store: store, Control: store, Replay: store, Plans: staticRecoveryPlans{graph: workflow}}).Explain(ctx, "fan-replay", base.Add(16*time.Second))
	if explainErr != nil || explained.Replay == nil || explained.TerminalIntent == nil || len(explained.Decisions) != 1 || len(explained.Invocations) != 5 || len(explained.Events) == 0 || len(explained.Recovery.ActiveRuns) != 1 || explained.Recovery.ActiveRuns[0].ID != "fan-replay" {
		t.Fatalf("replay explanation = %#v, %v", explained, explainErr)
	}
	dynamicAttempts := 0
	for _, invocation := range explained.Invocations {
		if invocation.Node.ID.NodeID == "fan" && invocation.Node.ID.Iteration != "" {
			dynamicAttempts += len(invocation.Attempts)
		}
	}
	if dynamicAttempts != 2 || explained.Decisions[0].ID.Source != invocationID("fan-replay", "fan") {
		t.Fatalf("dynamic attempt/control explanation = %#v / %#v", explained.Invocations, explained.Decisions)
	}
	missing := workflow
	missing.Nodes = missing.Nodes[:2]
	if _, err := (workflowruntime.ExplainService{Store: store, Control: store, Replay: store, Plans: staticRecoveryPlans{graph: missing}}).Explain(ctx, "fan-replay", base.Add(16*time.Second)); !errors.Is(err, workflowruntime.ErrInvalidRecovery) {
		t.Fatalf("explain accepted invocation absent from pinned graph: %v", err)
	}
}

type barrierRecoveryStore struct {
	workflowruntime.RecoveryStore
	ready   chan struct{}
	release chan struct{}
}

type terminalIntentErrorStore struct {
	workflowruntime.ControlFlowStore
	err error
}

func (s terminalIntentErrorStore) LoadTerminalIntent(context.Context, workflowruntime.RunID) (workflowruntime.TerminalIntentSnapshot, error) {
	return workflowruntime.TerminalIntentSnapshot{}, s.err
}

func (s *barrierRecoveryStore) ReconcileCrashedAttempt(ctx context.Context, request workflowruntime.ReconcileCrashedAttemptRequest) (workflowruntime.ReconcileCrashedAttemptResult, error) {
	s.ready <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return workflowruntime.ReconcileCrashedAttemptResult{}, ctx.Err()
	}
	return s.RecoveryStore.ReconcileCrashedAttempt(ctx, request)
}

type staticRecoveryPlans struct{ graph graph.Graph }

func (s staticRecoveryPlans) LoadRecoveryPlan(_ context.Context, run workflowruntime.RunSnapshot) (workflowruntime.RecoveryPlan, error) {
	plan := workflowcompile.ExecutionPlan{SchemaVersion: run.Plan.SchemaVersion, ID: run.Plan.ID, Digest: run.Plan.Digest, Graph: s.graph}
	inferred := workflowcompile.InferValueDependencies(&plan, workflowcompile.DependencyOptions{})
	return workflowruntime.RecoveryPlan{Ref: run.Plan, Plan: plan, Visibility: inferred.Visibility}, nil
}

type exactRecoveryPlanSource struct{ plan workflowcompile.ExecutionPlan }

func (s exactRecoveryPlanSource) LoadRecoveryPlan(_ context.Context, run workflowruntime.RunSnapshot) (workflowruntime.RecoveryPlan, error) {
	plan := s.plan
	inferred := workflowcompile.InferValueDependencies(&plan, workflowcompile.DependencyOptions{})
	return workflowruntime.RecoveryPlan{Ref: run.Plan, Plan: plan, Visibility: inferred.Visibility}, nil
}

func recoveryRegistry(t *testing.T, effects graph.EffectSet, idempotency graph.IdempotencyMode, safety stepkind.RetrySafety) *stepkind.MemoryRegistry {
	t.Helper()
	kind := stepkindtest.NewNoopKind("safe", "v1")
	kind.SpecValue.Effects, kind.SpecValue.Idempotency, kind.SpecValue.RetrySafety = effects, idempotency, safety
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	return registry
}

func startAttemptForRecovery(t *testing.T, store workflowruntime.StateStore, id workflowruntime.NodeInvocationID, kind, version string, at, leaseUntil time.Time) workflowruntime.StartNodeAttemptResult {
	t.Helper()
	node, _ := store.LoadNodeInvocation(context.Background(), id)
	proof := claimNode(t, store, id, node.ClaimGeneration, "worker", "token-"+id.NodeID+"-"+id.Iteration, "claim-"+string(id.RunID)+"-"+id.NodeID+"-"+id.Iteration, at, leaseUntil)
	node, _ = store.LoadNodeInvocation(context.Background(), id)
	result, err := store.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: node.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: kind, Version: version}, At: at})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func finishSucceededForRecovery(t *testing.T, store workflowruntime.StateStore, started workflowruntime.StartNodeAttemptResult, outputs *values.ValueSetRef, at time.Time) {
	t.Helper()
	proof := workflowruntime.ClaimProof{Owner: started.Node.Lease.Owner, Token: started.Node.Lease.Token, Generation: started.Node.Lease.Generation}
	_, err := store.FinishNodeAttempt(context.Background(), workflowruntime.FinishNodeAttemptRequest{InvocationID: started.Node.ID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, Outputs: outputs, At: at})
	if err != nil {
		t.Fatal(err)
	}
}
