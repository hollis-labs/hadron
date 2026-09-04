package persistence

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
)

type sqliteCompensationPlanSource struct{ graph graph.Graph }

func (s sqliteCompensationPlanSource) LoadRecoveryPlan(_ context.Context, run workflowruntime.RunSnapshot) (workflowruntime.RecoveryPlan, error) {
	plan := workflowcompile.ExecutionPlan{SchemaVersion: run.Plan.SchemaVersion, ID: run.Plan.ID, Digest: run.Plan.Digest, Graph: s.graph}
	inferred := workflowcompile.InferValueDependencies(&plan, workflowcompile.DependencyOptions{})
	return workflowruntime.RecoveryPlan{Ref: run.Plan, Plan: plan, Visibility: inferred.Visibility}, nil
}

func TestWorkflowSQLiteCompensationReopenContentionRetryAndStableHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compensation.db")
	firstDB, first := openWorkflowStateTest(t, path)
	secondDB, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	second, err := NewWorkflowStateStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, first, "sqlite-compensation", base)
	running, err := first.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	entry := seedSQLiteCompensationEligibility(t, first, running.Snapshot, "effect", "undo", base.Add(2*time.Second))
	runNow, _ := first.LoadRun(ctx, run.ID)
	succeeded, err := first.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: runNow.Generation, To: workflowruntime.RunSucceeded, At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	manualRequest := workflowruntime.BeginManualCompensationRequest{RunID: run.ID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: succeeded.Snapshot.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "sqlite-manual", Authorization: values.SHA256Digest([]byte("sqlite-manual-authorization")), At: base.Add(7 * time.Second)}
	manual, err := first.BeginManualCompensation(ctx, manualRequest)
	if err != nil || manual.Ledger.Status != workflowruntime.CompensationFrozen {
		t.Fatalf("manual = %#v, %v", manual, err)
	}
	if _, cancelErr := first.CancelCompensation(ctx, workflowruntime.CancelCompensationRequest{RunID: run.ID, ExpectedLedgerGeneration: manual.Ledger.Generation, IdempotencyKey: manualRequest.IdempotencyKey, Reason: "cross-operation collision", At: base.Add(8 * time.Second)}); !errors.Is(cancelErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("cross-operation SQLite compensation key = %v", cancelErr)
	}
	plans := sqliteCompensationPlanSource{graph: graph.Graph{ID: run.Plan.ID, Version: run.Plan.Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}}
	recovered, err := second.RecoverCompensation(ctx, 10)
	if err != nil || len(recovered) != 1 || recovered[0].RunID != run.ID {
		t.Fatalf("reopen recovery = %#v, %v", recovered, err)
	}

	start := make(chan struct{})
	type outcome struct {
		progress workflowruntime.CompensationProgressResult
		err      error
	}
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, state := range []*WorkflowStateStore{first, second} {
		wg.Add(1)
		go func(state *WorkflowStateStore) {
			defer wg.Done()
			<-start
			progress, progressErr := (workflowruntime.CompensationCoordinator{Store: state, Compensation: state, Plans: plans}).Progress(ctx, run.ID, base.Add(8*time.Second))
			results <- outcome{progress: progress, err: progressErr}
		}(state)
	}
	close(start)
	wg.Wait()
	close(results)
	activated := 0
	for result := range results {
		if result.err != nil && !errors.Is(result.err, workflowruntime.ErrCASMismatch) && !errors.Is(result.err, workflowruntime.ErrCompensationConflict) {
			t.Fatalf("contention progress = %#v, %v", result.progress, result.err)
		}
		activated += len(result.progress.Activated)
	}
	entries, err := first.ListCompensationEntries(ctx, run.ID)
	if err != nil || len(entries) != 1 || entries[0].Status != workflowruntime.CompensationActive || activated != 1 {
		t.Fatalf("contention entries=%#v activated=%d err=%v", entries, activated, err)
	}
	handler := entries[0].Handler
	finishSQLiteHandler(t, first, handler, workflowruntime.NodeFailed, base.Add(9*time.Second))
	progress, err := (workflowruntime.CompensationCoordinator{Store: second, Compensation: second, Plans: plans}).Progress(ctx, run.ID, base.Add(11*time.Second))
	if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeFailed {
		t.Fatalf("failed cycle = %#v, %v", progress, err)
	}
	manualReplay, err := first.BeginManualCompensation(ctx, manualRequest)
	if err != nil || manualReplay.Outcome != workflowruntime.IdempotencyReplayed || manualReplay.Ledger.Generation != progress.Ledger.Generation || manualReplay.Ledger.Outcome != workflowruntime.CompensationOutcomeFailed {
		t.Fatalf("post-progress manual replay = %#v, %v", manualReplay, err)
	}
	retryRequest := workflowruntime.RetryCompensationRequest{RunID: run.ID, ExpectedLedgerGeneration: progress.Ledger.Generation, IdempotencyKey: "sqlite-retry", Attestation: values.SHA256Digest([]byte("sqlite-retry-authorization")), At: base.Add(12 * time.Second)}
	retried, err := second.RetryCompensation(ctx, retryRequest)
	if err != nil || retried.Status != workflowruntime.CompensationFrozen {
		t.Fatalf("retry = %#v, %v", retried, err)
	}
	thirdDB, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = thirdDB.Close() })
	third, err := NewWorkflowStateStore(thirdDB)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err = third.RecoverCompensation(ctx, 10)
	if err != nil || len(recovered) != 1 || recovered[0].Generation != retried.Generation {
		t.Fatalf("retry reopen recovery = %#v, %v", recovered, err)
	}
	progress, err = (workflowruntime.CompensationCoordinator{Store: third, Compensation: third, Plans: plans}).Progress(ctx, run.ID, base.Add(13*time.Second))
	if err != nil || len(progress.Activated) != 1 || progress.Activated[0].Entry.ID != entry.ID || len(progress.Activated[0].Entry.History) != 1 || progress.Activated[0].Entry.Handler == handler {
		t.Fatalf("retry activation = %#v, %v", progress, err)
	}
	finishSQLiteHandler(t, third, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(14*time.Second))
	progress, err = (workflowruntime.CompensationCoordinator{Store: third, Compensation: third, Plans: plans}).Progress(ctx, run.ID, base.Add(16*time.Second))
	if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded || len(progress.Ledger.Cycles) != 2 {
		t.Fatalf("retry completion = %#v, %v", progress, err)
	}
	retryReplay, err := first.RetryCompensation(ctx, retryRequest)
	if err != nil || retryReplay.Generation != progress.Ledger.Generation || retryReplay.Outcome != workflowruntime.CompensationOutcomeSucceeded {
		t.Fatalf("post-progress retry replay = %#v, %v", retryReplay, err)
	}
	immutable, err := first.LoadRun(ctx, run.ID)
	if err != nil || immutable.Status != workflowruntime.RunSucceeded || immutable.Generation != succeeded.Snapshot.Generation {
		t.Fatalf("original run changed = %#v, %v", immutable, err)
	}
	_ = firstDB
}

func TestWorkflowSQLiteFinalizerAdmissionAndAutomaticRetryAreAtomicallyOrdered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compensation-finalizer-order.db")
	_, first := openWorkflowStateTest(t, path)
	secondDB, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	second, err := NewWorkflowStateStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, first, "sqlite-compensation-finalizer-order", base)
	running, err := first.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	seedSQLiteCompensationEligibility(t, first, running.Snapshot, "effect", "undo", base.Add(2*time.Second))
	cleanup := createWorkflowTestNode(t, first, run.ID, "cleanup", base.Add(time.Second))
	failureNode := createWorkflowTestNode(t, first, run.ID, "failure", base.Add(time.Second))
	reason := workflowruntime.Failure{Code: "forward_failed", Message: "forward failed"}
	finishWorkflowControlNode(t, first, failureNode, workflowruntime.NodeFailed, reason, base.Add(5*time.Second))
	currentRun, err := first.LoadRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	typed, err := workflowruntime.NewRunFailureValue(run.ID, workflowruntime.RunFailed, reason)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := first.BeginTerminalIntent(ctx, workflowruntime.BeginTerminalIntentRequest{
		RunID: run.ID, ExpectedRunGeneration: currentRun.Generation, IntendedStatus: workflowruntime.RunFailed,
		CompensationRequired: true, Reason: &reason, ErrorValues: values.ValueSet{"error": typed}, IdempotencyKey: "sqlite-finalizer-intent",
		Finalizers: []workflowruntime.FinalizerScope{{Invocation: cleanup.ID, Order: 0}}, At: base.Add(9 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := first.FreezeCompensation(ctx, workflowruntime.FreezeCompensationRequest{
		RunID: run.ID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: intent.Run.Generation, ExpectedIntentGeneration: intent.Intent.Generation,
		Trigger: graph.CompensationOnFailure, OriginalStatus: workflowruntime.RunFailed, OriginalFailure: intent.Intent.Error,
		IdempotencyKey: "sqlite-finalizer-freeze", At: base.Add(10 * time.Second),
	})
	if err != nil || frozen.Ledger.Status != workflowruntime.CompensationFrozen {
		t.Fatalf("freeze = %#v, %v", frozen, err)
	}
	if _, transitionErr := first.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: cleanup.ID, ExpectedGeneration: cleanup.Generation, To: workflowruntime.NodeReady, At: base.Add(11 * time.Second)}); !errors.Is(transitionErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("store admitted finalizer before terminal compensation = %v", transitionErr)
	}
	workflow := graph.Graph{ID: run.Plan.ID, Version: run.Plan.Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}, Nodes: []graph.Node{
		{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "failure"}, {ID: "undo"}, {ID: "cleanup", Finally: &graph.FinallySpec{}},
	}}
	coordinator := workflowruntime.CompensationCoordinator{Store: first, Compensation: first, Plans: sqliteCompensationPlanSource{graph: workflow}}
	progress, err := coordinator.Progress(ctx, run.ID, base.Add(11*time.Second))
	if err != nil || len(progress.Activated) != 1 {
		t.Fatalf("activate = %#v, %v", progress, err)
	}
	finishSQLiteHandler(t, first, progress.Activated[0].Node.ID, workflowruntime.NodeFailed, base.Add(12*time.Second))
	progress, err = coordinator.Progress(ctx, run.ID, base.Add(14*time.Second))
	if err != nil || progress.Ledger.Status != workflowruntime.CompensationTerminal || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeFailed {
		t.Fatalf("terminal failed ledger = %#v, %v", progress, err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		_, transitionErr := first.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: cleanup.ID, ExpectedGeneration: cleanup.Generation, To: workflowruntime.NodeReady, At: base.Add(15 * time.Second)})
		results <- transitionErr
	}()
	go func() {
		<-start
		_, retryErr := second.RetryCompensation(ctx, workflowruntime.RetryCompensationRequest{RunID: run.ID, ExpectedLedgerGeneration: progress.Ledger.Generation, IdempotencyKey: "sqlite-finalizer-race-retry", Attestation: values.SHA256Digest([]byte("sqlite-finalizer-race-retry")), At: base.Add(15 * time.Second)})
		results <- retryErr
	}()
	close(start)
	successes := 0
	for range 2 {
		if raceErr := <-results; raceErr == nil {
			successes++
		} else if !errors.Is(raceErr, workflowruntime.ErrInvalidRecord) && !errors.Is(raceErr, workflowruntime.ErrCASMismatch) {
			t.Fatalf("unexpected race result = %v", raceErr)
		}
	}
	if successes != 1 {
		t.Fatalf("finalizer/retry race successes = %d, want exactly one", successes)
	}
	currentCleanup, err := first.LoadNodeInvocation(ctx, cleanup.ID)
	if err != nil {
		t.Fatal(err)
	}
	currentLedger, err := first.LoadCompensationLedger(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if currentCleanup.Status == workflowruntime.NodeReady && currentLedger.Status != workflowruntime.CompensationTerminal || currentLedger.Status == workflowruntime.CompensationFrozen && currentCleanup.Status != workflowruntime.NodePending {
		t.Fatalf("non-atomic finalizer/retry state cleanup=%s ledger=%s", currentCleanup.Status, currentLedger.Status)
	}
}

func TestWorkflowSQLiteCompensationBindingFailureConvergesBestEffort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binding-failure.db")
	_, state := openWorkflowStateTest(t, path)
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "sqlite-compensation-binding-failure", base)
	running, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	seedSQLiteCompensationEligibility(t, state, running.Snapshot, "a-bad", "undo-bad", base.Add(2*time.Second))
	seedSQLiteCompensationEligibility(t, state, running.Snapshot, "z-good", "undo-good", base.Add(3*time.Second))
	current, err := state.LoadRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	succeeded, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: current.Generation, To: workflowruntime.RunSucceeded, At: base.Add(7 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	manual, err := state.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{
		RunID: run.ID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: succeeded.Snapshot.Generation,
		OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "sqlite-binding-failure-manual",
		Authorization: values.SHA256Digest([]byte("sqlite-binding-failure-authorization")), At: base.Add(8 * time.Second),
	})
	if err != nil || manual.Ledger.Status != workflowruntime.CompensationFrozen {
		t.Fatalf("manual = %#v, %v", manual, err)
	}
	workflow := graph.Graph{ID: run.Plan.ID, Version: run.Plan.Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{
		{ID: "a-bad", Compensation: &graph.CompensationSpec{Handler: "undo-bad"}},
		{ID: "z-good", Compensation: &graph.CompensationSpec{Handler: "undo-good"}},
		{ID: "undo-bad", InputBindings: map[string]graph.Binding{"token": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: `compensation["compensation.original.outputs.7:missing"]`}}}},
		{ID: "undo-good"},
	}}
	coordinator := workflowruntime.CompensationCoordinator{Store: state, Compensation: state, Plans: sqliteCompensationPlanSource{graph: workflow}}
	progress, err := coordinator.Progress(ctx, run.ID, base.Add(9*time.Second))
	if err != nil || len(progress.Sealed) != 1 || len(progress.Activated) != 1 || progress.Sealed[0].Entry.Status != workflowruntime.CompensationFailed {
		t.Fatalf("binding failure progress = %#v, %v", progress, err)
	}
	if _, loadErr := state.LoadNodeInvocation(ctx, progress.Sealed[0].Entry.Handler); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		t.Fatalf("invalid binding materialized a handler: %v", loadErr)
	}
	finishSQLiteHandler(t, state, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(10*time.Second))
	progress, err = coordinator.Progress(ctx, run.ID, base.Add(12*time.Second))
	if err != nil || progress.Ledger.Status != workflowruntime.CompensationTerminal || progress.Ledger.Outcome != workflowruntime.CompensationOutcomePartial {
		t.Fatalf("binding failure convergence = %#v, %v", progress, err)
	}
	reopenedDB, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedDB.Close() })
	reopened, err := NewWorkflowStateStore(reopenedDB)
	if err != nil {
		t.Fatal(err)
	}
	progress, err = (workflowruntime.CompensationCoordinator{Store: reopened, Compensation: reopened, Plans: sqliteCompensationPlanSource{graph: workflow}}).Progress(ctx, run.ID, base.Add(13*time.Second))
	if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomePartial || len(progress.Activated) != 0 || len(progress.Sealed) != 0 {
		t.Fatalf("binding failure replay = %#v, %v", progress, err)
	}
}

func TestWorkflowSQLiteTerminalIntentCannotBypassZeroEntryCompensation(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "terminal-fence.db"))
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "sqlite-compensation-fence", base)
	running, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	reason := workflowruntime.Failure{Code: "test_cancel", Message: "cancel with compensation"}
	typed, err := workflowruntime.NewRunFailureValue(run.ID, workflowruntime.RunCanceled, reason)
	if err != nil {
		t.Fatal(err)
	}
	begin, err := state.BeginTerminalIntent(ctx, workflowruntime.BeginTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: running.Snapshot.Generation, IntendedStatus: workflowruntime.RunCanceled, CompensationRequired: true, Reason: &reason, ErrorValues: values.ValueSet{"error": typed}, IdempotencyKey: "sqlite-compensation-intent", At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, completionErr := state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation, At: base.Add(3 * time.Second)}); !errors.Is(completionErr, workflowruntime.ErrCompensationPending) {
		t.Fatalf("direct terminal bypass = %v", completionErr)
	}
	freezeRequest := workflowruntime.FreezeCompensationRequest{RunID: run.ID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation, Trigger: graph.CompensationOnCancel, OriginalStatus: workflowruntime.RunCanceled, OriginalFailure: begin.Intent.Error, IdempotencyKey: "sqlite-compensation-freeze", At: base.Add(3 * time.Second)}
	frozen, err := state.FreezeCompensation(ctx, freezeRequest)
	if err != nil || frozen.Ledger.Status != workflowruntime.CompensationTerminal || frozen.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded || len(frozen.Entries) != 0 {
		t.Fatalf("zero-entry freeze = %#v, %v", frozen, err)
	}
	completed, err := state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation, At: base.Add(4 * time.Second)})
	if err != nil || completed.Run.Status != workflowruntime.RunCanceled {
		t.Fatalf("completion after zero-entry ledger = %#v, %v", completed, err)
	}
	replayed, err := state.FreezeCompensation(ctx, freezeRequest)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Ledger.Generation != frozen.Ledger.Generation {
		t.Fatalf("zero-entry SQLite replay = %#v, %v", replayed, err)
	}
	events, err := state.ListEvents(ctx, workflowruntime.EventQuery{RunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	completedEvents := 0
	for _, event := range events {
		if event.Type == workflowruntime.EventCompensationCompleted {
			completedEvents++
		}
	}
	if completedEvents != 1 {
		t.Fatalf("completed SQLite events = %d, want 1", completedEvents)
	}
}

func TestWorkflowSQLiteFinishCompensableAttemptReplayBindsFullRequest(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "finish-compensable-replay.db"))
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "sqlite-compensation-finish-replay", base)
	running, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	entry, request := seedSQLiteCompensationEligibilityRequest(t, state, running.Snapshot, "effect", "undo", base.Add(2*time.Second), "child-a")
	replayed, err := state.FinishCompensableAttempt(t.Context(), request)
	if err != nil || replayed.Entry.ID != entry.ID || replayed.Finish.Attempt.ID != entry.SourceAttempt {
		t.Fatalf("same-request SQLite replay = %#v, %v", replayed, err)
	}

	checks := map[string]func(workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest{
		"handler": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.HandlerNodeID = "undo-other"
			return changed
		},
		"child": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.ChildRunID = "child-b"
			changed.Eligibility.Receipt.ChildRunID = "child-b"
			return changed
		},
		"evidence": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.Evidence.Operation = "fixture.changed"
			changed.Eligibility.Receipt.Operation = "fixture.changed"
			return changed
		},
		"original output": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.OriginalOutputs = values.ValueSet{"changed": workflowTestValue(t, "different")}
			return changed
		},
		"original error": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.OriginalError = values.ValueSet{"error": workflowTestValue(t, "different")}
			return changed
		},
		"terminal failure": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Finish.AttemptStatus = workflowruntime.NodeFailed
			changed.Finish.NextNodeStatus = workflowruntime.NodeFailed
			changed.Finish.Failure = &workflowruntime.Failure{Code: "changed", Message: "different terminal result"}
			return changed
		},
	}
	for name, change := range checks {
		t.Run(name, func(t *testing.T) {
			if _, replayErr := state.FinishCompensableAttempt(t.Context(), change(request)); !errors.Is(replayErr, workflowruntime.ErrIdempotencyConflict) {
				t.Fatalf("changed SQLite replay error = %v", replayErr)
			}
		})
	}
}

func TestWorkflowSQLiteCrashedCompensationHandlerOnTerminalParentCreatesRetry(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "compensation-handler-crash.db"))
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "sqlite-compensation-handler-crash", base)
	running, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	seedSQLiteCompensationEligibility(t, state, running.Snapshot, "effect", "undo", base.Add(2*time.Second))
	current, _ := state.LoadRun(ctx, run.ID)
	succeeded, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: current.Generation, To: workflowruntime.RunSucceeded, At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, manualErr := state.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{RunID: run.ID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: succeeded.Snapshot.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "sqlite-crash-manual", Authorization: values.SHA256Digest([]byte("sqlite-crash-manual")), At: base.Add(7 * time.Second)}); manualErr != nil {
		t.Fatal(manualErr)
	}
	plans := sqliteCompensationPlanSource{graph: graph.Graph{ID: run.Plan.ID, Version: run.Plan.Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}}
	progress, err := (workflowruntime.CompensationCoordinator{Store: state, Compensation: state, Plans: plans}).Progress(ctx, run.ID, base.Add(8*time.Second))
	if err != nil || len(progress.Activated) != 1 {
		t.Fatalf("activation = %#v, %v", progress, err)
	}
	handler := progress.Activated[0].Node
	claimed, err := state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: handler.ID, ExpectedClaimGeneration: handler.ClaimGeneration, Owner: "crash-worker", Token: "crash-token", IdempotencyKey: "crash-claim", Now: base.Add(9 * time.Second), LeaseUntil: base.Add(10 * time.Second)})
	if err != nil || claimed.Lease == nil {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	proof := workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}
	loaded, _ := state.LoadNodeInvocation(ctx, handler.ID)
	started, err := state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: handler.ID, ExpectedNodeGeneration: loaded.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "undo", Version: "v1"}, Inputs: handler.Inputs, At: base.Add(9 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := state.ReconcileCrashedAttempt(ctx, workflowruntime.ReconcileCrashedAttemptRequest{Attempt: started.Attempt.ID, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, IdempotencyKey: "sqlite-handler-crash-recovery", At: base.Add(11 * time.Second), Decision: workflowruntime.CrashRecoveryDecision{Action: workflowruntime.CrashRetry, Policy: workflowruntime.RepeatPolicyDecision{Allow: true, Code: "handler_retry", Reason: "stable compensation handler is retryable"}, Retry: &workflowruntime.RetryDecision{Retry: true, Reason: workflowruntime.RetryReasonEligible, FireAt: base.Add(12 * time.Second), Delay: time.Second}}})
	if err != nil || recovered.Node.Status != workflowruntime.NodeWaiting || recovered.Attempt.Status != workflowruntime.NodeCrashed || recovered.Activation == nil {
		t.Fatalf("crash recovery = %#v, %v", recovered, err)
	}
	immutable, err := state.LoadRun(ctx, run.ID)
	if err != nil || immutable.Status != workflowruntime.RunSucceeded || immutable.Generation != succeeded.Snapshot.Generation {
		t.Fatalf("terminal parent changed = %#v, %v", immutable, err)
	}
}

func TestWorkflowSQLiteCompensationImmutableEvidenceTriggersRejectDirectSQL(t *testing.T) {
	db, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "immutable-compensation.db"))
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "sqlite-compensation-immutable", base)
	running, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	entry := seedSQLiteCompensationEligibility(t, state, running.Snapshot, "effect", "undo", base.Add(2*time.Second))
	collecting, err := state.LoadCompensationLedger(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, cancelErr := state.CancelCompensation(ctx, workflowruntime.CancelCompensationRequest{RunID: run.ID, ExpectedLedgerGeneration: collecting.Generation, IdempotencyKey: "immutable-pretrigger-cancel", Reason: "must not preempt trigger", At: base.Add(5 * time.Second)}); !errors.Is(cancelErr, workflowruntime.ErrCompensationConflict) {
		t.Fatalf("pre-trigger SQLite cancellation = %v", cancelErr)
	}
	current, err := state.LoadRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	succeeded, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: current.Generation, To: workflowruntime.RunSucceeded, At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	manual, err := state.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{RunID: run.ID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: succeeded.Snapshot.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "immutable-manual", Authorization: values.SHA256Digest([]byte("immutable-manual-authorization")), At: base.Add(7 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	plans := sqliteCompensationPlanSource{graph: graph.Graph{ID: run.Plan.ID, Version: run.Plan.Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}}
	progress, err := (workflowruntime.CompensationCoordinator{Store: state, Compensation: state, Plans: plans}).Progress(ctx, run.ID, base.Add(8*time.Second))
	if err != nil || len(progress.Activated) != 1 {
		t.Fatalf("activate immutable fixture = %#v, %v", progress, err)
	}
	for _, phase := range []string{"", "invalid"} {
		if _, err := db.DB().ExecContext(ctx, `UPDATE workflow_node_invocations SET phase=? WHERE run_id=? AND node_id=? AND iteration=?`, phase, run.ID, progress.Activated[0].Node.ID.NodeID, progress.Activated[0].Node.ID.Iteration); err == nil {
			t.Fatalf("compensation handler phase mutation to %q unexpectedly succeeded", phase)
		}
	}
	_ = manual

	ledgerMutations := []string{
		`json_set(snapshot_json,'$.trigger','failure')`,
		`json_set(snapshot_json,'$.original_status','failed')`,
		`json_set(snapshot_json,'$.original_failure',json('{}'))`,
		`json_set(snapshot_json,'$.created_at','2000-01-01T00:00:00Z')`,
	}
	for _, mutation := range ledgerMutations {
		if _, err := db.DB().ExecContext(ctx, `UPDATE workflow_compensation_ledgers SET snapshot_json=`+mutation+` WHERE run_id=?`, run.ID); err == nil {
			t.Fatalf("ledger immutable mutation unexpectedly succeeded: %s", mutation)
		}
	}
	for _, mutation := range []string{
		`json_set(snapshot_json,'$.cycles',json_array())`,
		`json_set(snapshot_json,'$.cycles[0].number',99)`,
	} {
		if _, err := db.DB().ExecContext(ctx, `UPDATE workflow_compensation_ledgers SET snapshot_json=`+mutation+` WHERE run_id=?`, run.ID); err == nil {
			t.Fatalf("ledger cycle history mutation unexpectedly succeeded: %s", mutation)
		}
	}
	entryMutations := []string{
		`json_set(snapshot_json,'$.plan_digest','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`,
		`json_set(snapshot_json,'$.operation','tampered.operation')`,
		`json_remove(snapshot_json,'$.operation')`,
		`json_set(snapshot_json,'$.evidence_digest','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')`,
		`json_set(snapshot_json,'$.original_inputs',json('{}'))`,
		`json_set(snapshot_json,'$.original_outputs',json('{}'))`,
		`json_set(snapshot_json,'$.original_error',json('{}'))`,
		`json_set(snapshot_json,'$.receipt',json('{}'))`,
		`json_set(snapshot_json,'$.prerequisites',json_array('tampered-entry'))`,
		`json_set(snapshot_json,'$.child_run_id','tampered-child')`,
		`json_set(snapshot_json,'$.created_at','2000-01-01T00:00:00Z')`,
	}
	for _, mutation := range entryMutations {
		if _, err := db.DB().ExecContext(ctx, `UPDATE workflow_compensation_entries SET snapshot_json=`+mutation+` WHERE run_id=? AND entry_id=?`, run.ID, entry.ID); err == nil {
			t.Fatalf("entry immutable mutation unexpectedly succeeded: %s", mutation)
		}
	}
	if _, err := db.DB().ExecContext(ctx, `UPDATE workflow_compensation_entries SET snapshot_json=json_set(snapshot_json,'$.history',json_array(json_object('cycle',1))) WHERE run_id=? AND entry_id=?`, run.ID, entry.ID); err != nil {
		t.Fatalf("append history fixture: %v", err)
	}
	for _, mutation := range []string{
		`json_set(snapshot_json,'$.history',json_array())`,
		`json_set(snapshot_json,'$.history[0].cycle',2)`,
	} {
		if _, err := db.DB().ExecContext(ctx, `UPDATE workflow_compensation_entries SET snapshot_json=`+mutation+` WHERE run_id=? AND entry_id=?`, run.ID, entry.ID); err == nil {
			t.Fatalf("entry history mutation unexpectedly succeeded: %s", mutation)
		}
	}
}

func seedSQLiteCompensationEligibility(t *testing.T, state *WorkflowStateStore, run workflowruntime.RunSnapshot, source, handler string, at time.Time) workflowruntime.CompensationEntrySnapshot {
	t.Helper()
	entry, _ := seedSQLiteCompensationEligibilityRequest(t, state, run, source, handler, at, "")
	return entry
}

func seedSQLiteCompensationEligibilityRequest(t *testing.T, state *WorkflowStateStore, run workflowruntime.RunSnapshot, source, handler string, at time.Time, child workflowruntime.RunID) (workflowruntime.CompensationEntrySnapshot, workflowruntime.FinishCompensableAttemptRequest) {
	t.Helper()
	node := createWorkflowTestNode(t, state, run.ID, source, at)
	ready, err := state.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: at})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "forward-" + source, Token: "forward-token-" + source, IdempotencyKey: "forward-claim-" + source, Now: at.Add(time.Second), LeaseUntil: at.Add(time.Hour)})
	if err != nil || claimed.Lease == nil {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	proof := workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}
	current, _ := state.LoadNodeInvocation(t.Context(), node.ID)
	started, err := state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: current.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "effect", Version: "v1"}, At: at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	evidence := stepkind.ReversibilityEvidence{Operation: "fixture.effect", ReceiptSchema: graph.Schema{}}
	if child != "" {
		if _, loadErr := state.LoadRun(t.Context(), child); errors.Is(loadErr, workflowruntime.ErrNotFound) {
			createWorkflowTestRun(t, state, string(child), at.Add(-time.Second))
		} else if loadErr != nil {
			t.Fatal(loadErr)
		}
		if recordErr := state.RecordChildRun(t.Context(), workflowruntime.ChildRunLink{ParentRunID: run.ID, Invocation: node.ID, ChildRunID: child, Policy: graph.ParentCloseCancel, CreatedAt: at}); recordErr != nil {
			t.Fatal(recordErr)
		}
	}
	request := workflowruntime.FinishCompensableAttemptRequest{Finish: workflowruntime.FinishNodeAttemptRequest{InvocationID: node.ID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: at.Add(2 * time.Second)}, Eligibility: workflowruntime.CompensationEligibility{PlanDigest: run.Plan.Digest, HandlerNodeID: handler, Evidence: evidence, Receipt: stepkind.CompensationReceipt{Operation: evidence.Operation, Values: values.ValueSet{}, ChildRunID: string(child)}, ChildRunID: child}}
	finished, err := state.FinishCompensableAttempt(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	return finished.Entry, request
}

func finishSQLiteHandler(t *testing.T, state *WorkflowStateStore, id workflowruntime.NodeInvocationID, status workflowruntime.NodeStatus, at time.Time) {
	t.Helper()
	node, err := state.LoadNodeInvocation(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: id, ExpectedClaimGeneration: node.ClaimGeneration, Owner: "rollback-" + id.Iteration, Token: "token-" + id.Iteration, IdempotencyKey: "claim-" + id.Iteration, Now: at, LeaseUntil: at.Add(time.Hour)})
	if err != nil || claim.Lease == nil {
		t.Fatalf("handler claim = %#v, %v", claim, err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	claimed, _ := state.LoadNodeInvocation(t.Context(), id)
	started, err := state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "undo", Version: "v1"}, Inputs: claimed.Inputs, At: at})
	if err != nil {
		t.Fatal(err)
	}
	var failure *workflowruntime.Failure
	if status != workflowruntime.NodeSucceeded {
		failure = &workflowruntime.Failure{Code: "rollback_failed", Message: "rollback failed"}
	}
	if _, err := state.FinishNodeAttempt(t.Context(), workflowruntime.FinishNodeAttemptRequest{InvocationID: id, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: status, NextNodeStatus: status, Failure: failure, At: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
}
