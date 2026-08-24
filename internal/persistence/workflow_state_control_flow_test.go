package persistence

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestWorkflowSQLiteControlFlowAtomicReplayFencingAndCorruption(t *testing.T) {
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "control.db"))
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "control-sql", base)
	running, transitionErr := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base})
	if transitionErr != nil {
		t.Fatal(transitionErr)
	}
	run = running.Snapshot
	source := createWorkflowTestNode(t, state, run.ID, "source", base)
	target := createWorkflowTestNode(t, state, run.ID, "target", base)
	cleanup := createWorkflowTestNode(t, state, run.ID, "cleanup", base)
	timeoutSource := createWorkflowTestNode(t, state, run.ID, "timeout-source", base)
	failure := workflowruntime.Failure{Code: "source_failed", Message: "source failed", Details: map[string]string{"safe": "detail"}}
	failed := finishWorkflowControlNode(t, state, source, workflowruntime.NodeFailed, failure, base)
	timeoutFailure := workflowruntime.Failure{Code: "timed_out", Message: "timed out", Details: map[string]string{"timeout_kind": string(workflowruntime.TimeoutExecution)}}
	timedOut := finishWorkflowControlNode(t, state, timeoutSource, workflowruntime.NodeTimedOut, timeoutFailure, base)
	readyTarget, targetErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: target.ID, ExpectedGeneration: target.Generation, To: workflowruntime.NodeReady, At: base.Add(time.Second)})
	if targetErr != nil {
		t.Fatal(targetErr)
	}
	rule := 0
	decision := workflowruntime.ControlDecisionSnapshot{
		ID: workflowruntime.ControlDecisionID{Source: source.ID, Kind: workflowruntime.ControlCatch}, Outcome: workflowruntime.ControlSelected,
		RuleIndex: &rule, Targets: []workflowruntime.NodeInvocationID{target.ID}, BindAs: "source_error",
	}
	otherOrigin := workflowruntime.NodeInvocationID{RunID: "other-run", NodeID: "source"}
	otherAttempt := workflowruntime.AttemptID{Invocation: otherOrigin, Number: 1}
	otherError, otherErrorErr := workflowruntime.NewFailureValue(otherOrigin, &otherAttempt, workflowruntime.NodeFailed, "", failure)
	if otherErrorErr != nil {
		t.Fatal(otherErrorErr)
	}
	valuesBefore := workflowRowCount(t, store, "workflow_value_sets")
	timeoutAttempt := workflowruntime.AttemptID{Invocation: timeoutSource.ID, Number: 1}
	forgedTimeout, timeoutErr := workflowruntime.NewFailureValue(timeoutSource.ID, &timeoutAttempt, workflowruntime.NodeTimedOut, workflowruntime.TimeoutExecution, timeoutFailure)
	if timeoutErr != nil {
		t.Fatal(timeoutErr)
	}
	forgedTimeout.Inline.(map[string]any)["timeout_kind"] = string(workflowruntime.TimeoutHeartbeat)
	if _, err := state.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{
		Decision:    workflowruntime.ControlDecisionSnapshot{ID: workflowruntime.ControlDecisionID{Source: timeoutSource.ID, Kind: workflowruntime.ControlCatch}, Outcome: workflowruntime.ControlSelected, RuleIndex: &rule, Targets: []workflowruntime.NodeInvocationID{target.ID}},
		ErrorValues: values.ValueSet{"error": forgedTimeout}, ExpectedSourceGeneration: timedOut.Generation, At: base.Add(4 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("forged timeout metadata = %v", err)
	}
	if got := workflowRowCount(t, store, "workflow_value_sets"); got != valuesBefore {
		t.Fatalf("forged timeout left %d value rows, want %d", got, valuesBefore)
	}
	if _, err := state.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: decision, ErrorValues: values.ValueSet{"error": otherError}, ExpectedSourceGeneration: failed.Generation, At: base.Add(4 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("cross-run catch error = %v", err)
	}
	if got := workflowRowCount(t, store, "workflow_value_sets"); got != valuesBefore {
		t.Fatalf("rejected catch left %d value rows, want %d", got, valuesBefore)
	}
	sameRunOrigin := workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: target.ID.NodeID}
	sameRunAttempt := workflowruntime.AttemptID{Invocation: sameRunOrigin, Number: 1}
	sameRunError, _ := workflowruntime.NewFailureValue(sameRunOrigin, &sameRunAttempt, workflowruntime.NodeFailed, "", failure)
	if _, err := state.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: decision, ErrorValues: values.ValueSet{"error": sameRunError}, ExpectedSourceGeneration: failed.Generation, At: base.Add(4 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("same-run cross-node catch error = %v", err)
	}
	forgedFailure := failure
	forgedFailure.Message = "forged persisted message"
	sourceAttempt := workflowruntime.AttemptID{Invocation: source.ID, Number: 1}
	forgedError, _ := workflowruntime.NewFailureValue(source.ID, &sourceAttempt, workflowruntime.NodeFailed, "", forgedFailure)
	if _, err := state.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: decision, ErrorValues: values.ValueSet{"error": forgedError}, ExpectedSourceGeneration: failed.Generation, At: base.Add(4 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("forged catch error = %v", err)
	}
	extraError, _ := workflowruntime.NewFailureValue(source.ID, &sourceAttempt, workflowruntime.NodeFailed, "", failure)
	extraError.Inline.(map[string]any)["unexpected"] = "not part of the durable envelope"
	if _, err := state.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: decision, ErrorValues: values.ValueSet{"error": extraError}, ExpectedSourceGeneration: failed.Generation, At: base.Add(4 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("extra catch error field = %v", err)
	}
	typedError, typedErrorErr := workflowruntime.NewFailureValue(source.ID, &sourceAttempt, workflowruntime.NodeFailed, "", failure)
	if typedErrorErr != nil {
		t.Fatal(typedErrorErr)
	}
	if _, err := store.DB().Exec(`UPDATE workflow_attempts SET status = 'timed_out' WHERE run_id = ? AND node_id = ? AND iteration = ? AND attempt_number = ?`, sourceAttempt.Invocation.RunID, sourceAttempt.Invocation.NodeID, sourceAttempt.Invocation.Iteration, sourceAttempt.Number); err != nil {
		t.Fatal(err)
	}
	if _, err := state.RecordControlDecision(ctx, workflowruntime.RecordControlDecisionRequest{Decision: decision, ErrorValues: values.ValueSet{"error": typedError}, ExpectedSourceGeneration: failed.Generation, At: base.Add(4 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("catch attempt status mismatch = %v", err)
	}
	if _, err := store.DB().Exec(`UPDATE workflow_attempts SET status = 'failed' WHERE run_id = ? AND node_id = ? AND iteration = ? AND attempt_number = ?`, sourceAttempt.Invocation.RunID, sourceAttempt.Invocation.NodeID, sourceAttempt.Invocation.Iteration, sourceAttempt.Number); err != nil {
		t.Fatal(err)
	}
	if got := workflowRowCount(t, store, "workflow_value_sets"); got != valuesBefore {
		t.Fatalf("forged catch errors left %d value rows, want %d", got, valuesBefore)
	}

	request := workflowruntime.RecordControlDecisionRequest{Decision: decision, ErrorValues: values.ValueSet{"error": typedError}, ExpectedSourceGeneration: failed.Generation, At: base.Add(4 * time.Second)}
	created, createErr := state.RecordControlDecision(ctx, request)
	if createErr != nil || created.Outcome != workflowruntime.IdempotencyApplied || created.Decision.Error == nil || created.Event == nil {
		t.Fatalf("RecordControlDecision = %#v, %v", created, createErr)
	}
	request.At = base.Add(time.Hour)
	replayed, replayErr := state.RecordControlDecision(ctx, request)
	if replayErr != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Event != nil || replayed.Decision.Error == nil || *replayed.Decision.Error != *created.Decision.Error {
		t.Fatalf("RecordControlDecision replay = %#v, %v", replayed, replayErr)
	}

	wrongRunError, runErrorErr := workflowruntime.NewRunFailureValue("other-run", workflowruntime.RunFailed, failure)
	if runErrorErr != nil {
		t.Fatal(runErrorErr)
	}
	runNow, _ := state.LoadRun(ctx, run.ID)
	scope := []workflowruntime.FinalizerScope{{Invocation: cleanup.ID, Scope: []workflowruntime.NodeInvocationID{source.ID, target.ID}, Order: 0}}
	if _, err := state.BeginTerminalIntent(ctx, workflowruntime.BeginTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: runNow.Generation, IntendedStatus: workflowruntime.RunFailed, Reason: &failure, ErrorValues: values.ValueSet{"error": wrongRunError}, IdempotencyKey: "terminal-control", Finalizers: scope, At: base.Add(5 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("cross-run terminal error = %v", err)
	}
	if got := workflowRowCount(t, store, "workflow_value_sets"); got != valuesBefore+1 {
		t.Fatalf("rejected terminal intent left value rows = %d, want %d", got, valuesBefore+1)
	}
	begin, beginErr := state.BeginTerminalIntent(ctx, workflowruntime.BeginTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: runNow.Generation, IntendedStatus: workflowruntime.RunFailed, Reason: &failure, ErrorValues: values.ValueSet{"error": typedError}, IdempotencyKey: "terminal-control", Finalizers: scope, At: base.Add(5 * time.Second)})
	if beginErr != nil || begin.Intent.Status != workflowruntime.TerminalIntentPending || !begin.Run.Status.Active() {
		t.Fatalf("BeginTerminalIntent = %#v, %v", begin, beginErr)
	}

	if _, err := state.SaveRun(ctx, workflowruntime.SaveRunRequest{Snapshot: begin.Run, ExpectedGeneration: begin.Run.Generation}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SaveRun during terminal intent = %v", err)
	}
	if _, err := state.SaveNodeInvocation(ctx, workflowruntime.SaveNodeInvocationRequest{Snapshot: readyTarget.Snapshot, ExpectedGeneration: readyTarget.Snapshot.Generation}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SaveNodeInvocation during terminal intent = %v", err)
	}
	if _, err := state.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "ordinary-output", RunID: run.ID, Invocation: &target.ID}, Values: workflowTestValues(t, "blocked")}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SaveValues during terminal intent = %v", err)
	}
	beforeTarget, _ := state.LoadNodeInvocation(ctx, target.ID)
	beforePublicEvents, _ := state.ListEvents(ctx, workflowruntime.EventQuery{RunID: run.ID})
	if _, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: target.ID, ExpectedGeneration: beforeTarget.Generation, To: workflowruntime.NodeCanceled, At: base.Add(6 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("public canceled transition during terminal intent = %v", err)
	}
	if _, err := state.AppendEvent(ctx, workflowruntime.AppendEventRequest{RunID: run.ID, Invocation: &target.ID, Type: "ordinary.event", OccurredAt: base.Add(6 * time.Second), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("public ordinary event during terminal intent = %v", err)
	}
	if _, err := state.AppendEvent(ctx, workflowruntime.AppendEventRequest{RunID: run.ID, Type: "ordinary.run_event", OccurredAt: base.Add(6 * time.Second), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("public anonymous event during terminal intent = %v", err)
	}
	afterTarget, _ := state.LoadNodeInvocation(ctx, target.ID)
	afterPublicEvents, _ := state.ListEvents(ctx, workflowruntime.EventQuery{RunID: run.ID})
	if afterTarget.Generation != beforeTarget.Generation || len(afterPublicEvents) != len(beforePublicEvents) {
		t.Fatalf("rejected public mutations changed target/events = %#v/%#v %d/%d", beforeTarget, afterTarget, len(beforePublicEvents), len(afterPublicEvents))
	}
	if _, err := state.AppendEvent(ctx, workflowruntime.AppendEventRequest{RunID: run.ID, Invocation: &cleanup.ID, Type: "cleanup.event", OccurredAt: base.Add(6 * time.Second), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		t.Fatalf("finalizer event during terminal intent = %v", err)
	}
	claim, claimErr := state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: target.ID, ExpectedClaimGeneration: 0, Owner: "late", Token: "late", IdempotencyKey: "late-control-claim", Now: base.Add(6 * time.Second), LeaseUntil: base.Add(time.Minute)})
	if claimErr != nil || claim.Acquired {
		t.Fatalf("ordinary claim after terminal intent = %#v, %v", claim, claimErr)
	}
	cleanupDone, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: cleanup.ID, ExpectedGeneration: cleanup.Generation, To: workflowruntime.NodeSkipped, At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatalf("finalizer transition = %v", err)
	}
	completed, err := state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation, At: base.Add(7 * time.Second)})
	if err != nil || completed.Run.Status != workflowruntime.RunFailed || completed.Intent.Status != workflowruntime.TerminalIntentCompleted || cleanupDone.Snapshot.Status != workflowruntime.NodeSkipped {
		t.Fatalf("CompleteTerminalIntent = %#v, %v", completed, err)
	}
	if _, err := state.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: "late-node"}, Status: workflowruntime.NodePending, CreatedAt: base.Add(8 * time.Second), UpdatedAt: base.Add(8 * time.Second)}}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("terminal parent node creation = %v", err)
	}
	if _, err := store.DB().Exec(`UPDATE workflow_control_decisions SET outcome = 'unmatched' WHERE run_id = ?`, run.ID); err == nil {
		t.Fatal("immutable control decision update succeeded")
	}
	if _, err := store.DB().Exec(`DELETE FROM workflow_terminal_intents WHERE run_id = ?`, run.ID); err == nil {
		t.Fatal("durable terminal intent delete succeeded")
	}
	if _, err := store.DB().Exec(`UPDATE workflow_terminal_intents SET status = 'pending', generation = generation + 1, completed_at = NULL, snapshot_json = snapshot_json WHERE run_id = ?`, run.ID); err == nil {
		t.Fatal("completed terminal intent resurrection succeeded")
	}
	if loaded, err := state.LoadControlDecision(ctx, decision.ID); err != nil || loaded.Outcome != workflowruntime.ControlSelected {
		t.Fatalf("decision damaged by rejected mutation = %#v, %v", loaded, err)
	}
}

func TestWorkflowSQLiteTerminalIntentPublishesRequiredOutputsAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "terminal-outputs.db")
	store, state := openWorkflowStateTest(t, path)
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "terminal-outputs", base)
	running, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	cleanup := createWorkflowTestNode(t, state, run.ID, "cleanup", base.Add(time.Second))
	wrongOwner, err := state.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "fixture", RunID: run.ID}, Values: workflowTestValues(t, "wrong")})
	if err != nil {
		t.Fatal(err)
	}
	begin, err := state.BeginTerminalIntent(ctx, workflowruntime.BeginTerminalIntentRequest{
		RunID: run.ID, ExpectedRunGeneration: running.Snapshot.Generation, IntendedStatus: workflowruntime.RunSucceeded,
		SuccessOutputsRequired: true, IdempotencyKey: "terminal-outputs", Finalizers: []workflowruntime.FinalizerScope{{Invocation: cleanup.ID, Order: 0}}, At: base.Add(2 * time.Second),
	})
	if err != nil || !begin.Intent.SuccessOutputsRequired {
		t.Fatalf("BeginTerminalIntent = %#v, %v", begin, err)
	}
	cleanupDone, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: cleanup.ID, ExpectedGeneration: cleanup.Generation, To: workflowruntime.NodeSkipped, At: base.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, completeErr := state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation, At: base.Add(4 * time.Second)}); !errors.Is(completeErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("missing terminal outputs = %v", completeErr)
	}
	if _, completeErr := state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation, Outputs: &wrongOwner, At: base.Add(4 * time.Second)}); !errors.Is(completeErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("wrong terminal output owner = %v", completeErr)
	}
	outputRef, err := state.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "run-outputs", RunID: run.ID}, Values: workflowTestValues(t, "published")})
	if err != nil {
		t.Fatalf("save terminal outputs while intent pending: %v", err)
	}
	otherRunRef, err := state.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "run-outputs", RunID: "other-run"}, Values: workflowTestValues(t, "other")})
	if err != nil {
		t.Fatal(err)
	}
	if _, completeErr := state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation, Outputs: &otherRunRef, At: base.Add(4 * time.Second)}); !errors.Is(completeErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("other-run terminal outputs = %v", completeErr)
	}
	tamperedRef := outputRef
	tamperedRef.Digest = values.SHA256Digest([]byte("tampered"))
	if _, completeErr := state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation, Outputs: &tamperedRef, At: base.Add(4 * time.Second)}); !errors.Is(completeErr, workflowruntime.ErrCASMismatch) {
		t.Fatalf("tampered terminal output digest = %v", completeErr)
	}
	completed, err := state.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{
		RunID: run.ID, ExpectedRunGeneration: begin.Run.Generation, ExpectedIntentGeneration: begin.Intent.Generation,
		Outputs: &outputRef, At: base.Add(4 * time.Second),
	})
	if err != nil || completed.Run.Status != workflowruntime.RunSucceeded || completed.Run.Outputs == nil || *completed.Run.Outputs != outputRef || completed.Event.Values == nil || *completed.Event.Values != outputRef || cleanupDone.Snapshot.Status != workflowruntime.NodeSkipped {
		t.Fatalf("CompleteTerminalIntent = %#v, %v", completed, err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, err := NewWorkflowStateStore(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	loadedRun, err := reopened.LoadRun(ctx, run.ID)
	if err != nil || loadedRun.Outputs == nil || *loadedRun.Outputs != outputRef || loadedRun.Status != workflowruntime.RunSucceeded {
		t.Fatalf("reopened successful outputs = %#v, %v", loadedRun, err)
	}
	loadedIntent, err := reopened.LoadTerminalIntent(ctx, run.ID)
	if err != nil || loadedIntent.Status != workflowruntime.TerminalIntentCompleted || !loadedIntent.SuccessOutputsRequired {
		t.Fatalf("reopened terminal intent = %#v, %v", loadedIntent, err)
	}
	loadedValues, err := reopened.LoadValues(ctx, outputRef)
	if err != nil || loadedValues["message"].Inline != "published" {
		t.Fatalf("reopened outputs = %#v, %v", loadedValues, err)
	}
}

func TestWorkflowSQLiteReconcileFinalizeOutputsReopenReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "finalize-reopen.db")
	store, state := openWorkflowStateTest(t, path)
	ctx, base := context.Background(), workflowTestTime()
	planRef := workflowTestPlan("finalize-reopen")
	plan := &workflowcompile.ExecutionPlan{
		SchemaVersion: planRef.SchemaVersion, ID: planRef.ID, Digest: planRef.Digest,
		Provenance: graph.Provenance{Authority: "project", Origin: "fixture", Locator: "fixture.workflow.yaml", Digest: values.SHA256Digest([]byte("source"))},
		Graph: graph.Graph{ID: planRef.ID, Version: planRef.Version, Digest: values.SHA256Digest([]byte("graph")), Nodes: []graph.Node{
			{ID: "work", Kind: "test"}, {ID: "cleanup", Kind: "test", Finally: &graph.FinallySpec{}},
		}, Outputs: []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}, Value: &graph.Binding{Kind: graph.BindingLiteral, Literal: "published"}}}},
	}
	boundResult, err := workflowruntime.BindRun(ctx, state, workflowruntime.BindRunRequest{ID: "finalize-reopen", Plan: plan, CreatedAt: base})
	if err != nil || boundResult.Run == nil || len(boundResult.Diagnostics) != 0 {
		t.Fatalf("BindRun = %#v, %v", boundResult, err)
	}
	bound := *boundResult.Run
	pending, _, err := workflowruntime.StartBoundRun(ctx, state, bound, "start-finalize-reopen")
	if err != nil {
		t.Fatal(err)
	}
	running, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: pending.ID, ExpectedGeneration: pending.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	work := createWorkflowTestNode(t, state, running.Snapshot.ID, "work", base.Add(time.Second))
	cleanup := createWorkflowTestNode(t, state, running.Snapshot.ID, "cleanup", base.Add(time.Second))
	if _, transitionErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: work.ID, ExpectedGeneration: work.Generation, To: workflowruntime.NodeSkipped, At: base.Add(2 * time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(state, state, nil)
	if _, intent, reconcileErr := coordinator.ReconcileRunCompletion(ctx, plan.Graph, running.Snapshot.ID, "complete-finalize-reopen", base.Add(3*time.Second)); !errors.Is(reconcileErr, workflowruntime.ErrControlFlowPending) || intent == nil || !intent.SuccessOutputsRequired {
		t.Fatalf("begin output intent = %#v, %v", intent, reconcileErr)
	}
	if _, transitionErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: cleanup.ID, ExpectedGeneration: cleanup.Generation, To: workflowruntime.NodeSkipped, At: base.Add(4 * time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	ready, intent, err := coordinator.ReconcileRunCompletion(ctx, plan.Graph, running.Snapshot.ID, "complete-finalize-reopen", base.Add(5*time.Second))
	if !errors.Is(err, workflowruntime.ErrRunOutputsPending) || intent == nil || ready.Outputs != nil || ready.Status == workflowruntime.RunSucceeded {
		t.Fatalf("output boundary = run=%#v intent=%#v err=%v", ready, intent, err)
	}
	expression, err := workflowruntime.BuildExpressionContext(ctx, state, state, plan.Graph, ready.ID)
	if err != nil {
		t.Fatal(err)
	}
	finalized, err := workflowruntime.FinalizeRunOutputs(ctx, state, workflowruntime.FinalizeRunRequest{BoundRun: bound, Run: ready, Plan: plan, Context: expression, Control: state, At: base.Add(6 * time.Second)})
	if err != nil || len(finalized.Diagnostics) != 0 || finalized.Run.Status != workflowruntime.RunSucceeded || finalized.Run.Outputs == nil {
		t.Fatalf("FinalizeRunOutputs = %#v, %v", finalized, err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, err := NewWorkflowStateStore(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	loadedRun, err := reopened.LoadRun(ctx, ready.ID)
	if err != nil || loadedRun.Outputs == nil || *loadedRun.Outputs != finalized.OutputsRef {
		t.Fatalf("reopened run outputs = %#v, %v", loadedRun, err)
	}
	reopenedExpression, err := workflowruntime.BuildExpressionContext(ctx, reopened, reopened, plan.Graph, ready.ID)
	if err != nil {
		t.Fatal(err)
	}
	hookCalls := 0
	replayed, err := workflowruntime.FinalizeRunOutputs(ctx, reopened, workflowruntime.FinalizeRunRequest{
		BoundRun: bound, Run: loadedRun, Plan: plan, Context: reopenedExpression, Control: reopened,
		RetentionHook: workflowruntime.RetentionHookFuncs{Before: func(context.Context, workflowruntime.RetentionPlan) error { hookCalls++; return nil }, After: func(context.Context, workflowruntime.RetentionRecord) error { hookCalls++; return nil }},
		At:            base.Add(7 * time.Second),
	})
	if err != nil || replayed.Outcome != workflowruntime.OutputFinalizationReplayed || replayed.OutputsRef != finalized.OutputsRef || hookCalls != 0 {
		t.Fatalf("reopened finalization replay = %#v hooks=%d err=%v", replayed, hookCalls, err)
	}
}

func TestWorkflowSQLiteControlFlowRollbackAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control-reopen.db")
	store, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	state, _ := NewWorkflowStateStore(store)
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "control-rollback", base)
	work := createWorkflowTestNode(t, state, run.ID, "work", base)
	cleanup := createWorkflowTestNode(t, state, run.ID, "cleanup", base)
	failure := workflowruntime.Failure{Code: "operator_cancel", Message: "operator canceled"}
	typed, _ := workflowruntime.NewRunFailureValue(run.ID, workflowruntime.RunCanceled, failure)
	beforeValues, beforeEvents := workflowRowCount(t, store, "workflow_value_sets"), workflowRowCount(t, store, "workflow_events")
	_, pendingErr := state.RequestRunCancellationWithFinalizers(ctx, workflowruntime.RequestRunCancellationWithFinalizersRequest{
		Cancellation: workflowruntime.RequestRunCancellationRequest{RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "pending-cancel", Reason: failure, At: base.Add(time.Second)},
		Finalizers:   []workflowruntime.FinalizerScope{{Invocation: cleanup.ID, Scope: []workflowruntime.NodeInvocationID{work.ID}, Order: 0}}, ErrorValues: values.ValueSet{"error": typed},
	})
	var transitionErr *workflowruntime.TransitionError
	if !errors.As(pendingErr, &transitionErr) {
		t.Fatalf("pending cancellation with finalizer = %v, want transition error", pendingErr)
	}
	if got := workflowRowCount(t, store, "workflow_value_sets"); got != beforeValues {
		t.Fatalf("unreachable pending intent values = %d, want %d", got, beforeValues)
	}
	if got := workflowRowCount(t, store, "workflow_events"); got != beforeEvents {
		t.Fatalf("unreachable pending intent events = %d, want %d", got, beforeEvents)
	}
	if _, err := state.LoadTerminalIntent(ctx, run.ID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("unreachable pending intent persisted = %v", err)
	}
	running, runTransitionErr := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if runTransitionErr != nil {
		t.Fatal(runTransitionErr)
	}
	run = running.Snapshot
	beforeValues, beforeEvents = workflowRowCount(t, store, "workflow_value_sets"), workflowRowCount(t, store, "workflow_events")
	if _, err := store.DB().Exec(`CREATE TRIGGER fail_control_cancel AFTER INSERT ON workflow_events WHEN NEW.event_type = 'node.status_changed' BEGIN SELECT RAISE(ABORT, 'injected cancellation failure'); END`); err != nil {
		t.Fatal(err)
	}
	_, cancelErr := state.RequestRunCancellationWithFinalizers(ctx, workflowruntime.RequestRunCancellationWithFinalizersRequest{
		Cancellation: workflowruntime.RequestRunCancellationRequest{RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "rollback-cancel", Reason: failure, At: base.Add(2 * time.Second)},
		Finalizers:   []workflowruntime.FinalizerScope{{Invocation: cleanup.ID, Scope: []workflowruntime.NodeInvocationID{work.ID}, Order: 0}}, ErrorValues: values.ValueSet{"error": typed},
	})
	if cancelErr == nil {
		t.Fatal("injected cancellation failure succeeded")
	}
	if got := workflowRowCount(t, store, "workflow_value_sets"); got != beforeValues {
		t.Fatalf("rollback values = %d, want %d", got, beforeValues)
	}
	if got := workflowRowCount(t, store, "workflow_events"); got != beforeEvents {
		t.Fatalf("rollback events = %d, want %d", got, beforeEvents)
	}
	if _, err := state.LoadTerminalIntent(ctx, run.ID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("rolled back terminal intent = %v", err)
	}
	currentRun, _ := state.LoadRun(ctx, run.ID)
	currentWork, _ := state.LoadNodeInvocation(ctx, work.ID)
	if currentRun.Generation != run.Generation || currentWork.Status != workflowruntime.NodePending || currentWork.Generation != work.Generation {
		t.Fatalf("rollback state = run %#v work %#v", currentRun, currentWork)
	}
	if _, err := store.DB().Exec(`DROP TRIGGER fail_control_cancel`); err != nil {
		t.Fatal(err)
	}
	if _, err := state.RequestRunCancellationWithFinalizers(ctx, workflowruntime.RequestRunCancellationWithFinalizersRequest{
		Cancellation: workflowruntime.RequestRunCancellationRequest{RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "rollback-cancel", Reason: failure, At: base.Add(2 * time.Second)},
		Finalizers:   []workflowruntime.FinalizerScope{{Invocation: cleanup.ID, Scope: []workflowruntime.NodeInvocationID{work.ID}, Order: 0}}, ErrorValues: values.ValueSet{"error": typed},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedState, _ := NewWorkflowStateStore(reopened)
	intent, err := reopenedState.LoadTerminalIntent(ctx, run.ID)
	if err != nil || intent.Status != workflowruntime.TerminalIntentPending || intent.Error == nil {
		t.Fatalf("reopened terminal intent = %#v, %v", intent, err)
	}
	loadedError, err := reopenedState.LoadValues(ctx, *intent.Error)
	if err != nil || workflowruntime.ValidateRunControlErrorValues(loadedError, run.ID, workflowruntime.RunCanceled) != nil {
		t.Fatalf("reopened terminal error = %#v, %v", loadedError, err)
	}
}

func TestWorkflowSQLiteControlDecisionCoordinatorContentionAcrossHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control-contention.db")
	firstStore, first := openWorkflowStateTest(t, path)
	secondStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, _ := NewWorkflowStateStore(secondStore)
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, first, "control-contention", base)
	source := createWorkflowTestNode(t, first, run.ID, "source", base)
	target := createWorkflowTestNode(t, first, run.ID, "target", base)
	failure := workflowruntime.Failure{Code: "boom", Message: "boom"}
	finishWorkflowControlNode(t, first, source, workflowruntime.NodeFailed, failure, base)
	node := graph.Node{ID: "source", Catch: []graph.CatchRule{{Targets: []string{"target"}, BindAs: "source_error"}}}
	results := make(chan workflowruntime.RecordControlDecisionResult, 24)
	errs := make(chan error, 24)
	var wait sync.WaitGroup
	for index := range 24 {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			store := first
			if index%2 == 1 {
				store = second
			}
			result, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).DecideCatch(ctx, workflowruntime.DecideCatchRequest{Source: source.ID, Node: node, Failure: &failure, At: base.Add(4 * time.Second)})
			results <- result
			errs <- err
		}(index)
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("contended DecideCatch = %v", err)
		}
	}
	applied := 0
	var ref *values.ValueSetRef
	for result := range results {
		if result.Outcome == workflowruntime.IdempotencyApplied {
			applied++
		}
		if result.Decision.Error == nil {
			t.Fatal("contended decision omitted error")
		}
		if ref == nil {
			copyRef := *result.Decision.Error
			ref = &copyRef
		} else if *ref != *result.Decision.Error {
			t.Fatalf("contended refs differ: %#v != %#v", ref, result.Decision.Error)
		}
	}
	if applied != 1 || workflowRowCount(t, firstStore, "workflow_control_decisions") != 1 || workflowRowCount(t, firstStore, "workflow_value_sets") != 1 {
		t.Fatalf("contention rows applied=%d decisions=%d values=%d", applied, workflowRowCount(t, firstStore, "workflow_control_decisions"), workflowRowCount(t, firstStore, "workflow_value_sets"))
	}
	loaded, err := second.LoadControlDecision(ctx, workflowruntime.ControlDecisionID{Source: source.ID, Kind: workflowruntime.ControlCatch})
	if err != nil || loaded.Error == nil || *loaded.Error != *ref || len(loaded.Targets) != 1 || loaded.Targets[0] != target.ID {
		t.Fatalf("cross-handle decision = %#v, %v", loaded, err)
	}
}

func TestWorkflowSQLiteCancellationTreeAtomicReplayRollbackAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cancel-tree.db")
	firstStore, first := openWorkflowStateTest(t, path)
	secondStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, stateErr := NewWorkflowStateStore(secondStore)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	ctx, base := context.Background(), workflowTestTime()
	root := createWorkflowTestRun(t, first, "tree-root", base)
	child := createWorkflowTestRun(t, first, "tree-child", base)
	for _, id := range []workflowruntime.RunID{root.ID, child.ID} {
		run, _ := first.LoadRun(ctx, id)
		transitioned, transitionErr := first.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: id, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
		if transitionErr != nil {
			t.Fatal(transitionErr)
		}
		if id == root.ID {
			root = transitioned.Snapshot
		} else {
			child = transitioned.Snapshot
		}
	}
	call := createWorkflowTestNode(t, first, root.ID, "call-child", base)
	rootCleanup := createWorkflowTestNode(t, first, root.ID, "root-cleanup", base)
	work := createWorkflowTestNode(t, first, child.ID, "work", base)
	cleanup := createWorkflowTestNode(t, first, child.ID, "cleanup", base)
	if err := first.RecordChildRun(ctx, workflowruntime.ChildRunLink{ParentRunID: root.ID, Invocation: call.ID, ChildRunID: child.ID, Policy: graph.ParentCloseCancel, CreatedAt: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	rootGraph := graph.Graph{ID: root.Plan.ID, Version: root.Plan.Version, Nodes: []graph.Node{{ID: call.ID.NodeID}, {ID: rootCleanup.ID.NodeID, Finally: &graph.FinallySpec{}}}}
	childGraph := graph.Graph{ID: child.Plan.ID, Version: child.Plan.Version, Nodes: []graph.Node{{ID: work.ID.NodeID}, {ID: cleanup.ID.NodeID, Finally: &graph.FinallySpec{}}}}
	request := workflowruntime.RequestRunCancellationRequest{RunID: root.ID, ExpectedGeneration: root.Generation, IdempotencyKey: "tree-cancel", Reason: workflowruntime.Failure{Code: "operator_cancel", Message: "operator canceled"}, At: base.Add(2 * time.Second)}
	coordinator := workflowruntime.NewControlFlowCoordinator(first, first, nil)
	if _, err := coordinator.RequestRunCancellationTree(ctx, rootGraph, request, nil); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("omitted descendant = %v", err)
	}
	forged := childGraph
	forged.ID = "forged-plan"
	if _, err := coordinator.RequestRunCancellationTree(ctx, rootGraph, request, []workflowruntime.CancellationDescendantGraph{{Run: child, Graph: forged}}); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
		t.Fatalf("forged child graph = %v", err)
	}
	extra := createWorkflowTestRun(t, first, "tree-extra", base)
	extraRun, _ := first.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: extra.ID, ExpectedGeneration: extra.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	extraGraph := graph.Graph{ID: extra.Plan.ID, Version: extra.Plan.Version}
	beforeValues, beforeEvents := workflowRowCount(t, firstStore, "workflow_value_sets"), workflowRowCount(t, firstStore, "workflow_events")
	if _, err := coordinator.RequestRunCancellationTree(ctx, rootGraph, request, []workflowruntime.CancellationDescendantGraph{{Run: child, Graph: childGraph}, {Run: extraRun.Snapshot, Graph: extraGraph}}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("extra descendant = %v", err)
	}
	rootUnchanged, _ := first.LoadRun(ctx, root.ID)
	childUnchanged, _ := first.LoadRun(ctx, child.ID)
	if rootUnchanged.Generation != root.Generation || childUnchanged.Generation != child.Generation || workflowRowCount(t, firstStore, "workflow_value_sets") != beforeValues || workflowRowCount(t, firstStore, "workflow_events") != beforeEvents {
		t.Fatalf("rejected tree mutated durable state: root=%#v child=%#v", rootUnchanged, childUnchanged)
	}

	results := make(chan workflowruntime.RequestRunCancellationWithFinalizersResult, 12)
	errs := make(chan error, 12)
	var wg sync.WaitGroup
	for index := range 12 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			state := first
			if index%2 == 1 {
				state = second
			}
			result, callErr := workflowruntime.NewControlFlowCoordinator(state, state, nil).RequestRunCancellationTree(ctx, rootGraph, request, []workflowruntime.CancellationDescendantGraph{{Run: child, Graph: childGraph}})
			results <- result
			errs <- callErr
		}(index)
	}
	wg.Wait()
	close(results)
	close(errs)
	for callErr := range errs {
		if callErr != nil {
			t.Fatalf("contended tree cancellation = %v", callErr)
		}
	}
	applied := 0
	for result := range results {
		if result.Cancellation.Outcome == workflowruntime.IdempotencyApplied {
			applied++
		}
		if !result.Cancellation.Run.Status.Active() || result.Intent.RunID != root.ID || len(result.TerminalIntents) != 2 || result.TerminalIntents[0].RunID != root.ID || result.TerminalIntents[1].RunID != child.ID {
			t.Fatalf("tree result = %#v", result)
		}
	}
	if applied != 1 || workflowRowCount(t, firstStore, "workflow_control_cancellation_trees") != 1 || workflowRowCount(t, firstStore, "workflow_terminal_intents") != 2 || workflowRowCount(t, firstStore, "workflow_value_sets") != beforeValues+2 {
		t.Fatalf("tree rows applied=%d trees=%d intents=%d values=%d", applied, workflowRowCount(t, firstStore, "workflow_control_cancellation_trees"), workflowRowCount(t, firstStore, "workflow_terminal_intents"), workflowRowCount(t, firstStore, "workflow_value_sets"))
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, _ := NewWorkflowStateStore(reopenedStore)
	intent, err := reopened.LoadTerminalIntent(ctx, child.ID)
	if err != nil || intent.Status != workflowruntime.TerminalIntentPending || len(intent.Finalizers) != 1 || intent.Finalizers[0].Invocation != cleanup.ID {
		t.Fatalf("reopened child intent = %#v, %v", intent, err)
	}
	rootAfter, _ := reopened.LoadRun(ctx, root.ID)
	childAfter, _ := reopened.LoadRun(ctx, child.ID)
	workAfter, _ := reopened.LoadNodeInvocation(ctx, work.ID)
	if !rootAfter.Status.Active() || !childAfter.Status.Active() || workAfter.Status != workflowruntime.NodeCanceled {
		t.Fatalf("reopened tree = root %#v child %#v work %#v", rootAfter, childAfter, workAfter)
	}
	replayRequest := request
	replayRequest.ExpectedGeneration = rootAfter.Generation
	rowsBefore := map[string]int{
		"events": workflowRowCount(t, reopenedStore, "workflow_events"), "values": workflowRowCount(t, reopenedStore, "workflow_value_sets"),
		"trees": workflowRowCount(t, reopenedStore, "workflow_control_cancellation_trees"), "intents": workflowRowCount(t, reopenedStore, "workflow_terminal_intents"),
	}
	replayed, err := workflowruntime.NewControlFlowCoordinator(reopened, reopened, nil).RequestRunCancellationTree(ctx, rootGraph, replayRequest, []workflowruntime.CancellationDescendantGraph{{Run: childAfter, Graph: childGraph}})
	if err != nil || replayed.Cancellation.Outcome != workflowruntime.IdempotencyReplayed || replayed.Cancellation.Run.Generation != rootAfter.Generation || len(replayed.TerminalIntents) != 2 {
		t.Fatalf("reopened current-generation replay = %#v, %v", replayed, err)
	}
	for table, before := range rowsBefore {
		name := map[string]string{"events": "workflow_events", "values": "workflow_value_sets", "trees": "workflow_control_cancellation_trees", "intents": "workflow_terminal_intents"}[table]
		if got := workflowRowCount(t, reopenedStore, name); got != before {
			t.Fatalf("reopened replay changed %s rows: %d != %d", table, got, before)
		}
	}
}

func workflowRowCount(t *testing.T, store *Store, table string) int {
	t.Helper()
	var count int
	if err := store.DB().QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s", table)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func finishWorkflowControlNode(t *testing.T, state *WorkflowStateStore, node workflowruntime.NodeInvocationSnapshot, status workflowruntime.NodeStatus, failure workflowruntime.Failure, base time.Time) workflowruntime.NodeInvocationSnapshot {
	t.Helper()
	ctx := context.Background()
	ready, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "control-worker", Token: "control-token-" + node.ID.NodeID, IdempotencyKey: "control-claim-" + node.ID.NodeID, Now: base.Add(time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("ClaimNode = %#v, %v", claim, err)
	}
	claimed, err := state.LoadNodeInvocation(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: workflowTestExecutor(), At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	finished, err := state.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: node.ID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: status, NextNodeStatus: status, Failure: &failure, At: base.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	return finished.Node
}
