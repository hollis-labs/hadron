package persistence

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestWorkflowSQLiteRetryReopenActivationHistoryAndCAS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retry.db")
	store, state := openWorkflowStateTest(t, path)
	base := workflowTestTime()
	started, claim := prepareWorkflowSQLiteRunning(t, state, "retry", base)
	failure := workflowruntime.Failure{Code: "rate_limited", Message: "retry later", Retryable: true}
	scheduled, err := state.ScheduleNodeRetry(context.Background(), workflowruntime.ScheduleNodeRetryRequest{
		Activation:             workflowruntime.RetryActivationSnapshot{ID: "retry-sqlite", Attempt: started.Attempt.ID, Failure: failure, FireAt: base.Add(10 * time.Second), Status: workflowruntime.RetryScheduled},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: claim, AttemptStatus: workflowruntime.NodeFailed, At: base.Add(3 * time.Second),
	})
	if err != nil || scheduled.Node.Status != workflowruntime.NodeWaiting || scheduled.Node.Lease != nil || scheduled.Attempt.Status != workflowruntime.NodeFailed {
		t.Fatalf("ScheduleNodeRetry = %#v, %v", scheduled, err)
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
	recovered, err := reopened.RecoverRetryActivations(context.Background(), workflowruntime.RetryActivationQuery{DueBefore: base.Add(11 * time.Second)})
	if err != nil || len(recovered) != 1 || recovered[0].ID != scheduled.Activation.ID {
		t.Fatalf("recovered retry = %#v, %v", recovered, err)
	}
	secondStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, err := NewWorkflowStateStore(secondStore)
	if err != nil {
		t.Fatal(err)
	}
	requests := []workflowruntime.ActivateNodeRetryRequest{
		{ActivationID: scheduled.Activation.ID, ExpectedActivationGeneration: 1, ExpectedNodeGeneration: scheduled.Node.Generation, IdempotencyKey: "activate-a", Now: base.Add(10 * time.Second)},
		{ActivationID: scheduled.Activation.ID, ExpectedActivationGeneration: 1, ExpectedNodeGeneration: scheduled.Node.Generation, IdempotencyKey: "activate-b", Now: base.Add(10 * time.Second)},
	}
	stores := []*WorkflowStateStore{reopened, second}
	errorsByCall := make(chan error, 2)
	var wg sync.WaitGroup
	for index := range requests {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, activationErr := stores[index].ActivateNodeRetry(context.Background(), requests[index])
			errorsByCall <- activationErr
		}(index)
	}
	wg.Wait()
	close(errorsByCall)
	successes, stale := 0, 0
	for err := range errorsByCall {
		if err == nil {
			successes++
		} else if errors.Is(err, workflowruntime.ErrCASMismatch) {
			stale++
		} else {
			t.Fatalf("contending activation error = %v", err)
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("retry activation contention successes=%d stale=%d", successes, stale)
	}
	replays := 0
	for _, request := range requests {
		request.Now = request.Now.In(time.FixedZone("equivalent", 9*60*60))
		result, replayErr := reopened.ActivateNodeRetry(context.Background(), request)
		if replayErr == nil {
			if result.Outcome != workflowruntime.IdempotencyReplayed {
				t.Fatalf("equivalent-time activation outcome = %q", result.Outcome)
			}
			replays++
		}
	}
	if replays != 1 {
		t.Fatalf("equivalent-time activation replays=%d, want 1", replays)
	}
	node, err := reopened.LoadNodeInvocation(context.Background(), started.Node.ID)
	attempts, historyErr := reopened.ListAttempts(context.Background(), started.Node.ID)
	if err != nil || historyErr != nil || node.Status != workflowruntime.NodeReady || len(attempts) != 1 || attempts[0].Status != workflowruntime.NodeFailed {
		t.Fatalf("reopened retry state node=%#v attempts=%#v errors=%v/%v", node, attempts, err, historyErr)
	}
}

func TestWorkflowSQLiteFanOutCrossHandleClaimLimitAndCanceledRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fanout.db")
	store, first := openWorkflowStateTest(t, path)
	base := workflowTestTime()
	run := createWorkflowTestRun(t, first, "fanout-sqlite", base)
	parent := createWorkflowTestNode(t, first, run.ID, "bulk", base)
	coordinator := workflowruntime.FanOutCoordinator{Store: first}
	expanded, err := coordinator.Expand(context.Background(), workflowruntime.FanOutExpandCommand{
		Parent: parent.ID, ExpectedParentGeneration: parent.Generation,
		Spec: graph.ForEachSpec{Items: graph.Expression{Text: `[1, 2, 3, 4]`}, MaxConcurrency: 1, Tolerate: &graph.ToleratedFailurePolicy{Count: 1}},
		At:   base.Add(time.Second),
	})
	if err != nil || len(expanded.Children) != 4 {
		t.Fatalf("Expand = %#v, %v", expanded, err)
	}
	secondStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, _ := NewWorkflowStateStore(secondStore)
	stores := []*WorkflowStateStore{first, second}
	type claimOutcome struct {
		index int
		claim workflowruntime.ClaimResult
		err   error
	}
	results := make(chan claimOutcome, len(expanded.Children))
	var wg sync.WaitGroup
	for index, child := range expanded.Children {
		wg.Add(1)
		go func(index int, child workflowruntime.NodeInvocationSnapshot) {
			defer wg.Done()
			claim, claimErr := stores[index%2].ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{InvocationID: child.ID, ExpectedClaimGeneration: 0, Owner: "worker", Token: "token-" + child.ID.Iteration, IdempotencyKey: "claim-" + child.ID.Iteration, Now: base.Add(2 * time.Second), LeaseUntil: base.Add(time.Minute)})
			results <- claimOutcome{index: index, claim: claim, err: claimErr}
		}(index, child)
	}
	wg.Wait()
	close(results)
	acquired := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("ClaimNode(%d): %v", result.index, result.err)
		}
		if result.claim.Acquired {
			acquired++
		}
	}
	if acquired != 1 {
		t.Fatalf("fan-out cross-handle acquired=%d, want 1", acquired)
	}

	// Directly cancel one item with a durable attempt failure and verify the
	// exact typed item result survives a SQLite reopen.
	var winner workflowruntime.NodeInvocationSnapshot
	for _, child := range expanded.Children {
		loaded, _ := first.LoadNodeInvocation(context.Background(), child.ID)
		if loaded.Lease != nil {
			winner = loaded
			break
		}
	}
	claim := workflowruntime.ClaimProof{Owner: winner.Lease.Owner, Token: winner.Lease.Token, Generation: winner.Lease.Generation}
	started, err := first.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{InvocationID: winner.ID, ExpectedNodeGeneration: winner.Generation, Claim: claim, Executor: workflowTestExecutor(), Inputs: winner.Inputs, At: base.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	failure := workflowruntime.Failure{Code: "canceled_item", Message: "item canceled"}
	_, err = first.FinishNodeAttempt(context.Background(), workflowruntime.FinishNodeAttemptRequest{InvocationID: winner.ID, AttemptNumber: 1, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, AttemptStatus: workflowruntime.NodeCanceled, NextNodeStatus: workflowruntime.NodeCanceled, Failure: &failure, At: base.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, _ := NewWorkflowStateStore(reopenedStore)
	items, err := reopened.LoadFanOutItemResults(context.Background(), parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range items {
		if item.Invocation == winner.ID {
			found = item.Status == workflowruntime.NodeCanceled && item.Failure != nil && item.Failure.Code == failure.Code
		}
	}
	if !found {
		t.Fatalf("canceled item failure disappeared after reopen: %#v", items)
	}
}

func TestWorkflowSQLiteCancellationClosesWaitAndFencesLaterWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cancel.db")
	store, state := openWorkflowStateTest(t, path)
	base := workflowTestTime()
	waitFixture := prepareWorkflowSQLiteWait(t, state, "run-cancel", base, time.Hour)
	suspended, err := (workflowruntime.WaitCoordinator{Store: state}).Suspend(context.Background(), workflowruntime.SuspendCommand{Request: waitFixture.request, ResumeToken: waitFixture.token})
	if err != nil {
		t.Fatal(err)
	}
	run, err := state.LoadRun(context.Background(), suspended.Node.ID.RunID)
	if err != nil {
		t.Fatal(err)
	}
	reason := workflowruntime.Failure{Code: "user_canceled", Message: "run canceled by user"}
	request := workflowruntime.RequestRunCancellationRequest{RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "cancel-run", Reason: reason, At: base.Add(5 * time.Second)}
	result, err := state.RequestRunCancellation(context.Background(), request)
	if err != nil || result.Run.Status != workflowruntime.RunCanceled || len(result.Nodes) != 1 || result.Nodes[0].Status != workflowruntime.NodeCanceled {
		t.Fatalf("RequestRunCancellation = %#v, %v", result, err)
	}
	replayRequest := request
	replayRequest.At = request.At.In(time.FixedZone("equivalent", -6*60*60))
	replayed, err := state.RequestRunCancellation(context.Background(), replayRequest)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("cancellation replay = %#v, %v", replayed, err)
	}
	wait, err := state.LoadWait(context.Background(), suspended.Wait.Ref.ID)
	attempt, attemptErr := state.LoadAttempt(context.Background(), suspended.Attempt.ID)
	if err != nil || attemptErr != nil || wait.Status != workflowruntime.WaitCanceled || attempt.Status != workflowruntime.NodeCanceled {
		t.Fatalf("canceled wait/attempt = %#v / %#v, %v / %v", wait, attempt, err, attemptErr)
	}
	claim, err := state.ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{InvocationID: suspended.Node.ID, ExpectedClaimGeneration: result.Nodes[0].ClaimGeneration, Owner: "late", Token: "late", IdempotencyKey: "late-claim", Now: base.Add(6 * time.Second), LeaseUntil: base.Add(time.Hour)})
	if err != nil || claim.Acquired {
		t.Fatalf("claim after run cancel = %#v, %v", claim, err)
	}
	if _, resumeErr := (workflowruntime.WaitCoordinator{Store: state}).Resume(context.Background(), workflowruntime.ResumeCommand{WaitID: wait.Ref.ID, Correlation: wait.Correlation, Token: waitFixture.token, WakeSource: wait.WakeSource, Responder: workflowwaitResponder(), Payload: workflowTestValue(t, "late"), ReceivedAt: base.Add(7 * time.Second)}); resumeErr == nil {
		t.Fatal("late resume after run cancellation unexpectedly reopened wait")
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, _ := NewWorkflowStateStore(reopenedStore)
	reopenedRun, _ := reopened.LoadRun(context.Background(), run.ID)
	reopenedNode, _ := reopened.LoadNodeInvocation(context.Background(), suspended.Node.ID)
	if reopenedRun.Status != workflowruntime.RunCanceled || reopenedNode.Status != workflowruntime.NodeCanceled || reopenedNode.Lease != nil {
		t.Fatalf("reopened cancellation = %#v / %#v", reopenedRun, reopenedNode)
	}
}

func TestWorkflowSQLiteCancellationFencesLateAttemptCompletion(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "cancel-late-finish.db"))
	base := workflowTestTime()
	started, claim := prepareWorkflowSQLiteRunning(t, state, "cancel-late-finish", base)
	run, _ := state.LoadRun(context.Background(), started.Node.ID.RunID)
	_, err := state.RequestRunCancellation(context.Background(), workflowruntime.RequestRunCancellationRequest{
		RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "cancel-before-finish",
		Reason: workflowruntime.Failure{Code: "user_canceled", Message: "run canceled by user"}, At: base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeNode, _ := state.LoadNodeInvocation(context.Background(), started.Node.ID)
	beforeAttempt, _ := state.LoadAttempt(context.Background(), started.Attempt.ID)
	beforeEvents, _ := state.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
	_, finishErr := state.FinishNodeAttempt(context.Background(), workflowruntime.FinishNodeAttemptRequest{
		InvocationID: started.Node.ID, AttemptNumber: started.Attempt.ID.Number,
		ExpectedNodeGeneration: beforeNode.Generation, ExpectedAttemptGeneration: beforeAttempt.Generation,
		Claim: claim, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: base.Add(4 * time.Second),
	})
	if !errors.Is(finishErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("late FinishNodeAttempt() error = %v", finishErr)
	}
	afterNode, _ := state.LoadNodeInvocation(context.Background(), started.Node.ID)
	afterAttempt, _ := state.LoadAttempt(context.Background(), started.Attempt.ID)
	afterEvents, _ := state.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
	if afterNode.Generation != beforeNode.Generation || afterNode.Status != beforeNode.Status || afterAttempt.Generation != beforeAttempt.Generation || afterAttempt.Status != beforeAttempt.Status || len(afterEvents) != len(beforeEvents) {
		t.Fatalf("rejected late completion mutated state node=%#v/%#v attempt=%#v/%#v events=%d/%d", beforeNode, afterNode, beforeAttempt, afterAttempt, len(beforeEvents), len(afterEvents))
	}
}

func TestWorkflowSQLiteParentCancellationLeavesTerminalDirectChild(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "cancel-terminal-child.db"))
	base := workflowTestTime()
	parent := createWorkflowTestRun(t, state, "parent-terminal-child", base)
	child := createWorkflowTestRun(t, state, "already-terminal-child", base)
	call := createWorkflowTestNode(t, state, parent.ID, "call", base)
	childNode := createWorkflowTestNode(t, state, child.ID, "work", base)
	if err := state.RecordChildRun(context.Background(), workflowruntime.ChildRunLink{ParentRunID: parent.ID, Invocation: call.ID, ChildRunID: child.ID, Policy: graph.ParentCloseCancel, CreatedAt: base}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: childNode.ID, ExpectedGeneration: childNode.Generation, To: workflowruntime.NodeSkipped, Explanation: &workflowruntime.BlockedReason{Code: "already_done", Message: "child completed before parent cancellation"}, At: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	running, err := state.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: child.ID, ExpectedGeneration: child.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := state.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: child.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunSucceeded, At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := state.RequestRunCancellation(context.Background(), workflowruntime.RequestRunCancellationRequest{
		RunID: parent.ID, ExpectedGeneration: parent.Generation, IdempotencyKey: "cancel-parent-terminal-child",
		Reason: workflowruntime.Failure{Code: "user_canceled", Message: "run canceled by user"}, At: base.Add(3 * time.Second),
	})
	if err != nil || result.Run.Status != workflowruntime.RunCanceled {
		t.Fatalf("RequestRunCancellation() = %#v, %v", result, err)
	}
	loadedChild, _ := state.LoadRun(context.Background(), child.ID)
	loadedNode, _ := state.LoadNodeInvocation(context.Background(), childNode.ID)
	if loadedChild.Status != workflowruntime.RunSucceeded || loadedChild.Generation != completed.Snapshot.Generation || loadedNode.Status != workflowruntime.NodeSkipped {
		t.Fatalf("terminal direct child mutated: run=%#v node=%#v", loadedChild, loadedNode)
	}
}

func TestWorkflowSQLiteCompleteFanOutCannotMutateTerminalRun(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "fanout-terminal-run.db"))
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "fanout-terminal-run", base)
	parent := createWorkflowTestNode(t, state, run.ID, "bulk", base)
	expanded, err := (workflowruntime.FanOutCoordinator{Store: state}).Expand(context.Background(), workflowruntime.FanOutExpandCommand{
		Parent: parent.ID, ExpectedParentGeneration: parent.Generation, Spec: graph.ForEachSpec{Items: graph.Expression{Text: `[1]`}}, At: base.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	running, err := state.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := state.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunSucceeded, At: base.Add(3 * time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	outputs, err := state.SaveValues(context.Background(), workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "fanout-test", RunID: run.ID}, Values: workflowTestValues(t, "aggregate")})
	if err != nil {
		t.Fatal(err)
	}
	beforeFanOut, _ := state.LoadFanOut(context.Background(), parent.ID)
	beforeParent, _ := state.LoadNodeInvocation(context.Background(), parent.ID)
	beforeEvents, _ := state.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
	failure := workflowruntime.Failure{Code: "canceled", Message: "fan-out canceled"}
	_, completeErr := state.CompleteFanOut(context.Background(), workflowruntime.CompleteFanOutRequest{
		Parent: parent.ID, ExpectedParentGeneration: expanded.Parent.Generation, ExpectedFanOutGeneration: expanded.FanOut.Generation,
		ExpectedChildGenerations: map[workflowruntime.NodeInvocationID]uint64{expanded.Children[0].ID: expanded.Children[0].Generation},
		Status:                   workflowruntime.FanOutCanceled, Outputs: outputs, Failure: &failure, At: base.Add(4 * time.Second),
	})
	if !errors.Is(completeErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("CompleteFanOut(terminal run) error = %v", completeErr)
	}
	afterFanOut, _ := state.LoadFanOut(context.Background(), parent.ID)
	afterParent, _ := state.LoadNodeInvocation(context.Background(), parent.ID)
	afterEvents, _ := state.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
	if afterFanOut.Generation != beforeFanOut.Generation || afterParent.Generation != beforeParent.Generation || len(afterEvents) != len(beforeEvents) {
		t.Fatalf("rejected fan-out completion mutated state fanout=%#v/%#v parent=%#v/%#v events=%d/%d", beforeFanOut, afterFanOut, beforeParent, afterParent, len(beforeEvents), len(afterEvents))
	}
}

func prepareWorkflowSQLiteRunning(t *testing.T, state *WorkflowStateStore, suffix string, base time.Time) (workflowruntime.StartNodeAttemptResult, workflowruntime.ClaimProof) {
	t.Helper()
	run := createWorkflowTestRun(t, state, "run-"+suffix, base)
	node := createWorkflowTestNode(t, state, run.ID, "node", base)
	ready, err := state.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := state.ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: 0, Owner: "worker", Token: "token", IdempotencyKey: "claim-" + suffix, Now: base.Add(time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || !claimed.Acquired {
		t.Fatalf("ClaimNode = %#v, %v", claimed, err)
	}
	proof := workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}
	claimedNode, _ := state.LoadNodeInvocation(context.Background(), ready.Snapshot.ID)
	started, err := state.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: claimedNode.Generation, Claim: proof, Executor: workflowTestExecutor(), At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	return started, proof
}

func workflowTestExecutor() workflowruntime.ExecutorMetadata {
	return workflowruntime.ExecutorMetadata{Kind: "test", Version: "v1", Target: "local"}
}

func workflowwaitResponder() workflowwait.Responder {
	return workflowwait.Responder{Kind: "test", Reference: "late"}
}
