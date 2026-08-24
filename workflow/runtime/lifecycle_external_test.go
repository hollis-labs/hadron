package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestRunTransitionMatrix(t *testing.T) {
	statuses := []workflowruntime.RunStatus{
		workflowruntime.RunPending, workflowruntime.RunRunning, workflowruntime.RunWaiting,
		workflowruntime.RunSucceeded, workflowruntime.RunFailed, workflowruntime.RunCanceled,
		workflowruntime.RunTimedOut, workflowruntime.RunCrashed,
	}
	legal := map[workflowruntime.RunStatus]map[workflowruntime.RunStatus]bool{
		workflowruntime.RunPending: {
			workflowruntime.RunPending: true, workflowruntime.RunRunning: true,
			workflowruntime.RunCanceled: true, workflowruntime.RunTimedOut: true,
		},
		workflowruntime.RunRunning: {
			workflowruntime.RunRunning: true, workflowruntime.RunWaiting: true,
			workflowruntime.RunSucceeded: true, workflowruntime.RunFailed: true,
			workflowruntime.RunCanceled: true, workflowruntime.RunTimedOut: true, workflowruntime.RunCrashed: true,
		},
		workflowruntime.RunWaiting: {
			workflowruntime.RunWaiting: true, workflowruntime.RunRunning: true,
			workflowruntime.RunSucceeded: true, workflowruntime.RunFailed: true,
			workflowruntime.RunCanceled: true, workflowruntime.RunTimedOut: true, workflowruntime.RunCrashed: true,
		},
		workflowruntime.RunSucceeded: {workflowruntime.RunSucceeded: true},
		workflowruntime.RunFailed:    {workflowruntime.RunFailed: true},
		workflowruntime.RunCanceled:  {workflowruntime.RunCanceled: true},
		workflowruntime.RunTimedOut:  {workflowruntime.RunTimedOut: true},
		workflowruntime.RunCrashed:   {workflowruntime.RunCrashed: true},
	}
	for _, from := range statuses {
		for _, to := range statuses {
			err := workflowruntime.ValidateRunStatusTransition(from, to)
			if legal[from][to] && err != nil {
				t.Errorf("%s -> %s should be legal: %v", from, to, err)
			}
			if !legal[from][to] && !errors.Is(err, workflowruntime.ErrInvalidTransition) {
				t.Errorf("%s -> %s should be invalid, got %v", from, to, err)
			}
		}
	}
}

func TestGenericNodeTransitionMatrix(t *testing.T) {
	statuses := []workflowruntime.NodeStatus{
		workflowruntime.NodePending, workflowruntime.NodeReady, workflowruntime.NodeRunning,
		workflowruntime.NodeWaiting, workflowruntime.NodeSucceeded, workflowruntime.NodeFailed,
		workflowruntime.NodeSkipped, workflowruntime.NodeCanceled, workflowruntime.NodeTimedOut,
		workflowruntime.NodeCrashed, workflowruntime.NodeBlocked,
	}
	legal := map[workflowruntime.NodeStatus]map[workflowruntime.NodeStatus]bool{
		workflowruntime.NodePending: {
			workflowruntime.NodePending: true, workflowruntime.NodeReady: true,
			workflowruntime.NodeSkipped: true, workflowruntime.NodeCanceled: true,
			workflowruntime.NodeTimedOut: true, workflowruntime.NodeBlocked: true,
		},
		workflowruntime.NodeReady: {
			workflowruntime.NodeReady: true, workflowruntime.NodeSkipped: true,
			workflowruntime.NodeCanceled: true, workflowruntime.NodeTimedOut: true, workflowruntime.NodeBlocked: true,
		},
		workflowruntime.NodeRunning: {
			workflowruntime.NodeRunning: true, workflowruntime.NodeWaiting: true,
		},
		workflowruntime.NodeWaiting: {
			workflowruntime.NodeWaiting: true, workflowruntime.NodeReady: true,
		},
		workflowruntime.NodeBlocked: {
			workflowruntime.NodeBlocked: true, workflowruntime.NodePending: true,
			workflowruntime.NodeReady: true, workflowruntime.NodeSkipped: true,
			workflowruntime.NodeCanceled: true, workflowruntime.NodeTimedOut: true,
		},
		workflowruntime.NodeSucceeded: {workflowruntime.NodeSucceeded: true},
		workflowruntime.NodeFailed:    {workflowruntime.NodeFailed: true},
		workflowruntime.NodeSkipped:   {workflowruntime.NodeSkipped: true},
		workflowruntime.NodeCanceled:  {workflowruntime.NodeCanceled: true},
		workflowruntime.NodeTimedOut:  {workflowruntime.NodeTimedOut: true},
		workflowruntime.NodeCrashed:   {workflowruntime.NodeCrashed: true},
	}
	for _, from := range statuses {
		for _, to := range statuses {
			err := workflowruntime.ValidateNodeStatusTransition(from, to)
			if legal[from][to] && err != nil {
				t.Errorf("%s -> %s should be legal: %v", from, to, err)
			}
			if !legal[from][to] && !errors.Is(err, workflowruntime.ErrInvalidTransition) {
				t.Errorf("%s -> %s should be invalid, got %v", from, to, err)
			}
		}
	}
}

func TestRunLifecycleEventsNoOpAndTerminalFencing(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 13, 0, 0, 0, time.UTC)
	createRun(t, store, "run-lifecycle", now)

	running, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: "run-lifecycle", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertLifecycleEvent(t, running.Event, workflowruntime.EventRunStatusChanged, now.Add(time.Second), values.RedactionPrivate, values.RetentionRun)
	if running.Event.Invocation != nil || running.Event.Attempt != nil ||
		running.Event.Attributes["from_status"] != "pending" || running.Event.Attributes["to_status"] != "running" {
		t.Fatalf("unexpected run event: %#v", running.Event)
	}

	noOp, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: "run-lifecycle", ExpectedGeneration: 2, To: workflowruntime.RunRunning, At: now.Add(time.Second),
	})
	if err != nil || noOp.Outcome != workflowruntime.TransitionNoOp || noOp.Event != nil || noOp.Snapshot.Generation != 2 {
		t.Fatalf("exact run no-op = %#v, %v", noOp, err)
	}
	if _, err = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: "run-lifecycle", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: now.Add(time.Second),
	}); !errors.Is(err, workflowruntime.ErrCASMismatch) {
		t.Fatalf("CAS must precede no-op handling, got %v", err)
	}
	if _, err = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: "run-lifecycle", ExpectedGeneration: 2, To: workflowruntime.RunRunning, At: now.Add(2 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("non-exact same-state request should conflict: %v", err)
	}
	if _, err = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: "run-lifecycle", ExpectedGeneration: 2, To: workflowruntime.RunPending, At: now.Add(2 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidTransition) {
		t.Fatalf("illegal run edge = %v", err)
	}

	outputs := persistedValues(t, store, "run-lifecycle", "run-output", "done")
	succeeded, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: "run-lifecycle", ExpectedGeneration: 2, To: workflowruntime.RunSucceeded,
		Outputs: &outputs, At: now.Add(3 * time.Second),
	})
	if err != nil || succeeded.Snapshot.Status != workflowruntime.RunSucceeded ||
		succeeded.Event.Values == nil || *succeeded.Event.Values != outputs {
		t.Fatalf("succeed run = %#v, %v", succeeded, err)
	}
	if _, err = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: "run-lifecycle", ExpectedGeneration: 3, To: workflowruntime.RunRunning, At: now.Add(4 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidTransition) {
		t.Fatalf("terminal run reopened: %v", err)
	}
	loaded, err := store.LoadRun(ctx, "run-lifecycle")
	events, eventErr := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: "run-lifecycle"})
	if err != nil || eventErr != nil || loaded.Status != workflowruntime.RunSucceeded || loaded.Generation != 3 || len(events) != 2 {
		t.Fatalf("failed transitions mutated run/events: %#v, events=%d, %v, %v", loaded, len(events), err, eventErr)
	}
}

func TestBlockedReasonReplayConflictAndTerminalNode(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 13, 0, 0, 0, time.UTC)
	id := invocationID("run-blocked", "node")
	createNode(t, store, id, workflowruntime.NodePending, 0, now)
	reason := &workflowruntime.BlockedReason{
		Code: "dependency_missing", Message: "dependency unavailable",
		Details: map[string]string{"dependency": "prepare"},
	}
	blocked, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 1, To: workflowruntime.NodeBlocked, Blocked: reason, At: now.Add(time.Second),
	})
	if err != nil || blocked.Event.Attributes["blocked_reason"] != reason.Code {
		t.Fatalf("blocked transition = %#v, %v", blocked, err)
	}
	reason.Details["dependency"] = "mutated"
	equivalentReason := &workflowruntime.BlockedReason{
		Code: "dependency_missing", Message: "dependency unavailable",
		Details: map[string]string{"dependency": "prepare"},
	}
	noOp, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 2, To: workflowruntime.NodeBlocked,
		Blocked: equivalentReason, At: now.Add(time.Second),
	})
	if err != nil || noOp.Outcome != workflowruntime.TransitionNoOp || noOp.Event != nil {
		t.Fatalf("blocked exact no-op = %#v, %v", noOp, err)
	}
	different := cloneBlockedForTest(equivalentReason)
	different.Message = "different"
	if _, err = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 2, To: workflowruntime.NodeBlocked,
		Blocked: different, At: now.Add(time.Second),
	}); !errors.Is(err, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("different blocked reason should conflict: %v", err)
	}
	ready, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 2, To: workflowruntime.NodeReady, At: now.Add(2 * time.Second),
	})
	if err != nil || ready.Snapshot.Blocked != nil || ready.Event == nil ||
		ready.Event.Attributes["blocked_reason"] != equivalentReason.Code {
		t.Fatalf("leaving blocked must clear reason: %#v, %v", ready, err)
	}
	skipped, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 3, To: workflowruntime.NodeSkipped, At: now.Add(3 * time.Second),
	})
	if err != nil || !skipped.Snapshot.Status.Terminal() || skipped.Snapshot.Lease != nil {
		t.Fatalf("skip node = %#v, %v", skipped, err)
	}
	if _, err = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 4, To: workflowruntime.NodeReady, At: now.Add(4 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidTransition) {
		t.Fatalf("terminal node reopened: %v", err)
	}
	loaded, loadErr := store.LoadNodeInvocation(ctx, id)
	events, eventErr := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: id.RunID})
	if loadErr != nil || eventErr != nil || loaded.Generation != 4 || loaded.Status != workflowruntime.NodeSkipped || len(events) != 3 {
		t.Fatalf("failed node transitions mutated state: %#v events=%d %v %v", loaded, len(events), loadErr, eventErr)
	}
}

func TestAttemptLifecycleHistorySuspendResumeAndRetryReady(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 14, 0, 0, 0, time.UTC)
	id := invocationID("run-attempt", "execute")
	createNode(t, store, id, workflowruntime.NodeReady, 0, now)
	inputs := persistedValues(t, store, id.RunID, "attempt-input", map[string]any{"task": "one"})
	claim := claimNode(t, store, id, 0, "worker-1", "token-1", "claim-1", now.Add(time.Second), now.Add(time.Hour))
	executor := testExecutor()
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: id, ExpectedNodeGeneration: 3, Claim: claim,
		Executor: executor, Inputs: &inputs, At: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if started.Node.Status != workflowruntime.NodeRunning || started.Node.LatestAttempt != 1 ||
		started.Attempt.ID.Number != 1 || started.Attempt.Status != workflowruntime.NodeRunning ||
		!started.Attempt.StartedAt.Equal(now.Add(2*time.Second)) || !started.Attempt.FinishedAt.IsZero() {
		t.Fatalf("started attempt = %#v", started)
	}
	assertAttemptEvent(t, started.Event, workflowruntime.EventNodeAttemptStarted, started.Attempt, "ready", "running")
	executor.Attributes["mode"] = "mutated"
	started.Attempt.Executor.Attributes["mode"] = "also-mutated"
	loadedAttempt, err := store.LoadAttempt(ctx, started.Attempt.ID)
	if err != nil || loadedAttempt.Executor.Attributes["mode"] != "fake" {
		t.Fatalf("executor metadata was aliased: %#v, %v", loadedAttempt, err)
	}
	if _, err = store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: id, ExpectedNodeGeneration: 4, Claim: claim,
		Executor: testExecutor(), At: now.Add(3 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidTransition) {
		t.Fatalf("second unfinished attempt should fail: %v", err)
	}
	if _, err = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 4, To: workflowruntime.NodeSucceeded,
		Claim: &claim, At: now.Add(3 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidTransition) {
		t.Fatalf("bare terminal transition with attempt should fail: %v", err)
	}

	waiting, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 4, To: workflowruntime.NodeWaiting,
		Claim: &claim, At: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Snapshot.Lease != nil {
		t.Fatalf("waiting transition retained lease: %#v", waiting.Snapshot.Lease)
	}
	if waiting.Event == nil || waiting.Event.Attempt == nil || *waiting.Event.Attempt != started.Attempt.ID ||
		waiting.Event.Attributes["attempt_number"] != "1" ||
		waiting.Event.Attributes["executor_kind"] != started.Attempt.Executor.Kind ||
		waiting.Event.Attributes["executor_version"] != started.Attempt.Executor.Version {
		t.Fatalf("waiting transition lost attempt identity: %#v", waiting.Event)
	}
	ready, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: waiting.Snapshot.Generation, To: workflowruntime.NodeReady,
		At: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: ready.Snapshot.Generation, To: workflowruntime.NodeRunning,
		At: now.Add(5 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrClaimMismatch) {
		t.Fatalf("claimless attempt resume should fail: %v", err)
	}
	resumeClaim := claimNode(t, store, id, 1, "worker-resume", "token-resume", "claim-resume", now.Add(5*time.Second), now.Add(time.Hour))
	resumed, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 7, To: workflowruntime.NodeRunning,
		Claim: &resumeClaim, At: now.Add(5 * time.Second),
	})
	if err != nil || resumed.Snapshot.LatestAttempt != 1 {
		t.Fatalf("resume unfinished attempt = %#v, %v", resumed, err)
	}
	history, err := store.ListAttempts(ctx, id)
	if err != nil || len(history) != 1 || history[0].Status != workflowruntime.NodeRunning {
		t.Fatalf("suspend/resume changed attempt history: %#v, %v", history, err)
	}

	partialOutputs := persistedValues(t, store, id.RunID, "partial-output", "partial")
	failure := &workflowruntime.Failure{
		Code: "temporary", Message: "retryable failure", Retryable: true,
		Details: map[string]string{"class": "transient"},
	}
	finished, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: id, AttemptNumber: 1,
		ExpectedNodeGeneration: resumed.Snapshot.Generation, ExpectedAttemptGeneration: 1,
		Claim: resumeClaim, AttemptStatus: workflowruntime.NodeFailed, NextNodeStatus: workflowruntime.NodeReady,
		Outputs: &partialOutputs, Failure: failure, At: now.Add(6 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if finished.Node.Status != workflowruntime.NodeReady || finished.Node.Lease != nil ||
		finished.Node.Outputs != nil || finished.Node.LatestAttempt != 1 ||
		finished.Attempt.Status != workflowruntime.NodeFailed || finished.Attempt.Outputs == nil ||
		finished.Attempt.Failure.Code != "temporary" || !finished.Attempt.FinishedAt.Equal(now.Add(6*time.Second)) {
		t.Fatalf("retry-ready finish = %#v", finished)
	}
	assertAttemptEvent(t, finished.Event, workflowruntime.EventNodeAttemptFinished, finished.Attempt, "running", "ready")
	if finished.Event.Attributes["attempt_status"] != "failed" || finished.Event.Attributes["failure_code"] != "temporary" {
		t.Fatalf("finish event attributes = %#v", finished.Event.Attributes)
	}
	failure.Details["class"] = "mutated"
	finished.Attempt.Failure.Details["class"] = "also-mutated"

	secondClaim := claimNode(t, store, id, 2, "worker-2", "token-2", "claim-2", now.Add(7*time.Second), now.Add(time.Hour))
	second, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: id, ExpectedNodeGeneration: 10, Claim: secondClaim,
		Executor: testExecutor(), Inputs: &inputs, At: now.Add(8 * time.Second),
	})
	if err != nil || second.Attempt.ID.Number != 2 {
		t.Fatalf("second attempt = %#v, %v", second, err)
	}
	outputs := persistedValues(t, store, id.RunID, "attempt-output", "complete")
	succeeded, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: id, AttemptNumber: 2,
		ExpectedNodeGeneration: second.Node.Generation, ExpectedAttemptGeneration: 1,
		Claim: secondClaim, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded,
		Outputs: &outputs, At: now.Add(9 * time.Second),
	})
	if err != nil || succeeded.Node.Status != workflowruntime.NodeSucceeded ||
		succeeded.Node.Outputs == nil || succeeded.Node.Lease != nil {
		t.Fatalf("terminal attempt = %#v, %v", succeeded, err)
	}
	history, err = store.ListAttempts(ctx, id)
	if err != nil || len(history) != 2 || history[0].ID.Number != 1 || history[1].ID.Number != 2 ||
		history[0].Failure.Details["class"] != "transient" {
		t.Fatalf("durable attempt history = %#v, %v", history, err)
	}
	if _, err = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: succeeded.Node.Generation,
		To: workflowruntime.NodeReady, At: now.Add(10 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidTransition) {
		t.Fatalf("terminal aggregate node reopened: %v", err)
	}
}

func TestFinishAttemptFailureIsAtomicAfterWaitResume(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 15, 0, 0, 0, time.UTC)
	id := invocationID("run-waiting-attempt", "waiter")
	createNode(t, store, id, workflowruntime.NodeReady, 0, now)
	claim := claimNode(t, store, id, 0, "worker", "token", "claim", now.Add(time.Second), now.Add(time.Hour))
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: id, ExpectedNodeGeneration: 3, Claim: claim,
		Executor: testExecutor(), At: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: started.Node.Generation,
		To: workflowruntime.NodeWaiting, Claim: &claim, At: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Snapshot.Lease != nil {
		t.Fatalf("waiting transition retained lease: %#v", waiting.Snapshot.Lease)
	}
	waitFailure := &workflowruntime.Failure{Code: "external_timeout", Message: "wait timed out"}
	if _, err = store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: id, AttemptNumber: 1,
		ExpectedNodeGeneration: waiting.Snapshot.Generation, ExpectedAttemptGeneration: 1,
		Claim: claim, AttemptStatus: workflowruntime.NodeTimedOut, NextNodeStatus: workflowruntime.NodeTimedOut,
		Failure: waitFailure, At: now.Add(4 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidTransition) {
		t.Fatalf("waiting attempt must resume before finish: %v", err)
	}
	_, err = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: waiting.Snapshot.Generation,
		To: workflowruntime.NodeReady, At: now.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	finishClaim := claimNode(t, store, id, 1, "finisher", "finish-token", "finish-claim", now.Add(5*time.Second), now.Add(time.Hour))
	claimed, err := store.LoadNodeInvocation(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: claimed.Generation,
		To: workflowruntime.NodeRunning, Claim: &finishClaim, At: now.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeEvents, _ := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: id.RunID})
	if _, err = store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: id, AttemptNumber: 1,
		ExpectedNodeGeneration: resumed.Snapshot.Generation, ExpectedAttemptGeneration: 1,
		Claim: finishClaim, AttemptStatus: workflowruntime.NodeTimedOut, NextNodeStatus: workflowruntime.NodeTimedOut,
		At: now.Add(6 * time.Second),
	}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("missing failure should be rejected: %v", err)
	}
	unchangedNode, nodeErr := store.LoadNodeInvocation(ctx, id)
	unchangedAttempt, attemptErr := store.LoadAttempt(ctx, workflowruntime.AttemptID{Invocation: id, Number: 1})
	unchangedEvents, eventsErr := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: id.RunID})
	if nodeErr != nil || attemptErr != nil || eventsErr != nil ||
		unchangedNode.Generation != resumed.Snapshot.Generation ||
		unchangedAttempt.Generation != 1 || unchangedAttempt.Status != workflowruntime.NodeRunning ||
		len(unchangedEvents) != len(beforeEvents) {
		t.Fatalf("failed finish partially mutated: node=%#v attempt=%#v events=%d/%d errs=%v,%v,%v",
			unchangedNode, unchangedAttempt, len(unchangedEvents), len(beforeEvents), nodeErr, attemptErr, eventsErr)
	}
	timedOut, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: id, AttemptNumber: 1,
		ExpectedNodeGeneration: resumed.Snapshot.Generation, ExpectedAttemptGeneration: 1,
		Claim: finishClaim, AttemptStatus: workflowruntime.NodeTimedOut, NextNodeStatus: workflowruntime.NodeTimedOut,
		Failure: waitFailure, At: now.Add(6 * time.Second),
	})
	if err != nil || timedOut.Node.Status != workflowruntime.NodeTimedOut ||
		timedOut.Attempt.Status != workflowruntime.NodeTimedOut || timedOut.Node.Lease != nil {
		t.Fatalf("finish resumed attempt = %#v, %v", timedOut, err)
	}
}

func TestConcurrentRunTransitionCASProducesOneEvent(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC)
	createRun(t, store, "run-concurrent-transition", now)
	targets := []workflowruntime.RunStatus{workflowruntime.RunCanceled, workflowruntime.RunTimedOut}
	var wg sync.WaitGroup
	results := make(chan error, len(targets))
	for _, target := range targets {
		target := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
				RunID: "run-concurrent-transition", ExpectedGeneration: 1, To: target, At: now.Add(time.Second),
			})
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	applied, stale := 0, 0
	for err := range results {
		switch {
		case err == nil:
			applied++
		case errors.Is(err, workflowruntime.ErrCASMismatch):
			stale++
		default:
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	events, err := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: "run-concurrent-transition"})
	if err != nil || applied != 1 || stale != 1 || len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("applied=%d stale=%d events=%#v err=%v", applied, stale, events, err)
	}
}

func TestSnapshotSavesCannotBypassLifecycle(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 17, 0, 0, 0, time.UTC)
	createRun(t, store, "run-save-fence", now)
	run, err := store.LoadRun(ctx, "run-save-fence")
	if err != nil {
		t.Fatal(err)
	}
	run.Status = workflowruntime.RunRunning
	run.UpdatedAt = now.Add(time.Second)
	if _, err = store.SaveRun(ctx, workflowruntime.SaveRunRequest{
		Snapshot: run, ExpectedGeneration: 1,
	}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SaveRun bypass = %v", err)
	}
	id := invocationID("run-save-fence", "node")
	createNode(t, store, id, workflowruntime.NodePending, 0, now)
	node, err := store.LoadNodeInvocation(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	node.Status = workflowruntime.NodeReady
	node.UpdatedAt = now.Add(time.Second)
	if _, err = store.SaveNodeInvocation(ctx, workflowruntime.SaveNodeInvocationRequest{
		Snapshot: node, ExpectedGeneration: 1,
	}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SaveNodeInvocation bypass = %v", err)
	}
	unchangedRun, runErr := store.LoadRun(ctx, "run-save-fence")
	unchangedNode, nodeErr := store.LoadNodeInvocation(ctx, id)
	events, eventErr := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: "run-save-fence"})
	if runErr != nil || nodeErr != nil || eventErr != nil ||
		unchangedRun.Generation != 1 || unchangedRun.Status != workflowruntime.RunPending ||
		unchangedNode.Generation != 1 || unchangedNode.Status != workflowruntime.NodePending ||
		len(events) != 0 {
		t.Fatalf("bypass attempts mutated state: run=%#v node=%#v events=%#v errs=%v,%v,%v",
			unchangedRun, unchangedNode, events, runErr, nodeErr, eventErr)
	}
}

func TestAttemptSnapshotInvariants(t *testing.T) {
	now := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
	base := workflowruntime.AttemptSnapshot{
		ID:     workflowruntime.AttemptID{Invocation: invocationID("run-record", "node"), Number: 1},
		Status: workflowruntime.NodeRunning, Executor: testExecutor(), StartedAt: now,
		Generation: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("running attempt should validate: %v", err)
	}
	unsupported := base
	unsupported.Status = workflowruntime.NodeReady
	if err := unsupported.Validate(); err == nil {
		t.Fatal("ready is not an attempt status")
	}
	finishedRunning := base
	finishedRunning.FinishedAt = now.Add(time.Second)
	if err := finishedRunning.Validate(); err == nil {
		t.Fatal("running attempt must not carry finish time")
	}
	missingFailure := base
	missingFailure.Status = workflowruntime.NodeFailed
	missingFailure.FinishedAt = now.Add(time.Second)
	missingFailure.UpdatedAt = missingFailure.FinishedAt
	if err := missingFailure.Validate(); err == nil {
		t.Fatal("failed attempt must carry typed failure")
	}
	succeededWithFailure := base
	succeededWithFailure.Status = workflowruntime.NodeSucceeded
	succeededWithFailure.Failure = &workflowruntime.Failure{Code: "unexpected", Message: "not allowed"}
	succeededWithFailure.FinishedAt = now.Add(time.Second)
	succeededWithFailure.UpdatedAt = succeededWithFailure.FinishedAt
	if err := succeededWithFailure.Validate(); err == nil {
		t.Fatal("succeeded attempt must not carry failure")
	}
	invalidExecutor := base
	invalidExecutor.Executor.Kind = ""
	if err := invalidExecutor.Validate(); err == nil {
		t.Fatal("attempt must carry valid executor metadata")
	}
}

func createRun(t *testing.T, store workflowruntime.StateStore, id workflowruntime.RunID, at time.Time) {
	t.Helper()
	_, _, err := store.CreateRun(context.Background(), workflowruntime.CreateRunRequest{
		ID: id, Plan: testPlan(), Status: workflowruntime.RunPending,
		StartIdempotencyKey: "start-" + string(id), CreatedAt: at,
	})
	if err != nil {
		t.Fatalf("CreateRun(%s): %v", id, err)
	}
}

func claimNode(t *testing.T, store workflowruntime.StateStore, id workflowruntime.NodeInvocationID, generation uint64, owner, token, key string, now, until time.Time) workflowruntime.ClaimProof {
	t.Helper()
	claim, err := store.ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: generation,
		Owner: owner, Token: token, IdempotencyKey: key, Now: now, LeaseUntil: until,
	})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("ClaimNode(%v): %#v, %v", id, claim, err)
	}
	return workflowruntime.ClaimProof{
		Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation,
	}
}

func persistedValues(t *testing.T, store workflowruntime.StateStore, runID workflowruntime.RunID, kind string, payload any) values.ValueSetRef {
	t.Helper()
	ref, err := store.SaveValues(context.Background(), workflowruntime.SaveValuesRequest{
		Owner:  workflowruntime.ValueOwner{Kind: kind, RunID: runID},
		Values: testValueSet(t, payload),
	})
	if err != nil {
		t.Fatalf("SaveValues(%s): %v", kind, err)
	}
	return ref
}

func assertLifecycleEvent(t *testing.T, event *workflowruntime.Event, eventType string, at time.Time, redaction values.RedactionClass, retention values.RetentionClass) {
	t.Helper()
	if event == nil || event.Type != eventType || !event.OccurredAt.Equal(at) ||
		event.Redaction != redaction || event.Retention != retention {
		t.Fatalf("lifecycle event = %#v", event)
	}
}

func assertAttemptEvent(t *testing.T, event workflowruntime.Event, eventType string, attempt workflowruntime.AttemptSnapshot, from, to string) {
	t.Helper()
	assertLifecycleEvent(t, &event, eventType, event.OccurredAt, values.RedactionPrivate, values.RetentionRun)
	if event.Invocation == nil || *event.Invocation != attempt.ID.Invocation ||
		event.Attempt == nil || *event.Attempt != attempt.ID ||
		event.Attributes["from_status"] != from || event.Attributes["to_status"] != to ||
		event.Attributes["attempt_number"] != "1" && event.Attributes["attempt_number"] != "2" ||
		event.Attributes["executor_kind"] != attempt.Executor.Kind ||
		event.Attributes["executor_version"] != attempt.Executor.Version {
		t.Fatalf("attempt event identity/attributes = %#v", event)
	}
	expectedAt := attempt.StartedAt
	if eventType == workflowruntime.EventNodeAttemptFinished {
		expectedAt = attempt.FinishedAt
	}
	if !event.OccurredAt.Equal(expectedAt) {
		t.Fatalf("event timestamp %v != attempt timestamp %v", event.OccurredAt, expectedAt)
	}
}

func cloneBlockedForTest(reason *workflowruntime.BlockedReason) *workflowruntime.BlockedReason {
	copyReason := *reason
	copyReason.Dependencies = append([]workflowruntime.NodeInvocationID(nil), reason.Dependencies...)
	copyReason.Details = make(map[string]string, len(reason.Details))
	for key, value := range reason.Details {
		copyReason.Details[key] = value
	}
	return &copyReason
}
