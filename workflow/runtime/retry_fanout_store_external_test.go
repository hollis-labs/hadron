package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestRetryActivationClosesAttemptReleasesClaimAndSurvivesRecovery(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	base := time.Date(2026, time.August, 24, 15, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("run-retry-durable")
	id := invocationID(runID, "fetch")
	createRun(t, store, runID, base)
	createNode(t, store, id, workflowruntime.NodeReady, 0, base)
	claim := claimNode(t, store, id, 0, "worker", "token-1", "claim-retry-1", base.Add(time.Second), base.Add(time.Minute))
	claimed, _ := store.LoadNodeInvocation(ctx, id)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: testExecutor(), At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	failure := workflowruntime.Failure{Code: "rate_limited", Message: "retry later", Retryable: true}
	scheduled, err := store.ScheduleNodeRetry(ctx, workflowruntime.ScheduleNodeRetryRequest{
		Activation:             workflowruntime.RetryActivationSnapshot{ID: "retry-activation-1", Attempt: started.Attempt.ID, Failure: failure, FireAt: base.Add(10 * time.Second), Status: workflowruntime.RetryScheduled},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: claim, AttemptStatus: workflowruntime.NodeFailed, At: base.Add(3 * time.Second),
	})
	if err != nil || scheduled.Node.Status != workflowruntime.NodeWaiting || scheduled.Node.Lease != nil || scheduled.Attempt.Status != workflowruntime.NodeFailed || len(scheduled.Events) != 2 {
		t.Fatalf("ScheduleNodeRetry = %#v, %v", scheduled, err)
	}
	recovered, err := store.RecoverRetryActivations(ctx, workflowruntime.RetryActivationQuery{DueBefore: base.Add(11 * time.Second)})
	if err != nil || len(recovered) != 1 || recovered[0].ID != "retry-activation-1" {
		t.Fatalf("RecoverRetryActivations = %#v, %v", recovered, err)
	}
	if _, activateErr := store.ActivateNodeRetry(ctx, workflowruntime.ActivateNodeRetryRequest{ActivationID: scheduled.Activation.ID, ExpectedActivationGeneration: 1, ExpectedNodeGeneration: scheduled.Node.Generation, IdempotencyKey: "activate-early", Now: base.Add(9 * time.Second)}); !errors.Is(activateErr, workflowruntime.ErrRetryNotDue) {
		t.Fatalf("early activation = %v", activateErr)
	}
	activateRequest := workflowruntime.ActivateNodeRetryRequest{ActivationID: scheduled.Activation.ID, ExpectedActivationGeneration: 1, ExpectedNodeGeneration: scheduled.Node.Generation, IdempotencyKey: "activate-due", Now: base.Add(10 * time.Second)}
	activated, err := store.ActivateNodeRetry(ctx, activateRequest)
	if err != nil || activated.Node.Status != workflowruntime.NodeReady || activated.Activation.Status != workflowruntime.RetryActivated {
		t.Fatalf("ActivateNodeRetry = %#v, %v", activated, err)
	}
	replayRequest := activateRequest
	replayRequest.Now = activateRequest.Now.In(time.FixedZone("equivalent", -7*60*60))
	replayed, err := store.ActivateNodeRetry(ctx, replayRequest)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Activation.Generation != activated.Activation.Generation {
		t.Fatalf("ActivateNodeRetry(equivalent instant replay) = %#v, %v", replayed, err)
	}
	claim2 := claimNode(t, store, id, 1, "worker", "token-2", "claim-retry-2", base.Add(11*time.Second), base.Add(time.Minute))
	claimed2, _ := store.LoadNodeInvocation(ctx, id)
	second, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed2.Generation, Claim: claim2, Executor: testExecutor(), At: base.Add(12 * time.Second)})
	if err != nil || second.Attempt.ID.Number != 2 {
		t.Fatalf("second attempt = %#v, %v", second, err)
	}
	attempts, err := store.ListAttempts(ctx, id)
	if err != nil || len(attempts) != 2 || attempts[0].Status != workflowruntime.NodeFailed || attempts[1].Status != workflowruntime.NodeRunning {
		t.Fatalf("attempt history = %#v, %v", attempts, err)
	}
}

func TestRetryCoordinatorSchedulerFailureKeepsDurableActivationRecoverable(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	base := time.Date(2026, time.August, 24, 15, 10, 0, 0, time.UTC)
	runID := workflowruntime.RunID("run-retry-scheduler-failure")
	id := invocationID(runID, "fetch")
	createRun(t, store, runID, base)
	createNode(t, store, id, workflowruntime.NodeReady, 0, base)
	claim := claimNode(t, store, id, 0, "worker", "token", "claim-retry-scheduler", base.Add(time.Second), base.Add(time.Minute))
	claimed, _ := store.LoadNodeInvocation(ctx, id)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: testExecutor(), At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	schedulerErr := errors.New("host scheduler unavailable")
	scheduler := &recordingScheduler{scheduleErr: schedulerErr}
	failure := workflowruntime.Failure{Code: "temporary", Message: "retry later", Retryable: true}
	coordinator := workflowruntime.RetryCoordinator{Store: store, Scheduler: scheduler}
	result, decision, err := coordinator.Schedule(ctx, workflowruntime.ScheduleRetryCommand{
		Node:         graph.Node{ID: id.NodeID, Retry: &graph.RetryPolicy{Attempts: 2, On: []string{"temporary"}, Backoff: graph.BackoffPolicy{Strategy: graph.BackoffFixed, InitialDelay: "5s"}}},
		Spec:         stepkind.StepKindSpec{Effects: graph.EffectSet{graph.EffectRead}, Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe},
		NodeSnapshot: started.Node, Attempt: started.Attempt, Claim: claim, Failure: failure,
		AttemptStatus: workflowruntime.NodeFailed, At: base.Add(3 * time.Second),
	})
	if !errors.Is(err, schedulerErr) || !decision.Retry || result.Activation.Status != workflowruntime.RetryScheduled || result.Node.Status != workflowruntime.NodeWaiting || result.Node.Lease != nil {
		t.Fatalf("Schedule(post-commit failure) = %#v decision=%#v err=%v", result, decision, err)
	}
	recovered, recoverErr := store.RecoverRetryActivations(ctx, workflowruntime.RetryActivationQuery{DueBefore: base.Add(9 * time.Second)})
	if recoverErr != nil || len(recovered) != 1 || recovered[0].ID != result.Activation.ID {
		t.Fatalf("RecoverRetryActivations() = %#v, %v", recovered, recoverErr)
	}
}

func TestRunCancellationCancelsRetryAndFencesActivationAndClaims(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	base := time.Date(2026, time.August, 24, 15, 15, 0, 0, time.UTC)
	runID := workflowruntime.RunID("run-retry-canceled")
	id := invocationID(runID, "fetch")
	createRun(t, store, runID, base)
	createNode(t, store, id, workflowruntime.NodeReady, 0, base)
	claim := claimNode(t, store, id, 0, "worker", "token", "claim-canceled-retry", base.Add(time.Second), base.Add(time.Minute))
	claimed, _ := store.LoadNodeInvocation(ctx, id)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: testExecutor(), At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	failure := workflowruntime.Failure{Code: "temporary", Message: "retry later", Retryable: true}
	scheduled, err := store.ScheduleNodeRetry(ctx, workflowruntime.ScheduleNodeRetryRequest{
		Activation:             workflowruntime.RetryActivationSnapshot{ID: "retry-to-cancel", Attempt: started.Attempt.ID, Failure: failure, FireAt: base.Add(time.Minute), Status: workflowruntime.RetryScheduled},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, AttemptStatus: workflowruntime.NodeFailed, At: base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelRequest := workflowruntime.RequestRunCancellationRequest{RunID: runID, ExpectedGeneration: 1, IdempotencyKey: "cancel-retry-run", Reason: workflowruntime.Failure{Code: "user_canceled", Message: "canceled"}, At: base.Add(4 * time.Second)}
	canceled, err := store.RequestRunCancellation(ctx, cancelRequest)
	if err != nil || canceled.Run.Status != workflowruntime.RunCanceled || len(canceled.Nodes) != 1 || canceled.Nodes[0].Status != workflowruntime.NodeCanceled {
		t.Fatalf("RequestRunCancellation = %#v, %v", canceled, err)
	}
	replayCancel := cancelRequest
	replayCancel.At = cancelRequest.At.In(time.FixedZone("equivalent", 5*60*60+30*60))
	replayedCancel, err := store.RequestRunCancellation(ctx, replayCancel)
	if err != nil || replayedCancel.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("RequestRunCancellation(equivalent instant replay) = %#v, %v", replayedCancel, err)
	}
	activation, err := store.LoadRetryActivation(ctx, scheduled.Activation.ID)
	if err != nil || activation.Status != workflowruntime.RetryCanceled {
		t.Fatalf("canceled activation = %#v, %v", activation, err)
	}
	if _, activateErr := store.ActivateNodeRetry(ctx, workflowruntime.ActivateNodeRetryRequest{ActivationID: activation.ID, ExpectedActivationGeneration: activation.Generation, ExpectedNodeGeneration: canceled.Nodes[0].Generation, IdempotencyKey: "late-activation", Now: base.Add(2 * time.Minute)}); activateErr == nil {
		t.Fatal("canceled retry activation reopened work")
	}
	recovered, err := store.RecoverRetryActivations(ctx, workflowruntime.RetryActivationQuery{})
	if err != nil || len(recovered) != 0 {
		t.Fatalf("RecoverRetryActivations after cancel = %#v, %v", recovered, err)
	}
	claimResult, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: id, ExpectedClaimGeneration: canceled.Nodes[0].ClaimGeneration, Owner: "late", Token: "late", IdempotencyKey: "late-claim-canceled-retry", Now: base.Add(2 * time.Minute), LeaseUntil: base.Add(3 * time.Minute)})
	if err != nil || claimResult.Acquired {
		t.Fatalf("claim canceled run = %#v, %v", claimResult, err)
	}
}

func TestFanOutClaimSlotsPersistAcrossWaitAndTypedItemsRecover(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	base := time.Date(2026, time.August, 24, 15, 30, 0, 0, time.UTC)
	runID := workflowruntime.RunID("run-fanout")
	parent := invocationID(runID, "bulk")
	createRun(t, store, runID, base)
	createNode(t, store, parent, workflowruntime.NodePending, 0, base)
	coordinator := workflowruntime.FanOutCoordinator{Store: store}
	expanded, err := coordinator.Expand(ctx, workflowruntime.FanOutExpandCommand{
		Parent: parent, ExpectedParentGeneration: 1,
		Spec: graph.ForEachSpec{Items: graph.Expression{Text: `["a", "b"]`}, MaxConcurrency: 1, Tolerate: &graph.ToleratedFailurePolicy{Count: 1}},
		At:   base.Add(time.Second),
	})
	if err != nil || len(expanded.Children) != 2 || expanded.Parent.Status != workflowruntime.NodeWaiting {
		t.Fatalf("Expand = %#v, %v", expanded, err)
	}
	first, second := expanded.Children[0], expanded.Children[1]
	claim1 := claimNode(t, store, first.ID, 0, "worker-1", "token-1", "fanout-claim-1", base.Add(2*time.Second), base.Add(time.Minute))
	denied, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: second.ID, ExpectedClaimGeneration: 0, Owner: "worker-2", Token: "token-2", IdempotencyKey: "fanout-claim-2-denied", Now: base.Add(2 * time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || denied.Acquired {
		t.Fatalf("second claim while first slot reserved = %#v, %v", denied, err)
	}
	firstClaimed, _ := store.LoadNodeInvocation(ctx, first.ID)
	started1, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: first.ID, ExpectedNodeGeneration: firstClaimed.Generation, Claim: claim1, Executor: testExecutor(), Inputs: first.Inputs, At: base.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: first.ID, ExpectedGeneration: started1.Node.Generation, To: workflowruntime.NodeWaiting, Claim: &claim1, At: base.Add(4 * time.Second)})
	if err != nil || waiting.Snapshot.Lease != nil {
		t.Fatalf("first item wait = %#v, %v", waiting, err)
	}
	denied, err = store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: second.ID, ExpectedClaimGeneration: 0, Owner: "worker-2", Token: "token-2b", IdempotencyKey: "fanout-claim-2-wait", Now: base.Add(5 * time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || denied.Acquired {
		t.Fatalf("waiting item released fan-out slot: %#v, %v", denied, err)
	}
	ready1, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: first.ID, ExpectedGeneration: waiting.Snapshot.Generation, To: workflowruntime.NodeReady, At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	claim1Resume := claimNode(t, store, first.ID, 1, "worker-1", "token-1b", "fanout-claim-1-resume", base.Add(7*time.Second), base.Add(time.Minute))
	running1, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: first.ID, ExpectedGeneration: ready1.Snapshot.Generation + 1, To: workflowruntime.NodeRunning, Claim: &claim1Resume, At: base.Add(8 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	secretRef, err := values.ParseSecretRef("secret://project/fanout#token")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := values.NewSecretRef(secretRef, values.Metadata{Producer: values.Producer{Kind: "fanout-test", Reference: "first"}, MediaType: "text/plain", Redaction: values.RedactionSecret, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	outputs1, err := store.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "fanout-output", RunID: runID, Invocation: &first.ID, Attempt: &started1.Attempt.ID}, Values: values.ValueSet{"token": secret}})
	if err != nil {
		t.Fatal(err)
	}
	finished1, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: first.ID, AttemptNumber: 1, ExpectedNodeGeneration: running1.Snapshot.Generation, ExpectedAttemptGeneration: started1.Attempt.Generation, Claim: claim1Resume, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, Outputs: &outputs1, At: base.Add(9 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}

	claim2 := claimNode(t, store, second.ID, 0, "worker-2", "token-2", "fanout-claim-2", base.Add(10*time.Second), base.Add(time.Minute))
	secondClaimed, _ := store.LoadNodeInvocation(ctx, second.ID)
	started2, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: second.ID, ExpectedNodeGeneration: secondClaimed.Generation, Claim: claim2, Executor: testExecutor(), Inputs: second.Inputs, At: base.Add(11 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	failure := workflowruntime.Failure{Code: "item_failed", Message: "item failed"}
	finished2, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: second.ID, AttemptNumber: 1, ExpectedNodeGeneration: started2.Node.Generation, ExpectedAttemptGeneration: started2.Attempt.Generation, Claim: claim2, AttemptStatus: workflowruntime.NodeFailed, NextNodeStatus: workflowruntime.NodeFailed, Failure: &failure, At: base.Add(12 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if finished1.Node.Status != workflowruntime.NodeSucceeded || finished2.Node.Status != workflowruntime.NodeFailed {
		t.Fatalf("item statuses = %s, %s", finished1.Node.Status, finished2.Node.Status)
	}
	completed, aggregate, items, err := coordinator.Collect(ctx, parent, base.Add(13*time.Second))
	if err != nil || completed.FanOut.Status != workflowruntime.FanOutSucceeded || len(items) != 2 || items[0].OutputValues["token"].Type != values.TypeSecretRef {
		t.Fatalf("Collect = %#v aggregate=%#v items=%#v, %v", completed, aggregate, items, err)
	}
	loadedItems, err := store.LoadFanOutItemResults(ctx, parent)
	if err != nil || len(loadedItems) != 2 || loadedItems[0].OutputValues["token"].Type != values.TypeSecretRef || loadedItems[0].Outputs == nil {
		t.Fatalf("LoadFanOutItemResults = %#v, %v", loadedItems, err)
	}
	if rendered := fmt.Sprint(aggregate["items"].Inline); strings.Contains(rendered, string(secretRef)) {
		t.Fatalf("aggregate metadata leaked secret reference: %s", rendered)
	}
}

func TestFanOutToleranceCountAndPercentageBoundaries(t *testing.T) {
	for _, test := range []struct {
		name     string
		policy   *graph.ToleratedFailurePolicy
		failures int
		total    int
		want     bool
	}{
		{name: "default rejects", failures: 1, total: 3},
		{name: "count inclusive", policy: &graph.ToleratedFailurePolicy{Count: 1}, failures: 1, total: 3, want: true},
		{name: "percentage below", policy: &graph.ToleratedFailurePolicy{Percentage: 33}, failures: 1, total: 3},
		{name: "percentage above", policy: &graph.ToleratedFailurePolicy{Percentage: 34}, failures: 1, total: 3, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := workflowruntime.FanOutFailuresTolerated(test.policy, test.failures, test.total)
			if err != nil || got != test.want {
				t.Fatalf("FanOutFailuresTolerated() = %t, %v, want %t", got, err, test.want)
			}
		})
	}
	if err := workflowruntime.ValidateFanOutTolerance(&graph.ToleratedFailurePolicy{Percentage: math.NaN()}); !errors.Is(err, workflowruntime.ErrInvalidFanOut) {
		t.Fatalf("NaN tolerance error = %v", err)
	}
}

func TestCompleteFanOutCannotMutateTerminalRun(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	base := time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("fanout-terminal-run")
	parent := invocationID(runID, "bulk")
	createRun(t, store, runID, base)
	createNode(t, store, parent, workflowruntime.NodePending, 0, base)
	expanded, err := (workflowruntime.FanOutCoordinator{Store: store}).Expand(ctx, workflowruntime.FanOutExpandCommand{
		Parent: parent, ExpectedParentGeneration: 1, Spec: graph.ForEachSpec{Items: graph.Expression{Text: `[1]`}}, At: base.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	run, _ := store.LoadRun(ctx, runID)
	running, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunSucceeded, At: base.Add(3 * time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	outputs, err := store.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "fanout-test", RunID: runID}, Values: values.ValueSet{}})
	if err != nil {
		t.Fatal(err)
	}
	beforeFanOut, _ := store.LoadFanOut(ctx, parent)
	beforeParent, _ := store.LoadNodeInvocation(ctx, parent)
	beforeEvents, _ := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	failure := workflowruntime.Failure{Code: "canceled", Message: "fan-out canceled"}
	_, completeErr := store.CompleteFanOut(ctx, workflowruntime.CompleteFanOutRequest{
		Parent: parent, ExpectedParentGeneration: expanded.Parent.Generation, ExpectedFanOutGeneration: expanded.FanOut.Generation,
		ExpectedChildGenerations: map[workflowruntime.NodeInvocationID]uint64{expanded.Children[0].ID: expanded.Children[0].Generation},
		Status:                   workflowruntime.FanOutCanceled, Outputs: outputs, Failure: &failure, At: base.Add(4 * time.Second),
	})
	if !errors.Is(completeErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("CompleteFanOut(terminal run) error = %v", completeErr)
	}
	afterFanOut, _ := store.LoadFanOut(ctx, parent)
	afterParent, _ := store.LoadNodeInvocation(ctx, parent)
	afterEvents, _ := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if afterFanOut.Generation != beforeFanOut.Generation || afterParent.Generation != beforeParent.Generation || len(afterEvents) != len(beforeEvents) {
		t.Fatalf("rejected fan-out completion mutated state fanout=%#v/%#v parent=%#v/%#v events=%d/%d", beforeFanOut, afterFanOut, beforeParent, afterParent, len(beforeEvents), len(afterEvents))
	}
}
