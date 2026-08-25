package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestRetryActivationClosesAttemptReleasesClaimAndSurvivesRecovery(t *testing.T) {
	ctx := context.Background()
	store := inmemory.NewStore()
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
	store := inmemory.NewStore()
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
	store := inmemory.NewStore()
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
	store := inmemory.NewStore()
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
	failure := workflowruntime.Failure{Code: "item_timed_out", Message: "item timed out", Details: map[string]string{"timeout_kind": string(workflowruntime.TimeoutExecution)}}
	finished2, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: second.ID, AttemptNumber: 1, ExpectedNodeGeneration: started2.Node.Generation, ExpectedAttemptGeneration: started2.Attempt.Generation, Claim: claim2, AttemptStatus: workflowruntime.NodeTimedOut, NextNodeStatus: workflowruntime.NodeTimedOut, Failure: &failure, At: base.Add(12 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if finished1.Node.Status != workflowruntime.NodeSucceeded || finished2.Node.Status != workflowruntime.NodeTimedOut {
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
	expressionContext, err := workflowruntime.BuildExpressionContext(ctx, store, nil, graph.Graph{Nodes: []graph.Node{{ID: parent.NodeID, ForEach: &graph.ForEachSpec{Items: graph.Expression{Text: `["a", "b"]`}}}}}, runID)
	itemsContext := expressionContext.Steps[parent.NodeID].Items
	if err != nil || len(itemsContext) != 2 || itemsContext[0].Outputs["token"].Type != values.TypeSecretRef || itemsContext[1].Error == nil {
		t.Fatalf("fan-out expression context = %#v, %v", expressionContext, err)
	}
	errorPayload, ok := itemsContext[1].Error.Inline.(map[string]any)
	attemptNumber, numberOK := errorPayload["attempt"].(json.Number)
	if !ok || !numberOK || attemptNumber.String() != "1" || errorPayload["code"] != "item_timed_out" || errorPayload["timeout_kind"] != string(workflowruntime.TimeoutExecution) {
		t.Fatalf("fan-out typed item error = %#v", itemsContext[1].Error)
	}
	if rendered := fmt.Sprint(aggregate["items"].Inline); strings.Contains(rendered, string(secretRef)) {
		t.Fatalf("aggregate metadata leaked secret reference: %s", rendered)
	}
}

func TestFanOutExpandPersistsSyntheticAndEvaluatedNodeInputs(t *testing.T) {
	ctx := context.Background()
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("run-fanout-input-bindings")
	parent := invocationID(runID, "bulk")
	createRun(t, store, runID, base)
	createNode(t, store, parent, workflowruntime.NodePending, 0, base)
	project, err := values.NewInline("project-7", values.Metadata{Producer: values.Producer{Kind: "test", Reference: "run", Output: "project"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	bindings := map[string]graph.Binding{
		"project-id": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.project"}},
		"title":      {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "item.title"}},
		"ordinal":    {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "index"}},
	}
	expanded, err := (workflowruntime.FanOutCoordinator{Store: store}).Expand(ctx, workflowruntime.FanOutExpandCommand{
		Parent: parent, ExpectedParentGeneration: 1,
		Spec:              graph.ForEachSpec{Items: graph.Expression{Text: `[{"title":"one"},{"title":"two"}]`}},
		InputBindings:     bindings,
		ExpressionContext: values.ExpressionContext{Inputs: values.ValueSet{"project": project}},
		At:                base.Add(time.Second),
	})
	if err != nil || len(expanded.Children) != 2 {
		t.Fatalf("Expand = %#v, %v", expanded, err)
	}
	for index, child := range expanded.Children {
		if child.Status != workflowruntime.NodeReady || child.Inputs == nil {
			t.Fatalf("child %d = %#v", index, child)
		}
		inputs, loadErr := store.LoadValues(ctx, *child.Inputs)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if len(inputs) != 5 || inputs["title"].Inline != []string{"one", "two"}[index] || inputs["project-id"].Inline != "project-7" {
			t.Fatalf("child %d inputs = %#v", index, inputs)
		}
		if got := inputs["ordinal"].Inline.(json.Number).String(); got != fmt.Sprint(index) {
			t.Fatalf("child %d ordinal = %s", index, got)
		}
		if inputs["project-id"].Digest != project.Digest || inputs["project-id"].Producer != project.Producer {
			t.Fatalf("exact workflow-input passthrough did not preserve envelope: %#v", inputs["project-id"])
		}
	}

	collisionRun := workflowruntime.RunID("run-fanout-input-collision")
	collisionParent := invocationID(collisionRun, "bulk")
	createRun(t, store, collisionRun, base)
	createNode(t, store, collisionParent, workflowruntime.NodePending, 0, base)
	_, err = (workflowruntime.FanOutCoordinator{Store: store}).Expand(ctx, workflowruntime.FanOutExpandCommand{
		Parent: collisionParent, ExpectedParentGeneration: 1,
		Spec:          graph.ForEachSpec{Items: graph.Expression{Text: `[1]`}},
		InputBindings: map[string]graph.Binding{"item": {Kind: graph.BindingLiteral, Literal: "collision"}},
		At:            base.Add(time.Second),
	})
	if !errors.Is(err, workflowruntime.ErrInvalidFanOut) || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("collision error = %v", err)
	}
	collisionNode, loadErr := store.LoadNodeInvocation(ctx, collisionParent)
	if loadErr != nil || collisionNode.Status != workflowruntime.NodePending || collisionNode.Generation != 1 {
		t.Fatalf("collision mutated parent = %#v, %v", collisionNode, loadErr)
	}
}

func TestFanOutFailFastFencesAndCancelsUnstartedItems(t *testing.T) {
	ctx := context.Background()
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 16, 30, 0, 0, time.UTC)
	runID := workflowruntime.RunID("run-fanout-fail-fast")
	parent := invocationID(runID, "matrix")
	createRun(t, store, runID, base)
	createNode(t, store, parent, workflowruntime.NodePending, 0, base)
	coordinator := workflowruntime.FanOutCoordinator{Store: store}
	expanded, err := coordinator.Expand(ctx, workflowruntime.FanOutExpandCommand{
		Parent: parent, ExpectedParentGeneration: 1,
		Spec: graph.ForEachSpec{Items: graph.Expression{Text: `[1, 2, 3]`}, MaxConcurrency: 1, FailFast: true},
		At:   base.Add(time.Second),
	})
	if err != nil || !expanded.FanOut.FailFast || len(expanded.Children) != 3 {
		t.Fatalf("Expand = %#v, %v", expanded, err)
	}
	first := expanded.Children[0]
	claim := claimNode(t, store, first.ID, 0, "worker", "first", "fail-fast-first", base.Add(2*time.Second), base.Add(time.Minute))
	claimed, _ := store.LoadNodeInvocation(ctx, first.ID)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: first.ID, ExpectedNodeGeneration: claimed.Generation, Claim: claim,
		Executor: testExecutor(), Inputs: first.Inputs, At: base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	failure := workflowruntime.Failure{Code: "matrix_failed", Message: "matrix item failed"}
	if _, finishErr := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: first.ID, AttemptNumber: started.Attempt.ID.Number,
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: claim, AttemptStatus: workflowruntime.NodeFailed, NextNodeStatus: workflowruntime.NodeFailed,
		Failure: &failure, At: base.Add(4 * time.Second),
	}); finishErr != nil {
		t.Fatal(finishErr)
	}
	second := expanded.Children[1]
	denied, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: second.ID, ExpectedClaimGeneration: 0, Owner: "worker", Token: "second",
		IdempotencyKey: "fail-fast-second", Now: base.Add(5 * time.Second), LeaseUntil: base.Add(time.Minute),
	})
	if err != nil || denied.Acquired {
		t.Fatalf("claim after fail-fast failure = %#v, %v", denied, err)
	}
	registry := recoveryRegistry(t, graph.EffectSet{graph.EffectCompute}, graph.IdempotencyIntrinsic, stepkind.RetrySafe)
	recovery := workflowruntime.RecoveryCoordinator{
		Store: store, Recovery: store, Inputs: store, Control: store,
		Plans:    staticRecoveryPlans{graph: graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{{ID: "matrix", Kind: "safe", KindVersion: "v1", ForEach: &graph.ForEachSpec{Items: graph.Expression{Text: `[1,2,3]`}, FailFast: true}}}}},
		Registry: registry,
	}
	recovered, err := recovery.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(6 * time.Second)})
	if err != nil || len(recovered.FanOutCanceled) != 2 || len(recovered.FanOuts) != 1 || recovered.FanOuts[0].FanOut.Status != workflowruntime.FanOutFailed {
		t.Fatalf("Recover fail-fast = %#v, %v", recovered, err)
	}
	for _, item := range expanded.Children[1:] {
		node, loadErr := store.LoadNodeInvocation(ctx, item.ID)
		if loadErr != nil || node.Status != workflowruntime.NodeCanceled || node.LatestAttempt != 0 {
			t.Fatalf("unstarted item = %#v, %v", node, loadErr)
		}
	}
	if replay, replayErr := recovery.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(7 * time.Second)}); replayErr != nil || len(replay.FanOutCanceled) != 0 || len(replay.FanOuts) != 0 {
		t.Fatalf("Recover fail-fast replay = %#v, %v", replay, replayErr)
	}
	completed, err := store.LoadFanOut(ctx, parent)
	items, itemsErr := store.LoadFanOutItemResults(ctx, parent)
	if err != nil || itemsErr != nil || completed.Status != workflowruntime.FanOutFailed || len(items) != 3 {
		t.Fatalf("durable fan-out = %#v items=%#v, %v/%v", completed, items, err, itemsErr)
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
	store := inmemory.NewStore()
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
