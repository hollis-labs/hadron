package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

func TestCancellationCoordinatorStopsRunningContextAttempt(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "cancel-running-context")
	proof := readyClaimProof(claim)
	started, err := store.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{
		InvocationID: node.ID, ExpectedNodeGeneration: node.Generation, Claim: proof,
		Executor: testExecutor(), Inputs: node.Inputs, At: base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := stepkind.NewRegistry()
	if registerErr := registry.Register(stepkindtest.NewNoopKind("test", "v1")); registerErr != nil {
		t.Fatal(registerErr)
	}
	canceled := 0
	coordinator := workflowruntime.CancellationCoordinator{
		Store: store, Registry: registry,
		Attempts: attemptCancelerFunc(func(_ context.Context, attempt workflowruntime.AttemptSnapshot) error {
			canceled++
			if attempt.ID != started.Attempt.ID {
				t.Fatalf("CancelAttempt() attempt = %#v", attempt.ID)
			}
			return nil
		}),
		Now: func() time.Time { return base.Add(5 * time.Second) },
	}
	run, _ := store.LoadRun(context.Background(), node.ID.RunID)
	result, intentErrors, err := coordinator.Request(context.Background(), cancellationRequest(run, "cancel-running", base.Add(4*time.Second)))
	if err != nil || len(intentErrors) != 0 || canceled != 1 || result.Run.Status != workflowruntime.RunCanceled || len(result.Intents) != 1 {
		t.Fatalf("Request() = %#v errors=%v err=%v canceled=%d", result, intentErrors, err, canceled)
	}
	attempt, _ := store.LoadAttempt(context.Background(), started.Attempt.ID)
	loadedNode, _ := store.LoadNodeInvocation(context.Background(), node.ID)
	events, _ := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
	pending, recoverErrors, recoverErr := coordinator.Recover(context.Background(), workflowruntime.CancellationIntentQuery{RunID: run.ID})
	if attempt.Status != workflowruntime.NodeCanceled || loadedNode.Status != workflowruntime.NodeCanceled || loadedNode.Lease != nil || !eventTypesContain(events, workflowruntime.EventNodeAttemptFinished, workflowruntime.EventNodeStatusChanged, workflowruntime.EventCancellationResolved) || len(pending) != 0 || len(recoverErrors) != 0 || recoverErr != nil {
		t.Fatalf("resolved running cancellation attempt=%#v node=%#v pending=%#v errors=%v/%v", attempt, loadedNode, pending, recoverErrors, recoverErr)
	}
}

func TestRunCancellationCommitFencesLateAttemptCompletion(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "cancel-fences-late-finish")
	proof := readyClaimProof(claim)
	started, err := store.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{
		InvocationID: node.ID, ExpectedNodeGeneration: node.Generation, Claim: proof,
		Executor: testExecutor(), Inputs: node.Inputs, At: base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	run, _ := store.LoadRun(context.Background(), node.ID.RunID)
	if _, err := store.RequestRunCancellation(context.Background(), cancellationRequest(run, "cancel-before-finish", base.Add(4*time.Second))); err != nil {
		t.Fatal(err)
	}
	beforeNode, _ := store.LoadNodeInvocation(context.Background(), node.ID)
	beforeAttempt, _ := store.LoadAttempt(context.Background(), started.Attempt.ID)
	beforeEvents, _ := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
	_, finishErr := store.FinishNodeAttempt(context.Background(), workflowruntime.FinishNodeAttemptRequest{
		InvocationID: node.ID, AttemptNumber: started.Attempt.ID.Number,
		ExpectedNodeGeneration: beforeNode.Generation, ExpectedAttemptGeneration: beforeAttempt.Generation,
		Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: base.Add(5 * time.Second),
	})
	if !errors.Is(finishErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("late FinishNodeAttempt() error = %v", finishErr)
	}
	afterNode, _ := store.LoadNodeInvocation(context.Background(), node.ID)
	afterAttempt, _ := store.LoadAttempt(context.Background(), started.Attempt.ID)
	afterEvents, _ := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
	if afterNode.Generation != beforeNode.Generation || afterNode.Status != beforeNode.Status || afterAttempt.Generation != beforeAttempt.Generation || afterAttempt.Status != beforeAttempt.Status || len(afterEvents) != len(beforeEvents) {
		t.Fatalf("rejected late completion mutated state node=%#v/%#v attempt=%#v/%#v events=%d/%d", beforeNode, afterNode, beforeAttempt, afterAttempt, len(beforeEvents), len(afterEvents))
	}
}

func TestCancellationCoordinatorExplicitExternalAndUnsupportedRecovery(t *testing.T) {
	t.Run("explicit", func(t *testing.T) {
		fixture := dispatchExternalOperation(t, "cancel-external-explicit")
		cancels := 0
		fixture.kind.CancelFunc = func(context.Context, stepkind.ExternalOperationRef) error {
			cancels++
			return nil
		}
		fixture.kind.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
			return stepkind.Observation{State: stepkind.ObservationCanceled, Failure: &stepkind.ExecutionError{Code: "remote_canceled", Message: "remote operation canceled", Classification: stepkind.RetryPermanent}}, nil
		}
		run, _ := fixture.store.LoadRun(context.Background(), fixture.attempt.Invocation.RunID)
		coordinator := workflowruntime.CancellationCoordinator{
			Store: fixture.store, Registry: fixture.registry, External: fixture.coordinator(t),
			Now: func() time.Time { return fixture.now.Add(time.Second) },
		}
		result, intentErrors, err := coordinator.Request(context.Background(), cancellationRequest(run, "cancel-external", fixture.now))
		if err != nil || len(intentErrors) != 0 || len(result.Intents) != 1 || cancels != 1 || *fixture.executions != 1 {
			t.Fatalf("Request(explicit) = %#v errors=%v err=%v cancels=%d executions=%d", result, intentErrors, err, cancels, *fixture.executions)
		}
		operation, _ := fixture.store.LoadExternalOperation(context.Background(), fixture.attempt)
		node, _ := fixture.store.LoadNodeInvocation(context.Background(), fixture.attempt.Invocation)
		pending, _ := fixture.store.RecoverCancellationIntents(context.Background(), workflowruntime.CancellationIntentQuery{RunID: run.ID})
		if operation.Status != stepkind.ObservationCanceled || node.Status != workflowruntime.NodeCanceled || len(pending) != 0 {
			t.Fatalf("explicit cancellation operation=%#v node=%#v pending=%#v", operation, node, pending)
		}
	})

	t.Run("unsupported remains recoverable", func(t *testing.T) {
		fixture := dispatchExternalOperation(t, "cancel-external-unsupported")
		registry := stepkind.NewRegistry()
		unsupported := stepkindtest.NewNoopKind("external-kind", "v1")
		unsupported.SpecValue.Cancellation.Mode = stepkind.CancellationNone
		if err := registry.Register(unsupported); err != nil {
			t.Fatal(err)
		}
		run, _ := fixture.store.LoadRun(context.Background(), fixture.attempt.Invocation.RunID)
		coordinator := workflowruntime.CancellationCoordinator{Store: fixture.store, Registry: registry, Now: func() time.Time { return fixture.now.Add(time.Second) }}
		result, intentErrors, err := coordinator.Request(context.Background(), cancellationRequest(run, "cancel-unsupported", fixture.now))
		if err != nil || len(result.Intents) != 1 || len(intentErrors) != 1 || !errors.Is(intentErrors[0], workflowruntime.ErrCancellationUnsupported) {
			t.Fatalf("Request(unsupported) = %#v errors=%v err=%v", result, intentErrors, err)
		}
		pending, replayErrors, recoverErr := coordinator.Recover(context.Background(), workflowruntime.CancellationIntentQuery{RunID: run.ID})
		operation, _ := fixture.store.LoadExternalOperation(context.Background(), fixture.attempt)
		if recoverErr != nil || len(pending) != 1 || len(replayErrors) != 1 || !errors.Is(replayErrors[0], workflowruntime.ErrCancellationUnsupported) || operation.Status != stepkind.ObservationPending || operation.CancelRequestedAt.IsZero() || *fixture.executions != 1 {
			t.Fatalf("Recover(unsupported) pending=%#v errors=%v err=%v operation=%#v executions=%d", pending, replayErrors, recoverErr, operation, *fixture.executions)
		}
		node, _ := fixture.store.LoadNodeInvocation(context.Background(), fixture.attempt.Invocation)
		attempt, _ := fixture.store.LoadAttempt(context.Background(), fixture.attempt)
		beforeEvents, _ := fixture.store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
		_, pendingErr := fixture.store.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
			Attempt: fixture.attempt, ExpectedOperationGeneration: operation.Generation,
			ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation,
			Status: stepkind.ObservationPending, Progress: map[string]string{"state": "canceling"},
			ObservedAt: fixture.now.Add(2 * time.Second), At: fixture.now.Add(2 * time.Second),
		})
		if !errors.Is(pendingErr, workflowruntime.ErrInvalidRecord) {
			t.Fatalf("terminal canceled run allowed pending external mutation: %v", pendingErr)
		}
		failure := workflowruntime.Failure{Code: "remote_failed", Message: "remote operation failed", Retryable: true}
		for index, next := range []workflowruntime.NodeStatus{workflowruntime.NodeReady, workflowruntime.NodeFailed} {
			_, applyErr := fixture.store.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
				Attempt: fixture.attempt, ExpectedOperationGeneration: operation.Generation,
				ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation,
				Status: stepkind.ObservationFailed, Failure: &failure, NextNodeStatus: next,
				ObservedAt: fixture.now.Add(time.Duration(index+2) * time.Second), At: fixture.now.Add(time.Duration(index+2) * time.Second),
			})
			if !errors.Is(applyErr, workflowruntime.ErrInvalidRecord) {
				t.Fatalf("terminal canceled run allowed external outcome %s: %v", next, applyErr)
			}
		}
		afterOperation, _ := fixture.store.LoadExternalOperation(context.Background(), fixture.attempt)
		afterNode, _ := fixture.store.LoadNodeInvocation(context.Background(), fixture.attempt.Invocation)
		afterAttempt, _ := fixture.store.LoadAttempt(context.Background(), fixture.attempt)
		afterEvents, _ := fixture.store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
		if afterOperation.Generation != operation.Generation || afterNode.Generation != node.Generation || afterAttempt.Generation != attempt.Generation || len(afterEvents) != len(beforeEvents) {
			t.Fatalf("rejected external completion mutated state operation=%#v/%#v node=%#v/%#v attempt=%#v/%#v events=%d/%d", operation, afterOperation, node, afterNode, attempt, afterAttempt, len(beforeEvents), len(afterEvents))
		}
	})

	t.Run("canceled observation cannot mutate succeeded run", func(t *testing.T) {
		fixture := dispatchExternalOperation(t, "cancel-external-succeeded-run")
		run, _ := fixture.store.LoadRun(context.Background(), fixture.attempt.Invocation.RunID)
		running, err := fixture.store.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: fixture.now})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunSucceeded, At: fixture.now.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
		operation, _ := fixture.store.LoadExternalOperation(context.Background(), fixture.attempt)
		node, _ := fixture.store.LoadNodeInvocation(context.Background(), fixture.attempt.Invocation)
		attempt, _ := fixture.store.LoadAttempt(context.Background(), fixture.attempt)
		beforeEvents, _ := fixture.store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
		failure := workflowruntime.Failure{Code: "remote_canceled", Message: "remote operation canceled"}
		_, applyErr := fixture.store.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
			Attempt: fixture.attempt, ExpectedOperationGeneration: operation.Generation,
			ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation,
			Status: stepkind.ObservationCanceled, Failure: &failure, NextNodeStatus: workflowruntime.NodeCanceled,
			ObservedAt: fixture.now.Add(2 * time.Second), At: fixture.now.Add(2 * time.Second),
		})
		if !errors.Is(applyErr, workflowruntime.ErrInvalidRecord) {
			t.Fatalf("succeeded run accepted canceled external completion: %v", applyErr)
		}
		afterOperation, _ := fixture.store.LoadExternalOperation(context.Background(), fixture.attempt)
		afterEvents, _ := fixture.store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
		if afterOperation.Generation != operation.Generation || len(afterEvents) != len(beforeEvents) {
			t.Fatalf("rejected canceled completion mutated state: %#v/%#v events=%d/%d", operation, afterOperation, len(beforeEvents), len(afterEvents))
		}
	})
}

func TestCancellationCoordinatorPropagatesDirectAndRequestCancelChildren(t *testing.T) {
	store := runtimetest.NewStore()
	base := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
	parent, directChild := workflowruntime.RunID("parent"), workflowruntime.RunID("direct-child")
	requestedChild, terminalChild := workflowruntime.RunID("requested-child"), workflowruntime.RunID("terminal-child")
	for _, runID := range []workflowruntime.RunID{parent, directChild, requestedChild, terminalChild} {
		createRun(t, store, runID, base)
	}
	parentDirect := invocationID(parent, "call-direct")
	parentRequested := invocationID(parent, "call-request")
	parentTerminal := invocationID(parent, "call-terminal")
	directNode := invocationID(directChild, "work")
	requestedNode := invocationID(requestedChild, "work")
	terminalNode := invocationID(terminalChild, "work")
	for _, id := range []workflowruntime.NodeInvocationID{parentDirect, parentRequested, parentTerminal, directNode, requestedNode, terminalNode} {
		if _, err := store.CreateNodeInvocation(context.Background(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: id, Status: workflowruntime.NodePending, CreatedAt: base, UpdatedAt: base}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, link := range []workflowruntime.ChildRunLink{
		{ParentRunID: parent, Invocation: parentDirect, ChildRunID: directChild, Policy: graph.ParentCloseCancel, CreatedAt: base},
		{ParentRunID: parent, Invocation: parentRequested, ChildRunID: requestedChild, Policy: graph.ParentCloseRequestCancel, CreatedAt: base},
		{ParentRunID: parent, Invocation: parentTerminal, ChildRunID: terminalChild, Policy: graph.ParentCloseCancel, CreatedAt: base},
	} {
		if err := store.RecordChildRun(context.Background(), link); err != nil {
			t.Fatal(err)
		}
	}
	terminalNodeState, _ := store.LoadNodeInvocation(context.Background(), terminalNode)
	if _, err := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: terminalNode, ExpectedGeneration: terminalNodeState.Generation, To: workflowruntime.NodeSkipped, Explanation: &workflowruntime.BlockedReason{Code: "already_done", Message: "child completed before parent cancellation"}, At: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	terminalRun, _ := store.LoadRun(context.Background(), terminalChild)
	runningTerminal, err := store.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: terminalChild, ExpectedGeneration: terminalRun.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := store.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: terminalChild, ExpectedGeneration: runningTerminal.Snapshot.Generation, To: workflowruntime.RunSucceeded, At: base.Add(2 * time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	parentRun, _ := store.LoadRun(context.Background(), parent)
	result, err := store.RequestRunCancellation(context.Background(), cancellationRequest(parentRun, "cancel-parent", base.Add(3*time.Second)))
	if err != nil || result.Run.Status != workflowruntime.RunCanceled {
		t.Fatalf("RequestRunCancellation() = %#v, %v", result, err)
	}
	directRun, _ := store.LoadRun(context.Background(), directChild)
	directState, _ := store.LoadNodeInvocation(context.Background(), directNode)
	requestedRun, _ := store.LoadRun(context.Background(), requestedChild)
	terminalRun, _ = store.LoadRun(context.Background(), terminalChild)
	terminalState, _ := store.LoadNodeInvocation(context.Background(), terminalNode)
	if directRun.Status != workflowruntime.RunCanceled || directState.Status != workflowruntime.NodeCanceled || requestedRun.Status != workflowruntime.RunPending || terminalRun.Status != workflowruntime.RunSucceeded || terminalState.Status != workflowruntime.NodeSkipped {
		t.Fatalf("child states direct=%#v/%#v requested=%#v terminal=%#v/%#v", directRun, directState, requestedRun, terminalRun, terminalState)
	}
	requested := 0
	coordinator := workflowruntime.CancellationCoordinator{
		Store: store, Registry: stepkind.NewRegistry(),
		Children: childRunCancelerFunc(func(_ context.Context, link workflowruntime.ChildRunLink) error {
			requested++
			if link.ChildRunID != requestedChild || link.Policy != graph.ParentCloseRequestCancel {
				t.Fatalf("RequestChildRunCancellation() = %#v", link)
			}
			return nil
		}),
		Now: func() time.Time { return base.Add(4 * time.Second) },
	}
	pending, intentErrors, err := coordinator.Recover(context.Background(), workflowruntime.CancellationIntentQuery{RunID: parent})
	if err != nil || len(intentErrors) != 0 || len(pending) != 1 || pending[0].Kind != workflowruntime.CancellationChildRun || requested != 1 {
		t.Fatalf("Recover(child request) = %#v errors=%v err=%v requested=%d", pending, intentErrors, err, requested)
	}
	remaining, _ := store.RecoverCancellationIntents(context.Background(), workflowruntime.CancellationIntentQuery{RunID: parent})
	if len(remaining) != 0 {
		t.Fatalf("resolved request_cancel intent remained pending: %#v", remaining)
	}
}

func cancellationRequest(run workflowruntime.RunSnapshot, key string, at time.Time) workflowruntime.RequestRunCancellationRequest {
	return workflowruntime.RequestRunCancellationRequest{
		RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: key,
		Reason: workflowruntime.Failure{Code: "user_canceled", Message: "run canceled by user"}, At: at,
	}
}

func readyClaimProof(claim workflowruntime.ReadyClaim) workflowruntime.ClaimProof {
	return workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
}

type attemptCancelerFunc func(context.Context, workflowruntime.AttemptSnapshot) error

func (f attemptCancelerFunc) CancelAttempt(ctx context.Context, attempt workflowruntime.AttemptSnapshot) error {
	return f(ctx, attempt)
}

type childRunCancelerFunc func(context.Context, workflowruntime.ChildRunLink) error

func (f childRunCancelerFunc) RequestChildRunCancellation(ctx context.Context, link workflowruntime.ChildRunLink) error {
	return f(ctx, link)
}

func eventTypesContain(events []workflowruntime.Event, types ...string) bool {
	seen := make(map[string]bool, len(events))
	for _, event := range events {
		seen[event.Type] = true
	}
	for _, eventType := range types {
		if !seen[eventType] {
			return false
		}
	}
	return true
}
