package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestWorkflowSQLiteExternalOperationReopenFidelityAndImmutableBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "external-reopen.db")
	store, state := openWorkflowStateTest(t, path)
	fixture := prepareWorkflowSQLiteExternal(t, state, "reopen", workflowTestTime())
	if !fixture.operation.LastObservedAt.IsZero() || fixture.node.Status != workflowruntime.NodeWaiting || fixture.node.Lease != nil {
		t.Fatalf("suspended external operation = %#v / %#v", fixture.operation, fixture.node)
	}
	large, ok := fixture.operation.Invocation.Config["large"].(json.Number)
	if !ok || large.String() != "9007199254740993" {
		t.Fatalf("suspended large config number = %#v (%T)", fixture.operation.Invocation.Config["large"], fixture.operation.Invocation.Config["large"])
	}
	if _, err := store.DB().Exec(`UPDATE workflow_external_operations SET ref_json = '{}' WHERE run_id = ? AND node_id = ? AND iteration = ? AND attempt_number = ?`,
		fixture.attempt.ID.Invocation.RunID, fixture.attempt.ID.Invocation.NodeID, fixture.attempt.ID.Invocation.Iteration, fixture.attempt.ID.Number); err == nil {
		t.Fatal("immutable external operation trigger accepted ref update")
	}
	if _, err := store.DB().Exec(`DELETE FROM workflow_external_operations WHERE run_id = ? AND node_id = ? AND iteration = ? AND attempt_number = ?`,
		fixture.attempt.ID.Invocation.RunID, fixture.attempt.ID.Invocation.NodeID, fixture.attempt.ID.Invocation.Iteration, fixture.attempt.ID.Number); err == nil {
		t.Fatal("durable external operation trigger accepted delete")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
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
	loaded, err := reopened.LoadExternalOperation(context.Background(), fixture.attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	large, ok = loaded.Invocation.Config["large"].(json.Number)
	if !ok || large.String() != "9007199254740993" || loaded.Ref.ID != fixture.operation.Ref.ID || loaded.Generation != fixture.operation.Generation {
		t.Fatalf("reopened external operation = %#v", loaded)
	}
	requested, err := reopened.RequestExternalOperationCancel(context.Background(), workflowruntime.RequestExternalOperationCancelRequest{
		Attempt: loaded.Attempt, ExpectedOperationGeneration: loaded.Generation, At: fixture.base.Add(4 * time.Second),
	})
	if err != nil || requested.Event == nil {
		t.Fatalf("RequestExternalOperationCancel() = %#v, %v", requested, err)
	}
	replayed, err := reopened.RequestExternalOperationCancel(context.Background(), workflowruntime.RequestExternalOperationCancelRequest{
		Attempt: loaded.Attempt, ExpectedOperationGeneration: requested.Operation.Generation, At: fixture.base.Add(9 * time.Second),
	})
	if err != nil || replayed.Event != nil || replayed.Operation.Generation != requested.Operation.Generation ||
		!replayed.Operation.CancelRequestedAt.Equal(requested.Operation.CancelRequestedAt) {
		t.Fatalf("cancel-intent restart replay = %#v, %v", replayed, err)
	}

	outputRef, err := reopened.SaveValues(context.Background(), workflowruntime.SaveValuesRequest{
		Owner:  workflowruntime.ValueOwner{Kind: "external-output", RunID: loaded.Attempt.Invocation.RunID, Invocation: &loaded.Attempt.Invocation, Attempt: &loaded.Attempt},
		Values: workflowTestValues(t, "complete"),
	})
	if err != nil {
		t.Fatal(err)
	}
	finished, err := reopened.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
		Attempt: loaded.Attempt, ExpectedOperationGeneration: replayed.Operation.Generation,
		ExpectedNodeGeneration: fixture.node.Generation, ExpectedAttemptGeneration: fixture.attempt.Generation,
		Status: stepkind.ObservationSucceeded, Outputs: &outputRef, NextNodeStatus: workflowruntime.NodeSucceeded,
		ObservedAt: fixture.base.Add(10 * time.Second), At: fixture.base.Add(10 * time.Second),
	})
	if err != nil || finished.Operation.Status != stepkind.ObservationSucceeded || finished.Node.Status != workflowruntime.NodeSucceeded ||
		finished.Attempt.Status != workflowruntime.NodeSucceeded || len(finished.Events) != 3 {
		t.Fatalf("ApplyExternalOperation(terminal) = %#v, %v", finished, err)
	}
}

func TestWorkflowSQLiteExternalOperationRollbackAndContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "external-contention.db")
	store, first := openWorkflowStateTest(t, path)
	fixture := prepareWorkflowSQLiteExternal(t, first, "contention", workflowTestTime())
	seeded, err := first.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
		Attempt: fixture.attempt.ID, ExpectedOperationGeneration: fixture.operation.Generation,
		ExpectedNodeGeneration: fixture.node.Generation, ExpectedAttemptGeneration: fixture.attempt.Generation,
		Status: stepkind.ObservationPending, Progress: map[string]string{"phase": "seed"},
		ObservedAt: fixture.base.Add(4 * time.Second), HeartbeatAt: fixture.base.Add(3500 * time.Millisecond), At: fixture.base.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = first.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
		Attempt: fixture.attempt.ID, ExpectedOperationGeneration: seeded.Operation.Generation,
		ExpectedNodeGeneration: fixture.node.Generation, ExpectedAttemptGeneration: fixture.attempt.Generation,
		Status: stepkind.ObservationPending, Progress: map[string]string{"phase": "regressed"},
		ObservedAt: fixture.base.Add(5 * time.Second), HeartbeatAt: fixture.base.Add(3400 * time.Millisecond), At: fixture.base.Add(5 * time.Second),
	})
	if !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("regressing SQLite heartbeat error = %v", err)
	}
	fixture.operation = seeded.Operation
	if _, execErr := store.DB().Exec(`
CREATE TRIGGER test_reject_external_observation
BEFORE INSERT ON workflow_events
WHEN NEW.event_type = 'external_operation.observed'
BEGIN SELECT RAISE(ABORT, 'reject external observation'); END`); execErr != nil {
		t.Fatal(execErr)
	}
	_, err = first.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
		Attempt: fixture.attempt.ID, ExpectedOperationGeneration: fixture.operation.Generation,
		ExpectedNodeGeneration: fixture.node.Generation, ExpectedAttemptGeneration: fixture.attempt.Generation,
		Status: stepkind.ObservationPending, Progress: map[string]string{"winner": "none"}, ObservedAt: fixture.base.Add(5 * time.Second), At: fixture.base.Add(5 * time.Second),
	})
	if err == nil {
		t.Fatal("event trigger did not reject external observation")
	}
	afterRollback, err := first.LoadExternalOperation(context.Background(), fixture.attempt.ID)
	if err != nil || afterRollback.Generation != fixture.operation.Generation || afterRollback.Progress["phase"] != "seed" ||
		!afterRollback.LastObservedAt.Equal(fixture.operation.LastObservedAt) || !afterRollback.LastHeartbeatAt.Equal(fixture.operation.LastHeartbeatAt) {
		t.Fatalf("failed transaction mutated operation: %#v, %v", afterRollback, err)
	}
	if _, execErr := store.DB().Exec(`DROP TRIGGER test_reject_external_observation`); execErr != nil {
		t.Fatal(execErr)
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
	stores := []*WorkflowStateStore{first, second}
	const contenders = 12
	var wg sync.WaitGroup
	results := make(chan error, contenders)
	for index := 0; index < contenders; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := stores[index%len(stores)].ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
				Attempt: fixture.attempt.ID, ExpectedOperationGeneration: fixture.operation.Generation,
				ExpectedNodeGeneration: fixture.node.Generation, ExpectedAttemptGeneration: fixture.attempt.Generation,
				Status: stepkind.ObservationPending, Progress: map[string]string{"winner": string(rune('a' + index))}, ObservedAt: fixture.base.Add(5 * time.Second), At: fixture.base.Add(5 * time.Second),
			})
			results <- err
		}(index)
	}
	wg.Wait()
	close(results)
	succeeded, stale := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, workflowruntime.ErrCASMismatch):
			stale++
		default:
			t.Fatalf("contending ApplyExternalOperation() error = %v", err)
		}
	}
	if succeeded != 1 || stale != contenders-1 {
		t.Fatalf("external contention succeeded=%d stale=%d", succeeded, stale)
	}
}

func TestWorkflowSQLiteTerminalRunFencesExternalMutation(t *testing.T) {
	t.Run("pending after canceled", func(t *testing.T) {
		_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "external-canceled-run.db"))
		fixture := prepareWorkflowSQLiteExternal(t, state, "canceled-run", workflowTestTime())
		run, _ := state.LoadRun(context.Background(), fixture.attempt.ID.Invocation.RunID)
		_, err := state.RequestRunCancellation(context.Background(), workflowruntime.RequestRunCancellationRequest{
			RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "cancel-external-run",
			Reason: workflowruntime.Failure{Code: "user_canceled", Message: "run canceled by user"}, At: fixture.base.Add(4 * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
		operation, _ := state.LoadExternalOperation(context.Background(), fixture.attempt.ID)
		node, _ := state.LoadNodeInvocation(context.Background(), fixture.node.ID)
		attempt, _ := state.LoadAttempt(context.Background(), fixture.attempt.ID)
		beforeEvents, _ := state.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
		_, applyErr := state.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
			Attempt: fixture.attempt.ID, ExpectedOperationGeneration: operation.Generation,
			ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation,
			Status: stepkind.ObservationPending, Progress: map[string]string{"state": "canceling"},
			ObservedAt: fixture.base.Add(5 * time.Second), At: fixture.base.Add(5 * time.Second),
		})
		if !errors.Is(applyErr, workflowruntime.ErrInvalidRecord) {
			t.Fatalf("pending mutation after cancellation error = %v", applyErr)
		}
		after, _ := state.LoadExternalOperation(context.Background(), fixture.attempt.ID)
		afterEvents, _ := state.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
		if after.Generation != operation.Generation || len(afterEvents) != len(beforeEvents) {
			t.Fatalf("rejected pending mutation changed operation=%#v/%#v events=%d/%d", operation, after, len(beforeEvents), len(afterEvents))
		}
	})

	t.Run("canceled observation after succeeded", func(t *testing.T) {
		_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "external-succeeded-run.db"))
		fixture := prepareWorkflowSQLiteExternal(t, state, "succeeded-run", workflowTestTime())
		run, _ := state.LoadRun(context.Background(), fixture.attempt.ID.Invocation.RunID)
		running, err := state.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: fixture.base.Add(4 * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := state.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunSucceeded, At: fixture.base.Add(5 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		operation, _ := state.LoadExternalOperation(context.Background(), fixture.attempt.ID)
		beforeEvents, _ := state.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
		failure := workflowruntime.Failure{Code: "remote_canceled", Message: "remote operation canceled"}
		_, applyErr := state.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
			Attempt: fixture.attempt.ID, ExpectedOperationGeneration: operation.Generation,
			ExpectedNodeGeneration: fixture.node.Generation, ExpectedAttemptGeneration: fixture.attempt.Generation,
			Status: stepkind.ObservationCanceled, Failure: &failure, NextNodeStatus: workflowruntime.NodeCanceled,
			ObservedAt: fixture.base.Add(6 * time.Second), At: fixture.base.Add(6 * time.Second),
		})
		if !errors.Is(applyErr, workflowruntime.ErrInvalidRecord) {
			t.Fatalf("canceled completion after success error = %v", applyErr)
		}
		after, _ := state.LoadExternalOperation(context.Background(), fixture.attempt.ID)
		afterEvents, _ := state.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: run.ID})
		if after.Generation != operation.Generation || len(afterEvents) != len(beforeEvents) {
			t.Fatalf("rejected canceled completion changed operation=%#v/%#v events=%d/%d", operation, after, len(beforeEvents), len(afterEvents))
		}
	})
}

func TestWorkflowSQLiteExternalRecoveryUsesSemanticFractionalTimeOrder(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "external-order.db"))
	target := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
	exact := prepareWorkflowSQLiteExternal(t, state, "order-exact", target.Add(-3*time.Second))
	fractional := prepareWorkflowSQLiteExternal(t, state, "order-fractional", target.Add(-2900*time.Millisecond))
	recovered, err := state.RecoverExternalOperations(context.Background(), workflowruntime.ExternalOperationQuery{Limit: 1})
	if err != nil || len(recovered) != 1 || recovered[0].Attempt != exact.attempt.ID || !recovered[0].UpdatedAt.Before(fractional.operation.UpdatedAt) {
		t.Fatalf("RecoverExternalOperations(fractional) = %#v, %v", recovered, err)
	}
}

func TestWorkflowSQLiteWaitContinuationExactBinding(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "wait-continuation.db"))
	fixture := prepareWorkflowSQLiteWait(t, state, "continuation", workflowTestTime(), time.Hour)
	coordinator := workflowruntime.WaitCoordinator{Store: state}
	suspended, err := coordinator.Suspend(context.Background(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token})
	if err != nil {
		t.Fatal(err)
	}
	payload := workflowTestValue(t, "accepted")
	resumed, err := coordinator.Resume(context.Background(), workflowruntime.ResumeCommand{
		WaitID: suspended.Wait.Ref.ID, Correlation: fixture.correlation, Token: fixture.token, WakeSource: fixture.source,
		Responder: workflowwait.Responder{Kind: "test", Reference: "responder"}, Payload: payload, IdempotencyKey: "continuation-response-key", ReceivedAt: fixture.base.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	continuation, err := state.LoadWaitContinuation(context.Background(), resumed.Attempt.ID)
	if err != nil || continuation.Status != workflowruntime.WaitResumed || continuation.ResumeValues == nil || continuation.Ref.ID != suspended.Wait.Ref.ID {
		t.Fatalf("LoadWaitContinuation() = %#v, %v", continuation, err)
	}
	valuesSet, err := state.LoadValues(context.Background(), *continuation.ResumeValues)
	if err != nil || valuesSet[workflowruntime.ResumeValueName].Inline != "accepted" {
		t.Fatalf("LoadValues(continuation) = %#v, %v", valuesSet, err)
	}
	encoded, _ := json.Marshal(continuation)
	if containsBytes(encoded, []byte(fixture.token)) {
		t.Fatalf("persisted continuation leaked raw token: %s", encoded)
	}
}

type workflowSQLiteExternalFixture struct {
	base      time.Time
	operation workflowruntime.ExternalOperationSnapshot
	node      workflowruntime.NodeInvocationSnapshot
	attempt   workflowruntime.AttemptSnapshot
}

func prepareWorkflowSQLiteExternal(t *testing.T, state *WorkflowStateStore, suffix string, base time.Time) workflowSQLiteExternalFixture {
	t.Helper()
	running := prepareWorkflowSQLiteWait(t, state, "external-"+suffix, base, time.Hour)
	node, err := state.LoadNodeInvocation(context.Background(), running.invocation)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := state.LoadAttempt(context.Background(), workflowruntime.AttemptID{Invocation: running.invocation, Number: node.LatestAttempt})
	if err != nil {
		t.Fatal(err)
	}
	result, err := state.SuspendExternalOperation(context.Background(), workflowruntime.SuspendExternalOperationRequest{
		Operation: workflowruntime.ExternalOperationSnapshot{
			Attempt: attempt.ID, Ref: stepkind.ExternalOperationRef{Kind: "job", ID: "job-" + suffix},
			Invocation: stepkind.Invocation{
				Identity: stepkind.InvocationIdentity{RunID: string(attempt.ID.Invocation.RunID), NodeID: attempt.ID.Invocation.NodeID, Iteration: attempt.ID.Invocation.Iteration, Attempt: attempt.ID.Number},
				Config:   graph.Config{"large": json.Number("9007199254740993")}, Inputs: values.ValueSet{},
			},
			Status: stepkind.ObservationPending,
		},
		ExpectedNodeGeneration: node.Generation, ExpectedAttemptGeneration: attempt.Generation,
		Claim: running.request.Claim, At: running.request.At,
	})
	if err != nil {
		t.Fatal(err)
	}
	return workflowSQLiteExternalFixture{base: base, operation: result.Operation, node: result.Node, attempt: result.Attempt}
}

func containsBytes(value, search []byte) bool {
	if len(search) == 0 || len(value) < len(search) {
		return false
	}
	for index := 0; index+len(search) <= len(value); index++ {
		matched := true
		for offset := range len(search) {
			if value[index+offset] != search[offset] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
