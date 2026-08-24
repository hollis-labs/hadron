package runtime_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestExternalOperationStoreSuspendsObservesAndRecoversAtomically(t *testing.T) {
	running := prepareRunningWait(t, "external-operation", "webhook", "webhook", time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC), time.Hour)
	startedNode, err := running.store.LoadNodeInvocation(context.Background(), running.invocation)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := running.store.LoadAttempt(context.Background(), workflowruntime.AttemptID{Invocation: running.invocation, Number: startedNode.LatestAttempt})
	if err != nil {
		t.Fatal(err)
	}
	suspended, err := running.store.SuspendExternalOperation(context.Background(), workflowruntime.SuspendExternalOperationRequest{
		Operation: workflowruntime.ExternalOperationSnapshot{
			Attempt: attempt.ID,
			Ref:     stepkind.ExternalOperationRef{Kind: "job", ID: "job-1", Metadata: map[string]string{"region": "test"}},
			Invocation: stepkind.Invocation{
				Identity: stepkind.InvocationIdentity{RunID: string(attempt.ID.Invocation.RunID), NodeID: attempt.ID.Invocation.NodeID, Iteration: attempt.ID.Invocation.Iteration, Attempt: attempt.ID.Number},
				Config:   graph.Config{"mode": "async"}, Inputs: values.ValueSet{}, IdempotencyKey: "invoke-1",
			},
			Status: stepkind.ObservationPending,
		},
		ExpectedNodeGeneration: startedNode.Generation, ExpectedAttemptGeneration: attempt.Generation,
		Claim: running.request.Claim, At: running.base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if suspended.Node.Status != workflowruntime.NodeWaiting || suspended.Node.Lease != nil || suspended.Node.Wait != nil ||
		suspended.Operation.Generation != 1 || !suspended.Operation.LastObservedAt.IsZero() || len(suspended.Events) != 2 {
		t.Fatalf("SuspendExternalOperation() = %#v", suspended)
	}
	suspended.Operation.Ref.Metadata["region"] = "mutated"
	suspended.Operation.Invocation.Config["mode"] = "mutated"
	loaded, err := running.store.LoadExternalOperation(context.Background(), attempt.ID)
	if err != nil || loaded.Ref.Metadata["region"] != "test" || loaded.Invocation.Config["mode"] != "async" {
		t.Fatalf("LoadExternalOperation() = %#v, %v", loaded, err)
	}
	recovered, err := running.store.RecoverExternalOperations(context.Background(), workflowruntime.ExternalOperationQuery{RunID: running.invocation.RunID})
	if err != nil || len(recovered) != 1 || recovered[0].Attempt != attempt.ID {
		t.Fatalf("RecoverExternalOperations() = %#v, %v", recovered, err)
	}

	beforeEvents, _ := running.store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: running.invocation.RunID})
	_, err = running.store.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
		Attempt: attempt.ID, ExpectedOperationGeneration: loaded.Generation + 1,
		ExpectedNodeGeneration: suspended.Node.Generation, ExpectedAttemptGeneration: attempt.Generation,
		Status: stepkind.ObservationPending, Progress: map[string]string{"percent": "10"}, ObservedAt: running.base.Add(4 * time.Second), At: running.base.Add(4 * time.Second),
	})
	if !errors.Is(err, workflowruntime.ErrCASMismatch) {
		t.Fatalf("stale ApplyExternalOperation() error = %v", err)
	}
	afterStale, _ := running.store.LoadExternalOperation(context.Background(), attempt.ID)
	afterEvents, _ := running.store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: running.invocation.RunID})
	if afterStale.Generation != loaded.Generation || len(afterEvents) != len(beforeEvents) {
		t.Fatalf("failed apply mutated state/events: %#v, %d -> %d", afterStale, len(beforeEvents), len(afterEvents))
	}

	pending, err := running.store.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
		Attempt: attempt.ID, ExpectedOperationGeneration: loaded.Generation,
		ExpectedNodeGeneration: suspended.Node.Generation, ExpectedAttemptGeneration: attempt.Generation,
		Status: stepkind.ObservationPending, Progress: map[string]string{"percent": "10"},
		ObservedAt: running.base.Add(4 * time.Second), HeartbeatAt: running.base.Add(3500 * time.Millisecond), At: running.base.Add(4 * time.Second),
	})
	if err != nil || pending.Operation.Progress["percent"] != "10" || !pending.Operation.LastObservedAt.Equal(running.base.Add(4*time.Second)) ||
		!pending.Operation.LastHeartbeatAt.Equal(running.base.Add(3500*time.Millisecond)) || pending.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("pending ApplyExternalOperation() = %#v, %v", pending, err)
	}
	_, err = running.store.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
		Attempt: attempt.ID, ExpectedOperationGeneration: pending.Operation.Generation,
		ExpectedNodeGeneration: pending.Node.Generation, ExpectedAttemptGeneration: pending.Attempt.Generation,
		Status: stepkind.ObservationPending, Progress: map[string]string{"percent": "20"},
		ObservedAt: running.base.Add(4500 * time.Millisecond), HeartbeatAt: running.base.Add(3400 * time.Millisecond), At: running.base.Add(4500 * time.Millisecond),
	})
	if !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("regressing heartbeat ApplyExternalOperation() error = %v", err)
	}

	failure := workflowruntime.Failure{Code: "remote-failed", Message: "remote operation failed"}
	terminal, err := running.store.ApplyExternalOperation(context.Background(), workflowruntime.ApplyExternalOperationRequest{
		Attempt: attempt.ID, ExpectedOperationGeneration: pending.Operation.Generation,
		ExpectedNodeGeneration: pending.Node.Generation, ExpectedAttemptGeneration: pending.Attempt.Generation,
		Status: stepkind.ObservationFailed, Progress: map[string]string{"percent": "100"}, Failure: &failure,
		NextNodeStatus: workflowruntime.NodeReady, ObservedAt: running.base.Add(5 * time.Second), At: running.base.Add(5 * time.Second),
	})
	if err != nil || terminal.Operation.Status != stepkind.ObservationFailed || terminal.Attempt.Status != workflowruntime.NodeFailed ||
		terminal.Node.Status != workflowruntime.NodeReady || terminal.Node.Lease != nil || len(terminal.Events) != 3 {
		t.Fatalf("terminal ApplyExternalOperation() = %#v, %v", terminal, err)
	}
	recovered, err = running.store.RecoverExternalOperations(context.Background(), workflowruntime.ExternalOperationQuery{})
	if err != nil || len(recovered) != 0 {
		t.Fatalf("terminal recovery = %#v, %v", recovered, err)
	}
}

func TestExternalOperationCancelIntentAndDeterministicTimestampDiagnostics(t *testing.T) {
	running := prepareRunningWait(t, "external-cancel", "webhook", "webhook", time.Date(2026, time.August, 24, 17, 0, 0, 0, time.UTC), time.Hour)
	node, _ := running.store.LoadNodeInvocation(context.Background(), running.invocation)
	attempt, _ := running.store.LoadAttempt(context.Background(), workflowruntime.AttemptID{Invocation: running.invocation, Number: node.LatestAttempt})
	suspended, err := running.store.SuspendExternalOperation(context.Background(), workflowruntime.SuspendExternalOperationRequest{
		Operation: externalOperationFixture(attempt.ID), ExpectedNodeGeneration: node.Generation,
		ExpectedAttemptGeneration: attempt.Generation, Claim: running.request.Claim, At: running.base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	canceled, err := running.store.RequestExternalOperationCancel(context.Background(), workflowruntime.RequestExternalOperationCancelRequest{
		Attempt: attempt.ID, ExpectedOperationGeneration: suspended.Operation.Generation, At: running.base.Add(4 * time.Second),
	})
	if err != nil || canceled.Event == nil || canceled.Operation.CancelRequestedAt.IsZero() {
		t.Fatalf("RequestExternalOperationCancel() = %#v, %v", canceled, err)
	}
	replay, err := running.store.RequestExternalOperationCancel(context.Background(), workflowruntime.RequestExternalOperationCancelRequest{
		Attempt: attempt.ID, ExpectedOperationGeneration: canceled.Operation.Generation, At: running.base.Add(4 * time.Second),
	})
	if err != nil || replay.Event != nil || replay.Operation.Generation != canceled.Operation.Generation {
		t.Fatalf("cancel replay = %#v, %v", replay, err)
	}

	invalid := canceled.Operation
	invalid.UpdatedAt = invalid.CreatedAt.Add(10 * time.Second)
	invalid.CancelRequestedAt = invalid.CreatedAt.Add(-time.Second)
	invalid.LastObservedAt = invalid.UpdatedAt.Add(time.Second)
	for range 20 {
		err := invalid.Validate()
		if err == nil || !strings.Contains(err.Error(), "cancel_requested_at") {
			t.Fatalf("ExternalOperationSnapshot.Validate() diagnostic = %v", err)
		}
	}
}

func externalOperationFixture(attempt workflowruntime.AttemptID) workflowruntime.ExternalOperationSnapshot {
	return workflowruntime.ExternalOperationSnapshot{
		Attempt: attempt,
		Ref:     stepkind.ExternalOperationRef{Kind: "job", ID: "job-cancel"},
		Invocation: stepkind.Invocation{
			Identity: stepkind.InvocationIdentity{RunID: string(attempt.Invocation.RunID), NodeID: attempt.Invocation.NodeID, Iteration: attempt.Invocation.Iteration, Attempt: attempt.Number},
			Config:   graph.Config{}, Inputs: values.ValueSet{},
		},
		Status: stepkind.ObservationPending,
	}
}
