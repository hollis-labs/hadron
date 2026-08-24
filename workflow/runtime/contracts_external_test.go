package runtime_test

import (
	"errors"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestClosedStatusContracts(t *testing.T) {
	runStatuses := []workflowruntime.RunStatus{
		workflowruntime.RunPending, workflowruntime.RunRunning, workflowruntime.RunWaiting,
		workflowruntime.RunSucceeded, workflowruntime.RunFailed, workflowruntime.RunCanceled,
		workflowruntime.RunTimedOut, workflowruntime.RunCrashed,
	}
	for _, status := range runStatuses {
		if !status.Valid() {
			t.Fatalf("expected run status %q to be valid", status)
		}
	}
	if workflowruntime.RunStatus("blocked").Valid() || workflowruntime.RunStatus("unknown").Valid() {
		t.Fatal("run statuses must remain a purposeful closed set")
	}

	nodeStatuses := []workflowruntime.NodeStatus{
		workflowruntime.NodePending, workflowruntime.NodeReady, workflowruntime.NodeRunning,
		workflowruntime.NodeWaiting, workflowruntime.NodeSucceeded, workflowruntime.NodeFailed,
		workflowruntime.NodeSkipped, workflowruntime.NodeCanceled, workflowruntime.NodeTimedOut,
		workflowruntime.NodeCrashed, workflowruntime.NodeBlocked,
	}
	for _, status := range nodeStatuses {
		if !status.Valid() {
			t.Fatalf("expected node status %q to be valid", status)
		}
	}
	if workflowruntime.NodeStatus("unknown").Valid() {
		t.Fatal("unknown node status must be rejected")
	}

	for _, status := range []workflowruntime.WaitStatus{
		workflowruntime.WaitOpen, workflowruntime.WaitResumed,
		workflowruntime.WaitTimedOut, workflowruntime.WaitCanceled,
	} {
		if !status.Valid() {
			t.Fatalf("expected wait status %q to be valid", status)
		}
	}
	if workflowruntime.WaitStatus("unknown").Valid() {
		t.Fatal("unknown wait status must be rejected")
	}
}

func TestStructuredBlockedReasonAndReferenceValidation(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-1", "review")
	blocked := workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodeBlocked,
		Blocked: &workflowruntime.BlockedReason{
			Code: "dependency_failed", Message: "approval cannot start",
			Dependencies: []workflowruntime.NodeInvocationID{invocationID("run-1", "prepare")},
			Details:      map[string]string{"dependency": "prepare"},
		},
		Generation: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := blocked.Validate(); err != nil {
		t.Fatalf("blocked snapshot should validate: %v", err)
	}
	blocked.Blocked.Dependencies[0].RunID = "other-run"
	if err := blocked.Validate(); err == nil {
		t.Fatal("malformed dependency identity should be rejected")
	}
	blocked.Blocked = nil
	if err := blocked.Validate(); err == nil {
		t.Fatal("blocked status without reason should be rejected")
	}

	if err := (workflowruntime.NodeInvocationID{RunID: "run-1", NodeID: "bad id"}).Validate(); err == nil {
		t.Fatal("invalid graph node id should be rejected")
	}
	if err := (workflowruntime.AttemptID{Invocation: id}).Validate(); err == nil {
		t.Fatal("non-positive attempt number should be rejected")
	}
	plan := testPlan()
	if err := plan.Validate(); err != nil {
		t.Fatalf("plan ref should validate: %v", err)
	}
	set := testValueSet(t, "payload")
	ref, err := values.NewValueSetRef("values-1", set)
	if err != nil {
		t.Fatal(err)
	}
	if err := (workflowruntime.ValueRef{Set: ref, Name: "payload"}).Validate(); err != nil {
		t.Fatalf("value ref should validate: %v", err)
	}
}

func TestWaitTerminalSnapshotValidation(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	base := workflowruntime.WaitSnapshot{
		Ref: workflowruntime.WaitRef{ID: "wait-1"}, Invocation: invocationID("run-1", "pause"),
		Generation: 1, CreatedAt: now, UpdatedAt: now.Add(time.Minute), ResolvedAt: now.Add(time.Minute),
	}
	for _, status := range []workflowruntime.WaitStatus{
		workflowruntime.WaitResumed, workflowruntime.WaitTimedOut, workflowruntime.WaitCanceled,
	} {
		snapshot := base
		snapshot.Status = status
		if err := snapshot.Validate(); err != nil {
			t.Fatalf("%s terminal wait should validate: %v", status, err)
		}
	}
	terminal := base
	terminal.Status = workflowruntime.WaitTimedOut
	ref := values.ValueSetRef{ID: "values-1", Digest: values.SHA256Digest(nil)}
	terminal.ResumeValues = &ref
	if err := terminal.Validate(); err == nil {
		t.Fatal("timed-out wait carrying resume values should be rejected")
	}
	open := base
	open.Status = workflowruntime.WaitOpen
	if err := open.Validate(); err == nil {
		t.Fatal("open wait carrying resolved_at should be rejected")
	}
}

func TestTypedStoreErrorsRemainSearchable(t *testing.T) {
	cas := &workflowruntime.CASMismatchError{Resource: "run", Expected: 2, Actual: 3}
	if !errors.Is(cas, workflowruntime.ErrCASMismatch) {
		t.Fatal("CAS error must unwrap to ErrCASMismatch")
	}
	conflict := &workflowruntime.IdempotencyConflictError{Operation: "start", Key: "key-1"}
	if !errors.Is(conflict, workflowruntime.ErrIdempotencyConflict) {
		t.Fatal("idempotency error must unwrap to ErrIdempotencyConflict")
	}
}

func TestEventRequiresConsistentInvocationIdentity(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	invocation := invocationID("run-1", "first")
	event := workflowruntime.Event{
		Sequence: 1, RunID: "run-1", Invocation: &invocation,
		Attempt: &workflowruntime.AttemptID{Invocation: invocationID("run-1", "second"), Number: 1},
		Type:    "attempt.started", OccurredAt: now,
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
	if err := event.Validate(); err == nil {
		t.Fatal("event with disagreeing invocation and attempt must be rejected")
	}
}

func testPlan() workflowruntime.PlanRef {
	return workflowruntime.PlanRef{
		ID: "plan", Version: "v1", Digest: values.SHA256Digest([]byte("plan")), SchemaVersion: "workflow.execution-plan/v1",
	}
}

func invocationID(runID workflowruntime.RunID, nodeID string) workflowruntime.NodeInvocationID {
	return workflowruntime.NodeInvocationID{RunID: runID, NodeID: nodeID}
}

func testValueSet(t *testing.T, payload any) values.ValueSet {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{
		Producer:  values.Producer{Kind: "test", Reference: "fixture"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatalf("new value: %v", err)
	}
	return values.ValueSet{"payload": value}
}
