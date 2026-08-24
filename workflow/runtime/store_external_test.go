package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestRunCASIdempotencyAndDefensiveCopies(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Now()
	inputSet := testValueSet(t, map[string]any{"items": []any{"one", "two"}})
	inputRef, err := store.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner: workflowruntime.ValueOwner{Kind: "run-inputs", RunID: "run-1"}, Values: inputSet,
	})
	if err != nil {
		t.Fatalf("SaveValues: %v", err)
	}
	request := workflowruntime.CreateRunRequest{
		ID: "run-1", Plan: testPlan(), Status: workflowruntime.RunPending,
		Inputs: &inputRef, StartIdempotencyKey: "start-1", CreatedAt: now,
	}
	created, outcome, err := store.CreateRun(ctx, request)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || created.Generation != 1 {
		t.Fatalf("CreateRun = %#v, %q, %v", created, outcome, err)
	}
	replayRequest := request
	replayRequest.CreatedAt = equivalentInstant(request.CreatedAt)
	replayed, outcome, err := store.CreateRun(ctx, replayRequest)
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || replayed.Generation != 1 {
		t.Fatalf("replayed CreateRun = %#v, %q, %v", replayed, outcome, err)
	}
	conflicting := request
	conflicting.ID = "different-run"
	if _, _, conflictErr := store.CreateRun(ctx, conflicting); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", conflictErr)
	}

	created.Inputs.ID = "mutated"
	loaded, err := store.LoadRun(ctx, "run-1")
	if err != nil || loaded.Inputs.ID != inputRef.ID {
		t.Fatalf("stored run was aliased: %#v, %v", loaded, err)
	}
	loaded.UpdatedAt = now.Add(time.Minute)
	updated, err := store.SaveRun(ctx, workflowruntime.SaveRunRequest{Snapshot: loaded, ExpectedGeneration: 1})
	if err != nil || updated.Generation != 2 {
		t.Fatalf("SaveRun = %#v, %v", updated, err)
	}
	if _, staleErr := store.SaveRun(ctx, workflowruntime.SaveRunRequest{Snapshot: loaded, ExpectedGeneration: 1}); !errors.Is(staleErr, workflowruntime.ErrCASMismatch) {
		t.Fatalf("expected stale run CAS error, got %v", staleErr)
	}
	changedPlan := updated
	changedPlan.Plan.Version = "v2"
	changedPlan.UpdatedAt = now.Add(2 * time.Minute)
	if _, identityErr := store.SaveRun(ctx, workflowruntime.SaveRunRequest{Snapshot: changedPlan, ExpectedGeneration: 2}); !errors.Is(identityErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("expected immutable plan rejection, got %v", identityErr)
	}
	unchangedRun, err := store.LoadRun(ctx, "run-1")
	if err != nil || unchangedRun.Generation != 2 || unchangedRun.Plan != request.Plan {
		t.Fatalf("rejected plan replacement mutated run: %#v, %v", unchangedRun, err)
	}

	inputSet["payload"] = testValueSet(t, "replacement")["payload"]
	loadedSet, err := store.LoadValues(ctx, inputRef)
	if err != nil {
		t.Fatalf("LoadValues: %v", err)
	}
	loadedSet["payload"] = testValueSet(t, "changed-return")["payload"]
	reloadedSet, err := store.LoadValues(ctx, inputRef)
	if err != nil || reloadedSet["payload"].Digest == loadedSet["payload"].Digest {
		t.Fatalf("stored value set was aliased: %v", err)
	}
}

func TestNodePersistenceDefensiveCopies(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-1", "execute")
	createRun(t, store, id.RunID, now)
	blocked := &workflowruntime.BlockedReason{Code: "dependency", Message: "waiting", Details: map[string]string{"node": "prepare"}}
	_, err := store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodePending, CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatalf("CreateNodeInvocation: %v", err)
	}
	transitioned, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: 1, To: workflowruntime.NodeBlocked, Blocked: blocked, At: now,
	})
	if err != nil {
		t.Fatalf("TransitionNode(blocked): %v", err)
	}
	node := transitioned.Snapshot
	blocked.Details["node"] = "mutated"
	node.Blocked.Details["node"] = "also-mutated"
	loadedNode, err := store.LoadNodeInvocation(ctx, id)
	if err != nil || loadedNode.Blocked.Details["node"] != "prepare" {
		t.Fatalf("node snapshot was aliased: %#v, %v", loadedNode, err)
	}

}

func TestPlanCachePinsAndExternalActivation(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	plan := testPlan()
	if err := store.RecordPlan(ctx, plan); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordPlan(ctx, plan); err != nil {
		t.Fatalf("identical plan replay: %v", err)
	}
	if loaded, err := store.LoadPlan(ctx, plan.Digest); err != nil || loaded != plan {
		t.Fatalf("LoadPlan = %#v, %v", loaded, err)
	}

	set := testValueSet(t, "cached")
	ref, err := store.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner: workflowruntime.ValueOwner{Kind: "node-output", RunID: "run-1"}, Values: set,
	})
	if err != nil {
		t.Fatal(err)
	}
	cache := workflowruntime.CacheEntry{
		Key: "cache-1", PlanDigest: plan.Digest, NodeID: "execute",
		InputDigest: values.SHA256Digest([]byte("inputs")), Outputs: ref,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	if putErr := store.PutCacheEntry(ctx, cache); putErr != nil {
		t.Fatal(putErr)
	}
	if _, found, getErr := store.GetCacheEntry(ctx, cache.Key, now.Add(2*time.Hour)); getErr != nil || found {
		t.Fatalf("expired cache = %v, %v", found, getErr)
	}
	pin := workflowruntime.PinnedValue{Key: "result", Value: workflowruntime.ValueRef{Set: ref, Name: "payload"}, PinnedAt: now}
	if putErr := store.PutPinnedValue(ctx, pin); putErr != nil {
		t.Fatal(putErr)
	}
	if pins, listErr := store.ListPinnedValues(ctx, now); listErr != nil || len(pins) != 1 || pins[0] != pin {
		t.Fatalf("ListPinnedValues = %#v, %v", pins, listErr)
	}

	activation := workflowruntime.ExternalActivationRequest{
		ActivationID: "activation-1", IdempotencyKey: "external-1", RequestedRunID: "run-external",
		Plan: plan, Inputs: &ref, OccurredAt: now,
	}
	accepted, outcome, err := store.RecordExternalActivation(ctx, activation)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || accepted.Inputs.ID != ref.ID {
		t.Fatalf("RecordExternalActivation = %#v, %q, %v", accepted, outcome, err)
	}
	accepted.Inputs.ID = "mutated"
	activationReplay := activation
	activationReplay.OccurredAt = equivalentInstant(activation.OccurredAt)
	replayed, outcome, err := store.RecordExternalActivation(ctx, activationReplay)
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || replayed.Inputs.ID != ref.ID {
		t.Fatalf("activation replay = %#v, %q, %v", replayed, outcome, err)
	}
	activation.RequestedRunID = "different-run"
	if _, _, err := store.RecordExternalActivation(ctx, activation); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("expected activation conflict, got %v", err)
	}
}

func equivalentInstant(value time.Time) time.Time {
	return value.Round(0).In(time.FixedZone("equivalent", 0))
}
