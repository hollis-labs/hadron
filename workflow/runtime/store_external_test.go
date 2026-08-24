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
	conflicting.Status = workflowruntime.RunRunning
	if _, _, conflictErr := store.CreateRun(ctx, conflicting); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", conflictErr)
	}

	created.Inputs.ID = "mutated"
	loaded, err := store.LoadRun(ctx, "run-1")
	if err != nil || loaded.Inputs.ID != inputRef.ID {
		t.Fatalf("stored run was aliased: %#v, %v", loaded, err)
	}
	loaded.Status = workflowruntime.RunRunning
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

func TestNodeAttemptAndWaitPersistence(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-1", "execute")
	blocked := &workflowruntime.BlockedReason{Code: "dependency", Message: "waiting", Details: map[string]string{"node": "prepare"}}
	node, err := store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodeBlocked, Blocked: blocked, CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatalf("CreateNodeInvocation: %v", err)
	}
	blocked.Details["node"] = "mutated"
	node.Blocked.Details["node"] = "also-mutated"
	loadedNode, err := store.LoadNodeInvocation(ctx, id)
	if err != nil || loadedNode.Blocked.Details["node"] != "prepare" {
		t.Fatalf("node snapshot was aliased: %#v, %v", loadedNode, err)
	}

	attemptID := workflowruntime.AttemptID{Invocation: id, Number: 1}
	failure := &workflowruntime.Failure{Code: "temporary", Message: "try again", Retryable: true, Details: map[string]string{"source": "adapter"}}
	attempt, err := store.CreateAttempt(ctx, workflowruntime.CreateAttemptRequest{Snapshot: workflowruntime.AttemptSnapshot{
		ID: attemptID, Status: workflowruntime.NodeFailed, Failure: failure, CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatalf("CreateAttempt: %v", err)
	}
	failure.Details["source"] = "mutated"
	attempt.Failure.Details["source"] = "also-mutated"
	loadedAttempt, err := store.LoadAttempt(ctx, attemptID)
	if err != nil || loadedAttempt.Failure.Details["source"] != "adapter" {
		t.Fatalf("attempt snapshot was aliased: %#v, %v", loadedAttempt, err)
	}
	attempts, err := store.ListAttempts(ctx, id)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("ListAttempts = %#v, %v", attempts, err)
	}

	wait, err := store.CreateWait(ctx, workflowruntime.CreateWaitRequest{Snapshot: workflowruntime.WaitSnapshot{
		Ref: workflowruntime.WaitRef{ID: "wait-1"}, Invocation: id, Status: workflowruntime.WaitOpen,
		CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil || wait.Generation != 1 {
		t.Fatalf("CreateWait = %#v, %v", wait, err)
	}
	resume := workflowruntime.ResumeWaitRequest{WaitID: "wait-1", IdempotencyKey: "resume-1", ResumedAt: now.Add(time.Minute)}
	resumed, outcome, err := store.ResumeWait(ctx, resume)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || resumed.Status != workflowruntime.WaitResumed {
		t.Fatalf("ResumeWait = %#v, %q, %v", resumed, outcome, err)
	}
	resumeReplay := resume
	resumeReplay.ResumedAt = equivalentInstant(resume.ResumedAt)
	if _, replayOutcome, replayErr := store.ResumeWait(ctx, resumeReplay); replayErr != nil || replayOutcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("resume replay = %q, %v", replayOutcome, replayErr)
	}
	conflict := resume
	conflict.ResumedAt = now.Add(2 * time.Minute)
	if _, _, conflictErr := store.ResumeWait(ctx, conflict); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("expected resume idempotency conflict, got %v", conflictErr)
	}

	wait2, err := store.CreateWait(ctx, workflowruntime.CreateWaitRequest{Snapshot: workflowruntime.WaitSnapshot{
		Ref: workflowruntime.WaitRef{ID: "wait-2"}, Invocation: id, Status: workflowruntime.WaitOpen,
		CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatal(err)
	}
	wait2.Status = workflowruntime.WaitTimedOut
	wait2.ResolvedAt = now.Add(time.Minute)
	wait2.UpdatedAt = wait2.ResolvedAt
	timedOut, err := store.SaveWait(ctx, workflowruntime.SaveWaitRequest{Snapshot: wait2, ExpectedGeneration: 1})
	if err != nil || timedOut.Status != workflowruntime.WaitTimedOut || timedOut.Generation != 2 {
		t.Fatalf("SaveWait = %#v, %v", timedOut, err)
	}
	reassigned := timedOut
	reassigned.Invocation = invocationID("run-1", "other")
	reassigned.UpdatedAt = now.Add(2 * time.Minute)
	if _, identityErr := store.SaveWait(ctx, workflowruntime.SaveWaitRequest{Snapshot: reassigned, ExpectedGeneration: 2}); !errors.Is(identityErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("expected immutable wait invocation rejection, got %v", identityErr)
	}
	unchangedWait, err := store.LoadWait(ctx, "wait-2")
	if err != nil || unchangedWait.Generation != 2 || unchangedWait.Invocation != id {
		t.Fatalf("rejected wait reassignment mutated wait: %#v, %v", unchangedWait, err)
	}

	_, err = store.CreateWait(ctx, workflowruntime.CreateWaitRequest{Snapshot: workflowruntime.WaitSnapshot{
		Ref: workflowruntime.WaitRef{ID: "wait-3"}, Invocation: id, Status: workflowruntime.WaitOpen,
		CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatal(err)
	}
	emptyKeyResume := workflowruntime.ResumeWaitRequest{WaitID: "wait-3", ResumedAt: now.Add(time.Minute)}
	withoutKey, outcome, err := store.ResumeWait(ctx, emptyKeyResume)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || withoutKey.Generation != 2 {
		t.Fatalf("empty-key ResumeWait = %#v, %q, %v", withoutKey, outcome, err)
	}
	if _, _, duplicateErr := store.ResumeWait(ctx, emptyKeyResume); !errors.Is(duplicateErr, workflowruntime.ErrAlreadyResumed) {
		t.Fatalf("expected empty-key duplicate to report already resumed, got %v", duplicateErr)
	}
	unchanged, err := store.LoadWait(ctx, "wait-3")
	if err != nil || unchanged.Generation != 2 || unchanged.Status != workflowruntime.WaitResumed {
		t.Fatalf("empty-key duplicate mutated wait: %#v, %v", unchanged, err)
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
