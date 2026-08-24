package persistence

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
)

type memoOrderingStore interface {
	workflowruntime.StateStore
	workflowruntime.NodeInputStore
	workflowruntime.MemoStore
}

func TestWorkflowMemoEntryOrderingParity(t *testing.T) {
	base := workflowTestTime().Add(2 * time.Hour)
	plan := workflowTestPlan("memo-ordering")
	key := values.SHA256Digest([]byte("shared-ordering-key"))
	memoDigest := values.SHA256Digest([]byte("shared-expression-key"))
	schemaDigest := values.SHA256Digest([]byte("shared-output-schema"))
	tests := []struct {
		name  string
		store func(*testing.T) memoOrderingStore
	}{
		{name: "runtimetest", store: func(*testing.T) memoOrderingStore { return runtimetest.NewStore() }},
		{name: "sqlite", store: func(t *testing.T) memoOrderingStore {
			_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "memo-ordering.db"))
			return state
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := test.store(t)
			first := publishMemoOrderingEntry(t, store, plan, "z-source", "first", key, memoDigest, schemaDigest, base)
			second := publishMemoOrderingEntry(t, store, plan, "a-source", "second", key, memoDigest, schemaDigest, base)
			loaded, err := store.LoadMemoEntry(context.Background(), key)
			if err != nil || loaded.Source != second.Source || loaded.Outputs != second.Outputs || loaded.Source == first.Source {
				t.Fatalf("equal-time ordering = %#v, %v; want last publication %#v", loaded, err, second.Source)
			}

			newer := publishMemoOrderingEntry(t, store, plan, "m-newer", "newer", key, memoDigest, schemaDigest, base.Add(time.Second))
			_ = publishMemoOrderingEntry(t, store, plan, "zz-older-last", "older", key, memoDigest, schemaDigest, base.Add(-time.Second))
			loaded, err = store.LoadMemoEntry(context.Background(), key)
			if err != nil || loaded.Source != newer.Source || loaded.Outputs != newer.Outputs {
				t.Fatalf("created-at ordering = %#v, %v; want newer publication %#v", loaded, err, newer.Source)
			}
		})
	}
}

func publishMemoOrderingEntry(t *testing.T, store memoOrderingStore, plan workflowruntime.PlanRef, runID workflowruntime.RunID, payload string, key, memoDigest, schemaDigest string, createdAt time.Time) workflowruntime.MemoEntry {
	t.Helper()
	ctx := context.Background()
	start := createdAt.Add(-4 * time.Second)
	if err := store.RecordPlan(ctx, plan); err != nil {
		t.Fatal(err)
	}
	run, outcome, err := store.CreateRun(ctx, workflowruntime.CreateRunRequest{ID: runID, Plan: plan, Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + string(runID), CreatedAt: start})
	if err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("CreateRun = %#v/%q/%v", run, outcome, err)
	}
	running, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: start})
	if err != nil {
		t.Fatal(err)
	}
	id := workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: "work"}
	node, err := store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: id, Status: workflowruntime.NodePending, CreatedAt: start, UpdatedAt: start}})
	if err != nil {
		t.Fatal(err)
	}
	inputs := values.ValueSet{"input": workflowMemoValue(t, "input-"+payload)}
	bound, err := store.BindNodeInputs(ctx, workflowruntime.BindNodeInputsRequest{InvocationID: id, ExpectedGeneration: node.Generation, IdempotencyKey: "inputs-" + string(runID), Values: inputs, MemoKeyDigest: memoDigest, At: start})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: bound.Node.Generation, To: workflowruntime.NodeReady, At: start.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: id, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "worker", Token: "token-" + string(runID), IdempotencyKey: "claim-" + string(runID), Now: start.Add(2 * time.Second), LeaseUntil: createdAt.Add(time.Hour)})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("ClaimNode = %#v, %v", claim, err)
	}
	claimed, err := store.LoadNodeInvocation(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: workflowTestExecutor(), Inputs: &bound.Inputs, At: start.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	outputRef, err := store.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "node-attempt-outputs", RunID: run.ID, Invocation: &id, Attempt: &started.Attempt.ID}, Values: values.ValueSet{"result": workflowMemoValue(t, payload)}})
	if err != nil {
		t.Fatal(err)
	}
	finished, err := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: id, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, Outputs: &outputRef, At: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	entry := workflowruntime.MemoEntry{Key: key, PlanDigest: running.Snapshot.Plan.Digest, NodeID: id.NodeID, Kind: "test", KindVersion: "v1", MemoKeyDigest: memoDigest, InputDigest: bound.Inputs.Digest, OutputSchemaDigest: schemaDigest, OutputDigest: outputRef.Digest, Outputs: outputRef, Source: id, SourceAttempt: started.Attempt.ID, SourceOrigin: workflowruntime.OriginExecuted, Effects: graph.EffectSet{graph.EffectCompute}, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "safe_default", Reason: "compute output"}, CreatedAt: createdAt, ExpiresAt: createdAt.Add(time.Hour)}
	recorded, recordOutcome, err := store.RecordMemoEntry(ctx, entry)
	if err != nil || recordOutcome != workflowruntime.IdempotencyApplied || recorded.Source != finished.Node.ID {
		t.Fatalf("RecordMemoEntry = %#v/%q/%v", recorded, recordOutcome, err)
	}
	return recorded
}

func TestWorkflowSQLiteMemoPinReuseReopenAndContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memo-pin.db")
	store, first := openWorkflowStateTest(t, path)
	secondStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, err := NewWorkflowStateStore(secondStore)
	if err != nil {
		t.Fatal(err)
	}
	ctx, base := context.Background(), workflowTestTime()
	plan := workflowTestPlan("memo-shared")
	sourceRun := createWorkflowRunWithPlan(t, first, "memo-source", plan, base)
	targetRun := createWorkflowRunWithPlan(t, first, "memo-target", plan, base)
	sourceNode := createWorkflowTestNode(t, first, sourceRun.ID, "work", base)
	targetNode := createWorkflowTestNode(t, first, targetRun.ID, "work", base)
	memoKeyDigest := values.SHA256Digest([]byte("expression"))
	boundInputs, err := first.BindNodeInputs(ctx, workflowruntime.BindNodeInputsRequest{InvocationID: sourceNode.ID, ExpectedGeneration: sourceNode.Generation, IdempotencyKey: "memo-source-inputs", Values: values.ValueSet{"input": workflowMemoValue(t, "input")}, MemoKeyDigest: memoKeyDigest, At: base})
	if err != nil {
		t.Fatal(err)
	}
	sourceNode = boundInputs.Node
	sourceReady, err := first.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: sourceNode.ID, ExpectedGeneration: sourceNode.Generation, To: workflowruntime.NodeReady, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := first.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: sourceNode.ID, ExpectedClaimGeneration: sourceReady.Snapshot.ClaimGeneration, Owner: "worker", Token: "source-token", IdempotencyKey: "source-claim", Now: base.Add(2 * time.Second), LeaseUntil: base.Add(time.Hour)})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("source claim = %#v, %v", claim, err)
	}
	claimedSource, _ := first.LoadNodeInvocation(ctx, sourceNode.ID)
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := first.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: sourceNode.ID, ExpectedNodeGeneration: claimedSource.Generation, Claim: proof, Executor: workflowTestExecutor(), Inputs: &boundInputs.Inputs, At: base.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	outputs := values.ValueSet{"result": workflowMemoValue(t, "memoized")}
	outputRef, err := first.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "node-attempt-outputs", RunID: sourceRun.ID, Invocation: &sourceNode.ID, Attempt: &started.Attempt.ID}, Values: outputs})
	if err != nil {
		t.Fatal(err)
	}
	finished, err := first.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: sourceNode.ID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, Outputs: &outputRef, At: base.Add(4 * time.Second)})
	if err != nil || finished.Node.Origin != workflowruntime.OriginExecuted {
		t.Fatalf("finish source = %#v, %v", finished, err)
	}
	if finished.Node.Inputs == nil || finished.Node.Inputs.Digest != boundInputs.Inputs.Digest || finished.Node.MemoKeyDigest != memoKeyDigest {
		t.Fatalf("memo source binding = %#v, inputs %#v", finished.Node, boundInputs.Inputs)
	}

	key := values.SHA256Digest([]byte("memo-key"))
	schemaDigest := values.SHA256Digest([]byte("schema"))
	entry := workflowruntime.MemoEntry{Key: key, PlanDigest: plan.Digest, NodeID: "work", Kind: "test", KindVersion: "v1", MemoKeyDigest: memoKeyDigest, InputDigest: boundInputs.Inputs.Digest, OutputSchemaDigest: schemaDigest, OutputDigest: outputRef.Digest, Outputs: outputRef, Source: sourceNode.ID, SourceAttempt: started.Attempt.ID, SourceOrigin: workflowruntime.OriginExecuted, Effects: graph.EffectSet{graph.EffectCompute}, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "safe_default", Reason: "compute output", Attributes: map[string]string{"scope": "project"}}, CreatedAt: base.Add(4 * time.Second), ExpiresAt: base.Add(time.Hour)}
	recorded, outcome, err := first.RecordMemoEntry(ctx, entry)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || recorded.Outputs != outputRef {
		t.Fatalf("RecordMemoEntry = %#v/%q/%v", recorded, outcome, err)
	}
	offset := time.FixedZone("same-instant", -7*60*60)
	replayEntry := entry
	replayEntry.CreatedAt, replayEntry.ExpiresAt = entry.CreatedAt.In(offset), entry.ExpiresAt.In(offset)
	replayedEntry, outcome, err := second.RecordMemoEntry(ctx, replayEntry)
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || !reflect.DeepEqual(replayedEntry, recorded) {
		t.Fatalf("RecordMemoEntry(replay) = %#v/%q/%v", replayedEntry, outcome, err)
	}
	divergent := entry
	divergent.OutputSchemaDigest = values.SHA256Digest([]byte("changed-schema"))
	if _, _, conflictErr := second.RecordMemoEntry(ctx, divergent); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("RecordMemoEntry(divergent) = %v", conflictErr)
	}
	recorded.Effects[0] = graph.EffectRead
	recorded.Policy.Attributes["scope"] = "mutated"
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	loaded, err := second.LoadMemoEntry(ctx, key)
	if err != nil || loaded.Outputs != outputRef || loaded.SourceOrigin != workflowruntime.OriginExecuted || loaded.Effects[0] != graph.EffectCompute || loaded.Policy.Attributes["scope"] != "project" {
		t.Fatalf("LoadMemoEntry(reopen) = %#v, %v", loaded, err)
	}

	binding := workflowruntime.PinBinding{Target: targetNode.ID, PlanDigest: plan.Digest, Outputs: outputRef, OutputSchemaDigest: schemaDigest, Source: sourceNode.ID, SourcePlanDigest: plan.Digest, SourceOrigin: workflowruntime.OriginExecuted, Authority: workflowruntime.ReuseAuthority{Principal: "developer", Scope: "project", Attributes: map[string]string{"role": "developer"}}, Policy: workflowruntime.ReusePolicyDecision{Allow: true, Code: "pin_allowed", Reason: "authorized developer", Attributes: map[string]string{"decision": "stable"}}, BoundAt: base.Add(5 * time.Second)}
	bound, err := second.BindPin(ctx, workflowruntime.BindPinRequest{Binding: binding, ExpectedGeneration: targetNode.Generation, IdempotencyKey: "pin-key"})
	if err != nil || bound.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("BindPin = %#v, %v", bound, err)
	}
	replayBinding := binding
	replayBinding.BoundAt = binding.BoundAt.In(offset)
	replay, err := second.BindPin(ctx, workflowruntime.BindPinRequest{Binding: replayBinding, ExpectedGeneration: bound.Node.Generation, IdempotencyKey: "pin-key"})
	if err != nil || replay.Outcome != workflowruntime.IdempotencyReplayed || replay.Node.Generation != bound.Node.Generation {
		t.Fatalf("BindPin(replay) = %#v, %v", replay, err)
	}
	bound.Binding.Authority.Attributes["role"] = "mutated"
	bound.Binding.Policy.Attributes["decision"] = "mutated"
	loadedPin, err := second.LoadPin(ctx, targetNode.ID)
	if err != nil || loadedPin.Authority.Attributes["role"] != "developer" || loadedPin.Policy.Attributes["decision"] != "stable" {
		t.Fatalf("LoadPin(defensive) = %#v, %v", loadedPin, err)
	}
	targetReady, err := second.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: targetNode.ID, ExpectedGeneration: bound.Node.Generation, To: workflowruntime.NodeReady, At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	targetClaim, err := second.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: targetNode.ID, ExpectedClaimGeneration: targetReady.Snapshot.ClaimGeneration, Owner: "worker", Token: "target-token", IdempotencyKey: "target-claim", Now: base.Add(7 * time.Second), LeaseUntil: base.Add(time.Hour)})
	if err != nil || !targetClaim.Acquired {
		t.Fatalf("target claim = %#v, %v", targetClaim, err)
	}
	claimedTarget, _ := second.LoadNodeInvocation(ctx, targetNode.ID)
	reuse := workflowruntime.ReuseNodeOutputsRequest{InvocationID: targetNode.ID, ExpectedGeneration: claimedTarget.Generation, Claim: workflowruntime.ClaimProof{Owner: targetClaim.Lease.Owner, Token: targetClaim.Lease.Token, Generation: targetClaim.Lease.Generation}, Origin: workflowruntime.OriginPinned, Outputs: outputRef, Source: sourceNode.ID, SourceOrigin: workflowruntime.OriginExecuted, PlanDigest: plan.Digest, Policy: binding.Policy, IdempotencyKey: "reuse-key", At: base.Add(8 * time.Second)}
	forgedPolicy := reuse
	forgedPolicy.Policy.Reason = "forged replacement decision"
	forgedPolicy.IdempotencyKey = "forged-policy"
	if _, forgedErr := second.ReuseNodeOutputs(ctx, forgedPolicy); !errors.Is(forgedErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("ReuseNodeOutputs(forged pin policy) = %v", forgedErr)
	}
	results := make(chan workflowruntime.ReuseNodeOutputsResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, state := range []*WorkflowStateStore{second, mustWorkflowStateStore(t, path)} {
		wg.Add(1)
		go func(state *WorkflowStateStore) {
			defer wg.Done()
			result, callErr := state.ReuseNodeOutputs(ctx, reuse)
			results <- result
			errs <- callErr
		}(state)
	}
	wg.Wait()
	close(results)
	close(errs)
	var applied, replayed int
	for callErr := range errs {
		if callErr != nil {
			t.Fatalf("concurrent reuse: %v", callErr)
		}
	}
	for result := range results {
		if result.Outcome == workflowruntime.IdempotencyApplied {
			applied++
		}
		if result.Outcome == workflowruntime.IdempotencyReplayed {
			replayed++
		}
	}
	if applied != 1 || replayed != 1 {
		t.Fatalf("reuse outcomes applied=%d replayed=%d", applied, replayed)
	}
	changedReuse := reuse
	changedReuse.Policy.Reason = "different decision"
	if _, conflictErr := second.ReuseNodeOutputs(ctx, changedReuse); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("ReuseNodeOutputs(divergent) = %v", conflictErr)
	}
	final, err := second.LoadNodeInvocation(ctx, targetNode.ID)
	events, eventErr := second.ListEvents(ctx, workflowruntime.EventQuery{RunID: targetRun.ID})
	if err != nil || eventErr != nil || final.Origin != workflowruntime.OriginPinned || final.Outputs == nil || *final.Outputs != outputRef || countWorkflowEvents(events, workflowruntime.EventNodeOutcomeReused) != 1 {
		t.Fatalf("final reuse = %#v events=%#v errors=%v/%v", final, events, err, eventErr)
	}
}

func createWorkflowRunWithPlan(t *testing.T, state *WorkflowStateStore, id string, plan workflowruntime.PlanRef, at time.Time) workflowruntime.RunSnapshot {
	t.Helper()
	run, outcome, err := state.CreateRun(context.Background(), workflowruntime.CreateRunRequest{ID: workflowruntime.RunID(id), Plan: plan, Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + id, CreatedAt: at})
	if err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("CreateRun = %#v/%q/%v", run, outcome, err)
	}
	started, err := state.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: at})
	if err != nil {
		t.Fatal(err)
	}
	return started.Snapshot
}

func workflowMemoValue(t *testing.T, payload string) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{Producer: values.Producer{Kind: "fixture", Reference: "work", Output: "result"}, MediaType: "application/json", Redaction: values.RedactionPublic, Retention: values.RetentionProject})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustWorkflowStateStore(t *testing.T, path string) *WorkflowStateStore {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func countWorkflowEvents(events []workflowruntime.Event, kind string) int {
	count := 0
	for _, event := range events {
		if event.Type == kind {
			count++
		}
	}
	return count
}
