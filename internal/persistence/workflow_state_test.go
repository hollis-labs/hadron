package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestWorkflowStateMigrationTablesAndIndexes(t *testing.T) {
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "hadron.db"))
	objects := map[string]string{
		"workflow_plan_refs": "table", "workflow_runs": "table",
		"workflow_run_start_idempotency": "table", "workflow_node_invocations": "table",
		"workflow_node_leases": "table", "workflow_claim_idempotency": "table",
		"workflow_attempts": "table", "workflow_waits": "table",
		"workflow_wait_resume_idempotency": "table", "workflow_value_sets": "table",
		"workflow_event_sequences": "table", "workflow_events": "table",
		"workflow_cache_entries": "table", "workflow_pinned_values": "table",
		"workflow_external_activations": "table",
		"idx_workflow_runs_recovery":    "index", "idx_workflow_nodes_recovery": "index",
		"idx_workflow_node_leases_expiry": "index", "idx_workflow_attempts_invocation": "index",
		"idx_workflow_waits_recovery": "index", "idx_workflow_value_sets_digest": "index",
		"idx_workflow_events_type": "index", "idx_workflow_cache_expiry": "index",
		"idx_workflow_pins_expiry": "index", "idx_workflow_activations_run": "index",
		"idx_workflow_activations_registration": "index",
		"workflow_events_reject_update":         "trigger", "workflow_events_reject_delete": "trigger",
	}
	for name, kind := range objects {
		var found string
		if err := store.DB().QueryRow(`
SELECT name FROM sqlite_master WHERE type = ? AND name = ?`, kind, name).Scan(&found); err != nil {
			t.Fatalf("missing %s %s: %v", kind, name, err)
		}
	}
	var migrations int
	if err := store.DB().QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = 14`).Scan(&migrations); err != nil {
		t.Fatalf("read migration version: %v", err)
	}
	if migrations != 1 {
		t.Fatalf("migration 14 count = %d, want 1", migrations)
	}

	var planSQL string
	if err := store.DB().QueryRow(`
SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'workflow_plan_refs'`).Scan(&planSQL); err != nil {
		t.Fatalf("read plan table definition: %v", err)
	}
	for _, reserved := range []string{"plan_snapshot_json", "source_map_json", "source_snapshot_json"} {
		if !hasColumn(t, store, "workflow_plan_refs", reserved) {
			t.Fatalf("workflow_plan_refs missing reserved column %s; sql=%s", reserved, planSQL)
		}
	}
}

func TestWorkflowStateRejectsMissingEntityParentsWithForeignKeysOff(t *testing.T) {
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "hadron.db"))
	ctx := context.Background()
	base := workflowTestTime()
	var foreignKeys int
	if err := store.DB().QueryRow(`PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
		t.Fatalf("read foreign_keys pragma: %v", err)
	}
	if foreignKeys != 0 {
		t.Fatalf("foreign_keys pragma = %d, want 0 for legacy compatibility", foreignKeys)
	}
	missingRunNode := workflowruntime.NodeInvocationSnapshot{
		ID:     workflowruntime.NodeInvocationID{RunID: "missing-run", NodeID: "orphan-node"},
		Status: workflowruntime.NodePending, CreatedAt: base, UpdatedAt: base,
	}
	if _, err := state.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{
		Snapshot: missingRunNode,
	}); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("CreateNodeInvocation missing run error = %v", err)
	}
	var nodeCount int
	if err := store.DB().QueryRow(`
SELECT COUNT(1) FROM workflow_node_invocations WHERE run_id = ?`, missingRunNode.ID.RunID).Scan(&nodeCount); err != nil {
		t.Fatalf("count rejected node: %v", err)
	}
	if nodeCount != 0 {
		t.Fatalf("missing-parent node persisted: count=%d", nodeCount)
	}

	run := createWorkflowTestRun(t, state, "run-missing-node", base)
	missingNodeWait := workflowruntime.WaitSnapshot{
		Ref:        workflowruntime.WaitRef{ID: "orphan-wait"},
		Invocation: workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: "missing-node"},
		Status:     workflowruntime.WaitOpen, CreatedAt: base, UpdatedAt: base,
	}
	if _, err := state.CreateWait(ctx, workflowruntime.CreateWaitRequest{
		Snapshot: missingNodeWait,
	}); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("CreateWait missing node error = %v", err)
	}
	var waitCount int
	if err := store.DB().QueryRow(`
SELECT COUNT(1) FROM workflow_waits WHERE wait_id = ?`, missingNodeWait.Ref.ID).Scan(&waitCount); err != nil {
		t.Fatalf("count rejected wait: %v", err)
	}
	if waitCount != 0 {
		t.Fatalf("missing-parent wait persisted: count=%d", waitCount)
	}
}

func TestWorkflowStateRunPlanIdempotencyAndAtomicRollback(t *testing.T) {
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "hadron.db"))
	ctx := context.Background()
	plan := workflowTestPlan("rollback")
	createdAt := workflowTestTime()
	request := workflowruntime.CreateRunRequest{
		ID: "run-rollback", Plan: plan, Status: workflowruntime.RunPending,
		StartIdempotencyKey: "start-rollback", CreatedAt: createdAt,
	}
	created, outcome, err := state.CreateRun(ctx, request)
	if err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("CreateRun = (%+v, %q, %v)", created, outcome, err)
	}
	// Same semantic instant with a different location/monotonic representation
	// must replay instead of conflicting.
	replayRequest := request
	replayRequest.CreatedAt = createdAt.In(time.FixedZone("same-instant", -5*60*60))
	replayed, outcome, err := state.CreateRun(ctx, replayRequest)
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || replayed.ID != created.ID {
		t.Fatalf("CreateRun replay = (%+v, %q, %v)", replayed, outcome, err)
	}
	conflict := request
	conflict.ID = "run-other"
	if _, _, conflictErr := state.CreateRun(ctx, conflict); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("CreateRun conflicting idempotency error = %v", conflictErr)
	}
	loadedPlan, err := state.LoadPlan(ctx, plan.Digest)
	if err != nil || loadedPlan != plan {
		t.Fatalf("LoadPlan = (%+v, %v), want %+v", loadedPlan, err, plan)
	}
	conflictingPlan := plan
	conflictingPlan.Version = "v2"
	if planErr := state.RecordPlan(ctx, conflictingPlan); !errors.Is(planErr, workflowruntime.ErrAlreadyExists) {
		t.Fatalf("RecordPlan digest metadata conflict error = %v", planErr)
	}
	changedPlan := created
	changedPlan.Plan = workflowTestPlan("different")
	changedPlan.UpdatedAt = createdAt.Add(time.Second)
	if _, saveErr := state.SaveRun(ctx, workflowruntime.SaveRunRequest{
		Snapshot: changedPlan, ExpectedGeneration: created.Generation,
	}); !errors.Is(saveErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SaveRun plan replacement error = %v", saveErr)
	}
	unchanged, err := state.LoadRun(ctx, created.ID)
	if err != nil || unchanged.Generation != created.Generation || unchanged.Plan != plan {
		t.Fatalf("rejected SaveRun mutated identity: %+v, %v", unchanged, err)
	}

	if _, triggerErr := store.DB().Exec(`
CREATE TRIGGER workflow_test_fail_events
BEFORE INSERT ON workflow_events
BEGIN SELECT RAISE(ABORT, 'test event failure'); END`); triggerErr != nil {
		t.Fatalf("create failure trigger: %v", triggerErr)
	}
	_, err = state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: created.ID, ExpectedGeneration: created.Generation,
		To: workflowruntime.RunRunning, At: createdAt.Add(time.Second),
	})
	if err == nil {
		t.Fatal("TransitionRun unexpectedly succeeded with failing event insert")
	}
	after, err := state.LoadRun(ctx, created.ID)
	if err != nil {
		t.Fatalf("LoadRun after rollback: %v", err)
	}
	if after.Status != workflowruntime.RunPending || after.Generation != created.Generation {
		t.Fatalf("failed transition mutated run: %+v", after)
	}
	events, err := state.ListEvents(ctx, workflowruntime.EventQuery{RunID: created.ID})
	if err != nil || len(events) != 0 {
		t.Fatalf("events after rollback = %+v, %v", events, err)
	}
}

func TestWorkflowStateLifecycleAttemptsEventsAndReopenRecovery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, state := openWorkflowStateTest(t, dbPath)
	ctx := context.Background()
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "run-lifecycle", base)
	transitionedRun, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
		RunID: run.ID, ExpectedGeneration: run.Generation,
		To: workflowruntime.RunRunning, At: base.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("TransitionRun: %v", err)
	}
	node := createWorkflowTestNode(t, state, run.ID, "execute", base)
	readyResult, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: node.Generation,
		To: workflowruntime.NodeReady, At: base.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("pending->ready: %v", err)
	}
	claimRequest := workflowruntime.ClaimNodeRequest{
		InvocationID: node.ID, ExpectedClaimGeneration: 0,
		Owner: "worker-a", Token: "claim-a", IdempotencyKey: "claim-lifecycle",
		Now: base.Add(3 * time.Second), LeaseUntil: base.Add(time.Minute),
	}
	claim, err := state.ClaimNode(ctx, claimRequest)
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("ClaimNode = (%+v, %v)", claim, err)
	}
	claimReplay := claimRequest
	claimReplay.Now = claimRequest.Now.In(time.FixedZone("equivalent", 3*60*60))
	claimReplay.LeaseUntil = claimRequest.LeaseUntil.In(time.FixedZone("equivalent", 3*60*60))
	replayedClaim, err := state.ClaimNode(ctx, claimReplay)
	if err != nil || !replayedClaim.Acquired || !replayedClaim.Replayed {
		t.Fatalf("ClaimNode replay = (%+v, %v)", replayedClaim, err)
	}
	claimedNode, err := state.LoadNodeInvocation(ctx, node.ID)
	if err != nil {
		t.Fatalf("LoadNodeInvocation: %v", err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: node.ID, ExpectedNodeGeneration: claimedNode.Generation,
		Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "noop", Version: "v1", Target: "local"},
		At: base.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatalf("StartNodeAttempt: %v", err)
	}
	finished, err := state.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: node.ID, AttemptNumber: started.Attempt.ID.Number,
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded,
		NextNodeStatus: workflowruntime.NodeSucceeded, At: base.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatalf("FinishNodeAttempt: %v", err)
	}
	if finished.Node.Lease != nil || finished.Node.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("finished node = %+v", finished.Node)
	}
	attempts, err := state.ListAttempts(ctx, node.ID)
	if err != nil || len(attempts) != 1 || attempts[0].Status != workflowruntime.NodeSucceeded {
		t.Fatalf("ListAttempts = %+v, %v", attempts, err)
	}
	events, err := state.ListEvents(ctx, workflowruntime.EventQuery{RunID: run.ID})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("event count = %d, want 4: %+v", len(events), events)
	}
	for index, event := range events {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("event[%d].Sequence = %d", index, event.Sequence)
		}
	}
	if transitionedRun.Event == nil || events[0].Type != workflowruntime.EventRunStatusChanged ||
		events[2].Type != workflowruntime.EventNodeAttemptStarted || events[3].Type != workflowruntime.EventNodeAttemptFinished {
		t.Fatalf("unexpected lifecycle events: %+v", events)
	}

	active := createWorkflowTestRun(t, state, "run-recovery", base.Add(10*time.Second))
	ready := createWorkflowTestNode(t, state, active.ID, "ready-node", base.Add(10*time.Second))
	readyResult, err = state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: ready.ID, ExpectedGeneration: ready.Generation,
		To: workflowruntime.NodeReady, At: base.Add(11 * time.Second),
	})
	if err != nil {
		t.Fatalf("recovery node ready: %v", err)
	}
	_, err = state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: ready.ID, ExpectedClaimGeneration: readyResult.Snapshot.ClaimGeneration,
		Owner: "recovery-worker", Token: "recovery-token", IdempotencyKey: "claim-recovery",
		Now: base.Add(12 * time.Second), LeaseUntil: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("claim recovery node: %v", err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatalf("close before reopen: %v", closeErr)
	}

	reopened, reopenedState := openWorkflowStateTest(t, dbPath)
	t.Cleanup(func() { _ = reopened.Close() })
	recovery, err := reopenedState.Recovery(ctx, workflowruntime.RecoveryQuery{Now: base.Add(20 * time.Second)})
	if err != nil {
		t.Fatalf("Recovery after reopen: %v", err)
	}
	if len(recovery.ActiveRuns) != 2 || len(recovery.Ready) != 1 || len(recovery.Leased) != 1 {
		t.Fatalf("unexpected recovery snapshot: %+v", recovery)
	}
	if recovery.Ready[0].ID != ready.ID || recovery.Leased[0].ID != ready.ID {
		t.Fatalf("ready/live-lease overlap lost: %+v", recovery)
	}
}

func TestWorkflowStateSkippedExplanationAndBlockedRefreshParity(t *testing.T) {
	_, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "hadron.db"))
	ctx := context.Background()
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "run-progression-parity", base)
	node := createWorkflowTestNode(t, state, run.ID, "target", base)

	initialReason := &workflowruntime.BlockedReason{
		Code: workflowruntime.ReasonReadinessWaiting, Message: "waiting for dependencies",
		Details: map[string]string{"terminal": "0", "nonterminal": "2"},
	}
	blocked, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: node.Generation,
		To: workflowruntime.NodeBlocked, Blocked: initialReason, At: base.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("pending->blocked: %v", err)
	}
	exact, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: blocked.Snapshot.Generation,
		To: workflowruntime.NodeBlocked, Blocked: initialReason, At: base.Add(time.Second),
	})
	if err != nil || exact.Outcome != workflowruntime.TransitionNoOp || exact.Event != nil {
		t.Fatalf("exact blocked replay = (%+v, %v)", exact, err)
	}
	changedReason := &workflowruntime.BlockedReason{
		Code: workflowruntime.ReasonReadinessWaiting, Message: "waiting for dependencies",
		Details: map[string]string{"terminal": "1", "nonterminal": "1"},
	}
	refreshed, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: blocked.Snapshot.Generation,
		To: workflowruntime.NodeBlocked, Blocked: changedReason, At: base.Add(2 * time.Second),
	})
	if err != nil || refreshed.Outcome != workflowruntime.TransitionApplied ||
		refreshed.Snapshot.Generation != blocked.Snapshot.Generation+1 {
		t.Fatalf("blocked diagnostic refresh = (%+v, %v)", refreshed, err)
	}
	if _, transitionErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: refreshed.Snapshot.Generation,
		To: workflowruntime.NodeBlocked, Blocked: changedReason, At: base.Add(3 * time.Second),
	}); !errors.Is(transitionErr, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("later unchanged blocked reason = %v", transitionErr)
	}

	explanation := &workflowruntime.BlockedReason{
		Code: workflowruntime.ReasonReadinessUnsatisfied, Message: "all_success cannot be satisfied",
		Dependencies: []workflowruntime.NodeInvocationID{
			{RunID: run.ID, NodeID: "z-dependency"},
			{RunID: run.ID, NodeID: "a-dependency"},
		},
		Details: map[string]string{"rule": "all_success", "failed": "1"},
	}
	skipped, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: refreshed.Snapshot.Generation,
		To: workflowruntime.NodeSkipped, Explanation: explanation, At: base.Add(3 * time.Second),
	})
	if err != nil || skipped.Outcome != workflowruntime.TransitionApplied || skipped.Event == nil {
		t.Fatalf("blocked->skipped with explanation = (%+v, %v)", skipped, err)
	}
	if skipped.Event.Attributes["explanation_code"] != explanation.Code || skipped.Event.Attributes["explanation"] == "" {
		t.Fatalf("skipped explanation event = %+v", skipped.Event)
	}
	var durableExplanation workflowruntime.BlockedReason
	if decodeErr := json.Unmarshal([]byte(skipped.Event.Attributes["explanation"]), &durableExplanation); decodeErr != nil {
		t.Fatalf("decode durable explanation: %v", decodeErr)
	}
	if len(durableExplanation.Dependencies) != 2 || durableExplanation.Dependencies[0].NodeID != "a-dependency" ||
		durableExplanation.Dependencies[1].NodeID != "z-dependency" {
		t.Fatalf("durable explanation dependencies are not canonical: %+v", durableExplanation.Dependencies)
	}
	reordered := *explanation
	reordered.Dependencies = append([]workflowruntime.NodeInvocationID(nil), explanation.Dependencies...)
	reordered.Dependencies[0], reordered.Dependencies[1] = reordered.Dependencies[1], reordered.Dependencies[0]
	replayed, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: skipped.Snapshot.Generation,
		To: workflowruntime.NodeSkipped, Explanation: &reordered, At: base.Add(3 * time.Second),
	})
	if err != nil || replayed.Outcome != workflowruntime.TransitionNoOp || replayed.Event != nil {
		t.Fatalf("exact skipped explanation replay = (%+v, %v)", replayed, err)
	}
	different := *explanation
	different.Message = "different reason"
	if _, transitionErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: skipped.Snapshot.Generation,
		To: workflowruntime.NodeSkipped, Explanation: &different, At: base.Add(3 * time.Second),
	}); !errors.Is(transitionErr, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("different skipped explanation replay = %v", transitionErr)
	}
	if _, transitionErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: skipped.Snapshot.Generation,
		To: workflowruntime.NodeCanceled, Explanation: explanation, At: base.Add(4 * time.Second),
	}); !errors.Is(transitionErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("non-skipped explanation = %v", transitionErr)
	}

	events, err := state.ListEvents(ctx, workflowruntime.EventQuery{RunID: run.ID})
	if err != nil || len(events) != 3 {
		t.Fatalf("progression event count = %d, %v; events=%+v", len(events), err, events)
	}
}

func TestWorkflowStateClaimsContentionExpiryAndNonReady(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	firstStore, first := openWorkflowStateTest(t, dbPath)
	secondStore, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open second handle: %v", err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, err := NewWorkflowStateStore(secondStore)
	if err != nil {
		t.Fatalf("NewWorkflowStateStore(second): %v", err)
	}
	_ = firstStore
	ctx := context.Background()
	base := workflowTestTime()
	run := createWorkflowTestRun(t, first, "run-claims", base)
	node := createWorkflowTestNode(t, first, run.ID, "claim-node", base)

	nonReady, err := first.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: node.ID, ExpectedClaimGeneration: 0,
		Owner: "worker", Token: "pending", IdempotencyKey: "claim-pending",
		Now: base.Add(time.Second), LeaseUntil: base.Add(time.Minute),
	})
	if err != nil || nonReady.Acquired {
		t.Fatalf("non-ready claim = (%+v, %v)", nonReady, err)
	}
	ready, err := first.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: node.Generation,
		To: workflowruntime.NodeReady, At: base.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("ready transition: %v", err)
	}

	const contenders = 16
	requests := make([]workflowruntime.ClaimNodeRequest, contenders)
	for index := range requests {
		requests[index] = workflowruntime.ClaimNodeRequest{
			InvocationID: node.ID, ExpectedClaimGeneration: 0,
			Owner: fmt.Sprintf("worker-%02d", index), Token: fmt.Sprintf("token-%02d", index),
			IdempotencyKey: fmt.Sprintf("claim-%02d", index),
			Now:            base.Add(3 * time.Second), LeaseUntil: base.Add(10 * time.Second),
		}
	}
	stores := []*WorkflowStateStore{first, second}
	type claimOutcome struct {
		result workflowruntime.ClaimResult
		err    error
	}
	outcomes := make(chan claimOutcome, contenders)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range requests {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			result, claimErr := stores[index%len(stores)].ClaimNode(ctx, requests[index])
			outcomes <- claimOutcome{result: result, err: claimErr}
		}(index)
	}
	close(start)
	group.Wait()
	close(outcomes)
	acquired, mismatches := 0, 0
	for outcome := range outcomes {
		if outcome.err == nil && outcome.result.Acquired {
			acquired++
		}
		if errors.Is(outcome.err, workflowruntime.ErrCASMismatch) {
			mismatches++
		}
	}
	if acquired != 1 || mismatches != contenders-1 {
		t.Fatalf("claim contention acquired=%d mismatches=%d", acquired, mismatches)
	}
	claimed, err := first.LoadNodeInvocation(ctx, node.ID)
	if err != nil || claimed.Lease == nil {
		t.Fatalf("load claimed node = (%+v, %v)", claimed, err)
	}

	liveResult, err := first.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: node.ID, ExpectedClaimGeneration: claimed.ClaimGeneration,
		Owner: "worker-live", Token: "live", IdempotencyKey: "claim-live",
		Now: base.Add(4 * time.Second), LeaseUntil: base.Add(20 * time.Second),
	})
	if err != nil || liveResult.Acquired {
		t.Fatalf("live lease contention = (%+v, %v)", liveResult, err)
	}
	liveReplay, err := first.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: node.ID, ExpectedClaimGeneration: claimed.ClaimGeneration,
		Owner: "worker-live", Token: "live", IdempotencyKey: "claim-live",
		Now: base.Add(4 * time.Second), LeaseUntil: base.Add(20 * time.Second),
	})
	if err != nil || liveReplay.Acquired || !liveReplay.Replayed {
		t.Fatalf("live lease replay = (%+v, %v)", liveReplay, err)
	}
	conflictingLiveRequest := workflowruntime.ClaimNodeRequest{
		InvocationID: node.ID, ExpectedClaimGeneration: claimed.ClaimGeneration,
		Owner: "worker-live", Token: "different-token", IdempotencyKey: "claim-live",
		Now: base.Add(4 * time.Second), LeaseUntil: base.Add(20 * time.Second),
	}
	if _, conflictErr := first.ClaimNode(ctx, conflictingLiveRequest); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("conflicting claim idempotency error = %v", conflictErr)
	}
	reclaimed, err := first.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: node.ID, ExpectedClaimGeneration: claimed.ClaimGeneration,
		Owner: "worker-new", Token: "new", IdempotencyKey: "claim-expired",
		Now: base.Add(11 * time.Second), LeaseUntil: base.Add(30 * time.Second),
	})
	if err != nil || !reclaimed.Acquired || reclaimed.Lease.Generation != claimed.ClaimGeneration+1 {
		t.Fatalf("expired lease reclaim = (%+v, %v); ready=%+v", reclaimed, err, ready)
	}
	renewed, err := first.RenewNodeLease(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: node.ID, Owner: reclaimed.Lease.Owner, Token: reclaimed.Lease.Token,
		Generation: reclaimed.Lease.Generation, Now: base.Add(12 * time.Second),
		LeaseUntil: base.Add(40 * time.Second),
	})
	if err != nil || !renewed.ExpiresAt.Equal(base.Add(40*time.Second)) {
		t.Fatalf("RenewNodeLease = (%+v, %v)", renewed, err)
	}
	if releaseErr := first.ReleaseNodeClaim(ctx, workflowruntime.ReleaseClaimRequest{
		InvocationID: node.ID, Owner: renewed.Owner, Token: renewed.Token,
		Generation: renewed.Generation, Now: base.Add(13 * time.Second),
	}); releaseErr != nil {
		t.Fatalf("ReleaseNodeClaim: %v", releaseErr)
	}
	released, err := first.LoadNodeInvocation(ctx, node.ID)
	if err != nil || released.Lease != nil || released.ClaimGeneration != renewed.Generation {
		t.Fatalf("released node = (%+v, %v)", released, err)
	}
}

func TestWorkflowStateWaitingResumeAndRecoveryOrdering(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	store, state := openWorkflowStateTest(t, dbPath)
	ctx := context.Background()
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "run-wait-path", base)

	var nodes []workflowruntime.NodeInvocationSnapshot
	for index, priority := range []int{1, 10, 5} {
		nodeID := fmt.Sprintf("ordered-%d", index)
		node, err := state.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
			ID:     workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: nodeID},
			Status: workflowruntime.NodePending, Priority: priority, CreatedAt: base, UpdatedAt: base,
		}})
		if err != nil {
			t.Fatalf("CreateNodeInvocation(%s): %v", nodeID, err)
		}
		ready, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
			InvocationID: node.ID, ExpectedGeneration: node.Generation,
			To: workflowruntime.NodeReady, At: base.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("TransitionNode(%s): %v", nodeID, err)
		}
		nodes = append(nodes, ready.Snapshot)
	}
	recovery, err := state.Recovery(ctx, workflowruntime.RecoveryQuery{RunID: run.ID, Now: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatalf("Recovery ordered: %v", err)
	}
	if len(recovery.Ready) != 3 || recovery.Ready[0].Priority != 10 || recovery.Ready[1].Priority != 5 || recovery.Ready[2].Priority != 1 {
		t.Fatalf("ready recovery ordering = %+v", recovery.Ready)
	}
	limited, err := state.Recovery(ctx, workflowruntime.RecoveryQuery{RunID: run.ID, Now: base.Add(2 * time.Second), Limit: 1})
	if err != nil || len(limited.ActiveRuns) != 1 || len(limited.Ready) != 1 || limited.Ready[0].Priority != 10 {
		t.Fatalf("limited recovery = %+v, %v", limited, err)
	}

	executing := nodes[1]
	claim, err := state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: executing.ID, ExpectedClaimGeneration: executing.ClaimGeneration,
		Owner: "wait-worker", Token: "wait-token", IdempotencyKey: "claim-wait-path",
		Now: base.Add(2 * time.Second), LeaseUntil: base.Add(time.Minute),
	})
	if err != nil || !claim.Acquired {
		t.Fatalf("ClaimNode(wait path) = (%+v, %v)", claim, err)
	}
	claimed, err := state.LoadNodeInvocation(ctx, executing.ID)
	if err != nil {
		t.Fatalf("LoadNodeInvocation(claimed): %v", err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: executing.ID, ExpectedNodeGeneration: claimed.Generation, Claim: proof,
		Executor: workflowruntime.ExecutorMetadata{Kind: "wait", Version: "v1"}, At: base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("StartNodeAttempt(wait path): %v", err)
	}
	waiting, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: executing.ID, ExpectedGeneration: started.Node.Generation,
		To: workflowruntime.NodeWaiting, Claim: &proof, At: base.Add(4 * time.Second),
	})
	if err != nil {
		t.Fatalf("running->waiting: %v", err)
	}
	if waiting.Snapshot.Lease != nil {
		t.Fatalf("running->waiting retained lease: %+v", waiting.Snapshot.Lease)
	}
	waitRef := workflowruntime.WaitRef{ID: "wait-reopen"}
	waitingSnapshot := waiting.Snapshot
	waitingSnapshot.Wait = &waitRef
	waitingSnapshot.UpdatedAt = base.Add(4 * time.Second)
	waitingSnapshot, err = state.SaveNodeInvocation(ctx, workflowruntime.SaveNodeInvocationRequest{
		Snapshot: waitingSnapshot, ExpectedGeneration: waiting.Snapshot.Generation,
	})
	if err != nil {
		t.Fatalf("attach wait reference: %v", err)
	}
	waitSnapshot, err := state.CreateWait(ctx, workflowruntime.CreateWaitRequest{Snapshot: workflowruntime.WaitSnapshot{
		Ref: waitRef, Invocation: executing.ID, Status: workflowruntime.WaitOpen,
		CreatedAt: base.Add(4 * time.Second), UpdatedAt: base.Add(4 * time.Second),
	}})
	if err != nil {
		t.Fatalf("CreateWait(reopen): %v", err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatalf("close waiting store: %v", closeErr)
	}
	_, reopenedState := openWorkflowStateTest(t, dbPath)
	state = reopenedState
	reopenedNode, err := state.LoadNodeInvocation(ctx, executing.ID)
	if err != nil || reopenedNode.Status != workflowruntime.NodeWaiting || reopenedNode.Wait == nil || reopenedNode.Wait.ID != waitRef.ID {
		t.Fatalf("reopened waiting node = (%+v, %v)", reopenedNode, err)
	}
	reopenedAttempts, err := state.ListAttempts(ctx, executing.ID)
	if err != nil || len(reopenedAttempts) != 1 || reopenedAttempts[0].Status != workflowruntime.NodeRunning ||
		reopenedAttempts[0].ID != started.Attempt.ID {
		t.Fatalf("reopened unfinished attempts = (%+v, %v)", reopenedAttempts, err)
	}
	reopenedWait, err := state.LoadWait(ctx, waitRef.ID)
	if err != nil || reopenedWait.Ref != waitSnapshot.Ref || reopenedWait.Status != workflowruntime.WaitOpen ||
		reopenedWait.Invocation != executing.ID {
		t.Fatalf("reopened wait snapshot = (%+v, %v)", reopenedWait, err)
	}
	waitRecovery, err := state.Recovery(ctx, workflowruntime.RecoveryQuery{RunID: run.ID, Now: base.Add(5 * time.Second)})
	if err != nil || len(waitRecovery.Waiting) != 1 || len(waitRecovery.Leased) != 0 {
		t.Fatalf("waiting recovery = %+v, %v", waitRecovery, err)
	}
	readyAgain, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: executing.ID, ExpectedGeneration: reopenedNode.Generation,
		To: workflowruntime.NodeReady, At: base.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatalf("waiting->ready: %v", err)
	}
	resumeClaim, err := state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: executing.ID, ExpectedClaimGeneration: readyAgain.Snapshot.ClaimGeneration,
		Owner: "resume-worker", Token: "resume-token", IdempotencyKey: "claim-resume-path",
		Now: base.Add(6 * time.Second), LeaseUntil: base.Add(2 * time.Minute),
	})
	if err != nil || !resumeClaim.Acquired {
		t.Fatalf("ClaimNode(resume) = (%+v, %v)", resumeClaim, err)
	}
	resumeReady, err := state.LoadNodeInvocation(ctx, executing.ID)
	if err != nil {
		t.Fatalf("LoadNodeInvocation(resume): %v", err)
	}
	resumeProof := workflowruntime.ClaimProof{Owner: resumeClaim.Lease.Owner, Token: resumeClaim.Lease.Token, Generation: resumeClaim.Lease.Generation}
	runningAgain, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: executing.ID, ExpectedGeneration: resumeReady.Generation,
		To: workflowruntime.NodeRunning, Claim: &resumeProof, At: base.Add(7 * time.Second),
	})
	if err != nil {
		t.Fatalf("ready->running resume: %v", err)
	}
	if runningAgain.Snapshot.LatestAttempt != started.Attempt.ID.Number {
		t.Fatalf("resume changed attempt number: %+v", runningAgain.Snapshot)
	}
	finished, err := state.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{
		InvocationID: executing.ID, AttemptNumber: started.Attempt.ID.Number,
		ExpectedNodeGeneration:    runningAgain.Snapshot.Generation,
		ExpectedAttemptGeneration: started.Attempt.Generation, Claim: resumeProof,
		AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded,
		At: base.Add(8 * time.Second),
	})
	if err != nil || finished.Node.LatestAttempt != 1 {
		t.Fatalf("FinishNodeAttempt(resumed) = (%+v, %v)", finished, err)
	}
}

func TestWorkflowStateValuesWaitCachePinsAndActivations(t *testing.T) {
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "hadron.db"))
	_ = store
	ctx := context.Background()
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "run-aux", base)
	node := createWorkflowTestNode(t, state, run.ID, "wait-node", base)
	set := workflowTestValues(t, "hello")
	ref, err := state.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner: workflowruntime.ValueOwner{Kind: "run_input", RunID: run.ID}, Values: set,
	})
	if err != nil {
		t.Fatalf("SaveValues: %v", err)
	}
	set["message"] = workflowTestValue(t, "mutated")
	loaded, err := state.LoadValues(ctx, ref)
	if err != nil {
		t.Fatalf("LoadValues: %v", err)
	}
	if got := loaded["message"].Inline; got != "hello" {
		t.Fatalf("persisted values aliased caller mutation: %v", got)
	}
	wrongRef := ref
	wrongRef.Digest = values.SHA256Digest([]byte("wrong"))
	if _, loadErr := state.LoadValues(ctx, wrongRef); !errors.Is(loadErr, workflowruntime.ErrCASMismatch) {
		t.Fatalf("LoadValues wrong digest error = %v", loadErr)
	}

	wait, err := state.CreateWait(ctx, workflowruntime.CreateWaitRequest{Snapshot: workflowruntime.WaitSnapshot{
		Ref: workflowruntime.WaitRef{ID: "wait-aux"}, Invocation: node.ID,
		Status: workflowruntime.WaitOpen, CreatedAt: base, UpdatedAt: base,
	}})
	if err != nil {
		t.Fatalf("CreateWait: %v", err)
	}
	resumeRequest := workflowruntime.ResumeWaitRequest{
		WaitID: wait.Ref.ID, IdempotencyKey: "resume-aux", Values: &ref,
		ResumedAt: base.Add(time.Second),
	}
	resumed, outcome, err := state.ResumeWait(ctx, resumeRequest)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || resumed.Status != workflowruntime.WaitResumed {
		t.Fatalf("ResumeWait = (%+v, %q, %v)", resumed, outcome, err)
	}
	replayRequest := resumeRequest
	replayRequest.ResumedAt = resumeRequest.ResumedAt.In(time.FixedZone("resume", 2*60*60))
	_, outcome, err = state.ResumeWait(ctx, replayRequest)
	if err != nil || outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("ResumeWait replay = (%q, %v)", outcome, err)
	}
	conflictResume := resumeRequest
	conflictResume.ResumedAt = base.Add(2 * time.Second)
	if _, _, conflictErr := state.ResumeWait(ctx, conflictResume); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("ResumeWait conflict error = %v", conflictErr)
	}
	otherNode := createWorkflowTestNode(t, state, run.ID, "other-wait-node", base)
	immutableWait := wait
	immutableWait.Invocation = otherNode.ID
	immutableWait.UpdatedAt = base.Add(2 * time.Second)
	if _, saveErr := state.SaveWait(ctx, workflowruntime.SaveWaitRequest{
		Snapshot: immutableWait, ExpectedGeneration: resumed.Generation,
	}); !errors.Is(saveErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SaveWait invocation replacement error = %v", saveErr)
	}
	unchangedWait, err := state.LoadWait(ctx, wait.Ref.ID)
	if err != nil || unchangedWait.Generation != resumed.Generation || unchangedWait.Invocation != node.ID {
		t.Fatalf("rejected SaveWait mutated identity: %+v, %v", unchangedWait, err)
	}
	emptyKeyWait, err := state.CreateWait(ctx, workflowruntime.CreateWaitRequest{Snapshot: workflowruntime.WaitSnapshot{
		Ref: workflowruntime.WaitRef{ID: "wait-empty-key"}, Invocation: otherNode.ID,
		Status: workflowruntime.WaitOpen, CreatedAt: base, UpdatedAt: base,
	}})
	if err != nil {
		t.Fatalf("CreateWait(empty key): %v", err)
	}
	emptyRequest := workflowruntime.ResumeWaitRequest{WaitID: emptyKeyWait.Ref.ID, ResumedAt: base.Add(time.Second)}
	if _, emptyOutcome, emptyErr := state.ResumeWait(ctx, emptyRequest); emptyErr != nil || emptyOutcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("ResumeWait(empty key) = (%q, %v)", emptyOutcome, emptyErr)
	}
	if _, _, duplicateErr := state.ResumeWait(ctx, emptyRequest); !errors.Is(duplicateErr, workflowruntime.ErrAlreadyResumed) {
		t.Fatalf("duplicate empty-key ResumeWait error = %v", duplicateErr)
	}

	planDigest := run.Plan.Digest
	inputDigest := values.SHA256Digest([]byte("input"))
	cache := workflowruntime.CacheEntry{Key: "cache-a", PlanDigest: planDigest, NodeID: node.ID.NodeID,
		InputDigest: inputDigest, Outputs: ref, CreatedAt: base, ExpiresAt: base.Add(time.Hour)}
	if cachePutErr := state.PutCacheEntry(ctx, cache); cachePutErr != nil {
		t.Fatalf("PutCacheEntry: %v", cachePutErr)
	}
	if got, found, cacheGetErr := state.GetCacheEntry(ctx, cache.Key, base.Add(time.Minute)); cacheGetErr != nil || !found || got != cache {
		t.Fatalf("GetCacheEntry = (%+v, %t, %v)", got, found, cacheGetErr)
	}
	if _, found, cacheExpiryErr := state.GetCacheEntry(ctx, cache.Key, cache.ExpiresAt); cacheExpiryErr != nil || found {
		t.Fatalf("expired GetCacheEntry = (%t, %v)", found, cacheExpiryErr)
	}
	pins := []workflowruntime.PinnedValue{
		{Key: "pin-b", Value: workflowruntime.ValueRef{Set: ref, Name: "message"}, PinnedAt: base},
		{Key: "pin-a", Value: workflowruntime.ValueRef{Set: ref, Name: "message"}, PinnedAt: base, ExpiresAt: base.Add(time.Hour)},
	}
	for _, pin := range pins {
		if pinPutErr := state.PutPinnedValue(ctx, pin); pinPutErr != nil {
			t.Fatalf("PutPinnedValue(%s): %v", pin.Key, pinPutErr)
		}
	}
	listed, err := state.ListPinnedValues(ctx, base.Add(time.Minute))
	if err != nil || len(listed) != 2 || listed[0].Key != "pin-a" || listed[1].Key != "pin-b" {
		t.Fatalf("ListPinnedValues = %+v, %v", listed, err)
	}
	if _, found, pinExpiryErr := state.GetPinnedValue(ctx, "pin-a", base.Add(2*time.Hour)); pinExpiryErr != nil || found {
		t.Fatalf("expired GetPinnedValue = (%t, %v)", found, pinExpiryErr)
	}

	activation := workflowruntime.ExternalActivationRequest{
		ActivationID: "schedule-nightly", IdempotencyKey: "activation-one",
		RequestedRunID: "activated-run-one", Plan: run.Plan, Inputs: &ref, OccurredAt: base,
	}
	first, outcome, err := state.RecordExternalActivation(ctx, activation)
	if err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("RecordExternalActivation(first) = (%+v, %q, %v)", first, outcome, err)
	}
	replayActivation := activation
	replayActivation.OccurredAt = activation.OccurredAt.In(time.FixedZone("activation", -2*60*60))
	_, outcome, err = state.RecordExternalActivation(ctx, replayActivation)
	if err != nil || outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("RecordExternalActivation(replay) = (%q, %v)", outcome, err)
	}
	secondActivation := activation
	secondActivation.IdempotencyKey = "activation-two"
	secondActivation.RequestedRunID = "activated-run-two"
	second, outcome, err := state.RecordExternalActivation(ctx, secondActivation)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || second.ActivationID != first.ActivationID {
		t.Fatalf("RecordExternalActivation(second firing) = (%+v, %q, %v)", second, outcome, err)
	}
	var activationCount int
	if err := store.DB().QueryRow(`
SELECT COUNT(1) FROM workflow_external_activations WHERE activation_id = ?`, activation.ActivationID).Scan(&activationCount); err != nil {
		t.Fatalf("count activation firings: %v", err)
	}
	if activationCount != 2 {
		t.Fatalf("activation firing count = %d, want 2", activationCount)
	}
	conflictingActivation := activation
	conflictingActivation.RequestedRunID = "activated-run-conflict"
	if _, _, err := state.RecordExternalActivation(ctx, conflictingActivation); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("RecordExternalActivation conflict error = %v", err)
	}
}

func TestWorkflowStateSaveValuesEnforcesSecretAndRetentionInvariantsBeforeWrite(t *testing.T) {
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "hadron.db"))
	ctx := context.Background()
	owner := workflowruntime.ValueOwner{Kind: "classification-test", RunID: "run-classification"}

	none, err := values.NewInline("ephemeral", values.Metadata{
		Producer: values.Producer{Kind: "test", Reference: "none"}, MediaType: "text/plain",
		Redaction: values.RedactionPrivate, Retention: values.RetentionNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, saveErr := state.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner: owner, Values: values.ValueSet{"none": none},
	}); !errors.Is(saveErr, values.ErrRetentionViolation) {
		t.Fatalf("retention-none SaveValues error = %v", saveErr)
	}

	raw := workflowTestValue(t, "raw-secret")
	raw.Redaction = values.RedactionSecret
	if _, saveErr := state.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner: owner, Values: values.ValueSet{"raw": raw},
	}); !errors.Is(saveErr, values.ErrSecretMaterial) {
		t.Fatalf("secret-inline SaveValues error = %v", saveErr)
	}
	var rows int
	if queryErr := store.DB().QueryRowContext(ctx, `SELECT COUNT(1) FROM workflow_value_sets`).Scan(&rows); queryErr != nil || rows != 0 {
		t.Fatalf("invalid values wrote rows=%d err=%v", rows, queryErr)
	}

	artifact, err := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://vault/secret", Digest: values.SHA256Digest([]byte("secret")),
		MediaType: "application/octet-stream", SizeBytes: 6,
		Producer:  values.Producer{Kind: "test", Reference: "artifact"},
		Redaction: values.RedactionSecret, Retention: values.RetentionExternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := state.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner: owner, Values: values.ValueSet{"artifact": artifact},
	})
	if err != nil {
		t.Fatalf("secret ArtifactRef SaveValues: %v", err)
	}
	loaded, err := state.LoadValues(ctx, ref)
	if err != nil || !reflect.DeepEqual(loaded["artifact"], artifact) {
		t.Fatalf("loaded secret ArtifactRef = %#v, %v", loaded, err)
	}
}

func openWorkflowStateTest(t *testing.T, path string) (*Store, *WorkflowStateStore) {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatalf("NewWorkflowStateStore: %v", err)
	}
	return store, state
}

func workflowTestPlan(suffix string) workflowruntime.PlanRef {
	return workflowruntime.PlanRef{
		ID: "plan-" + suffix, Version: "v1", SchemaVersion: "workflow.execution-plan/v1",
		Digest: values.SHA256Digest([]byte("plan-" + suffix)),
	}
}

func workflowTestTime() time.Time {
	return time.Date(2026, time.August, 24, 12, 0, 0, 123456789, time.UTC)
}

func createWorkflowTestRun(t *testing.T, state *WorkflowStateStore, id string, at time.Time) workflowruntime.RunSnapshot {
	t.Helper()
	run, outcome, err := state.CreateRun(context.Background(), workflowruntime.CreateRunRequest{
		ID: workflowruntime.RunID(id), Plan: workflowTestPlan(id), Status: workflowruntime.RunPending,
		StartIdempotencyKey: "start-" + id, CreatedAt: at,
	})
	if err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("CreateRun(%s) = (%+v, %q, %v)", id, run, outcome, err)
	}
	return run
}

func createWorkflowTestNode(t *testing.T, state *WorkflowStateStore, runID workflowruntime.RunID, nodeID string, at time.Time) workflowruntime.NodeInvocationSnapshot {
	t.Helper()
	node, err := state.CreateNodeInvocation(context.Background(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID:     workflowruntime.NodeInvocationID{RunID: runID, NodeID: nodeID},
		Status: workflowruntime.NodePending, CreatedAt: at, UpdatedAt: at,
	}})
	if err != nil {
		t.Fatalf("CreateNodeInvocation(%s): %v", nodeID, err)
	}
	return node
}

func workflowTestValues(t *testing.T, text string) values.ValueSet {
	t.Helper()
	return values.ValueSet{"message": workflowTestValue(t, text)}
}

func workflowTestValue(t *testing.T, text string) values.Value {
	t.Helper()
	value, err := values.NewInline(text, values.Metadata{
		Producer:  values.Producer{Kind: "test", Reference: "fixture"},
		MediaType: "text/plain", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatalf("NewInline(%q): %v", text, err)
	}
	return value
}

func TestWorkflowStateEventSequenceConcurrentPerRun(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "hadron.db")
	firstStore, first := openWorkflowStateTest(t, dbPath)
	secondStore, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open second store: %v", err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, err := NewWorkflowStateStore(secondStore)
	if err != nil {
		t.Fatalf("NewWorkflowStateStore(second): %v", err)
	}
	ctx := context.Background()
	runID := workflowruntime.RunID("run-events")
	stores := []*WorkflowStateStore{first, second}
	const total = 20
	errs := make(chan error, total)
	var group sync.WaitGroup
	for index := 0; index < total; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			_, appendErr := stores[index%len(stores)].AppendEvent(ctx, workflowruntime.AppendEventRequest{
				RunID: runID, Type: fmt.Sprintf("test.%02d", index),
				OccurredAt: workflowTestTime().Add(time.Duration(index) * time.Millisecond),
				Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
			})
			errs <- appendErr
		}(index)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("AppendEvent concurrent: %v", err)
		}
	}
	events, err := first.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if err != nil || len(events) != total {
		t.Fatalf("ListEvents = %d events, %v", len(events), err)
	}
	for index, event := range events {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("event[%d].Sequence = %d", index, event.Sequence)
		}
	}
	if _, updateErr := firstStore.DB().Exec(`
UPDATE workflow_events SET event_type = 'mutated' WHERE run_id = ? AND sequence = 1`, runID); updateErr == nil {
		t.Fatal("workflow event update unexpectedly succeeded")
	}
	if _, deleteErr := firstStore.DB().Exec(`
DELETE FROM workflow_events WHERE run_id = ? AND sequence = 1`, runID); deleteErr == nil {
		t.Fatal("workflow event delete unexpectedly succeeded")
	}
	preserved, err := first.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if err != nil || len(preserved) != total || preserved[0].Type != events[0].Type {
		t.Fatalf("append-only trigger damaged event history: %+v, %v", preserved, err)
	}
}
