package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestClaimLeaseCASFencingAndExpiry(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Now()
	id := invocationID("run-1", "execute")
	createNode(t, store, id, workflowruntime.NodeReady, 0, now)

	claim := workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: 0, Owner: "worker-a", Token: "token-a",
		IdempotencyKey: "claim-a", Now: now, LeaseUntil: now.Add(time.Minute),
	}
	result, err := store.ClaimNode(ctx, claim)
	if err != nil || !result.Acquired || result.Replayed || result.Lease.Generation != 1 {
		t.Fatalf("ClaimNode = %#v, %v", result, err)
	}
	claimReplay := claim
	claimReplay.Now = equivalentInstant(claim.Now)
	claimReplay.LeaseUntil = equivalentInstant(claim.LeaseUntil)
	replay, err := store.ClaimNode(ctx, claimReplay)
	if err != nil || !replay.Acquired || !replay.Replayed || replay.Lease.Token != "token-a" {
		t.Fatalf("claim replay = %#v, %v", replay, err)
	}
	changed := claim
	changed.Token = "different"
	if _, conflictErr := store.ClaimNode(ctx, changed); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("expected claim idempotency conflict, got %v", conflictErr)
	}
	if _, staleErr := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: 0, Owner: "worker-b", Token: "token-b",
		IdempotencyKey: "stale", Now: now, LeaseUntil: now.Add(time.Minute),
	}); !errors.Is(staleErr, workflowruntime.ErrCASMismatch) {
		t.Fatalf("expected stale claim CAS, got %v", staleErr)
	}
	contended, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: 1, Owner: "worker-b", Token: "token-b",
		IdempotencyKey: "contended", Now: now.Add(30 * time.Second), LeaseUntil: now.Add(2 * time.Minute),
	})
	if err != nil || contended.Acquired || contended.Lease != nil {
		t.Fatalf("contended claim leaked/acquired lease: %#v, %v", contended, err)
	}
	contendedReplay, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: 1, Owner: "worker-b", Token: "token-b",
		IdempotencyKey: "contended", Now: now.Add(30 * time.Second), LeaseUntil: now.Add(2 * time.Minute),
	})
	if err != nil || contendedReplay.Acquired || !contendedReplay.Replayed || contendedReplay.Lease != nil {
		t.Fatalf("contended replay changed outcome: %#v, %v", contendedReplay, err)
	}

	renewed, err := store.RenewNodeLease(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: id, Owner: "worker-a", Token: "token-a", Generation: 1,
		Now: now.Add(30 * time.Second), LeaseUntil: now.Add(2 * time.Minute),
	})
	if err != nil || !renewed.ExpiresAt.Equal(now.Add(2*time.Minute)) || renewed.Generation != 1 {
		t.Fatalf("RenewNodeLease = %#v, %v", renewed, err)
	}
	withEquivalentLease, err := store.LoadNodeInvocation(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	withEquivalentLease.Lease.ExpiresAt = equivalentInstant(withEquivalentLease.Lease.ExpiresAt)
	withEquivalentLease.UpdatedAt = equivalentInstant(withEquivalentLease.UpdatedAt)
	savedWithEquivalentLease, err := store.SaveNodeInvocation(ctx, workflowruntime.SaveNodeInvocationRequest{
		Snapshot: withEquivalentLease, ExpectedGeneration: withEquivalentLease.Generation,
	})
	if err != nil || savedWithEquivalentLease.Generation != withEquivalentLease.Generation+1 {
		t.Fatalf("SaveNodeInvocation with equivalent lease = %#v, %v", savedWithEquivalentLease, err)
	}
	if _, mismatchErr := store.RenewNodeLease(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: id, Owner: "worker-a", Token: "wrong", Generation: 1,
		Now: now.Add(time.Minute), LeaseUntil: now.Add(3 * time.Minute),
	}); !errors.Is(mismatchErr, workflowruntime.ErrClaimMismatch) {
		t.Fatalf("expected claim mismatch, got %v", mismatchErr)
	}
	if _, shortenErr := store.RenewNodeLease(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: id, Owner: "worker-a", Token: "token-a", Generation: 1,
		Now: now.Add(time.Minute), LeaseUntil: now.Add(90 * time.Second),
	}); !errors.Is(shortenErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("expected shortening renewal rejection, got %v", shortenErr)
	}
	if _, regressionErr := store.RenewNodeLease(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: id, Owner: "worker-a", Token: "token-a", Generation: 1,
		Now: now.Add(15 * time.Second), LeaseUntil: now.Add(3 * time.Minute),
	}); !errors.Is(regressionErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("expected renewal time regression rejection, got %v", regressionErr)
	}
	if _, expiryErr := store.RenewNodeLease(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: id, Owner: "worker-a", Token: "token-a", Generation: 1,
		Now: now.Add(2 * time.Minute), LeaseUntil: now.Add(3 * time.Minute),
	}); !errors.Is(expiryErr, workflowruntime.ErrLeaseExpired) {
		t.Fatalf("expected lease expired, got %v", expiryErr)
	}

	replacement, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: 1, Owner: "worker-b", Token: "token-b",
		IdempotencyKey: "claim-b", Now: now.Add(2 * time.Minute), LeaseUntil: now.Add(3 * time.Minute),
	})
	if err != nil || !replacement.Acquired || replacement.Lease.Generation != 2 {
		t.Fatalf("replacement claim = %#v, %v", replacement, err)
	}
	if releaseErr := store.ReleaseNodeClaim(ctx, workflowruntime.ReleaseClaimRequest{
		InvocationID: id, Owner: "worker-b", Token: "wrong", Generation: 2, Now: now.Add(2 * time.Minute),
	}); !errors.Is(releaseErr, workflowruntime.ErrClaimMismatch) {
		t.Fatalf("expected release mismatch, got %v", releaseErr)
	}
	if releaseErr := store.ReleaseNodeClaim(ctx, workflowruntime.ReleaseClaimRequest{
		InvocationID: id, Owner: "worker-b", Token: "token-b", Generation: 2, Now: now.Add(2 * time.Minute),
	}); releaseErr != nil {
		t.Fatalf("ReleaseNodeClaim: %v", releaseErr)
	}
	loaded, err := store.LoadNodeInvocation(ctx, id)
	if err != nil || loaded.Lease != nil || loaded.ClaimGeneration != 2 || !loaded.UpdatedAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("released snapshot = %#v, %v", loaded, err)
	}
}

func TestClaimRejectsNodeTimestampRegression(t *testing.T) {
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-time", "execute")
	createNode(t, store, id, workflowruntime.NodeReady, 0, now)
	_, err := store.ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{
		InvocationID: id, Owner: "worker", Token: "token", IdempotencyKey: "claim-time",
		Now: now.Add(-time.Second), LeaseUntil: now.Add(time.Minute),
	})
	if !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("expected regressing claim time rejection, got %v", err)
	}
	loaded, loadErr := store.LoadNodeInvocation(context.Background(), id)
	if loadErr != nil || loaded.Lease != nil || loaded.Generation != 2 {
		t.Fatalf("rejected claim mutated snapshot: %#v, %v", loaded, loadErr)
	}
}

func TestConcurrentDuplicateClaimPrevention(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-concurrent", "execute")
	createNode(t, store, id, workflowruntime.NodeReady, 0, now)

	var acquired atomic.Int64
	var unexpected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
				InvocationID: id, ExpectedClaimGeneration: 0,
				Owner: fmt.Sprintf("worker-%d", i), Token: fmt.Sprintf("token-%d", i),
				IdempotencyKey: fmt.Sprintf("claim-%d", i), Now: now, LeaseUntil: now.Add(time.Minute),
			})
			if err == nil && result.Acquired {
				acquired.Add(1)
				return
			}
			if !errors.Is(err, workflowruntime.ErrCASMismatch) {
				unexpected.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if acquired.Load() != 1 || unexpected.Load() != 0 {
		t.Fatalf("acquired=%d unexpected=%d", acquired.Load(), unexpected.Load())
	}
}

func TestAppendOnlyEventsAreAtomicOrderedAndImmutable(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	const count = 80
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.AppendEvent(ctx, workflowruntime.AppendEventRequest{
				RunID: "run-events", Type: "node.observed", OccurredAt: now.Add(time.Duration(i) * time.Millisecond),
				Attributes: map[string]string{"worker": fmt.Sprint(i)},
				Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
			})
			if err != nil {
				t.Errorf("AppendEvent: %v", err)
			}
		}(i)
	}
	wg.Wait()
	events, err := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: "run-events"})
	if err != nil || len(events) != count {
		t.Fatalf("ListEvents count=%d err=%v", len(events), err)
	}
	for i, event := range events {
		if event.Sequence != uint64(i+1) {
			t.Fatalf("event[%d] sequence=%d", i, event.Sequence)
		}
	}
	events[0].Attributes["worker"] = "mutated"
	after, err := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: "run-events", AfterSequence: 0, Limit: 1})
	if err != nil || after[0].Attributes["worker"] == "mutated" {
		t.Fatalf("event storage was aliased: %#v, %v", after, err)
	}
	other, err := store.AppendEvent(ctx, workflowruntime.AppendEventRequest{
		RunID: "other-run", Type: "run.started", OccurredAt: now,
		Redaction: values.RedactionPublic, Retention: values.RetentionRun,
	})
	if err != nil || other.Sequence != 1 {
		t.Fatalf("per-run sequence = %#v, %v", other, err)
	}
}

func TestRecoveryFiltersAndOrdering(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	for _, item := range []struct {
		id     workflowruntime.RunID
		status workflowruntime.RunStatus
	}{
		{"run-b", workflowruntime.RunRunning},
		{"run-a", workflowruntime.RunPending},
		{"run-done", workflowruntime.RunSucceeded},
	} {
		_, _, err := store.CreateRun(ctx, workflowruntime.CreateRunRequest{
			ID: item.id, Plan: testPlan(), Status: workflowruntime.RunPending, StartIdempotencyKey: "start-" + string(item.id), CreatedAt: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		if item.status != workflowruntime.RunPending {
			running, transitionErr := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
				RunID: item.id, ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: now,
			})
			if transitionErr != nil {
				t.Fatal(transitionErr)
			}
			if item.status != workflowruntime.RunRunning {
				if _, transitionErr = store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{
					RunID: item.id, ExpectedGeneration: running.Snapshot.Generation, To: item.status, At: now.Add(time.Second),
				}); transitionErr != nil {
					t.Fatal(transitionErr)
				}
			}
		}
	}
	readyLow := invocationID("run-a", "low")
	readyHigh := invocationID("run-a", "high")
	waiting := invocationID("run-b", "waiting")
	expired := invocationID("run-b", "expired")
	createNode(t, store, readyLow, workflowruntime.NodeReady, 1, now)
	createNode(t, store, readyHigh, workflowruntime.NodeReady, 10, now)
	waitingCreatedAt := now.Add(-3 * time.Minute)
	createNode(t, store, waiting, workflowruntime.NodeReady, 0, waitingCreatedAt)
	createNode(t, store, expired, workflowruntime.NodeReady, 0, waitingCreatedAt)
	_, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: readyLow, Owner: "live", Token: "live-token", IdempotencyKey: "live-claim",
		Now: now, LeaseUntil: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	expiredClaim, err := store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: waiting, Owner: "expired", Token: "expired-token", IdempotencyKey: "expired-claim",
		Now: now.Add(-2 * time.Minute), LeaseUntil: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: expired, Owner: "expired-worker", Token: "expired-work-token", IdempotencyKey: "expired-work-claim",
		Now: now.Add(-2 * time.Minute), LeaseUntil: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: waiting, ExpectedNodeGeneration: 3,
		Claim:    workflowruntime.ClaimProof{Owner: expiredClaim.Lease.Owner, Token: expiredClaim.Lease.Token, Generation: expiredClaim.Lease.Generation},
		Executor: testExecutor(), At: now.Add(-2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{
		InvocationID: waiting, ExpectedGeneration: started.Node.Generation, To: workflowruntime.NodeWaiting,
		Claim: &workflowruntime.ClaimProof{Owner: expiredClaim.Lease.Owner, Token: expiredClaim.Lease.Token, Generation: expiredClaim.Lease.Generation},
		At:    now.Add(-2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}

	recovery, err := store.Recovery(ctx, workflowruntime.RecoveryQuery{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.ActiveRuns) != 2 || recovery.ActiveRuns[0].ID != "run-a" || recovery.ActiveRuns[1].ID != "run-b" {
		t.Fatalf("active runs = %#v", recovery.ActiveRuns)
	}
	if len(recovery.Ready) != 3 || recovery.Ready[0].ID != readyHigh ||
		recovery.Ready[1].ID != readyLow || recovery.Ready[2].ID != expired {
		t.Fatalf("ready order = %#v", recovery.Ready)
	}
	if len(recovery.Waiting) != 1 || len(recovery.Leased) != 1 || len(recovery.ExpiredLeases) != 1 {
		t.Fatalf("recovery categories = %#v", recovery)
	}
	if recovery.Waiting[0].ID != waiting || recovery.Waiting[0].Lease != nil ||
		recovery.ExpiredLeases[0].ID == waiting || recovery.Leased[0].ID == waiting {
		t.Fatalf("waiting work must not retain or recover as a lease: %#v", recovery)
	}
	filtered, err := store.Recovery(ctx, workflowruntime.RecoveryQuery{RunID: "run-a", Now: now, Limit: 1})
	if err != nil || len(filtered.ActiveRuns) != 1 || len(filtered.Ready) != 1 || filtered.Ready[0].ID != readyHigh || len(filtered.ExpiredLeases) != 0 {
		t.Fatalf("filtered recovery = %#v, %v", filtered, err)
	}
	filtered.Ready[0].Lease = nil
	loaded, err := store.LoadNodeInvocation(ctx, readyHigh)
	if err != nil || loaded.ID != readyHigh {
		t.Fatalf("recovery result mutation affected store: %#v, %v", loaded, err)
	}
}

func createNode(t *testing.T, store workflowruntime.StateStore, id workflowruntime.NodeInvocationID, status workflowruntime.NodeStatus, priority int, now time.Time) {
	t.Helper()
	if _, loadErr := store.LoadRun(context.Background(), id.RunID); errors.Is(loadErr, workflowruntime.ErrNotFound) {
		if _, _, createErr := store.CreateRun(context.Background(), workflowruntime.CreateRunRequest{ID: id.RunID, Plan: testPlan(), Status: workflowruntime.RunPending, StartIdempotencyKey: "fixture-run-" + string(id.RunID), CreatedAt: now}); createErr != nil {
			t.Fatalf("CreateRun(%s): %v", id.RunID, createErr)
		}
	} else if loadErr != nil {
		t.Fatalf("LoadRun(%s): %v", id.RunID, loadErr)
	}
	_, err := store.CreateNodeInvocation(context.Background(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodePending, Priority: priority, CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatalf("CreateNodeInvocation(%v): %v", id, err)
	}
	if status != workflowruntime.NodePending {
		if status != workflowruntime.NodeReady {
			t.Fatalf("createNode helper only supports pending or ready, got %q", status)
		}
		if _, err = store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
			InvocationID: id, ExpectedGeneration: 1, To: status, At: now,
		}); err != nil {
			t.Fatalf("TransitionNode(%v): %v", id, err)
		}
	}
}
