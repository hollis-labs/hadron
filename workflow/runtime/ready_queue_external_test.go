package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
)

func TestReadyQueueDefaultsToFIFOWithStableInvocationTieBreak(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	oldest := invocationID("run-z", "oldest")
	tieIterationA := workflowruntime.NodeInvocationID{RunID: "run-a", NodeID: "expand", Iteration: "a"}
	tieIterationB := workflowruntime.NodeInvocationID{RunID: "run-a", NodeID: "expand", Iteration: "b"}
	tieNode := invocationID("run-a", "later-node")
	tieRun := invocationID("run-b", "first-node")
	newest := invocationID("run-a", "newest")

	createNode(t, store, newest, workflowruntime.NodeReady, 100, now.Add(-time.Minute))
	createNode(t, store, tieRun, workflowruntime.NodeReady, 20, now.Add(-2*time.Minute))
	createNode(t, store, tieNode, workflowruntime.NodeReady, 30, now.Add(-2*time.Minute))
	createNode(t, store, tieIterationB, workflowruntime.NodeReady, 40, now.Add(-2*time.Minute))
	createNode(t, store, tieIterationA, workflowruntime.NodeReady, 50, now.Add(-2*time.Minute))
	createNode(t, store, oldest, workflowruntime.NodeReady, -10, now.Add(-3*time.Minute))

	queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	want := []workflowruntime.NodeInvocationID{oldest, tieIterationA, tieIterationB, tieNode, tieRun, newest}
	for i, expected := range want {
		request := readyClaimRequest(now, i)
		claimed, ok, err := queue.ClaimNext(ctx, request)
		if err != nil || !ok || claimed.Candidate.InvocationID != expected {
			t.Fatalf("claim %d = %#v, %v, %v; want %v", i, claimed, ok, err, expected)
		}
		retireReadyClaim(t, store, claimed, now)
	}
}

func TestReadyQueuePolicyCanSelectPriorityAndPerRunFairness(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	t.Run("priority", func(t *testing.T) {
		store := runtimetest.NewStore()
		low := invocationID("run-a", "low")
		high := invocationID("run-a", "high")
		createNode(t, store, low, workflowruntime.NodeReady, 1, now.Add(-time.Minute))
		createNode(t, store, high, workflowruntime.NodeReady, 100, now)
		policy := workflowruntime.ReadyQueuePolicyFunc(func(_ context.Context, candidates []workflowruntime.ReadyCandidate) ([]workflowruntime.NodeInvocationID, error) {
			sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Priority > candidates[j].Priority })
			result := make([]workflowruntime.NodeInvocationID, len(candidates))
			for i := range candidates {
				result[i] = candidates[i].InvocationID
			}
			return result, nil
		})
		claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, policy).ClaimNext(context.Background(), readyClaimRequest(now, 1))
		if err != nil || !ok || claim.Candidate.InvocationID != high {
			t.Fatalf("priority claim = %#v, %v, %v", claim, ok, err)
		}
	})

	t.Run("per_run_selection", func(t *testing.T) {
		store := runtimetest.NewStore()
		runA := invocationID("run-a", "first")
		runB := invocationID("run-b", "first")
		createNode(t, store, runA, workflowruntime.NodeReady, 0, now.Add(-time.Minute))
		createNode(t, store, runB, workflowruntime.NodeReady, 0, now)
		policy := workflowruntime.ReadyQueuePolicyFunc(func(_ context.Context, candidates []workflowruntime.ReadyCandidate) ([]workflowruntime.NodeInvocationID, error) {
			for _, candidate := range candidates {
				if candidate.InvocationID.RunID == "run-b" {
					return []workflowruntime.NodeInvocationID{candidate.InvocationID}, nil
				}
			}
			return nil, nil
		})
		claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, policy).ClaimNext(context.Background(), readyClaimRequest(now, 2))
		if err != nil || !ok || claim.Candidate.InvocationID != runB {
			t.Fatalf("per-run claim = %#v, %v, %v", claim, ok, err)
		}
	})
}

func TestReadyQueueReplaysOwnLiveClaimAndSkipsOtherLiveClaims(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-replay", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, now.Add(-time.Minute))
	queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	request := readyClaimRequest(now, 1)

	first, ok, err := queue.ClaimNext(ctx, request)
	if err != nil || !ok || first.Replayed {
		t.Fatalf("first claim = %#v, %v, %v", first, ok, err)
	}
	replay, ok, err := queue.ClaimNext(ctx, request)
	if err != nil || !ok || !replay.Replayed || replay.Lease != first.Lease {
		t.Fatalf("claim replay = %#v, %v, %v", replay, ok, err)
	}

	other := readyClaimRequest(now, 2)
	if claim, acquired, otherErr := queue.ClaimNext(ctx, other); otherErr != nil || acquired || claim != (workflowruntime.ReadyClaim{}) {
		t.Fatalf("other live claim = %#v, %v, %v", claim, acquired, otherErr)
	}
	if releaseErr := queue.Release(ctx, workflowruntime.ReleaseClaimRequest{
		InvocationID: id, Owner: first.Lease.Owner, Token: first.Lease.Token,
		Generation: first.Lease.Generation, Now: now,
	}); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	if _, _, staleErr := queue.ClaimNext(ctx, request); !errors.Is(staleErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("released claim replay should be rejected, got %v", staleErr)
	}
	newRequest := readyClaimRequest(now, 3)
	replacement, ok, err := queue.ClaimNext(ctx, newRequest)
	if err != nil || !ok || replacement.Lease.Generation != 2 {
		t.Fatalf("post-release replacement = %#v, %v, %v", replacement, ok, err)
	}
}

func TestReadyQueueReplayPrecedesChangedPolicySelection(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	firstID := invocationID("run-policy-replay", "first")
	secondID := invocationID("run-policy-replay", "second")
	createNode(t, store, firstID, workflowruntime.NodeReady, 0, now.Add(-time.Minute))
	createNode(t, store, secondID, workflowruntime.NodeReady, 0, now)
	var calls atomic.Int64
	policy := workflowruntime.ReadyQueuePolicyFunc(func(_ context.Context, candidates []workflowruntime.ReadyCandidate) ([]workflowruntime.NodeInvocationID, error) {
		if calls.Add(1) == 1 {
			return []workflowruntime.NodeInvocationID{firstID}, nil
		}
		return []workflowruntime.NodeInvocationID{secondID}, nil
	})
	queue := workflowruntime.NewReadyQueueCoordinator(store, policy)
	request := readyClaimRequest(now, 1)
	first, ok, err := queue.ClaimNext(ctx, request)
	if err != nil || !ok || first.Candidate.InvocationID != firstID || first.Replayed {
		t.Fatalf("first claim = %#v, %v, %v", first, ok, err)
	}
	replay, ok, err := queue.ClaimNext(ctx, request)
	if err != nil || !ok || replay.Candidate.InvocationID != firstID || !replay.Replayed {
		t.Fatalf("policy-changed replay = %#v, %v, %v", replay, ok, err)
	}
	second, loadErr := store.LoadNodeInvocation(ctx, secondID)
	if loadErr != nil || second.Lease != nil || second.ClaimGeneration != 0 {
		t.Fatalf("replay acquired policy's new selection: %#v, %v", second, loadErr)
	}
}

func TestReadyQueueRestartReclaimsExpiredLease(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	start := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-restart", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, start.Add(-time.Minute))
	firstQueue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	first, ok, err := firstQueue.ClaimNext(ctx, readyClaimRequest(start, 1))
	if err != nil || !ok || first.Lease.Generation != 1 {
		t.Fatalf("initial claim = %#v, %v, %v", first, ok, err)
	}

	// A fresh adapter view and coordinator retain no queue state; both observe
	// the same persisted backing store after the first worker disappears.
	restartedStore := &stateStoreHooks{StateStore: store}
	restartedQueue := workflowruntime.NewReadyQueueCoordinator(restartedStore, nil)
	restartTime := start.Add(2 * time.Minute)
	replacement, ok, err := restartedQueue.ClaimNext(ctx, readyClaimRequest(restartTime, 2))
	if err != nil || !ok || replacement.Candidate.InvocationID != id || replacement.Lease.Generation != 2 {
		t.Fatalf("restart replacement = %#v, %v, %v", replacement, ok, err)
	}
	loaded, loadErr := store.LoadNodeInvocation(ctx, id)
	if loadErr != nil || loaded.ClaimGeneration != 2 || loaded.Generation != 4 || loaded.Lease.Token != replacement.Lease.Token {
		t.Fatalf("persisted replacement = %#v, %v", loaded, loadErr)
	}
}

func TestReadyQueueRenewAndReleaseRemainFenced(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-fenced", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, now)
	queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	claim, ok, err := queue.ClaimNext(ctx, readyClaimRequest(now, 1))
	if err != nil || !ok {
		t.Fatalf("claim = %#v, %v, %v", claim, ok, err)
	}
	if _, renewErr := queue.Renew(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: id, Owner: claim.Lease.Owner, Token: "wrong", Generation: claim.Lease.Generation,
		Now: now.Add(30 * time.Second), LeaseUntil: now.Add(2 * time.Minute),
	}); !errors.Is(renewErr, workflowruntime.ErrClaimMismatch) {
		t.Fatalf("wrong-token renewal = %v", renewErr)
	}
	renewed, err := queue.Renew(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: id, Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation,
		Now: now.Add(30 * time.Second), LeaseUntil: now.Add(2 * time.Minute),
	})
	if err != nil || !renewed.ExpiresAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("renewed = %#v, %v", renewed, err)
	}
	if err := queue.Release(ctx, workflowruntime.ReleaseClaimRequest{
		InvocationID: id, Owner: claim.Lease.Owner, Token: "wrong", Generation: claim.Lease.Generation,
		Now: now.Add(time.Minute),
	}); !errors.Is(err, workflowruntime.ErrClaimMismatch) {
		t.Fatalf("wrong-token release = %v", err)
	}
	if err := queue.Release(ctx, workflowruntime.ReleaseClaimRequest{
		InvocationID: id, Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation,
		Now: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("matching release = %v", err)
	}
}

func TestReadyQueueCanceledEmptyAndErrorPaths(t *testing.T) {
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	t.Run("empty", func(t *testing.T) {
		claim, ok, err := workflowruntime.NewReadyQueueCoordinator(runtimetest.NewStore(), nil).ClaimNext(
			context.Background(), readyClaimRequest(now, 1),
		)
		if err != nil || ok || claim != (workflowruntime.ReadyClaim{}) {
			t.Fatalf("empty queue = %#v, %v, %v", claim, ok, err)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := atomic.Int64{}
		store := &stateStoreHooks{StateStore: runtimetest.NewStore()}
		store.recovery = func(ctx context.Context, query workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error) {
			calls.Add(1)
			return store.StateStore.Recovery(ctx, query)
		}
		_, _, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(ctx, readyClaimRequest(now, 1))
		if !errors.Is(err, context.Canceled) || calls.Load() != 0 {
			t.Fatalf("canceled claim err=%v recovery calls=%d", err, calls.Load())
		}
	})

	t.Run("nil_context", func(t *testing.T) {
		queue := workflowruntime.NewReadyQueueCoordinator(runtimetest.NewStore(), nil)
		var nilContext context.Context
		if _, _, err := queue.ClaimNext(nilContext, readyClaimRequest(now, 1)); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
			t.Fatalf("nil ClaimNext context = %v", err)
		}
		if _, err := queue.Renew(nilContext, workflowruntime.RenewLeaseRequest{}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
			t.Fatalf("nil Renew context = %v", err)
		}
		if err := queue.Release(nilContext, workflowruntime.ReleaseClaimRequest{}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
			t.Fatalf("nil Release context = %v", err)
		}
	})

	t.Run("store_error", func(t *testing.T) {
		failure := errors.New("recovery unavailable")
		store := &stateStoreHooks{StateStore: runtimetest.NewStore(), recovery: func(context.Context, workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error) {
			return workflowruntime.RecoverySnapshot{}, failure
		}}
		_, _, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(context.Background(), readyClaimRequest(now, 1))
		if !errors.Is(err, failure) {
			t.Fatalf("store failure not surfaced: %v", err)
		}
	})

	t.Run("claim_error", func(t *testing.T) {
		store := runtimetest.NewStore()
		createNode(t, store, invocationID("run-error", "work"), workflowruntime.NodeReady, 0, now)
		failure := errors.New("claim unavailable")
		wrapped := &stateStoreHooks{StateStore: store, claim: func(context.Context, workflowruntime.ClaimNodeRequest) (workflowruntime.ClaimResult, error) {
			return workflowruntime.ClaimResult{}, failure
		}}
		_, _, err := workflowruntime.NewReadyQueueCoordinator(wrapped, nil).ClaimNext(context.Background(), readyClaimRequest(now, 1))
		if !errors.Is(err, failure) {
			t.Fatalf("claim failure not surfaced: %v", err)
		}
	})

	t.Run("malformed_claim_result", func(t *testing.T) {
		store := runtimetest.NewStore()
		createNode(t, store, invocationID("run-malformed", "work"), workflowruntime.NodeReady, 0, now)
		wrapped := &stateStoreHooks{StateStore: store, claim: func(context.Context, workflowruntime.ClaimNodeRequest) (workflowruntime.ClaimResult, error) {
			return workflowruntime.ClaimResult{Acquired: true}, nil
		}}
		claim, ok, err := workflowruntime.NewReadyQueueCoordinator(wrapped, nil).ClaimNext(context.Background(), readyClaimRequest(now, 1))
		if !errors.Is(err, workflowruntime.ErrInvalidRecord) || ok || claim != (workflowruntime.ReadyClaim{}) {
			t.Fatalf("malformed claim result = %#v, %v, %v", claim, ok, err)
		}
	})

	t.Run("invalid_policy_selection", func(t *testing.T) {
		store := runtimetest.NewStore()
		id := invocationID("run-policy", "work")
		createNode(t, store, id, workflowruntime.NodeReady, 0, now)
		policy := workflowruntime.ReadyQueuePolicyFunc(func(context.Context, []workflowruntime.ReadyCandidate) ([]workflowruntime.NodeInvocationID, error) {
			return []workflowruntime.NodeInvocationID{invocationID("run-policy", "injected")}, nil
		})
		_, _, err := workflowruntime.NewReadyQueueCoordinator(store, policy).ClaimNext(context.Background(), readyClaimRequest(now, 1))
		if !errors.Is(err, workflowruntime.ErrInvalidRecord) {
			t.Fatalf("invalid policy result = %v", err)
		}
		loaded, loadErr := store.LoadNodeInvocation(context.Background(), id)
		if loadErr != nil || loaded.Lease != nil {
			t.Fatalf("policy error mutated candidate: %#v, %v", loaded, loadErr)
		}
	})
}

func TestClaimNodeReturnsDurableNegativeForNonReadyAfterCAS(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-pending", "work")
	createNode(t, store, id, workflowruntime.NodePending, 0, now)
	request := workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: 1, Owner: "worker", Token: "token",
		IdempotencyKey: "pending-claim", Now: now, LeaseUntil: now.Add(time.Minute),
	}
	if _, err := store.ClaimNode(ctx, request); !errors.Is(err, workflowruntime.ErrCASMismatch) {
		t.Fatalf("stale non-ready claim must fail CAS, got %v", err)
	}
	request.ExpectedClaimGeneration = 0
	result, err := store.ClaimNode(ctx, request)
	if err != nil || result.Acquired || result.Replayed || result.Lease != nil {
		t.Fatalf("non-ready claim = %#v, %v", result, err)
	}
	replay, err := store.ClaimNode(ctx, request)
	if err != nil || replay.Acquired || !replay.Replayed || replay.Lease != nil {
		t.Fatalf("non-ready replay = %#v, %v", replay, err)
	}
	conflict := request
	conflict.Token = "different"
	if _, conflictErr := store.ClaimNode(ctx, conflict); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("negative-result idempotency conflict = %v", conflictErr)
	}
	loaded, err := store.LoadNodeInvocation(ctx, id)
	if err != nil || loaded.Generation != 1 || loaded.ClaimGeneration != 0 || loaded.Lease != nil {
		t.Fatalf("negative claim mutated node: %#v, %v", loaded, err)
	}
}

func TestReadyQueueHighContentionPreventsDuplicateOwnership(t *testing.T) {
	ctx := context.Background()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	id := invocationID("run-contention", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, now)
	const workers = 64
	barrier := newRecoveryBarrier(store, workers)

	var acquired atomic.Int64
	var unexpected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			queue := workflowruntime.NewReadyQueueCoordinator(barrier, nil)
			request := readyClaimRequest(now, i)
			claim, ok, err := queue.ClaimNext(ctx, request)
			if err != nil {
				unexpected.Add(1)
				return
			}
			if ok {
				if claim.Candidate.InvocationID != id {
					unexpected.Add(1)
					return
				}
				acquired.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if acquired.Load() != 1 || unexpected.Load() != 0 {
		t.Fatalf("acquired=%d unexpected=%d", acquired.Load(), unexpected.Load())
	}
}

func readyClaimRequest(now time.Time, sequence int) workflowruntime.ReadyClaimRequest {
	return workflowruntime.ReadyClaimRequest{
		Owner: fmt.Sprintf("worker-%d", sequence), Token: fmt.Sprintf("token-%d", sequence),
		IdempotencyKey: fmt.Sprintf("claim-cycle-%d", sequence), Now: now, LeaseUntil: now.Add(time.Minute),
	}
}

func retireReadyClaim(t *testing.T, store workflowruntime.StateStore, claim workflowruntime.ReadyClaim, at time.Time) {
	t.Helper()
	node, err := store.LoadNodeInvocation(context.Background(), claim.Candidate.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
		InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeSkipped,
		Claim: &workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation},
		At:    at,
	})
	if err != nil {
		t.Fatalf("retire %v: %v", node.ID, err)
	}
}

type stateStoreHooks struct {
	workflowruntime.StateStore
	recovery func(context.Context, workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error)
	claim    func(context.Context, workflowruntime.ClaimNodeRequest) (workflowruntime.ClaimResult, error)
}

func (s *stateStoreHooks) Recovery(ctx context.Context, query workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error) {
	if s.recovery != nil {
		return s.recovery(ctx, query)
	}
	return s.StateStore.Recovery(ctx, query)
}

func (s *stateStoreHooks) ClaimNode(ctx context.Context, request workflowruntime.ClaimNodeRequest) (workflowruntime.ClaimResult, error) {
	if s.claim != nil {
		return s.claim(ctx, request)
	}
	return s.StateStore.ClaimNode(ctx, request)
}

type recoveryBarrier struct {
	workflowruntime.StateStore
	total   int64
	arrived atomic.Int64
	release chan struct{}
	once    sync.Once
}

func newRecoveryBarrier(store workflowruntime.StateStore, total int) *recoveryBarrier {
	return &recoveryBarrier{StateStore: store, total: int64(total), release: make(chan struct{})}
}

func (s *recoveryBarrier) Recovery(ctx context.Context, query workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error) {
	snapshot, err := s.StateStore.Recovery(ctx, query)
	if err != nil {
		return workflowruntime.RecoverySnapshot{}, err
	}
	if s.arrived.Add(1) == s.total {
		s.once.Do(func() { close(s.release) })
	}
	select {
	case <-ctx.Done():
		return workflowruntime.RecoverySnapshot{}, ctx.Err()
	case <-s.release:
		return snapshot, nil
	}
}
