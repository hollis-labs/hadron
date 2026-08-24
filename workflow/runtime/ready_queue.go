package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"
)

// ReadyCandidate is the immutable scheduling metadata exposed to a host
// policy. Claim generations and leases remain owned by the StateStore and
// cannot be rewritten by policy code.
type ReadyCandidate struct {
	InvocationID NodeInvocationID
	Priority     int
	CreatedAt    time.Time
}

// ReadyQueuePolicy may reorder or select from the FIFO candidate list. Every
// returned identity must occur exactly once in candidates. Returning no
// identities elects not to acquire new work from the current persisted
// snapshot. A matching live exact idempotency replay is prioritized ahead of
// returned identities because it is already owned work, not a new scheduling
// decision.
type ReadyQueuePolicy interface {
	SelectReady(context.Context, []ReadyCandidate) ([]NodeInvocationID, error)
}

// ReadyQueuePolicyFunc adapts a function to ReadyQueuePolicy.
type ReadyQueuePolicyFunc func(context.Context, []ReadyCandidate) ([]NodeInvocationID, error)

// SelectReady implements ReadyQueuePolicy.
func (f ReadyQueuePolicyFunc) SelectReady(ctx context.Context, candidates []ReadyCandidate) ([]NodeInvocationID, error) {
	return f(ctx, candidates)
}

// ReadyClaimRequest supplies host-owned claim identity and lease timing for
// one persisted ready-queue scan. RunID optionally restricts the scan.
// IdempotencyKey is scoped deterministically to each candidate before the
// underlying StateStore is called. Retrying the same request may replay its
// live acquired lease. A new acquisition after release or expiry requires a
// new IdempotencyKey.
type ReadyClaimRequest struct {
	RunID          RunID
	Owner          string
	Token          string
	IdempotencyKey string
	Now            time.Time
	LeaseUntil     time.Time
}

// ReadyClaim identifies the candidate and lease acquired by ClaimNext.
type ReadyClaim struct {
	Candidate ReadyCandidate
	Lease     ClaimLease
	Replayed  bool
}

// ReadyQueueCoordinator discovers and claims work exclusively through a
// StateStore. It retains no candidate or ownership state between calls.
type ReadyQueueCoordinator struct {
	store  StateStore
	policy ReadyQueuePolicy
}

// NewReadyQueueCoordinator constructs a stateless durable-ready-queue
// coordinator. A nil policy preserves FIFO ordering.
func NewReadyQueueCoordinator(store StateStore, policy ReadyQueuePolicy) *ReadyQueueCoordinator {
	return &ReadyQueueCoordinator{store: store, policy: policy}
}

// ClaimNext discovers persisted ready candidates, applies FIFO and optional
// host selection, then claims at most one candidate under claim-generation
// CAS. Expected lost CAS and negative claim races are skipped; all other store
// errors are returned. The boolean is false when no acquisition or live exact
// replay is available, in which case the returned ReadyClaim is empty.
func (c *ReadyQueueCoordinator) ClaimNext(ctx context.Context, request ReadyClaimRequest) (ReadyClaim, bool, error) {
	if ctx == nil {
		return ReadyClaim{}, false, fmt.Errorf("%w: ready queue context is required", ErrInvalidRecord)
	}
	if err := ctx.Err(); err != nil {
		return ReadyClaim{}, false, err
	}
	if c == nil || c.store == nil {
		return ReadyClaim{}, false, fmt.Errorf("%w: ready queue store is required", ErrInvalidRecord)
	}
	if err := validateReadyClaimRequest(request); err != nil {
		return ReadyClaim{}, false, fmt.Errorf("%w: %w", ErrInvalidRecord, err)
	}

	recovery, err := c.store.Recovery(ctx, RecoveryQuery{RunID: request.RunID, Now: request.Now})
	if err != nil {
		return ReadyClaim{}, false, err
	}
	candidates, snapshots := readyCandidates(recovery.Ready)
	if len(candidates) == 0 {
		return ReadyClaim{}, false, nil
	}

	ordered, err := c.selectCandidates(ctx, candidates)
	if err != nil {
		return ReadyClaim{}, false, err
	}
	ordered = prioritizeReplayCandidates(ordered, candidates, snapshots, request)
	for _, id := range ordered {
		snapshot := snapshots[id]
		expectedClaimGeneration := snapshot.ClaimGeneration
		if snapshot.Lease != nil && snapshot.ClaimGeneration > 0 &&
			snapshot.Lease.Owner == request.Owner && snapshot.Lease.Token == request.Token &&
			snapshot.Lease.ExpiresAt.Equal(request.LeaseUntil) {
			// An exact retry must reproduce the pre-acquisition CAS request so
			// the store can find and replay its durable idempotency outcome.
			expectedClaimGeneration--
		}
		claim, claimErr := c.store.ClaimNode(ctx, ClaimNodeRequest{
			InvocationID:            id,
			ExpectedClaimGeneration: expectedClaimGeneration,
			Owner:                   request.Owner,
			Token:                   request.Token,
			IdempotencyKey:          scopedClaimIdempotencyKey(request.IdempotencyKey, id),
			Now:                     request.Now,
			LeaseUntil:              request.LeaseUntil,
		})
		if errors.Is(claimErr, ErrCASMismatch) {
			continue
		}
		if claimErr != nil {
			return ReadyClaim{}, false, claimErr
		}
		if !claim.Acquired {
			if claim.Lease != nil {
				return ReadyClaim{}, false, fmt.Errorf("%w: negative ready claim exposed a lease", ErrInvalidRecord)
			}
			continue
		}
		if err := validateClaimResult(snapshot, request, claim); err != nil {
			return ReadyClaim{}, false, err
		}
		return ReadyClaim{
			Candidate: candidateFor(snapshot),
			Lease:     *claim.Lease,
			Replayed:  claim.Replayed,
		}, true, nil
	}
	return ReadyClaim{}, false, nil
}

// Renew extends a matching live lease through the durable store.
func (c *ReadyQueueCoordinator) Renew(ctx context.Context, request RenewLeaseRequest) (ClaimLease, error) {
	if ctx == nil {
		return ClaimLease{}, fmt.Errorf("%w: ready queue context is required", ErrInvalidRecord)
	}
	if err := ctx.Err(); err != nil {
		return ClaimLease{}, err
	}
	if c == nil || c.store == nil {
		return ClaimLease{}, fmt.Errorf("%w: ready queue store is required", ErrInvalidRecord)
	}
	return c.store.RenewNodeLease(ctx, request)
}

// Release clears a matching lease through the durable store.
func (c *ReadyQueueCoordinator) Release(ctx context.Context, request ReleaseClaimRequest) error {
	if ctx == nil {
		return fmt.Errorf("%w: ready queue context is required", ErrInvalidRecord)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.store == nil {
		return fmt.Errorf("%w: ready queue store is required", ErrInvalidRecord)
	}
	return c.store.ReleaseNodeClaim(ctx, request)
}

func (c *ReadyQueueCoordinator) selectCandidates(ctx context.Context, candidates []ReadyCandidate) ([]NodeInvocationID, error) {
	if c.policy == nil {
		ordered := make([]NodeInvocationID, len(candidates))
		for i := range candidates {
			ordered[i] = candidates[i].InvocationID
		}
		return ordered, nil
	}

	policyInput := append([]ReadyCandidate(nil), candidates...)
	selected, err := c.policy.SelectReady(ctx, policyInput)
	if err != nil {
		return nil, fmt.Errorf("ready queue policy: %w", err)
	}
	available := make(map[NodeInvocationID]struct{}, len(candidates))
	for _, candidate := range candidates {
		available[candidate.InvocationID] = struct{}{}
	}
	seen := make(map[NodeInvocationID]struct{}, len(selected))
	for _, id := range selected {
		if _, ok := available[id]; !ok {
			return nil, fmt.Errorf("%w: ready queue policy selected unknown invocation %v", ErrInvalidRecord, id)
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("%w: ready queue policy selected invocation %v more than once", ErrInvalidRecord, id)
		}
		seen[id] = struct{}{}
	}
	return append([]NodeInvocationID(nil), selected...), nil
}

func readyCandidates(nodes []NodeInvocationSnapshot) ([]ReadyCandidate, map[NodeInvocationID]NodeInvocationSnapshot) {
	snapshots := make(map[NodeInvocationID]NodeInvocationSnapshot, len(nodes))
	for _, node := range nodes {
		if node.Status != NodeReady {
			continue
		}
		if _, exists := snapshots[node.ID]; !exists {
			snapshots[node.ID] = node
		}
	}
	candidates := make([]ReadyCandidate, 0, len(snapshots))
	for _, node := range snapshots {
		candidates = append(candidates, candidateFor(node))
	}
	sort.Slice(candidates, func(i, j int) bool {
		if !candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return invocationLess(candidates[i].InvocationID, candidates[j].InvocationID)
	})
	return candidates, snapshots
}

func candidateFor(node NodeInvocationSnapshot) ReadyCandidate {
	return ReadyCandidate{InvocationID: node.ID, Priority: node.Priority, CreatedAt: node.CreatedAt}
}

func prioritizeReplayCandidates(
	selected []NodeInvocationID,
	candidates []ReadyCandidate,
	snapshots map[NodeInvocationID]NodeInvocationSnapshot,
	request ReadyClaimRequest,
) []NodeInvocationID {
	result := make([]NodeInvocationID, 0, len(selected)+1)
	added := make(map[NodeInvocationID]struct{}, len(selected))
	for _, candidate := range candidates {
		snapshot := snapshots[candidate.InvocationID]
		if snapshot.Lease == nil || !snapshot.Lease.ExpiresAt.After(request.Now) ||
			snapshot.Lease.Owner != request.Owner || snapshot.Lease.Token != request.Token ||
			!snapshot.Lease.ExpiresAt.Equal(request.LeaseUntil) {
			continue
		}
		result = append(result, candidate.InvocationID)
		added[candidate.InvocationID] = struct{}{}
	}
	for _, id := range selected {
		if _, exists := added[id]; exists {
			continue
		}
		result = append(result, id)
		added[id] = struct{}{}
	}
	return result
}

func invocationLess(left, right NodeInvocationID) bool {
	if left.RunID != right.RunID {
		return left.RunID < right.RunID
	}
	if left.NodeID != right.NodeID {
		return left.NodeID < right.NodeID
	}
	return left.Iteration < right.Iteration
}

func validateReadyClaimRequest(request ReadyClaimRequest) error {
	if request.RunID != "" {
		if err := validateOpaqueID("run id", string(request.RunID)); err != nil {
			return err
		}
	}
	if err := validateRequiredText("claim owner", request.Owner); err != nil {
		return err
	}
	if err := validateRequiredText("claim token", request.Token); err != nil {
		return err
	}
	if err := validateRequiredText("claim idempotency key", request.IdempotencyKey); err != nil {
		return err
	}
	if request.Now.IsZero() || !request.LeaseUntil.After(request.Now) {
		return fmt.Errorf("claim lease must end after non-zero now")
	}
	return nil
}

func validateClaimResult(candidate NodeInvocationSnapshot, request ReadyClaimRequest, result ClaimResult) error {
	if result.Lease == nil {
		return fmt.Errorf("%w: acquired ready claim omitted its lease", ErrInvalidRecord)
	}
	if err := result.Lease.Validate(); err != nil {
		return fmt.Errorf("%w: acquired ready claim: %w", ErrInvalidRecord, err)
	}
	if result.Lease.Owner != request.Owner || result.Lease.Token != request.Token ||
		!result.Lease.ExpiresAt.Equal(request.LeaseUntil) {
		return fmt.Errorf("%w: acquired ready claim does not match the requested fenced lease", ErrInvalidRecord)
	}
	if result.Replayed {
		if candidate.Lease == nil || !candidate.Lease.ExpiresAt.After(request.Now) ||
			candidate.Lease.Owner != result.Lease.Owner || candidate.Lease.Token != result.Lease.Token ||
			candidate.Lease.Generation != result.Lease.Generation ||
			!candidate.Lease.ExpiresAt.Equal(result.Lease.ExpiresAt) {
			return fmt.Errorf("%w: replayed ready claim is not the persisted live lease", ErrInvalidRecord)
		}
		return nil
	}
	if candidate.Lease != nil && candidate.Lease.ExpiresAt.After(request.Now) {
		return fmt.Errorf("%w: acquired ready claim replaced a live lease", ErrInvalidRecord)
	}
	if result.Lease.Generation != candidate.ClaimGeneration+1 {
		return fmt.Errorf("%w: acquired ready claim did not advance claim generation", ErrInvalidRecord)
	}
	return nil
}

func scopedClaimIdempotencyKey(key string, id NodeInvocationID) string {
	hasher := sha256.New()
	var size [8]byte
	for _, field := range []string{key, string(id.RunID), id.NodeID, id.Iteration} {
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		_, _ = hasher.Write(size[:])
		_, _ = hasher.Write([]byte(field))
	}
	return fmt.Sprintf("ready-claim:%x", hasher.Sum(nil))
}
