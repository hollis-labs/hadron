// Package runtimetest provides reusable, concurrency-safe runtime test doubles.
package runtimetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

// Store is a concurrency-safe in-memory StateStore. It implements the core
// lifecycle contract and persistence semantics, but no scheduling policy.
type Store struct {
	mu sync.RWMutex

	runs      map[workflowruntime.RunID]workflowruntime.RunSnapshot
	runStarts map[string]runStartRecord
	nodes     map[workflowruntime.NodeInvocationID]workflowruntime.NodeInvocationSnapshot
	attempts  map[workflowruntime.AttemptID]workflowruntime.AttemptSnapshot
	waits     map[workflowruntime.WaitID]workflowruntime.WaitSnapshot
	resumes   map[string]resumeRecord
	timeouts  map[string]timeoutRecord

	valueSets    map[string]storedValues
	nextValueSet uint64
	plans        map[string]workflowruntime.PlanRef
	events       map[workflowruntime.RunID][]workflowruntime.Event

	claims map[string]claimRecord
	cache  map[string]workflowruntime.CacheEntry
	pins   map[string]workflowruntime.PinnedValue

	activations map[string]activationRecord
}

type runStartRecord struct {
	request workflowruntime.CreateRunRequest
	result  workflowruntime.RunSnapshot
}

type resumeRecord struct {
	request workflowruntime.ResumeWaitRequest
	result  workflowruntime.WaitSnapshot
}

type timeoutRecord struct {
	request workflowruntime.TimeoutWaitRequest
	result  workflowruntime.WaitTimeoutResult
}

type storedValues struct {
	ref    values.ValueSetRef
	owner  workflowruntime.ValueOwner
	values values.ValueSet
}

type claimRecord struct {
	request workflowruntime.ClaimNodeRequest
	result  workflowruntime.ClaimResult
}

type activationRecord struct {
	request workflowruntime.ExternalActivationRequest
	result  workflowruntime.ExternalActivationSnapshot
}

var _ workflowruntime.StateStore = (*Store)(nil)

// NewStore returns an empty StateStore fake.
func NewStore() *Store {
	return &Store{
		runs:        make(map[workflowruntime.RunID]workflowruntime.RunSnapshot),
		runStarts:   make(map[string]runStartRecord),
		nodes:       make(map[workflowruntime.NodeInvocationID]workflowruntime.NodeInvocationSnapshot),
		attempts:    make(map[workflowruntime.AttemptID]workflowruntime.AttemptSnapshot),
		waits:       make(map[workflowruntime.WaitID]workflowruntime.WaitSnapshot),
		resumes:     make(map[string]resumeRecord),
		timeouts:    make(map[string]timeoutRecord),
		valueSets:   make(map[string]storedValues),
		plans:       make(map[string]workflowruntime.PlanRef),
		events:      make(map[workflowruntime.RunID][]workflowruntime.Event),
		claims:      make(map[string]claimRecord),
		cache:       make(map[string]workflowruntime.CacheEntry),
		pins:        make(map[string]workflowruntime.PinnedValue),
		activations: make(map[string]activationRecord),
	}
}

// CreateRun implements runtime.StateStore.
func (s *Store) CreateRun(ctx context.Context, request workflowruntime.CreateRunRequest) (workflowruntime.RunSnapshot, workflowruntime.IdempotencyOutcome, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RunSnapshot{}, "", err
	}
	if err := validateCreateRun(request); err != nil {
		return workflowruntime.RunSnapshot{}, "", invalid(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.runStarts[request.StartIdempotencyKey]; ok {
		if equalCreateRunRequest(prior.request, request) {
			return cloneRun(prior.result), workflowruntime.IdempotencyReplayed, nil
		}
		return workflowruntime.RunSnapshot{}, "", idempotencyConflict("create run", request.StartIdempotencyKey)
	}
	if _, ok := s.runs[request.ID]; ok {
		return workflowruntime.RunSnapshot{}, "", fmt.Errorf("%w: run %q", workflowruntime.ErrAlreadyExists, request.ID)
	}
	snapshot := workflowruntime.RunSnapshot{
		ID: request.ID, Plan: request.Plan, Status: request.Status, Inputs: cloneValueSetRef(request.Inputs),
		Generation: 1, CreatedAt: request.CreatedAt, UpdatedAt: request.CreatedAt,
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.RunSnapshot{}, "", invalid(err)
	}
	snapshot = cloneRun(snapshot)
	s.runs[snapshot.ID] = snapshot
	s.runStarts[request.StartIdempotencyKey] = runStartRecord{request: cloneCreateRunRequest(request), result: snapshot}
	return cloneRun(snapshot), workflowruntime.IdempotencyApplied, nil
}

// LoadRun implements runtime.StateStore.
func (s *Store) LoadRun(ctx context.Context, id workflowruntime.RunID) (workflowruntime.RunSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RunSnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.runs[id]
	if !ok {
		return workflowruntime.RunSnapshot{}, fmt.Errorf("%w: run %q", workflowruntime.ErrNotFound, id)
	}
	return cloneRun(snapshot), nil
}

// SaveRun implements runtime.StateStore.
func (s *Store) SaveRun(ctx context.Context, request workflowruntime.SaveRunRequest) (workflowruntime.RunSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RunSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.runs[request.Snapshot.ID]
	if !ok {
		return workflowruntime.RunSnapshot{}, fmt.Errorf("%w: run %q", workflowruntime.ErrNotFound, request.Snapshot.ID)
	}
	if current.Generation != request.ExpectedGeneration {
		return workflowruntime.RunSnapshot{}, casMismatch("run", request.ExpectedGeneration, current.Generation)
	}
	if request.Snapshot.Status != current.Status {
		return workflowruntime.RunSnapshot{}, invalid(errors.New("run status changes require TransitionRun"))
	}
	if !equalValueSetRef(request.Snapshot.Outputs, current.Outputs) {
		return workflowruntime.RunSnapshot{}, invalid(errors.New("run outputs are lifecycle-managed"))
	}
	if request.Snapshot.Plan != current.Plan {
		return workflowruntime.RunSnapshot{}, invalid(errors.New("run plan reference is immutable"))
	}
	next := cloneRun(request.Snapshot)
	next.Generation = current.Generation + 1
	next.CreatedAt = current.CreatedAt
	if next.UpdatedAt.Before(current.UpdatedAt) {
		return workflowruntime.RunSnapshot{}, invalid(errors.New("run updated_at must not regress"))
	}
	if err := next.Validate(); err != nil {
		return workflowruntime.RunSnapshot{}, invalid(err)
	}
	s.runs[next.ID] = next
	return cloneRun(next), nil
}

// CreateNodeInvocation implements runtime.StateStore.
func (s *Store) CreateNodeInvocation(ctx context.Context, request workflowruntime.CreateNodeInvocationRequest) (workflowruntime.NodeInvocationSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	next := cloneNode(request.Snapshot)
	if next.Generation != 0 || next.ClaimGeneration != 0 || next.Lease != nil {
		return workflowruntime.NodeInvocationSnapshot{}, invalid(errors.New("new node must have zero generations and no lease"))
	}
	if next.Status != workflowruntime.NodePending || next.Blocked != nil || next.LatestAttempt != 0 {
		return workflowruntime.NodeInvocationSnapshot{}, invalid(errors.New("new node must enter lifecycle as pending without attempts"))
	}
	next.Generation = 1
	if err := next.Validate(); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[next.ID]; ok {
		return workflowruntime.NodeInvocationSnapshot{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrAlreadyExists)
	}
	s.nodes[next.ID] = next
	return cloneNode(next), nil
}

// LoadNodeInvocation implements runtime.StateStore.
func (s *Store) LoadNodeInvocation(ctx context.Context, id workflowruntime.NodeInvocationID) (workflowruntime.NodeInvocationSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.nodes[id]
	if !ok {
		return workflowruntime.NodeInvocationSnapshot{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	return cloneNode(snapshot), nil
}

// SaveNodeInvocation implements runtime.StateStore.
func (s *Store) SaveNodeInvocation(ctx context.Context, request workflowruntime.SaveNodeInvocationRequest) (workflowruntime.NodeInvocationSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.nodes[request.Snapshot.ID]
	if !ok {
		return workflowruntime.NodeInvocationSnapshot{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if current.Generation != request.ExpectedGeneration {
		return workflowruntime.NodeInvocationSnapshot{}, casMismatch("node invocation", request.ExpectedGeneration, current.Generation)
	}
	if request.Snapshot.Status != current.Status || !equalBlockedReason(request.Snapshot.Blocked, current.Blocked) {
		return workflowruntime.NodeInvocationSnapshot{}, invalid(errors.New("node lifecycle changes require TransitionNode"))
	}
	if request.Snapshot.LatestAttempt != current.LatestAttempt {
		return workflowruntime.NodeInvocationSnapshot{}, invalid(errors.New("latest attempt is lifecycle-managed"))
	}
	if !equalValueSetRef(request.Snapshot.Inputs, current.Inputs) ||
		!equalValueSetRef(request.Snapshot.Outputs, current.Outputs) {
		return workflowruntime.NodeInvocationSnapshot{}, invalid(errors.New("node input and output references are lifecycle-managed"))
	}
	if request.Snapshot.ClaimGeneration != current.ClaimGeneration || !equalLease(request.Snapshot.Lease, current.Lease) {
		return workflowruntime.NodeInvocationSnapshot{}, fmt.Errorf("%w: claim fields may only change through claim methods", workflowruntime.ErrClaimMismatch)
	}
	next := cloneNode(request.Snapshot)
	next.Generation = current.Generation + 1
	next.CreatedAt = current.CreatedAt
	if next.UpdatedAt.Before(current.UpdatedAt) {
		return workflowruntime.NodeInvocationSnapshot{}, invalid(errors.New("node updated_at must not regress"))
	}
	if err := next.Validate(); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, invalid(err)
	}
	s.nodes[next.ID] = next
	return cloneNode(next), nil
}

// LoadAttempt implements runtime.StateStore.
func (s *Store) LoadAttempt(ctx context.Context, id workflowruntime.AttemptID) (workflowruntime.AttemptSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.attempts[id]
	if !ok {
		return workflowruntime.AttemptSnapshot{}, fmt.Errorf("%w: attempt", workflowruntime.ErrNotFound)
	}
	return cloneAttempt(snapshot), nil
}

// ListAttempts implements runtime.StateStore in attempt-number order.
func (s *Store) ListAttempts(ctx context.Context, id workflowruntime.NodeInvocationID) ([]workflowruntime.AttemptSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]workflowruntime.AttemptSnapshot, 0)
	for attemptID, snapshot := range s.attempts {
		if attemptID.Invocation == id {
			result = append(result, cloneAttempt(snapshot))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID.Number < result[j].ID.Number })
	return result, nil
}

// CreateWait implements runtime.StateStore.
func (s *Store) CreateWait(ctx context.Context, request workflowruntime.CreateWaitRequest) (workflowruntime.WaitSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	next := cloneWait(request.Snapshot)
	if next.Generation != 0 || next.Status != workflowruntime.WaitOpen {
		return workflowruntime.WaitSnapshot{}, invalid(errors.New("new wait must be open with zero generation"))
	}
	next.Generation = 1
	if err := next.Validate(); err != nil {
		return workflowruntime.WaitSnapshot{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.waits[next.Ref.ID]; ok {
		return workflowruntime.WaitSnapshot{}, fmt.Errorf("%w: wait %q", workflowruntime.ErrAlreadyExists, next.Ref.ID)
	}
	s.waits[next.Ref.ID] = next
	return cloneWait(next), nil
}

// LoadWait implements runtime.StateStore.
func (s *Store) LoadWait(ctx context.Context, id workflowruntime.WaitID) (workflowruntime.WaitSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.waits[id]
	if !ok {
		return workflowruntime.WaitSnapshot{}, fmt.Errorf("%w: wait %q", workflowruntime.ErrNotFound, id)
	}
	return cloneWait(snapshot), nil
}

// SaveWait implements runtime.StateStore without imposing transition policy.
func (s *Store) SaveWait(ctx context.Context, request workflowruntime.SaveWaitRequest) (workflowruntime.WaitSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.waits[request.Snapshot.Ref.ID]
	if !ok {
		return workflowruntime.WaitSnapshot{}, fmt.Errorf("%w: wait %q", workflowruntime.ErrNotFound, request.Snapshot.Ref.ID)
	}
	if current.Generation != request.ExpectedGeneration {
		return workflowruntime.WaitSnapshot{}, casMismatch("wait", request.ExpectedGeneration, current.Generation)
	}
	if request.Snapshot.Invocation != current.Invocation {
		return workflowruntime.WaitSnapshot{}, invalid(errors.New("wait invocation is immutable"))
	}
	next := cloneWait(request.Snapshot)
	next.Generation = current.Generation + 1
	next.CreatedAt = current.CreatedAt
	if next.UpdatedAt.Before(current.UpdatedAt) {
		return workflowruntime.WaitSnapshot{}, invalid(errors.New("wait updated_at must not regress"))
	}
	if err := next.Validate(); err != nil {
		return workflowruntime.WaitSnapshot{}, invalid(err)
	}
	s.waits[next.Ref.ID] = next
	return cloneWait(next), nil
}

// ResumeWait implements runtime.StateStore.
func (s *Store) ResumeWait(ctx context.Context, request workflowruntime.ResumeWaitRequest) (workflowruntime.WaitSnapshot, workflowruntime.IdempotencyOutcome, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.WaitSnapshot{}, "", err
	}
	if err := validateResume(request); err != nil {
		return workflowruntime.WaitSnapshot{}, "", invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if request.IdempotencyKey != "" {
		if prior, ok := s.resumes[request.IdempotencyKey]; ok {
			if equalResumeWaitRequest(prior.request, request) {
				return cloneWait(prior.result), workflowruntime.IdempotencyReplayed, nil
			}
			return workflowruntime.WaitSnapshot{}, "", idempotencyConflict("resume wait", request.IdempotencyKey)
		}
	}
	current, ok := s.waits[request.WaitID]
	if !ok {
		return workflowruntime.WaitSnapshot{}, "", fmt.Errorf("%w: wait %q", workflowruntime.ErrNotFound, request.WaitID)
	}
	if current.Status != workflowruntime.WaitOpen {
		return workflowruntime.WaitSnapshot{}, "", fmt.Errorf("%w: wait %q", workflowruntime.ErrAlreadyResumed, request.WaitID)
	}
	if request.ResumedAt.Before(current.UpdatedAt) {
		return workflowruntime.WaitSnapshot{}, "", invalid(errors.New("wait resume time must not regress"))
	}
	current.Status = workflowruntime.WaitResumed
	current.ResumeValues = cloneValueSetRef(request.Values)
	current.ResolvedAt = request.ResumedAt
	current.UpdatedAt = request.ResumedAt
	current.Generation++
	if err := current.Validate(); err != nil {
		return workflowruntime.WaitSnapshot{}, "", invalid(err)
	}
	s.waits[current.Ref.ID] = current
	if request.IdempotencyKey != "" {
		s.resumes[request.IdempotencyKey] = resumeRecord{request: cloneResumeRequest(request), result: current}
	}
	return cloneWait(current), workflowruntime.IdempotencyApplied, nil
}

// SaveValues implements runtime.StateStore.
func (s *Store) SaveValues(ctx context.Context, request workflowruntime.SaveValuesRequest) (values.ValueSetRef, error) {
	if err := checkContext(ctx); err != nil {
		return values.ValueSetRef{}, err
	}
	if err := request.Owner.Validate(); err != nil {
		return values.ValueSetRef{}, invalid(err)
	}
	if err := values.ValidatePersistableSet(request.Values); err != nil {
		return values.ValueSetRef{}, invalid(err)
	}
	copySet, err := cloneValueSet(request.Values)
	if err != nil {
		return values.ValueSetRef{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextValueSet++
	id := fmt.Sprintf("values-%012d", s.nextValueSet)
	ref, err := values.NewValueSetRef(id, copySet)
	if err != nil {
		return values.ValueSetRef{}, invalid(err)
	}
	s.valueSets[id] = storedValues{ref: ref, owner: cloneValueOwner(request.Owner), values: copySet}
	return ref, nil
}

// LoadValues implements runtime.StateStore.
func (s *Store) LoadValues(ctx context.Context, ref values.ValueSetRef) (values.ValueSet, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		return nil, invalid(err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	stored, ok := s.valueSets[ref.ID]
	if !ok {
		return nil, fmt.Errorf("%w: value set %q", workflowruntime.ErrNotFound, ref.ID)
	}
	if stored.ref.Digest != ref.Digest {
		return nil, fmt.Errorf("%w: value set digest", workflowruntime.ErrCASMismatch)
	}
	return cloneValueSet(stored.values)
}

// RecordPlan implements runtime.StateStore.
func (s *Store) RecordPlan(ctx context.Context, plan workflowruntime.PlanRef) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := plan.Validate(); err != nil {
		return invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.plans[plan.Digest]; ok {
		if current == plan {
			return nil
		}
		return fmt.Errorf("%w: plan digest %q has different metadata", workflowruntime.ErrAlreadyExists, plan.Digest)
	}
	s.plans[plan.Digest] = plan
	return nil
}

// LoadPlan implements runtime.StateStore.
func (s *Store) LoadPlan(ctx context.Context, digest string) (workflowruntime.PlanRef, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.PlanRef{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	plan, ok := s.plans[digest]
	if !ok {
		return workflowruntime.PlanRef{}, fmt.Errorf("%w: plan %q", workflowruntime.ErrNotFound, digest)
	}
	return plan, nil
}

// AppendEvent atomically allocates a monotonic sequence within request.RunID.
func (s *Store) AppendEvent(ctx context.Context, request workflowruntime.AppendEventRequest) (workflowruntime.Event, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.Event{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendEventLocked(request)
}

func (s *Store) appendEventLocked(request workflowruntime.AppendEventRequest) (workflowruntime.Event, error) {
	sequence := uint64(len(s.events[request.RunID]) + 1)
	event := workflowruntime.Event{
		Sequence: sequence, RunID: request.RunID, Invocation: cloneInvocationID(request.Invocation),
		Attempt: cloneAttemptID(request.Attempt), Type: request.Type, OccurredAt: request.OccurredAt,
		Attributes: cloneStringMap(request.Attributes), Values: cloneValueSetRef(request.Values),
		Redaction: request.Redaction, Retention: request.Retention,
	}
	if err := event.Validate(); err != nil {
		return workflowruntime.Event{}, invalid(err)
	}
	s.events[request.RunID] = append(s.events[request.RunID], event)
	return cloneEvent(event), nil
}

// ListEvents implements runtime.StateStore in ascending per-run sequence.
func (s *Store) ListEvents(ctx context.Context, query workflowruntime.EventQuery) ([]workflowruntime.Event, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if query.RunID == "" || query.Limit < 0 {
		return nil, invalid(errors.New("event query requires run id and non-negative limit"))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]workflowruntime.Event, 0)
	for _, event := range s.events[query.RunID] {
		if event.Sequence <= query.AfterSequence {
			continue
		}
		result = append(result, cloneEvent(event))
		if query.Limit > 0 && len(result) == query.Limit {
			break
		}
	}
	return result, nil
}

// ClaimNode atomically attempts to acquire a ready node under
// claim-generation CAS. Non-ready nodes and live leases produce replayable
// negative results without exposing another owner's lease.
func (s *Store) ClaimNode(ctx context.Context, request workflowruntime.ClaimNodeRequest) (workflowruntime.ClaimResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ClaimResult{}, err
	}
	if err := validateClaim(request); err != nil {
		return workflowruntime.ClaimResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.claims[request.IdempotencyKey]; ok {
		if equalClaimNodeRequest(prior.request, request) {
			result := cloneClaimResult(prior.result)
			result.Replayed = true
			return result, nil
		}
		return workflowruntime.ClaimResult{}, idempotencyConflict("claim node", request.IdempotencyKey)
	}
	current, ok := s.nodes[request.InvocationID]
	if !ok {
		return workflowruntime.ClaimResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if current.ClaimGeneration != request.ExpectedClaimGeneration {
		return workflowruntime.ClaimResult{}, casMismatch("node claim", request.ExpectedClaimGeneration, current.ClaimGeneration)
	}
	if current.Status != workflowruntime.NodeReady {
		result := workflowruntime.ClaimResult{Acquired: false}
		s.claims[request.IdempotencyKey] = claimRecord{request: request, result: result}
		return result, nil
	}
	if current.Lease != nil && current.Lease.ExpiresAt.After(request.Now) {
		result := workflowruntime.ClaimResult{Acquired: false}
		s.claims[request.IdempotencyKey] = claimRecord{request: request, result: result}
		return result, nil
	}
	if request.Now.Before(current.UpdatedAt) {
		return workflowruntime.ClaimResult{}, invalid(errors.New("claim time must not regress node updated_at"))
	}
	current.ClaimGeneration++
	current.Lease = &workflowruntime.ClaimLease{
		Owner: request.Owner, Token: request.Token, Generation: current.ClaimGeneration, ExpiresAt: request.LeaseUntil,
	}
	current.Generation++
	current.UpdatedAt = request.Now
	if err := current.Validate(); err != nil {
		return workflowruntime.ClaimResult{}, invalid(err)
	}
	s.nodes[current.ID] = current
	result := workflowruntime.ClaimResult{Acquired: true, Lease: cloneLease(current.Lease)}
	s.claims[request.IdempotencyKey] = claimRecord{request: request, result: result}
	return cloneClaimResult(result), nil
}

// RenewNodeLease implements runtime.StateStore.
func (s *Store) RenewNodeLease(ctx context.Context, request workflowruntime.RenewLeaseRequest) (workflowruntime.ClaimLease, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ClaimLease{}, err
	}
	if err := validateRenew(request); err != nil {
		return workflowruntime.ClaimLease{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.nodes[request.InvocationID]
	if !ok {
		return workflowruntime.ClaimLease{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if !matchesLease(current.Lease, request.Owner, request.Token, request.Generation) {
		return workflowruntime.ClaimLease{}, workflowruntime.ErrClaimMismatch
	}
	if !current.Lease.ExpiresAt.After(request.Now) {
		return workflowruntime.ClaimLease{}, workflowruntime.ErrLeaseExpired
	}
	if request.Now.Before(current.UpdatedAt) {
		return workflowruntime.ClaimLease{}, invalid(errors.New("renewal time must not regress node updated_at"))
	}
	if !request.LeaseUntil.After(current.Lease.ExpiresAt) {
		return workflowruntime.ClaimLease{}, invalid(errors.New("renewal must extend the current lease expiry"))
	}
	current.Lease.ExpiresAt = request.LeaseUntil
	current.Generation++
	current.UpdatedAt = request.Now
	if err := current.Validate(); err != nil {
		return workflowruntime.ClaimLease{}, invalid(err)
	}
	s.nodes[current.ID] = current
	return *cloneLease(current.Lease), nil
}

// ReleaseNodeClaim implements runtime.StateStore.
func (s *Store) ReleaseNodeClaim(ctx context.Context, request workflowruntime.ReleaseClaimRequest) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := request.InvocationID.Validate(); err != nil {
		return invalid(err)
	}
	if request.Now.IsZero() {
		return invalid(errors.New("release now is required"))
	}
	if request.Owner == "" || request.Token == "" || request.Generation == 0 {
		return invalid(errors.New("release requires owner, token, and generation"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.nodes[request.InvocationID]
	if !ok {
		return fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if !matchesLease(current.Lease, request.Owner, request.Token, request.Generation) {
		return workflowruntime.ErrClaimMismatch
	}
	if request.Now.Before(current.UpdatedAt) {
		return invalid(errors.New("release time must not regress node updated_at"))
	}
	current.Lease = nil
	current.Generation++
	current.UpdatedAt = request.Now
	if err := current.Validate(); err != nil {
		return invalid(err)
	}
	s.nodes[current.ID] = current
	return nil
}

// PutCacheEntry implements runtime.StateStore.
func (s *Store) PutCacheEntry(ctx context.Context, entry workflowruntime.CacheEntry) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := entry.Validate(); err != nil {
		return invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[entry.Key] = entry
	return nil
}

// GetCacheEntry implements runtime.StateStore.
func (s *Store) GetCacheEntry(ctx context.Context, key string, now time.Time) (workflowruntime.CacheEntry, bool, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.CacheEntry{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.cache[key]
	if !ok || (!entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now)) {
		return workflowruntime.CacheEntry{}, false, nil
	}
	return entry, true, nil
}

// PutPinnedValue implements runtime.StateStore.
func (s *Store) PutPinnedValue(ctx context.Context, pin workflowruntime.PinnedValue) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := pin.Validate(); err != nil {
		return invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pins[pin.Key] = pin
	return nil
}

// GetPinnedValue implements runtime.StateStore.
func (s *Store) GetPinnedValue(ctx context.Context, key string, now time.Time) (workflowruntime.PinnedValue, bool, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.PinnedValue{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	pin, ok := s.pins[key]
	if !ok || (!pin.ExpiresAt.IsZero() && !pin.ExpiresAt.After(now)) {
		return workflowruntime.PinnedValue{}, false, nil
	}
	return pin, true, nil
}

// ListPinnedValues implements runtime.StateStore in key order.
func (s *Store) ListPinnedValues(ctx context.Context, now time.Time) ([]workflowruntime.PinnedValue, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]workflowruntime.PinnedValue, 0, len(s.pins))
	for _, pin := range s.pins {
		if pin.ExpiresAt.IsZero() || pin.ExpiresAt.After(now) {
			result = append(result, pin)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

// RecordExternalActivation implements runtime.StateStore.
func (s *Store) RecordExternalActivation(ctx context.Context, request workflowruntime.ExternalActivationRequest) (workflowruntime.ExternalActivationSnapshot, workflowruntime.IdempotencyOutcome, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ExternalActivationSnapshot{}, "", err
	}
	if err := validateActivation(request); err != nil {
		return workflowruntime.ExternalActivationSnapshot{}, "", invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.activations[request.IdempotencyKey]; ok {
		if equalActivationRequest(prior.request, request) {
			return cloneActivation(prior.result), workflowruntime.IdempotencyReplayed, nil
		}
		return workflowruntime.ExternalActivationSnapshot{}, "", idempotencyConflict("external activation", request.IdempotencyKey)
	}
	result := workflowruntime.ExternalActivationSnapshot{
		ActivationID: request.ActivationID, IdempotencyKey: request.IdempotencyKey,
		RequestedRunID: request.RequestedRunID, Plan: request.Plan,
		Inputs: cloneValueSetRef(request.Inputs), OccurredAt: request.OccurredAt,
	}
	s.activations[request.IdempotencyKey] = activationRecord{request: cloneActivationRequest(request), result: result}
	return cloneActivation(result), workflowruntime.IdempotencyApplied, nil
}

// Recovery implements runtime.StateStore with deterministic category ordering.
func (s *Store) Recovery(ctx context.Context, query workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RecoverySnapshot{}, err
	}
	if query.Now.IsZero() || query.Limit < 0 {
		return workflowruntime.RecoverySnapshot{}, invalid(errors.New("recovery requires now and non-negative limit"))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result workflowruntime.RecoverySnapshot
	for _, run := range s.runs {
		if run.Status.Active() && (query.RunID == "" || query.RunID == run.ID) {
			result.ActiveRuns = append(result.ActiveRuns, cloneRun(run))
		}
	}
	for _, node := range s.nodes {
		if query.RunID != "" && query.RunID != node.ID.RunID {
			continue
		}
		copyNode := cloneNode(node)
		if node.Status == workflowruntime.NodeReady {
			result.Ready = append(result.Ready, copyNode)
		}
		if node.Status == workflowruntime.NodeWaiting {
			result.Waiting = append(result.Waiting, copyNode)
		}
		if node.Lease != nil {
			if node.Lease.ExpiresAt.After(query.Now) {
				result.Leased = append(result.Leased, copyNode)
			} else {
				result.ExpiredLeases = append(result.ExpiredLeases, copyNode)
			}
		}
	}
	sort.Slice(result.ActiveRuns, func(i, j int) bool { return result.ActiveRuns[i].ID < result.ActiveRuns[j].ID })
	sortNodes(result.Ready)
	sortNodes(result.Waiting)
	sortNodes(result.Leased)
	sortNodes(result.ExpiredLeases)
	result.ActiveRuns = limit(result.ActiveRuns, query.Limit)
	result.Ready = limit(result.Ready, query.Limit)
	result.Waiting = limit(result.Waiting, query.Limit)
	result.Leased = limit(result.Leased, query.Limit)
	result.ExpiredLeases = limit(result.ExpiredLeases, query.Limit)
	return result, nil
}

func validateCreateRun(request workflowruntime.CreateRunRequest) error {
	if request.ID == "" || request.CreatedAt.IsZero() || request.StartIdempotencyKey == "" {
		return errors.New("create run requires id, created_at, and idempotency key")
	}
	if err := request.Plan.Validate(); err != nil {
		return err
	}
	if request.Status != workflowruntime.RunPending {
		return errors.New("new run must enter lifecycle as pending")
	}
	if request.Inputs != nil {
		return request.Inputs.Validate()
	}
	return nil
}

func validateResume(request workflowruntime.ResumeWaitRequest) error {
	if request.WaitID == "" || request.ResumedAt.IsZero() {
		return errors.New("resume wait requires wait id and resumed_at")
	}
	if request.Values != nil {
		return request.Values.Validate()
	}
	return nil
}

func validateClaim(request workflowruntime.ClaimNodeRequest) error {
	if err := request.InvocationID.Validate(); err != nil {
		return err
	}
	if request.Owner == "" || request.Token == "" || request.IdempotencyKey == "" {
		return errors.New("claim requires owner, token, and idempotency key")
	}
	if request.Now.IsZero() || !request.LeaseUntil.After(request.Now) {
		return errors.New("claim lease must end after non-zero now")
	}
	return nil
}

func validateRenew(request workflowruntime.RenewLeaseRequest) error {
	if err := request.InvocationID.Validate(); err != nil {
		return err
	}
	if request.Owner == "" || request.Token == "" || request.Generation == 0 {
		return errors.New("renewal requires owner, token, and generation")
	}
	if request.Now.IsZero() || !request.LeaseUntil.After(request.Now) {
		return errors.New("renewal lease must end after non-zero now")
	}
	return nil
}

func validateActivation(request workflowruntime.ExternalActivationRequest) error {
	if request.ActivationID == "" || request.IdempotencyKey == "" || request.RequestedRunID == "" || request.OccurredAt.IsZero() {
		return errors.New("activation requires ids, idempotency key, and occurred_at")
	}
	if err := request.Plan.Validate(); err != nil {
		return err
	}
	if request.Inputs != nil {
		return request.Inputs.Validate()
	}
	return nil
}

func matchesLease(lease *workflowruntime.ClaimLease, owner, token string, generation uint64) bool {
	return lease != nil && lease.Owner == owner && lease.Token == token && lease.Generation == generation
}

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func invalid(err error) error { return fmt.Errorf("%w: %w", workflowruntime.ErrInvalidRecord, err) }

func casMismatch(resource string, expected, actual uint64) error {
	return &workflowruntime.CASMismatchError{Resource: resource, Expected: expected, Actual: actual}
}

func idempotencyConflict(operation, key string) error {
	return &workflowruntime.IdempotencyConflictError{Operation: operation, Key: key}
}

func equalCreateRunRequest(left, right workflowruntime.CreateRunRequest) bool {
	return left.ID == right.ID && left.Plan == right.Plan && left.Status == right.Status &&
		equalValueSetRef(left.Inputs, right.Inputs) &&
		left.StartIdempotencyKey == right.StartIdempotencyKey && left.CreatedAt.Equal(right.CreatedAt)
}

func equalResumeWaitRequest(left, right workflowruntime.ResumeWaitRequest) bool {
	return left.WaitID == right.WaitID && left.IdempotencyKey == right.IdempotencyKey &&
		equalValueSetRef(left.Values, right.Values) && left.ResumedAt.Equal(right.ResumedAt)
}

func equalClaimNodeRequest(left, right workflowruntime.ClaimNodeRequest) bool {
	return left.InvocationID == right.InvocationID &&
		left.ExpectedClaimGeneration == right.ExpectedClaimGeneration &&
		left.Owner == right.Owner && left.Token == right.Token &&
		left.IdempotencyKey == right.IdempotencyKey && left.Now.Equal(right.Now) &&
		left.LeaseUntil.Equal(right.LeaseUntil)
}

func equalActivationRequest(left, right workflowruntime.ExternalActivationRequest) bool {
	return left.ActivationID == right.ActivationID && left.IdempotencyKey == right.IdempotencyKey &&
		left.RequestedRunID == right.RequestedRunID && left.Plan == right.Plan &&
		equalValueSetRef(left.Inputs, right.Inputs) && left.OccurredAt.Equal(right.OccurredAt)
}

func equalLease(left, right *workflowruntime.ClaimLease) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Owner == right.Owner && left.Token == right.Token &&
		left.Generation == right.Generation && left.ExpiresAt.Equal(right.ExpiresAt)
}

func equalBlockedReason(left, right *workflowruntime.BlockedReason) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Code != right.Code || left.Message != right.Message ||
		len(left.Dependencies) != len(right.Dependencies) || len(left.Details) != len(right.Details) {
		return false
	}
	for i := range left.Dependencies {
		if left.Dependencies[i] != right.Dependencies[i] {
			return false
		}
	}
	for key, value := range left.Details {
		if right.Details[key] != value {
			return false
		}
	}
	return true
}

func equalValueSetRef(left, right *values.ValueSetRef) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func cloneCreateRunRequest(request workflowruntime.CreateRunRequest) workflowruntime.CreateRunRequest {
	request.Inputs = cloneValueSetRef(request.Inputs)
	return request
}

func cloneRun(snapshot workflowruntime.RunSnapshot) workflowruntime.RunSnapshot {
	snapshot.Inputs = cloneValueSetRef(snapshot.Inputs)
	snapshot.Outputs = cloneValueSetRef(snapshot.Outputs)
	return snapshot
}

func cloneNode(snapshot workflowruntime.NodeInvocationSnapshot) workflowruntime.NodeInvocationSnapshot {
	snapshot.Blocked = cloneBlocked(snapshot.Blocked)
	snapshot.Inputs = cloneValueSetRef(snapshot.Inputs)
	snapshot.Outputs = cloneValueSetRef(snapshot.Outputs)
	if snapshot.Wait != nil {
		wait := *snapshot.Wait
		snapshot.Wait = &wait
	}
	snapshot.Lease = cloneLease(snapshot.Lease)
	return snapshot
}

func cloneAttempt(snapshot workflowruntime.AttemptSnapshot) workflowruntime.AttemptSnapshot {
	snapshot.Executor = cloneExecutor(snapshot.Executor)
	snapshot.Inputs = cloneValueSetRef(snapshot.Inputs)
	snapshot.Outputs = cloneValueSetRef(snapshot.Outputs)
	snapshot.Failure = cloneFailure(snapshot.Failure)
	return snapshot
}

func cloneExecutor(executor workflowruntime.ExecutorMetadata) workflowruntime.ExecutorMetadata {
	executor.Attributes = cloneStringMap(executor.Attributes)
	return executor
}

func cloneFailure(failure *workflowruntime.Failure) *workflowruntime.Failure {
	if failure == nil {
		return nil
	}
	copyFailure := *failure
	copyFailure.Details = cloneStringMap(failure.Details)
	return &copyFailure
}

func cloneWait(snapshot workflowruntime.WaitSnapshot) workflowruntime.WaitSnapshot {
	snapshot.ResumeValues = cloneValueSetRef(snapshot.ResumeValues)
	return snapshot
}

func cloneEvent(event workflowruntime.Event) workflowruntime.Event {
	event.Invocation = cloneInvocationID(event.Invocation)
	event.Attempt = cloneAttemptID(event.Attempt)
	event.Attributes = cloneStringMap(event.Attributes)
	event.Values = cloneValueSetRef(event.Values)
	return event
}

func cloneBlocked(reason *workflowruntime.BlockedReason) *workflowruntime.BlockedReason {
	if reason == nil {
		return nil
	}
	copyReason := *reason
	copyReason.Dependencies = append([]workflowruntime.NodeInvocationID(nil), reason.Dependencies...)
	copyReason.Details = cloneStringMap(reason.Details)
	return &copyReason
}

func cloneLease(lease *workflowruntime.ClaimLease) *workflowruntime.ClaimLease {
	if lease == nil {
		return nil
	}
	copyLease := *lease
	return &copyLease
}

func cloneClaimResult(result workflowruntime.ClaimResult) workflowruntime.ClaimResult {
	result.Lease = cloneLease(result.Lease)
	return result
}

func cloneResumeRequest(request workflowruntime.ResumeWaitRequest) workflowruntime.ResumeWaitRequest {
	request.Values = cloneValueSetRef(request.Values)
	return request
}

func cloneActivationRequest(request workflowruntime.ExternalActivationRequest) workflowruntime.ExternalActivationRequest {
	request.Inputs = cloneValueSetRef(request.Inputs)
	return request
}

func cloneActivation(snapshot workflowruntime.ExternalActivationSnapshot) workflowruntime.ExternalActivationSnapshot {
	snapshot.Inputs = cloneValueSetRef(snapshot.Inputs)
	return snapshot
}

func cloneValueOwner(owner workflowruntime.ValueOwner) workflowruntime.ValueOwner {
	owner.Invocation = cloneInvocationID(owner.Invocation)
	owner.Attempt = cloneAttemptID(owner.Attempt)
	return owner
}

func cloneValueSetRef(ref *values.ValueSetRef) *values.ValueSetRef {
	if ref == nil {
		return nil
	}
	copyRef := *ref
	return &copyRef
}

func cloneInvocationID(id *workflowruntime.NodeInvocationID) *workflowruntime.NodeInvocationID {
	if id == nil {
		return nil
	}
	copyID := *id
	return &copyID
}

func cloneAttemptID(id *workflowruntime.AttemptID) *workflowruntime.AttemptID {
	if id == nil {
		return nil
	}
	copyID := *id
	return &copyID
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneValueSet(input values.ValueSet) (values.ValueSet, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	var result values.ValueSet
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func sortNodes(nodes []workflowruntime.NodeInvocationSnapshot) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Priority != nodes[j].Priority {
			return nodes[i].Priority > nodes[j].Priority
		}
		if nodes[i].ID.RunID != nodes[j].ID.RunID {
			return nodes[i].ID.RunID < nodes[j].ID.RunID
		}
		if nodes[i].ID.NodeID != nodes[j].ID.NodeID {
			return nodes[i].ID.NodeID < nodes[j].ID.NodeID
		}
		return nodes[i].ID.Iteration < nodes[j].ID.Iteration
	})
}

func limit[T any](values []T, maximum int) []T {
	if maximum > 0 && len(values) > maximum {
		return values[:maximum]
	}
	return values
}
