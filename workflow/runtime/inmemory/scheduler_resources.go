package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

var _ workflowruntime.SchedulerResourceStore = (*Store)(nil)

func (s *Store) AdmitNode(ctx context.Context, request workflowruntime.AdmitNodeRequest) (workflowruntime.AdmitNodeResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.AdmitNodeResult{}, err
	}
	request.Claim.Now = request.Claim.Now.UTC()
	request.Claim.LeaseUntil = request.Claim.LeaseUntil.UTC()
	request.EnqueuedAt = request.EnqueuedAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.AdmitNodeResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.schedulerAdmissions[request.Claim.IdempotencyKey]; ok {
		if !equalSchedulerAdmissionRequest(prior.request, request) {
			return workflowruntime.AdmitNodeResult{}, idempotencyConflict("scheduler admission", request.Claim.IdempotencyKey)
		}
		if !s.controlAdmissionAllowedLocked(request.Claim.InvocationID) {
			return workflowruntime.AdmitNodeResult{Claim: workflowruntime.ClaimResult{Replayed: true}}, nil
		}
		result := cloneSchedulerAdmissionResult(prior.result)
		if result.Claim.Acquired && !s.schedulerClaimLiveLocked(request.Claim.InvocationID, result.Claim.Lease, prior.request.Requirements, request.Claim.Now) {
			return workflowruntime.AdmitNodeResult{}, idempotencyConflict("scheduler admission", request.Claim.IdempotencyKey)
		}
		result.Claim.Replayed = true
		return result, nil
	}
	current, ok := s.nodes[request.Claim.InvocationID]
	if !ok {
		return workflowruntime.AdmitNodeResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if current.ClaimGeneration != request.Claim.ExpectedClaimGeneration {
		return workflowruntime.AdmitNodeResult{}, casMismatch("scheduler node claim", request.Claim.ExpectedClaimGeneration, current.ClaimGeneration)
	}
	for _, requirement := range request.Requirements {
		if limit, exists := s.schedulerDefinitions[requirement.Resource]; exists && limit != requirement.Limit {
			return workflowruntime.AdmitNodeResult{}, invalid(fmt.Errorf("%w: resource limit differs from durable definition", workflowruntime.ErrInvalidSchedulerResource))
		}
	}
	result := workflowruntime.AdmitNodeResult{}
	if current.Status != workflowruntime.NodeReady || !s.runAllowsExecutionLocked(current.ID) || !s.controlAdmissionAllowedLocked(current.ID) || current.Lease != nil && current.Lease.ExpiresAt.After(request.Claim.Now) {
		s.recordSchedulerDefinitionsLocked(request.Requirements)
		s.expireSchedulerHoldersLocked(request.Claim.Now)
		delete(s.schedulerWaiters, current.ID)
		return s.recordSchedulerAdmissionLocked(request, result), nil
	}
	if request.Claim.Now.Before(current.UpdatedAt) {
		return workflowruntime.AdmitNodeResult{}, invalid(errors.New("claim time must not regress node updated_at"))
	}
	blocked := s.blockedSchedulerResourcesLocked(current, request.Requirements, request.Claim.Now)
	if !s.fanOutClaimEligibleLocked(current, request.Claim.Now) {
		blocked = append(blocked, fanOutResourceID(current.ID))
	}
	sortSchedulerResourceIDs(blocked)
	if len(blocked) != 0 {
		result.Blocked = append([]workflowruntime.SchedulerResourceID(nil), blocked...)
		waiter := workflowruntime.SchedulerResourceWaiter{
			Invocation: current.ID, Requirements: cloneSchedulerRequirements(request.Requirements), Blocked: append([]workflowruntime.SchedulerResourceID(nil), blocked...),
			Priority: request.Priority, EnqueuedAt: request.EnqueuedAt, UpdatedAt: request.Claim.Now,
		}
		if err := waiter.Validate(); err != nil {
			return workflowruntime.AdmitNodeResult{}, invalid(err)
		}
		s.recordSchedulerDefinitionsLocked(request.Requirements)
		s.expireSchedulerHoldersLocked(request.Claim.Now)
		s.schedulerWaiters[current.ID] = waiter
		return s.recordSchedulerAdmissionLocked(request, result), nil
	}
	current.ClaimGeneration++
	current.Lease = &workflowruntime.ClaimLease{Owner: request.Claim.Owner, Token: request.Claim.Token, Generation: current.ClaimGeneration, ExpiresAt: request.Claim.LeaseUntil}
	current.Generation++
	current.UpdatedAt = request.Claim.Now
	if err := current.Validate(); err != nil {
		return workflowruntime.AdmitNodeResult{}, invalid(err)
	}
	holders := make([]workflowruntime.SchedulerResourceHolder, 0, len(request.Requirements))
	for _, requirement := range request.Requirements {
		holder := workflowruntime.SchedulerResourceHolder{
			Resource: requirement.Resource, Invocation: current.ID, Units: requirement.Units,
			ClaimGeneration: current.ClaimGeneration, Owner: request.Claim.Owner, AcquiredAt: request.Claim.Now, ExpiresAt: request.Claim.LeaseUntil,
		}
		if err := holder.Validate(); err != nil {
			return workflowruntime.AdmitNodeResult{}, invalid(err)
		}
		holders = append(holders, holder)
	}
	s.recordSchedulerDefinitionsLocked(request.Requirements)
	s.expireSchedulerHoldersLocked(request.Claim.Now)
	for _, holder := range holders {
		resourceHolders := s.schedulerHolders[holder.Resource]
		if resourceHolders == nil {
			resourceHolders = make(map[workflowruntime.NodeInvocationID]workflowruntime.SchedulerResourceHolder)
			s.schedulerHolders[holder.Resource] = resourceHolders
		}
		resourceHolders[current.ID] = holder
	}
	delete(s.schedulerWaiters, current.ID)
	s.nodes[current.ID] = current
	result.Claim = workflowruntime.ClaimResult{Acquired: true, Lease: cloneLease(current.Lease)}
	return s.recordSchedulerAdmissionLocked(request, result), nil
}

func (s *Store) recordSchedulerDefinitionsLocked(requirements []workflowruntime.SchedulerResourceRequirement) {
	for _, requirement := range requirements {
		s.schedulerDefinitions[requirement.Resource] = requirement.Limit
	}
}

func (s *Store) InspectSchedulerResources(ctx context.Context, query workflowruntime.SchedulerResourceQuery) (workflowruntime.SchedulerResourceState, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.SchedulerResourceState{}, err
	}
	if query.RunID != "" {
		if err := (workflowruntime.NodeInvocationID{RunID: query.RunID, NodeID: "valid"}).Validate(); err != nil {
			return workflowruntime.SchedulerResourceState{}, invalid(err)
		}
	}
	if query.Now.IsZero() || query.Limit < 0 {
		return workflowruntime.SchedulerResourceState{}, invalid(errors.New("resource inspection requires now and nonnegative limit"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireSchedulerHoldersLocked(query.Now.UTC())
	state := workflowruntime.SchedulerResourceState{}
	for _, holders := range s.schedulerHolders {
		for _, holder := range holders {
			if query.RunID == "" || holder.Invocation.RunID == query.RunID {
				state.Holders = append(state.Holders, holder)
			}
		}
	}
	for id, waiter := range s.schedulerWaiters {
		node, exists := s.nodes[id]
		if !exists || node.Status != workflowruntime.NodeReady || node.Lease != nil && node.Lease.ExpiresAt.After(query.Now) || !s.runAllowsExecutionLocked(id) || !s.controlAdmissionAllowedLocked(id) {
			delete(s.schedulerWaiters, id)
			continue
		}
		blocked := s.blockedSchedulerResourcesLocked(node, waiter.Requirements, query.Now)
		if !s.fanOutClaimEligibleLocked(node, query.Now) {
			blocked = append(blocked, fanOutResourceID(id))
		}
		sortSchedulerResourceIDs(blocked)
		if len(blocked) == 0 {
			delete(s.schedulerWaiters, id)
			continue
		}
		waiter.Blocked = blocked
		s.schedulerWaiters[id] = waiter
		if query.RunID == "" || id.RunID == query.RunID {
			state.Waiters = append(state.Waiters, cloneSchedulerWaiter(waiter))
		}
	}
	sort.Slice(state.Holders, func(i, j int) bool {
		if state.Holders[i].Resource != state.Holders[j].Resource {
			return schedulerResourceIDLess(state.Holders[i].Resource, state.Holders[j].Resource)
		}
		return invocationIDLess(state.Holders[i].Invocation, state.Holders[j].Invocation)
	})
	sort.Slice(state.Waiters, func(i, j int) bool {
		if !state.Waiters[i].EnqueuedAt.Equal(state.Waiters[j].EnqueuedAt) {
			return state.Waiters[i].EnqueuedAt.Before(state.Waiters[j].EnqueuedAt)
		}
		if state.Waiters[i].Priority != state.Waiters[j].Priority {
			return state.Waiters[i].Priority > state.Waiters[j].Priority
		}
		return invocationIDLess(state.Waiters[i].Invocation, state.Waiters[j].Invocation)
	})
	if query.Limit > 0 {
		if len(state.Holders) > query.Limit {
			state.Holders = state.Holders[:query.Limit]
		}
		if len(state.Waiters) > query.Limit {
			state.Waiters = state.Waiters[:query.Limit]
		}
	}
	return state, nil
}

func (s *Store) recordSchedulerAdmissionLocked(request workflowruntime.AdmitNodeRequest, result workflowruntime.AdmitNodeResult) workflowruntime.AdmitNodeResult {
	cloned := cloneSchedulerAdmissionResult(result)
	s.schedulerAdmissions[request.Claim.IdempotencyKey] = schedulerAdmissionRecord{request: cloneSchedulerAdmissionRequest(request), result: cloned}
	return cloneSchedulerAdmissionResult(cloned)
}

func (s *Store) blockedSchedulerResourcesLocked(node workflowruntime.NodeInvocationSnapshot, requirements []workflowruntime.SchedulerResourceRequirement, now time.Time) []workflowruntime.SchedulerResourceID {
	blocked := make([]workflowruntime.SchedulerResourceID, 0)
	for _, requirement := range requirements {
		occupied := 0
		for _, holder := range s.schedulerHolders[requirement.Resource] {
			if holder.ExpiresAt.After(now) && holder.Invocation != node.ID {
				occupied += holder.Units
			}
		}
		if occupied+requirement.Units > requirement.Limit {
			blocked = append(blocked, requirement.Resource)
		}
	}
	return blocked
}

func (s *Store) expireSchedulerHoldersLocked(now time.Time) {
	for resource, holders := range s.schedulerHolders {
		for invocation, holder := range holders {
			if !holder.ExpiresAt.After(now) {
				delete(holders, invocation)
			}
		}
		if len(holders) == 0 {
			delete(s.schedulerHolders, resource)
		}
	}
}

func (s *Store) releaseSchedulerResourcesLocked(invocation workflowruntime.NodeInvocationID) {
	for resource, holders := range s.schedulerHolders {
		delete(holders, invocation)
		if len(holders) == 0 {
			delete(s.schedulerHolders, resource)
		}
	}
	delete(s.schedulerWaiters, invocation)
}

func (s *Store) renewSchedulerResourcesLocked(invocation workflowruntime.NodeInvocationID, generation uint64, expiresAt time.Time) error {
	for _, holders := range s.schedulerHolders {
		holder, exists := holders[invocation]
		if exists && holder.ClaimGeneration != generation {
			return invalid(errors.New("scheduler holder generation differs from node lease"))
		}
	}
	for resource, holders := range s.schedulerHolders {
		holder, exists := holders[invocation]
		if !exists {
			continue
		}
		holder.ExpiresAt = expiresAt
		holders[invocation] = holder
		s.schedulerHolders[resource] = holders
	}
	return nil
}

func (s *Store) schedulerClaimLiveLocked(id workflowruntime.NodeInvocationID, lease *workflowruntime.ClaimLease, requirements []workflowruntime.SchedulerResourceRequirement, now time.Time) bool {
	if lease == nil {
		return false
	}
	node, exists := s.nodes[id]
	if !exists || node.Lease == nil || node.Lease.Generation != lease.Generation || node.Lease.Owner != lease.Owner || node.Lease.Token != lease.Token || !node.Lease.ExpiresAt.Equal(lease.ExpiresAt) || !node.Lease.ExpiresAt.After(now) {
		return false
	}
	for _, requirement := range requirements {
		holder, exists := s.schedulerHolders[requirement.Resource][id]
		if !exists || holder.Units != requirement.Units || holder.ClaimGeneration != lease.Generation || holder.Owner != lease.Owner || !holder.ExpiresAt.Equal(lease.ExpiresAt) {
			return false
		}
	}
	return true
}

func (s *Store) runActiveLocked(id workflowruntime.RunID) bool {
	run, exists := s.runs[id]
	return exists && run.Status.Active()
}

func fanOutResourceID(id workflowruntime.NodeInvocationID) workflowruntime.SchedulerResourceID {
	return workflowruntime.SchedulerResourceID{Kind: workflowruntime.SchedulerResourceFanOut, RunID: id.RunID, NodeID: id.NodeID}
}

func cloneSchedulerAdmissionRequest(request workflowruntime.AdmitNodeRequest) workflowruntime.AdmitNodeRequest {
	request.Requirements = cloneSchedulerRequirements(request.Requirements)
	return request
}

func equalSchedulerAdmissionRequest(left, right workflowruntime.AdmitNodeRequest) bool {
	if !equalClaimNodeRequest(left.Claim, right.Claim) || left.Priority != right.Priority || !left.EnqueuedAt.Equal(right.EnqueuedAt) || len(left.Requirements) != len(right.Requirements) {
		return false
	}
	for i := range left.Requirements {
		if left.Requirements[i] != right.Requirements[i] {
			return false
		}
	}
	return true
}

func cloneSchedulerAdmissionResult(result workflowruntime.AdmitNodeResult) workflowruntime.AdmitNodeResult {
	result.Claim = cloneClaimResult(result.Claim)
	result.Blocked = append([]workflowruntime.SchedulerResourceID(nil), result.Blocked...)
	return result
}

func cloneSchedulerRequirements(input []workflowruntime.SchedulerResourceRequirement) []workflowruntime.SchedulerResourceRequirement {
	return append([]workflowruntime.SchedulerResourceRequirement(nil), input...)
}

func cloneSchedulerWaiter(waiter workflowruntime.SchedulerResourceWaiter) workflowruntime.SchedulerResourceWaiter {
	waiter.Requirements = cloneSchedulerRequirements(waiter.Requirements)
	waiter.Blocked = append([]workflowruntime.SchedulerResourceID(nil), waiter.Blocked...)
	return waiter
}

func sortSchedulerResourceIDs(ids []workflowruntime.SchedulerResourceID) {
	sort.Slice(ids, func(i, j int) bool { return schedulerResourceIDLess(ids[i], ids[j]) })
}

func schedulerResourceIDLess(left, right workflowruntime.SchedulerResourceID) bool {
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	if left.Name != right.Name {
		return left.Name < right.Name
	}
	if left.RunID != right.RunID {
		return left.RunID < right.RunID
	}
	return left.NodeID < right.NodeID
}

func invocationIDLess(left, right workflowruntime.NodeInvocationID) bool {
	if left.RunID != right.RunID {
		return left.RunID < right.RunID
	}
	if left.NodeID != right.NodeID {
		return left.NodeID < right.NodeID
	}
	return left.Iteration < right.Iteration
}
