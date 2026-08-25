package inmemory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

type crashRecoveryRecord struct {
	request workflowruntime.ReconcileCrashedAttemptRequest
	result  workflowruntime.ReconcileCrashedAttemptResult
}

type replayRecord struct {
	request workflowruntime.BeginReplayRequest
	result  workflowruntime.BeginReplayResult
}

type nodeInputBindingRecord struct {
	request workflowruntime.BindNodeInputsRequest
	result  workflowruntime.BindNodeInputsResult
}

var (
	_ workflowruntime.RecoveryStore  = (*Store)(nil)
	_ workflowruntime.ReplayStore    = (*Store)(nil)
	_ workflowruntime.NodeInputStore = (*Store)(nil)
)

func (s *Store) BindNodeInputs(ctx context.Context, request workflowruntime.BindNodeInputsRequest) (workflowruntime.BindNodeInputsResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.BindNodeInputsResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.BindNodeInputsResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, exists := s.nodeInputBindings[request.IdempotencyKey]; exists {
		if workflowruntime.SemanticallyEqualNodeInputRequest(prior.request, request) {
			result := cloneNodeInputResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.BindNodeInputsResult{}, idempotencyConflict("node input binding", request.IdempotencyKey)
	}
	if _, exists := s.nodeInputOwners[request.InvocationID]; exists {
		return workflowruntime.BindNodeInputsResult{}, invalid(errors.New("node inputs are already durably bound"))
	}
	node, exists := s.nodes[request.InvocationID]
	if !exists {
		return workflowruntime.BindNodeInputsResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	_, exists = s.runs[request.InvocationID.RunID]
	if !exists {
		return workflowruntime.BindNodeInputsResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if !s.runAllowsExecutionLocked(node.ID) || !s.controlAdmissionAllowedLocked(node.ID) {
		return workflowruntime.BindNodeInputsResult{}, invalid(errors.New("run admission fences node input binding"))
	}
	if node.Generation != request.ExpectedGeneration {
		return workflowruntime.BindNodeInputsResult{}, casMismatch("node invocation", request.ExpectedGeneration, node.Generation)
	}
	if node.Status != workflowruntime.NodePending && node.Status != workflowruntime.NodeBlocked {
		return workflowruntime.BindNodeInputsResult{}, invalid(errors.New("node input binding requires unclaimable pending or blocked status"))
	}
	if node.Inputs != nil || node.LatestAttempt != 0 || node.Lease != nil || node.Wait != nil || node.Outputs != nil {
		return workflowruntime.BindNodeInputsResult{}, invalid(errors.New("node input binding requires an unbound pristine invocation"))
	}
	if request.At.Before(node.UpdatedAt) {
		return workflowruntime.BindNodeInputsResult{}, invalid(errors.New("node input binding time must not regress"))
	}
	set, cloneErr := cloneValueSet(request.Values)
	if cloneErr != nil {
		return workflowruntime.BindNodeInputsResult{}, invalid(cloneErr)
	}
	request.Values = set
	id := fmt.Sprintf("values-%012d", s.nextValueSet+1)
	ref, refErr := values.NewValueSetRef(id, set)
	if refErr != nil {
		return workflowruntime.BindNodeInputsResult{}, invalid(refErr)
	}
	next := cloneNode(node)
	next.Inputs, next.MemoKeyDigest, next.UpdatedAt = &ref, request.MemoKeyDigest, request.At
	next.Generation++
	if err := next.Validate(); err != nil {
		return workflowruntime.BindNodeInputsResult{}, invalid(err)
	}
	invocation := next.ID
	event, eventErr := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Type: workflowruntime.EventNodeInputsBound, OccurredAt: request.At, Attributes: map[string]string{"digest": ref.Digest}, Values: &ref, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if eventErr != nil {
		return workflowruntime.BindNodeInputsResult{}, eventErr
	}
	s.nextValueSet++
	s.valueSets[ref.ID] = storedValues{ref: ref, owner: workflowruntime.ValueOwner{Kind: "node-inputs", RunID: invocation.RunID, Invocation: &invocation}, values: set}
	s.nodes[invocation] = next
	result := workflowruntime.BindNodeInputsResult{Outcome: workflowruntime.IdempotencyApplied, Node: cloneNode(next), Inputs: ref, Event: cloneEvent(event)}
	s.nodeInputBindings[request.IdempotencyKey] = nodeInputBindingRecord{request: request, result: result}
	s.nodeInputOwners[invocation] = request.IdempotencyKey
	return cloneNodeInputResult(result), nil
}

func cloneNodeInputResult(input workflowruntime.BindNodeInputsResult) workflowruntime.BindNodeInputsResult {
	input.Node = cloneNode(input.Node)
	input.Event = cloneEvent(input.Event)
	return input
}

func (s *Store) ReconcileCrashedAttempt(ctx context.Context, request workflowruntime.ReconcileCrashedAttemptRequest) (workflowruntime.ReconcileCrashedAttemptResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ReconcileCrashedAttemptResult{}, err
	}
	request = cloneCrashRequest(request)
	request.At = request.At.UTC()
	if request.Decision.Retry != nil {
		request.Decision.Retry.FireAt = request.Decision.Retry.FireAt.UTC()
	}
	if err := validateCrashRequest(request); err != nil {
		return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.crashRecoveries[request.IdempotencyKey]; ok {
		if equalCrashRequest(prior.request, request) {
			result := cloneCrashResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.ReconcileCrashedAttemptResult{}, idempotencyConflict("crash recovery", request.IdempotencyKey)
	}
	node, ok := s.nodes[request.Attempt.Invocation]
	if !ok {
		return workflowruntime.ReconcileCrashedAttemptResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if node.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.ReconcileCrashedAttemptResult{}, casMismatch("node invocation", request.ExpectedNodeGeneration, node.Generation)
	}
	_, ok = s.runs[node.ID.RunID]
	if !ok {
		return workflowruntime.ReconcileCrashedAttemptResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if !s.runAllowsExecutionLocked(node.ID) || !s.controlAdmissionAllowedLocked(node.ID) {
		return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(errors.New("run admission fences crash reconciliation"))
	}
	if node.Status != workflowruntime.NodeRunning || node.LatestAttempt != request.Attempt.Number {
		return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(errors.New("crash recovery requires latest running invocation"))
	}
	attempt, ok := s.attempts[request.Attempt]
	if !ok {
		return workflowruntime.ReconcileCrashedAttemptResult{}, fmt.Errorf("%w: attempt", workflowruntime.ErrNotFound)
	}
	if attempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.ReconcileCrashedAttemptResult{}, casMismatch("attempt", request.ExpectedAttemptGeneration, attempt.Generation)
	}
	if attempt.Status != workflowruntime.NodeRunning || !attempt.FinishedAt.IsZero() {
		return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(errors.New("crash recovery requires unfinished attempt"))
	}
	if node.Lease != nil && node.Lease.ExpiresAt.After(request.At) {
		return workflowruntime.ReconcileCrashedAttemptResult{}, workflowruntime.ErrLeaseExpired
	}
	if request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) {
		return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(errors.New("crash recovery time must not regress"))
	}
	failure := &workflowruntime.Failure{Code: "HADR-PERSIST-001", Message: "executor interrupted before durable completion", Retryable: request.Decision.Action == workflowruntime.CrashRetry, Details: map[string]string{"policy_code": request.Decision.Policy.Code}}
	nextAttempt := cloneAttempt(attempt)
	nextAttempt.Status, nextAttempt.Failure, nextAttempt.FinishedAt, nextAttempt.UpdatedAt = workflowruntime.NodeCrashed, failure, request.At, request.At
	nextAttempt.Generation++
	nextNode := cloneNode(node)
	nextNode.Status, nextNode.Lease, nextNode.Blocked, nextNode.UpdatedAt = workflowruntime.NodeCrashed, nil, nil, request.At
	var activation *workflowruntime.RetryActivationSnapshot
	if request.Decision.Action == workflowruntime.CrashRetry {
		nextNode.Status = workflowruntime.NodeWaiting
		encoded, encodeErr := workflowruntime.EncodeAttemptIdentity(attempt.ID)
		if encodeErr != nil {
			return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(encodeErr)
		}
		candidate := workflowruntime.RetryActivationSnapshot{ID: "retry:" + encoded, Attempt: attempt.ID, Failure: *cloneFailure(failure), FireAt: request.Decision.Retry.FireAt.UTC(), Status: workflowruntime.RetryScheduled, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
		if _, exists := s.retryActivations[candidate.ID]; exists {
			return workflowruntime.ReconcileCrashedAttemptResult{}, fmt.Errorf("%w: retry activation", workflowruntime.ErrAlreadyExists)
		}
		if err := candidate.Validate(); err != nil {
			return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(err)
		}
		activation = &candidate
	} else {
		nextNode.Origin = workflowruntime.OriginExecuted
	}
	nextNode.Outputs = nil
	nextNode.Generation++
	if err := nextAttempt.Validate(); err != nil {
		return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.ReconcileCrashedAttemptResult{}, invalid(err)
	}
	invocation, attemptID := nextNode.ID, nextAttempt.ID
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventCrashReconciled, OccurredAt: request.At, Attributes: map[string]string{"action": string(request.Decision.Action), "policy_code": request.Decision.Policy.Code, "executor_kind": attempt.Executor.Kind, "executor_version": attempt.Executor.Version}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return workflowruntime.ReconcileCrashedAttemptResult{}, err
	}
	s.releaseSchedulerResourcesLocked(node.ID)
	s.nodes[node.ID], s.attempts[attempt.ID] = nextNode, nextAttempt
	if activation != nil {
		s.retryActivations[activation.ID] = cloneRetryActivation(*activation)
	}
	result := workflowruntime.ReconcileCrashedAttemptResult{Outcome: workflowruntime.IdempotencyApplied, Node: cloneNode(nextNode), Attempt: cloneAttempt(nextAttempt), Event: cloneEvent(event), Activation: cloneRetryActivationPointer(activation)}
	s.crashRecoveries[request.IdempotencyKey] = crashRecoveryRecord{request: cloneCrashRequest(request), result: cloneCrashResult(result)}
	return result, nil
}

func (s *Store) BeginReplay(ctx context.Context, request workflowruntime.BeginReplayRequest) (workflowruntime.BeginReplayResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.BeginReplayResult{}, err
	}
	request = cloneReplayRequest(request)
	request.Provenance.CreatedAt = request.Provenance.CreatedAt.UTC()
	for i := range request.Nodes {
		request.Nodes[i].Source.CreatedAt = request.Nodes[i].Source.CreatedAt.UTC()
		request.Nodes[i].Source.UpdatedAt = request.Nodes[i].Source.UpdatedAt.UTC()
		if request.Nodes[i].Source.Lease != nil {
			request.Nodes[i].Source.Lease.ExpiresAt = request.Nodes[i].Source.Lease.ExpiresAt.UTC()
		}
		for j := range request.Nodes[i].Attempts {
			request.Nodes[i].Attempts[j].StartedAt = request.Nodes[i].Attempts[j].StartedAt.UTC()
			request.Nodes[i].Attempts[j].FinishedAt = request.Nodes[i].Attempts[j].FinishedAt.UTC()
			request.Nodes[i].Attempts[j].CreatedAt = request.Nodes[i].Attempts[j].CreatedAt.UTC()
			request.Nodes[i].Attempts[j].UpdatedAt = request.Nodes[i].Attempts[j].UpdatedAt.UTC()
		}
		for j := range request.Nodes[i].Control {
			request.Nodes[i].Control[j].CreatedAt = request.Nodes[i].Control[j].CreatedAt.UTC()
		}
	}
	for i := range request.FanOuts {
		request.FanOuts[i].Source.CreatedAt = request.FanOuts[i].Source.CreatedAt.UTC()
		request.FanOuts[i].Source.UpdatedAt = request.FanOuts[i].Source.UpdatedAt.UTC()
		request.FanOuts[i].Target.CreatedAt = request.FanOuts[i].Target.CreatedAt.UTC()
		request.FanOuts[i].Target.UpdatedAt = request.FanOuts[i].Target.UpdatedAt.UTC()
	}
	if err := request.Validate(); err != nil {
		return workflowruntime.BeginReplayResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.replayKeys[request.Provenance.IdempotencyKey]; ok {
		if equalReplayRequest(prior.request, request) {
			result := cloneReplayResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.BeginReplayResult{}, idempotencyConflict("begin replay", request.Provenance.IdempotencyKey)
	}
	if _, exists := s.runs[request.Provenance.RunID]; exists {
		return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay run", workflowruntime.ErrAlreadyExists)
	}
	source, exists := s.runs[request.Provenance.SourceRunID]
	if !exists {
		return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: source run", workflowruntime.ErrNotFound)
	}
	if source.Generation != request.ExpectedSourceGeneration {
		return workflowruntime.BeginReplayResult{}, casMismatch("source run", request.ExpectedSourceGeneration, source.Generation)
	}
	if !source.Status.Terminal() || source.Plan != request.Plan || !equalValueSetRef(source.Inputs, request.Inputs) {
		return workflowruntime.BeginReplayResult{}, invalid(errors.New("replay source binding changed"))
	}
	newNodes := make([]workflowruntime.NodeInvocationSnapshot, 0, len(request.Nodes))
	newAttempts := make([]workflowruntime.AttemptSnapshot, 0)
	newDecisions := make([]workflowruntime.ControlDecisionSnapshot, 0)
	newFanOuts := make([]workflowruntime.FanOutSnapshot, 0, len(request.FanOuts))
	for _, binding := range request.Nodes {
		current, ok := s.nodes[binding.Source.ID]
		if !ok {
			return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay source node", workflowruntime.ErrNotFound)
		}
		if !equalNodeSemantic(current, binding.Source) {
			return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay source node changed", workflowruntime.ErrCASMismatch)
		}
		for _, ref := range []*values.ValueSetRef{binding.Source.Inputs, binding.Source.Outputs} {
			if ref == nil {
				continue
			}
			stored, ok := s.valueSets[ref.ID]
			if !ok {
				return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay value set", workflowruntime.ErrNotFound)
			}
			if stored.ref.Digest != ref.Digest {
				return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay value digest", workflowruntime.ErrCASMismatch)
			}
		}
		next := workflowruntime.NodeInvocationSnapshot{ID: binding.Target, Status: workflowruntime.NodePending, Priority: binding.Source.Priority, Generation: 1, CreatedAt: request.Provenance.CreatedAt, UpdatedAt: request.Provenance.CreatedAt}
		if binding.Reuse {
			next.Status, next.Blocked, next.Inputs, next.Outputs, next.LatestAttempt = binding.Source.Status, cloneBlocked(binding.Source.Blocked), cloneValueSetRef(binding.Source.Inputs), cloneValueSetRef(binding.Source.Outputs), binding.Source.LatestAttempt
			next.Origin = workflowruntime.OriginReplayed
			for _, sourceAttempt := range binding.Attempts {
				persisted, exists := s.attempts[sourceAttempt.ID]
				if !exists || !equalAttemptSemantic(persisted, sourceAttempt) {
					return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay source attempt changed", workflowruntime.ErrCASMismatch)
				}
				rebound := cloneAttempt(sourceAttempt)
				rebound.ID.Invocation = binding.Target
				newAttempts = append(newAttempts, rebound)
			}
		}
		if err := next.Validate(); err != nil {
			return workflowruntime.BeginReplayResult{}, invalid(err)
		}
		newNodes = append(newNodes, next)
		for _, decision := range binding.Control {
			persisted, ok := s.controlDecisions[decision.ID]
			if !ok || !equalControlDecision(persisted, decision) {
				return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay control decision changed", workflowruntime.ErrCASMismatch)
			}
			if decision.Error != nil {
				stored, ok := s.valueSets[decision.Error.ID]
				if !ok || stored.ref.Digest != decision.Error.Digest {
					return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay control error", workflowruntime.ErrCASMismatch)
				}
			}
			rebound, err := workflowruntime.RebindReplayControlDecision(decision, request.Provenance.RunID, request.Provenance.CreatedAt)
			if err != nil {
				return workflowruntime.BeginReplayResult{}, invalid(err)
			}
			newDecisions = append(newDecisions, rebound)
		}
	}
	for _, binding := range request.FanOuts {
		persisted, exists := s.fanOuts[binding.Source.Parent]
		if !exists || !reflect.DeepEqual(cloneFanOut(persisted), binding.Source) {
			return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay source fan-out changed", workflowruntime.ErrCASMismatch)
		}
		for _, item := range binding.Results {
			node, exists := s.nodes[item.Invocation]
			if !exists || node.Status != item.Status || !equalValueSetRef(node.Outputs, item.Outputs) {
				return workflowruntime.BeginReplayResult{}, fmt.Errorf("%w: replay fan-out item changed", workflowruntime.ErrCASMismatch)
			}
		}
		newFanOuts = append(newFanOuts, cloneFanOut(binding.Target))
	}
	run := workflowruntime.RunSnapshot{ID: request.Provenance.RunID, Plan: request.Plan, Status: workflowruntime.RunRunning, Inputs: cloneValueSetRef(request.Inputs), Generation: 1, CreatedAt: request.Provenance.CreatedAt, UpdatedAt: request.Provenance.CreatedAt}
	if err := run.Validate(); err != nil {
		return workflowruntime.BeginReplayResult{}, invalid(err)
	}
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: run.ID, Type: workflowruntime.EventReplayCreated, OccurredAt: run.CreatedAt, Attributes: map[string]string{"source_run_id": string(source.ID), "from_node_id": request.Provenance.FromNodeID, "plan_digest": request.Plan.Digest}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return workflowruntime.BeginReplayResult{}, err
	}
	s.runs[run.ID] = cloneRun(run)
	for _, node := range newNodes {
		s.nodes[node.ID] = cloneNode(node)
	}
	for _, attempt := range newAttempts {
		s.attempts[attempt.ID] = cloneAttempt(attempt)
	}
	for _, fanOut := range newFanOuts {
		s.fanOuts[fanOut.Parent] = cloneFanOut(fanOut)
	}
	for _, decision := range newDecisions {
		s.controlDecisions[decision.ID] = cloneControlDecision(decision)
	}
	s.replays[run.ID] = cloneReplayProvenance(request.Provenance)
	result := workflowruntime.BeginReplayResult{Outcome: workflowruntime.IdempotencyApplied, Run: cloneRun(run), Provenance: cloneReplayProvenance(request.Provenance), Nodes: cloneNodes(newNodes), Event: cloneEvent(event)}
	s.replayKeys[request.Provenance.IdempotencyKey] = replayRecord{request: cloneReplayRequest(request), result: cloneReplayResult(result)}
	return result, nil
}

func (s *Store) ListRunInvocations(ctx context.Context, runID workflowruntime.RunID) ([]workflowruntime.NodeInvocationSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.runs[runID]; !ok {
		return nil, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	result := make([]workflowruntime.NodeInvocationSnapshot, 0)
	for id, node := range s.nodes {
		if id.RunID == runID {
			result = append(result, cloneNode(node))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ID.NodeID != result[j].ID.NodeID {
			return result[i].ID.NodeID < result[j].ID.NodeID
		}
		return result[i].ID.Iteration < result[j].ID.Iteration
	})
	return result, nil
}

func (s *Store) LoadReplayProvenance(ctx context.Context, runID workflowruntime.RunID) (workflowruntime.ReplayProvenance, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ReplayProvenance{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, ok := s.replays[runID]
	if !ok {
		return workflowruntime.ReplayProvenance{}, fmt.Errorf("%w: replay provenance", workflowruntime.ErrNotFound)
	}
	return cloneReplayProvenance(result), nil
}

func validateCrashRequest(request workflowruntime.ReconcileCrashedAttemptRequest) error {
	if err := request.Attempt.Validate(); err != nil {
		return err
	}
	if request.ExpectedNodeGeneration == 0 || request.ExpectedAttemptGeneration == 0 || request.At.IsZero() {
		return errors.New("crash recovery requires generations and timestamp")
	}
	if request.IdempotencyKey == "" {
		return errors.New("crash recovery idempotency key is required")
	}
	return request.Decision.Validate()
}

func equalCrashRequest(a, b workflowruntime.ReconcileCrashedAttemptRequest) bool {
	return a.Attempt == b.Attempt && a.ExpectedNodeGeneration == b.ExpectedNodeGeneration && a.ExpectedAttemptGeneration == b.ExpectedAttemptGeneration && a.IdempotencyKey == b.IdempotencyKey && a.At.Equal(b.At) && reflect.DeepEqual(a.Decision, b.Decision)
}

func cloneCrashRequest(r workflowruntime.ReconcileCrashedAttemptRequest) workflowruntime.ReconcileCrashedAttemptRequest {
	r.Decision.Policy.Attributes = cloneStringMap(r.Decision.Policy.Attributes)
	if r.Decision.Retry != nil {
		decision := *r.Decision.Retry
		r.Decision.Retry = &decision
	}
	return r
}
func cloneCrashResult(r workflowruntime.ReconcileCrashedAttemptResult) workflowruntime.ReconcileCrashedAttemptResult {
	r.Node, r.Attempt, r.Event = cloneNode(r.Node), cloneAttempt(r.Attempt), cloneEvent(r.Event)
	r.Activation = cloneRetryActivationPointer(r.Activation)
	return r
}

func cloneRetryActivationPointer(input *workflowruntime.RetryActivationSnapshot) *workflowruntime.RetryActivationSnapshot {
	if input == nil {
		return nil
	}
	cloned := cloneRetryActivation(*input)
	return &cloned
}

func equalNodeSemantic(a, b workflowruntime.NodeInvocationSnapshot) bool {
	return a.ID == b.ID && a.Status == b.Status && equalBlockedReason(a.Blocked, b.Blocked) && equalValueSetRef(a.Inputs, b.Inputs) && equalValueSetRef(a.Outputs, b.Outputs) && a.Origin == b.Origin && a.MemoKeyDigest == b.MemoKeyDigest && reflect.DeepEqual(a.Wait, b.Wait) && a.LatestAttempt == b.LatestAttempt && a.Priority == b.Priority && a.ClaimGeneration == b.ClaimGeneration && equalLease(a.Lease, b.Lease) && a.Generation == b.Generation && a.CreatedAt.Equal(b.CreatedAt) && a.UpdatedAt.Equal(b.UpdatedAt)
}

func equalAttemptSemantic(a, b workflowruntime.AttemptSnapshot) bool {
	return a.ID == b.ID && a.Status == b.Status && reflect.DeepEqual(a.Executor, b.Executor) && equalValueSetRef(a.Inputs, b.Inputs) && equalValueSetRef(a.Outputs, b.Outputs) && reflect.DeepEqual(a.Failure, b.Failure) && a.StartedAt.Equal(b.StartedAt) && a.FinishedAt.Equal(b.FinishedAt) && a.Generation == b.Generation && a.CreatedAt.Equal(b.CreatedAt) && a.UpdatedAt.Equal(b.UpdatedAt)
}

func cloneReplayProvenance(p workflowruntime.ReplayProvenance) workflowruntime.ReplayProvenance {
	if p.CompensationAuthorization != nil {
		cloned := *p.CompensationAuthorization
		p.CompensationAuthorization = &cloned
	}
	p.Policy = append([]workflowruntime.ReplayNodePolicy(nil), p.Policy...)
	for i := range p.Policy {
		p.Policy[i].Attempt = cloneAttemptID(p.Policy[i].Attempt)
		p.Policy[i].Decision.Attributes = cloneStringMap(p.Policy[i].Decision.Attributes)
	}
	return p
}
func cloneReplayRequest(r workflowruntime.BeginReplayRequest) workflowruntime.BeginReplayRequest {
	r.Provenance = cloneReplayProvenance(r.Provenance)
	r.Inputs = cloneValueSetRef(r.Inputs)
	r.Nodes = append([]workflowruntime.ReplayNodeBinding(nil), r.Nodes...)
	for i := range r.Nodes {
		r.Nodes[i].Source = cloneNode(r.Nodes[i].Source)
		r.Nodes[i].Attempts = append([]workflowruntime.AttemptSnapshot(nil), r.Nodes[i].Attempts...)
		for j := range r.Nodes[i].Attempts {
			r.Nodes[i].Attempts[j] = cloneAttempt(r.Nodes[i].Attempts[j])
		}
		r.Nodes[i].Control = append([]workflowruntime.ControlDecisionSnapshot(nil), r.Nodes[i].Control...)
		for j := range r.Nodes[i].Control {
			r.Nodes[i].Control[j] = cloneControlDecision(r.Nodes[i].Control[j])
		}
	}
	r.FanOuts = append([]workflowruntime.ReplayFanOutBinding(nil), r.FanOuts...)
	for i := range r.FanOuts {
		r.FanOuts[i].Source = cloneFanOut(r.FanOuts[i].Source)
		r.FanOuts[i].Target = cloneFanOut(r.FanOuts[i].Target)
		r.FanOuts[i].Results = cloneFanOutItemResults(r.FanOuts[i].Results)
	}
	return r
}
func cloneReplayResult(r workflowruntime.BeginReplayResult) workflowruntime.BeginReplayResult {
	r.Run = cloneRun(r.Run)
	r.Provenance = cloneReplayProvenance(r.Provenance)
	r.Nodes = cloneNodes(r.Nodes)
	r.Event = cloneEvent(r.Event)
	return r
}
func equalReplayRequest(a, b workflowruntime.BeginReplayRequest) bool {
	if a.Plan != b.Plan || !equalValueSetRef(a.Inputs, b.Inputs) || a.ExpectedSourceGeneration != b.ExpectedSourceGeneration || !a.Provenance.CreatedAt.Equal(b.Provenance.CreatedAt) {
		return false
	}
	a.Provenance.CreatedAt, b.Provenance.CreatedAt = time.Time{}, time.Time{}
	return reflect.DeepEqual(a, b)
}
