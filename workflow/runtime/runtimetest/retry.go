package runtimetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func (s *Store) LoadRetryActivation(ctx context.Context, id string) (workflowruntime.RetryActivationSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RetryActivationSnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.retryActivations[id]
	if !ok {
		return workflowruntime.RetryActivationSnapshot{}, fmt.Errorf("%w: retry activation %q", workflowruntime.ErrNotFound, id)
	}
	return cloneRetryActivation(snapshot), nil
}

func (s *Store) ScheduleNodeRetry(ctx context.Context, request workflowruntime.ScheduleNodeRetryRequest) (workflowruntime.ScheduleNodeRetryResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, err
	}
	request.At = request.At.UTC()
	request.Activation.FireAt = request.Activation.FireAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[request.Activation.Attempt.Invocation.RunID]
	if !ok {
		return workflowruntime.ScheduleNodeRetryResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if !run.Status.Active() {
		return workflowruntime.ScheduleNodeRetryResult{}, invalid(errors.New("terminal run cannot schedule retry"))
	}
	currentNode, ok := s.nodes[request.Activation.Attempt.Invocation]
	if !ok {
		return workflowruntime.ScheduleNodeRetryResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if currentNode.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.ScheduleNodeRetryResult{}, casMismatch("node invocation", request.ExpectedNodeGeneration, currentNode.Generation)
	}
	if currentNode.Status != workflowruntime.NodeRunning || currentNode.LatestAttempt != request.Activation.Attempt.Number {
		return workflowruntime.ScheduleNodeRetryResult{}, attemptConflict(currentNode.ID, request.Activation.Attempt.Number, "retry requires latest running attempt")
	}
	if err := validateLifecycleClaim(currentNode, &request.Claim, request.At); err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, err
	}
	currentAttempt, ok := s.attempts[request.Activation.Attempt]
	if !ok {
		return workflowruntime.ScheduleNodeRetryResult{}, fmt.Errorf("%w: attempt", workflowruntime.ErrNotFound)
	}
	if currentAttempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.ScheduleNodeRetryResult{}, casMismatch("attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
	}
	if currentAttempt.Status != workflowruntime.NodeRunning || !currentAttempt.FinishedAt.IsZero() {
		return workflowruntime.ScheduleNodeRetryResult{}, attemptConflict(currentNode.ID, currentAttempt.ID.Number, "retry attempt is already finished")
	}
	if request.At.Before(currentNode.UpdatedAt) || request.At.Before(currentAttempt.UpdatedAt) {
		return workflowruntime.ScheduleNodeRetryResult{}, invalid(errors.New("retry schedule time must not regress persisted state"))
	}
	if _, exists := s.retryActivations[request.Activation.ID]; exists {
		return workflowruntime.ScheduleNodeRetryResult{}, fmt.Errorf("%w: retry activation %q", workflowruntime.ErrAlreadyExists, request.Activation.ID)
	}
	for _, existing := range s.retryActivations {
		if existing.Attempt == currentAttempt.ID {
			return workflowruntime.ScheduleNodeRetryResult{}, fmt.Errorf("%w: attempt already has retry activation", workflowruntime.ErrAlreadyExists)
		}
	}

	nextAttempt := cloneAttempt(currentAttempt)
	nextAttempt.Status = request.AttemptStatus
	nextAttempt.Failure = cloneFailure(&request.Activation.Failure)
	nextAttempt.FinishedAt = request.At
	nextAttempt.UpdatedAt = request.At
	nextAttempt.Generation++
	nextNode := cloneNode(currentNode)
	nextNode.Status = workflowruntime.NodeWaiting
	nextNode.Outputs = nil
	nextNode.Blocked = nil
	nextNode.Wait = nil
	nextNode.Lease = nil
	nextNode.Generation++
	nextNode.UpdatedAt = request.At
	activation := cloneRetryActivation(request.Activation)
	activation.Generation = 1
	activation.CreatedAt, activation.UpdatedAt = request.At, request.At
	if err := nextAttempt.Validate(); err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, invalid(err)
	}
	if err := activation.Validate(); err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, invalid(err)
	}
	invocation, attemptID := nextNode.ID, nextAttempt.ID
	finishedEvent, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID,
		Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: request.At,
		Attributes: map[string]string{
			"attempt_number": strconv.Itoa(attemptID.Number), "attempt_status": string(nextAttempt.Status),
			"from_status": string(currentNode.Status), "to_status": string(nextNode.Status),
			"failure_code": nextAttempt.Failure.Code,
		}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, err
	}
	retryEvent, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID,
		Type: workflowruntime.EventRetryScheduled, OccurredAt: request.At,
		Attributes: map[string]string{"activation_id": activation.ID, "fire_at": activation.FireAt.Format(timeLayout)},
		Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, err
	}
	s.nodes[nextNode.ID] = nextNode
	s.attempts[nextAttempt.ID] = nextAttempt
	s.retryActivations[activation.ID] = activation
	return workflowruntime.ScheduleNodeRetryResult{
		Activation: cloneRetryActivation(activation), Node: cloneNode(nextNode), Attempt: cloneAttempt(nextAttempt),
		Events: []workflowruntime.Event{cloneEvent(finishedEvent), cloneEvent(retryEvent)},
	}, nil
}

func (s *Store) ActivateNodeRetry(ctx context.Context, request workflowruntime.ActivateNodeRetryRequest) (workflowruntime.ActivateNodeRetryResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ActivateNodeRetryResult{}, err
	}
	request.Now = request.Now.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ActivateNodeRetryResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.retryActivationKeys[request.IdempotencyKey]; ok {
		if equalActivateRetryRequest(prior.request, request) {
			result := cloneActivateRetryResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.ActivateNodeRetryResult{}, idempotencyConflict("activate retry", request.IdempotencyKey)
	}
	activation, ok := s.retryActivations[request.ActivationID]
	if !ok {
		return workflowruntime.ActivateNodeRetryResult{}, fmt.Errorf("%w: retry activation", workflowruntime.ErrNotFound)
	}
	if activation.Generation != request.ExpectedActivationGeneration {
		return workflowruntime.ActivateNodeRetryResult{}, casMismatch("retry activation", request.ExpectedActivationGeneration, activation.Generation)
	}
	if activation.Status != workflowruntime.RetryScheduled {
		return workflowruntime.ActivateNodeRetryResult{}, invalid(errors.New("retry activation is not scheduled"))
	}
	if request.Now.Before(activation.FireAt) {
		return workflowruntime.ActivateNodeRetryResult{}, fmt.Errorf("%w: fire_at %s", workflowruntime.ErrRetryNotDue, activation.FireAt.Format(timeLayout))
	}
	run := s.runs[activation.Attempt.Invocation.RunID]
	if !run.Status.Active() {
		return workflowruntime.ActivateNodeRetryResult{}, invalid(errors.New("terminal run fences retry activation"))
	}
	node, ok := s.nodes[activation.Attempt.Invocation]
	if !ok {
		return workflowruntime.ActivateNodeRetryResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if node.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.ActivateNodeRetryResult{}, casMismatch("node invocation", request.ExpectedNodeGeneration, node.Generation)
	}
	if node.Status != workflowruntime.NodeWaiting || node.LatestAttempt != activation.Attempt.Number || node.Wait != nil || node.Lease != nil {
		return workflowruntime.ActivateNodeRetryResult{}, invalid(errors.New("retry activation requires unleased retry-waiting node"))
	}
	nextActivation := cloneRetryActivation(activation)
	nextActivation.Status = workflowruntime.RetryActivated
	nextActivation.Generation++
	nextActivation.UpdatedAt = request.Now
	nextNode := cloneNode(node)
	nextNode.Status = workflowruntime.NodeReady
	nextNode.Generation++
	nextNode.UpdatedAt = request.Now
	if err := nextActivation.Validate(); err != nil {
		return workflowruntime.ActivateNodeRetryResult{}, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.ActivateNodeRetryResult{}, invalid(err)
	}
	invocation, attemptID := nextNode.ID, activation.Attempt
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID,
		Type: workflowruntime.EventRetryActivated, OccurredAt: request.Now,
		Attributes: map[string]string{"activation_id": activation.ID, "from_status": string(node.Status), "to_status": string(nextNode.Status)},
		Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.ActivateNodeRetryResult{}, err
	}
	s.retryActivations[activation.ID] = nextActivation
	s.nodes[nextNode.ID] = nextNode
	eventCopy := cloneEvent(event)
	result := workflowruntime.ActivateNodeRetryResult{
		Outcome: workflowruntime.IdempotencyApplied, Activation: cloneRetryActivation(nextActivation),
		Node: cloneNode(nextNode), Event: &eventCopy,
	}
	s.retryActivationKeys[request.IdempotencyKey] = retryActivationRecord{request: request, result: cloneActivateRetryResult(result)}
	return result, nil
}

func (s *Store) RecoverRetryActivations(ctx context.Context, query workflowruntime.RetryActivationQuery) ([]workflowruntime.RetryActivationSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if query.RunID != "" {
		if err := (workflowruntime.NodeInvocationID{RunID: query.RunID, NodeID: "candidate"}).Validate(); err != nil {
			return nil, invalid(err)
		}
	}
	if query.Limit < 0 {
		return nil, invalid(errors.New("retry recovery limit must not be negative"))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]workflowruntime.RetryActivationSnapshot, 0)
	for _, activation := range s.retryActivations {
		if activation.Status != workflowruntime.RetryScheduled || query.RunID != "" && activation.Attempt.Invocation.RunID != query.RunID {
			continue
		}
		if !query.DueBefore.IsZero() && activation.FireAt.After(query.DueBefore) {
			continue
		}
		result = append(result, cloneRetryActivation(activation))
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].FireAt.Equal(result[j].FireAt) {
			return result[i].FireAt.Before(result[j].FireAt)
		}
		return result[i].ID < result[j].ID
	})
	return limit(result, query.Limit), nil
}

const timeLayout = "2006-01-02T15:04:05.999999999Z07:00"

func cloneRetryActivation(snapshot workflowruntime.RetryActivationSnapshot) workflowruntime.RetryActivationSnapshot {
	snapshot.Failure.Details = cloneStringMap(snapshot.Failure.Details)
	return snapshot
}

func equalActivateRetryRequest(left, right workflowruntime.ActivateNodeRetryRequest) bool {
	return left.ActivationID == right.ActivationID &&
		left.ExpectedActivationGeneration == right.ExpectedActivationGeneration &&
		left.ExpectedNodeGeneration == right.ExpectedNodeGeneration &&
		left.IdempotencyKey == right.IdempotencyKey && left.Now.Equal(right.Now)
}

func cloneActivateRetryResult(result workflowruntime.ActivateNodeRetryResult) workflowruntime.ActivateNodeRetryResult {
	result.Activation = cloneRetryActivation(result.Activation)
	result.Node = cloneNode(result.Node)
	if result.Event != nil {
		event := cloneEvent(*result.Event)
		result.Event = &event
	}
	return result
}
