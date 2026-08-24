package runtimetest

import (
	"context"
	"errors"
	"fmt"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var _ workflowruntime.WaitTimeoutStore = (*Store)(nil)

// TimeoutWait atomically marks an open wait, its waiting attempt, and its node
// timed out while appending all derived events under one store lock.
func (s *Store) TimeoutWait(ctx context.Context, request workflowruntime.TimeoutWaitRequest) (workflowruntime.WaitTimeoutResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.WaitTimeoutResult{}, err
	}
	if err := request.Validate(); err != nil {
		return workflowruntime.WaitTimeoutResult{}, invalid(err)
	}
	request.Deadline = request.Deadline.UTC()
	request.Now = request.Now.UTC()
	if request.Now.Before(request.Deadline) {
		return workflowruntime.WaitTimeoutResult{}, &workflowruntime.WaitTimeoutNotDueError{
			Now: request.Now, Deadline: request.Deadline,
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.timeouts[request.IdempotencyKey]; ok {
		if equalTimeoutWaitRequest(prior.request, request) {
			result := cloneWaitTimeoutResult(prior.result)
			result.Replayed = true
			return result, nil
		}
		return workflowruntime.WaitTimeoutResult{}, idempotencyConflict("timeout wait", request.IdempotencyKey)
	}

	currentWait, ok := s.waits[request.WaitID]
	if !ok {
		return workflowruntime.WaitTimeoutResult{}, fmt.Errorf("%w: wait %q", workflowruntime.ErrNotFound, request.WaitID)
	}
	if currentWait.Deadline.IsZero() || !currentWait.Deadline.Equal(request.Deadline) {
		return workflowruntime.WaitTimeoutResult{}, invalid(errors.New("timeout deadline must exactly match the persisted wait deadline"))
	}
	currentNode, ok := s.nodes[currentWait.Invocation]
	if !ok {
		return workflowruntime.WaitTimeoutResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if currentWait.Status != workflowruntime.WaitOpen {
		result := workflowruntime.WaitTimeoutResult{
			Applied: false, Wait: cloneWait(currentWait), Node: cloneNode(currentNode),
		}
		s.timeouts[request.IdempotencyKey] = timeoutRecord{request: request, result: cloneWaitTimeoutResult(result)}
		return result, nil
	}
	if !currentWait.WakeAt.IsZero() && !currentWait.WakeAt.After(request.Deadline) {
		return workflowruntime.WaitTimeoutResult{}, workflowruntime.ErrWaitWakePending
	}
	if currentWait.Generation != request.ExpectedWaitGeneration {
		return workflowruntime.WaitTimeoutResult{}, casMismatch("wait timeout", request.ExpectedWaitGeneration, currentWait.Generation)
	}
	if currentNode.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.WaitTimeoutResult{}, casMismatch("wait timeout node", request.ExpectedNodeGeneration, currentNode.Generation)
	}
	if currentNode.Status != workflowruntime.NodeWaiting {
		return workflowruntime.WaitTimeoutResult{}, invalid(errors.New("open wait timeout requires a waiting node"))
	}
	if currentWait.Invocation != currentNode.ID {
		return workflowruntime.WaitTimeoutResult{}, invalid(errors.New("wait invocation does not match waiting node"))
	}
	if currentNode.Wait == nil || currentNode.Wait.ID != currentWait.Ref.ID {
		return workflowruntime.WaitTimeoutResult{}, invalid(errors.New("waiting node does not reference the open wait"))
	}
	if request.Deadline.Before(currentWait.CreatedAt) {
		return workflowruntime.WaitTimeoutResult{}, invalid(errors.New("wait deadline must not precede wait creation"))
	}
	if request.Now.Before(currentWait.UpdatedAt) || request.Now.Before(currentNode.UpdatedAt) {
		return workflowruntime.WaitTimeoutResult{}, invalid(errors.New("wait timeout time must not regress persisted state"))
	}
	unfinished, err := s.unfinishedAttemptLocked(currentNode)
	if err != nil {
		return workflowruntime.WaitTimeoutResult{}, err
	}
	if unfinished == nil || unfinished.ID.Number != currentNode.LatestAttempt {
		return workflowruntime.WaitTimeoutResult{}, attemptConflict(currentNode.ID, currentNode.LatestAttempt, "wait timeout requires the matching unfinished attempt")
	}
	if request.Now.Before(unfinished.UpdatedAt) {
		return workflowruntime.WaitTimeoutResult{}, invalid(errors.New("wait timeout time must not regress attempt state"))
	}

	failure := workflowruntime.WaitTimeoutFailure(request.Deadline)
	nextWait := cloneWait(currentWait)
	nextWait.Status = workflowruntime.WaitTimedOut
	nextWait.Resolution = &workflowwait.Resolution{
		Source:     workflowwait.WakeTimer,
		Responder:  workflowwait.Responder{Kind: "system", Reference: "wait-timeout"},
		ResolvedAt: request.Now,
	}
	nextWait.ResolvedAt = request.Now
	nextWait.UpdatedAt = request.Now
	nextWait.Generation++
	nextNode := cloneNode(currentNode)
	nextNode.Status = workflowruntime.NodeTimedOut
	nextNode.Blocked = nil
	nextNode.Wait = nil
	nextNode.Lease = nil
	nextNode.Outputs = nil
	nextNode.UpdatedAt = request.Now
	nextNode.Generation++
	nextAttempt := cloneAttempt(*unfinished)
	nextAttempt.Status = workflowruntime.NodeTimedOut
	nextAttempt.Outputs = nil
	nextAttempt.Failure = cloneFailure(&failure)
	nextAttempt.FinishedAt = request.Now
	nextAttempt.UpdatedAt = request.Now
	nextAttempt.Generation++
	if err := nextWait.Validate(); err != nil {
		return workflowruntime.WaitTimeoutResult{}, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.WaitTimeoutResult{}, invalid(err)
	}
	if err := nextAttempt.Validate(); err != nil {
		return workflowruntime.WaitTimeoutResult{}, invalid(err)
	}

	invocation := nextNode.ID
	attemptID := nextAttempt.ID
	attemptEventAttrs := attemptAttributes("node_attempt", string(unfinished.Status), string(nextAttempt.Status), nextAttempt)
	attemptEventAttrs["attempt_status"] = string(nextAttempt.Status)
	attemptEventAttrs["failure_code"] = failure.Code
	nodeAttributes := attemptAttributes("node", string(currentNode.Status), string(nextNode.Status), nextAttempt)
	nodeAttributes["failure_code"] = failure.Code
	waitID := string(currentWait.Ref.ID)
	deadline := request.Deadline.Format(time.RFC3339Nano)
	attemptEventAttrs["wait_id"] = waitID
	attemptEventAttrs["deadline"] = deadline
	nodeAttributes["wait_id"] = waitID
	nodeAttributes["deadline"] = deadline
	waitAttributes := map[string]string{
		"entity": "wait", "from_status": string(currentWait.Status), "to_status": string(nextWait.Status),
		"wait_id": waitID, "deadline": deadline,
	}
	eventRequests := []workflowruntime.AppendEventRequest{
		{
			RunID: nextNode.ID.RunID, Invocation: &invocation, Attempt: &attemptID,
			Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: request.Now, Attributes: attemptEventAttrs,
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		},
		{
			RunID: nextNode.ID.RunID, Invocation: &invocation, Attempt: &attemptID,
			Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.Now, Attributes: nodeAttributes,
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		},
		{
			RunID: nextNode.ID.RunID, Invocation: &invocation, Attempt: &attemptID,
			Type: workflowruntime.EventWaitTimedOut, OccurredAt: request.Now, Attributes: waitAttributes,
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		},
	}
	baseSequence := len(s.events[nextNode.ID.RunID])
	for i, eventRequest := range eventRequests {
		candidate := workflowruntime.Event{
			Sequence: uint64(baseSequence + i + 1), RunID: eventRequest.RunID,
			Invocation: eventRequest.Invocation, Attempt: eventRequest.Attempt,
			Type: eventRequest.Type, OccurredAt: eventRequest.OccurredAt,
			Attributes: eventRequest.Attributes, Redaction: eventRequest.Redaction, Retention: eventRequest.Retention,
		}
		if err := candidate.Validate(); err != nil {
			return workflowruntime.WaitTimeoutResult{}, invalid(err)
		}
	}
	events := make([]workflowruntime.Event, 0, len(eventRequests))
	for _, eventRequest := range eventRequests {
		event, appendErr := s.appendEventLocked(eventRequest)
		if appendErr != nil {
			return workflowruntime.WaitTimeoutResult{}, appendErr
		}
		events = append(events, event)
	}

	s.waits[nextWait.Ref.ID] = nextWait
	s.nodes[nextNode.ID] = nextNode
	s.attempts[nextAttempt.ID] = nextAttempt
	result := workflowruntime.WaitTimeoutResult{
		Applied: true, Wait: cloneWait(nextWait), Node: cloneNode(nextNode),
		Attempt: cloneAttempt(nextAttempt), Events: cloneEvents(events),
	}
	s.timeouts[request.IdempotencyKey] = timeoutRecord{request: request, result: cloneWaitTimeoutResult(result)}
	return result, nil
}

func equalTimeoutWaitRequest(left, right workflowruntime.TimeoutWaitRequest) bool {
	return left.WaitID == right.WaitID &&
		left.ExpectedWaitGeneration == right.ExpectedWaitGeneration &&
		left.ExpectedNodeGeneration == right.ExpectedNodeGeneration &&
		left.IdempotencyKey == right.IdempotencyKey &&
		left.Deadline.Equal(right.Deadline) && left.Now.Equal(right.Now)
}

func cloneWaitTimeoutResult(result workflowruntime.WaitTimeoutResult) workflowruntime.WaitTimeoutResult {
	result.Wait = cloneWait(result.Wait)
	result.Node = cloneNode(result.Node)
	result.Attempt = cloneAttempt(result.Attempt)
	result.Events = cloneEvents(result.Events)
	return result
}

func cloneEvents(events []workflowruntime.Event) []workflowruntime.Event {
	result := make([]workflowruntime.Event, len(events))
	for i := range events {
		result[i] = cloneEvent(events[i])
	}
	return result
}
