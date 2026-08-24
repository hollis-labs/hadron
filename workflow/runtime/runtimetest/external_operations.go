package runtimetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.ExternalOperationStore = (*Store)(nil)

// LoadExternalOperation returns the durable operation bound to one attempt.
func (s *Store) LoadExternalOperation(ctx context.Context, id workflowruntime.AttemptID) (workflowruntime.ExternalOperationSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if err := id.Validate(); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, invalid(err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.externalOperations[id]
	if !ok {
		return workflowruntime.ExternalOperationSnapshot{}, fmt.Errorf("%w: external operation", workflowruntime.ErrNotFound)
	}
	return cloneExternalOperation(snapshot), nil
}

// SuspendExternalOperation atomically persists adapter-owned work, moves the
// aggregate node to waiting, and releases the worker lease.
func (s *Store) SuspendExternalOperation(ctx context.Context, request workflowruntime.SuspendExternalOperationRequest) (workflowruntime.SuspendExternalOperationResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.SuspendExternalOperationResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.SuspendExternalOperationResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.externalOperations[request.Operation.Attempt]; exists {
		return workflowruntime.SuspendExternalOperationResult{}, fmt.Errorf("%w: external operation", workflowruntime.ErrAlreadyExists)
	}
	currentNode, ok := s.nodes[request.Operation.Attempt.Invocation]
	if !ok {
		return workflowruntime.SuspendExternalOperationResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if currentNode.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.SuspendExternalOperationResult{}, casMismatch("external operation node", request.ExpectedNodeGeneration, currentNode.Generation)
	}
	if currentNode.Status != workflowruntime.NodeRunning || currentNode.Wait != nil {
		return workflowruntime.SuspendExternalOperationResult{}, invalid(errors.New("external suspension requires a running node without a generic wait"))
	}
	if err := validateLifecycleClaim(currentNode, &request.Claim, request.At); err != nil {
		return workflowruntime.SuspendExternalOperationResult{}, err
	}
	currentAttempt, attemptErr := s.unfinishedAttemptLocked(currentNode)
	if attemptErr != nil {
		return workflowruntime.SuspendExternalOperationResult{}, attemptErr
	}
	if currentAttempt == nil || currentAttempt.ID != request.Operation.Attempt {
		return workflowruntime.SuspendExternalOperationResult{}, attemptConflict(currentNode.ID, request.Operation.Attempt.Number, "external suspension requires latest unfinished attempt")
	}
	if currentAttempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.SuspendExternalOperationResult{}, casMismatch("external operation attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
	}
	if request.At.Before(currentNode.UpdatedAt) || request.At.Before(currentAttempt.UpdatedAt) {
		return workflowruntime.SuspendExternalOperationResult{}, invalid(errors.New("external suspension time must not regress persisted state"))
	}

	operation := cloneExternalOperation(request.Operation)
	operation.Status = stepkind.ObservationPending
	operation.Generation = 1
	operation.CreatedAt, operation.UpdatedAt = request.At, request.At
	node := cloneNode(currentNode)
	node.Status = workflowruntime.NodeWaiting
	node.Lease = nil
	node.Generation++
	node.UpdatedAt = request.At
	if validationErr := operation.Validate(); validationErr != nil {
		return workflowruntime.SuspendExternalOperationResult{}, invalid(validationErr)
	}
	if validationErr := node.Validate(); validationErr != nil {
		return workflowruntime.SuspendExternalOperationResult{}, invalid(validationErr)
	}
	eventRequests := externalEventRequests(operation, currentNode.Status, node.Status, request.At, workflowruntime.EventExternalOperationSuspended)
	if validationErr := s.validateEventRequestsLocked(eventRequests); validationErr != nil {
		return workflowruntime.SuspendExternalOperationResult{}, validationErr
	}
	events, err := s.appendEventRequestsLocked(eventRequests)
	if err != nil {
		return workflowruntime.SuspendExternalOperationResult{}, err
	}
	s.externalOperations[operation.Attempt] = cloneExternalOperation(operation)
	s.nodes[node.ID] = cloneNode(node)
	return workflowruntime.SuspendExternalOperationResult{
		Operation: cloneExternalOperation(operation), Node: cloneNode(node), Attempt: cloneAttempt(*currentAttempt), Events: cloneEvents(events),
	}, nil
}

// RequestExternalOperationCancel persists cancel intent before an adapter call.
func (s *Store) RequestExternalOperationCancel(ctx context.Context, request workflowruntime.RequestExternalOperationCancelRequest) (workflowruntime.RequestExternalOperationCancelResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RequestExternalOperationCancelResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.RequestExternalOperationCancelResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.externalOperations[request.Attempt]
	if !ok {
		return workflowruntime.RequestExternalOperationCancelResult{}, fmt.Errorf("%w: external operation", workflowruntime.ErrNotFound)
	}
	if current.Generation != request.ExpectedOperationGeneration {
		return workflowruntime.RequestExternalOperationCancelResult{}, casMismatch("external operation", request.ExpectedOperationGeneration, current.Generation)
	}
	if current.Status != stepkind.ObservationPending {
		return workflowruntime.RequestExternalOperationCancelResult{}, &workflowruntime.TransitionConflictError{Entity: "external operation", ID: externalAttemptIdentity(current.Attempt), Status: string(current.Status), Reason: "terminal operation cannot accept cancellation intent"}
	}
	if !current.CancelRequestedAt.IsZero() {
		return workflowruntime.RequestExternalOperationCancelResult{Operation: cloneExternalOperation(current)}, nil
	}
	if request.At.Before(current.UpdatedAt) {
		return workflowruntime.RequestExternalOperationCancelResult{}, invalid(errors.New("external cancel time must not regress persisted state"))
	}
	next := cloneExternalOperation(current)
	next.CancelRequestedAt = request.At
	next.Generation++
	next.UpdatedAt = request.At
	if err := next.Validate(); err != nil {
		return workflowruntime.RequestExternalOperationCancelResult{}, invalid(err)
	}
	invocation, attempt := next.Attempt.Invocation, next.Attempt
	eventRequest := workflowruntime.AppendEventRequest{
		RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt,
		Type: workflowruntime.EventExternalOperationCancelRequested, OccurredAt: request.At,
		Attributes: externalEventAttributes(next, string(current.Status), string(next.Status)),
		Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
	}
	if err := s.validateEventRequestsLocked([]workflowruntime.AppendEventRequest{eventRequest}); err != nil {
		return workflowruntime.RequestExternalOperationCancelResult{}, err
	}
	event, err := s.appendEventLocked(eventRequest)
	if err != nil {
		return workflowruntime.RequestExternalOperationCancelResult{}, err
	}
	s.externalOperations[next.Attempt] = cloneExternalOperation(next)
	eventCopy := cloneEvent(event)
	return workflowruntime.RequestExternalOperationCancelResult{Operation: cloneExternalOperation(next), Event: &eventCopy}, nil
}

// ApplyExternalOperation atomically records pending progress or a terminal
// observation. Terminal application also closes the unfinished attempt and
// moves the waiting aggregate node to its selected outcome.
func (s *Store) ApplyExternalOperation(ctx context.Context, request workflowruntime.ApplyExternalOperationRequest) (workflowruntime.ApplyExternalOperationResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ApplyExternalOperationResult{}, err
	}
	request.At, request.HeartbeatAt = request.At.UTC(), request.HeartbeatAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ApplyExternalOperationResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	currentOperation, ok := s.externalOperations[request.Attempt]
	if !ok {
		return workflowruntime.ApplyExternalOperationResult{}, fmt.Errorf("%w: external operation", workflowruntime.ErrNotFound)
	}
	if currentOperation.Generation != request.ExpectedOperationGeneration {
		return workflowruntime.ApplyExternalOperationResult{}, casMismatch("external operation", request.ExpectedOperationGeneration, currentOperation.Generation)
	}
	currentNode, ok := s.nodes[request.Attempt.Invocation]
	if !ok {
		return workflowruntime.ApplyExternalOperationResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if currentNode.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.ApplyExternalOperationResult{}, casMismatch("external operation node", request.ExpectedNodeGeneration, currentNode.Generation)
	}
	currentAttempt, ok := s.attempts[request.Attempt]
	if !ok {
		return workflowruntime.ApplyExternalOperationResult{}, fmt.Errorf("%w: attempt", workflowruntime.ErrNotFound)
	}
	if currentAttempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.ApplyExternalOperationResult{}, casMismatch("external operation attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
	}
	if currentOperation.Status != stepkind.ObservationPending || currentNode.Status != workflowruntime.NodeWaiting || currentNode.Wait != nil || currentNode.Lease != nil ||
		currentNode.LatestAttempt != request.Attempt.Number || currentAttempt.Status != workflowruntime.NodeRunning || !currentAttempt.FinishedAt.IsZero() {
		return workflowruntime.ApplyExternalOperationResult{}, invalid(errors.New("external observation requires a pending operation and matching waiting unfinished attempt"))
	}
	if request.At.Before(currentOperation.UpdatedAt) || request.At.Before(currentNode.UpdatedAt) || request.At.Before(currentAttempt.UpdatedAt) {
		return workflowruntime.ApplyExternalOperationResult{}, invalid(errors.New("external observation time must not regress persisted state"))
	}
	if !request.ObservedAt.IsZero() && !currentOperation.LastObservedAt.IsZero() && request.ObservedAt.Before(currentOperation.LastObservedAt) {
		return workflowruntime.ApplyExternalOperationResult{}, invalid(errors.New("external observed_at must not regress"))
	}
	if !request.HeartbeatAt.IsZero() && !currentOperation.LastHeartbeatAt.IsZero() && request.HeartbeatAt.Before(currentOperation.LastHeartbeatAt) {
		return workflowruntime.ApplyExternalOperationResult{}, invalid(errors.New("external heartbeat must not regress"))
	}

	nextOperation := cloneExternalOperation(currentOperation)
	nextOperation.Status = request.Status
	nextOperation.Progress = cloneStringMap(request.Progress)
	nextOperation.Outputs = cloneValueSetRef(request.Outputs)
	nextOperation.Failure = cloneFailure(request.Failure)
	if !request.ObservedAt.IsZero() {
		nextOperation.LastObservedAt = request.ObservedAt
	}
	if !request.HeartbeatAt.IsZero() {
		nextOperation.LastHeartbeatAt = request.HeartbeatAt
	}
	nextOperation.Generation++
	nextOperation.UpdatedAt = request.At
	if err := nextOperation.Validate(); err != nil {
		return workflowruntime.ApplyExternalOperationResult{}, invalid(err)
	}

	nextNode, nextAttempt := cloneNode(currentNode), cloneAttempt(currentAttempt)
	eventRequests := []workflowruntime.AppendEventRequest{externalObservationEventRequest(currentOperation, nextOperation, request.At)}
	if request.Status != stepkind.ObservationPending {
		attemptStatus := externalAttemptStatus(request.Status)
		nextAttempt.Status = attemptStatus
		nextAttempt.Outputs = cloneValueSetRef(request.Outputs)
		nextAttempt.Failure = cloneFailure(request.Failure)
		nextAttempt.FinishedAt, nextAttempt.UpdatedAt = request.At, request.At
		nextAttempt.Generation++
		nextNode.Status = request.NextNodeStatus
		nextNode.Lease = nil
		nextNode.Generation++
		nextNode.UpdatedAt = request.At
		if request.NextNodeStatus == workflowruntime.NodeReady {
			nextNode.Outputs = nil
		} else {
			nextNode.Outputs = cloneValueSetRef(request.Outputs)
		}
		if err := nextAttempt.Validate(); err != nil {
			return workflowruntime.ApplyExternalOperationResult{}, invalid(err)
		}
		if err := nextNode.Validate(); err != nil {
			return workflowruntime.ApplyExternalOperationResult{}, invalid(err)
		}
		invocation, attempt := nextNode.ID, nextAttempt.ID
		attributes := attemptAttributes("node_attempt", string(currentNode.Status), string(nextNode.Status), nextAttempt)
		attributes["attempt_status"] = string(nextAttempt.Status)
		if nextAttempt.Failure != nil {
			attributes["failure_code"] = nextAttempt.Failure.Code
		}
		eventRequests = append(eventRequests,
			workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: request.At, Attributes: attributes, Values: cloneValueSetRef(request.Outputs), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
			workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.At, Attributes: attemptAttributes("node", string(currentNode.Status), string(nextNode.Status), nextAttempt), Values: cloneValueSetRef(request.Outputs), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		)
	}
	if err := s.validateEventRequestsLocked(eventRequests); err != nil {
		return workflowruntime.ApplyExternalOperationResult{}, err
	}
	events, err := s.appendEventRequestsLocked(eventRequests)
	if err != nil {
		return workflowruntime.ApplyExternalOperationResult{}, err
	}
	s.externalOperations[nextOperation.Attempt] = cloneExternalOperation(nextOperation)
	if request.Status != stepkind.ObservationPending {
		s.nodes[nextNode.ID] = cloneNode(nextNode)
		s.attempts[nextAttempt.ID] = cloneAttempt(nextAttempt)
	}
	return workflowruntime.ApplyExternalOperationResult{Operation: cloneExternalOperation(nextOperation), Node: cloneNode(nextNode), Attempt: cloneAttempt(nextAttempt), Events: cloneEvents(events)}, nil
}

// RecoverExternalOperations returns pending work in deterministic storage
// order without assigning observation fairness or ownership policy.
func (s *Store) RecoverExternalOperations(ctx context.Context, query workflowruntime.ExternalOperationQuery) ([]workflowruntime.ExternalOperationSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, invalid(errors.New("external recovery limit must not be negative"))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]workflowruntime.ExternalOperationSnapshot, 0)
	for _, operation := range s.externalOperations {
		if operation.Status == stepkind.ObservationPending && (query.RunID == "" || operation.Attempt.Invocation.RunID == query.RunID) {
			result = append(result, cloneExternalOperation(operation))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].UpdatedAt.Before(result[j].UpdatedAt)
		}
		return externalAttemptIdentity(result[i].Attempt) < externalAttemptIdentity(result[j].Attempt)
	})
	return limit(result, query.Limit), nil
}

func externalEventRequests(operation workflowruntime.ExternalOperationSnapshot, from, to workflowruntime.NodeStatus, at time.Time, eventType string) []workflowruntime.AppendEventRequest {
	invocation, attempt := operation.Attempt.Invocation, operation.Attempt
	return []workflowruntime.AppendEventRequest{
		{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt, Type: eventType, OccurredAt: at, Attributes: externalEventAttributes(operation, "", string(operation.Status)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at, Attributes: map[string]string{"entity": "node", "from_status": string(from), "to_status": string(to), "attempt_number": strconv.Itoa(attempt.Number), "executor_kind": operation.Ref.Kind}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
	}
}

func externalObservationEventRequest(current, next workflowruntime.ExternalOperationSnapshot, at time.Time) workflowruntime.AppendEventRequest {
	invocation, attempt := next.Attempt.Invocation, next.Attempt
	return workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventExternalOperationObserved, OccurredAt: at, Attributes: externalEventAttributes(next, string(current.Status), string(next.Status)), Values: cloneValueSetRef(next.Outputs), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
}

func externalEventAttributes(operation workflowruntime.ExternalOperationSnapshot, from, to string) map[string]string {
	attributes := map[string]string{"entity": "external_operation", "operation_kind": operation.Ref.Kind, "operation_id": operation.Ref.ID, "attempt_number": strconv.Itoa(operation.Attempt.Number), "to_status": to}
	if from != "" {
		attributes["from_status"] = from
	}
	if operation.Failure != nil {
		attributes["failure_code"] = operation.Failure.Code
	}
	return attributes
}

func externalAttemptStatus(status stepkind.ObservationState) workflowruntime.NodeStatus {
	switch status {
	case stepkind.ObservationSucceeded:
		return workflowruntime.NodeSucceeded
	case stepkind.ObservationCanceled:
		return workflowruntime.NodeCanceled
	default:
		return workflowruntime.NodeFailed
	}
}

func externalAttemptIdentity(id workflowruntime.AttemptID) string {
	return string(id.Invocation.RunID) + "/" + id.Invocation.NodeID + "[" + id.Invocation.Iteration + "]#" + strconv.Itoa(id.Number)
}

func cloneExternalOperation(snapshot workflowruntime.ExternalOperationSnapshot) workflowruntime.ExternalOperationSnapshot {
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		panic("clone validated external operation: " + err.Error())
	}
	var cloned workflowruntime.ExternalOperationSnapshot
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&cloned); err != nil {
		panic("clone validated external operation: " + err.Error())
	}
	return cloned
}
