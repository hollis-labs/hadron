package inmemory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

// TransitionRun atomically validates and persists a run status change and its
// derived lifecycle event.
func (s *Store) TransitionRun(ctx context.Context, request workflowruntime.RunTransitionRequest) (workflowruntime.RunTransitionResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RunTransitionResult{}, err
	}
	if err := validateRunTransitionRequest(request); err != nil {
		return workflowruntime.RunTransitionResult{}, invalid(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.runs[request.RunID]
	if !ok {
		return workflowruntime.RunTransitionResult{}, fmt.Errorf("%w: run %q", workflowruntime.ErrNotFound, request.RunID)
	}
	if current.Generation != request.ExpectedGeneration {
		return workflowruntime.RunTransitionResult{}, casMismatch("run", request.ExpectedGeneration, current.Generation)
	}
	if intent, exists := s.terminalIntents[request.RunID]; exists && intent.Status == workflowruntime.TerminalIntentPending {
		return workflowruntime.RunTransitionResult{}, invalid(errors.New("pending terminal intent owns run completion"))
	}
	if request.At.Before(current.UpdatedAt) {
		return workflowruntime.RunTransitionResult{}, invalid(errors.New("run transition time must not regress"))
	}
	if current.Status == request.To {
		if request.At.Equal(current.UpdatedAt) && equalValueSetRef(request.Outputs, current.Outputs) {
			return workflowruntime.RunTransitionResult{
				Snapshot: cloneRun(current), Outcome: workflowruntime.TransitionNoOp,
			}, nil
		}
		return workflowruntime.RunTransitionResult{}, &workflowruntime.TransitionConflictError{
			Entity: "run", ID: string(current.ID), Status: string(current.Status),
			Reason: "same-status request is not an exact semantic replay",
		}
	}
	if err := workflowruntime.ValidateRunStatusTransition(current.Status, request.To); err != nil {
		return workflowruntime.RunTransitionResult{}, withTransitionID(err, string(current.ID))
	}
	if request.To != workflowruntime.RunSucceeded && request.Outputs != nil {
		return workflowruntime.RunTransitionResult{}, invalid(errors.New("only a succeeded run may record outputs"))
	}

	next := cloneRun(current)
	next.Status = request.To
	next.Outputs = cloneValueSetRef(request.Outputs)
	next.Generation++
	next.UpdatedAt = request.At
	if err := next.Validate(); err != nil {
		return workflowruntime.RunTransitionResult{}, invalid(err)
	}
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: current.ID, Type: workflowruntime.EventRunStatusChanged, OccurredAt: request.At,
		Attributes: transitionAttributes("run", string(current.Status), string(next.Status)),
		Values:     cloneValueSetRef(request.Outputs),
		Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.RunTransitionResult{}, err
	}
	s.runs[next.ID] = next
	eventCopy := cloneEvent(event)
	return workflowruntime.RunTransitionResult{
		Snapshot: cloneRun(next), Outcome: workflowruntime.TransitionApplied, Event: &eventCopy,
	}, nil
}

// TransitionNode atomically validates and persists a non-attempt lifecycle
// edge and its derived event.
func (s *Store) TransitionNode(ctx context.Context, request workflowruntime.NodeTransitionRequest) (workflowruntime.NodeTransitionResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.NodeTransitionResult{}, err
	}
	if err := validateNodeTransitionRequest(request); err != nil {
		return workflowruntime.NodeTransitionResult{}, invalid(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.nodes[request.InvocationID]
	if !ok {
		return workflowruntime.NodeTransitionResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if !s.controlAdmissionAllowedLocked(current.ID) {
		return workflowruntime.NodeTransitionResult{}, invalid(errors.New("pending terminal intent fences non-finalizer transition"))
	}
	if current.Generation != request.ExpectedGeneration {
		return workflowruntime.NodeTransitionResult{}, casMismatch("node invocation", request.ExpectedGeneration, current.Generation)
	}
	if err := validateLifecycleClaim(current, request.Claim, request.At); err != nil {
		return workflowruntime.NodeTransitionResult{}, err
	}
	if request.At.Before(current.UpdatedAt) {
		return workflowruntime.NodeTransitionResult{}, invalid(errors.New("node transition time must not regress"))
	}
	unfinished, unfinishedErr := s.unfinishedAttemptLocked(current)
	if unfinishedErr != nil {
		return workflowruntime.NodeTransitionResult{}, unfinishedErr
	}
	if current.Status == request.To {
		if request.At.Equal(current.UpdatedAt) && equalBlockedReason(request.Blocked, current.Blocked) &&
			s.transitionExplanationMatchesLocked(current, request.Explanation, request.At) {
			return workflowruntime.NodeTransitionResult{
				Snapshot: cloneNode(current), Outcome: workflowruntime.TransitionNoOp,
			}, nil
		}
		if current.Status != workflowruntime.NodeBlocked || request.Blocked == nil ||
			equalBlockedReason(request.Blocked, current.Blocked) || !request.At.After(current.UpdatedAt) {
			return workflowruntime.NodeTransitionResult{}, &workflowruntime.TransitionConflictError{
				Entity: "node", ID: nodeIdentity(current.ID), Status: string(current.Status),
				Reason: "same-status request is not an exact semantic replay or a later blocked-diagnostic refresh",
			}
		}
	}

	if request.To == workflowruntime.NodeRunning {
		if current.Status != workflowruntime.NodeReady || unfinished == nil {
			return workflowruntime.NodeTransitionResult{}, transitionError(current, request.To, "running requires StartNodeAttempt or an unfinished resumed attempt")
		}
		if current.Lease == nil || request.Claim == nil {
			return workflowruntime.NodeTransitionResult{}, workflowruntime.ErrClaimMismatch
		}
	} else {
		if err := workflowruntime.ValidateNodeStatusTransition(current.Status, request.To); err != nil {
			return workflowruntime.NodeTransitionResult{}, withTransitionID(err, nodeIdentity(current.ID))
		}
	}
	if request.To == workflowruntime.NodeWaiting && unfinished == nil {
		return workflowruntime.NodeTransitionResult{}, attemptConflict(current.ID, current.LatestAttempt, "entering waiting requires an unfinished attempt")
	}
	if request.To.Terminal() && unfinished != nil {
		return workflowruntime.NodeTransitionResult{}, attemptConflict(current.ID, unfinished.ID.Number, "terminal node transition must use FinishNodeAttempt")
	}

	next := cloneNode(current)
	next.Status = request.To
	next.Generation++
	next.UpdatedAt = request.At
	if request.To == workflowruntime.NodeBlocked {
		next.Blocked = cloneBlocked(request.Blocked)
	} else {
		next.Blocked = nil
	}
	if request.To == workflowruntime.NodeWaiting || request.To.Terminal() {
		next.Lease = nil
	}
	if err := next.Validate(); err != nil {
		return workflowruntime.NodeTransitionResult{}, invalid(err)
	}
	attributes := transitionAttributes("node", string(current.Status), string(next.Status))
	var eventAttempt *workflowruntime.AttemptID
	if unfinished != nil {
		attributes = attemptAttributes("node", string(current.Status), string(next.Status), *unfinished)
		attemptID := unfinished.ID
		eventAttempt = &attemptID
	}
	if next.Blocked != nil {
		attributes["blocked_reason"] = next.Blocked.Code
	} else if current.Blocked != nil {
		attributes["blocked_reason"] = current.Blocked.Code
	}
	if request.Explanation != nil {
		explanation, encodeErr := encodeTransitionExplanation(request.Explanation)
		if encodeErr != nil {
			return workflowruntime.NodeTransitionResult{}, invalid(encodeErr)
		}
		attributes["explanation"] = explanation
		attributes["explanation_code"] = request.Explanation.Code
	}
	invocation := next.ID
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: next.ID.RunID, Invocation: &invocation, Attempt: eventAttempt,
		Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.At, Attributes: attributes,
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.NodeTransitionResult{}, err
	}
	if next.Lease == nil {
		s.releaseSchedulerResourcesLocked(next.ID)
	}
	s.nodes[next.ID] = next
	eventCopy := cloneEvent(event)
	return workflowruntime.NodeTransitionResult{
		Snapshot: cloneNode(next), Outcome: workflowruntime.TransitionApplied, Event: &eventCopy,
	}, nil
}

// StartNodeAttempt atomically starts exactly LatestAttempt+1.
func (s *Store) StartNodeAttempt(ctx context.Context, request workflowruntime.StartNodeAttemptRequest) (workflowruntime.StartNodeAttemptResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.StartNodeAttemptResult{}, err
	}
	if err := validateStartAttemptRequest(request); err != nil {
		return workflowruntime.StartNodeAttemptResult{}, invalid(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.nodes[request.InvocationID]
	if !ok {
		return workflowruntime.StartNodeAttemptResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if !s.controlAdmissionAllowedLocked(current.ID) {
		return workflowruntime.StartNodeAttemptResult{}, invalid(errors.New("pending terminal intent fences non-finalizer attempt start"))
	}
	if current.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.StartNodeAttemptResult{}, casMismatch("node invocation", request.ExpectedNodeGeneration, current.Generation)
	}
	if err := validateLifecycleClaim(current, &request.Claim, request.At); err != nil {
		return workflowruntime.StartNodeAttemptResult{}, err
	}
	if request.At.Before(current.UpdatedAt) {
		return workflowruntime.StartNodeAttemptResult{}, invalid(errors.New("attempt start time must not regress node updated_at"))
	}
	if current.Status != workflowruntime.NodeReady {
		return workflowruntime.StartNodeAttemptResult{}, transitionError(current, workflowruntime.NodeRunning, "new attempt requires ready node")
	}
	unfinished, unfinishedErr := s.unfinishedAttemptLocked(current)
	if unfinishedErr != nil {
		return workflowruntime.StartNodeAttemptResult{}, unfinishedErr
	}
	if unfinished != nil {
		return workflowruntime.StartNodeAttemptResult{}, attemptConflict(current.ID, unfinished.ID.Number, "unfinished attempt must be resumed, not replaced")
	}
	if err := s.validateAttemptHistoryLocked(current); err != nil {
		return workflowruntime.StartNodeAttemptResult{}, err
	}
	if err := s.enforceFanOutStartLocked(current); err != nil {
		return workflowruntime.StartNodeAttemptResult{}, err
	}

	attemptNumber := current.LatestAttempt + 1
	attemptID := workflowruntime.AttemptID{Invocation: current.ID, Number: attemptNumber}
	if _, exists := s.attempts[attemptID]; exists {
		return workflowruntime.StartNodeAttemptResult{}, attemptConflict(current.ID, attemptNumber, "attempt already exists")
	}
	nextNode := cloneNode(current)
	nextNode.Status = workflowruntime.NodeRunning
	nextNode.Blocked = nil
	nextNode.Inputs = cloneValueSetRef(request.Inputs)
	nextNode.LatestAttempt = attemptNumber
	nextNode.Generation++
	nextNode.UpdatedAt = request.At
	attempt := workflowruntime.AttemptSnapshot{
		ID: attemptID, Status: workflowruntime.NodeRunning, Executor: cloneExecutor(request.Executor),
		Inputs: cloneValueSetRef(request.Inputs), StartedAt: request.At,
		Generation: 1, CreatedAt: request.At, UpdatedAt: request.At,
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.StartNodeAttemptResult{}, invalid(err)
	}
	if err := attempt.Validate(); err != nil {
		return workflowruntime.StartNodeAttemptResult{}, invalid(err)
	}
	invocation := nextNode.ID
	eventAttempt := attempt.ID
	attributes := attemptAttributes("node_attempt", string(current.Status), string(nextNode.Status), attempt)
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: nextNode.ID.RunID, Invocation: &invocation, Attempt: &eventAttempt,
		Type: workflowruntime.EventNodeAttemptStarted, OccurredAt: request.At,
		Attributes: attributes, Values: cloneValueSetRef(request.Inputs),
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.StartNodeAttemptResult{}, err
	}
	s.nodes[nextNode.ID] = nextNode
	s.attempts[attempt.ID] = attempt
	return workflowruntime.StartNodeAttemptResult{
		Node: cloneNode(nextNode), Attempt: cloneAttempt(attempt), Event: cloneEvent(event),
	}, nil
}

// FinishNodeAttempt atomically closes the latest unfinished running attempt,
// updates the aggregate node, clears its lease, and appends the derived event.
func (s *Store) FinishNodeAttempt(ctx context.Context, request workflowruntime.FinishNodeAttemptRequest) (workflowruntime.FinishNodeAttemptResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	if err := validateFinishAttemptRequest(request); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, invalid(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finishNodeAttemptLocked(request)
}

func (s *Store) finishNodeAttemptLocked(request workflowruntime.FinishNodeAttemptRequest) (workflowruntime.FinishNodeAttemptResult, error) {
	currentNode, ok := s.nodes[request.InvocationID]
	if !ok {
		return workflowruntime.FinishNodeAttemptResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if currentNode.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.FinishNodeAttemptResult{}, casMismatch("node invocation", request.ExpectedNodeGeneration, currentNode.Generation)
	}
	if !s.runAllowsExecutionLocked(currentNode.ID) {
		return workflowruntime.FinishNodeAttemptResult{}, invalid(errors.New("terminal run fences attempt completion"))
	}
	if !s.controlAdmissionAllowedLocked(currentNode.ID) {
		return workflowruntime.FinishNodeAttemptResult{}, invalid(errors.New("pending terminal intent fences non-finalizer attempt completion"))
	}
	if currentNode.Status != workflowruntime.NodeRunning {
		return workflowruntime.FinishNodeAttemptResult{}, transitionError(currentNode, request.NextNodeStatus, "finishing requires running node")
	}
	if err := validateLifecycleClaim(currentNode, &request.Claim, request.At); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	if currentNode.LatestAttempt != request.AttemptNumber {
		return workflowruntime.FinishNodeAttemptResult{}, attemptConflict(currentNode.ID, request.AttemptNumber, "only LatestAttempt may be finished")
	}
	attemptID := workflowruntime.AttemptID{Invocation: currentNode.ID, Number: request.AttemptNumber}
	currentAttempt, ok := s.attempts[attemptID]
	if !ok {
		return workflowruntime.FinishNodeAttemptResult{}, attemptConflict(currentNode.ID, request.AttemptNumber, "latest attempt is missing")
	}
	if currentAttempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.FinishNodeAttemptResult{}, casMismatch("attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
	}
	if currentAttempt.Status != workflowruntime.NodeRunning || !currentAttempt.FinishedAt.IsZero() {
		return workflowruntime.FinishNodeAttemptResult{}, attemptConflict(currentNode.ID, request.AttemptNumber, "attempt is already finished")
	}
	if request.At.Before(currentNode.UpdatedAt) || request.At.Before(currentAttempt.UpdatedAt) {
		return workflowruntime.FinishNodeAttemptResult{}, invalid(errors.New("attempt finish time must not regress persisted state"))
	}

	nextAttempt := cloneAttempt(currentAttempt)
	nextAttempt.Status = request.AttemptStatus
	nextAttempt.Outputs = cloneValueSetRef(request.Outputs)
	nextAttempt.Failure = cloneFailure(request.Failure)
	nextAttempt.FinishedAt = request.At
	nextAttempt.UpdatedAt = request.At
	nextAttempt.Generation++
	nextNode := cloneNode(currentNode)
	nextNode.Status = request.NextNodeStatus
	nextNode.Blocked = nil
	nextNode.Lease = nil
	nextNode.Generation++
	nextNode.UpdatedAt = request.At
	if request.NextNodeStatus == workflowruntime.NodeReady {
		nextNode.Outputs = nil
		nextNode.Origin = ""
	} else {
		nextNode.Outputs = cloneValueSetRef(request.Outputs)
		nextNode.Origin = workflowruntime.OriginExecuted
	}
	if err := nextAttempt.Validate(); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, invalid(err)
	}
	invocation := nextNode.ID
	eventAttempt := nextAttempt.ID
	attributes := attemptAttributes("node_attempt", string(currentNode.Status), string(nextNode.Status), nextAttempt)
	attributes["attempt_status"] = string(nextAttempt.Status)
	if nextAttempt.Failure != nil {
		attributes["failure_code"] = nextAttempt.Failure.Code
	}
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: nextNode.ID.RunID, Invocation: &invocation, Attempt: &eventAttempt,
		Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: request.At,
		Attributes: attributes, Values: cloneValueSetRef(request.Outputs),
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	s.releaseSchedulerResourcesLocked(nextNode.ID)
	s.nodes[nextNode.ID] = nextNode
	s.attempts[nextAttempt.ID] = nextAttempt
	return workflowruntime.FinishNodeAttemptResult{
		Node: cloneNode(nextNode), Attempt: cloneAttempt(nextAttempt), Event: cloneEvent(event),
	}, nil
}

func validateRunTransitionRequest(request workflowruntime.RunTransitionRequest) error {
	if request.RunID == "" || request.At.IsZero() {
		return errors.New("run transition requires run id and timestamp")
	}
	if !request.To.Valid() {
		return fmt.Errorf("unsupported run status %q", request.To)
	}
	if request.Outputs != nil {
		return request.Outputs.Validate()
	}
	return nil
}

func validateNodeTransitionRequest(request workflowruntime.NodeTransitionRequest) error {
	if err := request.InvocationID.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return errors.New("node transition timestamp is required")
	}
	if !request.To.Valid() {
		return fmt.Errorf("unsupported node status %q", request.To)
	}
	if request.To == workflowruntime.NodeBlocked {
		if request.Blocked == nil {
			return errors.New("blocked transition requires blocked reason")
		}
		if err := request.Blocked.Validate(); err != nil {
			return err
		}
	} else if request.Blocked != nil {
		return errors.New("blocked reason requires blocked target status")
	}
	if request.Explanation != nil {
		if request.To != workflowruntime.NodeSkipped {
			return errors.New("transition explanation requires skipped target status")
		}
		if err := request.Explanation.Validate(); err != nil {
			return err
		}
		for i, dependency := range request.Explanation.Dependencies {
			if dependency.RunID != request.InvocationID.RunID {
				return fmt.Errorf("explanation dependency[%d] must belong to invocation run", i)
			}
		}
	}
	if request.Claim != nil {
		return request.Claim.Validate()
	}
	return nil
}

func validateStartAttemptRequest(request workflowruntime.StartNodeAttemptRequest) error {
	if err := request.InvocationID.Validate(); err != nil {
		return err
	}
	if err := request.Claim.Validate(); err != nil {
		return err
	}
	if err := request.Executor.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return errors.New("attempt start timestamp is required")
	}
	if request.Inputs != nil {
		return request.Inputs.Validate()
	}
	return nil
}

func validateFinishAttemptRequest(request workflowruntime.FinishNodeAttemptRequest) error {
	if err := request.InvocationID.Validate(); err != nil {
		return err
	}
	if request.AttemptNumber < 1 {
		return errors.New("attempt number must be positive")
	}
	if err := request.Claim.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return errors.New("attempt finish timestamp is required")
	}
	if !attemptOutcome(request.AttemptStatus) {
		return errors.New("attempt status must be succeeded, failed, canceled, timed_out, or crashed")
	}
	if request.NextNodeStatus != request.AttemptStatus {
		if request.NextNodeStatus != workflowruntime.NodeReady ||
			(request.AttemptStatus != workflowruntime.NodeFailed &&
				request.AttemptStatus != workflowruntime.NodeTimedOut &&
				request.AttemptStatus != workflowruntime.NodeCrashed) {
			return errors.New("next node status must equal attempt outcome or be ready after failed, timed_out, or crashed")
		}
	}
	if request.Outputs != nil {
		if err := request.Outputs.Validate(); err != nil {
			return err
		}
	}
	if request.AttemptStatus == workflowruntime.NodeSucceeded {
		if request.Failure != nil {
			return errors.New("succeeded attempt must not contain failure")
		}
	} else {
		if request.Failure == nil {
			return errors.New("unsuccessful attempt requires failure")
		}
		if err := request.Failure.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func attemptOutcome(status workflowruntime.NodeStatus) bool {
	switch status {
	case workflowruntime.NodeSucceeded, workflowruntime.NodeFailed, workflowruntime.NodeCanceled,
		workflowruntime.NodeTimedOut, workflowruntime.NodeCrashed:
		return true
	default:
		return false
	}
}

func validateLifecycleClaim(node workflowruntime.NodeInvocationSnapshot, proof *workflowruntime.ClaimProof, at time.Time) error {
	if node.Lease == nil {
		if proof != nil {
			return workflowruntime.ErrClaimMismatch
		}
		return nil
	}
	if proof == nil || node.Lease.Owner != proof.Owner || node.Lease.Token != proof.Token ||
		node.Lease.Generation != proof.Generation {
		return workflowruntime.ErrClaimMismatch
	}
	if !node.Lease.ExpiresAt.After(at) {
		return workflowruntime.ErrLeaseExpired
	}
	return nil
}

func (s *Store) unfinishedAttemptLocked(node workflowruntime.NodeInvocationSnapshot) (*workflowruntime.AttemptSnapshot, error) {
	var unfinished *workflowruntime.AttemptSnapshot
	for id, attempt := range s.attempts {
		if id.Invocation != node.ID || attempt.Status != workflowruntime.NodeRunning {
			continue
		}
		if unfinished != nil {
			return nil, attemptConflict(node.ID, id.Number, "multiple unfinished attempts exist")
		}
		copyAttempt := cloneAttempt(attempt)
		unfinished = &copyAttempt
	}
	if unfinished != nil && unfinished.ID.Number != node.LatestAttempt {
		return nil, attemptConflict(node.ID, unfinished.ID.Number, "unfinished attempt is not LatestAttempt")
	}
	if (node.Status == workflowruntime.NodeRunning || node.Status == workflowruntime.NodeWaiting) && unfinished == nil {
		return nil, attemptConflict(node.ID, node.LatestAttempt, "node status requires an unfinished attempt")
	}
	return unfinished, nil
}

func (s *Store) validateAttemptHistoryLocked(node workflowruntime.NodeInvocationSnapshot) error {
	maximum := 0
	for id := range s.attempts {
		if id.Invocation == node.ID && id.Number > maximum {
			maximum = id.Number
		}
	}
	if maximum != node.LatestAttempt {
		return attemptConflict(node.ID, node.LatestAttempt, "LatestAttempt does not match durable history")
	}
	return nil
}

func transitionAttributes(entity, from, to string) map[string]string {
	return map[string]string{"entity": entity, "from_status": from, "to_status": to}
}

func encodeTransitionExplanation(reason *workflowruntime.BlockedReason) (string, error) {
	canonical := cloneBlocked(reason)
	sort.Slice(canonical.Dependencies, func(i, j int) bool {
		left, right := canonical.Dependencies[i], canonical.Dependencies[j]
		if left.RunID != right.RunID {
			return left.RunID < right.RunID
		}
		if left.NodeID != right.NodeID {
			return left.NodeID < right.NodeID
		}
		return left.Iteration < right.Iteration
	})
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("encode transition explanation: %w", err)
	}
	return string(encoded), nil
}

func (s *Store) transitionExplanationMatchesLocked(
	node workflowruntime.NodeInvocationSnapshot,
	explanation *workflowruntime.BlockedReason,
	at time.Time,
) bool {
	if node.Status != workflowruntime.NodeSkipped {
		return explanation == nil
	}
	var expected string
	if explanation != nil {
		encoded, err := encodeTransitionExplanation(explanation)
		if err != nil {
			return false
		}
		expected = encoded
	}
	events := s.events[node.ID.RunID]
	for i := len(events) - 1; i >= 0; i-- {
		event := events[i]
		if event.Invocation == nil || *event.Invocation != node.ID ||
			event.Type != workflowruntime.EventNodeStatusChanged ||
			event.Attributes["to_status"] != string(workflowruntime.NodeSkipped) ||
			!event.OccurredAt.Equal(at) {
			continue
		}
		return event.Attributes["explanation"] == expected
	}
	return expected == ""
}

func attemptAttributes(entity, from, to string, attempt workflowruntime.AttemptSnapshot) map[string]string {
	attributes := transitionAttributes(entity, from, to)
	attributes["attempt_number"] = strconv.Itoa(attempt.ID.Number)
	attributes["executor_kind"] = attempt.Executor.Kind
	attributes["executor_version"] = attempt.Executor.Version
	if attempt.Executor.Target != "" {
		attributes["executor_target"] = attempt.Executor.Target
	}
	return attributes
}

func transitionError(node workflowruntime.NodeInvocationSnapshot, to workflowruntime.NodeStatus, reason string) error {
	return &workflowruntime.TransitionError{
		Entity: "node", ID: nodeIdentity(node.ID), From: string(node.Status), To: string(to), Reason: reason,
	}
}

func withTransitionID(err error, id string) error {
	var transition *workflowruntime.TransitionError
	if errors.As(err, &transition) {
		copyTransition := *transition
		copyTransition.ID = id
		return &copyTransition
	}
	return err
}

func attemptConflict(id workflowruntime.NodeInvocationID, attempt int, reason string) error {
	return &workflowruntime.AttemptConflictError{Invocation: id, Attempt: attempt, Reason: reason}
}

func nodeIdentity(id workflowruntime.NodeInvocationID) string {
	if id.Iteration == "" {
		return string(id.RunID) + "/" + id.NodeID
	}
	return string(id.RunID) + "/" + id.NodeID + "[" + id.Iteration + "]"
}
