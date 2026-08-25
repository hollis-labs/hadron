package persistence

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

// TransitionNode implements runtime.StateStore with atomic node, lease, and
// lifecycle-event persistence.
func (s *WorkflowStateStore) TransitionNode(ctx context.Context, request workflowruntime.NodeTransitionRequest) (workflowruntime.NodeTransitionResult, error) {
	if err := validateWorkflowNodeTransition(request); err != nil {
		return workflowruntime.NodeTransitionResult{}, workflowInvalid(err)
	}
	var result workflowruntime.NodeTransitionResult
	writeErr := s.write(ctx, "transition workflow node", func(query workflowSQL) error {
		current, loadErr := loadWorkflowNode(ctx, query, request.InvocationID)
		if loadErr != nil {
			return loadErr
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("node invocation", request.ExpectedGeneration, current.Generation)
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, current.ID)
		if err != nil {
			return err
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences non-finalizer transition"))
		}
		at := request.At.UTC()
		if err := validateWorkflowLifecycleClaim(current, request.Claim, at); err != nil {
			return err
		}
		if at.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("node transition time must not regress"))
		}
		unfinished, attemptErr := unfinishedWorkflowAttempt(ctx, query, current)
		if attemptErr != nil {
			return attemptErr
		}
		if current.Status == request.To {
			explanationMatches, explanationErr := workflowTransitionExplanationMatches(
				ctx, query, current, request.Explanation, at,
			)
			if explanationErr != nil {
				return explanationErr
			}
			if at.Equal(current.UpdatedAt) && equalWorkflowBlocked(request.Blocked, current.Blocked) && explanationMatches {
				result = workflowruntime.NodeTransitionResult{
					Snapshot: current, Outcome: workflowruntime.TransitionNoOp,
				}
				return nil
			}
			if current.Status != workflowruntime.NodeBlocked || request.Blocked == nil ||
				equalWorkflowBlocked(request.Blocked, current.Blocked) || !at.After(current.UpdatedAt) {
				return &workflowruntime.TransitionConflictError{
					Entity: "node", ID: workflowNodeIdentity(current.ID), Status: string(current.Status),
					Reason: "same-status request is not an exact semantic replay or a later blocked-diagnostic refresh",
				}
			}
		}

		if request.To == workflowruntime.NodeRunning {
			if current.Status != workflowruntime.NodeReady || unfinished == nil {
				return workflowNodeTransitionError(current, request.To, "running requires StartNodeAttempt or an unfinished resumed attempt")
			}
			if current.Lease == nil || request.Claim == nil {
				return workflowruntime.ErrClaimMismatch
			}
		} else if err := workflowruntime.ValidateNodeStatusTransition(current.Status, request.To); err != nil {
			return withWorkflowTransitionID(err, workflowNodeIdentity(current.ID))
		}
		if request.To == workflowruntime.NodeWaiting && unfinished == nil {
			return workflowAttemptConflict(current.ID, current.LatestAttempt, "entering waiting requires an unfinished attempt")
		}
		if request.To.Terminal() && unfinished != nil {
			return workflowAttemptConflict(current.ID, unfinished.ID.Number, "terminal node transition must use FinishNodeAttempt")
		}

		next := cloneWorkflowNode(current)
		next.Status = request.To
		next.Generation++
		next.UpdatedAt = at
		if request.To == workflowruntime.NodeBlocked {
			next.Blocked = cloneWorkflowBlocked(request.Blocked)
		} else {
			next.Blocked = nil
		}
		if request.To == workflowruntime.NodeWaiting || request.To.Terminal() {
			next.Lease = nil
		}
		if err := next.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowNodeCAS(ctx, query, next, current.Generation); err != nil {
			return err
		}

		attributes := workflowTransitionAttributes("node", string(current.Status), string(next.Status))
		var eventAttempt *workflowruntime.AttemptID
		if unfinished != nil {
			attributes = workflowAttemptAttributes("node", string(current.Status), string(next.Status), *unfinished)
			attemptID := unfinished.ID
			eventAttempt = &attemptID
		}
		if next.Blocked != nil {
			attributes["blocked_reason"] = next.Blocked.Code
		} else if current.Blocked != nil {
			attributes["blocked_reason"] = current.Blocked.Code
		}
		if request.Explanation != nil {
			explanation, encodeErr := encodeWorkflowTransitionExplanation(request.Explanation)
			if encodeErr != nil {
				return workflowInvalid(encodeErr)
			}
			attributes["explanation"] = explanation
			attributes["explanation_code"] = request.Explanation.Code
		}
		invocation := next.ID
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: next.ID.RunID, Invocation: &invocation, Attempt: eventAttempt,
			Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at,
			Attributes: attributes,
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		eventCopy := event
		result = workflowruntime.NodeTransitionResult{
			Snapshot: next, Outcome: workflowruntime.TransitionApplied, Event: &eventCopy,
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.NodeTransitionResult{}, writeErr
	}
	return result, nil
}

// StartNodeAttempt implements runtime.StateStore with one transaction for the
// node transition, attempt creation, and lifecycle event.
func (s *WorkflowStateStore) StartNodeAttempt(ctx context.Context, request workflowruntime.StartNodeAttemptRequest) (workflowruntime.StartNodeAttemptResult, error) {
	if err := validateWorkflowStartAttempt(request); err != nil {
		return workflowruntime.StartNodeAttemptResult{}, workflowInvalid(err)
	}
	var result workflowruntime.StartNodeAttemptResult
	writeErr := s.write(ctx, "start workflow node attempt", func(query workflowSQL) error {
		current, loadErr := loadWorkflowNode(ctx, query, request.InvocationID)
		if loadErr != nil {
			return loadErr
		}
		if current.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("node invocation", request.ExpectedNodeGeneration, current.Generation)
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, current.ID)
		if err != nil {
			return err
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences non-finalizer attempt start"))
		}
		at := request.At.UTC()
		if err := validateWorkflowLifecycleClaim(current, &request.Claim, at); err != nil {
			return err
		}
		if at.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("attempt start time must not regress node updated_at"))
		}
		if current.Status != workflowruntime.NodeReady {
			return workflowNodeTransitionError(current, workflowruntime.NodeRunning, "new attempt requires ready node")
		}
		eligible, eligibilityErr := workflowFanOutClaimEligible(ctx, query, current, at)
		if eligibilityErr != nil {
			return eligibilityErr
		}
		if !eligible {
			return workflowInvalid(workflowruntime.ErrFanOutLimit)
		}
		unfinished, attemptErr := unfinishedWorkflowAttempt(ctx, query, current)
		if attemptErr != nil {
			return attemptErr
		}
		if unfinished != nil {
			return workflowAttemptConflict(current.ID, unfinished.ID.Number, "unfinished attempt must be resumed, not replaced")
		}
		if err := validateWorkflowAttemptHistory(ctx, query, current); err != nil {
			return err
		}

		attemptNumber := current.LatestAttempt + 1
		attemptID := workflowruntime.AttemptID{Invocation: current.ID, Number: attemptNumber}
		nextNode := cloneWorkflowNode(current)
		nextNode.Status = workflowruntime.NodeRunning
		nextNode.Blocked = nil
		nextNode.Inputs = cloneWorkflowValueRef(request.Inputs)
		nextNode.LatestAttempt = attemptNumber
		nextNode.Generation++
		nextNode.UpdatedAt = at
		attempt := workflowruntime.AttemptSnapshot{
			ID: attemptID, Status: workflowruntime.NodeRunning,
			Executor: cloneWorkflowExecutor(request.Executor),
			Inputs:   cloneWorkflowValueRef(request.Inputs), StartedAt: at,
			Generation: 1, CreatedAt: at, UpdatedAt: at,
		}
		if err := nextNode.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := attempt.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, current.Generation); err != nil {
			return err
		}
		if err := insertWorkflowAttempt(ctx, query, attempt); err != nil {
			return err
		}
		invocation := nextNode.ID
		eventAttempt := attempt.ID
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: nextNode.ID.RunID, Invocation: &invocation, Attempt: &eventAttempt,
			Type: workflowruntime.EventNodeAttemptStarted, OccurredAt: at,
			Attributes: workflowAttemptAttributes("node_attempt", string(current.Status), string(nextNode.Status), attempt),
			Values:     cloneWorkflowValueRef(request.Inputs),
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		result = workflowruntime.StartNodeAttemptResult{Node: nextNode, Attempt: attempt, Event: event}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.StartNodeAttemptResult{}, writeErr
	}
	return result, nil
}

// FinishNodeAttempt implements runtime.StateStore with one transaction for the
// running attempt, aggregate node, lease release, and lifecycle event.
func (s *WorkflowStateStore) FinishNodeAttempt(ctx context.Context, request workflowruntime.FinishNodeAttemptRequest) (workflowruntime.FinishNodeAttemptResult, error) {
	if err := validateWorkflowFinishAttempt(request); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, workflowInvalid(err)
	}
	var result workflowruntime.FinishNodeAttemptResult
	writeErr := s.write(ctx, "finish workflow node attempt", func(query workflowSQL) error {
		currentNode, nodeLoadErr := loadWorkflowNode(ctx, query, request.InvocationID)
		if nodeLoadErr != nil {
			return nodeLoadErr
		}
		if currentNode.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("node invocation", request.ExpectedNodeGeneration, currentNode.Generation)
		}
		run, runLoadErr := loadWorkflowRun(ctx, query, currentNode.ID.RunID)
		if runLoadErr != nil {
			return runLoadErr
		}
		allowedRun, admissionErr := workflowRunAllowsCompensationExecution(ctx, query, run, currentNode)
		if admissionErr != nil {
			return admissionErr
		}
		if !allowedRun {
			return workflowInvalid(errors.New("terminal run fences attempt completion"))
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, currentNode.ID)
		if err != nil {
			return err
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences non-finalizer attempt completion"))
		}
		if currentNode.Status != workflowruntime.NodeRunning {
			return workflowNodeTransitionError(currentNode, request.NextNodeStatus, "finishing requires running node")
		}
		at := request.At.UTC()
		if err := validateWorkflowLifecycleClaim(currentNode, &request.Claim, at); err != nil {
			return err
		}
		if currentNode.LatestAttempt != request.AttemptNumber {
			return workflowAttemptConflict(currentNode.ID, request.AttemptNumber, "only LatestAttempt may be finished")
		}
		attemptID := workflowruntime.AttemptID{Invocation: currentNode.ID, Number: request.AttemptNumber}
		currentAttempt, attemptLoadErr := loadWorkflowAttempt(ctx, query, attemptID)
		if attemptLoadErr != nil {
			if errors.Is(attemptLoadErr, workflowruntime.ErrNotFound) {
				return workflowAttemptConflict(currentNode.ID, request.AttemptNumber, "latest attempt is missing")
			}
			return attemptLoadErr
		}
		if currentAttempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
		}
		if currentAttempt.Status != workflowruntime.NodeRunning || !currentAttempt.FinishedAt.IsZero() {
			return workflowAttemptConflict(currentNode.ID, request.AttemptNumber, "attempt is already finished")
		}
		if at.Before(currentNode.UpdatedAt) || at.Before(currentAttempt.UpdatedAt) {
			return workflowInvalid(errors.New("attempt finish time must not regress persisted state"))
		}

		nextAttempt := cloneWorkflowAttempt(currentAttempt)
		nextAttempt.Status = request.AttemptStatus
		nextAttempt.Outputs = cloneWorkflowValueRef(request.Outputs)
		nextAttempt.Failure = cloneWorkflowFailure(request.Failure)
		nextAttempt.FinishedAt = at
		nextAttempt.UpdatedAt = at
		nextAttempt.Generation++
		nextNode := cloneWorkflowNode(currentNode)
		nextNode.Status = request.NextNodeStatus
		nextNode.Blocked = nil
		nextNode.Lease = nil
		nextNode.Generation++
		nextNode.UpdatedAt = at
		if request.NextNodeStatus == workflowruntime.NodeReady {
			nextNode.Outputs = nil
			nextNode.Origin = ""
		} else {
			nextNode.Outputs = cloneWorkflowValueRef(request.Outputs)
			nextNode.Origin = workflowruntime.OriginExecuted
		}
		if err := nextAttempt.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := nextNode.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowAttemptCAS(ctx, query, nextAttempt, currentAttempt.Generation); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, currentNode.Generation); err != nil {
			return err
		}
		invocation := nextNode.ID
		eventAttempt := nextAttempt.ID
		attributes := workflowAttemptAttributes("node_attempt", string(currentNode.Status), string(nextNode.Status), nextAttempt)
		attributes["attempt_status"] = string(nextAttempt.Status)
		if nextAttempt.Failure != nil {
			attributes["failure_code"] = nextAttempt.Failure.Code
		}
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: nextNode.ID.RunID, Invocation: &invocation, Attempt: &eventAttempt,
			Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: at,
			Attributes: attributes, Values: cloneWorkflowValueRef(request.Outputs),
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		result = workflowruntime.FinishNodeAttemptResult{Node: nextNode, Attempt: nextAttempt, Event: event}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.FinishNodeAttemptResult{}, writeErr
	}
	return result, nil
}

// LoadAttempt implements runtime.StateStore.
func (s *WorkflowStateStore) LoadAttempt(ctx context.Context, id workflowruntime.AttemptID) (workflowruntime.AttemptSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	return loadWorkflowAttempt(ctx, s.db, id)
}

// ListAttempts implements runtime.StateStore in attempt-number order.
func (s *WorkflowStateStore) ListAttempts(ctx context.Context, id workflowruntime.NodeInvocationID) ([]workflowruntime.AttemptSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if err := id.Validate(); err != nil {
		return nil, workflowInvalid(err)
	}
	rows, err := s.db.QueryContext(ctx, workflowAttemptSelect+`
WHERE run_id = ? AND node_id = ? AND iteration = ?
ORDER BY attempt_number ASC`, id.RunID, id.NodeID, id.Iteration)
	if err != nil {
		return nil, fmt.Errorf("list workflow attempts: %w", err)
	}
	defer closeRows(rows)
	result := make([]workflowruntime.AttemptSnapshot, 0)
	for rows.Next() {
		attempt, err := scanWorkflowAttempt(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workflow attempts: %w", err)
	}
	return result, nil
}

func unfinishedWorkflowAttempt(ctx context.Context, query workflowSQL, node workflowruntime.NodeInvocationSnapshot) (*workflowruntime.AttemptSnapshot, error) {
	rows, err := query.QueryContext(ctx, workflowAttemptSelect+`
WHERE run_id = ? AND node_id = ? AND iteration = ? AND status = ?
ORDER BY attempt_number ASC`, node.ID.RunID, node.ID.NodeID, node.ID.Iteration, workflowruntime.NodeRunning)
	if err != nil {
		return nil, fmt.Errorf("load unfinished workflow attempt: %w", err)
	}
	defer closeRows(rows)
	var unfinished *workflowruntime.AttemptSnapshot
	for rows.Next() {
		attempt, err := scanWorkflowAttempt(rows)
		if err != nil {
			return nil, err
		}
		if unfinished != nil {
			return nil, workflowAttemptConflict(node.ID, attempt.ID.Number, "multiple unfinished attempts exist")
		}
		attemptCopy := attempt
		unfinished = &attemptCopy
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load unfinished workflow attempt: %w", err)
	}
	if unfinished != nil && unfinished.ID.Number != node.LatestAttempt {
		return nil, workflowAttemptConflict(node.ID, unfinished.ID.Number, "unfinished attempt is not LatestAttempt")
	}
	if (node.Status == workflowruntime.NodeRunning || node.Status == workflowruntime.NodeWaiting) && unfinished == nil {
		return nil, workflowAttemptConflict(node.ID, node.LatestAttempt, "node status requires an unfinished attempt")
	}
	return unfinished, nil
}

func validateWorkflowAttemptHistory(ctx context.Context, query workflowSQL, node workflowruntime.NodeInvocationSnapshot) error {
	var maximum int
	if err := query.QueryRowContext(ctx, `
SELECT COALESCE(MAX(attempt_number), 0)
FROM workflow_attempts
WHERE run_id = ? AND node_id = ? AND iteration = ?`,
		node.ID.RunID, node.ID.NodeID, node.ID.Iteration,
	).Scan(&maximum); err != nil {
		return fmt.Errorf("load workflow attempt history: %w", err)
	}
	if maximum != node.LatestAttempt {
		return workflowAttemptConflict(node.ID, node.LatestAttempt, "LatestAttempt does not match durable history")
	}
	return nil
}

func validateWorkflowNodeTransition(request workflowruntime.NodeTransitionRequest) error {
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

func validateWorkflowStartAttempt(request workflowruntime.StartNodeAttemptRequest) error {
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

func validateWorkflowFinishAttempt(request workflowruntime.FinishNodeAttemptRequest) error {
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
	if !workflowAttemptOutcome(request.AttemptStatus) {
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

func workflowAttemptOutcome(status workflowruntime.NodeStatus) bool {
	switch status {
	case workflowruntime.NodeSucceeded, workflowruntime.NodeFailed, workflowruntime.NodeCanceled,
		workflowruntime.NodeTimedOut, workflowruntime.NodeCrashed:
		return true
	default:
		return false
	}
}

func validateWorkflowLifecycleClaim(node workflowruntime.NodeInvocationSnapshot, proof *workflowruntime.ClaimProof, at time.Time) error {
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

func workflowAttemptAttributes(entity, from, to string, attempt workflowruntime.AttemptSnapshot) map[string]string {
	attributes := workflowTransitionAttributes(entity, from, to)
	attributes["attempt_number"] = strconv.Itoa(attempt.ID.Number)
	attributes["executor_kind"] = attempt.Executor.Kind
	attributes["executor_version"] = attempt.Executor.Version
	if attempt.Executor.Target != "" {
		attributes["executor_target"] = attempt.Executor.Target
	}
	return attributes
}

func workflowNodeTransitionError(node workflowruntime.NodeInvocationSnapshot, to workflowruntime.NodeStatus, reason string) error {
	return &workflowruntime.TransitionError{
		Entity: "node", ID: workflowNodeIdentity(node.ID), From: string(node.Status), To: string(to), Reason: reason,
	}
}

func workflowAttemptConflict(id workflowruntime.NodeInvocationID, attempt int, reason string) error {
	return &workflowruntime.AttemptConflictError{Invocation: id, Attempt: attempt, Reason: reason}
}

func workflowNodeIdentity(id workflowruntime.NodeInvocationID) string {
	if id.Iteration == "" {
		return string(id.RunID) + "/" + id.NodeID
	}
	return string(id.RunID) + "/" + id.NodeID + "[" + id.Iteration + "]"
}

func encodeWorkflowTransitionExplanation(reason *workflowruntime.BlockedReason) (string, error) {
	canonical := cloneWorkflowBlocked(reason)
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

func workflowTransitionExplanationMatches(
	ctx context.Context,
	query workflowSQL,
	node workflowruntime.NodeInvocationSnapshot,
	explanation *workflowruntime.BlockedReason,
	at time.Time,
) (bool, error) {
	if node.Status != workflowruntime.NodeSkipped {
		return explanation == nil, nil
	}
	expected := ""
	if explanation != nil {
		encoded, err := encodeWorkflowTransitionExplanation(explanation)
		if err != nil {
			return false, workflowInvalid(err)
		}
		expected = encoded
	}
	rows, err := query.QueryContext(ctx, workflowEventSelect+`
WHERE run_id = ? AND event_type = ? AND occurred_at = ?
ORDER BY sequence DESC`, node.ID.RunID, workflowruntime.EventNodeStatusChanged, workflowTime(at))
	if err != nil {
		return false, fmt.Errorf("load skipped workflow transition explanation: %w", err)
	}
	defer closeRows(rows)
	for rows.Next() {
		event, scanErr := scanWorkflowEvent(rows)
		if scanErr != nil {
			return false, scanErr
		}
		if event.Invocation == nil || *event.Invocation != node.ID ||
			event.Attributes["to_status"] != string(workflowruntime.NodeSkipped) {
			continue
		}
		return event.Attributes["explanation"] == expected, nil
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("scan skipped workflow transition explanation: %w", err)
	}
	return expected == "", nil
}

func cloneWorkflowBlocked(reason *workflowruntime.BlockedReason) *workflowruntime.BlockedReason {
	if reason == nil {
		return nil
	}
	copyReason := *reason
	copyReason.Dependencies = append([]workflowruntime.NodeInvocationID(nil), reason.Dependencies...)
	copyReason.Details = cloneWorkflowStringMap(reason.Details)
	return &copyReason
}

func cloneWorkflowExecutor(executor workflowruntime.ExecutorMetadata) workflowruntime.ExecutorMetadata {
	executor.Attributes = cloneWorkflowStringMap(executor.Attributes)
	return executor
}

func cloneWorkflowFailure(failure *workflowruntime.Failure) *workflowruntime.Failure {
	if failure == nil {
		return nil
	}
	copyFailure := *failure
	copyFailure.Details = cloneWorkflowStringMap(failure.Details)
	return &copyFailure
}

func cloneWorkflowAttempt(attempt workflowruntime.AttemptSnapshot) workflowruntime.AttemptSnapshot {
	attempt.Executor = cloneWorkflowExecutor(attempt.Executor)
	attempt.Inputs = cloneWorkflowValueRef(attempt.Inputs)
	attempt.Outputs = cloneWorkflowValueRef(attempt.Outputs)
	attempt.Failure = cloneWorkflowFailure(attempt.Failure)
	return attempt
}
