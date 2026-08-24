package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.RetryStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) LoadRetryActivation(ctx context.Context, id string) (workflowruntime.RetryActivationSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.RetryActivationSnapshot{}, err
	}
	return loadWorkflowRetryActivation(ctx, s.db, id)
}

func loadWorkflowRetryActivation(ctx context.Context, query workflowSQL, id string) (workflowruntime.RetryActivationSnapshot, error) {
	var encoded string
	if err := query.QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_retry_activations WHERE activation_id = ?`, id).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.RetryActivationSnapshot{}, fmt.Errorf("%w: retry activation %q", workflowruntime.ErrNotFound, id)
		}
		return workflowruntime.RetryActivationSnapshot{}, fmt.Errorf("load workflow retry activation: %w", err)
	}
	var snapshot workflowruntime.RetryActivationSnapshot
	if err := decodeWorkflowJSON("retry activation", encoded, &snapshot); err != nil {
		return workflowruntime.RetryActivationSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.RetryActivationSnapshot{}, workflowInvalid(err)
	}
	return snapshot, nil
}

func (s *WorkflowStateStore) ScheduleNodeRetry(ctx context.Context, request workflowruntime.ScheduleNodeRetryRequest) (workflowruntime.ScheduleNodeRetryResult, error) {
	if err := request.Validate(); err != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, workflowInvalid(err)
	}
	var result workflowruntime.ScheduleNodeRetryResult
	writeErr := s.write(ctx, "schedule workflow node retry", func(query workflowSQL) error {
		run, runErr := loadWorkflowRun(ctx, query, request.Activation.Attempt.Invocation.RunID)
		if runErr != nil {
			return runErr
		}
		if !run.Status.Active() {
			return workflowInvalid(errors.New("terminal run cannot schedule retry"))
		}
		node, nodeErr := loadWorkflowNode(ctx, query, request.Activation.Attempt.Invocation)
		if nodeErr != nil {
			return nodeErr
		}
		if node.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("node invocation", request.ExpectedNodeGeneration, node.Generation)
		}
		if node.Status != workflowruntime.NodeRunning || node.LatestAttempt != request.Activation.Attempt.Number {
			return &workflowruntime.AttemptConflictError{Invocation: node.ID, Attempt: request.Activation.Attempt.Number, Reason: "retry requires latest running attempt"}
		}
		at := request.At.UTC()
		if claimErr := validateWorkflowLifecycleClaim(node, &request.Claim, at); claimErr != nil {
			return claimErr
		}
		attempt, attemptErr := loadWorkflowAttempt(ctx, query, request.Activation.Attempt)
		if attemptErr != nil {
			return attemptErr
		}
		if attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("attempt", request.ExpectedAttemptGeneration, attempt.Generation)
		}
		if attempt.Status != workflowruntime.NodeRunning || !attempt.FinishedAt.IsZero() {
			return &workflowruntime.AttemptConflictError{Invocation: node.ID, Attempt: attempt.ID.Number, Reason: "retry attempt is already finished"}
		}
		if at.Before(node.UpdatedAt) || at.Before(attempt.UpdatedAt) {
			return workflowInvalid(errors.New("retry schedule time must not regress persisted state"))
		}

		nextAttempt := cloneWorkflowAttempt(attempt)
		nextAttempt.Status = request.AttemptStatus
		nextAttempt.Failure = cloneWorkflowFailure(&request.Activation.Failure)
		nextAttempt.FinishedAt, nextAttempt.UpdatedAt = at, at
		nextAttempt.Generation++
		nextNode := cloneWorkflowNode(node)
		nextNode.Status = workflowruntime.NodeWaiting
		nextNode.Outputs, nextNode.Blocked, nextNode.Wait, nextNode.Lease = nil, nil, nil, nil
		nextNode.Generation++
		nextNode.UpdatedAt = at
		activation := cloneWorkflowRetryActivation(request.Activation)
		activation.Generation = 1
		activation.CreatedAt, activation.UpdatedAt = at, at
		activation.FireAt = activation.FireAt.UTC()
		if validationErr := nextAttempt.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if validationErr := nextNode.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if validationErr := activation.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		encoded, encodeErr := encodeWorkflowJSON(activation)
		if encodeErr != nil {
			return encodeErr
		}
		if _, execErr := query.ExecContext(ctx, `
INSERT INTO workflow_retry_activations(
    activation_id, run_id, node_id, iteration, attempt_number,
    status, fire_at, generation, snapshot_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, activation.ID, activation.Attempt.Invocation.RunID,
			activation.Attempt.Invocation.NodeID, activation.Attempt.Invocation.Iteration,
			activation.Attempt.Number, activation.Status, workflowTime(activation.FireAt),
			activation.Generation, encoded); execErr != nil {
			if isSQLiteConstraint(execErr) {
				return fmt.Errorf("%w: retry activation or attempt binding", workflowruntime.ErrAlreadyExists)
			}
			return fmt.Errorf("insert workflow retry activation: %w", execErr)
		}
		if updateErr := updateWorkflowAttemptCAS(ctx, query, nextAttempt, attempt.Generation); updateErr != nil {
			return updateErr
		}
		if updateErr := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); updateErr != nil {
			return updateErr
		}
		invocation, attemptID := nextNode.ID, nextAttempt.ID
		finished, finishedErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID,
			Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: at,
			Attributes: map[string]string{
				"attempt_number": strconv.Itoa(attemptID.Number), "attempt_status": string(nextAttempt.Status),
				"from_status": string(node.Status), "to_status": string(nextNode.Status), "failure_code": activation.Failure.Code,
			}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if finishedErr != nil {
			return finishedErr
		}
		retryEvent, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID,
			Type: workflowruntime.EventRetryScheduled, OccurredAt: at,
			Attributes: map[string]string{"activation_id": activation.ID, "fire_at": workflowTime(activation.FireAt)},
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		result = workflowruntime.ScheduleNodeRetryResult{Activation: activation, Node: nextNode, Attempt: nextAttempt, Events: []workflowruntime.Event{finished, retryEvent}}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.ScheduleNodeRetryResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) ActivateNodeRetry(ctx context.Context, request workflowruntime.ActivateNodeRetryRequest) (workflowruntime.ActivateNodeRetryResult, error) {
	if err := request.Validate(); err != nil {
		return workflowruntime.ActivateNodeRetryResult{}, workflowInvalid(err)
	}
	canonical := request
	canonical.Now = canonical.Now.UTC()
	requestJSON, encodeErr := encodeWorkflowJSON(canonical)
	if encodeErr != nil {
		return workflowruntime.ActivateNodeRetryResult{}, encodeErr
	}
	var result workflowruntime.ActivateNodeRetryResult
	writeErr := s.write(ctx, "activate workflow node retry", func(query workflowSQL) error {
		priorRequest, priorResult, found, loadErr := loadWorkflowIdempotency(ctx, query, "workflow_retry_activation_idempotency", request.IdempotencyKey)
		if loadErr != nil {
			return loadErr
		}
		if found {
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("activate retry", request.IdempotencyKey)
			}
			if decodeErr := decodeWorkflowJSON("retry activation result", priorResult, &result); decodeErr != nil {
				return decodeErr
			}
			result.Outcome = workflowruntime.IdempotencyReplayed
			return nil
		}
		activation, activationErr := loadWorkflowRetryActivation(ctx, query, request.ActivationID)
		if activationErr != nil {
			return activationErr
		}
		if activation.Generation != request.ExpectedActivationGeneration {
			return workflowCAS("retry activation", request.ExpectedActivationGeneration, activation.Generation)
		}
		if activation.Status != workflowruntime.RetryScheduled {
			return workflowInvalid(errors.New("retry activation is not scheduled"))
		}
		now := request.Now.UTC()
		if now.Before(activation.FireAt) {
			return fmt.Errorf("%w: fire_at %s", workflowruntime.ErrRetryNotDue, workflowTime(activation.FireAt))
		}
		run, runErr := loadWorkflowRun(ctx, query, activation.Attempt.Invocation.RunID)
		if runErr != nil {
			return runErr
		}
		if !run.Status.Active() {
			return workflowInvalid(errors.New("terminal run fences retry activation"))
		}
		node, nodeErr := loadWorkflowNode(ctx, query, activation.Attempt.Invocation)
		if nodeErr != nil {
			return nodeErr
		}
		if node.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("node invocation", request.ExpectedNodeGeneration, node.Generation)
		}
		if node.Status != workflowruntime.NodeWaiting || node.LatestAttempt != activation.Attempt.Number || node.Wait != nil || node.Lease != nil {
			return workflowInvalid(errors.New("retry activation requires unleased retry-waiting node"))
		}
		nextActivation := cloneWorkflowRetryActivation(activation)
		nextActivation.Status = workflowruntime.RetryActivated
		nextActivation.Generation++
		nextActivation.UpdatedAt = now
		nextNode := cloneWorkflowNode(node)
		nextNode.Status = workflowruntime.NodeReady
		nextNode.Generation++
		nextNode.UpdatedAt = now
		if validationErr := nextActivation.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if validationErr := nextNode.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		encoded, activationEncodeErr := encodeWorkflowJSON(nextActivation)
		if activationEncodeErr != nil {
			return activationEncodeErr
		}
		updated, updateErr := query.ExecContext(ctx, `
UPDATE workflow_retry_activations
SET status = ?, generation = ?, snapshot_json = ?
WHERE activation_id = ? AND generation = ?`, nextActivation.Status, nextActivation.Generation,
			encoded, nextActivation.ID, activation.Generation)
		if updateErr != nil {
			return fmt.Errorf("update workflow retry activation: %w", updateErr)
		}
		if rowErr := expectOneWorkflowRow(updated, "retry activation", activation.Generation, activation.Generation); rowErr != nil {
			return rowErr
		}
		if nodeUpdateErr := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); nodeUpdateErr != nil {
			return nodeUpdateErr
		}
		invocation, attemptID := nextNode.ID, activation.Attempt
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID,
			Type: workflowruntime.EventRetryActivated, OccurredAt: now,
			Attributes: map[string]string{"activation_id": activation.ID, "from_status": string(node.Status), "to_status": string(nextNode.Status)},
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		eventCopy := event
		result = workflowruntime.ActivateNodeRetryResult{Outcome: workflowruntime.IdempotencyApplied, Activation: nextActivation, Node: nextNode, Event: &eventCopy}
		resultJSON, resultEncodeErr := encodeWorkflowJSON(result)
		if resultEncodeErr != nil {
			return resultEncodeErr
		}
		if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_retry_activation_idempotency(idempotency_key, request_json, result_json) VALUES (?, ?, ?)`, request.IdempotencyKey, requestJSON, resultJSON); execErr != nil {
			if isSQLiteConstraint(execErr) {
				return workflowIdempotencyConflict("activate retry", request.IdempotencyKey)
			}
			return fmt.Errorf("record workflow retry activation idempotency: %w", execErr)
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.ActivateNodeRetryResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) RecoverRetryActivations(ctx context.Context, query workflowruntime.RetryActivationQuery) ([]workflowruntime.RetryActivationSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, workflowInvalid(errors.New("retry recovery limit must not be negative"))
	}
	rows, err := s.db.QueryContext(ctx, `SELECT snapshot_json FROM workflow_retry_activations WHERE status = 'scheduled'`)
	if err != nil {
		return nil, fmt.Errorf("recover workflow retry activations: %w", err)
	}
	defer closeRows(rows)
	result := make([]workflowruntime.RetryActivationSnapshot, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var snapshot workflowruntime.RetryActivationSnapshot
		if err := decodeWorkflowJSON("retry activation", encoded, &snapshot); err != nil {
			return nil, err
		}
		if err := snapshot.Validate(); err != nil {
			return nil, workflowInvalid(err)
		}
		if query.RunID != "" && snapshot.Attempt.Invocation.RunID != query.RunID || !query.DueBefore.IsZero() && snapshot.FireAt.After(query.DueBefore) {
			continue
		}
		result = append(result, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].FireAt.Equal(result[j].FireAt) {
			return result[i].FireAt.Before(result[j].FireAt)
		}
		return result[i].ID < result[j].ID
	})
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, nil
}

func cloneWorkflowRetryActivation(snapshot workflowruntime.RetryActivationSnapshot) workflowruntime.RetryActivationSnapshot {
	snapshot.Failure = *cloneWorkflowFailure(&snapshot.Failure)
	return snapshot
}
