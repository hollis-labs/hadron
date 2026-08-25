package persistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.ExternalOperationStore = (*WorkflowStateStore)(nil)

const workflowExternalOperationSelect = `
SELECT run_id, node_id, iteration, attempt_number, ref_json, invocation_json,
       status, progress_json, outputs_ref_json, failure_json,
       cancel_requested_at, last_observed_at, last_heartbeat_at,
       generation, created_at, updated_at
FROM workflow_external_operations`

func (s *WorkflowStateStore) LoadExternalOperation(ctx context.Context, id workflowruntime.AttemptID) (workflowruntime.ExternalOperationSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if err := id.Validate(); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, workflowInvalid(err)
	}
	return loadWorkflowExternalOperation(ctx, s.db, id)
}

func (s *WorkflowStateStore) SuspendExternalOperation(ctx context.Context, request workflowruntime.SuspendExternalOperationRequest) (workflowruntime.SuspendExternalOperationResult, error) {
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.SuspendExternalOperationResult{}, workflowInvalid(err)
	}
	var result workflowruntime.SuspendExternalOperationResult
	writeErr := s.write(ctx, "suspend workflow external operation", func(query workflowSQL) error {
		currentNode, err := loadWorkflowNode(ctx, query, request.Operation.Attempt.Invocation)
		if err != nil {
			return err
		}
		if currentNode.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("external operation node", request.ExpectedNodeGeneration, currentNode.Generation)
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, currentNode.ID)
		if err != nil {
			return err
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences external suspension"))
		}
		if currentNode.Status != workflowruntime.NodeRunning || currentNode.Wait != nil {
			return workflowInvalid(errors.New("external suspension requires a running node without a generic wait"))
		}
		if claimErr := validateWorkflowLifecycleClaim(currentNode, &request.Claim, request.At); claimErr != nil {
			return claimErr
		}
		currentAttempt, err := loadWorkflowAttempt(ctx, query, request.Operation.Attempt)
		if err != nil {
			return err
		}
		if currentAttempt.ID.Number != currentNode.LatestAttempt || currentAttempt.Status != workflowruntime.NodeRunning || !currentAttempt.FinishedAt.IsZero() {
			return workflowAttemptConflict(currentNode.ID, request.Operation.Attempt.Number, "external suspension requires latest unfinished attempt")
		}
		if currentAttempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("external operation attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
		}
		if request.At.Before(currentNode.UpdatedAt) || request.At.Before(currentAttempt.UpdatedAt) {
			return workflowInvalid(errors.New("external suspension time must not regress persisted state"))
		}
		if _, err := loadWorkflowExternalOperation(ctx, query, request.Operation.Attempt); err == nil {
			return fmt.Errorf("%w: external operation", workflowruntime.ErrAlreadyExists)
		} else if !errors.Is(err, workflowruntime.ErrNotFound) {
			return err
		}

		operation := cloneWorkflowExternalOperation(request.Operation)
		operation.Status = stepkind.ObservationPending
		operation.Generation = 1
		operation.CreatedAt, operation.UpdatedAt = request.At, request.At
		node := cloneWorkflowNode(currentNode)
		node.Status = workflowruntime.NodeWaiting
		node.Lease = nil
		node.Generation++
		node.UpdatedAt = request.At
		if validationErr := operation.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if validationErr := node.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if insertErr := insertWorkflowExternalOperation(ctx, query, operation); insertErr != nil {
			return insertErr
		}
		if updateErr := updateWorkflowNodeCAS(ctx, query, node, currentNode.Generation); updateErr != nil {
			return updateErr
		}
		events := make([]workflowruntime.Event, 0, 2)
		for _, eventRequest := range workflowExternalSuspensionEvents(operation, currentAttempt, currentNode.Status, node.Status, request.At) {
			event, err := appendWorkflowEvent(ctx, query, eventRequest)
			if err != nil {
				return err
			}
			events = append(events, event)
		}
		result = workflowruntime.SuspendExternalOperationResult{Operation: operation, Node: node, Attempt: currentAttempt, Events: events}
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) RequestExternalOperationCancel(ctx context.Context, request workflowruntime.RequestExternalOperationCancelRequest) (workflowruntime.RequestExternalOperationCancelResult, error) {
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.RequestExternalOperationCancelResult{}, workflowInvalid(err)
	}
	var result workflowruntime.RequestExternalOperationCancelResult
	writeErr := s.write(ctx, "request workflow external operation cancellation", func(query workflowSQL) error {
		current, loadErr := loadWorkflowExternalOperation(ctx, query, request.Attempt)
		if loadErr != nil {
			return loadErr
		}
		if current.Generation != request.ExpectedOperationGeneration {
			return workflowCAS("external operation", request.ExpectedOperationGeneration, current.Generation)
		}
		allowed, admissionErr := workflowControlAdmissionAllowed(ctx, query, request.Attempt.Invocation)
		if admissionErr != nil {
			return admissionErr
		}
		if !allowed {
			cancellationAllowed, intentErr := workflowHasPendingExternalCancellation(ctx, query, request.Attempt)
			if intentErr != nil {
				return intentErr
			}
			if !cancellationAllowed {
				return workflowInvalid(errors.New("pending terminal intent fences external cancellation request"))
			}
		}
		if current.Status != stepkind.ObservationPending {
			return &workflowruntime.TransitionConflictError{Entity: "external operation", ID: workflowExternalAttemptIdentity(current.Attempt), Status: string(current.Status), Reason: "terminal operation cannot accept cancellation intent"}
		}
		if !current.CancelRequestedAt.IsZero() {
			result.Operation = current
			return nil
		}
		if request.At.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("external cancel time must not regress persisted state"))
		}
		next := cloneWorkflowExternalOperation(current)
		next.CancelRequestedAt = request.At
		next.Generation++
		next.UpdatedAt = request.At
		if validationErr := next.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if updateErr := updateWorkflowExternalOperationCAS(ctx, query, next, current.Generation); updateErr != nil {
			return updateErr
		}
		invocation, attempt := next.Attempt.Invocation, next.Attempt
		event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt,
			Type: workflowruntime.EventExternalOperationCancelRequested, OccurredAt: request.At,
			Attributes: workflowExternalEventAttributes(next, string(current.Status), string(next.Status)),
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return err
		}
		result.Operation, result.Event = next, &event
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) ApplyExternalOperation(ctx context.Context, request workflowruntime.ApplyExternalOperationRequest) (workflowruntime.ApplyExternalOperationResult, error) {
	request.At, request.HeartbeatAt = request.At.UTC(), request.HeartbeatAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ApplyExternalOperationResult{}, workflowInvalid(err)
	}
	var result workflowruntime.ApplyExternalOperationResult
	err := s.write(ctx, "apply workflow external operation", func(query workflowSQL) error {
		currentOperation, err := loadWorkflowExternalOperation(ctx, query, request.Attempt)
		if err != nil {
			return err
		}
		if currentOperation.Generation != request.ExpectedOperationGeneration {
			return workflowCAS("external operation", request.ExpectedOperationGeneration, currentOperation.Generation)
		}
		currentNode, err := loadWorkflowNode(ctx, query, request.Attempt.Invocation)
		if err != nil {
			return err
		}
		if currentNode.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("external operation node", request.ExpectedNodeGeneration, currentNode.Generation)
		}
		currentAttempt, err := loadWorkflowAttempt(ctx, query, request.Attempt)
		if err != nil {
			return err
		}
		if currentAttempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("external operation attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
		}
		allowedCanceledResolution := false
		if request.Status == stepkind.ObservationCanceled && request.NextNodeStatus == workflowruntime.NodeCanceled {
			allowedCanceledResolution, err = workflowHasPendingExternalCancellation(ctx, query, request.Attempt)
			if err != nil {
				return err
			}
		}
		run, runErr := loadWorkflowRun(ctx, query, request.Attempt.Invocation.RunID)
		if runErr != nil {
			return runErr
		}
		allowedRun, runAdmissionErr := workflowRunAllowsCompensationExecution(ctx, query, run, currentNode)
		if runAdmissionErr != nil {
			return runAdmissionErr
		}
		if !allowedRun {
			if run.Status != workflowruntime.RunCanceled || !allowedCanceledResolution {
				return workflowInvalid(errors.New("terminal run fences external mutation"))
			}
		}
		allowed, err := workflowControlAdmissionAllowed(ctx, query, request.Attempt.Invocation)
		if err != nil {
			return err
		}
		if !allowed {
			if !allowedCanceledResolution {
				return workflowInvalid(errors.New("pending terminal intent fences external mutation"))
			}
		}
		if currentOperation.Status != stepkind.ObservationPending || currentNode.Status != workflowruntime.NodeWaiting || currentNode.Wait != nil || currentNode.Lease != nil ||
			currentNode.LatestAttempt != request.Attempt.Number || currentAttempt.Status != workflowruntime.NodeRunning || !currentAttempt.FinishedAt.IsZero() {
			return workflowInvalid(errors.New("external observation requires a pending operation and matching waiting unfinished attempt"))
		}
		if request.At.Before(currentOperation.UpdatedAt) || request.At.Before(currentNode.UpdatedAt) || request.At.Before(currentAttempt.UpdatedAt) {
			return workflowInvalid(errors.New("external observation time must not regress persisted state"))
		}
		if !request.ObservedAt.IsZero() && !currentOperation.LastObservedAt.IsZero() && request.ObservedAt.Before(currentOperation.LastObservedAt) {
			return workflowInvalid(errors.New("external observed_at must not regress"))
		}
		if !request.HeartbeatAt.IsZero() && !currentOperation.LastHeartbeatAt.IsZero() && request.HeartbeatAt.Before(currentOperation.LastHeartbeatAt) {
			return workflowInvalid(errors.New("external heartbeat must not regress"))
		}

		nextOperation := cloneWorkflowExternalOperation(currentOperation)
		nextOperation.Status = request.Status
		nextOperation.Progress = cloneWorkflowStringMap(request.Progress)
		nextOperation.Outputs = cloneWorkflowValueRef(request.Outputs)
		nextOperation.Failure = cloneWorkflowFailure(request.Failure)
		if !request.ObservedAt.IsZero() {
			nextOperation.LastObservedAt = request.ObservedAt
		}
		if !request.HeartbeatAt.IsZero() {
			nextOperation.LastHeartbeatAt = request.HeartbeatAt
		}
		nextOperation.Generation++
		nextOperation.UpdatedAt = request.At
		if err := nextOperation.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowExternalOperationCAS(ctx, query, nextOperation, currentOperation.Generation); err != nil {
			return err
		}
		nextNode, nextAttempt := cloneWorkflowNode(currentNode), cloneWorkflowAttempt(currentAttempt)
		eventRequests := make([]workflowruntime.AppendEventRequest, 0, 4)
		if request.Verification != nil {
			invocation, attempt := request.Attempt.Invocation, request.Attempt
			eventRequests = append(eventRequests, workflowruntime.AppendEventRequest{
				RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt,
				Type: workflowruntime.EventNodeVerificationCompleted, OccurredAt: request.At,
				Attributes: cloneWorkflowStringMap(request.Verification.Attributes), Values: cloneWorkflowValueRef(&request.Verification.Values),
				Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
			})
		}
		eventRequests = append(eventRequests, workflowExternalObservationEvent(currentOperation, nextOperation, request.At))
		if request.Status != stepkind.ObservationPending {
			nextAttempt.Status = workflowExternalAttemptStatus(request.Status)
			nextAttempt.Outputs = cloneWorkflowValueRef(request.Outputs)
			nextAttempt.Failure = cloneWorkflowFailure(request.Failure)
			nextAttempt.FinishedAt, nextAttempt.UpdatedAt = request.At, request.At
			nextAttempt.Generation++
			nextNode.Status = request.NextNodeStatus
			nextNode.Lease = nil
			nextNode.Generation++
			nextNode.UpdatedAt = request.At
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
			invocation, attempt := nextNode.ID, nextAttempt.ID
			attributes := workflowAttemptAttributes("node_attempt", string(currentNode.Status), string(nextNode.Status), nextAttempt)
			attributes["attempt_status"] = string(nextAttempt.Status)
			if nextAttempt.Failure != nil {
				attributes["failure_code"] = nextAttempt.Failure.Code
			}
			eventRequests = append(eventRequests,
				workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: request.At, Attributes: attributes, Values: cloneWorkflowValueRef(request.Outputs), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
				workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.At, Attributes: workflowAttemptAttributes("node", string(currentNode.Status), string(nextNode.Status), nextAttempt), Values: cloneWorkflowValueRef(request.Outputs), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
			)
		}
		events := make([]workflowruntime.Event, 0, len(eventRequests))
		for _, eventRequest := range eventRequests {
			event, err := appendWorkflowEvent(ctx, query, eventRequest)
			if err != nil {
				return err
			}
			events = append(events, event)
		}
		result = workflowruntime.ApplyExternalOperationResult{Operation: nextOperation, Node: nextNode, Attempt: nextAttempt, Events: events}
		return nil
	})
	return result, err
}

func workflowHasPendingExternalCancellation(ctx context.Context, query workflowSQL, attempt workflowruntime.AttemptID) (bool, error) {
	id, err := workflowCancellationIntentID(workflowruntime.CancellationExternalOperation, &attempt, "")
	if err != nil {
		return false, workflowInvalid(err)
	}
	intent, err := loadWorkflowCancellationIntent(ctx, query, id)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return intent.Status == workflowruntime.CancellationPending && intent.RunID == attempt.Invocation.RunID && intent.Kind == workflowruntime.CancellationExternalOperation && intent.Attempt != nil && *intent.Attempt == attempt, nil
}

func (s *WorkflowStateStore) RecoverExternalOperations(ctx context.Context, query workflowruntime.ExternalOperationQuery) ([]workflowruntime.ExternalOperationSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, workflowInvalid(errors.New("external recovery limit must not be negative"))
	}
	statement := workflowExternalOperationSelect + ` WHERE status = ?`
	args := []any{stepkind.ObservationPending}
	if query.RunID != "" {
		statement += ` AND run_id = ?`
		args = append(args, query.RunID)
	}
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("recover workflow external operations: %w", err)
	}
	defer closeRows(rows)
	result := make([]workflowruntime.ExternalOperationSnapshot, 0)
	for rows.Next() {
		operation, err := scanWorkflowExternalOperation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, operation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("recover workflow external operations: %w", err)
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.Before(right.UpdatedAt)
		}
		if left.Attempt.Invocation.RunID != right.Attempt.Invocation.RunID {
			return left.Attempt.Invocation.RunID < right.Attempt.Invocation.RunID
		}
		if left.Attempt.Invocation.NodeID != right.Attempt.Invocation.NodeID {
			return left.Attempt.Invocation.NodeID < right.Attempt.Invocation.NodeID
		}
		if left.Attempt.Invocation.Iteration != right.Attempt.Invocation.Iteration {
			return left.Attempt.Invocation.Iteration < right.Attempt.Invocation.Iteration
		}
		return left.Attempt.Number < right.Attempt.Number
	})
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, nil
}

func loadWorkflowExternalOperation(ctx context.Context, query workflowSQL, id workflowruntime.AttemptID) (workflowruntime.ExternalOperationSnapshot, error) {
	return scanWorkflowExternalOperation(query.QueryRowContext(ctx, workflowExternalOperationSelect+`
WHERE run_id = ? AND node_id = ? AND iteration = ? AND attempt_number = ?`,
		id.Invocation.RunID, id.Invocation.NodeID, id.Invocation.Iteration, id.Number))
}

func scanWorkflowExternalOperation(row workflowScanner) (workflowruntime.ExternalOperationSnapshot, error) {
	var (
		operation                              workflowruntime.ExternalOperationSnapshot
		refJSON, invocationJSON, status        string
		progressJSON, outputsJSON, failureJSON sql.NullString
		cancelAt, observedAt, heartbeatAt      sql.NullString
		generation                             int64
		createdAt, updatedAt                   string
	)
	if err := row.Scan(
		&operation.Attempt.Invocation.RunID, &operation.Attempt.Invocation.NodeID,
		&operation.Attempt.Invocation.Iteration, &operation.Attempt.Number,
		&refJSON, &invocationJSON, &status, &progressJSON, &outputsJSON, &failureJSON,
		&cancelAt, &observedAt, &heartbeatAt, &generation, &createdAt, &updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.ExternalOperationSnapshot{}, fmt.Errorf("%w: external operation", workflowruntime.ErrNotFound)
		}
		return workflowruntime.ExternalOperationSnapshot{}, fmt.Errorf("load workflow external operation: %w", err)
	}
	if err := decodeWorkflowJSON("external operation ref", refJSON, &operation.Ref); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if err := decodeWorkflowExternalJSON("external operation invocation", invocationJSON, &operation.Invocation); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	operation.Status = stepkind.ObservationState(status)
	var err error
	progress, err := decodeOptionalWorkflowJSON[map[string]string]("external operation progress", progressJSON)
	if err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if progress != nil {
		operation.Progress = *progress
	}
	if operation.Outputs, err = decodeOptionalWorkflowJSON[values.ValueSetRef]("external operation outputs", outputsJSON); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if operation.Failure, err = decodeOptionalWorkflowJSON[workflowruntime.Failure]("external operation failure", failureJSON); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if operation.CancelRequestedAt, err = parseOptionalWorkflowTime("external operation cancel_requested_at", cancelAt); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if operation.LastObservedAt, err = parseOptionalWorkflowTime("external operation last_observed_at", observedAt); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if operation.LastHeartbeatAt, err = parseOptionalWorkflowTime("external operation last_heartbeat_at", heartbeatAt); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if operation.Generation, err = workflowGeneration("external operation generation", generation); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if operation.CreatedAt, err = parseWorkflowTime("external operation created_at", createdAt); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if operation.UpdatedAt, err = parseWorkflowTime("external operation updated_at", updatedAt); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, err
	}
	if err := operation.Validate(); err != nil {
		return workflowruntime.ExternalOperationSnapshot{}, workflowInvalid(err)
	}
	return operation, nil
}

func insertWorkflowExternalOperation(ctx context.Context, query workflowSQL, operation workflowruntime.ExternalOperationSnapshot) error {
	refJSON, err := encodeWorkflowJSON(operation.Ref)
	if err != nil {
		return err
	}
	invocationJSON, err := encodeWorkflowJSON(operation.Invocation)
	if err != nil {
		return err
	}
	progressJSON, err := encodeOptionalWorkflowMap(operation.Progress)
	if err != nil {
		return err
	}
	outputsJSON, err := encodeOptionalWorkflowJSON(operation.Outputs)
	if err != nil {
		return err
	}
	failureJSON, err := encodeOptionalWorkflowJSON(operation.Failure)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("external operation generation", operation.Generation)
	if err != nil {
		return err
	}
	_, err = query.ExecContext(ctx, `
INSERT INTO workflow_external_operations(
    run_id, node_id, iteration, attempt_number, ref_json, invocation_json,
    status, progress_json, outputs_ref_json, failure_json,
    cancel_requested_at, last_observed_at, last_heartbeat_at,
    generation, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		operation.Attempt.Invocation.RunID, operation.Attempt.Invocation.NodeID, operation.Attempt.Invocation.Iteration, operation.Attempt.Number,
		refJSON, invocationJSON, operation.Status, progressJSON, outputsJSON, failureJSON,
		workflowOptionalTime(operation.CancelRequestedAt), workflowOptionalTime(operation.LastObservedAt), workflowOptionalTime(operation.LastHeartbeatAt),
		generation, workflowTime(operation.CreatedAt), workflowTime(operation.UpdatedAt),
	)
	if err != nil {
		if isSQLiteConstraint(err) {
			return fmt.Errorf("%w: external operation", workflowruntime.ErrAlreadyExists)
		}
		return fmt.Errorf("insert workflow external operation: %w", err)
	}
	return nil
}

func updateWorkflowExternalOperationCAS(ctx context.Context, query workflowSQL, operation workflowruntime.ExternalOperationSnapshot, expected uint64) error {
	progressJSON, err := encodeOptionalWorkflowMap(operation.Progress)
	if err != nil {
		return err
	}
	outputsJSON, err := encodeOptionalWorkflowJSON(operation.Outputs)
	if err != nil {
		return err
	}
	failureJSON, err := encodeOptionalWorkflowJSON(operation.Failure)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("external operation generation", operation.Generation)
	if err != nil {
		return err
	}
	expectedGeneration, err := sqliteGeneration("expected external operation generation", expected)
	if err != nil {
		return err
	}
	result, err := query.ExecContext(ctx, `
UPDATE workflow_external_operations
SET status = ?, progress_json = ?, outputs_ref_json = ?, failure_json = ?,
    cancel_requested_at = ?, last_observed_at = ?, last_heartbeat_at = ?,
    generation = ?, updated_at = ?
WHERE run_id = ? AND node_id = ? AND iteration = ? AND attempt_number = ? AND generation = ?`,
		operation.Status, progressJSON, outputsJSON, failureJSON,
		workflowOptionalTime(operation.CancelRequestedAt), workflowOptionalTime(operation.LastObservedAt), workflowOptionalTime(operation.LastHeartbeatAt),
		generation, workflowTime(operation.UpdatedAt), operation.Attempt.Invocation.RunID, operation.Attempt.Invocation.NodeID,
		operation.Attempt.Invocation.Iteration, operation.Attempt.Number, expectedGeneration,
	)
	if err != nil {
		return fmt.Errorf("update workflow external operation: %w", err)
	}
	return expectOneWorkflowRow(result, "external operation", expected, operation.Generation-1)
}

func encodeOptionalWorkflowMap(value map[string]string) (any, error) {
	if value == nil {
		return nil, nil
	}
	return encodeWorkflowJSON(value)
}

func cloneWorkflowExternalOperation(operation workflowruntime.ExternalOperationSnapshot) workflowruntime.ExternalOperationSnapshot {
	encoded, err := encodeWorkflowJSON(operation)
	if err != nil {
		panic("clone validated workflow external operation: " + err.Error())
	}
	var cloned workflowruntime.ExternalOperationSnapshot
	if err := decodeWorkflowExternalJSON("cloned external operation", encoded, &cloned); err != nil {
		panic("clone validated workflow external operation: " + err.Error())
	}
	return cloned
}

func decodeWorkflowExternalJSON(field, encoded string, target any) error {
	decoder := json.NewDecoder(bytes.NewBufferString(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return workflowInvalid(fmt.Errorf("decode %s: %w", field, err))
	}
	return nil
}

func workflowExternalSuspensionEvents(operation workflowruntime.ExternalOperationSnapshot, attempt workflowruntime.AttemptSnapshot, from, to workflowruntime.NodeStatus, at time.Time) []workflowruntime.AppendEventRequest {
	invocation, attemptID := operation.Attempt.Invocation, operation.Attempt
	return []workflowruntime.AppendEventRequest{
		{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventExternalOperationSuspended, OccurredAt: at, Attributes: workflowExternalEventAttributes(operation, "", string(operation.Status)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at, Attributes: workflowAttemptAttributes("node", string(from), string(to), attempt), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
	}
}

func workflowExternalObservationEvent(current, next workflowruntime.ExternalOperationSnapshot, at time.Time) workflowruntime.AppendEventRequest {
	invocation, attempt := next.Attempt.Invocation, next.Attempt
	return workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventExternalOperationObserved, OccurredAt: at, Attributes: workflowExternalEventAttributes(next, string(current.Status), string(next.Status)), Values: cloneWorkflowValueRef(next.Outputs), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
}

func workflowExternalEventAttributes(operation workflowruntime.ExternalOperationSnapshot, from, to string) map[string]string {
	attributes := map[string]string{"entity": "external_operation", "operation_kind": operation.Ref.Kind, "operation_id": operation.Ref.ID, "attempt_number": strconv.Itoa(operation.Attempt.Number), "to_status": to}
	if from != "" {
		attributes["from_status"] = from
	}
	if operation.Failure != nil {
		attributes["failure_code"] = operation.Failure.Code
	}
	return attributes
}

func workflowExternalAttemptStatus(status stepkind.ObservationState) workflowruntime.NodeStatus {
	if status == stepkind.ObservationSucceeded {
		return workflowruntime.NodeSucceeded
	}
	if status == stepkind.ObservationCanceled {
		return workflowruntime.NodeCanceled
	}
	return workflowruntime.NodeFailed
}

func workflowExternalAttemptIdentity(id workflowruntime.AttemptID) string {
	return strings.Join([]string{string(id.Invocation.RunID), id.Invocation.NodeID, id.Invocation.Iteration, strconv.Itoa(id.Number)}, "/")
}
