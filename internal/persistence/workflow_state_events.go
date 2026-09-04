package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

// AppendEvent implements runtime.StateStore with an atomic per-run sequence.
func (s *WorkflowStateStore) AppendEvent(ctx context.Context, request workflowruntime.AppendEventRequest) (workflowruntime.Event, error) {
	var result workflowruntime.Event
	err := s.write(ctx, "append workflow event", func(query workflowSQL) error {
		candidate := workflowruntime.Event{
			Sequence: 1, RunID: request.RunID, Invocation: cloneWorkflowInvocation(request.Invocation), Attempt: cloneWorkflowAttemptID(request.Attempt),
			Type: request.Type, OccurredAt: request.OccurredAt.UTC(), Attributes: cloneWorkflowStringMap(request.Attributes), Values: cloneWorkflowValueRef(request.Values),
			Redaction: request.Redaction, Retention: request.Retention,
		}
		if err := candidate.Validate(); err != nil {
			return workflowInvalid(err)
		}
		owner := candidate.Invocation
		if owner == nil && candidate.Attempt != nil {
			id := candidate.Attempt.Invocation
			owner = &id
		}
		if owner != nil {
			allowed, err := workflowControlAdmissionAllowed(ctx, query, *owner)
			if err != nil {
				return err
			}
			if !allowed {
				return workflowInvalid(errors.New("pending terminal intent fences non-finalizer event persistence"))
			}
		} else {
			pending, err := workflowRunHasPendingTerminalIntent(ctx, query, candidate.RunID)
			if err != nil {
				return err
			}
			if pending {
				return workflowInvalid(errors.New("pending terminal intent fences anonymous run-level event persistence"))
			}
		}
		var err error
		result, err = appendWorkflowEvent(ctx, query, request)
		return err
	})
	if err != nil {
		return workflowruntime.Event{}, err
	}
	return result, nil
}

func appendWorkflowEvent(ctx context.Context, query workflowSQL, request workflowruntime.AppendEventRequest) (workflowruntime.Event, error) {
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_event_sequences(run_id, last_sequence)
VALUES (?, 0)
ON CONFLICT(run_id) DO NOTHING`, request.RunID); err != nil {
		return workflowruntime.Event{}, fmt.Errorf("ensure workflow event sequence: %w", err)
	}
	if _, err := query.ExecContext(ctx, `
UPDATE workflow_event_sequences
SET last_sequence = last_sequence + 1
WHERE run_id = ?`, request.RunID); err != nil {
		return workflowruntime.Event{}, fmt.Errorf("allocate workflow event sequence: %w", err)
	}
	var sequence int64
	if err := query.QueryRowContext(ctx, `
SELECT last_sequence FROM workflow_event_sequences WHERE run_id = ?`, request.RunID).Scan(&sequence); err != nil {
		return workflowruntime.Event{}, fmt.Errorf("load workflow event sequence: %w", err)
	}
	sequenceValue, generationErr := workflowGeneration("event sequence", sequence)
	if generationErr != nil {
		return workflowruntime.Event{}, generationErr
	}
	event := workflowruntime.Event{
		Sequence: sequenceValue, RunID: request.RunID,
		Invocation: cloneWorkflowInvocation(request.Invocation),
		Attempt:    cloneWorkflowAttemptID(request.Attempt),
		Type:       request.Type, OccurredAt: request.OccurredAt.UTC(),
		Attributes: cloneWorkflowStringMap(request.Attributes),
		Values:     cloneWorkflowValueRef(request.Values),
		Redaction:  request.Redaction, Retention: request.Retention,
	}
	if err := event.Validate(); err != nil {
		return workflowruntime.Event{}, workflowInvalid(err)
	}
	invocationJSON, err := encodeOptionalWorkflowJSON(event.Invocation)
	if err != nil {
		return workflowruntime.Event{}, err
	}
	attemptJSON, err := encodeOptionalWorkflowJSON(event.Attempt)
	if err != nil {
		return workflowruntime.Event{}, err
	}
	attributesJSON := any(nil)
	if event.Attributes != nil {
		attributesJSON, err = encodeWorkflowJSON(event.Attributes)
		if err != nil {
			return workflowruntime.Event{}, err
		}
	}
	valuesJSON, err := encodeOptionalWorkflowJSON(event.Values)
	if err != nil {
		return workflowruntime.Event{}, err
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_events(
    run_id, sequence, invocation_json, attempt_json, event_type,
    occurred_at, attributes_json, values_ref_json, redaction, retention
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.RunID, sequence, invocationJSON, attemptJSON, event.Type,
		workflowTime(event.OccurredAt), attributesJSON, valuesJSON,
		event.Redaction, event.Retention,
	); err != nil {
		return workflowruntime.Event{}, fmt.Errorf("insert workflow event: %w", err)
	}
	return event, nil
}

// ListEvents implements runtime.StateStore in ascending per-run sequence.
func (s *WorkflowStateStore) ListEvents(ctx context.Context, query workflowruntime.EventQuery) ([]workflowruntime.Event, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if query.RunID == "" || query.Limit < 0 {
		return nil, workflowInvalid(errors.New("event query requires run id and non-negative limit"))
	}
	after, err := sqliteGeneration("event after sequence", query.AfterSequence)
	if err != nil {
		return nil, err
	}
	statement := workflowEventSelect + ` WHERE run_id = ? AND sequence > ? ORDER BY sequence ASC`
	arguments := []any{query.RunID, after}
	if query.Limit > 0 {
		statement += ` LIMIT ?`
		arguments = append(arguments, query.Limit)
	}
	rows, err := s.db.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return nil, fmt.Errorf("list workflow events: %w", err)
	}
	defer closeRows(rows)
	result := make([]workflowruntime.Event, 0)
	for rows.Next() {
		event, err := scanWorkflowEvent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workflow events: %w", err)
	}
	return result, nil
}

func cloneWorkflowInvocation(id *workflowruntime.NodeInvocationID) *workflowruntime.NodeInvocationID {
	if id == nil {
		return nil
	}
	copyID := *id
	return &copyID
}

func cloneWorkflowAttemptID(id *workflowruntime.AttemptID) *workflowruntime.AttemptID {
	if id == nil {
		return nil
	}
	copyID := *id
	return &copyID
}

func loadWorkflowIdempotency(ctx context.Context, query workflowSQL, table, key string) (string, string, bool, error) {
	allowed := map[string]bool{
		"workflow_run_start_idempotency":           true,
		"workflow_wait_resume_idempotency":         true,
		"workflow_claim_idempotency":               true,
		"workflow_external_activations":            true,
		"workflow_retry_activation_idempotency":    true,
		"workflow_run_cancellation_idempotency":    true,
		"workflow_scheduler_admission_idempotency": true,
	}
	if !allowed[table] {
		return "", "", false, workflowInvalid(fmt.Errorf("unsupported idempotency table %q", table))
	}
	var requestJSON, resultJSON string
	err := query.QueryRowContext(ctx,
		`SELECT request_json, result_json FROM `+table+` WHERE idempotency_key = ?`, key,
	).Scan(&requestJSON, &resultJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("load workflow idempotency record: %w", err)
	}
	return requestJSON, resultJSON, true, nil
}
