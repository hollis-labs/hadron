package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

const workflowRunSelect = `
SELECT r.run_id, p.plan_id, p.version, p.digest, p.schema_version,
       r.status, r.inputs_ref_json, r.outputs_ref_json, r.generation,
       r.created_at, r.updated_at
FROM workflow_runs r
JOIN workflow_plan_refs p ON p.digest = r.plan_digest`

const workflowNodeSelect = `
SELECT n.run_id, n.node_id, n.iteration, n.status, n.blocked_json,
       n.inputs_ref_json, n.outputs_ref_json, n.wait_id,
       n.latest_attempt, n.priority, n.claim_generation, n.generation,
       n.created_at, n.updated_at,
       l.owner, l.token, l.generation, l.expires_at
FROM workflow_node_invocations n
LEFT JOIN workflow_node_leases l
  ON l.run_id = n.run_id AND l.node_id = n.node_id AND l.iteration = n.iteration`

const workflowAttemptSelect = `
SELECT run_id, node_id, iteration, attempt_number, status, executor_json,
       inputs_ref_json, outputs_ref_json, failure_json, started_at,
       finished_at, generation, created_at, updated_at
FROM workflow_attempts`

const workflowWaitSelect = `
SELECT wait_id, run_id, node_id, iteration, status, resume_values_ref_json,
       generation, created_at, updated_at, resolved_at, record_json, deadline
FROM workflow_waits`

const workflowEventSelect = `
SELECT run_id, sequence, invocation_json, attempt_json, event_type,
       occurred_at, attributes_json, values_ref_json, redaction, retention
FROM workflow_events`

func loadWorkflowRun(ctx context.Context, query workflowSQL, id workflowruntime.RunID) (workflowruntime.RunSnapshot, error) {
	return scanWorkflowRun(query.QueryRowContext(ctx, workflowRunSelect+` WHERE r.run_id = ?`, id))
}

func scanWorkflowRun(row workflowScanner) (workflowruntime.RunSnapshot, error) {
	var (
		snapshot                     workflowruntime.RunSnapshot
		status, createdAt, updatedAt string
		inputsJSON, outputsJSON      sql.NullString
		generation                   int64
	)
	if err := row.Scan(
		&snapshot.ID, &snapshot.Plan.ID, &snapshot.Plan.Version, &snapshot.Plan.Digest,
		&snapshot.Plan.SchemaVersion, &status, &inputsJSON, &outputsJSON,
		&generation, &createdAt, &updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.RunSnapshot{}, fmt.Errorf("%w: run %q", workflowruntime.ErrNotFound, snapshot.ID)
		}
		return workflowruntime.RunSnapshot{}, fmt.Errorf("load workflow run: %w", err)
	}
	snapshot.Status = workflowruntime.RunStatus(status)
	var err error
	if snapshot.Inputs, err = decodeOptionalWorkflowJSON[values.ValueSetRef]("run inputs", inputsJSON); err != nil {
		return workflowruntime.RunSnapshot{}, err
	}
	if snapshot.Outputs, err = decodeOptionalWorkflowJSON[values.ValueSetRef]("run outputs", outputsJSON); err != nil {
		return workflowruntime.RunSnapshot{}, err
	}
	if snapshot.Generation, err = workflowGeneration("run generation", generation); err != nil {
		return workflowruntime.RunSnapshot{}, err
	}
	if snapshot.CreatedAt, err = parseWorkflowTime("run created_at", createdAt); err != nil {
		return workflowruntime.RunSnapshot{}, err
	}
	if snapshot.UpdatedAt, err = parseWorkflowTime("run updated_at", updatedAt); err != nil {
		return workflowruntime.RunSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.RunSnapshot{}, workflowInvalid(err)
	}
	return snapshot, nil
}

func loadWorkflowNode(ctx context.Context, query workflowSQL, id workflowruntime.NodeInvocationID) (workflowruntime.NodeInvocationSnapshot, error) {
	return scanWorkflowNode(query.QueryRowContext(ctx, workflowNodeSelect+`
WHERE n.run_id = ? AND n.node_id = ? AND n.iteration = ?`, id.RunID, id.NodeID, id.Iteration))
}

func scanWorkflowNode(row workflowScanner) (workflowruntime.NodeInvocationSnapshot, error) {
	var (
		snapshot                                     workflowruntime.NodeInvocationSnapshot
		status, createdAt, updatedAt                 string
		blockedJSON, inputsJSON, outputsJSON, waitID sql.NullString
		generation, claimGeneration                  int64
		leaseOwner, leaseToken, leaseExpiry          sql.NullString
		leaseGeneration                              sql.NullInt64
	)
	if err := row.Scan(
		&snapshot.ID.RunID, &snapshot.ID.NodeID, &snapshot.ID.Iteration,
		&status, &blockedJSON, &inputsJSON, &outputsJSON, &waitID,
		&snapshot.LatestAttempt, &snapshot.Priority, &claimGeneration, &generation,
		&createdAt, &updatedAt, &leaseOwner, &leaseToken, &leaseGeneration, &leaseExpiry,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.NodeInvocationSnapshot{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
		}
		return workflowruntime.NodeInvocationSnapshot{}, fmt.Errorf("load workflow node: %w", err)
	}
	snapshot.Status = workflowruntime.NodeStatus(status)
	var err error
	if snapshot.Blocked, err = decodeOptionalWorkflowJSON[workflowruntime.BlockedReason]("blocked reason", blockedJSON); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	if snapshot.Inputs, err = decodeOptionalWorkflowJSON[values.ValueSetRef]("node inputs", inputsJSON); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	if snapshot.Outputs, err = decodeOptionalWorkflowJSON[values.ValueSetRef]("node outputs", outputsJSON); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	if waitID.Valid {
		snapshot.Wait = &workflowruntime.WaitRef{ID: workflowruntime.WaitID(waitID.String)}
	}
	if snapshot.ClaimGeneration, err = workflowGeneration("node claim generation", claimGeneration); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	if snapshot.Generation, err = workflowGeneration("node generation", generation); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	if snapshot.CreatedAt, err = parseWorkflowTime("node created_at", createdAt); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	if snapshot.UpdatedAt, err = parseWorkflowTime("node updated_at", updatedAt); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	if leaseOwner.Valid || leaseToken.Valid || leaseGeneration.Valid || leaseExpiry.Valid {
		if !leaseOwner.Valid || !leaseToken.Valid || !leaseGeneration.Valid || !leaseExpiry.Valid {
			return workflowruntime.NodeInvocationSnapshot{}, workflowInvalid(errors.New("persisted node lease is incomplete"))
		}
		leaseGenerationValue, generationErr := workflowGeneration("lease generation", leaseGeneration.Int64)
		if generationErr != nil {
			return workflowruntime.NodeInvocationSnapshot{}, generationErr
		}
		leaseExpiryValue, expiryErr := parseWorkflowTime("lease expires_at", leaseExpiry.String)
		if expiryErr != nil {
			return workflowruntime.NodeInvocationSnapshot{}, expiryErr
		}
		snapshot.Lease = &workflowruntime.ClaimLease{
			Owner: leaseOwner.String, Token: leaseToken.String,
			Generation: leaseGenerationValue, ExpiresAt: leaseExpiryValue,
		}
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, workflowInvalid(err)
	}
	return snapshot, nil
}

func loadWorkflowAttempt(ctx context.Context, query workflowSQL, id workflowruntime.AttemptID) (workflowruntime.AttemptSnapshot, error) {
	return scanWorkflowAttempt(query.QueryRowContext(ctx, workflowAttemptSelect+`
WHERE run_id = ? AND node_id = ? AND iteration = ? AND attempt_number = ?`,
		id.Invocation.RunID, id.Invocation.NodeID, id.Invocation.Iteration, id.Number))
}

func scanWorkflowAttempt(row workflowScanner) (workflowruntime.AttemptSnapshot, error) {
	var (
		snapshot                             workflowruntime.AttemptSnapshot
		status, executorJSON                 string
		inputsJSON, outputsJSON, failureJSON sql.NullString
		startedAt, createdAt, updatedAt      string
		finishedAt                           sql.NullString
		generation                           int64
	)
	if err := row.Scan(
		&snapshot.ID.Invocation.RunID, &snapshot.ID.Invocation.NodeID,
		&snapshot.ID.Invocation.Iteration, &snapshot.ID.Number, &status,
		&executorJSON, &inputsJSON, &outputsJSON, &failureJSON,
		&startedAt, &finishedAt, &generation, &createdAt, &updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.AttemptSnapshot{}, fmt.Errorf("%w: attempt", workflowruntime.ErrNotFound)
		}
		return workflowruntime.AttemptSnapshot{}, fmt.Errorf("load workflow attempt: %w", err)
	}
	snapshot.Status = workflowruntime.NodeStatus(status)
	if err := decodeWorkflowJSON("attempt executor", executorJSON, &snapshot.Executor); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	var err error
	if snapshot.Inputs, err = decodeOptionalWorkflowJSON[values.ValueSetRef]("attempt inputs", inputsJSON); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	if snapshot.Outputs, err = decodeOptionalWorkflowJSON[values.ValueSetRef]("attempt outputs", outputsJSON); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	if snapshot.Failure, err = decodeOptionalWorkflowJSON[workflowruntime.Failure]("attempt failure", failureJSON); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	if snapshot.StartedAt, err = parseWorkflowTime("attempt started_at", startedAt); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	if snapshot.FinishedAt, err = parseOptionalWorkflowTime("attempt finished_at", finishedAt); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	if snapshot.Generation, err = workflowGeneration("attempt generation", generation); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	if snapshot.CreatedAt, err = parseWorkflowTime("attempt created_at", createdAt); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	if snapshot.UpdatedAt, err = parseWorkflowTime("attempt updated_at", updatedAt); err != nil {
		return workflowruntime.AttemptSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.AttemptSnapshot{}, workflowInvalid(err)
	}
	return snapshot, nil
}

func loadWorkflowWait(ctx context.Context, query workflowSQL, id workflowruntime.WaitID) (workflowruntime.WaitSnapshot, error) {
	return scanWorkflowWait(query.QueryRowContext(ctx, workflowWaitSelect+` WHERE wait_id = ?`, id))
}

func scanWorkflowWait(row workflowScanner) (workflowruntime.WaitSnapshot, error) {
	var (
		snapshot                                     workflowruntime.WaitSnapshot
		status, createdAt, updatedAt                 string
		resumeJSON, resolvedAt, recordJSON, deadline sql.NullString
		generation                                   int64
	)
	if err := row.Scan(
		&snapshot.Ref.ID, &snapshot.Invocation.RunID, &snapshot.Invocation.NodeID,
		&snapshot.Invocation.Iteration, &status, &resumeJSON, &generation,
		&createdAt, &updatedAt, &resolvedAt, &recordJSON, &deadline,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.WaitSnapshot{}, fmt.Errorf("%w: wait %q", workflowruntime.ErrNotFound, snapshot.Ref.ID)
		}
		return workflowruntime.WaitSnapshot{}, fmt.Errorf("load workflow wait: %w", err)
	}
	if !recordJSON.Valid {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(errors.New("wait semantic record is missing"))
	}
	if err := decodeWorkflowJSON("wait record", recordJSON.String, &snapshot.Record); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if snapshot.Status != workflowruntime.WaitStatus(status) {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(errors.New("wait status column diverges from semantic record"))
	}
	var err error
	resumeMirror, err := decodeOptionalWorkflowJSON[values.ValueSetRef]("wait resume values", resumeJSON)
	if err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if !equalWorkflowValueRef(snapshot.ResumeValues, resumeMirror) {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(errors.New("wait resume-values column diverges from semantic record"))
	}
	deadlineMirror, err := parseOptionalWorkflowTime("wait deadline", deadline)
	if err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if !snapshot.Deadline.Equal(deadlineMirror) {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(errors.New("wait deadline column diverges from semantic record"))
	}
	if snapshot.Generation, err = workflowGeneration("wait generation", generation); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if snapshot.CreatedAt, err = parseWorkflowTime("wait created_at", createdAt); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if snapshot.UpdatedAt, err = parseWorkflowTime("wait updated_at", updatedAt); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if snapshot.ResolvedAt, err = parseOptionalWorkflowTime("wait resolved_at", resolvedAt); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(err)
	}
	return snapshot, nil
}

func scanWorkflowEvent(row workflowScanner) (workflowruntime.Event, error) {
	var (
		event                                                   workflowruntime.Event
		sequence                                                int64
		invocationJSON, attemptJSON, attributesJSON, valuesJSON sql.NullString
		occurredAt, redaction, retention                        string
	)
	if err := row.Scan(
		&event.RunID, &sequence, &invocationJSON, &attemptJSON, &event.Type,
		&occurredAt, &attributesJSON, &valuesJSON, &redaction, &retention,
	); err != nil {
		return workflowruntime.Event{}, err
	}
	var err error
	if event.Sequence, err = workflowGeneration("event sequence", sequence); err != nil {
		return workflowruntime.Event{}, err
	}
	if event.Invocation, err = decodeOptionalWorkflowJSON[workflowruntime.NodeInvocationID]("event invocation", invocationJSON); err != nil {
		return workflowruntime.Event{}, err
	}
	if event.Attempt, err = decodeOptionalWorkflowJSON[workflowruntime.AttemptID]("event attempt", attemptJSON); err != nil {
		return workflowruntime.Event{}, err
	}
	if event.OccurredAt, err = parseWorkflowTime("event occurred_at", occurredAt); err != nil {
		return workflowruntime.Event{}, err
	}
	if attributesJSON.Valid {
		if decodeErr := decodeWorkflowJSON("event attributes", attributesJSON.String, &event.Attributes); decodeErr != nil {
			return workflowruntime.Event{}, decodeErr
		}
	}
	if event.Values, err = decodeOptionalWorkflowJSON[values.ValueSetRef]("event values", valuesJSON); err != nil {
		return workflowruntime.Event{}, err
	}
	event.Redaction = values.RedactionClass(redaction)
	event.Retention = values.RetentionClass(retention)
	if err := event.Validate(); err != nil {
		return workflowruntime.Event{}, workflowInvalid(err)
	}
	return event, nil
}
