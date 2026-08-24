package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

func ensureWorkflowPlan(ctx context.Context, query workflowSQL, plan workflowruntime.PlanRef) error {
	if err := plan.Validate(); err != nil {
		return workflowInvalid(err)
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_plan_refs(digest, plan_id, version, schema_version)
VALUES (?, ?, ?, ?)
ON CONFLICT(digest) DO NOTHING`, plan.Digest, plan.ID, plan.Version, plan.SchemaVersion); err != nil {
		return fmt.Errorf("record workflow plan: %w", err)
	}
	stored, err := loadWorkflowPlan(ctx, query, plan.Digest)
	if err != nil {
		return err
	}
	if stored != plan {
		return fmt.Errorf("%w: plan digest %q has different metadata", workflowruntime.ErrAlreadyExists, plan.Digest)
	}
	return nil
}

func loadWorkflowPlan(ctx context.Context, query workflowSQL, digest string) (workflowruntime.PlanRef, error) {
	var plan workflowruntime.PlanRef
	if err := query.QueryRowContext(ctx, `
SELECT plan_id, version, digest, schema_version
FROM workflow_plan_refs WHERE digest = ?`, digest).Scan(
		&plan.ID, &plan.Version, &plan.Digest, &plan.SchemaVersion,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.PlanRef{}, fmt.Errorf("%w: plan %q", workflowruntime.ErrNotFound, digest)
		}
		return workflowruntime.PlanRef{}, fmt.Errorf("load workflow plan: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return workflowruntime.PlanRef{}, workflowInvalid(err)
	}
	return plan, nil
}

func insertWorkflowRun(ctx context.Context, query workflowSQL, snapshot workflowruntime.RunSnapshot) error {
	inputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Inputs)
	if err != nil {
		return err
	}
	outputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Outputs)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("run generation", snapshot.Generation)
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_runs(
    run_id, plan_digest, status, inputs_ref_json, outputs_ref_json,
    generation, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.ID, snapshot.Plan.Digest, snapshot.Status, inputsJSON, outputsJSON,
		generation, workflowTime(snapshot.CreatedAt), workflowTime(snapshot.UpdatedAt),
	); err != nil {
		if isSQLiteConstraint(err) {
			return fmt.Errorf("%w: run %q", workflowruntime.ErrAlreadyExists, snapshot.ID)
		}
		return fmt.Errorf("insert workflow run: %w", err)
	}
	return nil
}

func updateWorkflowRunCAS(ctx context.Context, query workflowSQL, snapshot workflowruntime.RunSnapshot, expected uint64) error {
	inputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Inputs)
	if err != nil {
		return err
	}
	outputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Outputs)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("run generation", snapshot.Generation)
	if err != nil {
		return err
	}
	expectedGeneration, err := sqliteGeneration("expected run generation", expected)
	if err != nil {
		return err
	}
	result, err := query.ExecContext(ctx, `
UPDATE workflow_runs
SET status = ?, inputs_ref_json = ?, outputs_ref_json = ?, generation = ?, updated_at = ?
WHERE run_id = ? AND generation = ?`,
		snapshot.Status, inputsJSON, outputsJSON, generation, workflowTime(snapshot.UpdatedAt),
		snapshot.ID, expectedGeneration,
	)
	if err != nil {
		return fmt.Errorf("update workflow run: %w", err)
	}
	return expectOneWorkflowRow(result, "run", expected, snapshot.Generation-1)
}

func insertWorkflowNode(ctx context.Context, query workflowSQL, snapshot workflowruntime.NodeInvocationSnapshot) error {
	blockedJSON, err := encodeOptionalWorkflowJSON(snapshot.Blocked)
	if err != nil {
		return err
	}
	inputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Inputs)
	if err != nil {
		return err
	}
	outputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Outputs)
	if err != nil {
		return err
	}
	waitID := any(nil)
	if snapshot.Wait != nil {
		waitID = snapshot.Wait.ID
	}
	claimGeneration, err := sqliteGeneration("node claim generation", snapshot.ClaimGeneration)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("node generation", snapshot.Generation)
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_node_invocations(
    run_id, node_id, iteration, status, blocked_json, inputs_ref_json,
    outputs_ref_json, wait_id, latest_attempt, priority, claim_generation,
    generation, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.ID.RunID, snapshot.ID.NodeID, snapshot.ID.Iteration, snapshot.Status,
		blockedJSON, inputsJSON, outputsJSON, waitID, snapshot.LatestAttempt,
		snapshot.Priority, claimGeneration, generation,
		workflowTime(snapshot.CreatedAt), workflowTime(snapshot.UpdatedAt),
	); err != nil {
		if isSQLiteConstraint(err) {
			return fmt.Errorf("%w: node invocation", workflowruntime.ErrAlreadyExists)
		}
		return fmt.Errorf("insert workflow node: %w", err)
	}
	return replaceWorkflowLease(ctx, query, snapshot.ID, snapshot.Lease)
}

func updateWorkflowNodeCAS(ctx context.Context, query workflowSQL, snapshot workflowruntime.NodeInvocationSnapshot, expected uint64) error {
	blockedJSON, err := encodeOptionalWorkflowJSON(snapshot.Blocked)
	if err != nil {
		return err
	}
	inputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Inputs)
	if err != nil {
		return err
	}
	outputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Outputs)
	if err != nil {
		return err
	}
	waitID := any(nil)
	if snapshot.Wait != nil {
		waitID = snapshot.Wait.ID
	}
	claimGeneration, err := sqliteGeneration("node claim generation", snapshot.ClaimGeneration)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("node generation", snapshot.Generation)
	if err != nil {
		return err
	}
	expectedGeneration, err := sqliteGeneration("expected node generation", expected)
	if err != nil {
		return err
	}
	result, err := query.ExecContext(ctx, `
UPDATE workflow_node_invocations
SET status = ?, blocked_json = ?, inputs_ref_json = ?, outputs_ref_json = ?,
    wait_id = ?, latest_attempt = ?, priority = ?, claim_generation = ?,
    generation = ?, updated_at = ?
WHERE run_id = ? AND node_id = ? AND iteration = ? AND generation = ?`,
		snapshot.Status, blockedJSON, inputsJSON, outputsJSON, waitID,
		snapshot.LatestAttempt, snapshot.Priority, claimGeneration, generation,
		workflowTime(snapshot.UpdatedAt), snapshot.ID.RunID, snapshot.ID.NodeID,
		snapshot.ID.Iteration, expectedGeneration,
	)
	if err != nil {
		return fmt.Errorf("update workflow node: %w", err)
	}
	if err := expectOneWorkflowRow(result, "node invocation", expected, snapshot.Generation-1); err != nil {
		return err
	}
	return replaceWorkflowLease(ctx, query, snapshot.ID, snapshot.Lease)
}

func replaceWorkflowLease(ctx context.Context, query workflowSQL, id workflowruntime.NodeInvocationID, lease *workflowruntime.ClaimLease) error {
	if lease == nil {
		if _, err := query.ExecContext(ctx, `
DELETE FROM workflow_node_leases WHERE run_id = ? AND node_id = ? AND iteration = ?`,
			id.RunID, id.NodeID, id.Iteration,
		); err != nil {
			return fmt.Errorf("clear workflow node lease: %w", err)
		}
		return nil
	}
	generation, err := sqliteGeneration("lease generation", lease.Generation)
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_node_leases(
    run_id, node_id, iteration, owner, token, generation, expires_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(run_id, node_id, iteration) DO UPDATE SET
    owner = excluded.owner,
    token = excluded.token,
    generation = excluded.generation,
    expires_at = excluded.expires_at`,
		id.RunID, id.NodeID, id.Iteration, lease.Owner, lease.Token,
		generation, workflowTime(lease.ExpiresAt),
	); err != nil {
		return fmt.Errorf("store workflow node lease: %w", err)
	}
	return nil
}

func insertWorkflowAttempt(ctx context.Context, query workflowSQL, snapshot workflowruntime.AttemptSnapshot) error {
	executorJSON, err := encodeWorkflowJSON(snapshot.Executor)
	if err != nil {
		return err
	}
	inputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Inputs)
	if err != nil {
		return err
	}
	outputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Outputs)
	if err != nil {
		return err
	}
	failureJSON, err := encodeOptionalWorkflowJSON(snapshot.Failure)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("attempt generation", snapshot.Generation)
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_attempts(
    run_id, node_id, iteration, attempt_number, status, executor_json,
    inputs_ref_json, outputs_ref_json, failure_json, started_at, finished_at,
    generation, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.ID.Invocation.RunID, snapshot.ID.Invocation.NodeID,
		snapshot.ID.Invocation.Iteration, snapshot.ID.Number, snapshot.Status,
		executorJSON, inputsJSON, outputsJSON, failureJSON, workflowTime(snapshot.StartedAt),
		workflowOptionalTime(snapshot.FinishedAt), generation,
		workflowTime(snapshot.CreatedAt), workflowTime(snapshot.UpdatedAt),
	); err != nil {
		if isSQLiteConstraint(err) {
			return &workflowruntime.AttemptConflictError{
				Invocation: snapshot.ID.Invocation, Attempt: snapshot.ID.Number,
				Reason: "attempt already exists",
			}
		}
		return fmt.Errorf("insert workflow attempt: %w", err)
	}
	return nil
}

func updateWorkflowAttemptCAS(ctx context.Context, query workflowSQL, snapshot workflowruntime.AttemptSnapshot, expected uint64) error {
	executorJSON, err := encodeWorkflowJSON(snapshot.Executor)
	if err != nil {
		return err
	}
	inputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Inputs)
	if err != nil {
		return err
	}
	outputsJSON, err := encodeOptionalWorkflowJSON(snapshot.Outputs)
	if err != nil {
		return err
	}
	failureJSON, err := encodeOptionalWorkflowJSON(snapshot.Failure)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("attempt generation", snapshot.Generation)
	if err != nil {
		return err
	}
	expectedGeneration, err := sqliteGeneration("expected attempt generation", expected)
	if err != nil {
		return err
	}
	result, err := query.ExecContext(ctx, `
UPDATE workflow_attempts
SET status = ?, executor_json = ?, inputs_ref_json = ?, outputs_ref_json = ?,
    failure_json = ?, started_at = ?, finished_at = ?, generation = ?, updated_at = ?
WHERE run_id = ? AND node_id = ? AND iteration = ? AND attempt_number = ? AND generation = ?`,
		snapshot.Status, executorJSON, inputsJSON, outputsJSON, failureJSON,
		workflowTime(snapshot.StartedAt), workflowOptionalTime(snapshot.FinishedAt),
		generation, workflowTime(snapshot.UpdatedAt), snapshot.ID.Invocation.RunID,
		snapshot.ID.Invocation.NodeID, snapshot.ID.Invocation.Iteration,
		snapshot.ID.Number, expectedGeneration,
	)
	if err != nil {
		return fmt.Errorf("update workflow attempt: %w", err)
	}
	return expectOneWorkflowRow(result, "attempt", expected, snapshot.Generation-1)
}

func insertWorkflowWait(ctx context.Context, query workflowSQL, snapshot workflowruntime.WaitSnapshot) error {
	resumeJSON, err := encodeOptionalWorkflowJSON(snapshot.ResumeValues)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("wait generation", snapshot.Generation)
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_waits(
    wait_id, run_id, node_id, iteration, status, resume_values_ref_json,
    generation, created_at, updated_at, resolved_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.Ref.ID, snapshot.Invocation.RunID, snapshot.Invocation.NodeID,
		snapshot.Invocation.Iteration, snapshot.Status, resumeJSON, generation,
		workflowTime(snapshot.CreatedAt), workflowTime(snapshot.UpdatedAt),
		workflowOptionalTime(snapshot.ResolvedAt),
	); err != nil {
		if isSQLiteConstraint(err) {
			return fmt.Errorf("%w: wait %q", workflowruntime.ErrAlreadyExists, snapshot.Ref.ID)
		}
		return fmt.Errorf("insert workflow wait: %w", err)
	}
	return nil
}

func updateWorkflowWaitCAS(ctx context.Context, query workflowSQL, snapshot workflowruntime.WaitSnapshot, expected uint64) error {
	resumeJSON, err := encodeOptionalWorkflowJSON(snapshot.ResumeValues)
	if err != nil {
		return err
	}
	generation, err := sqliteGeneration("wait generation", snapshot.Generation)
	if err != nil {
		return err
	}
	expectedGeneration, err := sqliteGeneration("expected wait generation", expected)
	if err != nil {
		return err
	}
	result, err := query.ExecContext(ctx, `
UPDATE workflow_waits
SET status = ?, resume_values_ref_json = ?, generation = ?, updated_at = ?, resolved_at = ?
WHERE wait_id = ? AND generation = ?`,
		snapshot.Status, resumeJSON, generation, workflowTime(snapshot.UpdatedAt),
		workflowOptionalTime(snapshot.ResolvedAt), snapshot.Ref.ID, expectedGeneration,
	)
	if err != nil {
		return fmt.Errorf("update workflow wait: %w", err)
	}
	return expectOneWorkflowRow(result, "wait", expected, snapshot.Generation-1)
}
