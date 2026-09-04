package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

// LoadWait implements runtime.StateStore.
func (s *WorkflowStateStore) LoadWait(ctx context.Context, id workflowruntime.WaitID) (workflowruntime.WaitSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	return loadWorkflowWait(ctx, s.db, id)
}

// LoadWaitContinuation returns the single resumed wait bound to exactly id.
// Open, timed-out, canceled, and safely-unbound legacy rows never qualify.
func (s *WorkflowStateStore) LoadWaitContinuation(ctx context.Context, id workflowruntime.AttemptID) (workflowruntime.WaitSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if err := id.Validate(); err != nil {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(err)
	}
	var waitID workflowruntime.WaitID
	if err := s.db.QueryRowContext(ctx, `
SELECT b.wait_id
FROM workflow_wait_attempt_bindings b
JOIN workflow_waits w ON w.wait_id = b.wait_id
WHERE b.run_id = ? AND b.node_id = ? AND b.iteration = ?
  AND b.attempt_number = ? AND w.status = ?`,
		id.Invocation.RunID, id.Invocation.NodeID, id.Invocation.Iteration,
		id.Number, workflowruntime.WaitResumed,
	).Scan(&waitID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.WaitSnapshot{}, fmt.Errorf("%w: resumed wait continuation", workflowruntime.ErrNotFound)
		}
		return workflowruntime.WaitSnapshot{}, fmt.Errorf("load workflow wait continuation binding: %w", err)
	}
	return loadWorkflowWait(ctx, s.db, waitID)
}
