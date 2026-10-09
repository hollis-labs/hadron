package persistence

import (
	"context"
	"errors"

	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
	workflowwait "github.com/hollis-labs/libs/workflow/wait"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

var _ hoststate.ChildRunWaitStore = (*WorkflowStateStore)(nil)

// RecoverChildRunWaits returns every open child_run wait in a parent run whose
// correlation names one of that run's terminal child runs, regardless of which
// parent node opened the wait. Unlike RecoverChildTerminalWaits it does not
// require the wait to live on the call node itself.
func (s *WorkflowStateStore) RecoverChildRunWaits(ctx context.Context, limit int) ([]hoststate.ChildRunWait, error) {
	if limit < 0 || limit > workflowruntime.MaximumRunQueryLimit {
		return nil, workflowInvalid(errors.New("child run wait recovery limit is invalid"))
	}
	statement := `SELECT l.link_json,w.wait_id,COUNT(*) OVER (PARTITION BY l.child_run_id,w.run_id,w.node_id,w.iteration)
FROM workflow_child_runs l
JOIN workflow_runs c ON c.run_id=l.child_run_id
JOIN workflow_waits w ON w.run_id=l.parent_run_id
WHERE c.status IN ('succeeded','failed','canceled','timed_out','crashed')
  AND w.status='open'
  AND json_extract(w.record_json,'$.kind')=?
  AND json_extract(w.record_json,'$.wake_source')=?
  AND json_extract(w.record_json,'$.correlation')=l.child_run_id
ORDER BY c.updated_at,l.child_run_id,w.created_at,w.wait_id`
	args := []any{workflowwait.KindChildRun, workflowwait.WakeChildRun}
	if limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	type identity struct {
		link   workflowruntime.ChildRunLink
		waitID workflowruntime.WaitID
	}
	identities := make([]identity, 0)
	for rows.Next() {
		var encoded, waitID string
		var candidateCount int
		if err := rows.Scan(&encoded, &waitID, &candidateCount); err != nil {
			closeRows(rows)
			return nil, err
		}
		// One node invocation holds at most one open wait; two open waits on
		// the same parent invocation for one child are corrupt and must not be
		// resolved by picking either.
		if candidateCount != 1 {
			closeRows(rows)
			return nil, workflowInvalid(errors.New("child terminal recovery found ambiguous open waits for one child invocation"))
		}
		var link workflowruntime.ChildRunLink
		if err := decodeWorkflowJSON("child run wait link", encoded, &link); err != nil {
			closeRows(rows)
			return nil, err
		}
		identities = append(identities, identity{link: link, waitID: workflowruntime.WaitID(waitID)})
	}
	if err := rows.Err(); err != nil {
		closeRows(rows)
		return nil, err
	}
	closeRows(rows)

	result := make([]hoststate.ChildRunWait, 0, len(identities))
	for _, item := range identities {
		child, err := s.LoadRun(ctx, item.link.ChildRunID)
		if err != nil {
			return nil, err
		}
		wait, err := s.LoadWait(ctx, item.waitID)
		if err != nil {
			return nil, err
		}
		// A concurrent canonical resume can close the wait between the query
		// and this load; skip it rather than failing recovery.
		if wait.Status != workflowruntime.WaitOpen {
			continue
		}
		candidate := hoststate.ChildRunWait{Link: item.link, Child: child, Wait: wait}
		if err := candidate.Validate(); err != nil {
			return nil, workflowInvalid(err)
		}
		result = append(result, candidate)
	}
	return result, nil
}
