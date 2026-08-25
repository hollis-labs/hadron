package persistence

import (
	"context"
	"errors"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var _ workflowruntime.ChildTerminalWaitStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) RecoverChildTerminalWaits(ctx context.Context, limit int) ([]workflowruntime.ChildTerminalWait, error) {
	if limit < 0 || limit > workflowruntime.MaximumRunQueryLimit {
		return nil, workflowInvalid(errors.New("child terminal recovery limit is invalid"))
	}
	statement := `SELECT l.link_json,w.wait_id,COUNT(*) OVER (PARTITION BY l.parent_run_id,l.node_id,l.iteration)
FROM workflow_child_runs l
JOIN workflow_runs c ON c.run_id=l.child_run_id
JOIN workflow_waits w ON w.run_id=l.parent_run_id AND w.node_id=l.node_id AND w.iteration=l.iteration
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
	seen := make(map[workflowruntime.NodeInvocationID]workflowruntime.WaitID)
	for rows.Next() {
		var encoded, waitID string
		var candidateCount int
		if err := rows.Scan(&encoded, &waitID, &candidateCount); err != nil {
			closeRows(rows)
			return nil, err
		}
		if candidateCount != 1 {
			closeRows(rows)
			return nil, workflowInvalid(errors.New("child terminal recovery found ambiguous open waits for one child invocation"))
		}
		var link workflowruntime.ChildRunLink
		if err := decodeWorkflowJSON("child terminal link", encoded, &link); err != nil {
			closeRows(rows)
			return nil, err
		}
		id := workflowruntime.WaitID(waitID)
		if prior, duplicate := seen[link.Invocation]; duplicate && prior != id {
			closeRows(rows)
			return nil, workflowInvalid(errors.New("child terminal recovery found ambiguous open waits for one child invocation"))
		}
		seen[link.Invocation] = id
		identities = append(identities, identity{link: link, waitID: id})
	}
	if err := rows.Err(); err != nil {
		closeRows(rows)
		return nil, err
	}
	closeRows(rows)

	result := make([]workflowruntime.ChildTerminalWait, 0, len(identities))
	for _, item := range identities {
		child, err := s.LoadRun(ctx, item.link.ChildRunID)
		if err != nil {
			return nil, err
		}
		wait, err := s.LoadWait(ctx, item.waitID)
		if err != nil {
			return nil, err
		}
		// Querying and loading are intentionally separate bounded reads. A
		// concurrent canonical resume can close the wait between them; skip that
		// now-ineligible candidate instead of turning successful progress into a
		// recovery failure.
		if wait.Status != workflowruntime.WaitOpen {
			continue
		}
		candidate := workflowruntime.ChildTerminalWait{Link: item.link, Child: child, Wait: wait}
		if err := candidate.Validate(); err != nil {
			return nil, workflowInvalid(err)
		}
		result = append(result, candidate)
	}
	return result, nil
}
