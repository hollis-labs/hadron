package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

var _ hoststate.FailureHookJournal = (*WorkflowHostStore)(nil)

func (s *WorkflowHostStore) ListUnhandledFailedRuns(ctx context.Context, limit int) ([]workflowruntime.RunID, error) {
	if limit < 0 || limit > workflowruntime.MaximumRunQueryLimit {
		return nil, workflowInvalid(errors.New("failure hook recovery limit is invalid"))
	}
	statement := `SELECT source_run_id FROM (
SELECT r.run_id AS source_run_id,r.updated_at AS failed_at FROM workflow_runs r
JOIN workflow_host_starts s ON s.run_id=r.run_id
LEFT JOIN workflow_failure_hooks h ON h.source_run_id=r.run_id
WHERE r.status='failed' AND h.source_run_id IS NULL
UNION ALL
SELECT n.run_id,json_extract(n.record_json,'$.completed_at') FROM workflow_non_durable_runs n
LEFT JOIN workflow_failure_hooks h ON h.source_run_id=n.run_id
WHERE json_extract(n.record_json,'$.run.status')='failed' AND h.source_run_id IS NULL
) ORDER BY failed_at,source_run_id`
	args := []any{}
	if limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.state.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result []workflowruntime.RunID
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, err
		}
		result = append(result, workflowruntime.RunID(runID))
	}
	return result, rows.Err()
}

func (s *WorkflowHostStore) BindFailureHook(ctx context.Context, request hoststate.BindFailureHookRequest) (hoststate.FailureHookSnapshot, workflowruntime.IdempotencyOutcome, error) {
	request.At = request.At.UTC()
	if request.SourceRunID == "" || request.HandlerRunID == "" || request.MaximumDepth < 1 || request.MaximumDepth > 16 || request.At.IsZero() {
		return hoststate.FailureHookSnapshot{}, "", workflowInvalid(errors.New("failure hook binding request is malformed"))
	}
	var result hoststate.FailureHookSnapshot
	outcome := workflowruntime.IdempotencyApplied
	err := s.state.write(ctx, "bind workflow failure hook", func(query workflowSQL) error {
		prior, loadErr := loadFailureHook(ctx, query, request.SourceRunID)
		if loadErr == nil {
			if prior.Binding.SourcePlan != request.SourcePlan || prior.Binding.HandlerRunID != request.HandlerRunID || !reflect.DeepEqual(prior.Binding.Handler, request.Handler) || !reflect.DeepEqual(prior.Binding.Identity, request.Identity) || prior.Binding.MaximumDepth != request.MaximumDepth {
				return workflowIdempotencyConflict("workflow failure hook", string(request.SourceRunID))
			}
			result, outcome = prior, workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		depth, maximumDepth := 0, request.MaximumDepth
		var parentDepth, parentMaximumDepth int
		parentErr := query.QueryRowContext(ctx, `SELECT depth,json_extract(request_json,'$.maximum_depth') FROM workflow_failure_hooks WHERE handler_run_id=?`, request.SourceRunID).Scan(&parentDepth, &parentMaximumDepth)
		if parentErr == nil {
			depth = parentDepth + 1
			maximumDepth = parentMaximumDepth
		} else if !errors.Is(parentErr, sql.ErrNoRows) {
			return parentErr
		}
		status := hoststate.FailureHookPending
		if depth >= maximumDepth {
			status = hoststate.FailureHookSuppressed
		}
		binding := hoststate.FailureHookBinding{SourceRunID: request.SourceRunID, SourcePlan: request.SourcePlan, HandlerRunID: request.HandlerRunID, Handler: request.Handler, Identity: request.Identity.Clone(), Depth: depth, MaximumDepth: maximumDepth, BoundAt: request.At}
		result = hoststate.FailureHookSnapshot{Binding: binding, Status: status, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
		if err := result.Validate(); err != nil {
			return workflowInvalid(err)
		}
		requestJSON, _ := encodeWorkflowJSON(binding)
		_, execErr := query.ExecContext(ctx, `INSERT INTO workflow_failure_hooks(source_run_id,handler_run_id,depth,status,generation,request_json,created_at,updated_at) VALUES (?,?,?,?,1,?,?,?)`, request.SourceRunID, request.HandlerRunID, depth, status, requestJSON, workflowTime(request.At), workflowTime(request.At))
		return execErr
	})
	return result, outcome, err
}

func (s *WorkflowHostStore) CompleteFailureHook(ctx context.Context, sourceRunID workflowruntime.RunID, expected uint64, status hoststate.FailureHookStatus, failure string, at time.Time) (hoststate.FailureHookSnapshot, error) {
	at = at.UTC()
	if status != hoststate.FailureHookStarted && status != hoststate.FailureHookFailed || status == hoststate.FailureHookFailed && failure == "" || status == hoststate.FailureHookStarted && failure != "" {
		return hoststate.FailureHookSnapshot{}, workflowInvalid(errors.New("failure hook completion is malformed"))
	}
	var result hoststate.FailureHookSnapshot
	err := s.state.write(ctx, "complete workflow failure hook", func(query workflowSQL) error {
		current, err := loadFailureHook(ctx, query, sourceRunID)
		if err != nil {
			return err
		}
		if current.Status == status && current.Error == failure {
			result = current
			return nil
		}
		if current.Status != hoststate.FailureHookPending && current.Status != hoststate.FailureHookStarting || current.Generation != expected || at.IsZero() || at.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("failure hook completion is stale"))
		}
		current.Status, current.Error, current.Generation, current.UpdatedAt = status, failure, current.Generation+1, at
		var resultJSON any
		if failure != "" {
			encoded, encodeErr := encodeWorkflowJSON(map[string]string{"error": failure})
			if encodeErr != nil {
				return encodeErr
			}
			resultJSON = encoded
		}
		updated, err := query.ExecContext(ctx, `UPDATE workflow_failure_hooks SET status=?,generation=?,result_json=?,updated_at=? WHERE source_run_id=? AND generation=?`, status, current.Generation, resultJSON, workflowTime(at), sourceRunID, expected)
		if err == nil {
			count, countErr := updated.RowsAffected()
			if countErr != nil {
				return countErr
			}
			if count != 1 {
				return workflowruntime.ErrCASMismatch
			}
		}
		result = current
		return err
	})
	return result, err
}

func (s *WorkflowHostStore) RecoverFailureHooks(ctx context.Context, limit int) ([]hoststate.FailureHookSnapshot, error) {
	if limit < 0 || limit > workflowruntime.MaximumRunQueryLimit {
		return nil, workflowInvalid(errors.New("failure hook recovery limit is invalid"))
	}
	statement := `SELECT request_json,status,generation,result_json,created_at,updated_at FROM workflow_failure_hooks WHERE status IN ('pending','starting') ORDER BY updated_at,source_run_id`
	args := []any{}
	if limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.state.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result []hoststate.FailureHookSnapshot
	for rows.Next() {
		item, err := scanFailureHook(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func loadFailureHook(ctx context.Context, query workflowSQL, source workflowruntime.RunID) (hoststate.FailureHookSnapshot, error) {
	return scanFailureHook(query.QueryRowContext(ctx, `SELECT request_json,status,generation,result_json,created_at,updated_at FROM workflow_failure_hooks WHERE source_run_id=?`, source))
}

func scanFailureHook(row workflowScanner) (hoststate.FailureHookSnapshot, error) {
	var result hoststate.FailureHookSnapshot
	var requestJSON, status, createdAt, updatedAt string
	var resultJSON sql.NullString
	var generation int64
	if err := row.Scan(&requestJSON, &status, &generation, &resultJSON, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, fmt.Errorf("%w: workflow failure hook", workflowruntime.ErrNotFound)
		}
		return result, err
	}
	if err := decodeWorkflowJSON("workflow failure hook", requestJSON, &result.Binding); err != nil {
		return result, err
	}
	if generation < 0 {
		return result, workflowInvalid(errors.New("workflow failure hook generation is invalid"))
	}
	result.Status, result.Generation = hoststate.FailureHookStatus(status), uint64(generation)
	if resultJSON.Valid {
		var payload map[string]string
		if err := decodeWorkflowJSON("workflow failure hook result", resultJSON.String, &payload); err != nil {
			return result, err
		}
		result.Error = payload["error"]
	}
	var err error
	if result.CreatedAt, err = parseWorkflowTime("failure hook created_at", createdAt); err != nil {
		return result, err
	}
	if result.UpdatedAt, err = parseWorkflowTime("failure hook updated_at", updatedAt); err != nil {
		return result, err
	}
	return result, result.Validate()
}
