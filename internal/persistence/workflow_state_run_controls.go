package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var _ workflowruntime.RunControlStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) QueryRunState(ctx context.Context, request workflowruntime.RunStateQuery) (workflowruntime.RunStateView, error) {
	if err := request.Validate(); err != nil {
		return workflowruntime.RunStateView{}, workflowInvalid(err)
	}
	run, err := s.LoadRun(ctx, request.RunID)
	if err != nil {
		return workflowruntime.RunStateView{}, err
	}
	view := workflowruntime.RunStateView{Run: run}
	rows, err := s.db.QueryContext(ctx, workflowNodeSelect+` WHERE n.run_id = ? ORDER BY n.node_id, n.iteration LIMIT ?`, request.RunID, request.Limit)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		node, scanErr := scanWorkflowNode(rows)
		if scanErr != nil {
			closeRows(rows)
			return view, scanErr
		}
		view.Nodes = append(view.Nodes, node)
	}
	if err = rows.Err(); err != nil {
		closeRows(rows)
		return view, err
	}
	closeRows(rows)

	rows, err = s.db.QueryContext(ctx, workflowAttemptSelect+` WHERE run_id = ? ORDER BY node_id, iteration, attempt_number LIMIT ?`, request.RunID, request.Limit)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		attempt, scanErr := scanWorkflowAttempt(rows)
		if scanErr != nil {
			closeRows(rows)
			return view, scanErr
		}
		view.Attempts = append(view.Attempts, attempt)
	}
	if err = rows.Err(); err != nil {
		closeRows(rows)
		return view, err
	}
	closeRows(rows)

	rows, err = s.db.QueryContext(ctx, workflowWaitSelect+` WHERE run_id = ? ORDER BY created_at, wait_id LIMIT ?`, request.RunID, request.Limit)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		wait, scanErr := scanWorkflowWait(rows)
		if scanErr != nil {
			closeRows(rows)
			return view, scanErr
		}
		view.Waits = append(view.Waits, wait)
	}
	if err = rows.Err(); err != nil {
		closeRows(rows)
		return view, err
	}
	closeRows(rows)

	view.Events, err = s.ListEvents(ctx, workflowruntime.EventQuery(request))
	return view, err
}

func (s *WorkflowStateStore) FindOpenSignalWait(ctx context.Context, selector workflowruntime.SignalSelector) (workflowruntime.WaitSnapshot, error) {
	if err := selector.Validate(); err != nil {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(err)
	}
	rows, err := s.db.QueryContext(ctx, workflowWaitSelect+`
 WHERE run_id = ? AND status = 'open'
   AND json_extract(record_json, '$.kind') = ?
   AND json_extract(record_json, '$.signal_name') = ?
   AND json_extract(record_json, '$.correlation') = ?
 ORDER BY wait_id LIMIT 2`, selector.RunID, workflowwait.KindSignal, selector.Name, selector.Correlation)
	if err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	defer closeRows(rows)
	var matches []workflowruntime.WaitSnapshot
	for rows.Next() {
		match, scanErr := scanWorkflowWait(rows)
		if scanErr != nil {
			return workflowruntime.WaitSnapshot{}, scanErr
		}
		matches = append(matches, match)
	}
	if err := rows.Err(); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	switch len(matches) {
	case 0:
		return workflowruntime.WaitSnapshot{}, workflowruntime.ErrSignalNotOpen
	case 1:
		return matches[0], nil
	default:
		return workflowruntime.WaitSnapshot{}, workflowruntime.ErrSignalAmbiguous
	}
}

func (s *WorkflowStateStore) FindSignalWait(ctx context.Context, selector workflowruntime.SignalSelector, idempotencyKey string) (workflowruntime.WaitSnapshot, error) {
	if err := selector.Validate(); err != nil || idempotencyKey == "" {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(errors.New("named signal replay lookup is malformed"))
	}
	rows, err := s.db.QueryContext(ctx, workflowWaitSelect+`
 WHERE run_id = ?
   AND json_extract(record_json, '$.kind') = ?
   AND json_extract(record_json, '$.signal_name') = ?
   AND json_extract(record_json, '$.correlation') = ?
   AND (status = 'open' OR json_extract(record_json, '$.resolution.idempotency_key') = ?)
 ORDER BY CASE WHEN json_extract(record_json, '$.resolution.idempotency_key') = ? THEN 0 ELSE 1 END, wait_id LIMIT 2`, selector.RunID, workflowwait.KindSignal, selector.Name, selector.Correlation, idempotencyKey, idempotencyKey)
	if err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	defer closeRows(rows)
	var matches []workflowruntime.WaitSnapshot
	for rows.Next() {
		match, scanErr := scanWorkflowWait(rows)
		if scanErr != nil {
			return workflowruntime.WaitSnapshot{}, scanErr
		}
		matches = append(matches, match)
	}
	if err := rows.Err(); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	if len(matches) == 0 {
		return workflowruntime.WaitSnapshot{}, workflowruntime.ErrSignalNotOpen
	}
	if len(matches) > 1 {
		firstReplay := matches[0].Resolution != nil && matches[0].Resolution.IdempotencyKey == idempotencyKey
		secondReplay := matches[1].Resolution != nil && matches[1].Resolution.IdempotencyKey == idempotencyKey
		if firstReplay == secondReplay {
			return workflowruntime.WaitSnapshot{}, workflowruntime.ErrSignalAmbiguous
		}
	}
	return matches[0], nil
}

func (s *WorkflowStateStore) BeginRunUpdate(ctx context.Context, request workflowruntime.BeginRunUpdateRequest) (workflowruntime.RunUpdateSnapshot, workflowruntime.IdempotencyOutcome, error) {
	request.ReceivedAt = request.ReceivedAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.RunUpdateSnapshot{}, "", workflowInvalid(err)
	}
	requestJSON, err := encodeWorkflowJSON(request)
	if err != nil {
		return workflowruntime.RunUpdateSnapshot{}, "", err
	}
	var result workflowruntime.RunUpdateSnapshot
	outcome := workflowruntime.IdempotencyApplied
	err = s.write(ctx, "begin workflow run update", func(query workflowSQL) error {
		prior, loadErr := loadWorkflowRunUpdate(ctx, query, request.IdempotencyKey)
		if loadErr == nil {
			priorJSON, encodeErr := encodeWorkflowJSON(prior.Request)
			if encodeErr != nil {
				return encodeErr
			}
			if priorJSON != requestJSON {
				return workflowIdempotencyConflict("run update", request.IdempotencyKey)
			}
			result, outcome = prior, workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		wait, waitErr := loadWorkflowWait(ctx, query, request.WaitID)
		if waitErr != nil {
			return waitErr
		}
		if wait.Invocation.RunID != request.Selector.RunID || wait.SignalName != request.Selector.Name || wait.Correlation != request.Selector.Correlation ||
			wait.Status != workflowruntime.WaitOpen || request.ReceivedAt.Before(wait.UpdatedAt) {
			return workflowInvalid(errors.New("run update target is not the resolved open named signal"))
		}
		result = workflowruntime.RunUpdateSnapshot{Request: request, Status: workflowruntime.RunUpdatePending, Generation: 1, CreatedAt: request.ReceivedAt, UpdatedAt: request.ReceivedAt}
		if validationErr := result.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		_, execErr := query.ExecContext(ctx, `INSERT INTO workflow_run_updates(idempotency_key,run_id,signal_name,correlation,wait_id,status,generation,request_json,created_at,updated_at) VALUES (?,?,?,?,?,'pending',1,?,?,?)`, request.IdempotencyKey, request.Selector.RunID, request.Selector.Name, request.Selector.Correlation, request.WaitID, requestJSON, workflowTime(request.ReceivedAt), workflowTime(request.ReceivedAt))
		return execErr
	})
	return result, outcome, err
}

func (s *WorkflowStateStore) CompleteRunUpdate(ctx context.Context, request workflowruntime.CompleteRunUpdateRequest) (workflowruntime.RunUpdateSnapshot, error) {
	request.At = request.At.UTC()
	if request.IdempotencyKey == "" || request.ExpectedGeneration == 0 || (request.Status != workflowruntime.RunUpdateApplied && request.Status != workflowruntime.RunUpdateClosed) || request.At.IsZero() {
		return workflowruntime.RunUpdateSnapshot{}, workflowInvalid(errors.New("run update completion is malformed"))
	}
	var result workflowruntime.RunUpdateSnapshot
	err := s.write(ctx, "complete workflow run update", func(query workflowSQL) error {
		current, err := loadWorkflowRunUpdate(ctx, query, request.IdempotencyKey)
		if err != nil {
			return err
		}
		if current.Status != workflowruntime.RunUpdatePending {
			if current.Status == request.Status && current.Receipt != nil && *current.Receipt == request.Receipt {
				result = current
				return nil
			}
			return workflowInvalid(errors.New("run update is already terminal"))
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("run update", request.ExpectedGeneration, current.Generation)
		}
		if request.At.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("run update completion time regresses state"))
		}
		result = current
		result.Status, result.Receipt = request.Status, &request.Receipt
		result.Generation++
		result.UpdatedAt = request.At
		resultJSON, encodeErr := encodeWorkflowJSON(request.Receipt)
		if encodeErr != nil {
			return encodeErr
		}
		updated, execErr := query.ExecContext(ctx, `UPDATE workflow_run_updates SET status=?,generation=?,result_json=?,updated_at=? WHERE idempotency_key=? AND generation=?`, result.Status, result.Generation, resultJSON, workflowTime(result.UpdatedAt), request.IdempotencyKey, current.Generation)
		if execErr != nil {
			return execErr
		}
		count, countErr := updated.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if count != 1 {
			return workflowruntime.ErrCASMismatch
		}
		return result.Validate()
	})
	return result, err
}

func (s *WorkflowStateStore) LoadRunUpdate(ctx context.Context, key string) (workflowruntime.RunUpdateSnapshot, error) {
	return loadWorkflowRunUpdate(ctx, s.db, key)
}

func (s *WorkflowStateStore) RecoverRunUpdates(ctx context.Context, limit int) ([]workflowruntime.RunUpdateSnapshot, error) {
	if limit < 0 || limit > workflowruntime.MaximumRunQueryLimit {
		return nil, workflowInvalid(errors.New("run update recovery limit is invalid"))
	}
	statement := `SELECT request_json,status,generation,result_json,created_at,updated_at FROM workflow_run_updates WHERE status='pending' ORDER BY updated_at,idempotency_key`
	arguments := []any{}
	if limit > 0 {
		statement += ` LIMIT ?`
		arguments = append(arguments, limit)
	}
	rows, err := s.db.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result []workflowruntime.RunUpdateSnapshot
	for rows.Next() {
		item, scanErr := scanWorkflowRunUpdate(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func loadWorkflowRunUpdate(ctx context.Context, query workflowSQL, key string) (workflowruntime.RunUpdateSnapshot, error) {
	return scanWorkflowRunUpdate(query.QueryRowContext(ctx, `SELECT request_json,status,generation,result_json,created_at,updated_at FROM workflow_run_updates WHERE idempotency_key=?`, key))
}

func scanWorkflowRunUpdate(row workflowScanner) (workflowruntime.RunUpdateSnapshot, error) {
	var result workflowruntime.RunUpdateSnapshot
	var requestJSON, status, createdAt, updatedAt string
	var resultJSON sql.NullString
	var generation int64
	if err := row.Scan(&requestJSON, &status, &generation, &resultJSON, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, fmt.Errorf("%w: workflow run update", workflowruntime.ErrNotFound)
		}
		return result, err
	}
	if err := decodeWorkflowJSON("workflow run update request", requestJSON, &result.Request); err != nil {
		return result, err
	}
	result.Status = workflowruntime.RunUpdateStatus(status)
	if resultJSON.Valid {
		var receipt workflowruntime.RunUpdateReceipt
		if err := decodeWorkflowJSON("workflow run update result", resultJSON.String, &receipt); err != nil {
			return result, err
		}
		result.Receipt = &receipt
	}
	var err error
	if result.Generation, err = workflowGeneration("run update generation", generation); err != nil {
		return result, err
	}
	if result.CreatedAt, err = parseWorkflowTime("run update created_at", createdAt); err != nil {
		return result, err
	}
	if result.UpdatedAt, err = parseWorkflowTime("run update updated_at", updatedAt); err != nil {
		return result, err
	}
	return result, result.Validate()
}
