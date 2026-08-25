package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

var _ workflowruntime.ReactorStore = (*WorkflowStateStore)(nil)

const workflowReactorSelect = `SELECT snapshot_json,reactor_id,registration_id,registration_generation,correlation,current_generation,current_run_id,continue_after_events,event_count,status,generation,created_at,updated_at FROM workflow_reactors`

func (s *WorkflowStateStore) BeginReactorDelivery(ctx context.Context, request workflowruntime.BeginReactorDeliveryRequest) (workflowruntime.ReactorSnapshot, workflowruntime.ReactorDeliverySnapshot, workflowruntime.IdempotencyOutcome, error) {
	request.At, request.Delivery.OccurredAt, request.Delivery.ReceivedAt = request.At.UTC(), request.Delivery.OccurredAt.UTC(), request.Delivery.ReceivedAt.UTC()
	if err := request.Identity.Validate(); err != nil || request.InitialRunID == "" || request.ContinueAfterEvents == 0 || request.ContinueAfterEvents > 1_000_000 || request.Delivery.ReactorID != request.Identity.ID || request.At.IsZero() {
		return workflowruntime.ReactorSnapshot{}, workflowruntime.ReactorDeliverySnapshot{}, "", workflowInvalid(errors.New("begin reactor delivery is malformed"))
	}
	if err := request.Delivery.Validate(); err != nil {
		return workflowruntime.ReactorSnapshot{}, workflowruntime.ReactorDeliverySnapshot{}, "", workflowInvalid(err)
	}
	deliveryJSON, err := encodeWorkflowJSON(request.Delivery)
	if err != nil {
		return workflowruntime.ReactorSnapshot{}, workflowruntime.ReactorDeliverySnapshot{}, "", err
	}
	var reactor workflowruntime.ReactorSnapshot
	var delivery workflowruntime.ReactorDeliverySnapshot
	outcome := workflowruntime.IdempotencyApplied
	err = s.write(ctx, "begin reactor delivery", func(query workflowSQL) error {
		createdReactor := false
		current, loadErr := loadWorkflowReactor(ctx, query, request.Identity.ID)
		if errors.Is(loadErr, workflowruntime.ErrNotFound) {
			createdReactor = true
			var tupleID string
			tupleErr := query.QueryRowContext(ctx, `SELECT reactor_id FROM workflow_reactors WHERE registration_id=? AND registration_generation=? AND correlation=?`, request.Identity.RegistrationID, request.Identity.RegistrationGeneration, request.Identity.Correlation).Scan(&tupleID)
			if tupleErr == nil && tupleID != request.Identity.ID {
				return workflowInvalid(errors.New("reactor identity is not derived from the registration tuple"))
			}
			if tupleErr != nil && !errors.Is(tupleErr, sql.ErrNoRows) {
				return tupleErr
			}
			current = workflowruntime.ReactorSnapshot{Identity: request.Identity, CurrentGeneration: 1, CurrentRunID: request.InitialRunID, ContinueAfterEvents: request.ContinueAfterEvents, Status: workflowruntime.ReactorStarting, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
			encoded, encodeErr := encodeWorkflowJSON(current)
			if encodeErr != nil {
				return encodeErr
			}
			if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_reactors(reactor_id,registration_id,registration_generation,correlation,current_generation,current_run_id,continue_after_events,event_count,status,generation,snapshot_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,1,?,?,?)`, current.Identity.ID, current.Identity.RegistrationID, current.Identity.RegistrationGeneration, current.Identity.Correlation, current.CurrentGeneration, current.CurrentRunID, current.ContinueAfterEvents, current.EventCount, current.Status, encoded, workflowTime(current.CreatedAt), workflowTime(current.UpdatedAt)); execErr != nil {
				return execErr
			}
			if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_reactor_generations(reactor_id,reactor_generation,run_id,plan_digest,provenance_digest,event_count,snapshot_json,created_at) VALUES (?,?,?,?,?,0,?,?)`, current.Identity.ID, 1, current.CurrentRunID, current.Identity.Plan.Digest, current.Identity.Provenance.Digest, encoded, workflowTime(current.CreatedAt)); execErr != nil {
				return execErr
			}
		} else if loadErr != nil {
			return loadErr
		}
		if !reflect.DeepEqual(current.Identity, request.Identity) || current.ContinueAfterEvents != request.ContinueAfterEvents || current.CurrentGeneration == 1 && current.CurrentRunID != request.InitialRunID {
			return workflowInvalid(errors.New("reactor registration, plan, provenance, or initial run differs"))
		}
		prior, priorErr := loadWorkflowReactorDelivery(ctx, query, request.Identity.ID, request.Delivery.IdempotencyKey)
		if priorErr == nil {
			priorIntent, requestedIntent := prior.Request, request.Delivery
			priorIntent.ReceivedAt, requestedIntent.ReceivedAt = time.Time{}, time.Time{}
			if !reflect.DeepEqual(priorIntent, requestedIntent) {
				return workflowIdempotencyConflict("reactor delivery", request.Delivery.IdempotencyKey)
			}
			reactor, delivery, outcome = current, prior, workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(priorErr, workflowruntime.ErrNotFound) {
			return priorErr
		}
		if current.Status == workflowruntime.ReactorClosed || current.Status == workflowruntime.ReactorFailed {
			return workflowInvalid(workflowruntime.ErrReactorTerminal)
		}
		delivery = workflowruntime.ReactorDeliverySnapshot{Request: request.Delivery, ReactorGeneration: current.CurrentGeneration, RunID: current.CurrentRunID,
			StartsGeneration: createdReactor, Status: workflowruntime.ReactorDeliveryPending, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
		if validationErr := delivery.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		_, execErr := query.ExecContext(ctx, `INSERT INTO workflow_reactor_deliveries(reactor_id,idempotency_key,reactor_generation,run_id,starts_generation,claimed_wait_id,status,generation,request_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,1,?,?,?)`, request.Identity.ID, request.Delivery.IdempotencyKey, delivery.ReactorGeneration, delivery.RunID, delivery.StartsGeneration, nil, delivery.Status, deliveryJSON, workflowTime(delivery.CreatedAt), workflowTime(delivery.UpdatedAt))
		reactor = current
		return execErr
	})
	return reactor, delivery, outcome, err
}

func (s *WorkflowStateStore) LoadReactor(ctx context.Context, id string) (workflowruntime.ReactorSnapshot, error) {
	return loadWorkflowReactor(ctx, s.db, id)
}

func (s *WorkflowStateStore) MarkReactorWaiting(ctx context.Context, id string, expected uint64, at time.Time) (workflowruntime.ReactorSnapshot, error) {
	at = at.UTC()
	var result workflowruntime.ReactorSnapshot
	err := s.write(ctx, "mark reactor waiting", func(query workflowSQL) error {
		current, err := loadWorkflowReactor(ctx, query, id)
		if err != nil {
			return err
		}
		if current.Status == workflowruntime.ReactorWaiting {
			result = current
			return nil
		}
		if current.Generation != expected {
			return workflowCAS("reactor", expected, current.Generation)
		}
		if current.Status != workflowruntime.ReactorStarting || at.IsZero() || at.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("reactor cannot become waiting"))
		}
		current.Status, current.Generation, current.UpdatedAt = workflowruntime.ReactorWaiting, current.Generation+1, at
		encoded, encodeErr := encodeWorkflowJSON(current)
		if encodeErr != nil {
			return encodeErr
		}
		updated, err := query.ExecContext(ctx, `UPDATE workflow_reactors SET status=?,generation=?,snapshot_json=?,updated_at=? WHERE reactor_id=? AND generation=? AND status='starting'`, current.Status, current.Generation, encoded, workflowTime(at), id, expected)
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

func (s *WorkflowStateStore) LoadReactorDelivery(ctx context.Context, reactorID, key string) (workflowruntime.ReactorDeliverySnapshot, error) {
	return loadWorkflowReactorDelivery(ctx, s.db, reactorID, key)
}

func (s *WorkflowStateStore) ClaimReactorDelivery(ctx context.Context, request workflowruntime.ClaimReactorDeliveryRequest) (workflowruntime.ReactorDeliverySnapshot, error) {
	request.At = request.At.UTC()
	if request.ReactorID == "" || request.IdempotencyKey == "" || request.ExpectedGeneration == 0 || request.At.IsZero() {
		return workflowruntime.ReactorDeliverySnapshot{}, workflowInvalid(errors.New("reactor delivery claim is malformed"))
	}
	var result workflowruntime.ReactorDeliverySnapshot
	err := s.write(ctx, "claim reactor delivery", func(query workflowSQL) error {
		reactor, err := loadWorkflowReactor(ctx, query, request.ReactorID)
		if err != nil {
			return err
		}
		current, err := loadWorkflowReactorDelivery(ctx, query, request.ReactorID, request.IdempotencyKey)
		if err != nil {
			return err
		}
		if current.Status != workflowruntime.ReactorDeliveryPending {
			result = current
			return nil
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("reactor delivery", request.ExpectedGeneration, current.Generation)
		}
		if reactor.Status == workflowruntime.ReactorRolling {
			return workflowruntime.ErrReactorRolling
		}
		if !current.StartsGeneration && reactor.EventCount >= reactor.ContinueAfterEvents {
			return workflowruntime.ErrReactorRolling
		}
		if reactor.Status != workflowruntime.ReactorWaiting || current.ReactorGeneration != reactor.CurrentGeneration || current.RunID != reactor.CurrentRunID || request.At.IsZero() || request.At.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("reactor delivery is not claimable"))
		}
		if current.StartsGeneration && request.WaitID != "" || !current.StartsGeneration && request.WaitID == "" {
			return workflowInvalid(errors.New("reactor delivery claim target is malformed"))
		}
		if request.WaitID != "" {
			if waitRefErr := (workflowruntime.WaitRef{ID: request.WaitID}).Validate(); waitRefErr != nil {
				return workflowInvalid(waitRefErr)
			}
		}
		var applying int
		if countQueryErr := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_reactor_deliveries WHERE reactor_id=? AND status='applying'`, request.ReactorID).Scan(&applying); countQueryErr != nil {
			return countQueryErr
		}
		if applying != 0 {
			return workflowruntime.ErrReactorBusy
		}
		current.Status, current.ClaimedWaitID, current.Generation, current.UpdatedAt = workflowruntime.ReactorDeliveryApplying, request.WaitID, current.Generation+1, request.At
		var claimedWait any
		if request.WaitID != "" {
			claimedWait = string(request.WaitID)
		}
		updated, err := query.ExecContext(ctx, `UPDATE workflow_reactor_deliveries SET status='applying',claimed_wait_id=?,generation=?,updated_at=? WHERE reactor_id=? AND idempotency_key=? AND generation=? AND status='pending' AND claimed_wait_id IS NULL`, claimedWait, current.Generation, workflowTime(current.UpdatedAt), request.ReactorID, request.IdempotencyKey, request.ExpectedGeneration)
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

func (s *WorkflowStateStore) ReleaseReactorDelivery(ctx context.Context, request workflowruntime.ReleaseReactorDeliveryRequest) (workflowruntime.ReactorDeliverySnapshot, error) {
	request.At = request.At.UTC()
	if request.ReactorID == "" || request.IdempotencyKey == "" || request.ExpectedGeneration == 0 || request.At.IsZero() {
		return workflowruntime.ReactorDeliverySnapshot{}, workflowInvalid(errors.New("reactor delivery release is malformed"))
	}
	var result workflowruntime.ReactorDeliverySnapshot
	err := s.write(ctx, "release reactor delivery", func(query workflowSQL) error {
		current, err := loadWorkflowReactorDelivery(ctx, query, request.ReactorID, request.IdempotencyKey)
		if err != nil {
			return err
		}
		if current.Status == workflowruntime.ReactorDeliveryPending {
			result = current
			return nil
		}
		if current.Status != workflowruntime.ReactorDeliveryApplying || current.Generation != request.ExpectedGeneration || current.StartsGeneration || request.At.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("reactor delivery release does not match an applying wait delivery"))
		}
		current.Status, current.ClaimedWaitID, current.Generation, current.UpdatedAt = workflowruntime.ReactorDeliveryPending, "", current.Generation+1, request.At
		updated, err := query.ExecContext(ctx, `UPDATE workflow_reactor_deliveries SET status='pending',claimed_wait_id=NULL,generation=?,updated_at=? WHERE reactor_id=? AND idempotency_key=? AND generation=? AND status='applying' AND claimed_wait_id IS NOT NULL`, current.Generation, workflowTime(current.UpdatedAt), request.ReactorID, request.IdempotencyKey, request.ExpectedGeneration)
		if err != nil {
			return err
		}
		count, countErr := updated.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if count != 1 {
			return workflowruntime.ErrCASMismatch
		}
		result = current
		return result.Validate()
	})
	return result, err
}

func (s *WorkflowStateStore) CompleteReactorDelivery(ctx context.Context, request workflowruntime.CompleteReactorDeliveryRequest) (workflowruntime.ReactorSnapshot, workflowruntime.ReactorDeliverySnapshot, error) {
	request.At = request.At.UTC()
	if request.Status != workflowruntime.ReactorDeliveryApplied {
		return workflowruntime.ReactorSnapshot{}, workflowruntime.ReactorDeliverySnapshot{}, workflowInvalid(errors.New("reactor delivery terminal status is invalid"))
	}
	if err := request.Receipt.Validate(); err != nil {
		return workflowruntime.ReactorSnapshot{}, workflowruntime.ReactorDeliverySnapshot{}, workflowInvalid(err)
	}
	var reactor workflowruntime.ReactorSnapshot
	var delivery workflowruntime.ReactorDeliverySnapshot
	err := s.write(ctx, "complete reactor delivery", func(query workflowSQL) error {
		current, err := loadWorkflowReactorDelivery(ctx, query, request.ReactorID, request.IdempotencyKey)
		if err != nil {
			return err
		}
		reactor, err = loadWorkflowReactor(ctx, query, request.ReactorID)
		if err != nil {
			return err
		}
		if current.Status == workflowruntime.ReactorDeliveryApplied || current.Status == workflowruntime.ReactorDeliveryClosed {
			if current.Status == request.Status && current.Receipt != nil && reflect.DeepEqual(*current.Receipt, request.Receipt) {
				delivery = current
				return nil
			}
			return workflowInvalid(errors.New("reactor delivery is already terminal"))
		}
		if current.Status != workflowruntime.ReactorDeliveryApplying || current.Generation != request.ExpectedGeneration || current.ReactorGeneration != reactor.CurrentGeneration || current.RunID != reactor.CurrentRunID || request.At.IsZero() || request.At.Before(current.UpdatedAt) || request.At.Before(reactor.UpdatedAt) {
			return workflowInvalid(errors.New("reactor delivery completion does not match current generation"))
		}
		if current.StartsGeneration != (request.Receipt.Kind == workflowruntime.ReactorDeliveryStartedRun) || request.Receipt.RunID != current.RunID ||
			request.Receipt.Update != nil && request.Receipt.Update.WaitID != current.ClaimedWaitID {
			return workflowInvalid(errors.New("reactor delivery receipt does not match its exact consumption target"))
		}
		current.Status, current.Generation, current.Receipt, current.UpdatedAt = request.Status, current.Generation+1, &request.Receipt, request.At
		resultJSON, encodeErr := encodeWorkflowJSON(request.Receipt)
		if encodeErr != nil {
			return encodeErr
		}
		updated, err := query.ExecContext(ctx, `UPDATE workflow_reactor_deliveries SET status=?,generation=?,result_json=?,updated_at=? WHERE reactor_id=? AND idempotency_key=? AND reactor_generation=? AND run_id=? AND generation=? AND status='applying'`, current.Status, current.Generation, resultJSON, workflowTime(current.UpdatedAt), request.ReactorID, request.IdempotencyKey, current.ReactorGeneration, current.RunID, request.ExpectedGeneration)
		if err != nil {
			return err
		}
		count, countErr := updated.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if count != 1 {
			return workflowruntime.ErrCASMismatch
		}
		reactor.EventCount++
		reactor.Generation++
		reactor.UpdatedAt = request.At
		reactorJSON, encodeErr := encodeWorkflowJSON(reactor)
		if encodeErr != nil {
			return encodeErr
		}
		reactorUpdate, updateErr := query.ExecContext(ctx, `UPDATE workflow_reactors SET event_count=?,generation=?,snapshot_json=?,updated_at=? WHERE reactor_id=? AND generation=? AND status='waiting' AND current_generation=? AND current_run_id=? AND event_count=? AND continue_after_events=?`, reactor.EventCount, reactor.Generation, reactorJSON, workflowTime(reactor.UpdatedAt), reactor.Identity.ID, reactor.Generation-1, reactor.CurrentGeneration, reactor.CurrentRunID, reactor.EventCount-1, reactor.ContinueAfterEvents)
		if updateErr != nil {
			return updateErr
		}
		reactorCount, countErr := reactorUpdate.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if reactorCount != 1 {
			return workflowruntime.ErrCASMismatch
		}
		generationUpdate, updateErr := query.ExecContext(ctx, `UPDATE workflow_reactor_generations SET event_count=? WHERE reactor_id=? AND reactor_generation=?`, reactor.EventCount, reactor.Identity.ID, reactor.CurrentGeneration)
		if updateErr != nil {
			return updateErr
		}
		generationCount, countErr := generationUpdate.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if generationCount != 1 {
			return workflowruntime.ErrCASMismatch
		}
		delivery = current
		if err := delivery.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := reactor.Validate(); err != nil {
			return workflowInvalid(err)
		}
		return nil
	})
	return reactor, delivery, err
}

func (s *WorkflowStateStore) FailReactor(ctx context.Context, request workflowruntime.FailReactorRequest) (workflowruntime.ReactorSnapshot, error) {
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ReactorSnapshot{}, workflowInvalid(err)
	}
	var result workflowruntime.ReactorSnapshot
	err := s.write(ctx, "fail reactor", func(query workflowSQL) error {
		current, err := loadWorkflowReactor(ctx, query, request.ReactorID)
		if err != nil {
			return err
		}
		if current.CurrentRunID != request.RunID {
			return workflowInvalid(errors.New("reactor failure does not match its current run"))
		}
		var runStatus, runUpdatedAt string
		if runQueryErr := query.QueryRowContext(ctx, `SELECT status,updated_at FROM workflow_runs WHERE run_id=?`, request.RunID).Scan(&runStatus, &runUpdatedAt); runQueryErr != nil {
			return runQueryErr
		}
		parsedRunUpdatedAt, err := parseWorkflowTime("terminal reactor run updated_at", runUpdatedAt)
		if err != nil {
			return err
		}
		if workflowruntime.RunStatus(runStatus) != request.RunStatus {
			return workflowInvalid(errors.New("reactor failure status differs from its terminal run"))
		}
		if current.Status == workflowruntime.ReactorFailed {
			result = current
			return nil
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("reactor", request.ExpectedGeneration, current.Generation)
		}
		if current.Status != workflowruntime.ReactorWaiting {
			return workflowInvalid(errors.New("only a waiting reactor can fail from its terminal run"))
		}
		var applying int64
		if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_reactor_deliveries WHERE reactor_id=? AND status='applying'`, request.ReactorID).Scan(&applying); err != nil {
			return err
		}
		if applying != 0 {
			return workflowruntime.ErrReactorBusy
		}
		at := request.At
		if current.UpdatedAt.After(at) {
			at = current.UpdatedAt
		}
		if parsedRunUpdatedAt.After(at) {
			at = parsedRunUpdatedAt
		}
		var latestPending sql.NullString
		if err := query.QueryRowContext(ctx, `SELECT MAX(updated_at) FROM workflow_reactor_deliveries WHERE reactor_id=? AND status='pending'`, request.ReactorID).Scan(&latestPending); err != nil {
			return err
		}
		if latestPending.Valid {
			parsed, parseErr := parseWorkflowTime("pending reactor delivery updated_at", latestPending.String)
			if parseErr != nil {
				return parseErr
			}
			if parsed.After(at) {
				at = parsed
			}
		}
		receipt := workflowruntime.ReactorDeliveryReceipt{Kind: workflowruntime.ReactorDeliveryTerminalRun, RunID: request.RunID, RunStatus: request.RunStatus, ProcessedAt: at}
		if err := receipt.Validate(); err != nil {
			return workflowInvalid(err)
		}
		receiptJSON, encodeErr := encodeWorkflowJSON(receipt)
		if encodeErr != nil {
			return encodeErr
		}
		var allPending, currentPending int64
		if err := query.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN reactor_generation=? AND run_id=? AND claimed_wait_id IS NULL THEN 1 ELSE 0 END),0) FROM workflow_reactor_deliveries WHERE reactor_id=? AND status='pending'`, current.CurrentGeneration, current.CurrentRunID, request.ReactorID).Scan(&allPending, &currentPending); err != nil {
			return err
		}
		if allPending != currentPending {
			return workflowInvalid(errors.New("pending reactor deliveries differ from the current generation"))
		}
		closed, closeErr := query.ExecContext(ctx, `UPDATE workflow_reactor_deliveries SET status='closed',generation=generation+1,result_json=?,updated_at=? WHERE reactor_id=? AND reactor_generation=? AND run_id=? AND status='pending' AND claimed_wait_id IS NULL`, receiptJSON, workflowTime(at), request.ReactorID, current.CurrentGeneration, current.CurrentRunID)
		if closeErr != nil {
			return closeErr
		}
		closedCount, countErr := closed.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if closedCount != currentPending {
			return workflowruntime.ErrCASMismatch
		}
		priorGeneration := current.Generation
		current.Status, current.Generation, current.UpdatedAt = workflowruntime.ReactorFailed, current.Generation+1, at
		encoded, encodeErr := encodeWorkflowJSON(current)
		if encodeErr != nil {
			return encodeErr
		}
		updated, updateErr := query.ExecContext(ctx, `UPDATE workflow_reactors SET status='failed',generation=?,snapshot_json=?,updated_at=? WHERE reactor_id=? AND generation=? AND status='waiting' AND current_generation=? AND current_run_id=? AND event_count=?`, current.Generation, encoded, workflowTime(at), request.ReactorID, priorGeneration, current.CurrentGeneration, current.CurrentRunID, current.EventCount)
		if updateErr != nil {
			return updateErr
		}
		updatedCount, countErr := updated.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if updatedCount != 1 {
			return workflowruntime.ErrCASMismatch
		}
		if err := current.Validate(); err != nil {
			return workflowInvalid(err)
		}
		result = current
		return nil
	})
	return result, err
}

func (s *WorkflowStateStore) RecoverReactorDeliveries(ctx context.Context, limit int) ([]workflowruntime.ReactorDeliverySnapshot, error) {
	if limit < 0 || limit > workflowruntime.MaximumReactorRecoveryLimit {
		return nil, workflowInvalid(errors.New("reactor delivery recovery limit is invalid"))
	}
	statement := `SELECT d.request_json,d.reactor_generation,d.run_id,d.starts_generation,d.claimed_wait_id,d.status,d.generation,d.result_json,d.created_at,d.updated_at FROM workflow_reactor_deliveries d JOIN workflow_reactors r ON r.reactor_id=d.reactor_id WHERE d.status IN ('pending','applying') ORDER BY CASE WHEN r.status='starting' AND d.starts_generation=1 THEN 0 WHEN r.status='starting' THEN 2 ELSE 1 END,d.updated_at,d.reactor_id,d.idempotency_key`
	args := []any{}
	if limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result []workflowruntime.ReactorDeliverySnapshot
	for rows.Next() {
		item, scanErr := scanWorkflowReactorDelivery(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *WorkflowStateStore) RecoverReactors(ctx context.Context, limit int) ([]workflowruntime.ReactorSnapshot, error) {
	if limit < 0 || limit > workflowruntime.MaximumReactorRecoveryLimit {
		return nil, workflowInvalid(errors.New("reactor recovery limit is invalid"))
	}
	statement := workflowReactorSelect + ` WHERE status IN ('starting','rolling') OR (status='waiting' AND (event_count>=continue_after_events OR EXISTS (SELECT 1 FROM workflow_runs AS terminal_run WHERE terminal_run.run_id=current_run_id AND terminal_run.status IN ('succeeded','failed','canceled','timed_out','crashed')))) ORDER BY updated_at,reactor_id`
	args := []any{}
	if limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result []workflowruntime.ReactorSnapshot
	for rows.Next() {
		item, scanErr := scanWorkflowReactor(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *WorkflowStateStore) BeginReactorContinuation(ctx context.Context, request workflowruntime.ReactorContinuationRequest) (workflowruntime.ReactorSnapshot, workflowruntime.ReactorContinuationSnapshot, workflowruntime.IdempotencyOutcome, error) {
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ReactorSnapshot{}, workflowruntime.ReactorContinuationSnapshot{}, "", workflowInvalid(err)
	}
	requestJSON, encodeErr := encodeWorkflowJSON(request)
	if encodeErr != nil {
		return workflowruntime.ReactorSnapshot{}, workflowruntime.ReactorContinuationSnapshot{}, "", encodeErr
	}
	var reactor workflowruntime.ReactorSnapshot
	var continuation workflowruntime.ReactorContinuationSnapshot
	outcome := workflowruntime.IdempotencyApplied
	err := s.write(ctx, "begin reactor continuation", func(query workflowSQL) error {
		prior, priorErr := loadWorkflowReactorContinuation(ctx, query, request.IdempotencyKey)
		if priorErr == nil {
			priorJSON, priorEncodeErr := encodeWorkflowJSON(prior.Request)
			if priorEncodeErr != nil {
				return priorEncodeErr
			}
			if priorJSON != requestJSON {
				return workflowIdempotencyConflict("reactor continuation", request.IdempotencyKey)
			}
			continuation, outcome = prior, workflowruntime.IdempotencyReplayed
			var reactorErr error
			reactor, reactorErr = loadWorkflowReactor(ctx, query, request.ReactorID)
			return reactorErr
		}
		if !errors.Is(priorErr, workflowruntime.ErrNotFound) {
			return priorErr
		}
		var applying int
		if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_reactor_deliveries WHERE reactor_id=? AND status='applying'`, request.ReactorID).Scan(&applying); err != nil {
			return err
		}
		if applying != 0 {
			return workflowruntime.ErrReactorBusy
		}
		current, err := loadWorkflowReactor(ctx, query, request.ReactorID)
		if err != nil {
			return err
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("reactor", request.ExpectedGeneration, current.Generation)
		}
		if current.Status != workflowruntime.ReactorWaiting || current.CurrentGeneration != request.FromGeneration || current.CurrentRunID != request.FromRunID || request.At.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("reactor continuation source is stale"))
		}
		current.Status, current.Generation, current.UpdatedAt = workflowruntime.ReactorRolling, current.Generation+1, request.At
		encoded, currentEncodeErr := encodeWorkflowJSON(current)
		if currentEncodeErr != nil {
			return currentEncodeErr
		}
		updated, updateErr := query.ExecContext(ctx, `UPDATE workflow_reactors SET status='rolling',generation=?,snapshot_json=?,updated_at=? WHERE reactor_id=? AND generation=? AND status='waiting' AND current_generation=? AND current_run_id=? AND event_count=? AND continue_after_events=?`, current.Generation, encoded, workflowTime(current.UpdatedAt), request.ReactorID, request.ExpectedGeneration, request.FromGeneration, request.FromRunID, current.EventCount, current.ContinueAfterEvents)
		if updateErr != nil {
			return updateErr
		}
		count, countErr := updated.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if count != 1 {
			return workflowruntime.ErrCASMismatch
		}
		if err := current.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := request.State.Validate(); err != nil {
			return err
		}
		continuation = workflowruntime.ReactorContinuationSnapshot{Request: request, Status: workflowruntime.ReactorContinuationPending, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
		inserted, insertErr := query.ExecContext(ctx, `INSERT INTO workflow_reactor_continuations(idempotency_key,reactor_id,from_generation,to_generation,from_run_id,to_run_id,status,generation,request_json,created_at,updated_at) VALUES (?,?,?,?,?,?,'pending',1,?,?,?)`, request.IdempotencyKey, request.ReactorID, request.FromGeneration, request.FromGeneration+1, request.FromRunID, request.ToRunID, requestJSON, workflowTime(request.At), workflowTime(request.At))
		if insertErr != nil {
			return insertErr
		}
		insertedCount, countErr := inserted.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if insertedCount != 1 {
			return workflowruntime.ErrCASMismatch
		}
		reactor = current
		return nil
	})
	return reactor, continuation, outcome, err
}

func (s *WorkflowStateStore) CompleteReactorContinuation(ctx context.Context, key string, expected uint64, at time.Time) (workflowruntime.ReactorSnapshot, workflowruntime.ReactorContinuationSnapshot, error) {
	at = at.UTC()
	var reactor workflowruntime.ReactorSnapshot
	var continuation workflowruntime.ReactorContinuationSnapshot
	err := s.write(ctx, "complete reactor continuation", func(query workflowSQL) error {
		current, err := loadWorkflowReactorContinuation(ctx, query, key)
		if err != nil {
			return err
		}
		if current.Status == workflowruntime.ReactorContinuationCompleted {
			continuation = current
			reactor, err = loadWorkflowReactor(ctx, query, current.Request.ReactorID)
			return err
		}
		if current.Generation != expected || current.Status != workflowruntime.ReactorContinuationPending || at.IsZero() || at.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("reactor continuation completion is stale"))
		}
		reactor, err = loadWorkflowReactor(ctx, query, current.Request.ReactorID)
		if err != nil {
			return err
		}
		if reactor.Status != workflowruntime.ReactorRolling || reactor.CurrentGeneration != current.Request.FromGeneration || reactor.CurrentRunID != current.Request.FromRunID || at.Before(reactor.UpdatedAt) {
			return workflowInvalid(errors.New("reactor is not fenced on the continuation source"))
		}
		toGeneration := current.Request.FromGeneration + 1
		generationJSON, encodeErr := encodeWorkflowJSON(struct {
			Identity workflowruntime.ReactorIdentity `json:"identity"`
			RunID    workflowruntime.RunID           `json:"run_id"`
			State    any                             `json:"state,omitempty"`
		}{reactor.Identity, current.Request.ToRunID, current.Request.State})
		if encodeErr != nil {
			return encodeErr
		}
		insertedGeneration, insertErr := query.ExecContext(ctx, `INSERT INTO workflow_reactor_generations(reactor_id,reactor_generation,run_id,plan_digest,provenance_digest,state_ref_json,event_count,snapshot_json,created_at) VALUES (?,?,?,?,?,?,0,?,?)`, reactor.Identity.ID, toGeneration, current.Request.ToRunID, reactor.Identity.Plan.Digest, reactor.Identity.Provenance.Digest, generationJSON, generationJSON, workflowTime(at))
		if insertErr != nil {
			return insertErr
		}
		generationCount, countErr := insertedGeneration.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if generationCount != 1 {
			return workflowruntime.ErrCASMismatch
		}
		var pendingDeliveries int64
		if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_reactor_deliveries WHERE reactor_id=? AND status='pending'`, reactor.Identity.ID).Scan(&pendingDeliveries); err != nil {
			return err
		}
		reassigned, reassignErr := query.ExecContext(ctx, `UPDATE workflow_reactor_deliveries SET reactor_generation=?,run_id=? WHERE reactor_id=? AND status='pending'`, toGeneration, current.Request.ToRunID, reactor.Identity.ID)
		if reassignErr != nil {
			return reassignErr
		}
		reassignedCount, countErr := reassigned.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if reassignedCount != pendingDeliveries {
			return workflowruntime.ErrCASMismatch
		}
		priorReactorGeneration, priorEventCount := reactor.Generation, reactor.EventCount
		reactor.CurrentGeneration, reactor.CurrentRunID, reactor.Status, reactor.EventCount = toGeneration, current.Request.ToRunID, workflowruntime.ReactorWaiting, 0
		reactor.Generation++
		reactor.UpdatedAt = at
		reactorJSON, reactorEncodeErr := encodeWorkflowJSON(reactor)
		if reactorEncodeErr != nil {
			return reactorEncodeErr
		}
		updatedReactor, updateErr := query.ExecContext(ctx, `UPDATE workflow_reactors SET current_generation=?,current_run_id=?,event_count=0,status='waiting',generation=?,snapshot_json=?,updated_at=? WHERE reactor_id=? AND generation=? AND status='rolling' AND current_generation=? AND current_run_id=? AND event_count=?`, reactor.CurrentGeneration, reactor.CurrentRunID, reactor.Generation, reactorJSON, workflowTime(at), reactor.Identity.ID, priorReactorGeneration, current.Request.FromGeneration, current.Request.FromRunID, priorEventCount)
		if updateErr != nil {
			return updateErr
		}
		reactorCount, countErr := updatedReactor.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if reactorCount != 1 {
			return workflowruntime.ErrCASMismatch
		}
		current.Status, current.Generation, current.UpdatedAt = workflowruntime.ReactorContinuationCompleted, current.Generation+1, at
		updatedContinuation, updateErr := query.ExecContext(ctx, `UPDATE workflow_reactor_continuations SET status='completed',generation=?,updated_at=? WHERE idempotency_key=? AND generation=? AND status='pending'`, current.Generation, workflowTime(at), key, expected)
		if updateErr != nil {
			return updateErr
		}
		continuationCount, countErr := updatedContinuation.RowsAffected()
		if countErr != nil {
			return countErr
		}
		if continuationCount != 1 {
			return workflowruntime.ErrCASMismatch
		}
		continuation = current
		return nil
	})
	return reactor, continuation, err
}

func (s *WorkflowStateStore) RecoverReactorContinuations(ctx context.Context, limit int) ([]workflowruntime.ReactorContinuationSnapshot, error) {
	if limit < 0 || limit > workflowruntime.MaximumReactorRecoveryLimit {
		return nil, workflowInvalid(errors.New("reactor continuation recovery limit is invalid"))
	}
	statement := `SELECT request_json,status,generation,created_at,updated_at FROM workflow_reactor_continuations WHERE status!='completed' ORDER BY updated_at,idempotency_key`
	args := []any{}
	if limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	var result []workflowruntime.ReactorContinuationSnapshot
	for rows.Next() {
		item, scanErr := scanWorkflowReactorContinuation(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func loadWorkflowReactor(ctx context.Context, query workflowSQL, id string) (workflowruntime.ReactorSnapshot, error) {
	return scanWorkflowReactor(query.QueryRowContext(ctx, workflowReactorSelect+` WHERE reactor_id=?`, id))
}

func scanWorkflowReactor(row workflowScanner) (workflowruntime.ReactorSnapshot, error) {
	var result workflowruntime.ReactorSnapshot
	var encoded, reactorID, registrationID, correlation, runID, status, createdAt, updatedAt string
	var registrationGeneration, currentGeneration, continueAfterEvents, eventCount, generation int64
	if err := row.Scan(&encoded, &reactorID, &registrationID, &registrationGeneration, &correlation, &currentGeneration, &runID, &continueAfterEvents, &eventCount, &status, &generation, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, fmt.Errorf("%w: workflow reactor", workflowruntime.ErrNotFound)
		}
		return result, err
	}
	if err := decodeWorkflowJSON("workflow reactor", encoded, &result); err != nil {
		return result, err
	}
	parsedCreatedAt, err := parseWorkflowTime("reactor created_at", createdAt)
	if err != nil {
		return result, err
	}
	parsedUpdatedAt, err := parseWorkflowTime("reactor updated_at", updatedAt)
	if err != nil {
		return result, err
	}
	if registrationGeneration < 0 || currentGeneration < 0 || continueAfterEvents < 0 || eventCount < 0 || generation < 0 ||
		result.Identity.ID != reactorID || result.Identity.RegistrationID != registrationID || result.Identity.RegistrationGeneration != uint64(registrationGeneration) || result.Identity.Correlation != correlation ||
		result.CurrentGeneration != uint64(currentGeneration) || result.CurrentRunID != workflowruntime.RunID(runID) || result.ContinueAfterEvents != uint64(continueAfterEvents) || result.EventCount != uint64(eventCount) || result.Status != workflowruntime.ReactorStatus(status) || result.Generation != uint64(generation) || !result.CreatedAt.Equal(parsedCreatedAt) || !result.UpdatedAt.Equal(parsedUpdatedAt) {
		return result, workflowInvalid(errors.New("workflow reactor projection differs from canonical snapshot"))
	}
	return result, result.Validate()
}

func loadWorkflowReactorDelivery(ctx context.Context, query workflowSQL, reactorID, key string) (workflowruntime.ReactorDeliverySnapshot, error) {
	return scanWorkflowReactorDelivery(query.QueryRowContext(ctx, `SELECT request_json,reactor_generation,run_id,starts_generation,claimed_wait_id,status,generation,result_json,created_at,updated_at FROM workflow_reactor_deliveries WHERE reactor_id=? AND idempotency_key=?`, reactorID, key))
}

func scanWorkflowReactorDelivery(row workflowScanner) (workflowruntime.ReactorDeliverySnapshot, error) {
	var result workflowruntime.ReactorDeliverySnapshot
	var requestJSON, runID, status, createdAt, updatedAt string
	var resultJSON, claimedWaitID sql.NullString
	var reactorGeneration, startsGeneration, generation int64
	if err := row.Scan(&requestJSON, &reactorGeneration, &runID, &startsGeneration, &claimedWaitID, &status, &generation, &resultJSON, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, fmt.Errorf("%w: reactor delivery", workflowruntime.ErrNotFound)
		}
		return result, err
	}
	if err := decodeWorkflowJSON("reactor delivery request", requestJSON, &result.Request); err != nil {
		return result, err
	}
	if startsGeneration != 0 && startsGeneration != 1 {
		return result, workflowInvalid(errors.New("reactor delivery starts_generation projection is invalid"))
	}
	if reactorGeneration < 0 || generation < 0 {
		return result, workflowInvalid(errors.New("reactor delivery generation projection is invalid"))
	}
	result.ReactorGeneration, result.StartsGeneration, result.Generation = uint64(reactorGeneration), startsGeneration == 1, uint64(generation)
	result.RunID, result.Status = workflowruntime.RunID(runID), workflowruntime.ReactorDeliveryStatus(status)
	if claimedWaitID.Valid {
		result.ClaimedWaitID = workflowruntime.WaitID(claimedWaitID.String)
	}
	if resultJSON.Valid {
		var receipt workflowruntime.ReactorDeliveryReceipt
		if err := decodeWorkflowJSON("reactor delivery receipt", resultJSON.String, &receipt); err != nil {
			return result, err
		}
		result.Receipt = &receipt
	}
	var err error
	if result.CreatedAt, err = parseWorkflowTime("reactor delivery created_at", createdAt); err != nil {
		return result, err
	}
	if result.UpdatedAt, err = parseWorkflowTime("reactor delivery updated_at", updatedAt); err != nil {
		return result, err
	}
	return result, result.Validate()
}

func loadWorkflowReactorContinuation(ctx context.Context, query workflowSQL, key string) (workflowruntime.ReactorContinuationSnapshot, error) {
	return scanWorkflowReactorContinuation(query.QueryRowContext(ctx, `SELECT request_json,status,generation,created_at,updated_at FROM workflow_reactor_continuations WHERE idempotency_key=?`, key))
}

func scanWorkflowReactorContinuation(row workflowScanner) (workflowruntime.ReactorContinuationSnapshot, error) {
	var result workflowruntime.ReactorContinuationSnapshot
	var requestJSON, status, createdAt, updatedAt string
	var generation int64
	if err := row.Scan(&requestJSON, &status, &generation, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, fmt.Errorf("%w: reactor continuation", workflowruntime.ErrNotFound)
		}
		return result, err
	}
	if err := decodeWorkflowJSON("reactor continuation request", requestJSON, &result.Request); err != nil {
		return result, err
	}
	if generation < 0 {
		return result, workflowInvalid(errors.New("reactor continuation generation projection is invalid"))
	}
	result.Status, result.Generation = workflowruntime.ReactorContinuationStatus(status), uint64(generation)
	var err error
	if result.CreatedAt, err = parseWorkflowTime("reactor continuation created_at", createdAt); err != nil {
		return result, err
	}
	if result.UpdatedAt, err = parseWorkflowTime("reactor continuation updated_at", updatedAt); err != nil {
		return result, err
	}
	return result, result.Validate()
}
