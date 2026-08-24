package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

// RecordPlan implements runtime.StateStore.
func (s *WorkflowStateStore) RecordPlan(ctx context.Context, plan workflowruntime.PlanRef) error {
	return s.write(ctx, "record workflow plan", func(query workflowSQL) error {
		return ensureWorkflowPlan(ctx, query, plan)
	})
}

// LoadPlan implements runtime.StateStore.
func (s *WorkflowStateStore) LoadPlan(ctx context.Context, digest string) (workflowruntime.PlanRef, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.PlanRef{}, err
	}
	return loadWorkflowPlan(ctx, s.db, digest)
}

// CreateRun implements runtime.StateStore and records its plan reference and
// start idempotency outcome in the same transaction.
func (s *WorkflowStateStore) CreateRun(ctx context.Context, request workflowruntime.CreateRunRequest) (workflowruntime.RunSnapshot, workflowruntime.IdempotencyOutcome, error) {
	if err := validateWorkflowCreateRun(request); err != nil {
		return workflowruntime.RunSnapshot{}, "", workflowInvalid(err)
	}
	requestJSON, canonicalErr := canonicalCreateRunRequest(request)
	if canonicalErr != nil {
		return workflowruntime.RunSnapshot{}, "", canonicalErr
	}
	var (
		result  workflowruntime.RunSnapshot
		outcome workflowruntime.IdempotencyOutcome
	)
	writeErr := s.write(ctx, "create workflow run", func(query workflowSQL) error {
		var priorRequest, priorResult string
		replayErr := query.QueryRowContext(ctx, `
SELECT request_json, result_json
FROM workflow_run_start_idempotency WHERE idempotency_key = ?`,
			request.StartIdempotencyKey,
		).Scan(&priorRequest, &priorResult)
		switch {
		case replayErr == nil:
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("create run", request.StartIdempotencyKey)
			}
			if decodeErr := decodeWorkflowJSON("run start result", priorResult, &result); decodeErr != nil {
				return decodeErr
			}
			if validationErr := result.Validate(); validationErr != nil {
				return workflowInvalid(validationErr)
			}
			if result.ID != request.ID || result.Plan != request.Plan || result.Status != request.Status ||
				!equalWorkflowValueRef(result.Inputs, request.Inputs) || result.Outputs != nil ||
				result.Generation != 1 || !result.CreatedAt.Equal(request.CreatedAt) ||
				!result.UpdatedAt.Equal(request.CreatedAt) {
				return workflowInvalid(errors.New("persisted run-start result does not match its request"))
			}
			outcome = workflowruntime.IdempotencyReplayed
			return nil
		case !errors.Is(replayErr, sql.ErrNoRows):
			return fmt.Errorf("load workflow run start idempotency: %w", replayErr)
		}

		var existing int
		if identityErr := query.QueryRowContext(ctx, `SELECT COUNT(1) FROM workflow_runs WHERE run_id = ?`, request.ID).Scan(&existing); identityErr != nil {
			return fmt.Errorf("check workflow run identity: %w", identityErr)
		}
		if existing != 0 {
			return fmt.Errorf("%w: run %q", workflowruntime.ErrAlreadyExists, request.ID)
		}
		if planErr := ensureWorkflowPlan(ctx, query, request.Plan); planErr != nil {
			return planErr
		}
		createdAt := request.CreatedAt.UTC()
		result = workflowruntime.RunSnapshot{
			ID: request.ID, Plan: request.Plan, Status: request.Status,
			Inputs: cloneWorkflowValueRef(request.Inputs), Generation: 1,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}
		if validationErr := result.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if err := insertWorkflowRun(ctx, query, result); err != nil {
			return err
		}
		resultJSON, encodeErr := encodeWorkflowJSON(result)
		if encodeErr != nil {
			return encodeErr
		}
		if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_run_start_idempotency(idempotency_key, request_json, result_json)
VALUES (?, ?, ?)`, request.StartIdempotencyKey, requestJSON, resultJSON); err != nil {
			return fmt.Errorf("record workflow run start idempotency: %w", err)
		}
		outcome = workflowruntime.IdempotencyApplied
		return nil
	})
	if writeErr != nil {
		return workflowruntime.RunSnapshot{}, "", writeErr
	}
	return result, outcome, nil
}

// LoadRun implements runtime.StateStore.
func (s *WorkflowStateStore) LoadRun(ctx context.Context, id workflowruntime.RunID) (workflowruntime.RunSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.RunSnapshot{}, err
	}
	return loadWorkflowRun(ctx, s.db, id)
}

// SaveRun implements runtime.StateStore and rejects lifecycle bypasses before
// executing its SQL generation CAS.
func (s *WorkflowStateStore) SaveRun(ctx context.Context, request workflowruntime.SaveRunRequest) (workflowruntime.RunSnapshot, error) {
	var result workflowruntime.RunSnapshot
	writeErr := s.write(ctx, "save workflow run", func(query workflowSQL) error {
		current, loadErr := loadWorkflowRun(ctx, query, request.Snapshot.ID)
		if loadErr != nil {
			return loadErr
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("run", request.ExpectedGeneration, current.Generation)
		}
		if request.Snapshot.Status != current.Status {
			return workflowInvalid(errors.New("run status changes require TransitionRun"))
		}
		if !equalWorkflowValueRef(request.Snapshot.Outputs, current.Outputs) {
			return workflowInvalid(errors.New("run outputs are lifecycle-managed"))
		}
		if request.Snapshot.Plan != current.Plan {
			return workflowInvalid(errors.New("run plan reference is immutable"))
		}
		result = request.Snapshot
		result.Inputs = cloneWorkflowValueRef(request.Snapshot.Inputs)
		result.Outputs = cloneWorkflowValueRef(request.Snapshot.Outputs)
		result.Generation = current.Generation + 1
		result.CreatedAt = current.CreatedAt
		result.UpdatedAt = request.Snapshot.UpdatedAt.UTC()
		if result.UpdatedAt.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("run updated_at must not regress"))
		}
		if err := result.Validate(); err != nil {
			return workflowInvalid(err)
		}
		return updateWorkflowRunCAS(ctx, query, result, request.ExpectedGeneration)
	})
	if writeErr != nil {
		return workflowruntime.RunSnapshot{}, writeErr
	}
	return result, nil
}

// TransitionRun implements runtime.StateStore with atomic state/event writes.
func (s *WorkflowStateStore) TransitionRun(ctx context.Context, request workflowruntime.RunTransitionRequest) (workflowruntime.RunTransitionResult, error) {
	if err := validateWorkflowRunTransition(request); err != nil {
		return workflowruntime.RunTransitionResult{}, workflowInvalid(err)
	}
	var result workflowruntime.RunTransitionResult
	writeErr := s.write(ctx, "transition workflow run", func(query workflowSQL) error {
		current, loadErr := loadWorkflowRun(ctx, query, request.RunID)
		if loadErr != nil {
			return loadErr
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("run", request.ExpectedGeneration, current.Generation)
		}
		at := request.At.UTC()
		if at.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("run transition time must not regress"))
		}
		if current.Status == request.To {
			if at.Equal(current.UpdatedAt) && equalWorkflowValueRef(request.Outputs, current.Outputs) {
				result = workflowruntime.RunTransitionResult{
					Snapshot: current, Outcome: workflowruntime.TransitionNoOp,
				}
				return nil
			}
			return &workflowruntime.TransitionConflictError{
				Entity: "run", ID: string(current.ID), Status: string(current.Status),
				Reason: "same-status request is not an exact semantic replay",
			}
		}
		if err := workflowruntime.ValidateRunStatusTransition(current.Status, request.To); err != nil {
			return withWorkflowTransitionID(err, string(current.ID))
		}
		if request.To != workflowruntime.RunSucceeded && request.Outputs != nil {
			return workflowInvalid(errors.New("only a succeeded run may record outputs"))
		}

		next := current
		next.Status = request.To
		next.Outputs = cloneWorkflowValueRef(request.Outputs)
		next.Generation++
		next.UpdatedAt = at
		if err := next.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowRunCAS(ctx, query, next, current.Generation); err != nil {
			return err
		}
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: current.ID, Type: workflowruntime.EventRunStatusChanged, OccurredAt: at,
			Attributes: workflowTransitionAttributes("run", string(current.Status), string(next.Status)),
			Values:     cloneWorkflowValueRef(request.Outputs),
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		eventCopy := event
		result = workflowruntime.RunTransitionResult{
			Snapshot: next, Outcome: workflowruntime.TransitionApplied, Event: &eventCopy,
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.RunTransitionResult{}, writeErr
	}
	return result, nil
}

// CreateNodeInvocation implements runtime.StateStore.
func (s *WorkflowStateStore) CreateNodeInvocation(ctx context.Context, request workflowruntime.CreateNodeInvocationRequest) (workflowruntime.NodeInvocationSnapshot, error) {
	next := cloneWorkflowNode(request.Snapshot)
	if next.Generation != 0 || next.ClaimGeneration != 0 || next.Lease != nil {
		return workflowruntime.NodeInvocationSnapshot{}, workflowInvalid(errors.New("new node must have zero generations and no lease"))
	}
	if next.Status != workflowruntime.NodePending || next.Blocked != nil || next.LatestAttempt != 0 {
		return workflowruntime.NodeInvocationSnapshot{}, workflowInvalid(errors.New("new node must enter lifecycle as pending without attempts"))
	}
	next.Generation = 1
	next.CreatedAt = next.CreatedAt.UTC()
	next.UpdatedAt = next.UpdatedAt.UTC()
	if err := next.Validate(); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, workflowInvalid(err)
	}
	writeErr := s.write(ctx, "create workflow node", func(query workflowSQL) error {
		if _, parentErr := loadWorkflowRun(ctx, query, next.ID.RunID); parentErr != nil {
			return parentErr
		}
		return insertWorkflowNode(ctx, query, next)
	})
	if writeErr != nil {
		return workflowruntime.NodeInvocationSnapshot{}, writeErr
	}
	return next, nil
}

// LoadNodeInvocation implements runtime.StateStore.
func (s *WorkflowStateStore) LoadNodeInvocation(ctx context.Context, id workflowruntime.NodeInvocationID) (workflowruntime.NodeInvocationSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.NodeInvocationSnapshot{}, err
	}
	return loadWorkflowNode(ctx, s.db, id)
}

// SaveNodeInvocation implements runtime.StateStore and rejects lifecycle,
// attempt, value, and claim bypasses before its SQL generation CAS.
func (s *WorkflowStateStore) SaveNodeInvocation(ctx context.Context, request workflowruntime.SaveNodeInvocationRequest) (workflowruntime.NodeInvocationSnapshot, error) {
	var result workflowruntime.NodeInvocationSnapshot
	writeErr := s.write(ctx, "save workflow node", func(query workflowSQL) error {
		current, loadErr := loadWorkflowNode(ctx, query, request.Snapshot.ID)
		if loadErr != nil {
			return loadErr
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("node invocation", request.ExpectedGeneration, current.Generation)
		}
		if request.Snapshot.Status != current.Status || !equalWorkflowBlocked(request.Snapshot.Blocked, current.Blocked) {
			return workflowInvalid(errors.New("node lifecycle changes require TransitionNode"))
		}
		if request.Snapshot.LatestAttempt != current.LatestAttempt {
			return workflowInvalid(errors.New("latest attempt is lifecycle-managed"))
		}
		if !equalWorkflowValueRef(request.Snapshot.Inputs, current.Inputs) ||
			!equalWorkflowValueRef(request.Snapshot.Outputs, current.Outputs) {
			return workflowInvalid(errors.New("node input and output references are lifecycle-managed"))
		}
		if request.Snapshot.ClaimGeneration != current.ClaimGeneration ||
			!equalWorkflowLease(request.Snapshot.Lease, current.Lease) {
			return fmt.Errorf("%w: claim fields may only change through claim methods", workflowruntime.ErrClaimMismatch)
		}
		result = cloneWorkflowNode(request.Snapshot)
		result.Generation = current.Generation + 1
		result.CreatedAt = current.CreatedAt
		result.UpdatedAt = request.Snapshot.UpdatedAt.UTC()
		if result.UpdatedAt.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("node updated_at must not regress"))
		}
		if err := result.Validate(); err != nil {
			return workflowInvalid(err)
		}
		return updateWorkflowNodeCAS(ctx, query, result, request.ExpectedGeneration)
	})
	if writeErr != nil {
		return workflowruntime.NodeInvocationSnapshot{}, writeErr
	}
	return result, nil
}

func validateWorkflowCreateRun(request workflowruntime.CreateRunRequest) error {
	if request.ID == "" || request.CreatedAt.IsZero() || request.StartIdempotencyKey == "" {
		return errors.New("create run requires id, created_at, and idempotency key")
	}
	if err := request.Plan.Validate(); err != nil {
		return err
	}
	if request.Status != workflowruntime.RunPending {
		return errors.New("new run must enter lifecycle as pending")
	}
	if request.Inputs != nil {
		return request.Inputs.Validate()
	}
	return nil
}

func validateWorkflowRunTransition(request workflowruntime.RunTransitionRequest) error {
	if request.RunID == "" || request.At.IsZero() {
		return errors.New("run transition requires run id and timestamp")
	}
	if !request.To.Valid() {
		return fmt.Errorf("unsupported run status %q", request.To)
	}
	if request.Outputs != nil {
		return request.Outputs.Validate()
	}
	return nil
}

func cloneWorkflowValueRef(ref *values.ValueSetRef) *values.ValueSetRef {
	if ref == nil {
		return nil
	}
	copyRef := *ref
	return &copyRef
}

func cloneWorkflowNode(snapshot workflowruntime.NodeInvocationSnapshot) workflowruntime.NodeInvocationSnapshot {
	if snapshot.Blocked != nil {
		blocked := *snapshot.Blocked
		blocked.Dependencies = append([]workflowruntime.NodeInvocationID(nil), snapshot.Blocked.Dependencies...)
		blocked.Details = cloneWorkflowStringMap(snapshot.Blocked.Details)
		snapshot.Blocked = &blocked
	}
	snapshot.Inputs = cloneWorkflowValueRef(snapshot.Inputs)
	snapshot.Outputs = cloneWorkflowValueRef(snapshot.Outputs)
	if snapshot.Wait != nil {
		wait := *snapshot.Wait
		snapshot.Wait = &wait
	}
	if snapshot.Lease != nil {
		lease := *snapshot.Lease
		snapshot.Lease = &lease
	}
	return snapshot
}

func cloneWorkflowStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func withWorkflowTransitionID(err error, id string) error {
	var transition *workflowruntime.TransitionError
	if errors.As(err, &transition) {
		copyTransition := *transition
		copyTransition.ID = id
		return &copyTransition
	}
	return err
}

func workflowTransitionAttributes(entity, from, to string) map[string]string {
	return map[string]string{"entity": entity, "from_status": from, "to_status": to}
}
