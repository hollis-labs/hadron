package persistence

import (
	"context"
	"errors"
	"fmt"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

// CreateWait implements runtime.StateStore in an explicit transaction.
func (s *WorkflowStateStore) CreateWait(ctx context.Context, request workflowruntime.CreateWaitRequest) (workflowruntime.WaitSnapshot, error) {
	next := request.Snapshot
	next.ResumeValues = cloneWorkflowValueRef(next.ResumeValues)
	if next.Generation != 0 || next.Status != workflowruntime.WaitOpen {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(errors.New("new wait must be open with zero generation"))
	}
	next.Generation = 1
	next.CreatedAt = next.CreatedAt.UTC()
	next.UpdatedAt = next.UpdatedAt.UTC()
	if err := next.Validate(); err != nil {
		return workflowruntime.WaitSnapshot{}, workflowInvalid(err)
	}
	writeErr := s.write(ctx, "create workflow wait", func(query workflowSQL) error {
		if _, parentErr := loadWorkflowNode(ctx, query, next.Invocation); parentErr != nil {
			return parentErr
		}
		return insertWorkflowWait(ctx, query, next)
	})
	if writeErr != nil {
		return workflowruntime.WaitSnapshot{}, writeErr
	}
	return next, nil
}

// LoadWait implements runtime.StateStore.
func (s *WorkflowStateStore) LoadWait(ctx context.Context, id workflowruntime.WaitID) (workflowruntime.WaitSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.WaitSnapshot{}, err
	}
	return loadWorkflowWait(ctx, s.db, id)
}

// SaveWait implements runtime.StateStore with invocation identity and SQL CAS
// enforcement while leaving wait-transition policy to its owner.
func (s *WorkflowStateStore) SaveWait(ctx context.Context, request workflowruntime.SaveWaitRequest) (workflowruntime.WaitSnapshot, error) {
	var result workflowruntime.WaitSnapshot
	writeErr := s.write(ctx, "save workflow wait", func(query workflowSQL) error {
		current, loadErr := loadWorkflowWait(ctx, query, request.Snapshot.Ref.ID)
		if loadErr != nil {
			return loadErr
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("wait", request.ExpectedGeneration, current.Generation)
		}
		if request.Snapshot.Invocation != current.Invocation {
			return workflowInvalid(errors.New("wait invocation is immutable"))
		}
		result = request.Snapshot
		result.ResumeValues = cloneWorkflowValueRef(request.Snapshot.ResumeValues)
		result.Generation = current.Generation + 1
		result.CreatedAt = current.CreatedAt
		result.UpdatedAt = request.Snapshot.UpdatedAt.UTC()
		if !result.ResolvedAt.IsZero() {
			result.ResolvedAt = result.ResolvedAt.UTC()
		}
		if result.UpdatedAt.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("wait updated_at must not regress"))
		}
		if err := result.Validate(); err != nil {
			return workflowInvalid(err)
		}
		return updateWorkflowWaitCAS(ctx, query, result, request.ExpectedGeneration)
	})
	if writeErr != nil {
		return workflowruntime.WaitSnapshot{}, writeErr
	}
	return result, nil
}

// ResumeWait implements runtime.StateStore with durable optional-key replay and
// the wait mutation in one transaction.
func (s *WorkflowStateStore) ResumeWait(ctx context.Context, request workflowruntime.ResumeWaitRequest) (workflowruntime.WaitSnapshot, workflowruntime.IdempotencyOutcome, error) {
	if err := validateWorkflowResume(request); err != nil {
		return workflowruntime.WaitSnapshot{}, "", workflowInvalid(err)
	}
	requestJSON, canonicalErr := canonicalResumeRequest(request)
	if canonicalErr != nil {
		return workflowruntime.WaitSnapshot{}, "", canonicalErr
	}
	var (
		result  workflowruntime.WaitSnapshot
		outcome workflowruntime.IdempotencyOutcome
	)
	writeErr := s.write(ctx, "resume workflow wait", func(query workflowSQL) error {
		if request.IdempotencyKey != "" {
			priorRequest, priorResult, found, loadErr := loadWorkflowIdempotency(
				ctx, query, "workflow_wait_resume_idempotency", request.IdempotencyKey,
			)
			if loadErr != nil {
				return loadErr
			}
			if found {
				if priorRequest != requestJSON {
					return workflowIdempotencyConflict("resume wait", request.IdempotencyKey)
				}
				if decodeErr := decodeWorkflowJSON("wait resume result", priorResult, &result); decodeErr != nil {
					return decodeErr
				}
				if validationErr := result.Validate(); validationErr != nil {
					return workflowInvalid(validationErr)
				}
				if result.Ref.ID != request.WaitID || result.Status != workflowruntime.WaitResumed ||
					!equalWorkflowValueRef(result.ResumeValues, request.Values) ||
					!result.ResolvedAt.Equal(request.ResumedAt) || !result.UpdatedAt.Equal(request.ResumedAt) {
					return workflowInvalid(errors.New("persisted wait-resume result does not match its request"))
				}
				outcome = workflowruntime.IdempotencyReplayed
				return nil
			}
		}

		current, loadErr := loadWorkflowWait(ctx, query, request.WaitID)
		if loadErr != nil {
			return loadErr
		}
		if current.Status != workflowruntime.WaitOpen {
			return fmt.Errorf("%w: wait %q", workflowruntime.ErrAlreadyResumed, request.WaitID)
		}
		resumedAt := request.ResumedAt.UTC()
		if resumedAt.Before(current.UpdatedAt) {
			return workflowInvalid(errors.New("wait resume time must not regress"))
		}
		result = current
		result.Status = workflowruntime.WaitResumed
		result.ResumeValues = cloneWorkflowValueRef(request.Values)
		result.ResolvedAt = resumedAt
		result.UpdatedAt = resumedAt
		result.Generation++
		if err := result.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowWaitCAS(ctx, query, result, current.Generation); err != nil {
			return err
		}
		if request.IdempotencyKey != "" {
			resultJSON, encodeErr := encodeWorkflowJSON(result)
			if encodeErr != nil {
				return encodeErr
			}
			if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_wait_resume_idempotency(idempotency_key, request_json, result_json)
VALUES (?, ?, ?)`, request.IdempotencyKey, requestJSON, resultJSON); err != nil {
				if isSQLiteConstraint(err) {
					return workflowIdempotencyConflict("resume wait", request.IdempotencyKey)
				}
				return fmt.Errorf("record workflow wait resume idempotency: %w", err)
			}
		}
		outcome = workflowruntime.IdempotencyApplied
		return nil
	})
	if writeErr != nil {
		return workflowruntime.WaitSnapshot{}, "", writeErr
	}
	return result, outcome, nil
}

func validateWorkflowResume(request workflowruntime.ResumeWaitRequest) error {
	if request.WaitID == "" || request.ResumedAt.IsZero() {
		return errors.New("resume wait requires wait id and resumed_at")
	}
	if request.Values != nil {
		return request.Values.Validate()
	}
	return nil
}
