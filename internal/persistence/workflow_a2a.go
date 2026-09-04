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

// WorkflowA2ATaskStore persists immutable transport correlation separately
// from the canonical workflow lifecycle tables.
type WorkflowA2ATaskStore struct {
	state *WorkflowStateStore
	now   func() time.Time
}

func NewWorkflowA2ATaskStore(store *Store) (*WorkflowA2ATaskStore, error) {
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		return nil, err
	}
	return &WorkflowA2ATaskStore{state: state, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (s *WorkflowA2ATaskStore) PutA2ATaskCorrelation(ctx context.Context, correlation hoststate.A2ATaskCorrelation) (hoststate.A2ATaskCorrelation, workflowruntime.IdempotencyOutcome, error) {
	if correlation.CreatedAt.IsZero() {
		correlation.CreatedAt = s.now().UTC()
	}
	if err := correlation.Validate(); err != nil {
		return hoststate.A2ATaskCorrelation{}, "", workflowInvalid(fmt.Errorf("A2A correlation: %w", err))
	}
	definition, err := encodeWorkflowJSON(correlation.Definition)
	if err != nil {
		return hoststate.A2ATaskCorrelation{}, "", err
	}
	owner, err := encodeWorkflowJSON(correlation.Owner)
	if err != nil {
		return hoststate.A2ATaskCorrelation{}, "", err
	}
	result, outcome := correlation, workflowruntime.IdempotencyApplied
	err = s.state.write(ctx, "put workflow A2A task correlation", func(query workflowSQL) error {
		prior, loadErr := loadA2ATaskCorrelation(ctx, query, correlation.TaskID)
		if loadErr == nil {
			if !sameA2ATaskIntent(prior, correlation) {
				return fmt.Errorf("%w: A2A task id is bound to different immutable intent", workflowruntime.ErrIdempotencyConflict)
			}
			result, outcome = prior, workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		_, execErr := query.ExecContext(ctx, `INSERT INTO workflow_a2a_tasks(task_id, run_id, definition_json, request_digest, idempotency_key, host_start_key, owner_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, correlation.TaskID, correlation.RunID, definition, correlation.RequestDigest, correlation.IdempotencyKey, correlation.HostStartKey, owner, workflowTime(correlation.CreatedAt))
		if execErr != nil {
			if isSQLiteConstraint(execErr) {
				return fmt.Errorf("%w: A2A task or run correlation conflicts", workflowruntime.ErrIdempotencyConflict)
			}
			return fmt.Errorf("insert workflow A2A task correlation: %w", execErr)
		}
		return nil
	})
	return result, outcome, err
}

func sameA2ATaskIntent(left, right hoststate.A2ATaskCorrelation) bool {
	return left.TaskID == right.TaskID && left.RunID == right.RunID && left.Definition == right.Definition &&
		left.RequestDigest == right.RequestDigest && left.IdempotencyKey == right.IdempotencyKey && left.HostStartKey == right.HostStartKey && reflect.DeepEqual(left.Owner, right.Owner)
}

func (s *WorkflowA2ATaskStore) GetA2ATaskCorrelation(ctx context.Context, taskID string) (hoststate.A2ATaskCorrelation, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.A2ATaskCorrelation{}, err
	}
	return loadA2ATaskCorrelation(ctx, s.state.db, taskID)
}

func loadA2ATaskCorrelation(ctx context.Context, query workflowSQL, taskID string) (hoststate.A2ATaskCorrelation, error) {
	var correlation hoststate.A2ATaskCorrelation
	var runID, definition, owner, created string
	err := query.QueryRowContext(ctx, `SELECT task_id, run_id, definition_json, request_digest, idempotency_key, host_start_key, owner_json, created_at FROM workflow_a2a_tasks WHERE task_id = ?`, taskID).Scan(&correlation.TaskID, &runID, &definition, &correlation.RequestDigest, &correlation.IdempotencyKey, &correlation.HostStartKey, &owner, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return hoststate.A2ATaskCorrelation{}, fmt.Errorf("%w: A2A task", workflowruntime.ErrNotFound)
	}
	if err != nil {
		return hoststate.A2ATaskCorrelation{}, fmt.Errorf("load workflow A2A task correlation: %w", err)
	}
	correlation.RunID = workflowruntime.RunID(runID)
	if decodeErr := decodeWorkflowJSON("workflow A2A definition", definition, &correlation.Definition); decodeErr != nil {
		return hoststate.A2ATaskCorrelation{}, decodeErr
	}
	if decodeErr := decodeWorkflowJSON("workflow A2A owner", owner, &correlation.Owner); decodeErr != nil {
		return hoststate.A2ATaskCorrelation{}, decodeErr
	}
	createdAt, err := parseWorkflowTime("A2A correlation created_at", created)
	if err != nil {
		return hoststate.A2ATaskCorrelation{}, err
	}
	correlation.CreatedAt = createdAt
	if err := correlation.Validate(); err != nil {
		return hoststate.A2ATaskCorrelation{}, workflowInvalid(fmt.Errorf("stored A2A correlation is corrupt: %w", err))
	}
	return correlation.Clone(), nil
}
