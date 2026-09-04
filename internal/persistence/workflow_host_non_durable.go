package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
)

var _ hoststate.NonDurableJournal = (*WorkflowHostStore)(nil)

func (s *WorkflowHostStore) RecordNonDurableStart(ctx context.Context, record hoststate.NonDurableStartRecord) (hoststate.NonDurableStartRecord, workflowruntime.IdempotencyOutcome, error) {
	record.CompletedAt = record.CompletedAt.UTC()
	encoded, err := encodeWorkflowJSON(record)
	if err != nil {
		return hoststate.NonDurableStartRecord{}, "", err
	}
	var canonical hoststate.NonDurableStartRecord
	if decodeErr := decodeWorkflowJSON("non-durable workflow audit", encoded, &canonical); decodeErr != nil {
		return canonical, "", decodeErr
	}
	if validationErr := canonical.Validate(); validationErr != nil {
		return canonical, "", workflowInvalid(validationErr)
	}
	result, outcome := canonical, workflowruntime.IdempotencyApplied
	err = s.state.write(ctx, "record non-durable workflow audit", func(query workflowSQL) error {
		prior, loadErr := loadNonDurableStart(ctx, query, "run_id=? OR idempotency_key=?", canonical.RunID, canonical.StartKey)
		if loadErr == nil {
			if prior.RunID != canonical.RunID || prior.StartKey != canonical.StartKey || prior.RequestDigest != canonical.RequestDigest || prior.Plan != canonical.Plan || !reflect.DeepEqual(prior.Identity, canonical.Identity) {
				return &workflowruntime.IdempotencyConflictError{Operation: "non-durable workflow start", Key: canonical.StartKey}
			}
			result, outcome = prior, workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		_, execErr := query.ExecContext(ctx, `INSERT INTO workflow_non_durable_runs(run_id,idempotency_key,request_digest,record_json,completed_at) VALUES (?,?,?,?,?)`, canonical.RunID, canonical.StartKey, canonical.RequestDigest, encoded, workflowTime(canonical.CompletedAt))
		if isSQLiteConstraint(execErr) {
			return &workflowruntime.IdempotencyConflictError{Operation: "non-durable workflow start", Key: canonical.StartKey}
		}
		return execErr
	})
	return result, outcome, err
}

func (s *WorkflowHostStore) LoadNonDurableStart(ctx context.Context, runID workflowruntime.RunID) (hoststate.NonDurableStartRecord, error) {
	return loadNonDurableStart(ctx, s.state.db, "run_id=?", runID)
}

func (s *WorkflowHostStore) LoadNonDurableStartByKey(ctx context.Context, key string) (hoststate.NonDurableStartRecord, error) {
	return loadNonDurableStart(ctx, s.state.db, "idempotency_key=?", key)
}

func loadNonDurableStart(ctx context.Context, query workflowSQL, predicate string, args ...any) (hoststate.NonDurableStartRecord, error) {
	var encoded string
	if err := query.QueryRowContext(ctx, `SELECT record_json FROM workflow_non_durable_runs WHERE `+predicate, args...).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return hoststate.NonDurableStartRecord{}, fmt.Errorf("%w: non-durable workflow audit", workflowruntime.ErrNotFound)
		}
		return hoststate.NonDurableStartRecord{}, err
	}
	var result hoststate.NonDurableStartRecord
	if err := decodeWorkflowJSON("non-durable workflow audit", encoded, &result); err != nil {
		return result, err
	}
	return result, result.Validate()
}
