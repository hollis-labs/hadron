package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

// SaveValues implements runtime.StateStore with an immutable, digest-bound
// value-set row. SQLite assigns the opaque reference identity.
func (s *WorkflowStateStore) SaveValues(ctx context.Context, request workflowruntime.SaveValuesRequest) (values.ValueSetRef, error) {
	if err := request.Owner.Validate(); err != nil {
		return values.ValueSetRef{}, workflowInvalid(err)
	}
	if err := request.Values.Validate(); err != nil {
		return values.ValueSetRef{}, workflowInvalid(err)
	}
	ownerJSON, ownerEncodeErr := encodeWorkflowJSON(request.Owner)
	if ownerEncodeErr != nil {
		return values.ValueSetRef{}, ownerEncodeErr
	}
	valuesJSON, valuesEncodeErr := encodeWorkflowJSON(request.Values)
	if valuesEncodeErr != nil {
		return values.ValueSetRef{}, valuesEncodeErr
	}
	digest, digestErr := values.DigestValueSet(request.Values)
	if digestErr != nil {
		return values.ValueSetRef{}, workflowInvalid(digestErr)
	}

	var ref values.ValueSetRef
	writeErr := s.write(ctx, "save workflow values", func(query workflowSQL) error {
		result, err := query.ExecContext(ctx, `
INSERT INTO workflow_value_sets(digest, owner_json, values_json)
VALUES (?, ?, ?)`, digest, ownerJSON, valuesJSON)
		if err != nil {
			return fmt.Errorf("insert workflow value set: %w", err)
		}
		sequence, err := result.LastInsertId()
		if err != nil {
			return fmt.Errorf("read workflow value-set sequence: %w", err)
		}
		ref = values.ValueSetRef{ID: workflowValueID(sequence), Digest: digest}
		return ref.Validate()
	})
	if writeErr != nil {
		return values.ValueSetRef{}, writeErr
	}
	return ref, nil
}

// LoadValues implements runtime.StateStore and verifies both the caller's
// digest and the immutable row's content digest before returning decoded data.
func (s *WorkflowStateStore) LoadValues(ctx context.Context, ref values.ValueSetRef) (values.ValueSet, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		return nil, workflowInvalid(err)
	}
	sequence, parseErr := parseWorkflowValueID(ref.ID)
	if parseErr != nil {
		return nil, parseErr
	}
	var storedDigest, valuesJSON string
	if err := s.db.QueryRowContext(ctx, `
SELECT digest, values_json FROM workflow_value_sets WHERE sequence = ?`, sequence).Scan(
		&storedDigest, &valuesJSON,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: value set %q", workflowruntime.ErrNotFound, ref.ID)
		}
		return nil, fmt.Errorf("load workflow value set: %w", err)
	}
	if storedDigest != ref.Digest {
		return nil, fmt.Errorf("%w: value set digest", workflowruntime.ErrCASMismatch)
	}
	var set values.ValueSet
	if err := decodeWorkflowJSON("value set", valuesJSON, &set); err != nil {
		return nil, err
	}
	computed, err := values.DigestValueSet(set)
	if err != nil {
		return nil, workflowInvalid(err)
	}
	if computed != storedDigest {
		return nil, workflowInvalid(errors.New("persisted value-set content does not match its digest"))
	}
	return set, nil
}

// PutCacheEntry implements runtime.StateStore. A cache key is a mutable index
// to immutable output values, so a later valid entry replaces the prior row.
func (s *WorkflowStateStore) PutCacheEntry(ctx context.Context, entry workflowruntime.CacheEntry) error {
	if err := entry.Validate(); err != nil {
		return workflowInvalid(err)
	}
	outputsJSON, err := encodeWorkflowJSON(entry.Outputs)
	if err != nil {
		return err
	}
	return s.write(ctx, "put workflow cache entry", func(query workflowSQL) error {
		_, err := query.ExecContext(ctx, `
INSERT INTO workflow_cache_entries(
    cache_key, plan_digest, node_id, input_digest, outputs_ref_json,
    created_at, expires_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(cache_key) DO UPDATE SET
    plan_digest = excluded.plan_digest,
    node_id = excluded.node_id,
    input_digest = excluded.input_digest,
    outputs_ref_json = excluded.outputs_ref_json,
    created_at = excluded.created_at,
    expires_at = excluded.expires_at`,
			entry.Key, entry.PlanDigest, entry.NodeID, entry.InputDigest, outputsJSON,
			workflowTime(entry.CreatedAt), workflowOptionalTime(entry.ExpiresAt),
		)
		if err != nil {
			return fmt.Errorf("put workflow cache entry: %w", err)
		}
		return nil
	})
}

// GetCacheEntry implements runtime.StateStore. Expired rows are retained for
// later maintenance but are never returned as cache hits.
func (s *WorkflowStateStore) GetCacheEntry(ctx context.Context, key string, now time.Time) (workflowruntime.CacheEntry, bool, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.CacheEntry{}, false, err
	}
	entry, err := loadWorkflowCacheEntry(ctx, s.db, key)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return workflowruntime.CacheEntry{}, false, nil
	}
	if err != nil {
		return workflowruntime.CacheEntry{}, false, err
	}
	if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now) {
		return workflowruntime.CacheEntry{}, false, nil
	}
	return entry, true, nil
}

func loadWorkflowCacheEntry(ctx context.Context, query workflowSQL, key string) (workflowruntime.CacheEntry, error) {
	var (
		entry                  workflowruntime.CacheEntry
		outputsJSON, createdAt string
		expiresAt              sql.NullString
	)
	if err := query.QueryRowContext(ctx, `
SELECT cache_key, plan_digest, node_id, input_digest, outputs_ref_json,
       created_at, expires_at
FROM workflow_cache_entries WHERE cache_key = ?`, key).Scan(
		&entry.Key, &entry.PlanDigest, &entry.NodeID, &entry.InputDigest,
		&outputsJSON, &createdAt, &expiresAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.CacheEntry{}, fmt.Errorf("%w: cache entry %q", workflowruntime.ErrNotFound, key)
		}
		return workflowruntime.CacheEntry{}, fmt.Errorf("load workflow cache entry: %w", err)
	}
	if err := decodeWorkflowJSON("cache outputs", outputsJSON, &entry.Outputs); err != nil {
		return workflowruntime.CacheEntry{}, err
	}
	var err error
	if entry.CreatedAt, err = parseWorkflowTime("cache created_at", createdAt); err != nil {
		return workflowruntime.CacheEntry{}, err
	}
	if entry.ExpiresAt, err = parseOptionalWorkflowTime("cache expires_at", expiresAt); err != nil {
		return workflowruntime.CacheEntry{}, err
	}
	if err := entry.Validate(); err != nil {
		return workflowruntime.CacheEntry{}, workflowInvalid(err)
	}
	return entry, nil
}

// PutPinnedValue implements runtime.StateStore. Pin keys are mutable stable
// names, while their target value-set references remain immutable.
func (s *WorkflowStateStore) PutPinnedValue(ctx context.Context, pin workflowruntime.PinnedValue) error {
	if err := pin.Validate(); err != nil {
		return workflowInvalid(err)
	}
	valueJSON, err := encodeWorkflowJSON(pin.Value)
	if err != nil {
		return err
	}
	return s.write(ctx, "put workflow pinned value", func(query workflowSQL) error {
		_, err := query.ExecContext(ctx, `
INSERT INTO workflow_pinned_values(pin_key, value_ref_json, pinned_at, expires_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(pin_key) DO UPDATE SET
    value_ref_json = excluded.value_ref_json,
    pinned_at = excluded.pinned_at,
    expires_at = excluded.expires_at`,
			pin.Key, valueJSON, workflowTime(pin.PinnedAt), workflowOptionalTime(pin.ExpiresAt),
		)
		if err != nil {
			return fmt.Errorf("put workflow pinned value: %w", err)
		}
		return nil
	})
}

// GetPinnedValue implements runtime.StateStore and excludes expired pins.
func (s *WorkflowStateStore) GetPinnedValue(ctx context.Context, key string, now time.Time) (workflowruntime.PinnedValue, bool, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.PinnedValue{}, false, err
	}
	pin, err := loadWorkflowPinnedValue(ctx, s.db, key)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return workflowruntime.PinnedValue{}, false, nil
	}
	if err != nil {
		return workflowruntime.PinnedValue{}, false, err
	}
	if !pin.ExpiresAt.IsZero() && !pin.ExpiresAt.After(now) {
		return workflowruntime.PinnedValue{}, false, nil
	}
	return pin, true, nil
}

func loadWorkflowPinnedValue(ctx context.Context, query workflowSQL, key string) (workflowruntime.PinnedValue, error) {
	var (
		pin                 workflowruntime.PinnedValue
		valueJSON, pinnedAt string
		expiresAt           sql.NullString
	)
	if err := query.QueryRowContext(ctx, `
SELECT pin_key, value_ref_json, pinned_at, expires_at
FROM workflow_pinned_values WHERE pin_key = ?`, key).Scan(
		&pin.Key, &valueJSON, &pinnedAt, &expiresAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.PinnedValue{}, fmt.Errorf("%w: pinned value %q", workflowruntime.ErrNotFound, key)
		}
		return workflowruntime.PinnedValue{}, fmt.Errorf("load workflow pinned value: %w", err)
	}
	if err := decodeWorkflowJSON("pinned value", valueJSON, &pin.Value); err != nil {
		return workflowruntime.PinnedValue{}, err
	}
	var err error
	if pin.PinnedAt, err = parseWorkflowTime("pin pinned_at", pinnedAt); err != nil {
		return workflowruntime.PinnedValue{}, err
	}
	if pin.ExpiresAt, err = parseOptionalWorkflowTime("pin expires_at", expiresAt); err != nil {
		return workflowruntime.PinnedValue{}, err
	}
	if err := pin.Validate(); err != nil {
		return workflowruntime.PinnedValue{}, workflowInvalid(err)
	}
	return pin, nil
}

// ListPinnedValues implements runtime.StateStore in stable key order.
func (s *WorkflowStateStore) ListPinnedValues(ctx context.Context, now time.Time) ([]workflowruntime.PinnedValue, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT pin_key, value_ref_json, pinned_at, expires_at
FROM workflow_pinned_values ORDER BY pin_key`)
	if err != nil {
		return nil, fmt.Errorf("list workflow pinned values: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make([]workflowruntime.PinnedValue, 0)
	for rows.Next() {
		pin, err := scanWorkflowPinnedValue(rows)
		if err != nil {
			return nil, err
		}
		if pin.ExpiresAt.IsZero() || pin.ExpiresAt.After(now) {
			result = append(result, pin)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workflow pinned values: %w", err)
	}
	return result, nil
}

func scanWorkflowPinnedValue(row workflowScanner) (workflowruntime.PinnedValue, error) {
	var (
		pin                 workflowruntime.PinnedValue
		valueJSON, pinnedAt string
		expiresAt           sql.NullString
	)
	if err := row.Scan(&pin.Key, &valueJSON, &pinnedAt, &expiresAt); err != nil {
		return workflowruntime.PinnedValue{}, err
	}
	if err := decodeWorkflowJSON("pinned value", valueJSON, &pin.Value); err != nil {
		return workflowruntime.PinnedValue{}, err
	}
	var err error
	if pin.PinnedAt, err = parseWorkflowTime("pin pinned_at", pinnedAt); err != nil {
		return workflowruntime.PinnedValue{}, err
	}
	if pin.ExpiresAt, err = parseOptionalWorkflowTime("pin expires_at", expiresAt); err != nil {
		return workflowruntime.PinnedValue{}, err
	}
	if err := pin.Validate(); err != nil {
		return workflowruntime.PinnedValue{}, workflowInvalid(err)
	}
	return pin, nil
}

// RecordExternalActivation implements runtime.StateStore. One activation
// declaration may fire repeatedly; idempotency_key, not activation_id, is the
// immutable firing identity.
func (s *WorkflowStateStore) RecordExternalActivation(ctx context.Context, request workflowruntime.ExternalActivationRequest) (workflowruntime.ExternalActivationSnapshot, workflowruntime.IdempotencyOutcome, error) {
	if err := validateWorkflowActivation(request); err != nil {
		return workflowruntime.ExternalActivationSnapshot{}, "", workflowInvalid(err)
	}
	requestJSON, canonicalErr := canonicalActivationRequest(request)
	if canonicalErr != nil {
		return workflowruntime.ExternalActivationSnapshot{}, "", canonicalErr
	}
	var (
		result  workflowruntime.ExternalActivationSnapshot
		outcome workflowruntime.IdempotencyOutcome
	)
	writeErr := s.write(ctx, "record workflow external activation", func(query workflowSQL) error {
		priorRequest, priorResult, found, loadErr := loadWorkflowIdempotency(ctx, query, "workflow_external_activations", request.IdempotencyKey)
		if loadErr != nil {
			return loadErr
		}
		if found {
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("external activation", request.IdempotencyKey)
			}
			if decodeErr := decodeWorkflowJSON("external activation result", priorResult, &result); decodeErr != nil {
				return decodeErr
			}
			if validationErr := validateWorkflowActivationSnapshot(result); validationErr != nil {
				return workflowInvalid(validationErr)
			}
			if result.ActivationID != request.ActivationID || result.IdempotencyKey != request.IdempotencyKey ||
				result.RequestedRunID != request.RequestedRunID || result.Plan != request.Plan ||
				!equalWorkflowValueRef(result.Inputs, request.Inputs) || !result.OccurredAt.Equal(request.OccurredAt) {
				return workflowInvalid(errors.New("persisted external-activation result does not match its request"))
			}
			outcome = workflowruntime.IdempotencyReplayed
			return nil
		}
		if planErr := ensureWorkflowPlan(ctx, query, request.Plan); planErr != nil {
			return planErr
		}
		result = workflowruntime.ExternalActivationSnapshot{
			ActivationID: request.ActivationID, IdempotencyKey: request.IdempotencyKey,
			RequestedRunID: request.RequestedRunID, Plan: request.Plan,
			Inputs: cloneWorkflowValueRef(request.Inputs), OccurredAt: request.OccurredAt.UTC(),
		}
		resultJSON, err := encodeWorkflowJSON(result)
		if err != nil {
			return err
		}
		if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_external_activations(
    idempotency_key, activation_id, requested_run_id, request_json, result_json
) VALUES (?, ?, ?, ?, ?)`, request.IdempotencyKey, request.ActivationID,
			request.RequestedRunID, requestJSON, resultJSON,
		); err != nil {
			return fmt.Errorf("record workflow external activation: %w", err)
		}
		outcome = workflowruntime.IdempotencyApplied
		return nil
	})
	if writeErr != nil {
		return workflowruntime.ExternalActivationSnapshot{}, "", writeErr
	}
	return result, outcome, nil
}

// Recovery implements runtime.StateStore with deterministic, independently
// limited categories. Categories are persisted facts and may overlap.
func (s *WorkflowStateStore) Recovery(ctx context.Context, query workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.RecoverySnapshot{}, err
	}
	if query.Now.IsZero() || query.Limit < 0 {
		return workflowruntime.RecoverySnapshot{}, workflowInvalid(errors.New("recovery requires now and non-negative limit"))
	}
	result, err := s.loadWorkflowRecovery(ctx, query)
	if err != nil {
		return workflowruntime.RecoverySnapshot{}, err
	}
	return result, nil
}

func (s *WorkflowStateStore) loadWorkflowRecovery(ctx context.Context, query workflowruntime.RecoveryQuery) (workflowruntime.RecoverySnapshot, error) {
	var result workflowruntime.RecoverySnapshot
	runSQL := workflowRunSelect + ` WHERE r.status IN (?, ?, ?)`
	runArgs := []any{workflowruntime.RunPending, workflowruntime.RunRunning, workflowruntime.RunWaiting}
	if query.RunID != "" {
		runSQL += ` AND r.run_id = ?`
		runArgs = append(runArgs, query.RunID)
	}
	runSQL += ` ORDER BY r.run_id`
	runRows, runQueryErr := s.db.QueryContext(ctx, runSQL, runArgs...)
	if runQueryErr != nil {
		return result, fmt.Errorf("recover workflow runs: %w", runQueryErr)
	}
	for runRows.Next() {
		run, scanErr := scanWorkflowRun(runRows)
		if scanErr != nil {
			_ = runRows.Close()
			return result, scanErr
		}
		result.ActiveRuns = append(result.ActiveRuns, run)
	}
	if err := runRows.Err(); err != nil {
		_ = runRows.Close()
		return result, fmt.Errorf("recover workflow runs: %w", err)
	}
	_ = runRows.Close()

	nodeSQL := workflowNodeSelect
	nodeArgs := make([]any, 0, 1)
	if query.RunID != "" {
		nodeSQL += ` WHERE n.run_id = ?`
		nodeArgs = append(nodeArgs, query.RunID)
	}
	nodeRows, nodeQueryErr := s.db.QueryContext(ctx, nodeSQL, nodeArgs...)
	if nodeQueryErr != nil {
		return result, fmt.Errorf("recover workflow nodes: %w", nodeQueryErr)
	}
	for nodeRows.Next() {
		node, scanErr := scanWorkflowNode(nodeRows)
		if scanErr != nil {
			_ = nodeRows.Close()
			return result, scanErr
		}
		if node.Status == workflowruntime.NodeReady {
			result.Ready = append(result.Ready, node)
		}
		if node.Status == workflowruntime.NodeWaiting {
			result.Waiting = append(result.Waiting, node)
		}
		if node.Lease != nil {
			if node.Lease.ExpiresAt.After(query.Now) {
				result.Leased = append(result.Leased, node)
			} else {
				result.ExpiredLeases = append(result.ExpiredLeases, node)
			}
		}
	}
	if err := nodeRows.Err(); err != nil {
		_ = nodeRows.Close()
		return result, fmt.Errorf("recover workflow nodes: %w", err)
	}
	_ = nodeRows.Close()

	sortWorkflowRecoveryNodes(result.Ready)
	sortWorkflowRecoveryNodes(result.Waiting)
	sortWorkflowRecoveryNodes(result.Leased)
	sortWorkflowRecoveryNodes(result.ExpiredLeases)
	result.ActiveRuns = limitWorkflowRecovery(result.ActiveRuns, query.Limit)
	result.Ready = limitWorkflowRecovery(result.Ready, query.Limit)
	result.Waiting = limitWorkflowRecovery(result.Waiting, query.Limit)
	result.Leased = limitWorkflowRecovery(result.Leased, query.Limit)
	result.ExpiredLeases = limitWorkflowRecovery(result.ExpiredLeases, query.Limit)
	return result, nil
}

func sortWorkflowRecoveryNodes(nodes []workflowruntime.NodeInvocationSnapshot) {
	sort.Slice(nodes, func(left, right int) bool {
		if nodes[left].Priority != nodes[right].Priority {
			return nodes[left].Priority > nodes[right].Priority
		}
		if nodes[left].ID.RunID != nodes[right].ID.RunID {
			return nodes[left].ID.RunID < nodes[right].ID.RunID
		}
		if nodes[left].ID.NodeID != nodes[right].ID.NodeID {
			return nodes[left].ID.NodeID < nodes[right].ID.NodeID
		}
		return nodes[left].ID.Iteration < nodes[right].ID.Iteration
	})
}

func limitWorkflowRecovery[T any](items []T, limit int) []T {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func validateWorkflowActivation(request workflowruntime.ExternalActivationRequest) error {
	if strings.TrimSpace(request.ActivationID) == "" || request.IdempotencyKey == "" ||
		request.RequestedRunID == "" || request.OccurredAt.IsZero() {
		return errors.New("activation requires ids, idempotency key, and occurred_at")
	}
	if err := request.Plan.Validate(); err != nil {
		return err
	}
	if request.Inputs != nil {
		return request.Inputs.Validate()
	}
	return nil
}

func validateWorkflowActivationSnapshot(snapshot workflowruntime.ExternalActivationSnapshot) error {
	return validateWorkflowActivation(workflowruntime.ExternalActivationRequest(snapshot))
}
