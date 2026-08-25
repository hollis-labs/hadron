package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

// WorkflowHostStore is Hadron's SQLite-owned application journal. It shares a
// database and transaction discipline with WorkflowStateStore but is not part
// of the extraction-ready runtime.StateStore contract.
type WorkflowHostStore struct {
	state *WorkflowStateStore
	now   func() time.Time
}

func NewWorkflowHostStore(store *Store) (*WorkflowHostStore, error) {
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		return nil, err
	}
	return &WorkflowHostStore{state: state, now: func() time.Time { return time.Now().UTC() }}, nil
}

func canonicalHostStart(record hoststate.StartRecord) (hoststate.StartRecord, string, error) {
	if record.Snapshot == nil {
		sealed, err := hoststate.SealPlanSnapshot(hoststate.PlanSnapshot{
			SchemaVersion: hoststate.PlanSnapshotSchemaVersion, Plan: record.Plan,
			SourceMap: record.Plan.SourceMap,
			Compile:   hoststate.UnavailableCompileDescriptor("definition provider does not expose deterministic compile metadata or exact source material"),
		})
		if err != nil {
			return hoststate.StartRecord{}, "", err
		}
		record.Snapshot = &sealed
	}
	planSnapshot, err := record.Snapshot.Clone()
	if err != nil {
		return hoststate.StartRecord{}, "", err
	}
	if validationErr := planSnapshot.Validate(); validationErr != nil {
		return hoststate.StartRecord{}, "", fmt.Errorf("invalid workflow plan snapshot: %w", validationErr)
	}
	record.Snapshot = &planSnapshot
	record.Identity = record.Identity.Clone()
	record.Facts.Identity = record.Facts.Identity.Clone()
	record.Facts.RunScope = record.Facts.RunScope.Clone()
	if record.Facts.ExecutionTarget != nil {
		target := record.Facts.ExecutionTarget.Clone()
		record.Facts.ExecutionTarget = &target
	}
	record.Run.CreatedAt = record.Run.CreatedAt.UTC()
	record.RecordedAt = record.RecordedAt.UTC()
	record.Decision.DecidedAt = record.Decision.DecidedAt.UTC()
	if record.Activation != nil {
		copyActivation := *record.Activation
		copyActivation.OccurredAt = copyActivation.OccurredAt.UTC()
		record.Activation = &copyActivation
	}
	encoded, err := encodeWorkflowJSON(record)
	if err != nil {
		return hoststate.StartRecord{}, "", err
	}
	var cloned hoststate.StartRecord
	if err := decodeWorkflowJSON("workflow host start", encoded, &cloned); err != nil {
		return hoststate.StartRecord{}, "", err
	}
	cloned.Snapshot = &planSnapshot
	return cloned, encoded, nil
}

func canonicalPolicyEvaluation(evaluation hoststate.PolicyEvaluation) (hoststate.PolicyEvaluation, string, error) {
	evaluation.Facts.Identity = evaluation.Facts.Identity.Clone()
	evaluation.Facts.RunScope = evaluation.Facts.RunScope.Clone()
	if evaluation.Facts.ExecutionTarget != nil {
		target := evaluation.Facts.ExecutionTarget.Clone()
		evaluation.Facts.ExecutionTarget = &target
	}
	evaluation.Decision.DecidedAt = evaluation.Decision.DecidedAt.UTC()
	encoded, err := encodeWorkflowJSON(evaluation)
	if err != nil {
		return hoststate.PolicyEvaluation{}, "", err
	}
	var cloned hoststate.PolicyEvaluation
	if err := decodeWorkflowJSON("workflow host policy evaluation", encoded, &cloned); err != nil {
		return hoststate.PolicyEvaluation{}, "", err
	}
	return cloned, encoded, nil
}

func (s *WorkflowHostStore) RecordStart(ctx context.Context, input hoststate.StartRecord) (hoststate.StartSnapshot, workflowruntime.IdempotencyOutcome, error) {
	input, encoded, err := canonicalHostStart(input)
	if err != nil {
		return hoststate.StartSnapshot{}, "", err
	}
	if validationErr := input.Validate(); validationErr != nil {
		return hoststate.StartSnapshot{}, "", fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, validationErr)
	}
	var snapshot hoststate.StartSnapshot
	outcome := workflowruntime.IdempotencyApplied
	err = s.state.write(ctx, "record workflow host start", func(query workflowSQL) error {
		prior, loadErr := loadHostStart(ctx, query, "run_id = ? OR idempotency_key = ?", input.Run.ID, input.StartKey)
		if loadErr == nil {
			priorJSON, encodeErr := encodeWorkflowJSON(prior.Record)
			if encodeErr != nil {
				return encodeErr
			}
			if priorJSON != encoded {
				return &workflowruntime.IdempotencyConflictError{Operation: "record workflow host start", Key: input.StartKey}
			}
			snapshot, outcome = prior, workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		if snapshotErr := ensureWorkflowPlanSnapshot(ctx, query, *input.Snapshot, workflowTime(input.RecordedAt)); snapshotErr != nil {
			return snapshotErr
		}
		if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_host_starts(run_id, idempotency_key, request_json, recorded_at) VALUES (?, ?, ?, ?)`, input.Run.ID, input.StartKey, encoded, workflowTime(input.RecordedAt)); execErr != nil {
			if isSQLiteConstraint(execErr) {
				return &workflowruntime.IdempotencyConflictError{Operation: "record workflow host start", Key: input.StartKey}
			}
			return fmt.Errorf("insert workflow host start: %w", execErr)
		}
		if linkErr := linkWorkflowPlanSnapshot(ctx, query, input.Run.ID, *input.Snapshot); linkErr != nil {
			return linkErr
		}
		if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_host_start_progress(run_id, phase, generation, updated_at) VALUES (?, ?, 1, ?)`, input.Run.ID, hoststate.StartRecorded, workflowTime(input.RecordedAt)); execErr != nil {
			return fmt.Errorf("insert workflow host progress: %w", execErr)
		}
		evaluation := hoststate.PolicyEvaluation{StartKey: input.StartKey, RequestDigest: input.RequestDigest, Facts: input.Facts, Decision: input.Decision}
		decisionJSON, encodeErr := encodeWorkflowJSON(evaluation)
		if encodeErr != nil {
			return encodeErr
		}
		var priorDecision string
		decisionErr := query.QueryRowContext(ctx, `SELECT decision_json FROM workflow_host_policy_decisions WHERE decision_id = ? OR start_key = ?`, input.Decision.ID, input.StartKey).Scan(&priorDecision)
		if decisionErr == nil && priorDecision != decisionJSON {
			return fmt.Errorf("%w: policy decision %s", hoststate.ErrConflict, input.Decision.ID)
		}
		if errors.Is(decisionErr, sql.ErrNoRows) {
			if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_host_policy_decisions(decision_id, start_key, run_id, decision_json, decided_at) VALUES (?, ?, ?, ?, ?)`, input.Decision.ID, input.StartKey, input.Run.ID, decisionJSON, workflowTime(input.Decision.DecidedAt)); execErr != nil {
				return fmt.Errorf("insert workflow start policy decision: %w", execErr)
			}
		} else if decisionErr != nil {
			return decisionErr
		}
		snapshot = hoststate.StartSnapshot{Record: input, Phase: hoststate.StartRecorded, Generation: 1, UpdatedAt: input.RecordedAt}
		return nil
	})
	return snapshot, outcome, err
}

func (s *WorkflowHostStore) LoadStart(ctx context.Context, runID workflowruntime.RunID) (hoststate.StartSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.StartSnapshot{}, err
	}
	return loadHostStart(ctx, s.state.db, "run_id = ?", runID)
}

func (s *WorkflowHostStore) LoadStartByKey(ctx context.Context, key string) (hoststate.StartSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.StartSnapshot{}, err
	}
	return loadHostStart(ctx, s.state.db, "idempotency_key = ?", key)
}

func loadHostStart(ctx context.Context, query workflowSQL, predicate string, arguments ...any) (hoststate.StartSnapshot, error) {
	var encoded, phase, updated string
	var generation int64
	err := query.QueryRowContext(ctx, `SELECT s.request_json, p.phase, p.generation, p.updated_at FROM workflow_host_starts s JOIN workflow_host_start_progress p ON p.run_id = s.run_id WHERE s.`+predicate, arguments...).Scan(&encoded, &phase, &generation, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return hoststate.StartSnapshot{}, fmt.Errorf("%w: workflow host start", workflowruntime.ErrNotFound)
	}
	if err != nil {
		return hoststate.StartSnapshot{}, fmt.Errorf("load workflow host start: %w", err)
	}
	var record hoststate.StartRecord
	if decodeErr := decodeWorkflowJSON("workflow host start", encoded, &record); decodeErr != nil {
		return hoststate.StartSnapshot{}, decodeErr
	}
	planSnapshot, snapshotErr := loadWorkflowPlanSnapshotForRun(ctx, query, record.Run.ID)
	if errors.Is(snapshotErr, workflowruntime.ErrNotFound) {
		// Starts created before migration 0028 retain their exact plan in the
		// immutable request JSON, but have no recoverable raw source/compiler
		// material. Preserve that compatibility as an explicit unavailable state.
		planSnapshot, snapshotErr = hoststate.SealPlanSnapshot(hoststate.PlanSnapshot{
			SchemaVersion: hoststate.PlanSnapshotSchemaVersion, Plan: record.Plan,
			SourceMap: record.Plan.SourceMap,
			Compile:   hoststate.UnavailableCompileDescriptor("legacy durable start predates exact source and compile snapshot capture"),
		})
	}
	if snapshotErr != nil {
		return hoststate.StartSnapshot{}, snapshotErr
	}
	record.Snapshot = &planSnapshot
	parsed, err := parseWorkflowTime("host start updated_at", updated)
	if err != nil {
		return hoststate.StartSnapshot{}, err
	}
	gen, err := workflowGeneration("host start generation", generation)
	if err != nil {
		return hoststate.StartSnapshot{}, err
	}
	snapshot := hoststate.StartSnapshot{Record: record, Phase: hoststate.StartPhase(phase), Generation: gen, UpdatedAt: parsed}
	if err := snapshot.Validate(); err != nil {
		return hoststate.StartSnapshot{}, fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, err)
	}
	return snapshot, nil
}

func (s *WorkflowHostStore) ListIncompleteStarts(ctx context.Context, limit int) ([]hoststate.StartSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("%w: negative host recovery limit", hoststate.ErrInvalidRecord)
	}
	statement := `SELECT s.request_json, p.phase, p.generation, p.updated_at FROM workflow_host_starts s JOIN workflow_host_start_progress p ON p.run_id = s.run_id WHERE p.phase NOT IN (?, ?, ?) ORDER BY p.updated_at, s.run_id`
	args := []any{hoststate.StartRunning, hoststate.StartDryRunComplete, hoststate.StartPinsRejected}
	if limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.state.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]hoststate.StartSnapshot, 0)
	for rows.Next() {
		var encoded, phase, updated string
		var generation int64
		if err := rows.Scan(&encoded, &phase, &generation, &updated); err != nil {
			return nil, err
		}
		var record hoststate.StartRecord
		if err := decodeWorkflowJSON("workflow host start", encoded, &record); err != nil {
			return nil, err
		}
		parsed, err := parseWorkflowTime("host start updated_at", updated)
		if err != nil {
			return nil, err
		}
		gen, err := workflowGeneration("host start generation", generation)
		if err != nil {
			return nil, err
		}
		result = append(result, hoststate.StartSnapshot{Record: record, Phase: hoststate.StartPhase(phase), Generation: gen, UpdatedAt: parsed})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for index := range result {
		planSnapshot, err := loadWorkflowPlanSnapshotForRun(ctx, s.state.db, result[index].Record.Run.ID)
		if errors.Is(err, workflowruntime.ErrNotFound) {
			planSnapshot, err = hoststate.SealPlanSnapshot(hoststate.PlanSnapshot{
				SchemaVersion: hoststate.PlanSnapshotSchemaVersion, Plan: result[index].Record.Plan,
				SourceMap: result[index].Record.Plan.SourceMap,
				Compile:   hoststate.UnavailableCompileDescriptor("legacy durable start predates exact source and compile snapshot capture"),
			})
		}
		if err != nil {
			return nil, err
		}
		result[index].Record.Snapshot = &planSnapshot
		if err := result[index].Validate(); err != nil {
			return nil, fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, err)
		}
	}
	return result, nil
}

func (s *WorkflowHostStore) AdvanceStart(ctx context.Context, request hoststate.AdvanceStartRequest) (hoststate.StartSnapshot, error) {
	if request.RunID == "" || request.ExpectedGeneration == 0 || !validStartEdge(request.From, request.To) || request.At.IsZero() {
		return hoststate.StartSnapshot{}, fmt.Errorf("%w: invalid start checkpoint request", hoststate.ErrInvalidRecord)
	}
	request.At = request.At.UTC()
	var result hoststate.StartSnapshot
	err := s.state.write(ctx, "advance workflow host start", func(query workflowSQL) error {
		current, err := loadHostStart(ctx, query, "run_id = ?", request.RunID)
		if err != nil {
			return err
		}
		if startPhaseRank(current.Phase) >= startPhaseRank(request.To) {
			result = current
			return nil
		}
		if current.Generation != request.ExpectedGeneration {
			return workflowCAS("workflow host start", request.ExpectedGeneration, current.Generation)
		}
		if current.Phase != request.From {
			return fmt.Errorf("%w: start phase is %s, expected %s", hoststate.ErrConflict, current.Phase, request.From)
		}
		if request.At.Before(current.UpdatedAt) {
			return fmt.Errorf("%w: start checkpoint time regressed", hoststate.ErrInvalidRecord)
		}
		res, execErr := query.ExecContext(ctx, `UPDATE workflow_host_start_progress SET phase = ?, generation = generation + 1, updated_at = ? WHERE run_id = ? AND phase = ? AND generation = ?`, request.To, workflowTime(request.At), request.RunID, request.From, request.ExpectedGeneration)
		if execErr != nil {
			return execErr
		}
		if err := expectOneWorkflowRow(res, "workflow host start", request.ExpectedGeneration, current.Generation); err != nil {
			return err
		}
		result = current
		result.Phase, result.Generation, result.UpdatedAt = request.To, current.Generation+1, request.At
		return nil
	})
	return result, err
}

func validStartEdge(from, to hoststate.StartPhase) bool {
	return (from == hoststate.StartRecorded && (to == hoststate.StartRunCreated || to == hoststate.StartDryRunComplete)) ||
		(from == hoststate.StartRunCreated && to == hoststate.StartNodesMaterialized) ||
		(from == hoststate.StartNodesMaterialized && (to == hoststate.StartPinsBound || to == hoststate.StartPinsRejected)) ||
		(from == hoststate.StartPinsBound && to == hoststate.StartRunning)
}

func startPhaseRank(phase hoststate.StartPhase) int {
	switch phase {
	case hoststate.StartRecorded:
		return 1
	case hoststate.StartRunCreated:
		return 2
	case hoststate.StartNodesMaterialized:
		return 3
	case hoststate.StartPinsBound, hoststate.StartPinsRejected:
		return 4
	case hoststate.StartRunning, hoststate.StartDryRunComplete:
		return 5
	default:
		return 0
	}
}

func (s *WorkflowHostStore) RecordPolicyEvaluation(ctx context.Context, evaluation hoststate.PolicyEvaluation) (hoststate.PolicyEvaluation, workflowruntime.IdempotencyOutcome, error) {
	evaluation, encoded, err := canonicalPolicyEvaluation(evaluation)
	if err != nil {
		return hoststate.PolicyEvaluation{}, "", err
	}
	if validationErr := evaluation.Validate(); validationErr != nil {
		return hoststate.PolicyEvaluation{}, "", fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, validationErr)
	}
	result := evaluation
	outcome := workflowruntime.IdempotencyApplied
	err = s.state.write(ctx, "append workflow host policy decision", func(query workflowSQL) error {
		var prior string
		loadErr := query.QueryRowContext(ctx, `SELECT decision_json FROM workflow_host_policy_decisions WHERE decision_id = ? OR start_key = ?`, evaluation.Decision.ID, evaluation.StartKey).Scan(&prior)
		if loadErr == nil {
			if decodeErr := decodeWorkflowJSON("workflow host policy decision", prior, &result); decodeErr != nil {
				return decodeErr
			}
			if result.StartKey != evaluation.StartKey || result.RequestDigest != evaluation.RequestDigest || result.Decision.ID != evaluation.Decision.ID {
				return &workflowruntime.IdempotencyConflictError{Operation: "workflow policy evaluation", Key: evaluation.StartKey}
			}
			outcome = workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return loadErr
		}
		if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_host_policy_decisions(decision_id, start_key, run_id, decision_json, decided_at) VALUES (?, ?, ?, ?, ?)`, evaluation.Decision.ID, evaluation.StartKey, evaluation.Decision.RunID, encoded, workflowTime(evaluation.Decision.DecidedAt)); execErr != nil {
			if isSQLiteConstraint(execErr) {
				return &workflowruntime.IdempotencyConflictError{Operation: "workflow policy evaluation", Key: evaluation.StartKey}
			}
			return execErr
		}
		return nil
	})
	return result, outcome, err
}

func (s *WorkflowHostStore) LoadPolicyEvaluation(ctx context.Context, decisionID string) (hoststate.PolicyEvaluation, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.PolicyEvaluation{}, err
	}
	var encoded string
	err := s.state.db.QueryRowContext(ctx, `SELECT decision_json FROM workflow_host_policy_decisions WHERE decision_id = ?`, decisionID).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return hoststate.PolicyEvaluation{}, fmt.Errorf("%w: workflow host policy evaluation", workflowruntime.ErrNotFound)
	}
	if err != nil {
		return hoststate.PolicyEvaluation{}, err
	}
	var evaluation hoststate.PolicyEvaluation
	if err := decodeWorkflowJSON("workflow host policy evaluation", encoded, &evaluation); err != nil {
		return hoststate.PolicyEvaluation{}, err
	}
	if err := evaluation.Validate(); err != nil {
		return hoststate.PolicyEvaluation{}, fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, err)
	}
	return evaluation, nil
}

func (s *WorkflowHostStore) LoadPolicyEvaluationByStartKey(ctx context.Context, startKey string) (hoststate.PolicyEvaluation, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return hoststate.PolicyEvaluation{}, err
	}
	var encoded string
	err := s.state.db.QueryRowContext(ctx, `SELECT decision_json FROM workflow_host_policy_decisions WHERE start_key = ?`, startKey).Scan(&encoded)
	if errors.Is(err, sql.ErrNoRows) {
		return hoststate.PolicyEvaluation{}, fmt.Errorf("%w: workflow host policy evaluation", workflowruntime.ErrNotFound)
	}
	if err != nil {
		return hoststate.PolicyEvaluation{}, err
	}
	var evaluation hoststate.PolicyEvaluation
	if err := decodeWorkflowJSON("workflow host policy evaluation", encoded, &evaluation); err != nil {
		return hoststate.PolicyEvaluation{}, err
	}
	if err := evaluation.Validate(); err != nil {
		return hoststate.PolicyEvaluation{}, fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, err)
	}
	return evaluation, nil
}

func (s *WorkflowHostStore) ListPolicyDecisions(ctx context.Context, runID workflowruntime.RunID) ([]hoststate.PolicyDecision, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	rows, err := s.state.db.QueryContext(ctx, `SELECT decision_json FROM workflow_host_policy_decisions WHERE run_id = ? ORDER BY decided_at, decision_id`, runID)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]hoststate.PolicyDecision, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var evaluation hoststate.PolicyEvaluation
		if err := decodeWorkflowJSON("host policy decision", encoded, &evaluation); err != nil {
			return nil, err
		}
		if err := evaluation.Validate(); err != nil {
			return nil, err
		}
		result = append(result, evaluation.Decision)
	}
	return result, rows.Err()
}

func (s *WorkflowHostStore) ListRunNodes(ctx context.Context, runID workflowruntime.RunID) ([]workflowruntime.NodeInvocationSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	rows, err := s.state.db.QueryContext(ctx, workflowNodeSelect+` WHERE n.run_id = ? ORDER BY n.node_id, n.iteration`, runID)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]workflowruntime.NodeInvocationSnapshot, 0)
	for rows.Next() {
		node, err := scanWorkflowNode(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, node)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ID.NodeID == result[j].ID.NodeID {
			return result[i].ID.Iteration < result[j].ID.Iteration
		}
		return result[i].ID.NodeID < result[j].ID.NodeID
	})
	return result, rows.Err()
}

func (s *WorkflowHostStore) BindCancellation(ctx context.Context, request hoststate.BindCancellationRequest) (hoststate.CancellationBinding, workflowruntime.IdempotencyOutcome, error) {
	request.Intent.RequestedAt = request.Intent.RequestedAt.UTC()
	request.DefaultAt = request.DefaultAt.UTC()
	if err := request.Intent.Validate(); err != nil {
		return hoststate.CancellationBinding{}, "", fmt.Errorf("%w: invalid host cancellation: %w", hoststate.ErrInvalidRecord, err)
	}
	if request.DefaultAt.IsZero() {
		return hoststate.CancellationBinding{}, "", fmt.Errorf("%w: host cancellation default time is required", hoststate.ErrInvalidRecord)
	}
	requestJSON, err := encodeWorkflowJSON(request.Intent)
	if err != nil {
		return hoststate.CancellationBinding{}, "", err
	}
	var binding hoststate.CancellationBinding
	outcome := workflowruntime.IdempotencyApplied
	err = s.state.write(ctx, "bind workflow host cancellation", func(query workflowSQL) error {
		var priorRequest, priorBinding string
		loadErr := query.QueryRowContext(ctx, `SELECT request_json, binding_json FROM workflow_host_cancellations WHERE idempotency_key = ?`, request.Intent.IdempotencyKey).Scan(&priorRequest, &priorBinding)
		if loadErr == nil {
			if priorRequest != requestJSON {
				return &workflowruntime.IdempotencyConflictError{Operation: "bind workflow host cancellation", Key: request.Intent.IdempotencyKey}
			}
			if decodeErr := decodeWorkflowJSON("workflow host cancellation", priorBinding, &binding); decodeErr != nil {
				return decodeErr
			}
			if validationErr := binding.Validate(); validationErr != nil {
				return fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, validationErr)
			}
			outcome = workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return loadErr
		}
		run, loadErr := loadWorkflowRun(ctx, query, request.Intent.RunID)
		if loadErr != nil {
			return loadErr
		}
		if !run.Status.Active() {
			return &workflowruntime.TransitionError{Entity: "run", ID: string(run.ID), From: string(run.Status), To: string(workflowruntime.RunCanceled), Reason: "new host cancellation requires an active run"}
		}
		effectiveAt := request.Intent.RequestedAt
		if effectiveAt.IsZero() {
			effectiveAt = request.DefaultAt
			if effectiveAt.Before(run.UpdatedAt) {
				effectiveAt = run.UpdatedAt
			}
		} else if effectiveAt.Before(run.UpdatedAt) {
			return fmt.Errorf("%w: explicit cancellation time precedes the durable run", hoststate.ErrInvalidRecord)
		}
		binding = hoststate.CancellationBinding{Intent: request.Intent, EffectiveAt: effectiveAt, RecordedAt: request.DefaultAt}
		if validationErr := binding.Validate(); validationErr != nil {
			return fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, validationErr)
		}
		bindingJSON, encodeErr := encodeWorkflowJSON(binding)
		if encodeErr != nil {
			return encodeErr
		}
		if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_host_cancellations(idempotency_key, run_id, request_json, binding_json, recorded_at) VALUES (?, ?, ?, ?, ?)`, request.Intent.IdempotencyKey, request.Intent.RunID, requestJSON, bindingJSON, workflowTime(binding.RecordedAt)); execErr != nil {
			if isSQLiteConstraint(execErr) {
				return &workflowruntime.IdempotencyConflictError{Operation: "bind workflow host cancellation", Key: request.Intent.IdempotencyKey}
			}
			return execErr
		}
		return nil
	})
	return binding, outcome, err
}

// PrepareCancellation resolves the current CAS generation for a durable host
// intent. If the core operation already committed, it returns that exact
// persisted request so StateStore can replay its original semantic result.
func (s *WorkflowHostStore) PrepareCancellation(ctx context.Context, input hoststate.CancellationBinding) (workflowruntime.RequestRunCancellationRequest, error) {
	input.Intent.RequestedAt = input.Intent.RequestedAt.UTC()
	input.EffectiveAt = input.EffectiveAt.UTC()
	input.RecordedAt = input.RecordedAt.UTC()
	if err := input.Validate(); err != nil {
		return workflowruntime.RequestRunCancellationRequest{}, fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, err)
	}
	encoded, err := encodeWorkflowJSON(input)
	if err != nil {
		return workflowruntime.RequestRunCancellationRequest{}, err
	}
	var stored string
	if loadErr := s.state.db.QueryRowContext(ctx, `SELECT binding_json FROM workflow_host_cancellations WHERE idempotency_key = ?`, input.Intent.IdempotencyKey).Scan(&stored); loadErr != nil {
		if errors.Is(loadErr, sql.ErrNoRows) {
			return workflowruntime.RequestRunCancellationRequest{}, fmt.Errorf("%w: workflow host cancellation", workflowruntime.ErrNotFound)
		}
		return workflowruntime.RequestRunCancellationRequest{}, loadErr
	}
	if stored != encoded {
		return workflowruntime.RequestRunCancellationRequest{}, &workflowruntime.IdempotencyConflictError{Operation: "prepare workflow host cancellation", Key: input.Intent.IdempotencyKey}
	}
	priorRequest, _, found, err := loadWorkflowIdempotency(ctx, s.state.db, "workflow_run_cancellation_idempotency", input.Intent.IdempotencyKey)
	if err != nil {
		return workflowruntime.RequestRunCancellationRequest{}, err
	}
	if found {
		return validatePreparedCancellation(input, priorRequest)
	}
	run, err := s.state.LoadRun(ctx, input.Intent.RunID)
	if err != nil {
		return workflowruntime.RequestRunCancellationRequest{}, err
	}
	if !run.Status.Active() {
		priorRequest, _, found, replayErr := loadWorkflowIdempotency(ctx, s.state.db, "workflow_run_cancellation_idempotency", input.Intent.IdempotencyKey)
		if replayErr != nil {
			return workflowruntime.RequestRunCancellationRequest{}, replayErr
		}
		if found {
			return validatePreparedCancellation(input, priorRequest)
		}
		return workflowruntime.RequestRunCancellationRequest{}, &workflowruntime.TransitionError{Entity: "run", ID: string(run.ID), From: string(run.Status), To: string(workflowruntime.RunCanceled), Reason: "host cancellation requires an active run or exact core replay"}
	}
	at := input.EffectiveAt
	if at.Before(run.UpdatedAt) {
		at = run.UpdatedAt
	}
	return workflowruntime.RequestRunCancellationRequest{
		RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: input.Intent.IdempotencyKey,
		Reason: workflowruntime.Failure{Code: "host_cancel_requested", Message: input.Intent.Reason}, At: at,
	}, nil
}

func validatePreparedCancellation(input hoststate.CancellationBinding, encoded string) (workflowruntime.RequestRunCancellationRequest, error) {
	var exact workflowruntime.RequestRunCancellationRequest
	if err := decodeWorkflowJSON("workflow core cancellation request", encoded, &exact); err != nil {
		return workflowruntime.RequestRunCancellationRequest{}, err
	}
	expectedReason := workflowruntime.Failure{Code: "host_cancel_requested", Message: input.Intent.Reason}
	if exact.RunID != input.Intent.RunID || exact.IdempotencyKey != input.Intent.IdempotencyKey || !reflect.DeepEqual(exact.Reason, expectedReason) || exact.At.Before(input.EffectiveAt) {
		return workflowruntime.RequestRunCancellationRequest{}, fmt.Errorf("%w: core cancellation differs from host intent", hoststate.ErrConflict)
	}
	return exact, nil
}

func (s *WorkflowHostStore) ListPendingCancellations(ctx context.Context, limit int) ([]hoststate.CancellationBinding, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("%w: negative host cancellation recovery limit", hoststate.ErrInvalidRecord)
	}
	statement := `SELECT c.binding_json FROM workflow_host_cancellations c WHERE NOT EXISTS (SELECT 1 FROM workflow_run_cancellation_idempotency i WHERE i.idempotency_key = c.idempotency_key) ORDER BY c.recorded_at, c.idempotency_key`
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
	result := make([]hoststate.CancellationBinding, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var binding hoststate.CancellationBinding
		if err := decodeWorkflowJSON("pending workflow host cancellation", encoded, &binding); err != nil {
			return nil, err
		}
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %w", hoststate.ErrInvalidRecord, err)
		}
		result = append(result, binding)
	}
	return result, rows.Err()
}

var _ hoststate.Journal = (*WorkflowHostStore)(nil)
