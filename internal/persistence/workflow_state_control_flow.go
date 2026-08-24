package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.ControlFlowStore = (*WorkflowStateStore)(nil)

const workflowControlDecisionSelect = `
SELECT run_id, node_id, iteration, kind, outcome, source_generation,
       generation, created_at, error_values_sequence, snapshot_json
FROM workflow_control_decisions`

const workflowTerminalIntentSelect = `
SELECT run_id, intended_status, status, idempotency_key, generation,
       created_at, updated_at, completed_at, error_values_sequence,
       immutable_json, snapshot_json
FROM workflow_terminal_intents`

func (s *WorkflowStateStore) LoadControlDecision(ctx context.Context, id workflowruntime.ControlDecisionID) (workflowruntime.ControlDecisionSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, err
	}
	if err := id.Validate(); err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, workflowInvalid(err)
	}
	return loadWorkflowControlDecision(ctx, s.db, id)
}

func (s *WorkflowStateStore) RecordControlDecision(ctx context.Context, request workflowruntime.RecordControlDecisionRequest) (workflowruntime.RecordControlDecisionResult, error) {
	request.At = request.At.UTC()
	if request.ExpectedSourceGeneration == 0 || request.At.IsZero() || request.Decision.Error != nil {
		return workflowruntime.RecordControlDecisionResult{}, workflowInvalid(errors.New("control decision requires source generation, timestamp, and a store-managed error reference"))
	}
	candidate := cloneWorkflowControlDecision(request.Decision)
	candidate.SourceGeneration = request.ExpectedSourceGeneration
	candidate.Generation = 1
	candidate.CreatedAt = request.At
	if err := candidate.ID.Validate(); err != nil || !candidate.Outcome.Valid() {
		return workflowruntime.RecordControlDecisionResult{}, workflowInvalid(errors.New("control decision identity and outcome are required"))
	}
	if candidate.ID.Kind == workflowruntime.ControlSwitch {
		if len(request.ErrorValues) != 0 {
			return workflowruntime.RecordControlDecisionResult{}, workflowInvalid(errors.New("switch decision cannot persist error values"))
		}
		if err := candidate.Validate(); err != nil {
			return workflowruntime.RecordControlDecisionResult{}, workflowInvalid(err)
		}
	} else if err := workflowruntime.ValidateControlErrorValues(request.ErrorValues); err != nil {
		return workflowruntime.RecordControlDecisionResult{}, workflowInvalid(err)
	}

	result := workflowruntime.RecordControlDecisionResult{}
	writeErr := s.write(ctx, "record workflow control decision", func(query workflowSQL) error {
		prior, loadErr := loadWorkflowControlDecision(ctx, query, candidate.ID)
		if loadErr == nil {
			candidate.CreatedAt = prior.CreatedAt
			candidate.Error = cloneWorkflowValueRef(prior.Error)
			if err := candidate.Validate(); err != nil {
				return workflowInvalid(err)
			}
			equalValues, err := equalWorkflowControlValues(ctx, query, prior.Error, request.ErrorValues)
			if err != nil {
				return err
			}
			if !equalWorkflowControlDecision(prior, candidate) || !equalValues {
				return fmt.Errorf("%w: control decision already differs", workflowruntime.ErrControlFlowConflict)
			}
			result = workflowruntime.RecordControlDecisionResult{Outcome: workflowruntime.IdempotencyReplayed, Decision: prior}
			return nil
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		source, sourceErr := loadWorkflowNode(ctx, query, candidate.ID.Source)
		if sourceErr != nil {
			return sourceErr
		}
		allowed, admissionErr := workflowControlAdmissionAllowed(ctx, query, source.ID)
		if admissionErr != nil {
			return admissionErr
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences control decision"))
		}
		if source.Generation != request.ExpectedSourceGeneration {
			return workflowCAS("control decision source", request.ExpectedSourceGeneration, source.Generation)
		}
		if request.At.Before(source.UpdatedAt) {
			return workflowInvalid(errors.New("control decision time must not precede source update"))
		}
		if candidate.ID.Kind == workflowruntime.ControlSwitch && source.Status != workflowruntime.NodeSucceeded {
			return workflowInvalid(errors.New("switch decision requires succeeded source"))
		}
		if candidate.ID.Kind == workflowruntime.ControlCatch && !workflowHardFailure(source.Status) {
			return workflowInvalid(errors.New("catch decision requires hard-failed source"))
		}
		if candidate.ID.Kind == workflowruntime.ControlCatch {
			var attempt *workflowruntime.AttemptID
			var expectedFailure *workflowruntime.Failure
			if source.LatestAttempt > 0 {
				id := workflowruntime.AttemptID{Invocation: source.ID, Number: source.LatestAttempt}
				attempt = &id
				persisted, err := loadWorkflowAttempt(ctx, query, id)
				if err != nil {
					return err
				}
				if persisted.Failure == nil || persisted.Status != source.Status {
					return workflowInvalid(errors.New("catch source attempt has no durable failure"))
				}
				expectedFailure = persisted.Failure
			}
			if err := workflowruntime.ValidateNodeControlErrorValues(request.ErrorValues, source.ID, attempt, source.Status, expectedFailure); err != nil {
				return workflowInvalid(err)
			}
		}
		for _, target := range candidate.Targets {
			if _, err := loadWorkflowNode(ctx, query, target); err != nil {
				if errors.Is(err, workflowruntime.ErrNotFound) {
					return workflowInvalid(fmt.Errorf("control decision target %q does not exist", target.NodeID))
				}
				return err
			}
		}
		if candidate.ID.Kind == workflowruntime.ControlCatch {
			var attempt *workflowruntime.AttemptID
			if source.LatestAttempt > 0 {
				id := workflowruntime.AttemptID{Invocation: source.ID, Number: source.LatestAttempt}
				attempt = &id
			}
			ref, err := insertWorkflowValues(ctx, query, workflowruntime.ValueOwner{Kind: "control-error", RunID: source.ID.RunID, Invocation: &source.ID, Attempt: attempt}, request.ErrorValues)
			if err != nil {
				return err
			}
			candidate.Error = &ref
		}
		if err := candidate.Validate(); err != nil {
			return workflowInvalid(err)
		}
		encoded, encodeErr := encodeWorkflowJSON(candidate)
		if encodeErr != nil {
			return encodeErr
		}
		errorSequence, sequenceErr := workflowControlValueSequence(candidate.Error)
		if sequenceErr != nil {
			return sequenceErr
		}
		if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_control_decisions(
    run_id, node_id, iteration, kind, outcome, source_generation,
    generation, created_at, error_values_sequence, snapshot_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			candidate.ID.Source.RunID, candidate.ID.Source.NodeID, candidate.ID.Source.Iteration,
			candidate.ID.Kind, candidate.Outcome, candidate.SourceGeneration, candidate.Generation,
			workflowTime(candidate.CreatedAt), errorSequence, encoded,
		); err != nil {
			if isSQLiteConstraint(err) {
				return fmt.Errorf("%w: control decision already exists", workflowruntime.ErrControlFlowConflict)
			}
			return fmt.Errorf("insert workflow control decision: %w", err)
		}
		eventType := workflowruntime.EventSwitchDecided
		if candidate.ID.Kind == workflowruntime.ControlCatch {
			eventType = workflowruntime.EventCatchDecided
		}
		attributes := map[string]string{"outcome": string(candidate.Outcome), "source_generation": fmt.Sprint(candidate.SourceGeneration)}
		if candidate.RuleIndex != nil {
			attributes["rule_index"] = fmt.Sprint(*candidate.RuleIndex)
		}
		for index, target := range candidate.Targets {
			attributes[fmt.Sprintf("target.%06d", index)] = target.NodeID
		}
		if candidate.BindAs != "" {
			attributes["bind_as"] = candidate.BindAs
		}
		invocation := source.ID
		event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: source.ID.RunID, Invocation: &invocation, Type: eventType,
			OccurredAt: candidate.CreatedAt, Attributes: attributes, Values: cloneWorkflowValueRef(candidate.Error),
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return err
		}
		eventCopy := event
		result = workflowruntime.RecordControlDecisionResult{Outcome: workflowruntime.IdempotencyApplied, Decision: cloneWorkflowControlDecision(candidate), Event: &eventCopy}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.RecordControlDecisionResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) LoadTerminalIntent(ctx context.Context, runID workflowruntime.RunID) (workflowruntime.TerminalIntentSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	return loadWorkflowTerminalIntent(ctx, s.db, runID)
}

func (s *WorkflowStateStore) BeginTerminalIntent(ctx context.Context, request workflowruntime.BeginTerminalIntentRequest) (workflowruntime.BeginTerminalIntentResult, error) {
	request.At = request.At.UTC()
	if len(request.Finalizers) == 0 {
		return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(errors.New("public terminal intent requires at least one finalizer"))
	}
	var result workflowruntime.BeginTerminalIntentResult
	writeErr := s.write(ctx, "begin workflow terminal intent", func(query workflowSQL) error {
		var beginErr error
		result, beginErr = beginWorkflowTerminalIntent(ctx, query, request)
		return beginErr
	})
	if writeErr != nil {
		return workflowruntime.BeginTerminalIntentResult{}, writeErr
	}
	return result, nil
}

func beginWorkflowTerminalIntent(ctx context.Context, query workflowSQL, request workflowruntime.BeginTerminalIntentRequest) (workflowruntime.BeginTerminalIntentResult, error) {
	request.At = request.At.UTC()
	if request.ExpectedRunGeneration == 0 || request.At.IsZero() {
		return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(errors.New("terminal intent requires run generation and timestamp"))
	}
	if request.IntendedStatus == workflowruntime.RunSucceeded {
		if len(request.ErrorValues) != 0 {
			return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(errors.New("successful terminal intent cannot persist error values"))
		}
	} else if err := workflowruntime.ValidateControlErrorValues(request.ErrorValues); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(err)
	}
	candidate := workflowruntime.TerminalIntentSnapshot{
		RunID: request.RunID, IntendedStatus: request.IntendedStatus,
		SuccessOutputsRequired: request.SuccessOutputsRequired,
		Reason:                 cloneWorkflowFailure(request.Reason), IdempotencyKey: request.IdempotencyKey,
		Finalizers: cloneWorkflowFinalizerScopes(request.Finalizers), Status: workflowruntime.TerminalIntentPending,
		Generation: 1, CreatedAt: request.At, UpdatedAt: request.At,
	}
	var keyRun workflowruntime.RunID
	keyErr := query.QueryRowContext(ctx, `SELECT run_id FROM workflow_terminal_intents WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&keyRun)
	if keyErr == nil && keyRun != request.RunID {
		return workflowruntime.BeginTerminalIntentResult{}, workflowIdempotencyConflict("terminal intent", request.IdempotencyKey)
	}
	if keyErr != nil && !errors.Is(keyErr, sql.ErrNoRows) {
		return workflowruntime.BeginTerminalIntentResult{}, keyErr
	}
	prior, loadErr := loadWorkflowTerminalIntent(ctx, query, request.RunID)
	if loadErr == nil {
		candidate.Error = cloneWorkflowValueRef(prior.Error)
		if err := candidate.Validate(); err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(err)
		}
		left, err := encodeWorkflowTerminalImmutable(prior)
		if err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, err
		}
		right, err := encodeWorkflowTerminalImmutable(candidate)
		if err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, err
		}
		equalValues, err := equalWorkflowControlValues(ctx, query, prior.Error, request.ErrorValues)
		if err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, err
		}
		if left != right || !equalValues {
			return workflowruntime.BeginTerminalIntentResult{}, workflowIdempotencyConflict("terminal intent", request.IdempotencyKey)
		}
		run, err := loadWorkflowRun(ctx, query, request.RunID)
		if err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, err
		}
		return workflowruntime.BeginTerminalIntentResult{Outcome: workflowruntime.IdempotencyReplayed, Run: run, Intent: prior}, nil
	}
	if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		return workflowruntime.BeginTerminalIntentResult{}, loadErr
	}
	run, runErr := loadWorkflowRun(ctx, query, request.RunID)
	if runErr != nil {
		return workflowruntime.BeginTerminalIntentResult{}, runErr
	}
	if run.Generation != request.ExpectedRunGeneration {
		return workflowruntime.BeginTerminalIntentResult{}, workflowCAS("terminal intent run", request.ExpectedRunGeneration, run.Generation)
	}
	if err := workflowruntime.ValidateRunStatusTransition(run.Status, request.IntendedStatus); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, err
	}
	if len(request.Finalizers) != 0 {
		if err := workflowruntime.ValidateRunStatusTransition(run.Status, workflowruntime.RunFailed); err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, err
		}
	}
	if request.IntendedStatus != workflowruntime.RunSucceeded {
		origin, err := workflowruntime.ValidateTerminalControlErrorValues(request.ErrorValues, request.RunID, request.IntendedStatus, request.Reason)
		if err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(err)
		}
		if err := validateWorkflowTerminalErrorOrigin(ctx, query, request.ErrorValues, origin, request.IntendedStatus); err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, err
		}
	}
	if !run.Status.Active() || request.At.Before(run.UpdatedAt) {
		return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(errors.New("terminal intent requires active run and non-regressing time"))
	}
	for _, finalizer := range candidate.Finalizers {
		if _, err := loadWorkflowNode(ctx, query, finalizer.Invocation); err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, err
		}
		for _, member := range finalizer.Scope {
			if _, err := loadWorkflowNode(ctx, query, member); err != nil {
				return workflowruntime.BeginTerminalIntentResult{}, err
			}
		}
	}
	if request.IntendedStatus != workflowruntime.RunSucceeded {
		ref, err := insertWorkflowValues(ctx, query, workflowruntime.ValueOwner{Kind: "control-run-error", RunID: request.RunID}, request.ErrorValues)
		if err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, err
		}
		candidate.Error = &ref
	}
	if err := candidate.Validate(); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(err)
	}
	nextRun := run
	nextRun.Generation++
	nextRun.UpdatedAt = request.At
	if err := nextRun.Validate(); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, workflowInvalid(err)
	}
	if err := updateWorkflowRunCAS(ctx, query, nextRun, run.Generation); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, err
	}
	immutableJSON, encodeErr := encodeWorkflowTerminalImmutable(candidate)
	if encodeErr != nil {
		return workflowruntime.BeginTerminalIntentResult{}, encodeErr
	}
	snapshotJSON, snapshotErr := encodeWorkflowJSON(candidate)
	if snapshotErr != nil {
		return workflowruntime.BeginTerminalIntentResult{}, snapshotErr
	}
	errorSequence, sequenceErr := workflowControlValueSequence(candidate.Error)
	if sequenceErr != nil {
		return workflowruntime.BeginTerminalIntentResult{}, sequenceErr
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_terminal_intents(
    run_id, intended_status, status, idempotency_key, generation,
    created_at, updated_at, completed_at, error_values_sequence,
    immutable_json, snapshot_json
) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)`,
		candidate.RunID, candidate.IntendedStatus, candidate.Status, candidate.IdempotencyKey,
		candidate.Generation, workflowTime(candidate.CreatedAt), workflowTime(candidate.UpdatedAt),
		errorSequence, immutableJSON, snapshotJSON,
	); err != nil {
		if isSQLiteConstraint(err) {
			return workflowruntime.BeginTerminalIntentResult{}, workflowIdempotencyConflict("terminal intent", request.IdempotencyKey)
		}
		return workflowruntime.BeginTerminalIntentResult{}, fmt.Errorf("insert workflow terminal intent: %w", err)
	}
	event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
		RunID: run.ID, Type: workflowruntime.EventTerminalIntent, OccurredAt: request.At,
		Attributes: map[string]string{"intended_status": string(candidate.IntendedStatus), "finalizers": fmt.Sprint(len(candidate.Finalizers)), "reason_code": workflowFailureCode(candidate.Reason)},
		Values:     cloneWorkflowValueRef(candidate.Error), Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, err
	}
	eventCopy := event
	return workflowruntime.BeginTerminalIntentResult{Outcome: workflowruntime.IdempotencyApplied, Run: nextRun, Intent: cloneWorkflowTerminalIntent(candidate), Event: &eventCopy}, nil
}

func (s *WorkflowStateStore) CompleteTerminalIntent(ctx context.Context, request workflowruntime.CompleteTerminalIntentRequest) (workflowruntime.CompleteTerminalIntentResult, error) {
	request.At = request.At.UTC()
	request.Outputs = cloneWorkflowValueRef(request.Outputs)
	if request.Outputs != nil {
		if err := request.Outputs.Validate(); err != nil {
			return workflowruntime.CompleteTerminalIntentResult{}, workflowInvalid(err)
		}
	}
	if request.RunID == "" || request.ExpectedRunGeneration == 0 || request.ExpectedIntentGeneration == 0 || request.At.IsZero() {
		return workflowruntime.CompleteTerminalIntentResult{}, workflowInvalid(errors.New("terminal completion requires identity, generations, and timestamp"))
	}
	var result workflowruntime.CompleteTerminalIntentResult
	writeErr := s.write(ctx, "complete workflow terminal intent", func(query workflowSQL) error {
		intent, intentErr := loadWorkflowTerminalIntent(ctx, query, request.RunID)
		if intentErr != nil {
			return intentErr
		}
		run, runErr := loadWorkflowRun(ctx, query, request.RunID)
		if runErr != nil {
			return runErr
		}
		if run.Generation != request.ExpectedRunGeneration {
			return workflowCAS("terminal completion run", request.ExpectedRunGeneration, run.Generation)
		}
		if intent.Generation != request.ExpectedIntentGeneration {
			return workflowCAS("terminal intent", request.ExpectedIntentGeneration, intent.Generation)
		}
		if intent.Status != workflowruntime.TerminalIntentPending || !run.Status.Active() || request.At.Before(run.UpdatedAt) || request.At.Before(intent.UpdatedAt) {
			return workflowInvalid(errors.New("terminal intent is not pending or completion time regresses"))
		}
		to := intent.IntendedStatus
		cleanupFailure := ""
		for _, finalizer := range intent.Finalizers {
			node, err := loadWorkflowNode(ctx, query, finalizer.Invocation)
			if err != nil {
				return err
			}
			if !node.Status.Terminal() {
				return workflowruntime.ErrControlFlowPending
			}
			if request.At.Before(node.UpdatedAt) {
				return workflowInvalid(errors.New("terminal completion time must not precede finalizer completion"))
			}
			if workflowHardFailure(node.Status) {
				to, cleanupFailure = workflowruntime.RunFailed, node.ID.NodeID
			}
		}
		cancellations, cancellationErr := listWorkflowCancellationIntents(ctx, query, run.ID)
		if cancellationErr != nil {
			return cancellationErr
		}
		for _, cancellation := range cancellations {
			if cancellation.Status == workflowruntime.CancellationPending {
				return workflowruntime.ErrControlFlowPending
			}
		}
		if err := workflowruntime.ValidateRunStatusTransition(run.Status, to); err != nil {
			return err
		}
		var eventValues *values.ValueSetRef
		if to == workflowruntime.RunSucceeded && intent.SuccessOutputsRequired {
			if request.Outputs == nil {
				return workflowInvalid(errors.New("successful terminal completion requires outputs"))
			}
			record, recordErr := loadWorkflowValueRecord(ctx, query, *request.Outputs)
			if recordErr != nil {
				return recordErr
			}
			if record.Owner != (workflowruntime.ValueOwner{Kind: "run-outputs", RunID: run.ID}) {
				return workflowInvalid(errors.New("terminal outputs differ from the exact run-owned value record"))
			}
			eventValues = cloneWorkflowValueRef(request.Outputs)
		} else if request.Outputs != nil {
			return workflowInvalid(errors.New("non-successful terminal completion cannot publish outputs"))
		} else if to != workflowruntime.RunSucceeded {
			eventValues = cloneWorkflowValueRef(intent.Error)
		}
		nextRun := run
		nextRun.Status = to
		nextRun.Outputs = cloneWorkflowValueRef(request.Outputs)
		nextRun.Generation++
		nextRun.UpdatedAt = request.At
		nextIntent := cloneWorkflowTerminalIntent(intent)
		nextIntent.Status = workflowruntime.TerminalIntentCompleted
		nextIntent.Generation++
		nextIntent.UpdatedAt, nextIntent.CompletedAt = request.At, request.At
		if err := nextRun.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := nextIntent.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowRunCAS(ctx, query, nextRun, run.Generation); err != nil {
			return err
		}
		snapshotJSON, snapshotErr := encodeWorkflowJSON(nextIntent)
		if snapshotErr != nil {
			return snapshotErr
		}
		res, updateErr := query.ExecContext(ctx, `
UPDATE workflow_terminal_intents
SET status = ?, generation = ?, updated_at = ?, completed_at = ?, snapshot_json = ?
WHERE run_id = ? AND generation = ? AND status = ?`,
			nextIntent.Status, nextIntent.Generation, workflowTime(nextIntent.UpdatedAt), workflowTime(nextIntent.CompletedAt), snapshotJSON,
			nextIntent.RunID, intent.Generation, intent.Status,
		)
		if updateErr != nil {
			return fmt.Errorf("update workflow terminal intent: %w", updateErr)
		}
		if err := expectOneWorkflowRow(res, "terminal intent", intent.Generation, intent.Generation); err != nil {
			return err
		}
		attributes := map[string]string{"from_status": string(run.Status), "to_status": string(to), "intended_status": string(intent.IntendedStatus)}
		if cleanupFailure != "" {
			attributes["cleanup_failure"] = cleanupFailure
		}
		event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: run.ID, Type: workflowruntime.EventRunStatusChanged, OccurredAt: request.At,
			Attributes: attributes, Values: eventValues, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return err
		}
		result = workflowruntime.CompleteTerminalIntentResult{Run: nextRun, Intent: nextIntent, Event: event}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.CompleteTerminalIntentResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) RequestRunCancellationWithFinalizers(ctx context.Context, request workflowruntime.RequestRunCancellationWithFinalizersRequest) (workflowruntime.RequestRunCancellationWithFinalizersResult, error) {
	request.Cancellation.At = request.Cancellation.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, workflowInvalid(err)
	}
	rootRequestJSON, encodeErr := encodeWorkflowJSON(request.Cancellation)
	if encodeErr != nil {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, encodeErr
	}
	treeRequestJSON, encodeErr := encodeWorkflowJSON(request)
	if encodeErr != nil {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, encodeErr
	}
	var result workflowruntime.RequestRunCancellationWithFinalizersResult
	writeErr := s.write(ctx, "request workflow cancellation with finalizers", func(query workflowSQL) error {
		var priorTree string
		treeErr := query.QueryRowContext(ctx, `SELECT request_json FROM workflow_control_cancellation_trees WHERE idempotency_key = ?`, request.Cancellation.IdempotencyKey).Scan(&priorTree)
		if treeErr == nil {
			var priorTreeRequest workflowruntime.RequestRunCancellationWithFinalizersRequest
			if err := decodeWorkflowJSON("control cancellation tree request", priorTree, &priorTreeRequest); err != nil {
				return err
			}
			if !equalWorkflowControlCancellationTree(priorTreeRequest, request) {
				return workflowIdempotencyConflict("cancel run tree", request.Cancellation.IdempotencyKey)
			}
			priorRequest, priorResult, found, replayErr := loadWorkflowIdempotency(ctx, query, "workflow_run_cancellation_idempotency", request.Cancellation.IdempotencyKey)
			if replayErr != nil {
				return replayErr
			}
			if !found {
				return workflowInvalid(errors.New("cancellation tree is missing its root cancellation replay record"))
			}
			var priorRootRequest workflowruntime.RequestRunCancellationRequest
			if err := decodeWorkflowJSON("control cancellation root request", priorRequest, &priorRootRequest); err != nil {
				return err
			}
			if !equalWorkflowControlCancellationRoot(priorRootRequest, request.Cancellation) {
				return workflowInvalid(errors.New("cancellation tree root replay record differs from its immutable tree"))
			}
			var cancellation workflowruntime.RequestRunCancellationResult
			if err := decodeWorkflowJSON("cancellation with finalizers result", priorResult, &cancellation); err != nil {
				return err
			}
			cancellation.Outcome = workflowruntime.IdempotencyReplayed
			currentRun, err := loadWorkflowRun(ctx, query, request.Cancellation.RunID)
			if err != nil {
				return err
			}
			cancellation.Run = currentRun
			intents, rootIntent, err := loadWorkflowCancellationTreeIntents(ctx, query, request)
			if err != nil {
				return err
			}
			result = workflowruntime.RequestRunCancellationWithFinalizersResult{Cancellation: cancellation, Intent: rootIntent, TerminalIntents: intents}
			return nil
		}
		if !errors.Is(treeErr, sql.ErrNoRows) {
			return treeErr
		}
		if _, _, found, err := loadWorkflowIdempotency(ctx, query, "workflow_run_cancellation_idempotency", request.Cancellation.IdempotencyKey); err != nil {
			return err
		} else if found {
			return workflowIdempotencyConflict("cancel run with finalizers", request.Cancellation.IdempotencyKey)
		}
		reachable, reachabilityErr := workflowDirectCancelDescendants(ctx, query, request.Cancellation.RunID)
		if reachabilityErr != nil {
			return reachabilityErr
		}
		if len(reachable) != len(request.Descendants) {
			return workflowInvalid(errors.New("cancellation tree does not exactly cover direct-cancel descendants"))
		}
		for index, runID := range reachable {
			if request.Descendants[index].RunID != runID {
				return workflowInvalid(errors.New("cancellation tree does not exactly cover direct-cancel descendants"))
			}
		}
		plans := workflowCancellationTreePlans(request)
		for index, plan := range plans {
			run, err := loadWorkflowRun(ctx, query, plan.RunID)
			if err != nil {
				return err
			}
			if run.Generation != plan.ExpectedRunGeneration {
				return workflowCAS("cancellation tree run", plan.ExpectedRunGeneration, run.Generation)
			}
			if index == 0 && !run.Status.Active() {
				return workflowInvalid(errors.New("cancellation tree root must be active"))
			}
			if run.Status.Terminal() {
				continue
			}
			if request.Cancellation.At.Before(run.UpdatedAt) {
				return workflowInvalid(errors.New("cancellation tree time must not regress"))
			}
			if err := workflowruntime.ValidateRunStatusTransition(run.Status, workflowruntime.RunCanceled); err != nil {
				return err
			}
			if len(plan.Finalizers) != 0 {
				if err := workflowruntime.ValidateRunStatusTransition(run.Status, workflowruntime.RunFailed); err != nil {
					return err
				}
			}
		}
		collector := workflowCancellationCollector{}
		intents := make([]workflowruntime.TerminalIntentSnapshot, 0)
		var rootIntent workflowruntime.TerminalIntentSnapshot
		for index, plan := range plans {
			run, err := loadWorkflowRun(ctx, query, plan.RunID)
			if err != nil {
				return err
			}
			if run.Status.Terminal() {
				continue
			}
			excluded := make(map[workflowruntime.NodeInvocationID]struct{}, len(plan.Finalizers))
			for _, finalizer := range plan.Finalizers {
				excluded[finalizer.Invocation] = struct{}{}
			}
			terminalize := len(plan.Finalizers) == 0
			if !terminalize {
				begin, err := beginWorkflowTerminalIntent(ctx, query, workflowruntime.BeginTerminalIntentRequest{
					RunID: plan.RunID, ExpectedRunGeneration: plan.ExpectedRunGeneration,
					IntendedStatus: workflowruntime.RunCanceled, Reason: &request.Cancellation.Reason,
					ErrorValues: plan.ErrorValues, IdempotencyKey: plan.IdempotencyKey,
					Finalizers: plan.Finalizers, At: request.Cancellation.At,
				})
				if err != nil {
					return err
				}
				intents = append(intents, begin.Intent)
				if index == 0 {
					rootIntent = begin.Intent
				}
				if begin.Event != nil {
					collector.events = append(collector.events, *begin.Event)
				}
			}
			if err := cancelWorkflowRunWithOptions(ctx, query, plan.RunID, request.Cancellation.At, request.Cancellation.Reason, plan.IdempotencyKey, make(map[workflowruntime.RunID]bool), &collector, excluded, terminalize, false); err != nil {
				return err
			}
		}
		currentRun, err := loadWorkflowRun(ctx, query, request.Cancellation.RunID)
		if err != nil {
			return err
		}
		cancellation := workflowruntime.RequestRunCancellationResult{
			Outcome: workflowruntime.IdempotencyApplied, Run: currentRun,
			Nodes: collector.nodes, Intents: collector.intents, Events: collector.events,
		}
		resultJSON, err := encodeWorkflowJSON(cancellation)
		if err != nil {
			return err
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_run_cancellation_idempotency(idempotency_key, request_json, result_json) VALUES (?, ?, ?)`, request.Cancellation.IdempotencyKey, rootRequestJSON, resultJSON); err != nil {
			if isSQLiteConstraint(err) {
				return workflowIdempotencyConflict("cancel run with finalizers", request.Cancellation.IdempotencyKey)
			}
			return err
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_control_cancellation_trees(root_run_id, idempotency_key, request_json, created_at) VALUES (?, ?, ?, ?)`, request.Cancellation.RunID, request.Cancellation.IdempotencyKey, treeRequestJSON, workflowTime(request.Cancellation.At)); err != nil {
			if isSQLiteConstraint(err) {
				return workflowIdempotencyConflict("cancel run tree", request.Cancellation.IdempotencyKey)
			}
			return err
		}
		result = workflowruntime.RequestRunCancellationWithFinalizersResult{Cancellation: cancellation, Intent: rootIntent, TerminalIntents: intents}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, writeErr
	}
	return result, nil
}

func workflowCancellationTreePlans(request workflowruntime.RequestRunCancellationWithFinalizersRequest) []workflowruntime.CancellationDescendantPlan {
	plans := make([]workflowruntime.CancellationDescendantPlan, 0, len(request.Descendants)+1)
	plans = append(plans, workflowruntime.CancellationDescendantPlan{
		RunID: request.Cancellation.RunID, ExpectedRunGeneration: request.Cancellation.ExpectedGeneration,
		IdempotencyKey: request.Cancellation.IdempotencyKey, Finalizers: request.Finalizers, ErrorValues: request.ErrorValues,
	})
	plans = append(plans, request.Descendants...)
	return plans
}

func equalWorkflowControlCancellationTree(left, right workflowruntime.RequestRunCancellationWithFinalizersRequest) bool {
	left.Cancellation.ExpectedGeneration, right.Cancellation.ExpectedGeneration = 0, 0
	for index := range left.Descendants {
		left.Descendants[index].ExpectedRunGeneration = 0
	}
	for index := range right.Descendants {
		right.Descendants[index].ExpectedRunGeneration = 0
	}
	return reflect.DeepEqual(left, right)
}

func equalWorkflowControlCancellationRoot(left, right workflowruntime.RequestRunCancellationRequest) bool {
	left.ExpectedGeneration, right.ExpectedGeneration = 0, 0
	return reflect.DeepEqual(left, right)
}

func workflowDirectCancelDescendants(ctx context.Context, query workflowSQL, root workflowruntime.RunID) ([]workflowruntime.RunID, error) {
	seen := map[workflowruntime.RunID]bool{root: true}
	result := make([]workflowruntime.RunID, 0)
	var visit func(workflowruntime.RunID) error
	visit = func(parent workflowruntime.RunID) error {
		links, err := listWorkflowChildRuns(ctx, query, parent)
		if err != nil {
			return err
		}
		for _, link := range links {
			if link.Policy != graph.ParentCloseCancel {
				continue
			}
			if seen[link.ChildRunID] {
				return workflowInvalid(errors.New("direct-cancel child graph contains a cycle or duplicate descendant"))
			}
			seen[link.ChildRunID] = true
			result = append(result, link.ChildRunID)
			if err := visit(link.ChildRunID); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(root); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}

func loadWorkflowCancellationTreeIntents(ctx context.Context, query workflowSQL, request workflowruntime.RequestRunCancellationWithFinalizersRequest) ([]workflowruntime.TerminalIntentSnapshot, workflowruntime.TerminalIntentSnapshot, error) {
	intents := make([]workflowruntime.TerminalIntentSnapshot, 0)
	var root workflowruntime.TerminalIntentSnapshot
	for index, plan := range workflowCancellationTreePlans(request) {
		if len(plan.Finalizers) == 0 {
			continue
		}
		intent, err := loadWorkflowTerminalIntent(ctx, query, plan.RunID)
		if errors.Is(err, workflowruntime.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, workflowruntime.TerminalIntentSnapshot{}, err
		}
		intents = append(intents, intent)
		if index == 0 {
			root = intent
		}
	}
	return intents, root, nil
}

func loadWorkflowControlDecision(ctx context.Context, query workflowSQL, id workflowruntime.ControlDecisionID) (workflowruntime.ControlDecisionSnapshot, error) {
	var runID, nodeID, iteration, kind, outcome, createdAt, snapshotJSON string
	var sourceGeneration, generation int64
	var errorSequence sql.NullInt64
	scanErr := query.QueryRowContext(ctx, workflowControlDecisionSelect+` WHERE run_id = ? AND node_id = ? AND iteration = ? AND kind = ?`, id.Source.RunID, id.Source.NodeID, id.Source.Iteration, id.Kind).Scan(
		&runID, &nodeID, &iteration, &kind, &outcome, &sourceGeneration, &generation, &createdAt, &errorSequence, &snapshotJSON,
	)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return workflowruntime.ControlDecisionSnapshot{}, fmt.Errorf("%w: control decision", workflowruntime.ErrNotFound)
	}
	if scanErr != nil {
		return workflowruntime.ControlDecisionSnapshot{}, fmt.Errorf("load workflow control decision: %w", scanErr)
	}
	var snapshot workflowruntime.ControlDecisionSnapshot
	if err := decodeWorkflowJSON("control decision", snapshotJSON, &snapshot); err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, err
	}
	parsedCreated, err := parseWorkflowTime("control decision created_at", createdAt)
	if err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, err
	}
	parsedSource, err := workflowGeneration("control decision source generation", sourceGeneration)
	if err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, err
	}
	parsedGeneration, err := workflowGeneration("control decision generation", generation)
	if err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, err
	}
	columnsMatch := snapshot.ID.Source == (workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(runID), NodeID: nodeID, Iteration: iteration}) &&
		snapshot.ID.Kind == workflowruntime.ControlDecisionKind(kind) && snapshot.Outcome == workflowruntime.ControlDecisionOutcome(outcome) &&
		snapshot.SourceGeneration == parsedSource && snapshot.Generation == parsedGeneration && snapshot.CreatedAt.Equal(parsedCreated)
	if !columnsMatch {
		return workflowruntime.ControlDecisionSnapshot{}, workflowInvalid(errors.New("control decision columns diverge from semantic snapshot"))
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, workflowInvalid(err)
	}
	if err := validateWorkflowControlValueMirror(ctx, query, snapshot.Error, errorSequence); err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, err
	}
	return cloneWorkflowControlDecision(snapshot), nil
}

func loadWorkflowTerminalIntent(ctx context.Context, query workflowSQL, runID workflowruntime.RunID) (workflowruntime.TerminalIntentSnapshot, error) {
	var storedRunID, intendedStatus, status, key, createdAt, updatedAt, immutableJSON, snapshotJSON string
	var generation int64
	var completedAt sql.NullString
	var errorSequence sql.NullInt64
	scanErr := query.QueryRowContext(ctx, workflowTerminalIntentSelect+` WHERE run_id = ?`, runID).Scan(
		&storedRunID, &intendedStatus, &status, &key, &generation, &createdAt, &updatedAt,
		&completedAt, &errorSequence, &immutableJSON, &snapshotJSON,
	)
	if errors.Is(scanErr, sql.ErrNoRows) {
		return workflowruntime.TerminalIntentSnapshot{}, fmt.Errorf("%w: terminal intent", workflowruntime.ErrNotFound)
	}
	if scanErr != nil {
		return workflowruntime.TerminalIntentSnapshot{}, fmt.Errorf("load workflow terminal intent: %w", scanErr)
	}
	var snapshot workflowruntime.TerminalIntentSnapshot
	if err := decodeWorkflowJSON("terminal intent", snapshotJSON, &snapshot); err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	parsedCreated, err := parseWorkflowTime("terminal intent created_at", createdAt)
	if err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	parsedUpdated, err := parseWorkflowTime("terminal intent updated_at", updatedAt)
	if err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	parsedCompleted, err := parseOptionalWorkflowTime("terminal intent completed_at", completedAt)
	if err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	parsedGeneration, err := workflowGeneration("terminal intent generation", generation)
	if err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	columnsMatch := snapshot.RunID == workflowruntime.RunID(storedRunID) && snapshot.IntendedStatus == workflowruntime.RunStatus(intendedStatus) &&
		snapshot.Status == workflowruntime.TerminalIntentStatus(status) && snapshot.IdempotencyKey == key && snapshot.Generation == parsedGeneration &&
		snapshot.CreatedAt.Equal(parsedCreated) && snapshot.UpdatedAt.Equal(parsedUpdated) && snapshot.CompletedAt.Equal(parsedCompleted)
	if !columnsMatch {
		return workflowruntime.TerminalIntentSnapshot{}, workflowInvalid(errors.New("terminal intent columns diverge from semantic snapshot"))
	}
	if validationErr := snapshot.Validate(); validationErr != nil {
		return workflowruntime.TerminalIntentSnapshot{}, workflowInvalid(validationErr)
	}
	derivedImmutable, err := encodeWorkflowTerminalImmutable(snapshot)
	if err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	if derivedImmutable != immutableJSON {
		return workflowruntime.TerminalIntentSnapshot{}, workflowInvalid(errors.New("terminal intent immutable projection is corrupt"))
	}
	if err := validateWorkflowControlValueMirror(ctx, query, snapshot.Error, errorSequence); err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	return cloneWorkflowTerminalIntent(snapshot), nil
}

type workflowTerminalImmutable struct {
	RunID                  workflowruntime.RunID
	IntendedStatus         workflowruntime.RunStatus
	SuccessOutputsRequired bool `json:"SuccessOutputsRequired,omitempty"`
	Reason                 *workflowruntime.Failure
	Error                  *values.ValueSetRef
	IdempotencyKey         string
	Finalizers             []workflowruntime.FinalizerScope
	CreatedAt              time.Time
}

func encodeWorkflowTerminalImmutable(snapshot workflowruntime.TerminalIntentSnapshot) (string, error) {
	return encodeWorkflowJSON(workflowTerminalImmutable{
		RunID: snapshot.RunID, IntendedStatus: snapshot.IntendedStatus,
		SuccessOutputsRequired: snapshot.SuccessOutputsRequired,
		Reason:                 cloneWorkflowFailure(snapshot.Reason), Error: cloneWorkflowValueRef(snapshot.Error),
		IdempotencyKey: snapshot.IdempotencyKey, Finalizers: cloneWorkflowFinalizerScopes(snapshot.Finalizers),
		CreatedAt: snapshot.CreatedAt.UTC(),
	})
}

func workflowControlValueSequence(ref *values.ValueSetRef) (any, error) {
	if ref == nil {
		return nil, nil
	}
	return parseWorkflowValueID(ref.ID)
}

func validateWorkflowControlValueMirror(ctx context.Context, query workflowSQL, ref *values.ValueSetRef, sequence sql.NullInt64) error {
	if ref == nil {
		if sequence.Valid {
			return workflowInvalid(errors.New("control-flow error sequence exists without a reference"))
		}
		return nil
	}
	parsed, err := parseWorkflowValueID(ref.ID)
	if err != nil {
		return err
	}
	if !sequence.Valid || sequence.Int64 != parsed {
		return workflowInvalid(errors.New("control-flow error reference and sequence diverge"))
	}
	set, err := loadWorkflowValues(ctx, query, *ref)
	if err != nil {
		return err
	}
	if err := workflowruntime.ValidateControlErrorValues(set); err != nil {
		return workflowInvalid(err)
	}
	return nil
}

func equalWorkflowControlValues(ctx context.Context, query workflowSQL, ref *values.ValueSetRef, input values.ValueSet) (bool, error) {
	if ref == nil {
		return len(input) == 0, nil
	}
	if err := workflowruntime.ValidateControlErrorValues(input); err != nil {
		return false, workflowInvalid(err)
	}
	if _, err := loadWorkflowValues(ctx, query, *ref); err != nil {
		return false, err
	}
	digest, err := values.DigestValueSet(input)
	return err == nil && digest == ref.Digest, err
}

func workflowControlAdmissionAllowed(ctx context.Context, query workflowSQL, id workflowruntime.NodeInvocationID) (bool, error) {
	intent, err := loadWorkflowTerminalIntent(ctx, query, id.RunID)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if intent.Status != workflowruntime.TerminalIntentPending {
		return true, nil
	}
	for _, finalizer := range intent.Finalizers {
		if finalizer.Invocation == id {
			return true, nil
		}
	}
	return false, nil
}

func workflowRunHasPendingTerminalIntent(ctx context.Context, query workflowSQL, runID workflowruntime.RunID) (bool, error) {
	intent, err := loadWorkflowTerminalIntent(ctx, query, runID)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return intent.Status == workflowruntime.TerminalIntentPending, nil
}

func cloneWorkflowControlDecision(input workflowruntime.ControlDecisionSnapshot) workflowruntime.ControlDecisionSnapshot {
	if input.RuleIndex != nil {
		value := *input.RuleIndex
		input.RuleIndex = &value
	}
	input.Targets = append([]workflowruntime.NodeInvocationID(nil), input.Targets...)
	input.Error = cloneWorkflowValueRef(input.Error)
	return input
}

func equalWorkflowControlDecision(left, right workflowruntime.ControlDecisionSnapshot) bool {
	return reflect.DeepEqual(cloneWorkflowControlDecision(left), cloneWorkflowControlDecision(right))
}

func cloneWorkflowFinalizerScopes(input []workflowruntime.FinalizerScope) []workflowruntime.FinalizerScope {
	result := make([]workflowruntime.FinalizerScope, len(input))
	for index, scope := range input {
		scope.Scope = append([]workflowruntime.NodeInvocationID(nil), scope.Scope...)
		result[index] = scope
	}
	return result
}

func cloneWorkflowTerminalIntent(input workflowruntime.TerminalIntentSnapshot) workflowruntime.TerminalIntentSnapshot {
	input.Reason = cloneWorkflowFailure(input.Reason)
	input.Error = cloneWorkflowValueRef(input.Error)
	input.Finalizers = cloneWorkflowFinalizerScopes(input.Finalizers)
	return input
}

func workflowFailureCode(failure *workflowruntime.Failure) string {
	if failure == nil {
		return ""
	}
	return failure.Code
}

func workflowHardFailure(status workflowruntime.NodeStatus) bool {
	switch status {
	case workflowruntime.NodeFailed, workflowruntime.NodeTimedOut, workflowruntime.NodeCanceled, workflowruntime.NodeCrashed:
		return true
	default:
		return false
	}
}

func validateWorkflowTerminalErrorOrigin(ctx context.Context, query workflowSQL, input values.ValueSet, origin workflowruntime.ControlErrorOrigin, intended workflowruntime.RunStatus) error {
	if origin.Invocation == nil {
		return nil
	}
	node, err := loadWorkflowNode(ctx, query, *origin.Invocation)
	if err != nil {
		return err
	}
	if !workflowHardFailure(node.Status) || string(node.Status) != string(intended) {
		return workflowInvalid(errors.New("terminal error node status differs from intended status"))
	}
	var expectedFailure *workflowruntime.Failure
	if origin.Attempt != nil {
		if node.LatestAttempt != origin.Attempt.Number {
			return workflowInvalid(errors.New("terminal error attempt is not the durable latest attempt"))
		}
		attempt, err := loadWorkflowAttempt(ctx, query, *origin.Attempt)
		if err != nil {
			return err
		}
		if attempt.Failure == nil || attempt.Status != node.Status {
			return workflowInvalid(errors.New("terminal error attempt does not contain the durable node failure"))
		}
		expectedFailure = attempt.Failure
	} else if node.LatestAttempt != 0 {
		return workflowInvalid(errors.New("terminal error omits the durable latest attempt"))
	}
	if err := workflowruntime.ValidateNodeControlErrorValues(input, node.ID, origin.Attempt, node.Status, expectedFailure); err != nil {
		return workflowInvalid(err)
	}
	return nil
}
