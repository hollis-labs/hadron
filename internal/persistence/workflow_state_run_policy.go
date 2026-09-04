package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

var _ workflowruntime.RunPolicyStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) LoadRunPolicyDecision(ctx context.Context, runID workflowruntime.RunID) (workflowruntime.RunPolicyDecisionSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.RunPolicyDecisionSnapshot{}, err
	}
	return loadWorkflowRunPolicyDecision(ctx, s.db, runID)
}

func (s *WorkflowStateStore) ApplyRunFailurePolicy(ctx context.Context, request workflowruntime.ApplyRunFailurePolicyRequest) (workflowruntime.ApplyRunFailurePolicyResult, error) {
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ApplyRunFailurePolicyResult{}, workflowInvalid(err)
	}
	canonical := request
	canonical.ExpectedRunGeneration = 0
	canonical.ExpectedSourceGeneration = 0
	requestJSON, encodeErr := encodeWorkflowJSON(canonical)
	if encodeErr != nil {
		return workflowruntime.ApplyRunFailurePolicyResult{}, encodeErr
	}
	var result workflowruntime.ApplyRunFailurePolicyResult
	writeErr := s.write(ctx, "apply workflow run failure policy", func(query workflowSQL) error {
		prior, priorRequest, loadErr := loadWorkflowRunPolicyDecisionRecord(ctx, query, request.RunID)
		if loadErr == nil {
			if prior.IdempotencyKey == request.IdempotencyKey {
				if priorRequest != requestJSON {
					return workflowIdempotencyConflict("run failure policy", request.IdempotencyKey)
				}
				return loadWorkflowRunPolicyResult(ctx, query, prior, workflowruntime.RunFailureFailFast, &result)
			}
			return loadWorkflowRunPolicyResult(ctx, query, prior, workflowruntime.RunFailureAlreadyDecided, &result)
		}
		if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		var sameKeyRun workflowruntime.RunID
		if err := query.QueryRowContext(ctx, `SELECT run_id FROM workflow_run_policy_decisions WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&sameKeyRun); err == nil {
			return workflowIdempotencyConflict("run failure policy", request.IdempotencyKey)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		run, err := loadWorkflowRun(ctx, query, request.RunID)
		if err != nil {
			return err
		}
		if run.Generation != request.ExpectedRunGeneration {
			return workflowCAS("run failure policy", request.ExpectedRunGeneration, run.Generation)
		}
		if !run.Status.Active() || request.At.Before(run.UpdatedAt) {
			return workflowInvalid(errors.New("fail-fast requires active run and non-regressing time"))
		}
		source, err := loadWorkflowNode(ctx, query, request.Trigger)
		if err != nil {
			return err
		}
		if source.Generation != request.ExpectedSourceGeneration || !workflowHardFailure(source.Status) || source.ID.Iteration != "" {
			return workflowInvalid(errors.New("fail-fast source generation or status differs"))
		}
		var attempt *workflowruntime.AttemptID
		var expectedFailure *workflowruntime.Failure
		if source.LatestAttempt > 0 {
			id := workflowruntime.AttemptID{Invocation: source.ID, Number: source.LatestAttempt}
			persisted, attemptErr := loadWorkflowAttempt(ctx, query, id)
			if attemptErr != nil {
				return attemptErr
			}
			if persisted.Status != source.Status || persisted.Failure == nil {
				return workflowInvalid(errors.New("fail-fast source attempt has no durable failure"))
			}
			attempt, expectedFailure = &id, persisted.Failure
		}
		if validationErr := workflowruntime.ValidateNodeControlErrorValues(request.ErrorValues, source.ID, attempt, source.Status, expectedFailure); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		begin, err := beginWorkflowTerminalIntent(ctx, query, workflowruntime.BeginTerminalIntentRequest{RunID: request.RunID, ExpectedRunGeneration: request.ExpectedRunGeneration, IntendedStatus: request.IntendedStatus, Reason: &request.Reason, ErrorValues: request.ErrorValues, CompensationRequired: request.CompensationRequired, IdempotencyKey: request.IdempotencyKey, Finalizers: request.Finalizers, At: request.At})
		if err != nil {
			return err
		}
		excluded := map[workflowruntime.NodeInvocationID]struct{}{request.Trigger: struct{}{}}
		for _, finalizer := range request.Finalizers {
			excluded[finalizer.Invocation] = struct{}{}
		}
		collector := workflowCancellationCollector{}
		cancelReason := workflowruntime.Failure{Code: "run_fail_fast", Message: "run fail-fast policy stopped remaining ordinary work", Details: map[string]string{"trigger_node": request.Trigger.NodeID}}
		if cancelErr := cancelWorkflowRunWithOptions(ctx, query, request.RunID, request.At, cancelReason, request.IdempotencyKey, make(map[workflowruntime.RunID]bool), &collector, excluded, false, false); cancelErr != nil {
			return cancelErr
		}
		decision := workflowruntime.RunPolicyDecisionSnapshot{RunID: request.RunID, Mode: graph.CompletionFailFast, Trigger: request.Trigger, SourceGeneration: request.ExpectedSourceGeneration, IntendedStatus: request.IntendedStatus, IdempotencyKey: request.IdempotencyKey, Generation: 1, CreatedAt: request.At}
		if decisionErr := decision.Validate(); decisionErr != nil {
			return workflowInvalid(decisionErr)
		}
		encoded, err := encodeWorkflowJSON(decision)
		if err != nil {
			return err
		}
		if _, insertErr := query.ExecContext(ctx, `INSERT INTO workflow_run_policy_decisions(run_id, idempotency_key, request_json, snapshot_json, created_at) VALUES (?, ?, ?, ?, ?)`, decision.RunID, decision.IdempotencyKey, requestJSON, encoded, workflowTime(decision.CreatedAt)); insertErr != nil {
			return insertErr
		}
		policyEvent, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: request.RunID, Invocation: &request.Trigger, Type: workflowruntime.EventRunFailFastTriggered, OccurredAt: request.At, Attributes: map[string]string{"trigger_node": request.Trigger.NodeID, "intended_status": string(request.IntendedStatus), "reason_code": request.Reason.Code}, Values: cloneWorkflowValueRef(begin.Intent.Error), Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if err != nil {
			return err
		}
		events := make([]workflowruntime.Event, 0, len(collector.events)+2)
		if begin.Event != nil {
			events = append(events, *begin.Event)
		}
		events = append(events, collector.events...)
		events = append(events, policyEvent)
		result = workflowruntime.ApplyRunFailurePolicyResult{Disposition: workflowruntime.RunFailureFailFast, Decision: decision, Run: begin.Run, Intent: begin.Intent, Nodes: collector.nodes, Intents: collector.intents, Events: events}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.ApplyRunFailurePolicyResult{}, writeErr
	}
	return result, nil
}

func loadWorkflowRunPolicyDecision(ctx context.Context, query workflowSQL, runID workflowruntime.RunID) (workflowruntime.RunPolicyDecisionSnapshot, error) {
	decision, _, err := loadWorkflowRunPolicyDecisionRecord(ctx, query, runID)
	return decision, err
}

func loadWorkflowRunPolicyDecisionRecord(ctx context.Context, query workflowSQL, runID workflowruntime.RunID) (workflowruntime.RunPolicyDecisionSnapshot, string, error) {
	var storedRunID, idempotencyKey, requestJSON, encoded, createdAt string
	if err := query.QueryRowContext(ctx, `SELECT run_id, idempotency_key, request_json, snapshot_json, created_at FROM workflow_run_policy_decisions WHERE run_id = ?`, runID).Scan(&storedRunID, &idempotencyKey, &requestJSON, &encoded, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.RunPolicyDecisionSnapshot{}, "", fmt.Errorf("%w: run policy decision", workflowruntime.ErrNotFound)
		}
		return workflowruntime.RunPolicyDecisionSnapshot{}, "", err
	}
	var decision workflowruntime.RunPolicyDecisionSnapshot
	if err := decodeWorkflowJSON("run policy decision", encoded, &decision); err != nil {
		return workflowruntime.RunPolicyDecisionSnapshot{}, "", err
	}
	parsedCreated, err := parseWorkflowTime("run policy decision created_at", createdAt)
	if err != nil {
		return workflowruntime.RunPolicyDecisionSnapshot{}, "", err
	}
	var request workflowruntime.ApplyRunFailurePolicyRequest
	if err := decodeWorkflowJSON("run policy decision request", requestJSON, &request); err != nil {
		return workflowruntime.RunPolicyDecisionSnapshot{}, "", err
	}
	columnsMatch := decision.RunID == workflowruntime.RunID(storedRunID) && decision.IdempotencyKey == idempotencyKey && decision.CreatedAt.Equal(parsedCreated)
	requestMatches := request.RunID == decision.RunID && request.Trigger == decision.Trigger && request.IntendedStatus == decision.IntendedStatus &&
		request.IdempotencyKey == decision.IdempotencyKey && request.At.Equal(decision.CreatedAt)
	if !columnsMatch || !requestMatches {
		return workflowruntime.RunPolicyDecisionSnapshot{}, "", workflowInvalid(errors.New("run policy decision columns or request diverge from semantic snapshot"))
	}
	if err := decision.Validate(); err != nil {
		return workflowruntime.RunPolicyDecisionSnapshot{}, "", workflowInvalid(err)
	}
	return decision, requestJSON, nil
}

func loadWorkflowRunPolicyResult(ctx context.Context, query workflowSQL, decision workflowruntime.RunPolicyDecisionSnapshot, disposition workflowruntime.RunFailureDisposition, result *workflowruntime.ApplyRunFailurePolicyResult) error {
	run, err := loadWorkflowRun(ctx, query, decision.RunID)
	if err != nil {
		return err
	}
	intent, err := loadWorkflowTerminalIntent(ctx, query, decision.RunID)
	if err != nil {
		return err
	}
	intents, err := listWorkflowCancellationIntents(ctx, query, decision.RunID)
	if err != nil {
		return err
	}
	*result = workflowruntime.ApplyRunFailurePolicyResult{Disposition: disposition, Decision: decision, Run: run, Intent: intent, Intents: intents}
	return nil
}

func listWorkflowCancellationIntents(ctx context.Context, query workflowSQL, runID workflowruntime.RunID) ([]workflowruntime.CancellationIntentSnapshot, error) {
	rows, err := query.QueryContext(ctx, `SELECT snapshot_json FROM workflow_cancellation_intents WHERE run_id = ? ORDER BY intent_id`, runID)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]workflowruntime.CancellationIntentSnapshot, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var intent workflowruntime.CancellationIntentSnapshot
		if err := decodeWorkflowJSON("cancellation intent", encoded, &intent); err != nil {
			return nil, err
		}
		result = append(result, intent)
	}
	return result, rows.Err()
}
