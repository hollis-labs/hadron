package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var (
	_ workflowruntime.RecoveryStore  = (*WorkflowStateStore)(nil)
	_ workflowruntime.ReplayStore    = (*WorkflowStateStore)(nil)
	_ workflowruntime.NodeInputStore = (*WorkflowStateStore)(nil)
)

func cloneWorkflowRetryActivationPointer(input *workflowruntime.RetryActivationSnapshot) *workflowruntime.RetryActivationSnapshot {
	if input == nil {
		return nil
	}
	cloned := cloneWorkflowRetryActivation(*input)
	return &cloned
}

func (s *WorkflowStateStore) BindNodeInputs(ctx context.Context, request workflowruntime.BindNodeInputsRequest) (workflowruntime.BindNodeInputsResult, error) {
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.BindNodeInputsResult{}, workflowInvalid(err)
	}
	requestJSON, err := encodeWorkflowJSON(request)
	if err != nil {
		return workflowruntime.BindNodeInputsResult{}, err
	}
	var result workflowruntime.BindNodeInputsResult
	writeErr := s.write(ctx, "bind workflow node inputs", func(query workflowSQL) error {
		var priorRun, priorNode, priorIteration, priorRequestJSON, priorResultJSON string
		replayErr := query.QueryRowContext(ctx, `SELECT run_id, node_id, iteration, request_json, result_json FROM workflow_node_input_bindings WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&priorRun, &priorNode, &priorIteration, &priorRequestJSON, &priorResultJSON)
		switch {
		case replayErr == nil:
			var priorRequest workflowruntime.BindNodeInputsRequest
			if err := decodeWorkflowJSON("node input binding request", priorRequestJSON, &priorRequest); err != nil {
				return err
			}
			if priorRun != string(priorRequest.InvocationID.RunID) || priorNode != priorRequest.InvocationID.NodeID || priorIteration != priorRequest.InvocationID.Iteration || !workflowruntime.SemanticallyEqualNodeInputRequest(priorRequest, request) {
				return workflowIdempotencyConflict("node input binding", request.IdempotencyKey)
			}
			if err := decodeWorkflowJSON("node input binding result", priorResultJSON, &result); err != nil {
				return err
			}
			persisted, err := loadWorkflowNode(ctx, query, request.InvocationID)
			if err != nil {
				return err
			}
			if persisted.Inputs == nil || *persisted.Inputs != result.Inputs || persisted.Generation < result.Node.Generation {
				return workflowInvalid(errors.New("node input binding projection differs from node"))
			}
			if _, err := loadWorkflowValues(ctx, query, result.Inputs); err != nil {
				return err
			}
			result.Outcome = workflowruntime.IdempotencyReplayed
			return nil
		case !errors.Is(replayErr, sql.ErrNoRows):
			return fmt.Errorf("load node input binding idempotency: %w", replayErr)
		}

		node, loadErr := loadWorkflowNode(ctx, query, request.InvocationID)
		if loadErr != nil {
			return loadErr
		}
		run, loadErr := loadWorkflowRun(ctx, query, request.InvocationID.RunID)
		if loadErr != nil {
			return loadErr
		}
		if !run.Status.Active() {
			return workflowInvalid(errors.New("terminal run fences node input binding"))
		}
		allowed, loadErr := workflowControlAdmissionAllowed(ctx, query, node.ID)
		if loadErr != nil {
			return loadErr
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences node input binding"))
		}
		if node.Generation != request.ExpectedGeneration {
			return workflowCAS("node invocation", request.ExpectedGeneration, node.Generation)
		}
		if node.Status != workflowruntime.NodePending && node.Status != workflowruntime.NodeBlocked {
			return workflowInvalid(errors.New("node input binding requires unclaimable pending or blocked status"))
		}
		if node.Inputs != nil || node.LatestAttempt != 0 || node.Lease != nil || node.Wait != nil || node.Outputs != nil {
			return workflowInvalid(errors.New("node input binding requires an unbound pristine invocation"))
		}
		if request.At.Before(node.UpdatedAt) {
			return workflowInvalid(errors.New("node input binding time must not regress"))
		}
		invocation := node.ID
		ref, insertErr := insertWorkflowValues(ctx, query, workflowruntime.ValueOwner{Kind: "node-inputs", RunID: invocation.RunID, Invocation: &invocation}, request.Values)
		if insertErr != nil {
			return insertErr
		}
		next := cloneWorkflowNode(node)
		next.Inputs, next.MemoKeyDigest, next.UpdatedAt = &ref, request.MemoKeyDigest, request.At
		next.Generation++
		if err := next.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowNodeCAS(ctx, query, next, node.Generation); err != nil {
			return err
		}
		event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Type: workflowruntime.EventNodeInputsBound, OccurredAt: request.At, Attributes: map[string]string{"digest": ref.Digest}, Values: &ref, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if err != nil {
			return err
		}
		result = workflowruntime.BindNodeInputsResult{Outcome: workflowruntime.IdempotencyApplied, Node: next, Inputs: ref, Event: event}
		resultJSON, err := encodeWorkflowJSON(result)
		if err != nil {
			return err
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_node_input_bindings(idempotency_key, run_id, node_id, iteration, request_json, result_json) VALUES (?, ?, ?, ?, ?, ?)`, request.IdempotencyKey, invocation.RunID, invocation.NodeID, invocation.Iteration, requestJSON, resultJSON); err != nil {
			if isSQLiteConstraint(err) {
				return workflowInvalid(errors.New("node inputs are already durably bound"))
			}
			return fmt.Errorf("record node input binding: %w", err)
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.BindNodeInputsResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) ReconcileCrashedAttempt(ctx context.Context, request workflowruntime.ReconcileCrashedAttemptRequest) (workflowruntime.ReconcileCrashedAttemptResult, error) {
	if err := request.Validate(); err != nil {
		return workflowruntime.ReconcileCrashedAttemptResult{}, workflowInvalid(err)
	}
	request = canonicalWorkflowCrashRequest(request)
	requestJSON, err := encodeWorkflowJSON(request)
	if err != nil {
		return workflowruntime.ReconcileCrashedAttemptResult{}, err
	}
	var result workflowruntime.ReconcileCrashedAttemptResult
	writeErr := s.write(ctx, "reconcile crashed workflow attempt", func(query workflowSQL) error {
		var priorRequest, priorResult string
		replayErr := query.QueryRowContext(ctx, `SELECT request_json, result_json FROM workflow_crash_recovery_idempotency WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&priorRequest, &priorResult)
		switch {
		case replayErr == nil:
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("crash recovery", request.IdempotencyKey)
			}
			if err := decodeWorkflowJSON("crash recovery result", priorResult, &result); err != nil {
				return err
			}
			if err := validateWorkflowCrashResult(request, result); err != nil {
				return workflowInvalid(err)
			}
			result.Outcome = workflowruntime.IdempotencyReplayed
			return nil
		case !errors.Is(replayErr, sql.ErrNoRows):
			return fmt.Errorf("load crash recovery idempotency: %w", replayErr)
		}
		node, loadErr := loadWorkflowNode(ctx, query, request.Attempt.Invocation)
		if loadErr != nil {
			return loadErr
		}
		if node.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("node invocation", request.ExpectedNodeGeneration, node.Generation)
		}
		run, loadErr := loadWorkflowRun(ctx, query, node.ID.RunID)
		if loadErr != nil {
			return loadErr
		}
		if !run.Status.Active() {
			return workflowInvalid(errors.New("terminal run fences crash reconciliation"))
		}
		allowed, loadErr := workflowControlAdmissionAllowed(ctx, query, node.ID)
		if loadErr != nil {
			return loadErr
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences crash reconciliation"))
		}
		if node.Status != workflowruntime.NodeRunning || node.LatestAttempt != request.Attempt.Number {
			return workflowInvalid(errors.New("crash recovery requires latest running invocation"))
		}
		attempt, loadErr := loadWorkflowAttempt(ctx, query, request.Attempt)
		if loadErr != nil {
			return loadErr
		}
		if attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("attempt", request.ExpectedAttemptGeneration, attempt.Generation)
		}
		if attempt.Status != workflowruntime.NodeRunning || !attempt.FinishedAt.IsZero() {
			return workflowInvalid(errors.New("crash recovery requires unfinished attempt"))
		}
		if node.Lease != nil && node.Lease.ExpiresAt.After(request.At) {
			return workflowruntime.ErrLeaseExpired
		}
		if request.At.Before(node.UpdatedAt) || request.At.Before(attempt.UpdatedAt) {
			return workflowInvalid(errors.New("crash recovery time must not regress"))
		}
		failure := &workflowruntime.Failure{Code: "HADR-PERSIST-001", Message: "executor interrupted before durable completion", Retryable: request.Decision.Action == workflowruntime.CrashRetry, Details: map[string]string{"policy_code": request.Decision.Policy.Code}}
		nextAttempt := cloneWorkflowAttempt(attempt)
		nextAttempt.Status, nextAttempt.Failure, nextAttempt.FinishedAt, nextAttempt.UpdatedAt = workflowruntime.NodeCrashed, failure, request.At, request.At
		nextAttempt.Generation++
		nextNode := cloneWorkflowNode(node)
		nextNode.Status, nextNode.Lease, nextNode.Blocked, nextNode.Outputs, nextNode.UpdatedAt = workflowruntime.NodeCrashed, nil, nil, nil, request.At
		var activation *workflowruntime.RetryActivationSnapshot
		if request.Decision.Action == workflowruntime.CrashRetry {
			nextNode.Status = workflowruntime.NodeWaiting
			encodedID, encodeErr := workflowruntime.EncodeAttemptIdentity(attempt.ID)
			if encodeErr != nil {
				return workflowInvalid(encodeErr)
			}
			candidate := workflowruntime.RetryActivationSnapshot{ID: "retry:" + encodedID, Attempt: attempt.ID, Failure: *cloneWorkflowFailure(failure), FireAt: request.Decision.Retry.FireAt.UTC(), Status: workflowruntime.RetryScheduled, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
			if err := candidate.Validate(); err != nil {
				return workflowInvalid(err)
			}
			encoded, encodeErr := encodeWorkflowJSON(candidate)
			if encodeErr != nil {
				return encodeErr
			}
			if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_retry_activations(activation_id, run_id, node_id, iteration, attempt_number, status, fire_at, generation, snapshot_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, candidate.ID, candidate.Attempt.Invocation.RunID, candidate.Attempt.Invocation.NodeID, candidate.Attempt.Invocation.Iteration, candidate.Attempt.Number, candidate.Status, workflowTime(candidate.FireAt), candidate.Generation, encoded); execErr != nil {
				if isSQLiteConstraint(execErr) {
					return fmt.Errorf("%w: retry activation or attempt binding", workflowruntime.ErrAlreadyExists)
				}
				return fmt.Errorf("insert crash retry activation: %w", execErr)
			}
			activation = &candidate
		} else {
			nextNode.Origin = workflowruntime.OriginExecuted
		}
		nextNode.Generation++
		if err := nextAttempt.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := nextNode.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := releaseWorkflowSchedulerResources(ctx, query, node.ID); err != nil {
			return err
		}
		if err := updateWorkflowAttemptCAS(ctx, query, nextAttempt, attempt.Generation); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); err != nil {
			return err
		}
		invocation, attemptID := nextNode.ID, nextAttempt.ID
		event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventCrashReconciled, OccurredAt: request.At, Attributes: map[string]string{"action": string(request.Decision.Action), "policy_code": request.Decision.Policy.Code, "executor_kind": attempt.Executor.Kind, "executor_version": attempt.Executor.Version}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if err != nil {
			return err
		}
		result = workflowruntime.ReconcileCrashedAttemptResult{Outcome: workflowruntime.IdempotencyApplied, Node: nextNode, Attempt: nextAttempt, Event: event, Activation: cloneWorkflowRetryActivationPointer(activation)}
		resultJSON, err := encodeWorkflowJSON(result)
		if err != nil {
			return err
		}
		_, err = query.ExecContext(ctx, `INSERT INTO workflow_crash_recovery_idempotency(idempotency_key, run_id, node_id, iteration, attempt_number, request_json, result_json) VALUES (?, ?, ?, ?, ?, ?, ?)`, request.IdempotencyKey, request.Attempt.Invocation.RunID, request.Attempt.Invocation.NodeID, request.Attempt.Invocation.Iteration, request.Attempt.Number, requestJSON, resultJSON)
		if err != nil {
			return fmt.Errorf("record crash recovery idempotency: %w", err)
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.ReconcileCrashedAttemptResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) BeginReplay(ctx context.Context, request workflowruntime.BeginReplayRequest) (workflowruntime.BeginReplayResult, error) {
	request = canonicalWorkflowReplayRequest(request)
	if err := request.Validate(); err != nil {
		return workflowruntime.BeginReplayResult{}, workflowInvalid(err)
	}
	requestJSON, err := encodeWorkflowJSON(request)
	if err != nil {
		return workflowruntime.BeginReplayResult{}, err
	}
	var result workflowruntime.BeginReplayResult
	writeErr := s.write(ctx, "begin workflow replay", func(query workflowSQL) error {
		var priorRequest, priorResult string
		var storedRun, storedSource, storedFrom, storedDigest, storedCreated string
		replayErr := query.QueryRowContext(ctx, `SELECT run_id, source_run_id, from_node_id, plan_digest, created_at, request_json, snapshot_json FROM workflow_replay_provenance WHERE idempotency_key = ?`, request.Provenance.IdempotencyKey).Scan(&storedRun, &storedSource, &storedFrom, &storedDigest, &storedCreated, &priorRequest, &priorResult)
		switch {
		case replayErr == nil:
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("begin replay", request.Provenance.IdempotencyKey)
			}
			if err := decodeWorkflowJSON("replay result", priorResult, &result); err != nil {
				return err
			}
			if err := validateWorkflowReplayProjection(storedRun, storedSource, storedFrom, storedDigest, storedCreated, result); err != nil {
				return workflowInvalid(err)
			}
			result.Outcome = workflowruntime.IdempotencyReplayed
			return nil
		case !errors.Is(replayErr, sql.ErrNoRows):
			return fmt.Errorf("load replay idempotency: %w", replayErr)
		}
		var existing int
		if err := query.QueryRowContext(ctx, `SELECT COUNT(1) FROM workflow_runs WHERE run_id = ?`, request.Provenance.RunID).Scan(&existing); err != nil {
			return err
		}
		if existing != 0 {
			return fmt.Errorf("%w: replay run", workflowruntime.ErrAlreadyExists)
		}
		source, loadErr := loadWorkflowRun(ctx, query, request.Provenance.SourceRunID)
		if loadErr != nil {
			return loadErr
		}
		if source.Generation != request.ExpectedSourceGeneration {
			return workflowCAS("source run", request.ExpectedSourceGeneration, source.Generation)
		}
		if !source.Status.Terminal() || source.Plan != request.Plan || !equalWorkflowValueRef(source.Inputs, request.Inputs) {
			return workflowInvalid(errors.New("replay source binding changed"))
		}
		if err := ensureWorkflowPlan(ctx, query, request.Plan); err != nil {
			return err
		}
		newNodes := make([]workflowruntime.NodeInvocationSnapshot, 0, len(request.Nodes))
		newAttempts := make([]workflowruntime.AttemptSnapshot, 0)
		newDecisions := make([]workflowruntime.ControlDecisionSnapshot, 0)
		newFanOuts := make([]workflowruntime.FanOutSnapshot, 0, len(request.FanOuts))
		for _, binding := range request.Nodes {
			current, err := loadWorkflowNode(ctx, query, binding.Source.ID)
			if err != nil {
				return err
			}
			if !equalWorkflowNodeSnapshot(current, binding.Source) {
				return fmt.Errorf("%w: replay source node changed", workflowruntime.ErrCASMismatch)
			}
			for _, ref := range []*values.ValueSetRef{binding.Source.Inputs, binding.Source.Outputs} {
				if ref == nil {
					continue
				}
				if _, err := loadWorkflowValues(ctx, query, *ref); err != nil {
					return err
				}
			}
			next := workflowruntime.NodeInvocationSnapshot{ID: binding.Target, Status: workflowruntime.NodePending, Priority: binding.Source.Priority, Generation: 1, CreatedAt: request.Provenance.CreatedAt, UpdatedAt: request.Provenance.CreatedAt}
			if binding.Reuse {
				next.Status, next.Blocked, next.Inputs, next.Outputs, next.LatestAttempt = binding.Source.Status, cloneWorkflowBlocked(binding.Source.Blocked), cloneWorkflowValueRef(binding.Source.Inputs), cloneWorkflowValueRef(binding.Source.Outputs), binding.Source.LatestAttempt
				next.Origin = workflowruntime.OriginReplayed
				for _, sourceAttempt := range binding.Attempts {
					persisted, err := loadWorkflowAttempt(ctx, query, sourceAttempt.ID)
					if err != nil {
						return err
					}
					if !equalWorkflowAttemptSnapshot(persisted, sourceAttempt) {
						return fmt.Errorf("%w: replay source attempt changed", workflowruntime.ErrCASMismatch)
					}
					rebound := cloneWorkflowAttempt(sourceAttempt)
					rebound.ID.Invocation = binding.Target
					newAttempts = append(newAttempts, rebound)
				}
			}
			if err := next.Validate(); err != nil {
				return workflowInvalid(err)
			}
			newNodes = append(newNodes, next)
			for _, decision := range binding.Control {
				persisted, decisionErr := loadWorkflowControlDecision(ctx, query, decision.ID)
				if decisionErr != nil {
					return decisionErr
				}
				if !equalWorkflowControlDecision(persisted, decision) {
					return fmt.Errorf("%w: replay control decision changed", workflowruntime.ErrCASMismatch)
				}
				if decision.Error != nil {
					if _, err := loadWorkflowValues(ctx, query, *decision.Error); err != nil {
						return err
					}
				}
				rebound, rebindErr := workflowruntime.RebindReplayControlDecision(decision, request.Provenance.RunID, request.Provenance.CreatedAt)
				if rebindErr != nil {
					return workflowInvalid(rebindErr)
				}
				newDecisions = append(newDecisions, rebound)
			}
		}
		for _, binding := range request.FanOuts {
			persisted, err := loadWorkflowFanOut(ctx, query, binding.Source.Parent)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(persisted, binding.Source) {
				return fmt.Errorf("%w: replay source fan-out changed", workflowruntime.ErrCASMismatch)
			}
			for _, item := range binding.Results {
				node, err := loadWorkflowNode(ctx, query, item.Invocation)
				if err != nil {
					return err
				}
				if node.Status != item.Status || !equalWorkflowValueRef(node.Outputs, item.Outputs) {
					return fmt.Errorf("%w: replay fan-out item changed", workflowruntime.ErrCASMismatch)
				}
			}
			newFanOuts = append(newFanOuts, cloneWorkflowFanOut(binding.Target))
		}
		run := workflowruntime.RunSnapshot{ID: request.Provenance.RunID, Plan: request.Plan, Status: workflowruntime.RunRunning, Inputs: cloneWorkflowValueRef(request.Inputs), Generation: 1, CreatedAt: request.Provenance.CreatedAt, UpdatedAt: request.Provenance.CreatedAt}
		if err := run.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := insertWorkflowRun(ctx, query, run); err != nil {
			return err
		}
		for _, node := range newNodes {
			if err := insertWorkflowNode(ctx, query, node); err != nil {
				return err
			}
		}
		for _, attempt := range newAttempts {
			if err := insertWorkflowAttempt(ctx, query, attempt); err != nil {
				return err
			}
		}
		for _, fanOut := range newFanOuts {
			encoded, err := encodeWorkflowJSON(fanOut)
			if err != nil {
				return err
			}
			if _, err := query.ExecContext(ctx, `INSERT INTO workflow_fanouts(run_id, node_id, iteration, status, max_concurrency, generation, snapshot_json) VALUES (?, ?, '', ?, ?, ?, ?)`, fanOut.Parent.RunID, fanOut.Parent.NodeID, fanOut.Status, fanOut.MaxConcurrency, fanOut.Generation, encoded); err != nil {
				return fmt.Errorf("insert replay fan-out: %w", err)
			}
			for _, item := range fanOut.Items {
				encodedRef, err := encodeWorkflowJSON(item.Inputs)
				if err != nil {
					return err
				}
				if _, err := query.ExecContext(ctx, `INSERT INTO workflow_fanout_items(run_id, node_id, item_index, iteration, inputs_ref_json) VALUES (?, ?, ?, ?, ?)`, fanOut.Parent.RunID, fanOut.Parent.NodeID, item.Index, item.Iteration, encodedRef); err != nil {
					return fmt.Errorf("insert replay fan-out item: %w", err)
				}
			}
		}
		for _, decision := range newDecisions {
			encoded, err := encodeWorkflowJSON(decision)
			if err != nil {
				return err
			}
			errorSequence, err := workflowControlValueSequence(decision.Error)
			if err != nil {
				return err
			}
			_, err = query.ExecContext(ctx, `INSERT INTO workflow_control_decisions(run_id, node_id, iteration, kind, outcome, source_generation, generation, created_at, error_values_sequence, snapshot_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, decision.ID.Source.RunID, decision.ID.Source.NodeID, decision.ID.Source.Iteration, decision.ID.Kind, decision.Outcome, decision.SourceGeneration, decision.Generation, workflowTime(decision.CreatedAt), errorSequence, encoded)
			if err != nil {
				return fmt.Errorf("insert replay control decision: %w", err)
			}
		}
		event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: run.ID, Type: workflowruntime.EventReplayCreated, OccurredAt: run.CreatedAt, Attributes: map[string]string{"source_run_id": string(source.ID), "from_node_id": request.Provenance.FromNodeID, "plan_digest": request.Plan.Digest}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if err != nil {
			return err
		}
		result = workflowruntime.BeginReplayResult{Outcome: workflowruntime.IdempotencyApplied, Run: run, Provenance: request.Provenance, Nodes: newNodes, Event: event}
		resultJSON, err := encodeWorkflowJSON(result)
		if err != nil {
			return err
		}
		_, err = query.ExecContext(ctx, `INSERT INTO workflow_replay_provenance(run_id, source_run_id, from_node_id, plan_digest, idempotency_key, created_at, request_json, snapshot_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, run.ID, source.ID, request.Provenance.FromNodeID, request.Plan.Digest, request.Provenance.IdempotencyKey, workflowTime(request.Provenance.CreatedAt), requestJSON, resultJSON)
		if err != nil {
			return fmt.Errorf("record workflow replay provenance: %w", err)
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.BeginReplayResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) ListRunInvocations(ctx context.Context, runID workflowruntime.RunID) ([]workflowruntime.NodeInvocationSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if _, err := loadWorkflowRun(ctx, s.db, runID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, workflowNodeSelect+` WHERE n.run_id = ? ORDER BY n.node_id, n.iteration`, runID)
	if err != nil {
		return nil, fmt.Errorf("list workflow run invocations: %w", err)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workflow run invocations: %w", err)
	}
	return result, nil
}

func (s *WorkflowStateStore) LoadReplayProvenance(ctx context.Context, runID workflowruntime.RunID) (workflowruntime.ReplayProvenance, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.ReplayProvenance{}, err
	}
	var storedRun, source, from, digest, created, resultJSON string
	err := s.db.QueryRowContext(ctx, `SELECT run_id, source_run_id, from_node_id, plan_digest, created_at, snapshot_json FROM workflow_replay_provenance WHERE run_id = ?`, runID).Scan(&storedRun, &source, &from, &digest, &created, &resultJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return workflowruntime.ReplayProvenance{}, fmt.Errorf("%w: replay provenance", workflowruntime.ErrNotFound)
	}
	if err != nil {
		return workflowruntime.ReplayProvenance{}, fmt.Errorf("load workflow replay provenance: %w", err)
	}
	var result workflowruntime.BeginReplayResult
	if err := decodeWorkflowJSON("replay result", resultJSON, &result); err != nil {
		return workflowruntime.ReplayProvenance{}, err
	}
	if err := validateWorkflowReplayProjection(storedRun, source, from, digest, created, result); err != nil {
		return workflowruntime.ReplayProvenance{}, workflowInvalid(err)
	}
	return result.Provenance, nil
}

func canonicalWorkflowCrashRequest(r workflowruntime.ReconcileCrashedAttemptRequest) workflowruntime.ReconcileCrashedAttemptRequest {
	r.At = r.At.UTC()
	if r.Decision.Retry != nil {
		retry := *r.Decision.Retry
		retry.FireAt = retry.FireAt.UTC()
		r.Decision.Retry = &retry
	}
	return r
}
func canonicalWorkflowReplayRequest(r workflowruntime.BeginReplayRequest) workflowruntime.BeginReplayRequest {
	r = cloneWorkflowReplayRequest(r)
	r.Provenance.CreatedAt = r.Provenance.CreatedAt.UTC()
	for i := range r.Nodes {
		r.Nodes[i].Source.CreatedAt = r.Nodes[i].Source.CreatedAt.UTC()
		r.Nodes[i].Source.UpdatedAt = r.Nodes[i].Source.UpdatedAt.UTC()
		if r.Nodes[i].Source.Lease != nil {
			r.Nodes[i].Source.Lease.ExpiresAt = r.Nodes[i].Source.Lease.ExpiresAt.UTC()
		}
		for j := range r.Nodes[i].Attempts {
			r.Nodes[i].Attempts[j].StartedAt = r.Nodes[i].Attempts[j].StartedAt.UTC()
			r.Nodes[i].Attempts[j].FinishedAt = r.Nodes[i].Attempts[j].FinishedAt.UTC()
			r.Nodes[i].Attempts[j].CreatedAt = r.Nodes[i].Attempts[j].CreatedAt.UTC()
			r.Nodes[i].Attempts[j].UpdatedAt = r.Nodes[i].Attempts[j].UpdatedAt.UTC()
		}
		for j := range r.Nodes[i].Control {
			r.Nodes[i].Control[j].CreatedAt = r.Nodes[i].Control[j].CreatedAt.UTC()
		}
	}
	for i := range r.FanOuts {
		r.FanOuts[i].Source.CreatedAt = r.FanOuts[i].Source.CreatedAt.UTC()
		r.FanOuts[i].Source.UpdatedAt = r.FanOuts[i].Source.UpdatedAt.UTC()
		r.FanOuts[i].Target.CreatedAt = r.FanOuts[i].Target.CreatedAt.UTC()
		r.FanOuts[i].Target.UpdatedAt = r.FanOuts[i].Target.UpdatedAt.UTC()
	}
	return r
}

func cloneWorkflowReplayRequest(r workflowruntime.BeginReplayRequest) workflowruntime.BeginReplayRequest {
	r.Inputs = cloneWorkflowValueRef(r.Inputs)
	r.Provenance.Policy = append([]workflowruntime.ReplayNodePolicy(nil), r.Provenance.Policy...)
	for i := range r.Provenance.Policy {
		r.Provenance.Policy[i].Attempt = cloneWorkflowAttemptID(r.Provenance.Policy[i].Attempt)
		r.Provenance.Policy[i].Decision.Attributes = cloneWorkflowStringMap(r.Provenance.Policy[i].Decision.Attributes)
	}
	r.Nodes = append([]workflowruntime.ReplayNodeBinding(nil), r.Nodes...)
	for i := range r.Nodes {
		r.Nodes[i].Source = cloneWorkflowNode(r.Nodes[i].Source)
		r.Nodes[i].Attempts = append([]workflowruntime.AttemptSnapshot(nil), r.Nodes[i].Attempts...)
		for j := range r.Nodes[i].Attempts {
			r.Nodes[i].Attempts[j] = cloneWorkflowAttempt(r.Nodes[i].Attempts[j])
		}
		r.Nodes[i].Control = append([]workflowruntime.ControlDecisionSnapshot(nil), r.Nodes[i].Control...)
		for j := range r.Nodes[i].Control {
			r.Nodes[i].Control[j] = cloneWorkflowControlDecision(r.Nodes[i].Control[j])
		}
	}
	r.FanOuts = append([]workflowruntime.ReplayFanOutBinding(nil), r.FanOuts...)
	for i := range r.FanOuts {
		r.FanOuts[i].Source = cloneWorkflowFanOut(r.FanOuts[i].Source)
		r.FanOuts[i].Target = cloneWorkflowFanOut(r.FanOuts[i].Target)
		r.FanOuts[i].Results = append([]workflowruntime.FanOutItemResult(nil), r.FanOuts[i].Results...)
		for j := range r.FanOuts[i].Results {
			r.FanOuts[i].Results[j].Outputs = cloneWorkflowValueRef(r.FanOuts[i].Results[j].Outputs)
			r.FanOuts[i].Results[j].Failure = cloneWorkflowFailure(r.FanOuts[i].Results[j].Failure)
		}
	}
	return r
}

func validateWorkflowCrashResult(request workflowruntime.ReconcileCrashedAttemptRequest, result workflowruntime.ReconcileCrashedAttemptResult) error {
	if result.Outcome != workflowruntime.IdempotencyApplied || result.Attempt.ID != request.Attempt || result.Node.ID != request.Attempt.Invocation || result.Event.Type != workflowruntime.EventCrashReconciled || !result.Event.OccurredAt.Equal(request.At) {
		return errors.New("persisted crash result does not match request")
	}
	if err := result.Node.Validate(); err != nil {
		return err
	}
	if err := result.Attempt.Validate(); err != nil {
		return err
	}
	if request.Decision.Action == workflowruntime.CrashRetry {
		if result.Activation == nil || result.Node.Status != workflowruntime.NodeWaiting || result.Activation.Attempt != request.Attempt {
			return errors.New("persisted crash retry activation does not match request")
		}
		if err := result.Activation.Validate(); err != nil {
			return err
		}
	} else if result.Activation != nil {
		return errors.New("terminal crash result carries retry activation")
	}
	return result.Event.Validate()
}

func equalWorkflowNodeSnapshot(a, b workflowruntime.NodeInvocationSnapshot) bool {
	return a.ID == b.ID && a.Status == b.Status && equalWorkflowBlocked(a.Blocked, b.Blocked) && equalWorkflowValueRef(a.Inputs, b.Inputs) && equalWorkflowValueRef(a.Outputs, b.Outputs) && a.Origin == b.Origin && a.MemoKeyDigest == b.MemoKeyDigest && reflect.DeepEqual(a.Wait, b.Wait) && a.LatestAttempt == b.LatestAttempt && a.Priority == b.Priority && a.ClaimGeneration == b.ClaimGeneration && equalWorkflowLease(a.Lease, b.Lease) && a.Generation == b.Generation && a.CreatedAt.Equal(b.CreatedAt) && a.UpdatedAt.Equal(b.UpdatedAt)
}

func equalWorkflowAttemptSnapshot(a, b workflowruntime.AttemptSnapshot) bool {
	return a.ID == b.ID && a.Status == b.Status && reflect.DeepEqual(a.Executor, b.Executor) && equalWorkflowValueRef(a.Inputs, b.Inputs) && equalWorkflowValueRef(a.Outputs, b.Outputs) && reflect.DeepEqual(a.Failure, b.Failure) && a.StartedAt.Equal(b.StartedAt) && a.FinishedAt.Equal(b.FinishedAt) && a.Generation == b.Generation && a.CreatedAt.Equal(b.CreatedAt) && a.UpdatedAt.Equal(b.UpdatedAt)
}

func validateWorkflowReplayProjection(run, source, from, digest, created string, result workflowruntime.BeginReplayResult) error {
	if err := result.Provenance.Validate(); err != nil {
		return err
	}
	createdAt, err := parseWorkflowTime("replay created_at", created)
	if err != nil {
		return err
	}
	if string(result.Run.ID) != run || string(result.Provenance.RunID) != run || string(result.Provenance.SourceRunID) != source || result.Provenance.FromNodeID != from || result.Provenance.PlanDigest != digest || !result.Provenance.CreatedAt.Equal(createdAt) || result.Event.Type != workflowruntime.EventReplayCreated {
		return errors.New("replay indexed columns differ from immutable snapshot")
	}
	if err := result.Run.Validate(); err != nil {
		return err
	}
	if err := result.Event.Validate(); err != nil {
		return err
	}
	for _, node := range result.Nodes {
		if err := node.Validate(); err != nil {
			return err
		}
	}
	return nil
}
