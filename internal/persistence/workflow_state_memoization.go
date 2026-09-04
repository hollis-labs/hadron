package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

var (
	_ workflowruntime.MemoStore        = (*WorkflowStateStore)(nil)
	_ workflowruntime.PinStore         = (*WorkflowStateStore)(nil)
	_ workflowruntime.OutputReuseStore = (*WorkflowStateStore)(nil)
	_ workflowruntime.ValueRecordStore = (*WorkflowStateStore)(nil)
)

func (s *WorkflowStateStore) LoadValueRecord(ctx context.Context, ref values.ValueSetRef) (workflowruntime.ValueRecord, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.ValueRecord{}, err
	}
	return loadWorkflowValueRecord(ctx, s.db, ref)
}

func loadWorkflowValueRecord(ctx context.Context, query workflowSQL, ref values.ValueSetRef) (workflowruntime.ValueRecord, error) {
	if err := ref.Validate(); err != nil {
		return workflowruntime.ValueRecord{}, workflowInvalid(err)
	}
	sequence, parseErr := parseWorkflowValueID(ref.ID)
	if parseErr != nil {
		return workflowruntime.ValueRecord{}, parseErr
	}
	var storedDigest, ownerJSON, valuesJSON string
	if queryErr := query.QueryRowContext(ctx, `SELECT digest, owner_json, values_json FROM workflow_value_sets WHERE sequence = ?`, sequence).Scan(&storedDigest, &ownerJSON, &valuesJSON); queryErr != nil {
		if errors.Is(queryErr, sql.ErrNoRows) {
			return workflowruntime.ValueRecord{}, fmt.Errorf("%w: value set %q", workflowruntime.ErrNotFound, ref.ID)
		}
		return workflowruntime.ValueRecord{}, fmt.Errorf("load workflow value record: %w", queryErr)
	}
	if storedDigest != ref.Digest {
		return workflowruntime.ValueRecord{}, fmt.Errorf("%w: value set digest", workflowruntime.ErrCASMismatch)
	}
	var owner workflowruntime.ValueOwner
	if decodeErr := decodeWorkflowJSON("value owner", ownerJSON, &owner); decodeErr != nil {
		return workflowruntime.ValueRecord{}, decodeErr
	}
	var set values.ValueSet
	if decodeErr := decodeWorkflowJSON("value set", valuesJSON, &set); decodeErr != nil {
		return workflowruntime.ValueRecord{}, decodeErr
	}
	computed, digestErr := values.DigestValueSet(set)
	if digestErr != nil || computed != storedDigest {
		return workflowruntime.ValueRecord{}, workflowInvalid(errors.New("persisted value-set content does not match its digest"))
	}
	record := workflowruntime.ValueRecord{Ref: ref, Owner: owner, Values: set}
	if validationErr := record.Validate(); validationErr != nil {
		return workflowruntime.ValueRecord{}, workflowInvalid(validationErr)
	}
	return record, nil
}

func (s *WorkflowStateStore) RecordMemoEntry(ctx context.Context, entry workflowruntime.MemoEntry) (workflowruntime.MemoEntry, workflowruntime.IdempotencyOutcome, error) {
	entry = cloneWorkflowMemoEntry(entry)
	entry.CreatedAt, entry.ExpiresAt = entry.CreatedAt.UTC(), entry.ExpiresAt.UTC()
	if err := entry.Validate(); err != nil {
		return workflowruntime.MemoEntry{}, "", workflowInvalid(err)
	}
	var result workflowruntime.MemoEntry
	var outcome workflowruntime.IdempotencyOutcome
	writeErr := s.write(ctx, "record workflow memo entry", func(query workflowSQL) error {
		var priorJSON string
		replayErr := query.QueryRowContext(ctx, `SELECT entry_json FROM workflow_memo_entries WHERE source_run_id = ? AND source_node_id = ? AND source_iteration = ? AND source_attempt = ?`, entry.Source.RunID, entry.Source.NodeID, entry.Source.Iteration, entry.SourceAttempt.Number).Scan(&priorJSON)
		if replayErr == nil {
			if decodeErr := decodeWorkflowJSON("memo entry", priorJSON, &result); decodeErr != nil {
				return decodeErr
			}
			if !reflect.DeepEqual(result, entry) {
				return workflowIdempotencyConflict("memo publication", memoWorkflowSourceKey(entry.SourceAttempt))
			}
			outcome = workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(replayErr, sql.ErrNoRows) {
			return fmt.Errorf("load workflow memo publication: %w", replayErr)
		}
		source, err := loadWorkflowNode(ctx, query, entry.Source)
		if err != nil {
			return err
		}
		attempt, err := loadWorkflowAttempt(ctx, query, entry.SourceAttempt)
		if err != nil {
			return err
		}
		if source.Status != workflowruntime.NodeSucceeded || source.Outputs == nil || *source.Outputs != entry.Outputs || attempt.Status != workflowruntime.NodeSucceeded || attempt.Outputs == nil || *attempt.Outputs != entry.Outputs {
			return workflowInvalid(errors.New("memo source is not the durable succeeded output"))
		}
		if source.Origin != entry.SourceOrigin {
			return workflowInvalid(errors.New("memo source origin changed"))
		}
		if source.ID.NodeID != entry.NodeID || source.MemoKeyDigest != entry.MemoKeyDigest || source.Inputs == nil || source.Inputs.Digest != entry.InputDigest {
			return workflowInvalid(errors.New("memo source binding changed"))
		}
		sourceRun, err := loadWorkflowRun(ctx, query, source.ID.RunID)
		if err != nil {
			return err
		}
		if sourceRun.Plan.Digest != entry.PlanDigest {
			return workflowInvalid(errors.New("memo source plan changed"))
		}
		if attempt.Executor.Kind != entry.Kind || attempt.Executor.Version != entry.KindVersion {
			return workflowInvalid(errors.New("memo source executor changed"))
		}
		set, err := loadWorkflowValues(ctx, query, entry.Outputs)
		if err != nil {
			return err
		}
		if validationErr := workflowruntime.ValidateMemoizableValueSet(set); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		encoded, err := encodeWorkflowJSON(entry)
		if err != nil {
			return err
		}
		_, err = query.ExecContext(ctx, `INSERT INTO workflow_memo_entries(cache_key, source_run_id, source_node_id, source_iteration, source_attempt, entry_json, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, entry.Key, entry.Source.RunID, entry.Source.NodeID, entry.Source.Iteration, entry.SourceAttempt.Number, encoded, workflowTime(entry.CreatedAt), workflowTime(entry.ExpiresAt))
		if err != nil {
			return fmt.Errorf("insert workflow memo entry: %w", err)
		}
		result, outcome = entry, workflowruntime.IdempotencyApplied
		return nil
	})
	if writeErr != nil {
		return workflowruntime.MemoEntry{}, "", writeErr
	}
	return cloneWorkflowMemoEntry(result), outcome, nil
}

func (s *WorkflowStateStore) LoadMemoEntry(ctx context.Context, key string) (workflowruntime.MemoEntry, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.MemoEntry{}, err
	}
	if err := values.ValidateDigest(key); err != nil {
		return workflowruntime.MemoEntry{}, workflowInvalid(err)
	}
	var encoded string
	if err := s.db.QueryRowContext(ctx, `SELECT entry_json FROM workflow_memo_entries WHERE cache_key = ? ORDER BY created_at DESC, sequence DESC LIMIT 1`, key).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.MemoEntry{}, fmt.Errorf("%w: memo entry", workflowruntime.ErrNotFound)
		}
		return workflowruntime.MemoEntry{}, fmt.Errorf("load workflow memo entry: %w", err)
	}
	var entry workflowruntime.MemoEntry
	if err := decodeWorkflowJSON("memo entry", encoded, &entry); err != nil {
		return workflowruntime.MemoEntry{}, err
	}
	if err := entry.Validate(); err != nil {
		return workflowruntime.MemoEntry{}, workflowInvalid(err)
	}
	return entry, nil
}

func (s *WorkflowStateStore) BindPin(ctx context.Context, request workflowruntime.BindPinRequest) (workflowruntime.BindPinResult, error) {
	request.Binding = cloneWorkflowPinBinding(request.Binding)
	request.Binding.BoundAt = request.Binding.BoundAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.BindPinResult{}, workflowInvalid(err)
	}
	var result workflowruntime.BindPinResult
	writeErr := s.write(ctx, "bind workflow pin", func(query workflowSQL) error {
		var priorRequestJSON, priorResultJSON string
		replayErr := query.QueryRowContext(ctx, `SELECT request_json, result_json FROM workflow_pin_bindings WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&priorRequestJSON, &priorResultJSON)
		if replayErr == nil {
			return replayWorkflowPin(priorRequestJSON, priorResultJSON, request, &result)
		}
		if !errors.Is(replayErr, sql.ErrNoRows) {
			return fmt.Errorf("load workflow pin replay: %w", replayErr)
		}
		targetReplayErr := query.QueryRowContext(ctx, `SELECT request_json, result_json FROM workflow_pin_bindings WHERE run_id = ? AND node_id = ? AND iteration = ?`, request.Binding.Target.RunID, request.Binding.Target.NodeID, request.Binding.Target.Iteration).Scan(&priorRequestJSON, &priorResultJSON)
		if targetReplayErr == nil {
			return replayWorkflowPin(priorRequestJSON, priorResultJSON, request, &result)
		}
		if !errors.Is(targetReplayErr, sql.ErrNoRows) {
			return fmt.Errorf("load workflow pin target: %w", targetReplayErr)
		}
		node, err := loadWorkflowNode(ctx, query, request.Binding.Target)
		if err != nil {
			return err
		}
		if node.Generation != request.ExpectedGeneration {
			return workflowCAS("node invocation", request.ExpectedGeneration, node.Generation)
		}
		if node.Status != workflowruntime.NodePending && node.Status != workflowruntime.NodeBlocked {
			return workflowInvalid(errors.New("pin target must be pending or blocked"))
		}
		if node.Lease != nil || node.LatestAttempt != 0 || node.Outputs != nil {
			return workflowInvalid(errors.New("pin target is already claimed, attempted, or complete"))
		}
		if request.Binding.BoundAt.Before(node.UpdatedAt) {
			return workflowInvalid(errors.New("pin binding time must not regress"))
		}
		run, err := loadWorkflowRun(ctx, query, node.ID.RunID)
		if err != nil {
			return err
		}
		if !run.Status.Active() || run.Plan.Digest != request.Binding.PlanDigest {
			return workflowInvalid(errors.New("pin target run or plan changed"))
		}
		source, err := loadWorkflowNode(ctx, query, request.Binding.Source)
		if err != nil {
			return err
		}
		if source.Status != workflowruntime.NodeSucceeded || source.Outputs == nil || *source.Outputs != request.Binding.Outputs || source.Origin != request.Binding.SourceOrigin {
			return workflowInvalid(errors.New("pin source outcome changed"))
		}
		sourceRun, err := loadWorkflowRun(ctx, query, source.ID.RunID)
		if err != nil {
			return err
		}
		if sourceRun.Plan.Digest != request.Binding.SourcePlanDigest {
			return workflowInvalid(errors.New("pin source plan changed"))
		}
		set, err := loadWorkflowValues(ctx, query, request.Binding.Outputs)
		if err != nil {
			return err
		}
		if validationErr := workflowruntime.ValidatePinnableValueSet(set, source.ID.RunID == node.ID.RunID); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		next := cloneWorkflowNode(node)
		next.Generation++
		next.UpdatedAt = request.Binding.BoundAt
		if validationErr := next.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if updateErr := updateWorkflowNodeCAS(ctx, query, next, node.Generation); updateErr != nil {
			return updateErr
		}
		result = workflowruntime.BindPinResult{Outcome: workflowruntime.IdempotencyApplied, Binding: request.Binding, Node: next}
		requestJSON, err := encodeWorkflowJSON(request)
		if err != nil {
			return err
		}
		resultJSON, err := encodeWorkflowJSON(result)
		if err != nil {
			return err
		}
		_, err = query.ExecContext(ctx, `INSERT INTO workflow_pin_bindings(run_id, node_id, iteration, idempotency_key, request_json, result_json) VALUES (?, ?, ?, ?, ?, ?)`, next.ID.RunID, next.ID.NodeID, next.ID.Iteration, request.IdempotencyKey, requestJSON, resultJSON)
		if err != nil {
			return fmt.Errorf("insert workflow pin binding: %w", err)
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.BindPinResult{}, writeErr
	}
	result.Binding = cloneWorkflowPinBinding(result.Binding)
	return result, nil
}

func replayWorkflowPin(requestJSON, resultJSON string, request workflowruntime.BindPinRequest, result *workflowruntime.BindPinResult) error {
	var prior workflowruntime.BindPinRequest
	if err := decodeWorkflowJSON("pin request", requestJSON, &prior); err != nil {
		return err
	}
	if !workflowruntime.SemanticallyEqualBindPinRequest(prior, request) {
		return workflowIdempotencyConflict("pin binding", request.IdempotencyKey)
	}
	if err := decodeWorkflowJSON("pin result", resultJSON, result); err != nil {
		return err
	}
	result.Outcome = workflowruntime.IdempotencyReplayed
	return nil
}

func (s *WorkflowStateStore) LoadPin(ctx context.Context, id workflowruntime.NodeInvocationID) (workflowruntime.PinBinding, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.PinBinding{}, err
	}
	var encoded string
	if err := s.db.QueryRowContext(ctx, `SELECT result_json FROM workflow_pin_bindings WHERE run_id = ? AND node_id = ? AND iteration = ?`, id.RunID, id.NodeID, id.Iteration).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.PinBinding{}, fmt.Errorf("%w: pin binding", workflowruntime.ErrNotFound)
		}
		return workflowruntime.PinBinding{}, fmt.Errorf("load workflow pin: %w", err)
	}
	var result workflowruntime.BindPinResult
	if err := decodeWorkflowJSON("pin result", encoded, &result); err != nil {
		return workflowruntime.PinBinding{}, err
	}
	if err := result.Binding.Validate(); err != nil {
		return workflowruntime.PinBinding{}, workflowInvalid(err)
	}
	return result.Binding, nil
}

func (s *WorkflowStateStore) ReuseNodeOutputs(ctx context.Context, request workflowruntime.ReuseNodeOutputsRequest) (workflowruntime.ReuseNodeOutputsResult, error) {
	request.Policy.Attributes = cloneWorkflowStringMap(request.Policy.Attributes)
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ReuseNodeOutputsResult{}, workflowInvalid(err)
	}
	var result workflowruntime.ReuseNodeOutputsResult
	writeErr := s.write(ctx, "reuse workflow node outputs", func(query workflowSQL) error {
		var priorRequestJSON, priorResultJSON string
		replayErr := query.QueryRowContext(ctx, `SELECT request_json, result_json FROM workflow_reuse_idempotency WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&priorRequestJSON, &priorResultJSON)
		if replayErr == nil {
			var prior workflowruntime.ReuseNodeOutputsRequest
			if decodeErr := decodeWorkflowJSON("reuse request", priorRequestJSON, &prior); decodeErr != nil {
				return decodeErr
			}
			if !workflowruntime.SemanticallyEqualReuseRequest(prior, request) {
				return workflowIdempotencyConflict("reuse outputs", request.IdempotencyKey)
			}
			if decodeErr := decodeWorkflowJSON("reuse result", priorResultJSON, &result); decodeErr != nil {
				return decodeErr
			}
			result.Outcome = workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(replayErr, sql.ErrNoRows) {
			return fmt.Errorf("load workflow reuse replay: %w", replayErr)
		}
		node, err := loadWorkflowNode(ctx, query, request.InvocationID)
		if err != nil {
			return err
		}
		if node.Generation != request.ExpectedGeneration {
			return workflowCAS("node invocation", request.ExpectedGeneration, node.Generation)
		}
		if node.Status != workflowruntime.NodeReady || node.LatestAttempt != 0 {
			return workflowInvalid(errors.New("reuse requires an unattempted ready node"))
		}
		if claimErr := validateWorkflowLifecycleClaim(node, &request.Claim, request.At); claimErr != nil {
			return claimErr
		}
		run, err := loadWorkflowRun(ctx, query, node.ID.RunID)
		if err != nil {
			return err
		}
		if !run.Status.Active() || run.Plan.Digest != request.PlanDigest {
			return workflowInvalid(errors.New("reuse target run or plan changed"))
		}
		if _, loadErr := loadWorkflowValues(ctx, query, request.Outputs); loadErr != nil {
			return loadErr
		}
		source, err := loadWorkflowNode(ctx, query, request.Source)
		if err != nil {
			return err
		}
		if source.Status != workflowruntime.NodeSucceeded || source.Outputs == nil || *source.Outputs != request.Outputs || source.Origin != request.SourceOrigin {
			return workflowInvalid(errors.New("reuse source outcome changed"))
		}
		sourceRun, err := loadWorkflowRun(ctx, query, source.ID.RunID)
		if err != nil {
			return err
		}
		if sourceRun.Plan.Digest != request.PlanDigest {
			return workflowInvalid(errors.New("reuse source plan changed"))
		}
		if request.Origin == workflowruntime.OriginPinned {
			pin, pinErr := loadWorkflowPinBinding(ctx, query, node.ID)
			if pinErr != nil {
				return pinErr
			}
			if pin.PlanDigest != request.PlanDigest || pin.Outputs != request.Outputs || pin.Source != request.Source || pin.SourceOrigin != request.SourceOrigin || !reflect.DeepEqual(pin.Policy, request.Policy) {
				return workflowInvalid(errors.New("pin binding changed"))
			}
		} else {
			var entryJSON string
			entryErr := query.QueryRowContext(ctx, `SELECT entry_json FROM workflow_memo_entries WHERE cache_key = ? AND source_run_id = ? AND source_node_id = ? AND source_iteration = ? AND source_attempt = ?`, request.MemoEntryKey, request.Source.RunID, request.Source.NodeID, request.Source.Iteration, request.SourceAttempt.Number).Scan(&entryJSON)
			if errors.Is(entryErr, sql.ErrNoRows) {
				return workflowInvalid(errors.New("memo publication is absent"))
			}
			if entryErr != nil {
				return fmt.Errorf("load workflow memo publication: %w", entryErr)
			}
			var entry workflowruntime.MemoEntry
			if decodeErr := decodeWorkflowJSON("memo entry", entryJSON, &entry); decodeErr != nil {
				return decodeErr
			}
			if entry.Key != request.MemoEntryKey || entry.Outputs != request.Outputs || entry.Source != request.Source || entry.SourceOrigin != request.SourceOrigin || entry.PlanDigest != request.PlanDigest || entry.NodeID != node.ID.NodeID || node.MemoKeyDigest != entry.MemoKeyDigest {
				return workflowInvalid(errors.New("memo publication changed"))
			}
		}
		next := cloneWorkflowNode(node)
		next.Status = workflowruntime.NodeSucceeded
		next.Outputs = cloneWorkflowValueRef(&request.Outputs)
		next.Origin = request.Origin
		next.Lease = nil
		next.Generation++
		next.UpdatedAt = request.At
		if validationErr := next.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if updateErr := updateWorkflowNodeCAS(ctx, query, next, node.Generation); updateErr != nil {
			return updateErr
		}
		invocation := next.ID
		attributes := map[string]string{"origin": string(request.Origin), "source": workflowNodeIdentity(request.Source), "source_origin": string(request.SourceOrigin), "plan_digest": request.PlanDigest, "policy_code": request.Policy.Code, "policy_reason": request.Policy.Reason, "output_digest": request.Outputs.Digest}
		if request.SourceAttempt != nil {
			attributes["source_attempt"] = fmt.Sprintf("%d", request.SourceAttempt.Number)
		}
		for key, value := range request.Policy.Attributes {
			attributes["policy."+key] = value
		}
		event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: next.ID.RunID, Invocation: &invocation, Type: workflowruntime.EventNodeOutcomeReused, OccurredAt: request.At, Attributes: attributes, Values: &request.Outputs, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if err != nil {
			return err
		}
		result = workflowruntime.ReuseNodeOutputsResult{Outcome: workflowruntime.IdempotencyApplied, Node: next, Event: event}
		requestJSON, err := encodeWorkflowJSON(request)
		if err != nil {
			return err
		}
		resultJSON, err := encodeWorkflowJSON(result)
		if err != nil {
			return err
		}
		_, err = query.ExecContext(ctx, `INSERT INTO workflow_reuse_idempotency(idempotency_key, request_json, result_json) VALUES (?, ?, ?)`, request.IdempotencyKey, requestJSON, resultJSON)
		if err != nil {
			return fmt.Errorf("insert workflow reuse result: %w", err)
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.ReuseNodeOutputsResult{}, writeErr
	}
	return result, nil
}

func loadWorkflowPinBinding(ctx context.Context, query workflowSQL, id workflowruntime.NodeInvocationID) (workflowruntime.PinBinding, error) {
	var encoded string
	if err := query.QueryRowContext(ctx, `SELECT result_json FROM workflow_pin_bindings WHERE run_id = ? AND node_id = ? AND iteration = ?`, id.RunID, id.NodeID, id.Iteration).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.PinBinding{}, fmt.Errorf("%w: pin binding", workflowruntime.ErrNotFound)
		}
		return workflowruntime.PinBinding{}, err
	}
	var result workflowruntime.BindPinResult
	if err := decodeWorkflowJSON("pin result", encoded, &result); err != nil {
		return workflowruntime.PinBinding{}, err
	}
	return result.Binding, nil
}

func memoWorkflowSourceKey(id workflowruntime.AttemptID) string {
	return fmt.Sprintf("%s/%s/%s/%d", id.Invocation.RunID, id.Invocation.NodeID, id.Invocation.Iteration, id.Number)
}

func cloneWorkflowMemoEntry(entry workflowruntime.MemoEntry) workflowruntime.MemoEntry {
	entry.Effects = append(graph.EffectSet(nil), entry.Effects...)
	entry.Policy.Attributes = cloneWorkflowStringMap(entry.Policy.Attributes)
	return entry
}

func cloneWorkflowPinBinding(binding workflowruntime.PinBinding) workflowruntime.PinBinding {
	binding.Authority.Attributes = cloneWorkflowStringMap(binding.Authority.Attributes)
	binding.Policy.Attributes = cloneWorkflowStringMap(binding.Policy.Attributes)
	return binding
}
