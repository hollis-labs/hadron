package runtimetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var (
	_ workflowruntime.MemoStore        = (*Store)(nil)
	_ workflowruntime.PinStore         = (*Store)(nil)
	_ workflowruntime.OutputReuseStore = (*Store)(nil)
	_ workflowruntime.ValueRecordStore = (*Store)(nil)
)

func (s *Store) LoadValueRecord(ctx context.Context, ref values.ValueSetRef) (workflowruntime.ValueRecord, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ValueRecord{}, err
	}
	if err := ref.Validate(); err != nil {
		return workflowruntime.ValueRecord{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.valueSets[ref.ID]
	if !ok {
		return workflowruntime.ValueRecord{}, fmt.Errorf("%w: value set", workflowruntime.ErrNotFound)
	}
	if stored.ref != ref {
		return workflowruntime.ValueRecord{}, casMismatch("value set", 0, 0)
	}
	set, err := cloneValueSet(stored.values)
	if err != nil {
		return workflowruntime.ValueRecord{}, invalid(err)
	}
	record := workflowruntime.ValueRecord{Ref: ref, Owner: cloneValueOwner(stored.owner), Values: set}
	if err := record.Validate(); err != nil {
		return workflowruntime.ValueRecord{}, invalid(err)
	}
	return record, nil
}

func (s *Store) RecordMemoEntry(ctx context.Context, entry workflowruntime.MemoEntry) (workflowruntime.MemoEntry, workflowruntime.IdempotencyOutcome, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.MemoEntry{}, "", err
	}
	entry.CreatedAt, entry.ExpiresAt = entry.CreatedAt.UTC(), entry.ExpiresAt.UTC()
	if err := entry.Validate(); err != nil {
		return workflowruntime.MemoEntry{}, "", invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.memoSources[entry.SourceAttempt]; ok {
		if reflect.DeepEqual(prior, entry) {
			return cloneMemoEntry(prior), workflowruntime.IdempotencyReplayed, nil
		}
		return workflowruntime.MemoEntry{}, "", idempotencyConflict("memo publication", memoSourceKey(entry.SourceAttempt))
	}
	source, ok := s.nodes[entry.Source]
	if !ok || source.Status != workflowruntime.NodeSucceeded || source.Outputs == nil || *source.Outputs != entry.Outputs {
		return workflowruntime.MemoEntry{}, "", invalid(errors.New("memo source is not the durable succeeded output"))
	}
	if source.Origin != entry.SourceOrigin {
		return workflowruntime.MemoEntry{}, "", invalid(errors.New("memo source origin changed"))
	}
	if source.ID.NodeID != entry.NodeID || source.MemoKeyDigest != entry.MemoKeyDigest || source.Inputs == nil || source.Inputs.Digest != entry.InputDigest {
		return workflowruntime.MemoEntry{}, "", invalid(errors.New("memo source binding changed"))
	}
	sourceRun, ok := s.runs[source.ID.RunID]
	if !ok || sourceRun.Plan.Digest != entry.PlanDigest {
		return workflowruntime.MemoEntry{}, "", invalid(errors.New("memo source plan changed"))
	}
	attempt, ok := s.attempts[entry.SourceAttempt]
	if !ok || attempt.Status != workflowruntime.NodeSucceeded || attempt.Outputs == nil || *attempt.Outputs != entry.Outputs || attempt.Executor.Kind != entry.Kind || attempt.Executor.Version != entry.KindVersion {
		return workflowruntime.MemoEntry{}, "", invalid(errors.New("memo source attempt is not the durable succeeded output"))
	}
	stored, ok := s.valueSets[entry.Outputs.ID]
	if !ok || stored.ref != entry.Outputs {
		return workflowruntime.MemoEntry{}, "", fmt.Errorf("%w: memo values", workflowruntime.ErrNotFound)
	}
	if err := workflowruntime.ValidateMemoizableValueSet(stored.values); err != nil {
		return workflowruntime.MemoEntry{}, "", invalid(err)
	}
	entry = cloneMemoEntry(entry)
	s.memoSources[entry.SourceAttempt] = entry
	s.memoEntries[entry.Key] = append(s.memoEntries[entry.Key], entry)
	return cloneMemoEntry(entry), workflowruntime.IdempotencyApplied, nil
}

func (s *Store) LoadMemoEntry(ctx context.Context, key string) (workflowruntime.MemoEntry, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.MemoEntry{}, err
	}
	if err := values.ValidateDigest(key); err != nil {
		return workflowruntime.MemoEntry{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.memoEntries[key]
	if len(entries) == 0 {
		return workflowruntime.MemoEntry{}, fmt.Errorf("%w: memo entry", workflowruntime.ErrNotFound)
	}
	latest := entries[0]
	for _, entry := range entries[1:] {
		// Entries are append-only. SQLite breaks equal creation times with its
		// monotonically increasing sequence, so the later append wins here too.
		if !entry.CreatedAt.Before(latest.CreatedAt) {
			latest = entry
		}
	}
	return cloneMemoEntry(latest), nil
}

func (s *Store) BindPin(ctx context.Context, request workflowruntime.BindPinRequest) (workflowruntime.BindPinResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.BindPinResult{}, err
	}
	request.Binding.BoundAt = request.Binding.BoundAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.BindPinResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if target, ok := s.pinKeys[request.IdempotencyKey]; ok {
		prior := s.pinBindings[target]
		if workflowruntime.SemanticallyEqualBindPinRequest(prior.request, request) {
			result := cloneBindPinResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.BindPinResult{}, idempotencyConflict("pin binding", request.IdempotencyKey)
	}
	if prior, ok := s.pinBindings[request.Binding.Target]; ok {
		if workflowruntime.SemanticallyEqualBindPinRequest(prior.request, request) {
			result := cloneBindPinResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.BindPinResult{}, idempotencyConflict("pin target", request.IdempotencyKey)
	}
	node, ok := s.nodes[request.Binding.Target]
	if !ok {
		return workflowruntime.BindPinResult{}, fmt.Errorf("%w: pin target", workflowruntime.ErrNotFound)
	}
	if node.Generation != request.ExpectedGeneration {
		return workflowruntime.BindPinResult{}, casMismatch("node invocation", request.ExpectedGeneration, node.Generation)
	}
	if node.Status != workflowruntime.NodePending && node.Status != workflowruntime.NodeBlocked {
		return workflowruntime.BindPinResult{}, invalid(errors.New("pin target must be pending or blocked"))
	}
	if node.Lease != nil || node.LatestAttempt != 0 || node.Outputs != nil {
		return workflowruntime.BindPinResult{}, invalid(errors.New("pin target is already claimed, attempted, or complete"))
	}
	if request.Binding.BoundAt.Before(node.UpdatedAt) {
		return workflowruntime.BindPinResult{}, invalid(errors.New("pin binding time must not regress"))
	}
	run, ok := s.runs[node.ID.RunID]
	if !ok || !run.Status.Active() || run.Plan.Digest != request.Binding.PlanDigest {
		return workflowruntime.BindPinResult{}, invalid(errors.New("pin target run or plan changed"))
	}
	source, ok := s.nodes[request.Binding.Source]
	if !ok || source.Status != workflowruntime.NodeSucceeded || source.Outputs == nil || *source.Outputs != request.Binding.Outputs || source.Origin != request.Binding.SourceOrigin {
		return workflowruntime.BindPinResult{}, invalid(errors.New("pin source outcome changed"))
	}
	sourceRun, ok := s.runs[source.ID.RunID]
	if !ok || sourceRun.Plan.Digest != request.Binding.SourcePlanDigest {
		return workflowruntime.BindPinResult{}, invalid(errors.New("pin source plan changed"))
	}
	stored, ok := s.valueSets[request.Binding.Outputs.ID]
	if !ok || stored.ref != request.Binding.Outputs {
		return workflowruntime.BindPinResult{}, fmt.Errorf("%w: pin values", workflowruntime.ErrNotFound)
	}
	if err := workflowruntime.ValidatePinnableValueSet(stored.values, source.ID.RunID == node.ID.RunID); err != nil {
		return workflowruntime.BindPinResult{}, invalid(err)
	}
	next := cloneNode(node)
	next.Generation++
	next.UpdatedAt = request.Binding.BoundAt
	result := workflowruntime.BindPinResult{Outcome: workflowruntime.IdempotencyApplied, Binding: clonePinBinding(request.Binding), Node: cloneNode(next)}
	s.nodes[next.ID] = next
	s.pinBindings[next.ID] = pinBindingRecord{request: cloneBindPinRequest(request), result: result}
	s.pinKeys[request.IdempotencyKey] = next.ID
	return cloneBindPinResult(result), nil
}

func (s *Store) LoadPin(ctx context.Context, id workflowruntime.NodeInvocationID) (workflowruntime.PinBinding, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.PinBinding{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prior, ok := s.pinBindings[id]
	if !ok {
		return workflowruntime.PinBinding{}, fmt.Errorf("%w: pin binding", workflowruntime.ErrNotFound)
	}
	return clonePinBinding(prior.result.Binding), nil
}

func (s *Store) ReuseNodeOutputs(ctx context.Context, request workflowruntime.ReuseNodeOutputsRequest) (workflowruntime.ReuseNodeOutputsResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ReuseNodeOutputsResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ReuseNodeOutputsResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.reuseKeys[request.IdempotencyKey]; ok {
		if workflowruntime.SemanticallyEqualReuseRequest(prior.request, request) {
			result := cloneReuseResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.ReuseNodeOutputsResult{}, idempotencyConflict("reuse outputs", request.IdempotencyKey)
	}
	node, ok := s.nodes[request.InvocationID]
	if !ok {
		return workflowruntime.ReuseNodeOutputsResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if node.Generation != request.ExpectedGeneration {
		return workflowruntime.ReuseNodeOutputsResult{}, casMismatch("node invocation", request.ExpectedGeneration, node.Generation)
	}
	if node.Status != workflowruntime.NodeReady || node.LatestAttempt != 0 {
		return workflowruntime.ReuseNodeOutputsResult{}, invalid(errors.New("reuse requires an unattempted ready node"))
	}
	if err := validateLifecycleClaim(node, &request.Claim, request.At); err != nil {
		return workflowruntime.ReuseNodeOutputsResult{}, err
	}
	if run, runExists := s.runs[node.ID.RunID]; !runExists || !run.Status.Active() || run.Plan.Digest != request.PlanDigest {
		return workflowruntime.ReuseNodeOutputsResult{}, invalid(errors.New("reuse target run or plan changed"))
	}
	stored, ok := s.valueSets[request.Outputs.ID]
	if !ok || stored.ref != request.Outputs {
		return workflowruntime.ReuseNodeOutputsResult{}, fmt.Errorf("%w: reuse output values", workflowruntime.ErrNotFound)
	}
	source, ok := s.nodes[request.Source]
	if !ok || source.Status != workflowruntime.NodeSucceeded || source.Outputs == nil || *source.Outputs != request.Outputs || source.Origin != request.SourceOrigin {
		return workflowruntime.ReuseNodeOutputsResult{}, invalid(errors.New("reuse source outcome changed"))
	}
	sourceRun, ok := s.runs[source.ID.RunID]
	if !ok || sourceRun.Plan.Digest != request.PlanDigest {
		return workflowruntime.ReuseNodeOutputsResult{}, invalid(errors.New("reuse source plan changed"))
	}
	if request.Origin == workflowruntime.OriginPinned {
		pin, ok := s.pinBindings[node.ID]
		if !ok || pin.result.Binding.PlanDigest != request.PlanDigest || pin.result.Binding.Outputs != request.Outputs || pin.result.Binding.Source != request.Source || pin.result.Binding.SourceOrigin != request.SourceOrigin || !reflect.DeepEqual(pin.result.Binding.Policy, request.Policy) {
			return workflowruntime.ReuseNodeOutputsResult{}, invalid(errors.New("pin binding changed"))
		}
	} else {
		entry, ok := s.memoSources[*request.SourceAttempt]
		if !ok || entry.Key != request.MemoEntryKey || entry.Outputs != request.Outputs || entry.Source != request.Source || entry.SourceOrigin != request.SourceOrigin || entry.PlanDigest != request.PlanDigest || entry.NodeID != node.ID.NodeID || node.MemoKeyDigest != entry.MemoKeyDigest {
			return workflowruntime.ReuseNodeOutputsResult{}, invalid(errors.New("memo publication changed"))
		}
	}
	next := cloneNode(node)
	next.Status = workflowruntime.NodeSucceeded
	next.Outputs = cloneValueSetRef(&request.Outputs)
	next.Origin = request.Origin
	next.Lease = nil
	next.Generation++
	next.UpdatedAt = request.At
	if err := next.Validate(); err != nil {
		return workflowruntime.ReuseNodeOutputsResult{}, invalid(err)
	}
	invocation := next.ID
	attributes := map[string]string{"origin": string(request.Origin), "source": nodeIdentity(request.Source), "source_origin": string(request.SourceOrigin), "plan_digest": request.PlanDigest, "policy_code": request.Policy.Code, "policy_reason": request.Policy.Reason, "output_digest": request.Outputs.Digest}
	if request.SourceAttempt != nil {
		attributes["source_attempt"] = fmt.Sprintf("%d", request.SourceAttempt.Number)
	}
	for key, value := range request.Policy.Attributes {
		attributes["policy."+key] = value
	}
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: next.ID.RunID, Invocation: &invocation, Type: workflowruntime.EventNodeOutcomeReused, OccurredAt: request.At, Attributes: attributes, Values: &request.Outputs, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return workflowruntime.ReuseNodeOutputsResult{}, err
	}
	s.releaseSchedulerResourcesLocked(next.ID)
	s.nodes[next.ID] = next
	result := workflowruntime.ReuseNodeOutputsResult{Outcome: workflowruntime.IdempotencyApplied, Node: cloneNode(next), Event: cloneEvent(event)}
	s.reuseKeys[request.IdempotencyKey] = reuseRecord{request: cloneReuseRequest(request), result: result}
	return cloneReuseResult(result), nil
}

func cloneMemoEntry(entry workflowruntime.MemoEntry) workflowruntime.MemoEntry {
	entry.Effects = append(graph.EffectSet(nil), entry.Effects...)
	entry.Policy.Attributes = cloneStringMap(entry.Policy.Attributes)
	return entry
}
func clonePinBinding(binding workflowruntime.PinBinding) workflowruntime.PinBinding {
	binding.Authority.Attributes = cloneStringMap(binding.Authority.Attributes)
	binding.Policy.Attributes = cloneStringMap(binding.Policy.Attributes)
	return binding
}
func cloneBindPinRequest(request workflowruntime.BindPinRequest) workflowruntime.BindPinRequest {
	request.Binding = clonePinBinding(request.Binding)
	return request
}
func cloneBindPinResult(result workflowruntime.BindPinResult) workflowruntime.BindPinResult {
	result.Binding = clonePinBinding(result.Binding)
	result.Node = cloneNode(result.Node)
	return result
}
func cloneReuseRequest(request workflowruntime.ReuseNodeOutputsRequest) workflowruntime.ReuseNodeOutputsRequest {
	request.Policy.Attributes = cloneStringMap(request.Policy.Attributes)
	if request.SourceAttempt != nil {
		attempt := *request.SourceAttempt
		request.SourceAttempt = &attempt
	}
	return request
}
func cloneReuseResult(result workflowruntime.ReuseNodeOutputsResult) workflowruntime.ReuseNodeOutputsResult {
	result.Node = cloneNode(result.Node)
	result.Event = cloneEvent(result.Event)
	return result
}
func memoSourceKey(id workflowruntime.AttemptID) string {
	return fmt.Sprintf("%s/%s/%s/%d", id.Invocation.RunID, id.Invocation.NodeID, id.Invocation.Iteration, id.Number)
}
