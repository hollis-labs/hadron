package runtimetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.RunPolicyStore = (*Store)(nil)

func (s *Store) LoadRunPolicyDecision(ctx context.Context, runID workflowruntime.RunID) (workflowruntime.RunPolicyDecisionSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RunPolicyDecisionSnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	decision, exists := s.runPolicyDecisions[runID]
	if !exists {
		return workflowruntime.RunPolicyDecisionSnapshot{}, fmt.Errorf("%w: run policy decision", workflowruntime.ErrNotFound)
	}
	return decision, nil
}

func (s *Store) ApplyRunFailurePolicy(ctx context.Context, request workflowruntime.ApplyRunFailurePolicyRequest) (workflowruntime.ApplyRunFailurePolicyResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ApplyRunFailurePolicyResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ApplyRunFailurePolicyResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if priorRequest, exists := s.runPolicyRequests[request.IdempotencyKey]; exists {
		if !equalRunFailurePolicyRequest(priorRequest, request) {
			return workflowruntime.ApplyRunFailurePolicyResult{}, idempotencyConflict("run failure policy", request.IdempotencyKey)
		}
		return s.runPolicyResultLocked(request.RunID, workflowruntime.RunFailureFailFast), nil
	}
	if prior, exists := s.runPolicyDecisions[request.RunID]; exists {
		result := s.runPolicyResultLocked(request.RunID, workflowruntime.RunFailureAlreadyDecided)
		result.Decision = prior
		return result, nil
	}
	run, exists := s.runs[request.RunID]
	if !exists {
		return workflowruntime.ApplyRunFailurePolicyResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if run.Generation != request.ExpectedRunGeneration {
		return workflowruntime.ApplyRunFailurePolicyResult{}, casMismatch("run failure policy", request.ExpectedRunGeneration, run.Generation)
	}
	if !run.Status.Active() || request.At.Before(run.UpdatedAt) {
		return workflowruntime.ApplyRunFailurePolicyResult{}, invalid(errors.New("fail-fast requires active run and non-regressing time"))
	}
	source, exists := s.nodes[request.Trigger]
	if !exists {
		return workflowruntime.ApplyRunFailurePolicyResult{}, fmt.Errorf("%w: fail-fast source", workflowruntime.ErrNotFound)
	}
	if source.Generation != request.ExpectedSourceGeneration || !hardFailureStatus(source.Status) || source.ID.Iteration != "" {
		return workflowruntime.ApplyRunFailurePolicyResult{}, invalid(errors.New("fail-fast source generation or status differs"))
	}
	var attempt *workflowruntime.AttemptID
	var expectedFailure *workflowruntime.Failure
	if source.LatestAttempt > 0 {
		id := workflowruntime.AttemptID{Invocation: source.ID, Number: source.LatestAttempt}
		persisted, exists := s.attempts[id]
		if !exists || persisted.Status != source.Status || persisted.Failure == nil {
			return workflowruntime.ApplyRunFailurePolicyResult{}, invalid(errors.New("fail-fast source attempt has no durable failure"))
		}
		attempt, expectedFailure = &id, persisted.Failure
	}
	if err := workflowruntime.ValidateNodeControlErrorValues(request.ErrorValues, source.ID, attempt, source.Status, expectedFailure); err != nil {
		return workflowruntime.ApplyRunFailurePolicyResult{}, invalid(err)
	}
	backup := s.backupCancellationStateLocked()
	begin, err := s.beginTerminalIntentLocked(workflowruntime.BeginTerminalIntentRequest{RunID: request.RunID, ExpectedRunGeneration: request.ExpectedRunGeneration, IntendedStatus: request.IntendedStatus, Reason: &request.Reason, ErrorValues: request.ErrorValues, IdempotencyKey: request.IdempotencyKey, Finalizers: request.Finalizers, At: request.At})
	if err != nil {
		return workflowruntime.ApplyRunFailurePolicyResult{}, err
	}
	excluded := map[workflowruntime.NodeInvocationID]struct{}{request.Trigger: struct{}{}}
	for _, finalizer := range request.Finalizers {
		excluded[finalizer.Invocation] = struct{}{}
	}
	collector := cancellationCollector{}
	cancelReason := workflowruntime.Failure{Code: "run_fail_fast", Message: "run fail-fast policy stopped remaining ordinary work", Details: map[string]string{"trigger_node": request.Trigger.NodeID}}
	if cancelErr := s.cancelRunLockedWithOptions(request.RunID, request.At, cancelReason, request.IdempotencyKey, make(map[workflowruntime.RunID]struct{}), &collector, excluded, false, false); cancelErr != nil {
		s.restoreCancellationStateLocked(backup)
		return workflowruntime.ApplyRunFailurePolicyResult{}, cancelErr
	}
	decision := workflowruntime.RunPolicyDecisionSnapshot{RunID: request.RunID, Mode: graph.CompletionFailFast, Trigger: request.Trigger, SourceGeneration: request.ExpectedSourceGeneration, IntendedStatus: request.IntendedStatus, IdempotencyKey: request.IdempotencyKey, Generation: 1, CreatedAt: request.At}
	if decisionErr := decision.Validate(); decisionErr != nil {
		s.restoreCancellationStateLocked(backup)
		return workflowruntime.ApplyRunFailurePolicyResult{}, invalid(decisionErr)
	}
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Invocation: &request.Trigger, Type: workflowruntime.EventRunFailFastTriggered, OccurredAt: request.At, Attributes: map[string]string{"trigger_node": request.Trigger.NodeID, "intended_status": string(request.IntendedStatus), "reason_code": request.Reason.Code}, Values: begin.Intent.Error, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		s.restoreCancellationStateLocked(backup)
		return workflowruntime.ApplyRunFailurePolicyResult{}, err
	}
	s.runPolicyDecisions[request.RunID] = decision
	s.runPolicyRequests[request.IdempotencyKey] = cloneRunFailurePolicyRequest(request)
	events := make([]workflowruntime.Event, 0, len(collector.events)+2)
	if begin.Event != nil {
		events = append(events, cloneEvent(*begin.Event))
	}
	events = append(events, cloneEvents(collector.events)...)
	events = append(events, cloneEvent(event))
	return workflowruntime.ApplyRunFailurePolicyResult{Disposition: workflowruntime.RunFailureFailFast, Decision: decision, Run: cloneRun(begin.Run), Intent: cloneTerminalIntent(begin.Intent), Nodes: cloneCancellationNodes(collector.nodes), Intents: cloneCancellationIntents(collector.intents), Events: events}, nil
}

func (s *Store) runPolicyResultLocked(runID workflowruntime.RunID, disposition workflowruntime.RunFailureDisposition) workflowruntime.ApplyRunFailurePolicyResult {
	result := workflowruntime.ApplyRunFailurePolicyResult{Disposition: disposition, Decision: s.runPolicyDecisions[runID], Run: cloneRun(s.runs[runID]), Intent: cloneTerminalIntent(s.terminalIntents[runID])}
	for _, intent := range s.cancellationIntents {
		if intent.RunID == runID {
			result.Intents = append(result.Intents, cloneCancellationIntent(intent))
		}
	}
	sort.Slice(result.Intents, func(i, j int) bool { return result.Intents[i].ID < result.Intents[j].ID })
	return result
}

func cloneRunFailurePolicyRequest(request workflowruntime.ApplyRunFailurePolicyRequest) workflowruntime.ApplyRunFailurePolicyRequest {
	request.Reason.Details = cloneStringMap(request.Reason.Details)
	request.Finalizers = cloneFinalizerScopes(request.Finalizers)
	request.ErrorValues, _ = cloneValueSet(request.ErrorValues)
	return request
}

func equalRunFailurePolicyRequest(left, right workflowruntime.ApplyRunFailurePolicyRequest) bool {
	left.ExpectedRunGeneration, right.ExpectedRunGeneration = 0, 0
	left.ExpectedSourceGeneration, right.ExpectedSourceGeneration = 0, 0
	left.At, right.At = left.At.UTC(), right.At.UTC()
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}
