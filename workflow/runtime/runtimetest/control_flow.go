package runtimetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.ControlFlowStore = (*Store)(nil)

func (s *Store) LoadControlDecision(ctx context.Context, id workflowruntime.ControlDecisionID) (workflowruntime.ControlDecisionSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, err
	}
	if err := id.Validate(); err != nil {
		return workflowruntime.ControlDecisionSnapshot{}, invalid(err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	decision, ok := s.controlDecisions[id]
	if !ok {
		return workflowruntime.ControlDecisionSnapshot{}, fmt.Errorf("%w: control decision", workflowruntime.ErrNotFound)
	}
	return cloneControlDecision(decision), nil
}

func cancellationTreePlans(request workflowruntime.RequestRunCancellationWithFinalizersRequest) []workflowruntime.CancellationDescendantPlan {
	plans := make([]workflowruntime.CancellationDescendantPlan, 0, len(request.Descendants)+1)
	plans = append(plans, workflowruntime.CancellationDescendantPlan{
		RunID: request.Cancellation.RunID, ExpectedRunGeneration: request.Cancellation.ExpectedGeneration,
		IdempotencyKey: request.Cancellation.IdempotencyKey, Finalizers: request.Finalizers, ErrorValues: request.ErrorValues,
	})
	plans = append(plans, request.Descendants...)
	return plans
}

func (s *Store) directCancelDescendantsLocked(root workflowruntime.RunID) ([]workflowruntime.RunID, error) {
	seen := map[workflowruntime.RunID]bool{root: true}
	result := make([]workflowruntime.RunID, 0)
	var visit func(workflowruntime.RunID) error
	visit = func(parent workflowruntime.RunID) error {
		for _, link := range s.childRuns[parent] {
			if link.Policy != graph.ParentCloseCancel {
				continue
			}
			if seen[link.ChildRunID] {
				return invalid(errors.New("direct-cancel child graph contains a cycle or duplicate descendant"))
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

func cloneControlCancellationTree(request workflowruntime.RequestRunCancellationWithFinalizersRequest) (workflowruntime.RequestRunCancellationWithFinalizersRequest, error) {
	result := workflowruntime.RequestRunCancellationWithFinalizersRequest{
		Cancellation: cloneCancellationRequest(request.Cancellation), Finalizers: cloneFinalizerScopes(request.Finalizers),
		Descendants: make([]workflowruntime.CancellationDescendantPlan, len(request.Descendants)),
	}
	var err error
	if result.ErrorValues, err = cloneValueSet(request.ErrorValues); err != nil {
		return workflowruntime.RequestRunCancellationWithFinalizersRequest{}, err
	}
	for index, descendant := range request.Descendants {
		result.Descendants[index] = workflowruntime.CancellationDescendantPlan{
			RunID: descendant.RunID, ExpectedRunGeneration: descendant.ExpectedRunGeneration,
			IdempotencyKey: descendant.IdempotencyKey, Finalizers: cloneFinalizerScopes(descendant.Finalizers),
		}
		if result.Descendants[index].ErrorValues, err = cloneValueSet(descendant.ErrorValues); err != nil {
			return workflowruntime.RequestRunCancellationWithFinalizersRequest{}, err
		}
	}
	return result, nil
}

func equalControlCancellationTree(left, right workflowruntime.RequestRunCancellationWithFinalizersRequest) bool {
	left.Cancellation.ExpectedGeneration, right.Cancellation.ExpectedGeneration = 0, 0
	for index := range left.Descendants {
		left.Descendants[index].ExpectedRunGeneration = 0
	}
	for index := range right.Descendants {
		right.Descendants[index].ExpectedRunGeneration = 0
	}
	return reflect.DeepEqual(left, right)
}

func (s *Store) cancellationTreeIntentsLocked(request workflowruntime.RequestRunCancellationWithFinalizersRequest) ([]workflowruntime.TerminalIntentSnapshot, workflowruntime.TerminalIntentSnapshot) {
	intents := make([]workflowruntime.TerminalIntentSnapshot, 0)
	var root workflowruntime.TerminalIntentSnapshot
	for index, plan := range cancellationTreePlans(request) {
		if len(plan.Finalizers) == 0 {
			continue
		}
		intent, exists := s.terminalIntents[plan.RunID]
		if !exists {
			continue
		}
		cloned := cloneTerminalIntent(intent)
		intents = append(intents, cloned)
		if index == 0 {
			root = cloned
		}
	}
	return intents, root
}

func (s *Store) RecordControlDecision(ctx context.Context, request workflowruntime.RecordControlDecisionRequest) (workflowruntime.RecordControlDecisionResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RecordControlDecisionResult{}, err
	}
	request.At = request.At.UTC()
	if request.ExpectedSourceGeneration == 0 || request.At.IsZero() {
		return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("decision requires source generation and timestamp"))
	}
	if request.Decision.Error != nil {
		return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("decision error reference is store-managed"))
	}
	candidate := cloneControlDecision(request.Decision)
	candidate.SourceGeneration, candidate.Generation, candidate.CreatedAt = request.ExpectedSourceGeneration, 1, request.At
	if err := candidate.ID.Validate(); err != nil || !candidate.Outcome.Valid() {
		return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("decision identity and outcome are required"))
	}
	if candidate.ID.Kind == workflowruntime.ControlSwitch {
		if len(request.ErrorValues) != 0 {
			return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("switch decision cannot persist error values"))
		}
		if err := candidate.Validate(); err != nil {
			return workflowruntime.RecordControlDecisionResult{}, invalid(err)
		}
	} else if err := validateControlErrorValues(request.ErrorValues); err != nil {
		return workflowruntime.RecordControlDecisionResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.controlDecisions[candidate.ID]; ok {
		// The source/kind identity owns the immutable occurrence timestamp.
		// Recovery may observe the same semantic decision later without turning
		// that harmless wall-clock difference into a conflict.
		candidate.CreatedAt = prior.CreatedAt
		candidate.Error = cloneValueSetRef(prior.Error)
		if err := candidate.Validate(); err != nil {
			return workflowruntime.RecordControlDecisionResult{}, invalid(err)
		}
		if equalControlDecision(prior, candidate) && s.equalControlValuesLocked(prior.Error, request.ErrorValues) {
			return workflowruntime.RecordControlDecisionResult{Outcome: workflowruntime.IdempotencyReplayed, Decision: cloneControlDecision(prior)}, nil
		}
		return workflowruntime.RecordControlDecisionResult{}, fmt.Errorf("%w: decision already differs", workflowruntime.ErrControlFlowConflict)
	}
	source, ok := s.nodes[candidate.ID.Source]
	if !ok {
		return workflowruntime.RecordControlDecisionResult{}, fmt.Errorf("%w: decision source", workflowruntime.ErrNotFound)
	}
	if !s.controlAdmissionAllowedLocked(source.ID) {
		return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("pending terminal intent fences control decision"))
	}
	if source.Generation != request.ExpectedSourceGeneration {
		return workflowruntime.RecordControlDecisionResult{}, casMismatch("control decision source", request.ExpectedSourceGeneration, source.Generation)
	}
	if request.At.Before(source.UpdatedAt) {
		return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("decision time must not precede source update"))
	}
	if candidate.ID.Kind == workflowruntime.ControlSwitch && source.Status != workflowruntime.NodeSucceeded {
		return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("switch decision requires succeeded source"))
	}
	if candidate.ID.Kind == workflowruntime.ControlCatch && !hardFailureStatus(source.Status) {
		return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("catch decision requires hard-failed source"))
	}
	if candidate.ID.Kind == workflowruntime.ControlCatch {
		var attempt *workflowruntime.AttemptID
		var expectedFailure *workflowruntime.Failure
		if source.LatestAttempt > 0 {
			id := workflowruntime.AttemptID{Invocation: source.ID, Number: source.LatestAttempt}
			attempt = &id
			persisted, exists := s.attempts[id]
			if !exists || persisted.Failure == nil || persisted.Status != source.Status {
				return workflowruntime.RecordControlDecisionResult{}, invalid(errors.New("catch source attempt has no durable failure"))
			}
			expectedFailure = persisted.Failure
		}
		if err := workflowruntime.ValidateNodeControlErrorValues(request.ErrorValues, source.ID, attempt, source.Status, expectedFailure); err != nil {
			return workflowruntime.RecordControlDecisionResult{}, invalid(err)
		}
	}
	var pending *storedValues
	if candidate.ID.Kind == workflowruntime.ControlCatch {
		attempt := (*workflowruntime.AttemptID)(nil)
		if source.LatestAttempt > 0 {
			id := workflowruntime.AttemptID{Invocation: source.ID, Number: source.LatestAttempt}
			attempt = &id
		}
		ref, stored, err := s.prepareControlValuesLocked(workflowruntime.ValueOwner{Kind: "control-error", RunID: source.ID.RunID, Invocation: &source.ID, Attempt: attempt}, request.ErrorValues)
		if err != nil {
			return workflowruntime.RecordControlDecisionResult{}, invalid(err)
		}
		candidate.Error, pending = ref, stored
	}
	if err := candidate.Validate(); err != nil {
		return workflowruntime.RecordControlDecisionResult{}, invalid(err)
	}
	for _, target := range candidate.Targets {
		if _, exists := s.nodes[target]; !exists {
			return workflowruntime.RecordControlDecisionResult{}, invalid(fmt.Errorf("decision target %q does not exist", target.NodeID))
		}
	}
	invocation := source.ID
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
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: invocation.RunID, Invocation: &invocation, Type: eventType, OccurredAt: request.At, Attributes: attributes, Values: cloneValueSetRef(candidate.Error), Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return workflowruntime.RecordControlDecisionResult{}, err
	}
	s.commitControlValuesLocked(pending)
	s.controlDecisions[candidate.ID] = candidate
	eventCopy := cloneEvent(event)
	return workflowruntime.RecordControlDecisionResult{Outcome: workflowruntime.IdempotencyApplied, Decision: cloneControlDecision(candidate), Event: &eventCopy}, nil
}

func (s *Store) LoadTerminalIntent(ctx context.Context, runID workflowruntime.RunID) (workflowruntime.TerminalIntentSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.TerminalIntentSnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	intent, ok := s.terminalIntents[runID]
	if !ok {
		return workflowruntime.TerminalIntentSnapshot{}, fmt.Errorf("%w: terminal intent", workflowruntime.ErrNotFound)
	}
	return cloneTerminalIntent(intent), nil
}

func (s *Store) BeginTerminalIntent(ctx context.Context, request workflowruntime.BeginTerminalIntentRequest) (workflowruntime.BeginTerminalIntentResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, err
	}
	request.At = request.At.UTC()
	if len(request.Finalizers) == 0 {
		return workflowruntime.BeginTerminalIntentResult{}, invalid(errors.New("public terminal intent requires at least one finalizer"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.beginTerminalIntentLocked(request)
}

func (s *Store) beginTerminalIntentLocked(request workflowruntime.BeginTerminalIntentRequest) (workflowruntime.BeginTerminalIntentResult, error) {
	candidate := workflowruntime.TerminalIntentSnapshot{RunID: request.RunID, IntendedStatus: request.IntendedStatus, Reason: cloneFailure(request.Reason), IdempotencyKey: request.IdempotencyKey, Finalizers: cloneFinalizerScopes(request.Finalizers), Status: workflowruntime.TerminalIntentPending, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
	if request.ExpectedRunGeneration == 0 || request.At.IsZero() {
		return workflowruntime.BeginTerminalIntentResult{}, invalid(errors.New("terminal intent requires run generation and timestamp"))
	}
	if request.IntendedStatus == workflowruntime.RunSucceeded {
		if len(request.ErrorValues) != 0 {
			return workflowruntime.BeginTerminalIntentResult{}, invalid(errors.New("successful terminal intent cannot persist error values"))
		}
	} else if err := validateControlErrorValues(request.ErrorValues); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, invalid(err)
	}
	if owner, used := s.terminalKeys[request.IdempotencyKey]; used && owner != request.RunID {
		return workflowruntime.BeginTerminalIntentResult{}, idempotencyConflict("terminal intent", request.IdempotencyKey)
	}
	if prior, exists := s.terminalIntents[request.RunID]; exists {
		candidate.Error = cloneValueSetRef(prior.Error)
		if err := candidate.Validate(); err != nil {
			return workflowruntime.BeginTerminalIntentResult{}, invalid(err)
		}
		if equalTerminalIntentImmutable(prior, candidate) && s.equalControlValuesLocked(prior.Error, request.ErrorValues) {
			return workflowruntime.BeginTerminalIntentResult{Outcome: workflowruntime.IdempotencyReplayed, Run: cloneRun(s.runs[request.RunID]), Intent: cloneTerminalIntent(prior)}, nil
		}
		return workflowruntime.BeginTerminalIntentResult{}, idempotencyConflict("terminal intent", request.IdempotencyKey)
	}
	run, ok := s.runs[request.RunID]
	if !ok {
		return workflowruntime.BeginTerminalIntentResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if run.Generation != request.ExpectedRunGeneration {
		return workflowruntime.BeginTerminalIntentResult{}, casMismatch("terminal intent run", request.ExpectedRunGeneration, run.Generation)
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
			return workflowruntime.BeginTerminalIntentResult{}, invalid(err)
		}
		if origin.Invocation != nil {
			node, exists := s.nodes[*origin.Invocation]
			if !exists || !hardFailureStatus(node.Status) || string(node.Status) != string(request.IntendedStatus) {
				return workflowruntime.BeginTerminalIntentResult{}, invalid(errors.New("terminal error node status differs from intended status"))
			}
			var expectedFailure *workflowruntime.Failure
			if origin.Attempt != nil {
				attempt, exists := s.attempts[*origin.Attempt]
				if !exists || node.LatestAttempt != origin.Attempt.Number || attempt.Failure == nil || attempt.Status != node.Status {
					return workflowruntime.BeginTerminalIntentResult{}, invalid(errors.New("terminal error attempt does not contain the durable node failure"))
				}
				expectedFailure = attempt.Failure
			} else if node.LatestAttempt != 0 {
				return workflowruntime.BeginTerminalIntentResult{}, invalid(errors.New("terminal error omits the durable latest attempt"))
			}
			if err := workflowruntime.ValidateNodeControlErrorValues(request.ErrorValues, node.ID, origin.Attempt, node.Status, expectedFailure); err != nil {
				return workflowruntime.BeginTerminalIntentResult{}, invalid(err)
			}
		}
	}
	if !run.Status.Active() || request.At.Before(run.UpdatedAt) {
		return workflowruntime.BeginTerminalIntentResult{}, invalid(errors.New("terminal intent requires active run and non-regressing time"))
	}
	for _, finalizer := range candidate.Finalizers {
		if _, exists := s.nodes[finalizer.Invocation]; !exists {
			return workflowruntime.BeginTerminalIntentResult{}, invalid(fmt.Errorf("finalizer %q does not exist", finalizer.Invocation.NodeID))
		}
		for _, member := range finalizer.Scope {
			if _, exists := s.nodes[member]; !exists {
				return workflowruntime.BeginTerminalIntentResult{}, invalid(fmt.Errorf("finalizer scope member %q does not exist", member.NodeID))
			}
		}
	}
	var pending *storedValues
	if request.IntendedStatus != workflowruntime.RunSucceeded {
		ref, stored, valueErr := s.prepareControlValuesLocked(workflowruntime.ValueOwner{Kind: "control-run-error", RunID: request.RunID}, request.ErrorValues)
		if valueErr != nil {
			return workflowruntime.BeginTerminalIntentResult{}, invalid(valueErr)
		}
		candidate.Error, pending = ref, stored
	} else if len(request.ErrorValues) != 0 {
		return workflowruntime.BeginTerminalIntentResult{}, invalid(errors.New("successful terminal intent cannot persist error values"))
	}
	if err := candidate.Validate(); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, invalid(err)
	}
	nextRun := cloneRun(run)
	nextRun.Generation++
	nextRun.UpdatedAt = request.At
	if err := nextRun.Validate(); err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, invalid(err)
	}
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: run.ID, Type: workflowruntime.EventTerminalIntent, OccurredAt: request.At, Attributes: map[string]string{"intended_status": string(candidate.IntendedStatus), "finalizers": fmt.Sprint(len(candidate.Finalizers)), "reason_code": failureCode(candidate.Reason)}, Values: cloneValueSetRef(candidate.Error), Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return workflowruntime.BeginTerminalIntentResult{}, err
	}
	s.commitControlValuesLocked(pending)
	s.runs[run.ID], s.terminalIntents[run.ID], s.terminalKeys[request.IdempotencyKey] = nextRun, candidate, run.ID
	eventCopy := cloneEvent(event)
	return workflowruntime.BeginTerminalIntentResult{Outcome: workflowruntime.IdempotencyApplied, Run: cloneRun(nextRun), Intent: cloneTerminalIntent(candidate), Event: &eventCopy}, nil
}

func (s *Store) RequestRunCancellationWithFinalizers(ctx context.Context, request workflowruntime.RequestRunCancellationWithFinalizersRequest) (workflowruntime.RequestRunCancellationWithFinalizersResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, err
	}
	request.Cancellation.At = request.Cancellation.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, exists := s.controlCancelTrees[request.Cancellation.IdempotencyKey]; exists {
		if !equalControlCancellationTree(prior, request) {
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, idempotencyConflict("cancel run tree", request.Cancellation.IdempotencyKey)
		}
		record, recorded := s.cancellationKeys[request.Cancellation.IdempotencyKey]
		if !recorded || !equalControlCancellationRoot(record.request, request.Cancellation) {
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, invalid(errors.New("cancellation tree is missing its cancellation replay record"))
		}
		result := cloneCancellationResult(record.result)
		result.Outcome = workflowruntime.IdempotencyReplayed
		result.Run = cloneRun(s.runs[request.Cancellation.RunID])
		intents, rootIntent := s.cancellationTreeIntentsLocked(request)
		return workflowruntime.RequestRunCancellationWithFinalizersResult{Cancellation: result, Intent: rootIntent, TerminalIntents: intents}, nil
	}
	if _, exists := s.cancellationKeys[request.Cancellation.IdempotencyKey]; exists {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, idempotencyConflict("cancel run with finalizers", request.Cancellation.IdempotencyKey)
	}
	reachable, err := s.directCancelDescendantsLocked(request.Cancellation.RunID)
	if err != nil {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, err
	}
	if len(reachable) != len(request.Descendants) {
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, invalid(errors.New("cancellation tree does not exactly cover direct-cancel descendants"))
	}
	for index, runID := range reachable {
		if request.Descendants[index].RunID != runID {
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, invalid(errors.New("cancellation tree does not exactly cover direct-cancel descendants"))
		}
	}
	plans := cancellationTreePlans(request)
	for index, plan := range plans {
		run, exists := s.runs[plan.RunID]
		if !exists {
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, fmt.Errorf("%w: cancellation tree run", workflowruntime.ErrNotFound)
		}
		if run.Generation != plan.ExpectedRunGeneration {
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, casMismatch("cancellation tree run", plan.ExpectedRunGeneration, run.Generation)
		}
		if index == 0 && !run.Status.Active() {
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, invalid(errors.New("cancellation tree root must be active"))
		}
		if run.Status.Terminal() {
			continue
		}
		if request.Cancellation.At.Before(run.UpdatedAt) {
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, invalid(errors.New("cancellation tree time must not regress"))
		}
		if err := workflowruntime.ValidateRunStatusTransition(run.Status, workflowruntime.RunCanceled); err != nil {
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, err
		}
		if len(plan.Finalizers) != 0 {
			if err := workflowruntime.ValidateRunStatusTransition(run.Status, workflowruntime.RunFailed); err != nil {
				return workflowruntime.RequestRunCancellationWithFinalizersResult{}, err
			}
		}
	}
	backup := s.backupCancellationStateLocked()
	collector := cancellationCollector{}
	intents := make([]workflowruntime.TerminalIntentSnapshot, 0)
	var rootIntent workflowruntime.TerminalIntentSnapshot
	for index, plan := range plans {
		run := s.runs[plan.RunID]
		if run.Status.Terminal() {
			continue
		}
		excluded := make(map[workflowruntime.NodeInvocationID]struct{}, len(plan.Finalizers))
		for _, finalizer := range plan.Finalizers {
			excluded[finalizer.Invocation] = struct{}{}
		}
		terminalize := len(plan.Finalizers) == 0
		if !terminalize {
			begin, beginErr := s.beginTerminalIntentLocked(workflowruntime.BeginTerminalIntentRequest{RunID: plan.RunID, ExpectedRunGeneration: plan.ExpectedRunGeneration, IntendedStatus: workflowruntime.RunCanceled, Reason: &request.Cancellation.Reason, ErrorValues: plan.ErrorValues, IdempotencyKey: plan.IdempotencyKey, Finalizers: plan.Finalizers, At: request.Cancellation.At})
			if beginErr != nil {
				s.restoreCancellationStateLocked(backup)
				return workflowruntime.RequestRunCancellationWithFinalizersResult{}, beginErr
			}
			intents = append(intents, cloneTerminalIntent(begin.Intent))
			if index == 0 {
				rootIntent = cloneTerminalIntent(begin.Intent)
			}
			if begin.Event != nil {
				collector.events = append(collector.events, cloneEvent(*begin.Event))
			}
		}
		if err := s.cancelRunLockedWithOptions(plan.RunID, request.Cancellation.At, request.Cancellation.Reason, plan.IdempotencyKey, make(map[workflowruntime.RunID]struct{}), &collector, excluded, terminalize, false); err != nil {
			s.restoreCancellationStateLocked(backup)
			return workflowruntime.RequestRunCancellationWithFinalizersResult{}, err
		}
	}
	result := workflowruntime.RequestRunCancellationResult{Outcome: workflowruntime.IdempotencyApplied, Run: cloneRun(s.runs[request.Cancellation.RunID]), Nodes: cloneCancellationNodes(collector.nodes), Intents: cloneCancellationIntents(collector.intents), Events: cloneEvents(collector.events)}
	s.cancellationKeys[request.Cancellation.IdempotencyKey] = cancellationRecord{request: cloneCancellationRequest(request.Cancellation), result: cloneCancellationResult(result)}
	clonedRequest, cloneErr := cloneControlCancellationTree(request)
	if cloneErr != nil {
		s.restoreCancellationStateLocked(backup)
		delete(s.cancellationKeys, request.Cancellation.IdempotencyKey)
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, invalid(cloneErr)
	}
	s.controlCancelTrees[request.Cancellation.IdempotencyKey] = clonedRequest
	return workflowruntime.RequestRunCancellationWithFinalizersResult{Cancellation: result, Intent: rootIntent, TerminalIntents: intents}, nil
}

func equalControlCancellationRoot(left, right workflowruntime.RequestRunCancellationRequest) bool {
	left.ExpectedGeneration, right.ExpectedGeneration = 0, 0
	return equalCancellationRequest(left, right)
}

func (s *Store) CompleteTerminalIntent(ctx context.Context, request workflowruntime.CompleteTerminalIntentRequest) (workflowruntime.CompleteTerminalIntentResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.CompleteTerminalIntentResult{}, err
	}
	request.At = request.At.UTC()
	if request.ExpectedRunGeneration == 0 || request.ExpectedIntentGeneration == 0 || request.At.IsZero() {
		return workflowruntime.CompleteTerminalIntentResult{}, invalid(errors.New("terminal completion requires generations and timestamp"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	intent, ok := s.terminalIntents[request.RunID]
	if !ok {
		return workflowruntime.CompleteTerminalIntentResult{}, fmt.Errorf("%w: terminal intent", workflowruntime.ErrNotFound)
	}
	run, ok := s.runs[request.RunID]
	if !ok {
		return workflowruntime.CompleteTerminalIntentResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if run.Generation != request.ExpectedRunGeneration {
		return workflowruntime.CompleteTerminalIntentResult{}, casMismatch("terminal completion run", request.ExpectedRunGeneration, run.Generation)
	}
	if intent.Generation != request.ExpectedIntentGeneration {
		return workflowruntime.CompleteTerminalIntentResult{}, casMismatch("terminal intent", request.ExpectedIntentGeneration, intent.Generation)
	}
	if intent.Status != workflowruntime.TerminalIntentPending || !run.Status.Active() || request.At.Before(run.UpdatedAt) || request.At.Before(intent.UpdatedAt) {
		return workflowruntime.CompleteTerminalIntentResult{}, invalid(errors.New("terminal intent is not pending or completion time regresses"))
	}
	to := intent.IntendedStatus
	cleanupFailure := ""
	for _, finalizer := range intent.Finalizers {
		node := s.nodes[finalizer.Invocation]
		if !node.Status.Terminal() {
			return workflowruntime.CompleteTerminalIntentResult{}, workflowruntime.ErrControlFlowPending
		}
		if request.At.Before(node.UpdatedAt) {
			return workflowruntime.CompleteTerminalIntentResult{}, invalid(errors.New("terminal completion time must not precede finalizer completion"))
		}
		if hardFailureStatus(node.Status) {
			to, cleanupFailure = workflowruntime.RunFailed, node.ID.NodeID
		}
	}
	for _, cancellation := range s.cancellationIntents {
		if cancellation.RunID == run.ID && cancellation.Status == workflowruntime.CancellationPending {
			return workflowruntime.CompleteTerminalIntentResult{}, workflowruntime.ErrControlFlowPending
		}
	}
	if err := workflowruntime.ValidateRunStatusTransition(run.Status, to); err != nil {
		return workflowruntime.CompleteTerminalIntentResult{}, err
	}
	nextRun := cloneRun(run)
	nextRun.Status = to
	nextRun.Generation++
	nextRun.UpdatedAt = request.At
	nextIntent := cloneTerminalIntent(intent)
	nextIntent.Status = workflowruntime.TerminalIntentCompleted
	nextIntent.Generation++
	nextIntent.UpdatedAt, nextIntent.CompletedAt = request.At, request.At
	if err := nextRun.Validate(); err != nil {
		return workflowruntime.CompleteTerminalIntentResult{}, invalid(err)
	}
	if err := nextIntent.Validate(); err != nil {
		return workflowruntime.CompleteTerminalIntentResult{}, invalid(err)
	}
	attributes := map[string]string{"from_status": string(run.Status), "to_status": string(to), "intended_status": string(intent.IntendedStatus)}
	if cleanupFailure != "" {
		attributes["cleanup_failure"] = cleanupFailure
	}
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: run.ID, Type: workflowruntime.EventRunStatusChanged, OccurredAt: request.At, Attributes: attributes, Values: cloneValueSetRef(intent.Error), Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return workflowruntime.CompleteTerminalIntentResult{}, err
	}
	s.runs[run.ID], s.terminalIntents[run.ID] = nextRun, nextIntent
	return workflowruntime.CompleteTerminalIntentResult{Run: cloneRun(nextRun), Intent: cloneTerminalIntent(nextIntent), Event: cloneEvent(event)}, nil
}

func (s *Store) controlAdmissionAllowedLocked(id workflowruntime.NodeInvocationID) bool {
	intent, exists := s.terminalIntents[id.RunID]
	if !exists || intent.Status != workflowruntime.TerminalIntentPending {
		return true
	}
	for _, finalizer := range intent.Finalizers {
		if finalizer.Invocation == id {
			return true
		}
	}
	return false
}

func (s *Store) prepareControlValuesLocked(owner workflowruntime.ValueOwner, input values.ValueSet) (*values.ValueSetRef, *storedValues, error) {
	if err := owner.Validate(); err != nil {
		return nil, nil, err
	}
	if err := validateControlErrorValues(input); err != nil {
		return nil, nil, err
	}
	cloned, err := cloneValueSet(input)
	if err != nil {
		return nil, nil, err
	}
	id := fmt.Sprintf("values-%012d", s.nextValueSet+1)
	ref, err := values.NewValueSetRef(id, cloned)
	if err != nil {
		return nil, nil, err
	}
	return &ref, &storedValues{ref: ref, owner: cloneValueOwner(owner), values: cloned}, nil
}

func validateControlErrorValues(input values.ValueSet) error {
	return workflowruntime.ValidateControlErrorValues(input)
}

func (s *Store) commitControlValuesLocked(pending *storedValues) {
	if pending == nil {
		return
	}
	s.nextValueSet++
	s.valueSets[pending.ref.ID] = *pending
}

func (s *Store) equalControlValuesLocked(ref *values.ValueSetRef, input values.ValueSet) bool {
	if ref == nil {
		return len(input) == 0
	}
	if len(input) != 1 {
		return false
	}
	stored, exists := s.valueSets[ref.ID]
	if !exists || stored.ref != *ref {
		return false
	}
	digest, err := values.DigestValueSet(input)
	return err == nil && digest == ref.Digest
}

func cloneControlDecision(input workflowruntime.ControlDecisionSnapshot) workflowruntime.ControlDecisionSnapshot {
	if input.RuleIndex != nil {
		value := *input.RuleIndex
		input.RuleIndex = &value
	}
	input.Targets = append([]workflowruntime.NodeInvocationID(nil), input.Targets...)
	input.Error = cloneValueSetRef(input.Error)
	return input
}
func equalControlDecision(left, right workflowruntime.ControlDecisionSnapshot) bool {
	if left.ID != right.ID || left.Outcome != right.Outcome || left.BindAs != right.BindAs || left.SourceGeneration != right.SourceGeneration || left.Generation != right.Generation || !left.CreatedAt.Equal(right.CreatedAt) || !equalValueSetRef(left.Error, right.Error) || len(left.Targets) != len(right.Targets) {
		return false
	}
	if left.RuleIndex == nil != (right.RuleIndex == nil) || left.RuleIndex != nil && *left.RuleIndex != *right.RuleIndex {
		return false
	}
	for index := range left.Targets {
		if left.Targets[index] != right.Targets[index] {
			return false
		}
	}
	return true
}
func cloneFinalizerScopes(input []workflowruntime.FinalizerScope) []workflowruntime.FinalizerScope {
	result := make([]workflowruntime.FinalizerScope, len(input))
	for index, scope := range input {
		scope.Scope = append([]workflowruntime.NodeInvocationID(nil), scope.Scope...)
		result[index] = scope
	}
	return result
}
func cloneTerminalIntent(input workflowruntime.TerminalIntentSnapshot) workflowruntime.TerminalIntentSnapshot {
	input.Reason = cloneFailure(input.Reason)
	input.Error = cloneValueSetRef(input.Error)
	input.Finalizers = cloneFinalizerScopes(input.Finalizers)
	return input
}
func equalTerminalIntentImmutable(left, right workflowruntime.TerminalIntentSnapshot) bool {
	if left.RunID != right.RunID || left.IntendedStatus != right.IntendedStatus || left.IdempotencyKey != right.IdempotencyKey || !equalFailurePointers(left.Reason, right.Reason) || !equalValueSetRef(left.Error, right.Error) || len(left.Finalizers) != len(right.Finalizers) || !left.CreatedAt.Equal(right.CreatedAt) {
		return false
	}
	for index := range left.Finalizers {
		if left.Finalizers[index].Invocation != right.Finalizers[index].Invocation || left.Finalizers[index].Order != right.Finalizers[index].Order || len(left.Finalizers[index].Scope) != len(right.Finalizers[index].Scope) {
			return false
		}
		for member := range left.Finalizers[index].Scope {
			if left.Finalizers[index].Scope[member] != right.Finalizers[index].Scope[member] {
				return false
			}
		}
	}
	return true
}
func equalFailurePointers(left, right *workflowruntime.Failure) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return equalFailureValue(*left, *right)
}
func failureCode(failure *workflowruntime.Failure) string {
	if failure == nil {
		return ""
	}
	return failure.Code
}
