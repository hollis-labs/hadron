package runtimetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func (s *Store) RecordChildRun(ctx context.Context, link workflowruntime.ChildRunLink) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := link.Validate(); err != nil {
		return invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.runs[link.ParentRunID]; !ok {
		return fmt.Errorf("%w: parent run", workflowruntime.ErrNotFound)
	}
	if _, ok := s.runs[link.ChildRunID]; !ok {
		return fmt.Errorf("%w: child run", workflowruntime.ErrNotFound)
	}
	if _, ok := s.nodes[link.Invocation]; !ok {
		return fmt.Errorf("%w: child invocation", workflowruntime.ErrNotFound)
	}
	for _, existing := range s.childRuns[link.ParentRunID] {
		if existing.Invocation == link.Invocation || existing.ChildRunID == link.ChildRunID {
			if equalChildRunLink(existing, link) {
				return nil
			}
			return fmt.Errorf("%w: child run link", workflowruntime.ErrAlreadyExists)
		}
	}
	if !s.controlAdmissionAllowedLocked(link.Invocation) {
		return invalid(errors.New("pending terminal intent fences child run link"))
	}
	s.childRuns[link.ParentRunID] = append(s.childRuns[link.ParentRunID], link)
	sortChildRunLinks(s.childRuns[link.ParentRunID])
	return nil
}

func (s *Store) ListChildRuns(ctx context.Context, parent workflowruntime.RunID) ([]workflowruntime.ChildRunLink, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.runs[parent]; !ok {
		return nil, fmt.Errorf("%w: parent run", workflowruntime.ErrNotFound)
	}
	result := append([]workflowruntime.ChildRunLink(nil), s.childRuns[parent]...)
	sortChildRunLinks(result)
	return result, nil
}

func (s *Store) RequestRunCancellation(ctx context.Context, request workflowruntime.RequestRunCancellationRequest) (workflowruntime.RequestRunCancellationResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.RequestRunCancellationResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.RequestRunCancellationResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prior, ok := s.cancellationKeys[request.IdempotencyKey]; ok {
		if equalCancellationRequest(prior.request, request) {
			result := cloneCancellationResult(prior.result)
			result.Outcome = workflowruntime.IdempotencyReplayed
			return result, nil
		}
		return workflowruntime.RequestRunCancellationResult{}, idempotencyConflict("cancel run", request.IdempotencyKey)
	}
	if intent, exists := s.terminalIntents[request.RunID]; exists && intent.Status == workflowruntime.TerminalIntentPending {
		return workflowruntime.RequestRunCancellationResult{}, invalid(errors.New("pending terminal intent owns run cancellation"))
	}
	run, ok := s.runs[request.RunID]
	if !ok {
		return workflowruntime.RequestRunCancellationResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if run.Generation != request.ExpectedGeneration {
		return workflowruntime.RequestRunCancellationResult{}, casMismatch("run cancellation", request.ExpectedGeneration, run.Generation)
	}
	if run.Status == workflowruntime.RunCanceled {
		return workflowruntime.RequestRunCancellationResult{}, &workflowruntime.TransitionConflictError{Entity: "run", ID: string(run.ID), Status: string(run.Status), Reason: "cancellation requires exact idempotency replay"}
	}
	if !run.Status.Active() {
		return workflowruntime.RequestRunCancellationResult{}, &workflowruntime.TransitionError{Entity: "run", ID: string(run.ID), From: string(run.Status), To: string(workflowruntime.RunCanceled), Reason: "terminal status cannot be reopened"}
	}
	backup := s.backupCancellationStateLocked()
	collector := cancellationCollector{}
	if err := s.cancelRunLocked(request.RunID, request.At, request.Reason, request.IdempotencyKey, make(map[workflowruntime.RunID]struct{}), &collector); err != nil {
		s.restoreCancellationStateLocked(backup)
		return workflowruntime.RequestRunCancellationResult{}, err
	}
	result := workflowruntime.RequestRunCancellationResult{
		Outcome: workflowruntime.IdempotencyApplied, Run: cloneRun(s.runs[request.RunID]),
		Nodes: cloneCancellationNodes(collector.nodes), Intents: cloneCancellationIntents(collector.intents), Events: cloneEvents(collector.events),
	}
	s.cancellationKeys[request.IdempotencyKey] = cancellationRecord{request: cloneCancellationRequest(request), result: cloneCancellationResult(result)}
	return result, nil
}

type cancellationCollector struct {
	nodes   []workflowruntime.NodeInvocationSnapshot
	intents []workflowruntime.CancellationIntentSnapshot
	events  []workflowruntime.Event
}

func (s *Store) cancelRunLocked(runID workflowruntime.RunID, at time.Time, reason workflowruntime.Failure, key string, visited map[workflowruntime.RunID]struct{}, collector *cancellationCollector) error {
	return s.cancelRunLockedWithOptions(runID, at, reason, key, visited, collector, nil, true, true)
}

func (s *Store) cancelRunLockedWithOptions(runID workflowruntime.RunID, at time.Time, reason workflowruntime.Failure, key string, visited map[workflowruntime.RunID]struct{}, collector *cancellationCollector, excluded map[workflowruntime.NodeInvocationID]struct{}, terminalize, recurseDirect bool) error {
	if _, seen := visited[runID]; seen {
		return invalid(errors.New("child run cancellation cycle"))
	}
	visited[runID] = struct{}{}
	run, ok := s.runs[runID]
	if !ok {
		return fmt.Errorf("%w: run %q", workflowruntime.ErrNotFound, runID)
	}
	// The public root request rejects terminal runs before recursion. A direct
	// child that already finished is an honest parent-close race and remains
	// unchanged while cancellation continues for the parent and other children.
	if run.Status.Terminal() {
		return nil
	}
	if !run.Status.Active() || at.Before(run.UpdatedAt) {
		return invalid(fmt.Errorf("run %q cannot be canceled at requested time", runID))
	}
	if terminalize {
		nextRun := cloneRun(run)
		nextRun.Status = workflowruntime.RunCanceled
		nextRun.Generation++
		nextRun.UpdatedAt = at
		if err := nextRun.Validate(); err != nil {
			return invalid(err)
		}
		runEvent, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
			RunID: runID, Type: workflowruntime.EventRunCancellationRequested, OccurredAt: at,
			Attributes: map[string]string{"from_status": string(run.Status), "to_status": string(nextRun.Status), "reason_code": reason.Code},
			Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return err
		}
		s.runs[runID] = nextRun
		collector.events = append(collector.events, runEvent)
	}

	ids := make([]workflowruntime.NodeInvocationID, 0)
	for id, node := range s.nodes {
		_, skip := excluded[id]
		if id.RunID == runID && !node.Status.Terminal() && !skip {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].NodeID != ids[j].NodeID {
			return ids[i].NodeID < ids[j].NodeID
		}
		return ids[i].Iteration < ids[j].Iteration
	})
	for _, id := range ids {
		node := s.nodes[id]
		if node.Status.Terminal() {
			continue
		}
		switch node.Status {
		case workflowruntime.NodePending, workflowruntime.NodeReady, workflowruntime.NodeBlocked:
			if err := s.cancelUnstartedNodeLocked(node, at, reason, collector); err != nil {
				return err
			}
		case workflowruntime.NodeRunning:
			attemptID := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
			intent, err := s.ensureCancellationIntentLocked(runID, workflowruntime.CancellationRunningAttempt, &attemptID, "", "", at)
			if err != nil {
				return err
			}
			collector.intents = append(collector.intents, intent)
		case workflowruntime.NodeWaiting:
			if node.Wait != nil {
				if err := s.cancelGenericWaitLocked(node, at, reason, key, collector); err != nil {
					return err
				}
				continue
			}
			if s.hasPendingExternalLocked(node) {
				attemptID := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
				operation := s.externalOperations[attemptID]
				if operation.CancelRequestedAt.IsZero() {
					operation.CancelRequestedAt = at
					operation.Generation++
					operation.UpdatedAt = at
					if err := operation.Validate(); err != nil {
						return invalid(err)
					}
					s.externalOperations[attemptID] = operation
					invocation := node.ID
					event, appendErr := s.appendEventLocked(workflowruntime.AppendEventRequest{
						RunID: runID, Invocation: &invocation, Attempt: &attemptID,
						Type: workflowruntime.EventExternalOperationCancelRequested, OccurredAt: at,
						Attributes: map[string]string{"operation_kind": operation.Ref.Kind, "operation_id": operation.Ref.ID},
						Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
					})
					if appendErr != nil {
						return appendErr
					}
					collector.events = append(collector.events, event)
				}
				intent, err := s.ensureCancellationIntentLocked(runID, workflowruntime.CancellationExternalOperation, &attemptID, "", "", at)
				if err != nil {
					return err
				}
				collector.intents = append(collector.intents, intent)
				continue
			}
			matched, err := s.cancelRetryWaitingNodeLocked(node, at, reason, collector)
			if err != nil {
				return err
			}
			if matched {
				continue
			}
			matched, err = s.cancelFanOutParentLocked(node, at, reason, collector)
			if err != nil {
				return err
			}
			if matched {
				continue
			}
			attemptID := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
			intent, err := s.ensureCancellationIntentLocked(runID, workflowruntime.CancellationRunningAttempt, &attemptID, "", "", at)
			if err != nil {
				return err
			}
			collector.intents = append(collector.intents, intent)
		case workflowruntime.NodeSucceeded, workflowruntime.NodeFailed, workflowruntime.NodeSkipped,
			workflowruntime.NodeCanceled, workflowruntime.NodeTimedOut, workflowruntime.NodeCrashed:
			continue
		}
	}

	for _, link := range s.childRuns[runID] {
		switch link.Policy {
		case graph.ParentCloseCancel:
			if recurseDirect {
				if err := s.cancelRunLocked(link.ChildRunID, at, reason, key, visited, collector); err != nil {
					return err
				}
			}
		case graph.ParentCloseRequestCancel:
			intent, err := s.ensureCancellationIntentLocked(runID, workflowruntime.CancellationChildRun, nil, link.ChildRunID, link.Policy, at)
			if err != nil {
				return err
			}
			collector.intents = append(collector.intents, intent)
		case graph.ParentCloseAbandon:
			continue
		}
	}
	delete(visited, runID)
	return nil
}

func (s *Store) cancelUnstartedNodeLocked(node workflowruntime.NodeInvocationSnapshot, at time.Time, reason workflowruntime.Failure, collector *cancellationCollector) error {
	if at.Before(node.UpdatedAt) {
		return invalid(errors.New("node cancellation time must not regress"))
	}
	next := cloneNode(node)
	next.Status = workflowruntime.NodeCanceled
	next.Blocked = nil
	next.Lease = nil
	next.Generation++
	next.UpdatedAt = at
	if err := next.Validate(); err != nil {
		return invalid(err)
	}
	id := next.ID
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: id.RunID, Invocation: &id, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at,
		Attributes: map[string]string{"from_status": string(node.Status), "to_status": string(next.Status), "reason_code": reason.Code},
		Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return err
	}
	s.nodes[id] = next
	collector.nodes, collector.events = append(collector.nodes, next), append(collector.events, event)
	return nil
}

func (s *Store) cancelGenericWaitLocked(node workflowruntime.NodeInvocationSnapshot, at time.Time, reason workflowruntime.Failure, key string, collector *cancellationCollector) error {
	wait, ok := s.waits[node.Wait.ID]
	if !ok || wait.Status != workflowruntime.WaitOpen {
		return invalid(errors.New("waiting node has no open durable wait"))
	}
	attemptID := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
	attempt, ok := s.attempts[attemptID]
	if !ok || attempt.Status != workflowruntime.NodeRunning {
		return invalid(errors.New("waiting node has no unfinished attempt"))
	}
	if at.Before(wait.UpdatedAt) || at.Before(node.UpdatedAt) || at.Before(attempt.UpdatedAt) {
		return invalid(errors.New("wait cancellation time must not regress"))
	}
	nextWait := cloneWait(wait)
	nextWait.Status = workflowruntime.WaitCanceled
	nextWait.Resolution = &workflowwait.Resolution{
		Source: wait.WakeSource, Responder: workflowwait.Responder{Kind: "system", Reference: "run-cancellation"},
		IdempotencyKey: key, ResolvedAt: at,
	}
	nextWait.ResolvedAt, nextWait.UpdatedAt = at, at
	nextWait.Generation++
	nextAttempt := cloneAttempt(attempt)
	nextAttempt.Status = workflowruntime.NodeCanceled
	nextAttempt.Failure = cloneFailure(&reason)
	nextAttempt.FinishedAt, nextAttempt.UpdatedAt = at, at
	nextAttempt.Generation++
	nextNode := cloneNode(node)
	nextNode.Status = workflowruntime.NodeCanceled
	nextNode.Wait = nil
	nextNode.Lease = nil
	nextNode.Generation++
	nextNode.UpdatedAt = at
	if err := nextWait.Validate(); err != nil {
		return invalid(err)
	}
	if err := nextAttempt.Validate(); err != nil {
		return invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return invalid(err)
	}
	id := nextNode.ID
	events := []workflowruntime.AppendEventRequest{
		{RunID: id.RunID, Invocation: &id, Attempt: &attemptID, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: at, Attributes: map[string]string{"attempt_status": string(workflowruntime.NodeCanceled), "failure_code": reason.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{RunID: id.RunID, Invocation: &id, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at, Attributes: map[string]string{"from_status": string(node.Status), "to_status": string(workflowruntime.NodeCanceled)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{RunID: id.RunID, Invocation: &id, Attempt: &attemptID, Type: "wait.canceled", OccurredAt: at, Attributes: map[string]string{"wait_id": string(wait.Ref.ID)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
	}
	appended, err := s.appendEventRequestsLocked(events)
	if err != nil {
		return err
	}
	s.waits[nextWait.Ref.ID], s.attempts[attemptID], s.nodes[id] = nextWait, nextAttempt, nextNode
	collector.nodes = append(collector.nodes, nextNode)
	collector.events = append(collector.events, appended...)
	return nil
}

func (s *Store) cancelRetryWaitingNodeLocked(node workflowruntime.NodeInvocationSnapshot, at time.Time, reason workflowruntime.Failure, collector *cancellationCollector) (bool, error) {
	for id, activation := range s.retryActivations {
		if activation.Attempt.Invocation != node.ID || activation.Status != workflowruntime.RetryScheduled {
			continue
		}
		if at.Before(node.UpdatedAt) || at.Before(activation.UpdatedAt) {
			return true, invalid(errors.New("retry cancellation time must not regress"))
		}
		activation.Status = workflowruntime.RetryCanceled
		activation.Generation++
		activation.UpdatedAt = at
		nextNode := cloneNode(node)
		nextNode.Status = workflowruntime.NodeCanceled
		nextNode.Lease = nil
		nextNode.Generation++
		nextNode.UpdatedAt = at
		invocation, attemptID := node.ID, activation.Attempt
		event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
			RunID: node.ID.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventRetryCanceled, OccurredAt: at,
			Attributes: map[string]string{"activation_id": id, "reason_code": reason.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return true, err
		}
		s.retryActivations[id], s.nodes[node.ID] = activation, nextNode
		collector.nodes, collector.events = append(collector.nodes, nextNode), append(collector.events, event)
		return true, nil
	}
	return false, nil
}

func (s *Store) cancelFanOutParentLocked(node workflowruntime.NodeInvocationSnapshot, at time.Time, reason workflowruntime.Failure, collector *cancellationCollector) (bool, error) {
	fanOut, ok := s.fanOuts[node.ID]
	if !ok || fanOut.Status != workflowruntime.FanOutActive {
		return false, nil
	}
	if at.Before(node.UpdatedAt) || at.Before(fanOut.UpdatedAt) {
		return true, invalid(errors.New("fan-out cancellation time must not regress"))
	}
	fanOut.Status = workflowruntime.FanOutCanceled
	fanOut.Failure = cloneFailure(&reason)
	fanOut.Generation++
	fanOut.UpdatedAt = at
	nextNode := cloneNode(node)
	nextNode.Status = workflowruntime.NodeCanceled
	nextNode.Generation++
	nextNode.UpdatedAt = at
	if err := fanOut.Validate(); err != nil {
		return true, invalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return true, invalid(err)
	}
	id := node.ID
	event, err := s.appendEventLocked(workflowruntime.AppendEventRequest{
		RunID: id.RunID, Invocation: &id, Type: workflowruntime.EventFanOutCompleted, OccurredAt: at,
		Attributes: map[string]string{"status": string(workflowruntime.FanOutCanceled), "reason_code": reason.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return true, err
	}
	s.fanOuts[node.ID], s.nodes[node.ID] = fanOut, nextNode
	collector.nodes, collector.events = append(collector.nodes, nextNode), append(collector.events, event)
	return true, nil
}

func (s *Store) hasPendingExternalLocked(node workflowruntime.NodeInvocationSnapshot) bool {
	operation, ok := s.externalOperations[workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}]
	return ok && operation.Status == stepkind.ObservationPending
}

func (s *Store) ensureCancellationIntentLocked(runID workflowruntime.RunID, kind workflowruntime.CancellationIntentKind, attempt *workflowruntime.AttemptID, child workflowruntime.RunID, policy graph.ParentClosePolicy, at time.Time) (workflowruntime.CancellationIntentSnapshot, error) {
	id, err := cancellationIntentID(kind, attempt, child)
	if err != nil {
		return workflowruntime.CancellationIntentSnapshot{}, invalid(err)
	}
	if existing, ok := s.cancellationIntents[id]; ok {
		return cloneCancellationIntent(existing), nil
	}
	intent := workflowruntime.CancellationIntentSnapshot{ID: id, RunID: runID, Kind: kind, Attempt: cloneAttemptID(attempt), ChildRunID: child, Status: workflowruntime.CancellationPending, Generation: 1, RequestedAt: at, UpdatedAt: at}
	if kind == workflowruntime.CancellationChildRun {
		intent.ChildPolicy = policy
	}
	if err := intent.Validate(); err != nil {
		return workflowruntime.CancellationIntentSnapshot{}, invalid(err)
	}
	s.cancellationIntents[id] = intent
	return cloneCancellationIntent(intent), nil
}

func cancellationIntentID(kind workflowruntime.CancellationIntentKind, attempt *workflowruntime.AttemptID, child workflowruntime.RunID) (string, error) {
	if attempt != nil {
		encoded, err := workflowruntime.EncodeAttemptIdentity(*attempt)
		if err != nil {
			return "", err
		}
		return "cancel:" + string(kind) + ":" + encoded, nil
	}
	return "cancel:" + string(kind) + ":" + string(child), nil
}

func (s *Store) ResolveCancellationIntent(ctx context.Context, request workflowruntime.ResolveCancellationIntentRequest) (workflowruntime.ResolveCancellationIntentResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ResolveCancellationIntentResult{}, err
	}
	request.At = request.At.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ResolveCancellationIntentResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	intent, ok := s.cancellationIntents[request.IntentID]
	if !ok {
		return workflowruntime.ResolveCancellationIntentResult{}, fmt.Errorf("%w: cancellation intent", workflowruntime.ErrNotFound)
	}
	if intent.Generation != request.ExpectedGeneration {
		return workflowruntime.ResolveCancellationIntentResult{}, casMismatch("cancellation intent", request.ExpectedGeneration, intent.Generation)
	}
	if intent.Status == workflowruntime.CancellationResolved {
		if request.At.Equal(intent.ResolvedAt) {
			return workflowruntime.ResolveCancellationIntentResult{Intent: cloneCancellationIntent(intent)}, nil
		}
		return workflowruntime.ResolveCancellationIntentResult{}, &workflowruntime.TransitionConflictError{Entity: "cancellation intent", ID: intent.ID, Status: string(intent.Status), Reason: "resolution is not exact replay"}
	}
	if request.At.Before(intent.UpdatedAt) {
		return workflowruntime.ResolveCancellationIntentResult{}, invalid(errors.New("cancellation resolution time must not regress"))
	}
	result := workflowruntime.ResolveCancellationIntentResult{}
	eventRequests := make([]workflowruntime.AppendEventRequest, 0, 3)
	switch intent.Kind {
	case workflowruntime.CancellationRunningAttempt:
		attempt := s.attempts[*intent.Attempt]
		node := s.nodes[intent.Attempt.Invocation]
		if attempt.Status == workflowruntime.NodeRunning {
			fromStatus := node.Status
			failure := workflowruntime.Failure{Code: "run_canceled", Message: "run cancellation stopped the active attempt"}
			attempt.Status = workflowruntime.NodeCanceled
			attempt.Failure = &failure
			attempt.FinishedAt, attempt.UpdatedAt = request.At, request.At
			attempt.Generation++
			node.Status = workflowruntime.NodeCanceled
			node.Lease = nil
			node.Generation++
			node.UpdatedAt = request.At
			if err := attempt.Validate(); err != nil {
				return workflowruntime.ResolveCancellationIntentResult{}, invalid(err)
			}
			if err := node.Validate(); err != nil {
				return workflowruntime.ResolveCancellationIntentResult{}, invalid(err)
			}
			invocation, attemptID := node.ID, attempt.ID
			eventRequests = append(eventRequests,
				workflowruntime.AppendEventRequest{RunID: intent.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: request.At, Attributes: map[string]string{"attempt_status": string(workflowruntime.NodeCanceled), "failure_code": failure.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
				workflowruntime.AppendEventRequest{RunID: intent.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.At, Attributes: map[string]string{"from_status": string(fromStatus), "to_status": string(workflowruntime.NodeCanceled), "reason_code": failure.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
			)
		}
		attemptCopy, nodeCopy := cloneAttempt(attempt), cloneNode(node)
		result.Attempt, result.Node = &attemptCopy, &nodeCopy
	case workflowruntime.CancellationExternalOperation:
		operation := s.externalOperations[*intent.Attempt]
		if operation.Status == stepkind.ObservationPending {
			return workflowruntime.ResolveCancellationIntentResult{}, invalid(errors.New("pending external operation cancellation remains unresolved"))
		}
	case workflowruntime.CancellationChildRun:
	}
	intent.Status = workflowruntime.CancellationResolved
	intent.ResolvedAt, intent.UpdatedAt = request.At, request.At
	intent.Generation++
	if err := intent.Validate(); err != nil {
		return workflowruntime.ResolveCancellationIntentResult{}, invalid(err)
	}
	eventRequests = append(eventRequests, workflowruntime.AppendEventRequest{
		RunID: intent.RunID, Attempt: cloneAttemptID(intent.Attempt), Type: workflowruntime.EventCancellationResolved, OccurredAt: request.At,
		Attributes: map[string]string{"intent_id": intent.ID, "kind": string(intent.Kind)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err := s.validateEventRequestsLocked(eventRequests); err != nil {
		return workflowruntime.ResolveCancellationIntentResult{}, err
	}
	events, err := s.appendEventRequestsLocked(eventRequests)
	if err != nil {
		return workflowruntime.ResolveCancellationIntentResult{}, err
	}
	if result.Attempt != nil {
		s.attempts[result.Attempt.ID] = cloneAttempt(*result.Attempt)
		s.nodes[result.Node.ID] = cloneNode(*result.Node)
	}
	s.cancellationIntents[intent.ID] = intent
	eventCopy := cloneEvent(events[len(events)-1])
	result.Intent, result.Event = cloneCancellationIntent(intent), &eventCopy
	return result, nil
}

func (s *Store) RecoverCancellationIntents(ctx context.Context, query workflowruntime.CancellationIntentQuery) ([]workflowruntime.CancellationIntentSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, invalid(errors.New("cancellation recovery limit must not be negative"))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]workflowruntime.CancellationIntentSnapshot, 0)
	for _, intent := range s.cancellationIntents {
		if intent.Status == workflowruntime.CancellationPending && (query.RunID == "" || intent.RunID == query.RunID) {
			result = append(result, cloneCancellationIntent(intent))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].RequestedAt.Equal(result[j].RequestedAt) {
			return result[i].RequestedAt.Before(result[j].RequestedAt)
		}
		return result[i].ID < result[j].ID
	})
	return limit(result, query.Limit), nil
}

func cloneCancellationIntent(intent workflowruntime.CancellationIntentSnapshot) workflowruntime.CancellationIntentSnapshot {
	intent.Attempt = cloneAttemptID(intent.Attempt)
	return intent
}

func cloneCancellationIntents(intents []workflowruntime.CancellationIntentSnapshot) []workflowruntime.CancellationIntentSnapshot {
	result := make([]workflowruntime.CancellationIntentSnapshot, len(intents))
	for index := range intents {
		result[index] = cloneCancellationIntent(intents[index])
	}
	return result
}

func cloneCancellationRequest(request workflowruntime.RequestRunCancellationRequest) workflowruntime.RequestRunCancellationRequest {
	request.Reason.Details = cloneStringMap(request.Reason.Details)
	return request
}

func cloneCancellationResult(result workflowruntime.RequestRunCancellationResult) workflowruntime.RequestRunCancellationResult {
	result.Run = cloneRun(result.Run)
	result.Nodes = cloneCancellationNodes(result.Nodes)
	result.Intents = cloneCancellationIntents(result.Intents)
	result.Events = cloneEvents(result.Events)
	return result
}

func cloneCancellationNodes(nodes []workflowruntime.NodeInvocationSnapshot) []workflowruntime.NodeInvocationSnapshot {
	result := make([]workflowruntime.NodeInvocationSnapshot, len(nodes))
	for index := range nodes {
		result[index] = cloneNode(nodes[index])
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ID.RunID != result[j].ID.RunID {
			return result[i].ID.RunID < result[j].ID.RunID
		}
		if result[i].ID.NodeID != result[j].ID.NodeID {
			return result[i].ID.NodeID < result[j].ID.NodeID
		}
		return result[i].ID.Iteration < result[j].ID.Iteration
	})
	return result
}

func equalCancellationRequest(left, right workflowruntime.RequestRunCancellationRequest) bool {
	return left.RunID == right.RunID && left.ExpectedGeneration == right.ExpectedGeneration && left.IdempotencyKey == right.IdempotencyKey && left.At.Equal(right.At) && equalFailureValue(left.Reason, right.Reason)
}

func equalFailureValue(left, right workflowruntime.Failure) bool {
	if left.Code != right.Code || left.Message != right.Message || left.Retryable != right.Retryable || len(left.Details) != len(right.Details) {
		return false
	}
	for key, value := range left.Details {
		if right.Details[key] != value {
			return false
		}
	}
	return true
}

func equalChildRunLink(left, right workflowruntime.ChildRunLink) bool {
	return left.ParentRunID == right.ParentRunID && left.Invocation == right.Invocation && left.ChildRunID == right.ChildRunID && left.Policy == right.Policy && left.CreatedAt.Equal(right.CreatedAt)
}

func sortChildRunLinks(links []workflowruntime.ChildRunLink) {
	sort.Slice(links, func(i, j int) bool {
		if links[i].ChildRunID != links[j].ChildRunID {
			return links[i].ChildRunID < links[j].ChildRunID
		}
		return nodeIdentity(links[i].Invocation) < nodeIdentity(links[j].Invocation)
	})
}

type cancellationStateBackup struct {
	runs                map[workflowruntime.RunID]workflowruntime.RunSnapshot
	nodes               map[workflowruntime.NodeInvocationID]workflowruntime.NodeInvocationSnapshot
	attempts            map[workflowruntime.AttemptID]workflowruntime.AttemptSnapshot
	waits               map[workflowruntime.WaitID]workflowruntime.WaitSnapshot
	externalOperations  map[workflowruntime.AttemptID]workflowruntime.ExternalOperationSnapshot
	retryActivations    map[string]workflowruntime.RetryActivationSnapshot
	fanOuts             map[workflowruntime.NodeInvocationID]workflowruntime.FanOutSnapshot
	cancellationIntents map[string]workflowruntime.CancellationIntentSnapshot
	events              map[workflowruntime.RunID][]workflowruntime.Event
	controlDecisions    map[workflowruntime.ControlDecisionID]workflowruntime.ControlDecisionSnapshot
	terminalIntents     map[workflowruntime.RunID]workflowruntime.TerminalIntentSnapshot
	terminalKeys        map[string]workflowruntime.RunID
	valueSets           map[string]storedValues
	nextValueSet        uint64
}

func (s *Store) backupCancellationStateLocked() cancellationStateBackup {
	backup := cancellationStateBackup{
		runs:                make(map[workflowruntime.RunID]workflowruntime.RunSnapshot, len(s.runs)),
		nodes:               make(map[workflowruntime.NodeInvocationID]workflowruntime.NodeInvocationSnapshot, len(s.nodes)),
		attempts:            make(map[workflowruntime.AttemptID]workflowruntime.AttemptSnapshot, len(s.attempts)),
		waits:               make(map[workflowruntime.WaitID]workflowruntime.WaitSnapshot, len(s.waits)),
		externalOperations:  make(map[workflowruntime.AttemptID]workflowruntime.ExternalOperationSnapshot, len(s.externalOperations)),
		retryActivations:    make(map[string]workflowruntime.RetryActivationSnapshot, len(s.retryActivations)),
		fanOuts:             make(map[workflowruntime.NodeInvocationID]workflowruntime.FanOutSnapshot, len(s.fanOuts)),
		cancellationIntents: make(map[string]workflowruntime.CancellationIntentSnapshot, len(s.cancellationIntents)),
		events:              make(map[workflowruntime.RunID][]workflowruntime.Event, len(s.events)),
		controlDecisions:    make(map[workflowruntime.ControlDecisionID]workflowruntime.ControlDecisionSnapshot, len(s.controlDecisions)),
		terminalIntents:     make(map[workflowruntime.RunID]workflowruntime.TerminalIntentSnapshot, len(s.terminalIntents)),
		terminalKeys:        make(map[string]workflowruntime.RunID, len(s.terminalKeys)),
		valueSets:           make(map[string]storedValues, len(s.valueSets)),
		nextValueSet:        s.nextValueSet,
	}
	for id, snapshot := range s.runs {
		backup.runs[id] = cloneRun(snapshot)
	}
	for id, snapshot := range s.nodes {
		backup.nodes[id] = cloneNode(snapshot)
	}
	for id, snapshot := range s.attempts {
		backup.attempts[id] = cloneAttempt(snapshot)
	}
	for id, snapshot := range s.waits {
		backup.waits[id] = cloneWait(snapshot)
	}
	for id, snapshot := range s.externalOperations {
		backup.externalOperations[id] = cloneExternalOperation(snapshot)
	}
	for id, snapshot := range s.retryActivations {
		backup.retryActivations[id] = cloneRetryActivation(snapshot)
	}
	for id, snapshot := range s.fanOuts {
		backup.fanOuts[id] = cloneFanOut(snapshot)
	}
	for id, snapshot := range s.cancellationIntents {
		backup.cancellationIntents[id] = cloneCancellationIntent(snapshot)
	}
	for runID, events := range s.events {
		backup.events[runID] = cloneEvents(events)
	}
	for id, decision := range s.controlDecisions {
		backup.controlDecisions[id] = cloneControlDecision(decision)
	}
	for runID, intent := range s.terminalIntents {
		backup.terminalIntents[runID] = cloneTerminalIntent(intent)
	}
	for key, runID := range s.terminalKeys {
		backup.terminalKeys[key] = runID
	}
	for id, stored := range s.valueSets {
		backup.valueSets[id] = stored
	}
	return backup
}

func (s *Store) restoreCancellationStateLocked(backup cancellationStateBackup) {
	s.runs = backup.runs
	s.nodes = backup.nodes
	s.attempts = backup.attempts
	s.waits = backup.waits
	s.externalOperations = backup.externalOperations
	s.retryActivations = backup.retryActivations
	s.fanOuts = backup.fanOuts
	s.cancellationIntents = backup.cancellationIntents
	s.events = backup.events
	s.controlDecisions = backup.controlDecisions
	s.terminalIntents = backup.terminalIntents
	s.terminalKeys = backup.terminalKeys
	s.valueSets = backup.valueSets
	s.nextValueSet = backup.nextValueSet
}
