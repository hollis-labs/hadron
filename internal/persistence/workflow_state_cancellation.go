package persistence

import (
	"context"
	"database/sql"
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

var _ workflowruntime.CancellationStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) RecordChildRun(ctx context.Context, link workflowruntime.ChildRunLink) error {
	if err := link.Validate(); err != nil {
		return workflowInvalid(err)
	}
	return s.write(ctx, "record workflow child run", func(query workflowSQL) error {
		if _, err := loadWorkflowRun(ctx, query, link.ParentRunID); err != nil {
			return err
		}
		if _, err := loadWorkflowRun(ctx, query, link.ChildRunID); err != nil {
			return err
		}
		if _, err := loadWorkflowNode(ctx, query, link.Invocation); err != nil {
			return err
		}
		encoded, err := encodeWorkflowJSON(link)
		if err != nil {
			return err
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_child_runs(parent_run_id, node_id, iteration, child_run_id, policy, created_at, link_json) VALUES (?, ?, ?, ?, ?, ?, ?)`, link.ParentRunID, link.Invocation.NodeID, link.Invocation.Iteration, link.ChildRunID, link.Policy, workflowTime(link.CreatedAt), encoded); err != nil {
			if !isSQLiteConstraint(err) {
				return fmt.Errorf("record workflow child run: %w", err)
			}
			var prior string
			if loadErr := query.QueryRowContext(ctx, `SELECT link_json FROM workflow_child_runs WHERE (parent_run_id = ? AND node_id = ? AND iteration = ?) OR child_run_id = ?`, link.ParentRunID, link.Invocation.NodeID, link.Invocation.Iteration, link.ChildRunID).Scan(&prior); loadErr == nil && prior == encoded {
				return nil
			}
			return fmt.Errorf("%w: child run link", workflowruntime.ErrAlreadyExists)
		}
		return nil
	})
}

func (s *WorkflowStateStore) ListChildRuns(ctx context.Context, parent workflowruntime.RunID) ([]workflowruntime.ChildRunLink, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if _, err := loadWorkflowRun(ctx, s.db, parent); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT link_json FROM workflow_child_runs WHERE parent_run_id = ? ORDER BY child_run_id, node_id, iteration`, parent)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]workflowruntime.ChildRunLink, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var link workflowruntime.ChildRunLink
		if err := decodeWorkflowJSON("child run link", encoded, &link); err != nil {
			return nil, err
		}
		if err := link.Validate(); err != nil {
			return nil, workflowInvalid(err)
		}
		result = append(result, link)
	}
	return result, rows.Err()
}

func (s *WorkflowStateStore) RequestRunCancellation(ctx context.Context, request workflowruntime.RequestRunCancellationRequest) (workflowruntime.RequestRunCancellationResult, error) {
	if err := request.Validate(); err != nil {
		return workflowruntime.RequestRunCancellationResult{}, workflowInvalid(err)
	}
	canonical := request
	canonical.At = canonical.At.UTC()
	requestJSON, encodeErr := encodeWorkflowJSON(canonical)
	if encodeErr != nil {
		return workflowruntime.RequestRunCancellationResult{}, encodeErr
	}
	var result workflowruntime.RequestRunCancellationResult
	writeErr := s.write(ctx, "request workflow run cancellation", func(query workflowSQL) error {
		priorRequest, priorResult, found, loadErr := loadWorkflowIdempotency(ctx, query, "workflow_run_cancellation_idempotency", request.IdempotencyKey)
		if loadErr != nil {
			return loadErr
		}
		if found {
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("cancel run", request.IdempotencyKey)
			}
			if decodeErr := decodeWorkflowJSON("run cancellation result", priorResult, &result); decodeErr != nil {
				return decodeErr
			}
			result.Outcome = workflowruntime.IdempotencyReplayed
			return nil
		}
		run, runErr := loadWorkflowRun(ctx, query, request.RunID)
		if runErr != nil {
			return runErr
		}
		if run.Generation != request.ExpectedGeneration {
			return workflowCAS("run cancellation", request.ExpectedGeneration, run.Generation)
		}
		if run.Status == workflowruntime.RunCanceled {
			return &workflowruntime.TransitionConflictError{Entity: "run", ID: string(run.ID), Status: string(run.Status), Reason: "cancellation requires exact idempotency replay"}
		}
		if !run.Status.Active() {
			return &workflowruntime.TransitionError{Entity: "run", ID: string(run.ID), From: string(run.Status), To: string(workflowruntime.RunCanceled), Reason: "terminal status cannot be reopened"}
		}
		collector := workflowCancellationCollector{}
		if cancelErr := cancelWorkflowRun(ctx, query, request.RunID, request.At.UTC(), request.Reason, request.IdempotencyKey, make(map[workflowruntime.RunID]bool), &collector); cancelErr != nil {
			return cancelErr
		}
		result = workflowruntime.RequestRunCancellationResult{
			Outcome: workflowruntime.IdempotencyApplied, Run: collector.root,
			Nodes: collector.nodes, Intents: collector.intents, Events: collector.events,
		}
		resultJSON, resultEncodeErr := encodeWorkflowJSON(result)
		if resultEncodeErr != nil {
			return resultEncodeErr
		}
		if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_run_cancellation_idempotency(idempotency_key, request_json, result_json) VALUES (?, ?, ?)`, request.IdempotencyKey, requestJSON, resultJSON); execErr != nil {
			if isSQLiteConstraint(execErr) {
				return workflowIdempotencyConflict("cancel run", request.IdempotencyKey)
			}
			return execErr
		}
		return nil
	})
	if writeErr != nil {
		return workflowruntime.RequestRunCancellationResult{}, writeErr
	}
	return result, nil
}

type workflowCancellationCollector struct {
	root    workflowruntime.RunSnapshot
	nodes   []workflowruntime.NodeInvocationSnapshot
	intents []workflowruntime.CancellationIntentSnapshot
	events  []workflowruntime.Event
}

func cancelWorkflowRun(ctx context.Context, query workflowSQL, runID workflowruntime.RunID, at time.Time, reason workflowruntime.Failure, key string, visited map[workflowruntime.RunID]bool, collector *workflowCancellationCollector) error {
	if visited[runID] {
		return workflowInvalid(errors.New("child run cancellation cycle"))
	}
	visited[runID] = true
	defer delete(visited, runID)
	run, loadErr := loadWorkflowRun(ctx, query, runID)
	if loadErr != nil {
		return loadErr
	}
	// The public root request rejects terminal runs before recursion. A direct
	// child that already finished is an honest parent-close race and remains
	// unchanged while cancellation continues for the parent and other children.
	if run.Status.Terminal() {
		return nil
	}
	if !run.Status.Active() || at.Before(run.UpdatedAt) {
		return workflowInvalid(fmt.Errorf("run %q cannot be canceled at requested time", runID))
	}
	nextRun := run
	nextRun.Status = workflowruntime.RunCanceled
	nextRun.Generation++
	nextRun.UpdatedAt = at
	if validationErr := nextRun.Validate(); validationErr != nil {
		return workflowInvalid(validationErr)
	}
	if updateErr := updateWorkflowRunCAS(ctx, query, nextRun, run.Generation); updateErr != nil {
		return updateErr
	}
	event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
		RunID: runID, Type: workflowruntime.EventRunCancellationRequested, OccurredAt: at,
		Attributes: map[string]string{"from_status": string(run.Status), "to_status": string(nextRun.Status), "reason_code": reason.Code},
		Redaction:  values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if eventErr != nil {
		return eventErr
	}
	if collector.root.ID == "" {
		collector.root = nextRun
	}
	collector.events = append(collector.events, event)

	rows, err := query.QueryContext(ctx, workflowNodeSelect+` WHERE n.run_id = ? ORDER BY n.node_id, n.iteration`, runID)
	if err != nil {
		return err
	}
	nodes := make([]workflowruntime.NodeInvocationSnapshot, 0)
	for rows.Next() {
		node, scanErr := scanWorkflowNode(rows)
		if scanErr != nil {
			closeRows(rows)
			return scanErr
		}
		nodes = append(nodes, node)
	}
	closeRows(rows)
	for _, node := range nodes {
		if node.Status.Terminal() {
			continue
		}
		switch node.Status {
		case workflowruntime.NodePending, workflowruntime.NodeReady, workflowruntime.NodeBlocked:
			if cancelErr := cancelWorkflowUnstartedNode(ctx, query, node, at, reason, collector); cancelErr != nil {
				return cancelErr
			}
		case workflowruntime.NodeRunning:
			attempt := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
			intent, intentErr := ensureWorkflowCancellationIntent(ctx, query, runID, workflowruntime.CancellationRunningAttempt, &attempt, "", at)
			if intentErr != nil {
				return intentErr
			}
			collector.intents = append(collector.intents, intent)
		case workflowruntime.NodeWaiting:
			handled, waitingErr := cancelWorkflowWaitingNode(ctx, query, node, at, reason, key, collector)
			if waitingErr != nil {
				return waitingErr
			}
			if !handled {
				attempt := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
				intent, intentErr := ensureWorkflowCancellationIntent(ctx, query, runID, workflowruntime.CancellationRunningAttempt, &attempt, "", at)
				if intentErr != nil {
					return intentErr
				}
				collector.intents = append(collector.intents, intent)
			}
		case workflowruntime.NodeSucceeded, workflowruntime.NodeFailed, workflowruntime.NodeSkipped,
			workflowruntime.NodeCanceled, workflowruntime.NodeTimedOut, workflowruntime.NodeCrashed:
			continue
		}
	}

	links, linksErr := listWorkflowChildRuns(ctx, query, runID)
	if linksErr != nil {
		return linksErr
	}
	for _, link := range links {
		switch link.Policy {
		case graph.ParentCloseCancel:
			if cancelErr := cancelWorkflowRun(ctx, query, link.ChildRunID, at, reason, key, visited, collector); cancelErr != nil {
				return cancelErr
			}
		case graph.ParentCloseRequestCancel:
			intent, intentErr := ensureWorkflowCancellationIntent(ctx, query, runID, workflowruntime.CancellationChildRun, nil, link.ChildRunID, at)
			if intentErr != nil {
				return intentErr
			}
			collector.intents = append(collector.intents, intent)
		case graph.ParentCloseAbandon:
			continue
		}
	}
	return nil
}

func cancelWorkflowUnstartedNode(ctx context.Context, query workflowSQL, node workflowruntime.NodeInvocationSnapshot, at time.Time, reason workflowruntime.Failure, collector *workflowCancellationCollector) error {
	if at.Before(node.UpdatedAt) {
		return workflowInvalid(errors.New("node cancellation time must not regress"))
	}
	next := cloneWorkflowNode(node)
	next.Status, next.Blocked, next.Lease = workflowruntime.NodeCanceled, nil, nil
	next.Generation++
	next.UpdatedAt = at
	if err := next.Validate(); err != nil {
		return workflowInvalid(err)
	}
	if err := updateWorkflowNodeCAS(ctx, query, next, node.Generation); err != nil {
		return err
	}
	id := next.ID
	event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: id.RunID, Invocation: &id, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at, Attributes: map[string]string{"from_status": string(node.Status), "to_status": string(next.Status), "reason_code": reason.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return err
	}
	collector.nodes, collector.events = append(collector.nodes, next), append(collector.events, event)
	return nil
}

func cancelWorkflowWaitingNode(ctx context.Context, query workflowSQL, node workflowruntime.NodeInvocationSnapshot, at time.Time, reason workflowruntime.Failure, key string, collector *workflowCancellationCollector) (bool, error) {
	if node.Wait != nil {
		return true, cancelWorkflowGenericWait(ctx, query, node, at, reason, key, collector)
	}
	attemptID := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
	operation, operationErr := loadWorkflowExternalOperation(ctx, query, attemptID)
	if operationErr == nil && operation.Status == stepkind.ObservationPending {
		if operation.CancelRequestedAt.IsZero() {
			if at.Before(operation.UpdatedAt) {
				return true, workflowInvalid(errors.New("external cancellation time must not regress"))
			}
			previous := operation.Generation
			operation.CancelRequestedAt, operation.UpdatedAt = at, at
			operation.Generation++
			if updateErr := updateWorkflowExternalOperationCAS(ctx, query, operation, previous); updateErr != nil {
				return true, updateErr
			}
			invocation := node.ID
			event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: node.ID.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventExternalOperationCancelRequested, OccurredAt: at, Attributes: map[string]string{"operation_kind": operation.Ref.Kind, "operation_id": operation.Ref.ID}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
			if eventErr != nil {
				return true, eventErr
			}
			collector.events = append(collector.events, event)
		}
		intent, intentErr := ensureWorkflowCancellationIntent(ctx, query, node.ID.RunID, workflowruntime.CancellationExternalOperation, &attemptID, "", at)
		if intentErr != nil {
			return true, intentErr
		}
		collector.intents = append(collector.intents, intent)
		return true, nil
	}
	if operationErr != nil && !errors.Is(operationErr, workflowruntime.ErrNotFound) {
		return true, operationErr
	}

	activation, activationErr := loadWorkflowRetryForInvocation(ctx, query, node.ID)
	if activationErr == nil {
		if at.Before(node.UpdatedAt) || at.Before(activation.UpdatedAt) {
			return true, workflowInvalid(errors.New("retry cancellation time must not regress"))
		}
		activation.Status = workflowruntime.RetryCanceled
		activation.Generation++
		activation.UpdatedAt = at
		if updateErr := updateWorkflowRetryActivation(ctx, query, activation, activation.Generation-1); updateErr != nil {
			return true, updateErr
		}
		next := cloneWorkflowNode(node)
		next.Status, next.Lease = workflowruntime.NodeCanceled, nil
		next.Generation++
		next.UpdatedAt = at
		if updateErr := updateWorkflowNodeCAS(ctx, query, next, node.Generation); updateErr != nil {
			return true, updateErr
		}
		invocation, attempt := next.ID, activation.Attempt
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: node.ID.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventRetryCanceled, OccurredAt: at, Attributes: map[string]string{"activation_id": activation.ID, "reason_code": reason.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if eventErr != nil {
			return true, eventErr
		}
		collector.nodes, collector.events = append(collector.nodes, next), append(collector.events, event)
		return true, nil
	}
	if !errors.Is(activationErr, workflowruntime.ErrNotFound) {
		return true, activationErr
	}

	if node.ID.Iteration == "" {
		fanOut, fanOutErr := loadWorkflowFanOut(ctx, query, node.ID)
		if fanOutErr == nil && fanOut.Status == workflowruntime.FanOutActive {
			if at.Before(node.UpdatedAt) || at.Before(fanOut.UpdatedAt) {
				return true, workflowInvalid(errors.New("fan-out cancellation time must not regress"))
			}
			fanOut.Status, fanOut.Failure = workflowruntime.FanOutCanceled, cloneWorkflowFailure(&reason)
			fanOut.Generation++
			fanOut.UpdatedAt = at
			if updateErr := updateWorkflowFanOut(ctx, query, fanOut, fanOut.Generation-1); updateErr != nil {
				return true, updateErr
			}
			next := cloneWorkflowNode(node)
			next.Status = workflowruntime.NodeCanceled
			next.Generation++
			next.UpdatedAt = at
			if updateErr := updateWorkflowNodeCAS(ctx, query, next, node.Generation); updateErr != nil {
				return true, updateErr
			}
			id := next.ID
			event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: id.RunID, Invocation: &id, Type: workflowruntime.EventFanOutCompleted, OccurredAt: at, Attributes: map[string]string{"status": string(fanOut.Status), "reason_code": reason.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
			if eventErr != nil {
				return true, eventErr
			}
			collector.nodes, collector.events = append(collector.nodes, next), append(collector.events, event)
			return true, nil
		}
		if fanOutErr != nil && !errors.Is(fanOutErr, workflowruntime.ErrNotFound) {
			return true, fanOutErr
		}
	}
	return false, nil
}

func cancelWorkflowGenericWait(ctx context.Context, query workflowSQL, node workflowruntime.NodeInvocationSnapshot, at time.Time, reason workflowruntime.Failure, key string, collector *workflowCancellationCollector) error {
	wait, err := loadWorkflowWait(ctx, query, node.Wait.ID)
	if err != nil {
		return err
	}
	attemptID := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
	attempt, err := loadWorkflowAttempt(ctx, query, attemptID)
	if err != nil {
		return err
	}
	if wait.Status != workflowruntime.WaitOpen || attempt.Status != workflowruntime.NodeRunning || at.Before(wait.UpdatedAt) || at.Before(node.UpdatedAt) || at.Before(attempt.UpdatedAt) {
		return workflowInvalid(errors.New("wait cancellation requires open wait, unfinished attempt, and non-regressing time"))
	}
	nextWait := wait
	nextWait.Status = workflowruntime.WaitCanceled
	nextWait.Resolution = &workflowwait.Resolution{Source: wait.WakeSource, Responder: workflowwait.Responder{Kind: "system", Reference: "run-cancellation"}, IdempotencyKey: key, ResolvedAt: at}
	nextWait.ResolvedAt, nextWait.UpdatedAt = at, at
	nextWait.Generation++
	nextAttempt := cloneWorkflowAttempt(attempt)
	nextAttempt.Status, nextAttempt.Failure = workflowruntime.NodeCanceled, cloneWorkflowFailure(&reason)
	nextAttempt.FinishedAt, nextAttempt.UpdatedAt = at, at
	nextAttempt.Generation++
	nextNode := cloneWorkflowNode(node)
	nextNode.Status, nextNode.Wait, nextNode.Lease = workflowruntime.NodeCanceled, nil, nil
	nextNode.Generation++
	nextNode.UpdatedAt = at
	if err := nextWait.Validate(); err != nil {
		return workflowInvalid(err)
	}
	if err := nextAttempt.Validate(); err != nil {
		return workflowInvalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowInvalid(err)
	}
	if err := updateWorkflowWaitCAS(ctx, query, nextWait, wait.Generation); err != nil {
		return err
	}
	if err := updateWorkflowAttemptCAS(ctx, query, nextAttempt, attempt.Generation); err != nil {
		return err
	}
	if err := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); err != nil {
		return err
	}
	invocation := node.ID
	requests := []workflowruntime.AppendEventRequest{
		{RunID: node.ID.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: at, Attributes: map[string]string{"attempt_status": string(workflowruntime.NodeCanceled), "failure_code": reason.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{RunID: node.ID.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at, Attributes: map[string]string{"from_status": string(node.Status), "to_status": string(workflowruntime.NodeCanceled)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		{RunID: node.ID.RunID, Invocation: &invocation, Attempt: &attemptID, Type: "wait.canceled", OccurredAt: at, Attributes: map[string]string{"wait_id": string(wait.Ref.ID)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
	}
	for _, request := range requests {
		event, err := appendWorkflowEvent(ctx, query, request)
		if err != nil {
			return err
		}
		collector.events = append(collector.events, event)
	}
	collector.nodes = append(collector.nodes, nextNode)
	return nil
}

func ensureWorkflowCancellationIntent(ctx context.Context, query workflowSQL, runID workflowruntime.RunID, kind workflowruntime.CancellationIntentKind, attempt *workflowruntime.AttemptID, child workflowruntime.RunID, at time.Time) (workflowruntime.CancellationIntentSnapshot, error) {
	id, identityErr := workflowCancellationIntentID(kind, attempt, child)
	if identityErr != nil {
		return workflowruntime.CancellationIntentSnapshot{}, workflowInvalid(identityErr)
	}
	intent, loadErr := loadWorkflowCancellationIntent(ctx, query, id)
	if loadErr == nil {
		return intent, nil
	}
	if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		return workflowruntime.CancellationIntentSnapshot{}, loadErr
	}
	intent = workflowruntime.CancellationIntentSnapshot{ID: id, RunID: runID, Kind: kind, Attempt: cloneWorkflowAttemptID(attempt), ChildRunID: child, Status: workflowruntime.CancellationPending, Generation: 1, RequestedAt: at, UpdatedAt: at}
	if kind == workflowruntime.CancellationChildRun {
		intent.ChildPolicy = "request_cancel"
	}
	if validationErr := intent.Validate(); validationErr != nil {
		return workflowruntime.CancellationIntentSnapshot{}, workflowInvalid(validationErr)
	}
	encoded, encodeErr := encodeWorkflowJSON(intent)
	if encodeErr != nil {
		return workflowruntime.CancellationIntentSnapshot{}, encodeErr
	}
	if _, execErr := query.ExecContext(ctx, `INSERT INTO workflow_cancellation_intents(intent_id, run_id, kind, status, requested_at, generation, snapshot_json) VALUES (?, ?, ?, ?, ?, ?, ?)`, intent.ID, intent.RunID, intent.Kind, intent.Status, workflowTime(intent.RequestedAt), intent.Generation, encoded); execErr != nil {
		return workflowruntime.CancellationIntentSnapshot{}, execErr
	}
	return intent, nil
}

func workflowCancellationIntentID(kind workflowruntime.CancellationIntentKind, attempt *workflowruntime.AttemptID, child workflowruntime.RunID) (string, error) {
	if attempt != nil {
		encoded, err := workflowruntime.EncodeAttemptIdentity(*attempt)
		if err != nil {
			return "", err
		}
		return "cancel:" + string(kind) + ":" + encoded, nil
	}
	return "cancel:" + string(kind) + ":" + string(child), nil
}

func loadWorkflowCancellationIntent(ctx context.Context, query workflowSQL, id string) (workflowruntime.CancellationIntentSnapshot, error) {
	var encoded string
	if err := query.QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_cancellation_intents WHERE intent_id = ?`, id).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.CancellationIntentSnapshot{}, fmt.Errorf("%w: cancellation intent", workflowruntime.ErrNotFound)
		}
		return workflowruntime.CancellationIntentSnapshot{}, err
	}
	var intent workflowruntime.CancellationIntentSnapshot
	if err := decodeWorkflowJSON("cancellation intent", encoded, &intent); err != nil {
		return workflowruntime.CancellationIntentSnapshot{}, err
	}
	if err := intent.Validate(); err != nil {
		return workflowruntime.CancellationIntentSnapshot{}, workflowInvalid(err)
	}
	return intent, nil
}

func (s *WorkflowStateStore) ResolveCancellationIntent(ctx context.Context, request workflowruntime.ResolveCancellationIntentRequest) (workflowruntime.ResolveCancellationIntentResult, error) {
	if err := request.Validate(); err != nil {
		return workflowruntime.ResolveCancellationIntentResult{}, workflowInvalid(err)
	}
	var result workflowruntime.ResolveCancellationIntentResult
	writeErr := s.write(ctx, "resolve workflow cancellation intent", func(query workflowSQL) error {
		intent, loadErr := loadWorkflowCancellationIntent(ctx, query, request.IntentID)
		if loadErr != nil {
			return loadErr
		}
		if intent.Generation != request.ExpectedGeneration {
			return workflowCAS("cancellation intent", request.ExpectedGeneration, intent.Generation)
		}
		at := request.At.UTC()
		if intent.Status == workflowruntime.CancellationResolved {
			if at.Equal(intent.ResolvedAt) {
				result.Intent = intent
				return nil
			}
			return &workflowruntime.TransitionConflictError{Entity: "cancellation intent", ID: intent.ID, Status: string(intent.Status), Reason: "resolution is not exact replay"}
		}
		if at.Before(intent.UpdatedAt) {
			return workflowInvalid(errors.New("cancellation resolution time must not regress"))
		}
		if _, runErr := loadWorkflowRun(ctx, query, intent.RunID); runErr != nil {
			return runErr
		}
		switch intent.Kind {
		case workflowruntime.CancellationRunningAttempt:
			attempt, attemptErr := loadWorkflowAttempt(ctx, query, *intent.Attempt)
			if attemptErr != nil {
				return attemptErr
			}
			node, nodeErr := loadWorkflowNode(ctx, query, intent.Attempt.Invocation)
			if nodeErr != nil {
				return nodeErr
			}
			if attempt.Status == workflowruntime.NodeRunning {
				fromStatus := node.Status
				failure := workflowruntime.Failure{Code: "run_canceled", Message: "run cancellation stopped the active attempt"}
				nextAttempt := cloneWorkflowAttempt(attempt)
				nextAttempt.Status, nextAttempt.Failure = workflowruntime.NodeCanceled, &failure
				nextAttempt.FinishedAt, nextAttempt.UpdatedAt = at, at
				nextAttempt.Generation++
				nextNode := cloneWorkflowNode(node)
				nextNode.Status, nextNode.Lease = workflowruntime.NodeCanceled, nil
				nextNode.Generation++
				nextNode.UpdatedAt = at
				if validationErr := nextAttempt.Validate(); validationErr != nil {
					return workflowInvalid(validationErr)
				}
				if validationErr := nextNode.Validate(); validationErr != nil {
					return workflowInvalid(validationErr)
				}
				if updateErr := updateWorkflowAttemptCAS(ctx, query, nextAttempt, attempt.Generation); updateErr != nil {
					return updateErr
				}
				if updateErr := updateWorkflowNodeCAS(ctx, query, nextNode, node.Generation); updateErr != nil {
					return updateErr
				}
				invocation, attemptID := nextNode.ID, nextAttempt.ID
				for _, eventRequest := range []workflowruntime.AppendEventRequest{
					{RunID: intent.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: at, Attributes: map[string]string{"attempt_status": string(workflowruntime.NodeCanceled), "failure_code": failure.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
					{RunID: intent.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: at, Attributes: map[string]string{"from_status": string(fromStatus), "to_status": string(workflowruntime.NodeCanceled), "reason_code": failure.Code}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
				} {
					if _, eventErr := appendWorkflowEvent(ctx, query, eventRequest); eventErr != nil {
						return eventErr
					}
				}
				attempt, node = nextAttempt, nextNode
			}
			attemptCopy, nodeCopy := attempt, node
			result.Attempt, result.Node = &attemptCopy, &nodeCopy
		case workflowruntime.CancellationExternalOperation:
			operation, operationErr := loadWorkflowExternalOperation(ctx, query, *intent.Attempt)
			if operationErr != nil {
				return operationErr
			}
			if operation.Status == stepkind.ObservationPending {
				return workflowInvalid(errors.New("pending external operation cancellation remains unresolved"))
			}
		case workflowruntime.CancellationChildRun:
		}
		intent.Status = workflowruntime.CancellationResolved
		intent.ResolvedAt, intent.UpdatedAt = at, at
		intent.Generation++
		if validationErr := intent.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		encoded, encodeErr := encodeWorkflowJSON(intent)
		if encodeErr != nil {
			return encodeErr
		}
		updated, updateErr := query.ExecContext(ctx, `UPDATE workflow_cancellation_intents SET status = ?, generation = ?, snapshot_json = ? WHERE intent_id = ? AND generation = ?`, intent.Status, intent.Generation, encoded, intent.ID, request.ExpectedGeneration)
		if updateErr != nil {
			return updateErr
		}
		if rowErr := expectOneWorkflowRow(updated, "cancellation intent", request.ExpectedGeneration, request.ExpectedGeneration); rowErr != nil {
			return rowErr
		}
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: intent.RunID, Attempt: cloneWorkflowAttemptID(intent.Attempt), Type: workflowruntime.EventCancellationResolved, OccurredAt: at, Attributes: map[string]string{"intent_id": intent.ID, "kind": string(intent.Kind)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if eventErr != nil {
			return eventErr
		}
		eventCopy := event
		result.Intent, result.Event = intent, &eventCopy
		return nil
	})
	if writeErr != nil {
		return workflowruntime.ResolveCancellationIntentResult{}, writeErr
	}
	return result, nil
}

func (s *WorkflowStateStore) RecoverCancellationIntents(ctx context.Context, query workflowruntime.CancellationIntentQuery) ([]workflowruntime.CancellationIntentSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, workflowInvalid(errors.New("cancellation recovery limit must not be negative"))
	}
	rows, err := s.db.QueryContext(ctx, `SELECT snapshot_json FROM workflow_cancellation_intents WHERE status = 'pending'`)
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
		if query.RunID == "" || intent.RunID == query.RunID {
			result = append(result, intent)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].RequestedAt.Equal(result[j].RequestedAt) {
			return result[i].RequestedAt.Before(result[j].RequestedAt)
		}
		return result[i].ID < result[j].ID
	})
	if query.Limit > 0 && len(result) > query.Limit {
		result = result[:query.Limit]
	}
	return result, rows.Err()
}

func listWorkflowChildRuns(ctx context.Context, query workflowSQL, parent workflowruntime.RunID) ([]workflowruntime.ChildRunLink, error) {
	rows, err := query.QueryContext(ctx, `SELECT link_json FROM workflow_child_runs WHERE parent_run_id = ? ORDER BY child_run_id, node_id, iteration`, parent)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]workflowruntime.ChildRunLink, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var link workflowruntime.ChildRunLink
		if err := decodeWorkflowJSON("child run link", encoded, &link); err != nil {
			return nil, err
		}
		result = append(result, link)
	}
	return result, rows.Err()
}

func loadWorkflowRetryForInvocation(ctx context.Context, query workflowSQL, id workflowruntime.NodeInvocationID) (workflowruntime.RetryActivationSnapshot, error) {
	var encoded string
	if err := query.QueryRowContext(ctx, `SELECT snapshot_json FROM workflow_retry_activations WHERE run_id = ? AND node_id = ? AND iteration = ? AND status = 'scheduled'`, id.RunID, id.NodeID, id.Iteration).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowruntime.RetryActivationSnapshot{}, fmt.Errorf("%w: retry activation", workflowruntime.ErrNotFound)
		}
		return workflowruntime.RetryActivationSnapshot{}, err
	}
	var result workflowruntime.RetryActivationSnapshot
	if err := decodeWorkflowJSON("retry activation", encoded, &result); err != nil {
		return workflowruntime.RetryActivationSnapshot{}, err
	}
	return result, nil
}

func updateWorkflowRetryActivation(ctx context.Context, query workflowSQL, activation workflowruntime.RetryActivationSnapshot, expected uint64) error {
	encoded, err := encodeWorkflowJSON(activation)
	if err != nil {
		return err
	}
	result, err := query.ExecContext(ctx, `UPDATE workflow_retry_activations SET status = ?, generation = ?, snapshot_json = ? WHERE activation_id = ? AND generation = ?`, activation.Status, activation.Generation, encoded, activation.ID, expected)
	if err != nil {
		return err
	}
	return expectOneWorkflowRow(result, "retry activation", expected, expected)
}

func updateWorkflowFanOut(ctx context.Context, query workflowSQL, fanOut workflowruntime.FanOutSnapshot, expected uint64) error {
	encoded, err := encodeWorkflowJSON(fanOut)
	if err != nil {
		return err
	}
	result, err := query.ExecContext(ctx, `UPDATE workflow_fanouts SET status = ?, generation = ?, snapshot_json = ? WHERE run_id = ? AND node_id = ? AND generation = ?`, fanOut.Status, fanOut.Generation, encoded, fanOut.Parent.RunID, fanOut.Parent.NodeID, expected)
	if err != nil {
		return err
	}
	return expectOneWorkflowRow(result, "fan-out aggregate", expected, expected)
}

var _ = time.Time{}
