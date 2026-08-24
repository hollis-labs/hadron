package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

var _ workflowruntime.WaitStore = (*WorkflowStateStore)(nil)

func (s *WorkflowStateStore) SuspendNodeWait(ctx context.Context, request workflowruntime.SuspendNodeWaitRequest) (workflowruntime.SuspendWaitResult, error) {
	request.At = request.At.UTC()
	request.Wait.CreatedAt, request.Wait.UpdatedAt = request.At, request.At
	if !request.Wait.Deadline.IsZero() {
		request.Wait.Deadline = request.Wait.Deadline.UTC()
	}
	if err := request.Validate(); err != nil {
		return workflowruntime.SuspendWaitResult{}, workflowInvalid(err)
	}
	nextWait := request.Wait
	nextWait.Generation = 1
	if err := nextWait.Validate(); err != nil {
		return workflowruntime.SuspendWaitResult{}, workflowInvalid(err)
	}
	if !nextWait.Deadline.IsZero() && nextWait.Deadline.Before(request.At) {
		return workflowruntime.SuspendWaitResult{}, workflowInvalid(errors.New("wait deadline must not precede suspension"))
	}
	requestJSON, encodeErr := encodeWorkflowJSON(request)
	if encodeErr != nil {
		return workflowruntime.SuspendWaitResult{}, encodeErr
	}
	var result workflowruntime.SuspendWaitResult
	writeErr := s.write(ctx, "suspend workflow wait", func(query workflowSQL) error {
		var priorRequest, priorResult string
		loadErr := query.QueryRowContext(ctx, `SELECT request_json, result_json FROM workflow_wait_suspend_idempotency WHERE wait_id = ?`, nextWait.Ref.ID).Scan(&priorRequest, &priorResult)
		if loadErr == nil {
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("suspend wait", string(nextWait.Ref.ID))
			}
			if decodeErr := decodeWorkflowJSON("wait suspend result", priorResult, &result); decodeErr != nil {
				return decodeErr
			}
			result.Outcome = workflowruntime.IdempotencyReplayed
			return nil
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return fmt.Errorf("load wait suspension replay: %w", loadErr)
		}
		currentNode, loadErr := loadWorkflowNode(ctx, query, nextWait.Invocation)
		if loadErr != nil {
			return loadErr
		}
		if currentNode.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("suspend wait node", request.ExpectedNodeGeneration, currentNode.Generation)
		}
		if currentNode.Status != workflowruntime.NodeRunning || currentNode.Wait != nil {
			return workflowInvalid(errors.New("suspension requires a running node without a wait"))
		}
		if err := validateWorkflowLifecycleClaim(currentNode, &request.Claim, request.At); err != nil {
			return err
		}
		attempt, attemptErr := unfinishedWorkflowAttempt(ctx, query, currentNode)
		if attemptErr != nil {
			return attemptErr
		}
		if attempt == nil || attempt.ID.Number != currentNode.LatestAttempt {
			return workflowAttemptConflict(currentNode.ID, currentNode.LatestAttempt, "suspension requires the matching unfinished attempt")
		}
		if attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("suspend wait attempt", request.ExpectedAttemptGeneration, attempt.Generation)
		}
		if request.At.Before(currentNode.UpdatedAt) || request.At.Before(attempt.UpdatedAt) {
			return workflowInvalid(errors.New("suspend time must not regress persisted state"))
		}
		nextNode := cloneWorkflowNode(currentNode)
		nextNode.Status = workflowruntime.NodeWaiting
		nextNode.Wait = &workflowruntime.WaitRef{ID: nextWait.Ref.ID}
		nextNode.Lease = nil
		nextNode.Generation++
		nextNode.UpdatedAt = request.At
		if err := nextNode.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := insertWorkflowWait(ctx, query, nextWait); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, currentNode.Generation); err != nil {
			return err
		}
		invocation, attemptID := nextNode.ID, attempt.ID
		events := make([]workflowruntime.Event, 0, 2)
		for _, eventRequest := range []workflowruntime.AppendEventRequest{
			{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventWaitSuspended, OccurredAt: request.At, Attributes: workflowWaitEventAttributes(nextWait, "", string(workflowruntime.WaitOpen)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
			{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.At, Attributes: workflowTransitionAttributes("node", string(currentNode.Status), string(nextNode.Status)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		} {
			event, eventErr := appendWorkflowEvent(ctx, query, eventRequest)
			if eventErr != nil {
				return eventErr
			}
			events = append(events, event)
		}
		result = workflowruntime.SuspendWaitResult{Outcome: workflowruntime.IdempotencyApplied, Wait: nextWait, Node: nextNode, Attempt: *attempt, Events: events}
		resultJSON, resultErr := encodeWorkflowJSON(result)
		if resultErr != nil {
			return resultErr
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_wait_suspend_idempotency(wait_id, request_json, result_json) VALUES (?, ?, ?)`, nextWait.Ref.ID, requestJSON, resultJSON); err != nil {
			return fmt.Errorf("record wait suspension replay: %w", err)
		}
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) ResumeNodeWait(ctx context.Context, request workflowruntime.ResumeNodeWaitRequest) (workflowruntime.ResumeWaitResult, error) {
	request.ReceivedAt = request.ReceivedAt.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.ResumeWaitResult{}, workflowInvalid(err)
	}
	requestJSON, encodeErr := canonicalAtomicResumeRequest(request)
	if encodeErr != nil {
		return workflowruntime.ResumeWaitResult{}, encodeErr
	}
	var result workflowruntime.ResumeWaitResult
	writeErr := s.write(ctx, "resume workflow node wait", func(query workflowSQL) error {
		if request.IdempotencyKey != "" {
			priorRequest, priorResult, found, replayErr := loadWorkflowIdempotency(ctx, query, "workflow_wait_resume_idempotency", request.IdempotencyKey)
			if replayErr != nil {
				return replayErr
			}
			if found {
				if priorRequest != requestJSON {
					return workflowIdempotencyConflict("resume node wait", request.IdempotencyKey)
				}
				if err := decodeWorkflowJSON("atomic wait resume result", priorResult, &result); err != nil {
					return err
				}
				result.Outcome = workflowruntime.ResumeReplayed
				return nil
			}
		}
		currentWait, loadErr := loadWorkflowWait(ctx, query, request.WaitID)
		if loadErr != nil {
			return loadErr
		}
		tokenMatches := (currentWait.ResumeTokenDigest == "" && request.PresentedTokenDigest == "") || workflowwait.EqualTokenDigest(currentWait.ResumeTokenDigest, request.PresentedTokenDigest)
		if !tokenMatches {
			return workflowInvalid(workflowruntime.ErrInvalidResumeToken)
		}
		if currentWait.Correlation != request.Correlation || currentWait.WakeSource != request.WakeSource {
			return workflowInvalid(errors.New("resume does not match immutable wait correlation or wake source"))
		}
		if currentWait.Status != workflowruntime.WaitOpen {
			if currentWait.Status == workflowruntime.WaitResumed {
				if request.IdempotencyKey != "" {
					return workflowIdempotencyConflict("resume node wait", request.IdempotencyKey)
				}
				var encoded string
				if scanErr := query.QueryRowContext(ctx, `SELECT result_json FROM workflow_wait_resume_results WHERE wait_id = ?`, currentWait.Ref.ID).Scan(&encoded); scanErr != nil {
					return fmt.Errorf("load accepted wait resume: %w", scanErr)
				}
				if decodeErr := decodeWorkflowJSON("accepted wait resume", encoded, &result); decodeErr != nil {
					return decodeErr
				}
				result.Outcome = workflowruntime.ResumeAlreadyResumed
				return nil
			}
			result.Wait = currentWait
			return &workflowruntime.WaitClosedError{WaitID: currentWait.Ref.ID, Status: currentWait.Status, ResolvedAt: currentWait.ResolvedAt}
		}
		if !currentWait.Deadline.IsZero() && !request.ReceivedAt.Before(currentWait.Deadline) {
			return &workflowruntime.WaitClosedError{WaitID: currentWait.Ref.ID, Status: workflowruntime.WaitTimedOut, ResolvedAt: currentWait.Deadline}
		}
		if currentWait.Generation != request.ExpectedWaitGeneration {
			return workflowCAS("resume wait", request.ExpectedWaitGeneration, currentWait.Generation)
		}
		currentNode, nodeErr := loadWorkflowNode(ctx, query, currentWait.Invocation)
		if nodeErr != nil {
			return nodeErr
		}
		if currentNode.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("resume wait node", request.ExpectedNodeGeneration, currentNode.Generation)
		}
		if currentNode.Status != workflowruntime.NodeWaiting || currentNode.Wait == nil || currentNode.Wait.ID != currentWait.Ref.ID {
			return workflowInvalid(errors.New("open wait resume requires its matching waiting node"))
		}
		attempt, attemptErr := unfinishedWorkflowAttempt(ctx, query, currentNode)
		if attemptErr != nil {
			return attemptErr
		}
		if attempt == nil {
			return workflowAttemptConflict(currentNode.ID, currentNode.LatestAttempt, "resume requires matching unfinished attempt")
		}
		if attempt.Generation != request.ExpectedAttemptGeneration {
			return workflowCAS("resume wait attempt", request.ExpectedAttemptGeneration, attempt.Generation)
		}
		if request.ReceivedAt.Before(currentWait.UpdatedAt) || request.ReceivedAt.Before(currentNode.UpdatedAt) || request.ReceivedAt.Before(attempt.UpdatedAt) {
			return workflowInvalid(errors.New("resume time must not regress persisted state"))
		}
		if err := values.ValidateValueSchema(currentWait.ResumeSchema.Schema, request.Payload); err != nil {
			return workflowInvalid(err)
		}
		set := values.ValueSet{workflowruntime.ResumeValueName: request.Payload}
		invocation, attemptID := currentNode.ID, attempt.ID
		ref, valueErr := insertWorkflowValueSet(ctx, query, workflowruntime.ValueOwner{Kind: "wait_resume", RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID}, set)
		if valueErr != nil {
			return valueErr
		}
		resolution := &workflowwait.Resolution{Source: request.WakeSource, Responder: request.Responder, PayloadDigest: ref.Digest, IdempotencyKey: request.IdempotencyKey, ResolvedAt: request.ReceivedAt}
		nextWait := currentWait
		nextWait.Status = workflowruntime.WaitResumed
		nextWait.ResumeValues = &ref
		nextWait.Resolution = resolution
		nextWait.ResolvedAt = request.ReceivedAt
		nextWait.UpdatedAt = request.ReceivedAt
		nextWait.Generation++
		nextNode := cloneWorkflowNode(currentNode)
		nextNode.Status = workflowruntime.NodeReady
		nextNode.Wait = nil
		nextNode.Lease = nil
		nextNode.UpdatedAt = request.ReceivedAt
		nextNode.Generation++
		if err := nextWait.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := nextNode.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowWaitCAS(ctx, query, nextWait, currentWait.Generation); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, currentNode.Generation); err != nil {
			return err
		}
		events := make([]workflowruntime.Event, 0, 2)
		for _, eventRequest := range []workflowruntime.AppendEventRequest{
			{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventWaitResumed, OccurredAt: request.ReceivedAt, Attributes: workflowWaitEventAttributes(nextWait, string(workflowruntime.WaitOpen), string(workflowruntime.WaitResumed)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
			{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.ReceivedAt, Attributes: workflowTransitionAttributes("node", string(currentNode.Status), string(nextNode.Status)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		} {
			event, eventErr := appendWorkflowEvent(ctx, query, eventRequest)
			if eventErr != nil {
				return eventErr
			}
			events = append(events, event)
		}
		result = workflowruntime.ResumeWaitResult{Outcome: workflowruntime.ResumeApplied, Wait: nextWait, Node: nextNode, Attempt: *attempt, Values: ref, Events: events}
		resultJSON, resultErr := encodeWorkflowJSON(result)
		if resultErr != nil {
			return resultErr
		}
		if _, err := query.ExecContext(ctx, `INSERT INTO workflow_wait_resume_results(wait_id, result_json) VALUES (?, ?)`, currentWait.Ref.ID, resultJSON); err != nil {
			return fmt.Errorf("record accepted wait resume: %w", err)
		}
		if request.IdempotencyKey != "" {
			if _, err := query.ExecContext(ctx, `INSERT INTO workflow_wait_resume_idempotency(idempotency_key, request_json, result_json) VALUES (?, ?, ?)`, request.IdempotencyKey, requestJSON, resultJSON); err != nil {
				return fmt.Errorf("record wait resume idempotency: %w", err)
			}
		}
		return nil
	})
	return result, writeErr
}

func (s *WorkflowStateStore) TimeoutWait(ctx context.Context, request workflowruntime.TimeoutWaitRequest) (workflowruntime.WaitTimeoutResult, error) {
	request.Deadline, request.Now = request.Deadline.UTC(), request.Now.UTC()
	if err := request.Validate(); err != nil {
		return workflowruntime.WaitTimeoutResult{}, workflowInvalid(err)
	}
	if request.Now.Before(request.Deadline) {
		return workflowruntime.WaitTimeoutResult{}, &workflowruntime.WaitTimeoutNotDueError{Now: request.Now, Deadline: request.Deadline}
	}
	requestJSON, encodeErr := encodeWorkflowJSON(request)
	if encodeErr != nil {
		return workflowruntime.WaitTimeoutResult{}, encodeErr
	}
	var result workflowruntime.WaitTimeoutResult
	writeErr := s.write(ctx, "timeout workflow wait", func(query workflowSQL) error {
		var priorRequest, priorResult string
		loadErr := query.QueryRowContext(ctx, `SELECT request_json, result_json FROM workflow_wait_timeout_idempotency WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&priorRequest, &priorResult)
		if loadErr == nil {
			if priorRequest != requestJSON {
				return workflowIdempotencyConflict("timeout wait", request.IdempotencyKey)
			}
			if decodeErr := decodeWorkflowJSON("wait timeout result", priorResult, &result); decodeErr != nil {
				return decodeErr
			}
			result.Replayed = true
			return nil
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return fmt.Errorf("load wait timeout replay: %w", loadErr)
		}
		currentWait, waitErr := loadWorkflowWait(ctx, query, request.WaitID)
		if waitErr != nil {
			return waitErr
		}
		if currentWait.Deadline.IsZero() || !currentWait.Deadline.Equal(request.Deadline) {
			return workflowInvalid(errors.New("timeout deadline must exactly match persisted wait deadline"))
		}
		currentNode, nodeErr := loadWorkflowNode(ctx, query, currentWait.Invocation)
		if nodeErr != nil {
			return nodeErr
		}
		if currentWait.Status != workflowruntime.WaitOpen {
			result = workflowruntime.WaitTimeoutResult{Applied: false, Wait: currentWait, Node: currentNode}
			return storeWorkflowTimeoutReplay(ctx, query, request.IdempotencyKey, requestJSON, result)
		}
		if currentWait.Generation != request.ExpectedWaitGeneration {
			return workflowCAS("wait timeout", request.ExpectedWaitGeneration, currentWait.Generation)
		}
		if currentNode.Generation != request.ExpectedNodeGeneration {
			return workflowCAS("wait timeout node", request.ExpectedNodeGeneration, currentNode.Generation)
		}
		if currentNode.Status != workflowruntime.NodeWaiting || currentNode.Wait == nil || currentNode.Wait.ID != currentWait.Ref.ID {
			return workflowInvalid(errors.New("open wait timeout requires its matching waiting node"))
		}
		attempt, attemptErr := unfinishedWorkflowAttempt(ctx, query, currentNode)
		if attemptErr != nil {
			return attemptErr
		}
		if attempt == nil {
			return workflowAttemptConflict(currentNode.ID, currentNode.LatestAttempt, "wait timeout requires matching unfinished attempt")
		}
		if request.Now.Before(currentWait.UpdatedAt) || request.Now.Before(currentNode.UpdatedAt) || request.Now.Before(attempt.UpdatedAt) {
			return workflowInvalid(errors.New("wait timeout time must not regress persisted state"))
		}
		failure := workflowruntime.WaitTimeoutFailure(currentWait.Deadline)
		nextWait := currentWait
		nextWait.Status = workflowruntime.WaitTimedOut
		nextWait.Resolution = &workflowwait.Resolution{Source: workflowwait.WakeTimer, Responder: workflowwait.Responder{Kind: "system", Reference: "wait-timeout"}, ResolvedAt: request.Now}
		nextWait.ResolvedAt = request.Now
		nextWait.UpdatedAt = request.Now
		nextWait.Generation++
		nextNode := cloneWorkflowNode(currentNode)
		nextNode.Status = workflowruntime.NodeTimedOut
		nextNode.Blocked = nil
		nextNode.Wait = nil
		nextNode.Lease = nil
		nextNode.Outputs = nil
		nextNode.UpdatedAt = request.Now
		nextNode.Generation++
		nextAttempt := cloneWorkflowAttempt(*attempt)
		nextAttempt.Status = workflowruntime.NodeTimedOut
		nextAttempt.Outputs = nil
		nextAttempt.Failure = &failure
		nextAttempt.FinishedAt = request.Now
		nextAttempt.UpdatedAt = request.Now
		nextAttempt.Generation++
		if err := nextWait.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := nextNode.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := nextAttempt.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := updateWorkflowWaitCAS(ctx, query, nextWait, currentWait.Generation); err != nil {
			return err
		}
		if err := updateWorkflowNodeCAS(ctx, query, nextNode, currentNode.Generation); err != nil {
			return err
		}
		if err := updateWorkflowAttemptCAS(ctx, query, nextAttempt, attempt.Generation); err != nil {
			return err
		}
		invocation, attemptID := nextNode.ID, nextAttempt.ID
		deadline := currentWait.Deadline.Format(time.RFC3339Nano)
		waitID := string(currentWait.Ref.ID)
		attemptAttrs := workflowAttemptAttributes("node_attempt", string(attempt.Status), string(nextAttempt.Status), nextAttempt)
		attemptAttrs["attempt_status"] = string(nextAttempt.Status)
		attemptAttrs["failure_code"] = failure.Code
		attemptAttrs["wait_id"] = waitID
		attemptAttrs["deadline"] = deadline
		nodeAttrs := workflowAttemptAttributes("node", string(currentNode.Status), string(nextNode.Status), nextAttempt)
		nodeAttrs["failure_code"] = failure.Code
		nodeAttrs["wait_id"] = waitID
		nodeAttrs["deadline"] = deadline
		eventRequests := []workflowruntime.AppendEventRequest{
			{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: request.Now, Attributes: attemptAttrs, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
			{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventNodeStatusChanged, OccurredAt: request.Now, Attributes: nodeAttrs, Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
			{RunID: invocation.RunID, Invocation: &invocation, Attempt: &attemptID, Type: workflowruntime.EventWaitTimedOut, OccurredAt: request.Now, Attributes: workflowWaitEventAttributes(nextWait, string(workflowruntime.WaitOpen), string(workflowruntime.WaitTimedOut)), Redaction: values.RedactionPrivate, Retention: values.RetentionRun},
		}
		events := make([]workflowruntime.Event, 0, 3)
		for _, eventRequest := range eventRequests {
			event, eventErr := appendWorkflowEvent(ctx, query, eventRequest)
			if eventErr != nil {
				return eventErr
			}
			events = append(events, event)
		}
		result = workflowruntime.WaitTimeoutResult{Applied: true, Wait: nextWait, Node: nextNode, Attempt: nextAttempt, Events: events}
		return storeWorkflowTimeoutReplay(ctx, query, request.IdempotencyKey, requestJSON, result)
	})
	return result, writeErr
}

func (s *WorkflowStateStore) RecoverOpenWaits(ctx context.Context, query workflowruntime.OpenWaitQuery) ([]workflowruntime.WaitSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if query.Limit < 0 {
		return nil, workflowInvalid(errors.New("open wait recovery limit must not be negative"))
	}
	statement := workflowWaitSelect + ` WHERE status = 'open'`
	arguments := []any{}
	if query.RunID != "" {
		statement += ` AND run_id = ?`
		arguments = append(arguments, query.RunID)
	}
	statement += ` ORDER BY CASE WHEN deadline IS NULL THEN 1 ELSE 0 END, deadline ASC, created_at ASC, wait_id ASC`
	if query.Limit > 0 {
		statement += ` LIMIT ?`
		arguments = append(arguments, query.Limit)
	}
	rows, err := s.db.QueryContext(ctx, statement, arguments...)
	if err != nil {
		return nil, fmt.Errorf("recover open waits: %w", err)
	}
	defer closeRows(rows)
	result := make([]workflowruntime.WaitSnapshot, 0)
	for rows.Next() {
		snapshot, err := scanWorkflowWait(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func canonicalAtomicResumeRequest(request workflowruntime.ResumeNodeWaitRequest) (string, error) {
	request.ExpectedWaitGeneration, request.ExpectedNodeGeneration, request.ExpectedAttemptGeneration = 0, 0, 0
	request.ReceivedAt = time.Time{}
	return encodeWorkflowJSON(request)
}

func insertWorkflowValueSet(ctx context.Context, query workflowSQL, owner workflowruntime.ValueOwner, set values.ValueSet) (values.ValueSetRef, error) {
	if err := owner.Validate(); err != nil {
		return values.ValueSetRef{}, workflowInvalid(err)
	}
	if err := values.ValidatePersistableSet(set); err != nil {
		return values.ValueSetRef{}, workflowInvalid(err)
	}
	ownerJSON, err := encodeWorkflowJSON(owner)
	if err != nil {
		return values.ValueSetRef{}, err
	}
	setJSON, err := encodeWorkflowJSON(set)
	if err != nil {
		return values.ValueSetRef{}, err
	}
	digest, err := values.DigestValueSet(set)
	if err != nil {
		return values.ValueSetRef{}, workflowInvalid(err)
	}
	inserted, err := query.ExecContext(ctx, `INSERT INTO workflow_value_sets(digest, owner_json, values_json) VALUES (?, ?, ?)`, digest, ownerJSON, setJSON)
	if err != nil {
		return values.ValueSetRef{}, fmt.Errorf("insert workflow value set: %w", err)
	}
	sequence, err := inserted.LastInsertId()
	if err != nil {
		return values.ValueSetRef{}, err
	}
	ref := values.ValueSetRef{ID: workflowValueID(sequence), Digest: digest}
	return ref, ref.Validate()
}

func storeWorkflowTimeoutReplay(ctx context.Context, query workflowSQL, key, requestJSON string, result workflowruntime.WaitTimeoutResult) error {
	resultJSON, err := encodeWorkflowJSON(result)
	if err != nil {
		return err
	}
	if _, err := query.ExecContext(ctx, `INSERT INTO workflow_wait_timeout_idempotency(idempotency_key, request_json, result_json) VALUES (?, ?, ?)`, key, requestJSON, resultJSON); err != nil {
		return fmt.Errorf("record wait timeout replay: %w", err)
	}
	return nil
}

func workflowWaitEventAttributes(snapshot workflowruntime.WaitSnapshot, from, to string) map[string]string {
	attributes := map[string]string{"entity": "wait", "wait_id": string(snapshot.Ref.ID), "kind": string(snapshot.Kind), "wake_source": string(snapshot.WakeSource), "to_status": to}
	if from != "" {
		attributes["from_status"] = from
	}
	if !snapshot.Deadline.IsZero() {
		attributes["deadline"] = snapshot.Deadline.UTC().Format(time.RFC3339Nano)
	}
	if snapshot.Resolution != nil {
		attributes["responder_kind"] = snapshot.Resolution.Responder.Kind
		attributes["responder_reference"] = snapshot.Resolution.Responder.Reference
	}
	return attributes
}
