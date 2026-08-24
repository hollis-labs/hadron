package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	workflowCallResolvedEvent = "call.definition_resolved"
	workflowChildCreatedEvent = "child_run.created"
)

func (s *WorkflowHostStore) RecordCallResolution(ctx context.Context, request calladapter.RecordResolutionRequest) (calladapter.ResolutionRecord, calladapter.ResolutionOutcome, error) {
	record, cloneErr := cloneCallJSON(request.Record)
	if cloneErr != nil {
		return calladapter.ResolutionRecord{}, "", cloneErr
	}
	if validationErr := record.Validate(); validationErr != nil {
		return calladapter.ResolutionRecord{}, "", fmt.Errorf("invalid call resolution: %w", validationErr)
	}
	encoded, encodeErr := encodeWorkflowJSON(record)
	if encodeErr != nil {
		return calladapter.ResolutionRecord{}, "", encodeErr
	}
	var result calladapter.ResolutionRecord
	outcome := calladapter.ResolutionApplied
	writeErr := s.state.write(ctx, "record workflow call resolution", func(query workflowSQL) error {
		var prior, priorEvent string
		loadErr := query.QueryRowContext(ctx, `SELECT record_json, event_json FROM workflow_call_resolutions WHERE resolution_key = ?`, record.Key).Scan(&prior, &priorEvent)
		if loadErr == nil {
			if prior != encoded {
				return fmt.Errorf("%w: key %q", calladapter.ErrResolutionConflict, record.Key)
			}
			if decodeErr := decodeWorkflowJSON("call resolution", prior, &result); decodeErr != nil {
				return decodeErr
			}
			if validationErr := validateResolutionEvent(ctx, query, result, priorEvent); validationErr != nil {
				return validationErr
			}
			outcome = calladapter.ResolutionReplayed
			return nil
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return loadErr
		}
		invocation := workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(record.Invocation.RunID), NodeID: record.Invocation.NodeID, Iteration: record.Invocation.Iteration}
		if _, loadNodeErr := loadWorkflowNode(ctx, query, invocation); loadNodeErr != nil {
			return loadNodeErr
		}
		allowed, admissionErr := workflowControlAdmissionAllowed(ctx, query, invocation)
		if admissionErr != nil {
			return admissionErr
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences call resolution"))
		}
		now := s.now().UTC()
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{
			RunID: invocation.RunID, Invocation: &invocation, Type: workflowCallResolvedEvent,
			OccurredAt: now, Attributes: map[string]string{"resolution_key": record.Key, "requested_id": record.Requested.ID, "resolved_digest": record.Resolved.Digest, "input_digest": record.InputDigest},
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if eventErr != nil {
			return eventErr
		}
		eventJSON, eventEncodeErr := encodeWorkflowJSON(event)
		if eventEncodeErr != nil {
			return eventEncodeErr
		}
		if _, insertErr := query.ExecContext(ctx, `INSERT INTO workflow_call_resolutions(resolution_key, parent_run_id, node_id, iteration, record_json, event_json, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, record.Key, invocation.RunID, invocation.NodeID, invocation.Iteration, encoded, eventJSON, workflowTime(now)); insertErr != nil {
			if isSQLiteConstraint(insertErr) {
				return fmt.Errorf("%w: key %q", calladapter.ErrResolutionConflict, record.Key)
			}
			return insertErr
		}
		result = record
		return nil
	})
	if writeErr != nil {
		return calladapter.ResolutionRecord{}, "", writeErr
	}
	cloned, cloneErr := cloneCallJSON(result)
	return cloned, outcome, cloneErr
}

func (s *WorkflowHostStore) StartChildRun(ctx context.Context, request calladapter.ChildRunRequest) (calladapter.ChildRunResult, error) {
	request, cloneErr := cloneCallJSON(request)
	if cloneErr != nil {
		return calladapter.ChildRunResult{}, cloneErr
	}
	if validationErr := validateChildStartRequest(request); validationErr != nil {
		return calladapter.ChildRunResult{}, validationErr
	}
	requestJSON, encodeErr := encodeWorkflowJSON(request)
	if encodeErr != nil {
		return calladapter.ChildRunResult{}, encodeErr
	}
	var result calladapter.ChildRunResult
	writeErr := s.state.write(ctx, "start workflow child run", func(query workflowSQL) error {
		var priorRequest, priorResult string
		loadErr := query.QueryRowContext(ctx, `SELECT request_json, result_json FROM workflow_child_run_start_idempotency WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&priorRequest, &priorResult)
		if loadErr == nil {
			if priorRequest != requestJSON {
				return &workflowruntime.IdempotencyConflictError{Operation: "start workflow child run", Key: request.IdempotencyKey}
			}
			if decodeErr := decodeWorkflowJSON("child run start result", priorResult, &result); decodeErr != nil {
				return decodeErr
			}
			return validateChildStartReplay(ctx, query, request, result)
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return loadErr
		}
		parent := workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(request.Parent.RunID), NodeID: request.Parent.NodeID, Iteration: request.Parent.Iteration}
		parentNode, loadNodeErr := loadWorkflowNode(ctx, query, parent)
		if loadNodeErr != nil {
			return loadNodeErr
		}
		allowed, admissionErr := workflowControlAdmissionAllowed(ctx, query, parent)
		if admissionErr != nil {
			return admissionErr
		}
		if !allowed {
			return workflowInvalid(errors.New("pending terminal intent fences child run start"))
		}
		parentRun, loadRunErr := loadWorkflowRun(ctx, query, parent.RunID)
		if loadRunErr != nil {
			return loadRunErr
		}
		if !parentRun.Status.Active() || parentNode.Status != workflowruntime.NodeRunning {
			return &workflowruntime.TransitionConflictError{Entity: "call invocation", ID: parent.NodeID, Status: string(parentNode.Status), Reason: "new child requires an active parent run and running call invocation"}
		}
		unfinished, attemptErr := unfinishedWorkflowAttempt(ctx, query, parentNode)
		if attemptErr != nil {
			return attemptErr
		}
		if unfinished == nil {
			return workflowInvalid(errors.New("new child requires an unfinished parent call attempt"))
		}
		if planErr := ensureWorkflowPlan(ctx, query, request.Plan); planErr != nil {
			return planErr
		}
		now := s.now().UTC()
		inputRef, inputErr := insertWorkflowValueSet(ctx, query, workflowruntime.ValueOwner{Kind: "child-run-inputs", RunID: request.ChildRunID}, request.Inputs)
		if inputErr != nil {
			return inputErr
		}
		run := workflowruntime.RunSnapshot{ID: request.ChildRunID, Plan: request.Plan, Status: workflowruntime.RunPending, Inputs: &inputRef, Generation: 1, CreatedAt: now, UpdatedAt: now}
		if validationErr := run.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		if insertErr := insertWorkflowRun(ctx, query, run); insertErr != nil {
			return insertErr
		}
		link := workflowruntime.ChildRunLink{ParentRunID: parent.RunID, Invocation: parent, ChildRunID: request.ChildRunID, Policy: request.ParentClose, CreatedAt: now}
		if validationErr := link.Validate(); validationErr != nil {
			return workflowInvalid(validationErr)
		}
		linkJSON, linkEncodeErr := encodeWorkflowJSON(link)
		if linkEncodeErr != nil {
			return linkEncodeErr
		}
		if _, insertErr := query.ExecContext(ctx, `INSERT INTO workflow_child_runs(parent_run_id, node_id, iteration, child_run_id, policy, created_at, link_json) VALUES (?, ?, ?, ?, ?, ?, ?)`, link.ParentRunID, link.Invocation.NodeID, link.Invocation.Iteration, link.ChildRunID, link.Policy, workflowTime(link.CreatedAt), linkJSON); insertErr != nil {
			return insertErr
		}
		event, eventErr := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: parent.RunID, Invocation: &parent, Type: workflowChildCreatedEvent, OccurredAt: now, Attributes: map[string]string{"child_run_id": string(run.ID), "plan_digest": run.Plan.Digest, "parent_close": string(link.Policy)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
		if eventErr != nil {
			return eventErr
		}
		result = calladapter.ChildRunResult{Run: run, Link: link, EventsRef: fmt.Sprintf("workflow-events:%s:%d", parent.RunID, event.Sequence), Cancellation: calladapter.CancellationHandle{RunID: run.ID, Policy: link.Policy, Ref: "workflow-cancel:" + string(run.ID)}}
		resultJSON, resultEncodeErr := encodeWorkflowJSON(result)
		if resultEncodeErr != nil {
			return resultEncodeErr
		}
		if _, insertErr := query.ExecContext(ctx, `INSERT INTO workflow_child_run_start_idempotency(idempotency_key, child_run_id, request_json, result_json, created_at) VALUES (?, ?, ?, ?, ?)`, request.IdempotencyKey, request.ChildRunID, requestJSON, resultJSON, workflowTime(now)); insertErr != nil {
			if isSQLiteConstraint(insertErr) {
				return &workflowruntime.IdempotencyConflictError{Operation: "start workflow child run", Key: request.IdempotencyKey}
			}
			return insertErr
		}
		return nil
	})
	if writeErr != nil {
		return calladapter.ChildRunResult{}, writeErr
	}
	return cloneCallJSON(result)
}

func validateChildStartRequest(request calladapter.ChildRunRequest) error {
	if request.Parent.RunID == "" || request.Parent.NodeID == "" || request.ChildRunID == "" || request.IdempotencyKey == "" {
		return errors.New("child run request requires parent, child, and idempotency identities")
	}
	if err := request.Parent.Validate(); err != nil {
		return err
	}
	if workflowruntime.RunID(request.Parent.RunID) == request.ChildRunID {
		return errors.New("child run must differ from parent run")
	}
	if err := request.Plan.Validate(); err != nil {
		return err
	}
	inputDigest, err := values.DigestValueSet(request.Inputs)
	if err != nil {
		return err
	}
	resolution := calladapter.ResolutionRecord{Key: "child-run-validation", Invocation: request.Parent, Requested: request.Definition.Definition, Resolved: request.Definition.Definition, InputDigest: inputDigest, Lineage: request.Lineage}
	if err := resolution.Validate(); err != nil {
		return fmt.Errorf("child resolution: %w", err)
	}
	definition, resolvedGraph := request.Definition.Definition, request.Definition.Graph
	if err := resolvedGraph.ValidateEnums(); err != nil {
		return fmt.Errorf("child resolved graph: %w", err)
	}
	if definition.ID != resolvedGraph.ID || definition.Version != resolvedGraph.Version ||
		definition.Digest != resolvedGraph.Digest || definition.Provenance == nil ||
		!reflect.DeepEqual(*definition.Provenance, resolvedGraph.Provenance) {
		return errors.New("child definition and resolved graph identities must match")
	}
	if request.Plan.ID != resolvedGraph.ID || request.Plan.Version != resolvedGraph.Version || request.Plan.Digest != resolvedGraph.Digest {
		return errors.New("child resolved graph and plan identities must match")
	}
	if !request.ParentClose.Valid() {
		return errors.New("child run parent-close policy is invalid")
	}
	if err := values.ValidatePersistableSet(request.Inputs); err != nil {
		return err
	}
	return nil
}

func validateChildStartReplay(ctx context.Context, query workflowSQL, request calladapter.ChildRunRequest, result calladapter.ChildRunResult) error {
	invocation := workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(request.Parent.RunID), NodeID: request.Parent.NodeID, Iteration: request.Parent.Iteration}
	expectedLink := workflowruntime.ChildRunLink{ParentRunID: invocation.RunID, Invocation: invocation, ChildRunID: request.ChildRunID, Policy: request.ParentClose, CreatedAt: result.Run.CreatedAt}
	if result.Run.ID != request.ChildRunID || result.Run.Plan != request.Plan || !reflect.DeepEqual(result.Link, expectedLink) {
		return workflowInvalid(errors.New("child run replay result differs from request"))
	}
	current, err := loadWorkflowRun(ctx, query, request.ChildRunID)
	if err != nil {
		return err
	}
	if current.ID != result.Run.ID || current.Plan != result.Run.Plan || !current.CreatedAt.Equal(result.Run.CreatedAt) || !reflect.DeepEqual(current.Inputs, result.Run.Inputs) {
		return workflowInvalid(errors.New("child run replay no longer matches durable run"))
	}
	if current.Inputs == nil {
		return workflowInvalid(errors.New("child run replay is missing typed inputs"))
	}
	storedInputs, err := loadWorkflowValues(ctx, query, *current.Inputs)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(storedInputs, request.Inputs) {
		return workflowInvalid(errors.New("child run replay input values differ from request"))
	}
	links, err := listWorkflowChildRuns(ctx, query, workflowruntime.RunID(request.Parent.RunID))
	if err != nil {
		return err
	}
	linkFound := false
	for _, link := range links {
		if reflect.DeepEqual(link, result.Link) {
			linkFound = true
			break
		}
	}
	if !linkFound {
		return workflowInvalid(errors.New("child run replay is missing durable link"))
	}
	return validateChildCreationEvent(ctx, query, result)
}

func validateResolutionEvent(ctx context.Context, query workflowSQL, record calladapter.ResolutionRecord, expectedJSON string) error {
	var expected workflowruntime.Event
	if err := decodeWorkflowJSON("call resolution event", expectedJSON, &expected); err != nil {
		return err
	}
	actual, err := scanWorkflowEvent(query.QueryRowContext(ctx, workflowEventSelect+` WHERE run_id = ? AND sequence = ?`, record.Invocation.RunID, expected.Sequence))
	if err != nil {
		return err
	}
	actualJSON, err := encodeWorkflowJSON(actual)
	if err != nil {
		return err
	}
	if actualJSON != expectedJSON || actual.Type != workflowCallResolvedEvent || actual.Invocation == nil || actual.Invocation.NodeID != record.Invocation.NodeID || actual.Attributes["resolution_key"] != record.Key || actual.Attributes["resolved_digest"] != record.Resolved.Digest {
		return workflowInvalid(errors.New("call resolution event differs from immutable journal"))
	}
	return nil
}

func validateChildCreationEvent(ctx context.Context, query workflowSQL, result calladapter.ChildRunResult) error {
	prefix := "workflow-events:" + string(result.Link.ParentRunID) + ":"
	if !strings.HasPrefix(result.EventsRef, prefix) {
		return workflowInvalid(errors.New("child events reference is invalid"))
	}
	sequence, err := strconv.ParseUint(strings.TrimPrefix(result.EventsRef, prefix), 10, 64)
	if err != nil || sequence == 0 {
		return workflowInvalid(errors.New("child events reference sequence is invalid"))
	}
	event, err := scanWorkflowEvent(query.QueryRowContext(ctx, workflowEventSelect+` WHERE run_id = ? AND sequence = ?`, result.Link.ParentRunID, sequence))
	if err != nil {
		return err
	}
	expectedAttributes := map[string]string{
		"child_run_id": string(result.Run.ID),
		"parent_close": string(result.Link.Policy),
		"plan_digest":  result.Run.Plan.Digest,
	}
	if event.RunID != result.Link.ParentRunID || event.Type != workflowChildCreatedEvent ||
		event.Invocation == nil || *event.Invocation != result.Link.Invocation ||
		!event.OccurredAt.Equal(result.Run.CreatedAt) ||
		!reflect.DeepEqual(event.Attributes, expectedAttributes) ||
		event.Redaction != values.RedactionPrivate || event.Retention != values.RetentionRun {
		return workflowInvalid(errors.New("child creation event differs from durable result"))
	}
	expectedCancellation := calladapter.CancellationHandle{RunID: result.Run.ID, Policy: result.Link.Policy, Ref: "workflow-cancel:" + string(result.Run.ID)}
	if result.Cancellation != expectedCancellation {
		return workflowInvalid(errors.New("child cancellation handle differs from durable identity"))
	}
	return nil
}

// RecoverPendingChildRuns exposes deterministic, pinned requests for W05-T03's
// child graph materializer. The request already contains the resolved graph;
// no movable definition is re-resolved during recovery.
func (s *WorkflowHostStore) RecoverPendingChildRuns(ctx context.Context, limit int) ([]calladapter.ChildRunRequest, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, workflowInvalid(errors.New("negative child recovery limit"))
	}
	statement := `SELECT i.request_json FROM workflow_child_run_start_idempotency i JOIN workflow_runs r ON r.run_id = i.child_run_id WHERE r.status = ? ORDER BY i.created_at, i.child_run_id`
	args := []any{workflowruntime.RunPending}
	if limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.state.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := make([]calladapter.ChildRunRequest, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var request calladapter.ChildRunRequest
		if err := decodeWorkflowJSON("pending child run request", encoded, &request); err != nil {
			return nil, err
		}
		if err := validateChildStartRequest(request); err != nil {
			return nil, workflowInvalid(err)
		}
		result = append(result, request)
	}
	return result, rows.Err()
}

// LoadChildRunRequest returns the immutable pinned request that atomically
// created childRunID. It is the authoritative graph source for cancellation
// planning and never re-resolves the child definition.
func (s *WorkflowHostStore) LoadChildRunRequest(ctx context.Context, childRunID workflowruntime.RunID) (calladapter.ChildRunRequest, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return calladapter.ChildRunRequest{}, err
	}
	if childRunID == "" {
		return calladapter.ChildRunRequest{}, workflowInvalid(errors.New("child run id is required"))
	}
	var encoded string
	if err := s.state.db.QueryRowContext(ctx, `SELECT request_json FROM workflow_child_run_start_idempotency WHERE child_run_id = ?`, childRunID).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return calladapter.ChildRunRequest{}, fmt.Errorf("%w: child run request", workflowruntime.ErrNotFound)
		}
		return calladapter.ChildRunRequest{}, err
	}
	var request calladapter.ChildRunRequest
	if err := decodeWorkflowJSON("child run request", encoded, &request); err != nil {
		return calladapter.ChildRunRequest{}, err
	}
	if err := validateChildStartRequest(request); err != nil || request.ChildRunID != childRunID {
		if err == nil {
			err = errors.New("stored child run request belongs to another run")
		}
		return calladapter.ChildRunRequest{}, workflowInvalid(err)
	}
	return cloneCallJSON(request)
}

func cloneCallJSON[T any](input T) (T, error) {
	var output T
	encoded, err := encodeWorkflowJSON(input)
	if err != nil {
		return output, err
	}
	if err := decodeWorkflowJSON("workflow call contract", encoded, &output); err != nil {
		return output, err
	}
	return output, nil
}

var _ calladapter.ResolutionStore = (*WorkflowHostStore)(nil)
var _ calladapter.ChildRunExecutor = (*WorkflowHostStore)(nil)
