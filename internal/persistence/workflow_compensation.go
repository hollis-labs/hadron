package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.CompensationStore = (*WorkflowStateStore)(nil)

func workflowRunAllowsCompensationExecution(ctx context.Context, query workflowSQL, run workflowruntime.RunSnapshot, node workflowruntime.NodeInvocationSnapshot) (bool, error) {
	if run.Status.Active() {
		return true, nil
	}
	if node.Phase != workflowruntime.InvocationCompensation {
		return false, nil
	}
	ledger, err := loadWorkflowCompensationLedger(ctx, query, run.ID)
	if errors.Is(err, workflowruntime.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return ledger.Trigger == graph.CompensationManual && ledger.Status != workflowruntime.CompensationTerminal, nil
}

func loadWorkflowCompensationLedger(ctx context.Context, query workflowSQL, runID workflowruntime.RunID) (workflowruntime.CompensationLedgerSnapshot, error) {
	var snapshotJSON, status, outcome, updatedAt string
	var planDigest string
	var generation int64
	err := query.QueryRowContext(ctx, `SELECT plan_digest,status,outcome,generation,updated_at,snapshot_json FROM workflow_compensation_ledgers WHERE run_id=?`, runID).Scan(&planDigest, &status, &outcome, &generation, &updatedAt, &snapshotJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return workflowruntime.CompensationLedgerSnapshot{}, fmt.Errorf("%w: compensation ledger", workflowruntime.ErrNotFound)
	}
	if err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, fmt.Errorf("load workflow compensation ledger: %w", err)
	}
	var snapshot workflowruntime.CompensationLedgerSnapshot
	if err := decodeWorkflowJSON("compensation ledger", snapshotJSON, &snapshot); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, err
	}
	parsedGeneration, err := workflowGeneration("compensation ledger generation", generation)
	if err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, err
	}
	parsedTime, err := parseWorkflowTime("compensation ledger updated_at", updatedAt)
	if err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, err
	}
	if snapshot.RunID != runID || snapshot.PlanDigest != planDigest || string(snapshot.Status) != status || string(snapshot.Outcome) != outcome || snapshot.Generation != parsedGeneration || !snapshot.UpdatedAt.Equal(parsedTime) {
		return workflowruntime.CompensationLedgerSnapshot{}, workflowInvalid(errors.New("compensation ledger projection is corrupt"))
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, workflowInvalid(err)
	}
	return snapshot, nil
}

func loadWorkflowCompensationEntry(ctx context.Context, query workflowSQL, runID workflowruntime.RunID, entryID string) (workflowruntime.CompensationEntrySnapshot, error) {
	var snapshotJSON, status, updatedAt, sourceNode, sourceIteration, handlerNode, handlerIteration string
	var sourceAttempt, generation int64
	err := query.QueryRowContext(ctx, `SELECT source_node_id,source_iteration,source_attempt,handler_node_id,handler_iteration,status,generation,updated_at,snapshot_json FROM workflow_compensation_entries WHERE run_id=? AND entry_id=?`, runID, entryID).Scan(&sourceNode, &sourceIteration, &sourceAttempt, &handlerNode, &handlerIteration, &status, &generation, &updatedAt, &snapshotJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return workflowruntime.CompensationEntrySnapshot{}, fmt.Errorf("%w: compensation entry", workflowruntime.ErrNotFound)
	}
	if err != nil {
		return workflowruntime.CompensationEntrySnapshot{}, err
	}
	var snapshot workflowruntime.CompensationEntrySnapshot
	if err := decodeWorkflowJSON("compensation entry", snapshotJSON, &snapshot); err != nil {
		return workflowruntime.CompensationEntrySnapshot{}, err
	}
	parsedGeneration, err := workflowGeneration("compensation entry generation", generation)
	if err != nil {
		return workflowruntime.CompensationEntrySnapshot{}, err
	}
	parsedTime, err := parseWorkflowTime("compensation entry updated_at", updatedAt)
	if err != nil {
		return workflowruntime.CompensationEntrySnapshot{}, err
	}
	if snapshot.RunID != runID || snapshot.ID != entryID || snapshot.Source.NodeID != sourceNode || snapshot.Source.Iteration != sourceIteration || snapshot.SourceAttempt.Number != int(sourceAttempt) || snapshot.Handler.NodeID != handlerNode || snapshot.Handler.Iteration != handlerIteration || string(snapshot.Status) != status || snapshot.Generation != parsedGeneration || !snapshot.UpdatedAt.Equal(parsedTime) {
		return workflowruntime.CompensationEntrySnapshot{}, workflowInvalid(errors.New("compensation entry projection is corrupt"))
	}
	if err := snapshot.Validate(); err != nil {
		return workflowruntime.CompensationEntrySnapshot{}, workflowInvalid(err)
	}
	return snapshot, nil
}

func listWorkflowCompensationEntries(ctx context.Context, query workflowSQL, runID workflowruntime.RunID) ([]workflowruntime.CompensationEntrySnapshot, error) {
	rows, err := query.QueryContext(ctx, `SELECT entry_id FROM workflow_compensation_entries WHERE run_id=? ORDER BY entry_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]workflowruntime.CompensationEntrySnapshot, 0, len(ids))
	for _, id := range ids {
		entry, err := loadWorkflowCompensationEntry(ctx, query, runID, id)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, nil
}

func insertWorkflowCompensationLedger(ctx context.Context, query workflowSQL, s workflowruntime.CompensationLedgerSnapshot) error {
	if err := s.Validate(); err != nil {
		return workflowInvalid(err)
	}
	encoded, err := encodeWorkflowJSON(s)
	if err != nil {
		return err
	}
	_, err = query.ExecContext(ctx, `INSERT INTO workflow_compensation_ledgers(run_id,plan_digest,status,outcome,generation,updated_at,snapshot_json) VALUES(?,?,?,?,?,?,?)`, s.RunID, s.PlanDigest, s.Status, s.Outcome, s.Generation, workflowTime(s.UpdatedAt), encoded)
	return err
}
func updateWorkflowCompensationLedger(ctx context.Context, query workflowSQL, s workflowruntime.CompensationLedgerSnapshot, expected uint64) error {
	if err := s.Validate(); err != nil {
		return workflowInvalid(err)
	}
	encoded, err := encodeWorkflowJSON(s)
	if err != nil {
		return err
	}
	eg, err := sqliteGeneration("compensation expected generation", expected)
	if err != nil {
		return err
	}
	g, err := sqliteGeneration("compensation generation", s.Generation)
	if err != nil {
		return err
	}
	res, err := query.ExecContext(ctx, `UPDATE workflow_compensation_ledgers SET status=?,outcome=?,generation=?,updated_at=?,snapshot_json=? WHERE run_id=? AND generation=?`, s.Status, s.Outcome, g, workflowTime(s.UpdatedAt), encoded, s.RunID, eg)
	if err != nil {
		return err
	}
	return expectOneWorkflowRow(res, "compensation ledger", expected, expected)
}
func insertWorkflowCompensationEntry(ctx context.Context, query workflowSQL, s workflowruntime.CompensationEntrySnapshot) error {
	if err := s.Validate(); err != nil {
		return workflowInvalid(err)
	}
	encoded, err := encodeWorkflowJSON(s)
	if err != nil {
		return err
	}
	_, err = query.ExecContext(ctx, `INSERT INTO workflow_compensation_entries(run_id,entry_id,source_node_id,source_iteration,source_attempt,handler_node_id,handler_iteration,status,generation,updated_at,snapshot_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, s.RunID, s.ID, s.Source.NodeID, s.Source.Iteration, s.SourceAttempt.Number, s.Handler.NodeID, s.Handler.Iteration, s.Status, s.Generation, workflowTime(s.UpdatedAt), encoded)
	return err
}
func updateWorkflowCompensationEntry(ctx context.Context, query workflowSQL, s workflowruntime.CompensationEntrySnapshot, expected uint64) error {
	if err := s.Validate(); err != nil {
		return workflowInvalid(err)
	}
	encoded, err := encodeWorkflowJSON(s)
	if err != nil {
		return err
	}
	eg, err := sqliteGeneration("compensation entry expected generation", expected)
	if err != nil {
		return err
	}
	g, err := sqliteGeneration("compensation entry generation", s.Generation)
	if err != nil {
		return err
	}
	res, err := query.ExecContext(ctx, `UPDATE workflow_compensation_entries SET handler_iteration=?,status=?,generation=?,updated_at=?,snapshot_json=? WHERE run_id=? AND entry_id=? AND generation=?`, s.Handler.Iteration, s.Status, g, workflowTime(s.UpdatedAt), encoded, s.RunID, s.ID, eg)
	if err != nil {
		return err
	}
	return expectOneWorkflowRow(res, "compensation entry", expected, expected)
}

func workflowCompensationReplay(ctx context.Context, query workflowSQL, key, operation string, request any, result any) (bool, string, error) {
	digest, err := workflowruntime.CompensationRequestDigest(request)
	if err != nil {
		return false, "", err
	}
	var storedOperation, storedDigest, storedResult string
	err = query.QueryRowContext(ctx, `SELECT operation,request_digest,result_json FROM workflow_compensation_idempotency WHERE idempotency_key=?`, key).Scan(&storedOperation, &storedDigest, &storedResult)
	if errors.Is(err, sql.ErrNoRows) {
		return false, digest, nil
	}
	if err != nil {
		return false, "", err
	}
	if storedOperation != operation || storedDigest != digest {
		return false, "", workflowIdempotencyConflict("compensation "+operation, key)
	}
	if err := decodeWorkflowJSON("compensation "+operation+" result", storedResult, result); err != nil {
		return false, "", err
	}
	return true, digest, nil
}

func insertWorkflowCompensationReplay(ctx context.Context, query workflowSQL, key string, runID workflowruntime.RunID, operation, digest string, _ any, at time.Time) error {
	// Replay always reloads the current ledger projection after validating the
	// immutable request digest. Persisting the full entry/history result here
	// would redundantly duplicate an unbounded saga.
	_, err := query.ExecContext(ctx, `INSERT INTO workflow_compensation_idempotency(idempotency_key,run_id,operation,request_digest,result_json,created_at) VALUES(?,?,?,?,?,?)`, key, runID, operation, digest, `{}`, workflowTime(at))
	return err
}

func (s *WorkflowStateStore) LoadCompensationLedger(ctx context.Context, runID workflowruntime.RunID) (workflowruntime.CompensationLedgerSnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, err
	}
	return loadWorkflowCompensationLedger(ctx, s.db, runID)
}
func (s *WorkflowStateStore) ListCompensationEntries(ctx context.Context, runID workflowruntime.RunID) ([]workflowruntime.CompensationEntrySnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if _, err := loadWorkflowCompensationLedger(ctx, s.db, runID); err != nil {
		return nil, err
	}
	return listWorkflowCompensationEntries(ctx, s.db, runID)
}
func (s *WorkflowStateStore) LoadCompensationEntryByHandler(ctx context.Context, id workflowruntime.NodeInvocationID) (workflowruntime.CompensationEntrySnapshot, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return workflowruntime.CompensationEntrySnapshot{}, err
	}
	var entryID string
	err := s.db.QueryRowContext(ctx, `SELECT entry_id FROM workflow_compensation_entries WHERE run_id=? AND handler_node_id=? AND handler_iteration=?`, id.RunID, id.NodeID, id.Iteration).Scan(&entryID)
	if errors.Is(err, sql.ErrNoRows) {
		return workflowruntime.CompensationEntrySnapshot{}, fmt.Errorf("%w: compensation handler", workflowruntime.ErrNotFound)
	}
	if err != nil {
		return workflowruntime.CompensationEntrySnapshot{}, err
	}
	return loadWorkflowCompensationEntry(ctx, s.db, id.RunID, entryID)
}

func (s *WorkflowStateStore) FinishCompensableAttempt(ctx context.Context, request workflowruntime.FinishCompensableAttemptRequest) (workflowruntime.FinishCompensableAttemptResult, error) {
	if err := validateWorkflowFinishAttempt(request.Finish); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(err)
	}
	if request.Finish.NextNodeStatus != request.Finish.AttemptStatus {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(errors.New("applied compensation receipt requires a terminal forward node"))
	}
	e := request.Eligibility
	if err := values.ValidateDigest(e.PlanDigest); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(err)
	}
	if err := workflowruntime.ValidateCompensationHandlerNodeID(e.HandlerNodeID); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(err)
	}
	evidenceDigest, err := workflowruntime.CompensationEvidenceDigest(e.Evidence)
	if err != nil || e.Receipt.Operation != e.Evidence.Operation {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(errors.Join(err, errors.New("compensation receipt operation differs from evidence")))
	}
	if err := values.ValidateValueSetSchema(e.Evidence.ReceiptSchema, e.Receipt.Values); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(err)
	}
	if workflowruntime.RunID(e.Receipt.ChildRunID) != e.ChildRunID {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(errors.New("compensation receipt child run differs from eligibility"))
	}
	if err := workflowruntime.ValidateCompensationChildRunID(e.ChildRunID); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(err)
	}
	if e.ChildRunID != "" && e.ChildRunID == request.Finish.InvocationID.RunID {
		return workflowruntime.FinishCompensableAttemptResult{}, workflowInvalid(errors.New("compensation child run cannot be the owning run"))
	}
	var result workflowruntime.FinishCompensableAttemptResult
	err = s.write(ctx, "finish compensable workflow attempt", func(query workflowSQL) error {
		run, err := loadWorkflowRun(ctx, query, request.Finish.InvocationID.RunID)
		if err != nil {
			return err
		}
		if run.Plan.Digest != e.PlanDigest {
			return workflowInvalid(errors.New("compensation plan digest differs from run"))
		}
		source, err := loadWorkflowNode(ctx, query, request.Finish.InvocationID)
		if err != nil {
			return err
		}
		if source.Phase != workflowruntime.InvocationForward {
			return workflowInvalid(errors.New("compensation eligibility requires a forward invocation"))
		}
		attemptID := workflowruntime.AttemptID{Invocation: request.Finish.InvocationID, Number: request.Finish.AttemptNumber}
		entryID := workflowruntime.CompensationEntryID(attemptID)
		if prior, loadErr := loadWorkflowCompensationEntry(ctx, query, run.ID, entryID); loadErr == nil {
			attempt, err := loadWorkflowAttempt(ctx, query, attemptID)
			if err != nil {
				return err
			}
			matches, err := workflowCompensationEligibilityReplayMatches(ctx, query, request, evidenceDigest, prior, source, attempt)
			if err != nil {
				return err
			}
			if matches {
				ledger, err := loadWorkflowCompensationLedger(ctx, query, run.ID)
				if err != nil {
					return err
				}
				result = workflowruntime.FinishCompensableAttemptResult{Finish: workflowruntime.FinishNodeAttemptResult{Node: source, Attempt: attempt}, Ledger: ledger, Entry: prior}
				return nil
			}
			return workflowIdempotencyConflict("compensation eligibility", entryID)
		} else if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			return loadErr
		}
		if err := workflowruntime.ValidateCompensationTerminalEvidence(request.Finish.AttemptStatus, e.OriginalError); err != nil {
			return workflowInvalid(err)
		}
		if e.ChildRunID != "" {
			var count int
			if err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_child_runs WHERE parent_run_id=? AND node_id=? AND iteration=? AND child_run_id=?`, run.ID, request.Finish.InvocationID.NodeID, request.Finish.InvocationID.Iteration, e.ChildRunID).Scan(&count); err != nil {
				return err
			}
			if count != 1 {
				return workflowInvalid(errors.New("compensation child receipt requires one exact durable child run link"))
			}
		}
		finished, err := finishWorkflowNodeAttemptForCompensation(ctx, query, request.Finish)
		if err != nil {
			return err
		}
		receiptRef, err := insertWorkflowValues(ctx, query, workflowruntime.ValueOwner{Kind: "compensation-receipt", RunID: run.ID, Invocation: &attemptID.Invocation, Attempt: &attemptID}, e.Receipt.Values)
		if err != nil {
			return err
		}
		var outputRef, errorRef *values.ValueSetRef
		if len(e.OriginalOutputs) != 0 {
			ref, err := insertWorkflowValues(ctx, query, workflowruntime.ValueOwner{Kind: "compensation-original-output", RunID: run.ID, Invocation: &attemptID.Invocation, Attempt: &attemptID}, e.OriginalOutputs)
			if err != nil {
				return err
			}
			outputRef = &ref
		} else {
			outputRef = cloneWorkflowValueRef(finished.Attempt.Outputs)
		}
		if len(e.OriginalError) != 0 {
			ref, err := insertWorkflowValues(ctx, query, workflowruntime.ValueOwner{Kind: "compensation-original-error", RunID: run.ID, Invocation: &attemptID.Invocation, Attempt: &attemptID}, e.OriginalError)
			if err != nil {
				return err
			}
			errorRef = &ref
		}
		ledger, loadErr := loadWorkflowCompensationLedger(ctx, query, run.ID)
		if errors.Is(loadErr, workflowruntime.ErrNotFound) {
			ledger = workflowruntime.CompensationLedgerSnapshot{RunID: run.ID, PlanDigest: run.Plan.Digest, Status: workflowruntime.CompensationCollecting, Generation: 1, CreatedAt: request.Finish.At, UpdatedAt: request.Finish.At}
			if err := insertWorkflowCompensationLedger(ctx, query, ledger); err != nil {
				return err
			}
		} else if loadErr != nil {
			return loadErr
		} else {
			if ledger.Status != workflowruntime.CompensationCollecting {
				return workflowInvalid(errors.New("frozen compensation ledger rejects eligibility"))
			}
			if request.Finish.At.Before(ledger.UpdatedAt) {
				return workflowInvalid(errors.New("compensation eligibility time regresses ledger"))
			}
			prior := ledger.Generation
			ledger.Generation++
			ledger.UpdatedAt = request.Finish.At
			if err := updateWorkflowCompensationLedger(ctx, query, ledger, prior); err != nil {
				return err
			}
		}
		entry := workflowruntime.CompensationEntrySnapshot{ID: entryID, RunID: run.ID, PlanDigest: run.Plan.Digest, Source: attemptID.Invocation, SourceAttempt: attemptID, Handler: workflowruntime.CompensationHandlerID(run.ID, e.HandlerNodeID, entryID), Status: workflowruntime.CompensationEligible, Operation: e.Evidence.Operation, EvidenceDigest: evidenceDigest, OriginalInputs: cloneWorkflowValueRef(finished.Attempt.Inputs), OriginalOutputs: outputRef, OriginalError: errorRef, Receipt: receiptRef, ChildRunID: e.ChildRunID, Generation: 1, CreatedAt: request.Finish.At, UpdatedAt: request.Finish.At}
		if err := entry.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := insertWorkflowCompensationEntry(ctx, query, entry); err != nil {
			return err
		}
		invocation := entry.Source
		eventAttempt := entry.SourceAttempt
		if _, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: run.ID, Invocation: &invocation, Attempt: &eventAttempt, Type: workflowruntime.EventCompensationEligible, OccurredAt: request.Finish.At, Attributes: map[string]string{"entry_id": entry.ID, "handler": entry.Handler.NodeID, "operation": entry.Operation}, Values: &receiptRef, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return err
		}
		result = workflowruntime.FinishCompensableAttemptResult{Finish: finished, Ledger: ledger, Entry: entry}
		return nil
	})
	if err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, err
	}
	return result, nil
}

func workflowCompensationEligibilityReplayMatches(ctx context.Context, query workflowSQL, request workflowruntime.FinishCompensableAttemptRequest, evidenceDigest string, prior workflowruntime.CompensationEntrySnapshot, node workflowruntime.NodeInvocationSnapshot, attempt workflowruntime.AttemptSnapshot) (bool, error) {
	finish, eligibility := request.Finish, request.Eligibility
	attemptID := workflowruntime.AttemptID{Invocation: finish.InvocationID, Number: finish.AttemptNumber}
	if prior.RunID != finish.InvocationID.RunID || prior.PlanDigest != eligibility.PlanDigest || prior.Source != finish.InvocationID || prior.SourceAttempt != attemptID ||
		prior.Handler != workflowruntime.CompensationHandlerID(prior.RunID, eligibility.HandlerNodeID, prior.ID) || prior.Operation != eligibility.Evidence.Operation ||
		prior.EvidenceDigest != evidenceDigest || prior.ChildRunID != eligibility.ChildRunID || !prior.CreatedAt.Equal(finish.At) ||
		!equalWorkflowValueRef(prior.OriginalInputs, attempt.Inputs) {
		return false, nil
	}
	matches, err := workflowCompensationValuesMatch(ctx, query, prior.Receipt, eligibility.Receipt.Values)
	if err != nil || !matches {
		return matches, err
	}
	if len(eligibility.OriginalOutputs) != 0 {
		if prior.OriginalOutputs == nil {
			return false, nil
		}
		matches, err = workflowCompensationValuesMatch(ctx, query, *prior.OriginalOutputs, eligibility.OriginalOutputs)
		if err != nil || !matches {
			return matches, err
		}
	} else if !equalWorkflowValueRef(prior.OriginalOutputs, attempt.Outputs) {
		return false, nil
	}
	if len(eligibility.OriginalError) != 0 {
		if prior.OriginalError == nil {
			return false, nil
		}
		matches, err = workflowCompensationValuesMatch(ctx, query, *prior.OriginalError, eligibility.OriginalError)
		if err != nil || !matches {
			return matches, err
		}
	} else if prior.OriginalError != nil {
		return false, nil
	}
	return attempt.ID == attemptID && attempt.Status == finish.AttemptStatus && attempt.Generation == finish.ExpectedAttemptGeneration+1 &&
		equalWorkflowValueRef(attempt.Outputs, finish.Outputs) && reflect.DeepEqual(attempt.Failure, finish.Failure) && attempt.FinishedAt.Equal(finish.At) && attempt.UpdatedAt.Equal(finish.At) &&
		node.ID == finish.InvocationID && node.Status == finish.NextNodeStatus && node.LatestAttempt == finish.AttemptNumber && node.Generation == finish.ExpectedNodeGeneration+1 &&
		equalWorkflowValueRef(node.Outputs, finish.Outputs) && node.Origin == workflowruntime.OriginExecuted && node.UpdatedAt.Equal(finish.At), nil
}

func workflowCompensationValuesMatch(ctx context.Context, query workflowSQL, ref values.ValueSetRef, expected values.ValueSet) (bool, error) {
	_, err := loadWorkflowValues(ctx, query, ref)
	if err != nil {
		return false, err
	}
	digest, err := values.DigestValueSet(expected)
	if err != nil {
		return false, workflowInvalid(err)
	}
	return digest == ref.Digest, nil
}

func finishWorkflowNodeAttemptForCompensation(ctx context.Context, query workflowSQL, request workflowruntime.FinishNodeAttemptRequest) (workflowruntime.FinishNodeAttemptResult, error) {
	currentNode, err := loadWorkflowNode(ctx, query, request.InvocationID)
	if err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	if currentNode.Generation != request.ExpectedNodeGeneration {
		return workflowruntime.FinishNodeAttemptResult{}, workflowCAS("node invocation", request.ExpectedNodeGeneration, currentNode.Generation)
	}
	run, err := loadWorkflowRun(ctx, query, currentNode.ID.RunID)
	if err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	allowedRun, runAdmissionErr := workflowRunAllowsCompensationExecution(ctx, query, run, currentNode)
	if runAdmissionErr != nil {
		return workflowruntime.FinishNodeAttemptResult{}, runAdmissionErr
	}
	if !allowedRun {
		return workflowruntime.FinishNodeAttemptResult{}, workflowInvalid(errors.New("terminal run fences attempt completion"))
	}
	allowed, err := workflowControlAdmissionAllowed(ctx, query, currentNode.ID)
	if err != nil || !allowed {
		if err != nil {
			return workflowruntime.FinishNodeAttemptResult{}, err
		}
		return workflowruntime.FinishNodeAttemptResult{}, workflowInvalid(errors.New("pending terminal intent fences attempt completion"))
	}
	if currentNode.Status != workflowruntime.NodeRunning {
		return workflowruntime.FinishNodeAttemptResult{}, workflowNodeTransitionError(currentNode, request.NextNodeStatus, "finishing requires running node")
	}
	at := request.At.UTC()
	if err := validateWorkflowLifecycleClaim(currentNode, &request.Claim, at); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	if currentNode.LatestAttempt != request.AttemptNumber {
		return workflowruntime.FinishNodeAttemptResult{}, workflowAttemptConflict(currentNode.ID, request.AttemptNumber, "only LatestAttempt may be finished")
	}
	attemptID := workflowruntime.AttemptID{Invocation: currentNode.ID, Number: request.AttemptNumber}
	currentAttempt, err := loadWorkflowAttempt(ctx, query, attemptID)
	if err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	if currentAttempt.Generation != request.ExpectedAttemptGeneration {
		return workflowruntime.FinishNodeAttemptResult{}, workflowCAS("attempt", request.ExpectedAttemptGeneration, currentAttempt.Generation)
	}
	if currentAttempt.Status != workflowruntime.NodeRunning || !currentAttempt.FinishedAt.IsZero() {
		return workflowruntime.FinishNodeAttemptResult{}, workflowAttemptConflict(currentNode.ID, request.AttemptNumber, "attempt already finished")
	}
	if at.Before(currentNode.UpdatedAt) || at.Before(currentAttempt.UpdatedAt) {
		return workflowruntime.FinishNodeAttemptResult{}, workflowInvalid(errors.New("attempt finish time regresses"))
	}
	nextAttempt := cloneWorkflowAttempt(currentAttempt)
	nextAttempt.Status = request.AttemptStatus
	nextAttempt.Outputs = cloneWorkflowValueRef(request.Outputs)
	nextAttempt.Failure = cloneWorkflowFailure(request.Failure)
	nextAttempt.FinishedAt = at
	nextAttempt.UpdatedAt = at
	nextAttempt.Generation++
	nextNode := cloneWorkflowNode(currentNode)
	nextNode.Status = request.NextNodeStatus
	nextNode.Blocked = nil
	nextNode.Lease = nil
	nextNode.Generation++
	nextNode.UpdatedAt = at
	if request.NextNodeStatus == workflowruntime.NodeReady {
		nextNode.Outputs = nil
		nextNode.Origin = ""
	} else {
		nextNode.Outputs = cloneWorkflowValueRef(request.Outputs)
		nextNode.Origin = workflowruntime.OriginExecuted
	}
	if err := nextAttempt.Validate(); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, workflowInvalid(err)
	}
	if err := nextNode.Validate(); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, workflowInvalid(err)
	}
	if err := updateWorkflowAttemptCAS(ctx, query, nextAttempt, currentAttempt.Generation); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	if err := updateWorkflowNodeCAS(ctx, query, nextNode, currentNode.Generation); err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	invocation := nextNode.ID
	eventAttempt := nextAttempt.ID
	attributes := workflowAttemptAttributes("node_attempt", string(currentNode.Status), string(nextNode.Status), nextAttempt)
	attributes["attempt_status"] = string(nextAttempt.Status)
	if nextAttempt.Failure != nil {
		attributes["failure_code"] = nextAttempt.Failure.Code
	}
	event, err := appendWorkflowEvent(ctx, query, workflowruntime.AppendEventRequest{RunID: nextNode.ID.RunID, Invocation: &invocation, Attempt: &eventAttempt, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: at, Attributes: attributes, Values: cloneWorkflowValueRef(request.Outputs), Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		return workflowruntime.FinishNodeAttemptResult{}, err
	}
	return workflowruntime.FinishNodeAttemptResult{Node: nextNode, Attempt: nextAttempt, Event: event}, nil
}

// The remaining transitions use snapshot_json as the semantic source and
// exact generation predicates as their CAS boundary.
func (s *WorkflowStateStore) FreezeCompensation(ctx context.Context, r workflowruntime.FreezeCompensationRequest) (workflowruntime.FreezeCompensationResult, error) {
	var out workflowruntime.FreezeCompensationResult
	err := s.write(ctx, "freeze workflow compensation", func(q workflowSQL) error {
		if r.ExpectedRunGeneration == 0 || r.ExpectedIntentGeneration == 0 || r.At.IsZero() || strings.TrimSpace(r.IdempotencyKey) == "" || !r.Trigger.Valid() || r.Trigger == graph.CompensationManual || !r.OriginalStatus.Terminal() {
			return workflowInvalid(errors.New("invalid compensation freeze"))
		}
		replayed, requestDigest, replayErr := workflowCompensationReplay(ctx, q, r.IdempotencyKey, "freeze", r, &out)
		if replayErr != nil {
			return replayErr
		}
		if replayed {
			ledger, loadErr := loadWorkflowCompensationLedger(ctx, q, r.RunID)
			if loadErr != nil {
				return loadErr
			}
			entries, loadErr := listWorkflowCompensationEntries(ctx, q, r.RunID)
			if loadErr != nil {
				return loadErr
			}
			out = workflowruntime.FreezeCompensationResult{Outcome: workflowruntime.IdempotencyReplayed, Ledger: ledger, Entries: entries}
			return nil
		}
		run, err := loadWorkflowRun(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		intent, err := loadWorkflowTerminalIntent(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		if run.Generation != r.ExpectedRunGeneration {
			return workflowCAS("compensation run", r.ExpectedRunGeneration, run.Generation)
		}
		if intent.Generation != r.ExpectedIntentGeneration || intent.Status != workflowruntime.TerminalIntentPending || intent.IntendedStatus != r.OriginalStatus || !equalWorkflowValueRef(intent.Error, r.OriginalFailure) {
			return workflowInvalid(errors.New("compensation freeze differs from terminal intent"))
		}
		if r.At.Before(run.UpdatedAt) || r.At.Before(intent.UpdatedAt) {
			return workflowInvalid(errors.New("compensation freeze time regresses run or intent"))
		}
		if run.Plan.Digest != r.PlanDigest {
			return workflowInvalid(errors.New("compensation plan differs"))
		}
		finalizers := map[workflowruntime.NodeInvocationID]bool{}
		for _, f := range intent.Finalizers {
			finalizers[f.Invocation] = true
		}
		rows, err := q.QueryContext(ctx, workflowNodeSelect+` WHERE n.run_id=? AND n.phase=''`, r.RunID)
		if err != nil {
			return err
		}
		for rows.Next() {
			node, err := scanWorkflowNode(rows)
			if err != nil {
				rows.Close()
				return err
			}
			if finalizers[node.ID] {
				continue
			}
			if !node.Status.Terminal() {
				rows.Close()
				return workflowruntime.ErrCompensationPending
			}
			if r.At.Before(node.UpdatedAt) {
				rows.Close()
				return workflowInvalid(errors.New("compensation freeze time regresses forward node"))
			}
			if node.LatestAttempt > 0 {
				attempt, err := loadWorkflowAttempt(ctx, q, workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
				if err != nil || !attempt.Status.Terminal() || attempt.FinishedAt.IsZero() {
					rows.Close()
					if err != nil {
						return err
					}
					return workflowruntime.ErrCompensationPending
				}
				if r.At.Before(attempt.UpdatedAt) {
					rows.Close()
					return workflowInvalid(errors.New("compensation freeze time regresses forward attempt"))
				}
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		ledger, err := loadWorkflowCompensationLedger(ctx, q, r.RunID)
		if errors.Is(err, workflowruntime.ErrNotFound) {
			ledger = workflowruntime.CompensationLedgerSnapshot{RunID: r.RunID, PlanDigest: r.PlanDigest, Status: workflowruntime.CompensationCollecting, Generation: 1, CreatedAt: r.At, UpdatedAt: r.At}
			if err := insertWorkflowCompensationLedger(ctx, q, ledger); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if ledger.Status != workflowruntime.CompensationCollecting {
			return workflowruntime.ErrCompensationConflict
		}
		if r.At.Before(ledger.UpdatedAt) {
			return workflowInvalid(errors.New("compensation freeze time regresses ledger"))
		}
		entries, err := listWorkflowCompensationEntries(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		for i := range entries {
			if r.At.Before(entries[i].UpdatedAt) {
				return workflowInvalid(errors.New("compensation freeze time regresses entry"))
			}
			prereq := workflowCompensationPrerequisites(entries[i], entries, r.Dependencies)
			prior := entries[i].Generation
			entries[i].Prerequisites = prereq
			entries[i].Status = workflowruntime.CompensationPending
			entries[i].Generation++
			entries[i].UpdatedAt = r.At
			if err := updateWorkflowCompensationEntry(ctx, q, entries[i], prior); err != nil {
				return err
			}
		}
		if err := workflowruntime.ValidateCompensationEntryDependencies(entries); err != nil {
			return workflowInvalid(err)
		}
		prior := ledger.Generation
		ledger.Trigger = r.Trigger
		ledger.OriginalStatus = r.OriginalStatus
		ledger.OriginalFailure = cloneWorkflowValueRef(r.OriginalFailure)
		ledger.Cycles = []workflowruntime.CompensationCycle{{Number: 1, StartedAt: r.At}}
		ledger.Generation++
		ledger.UpdatedAt = r.At
		if len(entries) == 0 {
			ledger.Status = workflowruntime.CompensationTerminal
			ledger.Outcome = workflowruntime.CompensationOutcomeSucceeded
			ledger.CompletedAt = r.At
			ledger.Cycles[0].Outcome = ledger.Outcome
			ledger.Cycles[0].CompletedAt = r.At
		} else {
			ledger.Status = workflowruntime.CompensationFrozen
		}
		if err := updateWorkflowCompensationLedger(ctx, q, ledger, prior); err != nil {
			return err
		}
		if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Type: workflowruntime.EventCompensationFrozen, OccurredAt: r.At, Attributes: map[string]string{"trigger": string(r.Trigger), "entries": fmt.Sprint(len(entries))}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return err
		}
		if ledger.Status == workflowruntime.CompensationTerminal {
			if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: r.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
				return err
			}
		}
		out = workflowruntime.FreezeCompensationResult{Outcome: workflowruntime.IdempotencyApplied, Ledger: ledger, Entries: entries}
		if err := insertWorkflowCompensationReplay(ctx, q, r.IdempotencyKey, r.RunID, "freeze", requestDigest, out, r.At); err != nil {
			return err
		}
		return nil
	})
	return out, err
}

func (s *WorkflowStateStore) BeginManualCompensation(ctx context.Context, r workflowruntime.BeginManualCompensationRequest) (workflowruntime.FreezeCompensationResult, error) {
	var out workflowruntime.FreezeCompensationResult
	err := s.write(ctx, "begin manual workflow compensation", func(q workflowSQL) error {
		if r.ExpectedRunGeneration == 0 || r.At.IsZero() || strings.TrimSpace(r.IdempotencyKey) == "" || values.ValidateDigest(r.Authorization) != nil || r.OriginalStatus != workflowruntime.RunSucceeded {
			return workflowInvalid(errors.New("invalid manual compensation request"))
		}
		replayed, digest, err := workflowCompensationReplay(ctx, q, r.IdempotencyKey, "manual", r, &out)
		if err != nil {
			return err
		}
		if replayed {
			ledger, loadErr := loadWorkflowCompensationLedger(ctx, q, r.RunID)
			if loadErr != nil {
				return loadErr
			}
			entries, loadErr := listWorkflowCompensationEntries(ctx, q, r.RunID)
			if loadErr != nil {
				return loadErr
			}
			out = workflowruntime.FreezeCompensationResult{Outcome: workflowruntime.IdempotencyReplayed, Ledger: ledger, Entries: entries}
			return nil
		}
		run, err := loadWorkflowRun(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		if run.Generation != r.ExpectedRunGeneration {
			return workflowCAS("manual compensation run", r.ExpectedRunGeneration, run.Generation)
		}
		if run.Status != workflowruntime.RunSucceeded || run.Status != r.OriginalStatus || run.Plan.Digest != r.PlanDigest {
			return workflowInvalid(errors.New("manual compensation differs from immutable terminal run"))
		}
		if _, err := loadWorkflowTerminalIntent(ctx, q, r.RunID); err == nil {
			return workflowInvalid(errors.New("manual compensation cannot overlap terminal-intent cleanup"))
		} else if !errors.Is(err, workflowruntime.ErrNotFound) {
			return err
		}
		if r.At.Before(run.UpdatedAt) {
			return workflowInvalid(errors.New("manual compensation time regresses run"))
		}
		rows, err := q.QueryContext(ctx, workflowNodeSelect+` WHERE n.run_id=? AND n.phase=''`, r.RunID)
		if err != nil {
			return err
		}
		for rows.Next() {
			node, scanErr := scanWorkflowNode(rows)
			if scanErr != nil {
				rows.Close()
				return scanErr
			}
			if !node.Status.Terminal() {
				rows.Close()
				return workflowruntime.ErrCompensationPending
			}
			if r.At.Before(node.UpdatedAt) {
				rows.Close()
				return workflowInvalid(errors.New("manual compensation time regresses forward node"))
			}
			if node.LatestAttempt > 0 {
				attempt, loadErr := loadWorkflowAttempt(ctx, q, workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
				if loadErr != nil || !attempt.Status.Terminal() || attempt.FinishedAt.IsZero() {
					rows.Close()
					if loadErr != nil {
						return loadErr
					}
					return workflowruntime.ErrCompensationPending
				}
				if r.At.Before(attempt.UpdatedAt) {
					rows.Close()
					return workflowInvalid(errors.New("manual compensation time regresses forward attempt"))
				}
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		ledger, err := loadWorkflowCompensationLedger(ctx, q, r.RunID)
		if errors.Is(err, workflowruntime.ErrNotFound) {
			ledger = workflowruntime.CompensationLedgerSnapshot{RunID: r.RunID, PlanDigest: r.PlanDigest, Status: workflowruntime.CompensationCollecting, Generation: 1, CreatedAt: r.At, UpdatedAt: r.At}
			if err := insertWorkflowCompensationLedger(ctx, q, ledger); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if ledger.Status != workflowruntime.CompensationCollecting {
			return workflowruntime.ErrCompensationConflict
		}
		if r.At.Before(ledger.UpdatedAt) {
			return workflowInvalid(errors.New("manual compensation time regresses ledger"))
		}
		entries, err := listWorkflowCompensationEntries(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		for index := range entries {
			if r.At.Before(entries[index].UpdatedAt) {
				return workflowInvalid(errors.New("manual compensation time regresses entry"))
			}
			prerequisites := workflowCompensationPrerequisites(entries[index], entries, r.Dependencies)
			prior := entries[index].Generation
			entries[index].Prerequisites, entries[index].Status = prerequisites, workflowruntime.CompensationPending
			entries[index].Generation++
			entries[index].UpdatedAt = r.At
			if err := updateWorkflowCompensationEntry(ctx, q, entries[index], prior); err != nil {
				return err
			}
		}
		if err := workflowruntime.ValidateCompensationEntryDependencies(entries); err != nil {
			return workflowInvalid(err)
		}
		prior := ledger.Generation
		ledger.Trigger, ledger.OriginalStatus = graph.CompensationManual, r.OriginalStatus
		ledger.Cycles = []workflowruntime.CompensationCycle{{Number: 1, Attestation: r.Authorization, StartedAt: r.At}}
		ledger.Generation++
		ledger.UpdatedAt = r.At
		if len(entries) == 0 {
			ledger.Status, ledger.Outcome, ledger.CompletedAt = workflowruntime.CompensationTerminal, workflowruntime.CompensationOutcomeSucceeded, r.At
			ledger.Cycles[0].Outcome, ledger.Cycles[0].CompletedAt = ledger.Outcome, r.At
		} else {
			ledger.Status = workflowruntime.CompensationFrozen
		}
		if err := updateWorkflowCompensationLedger(ctx, q, ledger, prior); err != nil {
			return err
		}
		if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Type: workflowruntime.EventCompensationFrozen, OccurredAt: r.At, Attributes: map[string]string{"trigger": string(graph.CompensationManual), "entries": fmt.Sprint(len(entries))}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return err
		}
		if ledger.Status == workflowruntime.CompensationTerminal {
			if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: r.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
				return err
			}
		}
		out = workflowruntime.FreezeCompensationResult{Outcome: workflowruntime.IdempotencyApplied, Ledger: ledger, Entries: entries}
		return insertWorkflowCompensationReplay(ctx, q, r.IdempotencyKey, r.RunID, "manual", digest, out, r.At)
	})
	return out, err
}

func workflowCompensationPrerequisites(entry workflowruntime.CompensationEntrySnapshot, entries []workflowruntime.CompensationEntrySnapshot, dependencies map[string][]string) []string {
	set := make(map[string]struct{})
	for _, downstream := range dependencies[entry.Source.NodeID] {
		for _, candidate := range entries {
			if candidate.Source.NodeID == downstream {
				set[candidate.ID] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func (s *WorkflowStateStore) ActivateCompensationEntry(ctx context.Context, r workflowruntime.ActivateCompensationEntryRequest) (workflowruntime.ActivateCompensationEntryResult, error) {
	var out workflowruntime.ActivateCompensationEntryResult
	err := s.write(ctx, "activate workflow compensation", func(q workflowSQL) error {
		if err := values.ValidatePersistableSet(r.Inputs); err != nil || r.At.IsZero() {
			return workflowInvalid(err)
		}
		ledger, err := loadWorkflowCompensationLedger(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		entry, err := loadWorkflowCompensationEntry(ctx, q, r.RunID, r.EntryID)
		if err != nil {
			return err
		}
		if ledger.Generation != r.ExpectedLedgerGeneration || entry.Generation != r.ExpectedEntryGeneration {
			return workflowruntime.ErrCASMismatch
		}
		if r.At.Before(ledger.UpdatedAt) || r.At.Before(entry.UpdatedAt) {
			return workflowInvalid(errors.New("compensation activation time regresses ledger or entry"))
		}
		if !r.ChildResolution.Valid() || entry.ChildRunID == "" && r.ChildResolution != "" || entry.ChildRunID != "" && r.ChildResolution == "" {
			return workflowInvalid(errors.New("compensation activation child resolution is invalid"))
		}
		if entry.Status == workflowruntime.CompensationActive {
			if entry.ChildResolution != r.ChildResolution {
				return workflowIdempotencyConflict("compensation activation", r.EntryID)
			}
			node, loadErr := loadWorkflowNode(ctx, q, entry.Handler)
			if loadErr != nil {
				return loadErr
			}
			out = workflowruntime.ActivateCompensationEntryResult{Ledger: ledger, Entry: entry, Node: node}
			return nil
		}
		if entry.Status != workflowruntime.CompensationPending {
			return workflowruntime.ErrCompensationConflict
		}
		entries, err := listWorkflowCompensationEntries(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		byID := map[string]workflowruntime.CompensationEntrySnapshot{}
		for _, candidate := range entries {
			byID[candidate.ID] = candidate
		}
		for _, id := range entry.Prerequisites {
			if !byID[id].Status.Terminal() {
				return workflowruntime.ErrCompensationPending
			}
		}
		if ledger.Trigger == graph.CompensationManual {
			run, loadErr := loadWorkflowRun(ctx, q, r.RunID)
			if loadErr != nil || !run.Status.Terminal() {
				if loadErr != nil {
					return loadErr
				}
				return workflowruntime.ErrCompensationConflict
			}
			if r.At.Before(run.UpdatedAt) {
				return workflowInvalid(errors.New("compensation activation time regresses run"))
			}
		} else {
			intent, loadErr := loadWorkflowTerminalIntent(ctx, q, r.RunID)
			if loadErr != nil || intent.Status != workflowruntime.TerminalIntentPending {
				if loadErr != nil {
					return loadErr
				}
				return workflowruntime.ErrCompensationConflict
			}
			if r.At.Before(intent.UpdatedAt) {
				return workflowInvalid(errors.New("compensation activation time regresses terminal intent"))
			}
		}
		inputRef, err := insertWorkflowValues(ctx, q, workflowruntime.ValueOwner{Kind: "compensation-handler-input", RunID: r.RunID, Invocation: &entry.Handler}, r.Inputs)
		if err != nil {
			return err
		}
		node := workflowruntime.NodeInvocationSnapshot{ID: entry.Handler, Phase: workflowruntime.InvocationCompensation, Status: workflowruntime.NodeReady, Inputs: &inputRef, Generation: 1, CreatedAt: r.At, UpdatedAt: r.At}
		if err := node.Validate(); err != nil {
			return workflowInvalid(err)
		}
		if err := insertWorkflowNode(ctx, q, node); err != nil {
			return err
		}
		entryPrior, ledgerPrior := entry.Generation, ledger.Generation
		entry.Status, entry.ChildResolution = workflowruntime.CompensationActive, r.ChildResolution
		entry.Generation++
		entry.UpdatedAt = r.At
		ledger.Status = workflowruntime.CompensationRunning
		ledger.Generation++
		ledger.UpdatedAt = r.At
		if err := updateWorkflowCompensationEntry(ctx, q, entry, entryPrior); err != nil {
			return err
		}
		if err := updateWorkflowCompensationLedger(ctx, q, ledger, ledgerPrior); err != nil {
			return err
		}
		invocation := node.ID
		if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Invocation: &invocation, Type: workflowruntime.EventCompensationReady, OccurredAt: r.At, Attributes: map[string]string{"entry_id": entry.ID}, Values: &inputRef, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return err
		}
		out = workflowruntime.ActivateCompensationEntryResult{Ledger: ledger, Entry: entry, Node: node}
		return nil
	})
	return out, err
}

func (s *WorkflowStateStore) FailCompensationEntry(ctx context.Context, r workflowruntime.FailCompensationEntryRequest) (workflowruntime.SealCompensationEntryResult, error) {
	var out workflowruntime.SealCompensationEntryResult
	err := s.write(ctx, "fail workflow compensation entry", func(q workflowSQL) error {
		if r.At.IsZero() || r.Failure.Retryable {
			return workflowInvalid(errors.New("compensation entry failure requires a permanent failure and time"))
		}
		if err := r.Failure.Validate(); err != nil {
			return workflowInvalid(err)
		}
		ledger, err := loadWorkflowCompensationLedger(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		entry, err := loadWorkflowCompensationEntry(ctx, q, r.RunID, r.EntryID)
		if err != nil {
			return err
		}
		if entry.Status.Terminal() {
			out = workflowruntime.SealCompensationEntryResult{Ledger: ledger, Entry: entry}
			return nil
		}
		if ledger.Generation != r.ExpectedLedgerGeneration || entry.Generation != r.ExpectedEntryGeneration {
			return workflowruntime.ErrCASMismatch
		}
		if entry.Status != workflowruntime.CompensationPending || ledger.Status != workflowruntime.CompensationFrozen && ledger.Status != workflowruntime.CompensationRunning {
			return workflowruntime.ErrCompensationConflict
		}
		if r.At.Before(ledger.UpdatedAt) || r.At.Before(entry.UpdatedAt) {
			return workflowInvalid(errors.New("compensation failure time regresses durable state"))
		}
		if !r.ChildResolution.Valid() || entry.ChildRunID == "" && r.ChildResolution != "" || entry.ChildRunID != "" && r.ChildResolution == "" {
			return workflowInvalid(errors.New("compensation failure child resolution is invalid"))
		}
		entries, err := listWorkflowCompensationEntries(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		byID := make(map[string]workflowruntime.CompensationEntrySnapshot, len(entries))
		for _, candidate := range entries {
			byID[candidate.ID] = candidate
		}
		for _, id := range entry.Prerequisites {
			if !byID[id].Status.Terminal() {
				return workflowruntime.ErrCompensationPending
			}
		}
		if ledger.Trigger == graph.CompensationManual {
			run, loadErr := loadWorkflowRun(ctx, q, r.RunID)
			if loadErr != nil {
				return loadErr
			}
			if !run.Status.Terminal() || r.At.Before(run.UpdatedAt) {
				return workflowruntime.ErrCompensationConflict
			}
		} else {
			intent, loadErr := loadWorkflowTerminalIntent(ctx, q, r.RunID)
			if loadErr != nil {
				return loadErr
			}
			if intent.Status != workflowruntime.TerminalIntentPending || r.At.Before(intent.UpdatedAt) {
				return workflowruntime.ErrCompensationConflict
			}
		}
		ep, lp := entry.Generation, ledger.Generation
		entry.Status = workflowruntime.CompensationFailed
		entry.ChildResolution = r.ChildResolution
		entry.HandlerFailure = cloneWorkflowFailure(&r.Failure)
		entry.Generation++
		entry.UpdatedAt, entry.CompletedAt = r.At, r.At
		if err := updateWorkflowCompensationEntry(ctx, q, entry, ep); err != nil {
			return err
		}
		for index := range entries {
			if entries[index].ID == entry.ID {
				entries[index] = entry
				break
			}
		}
		ledger.Status = workflowruntime.CompensationRunning
		ledger.Generation++
		ledger.UpdatedAt = r.At
		allTerminal := completeWorkflowCompensationLedger(&ledger, entries, r.At)
		if err := updateWorkflowCompensationLedger(ctx, q, ledger, lp); err != nil {
			return err
		}
		invocation := entry.Handler
		if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Invocation: &invocation, Type: workflowruntime.EventCompensationFinished, OccurredAt: r.At, Attributes: map[string]string{"entry_id": entry.ID, "status": string(entry.Status), "stage": "binding"}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return err
		}
		if allTerminal {
			if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: r.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
				return err
			}
		}
		out = workflowruntime.SealCompensationEntryResult{Ledger: ledger, Entry: entry}
		return nil
	})
	return out, err
}

func completeWorkflowCompensationLedger(ledger *workflowruntime.CompensationLedgerSnapshot, entries []workflowruntime.CompensationEntrySnapshot, at time.Time) bool {
	allTerminal, succeeded, failed, partial, canceled := true, 0, 0, 0, 0
	for _, candidate := range entries {
		if !candidate.Status.Terminal() {
			allTerminal = false
		}
		switch candidate.Status {
		case workflowruntime.CompensationCanceled:
			canceled++
		case workflowruntime.CompensationFailed:
			failed++
		case workflowruntime.CompensationPartial:
			partial++
		case workflowruntime.CompensationSucceeded:
			succeeded++
		}
	}
	if !allTerminal {
		return false
	}
	ledger.Status = workflowruntime.CompensationTerminal
	ledger.CompletedAt = at
	switch {
	case ledger.CancelReason != "" || canceled > 0:
		ledger.Outcome = workflowruntime.CompensationOutcomeCanceled
	case failed == 0 && partial == 0:
		ledger.Outcome = workflowruntime.CompensationOutcomeSucceeded
	case succeeded == 0 && partial == 0:
		ledger.Outcome = workflowruntime.CompensationOutcomeFailed
	default:
		ledger.Outcome = workflowruntime.CompensationOutcomePartial
	}
	if len(ledger.Cycles) != 0 {
		ledger.Cycles[len(ledger.Cycles)-1].Outcome = ledger.Outcome
		ledger.Cycles[len(ledger.Cycles)-1].CompletedAt = at
		ledger.Cycles[len(ledger.Cycles)-1].CancelReason = ledger.CancelReason
	}
	return true
}

func (s *WorkflowStateStore) SealCompensationEntry(ctx context.Context, r workflowruntime.SealCompensationEntryRequest) (workflowruntime.SealCompensationEntryResult, error) {
	var out workflowruntime.SealCompensationEntryResult
	err := s.write(ctx, "seal workflow compensation", func(q workflowSQL) error {
		ledger, err := loadWorkflowCompensationLedger(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		entry, err := loadWorkflowCompensationEntry(ctx, q, r.RunID, r.EntryID)
		if err != nil {
			return err
		}
		if entry.Status.Terminal() {
			out = workflowruntime.SealCompensationEntryResult{Ledger: ledger, Entry: entry}
			return nil
		}
		if ledger.Generation != r.ExpectedLedgerGeneration || entry.Generation != r.ExpectedEntryGeneration {
			return workflowruntime.ErrCASMismatch
		}
		node, err := loadWorkflowNode(ctx, q, entry.Handler)
		if err != nil {
			return err
		}
		if node.Generation != r.ExpectedNodeGeneration || !node.Status.Terminal() {
			return workflowruntime.ErrCompensationPending
		}
		if r.At.Before(ledger.UpdatedAt) || r.At.Before(entry.UpdatedAt) || r.At.Before(node.UpdatedAt) {
			return workflowInvalid(errors.New("compensation seal time regresses durable state"))
		}
		entry.HandlerOutputs = cloneWorkflowValueRef(node.Outputs)
		switch node.Status {
		case workflowruntime.NodeSucceeded:
			switch entry.ChildResolution {
			case workflowruntime.CompensationChildCanceled:
				entry.Status = workflowruntime.CompensationCanceled
			case workflowruntime.CompensationChildPartial, workflowruntime.CompensationChildFailed:
				entry.Status = workflowruntime.CompensationPartial
			default:
				entry.Status = workflowruntime.CompensationSucceeded
			}
		case workflowruntime.NodeCanceled:
			entry.Status = workflowruntime.CompensationCanceled
		default:
			entry.Status = workflowruntime.CompensationFailed
		}
		if node.LatestAttempt > 0 {
			attempt, err := loadWorkflowAttempt(ctx, q, workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
			if err != nil {
				return err
			}
			entry.HandlerFailure = cloneWorkflowFailure(attempt.Failure)
		}
		ep, lp := entry.Generation, ledger.Generation
		entry.Generation++
		entry.UpdatedAt = r.At
		entry.CompletedAt = r.At
		if err := updateWorkflowCompensationEntry(ctx, q, entry, ep); err != nil {
			return err
		}
		entries, err := listWorkflowCompensationEntries(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		for index := range entries {
			if entries[index].ID == entry.ID {
				entries[index] = entry
				break
			}
		}
		ledger.Generation++
		ledger.UpdatedAt = r.At
		all := completeWorkflowCompensationLedger(&ledger, entries, r.At)
		if err := updateWorkflowCompensationLedger(ctx, q, ledger, lp); err != nil {
			return err
		}
		inv := entry.Handler
		if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Invocation: &inv, Type: workflowruntime.EventCompensationFinished, OccurredAt: r.At, Attributes: map[string]string{"entry_id": entry.ID, "status": string(entry.Status)}, Values: entry.HandlerOutputs, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return err
		}
		if all {
			if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: r.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
				return err
			}
		}
		out = workflowruntime.SealCompensationEntryResult{Ledger: ledger, Entry: entry}
		return nil
	})
	return out, err
}

func (s *WorkflowStateStore) CancelCompensation(ctx context.Context, r workflowruntime.CancelCompensationRequest) (workflowruntime.CompensationLedgerSnapshot, error) {
	var out workflowruntime.CompensationLedgerSnapshot
	err := s.write(ctx, "cancel workflow compensation", func(q workflowSQL) error {
		if r.ExpectedLedgerGeneration == 0 || strings.TrimSpace(r.IdempotencyKey) == "" || strings.TrimSpace(r.Reason) == "" || r.At.IsZero() {
			return workflowInvalid(errors.New("compensation cancellation requires generation, key, reason, and time"))
		}
		replayed, digest, err := workflowCompensationReplay(ctx, q, r.IdempotencyKey, "cancel", r, &out)
		if err != nil {
			return err
		}
		if replayed {
			out, err = loadWorkflowCompensationLedger(ctx, q, r.RunID)
			return err
		}
		ledger, err := loadWorkflowCompensationLedger(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		if ledger.Generation != r.ExpectedLedgerGeneration || ledger.Status == workflowruntime.CompensationTerminal {
			return workflowruntime.ErrCASMismatch
		}
		if ledger.Status != workflowruntime.CompensationFrozen && ledger.Status != workflowruntime.CompensationRunning {
			return workflowruntime.ErrCompensationConflict
		}
		if r.At.Before(ledger.UpdatedAt) {
			return workflowInvalid(errors.New("compensation cancellation time regresses ledger"))
		}
		entries, err := listWorkflowCompensationEntries(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		active := false
		for index := range entries {
			entry := entries[index]
			if r.At.Before(entry.UpdatedAt) {
				return workflowInvalid(errors.New("compensation cancellation time regresses entry"))
			}
			if entry.Status == workflowruntime.CompensationActive {
				active = true
				node, loadErr := loadWorkflowNode(ctx, q, entry.Handler)
				if loadErr != nil {
					return loadErr
				}
				if r.At.Before(node.UpdatedAt) {
					return workflowInvalid(errors.New("compensation cancellation time regresses handler"))
				}
				switch node.Status {
				case workflowruntime.NodePending, workflowruntime.NodeReady, workflowruntime.NodeBlocked:
					collector := workflowCancellationCollector{}
					reason := workflowruntime.Failure{Code: "compensation_canceled", Message: r.Reason}
					if err := cancelWorkflowUnstartedNode(ctx, q, node, r.At, reason, &collector); err != nil {
						return err
					}
					active = true
				case workflowruntime.NodeRunning:
					attempt := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
					if _, err := ensureWorkflowCancellationIntent(ctx, q, r.RunID, workflowruntime.CancellationRunningAttempt, &attempt, "", r.At); err != nil {
						return err
					}
				case workflowruntime.NodeWaiting:
					collector := workflowCancellationCollector{}
					reason := workflowruntime.Failure{Code: "compensation_canceled", Message: r.Reason}
					handled, cancelErr := cancelWorkflowWaitingNode(ctx, q, node, r.At, reason, r.IdempotencyKey, &collector)
					if cancelErr != nil {
						return cancelErr
					}
					if !handled {
						attempt := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
						if _, err := ensureWorkflowCancellationIntent(ctx, q, r.RunID, workflowruntime.CancellationRunningAttempt, &attempt, "", r.At); err != nil {
							return err
						}
					}
				}
				continue
			}
			if entry.Status != workflowruntime.CompensationEligible && entry.Status != workflowruntime.CompensationPending {
				continue
			}
			prior := entry.Generation
			entry.Status = workflowruntime.CompensationCanceled
			entry.Generation++
			entry.UpdatedAt, entry.CompletedAt = r.At, r.At
			if err := updateWorkflowCompensationEntry(ctx, q, entry, prior); err != nil {
				return err
			}
		}
		prior := ledger.Generation
		ledger.CancelReason = r.Reason
		ledger.Generation++
		ledger.UpdatedAt = r.At
		if !active {
			ledger.Status, ledger.Outcome, ledger.CompletedAt = workflowruntime.CompensationTerminal, workflowruntime.CompensationOutcomeCanceled, r.At
			if len(ledger.Cycles) != 0 {
				ledger.Cycles[len(ledger.Cycles)-1].Outcome, ledger.Cycles[len(ledger.Cycles)-1].CompletedAt = ledger.Outcome, r.At
				ledger.Cycles[len(ledger.Cycles)-1].CancelReason = r.Reason
			}
		}
		if err := updateWorkflowCompensationLedger(ctx, q, ledger, prior); err != nil {
			return err
		}
		if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Type: workflowruntime.EventCompensationCanceled, OccurredAt: r.At, Attributes: map[string]string{"reason": r.Reason}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return err
		}
		if ledger.Status == workflowruntime.CompensationTerminal {
			if _, err := appendWorkflowEvent(ctx, q, workflowruntime.AppendEventRequest{RunID: r.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: r.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
				return err
			}
		}
		out = ledger
		return insertWorkflowCompensationReplay(ctx, q, r.IdempotencyKey, r.RunID, "cancel", digest, out, r.At)
	})
	return out, err
}

func (s *WorkflowStateStore) RetryCompensation(ctx context.Context, r workflowruntime.RetryCompensationRequest) (workflowruntime.CompensationLedgerSnapshot, error) {
	var out workflowruntime.CompensationLedgerSnapshot
	err := s.write(ctx, "retry workflow compensation", func(q workflowSQL) error {
		if r.ExpectedLedgerGeneration == 0 || strings.TrimSpace(r.IdempotencyKey) == "" || values.ValidateDigest(r.Attestation) != nil || r.At.IsZero() {
			return workflowInvalid(errors.New("compensation retry requires key, attestation, and time"))
		}
		replayed, digest, err := workflowCompensationReplay(ctx, q, r.IdempotencyKey, "retry", r, &out)
		if err != nil {
			return err
		}
		if replayed {
			out, err = loadWorkflowCompensationLedger(ctx, q, r.RunID)
			return err
		}
		ledger, err := loadWorkflowCompensationLedger(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		if ledger.Generation != r.ExpectedLedgerGeneration {
			return workflowCAS("compensation retry ledger", r.ExpectedLedgerGeneration, ledger.Generation)
		}
		if ledger.Status != workflowruntime.CompensationTerminal || ledger.Outcome == workflowruntime.CompensationOutcomeSucceeded {
			return workflowInvalid(errors.New("compensation outcome is not retryable"))
		}
		if ledger.Trigger != graph.CompensationManual {
			intent, intentErr := loadWorkflowTerminalIntent(ctx, q, r.RunID)
			if intentErr != nil {
				return workflowInvalid(errors.New("automatic compensation retry requires its pending terminal intent"))
			}
			if intent.Status != workflowruntime.TerminalIntentPending || !intent.CompensationRequired {
				return workflowInvalid(errors.New("automatic compensation retry requires its pending terminal intent"))
			}
			for _, finalizer := range intent.Finalizers {
				node, nodeErr := loadWorkflowNode(ctx, q, finalizer.Invocation)
				if nodeErr != nil {
					return workflowInvalid(errors.New("automatic compensation retry requires every finalizer invocation"))
				}
				if node.Status != workflowruntime.NodePending || node.LatestAttempt != 0 || node.Inputs != nil || node.Outputs != nil || node.Wait != nil || node.Lease != nil {
					return workflowInvalid(errors.New("automatic compensation retry requires pristine finalizers"))
				}
				var attemptCount int
				if countErr := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM workflow_attempts WHERE run_id=? AND node_id=? AND iteration=?`, finalizer.Invocation.RunID, finalizer.Invocation.NodeID, finalizer.Invocation.Iteration).Scan(&attemptCount); countErr != nil {
					return countErr
				}
				if attemptCount != 0 {
					return workflowInvalid(errors.New("automatic compensation retry requires finalizers with no attempts"))
				}
			}
		}
		if r.At.Before(ledger.UpdatedAt) {
			return workflowInvalid(errors.New("compensation retry time regresses ledger"))
		}
		entries, err := listWorkflowCompensationEntries(ctx, q, r.RunID)
		if err != nil {
			return err
		}
		cycle := len(ledger.Cycles)
		for index := range entries {
			entry := entries[index]
			if entry.Status == workflowruntime.CompensationSucceeded {
				continue
			}
			if r.At.Before(entry.UpdatedAt) {
				return workflowInvalid(errors.New("compensation retry time regresses entry"))
			}
			prior := entry.Generation
			entry.History = append(entry.History, workflowruntime.CompensationEntryHistory{Cycle: cycle, Handler: entry.Handler, Status: entry.Status, ChildResolution: entry.ChildResolution, Outputs: cloneWorkflowValueRef(entry.HandlerOutputs), Failure: cloneWorkflowFailure(entry.HandlerFailure), CompletedAt: entry.CompletedAt})
			entry.Handler.Iteration = fmt.Sprintf("comp:%s:retry:%d", entry.ID, entry.Generation+1)
			entry.Status, entry.ChildResolution, entry.HandlerOutputs, entry.HandlerFailure, entry.CompletedAt = workflowruntime.CompensationPending, "", nil, nil, time.Time{}
			entry.Generation++
			entry.UpdatedAt = r.At
			if err := updateWorkflowCompensationEntry(ctx, q, entry, prior); err != nil {
				return err
			}
		}
		prior := ledger.Generation
		ledger.Status, ledger.Outcome, ledger.CancelReason, ledger.CompletedAt = workflowruntime.CompensationFrozen, "", "", time.Time{}
		ledger.Cycles = append(ledger.Cycles, workflowruntime.CompensationCycle{Number: cycle + 1, Attestation: r.Attestation, StartedAt: r.At})
		ledger.Generation++
		ledger.UpdatedAt = r.At
		if err := updateWorkflowCompensationLedger(ctx, q, ledger, prior); err != nil {
			return err
		}
		out = ledger
		return insertWorkflowCompensationReplay(ctx, q, r.IdempotencyKey, r.RunID, "retry", digest, out, r.At)
	})
	return out, err
}
func (s *WorkflowStateStore) RecoverCompensation(ctx context.Context, limit int) ([]workflowruntime.CompensationLedgerSnapshot, error) {
	if limit < 0 {
		return nil, workflowInvalid(errors.New("recovery limit must not be negative"))
	}
	query := `SELECT run_id FROM workflow_compensation_ledgers WHERE status IN ('frozen','running') ORDER BY updated_at,run_id`
	args := []any{}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []workflowruntime.RunID
	for rows.Next() {
		var id workflowruntime.RunID
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	// Open() deliberately limits SQLite to one connection. Drain and close the
	// recovery cursor before loading projections so recovery cannot deadlock
	// itself waiting for the cursor's connection.
	result := make([]workflowruntime.CompensationLedgerSnapshot, 0, len(ids))
	for _, id := range ids {
		ledger, err := loadWorkflowCompensationLedger(ctx, s.db, id)
		if err != nil {
			return nil, err
		}
		result = append(result, ledger)
	}
	return result, nil
}
