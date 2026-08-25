package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var _ workflowruntime.CompensationStore = (*Store)(nil)

func (s *Store) runAllowsExecutionLocked(id workflowruntime.NodeInvocationID) bool {
	run, exists := s.runs[id.RunID]
	if !exists {
		return false
	}
	if run.Status.Active() {
		return true
	}
	node, exists := s.nodes[id]
	if !exists || node.Phase != workflowruntime.InvocationCompensation {
		return false
	}
	ledger, exists := s.compensationLedgers[id.RunID]
	return exists && ledger.Trigger == graph.CompensationManual && ledger.Status != workflowruntime.CompensationTerminal
}

func (s *Store) FinishCompensableAttempt(ctx context.Context, request workflowruntime.FinishCompensableAttemptRequest) (workflowruntime.FinishCompensableAttemptResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, err
	}
	if err := validateFinishAttemptRequest(request.Finish); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	if request.Finish.NextNodeStatus != request.Finish.AttemptStatus {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.New("applied compensation receipt requires a terminal forward node"))
	}
	eligibility := request.Eligibility
	if err := values.ValidateDigest(eligibility.PlanDigest); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	if err := workflowruntime.ValidateCompensationHandlerNodeID(eligibility.HandlerNodeID); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	digest, evidenceErr := workflowruntime.CompensationEvidenceDigest(eligibility.Evidence)
	if evidenceErr != nil || eligibility.Receipt.Operation != eligibility.Evidence.Operation {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.Join(evidenceErr, errors.New("compensation receipt operation differs from evidence")))
	}
	if err := values.ValidatePersistableSet(eligibility.Receipt.Values); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	if err := values.ValidateValueSetSchema(eligibility.Evidence.ReceiptSchema, eligibility.Receipt.Values); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	if workflowruntime.RunID(eligibility.Receipt.ChildRunID) != eligibility.ChildRunID {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.New("compensation receipt child run differs from eligibility"))
	}
	if err := workflowruntime.ValidateCompensationChildRunID(eligibility.ChildRunID); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	if eligibility.ChildRunID != "" && eligibility.ChildRunID == request.Finish.InvocationID.RunID {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.New("compensation child run cannot be the owning run"))
	}
	if len(eligibility.OriginalOutputs) != 0 {
		if err := values.ValidatePersistableSet(eligibility.OriginalOutputs); err != nil {
			return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
		}
	}
	if len(eligibility.OriginalError) != 0 {
		if err := values.ValidatePersistableSet(eligibility.OriginalError); err != nil {
			return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	txn := snapshotCompensationTxn(s)
	committed := false
	defer func() {
		if !committed {
			txn.restore(s)
		}
	}()
	run, ok := s.runs[request.Finish.InvocationID.RunID]
	if !ok {
		return workflowruntime.FinishCompensableAttemptResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if run.Plan.Digest != eligibility.PlanDigest {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.New("compensation plan digest differs from run"))
	}
	source, ok := s.nodes[request.Finish.InvocationID]
	if !ok {
		return workflowruntime.FinishCompensableAttemptResult{}, fmt.Errorf("%w: node invocation", workflowruntime.ErrNotFound)
	}
	if source.Phase != workflowruntime.InvocationForward {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.New("compensation eligibility requires a forward invocation"))
	}
	attemptID := workflowruntime.AttemptID{Invocation: request.Finish.InvocationID, Number: request.Finish.AttemptNumber}
	entryID := workflowruntime.CompensationEntryID(attemptID)
	if prior, exists := s.compensationEntries[run.ID][entryID]; exists {
		attempt, attemptOK := s.attempts[attemptID]
		if attemptOK && s.compensationEligibilityReplayMatchesLocked(request, digest, prior, source, attempt) {
			return workflowruntime.FinishCompensableAttemptResult{Finish: workflowruntime.FinishNodeAttemptResult{Node: cloneNode(source), Attempt: cloneAttempt(attempt)}, Ledger: cloneCompensationLedger(s.compensationLedgers[run.ID]), Entry: cloneCompensationEntry(prior)}, nil
		}
		return workflowruntime.FinishCompensableAttemptResult{}, fmt.Errorf("%w: compensation eligibility replay differs", workflowruntime.ErrIdempotencyConflict)
	}
	if err := workflowruntime.ValidateCompensationTerminalEvidence(request.Finish.AttemptStatus, eligibility.OriginalError); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	if eligibility.ChildRunID != "" {
		matches := 0
		for _, link := range s.childRuns[run.ID] {
			if link.Invocation == request.Finish.InvocationID && link.ChildRunID == eligibility.ChildRunID {
				matches++
			}
		}
		if matches != 1 {
			return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.New("compensation child receipt requires one exact durable child run link"))
		}
	}
	finished, err := s.finishNodeAttemptLocked(request.Finish)
	if err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, err
	}
	receiptRef, err := s.storeCompensationValuesLocked("compensation-receipt", attemptID, eligibility.Receipt.Values)
	if err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, err
	}
	var outputRef, errorRef *values.ValueSetRef
	if len(eligibility.OriginalOutputs) != 0 {
		ref, valueErr := s.storeCompensationValuesLocked("compensation-original-output", attemptID, eligibility.OriginalOutputs)
		if valueErr != nil {
			return workflowruntime.FinishCompensableAttemptResult{}, valueErr
		}
		outputRef = &ref
	} else if finished.Attempt.Outputs != nil {
		outputRef = cloneValueSetRef(finished.Attempt.Outputs)
	}
	if len(eligibility.OriginalError) != 0 {
		ref, valueErr := s.storeCompensationValuesLocked("compensation-original-error", attemptID, eligibility.OriginalError)
		if valueErr != nil {
			return workflowruntime.FinishCompensableAttemptResult{}, valueErr
		}
		errorRef = &ref
	}
	ledger, exists := s.compensationLedgers[run.ID]
	if !exists {
		ledger = workflowruntime.CompensationLedgerSnapshot{RunID: run.ID, PlanDigest: run.Plan.Digest, Status: workflowruntime.CompensationCollecting, Generation: 1, CreatedAt: request.Finish.At, UpdatedAt: request.Finish.At}
	} else {
		if ledger.Status != workflowruntime.CompensationCollecting {
			return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.New("frozen compensation ledger rejects new eligibility"))
		}
		if request.Finish.At.Before(ledger.UpdatedAt) {
			return workflowruntime.FinishCompensableAttemptResult{}, invalid(errors.New("compensation eligibility time regresses ledger"))
		}
		ledger.Generation++
		ledger.UpdatedAt = request.Finish.At
	}
	entry := workflowruntime.CompensationEntrySnapshot{ID: entryID, RunID: run.ID, PlanDigest: run.Plan.Digest, Source: attemptID.Invocation, SourceAttempt: attemptID,
		Handler: workflowruntime.CompensationHandlerID(run.ID, eligibility.HandlerNodeID, entryID), Status: workflowruntime.CompensationEligible,
		Operation: eligibility.Evidence.Operation, EvidenceDigest: digest, OriginalInputs: cloneValueSetRef(finished.Attempt.Inputs), OriginalOutputs: outputRef,
		OriginalError: errorRef, Receipt: receiptRef, ChildRunID: eligibility.ChildRunID, Generation: 1, CreatedAt: request.Finish.At, UpdatedAt: request.Finish.At}
	if err := ledger.Validate(); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	if err := entry.Validate(); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, invalid(err)
	}
	if s.compensationEntries[run.ID] == nil {
		s.compensationEntries[run.ID] = make(map[string]workflowruntime.CompensationEntrySnapshot)
	}
	s.compensationLedgers[run.ID], s.compensationEntries[run.ID][entry.ID] = ledger, entry
	invocation, eventAttempt := entry.Source, entry.SourceAttempt
	if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: run.ID, Invocation: &invocation, Attempt: &eventAttempt, Type: workflowruntime.EventCompensationEligible, OccurredAt: request.Finish.At,
		Attributes: map[string]string{"entry_id": entry.ID, "handler": entry.Handler.NodeID, "operation": entry.Operation}, Values: &receiptRef, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		return workflowruntime.FinishCompensableAttemptResult{}, err
	}
	committed = true
	return workflowruntime.FinishCompensableAttemptResult{Finish: finished, Ledger: cloneCompensationLedger(ledger), Entry: cloneCompensationEntry(entry)}, nil
}

func (s *Store) compensationEligibilityReplayMatchesLocked(request workflowruntime.FinishCompensableAttemptRequest, evidenceDigest string, prior workflowruntime.CompensationEntrySnapshot, node workflowruntime.NodeInvocationSnapshot, attempt workflowruntime.AttemptSnapshot) bool {
	finish, eligibility := request.Finish, request.Eligibility
	attemptID := workflowruntime.AttemptID{Invocation: finish.InvocationID, Number: finish.AttemptNumber}
	if prior.RunID != finish.InvocationID.RunID || prior.PlanDigest != eligibility.PlanDigest || prior.Source != finish.InvocationID || prior.SourceAttempt != attemptID ||
		prior.Handler != workflowruntime.CompensationHandlerID(prior.RunID, eligibility.HandlerNodeID, prior.ID) || prior.Operation != eligibility.Evidence.Operation ||
		prior.EvidenceDigest != evidenceDigest || prior.ChildRunID != eligibility.ChildRunID || !prior.CreatedAt.Equal(finish.At) ||
		!equalValueSetRef(prior.OriginalInputs, attempt.Inputs) || !s.compensationValuesMatchLocked(prior.Receipt, eligibility.Receipt.Values) {
		return false
	}
	if len(eligibility.OriginalOutputs) != 0 {
		if prior.OriginalOutputs == nil || !s.compensationValuesMatchLocked(*prior.OriginalOutputs, eligibility.OriginalOutputs) {
			return false
		}
	} else if !equalValueSetRef(prior.OriginalOutputs, attempt.Outputs) {
		return false
	}
	if len(eligibility.OriginalError) != 0 {
		if prior.OriginalError == nil || !s.compensationValuesMatchLocked(*prior.OriginalError, eligibility.OriginalError) {
			return false
		}
	} else if prior.OriginalError != nil {
		return false
	}
	return attempt.ID == attemptID && attempt.Status == finish.AttemptStatus && attempt.Generation == finish.ExpectedAttemptGeneration+1 &&
		equalValueSetRef(attempt.Outputs, finish.Outputs) && equalFailurePointers(attempt.Failure, finish.Failure) && attempt.FinishedAt.Equal(finish.At) && attempt.UpdatedAt.Equal(finish.At) &&
		node.ID == finish.InvocationID && node.Status == finish.NextNodeStatus && node.LatestAttempt == finish.AttemptNumber && node.Generation == finish.ExpectedNodeGeneration+1 &&
		equalValueSetRef(node.Outputs, finish.Outputs) && node.Origin == workflowruntime.OriginExecuted && node.UpdatedAt.Equal(finish.At)
}

func (s *Store) compensationValuesMatchLocked(ref values.ValueSetRef, expected values.ValueSet) bool {
	stored, ok := s.valueSets[ref.ID]
	if !ok || stored.ref != ref {
		return false
	}
	digest, err := values.DigestValueSet(expected)
	return err == nil && digest == ref.Digest
}

func (s *Store) FreezeCompensation(ctx context.Context, request workflowruntime.FreezeCompensationRequest) (workflowruntime.FreezeCompensationResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.FreezeCompensationResult{}, err
	}
	if request.ExpectedRunGeneration == 0 || request.ExpectedIntentGeneration == 0 || request.At.IsZero() || strings.TrimSpace(request.IdempotencyKey) == "" || !request.Trigger.Valid() || request.Trigger == graph.CompensationManual || !request.OriginalStatus.Terminal() {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("freeze requires exact run, intent, trigger, status, key, and time"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	txn := snapshotCompensationTxn(s)
	committed := false
	defer func() {
		if !committed {
			txn.restore(s)
		}
	}()
	requestDigest, digestErr := compensationRequestDigest(request)
	if digestErr != nil {
		return workflowruntime.FreezeCompensationResult{}, invalid(digestErr)
	}
	if replayed, replayErr := s.compensationReplayLocked(request.IdempotencyKey, "freeze", request.RunID, requestDigest); replayErr != nil {
		return workflowruntime.FreezeCompensationResult{}, replayErr
	} else if replayed {
		ledger := s.compensationLedgers[request.RunID]
		if ledger.Trigger != request.Trigger || ledger.OriginalStatus != request.OriginalStatus || ledger.PlanDigest != request.PlanDigest {
			return workflowruntime.FreezeCompensationResult{}, fmt.Errorf("%w: compensation freeze replay differs", workflowruntime.ErrIdempotencyConflict)
		}
		return workflowruntime.FreezeCompensationResult{Outcome: workflowruntime.IdempotencyReplayed, Ledger: cloneCompensationLedger(ledger), Entries: cloneCompensationEntries(s.compensationEntries[request.RunID])}, nil
	}
	run, ok := s.runs[request.RunID]
	if !ok {
		return workflowruntime.FreezeCompensationResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	intent, ok := s.terminalIntents[request.RunID]
	if !ok {
		return workflowruntime.FreezeCompensationResult{}, fmt.Errorf("%w: terminal intent", workflowruntime.ErrNotFound)
	}
	if run.Generation != request.ExpectedRunGeneration {
		return workflowruntime.FreezeCompensationResult{}, casMismatch("compensation run", request.ExpectedRunGeneration, run.Generation)
	}
	if intent.Generation != request.ExpectedIntentGeneration || intent.Status != workflowruntime.TerminalIntentPending || intent.IntendedStatus != request.OriginalStatus {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("compensation freeze differs from pending terminal intent"))
	}
	if request.At.Before(run.UpdatedAt) || request.At.Before(intent.UpdatedAt) {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("compensation freeze time regresses run or intent"))
	}
	if !equalValueSetRef(intent.Error, request.OriginalFailure) {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("compensation original failure differs from pending terminal intent"))
	}
	finalizers := make(map[workflowruntime.NodeInvocationID]struct{}, len(intent.Finalizers))
	for _, finalizer := range intent.Finalizers {
		finalizers[finalizer.Invocation] = struct{}{}
	}
	for _, node := range s.nodes {
		if node.ID.RunID != request.RunID || node.Phase != workflowruntime.InvocationForward {
			continue
		}
		if _, finalizer := finalizers[node.ID]; finalizer {
			continue
		}
		if !node.Status.Terminal() {
			return workflowruntime.FreezeCompensationResult{}, workflowruntime.ErrCompensationPending
		}
		if request.At.Before(node.UpdatedAt) {
			return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("compensation freeze time regresses forward node"))
		}
		if node.LatestAttempt > 0 {
			attempt, exists := s.attempts[workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}]
			if !exists || !attempt.Status.Terminal() || attempt.FinishedAt.IsZero() {
				return workflowruntime.FreezeCompensationResult{}, workflowruntime.ErrCompensationPending
			}
			if request.At.Before(attempt.UpdatedAt) {
				return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("compensation freeze time regresses forward attempt"))
			}
		}
	}
	if run.Plan.Digest != request.PlanDigest {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("compensation plan digest differs"))
	}
	ledger, exists := s.compensationLedgers[run.ID]
	if !exists {
		ledger = workflowruntime.CompensationLedgerSnapshot{RunID: run.ID, PlanDigest: run.Plan.Digest, Status: workflowruntime.CompensationCollecting, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
	}
	if ledger.Status != workflowruntime.CompensationCollecting {
		return workflowruntime.FreezeCompensationResult{}, fmt.Errorf("%w: compensation ledger already frozen", workflowruntime.ErrCASMismatch)
	}
	entries := s.compensationEntries[run.ID]
	if request.At.Before(ledger.UpdatedAt) {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("compensation freeze time regresses ledger"))
	}
	for id, entry := range entries {
		if request.At.Before(entry.UpdatedAt) {
			return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("compensation freeze time regresses entry"))
		}
		prerequisiteSet := make(map[string]struct{})
		for _, downstream := range request.Dependencies[entry.Source.NodeID] {
			for candidateID, candidate := range entries {
				if candidate.Source.NodeID == downstream {
					prerequisiteSet[candidateID] = struct{}{}
				}
			}
		}
		prerequisites := make([]string, 0, len(prerequisiteSet))
		for candidateID := range prerequisiteSet {
			prerequisites = append(prerequisites, candidateID)
		}
		sort.Strings(prerequisites)
		entry.Prerequisites, entry.Status, entry.Generation, entry.UpdatedAt = prerequisites, workflowruntime.CompensationPending, entry.Generation+1, request.At
		if err := entry.Validate(); err != nil {
			return workflowruntime.FreezeCompensationResult{}, invalid(err)
		}
		entries[id] = entry
	}
	if err := workflowruntime.ValidateCompensationEntryDependencies(cloneCompensationEntries(entries)); err != nil {
		return workflowruntime.FreezeCompensationResult{}, invalid(err)
	}
	ledger.Trigger, ledger.OriginalStatus, ledger.OriginalFailure, ledger.Generation, ledger.UpdatedAt = request.Trigger, request.OriginalStatus, cloneValueSetRef(request.OriginalFailure), ledger.Generation+1, request.At
	ledger.Cycles = []workflowruntime.CompensationCycle{{Number: 1, StartedAt: request.At}}
	if len(entries) == 0 {
		ledger.Status, ledger.Outcome, ledger.CompletedAt = workflowruntime.CompensationTerminal, workflowruntime.CompensationOutcomeSucceeded, request.At
		ledger.Cycles[0].Outcome, ledger.Cycles[0].CompletedAt = ledger.Outcome, request.At
	} else {
		ledger.Status = workflowruntime.CompensationFrozen
	}
	if err := ledger.Validate(); err != nil {
		return workflowruntime.FreezeCompensationResult{}, invalid(err)
	}
	s.compensationLedgers[run.ID] = ledger
	s.compensationKeys[request.IdempotencyKey] = compensationIdempotencyRecord{operation: "freeze", runID: run.ID, digest: requestDigest}
	if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: run.ID, Type: workflowruntime.EventCompensationFrozen, OccurredAt: request.At, Attributes: map[string]string{"trigger": string(request.Trigger), "entries": fmt.Sprint(len(entries))}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		return workflowruntime.FreezeCompensationResult{}, err
	}
	if ledger.Status == workflowruntime.CompensationTerminal {
		if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: run.ID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: request.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return workflowruntime.FreezeCompensationResult{}, err
		}
	}
	committed = true
	return workflowruntime.FreezeCompensationResult{Outcome: workflowruntime.IdempotencyApplied, Ledger: cloneCompensationLedger(ledger), Entries: cloneCompensationEntries(entries)}, nil
}

func (s *Store) BeginManualCompensation(ctx context.Context, request workflowruntime.BeginManualCompensationRequest) (workflowruntime.FreezeCompensationResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.FreezeCompensationResult{}, err
	}
	if request.ExpectedRunGeneration == 0 || request.At.IsZero() || strings.TrimSpace(request.IdempotencyKey) == "" || values.ValidateDigest(request.Authorization) != nil || request.OriginalStatus != workflowruntime.RunSucceeded {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("manual compensation requires exact run, status, key, authorization, and time"))
	}
	requestDigest, err := compensationRequestDigest(request)
	if err != nil {
		return workflowruntime.FreezeCompensationResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	txn := snapshotCompensationTxn(s)
	committed := false
	defer func() {
		if !committed {
			txn.restore(s)
		}
	}()
	if replayed, replayErr := s.compensationReplayLocked(request.IdempotencyKey, "manual", request.RunID, requestDigest); replayErr != nil {
		return workflowruntime.FreezeCompensationResult{}, replayErr
	} else if replayed {
		ledger := s.compensationLedgers[request.RunID]
		return workflowruntime.FreezeCompensationResult{Outcome: workflowruntime.IdempotencyReplayed, Ledger: cloneCompensationLedger(ledger), Entries: cloneCompensationEntries(s.compensationEntries[request.RunID])}, nil
	}
	run, ok := s.runs[request.RunID]
	if !ok {
		return workflowruntime.FreezeCompensationResult{}, fmt.Errorf("%w: run", workflowruntime.ErrNotFound)
	}
	if run.Generation != request.ExpectedRunGeneration {
		return workflowruntime.FreezeCompensationResult{}, casMismatch("manual compensation run", request.ExpectedRunGeneration, run.Generation)
	}
	if run.Status != workflowruntime.RunSucceeded || run.Status != request.OriginalStatus || run.Plan.Digest != request.PlanDigest {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("manual compensation differs from immutable terminal run"))
	}
	if _, exists := s.terminalIntents[request.RunID]; exists {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("manual compensation cannot overlap terminal-intent cleanup"))
	}
	if request.At.Before(run.UpdatedAt) {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("manual compensation time regresses run"))
	}
	for _, node := range s.nodes {
		if node.ID.RunID != request.RunID || node.Phase != workflowruntime.InvocationForward {
			continue
		}
		if !node.Status.Terminal() {
			return workflowruntime.FreezeCompensationResult{}, workflowruntime.ErrCompensationPending
		}
		if request.At.Before(node.UpdatedAt) {
			return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("manual compensation time regresses forward node"))
		}
		if node.LatestAttempt > 0 {
			attempt, exists := s.attempts[workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}]
			if !exists || !attempt.Status.Terminal() || attempt.FinishedAt.IsZero() {
				return workflowruntime.FreezeCompensationResult{}, workflowruntime.ErrCompensationPending
			}
			if request.At.Before(attempt.UpdatedAt) {
				return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("manual compensation time regresses forward attempt"))
			}
		}
	}
	ledger, exists := s.compensationLedgers[request.RunID]
	if !exists {
		ledger = workflowruntime.CompensationLedgerSnapshot{RunID: request.RunID, PlanDigest: request.PlanDigest, Status: workflowruntime.CompensationCollecting, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
	}
	if ledger.Status != workflowruntime.CompensationCollecting {
		return workflowruntime.FreezeCompensationResult{}, workflowruntime.ErrCompensationConflict
	}
	entries := s.compensationEntries[request.RunID]
	if request.At.Before(ledger.UpdatedAt) {
		return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("manual compensation time regresses ledger"))
	}
	for id, entry := range entries {
		if request.At.Before(entry.UpdatedAt) {
			return workflowruntime.FreezeCompensationResult{}, invalid(errors.New("manual compensation time regresses entry"))
		}
		entry.Prerequisites = compensationPrerequisites(entry, entries, request.Dependencies)
		entry.Status, entry.Generation, entry.UpdatedAt = workflowruntime.CompensationPending, entry.Generation+1, request.At
		if err := entry.Validate(); err != nil {
			return workflowruntime.FreezeCompensationResult{}, invalid(err)
		}
		entries[id] = entry
	}
	if err := workflowruntime.ValidateCompensationEntryDependencies(cloneCompensationEntries(entries)); err != nil {
		return workflowruntime.FreezeCompensationResult{}, invalid(err)
	}
	ledger.Trigger, ledger.OriginalStatus, ledger.Generation, ledger.UpdatedAt = graph.CompensationManual, request.OriginalStatus, ledger.Generation+1, request.At
	ledger.Cycles = []workflowruntime.CompensationCycle{{Number: 1, Attestation: request.Authorization, StartedAt: request.At}}
	if len(entries) == 0 {
		ledger.Status, ledger.Outcome, ledger.CompletedAt = workflowruntime.CompensationTerminal, workflowruntime.CompensationOutcomeSucceeded, request.At
		ledger.Cycles[0].Outcome, ledger.Cycles[0].CompletedAt = ledger.Outcome, request.At
	} else {
		ledger.Status = workflowruntime.CompensationFrozen
	}
	if err := ledger.Validate(); err != nil {
		return workflowruntime.FreezeCompensationResult{}, invalid(err)
	}
	s.compensationLedgers[request.RunID] = ledger
	s.compensationKeys[request.IdempotencyKey] = compensationIdempotencyRecord{operation: "manual", runID: request.RunID, digest: requestDigest}
	if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Type: workflowruntime.EventCompensationFrozen, OccurredAt: request.At, Attributes: map[string]string{"trigger": string(graph.CompensationManual), "entries": fmt.Sprint(len(entries))}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		return workflowruntime.FreezeCompensationResult{}, err
	}
	if ledger.Status == workflowruntime.CompensationTerminal {
		if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: request.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return workflowruntime.FreezeCompensationResult{}, err
		}
	}
	committed = true
	return workflowruntime.FreezeCompensationResult{Outcome: workflowruntime.IdempotencyApplied, Ledger: cloneCompensationLedger(ledger), Entries: cloneCompensationEntries(entries)}, nil
}

func compensationPrerequisites(entry workflowruntime.CompensationEntrySnapshot, entries map[string]workflowruntime.CompensationEntrySnapshot, dependencies map[string][]string) []string {
	set := make(map[string]struct{})
	for _, downstream := range dependencies[entry.Source.NodeID] {
		for candidateID, candidate := range entries {
			if candidate.Source.NodeID == downstream {
				set[candidateID] = struct{}{}
			}
		}
	}
	prerequisites := make([]string, 0, len(set))
	for id := range set {
		prerequisites = append(prerequisites, id)
	}
	sort.Strings(prerequisites)
	return prerequisites
}

func (s *Store) LoadCompensationLedger(ctx context.Context, runID workflowruntime.RunID) (workflowruntime.CompensationLedgerSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ledger, ok := s.compensationLedgers[runID]
	if !ok {
		return workflowruntime.CompensationLedgerSnapshot{}, fmt.Errorf("%w: compensation ledger", workflowruntime.ErrNotFound)
	}
	return cloneCompensationLedger(ledger), nil
}

func (s *Store) ListCompensationEntries(ctx context.Context, runID workflowruntime.RunID) ([]workflowruntime.CompensationEntrySnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.compensationLedgers[runID]; !ok {
		return nil, fmt.Errorf("%w: compensation ledger", workflowruntime.ErrNotFound)
	}
	return cloneCompensationEntries(s.compensationEntries[runID]), nil
}

func (s *Store) LoadCompensationEntryByHandler(ctx context.Context, id workflowruntime.NodeInvocationID) (workflowruntime.CompensationEntrySnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.CompensationEntrySnapshot{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entryID, ok := s.compensationHandlers[id]
	if !ok {
		return workflowruntime.CompensationEntrySnapshot{}, fmt.Errorf("%w: compensation handler", workflowruntime.ErrNotFound)
	}
	entry, ok := s.compensationEntries[id.RunID][entryID]
	if !ok {
		return workflowruntime.CompensationEntrySnapshot{}, fmt.Errorf("%w: compensation entry", workflowruntime.ErrNotFound)
	}
	return cloneCompensationEntry(entry), nil
}

func (s *Store) ActivateCompensationEntry(ctx context.Context, request workflowruntime.ActivateCompensationEntryRequest) (workflowruntime.ActivateCompensationEntryResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.ActivateCompensationEntryResult{}, err
	}
	if err := values.ValidatePersistableSet(request.Inputs); err != nil || request.At.IsZero() {
		return workflowruntime.ActivateCompensationEntryResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	txn := snapshotCompensationTxn(s)
	committed := false
	defer func() {
		if !committed {
			txn.restore(s)
		}
	}()
	ledger, ok := s.compensationLedgers[request.RunID]
	if !ok {
		return workflowruntime.ActivateCompensationEntryResult{}, fmt.Errorf("%w: compensation ledger", workflowruntime.ErrNotFound)
	}
	entry, ok := s.compensationEntries[request.RunID][request.EntryID]
	if !ok {
		return workflowruntime.ActivateCompensationEntryResult{}, fmt.Errorf("%w: compensation entry", workflowruntime.ErrNotFound)
	}
	if ledger.Generation != request.ExpectedLedgerGeneration {
		return workflowruntime.ActivateCompensationEntryResult{}, casMismatch("compensation ledger", request.ExpectedLedgerGeneration, ledger.Generation)
	}
	if entry.Generation != request.ExpectedEntryGeneration {
		return workflowruntime.ActivateCompensationEntryResult{}, casMismatch("compensation entry", request.ExpectedEntryGeneration, entry.Generation)
	}
	if request.At.Before(ledger.UpdatedAt) || request.At.Before(entry.UpdatedAt) {
		return workflowruntime.ActivateCompensationEntryResult{}, invalid(errors.New("compensation activation time regresses ledger or entry"))
	}
	if entry.Status == workflowruntime.CompensationActive {
		if entry.ChildResolution != request.ChildResolution {
			return workflowruntime.ActivateCompensationEntryResult{}, fmt.Errorf("%w: compensation child resolution differs", workflowruntime.ErrIdempotencyConflict)
		}
		node, exists := s.nodes[entry.Handler]
		if !exists {
			return workflowruntime.ActivateCompensationEntryResult{}, invalid(errors.New("active compensation entry has no handler invocation"))
		}
		return workflowruntime.ActivateCompensationEntryResult{Ledger: cloneCompensationLedger(ledger), Entry: cloneCompensationEntry(entry), Node: cloneNode(node)}, nil
	}
	if entry.Status != workflowruntime.CompensationPending || ledger.Status != workflowruntime.CompensationFrozen && ledger.Status != workflowruntime.CompensationRunning {
		return workflowruntime.ActivateCompensationEntryResult{}, invalid(errors.New("compensation entry is not pending"))
	}
	if !request.ChildResolution.Valid() || entry.ChildRunID == "" && request.ChildResolution != "" || entry.ChildRunID != "" && request.ChildResolution == "" {
		return workflowruntime.ActivateCompensationEntryResult{}, invalid(errors.New("compensation activation child resolution is invalid"))
	}
	for _, prerequisite := range entry.Prerequisites {
		if candidate := s.compensationEntries[request.RunID][prerequisite]; !candidate.Status.Terminal() {
			return workflowruntime.ActivateCompensationEntryResult{}, workflowruntime.ErrCompensationPending
		}
	}
	if ledger.Trigger == graph.CompensationManual {
		run, exists := s.runs[request.RunID]
		if !exists || !run.Status.Terminal() {
			return workflowruntime.ActivateCompensationEntryResult{}, invalid(errors.New("manual compensation handler requires immutable terminal run"))
		}
		if request.At.Before(run.UpdatedAt) {
			return workflowruntime.ActivateCompensationEntryResult{}, invalid(errors.New("compensation activation time regresses run"))
		}
	} else {
		intent, exists := s.terminalIntents[request.RunID]
		if !exists || intent.Status != workflowruntime.TerminalIntentPending {
			return workflowruntime.ActivateCompensationEntryResult{}, invalid(errors.New("compensation handler requires pending terminal intent"))
		}
		if request.At.Before(intent.UpdatedAt) {
			return workflowruntime.ActivateCompensationEntryResult{}, invalid(errors.New("compensation activation time regresses terminal intent"))
		}
	}
	inputRef, err := s.storeCompensationHandlerValuesLocked(entry.Handler, request.Inputs)
	if err != nil {
		return workflowruntime.ActivateCompensationEntryResult{}, err
	}
	node := workflowruntime.NodeInvocationSnapshot{ID: entry.Handler, Phase: workflowruntime.InvocationCompensation, Status: workflowruntime.NodeReady, Inputs: &inputRef, Generation: 1, CreatedAt: request.At, UpdatedAt: request.At}
	if _, exists := s.nodes[node.ID]; exists {
		return workflowruntime.ActivateCompensationEntryResult{}, fmt.Errorf("%w: compensation handler invocation", workflowruntime.ErrAlreadyExists)
	}
	if err := node.Validate(); err != nil {
		return workflowruntime.ActivateCompensationEntryResult{}, invalid(err)
	}
	entry.Status, entry.ChildResolution, entry.Generation, entry.UpdatedAt = workflowruntime.CompensationActive, request.ChildResolution, entry.Generation+1, request.At
	ledger.Status, ledger.Generation, ledger.UpdatedAt = workflowruntime.CompensationRunning, ledger.Generation+1, request.At
	if err := entry.Validate(); err != nil {
		return workflowruntime.ActivateCompensationEntryResult{}, invalid(err)
	}
	if err := ledger.Validate(); err != nil {
		return workflowruntime.ActivateCompensationEntryResult{}, invalid(err)
	}
	s.nodes[node.ID], s.compensationHandlers[node.ID], s.compensationEntries[request.RunID][entry.ID], s.compensationLedgers[request.RunID] = node, entry.ID, entry, ledger
	invocation := node.ID
	if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Invocation: &invocation, Type: workflowruntime.EventCompensationReady, OccurredAt: request.At, Attributes: map[string]string{"entry_id": entry.ID}, Values: &inputRef, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		return workflowruntime.ActivateCompensationEntryResult{}, err
	}
	committed = true
	return workflowruntime.ActivateCompensationEntryResult{Ledger: cloneCompensationLedger(ledger), Entry: cloneCompensationEntry(entry), Node: cloneNode(node)}, nil
}

func (s *Store) FailCompensationEntry(ctx context.Context, request workflowruntime.FailCompensationEntryRequest) (workflowruntime.SealCompensationEntryResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, err
	}
	if request.At.IsZero() || request.Failure.Retryable {
		return workflowruntime.SealCompensationEntryResult{}, invalid(errors.New("compensation entry failure requires a permanent failure and time"))
	}
	if err := request.Failure.Validate(); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, invalid(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	txn := snapshotCompensationTxn(s)
	committed := false
	defer func() {
		if !committed {
			txn.restore(s)
		}
	}()
	ledger, ok := s.compensationLedgers[request.RunID]
	if !ok {
		return workflowruntime.SealCompensationEntryResult{}, fmt.Errorf("%w: compensation ledger", workflowruntime.ErrNotFound)
	}
	entry, ok := s.compensationEntries[request.RunID][request.EntryID]
	if !ok {
		return workflowruntime.SealCompensationEntryResult{}, fmt.Errorf("%w: compensation entry", workflowruntime.ErrNotFound)
	}
	if entry.Status.Terminal() {
		return workflowruntime.SealCompensationEntryResult{Ledger: cloneCompensationLedger(ledger), Entry: cloneCompensationEntry(entry)}, nil
	}
	if ledger.Generation != request.ExpectedLedgerGeneration || entry.Generation != request.ExpectedEntryGeneration {
		return workflowruntime.SealCompensationEntryResult{}, fmt.Errorf("%w: compensation failure generation", workflowruntime.ErrCASMismatch)
	}
	if entry.Status != workflowruntime.CompensationPending || ledger.Status != workflowruntime.CompensationFrozen && ledger.Status != workflowruntime.CompensationRunning {
		return workflowruntime.SealCompensationEntryResult{}, workflowruntime.ErrCompensationConflict
	}
	if request.At.Before(ledger.UpdatedAt) || request.At.Before(entry.UpdatedAt) {
		return workflowruntime.SealCompensationEntryResult{}, invalid(errors.New("compensation failure time regresses durable state"))
	}
	if !request.ChildResolution.Valid() || entry.ChildRunID == "" && request.ChildResolution != "" || entry.ChildRunID != "" && request.ChildResolution == "" {
		return workflowruntime.SealCompensationEntryResult{}, invalid(errors.New("compensation failure child resolution is invalid"))
	}
	for _, prerequisite := range entry.Prerequisites {
		if candidate := s.compensationEntries[request.RunID][prerequisite]; !candidate.Status.Terminal() {
			return workflowruntime.SealCompensationEntryResult{}, workflowruntime.ErrCompensationPending
		}
	}
	if ledger.Trigger == graph.CompensationManual {
		run, exists := s.runs[request.RunID]
		if !exists || !run.Status.Terminal() {
			return workflowruntime.SealCompensationEntryResult{}, invalid(errors.New("manual compensation failure requires immutable terminal run"))
		}
		if request.At.Before(run.UpdatedAt) {
			return workflowruntime.SealCompensationEntryResult{}, invalid(errors.New("compensation failure time regresses run"))
		}
	} else {
		intent, exists := s.terminalIntents[request.RunID]
		if !exists || intent.Status != workflowruntime.TerminalIntentPending {
			return workflowruntime.SealCompensationEntryResult{}, invalid(errors.New("compensation failure requires pending terminal intent"))
		}
		if request.At.Before(intent.UpdatedAt) {
			return workflowruntime.SealCompensationEntryResult{}, invalid(errors.New("compensation failure time regresses terminal intent"))
		}
	}
	entry.Status = workflowruntime.CompensationFailed
	entry.ChildResolution = request.ChildResolution
	entry.HandlerFailure = cloneFailure(&request.Failure)
	entry.Generation++
	entry.UpdatedAt, entry.CompletedAt = request.At, request.At
	if err := entry.Validate(); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, invalid(err)
	}
	s.compensationEntries[request.RunID][entry.ID] = entry
	ledger.Status = workflowruntime.CompensationRunning
	ledger.Generation++
	ledger.UpdatedAt = request.At
	allTerminal := completeInmemoryCompensationLedger(&ledger, s.compensationEntries[request.RunID], request.At)
	if err := ledger.Validate(); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, invalid(err)
	}
	s.compensationLedgers[request.RunID] = ledger
	invocation := entry.Handler
	if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Invocation: &invocation, Type: workflowruntime.EventCompensationFinished, OccurredAt: request.At, Attributes: map[string]string{"entry_id": entry.ID, "status": string(entry.Status), "stage": "binding"}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, err
	}
	if allTerminal {
		if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: request.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return workflowruntime.SealCompensationEntryResult{}, err
		}
	}
	committed = true
	return workflowruntime.SealCompensationEntryResult{Ledger: cloneCompensationLedger(ledger), Entry: cloneCompensationEntry(entry)}, nil
}

func completeInmemoryCompensationLedger(ledger *workflowruntime.CompensationLedgerSnapshot, entries map[string]workflowruntime.CompensationEntrySnapshot, at time.Time) bool {
	allTerminal, succeeded, failed, partial, canceled := true, 0, 0, 0, 0
	for _, candidate := range entries {
		if !candidate.Status.Terminal() {
			allTerminal = false
		}
		switch candidate.Status {
		case workflowruntime.CompensationEligible, workflowruntime.CompensationPending, workflowruntime.CompensationActive:
			// Nonterminal entries are accounted for by allTerminal.
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
	ledger.Status, ledger.CompletedAt = workflowruntime.CompensationTerminal, at
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
		ledger.Cycles[len(ledger.Cycles)-1].Outcome, ledger.Cycles[len(ledger.Cycles)-1].CompletedAt = ledger.Outcome, at
		ledger.Cycles[len(ledger.Cycles)-1].CancelReason = ledger.CancelReason
	}
	return true
}

func (s *Store) SealCompensationEntry(ctx context.Context, request workflowruntime.SealCompensationEntryRequest) (workflowruntime.SealCompensationEntryResult, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	txn := snapshotCompensationTxn(s)
	committed := false
	defer func() {
		if !committed {
			txn.restore(s)
		}
	}()
	ledger, ok := s.compensationLedgers[request.RunID]
	if !ok {
		return workflowruntime.SealCompensationEntryResult{}, fmt.Errorf("%w: compensation ledger", workflowruntime.ErrNotFound)
	}
	entry, ok := s.compensationEntries[request.RunID][request.EntryID]
	if !ok {
		return workflowruntime.SealCompensationEntryResult{}, fmt.Errorf("%w: compensation entry", workflowruntime.ErrNotFound)
	}
	if entry.Status.Terminal() {
		return workflowruntime.SealCompensationEntryResult{Ledger: cloneCompensationLedger(ledger), Entry: cloneCompensationEntry(entry)}, nil
	}
	if ledger.Generation != request.ExpectedLedgerGeneration || entry.Generation != request.ExpectedEntryGeneration {
		return workflowruntime.SealCompensationEntryResult{}, fmt.Errorf("%w: compensation seal generation", workflowruntime.ErrCASMismatch)
	}
	node, ok := s.nodes[entry.Handler]
	if !ok {
		return workflowruntime.SealCompensationEntryResult{}, fmt.Errorf("%w: compensation handler", workflowruntime.ErrNotFound)
	}
	if node.Generation != request.ExpectedNodeGeneration || !node.Status.Terminal() {
		return workflowruntime.SealCompensationEntryResult{}, workflowruntime.ErrCompensationPending
	}
	if request.At.Before(ledger.UpdatedAt) || request.At.Before(entry.UpdatedAt) || request.At.Before(node.UpdatedAt) {
		return workflowruntime.SealCompensationEntryResult{}, invalid(errors.New("compensation seal time regresses durable state"))
	}
	entry.HandlerOutputs = cloneValueSetRef(node.Outputs)
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
		if attempt, exists := s.attempts[workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}]; exists {
			entry.HandlerFailure = cloneFailure(attempt.Failure)
		}
	}
	entry.Generation++
	entry.UpdatedAt, entry.CompletedAt = request.At, request.At
	if err := entry.Validate(); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, invalid(err)
	}
	s.compensationEntries[request.RunID][entry.ID] = entry
	ledger.Generation++
	ledger.UpdatedAt = request.At
	allTerminal := completeInmemoryCompensationLedger(&ledger, s.compensationEntries[request.RunID], request.At)
	if err := ledger.Validate(); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, invalid(err)
	}
	s.compensationLedgers[request.RunID] = ledger
	invocation := entry.Handler
	if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Invocation: &invocation, Type: workflowruntime.EventCompensationFinished, OccurredAt: request.At, Attributes: map[string]string{"entry_id": entry.ID, "status": string(entry.Status)}, Values: cloneValueSetRef(entry.HandlerOutputs), Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		return workflowruntime.SealCompensationEntryResult{}, err
	}
	if allTerminal {
		if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: request.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return workflowruntime.SealCompensationEntryResult{}, err
		}
	}
	committed = true
	return workflowruntime.SealCompensationEntryResult{Ledger: cloneCompensationLedger(ledger), Entry: cloneCompensationEntry(entry)}, nil
}

func (s *Store) CancelCompensation(ctx context.Context, request workflowruntime.CancelCompensationRequest) (workflowruntime.CompensationLedgerSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, err
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" || strings.TrimSpace(request.Reason) == "" || request.At.IsZero() {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("compensation cancellation requires key, reason, and time"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	txn := snapshotCompensationTxn(s)
	committed := false
	defer func() {
		if !committed {
			txn.restore(s)
		}
	}()
	ledger, ok := s.compensationLedgers[request.RunID]
	if !ok {
		return workflowruntime.CompensationLedgerSnapshot{}, fmt.Errorf("%w: compensation ledger", workflowruntime.ErrNotFound)
	}
	requestDigest, digestErr := compensationRequestDigest(request)
	if digestErr != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(digestErr)
	}
	if replayed, replayErr := s.compensationReplayLocked(request.IdempotencyKey, "cancel", request.RunID, requestDigest); replayErr != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, replayErr
	} else if replayed {
		return cloneCompensationLedger(ledger), nil
	}
	if ledger.Generation != request.ExpectedLedgerGeneration || ledger.Status == workflowruntime.CompensationTerminal {
		return workflowruntime.CompensationLedgerSnapshot{}, fmt.Errorf("%w: compensation cancellation", workflowruntime.ErrCASMismatch)
	}
	if ledger.Status != workflowruntime.CompensationFrozen && ledger.Status != workflowruntime.CompensationRunning {
		return workflowruntime.CompensationLedgerSnapshot{}, fmt.Errorf("%w: compensation is not active", workflowruntime.ErrCompensationConflict)
	}
	if request.At.Before(ledger.UpdatedAt) {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("compensation cancellation time regresses ledger"))
	}
	active := false
	for id, entry := range s.compensationEntries[request.RunID] {
		if request.At.Before(entry.UpdatedAt) {
			return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("compensation cancellation time regresses entry"))
		}
		if entry.Status == workflowruntime.CompensationEligible || entry.Status == workflowruntime.CompensationPending {
			entry.Status, entry.Generation, entry.UpdatedAt, entry.CompletedAt = workflowruntime.CompensationCanceled, entry.Generation+1, request.At, request.At
			if err := entry.Validate(); err != nil {
				return workflowruntime.CompensationLedgerSnapshot{}, invalid(err)
			}
			s.compensationEntries[request.RunID][id] = entry
		} else if entry.Status == workflowruntime.CompensationActive {
			active = true
			node, exists := s.nodes[entry.Handler]
			if !exists {
				return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("active compensation entry has no handler"))
			}
			if request.At.Before(node.UpdatedAt) {
				return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("compensation cancellation time regresses handler"))
			}
			switch node.Status {
			case workflowruntime.NodePending, workflowruntime.NodeReady, workflowruntime.NodeBlocked:
				collector := cancellationCollector{}
				reason := workflowruntime.Failure{Code: "compensation_canceled", Message: request.Reason}
				if err := s.cancelUnstartedNodeLocked(node, request.At, reason, &collector); err != nil {
					return workflowruntime.CompensationLedgerSnapshot{}, err
				}
			case workflowruntime.NodeRunning:
				attempt := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
				if _, err := s.ensureCancellationIntentLocked(request.RunID, workflowruntime.CancellationRunningAttempt, &attempt, "", "", request.At); err != nil {
					return workflowruntime.CompensationLedgerSnapshot{}, err
				}
			case workflowruntime.NodeWaiting:
				collector := cancellationCollector{}
				reason := workflowruntime.Failure{Code: "compensation_canceled", Message: request.Reason}
				if node.Wait != nil {
					if err := s.cancelGenericWaitLocked(node, request.At, reason, request.IdempotencyKey, &collector); err != nil {
						return workflowruntime.CompensationLedgerSnapshot{}, err
					}
					break
				}
				attempt := workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt}
				if s.hasPendingExternalLocked(node) {
					operation := s.externalOperations[attempt]
					if operation.CancelRequestedAt.IsZero() {
						operation.CancelRequestedAt, operation.UpdatedAt = request.At, request.At
						operation.Generation++
						if err := operation.Validate(); err != nil {
							return workflowruntime.CompensationLedgerSnapshot{}, invalid(err)
						}
						s.externalOperations[attempt] = operation
						invocation := node.ID
						if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Invocation: &invocation, Attempt: &attempt, Type: workflowruntime.EventExternalOperationCancelRequested, OccurredAt: request.At, Attributes: map[string]string{"operation_kind": operation.Ref.Kind, "operation_id": operation.Ref.ID}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
							return workflowruntime.CompensationLedgerSnapshot{}, err
						}
					}
					if _, err := s.ensureCancellationIntentLocked(request.RunID, workflowruntime.CancellationExternalOperation, &attempt, "", "", request.At); err != nil {
						return workflowruntime.CompensationLedgerSnapshot{}, err
					}
					break
				}
				matched, err := s.cancelRetryWaitingNodeLocked(node, request.At, reason, &collector)
				if err != nil {
					return workflowruntime.CompensationLedgerSnapshot{}, err
				}
				if !matched {
					if _, err := s.ensureCancellationIntentLocked(request.RunID, workflowruntime.CancellationRunningAttempt, &attempt, "", "", request.At); err != nil {
						return workflowruntime.CompensationLedgerSnapshot{}, err
					}
				}
			case workflowruntime.NodeSucceeded, workflowruntime.NodeFailed, workflowruntime.NodeSkipped,
				workflowruntime.NodeCanceled, workflowruntime.NodeTimedOut, workflowruntime.NodeCrashed:
				// Terminal handlers are sealed by compensation progression.
			}
		}
	}
	ledger.CancelReason, ledger.Generation, ledger.UpdatedAt = request.Reason, ledger.Generation+1, request.At
	if !active {
		ledger.Status, ledger.Outcome, ledger.CompletedAt = workflowruntime.CompensationTerminal, workflowruntime.CompensationOutcomeCanceled, request.At
		if len(ledger.Cycles) != 0 {
			ledger.Cycles[len(ledger.Cycles)-1].Outcome, ledger.Cycles[len(ledger.Cycles)-1].CompletedAt = ledger.Outcome, request.At
			ledger.Cycles[len(ledger.Cycles)-1].CancelReason = request.Reason
		}
	}
	if err := ledger.Validate(); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(err)
	}
	s.compensationLedgers[request.RunID] = ledger
	s.compensationKeys[request.IdempotencyKey] = compensationIdempotencyRecord{operation: "cancel", runID: request.RunID, digest: requestDigest}
	if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Type: workflowruntime.EventCompensationCanceled, OccurredAt: request.At, Attributes: map[string]string{"reason": request.Reason}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, err
	}
	if ledger.Status == workflowruntime.CompensationTerminal {
		if _, err := s.appendEventLocked(workflowruntime.AppendEventRequest{RunID: request.RunID, Type: workflowruntime.EventCompensationCompleted, OccurredAt: request.At, Attributes: map[string]string{"outcome": string(ledger.Outcome)}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun}); err != nil {
			return workflowruntime.CompensationLedgerSnapshot{}, err
		}
	}
	committed = true
	return cloneCompensationLedger(ledger), nil
}

func (s *Store) RetryCompensation(ctx context.Context, request workflowruntime.RetryCompensationRequest) (workflowruntime.CompensationLedgerSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, err
	}
	if request.ExpectedLedgerGeneration == 0 || strings.TrimSpace(request.IdempotencyKey) == "" || values.ValidateDigest(request.Attestation) != nil || request.At.IsZero() {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("compensation retry requires key, attestation, and time"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	txn := snapshotCompensationTxn(s)
	committed := false
	defer func() {
		if !committed {
			txn.restore(s)
		}
	}()
	ledger, ok := s.compensationLedgers[request.RunID]
	if !ok {
		return workflowruntime.CompensationLedgerSnapshot{}, fmt.Errorf("%w: compensation ledger", workflowruntime.ErrNotFound)
	}
	requestDigest, digestErr := compensationRequestDigest(request)
	if digestErr != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(digestErr)
	}
	if replayed, replayErr := s.compensationReplayLocked(request.IdempotencyKey, "retry", request.RunID, requestDigest); replayErr != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, replayErr
	} else if replayed {
		return cloneCompensationLedger(ledger), nil
	}
	if ledger.Generation != request.ExpectedLedgerGeneration {
		return workflowruntime.CompensationLedgerSnapshot{}, casMismatch("compensation retry ledger", request.ExpectedLedgerGeneration, ledger.Generation)
	}
	if ledger.Status != workflowruntime.CompensationTerminal || ledger.Outcome == workflowruntime.CompensationOutcomeSucceeded {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("compensation outcome is not retryable"))
	}
	if ledger.Trigger != graph.CompensationManual {
		intent, exists := s.terminalIntents[request.RunID]
		if !exists || intent.Status != workflowruntime.TerminalIntentPending || !intent.CompensationRequired {
			return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("automatic compensation retry requires its pending terminal intent"))
		}
		for _, finalizer := range intent.Finalizers {
			node, exists := s.nodes[finalizer.Invocation]
			if !exists || node.Status != workflowruntime.NodePending || node.LatestAttempt != 0 || node.Inputs != nil || node.Outputs != nil || node.Wait != nil || node.Lease != nil {
				return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("automatic compensation retry requires pristine finalizers"))
			}
			for attemptID := range s.attempts {
				if attemptID.Invocation == finalizer.Invocation {
					return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("automatic compensation retry requires finalizers with no attempts"))
				}
			}
		}
	}
	if request.At.Before(ledger.UpdatedAt) {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("compensation retry time regresses ledger"))
	}
	for id, entry := range s.compensationEntries[request.RunID] {
		if entry.Status == workflowruntime.CompensationSucceeded {
			continue
		}
		if request.At.Before(entry.UpdatedAt) {
			return workflowruntime.CompensationLedgerSnapshot{}, invalid(errors.New("compensation retry time regresses entry"))
		}
		delete(s.compensationHandlers, entry.Handler)
		entry.History = append(entry.History, workflowruntime.CompensationEntryHistory{Cycle: len(ledger.Cycles), Handler: entry.Handler, Status: entry.Status, ChildResolution: entry.ChildResolution, Outputs: cloneValueSetRef(entry.HandlerOutputs), Failure: cloneFailure(entry.HandlerFailure), CompletedAt: entry.CompletedAt})
		entry.Handler.Iteration = fmt.Sprintf("comp:%s:retry:%d", entry.ID, entry.Generation+1)
		entry.Status, entry.ChildResolution, entry.HandlerOutputs, entry.HandlerFailure, entry.CompletedAt = workflowruntime.CompensationPending, "", nil, nil, time.Time{}
		entry.Generation++
		entry.UpdatedAt = request.At
		if err := entry.Validate(); err != nil {
			return workflowruntime.CompensationLedgerSnapshot{}, invalid(err)
		}
		s.compensationEntries[request.RunID][id] = entry
	}
	ledger.Status, ledger.Outcome, ledger.CancelReason, ledger.CompletedAt = workflowruntime.CompensationFrozen, "", "", time.Time{}
	ledger.Cycles = append(ledger.Cycles, workflowruntime.CompensationCycle{Number: len(ledger.Cycles) + 1, Attestation: request.Attestation, StartedAt: request.At})
	ledger.Generation++
	ledger.UpdatedAt = request.At
	if err := ledger.Validate(); err != nil {
		return workflowruntime.CompensationLedgerSnapshot{}, invalid(err)
	}
	s.compensationLedgers[request.RunID] = ledger
	s.compensationKeys[request.IdempotencyKey] = compensationIdempotencyRecord{operation: "retry", runID: request.RunID, digest: requestDigest}
	committed = true
	return cloneCompensationLedger(ledger), nil
}

func (s *Store) RecoverCompensation(ctx context.Context, limit int) ([]workflowruntime.CompensationLedgerSnapshot, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, invalid(errors.New("recovery limit must not be negative"))
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []workflowruntime.CompensationLedgerSnapshot
	for _, ledger := range s.compensationLedgers {
		if ledger.Status == workflowruntime.CompensationFrozen || ledger.Status == workflowruntime.CompensationRunning {
			result = append(result, cloneCompensationLedger(ledger))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].UpdatedAt.Equal(result[j].UpdatedAt) {
			return result[i].RunID < result[j].RunID
		}
		return result[i].UpdatedAt.Before(result[j].UpdatedAt)
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *Store) storeCompensationValuesLocked(kind string, attempt workflowruntime.AttemptID, input values.ValueSet) (values.ValueSetRef, error) {
	cloned, err := cloneValueSet(input)
	if err != nil {
		return values.ValueSetRef{}, invalid(err)
	}
	s.nextValueSet++
	id := fmt.Sprintf("values-%012d", s.nextValueSet)
	ref, err := values.NewValueSetRef(id, cloned)
	if err != nil {
		return values.ValueSetRef{}, invalid(err)
	}
	invocation := attempt.Invocation
	s.valueSets[id] = storedValues{ref: ref, owner: workflowruntime.ValueOwner{Kind: kind, RunID: invocation.RunID, Invocation: &invocation, Attempt: &attempt}, values: cloned}
	return ref, nil
}

func (s *Store) storeCompensationHandlerValuesLocked(invocation workflowruntime.NodeInvocationID, input values.ValueSet) (values.ValueSetRef, error) {
	cloned, err := cloneValueSet(input)
	if err != nil {
		return values.ValueSetRef{}, invalid(err)
	}
	s.nextValueSet++
	id := fmt.Sprintf("values-%012d", s.nextValueSet)
	ref, err := values.NewValueSetRef(id, cloned)
	if err != nil {
		return values.ValueSetRef{}, invalid(err)
	}
	s.valueSets[id] = storedValues{ref: ref, owner: workflowruntime.ValueOwner{Kind: "compensation-handler-input", RunID: invocation.RunID, Invocation: &invocation}, values: cloned}
	return ref, nil
}

func cloneCompensationLedger(input workflowruntime.CompensationLedgerSnapshot) workflowruntime.CompensationLedgerSnapshot {
	input.OriginalFailure = cloneValueSetRef(input.OriginalFailure)
	input.Cycles = append([]workflowruntime.CompensationCycle(nil), input.Cycles...)
	return input
}
func cloneCompensationEntry(input workflowruntime.CompensationEntrySnapshot) workflowruntime.CompensationEntrySnapshot {
	input.OriginalInputs = cloneValueSetRef(input.OriginalInputs)
	input.OriginalOutputs = cloneValueSetRef(input.OriginalOutputs)
	input.OriginalError = cloneValueSetRef(input.OriginalError)
	input.HandlerOutputs = cloneValueSetRef(input.HandlerOutputs)
	input.HandlerFailure = cloneFailure(input.HandlerFailure)
	input.Prerequisites = append([]string(nil), input.Prerequisites...)
	input.History = append([]workflowruntime.CompensationEntryHistory(nil), input.History...)
	for index := range input.History {
		input.History[index].Outputs = cloneValueSetRef(input.History[index].Outputs)
		input.History[index].Failure = cloneFailure(input.History[index].Failure)
	}
	return input
}
func cloneCompensationEntries(input map[string]workflowruntime.CompensationEntrySnapshot) []workflowruntime.CompensationEntrySnapshot {
	result := make([]workflowruntime.CompensationEntrySnapshot, 0, len(input))
	for _, entry := range input {
		result = append(result, cloneCompensationEntry(entry))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

type compensationTxnSnapshot struct {
	nodes        map[workflowruntime.NodeInvocationID]workflowruntime.NodeInvocationSnapshot
	attempts     map[workflowruntime.AttemptID]workflowruntime.AttemptSnapshot
	values       map[string]storedValues
	nextValueSet uint64
	events       map[workflowruntime.RunID][]workflowruntime.Event
	ledgers      map[workflowruntime.RunID]workflowruntime.CompensationLedgerSnapshot
	entries      map[workflowruntime.RunID]map[string]workflowruntime.CompensationEntrySnapshot
	handlers     map[workflowruntime.NodeInvocationID]string
	keys         map[string]compensationIdempotencyRecord
	holders      map[workflowruntime.SchedulerResourceID]map[workflowruntime.NodeInvocationID]workflowruntime.SchedulerResourceHolder
	waiters      map[workflowruntime.NodeInvocationID]workflowruntime.SchedulerResourceWaiter
	cancellation cancellationStateBackup
}

func snapshotCompensationTxn(s *Store) compensationTxnSnapshot {
	t := compensationTxnSnapshot{
		nodes: make(map[workflowruntime.NodeInvocationID]workflowruntime.NodeInvocationSnapshot, len(s.nodes)), attempts: make(map[workflowruntime.AttemptID]workflowruntime.AttemptSnapshot, len(s.attempts)),
		values: make(map[string]storedValues, len(s.valueSets)), nextValueSet: s.nextValueSet, events: make(map[workflowruntime.RunID][]workflowruntime.Event, len(s.events)),
		ledgers: make(map[workflowruntime.RunID]workflowruntime.CompensationLedgerSnapshot, len(s.compensationLedgers)), entries: make(map[workflowruntime.RunID]map[string]workflowruntime.CompensationEntrySnapshot, len(s.compensationEntries)), handlers: make(map[workflowruntime.NodeInvocationID]string, len(s.compensationHandlers)),
		keys:    cloneCompensationIdempotencyMap(s.compensationKeys),
		holders: make(map[workflowruntime.SchedulerResourceID]map[workflowruntime.NodeInvocationID]workflowruntime.SchedulerResourceHolder, len(s.schedulerHolders)), waiters: make(map[workflowruntime.NodeInvocationID]workflowruntime.SchedulerResourceWaiter, len(s.schedulerWaiters)),
		cancellation: s.backupCancellationStateLocked(),
	}
	for id, node := range s.nodes {
		t.nodes[id] = cloneNode(node)
	}
	for id, attempt := range s.attempts {
		t.attempts[id] = cloneAttempt(attempt)
	}
	for id, stored := range s.valueSets {
		t.values[id] = stored
	}
	for runID, events := range s.events {
		t.events[runID] = append([]workflowruntime.Event(nil), events...)
	}
	for runID, ledger := range s.compensationLedgers {
		t.ledgers[runID] = cloneCompensationLedger(ledger)
	}
	for runID, entries := range s.compensationEntries {
		cloned := make(map[string]workflowruntime.CompensationEntrySnapshot, len(entries))
		for id, entry := range entries {
			cloned[id] = cloneCompensationEntry(entry)
		}
		t.entries[runID] = cloned
	}
	for id, entryID := range s.compensationHandlers {
		t.handlers[id] = entryID
	}
	for resource, holders := range s.schedulerHolders {
		cloned := make(map[workflowruntime.NodeInvocationID]workflowruntime.SchedulerResourceHolder, len(holders))
		for id, holder := range holders {
			cloned[id] = holder
		}
		t.holders[resource] = cloned
	}
	for id, waiter := range s.schedulerWaiters {
		t.waiters[id] = cloneSchedulerWaiter(waiter)
	}
	return t
}

func (t compensationTxnSnapshot) restore(s *Store) {
	s.restoreCancellationStateLocked(t.cancellation)
	s.nodes, s.attempts, s.valueSets, s.nextValueSet, s.events = t.nodes, t.attempts, t.values, t.nextValueSet, t.events
	s.compensationLedgers, s.compensationEntries, s.compensationHandlers = t.ledgers, t.entries, t.handlers
	s.compensationKeys = t.keys
	s.schedulerHolders, s.schedulerWaiters = t.holders, t.waiters
}

type compensationIdempotencyRecord struct {
	operation string
	runID     workflowruntime.RunID
	digest    string
}

func (s *Store) compensationReplayLocked(key, operation string, runID workflowruntime.RunID, digest string) (bool, error) {
	prior, exists := s.compensationKeys[key]
	if !exists {
		return false, nil
	}
	if prior.operation != operation || prior.runID != runID || prior.digest != digest {
		return false, fmt.Errorf("%w: compensation idempotency key", workflowruntime.ErrIdempotencyConflict)
	}
	return true, nil
}

func cloneCompensationIdempotencyMap(input map[string]compensationIdempotencyRecord) map[string]compensationIdempotencyRecord {
	result := make(map[string]compensationIdempotencyRecord, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func compensationRequestDigest(input any) (string, error) {
	return workflowruntime.CompensationRequestDigest(input)
}
