package appworkflow

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

const maximumCompensationAttestationBytes = 1024

func (h *Host) CompensateWorkflowRun(ctx context.Context, request CompensateWorkflowRunRequest) (WorkflowCompensationResult, error) {
	store, ok := h.state.(runtime.CompensationStore)
	if !ok || nilInterface(store) || h.plans == nil {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: durable compensation is unavailable", ErrInvalidHost)
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: compensation idempotency key is required", ErrInvalidHost)
	}
	identity, err := h.authorizeRunControl(ctx, request.RunID, "compensate", request.Identity, request.Confirmed)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	run, err := h.state.LoadRun(ctx, request.RunID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	if run.Status != runtime.RunSucceeded {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: manual compensation requires an exactly succeeded run", runtime.ErrInvalidCompensation)
	}
	plan, err := h.plans.LoadRecoveryPlan(ctx, run)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	manual := false
	if plan.Plan.Graph.Compensation != nil {
		for _, trigger := range plan.Plan.Graph.Compensation.Triggers {
			manual = manual || trigger == graph.CompensationManual
		}
	}
	if !manual {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: exact plan does not declare manual compensation", runtime.ErrInvalidCompensation)
	}
	authorization := compensationAuthorizationDigest("compensate", identity, request.IdempotencyKey,
		string(run.ID), run.Plan.Digest, fmt.Sprint(run.Generation), string(run.Status))
	begun, err := store.BeginManualCompensation(context.WithoutCancel(ctx), runtime.BeginManualCompensationRequest{
		RunID: run.ID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation,
		OriginalStatus: run.Status, Dependencies: runtime.CompensationDependencies(plan.Plan.Graph),
		IdempotencyKey: request.IdempotencyKey, Authorization: authorization, At: maxTime(h.now(), run.UpdatedAt),
	})
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	if begun.Ledger.Status != runtime.CompensationTerminal {
		progressed, progressErr := (runtime.CompensationCoordinator{Store: h.state, Compensation: store, Plans: h.plans}).Progress(context.WithoutCancel(ctx), run.ID, maxTime(h.now(), begun.Ledger.UpdatedAt))
		if progressErr != nil && !errors.Is(progressErr, runtime.ErrCASMismatch) {
			return WorkflowCompensationResult{}, progressErr
		}
		if progressErr == nil {
			begun.Ledger = progressed.Ledger
		}
	}
	entries, err := store.ListCompensationEntries(ctx, run.ID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	ledger := begun.Ledger
	return h.boundedCompensationResult(ledger, entries), nil
}

func (h *Host) CancelWorkflowCompensation(ctx context.Context, request CancelWorkflowCompensationRequest) (WorkflowCompensationResult, error) {
	store, ok := h.state.(runtime.CompensationStore)
	if !ok || nilInterface(store) {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: durable compensation is unavailable", ErrInvalidHost)
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" || strings.TrimSpace(request.Reason) == "" {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: compensation cancellation requires key and reason", ErrInvalidHost)
	}
	if _, err := h.authorizeRunControl(ctx, request.RunID, "cancel_compensation", request.Identity, request.Confirmed); err != nil {
		return WorkflowCompensationResult{}, err
	}
	ledger, err := store.LoadCompensationLedger(ctx, request.RunID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	entries, err := store.ListCompensationEntries(ctx, request.RunID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	at := maxTime(h.now(), ledger.UpdatedAt)
	for _, entry := range entries {
		at = maxTime(at, entry.UpdatedAt)
		if entry.Status == runtime.CompensationActive {
			node, loadErr := h.state.LoadNodeInvocation(ctx, entry.Handler)
			if loadErr != nil {
				return WorkflowCompensationResult{}, loadErr
			}
			at = maxTime(at, node.UpdatedAt)
		}
	}
	ledger, err = store.CancelCompensation(context.WithoutCancel(ctx), runtime.CancelCompensationRequest{RunID: request.RunID, ExpectedLedgerGeneration: ledger.Generation, IdempotencyKey: request.IdempotencyKey, Reason: request.Reason, At: at})
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	if h.cancellation != nil {
		_, failures, recoverErr := h.cancellation.Recover(ctx, runtime.CancellationIntentQuery{RunID: request.RunID, Limit: h.batchLimit})
		if recoverErr != nil || len(failures) != 0 {
			return WorkflowCompensationResult{}, errors.Join(recoverErr, errors.Join(failures...))
		}
	}
	if ledger.Status != runtime.CompensationTerminal {
		progressed, progressErr := (runtime.CompensationCoordinator{Store: h.state, Compensation: store, Plans: h.plans}).Progress(context.WithoutCancel(ctx), request.RunID, maxTime(h.now(), ledger.UpdatedAt))
		if progressErr != nil && !errors.Is(progressErr, runtime.ErrCASMismatch) && !errors.Is(progressErr, runtime.ErrCompensationPending) {
			return WorkflowCompensationResult{}, progressErr
		}
		if progressErr == nil {
			ledger = progressed.Ledger
		}
	}
	ledger, err = store.LoadCompensationLedger(ctx, request.RunID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	entries, err = store.ListCompensationEntries(ctx, request.RunID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	return h.boundedCompensationResult(ledger, entries), nil
}

func (h *Host) RetryWorkflowCompensation(ctx context.Context, request RetryWorkflowCompensationRequest) (WorkflowCompensationResult, error) {
	store, ok := h.state.(runtime.CompensationStore)
	if !ok || nilInterface(store) {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: durable compensation is unavailable", ErrInvalidHost)
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" || !boundedAttestation(request.Attestation) {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: compensation retry requires key and attestation", ErrInvalidHost)
	}
	identity, err := h.authorizeRunControl(ctx, request.RunID, "retry_compensation", request.Identity, request.Confirmed)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	current, err := store.LoadCompensationLedger(ctx, request.RunID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	attestation := compensationAuthorizationDigest("retry_compensation", identity, request.IdempotencyKey,
		string(current.RunID), current.PlanDigest, values.SHA256Digest([]byte(request.Attestation)))
	ledger, err := store.RetryCompensation(context.WithoutCancel(ctx), runtime.RetryCompensationRequest{RunID: request.RunID, ExpectedLedgerGeneration: current.Generation, IdempotencyKey: request.IdempotencyKey, Attestation: attestation, At: maxTime(h.now(), current.UpdatedAt)})
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	entries, err := store.ListCompensationEntries(ctx, request.RunID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	return h.boundedCompensationResult(ledger, entries), nil
}

func (h *Host) InspectWorkflowCompensation(ctx context.Context, request InspectWorkflowCompensationRequest) (WorkflowCompensationResult, error) {
	store, ok := h.state.(runtime.CompensationStore)
	if !ok || nilInterface(store) || request.Limit < 0 {
		return WorkflowCompensationResult{}, fmt.Errorf("%w: invalid compensation inspection", ErrInvalidHost)
	}
	if _, err := h.authorizeRunControl(ctx, request.RunID, "inspect_compensation", request.Identity, false); err != nil {
		return WorkflowCompensationResult{}, err
	}
	ledger, err := store.LoadCompensationLedger(ctx, request.RunID)
	if errors.Is(err, runtime.ErrNotFound) {
		return WorkflowCompensationResult{Present: false}, nil
	}
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	entries, err := store.ListCompensationEntries(ctx, request.RunID)
	if err != nil {
		return WorkflowCompensationResult{}, err
	}
	// CompensationStore currently exposes an internal full-ledger read. Never
	// let that shape escape this authorized host boundary without the host cap.
	limit := request.Limit
	maximum := h.compensationResultLimit()
	if limit == 0 || limit > maximum {
		limit = maximum
	}
	return boundedCompensationProjection(ledger, entries, limit), nil
}

func (h *Host) boundedCompensationResult(ledger runtime.CompensationLedgerSnapshot, entries []runtime.CompensationEntrySnapshot) WorkflowCompensationResult {
	return boundedCompensationProjection(ledger, entries, h.compensationResultLimit())
}

func (h *Host) compensationResultLimit() int {
	if h.batchLimit > 0 {
		return h.batchLimit
	}
	return 128
}

func boundedCompensationProjection(ledger runtime.CompensationLedgerSnapshot, entries []runtime.CompensationEntrySnapshot, limit int) WorkflowCompensationResult {
	ledger.Cycles = append([]runtime.CompensationCycle(nil), ledger.Cycles...)
	result := WorkflowCompensationResult{Present: true, Ledger: &ledger}
	if len(ledger.Cycles) > limit {
		result.Ledger.Cycles = append([]runtime.CompensationCycle(nil), ledger.Cycles[len(ledger.Cycles)-limit:]...)
		result.Truncated = true
	}
	entryLimit := len(entries)
	if entryLimit > limit {
		entryLimit, result.Truncated = limit, true
	}
	result.Entries = make([]runtime.CompensationEntrySnapshot, entryLimit)
	for index := 0; index < entryLimit; index++ {
		entry := entries[index]
		entry.History = append([]runtime.CompensationEntryHistory(nil), entry.History...)
		if len(entry.History) > limit {
			entry.History = append([]runtime.CompensationEntryHistory(nil), entry.History[len(entry.History)-limit:]...)
			result.Truncated = true
		}
		result.Entries[index] = entry
	}
	return result
}

func boundedAttestation(value string) bool {
	return value == strings.TrimSpace(value) && value != "" && len(value) <= maximumCompensationAttestationBytes
}

func compensationAuthorizationDigest(operation string, identity hoststate.IdentityBinding, key string, facts ...string) string {
	parts := []string{operation, identity.Principal, identity.SourceAuthority, identity.Trust, key}
	parts = append(parts, facts...)
	return values.SHA256Digest([]byte(strings.Join(parts, "\x00")))
}

var _ WorkflowCompensationOperations = (*Host)(nil)
