package appworkflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/offline"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

var nonDurableInspectionLimitations = []string{
	"node, attempt, wait, and event histories are unavailable for durability:none",
	"only the immutable start policy binding and typed terminal result are retained",
}

func (h *Host) executeNonDurable(ctx context.Context, request StartRunRequest, requestDigest string, plan *compile.ExecutionPlan, facts hoststate.PolicyFacts, decision hoststate.PolicyDecision) (StartRunResult, error) {
	audit, ok := h.journal.(hoststate.NonDurableJournal)
	if !ok || nilInterface(audit) {
		return StartRunResult{}, fmt.Errorf("%w: durability none requires bounded host audit support", ErrInvalidHost)
	}
	built, err := offline.Build(ctx, plan, offline.BuildOptions{Registry: h.registry, Mode: offline.ModeCLI})
	if err != nil {
		return StartRunResult{}, err
	}
	if len(built.Diagnostics) != 0 || built.Manifest == nil {
		return StartRunResult{Decision: decision, Facts: facts, Diagnostics: built.Diagnostics, Durability: graph.DurabilityNone}, nil
	}
	executed, executeErr := offline.Execute(ctx, *built.Manifest, offline.ExecuteOptions{
		Registry: h.registry, Verifiers: h.verifiers, Inputs: request.Inputs, Now: h.now,
		RunID: request.RunID, IdempotencyKey: request.IdempotencyKey,
	})
	var failure *runtime.Failure
	if executeErr != nil {
		var runFailure *offline.RunFailureError
		if !errors.As(executeErr, &runFailure) || executed.Run.ID == "" {
			return StartRunResult{}, executeErr
		}
		failure = runFailure.Failure
		if failure == nil {
			failure = &runtime.Failure{Code: "run_failed", Message: "non-durable workflow failed without an executor failure"}
		}
	}
	run := executed.Run
	// Transient value refs cannot be dereferenced after this call. Typed
	// terminal outputs are retained inline in the bounded audit instead.
	run.Inputs, run.Outputs = nil, nil
	record := hoststate.NonDurableStartRecord{
		RunID: request.RunID, StartKey: request.IdempotencyKey, RequestDigest: requestDigest,
		Plan: facts.Plan, Identity: facts.Identity.Clone(), Facts: facts, Decision: decision,
		Run: run, Outputs: executed.Outputs, Failure: failure, CompletedAt: h.now(),
	}
	persisted, outcome, err := audit.RecordNonDurableStart(context.WithoutCancel(ctx), record)
	if err != nil {
		return StartRunResult{}, err
	}
	result, replayErr := nonDurableStartResult(persisted, outcome)
	if executeErr != nil && replayErr == nil {
		replayErr = executeErr
	}
	return result, replayErr
}

func (h *Host) authorizeNonDurableReplay(ctx context.Context, request IdentityRequest, prior hoststate.NonDurableStartRecord) error {
	current, err := h.bindIdentity(ctx, request)
	if err != nil {
		return err
	}
	if !sameIdentity(current, prior.Identity) {
		return fmt.Errorf("%w: current caller is not authorized to replay this non-durable start key", ErrPolicyDenied)
	}
	return nil
}

func nonDurableStartResult(record hoststate.NonDurableStartRecord, outcome runtime.IdempotencyOutcome) (StartRunResult, error) {
	run := record.Run
	rendered, err := values.RenderValueSet(record.Outputs, values.DisplayPolicy{})
	if err != nil {
		return StartRunResult{}, err
	}
	result := StartRunResult{
		Run: &run, Decision: record.Decision, Facts: record.Facts, Outcome: outcome,
		Durability: graph.DurabilityNone, Outputs: record.Outputs, RenderedOutputs: rendered,
		InspectionLimitations: append([]string(nil), nonDurableInspectionLimitations...),
	}
	if record.Failure != nil {
		failure := *record.Failure
		return result, &offline.RunFailureError{Run: run, Failure: &failure}
	}
	return result, nil
}
