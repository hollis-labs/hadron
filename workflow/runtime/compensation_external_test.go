package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

type compensableDispatchKind struct {
	*stepkindtest.Kind
	evidence stepkind.ReversibilityEvidence
}

type observedCompensableDispatchKind struct{ *compensableDispatchKind }

type configIsolationCompensableKind struct {
	*stepkindtest.Kind
	described []string
	executed  string
}

func (k *configIsolationCompensableKind) DescribeReversibility(_ context.Context, request stepkind.ReversibilityRequest) (stepkind.ReversibilityEvidence, error) {
	k.described = append(k.described, request.Config["operation"].(string))
	request.Config["operation"] = "provider-mutated"
	return stepkind.ReversibilityEvidence{Operation: "fixture.original", ReceiptSchema: graph.Schema{}}, nil
}

func (*observedCompensableDispatchKind) Observe(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
	return stepkind.Observation{State: stepkind.ObservationPending}, nil
}

type compensationAttemptCanceler func(context.Context, workflowruntime.AttemptSnapshot) error

type stateWithoutCompensation struct{ workflowruntime.StateStore }

type malformedCompensationEntries struct {
	workflowruntime.CompensationStore
	mutate func([]workflowruntime.CompensationEntrySnapshot)
}

func (s malformedCompensationEntries) ListCompensationEntries(ctx context.Context, runID workflowruntime.RunID) ([]workflowruntime.CompensationEntrySnapshot, error) {
	entries, err := s.CompensationStore.ListCompensationEntries(ctx, runID)
	if err == nil {
		s.mutate(entries)
	}
	return entries, err
}

type malformedRecoveryPlanSource struct {
	workflowruntime.RecoveryPlanSource
	mutate func(*workflowruntime.RecoveryPlan)
}

func (s malformedRecoveryPlanSource) LoadRecoveryPlan(ctx context.Context, run workflowruntime.RunSnapshot) (workflowruntime.RecoveryPlan, error) {
	plan, err := s.RecoveryPlanSource.LoadRecoveryPlan(ctx, run)
	if err == nil {
		s.mutate(&plan)
	}
	return plan, err
}

func (f compensationAttemptCanceler) CancelAttempt(ctx context.Context, attempt workflowruntime.AttemptSnapshot) error {
	return f(ctx, attempt)
}

func (k *compensableDispatchKind) DescribeReversibility(context.Context, stepkind.ReversibilityRequest) (stepkind.ReversibilityEvidence, error) {
	return k.evidence, nil
}

func TestCompensationSnapshotsRejectImpossiblePhaseAndChronology(t *testing.T) {
	base := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	ref, err := values.NewValueSetRef("compensation-snapshot-values", values.ValueSet{})
	if err != nil {
		t.Fatal(err)
	}
	ledger := workflowruntime.CompensationLedgerSnapshot{
		RunID: "snapshot-run", PlanDigest: testPlan().Digest, Status: workflowruntime.CompensationTerminal,
		Outcome: workflowruntime.CompensationOutcomeFailed, Trigger: graph.CompensationOnFailure, OriginalStatus: workflowruntime.RunFailed, OriginalFailure: &ref,
		Cycles:     []workflowruntime.CompensationCycle{{Number: 1, Outcome: workflowruntime.CompensationOutcomeFailed, StartedAt: base.Add(time.Second), CompletedAt: base.Add(2 * time.Second)}},
		Generation: 2, CreatedAt: base, UpdatedAt: base.Add(2 * time.Second), CompletedAt: base.Add(2 * time.Second),
	}
	if err := ledger.Validate(); err != nil {
		t.Fatalf("valid ledger = %v", err)
	}
	ledgerCases := map[string]func(workflowruntime.CompensationLedgerSnapshot) workflowruntime.CompensationLedgerSnapshot{
		"frozen missing intent": func(v workflowruntime.CompensationLedgerSnapshot) workflowruntime.CompensationLedgerSnapshot {
			v.Status, v.Outcome, v.CompletedAt = workflowruntime.CompensationFrozen, "", time.Time{}
			v.Trigger, v.OriginalStatus, v.OriginalFailure = "", "", nil
			v.Cycles = []workflowruntime.CompensationCycle{{Number: 1, StartedAt: base.Add(time.Second)}}
			return v
		},
		"frozen without cycle": func(v workflowruntime.CompensationLedgerSnapshot) workflowruntime.CompensationLedgerSnapshot {
			v.Status, v.Outcome, v.CompletedAt, v.Cycles = workflowruntime.CompensationFrozen, "", time.Time{}, nil
			return v
		},
		"open cycle has completion": func(v workflowruntime.CompensationLedgerSnapshot) workflowruntime.CompensationLedgerSnapshot {
			v.Status, v.Outcome, v.CompletedAt = workflowruntime.CompensationRunning, "", time.Time{}
			v.Cycles = []workflowruntime.CompensationCycle{{Number: 1, StartedAt: base.Add(time.Second), CompletedAt: base.Add(2 * time.Second)}}
			return v
		},
		"prior cycle remains open": func(v workflowruntime.CompensationLedgerSnapshot) workflowruntime.CompensationLedgerSnapshot {
			v.Status, v.Outcome, v.CompletedAt = workflowruntime.CompensationFrozen, "", time.Time{}
			v.Cycles = []workflowruntime.CompensationCycle{{Number: 1, StartedAt: base}, {Number: 2, StartedAt: base.Add(time.Second)}}
			return v
		},
		"cycle completes before start": func(v workflowruntime.CompensationLedgerSnapshot) workflowruntime.CompensationLedgerSnapshot {
			v.Cycles[0].CompletedAt = base
			v.CompletedAt = base
			return v
		},
		"terminal differs from final cycle": func(v workflowruntime.CompensationLedgerSnapshot) workflowruntime.CompensationLedgerSnapshot {
			v.Outcome = workflowruntime.CompensationOutcomePartial
			return v
		},
	}
	for name, mutate := range ledgerCases {
		t.Run("ledger "+name, func(t *testing.T) {
			candidate := ledger
			candidate.Cycles = append([]workflowruntime.CompensationCycle(nil), ledger.Cycles...)
			if err := mutate(candidate).Validate(); err == nil {
				t.Fatal("impossible ledger validated")
			}
		})
	}

	entry := workflowruntime.CompensationEntrySnapshot{
		ID: "snapshot-entry", RunID: "snapshot-run", PlanDigest: testPlan().Digest,
		Source: workflowruntime.NodeInvocationID{RunID: "snapshot-run", NodeID: "effect"}, SourceAttempt: workflowruntime.AttemptID{Invocation: workflowruntime.NodeInvocationID{RunID: "snapshot-run", NodeID: "effect"}, Number: 1},
		Handler: workflowruntime.NodeInvocationID{RunID: "snapshot-run", NodeID: "undo", Iteration: "comp:snapshot-entry:2"}, Status: workflowruntime.CompensationFailed,
		Operation: "fixture.effect", EvidenceDigest: values.SHA256Digest([]byte("evidence")), Receipt: ref,
		HandlerFailure: &workflowruntime.Failure{Code: "undo_failed", Message: "rollback failed"},
		History:        []workflowruntime.CompensationEntryHistory{{Cycle: 1, Handler: workflowruntime.NodeInvocationID{RunID: "snapshot-run", NodeID: "undo", Iteration: "comp:snapshot-entry:1"}, Status: workflowruntime.CompensationFailed, Failure: &workflowruntime.Failure{Code: "undo_failed", Message: "first rollback failed"}, CompletedAt: base.Add(time.Second)}},
		Generation:     4, CreatedAt: base, UpdatedAt: base.Add(3 * time.Second), CompletedAt: base.Add(3 * time.Second),
	}
	if err := entry.Validate(); err != nil {
		t.Fatalf("valid entry = %v", err)
	}
	entryCases := map[string]func(workflowruntime.CompensationEntrySnapshot) workflowruntime.CompensationEntrySnapshot{
		"completion before creation": func(v workflowruntime.CompensationEntrySnapshot) workflowruntime.CompensationEntrySnapshot {
			v.CompletedAt = base.Add(-time.Second)
			return v
		},
		"history before creation": func(v workflowruntime.CompensationEntrySnapshot) workflowruntime.CompensationEntrySnapshot {
			v.History[0].CompletedAt = base.Add(-time.Second)
			return v
		},
		"current precedes history": func(v workflowruntime.CompensationEntrySnapshot) workflowruntime.CompensationEntrySnapshot {
			v.CompletedAt = base.Add(500 * time.Millisecond)
			return v
		},
		"failed without failure": func(v workflowruntime.CompensationEntrySnapshot) workflowruntime.CompensationEntrySnapshot {
			v.HandlerFailure = nil
			return v
		},
	}
	for name, mutate := range entryCases {
		t.Run("entry "+name, func(t *testing.T) {
			candidate := entry
			candidate.History = append([]workflowruntime.CompensationEntryHistory(nil), entry.History...)
			if err := mutate(candidate).Validate(); err == nil {
				t.Fatal("impossible entry validated")
			}
		})
	}
}

func TestAppliedCompensationReceiptCommitsForwardFailureWithoutAuthoredRetry(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "compensable-applied-error")
	kind := newCompensableDispatchKind(t)
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{
			Outcome:      stepkind.StepCompleted,
			Outputs:      values.ValueSet{"result": dispatchValue(t, "result", "created-before-timeout")},
			Compensation: &stepkind.CompensationReceipt{Operation: kind.evidence.Operation, Values: values.ValueSet{"token": dispatchValue(t, "token", "undo-1")}},
		}, &stepkind.ExecutionError{Code: "timeout_after_commit", Message: "observer failed after apply", Classification: stepkind.Retryable}
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	scheduler := &recordingScheduler{}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: scheduler}})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed}, Retry: &graph.RetryPolicy{Attempts: 3, On: []string{"timeout_after_commit"}}}
	result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "forward-effect-key"})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepExecution) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Status != workflowruntime.NodeFailed {
		t.Fatalf("Dispatch = %#v, %v", result, dispatchErr)
	}
	if recovered, err := store.RecoverRetryActivations(t.Context(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID}); err != nil || len(recovered) != 0 {
		t.Fatalf("applied effect scheduled a forward retry: %#v, %v", recovered, err)
	}
	ledger, err := store.LoadCompensationLedger(t.Context(), node.ID.RunID)
	entries, entriesErr := store.ListCompensationEntries(t.Context(), node.ID.RunID)
	if err != nil || entriesErr != nil || ledger.Status != workflowruntime.CompensationCollecting || len(entries) != 1 || entries[0].OriginalError == nil || entries[0].OriginalOutputs == nil {
		t.Fatalf("durable applied-effect eligibility ledger=%#v entries=%#v errors=%v/%v", ledger, entries, err, entriesErr)
	}
}

func TestDispatcherIsolatesValidatorProviderAndExecutionConfig(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "compensation-config-isolation")
	kind := &configIsolationCompensableKind{Kind: stepkindtest.NewNoopKind("config-isolation", "v1")}
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = graph.Schema{}
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	kind.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	kind.ValidateConfigFunc = func(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
		config["operation"] = "validator-mutated"
		return nil
	}
	kind.ExecuteFunc = func(_ context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		kind.executed = invocation.Invocation.Config["operation"].(string)
		invocation.Invocation.Config["operation"] = "executor-mutated"
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}, Compensation: &stepkind.CompensationReceipt{Operation: "fixture.original", Values: values.ValueSet{}}}, nil
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	config := graph.Config{"operation": "original"}
	result, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "config-isolation", KindVersion: "v1", Config: config, Compensation: &graph.CompensationSpec{Handler: "undo"}}, IdempotencyKey: "config-isolation-dispatch"})
	if err != nil || result.Node.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("Dispatch = %#v, %v", result, err)
	}
	if config["operation"] != "original" || fmt.Sprint(kind.described) != "[original original]" || kind.executed != "original" {
		t.Fatalf("config aliases authored=%#v described=%#v executed=%q", config, kind.described, kind.executed)
	}
	entries, err := store.ListCompensationEntries(t.Context(), node.ID.RunID)
	if err != nil || len(entries) != 1 || entries[0].Operation != "fixture.original" {
		t.Fatalf("admitted evidence = %#v, %v", entries, err)
	}
}

func TestCompensableExternalObservationIsRejectedBeforeAttemptStart(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "compensable-external-observation")
	kind := &observedCompensableDispatchKind{compensableDispatchKind: newCompensableDispatchKind(t)}
	kind.SpecValue.Observation = stepkind.ObservationSpec{Mode: stepkind.ObservationPoll}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}}
	result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "external-effect-key"})
	if !errors.Is(dispatchErr, workflowruntime.ErrInvalidCompensation) || result.Node.Status != workflowruntime.NodeReady || result.Node.LatestAttempt != 0 {
		t.Fatalf("external compensable dispatch = %#v, %v", result, dispatchErr)
	}
	if attempts, loadErr := store.ListAttempts(t.Context(), node.ID); loadErr != nil || len(attempts) != 0 {
		t.Fatalf("external compensable attempts = %#v, %v", attempts, loadErr)
	}
}

func TestAppliedCompensationReceiptWithInvalidPublicOutputTerminalizesAndRemainsEligible(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "compensable-applied-invalid-output")
	kind := newCompensableDispatchKind(t)
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{
			Outcome:      stepkind.StepCompleted,
			Outputs:      values.ValueSet{"result": dispatchValue(t, "result", 42)},
			Compensation: &stepkind.CompensationReceipt{Operation: kind.evidence.Operation, Values: values.ValueSet{"token": dispatchValue(t, "token", "undo-invalid-output")}},
		}, &stepkind.ExecutionError{Code: "timeout_after_invalid_output", Message: "effect applied before invalid output", Classification: stepkind.Retryable}
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	scheduler := &recordingScheduler{}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: scheduler}})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed}, Retry: &graph.RetryPolicy{Attempts: 3, On: []string{"timeout_after_invalid_output", "step_result_invalid"}}}
	result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "invalid-output-effect-key"})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Code != "step_result_invalid" || result.Attempt.Failure.Retryable {
		t.Fatalf("Dispatch = %#v, %v", result, dispatchErr)
	}
	if retries, err := store.RecoverRetryActivations(t.Context(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID}); err != nil || len(retries) != 0 {
		t.Fatalf("invalid applied output scheduled retry = %#v, %v", retries, err)
	}
	entries, err := store.ListCompensationEntries(t.Context(), node.ID.RunID)
	if err != nil || len(entries) != 1 || entries[0].OriginalOutputs == nil || entries[0].OriginalError == nil {
		t.Fatalf("invalid applied output eligibility = %#v, %v", entries, err)
	}
}

func TestAppliedCompensationReceiptWithNonPersistableOutputOmitsOutputRefAndFencesRetry(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "compensable-applied-nonpersistable-output")
	kind := newCompensableDispatchKind(t)
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{
			Outcome:      stepkind.StepCompleted,
			Outputs:      values.ValueSet{"result": values.Value{}},
			Compensation: &stepkind.CompensationReceipt{Operation: kind.evidence.Operation, Values: values.ValueSet{"token": dispatchValue(t, "token", "undo-nonpersistable-output")}},
		}, &stepkind.ExecutionError{Code: "timeout_after_nonpersistable_output", Message: "effect applied before malformed envelope", Classification: stepkind.Retryable}
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	scheduler := &recordingScheduler{}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: scheduler}})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed}, Retry: &graph.RetryPolicy{Attempts: 3, On: []string{"timeout_after_nonpersistable_output", "step_result_invalid"}}}
	result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "nonpersistable-output-effect-key"})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Status != workflowruntime.NodeFailed {
		t.Fatalf("Dispatch = %#v, %v", result, dispatchErr)
	}
	entries, err := store.ListCompensationEntries(t.Context(), node.ID.RunID)
	if err != nil || len(entries) != 1 || entries[0].OriginalOutputs != nil || entries[0].OriginalError == nil {
		t.Fatalf("nonpersistable applied output eligibility = %#v, %v", entries, err)
	}
	if retries, err := store.RecoverRetryActivations(t.Context(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID}); err != nil || len(retries) != 0 {
		t.Fatalf("nonpersistable applied output scheduled retry = %#v, %v", retries, err)
	}
}

func TestMalformedAppliedCompensationReceiptTerminalizesWithoutEligibilityOrRetry(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "compensable-malformed-receipt")
	kind := newCompensableDispatchKind(t)
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		return stepkind.StepResult{
			Outcome:      stepkind.StepCompleted,
			Outputs:      values.ValueSet{"result": dispatchValue(t, "result", "created")},
			Compensation: &stepkind.CompensationReceipt{Operation: "fixture.different-operation", Values: values.ValueSet{"token": dispatchValue(t, "token", "untrusted")}},
		}, &stepkind.ExecutionError{Code: "timeout_after_malformed_receipt", Message: "effect may be applied", Classification: stepkind.Retryable}
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	scheduler := &recordingScheduler{}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: scheduler}})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed}, Retry: &graph.RetryPolicy{Attempts: 3, On: []string{"timeout_after_malformed_receipt", "step_result_invalid"}}}
	result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "malformed-receipt-effect-key"})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Retryable || executions != 1 {
		t.Fatalf("Dispatch = %#v, %v executions=%d", result, dispatchErr, executions)
	}
	if retries, err := store.RecoverRetryActivations(t.Context(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID}); err != nil || len(retries) != 0 {
		t.Fatalf("malformed receipt scheduled retry = %#v, %v", retries, err)
	}
	if _, err := store.LoadCompensationLedger(t.Context(), node.ID.RunID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("malformed receipt created eligibility: %v", err)
	}
	if recovered, err := store.Recovery(t.Context(), workflowruntime.RecoveryQuery{RunID: node.ID.RunID, Now: base.Add(time.Hour)}); err != nil || len(recovered.Ready)+len(recovered.Running)+len(recovered.Waiting) != 0 {
		t.Fatalf("malformed receipt remained recoverable = %#v, %v", recovered, err)
	}
}

func TestSuccessfulCompensableEffectWithInvalidReceiptIsPermanentlyFenced(t *testing.T) {
	for _, test := range []struct {
		name    string
		receipt func(*compensableDispatchKind) stepkind.CompensationReceipt
	}{
		{name: "operation mismatch", receipt: func(kind *compensableDispatchKind) stepkind.CompensationReceipt {
			return stepkind.CompensationReceipt{Operation: "fixture.wrong", Values: values.ValueSet{"token": dispatchValue(t, "token", "undo")}}
		}},
		{name: "schema mismatch", receipt: func(kind *compensableDispatchKind) stepkind.CompensationReceipt {
			return stepkind.CompensationReceipt{Operation: kind.evidence.Operation, Values: values.ValueSet{"token": dispatchValue(t, "token", 42)}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, claim, node, base := dispatchFixture(t, "compensable-success-invalid-receipt-"+strings.ReplaceAll(test.name, " ", "-"))
			kind := newCompensableDispatchKind(t)
			kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
				receipt := test.receipt(kind)
				return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "created")}, Compensation: &receipt}, nil
			}
			registry := stepkind.NewRegistry()
			if err := registry.Register(kind); err != nil {
				t.Fatal(err)
			}
			dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: &recordingScheduler{}}})
			if err != nil {
				t.Fatal(err)
			}
			definition := graph.Node{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}, Retry: &graph.RetryPolicy{Attempts: 3, On: []string{"step_result_invalid"}}}
			result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "invalid-receipt-key"})
			if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Retryable || result.Attempt.Failure.Details["effect_applied"] != "unverified_receipt" {
				t.Fatalf("invalid successful receipt = %#v, %v", result, dispatchErr)
			}
			if retries, retryErr := store.RecoverRetryActivations(t.Context(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID}); retryErr != nil || len(retries) != 0 {
				t.Fatalf("invalid successful receipt retries = %#v, %v", retries, retryErr)
			}
			if _, ledgerErr := store.LoadCompensationLedger(t.Context(), node.ID.RunID); !errors.Is(ledgerErr, workflowruntime.ErrNotFound) {
				t.Fatalf("invalid successful receipt created ledger: %v", ledgerErr)
			}
			risk, riskErr := workflowruntime.ReplayCompensationRiskDigest(t.Context(), store, store, node.ID.RunID)
			if riskErr != nil || values.ValidateDigest(risk) != nil {
				t.Fatalf("invalid successful receipt risk = %q, %v", risk, riskErr)
			}
		})
	}
}

func TestCompletedCompensableErrorWithoutReceiptIsIndeterminateAndReplayRequiresRiskProof(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "compensable-missing-receipt")
	kind := newCompensableDispatchKind(t)
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "possibly-created")}}, &stepkind.ExecutionError{Code: "timeout_after_apply", Message: "completion lost receipt", Classification: stepkind.Retryable}
	}
	// Make repeat safety intrinsic so the durable applied-effect risk proof is
	// the only replay fence under examination.
	kind.SpecValue.Idempotency = graph.IdempotencyIntrinsic
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	scheduler := &recordingScheduler{}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: scheduler}})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed}, Retry: &graph.RetryPolicy{Attempts: 3, On: []string{"timeout_after_apply", "step_result_invalid"}}}
	result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "missing-receipt-key"})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Details["effect_applied"] != "missing_receipt" || result.Attempt.Failure.Retryable {
		t.Fatalf("missing receipt dispatch = %#v, %v", result, dispatchErr)
	}
	if retries, retryErr := store.RecoverRetryActivations(t.Context(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID}); retryErr != nil || len(retries) != 0 {
		t.Fatalf("missing receipt retries = %#v, %v", retries, retryErr)
	}
	riskDigest, err := workflowruntime.ReplayCompensationRiskDigest(t.Context(), store, store, node.ID.RunID)
	if err != nil || values.ValidateDigest(riskDigest) != nil {
		t.Fatalf("risk digest = %q, %v", riskDigest, err)
	}
	run, _ := store.LoadRun(t.Context(), node.ID.RunID)
	running, err := store.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunFailed, At: base.Add(5 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}, Nodes: []graph.Node{{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", InputBindings: map[string]graph.Binding{"input": {Kind: graph.BindingLiteral, Literal: "replay-risk"}}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyIntrinsic}, Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo", Kind: "compensable", KindVersion: "v1"}}}
	allowRepeat := workflowruntime.RepeatPolicyFunc(func(context.Context, workflowruntime.RepeatCandidate) (workflowruntime.RepeatPolicyDecision, error) {
		return workflowruntime.RepeatPolicyDecision{Allow: true, Code: "risk_attested", Reason: "test host authorizes an exact attested replay"}, nil
	})
	service := workflowruntime.ReplayService{Store: store, Replay: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: registry, Policy: allowRepeat}
	request := workflowruntime.ReplayRequest{SourceRunID: node.ID.RunID, RunID: "missing-receipt-replay", FromNodeID: node.ID.NodeID, IdempotencyKey: "missing-receipt-replay", At: base.Add(6 * time.Second)}
	if _, err := service.Rerun(t.Context(), request); !errors.Is(err, workflowruntime.ErrRecoveryUnsafe) {
		t.Fatalf("replay without risk attestation = %v", err)
	}
	request.CompensationAuthorization = &workflowruntime.ReplayCompensationAuthorization{RiskDigest: riskDigest, Digest: values.SHA256Digest([]byte("bounded-risk-attestation"))}
	if replayed, err := service.Rerun(t.Context(), request); err != nil || replayed.Provenance.CompensationAuthorization == nil || replayed.Provenance.CompensationAuthorization.RiskDigest != riskDigest {
		t.Fatalf("attested risk replay = %#v, %v", replayed, err)
	}
}

func TestReceiptFreePreApplyFailureRetainsAuthoredRetry(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "compensable-preapply-error")
	kind := newCompensableDispatchKind(t)
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{}, &stepkind.ExecutionError{Code: "before_apply", Message: "not applied", Classification: stepkind.Retryable}
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	scheduler := &recordingScheduler{}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: scheduler}})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: node.ID.NodeID, Kind: "compensable", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed}, Retry: &graph.RetryPolicy{Attempts: 2, On: []string{"before_apply"}, Backoff: graph.BackoffPolicy{Strategy: graph.BackoffFixed, InitialDelay: "1s"}}}
	result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "forward-effect-key"})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepExecution) || result.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("Dispatch = %#v, %v", result, dispatchErr)
	}
	if recovered, err := store.RecoverRetryActivations(t.Context(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID}); err != nil || len(recovered) != 1 {
		t.Fatalf("pre-apply failure retry = %#v, %v", recovered, err)
	}
	if _, err := store.LoadCompensationLedger(t.Context(), node.ID.RunID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("receipt-free failure created compensation eligibility: %v", err)
	}
}

func TestUnrequestedCompensationReceiptIsPermanentReplayRiskWithoutAuthoredRetry(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "unrequested-compensation-receipt")
	kind := stepkindtest.NewNoopKind("unexpected-receipt", "v1")
	kind.SpecValue.OutputSchema = graph.Schema{}
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}, Compensation: &stepkind.CompensationReceipt{Operation: "unexpected.effect", Values: values.ValueSet{}}}, &stepkind.ExecutionError{Code: "timeout_after_apply", Message: "effect may have applied", Classification: stepkind.Retryable}
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Store: store}})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: node.ID.NodeID, Kind: "unexpected-receipt", KindVersion: "v1", Retry: &graph.RetryPolicy{Attempts: 2, On: []string{"timeout_after_apply", "step_result_invalid"}}}
	result, dispatchErr := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "unexpected-receipt-dispatch"})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Retryable || result.Attempt.Failure.Details["effect_applied"] != "unrequested_receipt" {
		t.Fatalf("unrequested receipt dispatch = %#v, %v", result, dispatchErr)
	}
	if retries, retryErr := store.RecoverRetryActivations(t.Context(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID}); retryErr != nil || len(retries) != 0 {
		t.Fatalf("unrequested receipt retries = %#v, %v", retries, retryErr)
	}
	risk, riskErr := workflowruntime.ReplayCompensationRiskDigest(t.Context(), store, store, node.ID.RunID)
	if riskErr != nil || values.ValidateDigest(risk) != nil {
		t.Fatalf("unrequested receipt risk = %q, %v", risk, riskErr)
	}
}

func TestMissingCompensationReceiptReconcilesFanOutParent(t *testing.T) {
	ctx := t.Context()
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("compensation-fanout-missing-receipt")
	createRun(t, store, runID, base)
	parent := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "effect"}
	createNode(t, store, parent, workflowruntime.NodePending, 0, base)
	forEach := graph.ForEachSpec{Items: graph.Expression{Text: `["one"]`}}
	expanded, err := (workflowruntime.FanOutCoordinator{Store: store}).Expand(ctx, workflowruntime.FanOutExpandCommand{Parent: parent, ExpectedParentGeneration: 1, Spec: forEach, At: base.Add(time.Second)})
	if err != nil || len(expanded.Children) != 1 {
		t.Fatalf("Expand = %#v, %v", expanded, err)
	}
	claim, acquired, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(ctx, workflowruntime.ReadyClaimRequest{RunID: runID, Owner: "worker", Token: "fanout-missing-receipt-token", IdempotencyKey: "fanout-missing-receipt-claim", Now: base.Add(2 * time.Second), LeaseUntil: base.Add(time.Hour)})
	if err != nil || !acquired || claim.Candidate.InvocationID != expanded.Children[0].ID {
		t.Fatalf("ClaimNext = %#v, %t, %v", claim, acquired, err)
	}
	kind := newCompensableDispatchKind(t)
	kind.SpecValue.InputSchema, kind.SpecValue.OutputSchema = graph.Schema{}, graph.Schema{}
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}, nil
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }, RetryCoordinator: &workflowruntime.RetryCoordinator{Store: store}})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{ID: parent.NodeID, Kind: "compensable", KindVersion: "v1", ForEach: &forEach, Compensation: &graph.CompensationSpec{Handler: "undo"}, Retry: &graph.RetryPolicy{Attempts: 2, On: []string{"step_result_invalid"}}}
	result, dispatchErr := dispatcher.Dispatch(ctx, workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "fanout-missing-receipt-dispatch"})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || result.Node.Status != workflowruntime.NodeFailed {
		t.Fatalf("Dispatch = %#v, %v", result, dispatchErr)
	}
	parentSnapshot, err := store.LoadNodeInvocation(ctx, parent)
	if err != nil || !parentSnapshot.Status.Terminal() || parentSnapshot.Status != workflowruntime.NodeFailed {
		t.Fatalf("fan-out parent did not reconcile = %#v, %v", parentSnapshot, err)
	}
	items, err := store.LoadFanOutItemResults(ctx, parent)
	if err != nil || len(items) != 1 || items[0].Status != workflowruntime.NodeFailed {
		t.Fatalf("fan-out item result = %#v, %v", items, err)
	}
}

func TestCollectingCompensationCannotBeCanceledBeforeDeclaredTrigger(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-pretrigger-cancel")
	seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
	ledger, err := store.LoadCompensationLedger(ctx, runID)
	if err != nil || ledger.Status != workflowruntime.CompensationCollecting {
		t.Fatalf("collecting ledger = %#v, %v", ledger, err)
	}
	if _, err := store.CancelCompensation(ctx, workflowruntime.CancelCompensationRequest{RunID: runID, ExpectedLedgerGeneration: ledger.Generation, IdempotencyKey: "pretrigger-cancel", Reason: "must not preempt declared trigger", At: base.Add(5 * time.Second)}); !errors.Is(err, workflowruntime.ErrCompensationConflict) {
		t.Fatalf("pre-trigger cancellation = %v", err)
	}
	unchanged, err := store.LoadCompensationLedger(ctx, runID)
	if err != nil || unchanged.Status != workflowruntime.CompensationCollecting || unchanged.Generation != ledger.Generation {
		t.Fatalf("pre-trigger cancellation changed ledger = %#v, %v", unchanged, err)
	}
}

func TestZeroEntryCompensationEmitsOneCompletedEventOnIdempotentReplay(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-zero-entry-event")
	run, err := store.LoadRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	reason := workflowruntime.Failure{Code: "zero_entry_failure", Message: "terminal failure without applied effects"}
	typed, err := workflowruntime.NewRunFailureValue(runID, workflowruntime.RunFailed, reason)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := store.BeginTerminalIntent(ctx, workflowruntime.BeginTerminalIntentRequest{RunID: runID, ExpectedRunGeneration: run.Generation, IntendedStatus: workflowruntime.RunFailed, CompensationRequired: true, Reason: &reason, ErrorValues: values.ValueSet{"error": typed}, IdempotencyKey: "zero-entry-intent", At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	request := workflowruntime.FreezeCompensationRequest{RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: intent.Run.Generation, ExpectedIntentGeneration: intent.Intent.Generation, Trigger: graph.CompensationOnFailure, OriginalStatus: workflowruntime.RunFailed, OriginalFailure: intent.Intent.Error, IdempotencyKey: "zero-entry-freeze", At: base.Add(3 * time.Second)}
	frozen, err := store.FreezeCompensation(ctx, request)
	if err != nil || frozen.Ledger.Status != workflowruntime.CompensationTerminal || frozen.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded {
		t.Fatalf("zero-entry freeze = %#v, %v", frozen, err)
	}
	replayed, err := store.FreezeCompensation(ctx, request)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Ledger.Generation != frozen.Ledger.Generation {
		t.Fatalf("zero-entry replay = %#v, %v", replayed, err)
	}
	events, err := store.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	for _, event := range events {
		if event.Type == workflowruntime.EventCompensationCompleted {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("completed events = %d, want 1; events=%#v", completed, events)
	}
}

func TestCompensationCoordinatorRollsBackReverseDependenciesBeforeTerminalIntent(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-order")
	seedCompensationEligibility(t, store, runID, "first", "undo-first", base.Add(2*time.Second))
	seedCompensationEligibility(t, store, runID, "second", "undo-second", base.Add(3*time.Second))
	createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
	makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(4*time.Second))
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version,
		Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}},
		Nodes: []graph.Node{
			{ID: "first", Compensation: &graph.CompensationSpec{Handler: "undo-first"}},
			{ID: "second", Needs: []graph.Need{{Node: "first"}}, Compensation: &graph.CompensationSpec{Handler: "undo-second"}},
			{ID: "failure"}, {ID: "undo-first"}, {ID: "undo-second"},
		}}
	coordinator := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	result, intent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "terminal-compensation-order", base.Add(5*time.Second))
	if !errors.Is(err, workflowruntime.ErrCompensationPending) || intent == nil || !intent.CompensationRequired || result.Status != workflowruntime.RunRunning {
		t.Fatalf("initial reconciliation = %#v, %#v, %v", result, intent, err)
	}
	ledger, err := store.LoadCompensationLedger(ctx, runID)
	if err != nil || ledger.Status != workflowruntime.CompensationFrozen || ledger.OriginalStatus != workflowruntime.RunFailed || ledger.OriginalFailure == nil {
		t.Fatalf("frozen ledger = %#v, %v", ledger, err)
	}
	if _, err := store.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: runID, ExpectedRunGeneration: result.Generation, ExpectedIntentGeneration: intent.Generation, At: base.Add(6 * time.Second)}); !errors.Is(err, workflowruntime.ErrCompensationPending) {
		t.Fatalf("terminal bypass before rollback = %v", err)
	}
	compensation := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
	progress, err := compensation.Progress(ctx, runID, base.Add(6*time.Second))
	if err != nil || len(progress.Activated) != 1 || progress.Activated[0].Entry.Source.NodeID != "second" {
		t.Fatalf("first rollback layer = %#v, %v", progress, err)
	}
	makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(7*time.Second))
	progress, err = compensation.Progress(ctx, runID, base.Add(8*time.Second))
	if err != nil || len(progress.Sealed) != 1 || len(progress.Activated) != 1 || progress.Activated[0].Entry.Source.NodeID != "first" {
		t.Fatalf("second rollback layer = %#v, %v", progress, err)
	}
	makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(9*time.Second))
	progress, err = compensation.Progress(ctx, runID, base.Add(10*time.Second))
	if err != nil || progress.Ledger.Status != workflowruntime.CompensationTerminal || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded {
		t.Fatalf("terminal rollback = %#v, %v", progress, err)
	}
	completed, completedIntent, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "terminal-compensation-order", base.Add(11*time.Second))
	if err != nil || completed.Status != workflowruntime.RunFailed || completedIntent == nil || completedIntent.IntendedStatus != workflowruntime.RunFailed || completedIntent.Error == nil {
		t.Fatalf("completed original failure = %#v, %#v, %v", completed, completedIntent, err)
	}
}

func TestCompensationProgressRejectsMalformedPinnedPlanAndDurableEntrySet(t *testing.T) {
	mutations := map[string]func([]workflowruntime.CompensationEntrySnapshot){
		"ledger identity": func(entries []workflowruntime.CompensationEntrySnapshot) {
			entries[0].PlanDigest = values.SHA256Digest([]byte("forged-plan"))
		},
		"source handler binding": func(entries []workflowruntime.CompensationEntrySnapshot) {
			entries[0].Handler.NodeID = "forged-handler"
		},
		"dependency cycle": func(entries []workflowruntime.CompensationEntrySnapshot) {
			entries[0].Prerequisites = []string{entries[0].ID}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			ctx, store, base, runID := controlFixture(t, "malformed-progress-"+strings.ReplaceAll(name, " ", "-"))
			entry := seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
			createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
			makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(5*time.Second))
			workflow := compensationFailureGraph("effect", "undo")
			if _, _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, runID, "malformed-progress", base.Add(6*time.Second)); !errors.Is(err, workflowruntime.ErrCompensationPending) {
				t.Fatal(err)
			}
			bad := malformedCompensationEntries{CompensationStore: store, mutate: mutate}
			_, err := (workflowruntime.CompensationCoordinator{Store: store, Compensation: bad, Plans: staticRecoveryPlans{graph: workflow}}).Progress(ctx, runID, base.Add(7*time.Second))
			if !errors.Is(err, workflowruntime.ErrCompensationConflict) {
				t.Fatalf("malformed entry set = %v", err)
			}
			if _, loadErr := store.LoadNodeInvocation(ctx, entry.Handler); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
				t.Fatalf("malformed entry materialized handler: %v", loadErr)
			}
		})
	}

	t.Run("plan reference", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "malformed-progress-plan")
		entry := seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
		createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(5*time.Second))
		workflow := compensationFailureGraph("effect", "undo")
		if _, _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, runID, "malformed-progress-plan", base.Add(6*time.Second)); !errors.Is(err, workflowruntime.ErrCompensationPending) {
			t.Fatal(err)
		}
		plans := malformedRecoveryPlanSource{RecoveryPlanSource: staticRecoveryPlans{graph: workflow}, mutate: func(plan *workflowruntime.RecoveryPlan) { plan.Ref.Version = "forged" }}
		_, err := (workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: plans}).Progress(ctx, runID, base.Add(7*time.Second))
		if !errors.Is(err, workflowruntime.ErrCompensationConflict) {
			t.Fatalf("malformed plan = %v", err)
		}
		if _, loadErr := store.LoadNodeInvocation(ctx, entry.Handler); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
			t.Fatalf("malformed plan materialized handler: %v", loadErr)
		}
	})
}

func TestCancelAndTimeoutTriggersFreezeExactOriginalOutcome(t *testing.T) {
	t.Run("cancellation tree", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "compensation-cancel-trigger")
		seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
		workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnCancel}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}
		run, _ := store.LoadRun(ctx, runID)
		reason := workflowruntime.Failure{Code: "operator_canceled", Message: "operator requested cancellation"}
		canceled, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).RequestRunCancellationTree(ctx, workflow, workflowruntime.RequestRunCancellationRequest{RunID: runID, ExpectedGeneration: run.Generation, IdempotencyKey: "compensation-cancel-tree", Reason: reason, At: base.Add(6 * time.Second)}, nil)
		if err != nil || canceled.Intent.RunID != runID || !canceled.Intent.CompensationRequired || canceled.Intent.IntendedStatus != workflowruntime.RunCanceled || canceled.Intent.Error == nil {
			t.Fatalf("cancellation intent = %#v, %v", canceled, err)
		}
		_, intent, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, runID, "compensation-cancel-complete", base.Add(7*time.Second))
		if !errors.Is(err, workflowruntime.ErrCompensationPending) || intent == nil {
			t.Fatalf("cancel reconciliation = %#v, %v", intent, err)
		}
		ledger, err := store.LoadCompensationLedger(ctx, runID)
		if err != nil || ledger.Trigger != graph.CompensationOnCancel || ledger.OriginalStatus != workflowruntime.RunCanceled || ledger.OriginalFailure == nil {
			t.Fatalf("cancel ledger = %#v, %v", ledger, err)
		}
		failure, loadErr := store.LoadValues(ctx, *ledger.OriginalFailure)
		if loadErr != nil || failure["error"].Inline == nil {
			t.Fatalf("cancel typed original failure = %#v, %v", failure, loadErr)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "compensation-timeout-trigger")
		seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
		createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "timeout"}, workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "timeout"}, workflowruntime.NodeTimedOut, base.Add(5*time.Second))
		workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnTimeout}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "timeout"}, {ID: "undo"}}}
		_, intent, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, runID, "compensation-timeout-complete", base.Add(6*time.Second))
		if !errors.Is(err, workflowruntime.ErrCompensationPending) || intent == nil || intent.IntendedStatus != workflowruntime.RunTimedOut || intent.Error == nil {
			t.Fatalf("timeout reconciliation = %#v, %v", intent, err)
		}
		ledger, err := store.LoadCompensationLedger(ctx, runID)
		if err != nil || ledger.Trigger != graph.CompensationOnTimeout || ledger.OriginalStatus != workflowruntime.RunTimedOut || ledger.OriginalFailure == nil {
			t.Fatalf("timeout ledger = %#v, %v", ledger, err)
		}
		failure, loadErr := store.LoadValues(ctx, *ledger.OriginalFailure)
		payload, _ := failure["error"].Inline.(map[string]any)
		if loadErr != nil || payload["timeout_kind"] != string(workflowruntime.TimeoutExecution) {
			t.Fatalf("timeout typed original failure = %#v, %v", failure, loadErr)
		}
	})
}

func TestFailFastPolicyPreservesFailureAndTimeoutCompensationTriggers(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  workflowruntime.NodeStatus
		run     workflowruntime.RunStatus
		trigger graph.CompensationTrigger
	}{
		{name: "failure", status: workflowruntime.NodeFailed, run: workflowruntime.RunFailed, trigger: graph.CompensationOnFailure},
		{name: "timeout", status: workflowruntime.NodeTimedOut, run: workflowruntime.RunTimedOut, trigger: graph.CompensationOnTimeout},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, store, base, runID := controlFixture(t, "compensation-fail-fast-"+test.name)
			seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
			triggerID := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "trigger"}
			createNode(t, store, triggerID, workflowruntime.NodePending, 0, base)
			makeTerminalNode(t, store, triggerID, test.status, base.Add(5*time.Second))
			workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version,
				Completion:   &graph.RunCompletionPolicy{Mode: graph.CompletionFailFast},
				Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{test.trigger}},
				Nodes:        []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "trigger"}, {ID: "undo"}},
			}
			policy, err := workflowruntime.NewRunPolicyCoordinator(store, store, store).HandleRunFailure(ctx, workflowruntime.HandleRunFailureRequest{Workflow: workflow, Source: triggerID, IdempotencyKey: "fail-fast-" + test.name, At: base.Add(6 * time.Second)})
			if err != nil || policy.Disposition != workflowruntime.RunFailureFailFast || !policy.Intent.CompensationRequired || policy.Intent.IntendedStatus != test.run || policy.Intent.Error == nil {
				t.Fatalf("fail-fast policy = %#v, %v", policy, err)
			}
			_, intent, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, runID, "reconcile-fail-fast-"+test.name, base.Add(7*time.Second))
			if !errors.Is(err, workflowruntime.ErrCompensationPending) || intent == nil || intent.Generation != policy.Intent.Generation {
				t.Fatalf("fail-fast reconciliation = %#v, %v", intent, err)
			}
			ledger, err := store.LoadCompensationLedger(ctx, runID)
			if err != nil || ledger.Trigger != test.trigger || ledger.OriginalStatus != test.run || ledger.OriginalFailure == nil {
				t.Fatalf("fail-fast compensation ledger = %#v, %v", ledger, err)
			}
		})
	}
}

func TestCompensationRequirementFailsBeforeTerminalOrCancellationIntentWithoutStoreExtension(t *testing.T) {
	t.Run("terminal failure", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "compensation-store-missing-terminal")
		createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "effect"}, workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "effect"}, workflowruntime.NodeFailed, base.Add(2*time.Second))
		workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}
		before, _ := store.LoadRun(ctx, runID)
		coordinator := workflowruntime.NewControlFlowCoordinator(stateWithoutCompensation{StateStore: store}, store, nil)
		if _, _, err := coordinator.ReconcileRunCompletion(ctx, workflow, runID, "missing-compensation-terminal", base.Add(3*time.Second)); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
			t.Fatalf("missing compensation terminal admission = %v", err)
		}
		after, _ := store.LoadRun(ctx, runID)
		if after.Generation != before.Generation || after.Status != before.Status {
			t.Fatalf("terminal admission mutated run = %#v -> %#v", before, after)
		}
		if _, err := store.LoadTerminalIntent(ctx, runID); !errors.Is(err, workflowruntime.ErrNotFound) {
			t.Fatalf("terminal admission persisted intent = %v", err)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "compensation-store-missing-cancel")
		workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnCancel}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}
		before, _ := store.LoadRun(ctx, runID)
		coordinator := workflowruntime.NewControlFlowCoordinator(stateWithoutCompensation{StateStore: store}, store, nil)
		request := workflowruntime.RequestRunCancellationRequest{RunID: runID, ExpectedGeneration: before.Generation, IdempotencyKey: "missing-compensation-cancel", Reason: workflowruntime.Failure{Code: "canceled", Message: "cancel"}, At: base.Add(2 * time.Second)}
		if _, err := coordinator.RequestRunCancellationTree(ctx, workflow, request, nil); !errors.Is(err, workflowruntime.ErrInvalidControlFlow) {
			t.Fatalf("missing compensation cancellation admission = %v", err)
		}
		after, _ := store.LoadRun(ctx, runID)
		if after.Generation != before.Generation || after.Status != before.Status {
			t.Fatalf("cancellation admission mutated run = %#v -> %#v", before, after)
		}
		if _, err := store.LoadTerminalIntent(ctx, runID); !errors.Is(err, workflowruntime.ErrNotFound) {
			t.Fatalf("cancellation admission persisted intent = %v", err)
		}
	})
}

func TestCompensationCoordinatorAdmitsIndependentRollbackEntriesTogether(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-parallel")
	seedCompensationEligibility(t, store, runID, "left", "undo-left", base.Add(2*time.Second))
	seedCompensationEligibility(t, store, runID, "right", "undo-right", base.Add(3*time.Second))
	createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
	makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(4*time.Second))
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}, Nodes: []graph.Node{
		{ID: "left", Compensation: &graph.CompensationSpec{Handler: "undo-left"}}, {ID: "right", Compensation: &graph.CompensationSpec{Handler: "undo-right"}}, {ID: "failure"}, {ID: "undo-left"}, {ID: "undo-right"},
	}}
	_, _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, runID, "parallel-freeze", base.Add(5*time.Second))
	if !errors.Is(err, workflowruntime.ErrCompensationPending) {
		t.Fatalf("freeze = %v", err)
	}
	compensation := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
	progress, err := compensation.Progress(ctx, runID, base.Add(6*time.Second))
	if err != nil || len(progress.Activated) != 2 {
		t.Fatalf("parallel activation = %#v, %v", progress, err)
	}
	got := []string{progress.Activated[0].Entry.Source.NodeID, progress.Activated[1].Entry.Source.NodeID}
	slices.Sort(got)
	if !slices.Equal(got, []string{"left", "right"}) {
		t.Fatalf("parallel sources = %v", got)
	}
	// Complete the handler that will be visited first at a later timestamp than
	// its peer. Progress must carry the advanced ledger clock into the second
	// seal instead of regressing it to the peer's older completion time.
	first, second := progress.Activated[0], progress.Activated[1]
	if second.Entry.ID < first.Entry.ID {
		first, second = second, first
	}
	makeTerminalNode(t, store, first.Node.ID, workflowruntime.NodeSucceeded, base.Add(10*time.Second))
	makeTerminalNode(t, store, second.Node.ID, workflowruntime.NodeSucceeded, base.Add(8*time.Second))
	progress, err = compensation.Progress(ctx, runID, base.Add(7*time.Second))
	if err != nil || len(progress.Sealed) != 2 || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded || !progress.Ledger.UpdatedAt.Equal(base.Add(10*time.Second)) {
		t.Fatalf("out-of-order parallel seal = %#v, %v", progress, err)
	}
}

func TestCompensationBestEffortProducesPartialOutcome(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-partial")
	seedCompensationEligibility(t, store, runID, "left", "undo-left", base.Add(2*time.Second))
	seedCompensationEligibility(t, store, runID, "right", "undo-right", base.Add(3*time.Second))
	createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
	makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(6*time.Second))
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}, Nodes: []graph.Node{
		{ID: "left", Compensation: &graph.CompensationSpec{Handler: "undo-left"}}, {ID: "right", Compensation: &graph.CompensationSpec{Handler: "undo-right"}}, {ID: "failure"}, {ID: "undo-left"}, {ID: "undo-right"},
	}}
	if _, _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, runID, "partial-freeze", base.Add(7*time.Second)); !errors.Is(err, workflowruntime.ErrCompensationPending) {
		t.Fatalf("partial freeze = %v", err)
	}
	coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
	progress, err := coordinator.Progress(ctx, runID, base.Add(8*time.Second))
	if err != nil || len(progress.Activated) != 2 {
		t.Fatalf("partial activation = %#v, %v", progress, err)
	}
	makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(9*time.Second))
	makeTerminalNode(t, store, progress.Activated[1].Node.ID, workflowruntime.NodeFailed, base.Add(10*time.Second))
	progress, err = coordinator.Progress(ctx, runID, base.Add(11*time.Second))
	if err != nil || progress.Ledger.Status != workflowruntime.CompensationTerminal || progress.Ledger.Outcome != workflowruntime.CompensationOutcomePartial || len(progress.Sealed) != 2 {
		t.Fatalf("partial terminal = %#v, %v", progress, err)
	}
	entries, err := store.ListCompensationEntries(ctx, runID)
	if err != nil || len(entries) != 2 {
		t.Fatalf("partial entries = %#v, %v", entries, err)
	}
	statuses := []workflowruntime.CompensationEntryStatus{entries[0].Status, entries[1].Status}
	slices.Sort(statuses)
	if !slices.Equal(statuses, []workflowruntime.CompensationEntryStatus{workflowruntime.CompensationFailed, workflowruntime.CompensationSucceeded}) {
		t.Fatalf("partial entry statuses = %v", statuses)
	}
}

func TestCompensationBindingFailureDurablyFailsEntryAndContinuesBestEffort(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-binding-failure")
	seedCompensationEligibility(t, store, runID, "a-bad", "undo-bad", base.Add(2*time.Second))
	seedCompensationEligibility(t, store, runID, "z-good", "undo-good", base.Add(3*time.Second))
	transitionRunToSucceeded(t, store, runID, base.Add(6*time.Second))
	run, err := store.LoadRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	manual, err := store.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{
		RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation,
		OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "binding-failure-manual",
		Authorization: values.SHA256Digest([]byte("binding-failure-authorization")), At: base.Add(7 * time.Second),
	})
	if err != nil || manual.Ledger.Status != workflowruntime.CompensationFrozen {
		t.Fatalf("manual = %#v, %v", manual, err)
	}
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{
		{ID: "a-bad", Compensation: &graph.CompensationSpec{Handler: "undo-bad"}},
		{ID: "z-good", Compensation: &graph.CompensationSpec{Handler: "undo-good"}},
		{ID: "undo-bad", InputBindings: map[string]graph.Binding{"token": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: `compensation["compensation.original.outputs.7:missing"]`}}}},
		{ID: "undo-good"},
	}}
	coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
	progress, err := coordinator.Progress(ctx, runID, base.Add(8*time.Second))
	if err != nil || len(progress.Sealed) != 1 || len(progress.Activated) != 1 {
		t.Fatalf("binding failure progress = %#v, %v", progress, err)
	}
	if progress.Sealed[0].Entry.Source.NodeID != "a-bad" || progress.Sealed[0].Entry.Status != workflowruntime.CompensationFailed || progress.Sealed[0].Entry.HandlerFailure == nil || progress.Sealed[0].Entry.HandlerFailure.Code != "compensation_handler_binding_invalid" {
		t.Fatalf("binding failure entry = %#v", progress.Sealed[0].Entry)
	}
	if _, loadErr := store.LoadNodeInvocation(ctx, progress.Sealed[0].Entry.Handler); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		t.Fatalf("invalid binding materialized a handler: %v", loadErr)
	}
	makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(9*time.Second))
	progress, err = coordinator.Progress(ctx, runID, base.Add(10*time.Second))
	if err != nil || progress.Ledger.Status != workflowruntime.CompensationTerminal || progress.Ledger.Outcome != workflowruntime.CompensationOutcomePartial {
		t.Fatalf("binding failure convergence = %#v, %v", progress, err)
	}
	progress, err = coordinator.Progress(ctx, runID, base.Add(11*time.Second))
	if err != nil || len(progress.Activated) != 0 || len(progress.Sealed) != 0 || progress.Ledger.Outcome != workflowruntime.CompensationOutcomePartial {
		t.Fatalf("binding failure replay = %#v, %v", progress, err)
	}
}

func TestCompensationCompletesBeforeFinallyBecomesEligible(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-before-finally")
	seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
	createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
	createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "cleanup"}, workflowruntime.NodePending, 0, base)
	makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(5*time.Second))
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}, Nodes: []graph.Node{
		{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "failure"}, {ID: "undo"}, {ID: "cleanup", Finally: &graph.FinallySpec{}},
	}}
	control := workflowruntime.NewControlFlowCoordinator(store, store, nil)
	if _, _, err := control.ReconcileRunCompletion(ctx, workflow, runID, "compensation-before-finally", base.Add(6*time.Second)); !errors.Is(err, workflowruntime.ErrCompensationPending) {
		t.Fatalf("freeze before finally = %v", err)
	}
	if _, err := control.ProgressFinally(ctx, workflow, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "cleanup"}, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(7*time.Second)); !errors.Is(err, workflowruntime.ErrCompensationPending) {
		t.Fatalf("finally admitted before compensation = %v", err)
	}
	cleanup, _ := store.LoadNodeInvocation(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "cleanup"})
	if cleanup.Status != workflowruntime.NodePending {
		t.Fatalf("finally changed before rollback = %#v", cleanup)
	}
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: cleanup.ID, ExpectedGeneration: cleanup.Generation, To: workflowruntime.NodeReady, At: base.Add(7 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("direct store admitted finalizer before compensation = %v", err)
	}
	compensation := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
	progress, err := compensation.Progress(ctx, runID, base.Add(7*time.Second))
	if err != nil || len(progress.Activated) != 1 {
		t.Fatalf("rollback activation = %#v, %v", progress, err)
	}
	makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeFailed, base.Add(8*time.Second))
	progress, err = compensation.Progress(ctx, runID, base.Add(9*time.Second))
	if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeFailed {
		t.Fatalf("rollback completion = %#v, %v", progress, err)
	}
	retried, err := store.RetryCompensation(ctx, workflowruntime.RetryCompensationRequest{RunID: runID, ExpectedLedgerGeneration: progress.Ledger.Generation, IdempotencyKey: "retry-before-finally", Attestation: values.SHA256Digest([]byte("retry-before-finally")), At: base.Add(10 * time.Second)})
	if err != nil || retried.Status != workflowruntime.CompensationFrozen {
		t.Fatalf("retry while finalizer pristine = %#v, %v", retried, err)
	}
	progress, err = compensation.Progress(ctx, runID, base.Add(11*time.Second))
	if err != nil || len(progress.Activated) != 1 {
		t.Fatalf("retry rollback activation = %#v, %v", progress, err)
	}
	makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeFailed, base.Add(12*time.Second))
	progress, err = compensation.Progress(ctx, runID, base.Add(13*time.Second))
	if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeFailed {
		t.Fatalf("retry rollback completion = %#v, %v", progress, err)
	}
	ready, err := control.ProgressFinally(ctx, workflow, cleanup.ID, values.ExpressionContext{}, values.ExpressionOptions{}, base.Add(14*time.Second))
	if err != nil || ready.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("finally after compensation = %#v, %v", ready, err)
	}
	if _, err := store.RetryCompensation(ctx, workflowruntime.RetryCompensationRequest{RunID: runID, ExpectedLedgerGeneration: progress.Ledger.Generation, IdempotencyKey: "retry-after-finally", Attestation: values.SHA256Digest([]byte("retry-after-finally")), At: base.Add(15 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("retry reopened compensation after finalizer readiness = %v", err)
	}
	makeTerminalNode(t, store, cleanup.ID, workflowruntime.NodeSucceeded, base.Add(16*time.Second))
	completed, intent, err := control.ReconcileRunCompletion(ctx, workflow, runID, "compensation-before-finally", base.Add(17*time.Second))
	if err != nil || completed.Status != workflowruntime.RunFailed || intent == nil || intent.IntendedStatus != workflowruntime.RunFailed {
		t.Fatalf("terminal after finally = %#v, %#v, %v", completed, intent, err)
	}
}

func TestReplayFencesCompensatedEffectsAndRequiresExactIndeterminateAttestation(t *testing.T) {
	t.Run("frozen ledger accepts exact empty-outcome generation proof", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "replay-frozen")
		seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
		transitionRunToSucceeded(t, store, runID, base.Add(5*time.Second))
		run, _ := store.LoadRun(ctx, runID)
		frozen, err := store.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "replay-frozen-manual", Authorization: values.SHA256Digest([]byte("replay-frozen-manual")), At: base.Add(6 * time.Second)})
		if err != nil || frozen.Ledger.Status != workflowruntime.CompensationFrozen || frozen.Ledger.Outcome != "" {
			t.Fatalf("frozen ledger = %#v, %v", frozen, err)
		}
		workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "effect", Kind: "test", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo", Kind: "test", KindVersion: "v1"}}}
		service := workflowruntime.ReplayService{Store: store, Replay: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: compensationReplayRegistry(t)}
		request := workflowruntime.ReplayRequest{SourceRunID: runID, RunID: "replay-frozen-target", FromNodeID: "effect", IdempotencyKey: "replay-frozen-target", At: base.Add(7 * time.Second)}
		if _, err := service.Rerun(ctx, request); !errors.Is(err, workflowruntime.ErrRecoveryUnsafe) {
			t.Fatalf("frozen replay without proof = %v", err)
		}
		request.CompensationAuthorization = &workflowruntime.ReplayCompensationAuthorization{LedgerGeneration: frozen.Ledger.Generation - 1, Digest: values.SHA256Digest([]byte("stale-frozen-proof"))}
		if _, err := service.Rerun(ctx, request); !errors.Is(err, workflowruntime.ErrRecoveryUnsafe) {
			t.Fatalf("frozen replay with stale generation = %v", err)
		}
		request.CompensationAuthorization = &workflowruntime.ReplayCompensationAuthorization{LedgerGeneration: frozen.Ledger.Generation, Digest: values.SHA256Digest([]byte("exact-frozen-proof"))}
		replayed, err := service.Rerun(ctx, request)
		if err != nil || replayed.Provenance.CompensationAuthorization == nil || replayed.Provenance.CompensationAuthorization.LedgerGeneration != frozen.Ledger.Generation || replayed.Provenance.CompensationAuthorization.LedgerOutcome != "" {
			t.Fatalf("frozen replay with exact proof = %#v, %v", replayed, err)
		}
	})

	t.Run("successfully compensated effect is fresh", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "replay-compensated")
		seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
		createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "after"}, workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "after"}, workflowruntime.NodeSucceeded, base.Add(5*time.Second))
		transitionRunToSucceeded(t, store, runID, base.Add(6*time.Second))
		run, _ := store.LoadRun(ctx, runID)
		manual, err := store.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "replay-manual", Authorization: values.SHA256Digest([]byte("replay-manual")), At: base.Add(7 * time.Second)})
		if err != nil || manual.Ledger.Status != workflowruntime.CompensationFrozen {
			t.Fatalf("manual = %#v, %v", manual, err)
		}
		workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "after"}, {ID: "undo"}}}
		coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
		progress, err := coordinator.Progress(ctx, runID, base.Add(8*time.Second))
		if err != nil || len(progress.Activated) != 1 {
			t.Fatalf("activation = %#v, %v", progress, err)
		}
		makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(9*time.Second))
		progress, err = coordinator.Progress(ctx, runID, base.Add(10*time.Second))
		if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded {
			t.Fatalf("compensation = %#v, %v", progress, err)
		}
		workflow = graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{
			{ID: "effect", Kind: "test", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "after", Kind: "test", KindVersion: "v1", Needs: []graph.Need{{Node: "effect"}}}, {ID: "undo", Kind: "test", KindVersion: "v1"},
		}}
		service := workflowruntime.ReplayService{Store: store, Replay: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: compensationReplayRegistry(t)}
		if _, err := service.Rerun(ctx, workflowruntime.ReplayRequest{SourceRunID: runID, RunID: "replay-handler-target", FromNodeID: "undo", IdempotencyKey: "replay-handler-target", At: base.Add(11 * time.Second)}); !errors.Is(err, workflowruntime.ErrInvalidReplay) {
			t.Fatalf("dormant handler replay boundary = %v", err)
		}
		result, err := service.Rerun(ctx, workflowruntime.ReplayRequest{SourceRunID: runID, RunID: "replay-compensated-target", FromNodeID: "after", IdempotencyKey: "replay-compensated-target", At: base.Add(11 * time.Second)})
		if err != nil || result.Outcome != workflowruntime.IdempotencyApplied {
			t.Fatalf("replay = %#v, %v", result, err)
		}
		effect, err := store.LoadNodeInvocation(ctx, workflowruntime.NodeInvocationID{RunID: result.Run.ID, NodeID: "effect"})
		if err != nil || effect.Status != workflowruntime.NodeReady || effect.Origin == workflowruntime.OriginReplayed || effect.LatestAttempt != 0 {
			t.Fatalf("compensated effect was reused = %#v, %v", effect, err)
		}
	})

	t.Run("partial outcome requires exact ledger proof", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "replay-partial")
		seedCompensationEligibility(t, store, runID, "left", "undo", base.Add(2*time.Second))
		seedCompensationEligibility(t, store, runID, "right", "undo", base.Add(3*time.Second))
		createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "after"}, workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "after"}, workflowruntime.NodeSucceeded, base.Add(6*time.Second))
		transitionRunToSucceeded(t, store, runID, base.Add(7*time.Second))
		run, _ := store.LoadRun(ctx, runID)
		_, err := store.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "partial-manual", Authorization: values.SHA256Digest([]byte("partial-manual")), At: base.Add(8 * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "left", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "right", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "after"}, {ID: "undo"}}}
		coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
		progress, err := coordinator.Progress(ctx, runID, base.Add(9*time.Second))
		if err != nil || len(progress.Activated) != 2 {
			t.Fatalf("partial activation = %#v, %v", progress, err)
		}
		makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(10*time.Second))
		makeTerminalNode(t, store, progress.Activated[1].Node.ID, workflowruntime.NodeFailed, base.Add(11*time.Second))
		progress, err = coordinator.Progress(ctx, runID, base.Add(12*time.Second))
		if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomePartial {
			t.Fatalf("partial outcome = %#v, %v", progress, err)
		}
		workflow = graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{
			{ID: "left", Kind: "test", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "right", Kind: "test", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "after", Kind: "test", KindVersion: "v1", Needs: []graph.Need{{Node: "left"}, {Node: "right"}}}, {ID: "undo", Kind: "test", KindVersion: "v1"},
		}}
		service := workflowruntime.ReplayService{Store: store, Replay: store, Inputs: store, Control: store, Plans: staticRecoveryPlans{graph: workflow}, Registry: compensationReplayRegistry(t)}
		request := workflowruntime.ReplayRequest{SourceRunID: runID, RunID: "replay-partial-target", FromNodeID: "after", IdempotencyKey: "replay-partial-target", At: base.Add(13 * time.Second)}
		if _, err := service.Rerun(ctx, request); !errors.Is(err, workflowruntime.ErrRecoveryUnsafe) {
			t.Fatalf("partial replay without attestation = %v", err)
		}
		request.CompensationAuthorization = &workflowruntime.ReplayCompensationAuthorization{LedgerGeneration: progress.Ledger.Generation - 1, LedgerOutcome: progress.Ledger.Outcome, Digest: values.SHA256Digest([]byte("wrong-generation"))}
		if _, err := service.Rerun(ctx, request); !errors.Is(err, workflowruntime.ErrRecoveryUnsafe) {
			t.Fatalf("partial replay with stale attestation = %v", err)
		}
		request.CompensationAuthorization = &workflowruntime.ReplayCompensationAuthorization{LedgerGeneration: progress.Ledger.Generation, LedgerOutcome: progress.Ledger.Outcome, Digest: values.SHA256Digest([]byte("host-bound-attestation"))}
		result, err := service.Rerun(ctx, request)
		if err != nil || result.Provenance.CompensationAuthorization == nil || result.Provenance.CompensationAuthorization.Digest != request.CompensationAuthorization.Digest {
			t.Fatalf("authorized partial replay = %#v, %v", result, err)
		}
	})
}

func compensationReplayRegistry(t *testing.T) *stepkind.MemoryRegistry {
	t.Helper()
	kind := stepkindtest.NewNoopKind("test", "v1")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectCompute}
	kind.SpecValue.Idempotency = graph.IdempotencyIntrinsic
	kind.SpecValue.RetrySafety = stepkind.RetrySafe
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestCompensationCoordinatorRecordsNoLedgerChildAndWaitsForRealChildLedger(t *testing.T) {
	t.Run("no ledger", func(t *testing.T) {
		ctx, store, base, runID := controlFixture(t, "compensation-child-none")
		childID := workflowruntime.RunID("compensation-child-none-child")
		createRun(t, store, childID, base)
		transitionRunToSucceeded(t, store, childID, base.Add(time.Second))
		seedCompensationEligibility(t, store, runID, "parent-call", "undo-parent", base.Add(2*time.Second), childID)
		workflow := compensationFailureGraph("parent-call", "undo-parent")
		createNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(4*time.Second))
		_, _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, runID, "child-none-freeze", base.Add(5*time.Second))
		if !errors.Is(err, workflowruntime.ErrCompensationPending) {
			t.Fatal(err)
		}
		progress, err := (workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}).Progress(ctx, runID, base.Add(6*time.Second))
		if err != nil || len(progress.Activated) != 1 || progress.Activated[0].Entry.ChildResolution != workflowruntime.CompensationChildNoLedger {
			t.Fatalf("no-ledger child = %#v, %v", progress, err)
		}
		inputs, loadErr := store.LoadValues(ctx, *progress.Activated[0].Node.Inputs)
		if loadErr != nil || inputs[workflowruntime.CompensationHandlerChildRunIDInput].Inline != string(childID) || inputs[workflowruntime.CompensationHandlerChildResolutionInput].Inline != string(workflowruntime.CompensationChildNoLedger) {
			t.Fatalf("no-ledger handler child inputs = %#v, %v", inputs, loadErr)
		}
	})

	t.Run("real child ledger", func(t *testing.T) {
		ctx, store, base, parentID := controlFixture(t, "compensation-child-ledger")
		childID := workflowruntime.RunID("compensation-child-ledger-child")
		createRun(t, store, childID, base)
		childRun, _ := store.LoadRun(ctx, childID)
		if _, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: childID, ExpectedGeneration: childRun.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
		seedCompensationEligibility(t, store, childID, "child-effect", "undo-child", base.Add(2*time.Second))
		transitionRunToSucceeded(t, store, childID, base.Add(5*time.Second))
		seedCompensationEligibility(t, store, parentID, "parent-call", "undo-parent", base.Add(2*time.Second), childID)
		parentGraph := compensationFailureGraph("parent-call", "undo-parent")
		createNode(t, store, workflowruntime.NodeInvocationID{RunID: parentID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
		makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: parentID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(5*time.Second))
		_, _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, parentGraph, parentID, "parent-freeze", base.Add(6*time.Second))
		if !errors.Is(err, workflowruntime.ErrCompensationPending) {
			t.Fatal(err)
		}
		childGraph := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "child-effect", Compensation: &graph.CompensationSpec{Handler: "undo-child"}}, {ID: "undo-child"}, {ID: "parent-call", Compensation: &graph.CompensationSpec{Handler: "undo-parent"}}, {ID: "undo-parent"}}}
		coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: childGraph}}
		parentProgress, err := coordinator.Progress(ctx, parentID, base.Add(7*time.Second))
		if err != nil || len(parentProgress.Activated) != 0 {
			t.Fatalf("parent did not wait for child = %#v, %v", parentProgress, err)
		}
		childEntries, err := store.ListCompensationEntries(ctx, childID)
		if err != nil || len(childEntries) != 1 || childEntries[0].Status != workflowruntime.CompensationActive {
			t.Fatalf("parent-driven child activation = %#v, %v", childEntries, err)
		}
		makeTerminalNode(t, store, childEntries[0].Handler, workflowruntime.NodeSucceeded, base.Add(9*time.Second))
		recovered, err := store.RecoverCompensation(ctx, 1)
		if err != nil || len(recovered) != 1 || recovered[0].RunID != parentID {
			t.Fatalf("oldest-only recovery page = %#v, %v", recovered, err)
		}
		parentProgress, err = coordinator.Progress(ctx, recovered[0].RunID, base.Add(10*time.Second))
		if err != nil || len(parentProgress.Activated) != 1 || parentProgress.Activated[0].Entry.ChildResolution != workflowruntime.CompensationChildSucceeded {
			t.Fatalf("limit-one recovery did not progress child then parent = %#v, %v", parentProgress, err)
		}
		inputs, loadErr := store.LoadValues(ctx, *parentProgress.Activated[0].Node.Inputs)
		if loadErr != nil || inputs[workflowruntime.CompensationHandlerChildResolutionInput].Inline != string(workflowruntime.CompensationChildSucceeded) {
			t.Fatalf("parent handler child outcome input = %#v, %v", inputs, loadErr)
		}
	})
}

func TestCompensationCoordinatorConvergesWhenTerminalChildLedgerCannotRun(t *testing.T) {
	for _, test := range []struct {
		name        string
		childStatus workflowruntime.RunStatus
	}{
		{name: "failed child collecting ledger", childStatus: workflowruntime.RunFailed},
		{name: "succeeded child without manual authorization", childStatus: workflowruntime.RunSucceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, store, base, parentID := controlFixture(t, "child-unavailable-"+string(test.childStatus))
			childID := workflowruntime.RunID("child-unavailable-run-" + string(test.childStatus))
			createRun(t, store, childID, base)
			child, err := store.LoadRun(ctx, childID)
			if err != nil {
				t.Fatal(err)
			}
			childRunning, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: childID, ExpectedGeneration: child.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)})
			if err != nil {
				t.Fatal(err)
			}
			seedCompensationEligibility(t, store, childID, "child-effect", "undo-child", base.Add(2*time.Second))
			child, err = store.LoadRun(ctx, childID)
			if err != nil {
				t.Fatal(err)
			}
			if child.Generation != childRunning.Snapshot.Generation {
				t.Fatalf("eligibility changed child run = %#v", child)
			}
			if _, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: childID, ExpectedGeneration: child.Generation, To: test.childStatus, At: base.Add(5 * time.Second)}); err != nil {
				t.Fatal(err)
			}
			seedCompensationEligibility(t, store, parentID, "parent-call", "undo-parent", base.Add(2*time.Second), childID)
			createNode(t, store, workflowruntime.NodeInvocationID{RunID: parentID, NodeID: "child-effect"}, workflowruntime.NodePending, 0, base)
			makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: parentID, NodeID: "child-effect"}, workflowruntime.NodeSucceeded, base.Add(5*time.Second))
			createNode(t, store, workflowruntime.NodeInvocationID{RunID: parentID, NodeID: "failure"}, workflowruntime.NodePending, 0, base)
			makeTerminalNode(t, store, workflowruntime.NodeInvocationID{RunID: parentID, NodeID: "failure"}, workflowruntime.NodeFailed, base.Add(6*time.Second))
			workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}, Nodes: []graph.Node{
				{ID: "parent-call", Compensation: &graph.CompensationSpec{Handler: "undo-parent"}}, {ID: "undo-parent"}, {ID: "failure"},
				{ID: "child-effect", Compensation: &graph.CompensationSpec{Handler: "undo-child"}}, {ID: "undo-child"},
			}}
			if _, _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).ReconcileRunCompletion(ctx, workflow, parentID, "child-unavailable-freeze-"+string(test.childStatus), base.Add(7*time.Second)); !errors.Is(err, workflowruntime.ErrCompensationPending) {
				t.Fatalf("parent freeze = %v", err)
			}
			coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
			progress, err := coordinator.Progress(ctx, parentID, base.Add(8*time.Second))
			if err != nil || len(progress.Activated) != 1 || progress.Activated[0].Entry.ChildResolution != workflowruntime.CompensationChildFailed {
				t.Fatalf("parent child failure resolution = %#v, %v", progress, err)
			}
			inputs, loadErr := store.LoadValues(ctx, *progress.Activated[0].Node.Inputs)
			if loadErr != nil || inputs[workflowruntime.CompensationHandlerChildResolutionInput].Inline != string(workflowruntime.CompensationChildFailed) {
				t.Fatalf("parent handler child resolution input = %#v, %v", inputs, loadErr)
			}
			makeTerminalNode(t, store, progress.Activated[0].Node.ID, workflowruntime.NodeSucceeded, base.Add(9*time.Second))
			progress, err = coordinator.Progress(ctx, parentID, base.Add(10*time.Second))
			if err != nil || progress.Ledger.Status != workflowruntime.CompensationTerminal || progress.Ledger.Outcome != workflowruntime.CompensationOutcomePartial {
				t.Fatalf("parent best-effort convergence = %#v, %v", progress, err)
			}
		})
	}
}

func TestCompensationHandlerInputNamesArePublicExactAndInjective(t *testing.T) {
	underscore, err := workflowruntime.CompensationHandlerInputName(workflowruntime.CompensationHandlerOriginalInputs, "foo_bar")
	if err != nil {
		t.Fatal(err)
	}
	dash, err := workflowruntime.CompensationHandlerInputName(workflowruntime.CompensationHandlerOriginalInputs, "foo-bar")
	if err != nil {
		t.Fatal(err)
	}
	unicodeName, err := workflowruntime.CompensationHandlerInputName(workflowruntime.CompensationHandlerReceipt, "résumé.原文")
	if err != nil {
		t.Fatal(err)
	}
	if underscore != "compensation.original.inputs.7:foo_bar" || dash != "compensation.original.inputs.7:foo-bar" || underscore == dash || unicodeName != "compensation.receipt.15:résumé.原文" {
		t.Fatalf("handler names underscore=%q dash=%q unicode=%q", underscore, dash, unicodeName)
	}
	if _, err := workflowruntime.CompensationHandlerInputName("unknown", "value"); !errors.Is(err, workflowruntime.ErrInvalidCompensation) {
		t.Fatalf("unknown namespace = %v", err)
	}
}

func TestNestedChildOutcomeContributesToParentLedgerSummary(t *testing.T) {
	for _, test := range []struct {
		name       string
		resolution workflowruntime.CompensationChildResolution
		want       workflowruntime.CompensationOutcome
	}{
		{name: "failed child and successful parent handler", resolution: workflowruntime.CompensationChildFailed, want: workflowruntime.CompensationOutcomePartial},
		{name: "canceled child and successful parent handler", resolution: workflowruntime.CompensationChildCanceled, want: workflowruntime.CompensationOutcomeCanceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, store, base, runID := controlFixture(t, "nested-summary-"+string(test.resolution))
			entry := seedCompensationEligibility(t, store, runID, "parent-call", "undo-parent", base.Add(2*time.Second), workflowruntime.RunID("nested-child-"+string(test.resolution)))
			transitionRunToSucceeded(t, store, runID, base.Add(5*time.Second))
			run, _ := store.LoadRun(ctx, runID)
			manual, err := store.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "nested-manual-" + string(test.resolution), Authorization: values.SHA256Digest([]byte("nested-manual-" + string(test.resolution))), At: base.Add(6 * time.Second)})
			if err != nil || len(manual.Entries) != 1 {
				t.Fatalf("manual = %#v, %v", manual, err)
			}
			activated, err := store.ActivateCompensationEntry(ctx, workflowruntime.ActivateCompensationEntryRequest{RunID: runID, EntryID: entry.ID, ExpectedLedgerGeneration: manual.Ledger.Generation, ExpectedEntryGeneration: manual.Entries[0].Generation, Inputs: values.ValueSet{}, ChildResolution: test.resolution, At: base.Add(7 * time.Second)})
			if err != nil {
				t.Fatal(err)
			}
			makeTerminalNode(t, store, activated.Node.ID, workflowruntime.NodeSucceeded, base.Add(8*time.Second))
			handler, _ := store.LoadNodeInvocation(ctx, activated.Node.ID)
			sealed, err := store.SealCompensationEntry(ctx, workflowruntime.SealCompensationEntryRequest{RunID: runID, EntryID: entry.ID, ExpectedLedgerGeneration: activated.Ledger.Generation, ExpectedEntryGeneration: activated.Entry.Generation, ExpectedNodeGeneration: handler.Generation, At: base.Add(9 * time.Second)})
			wantEntry := workflowruntime.CompensationPartial
			if test.resolution == workflowruntime.CompensationChildCanceled {
				wantEntry = workflowruntime.CompensationCanceled
			}
			if err != nil || sealed.Entry.Status != wantEntry || sealed.Entry.ChildResolution != test.resolution || sealed.Ledger.Outcome != test.want {
				t.Fatalf("nested summary = %#v, %v", sealed, err)
			}
		})
	}
}

func TestManualCompensationCancellationAndRetryResumeSameLedgerWithStableKey(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-manual-retry")
	entry := seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
	transitionRunToSucceeded(t, store, runID, base.Add(5*time.Second))
	run, err := store.LoadRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	manualRequest := workflowruntime.BeginManualCompensationRequest{RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "manual-start", Authorization: values.SHA256Digest([]byte("authorized-manual")), At: base.Add(6 * time.Second)}
	manual, err := store.BeginManualCompensation(ctx, manualRequest)
	if err != nil || manual.Ledger.Trigger != graph.CompensationManual || manual.Ledger.Status != workflowruntime.CompensationFrozen {
		t.Fatalf("BeginManualCompensation = %#v, %v", manual, err)
	}
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}
	coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
	progress, err := coordinator.Progress(ctx, runID, base.Add(7*time.Second))
	if err != nil || len(progress.Activated) != 1 {
		t.Fatalf("manual activation = %#v, %v", progress, err)
	}
	if _, err := store.CancelCompensation(ctx, workflowruntime.CancelCompensationRequest{RunID: runID, ExpectedLedgerGeneration: progress.Ledger.Generation, IdempotencyKey: manualRequest.IdempotencyKey, Reason: "cross-operation collision", At: base.Add(8 * time.Second)}); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("cross-operation compensation key = %v", err)
	}
	handler := progress.Activated[0].Node
	claim := claimNode(t, store, handler.ID, handler.ClaimGeneration, "rollback-worker", "rollback-token", "rollback-claim", base.Add(8*time.Second), base.Add(time.Hour))
	claimed, _ := store.LoadNodeInvocation(ctx, handler.ID)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: handler.ID, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: workflowruntime.ExecutorMetadata{Kind: "undo", Version: "v1"}, Inputs: handler.Inputs, At: base.Add(8 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	cancelRequest := workflowruntime.CancelCompensationRequest{RunID: runID, ExpectedLedgerGeneration: progress.Ledger.Generation, IdempotencyKey: "cancel-rollback", Reason: "operator canceled rollback", At: base.Add(9 * time.Second)}
	ledger, err := store.CancelCompensation(ctx, cancelRequest)
	if err != nil || ledger.Status == workflowruntime.CompensationTerminal {
		t.Fatalf("CancelCompensation = %#v, %v", ledger, err)
	}
	intents, err := store.RecoverCancellationIntents(ctx, workflowruntime.CancellationIntentQuery{RunID: runID})
	if err != nil || len(intents) != 1 || intents[0].Attempt == nil || *intents[0].Attempt != started.Attempt.ID {
		t.Fatalf("ordinary cancellation intent = %#v, %v", intents, err)
	}
	undo := stepkindtest.NewNoopKind("undo", "v1")
	undo.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	undo.SpecValue.Idempotency = graph.IdempotencyKeyed
	registry := stepkind.NewRegistry()
	if err := registry.Register(undo); err != nil {
		t.Fatal(err)
	}
	canceledAttempt := false
	cancellation := workflowruntime.CancellationCoordinator{Store: store, Registry: registry, Attempts: compensationAttemptCanceler(func(_ context.Context, attempt workflowruntime.AttemptSnapshot) error {
		canceledAttempt = attempt.ID == started.Attempt.ID
		return nil
	}), Now: func() time.Time { return base.Add(10 * time.Second) }}
	if _, failures, err := cancellation.Recover(ctx, workflowruntime.CancellationIntentQuery{RunID: runID}); err != nil || len(failures) != 0 || !canceledAttempt {
		t.Fatalf("recover cancellation failures=%v err=%v canceled=%t", failures, err, canceledAttempt)
	}
	progress, err = coordinator.Progress(ctx, runID, base.Add(11*time.Second))
	if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeCanceled {
		t.Fatalf("seal canceled compensation = %#v, %v", progress, err)
	}
	manualReplay, err := store.BeginManualCompensation(ctx, manualRequest)
	if err != nil || manualReplay.Outcome != workflowruntime.IdempotencyReplayed || manualReplay.Ledger.Generation != progress.Ledger.Generation || manualReplay.Ledger.Outcome != workflowruntime.CompensationOutcomeCanceled {
		t.Fatalf("post-progress manual replay = %#v, %v", manualReplay, err)
	}
	cancelReplay, err := store.CancelCompensation(ctx, cancelRequest)
	if err != nil || cancelReplay.Generation != progress.Ledger.Generation || cancelReplay.Outcome != workflowruntime.CompensationOutcomeCanceled {
		t.Fatalf("post-progress cancel replay = %#v, %v", cancelReplay, err)
	}
	currentRun, err := store.LoadRun(ctx, runID)
	if err != nil || currentRun.Status != workflowruntime.RunSucceeded || currentRun.Generation != run.Generation {
		t.Fatalf("manual rollback mutated original run = %#v, %v", currentRun, err)
	}
	retryRequest := workflowruntime.RetryCompensationRequest{RunID: runID, ExpectedLedgerGeneration: progress.Ledger.Generation, IdempotencyKey: "retry-rollback", Attestation: values.SHA256Digest([]byte("authorized-retry")), At: base.Add(12 * time.Second)}
	retried, err := store.RetryCompensation(ctx, retryRequest)
	if err != nil || retried.Status != workflowruntime.CompensationFrozen || len(retried.Cycles) != 2 {
		t.Fatalf("RetryCompensation = %#v, %v", retried, err)
	}
	progress, err = coordinator.Progress(ctx, runID, base.Add(13*time.Second))
	if err != nil || len(progress.Activated) != 1 || progress.Activated[0].Entry.ID != entry.ID || len(progress.Activated[0].Entry.History) != 1 || progress.Activated[0].Entry.Handler == handler.ID {
		t.Fatalf("retry activation = %#v, %v", progress, err)
	}
	stableKey := ""
	undo.ExecuteFunc = func(_ context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		stableKey = invocation.Invocation.IdempotencyKey
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}, nil
	}
	retryHandler := progress.Activated[0].Node
	retryClaim, ok, claimErr := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(ctx, workflowruntime.ReadyClaimRequest{RunID: runID, Owner: "rollback-worker-2", Token: "rollback-token-2", IdempotencyKey: "rollback-claim-2", Now: base.Add(14 * time.Second), LeaseUntil: base.Add(time.Hour)})
	if claimErr != nil || !ok || retryClaim.Candidate.InvocationID != retryHandler.ID {
		t.Fatalf("retry claim = %#v, %t, %v", retryClaim, ok, claimErr)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(15 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	dispatched, err := dispatcher.Dispatch(ctx, workflowruntime.DispatchRequest{Claim: retryClaim, Node: graph.Node{ID: "undo", Kind: "undo", KindVersion: "v1"}, IdempotencyKey: "caller-must-not-control-rollback-key"})
	if err != nil || dispatched.Node.Status != workflowruntime.NodeSucceeded || stableKey != "compensation:"+entry.ID {
		t.Fatalf("retry dispatch = %#v, %v stable_key=%q", dispatched, err, stableKey)
	}
	progress, err = coordinator.Progress(ctx, runID, base.Add(16*time.Second))
	if err != nil || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded || len(progress.Ledger.Cycles) != 2 || progress.Ledger.Cycles[1].Attestation == "" {
		t.Fatalf("retry completion = %#v, %v", progress, err)
	}
	retryReplay, err := store.RetryCompensation(ctx, retryRequest)
	if err != nil || retryReplay.Generation != progress.Ledger.Generation || retryReplay.Outcome != workflowruntime.CompensationOutcomeSucceeded {
		t.Fatalf("post-progress retry replay = %#v, %v", retryReplay, err)
	}
}

func TestCompensationHandlerPrestartContractFailureConvergesLedger(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-handler-contract")
	seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
	transitionRunToSucceeded(t, store, runID, base.Add(5*time.Second))
	run, _ := store.LoadRun(ctx, runID)
	manual, err := store.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "contract-manual", Authorization: values.SHA256Digest([]byte("contract-manual")), At: base.Add(6 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}
	coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
	progress, err := coordinator.Progress(ctx, runID, base.Add(7*time.Second))
	if err != nil || len(progress.Activated) != 1 {
		t.Fatalf("activation = %#v, %v", progress, err)
	}
	undo := stepkindtest.NewNoopKind("undo", "v1")
	undo.SpecValue.InputSchema = objectSchema("required-input", "string")
	undo.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	undo.SpecValue.Idempotency = graph.IdempotencyKeyed
	registry := stepkind.NewRegistry()
	if err := registry.Register(undo); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(ctx, workflowruntime.ReadyClaimRequest{RunID: runID, Owner: "contract-worker", Token: "contract-token", IdempotencyKey: "contract-claim", Now: base.Add(8 * time.Second), LeaseUntil: base.Add(time.Hour)})
	if err != nil || !ok {
		t.Fatalf("claim = %#v, %t, %v", claim, ok, err)
	}
	dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(9 * time.Second) }})
	result, dispatchErr := dispatcher.Dispatch(ctx, workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: "undo", Kind: "undo", KindVersion: "v1"}})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Retryable {
		t.Fatalf("prestart terminal result = %#v, %v", result, dispatchErr)
	}
	progress, err = coordinator.Progress(ctx, runID, base.Add(10*time.Second))
	if err != nil || progress.Ledger.Status != workflowruntime.CompensationTerminal || progress.Ledger.Outcome != workflowruntime.CompensationOutcomeFailed {
		t.Fatalf("converged ledger = %#v, %v", progress, err)
	}
	if manual.Ledger.RunID != progress.Ledger.RunID {
		t.Fatal("compensation ledger identity changed")
	}
}

func TestCrashedCompensationHandlerOnTerminalParentCreatesDurableRetry(t *testing.T) {
	ctx, store, base, runID := controlFixture(t, "compensation-handler-crash")
	seedCompensationEligibility(t, store, runID, "effect", "undo", base.Add(2*time.Second))
	transitionRunToSucceeded(t, store, runID, base.Add(5*time.Second))
	run, _ := store.LoadRun(ctx, runID)
	if _, err := store.BeginManualCompensation(ctx, workflowruntime.BeginManualCompensationRequest{RunID: runID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: run.Generation, OriginalStatus: workflowruntime.RunSucceeded, IdempotencyKey: "crash-manual", Authorization: values.SHA256Digest([]byte("crash-manual")), At: base.Add(6 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	workflow := graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}, Nodes: []graph.Node{{ID: "effect", Compensation: &graph.CompensationSpec{Handler: "undo"}}, {ID: "undo"}}}
	coordinator := workflowruntime.CompensationCoordinator{Store: store, Compensation: store, Plans: staticRecoveryPlans{graph: workflow}}
	progress, err := coordinator.Progress(ctx, runID, base.Add(7*time.Second))
	if err != nil || len(progress.Activated) != 1 {
		t.Fatalf("activation = %#v, %v", progress, err)
	}
	handler := progress.Activated[0].Node
	claim := claimNode(t, store, handler.ID, handler.ClaimGeneration, "crash-worker", "crash-token", "crash-claim", base.Add(8*time.Second), base.Add(9*time.Second))
	claimed, _ := store.LoadNodeInvocation(ctx, handler.ID)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: handler.ID, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: workflowruntime.ExecutorMetadata{Kind: "undo", Version: "v1"}, Inputs: handler.Inputs, At: base.Add(8 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := store.ReconcileCrashedAttempt(ctx, workflowruntime.ReconcileCrashedAttemptRequest{
		Attempt: started.Attempt.ID, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		IdempotencyKey: "crash-handler-recovery", At: base.Add(10 * time.Second),
		Decision: workflowruntime.CrashRecoveryDecision{Action: workflowruntime.CrashRetry, Policy: workflowruntime.RepeatPolicyDecision{Allow: true, Code: "handler_retry", Reason: "stable compensation handler is retryable"}, Retry: &workflowruntime.RetryDecision{Retry: true, Reason: workflowruntime.RetryReasonEligible, FireAt: base.Add(11 * time.Second), Delay: time.Second}},
	})
	if err != nil || recovered.Node.Status != workflowruntime.NodeWaiting || recovered.Attempt.Status != workflowruntime.NodeCrashed || recovered.Activation == nil {
		t.Fatalf("crash recovery = %#v, %v", recovered, err)
	}
	immutable, err := store.LoadRun(ctx, runID)
	if err != nil || immutable.Status != workflowruntime.RunSucceeded || immutable.Generation != run.Generation {
		t.Fatalf("terminal parent changed = %#v, %v", immutable, err)
	}
}

func compensationFailureGraph(source, handler string) graph.Graph {
	return graph.Graph{ID: testPlan().ID, Version: testPlan().Version, Compensation: &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}, Nodes: []graph.Node{{ID: source, Compensation: &graph.CompensationSpec{Handler: handler}}, {ID: "failure"}, {ID: handler}}}
}

func transitionRunToSucceeded(t *testing.T, store workflowruntime.StateStore, runID workflowruntime.RunID, at time.Time) {
	t.Helper()
	run, err := store.LoadRun(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status == workflowruntime.RunPending {
		transition, transitionErr := store.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: at})
		if transitionErr != nil {
			t.Fatal(transitionErr)
		}
		run = transition.Snapshot
	}
	if _, err := store.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: runID, ExpectedGeneration: run.Generation, To: workflowruntime.RunSucceeded, At: at}); err != nil {
		t.Fatal(err)
	}
}

func TestFinishCompensableAttemptReplayBindsFullEligibilityAndTerminalResult(t *testing.T) {
	_, store, base, runID := controlFixture(t, "compensation-finish-replay")
	entry, request := seedCompensationEligibilityRequest(t, store, runID, "effect", "undo", base.Add(2*time.Second), "child-a")
	var compensation workflowruntime.CompensationStore = store
	replayed, err := compensation.FinishCompensableAttempt(t.Context(), request)
	if err != nil || replayed.Entry.ID != entry.ID || replayed.Finish.Attempt.ID != entry.SourceAttempt {
		t.Fatalf("same-request replay = %#v, %v", replayed, err)
	}

	checks := map[string]func(workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest{
		"handler": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.HandlerNodeID = "undo-other"
			return changed
		},
		"child": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.ChildRunID = "child-b"
			changed.Eligibility.Receipt.ChildRunID = "child-b"
			return changed
		},
		"evidence": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.Evidence.Operation = "fixture.changed"
			changed.Eligibility.Receipt.Operation = "fixture.changed"
			return changed
		},
		"original output": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.OriginalOutputs = values.ValueSet{"result": dispatchValue(t, "result", "different")}
			return changed
		},
		"original error": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Eligibility.OriginalError = values.ValueSet{"error": dispatchValue(t, "error", "different")}
			return changed
		},
		"terminal failure": func(changed workflowruntime.FinishCompensableAttemptRequest) workflowruntime.FinishCompensableAttemptRequest {
			changed.Finish.AttemptStatus = workflowruntime.NodeFailed
			changed.Finish.NextNodeStatus = workflowruntime.NodeFailed
			changed.Finish.Failure = &workflowruntime.Failure{Code: "changed", Message: "different terminal result"}
			return changed
		},
	}
	for name, change := range checks {
		t.Run(name, func(t *testing.T) {
			if _, replayErr := compensation.FinishCompensableAttempt(t.Context(), change(request)); !errors.Is(replayErr, workflowruntime.ErrIdempotencyConflict) {
				t.Fatalf("changed replay error = %v", replayErr)
			}
		})
	}
}

func seedCompensationEligibility(t *testing.T, store workflowruntime.StateStore, runID workflowruntime.RunID, source, handler string, at time.Time, child ...workflowruntime.RunID) workflowruntime.CompensationEntrySnapshot {
	t.Helper()
	entry, _ := seedCompensationEligibilityRequest(t, store, runID, source, handler, at, child...)
	return entry
}

func seedCompensationEligibilityRequest(t *testing.T, store workflowruntime.StateStore, runID workflowruntime.RunID, source, handler string, at time.Time, child ...workflowruntime.RunID) (workflowruntime.CompensationEntrySnapshot, workflowruntime.FinishCompensableAttemptRequest) {
	t.Helper()
	compensation, ok := store.(workflowruntime.CompensationStore)
	if !ok {
		t.Fatal("fixture store lacks compensation")
	}
	id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: source}
	createNode(t, store, id, workflowruntime.NodeReady, 0, at)
	node, err := store.LoadNodeInvocation(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	claim := claimNode(t, store, id, node.ClaimGeneration, "forward-"+source, "token-"+source, "claim-"+source, at.Add(time.Second), at.Add(time.Hour))
	claimed, err := store.LoadNodeInvocation(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: claim, Executor: testExecutor(), At: at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	evidence := stepkind.ReversibilityEvidence{Operation: "fixture." + source, ReceiptSchema: graph.Schema{}}
	var childRunID workflowruntime.RunID
	if len(child) != 0 {
		childRunID = child[0]
		if _, loadErr := store.LoadRun(t.Context(), childRunID); errors.Is(loadErr, workflowruntime.ErrNotFound) {
			createRun(t, store, childRunID, at.Add(-time.Second))
		} else if loadErr != nil {
			t.Fatal(loadErr)
		}
		cancellation, ok := store.(workflowruntime.CancellationStore)
		if !ok {
			t.Fatal("fixture store lacks child run links")
		}
		if err := cancellation.RecordChildRun(t.Context(), workflowruntime.ChildRunLink{ParentRunID: runID, Invocation: id, ChildRunID: childRunID, Policy: graph.ParentCloseCancel, CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	request := workflowruntime.FinishCompensableAttemptRequest{
		Finish:      workflowruntime.FinishNodeAttemptRequest{InvocationID: id, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: claim, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: at.Add(2 * time.Second)},
		Eligibility: workflowruntime.CompensationEligibility{PlanDigest: testPlan().Digest, HandlerNodeID: handler, Evidence: evidence, Receipt: stepkind.CompensationReceipt{Operation: evidence.Operation, Values: values.ValueSet{}, ChildRunID: string(childRunID)}, ChildRunID: childRunID},
	}
	finished, err := compensation.FinishCompensableAttempt(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	return finished.Entry, request
}

func newCompensableDispatchKind(t *testing.T) *compensableDispatchKind {
	t.Helper()
	kind := &compensableDispatchKind{Kind: stepkindtest.NewNoopKind("compensable", "v1")}
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	kind.SpecValue.Idempotency = graph.IdempotencyKeyed
	kind.SpecValue.RetrySafety = stepkind.RetryRequiresIdempotency
	kind.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	kind.evidence = stepkind.ReversibilityEvidence{Operation: "fixture.create", ReceiptSchema: objectSchema("token", "string")}
	return kind
}
