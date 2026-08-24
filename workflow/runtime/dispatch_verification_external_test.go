package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestStepDispatcherVerifiesLiteralEvidenceBeforeDurableSuccess(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "verification-success")
	kinds := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("fixture", "v1")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	kind.ExecuteFunc = func(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		if prepared.Invocation.Activity == nil {
			t.Fatal("runtime did not issue an activity recorder")
		}
		if err := prepared.Invocation.Activity.RecordToolCall(context.Background(), verification.ToolCall{Server: "fixture", Tool: "read", Outcome: verification.ActivitySucceeded}); err != nil {
			t.Fatal(err)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "ok")}}, nil
	}
	if registerErr := kinds.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	modifier := &graph.VerificationSpec{Checks: []graph.VerificationCheck{
		{Kind: verification.CheckNoError, Config: graph.Config{}},
		{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"server": "fixture", "tool": "read"}},
	}}
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}, Verification: modifier}})
	if err != nil || result.Node.Status != workflowruntime.NodeSucceeded || result.Verification == nil || result.Verification.Report.Status != verification.ReportPassed {
		t.Fatalf("Dispatch() = %#v, %v", result, err)
	}
	events, err := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: node.ID.RunID})
	if err != nil {
		t.Fatal(err)
	}
	verificationIndex, finishIndex := -1, -1
	for index, event := range events {
		switch event.Type {
		case workflowruntime.EventNodeVerificationCompleted:
			verificationIndex = index
			if event.Values == nil || event.Redaction != values.RedactionPrivate || event.Retention != values.RetentionRun || event.Attempt == nil || *event.Attempt != result.Attempt.ID {
				t.Fatalf("verification event = %#v", event)
			}
		case workflowruntime.EventNodeAttemptFinished:
			finishIndex = index
		}
	}
	if verificationIndex < 0 || finishIndex < 0 || verificationIndex >= finishIndex {
		t.Fatalf("event order = %#v", events)
	}
	loaded, err := store.LoadValues(context.Background(), result.Verification.Ref)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("LoadValues(verification) = %#v, %v", loaded, err)
	}
}

func TestVerificationDecisionUsesOrdinaryRetryAndCatchFailureContract(t *testing.T) {
	t.Run("retry disposition", func(t *testing.T) {
		store, claim, node, now := dispatchFixture(t, "verification-retry")
		kinds := stepkind.NewRegistry()
		kind := stepkindtest.NewNoopKind("fixture", "v1")
		if err := kinds.Register(kind); err != nil {
			t.Fatal(err)
		}
		dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
			Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) },
			FailureDisposition: workflowruntime.FailureDispositionFunc(func(context.Context, workflowruntime.FailureDispositionRequest) (workflowruntime.NodeStatus, error) {
				return workflowruntime.NodeReady, nil
			}),
		})
		if err != nil {
			t.Fatal(err)
		}
		result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{
			ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{},
			Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "write"}}}},
		}})
		if dispatchErr == nil || result.Node.Status != workflowruntime.NodeReady || result.Attempt.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Code != "verification_failed" || result.Verification == nil {
			t.Fatalf("Dispatch() = %#v, %v", result, dispatchErr)
		}
	})

	t.Run("catch selector", func(t *testing.T) {
		store, claim, node, now := dispatchFixture(t, "verification-catch")
		kinds := stepkind.NewRegistry()
		if err := kinds.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
			t.Fatal(err)
		}
		dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) }})
		source := graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{},
			Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "write"}}}},
			Catch:        []graph.CatchRule{{Errors: []string{"verification_failed"}, Targets: []string{"handler"}}},
		}
		result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: source})
		if dispatchErr == nil || result.Node.Status != workflowruntime.NodeFailed {
			t.Fatalf("Dispatch() = %#v, %v", result, dispatchErr)
		}
		handlerID := workflowruntime.NodeInvocationID{RunID: node.ID.RunID, NodeID: "handler"}
		if _, err := store.CreateNodeInvocation(context.Background(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: handlerID, Status: workflowruntime.NodePending, CreatedAt: now, UpdatedAt: now}}); err != nil {
			t.Fatal(err)
		}
		decision, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).DecideCatch(context.Background(), workflowruntime.DecideCatchRequest{Source: node.ID, Node: source, At: now.Add(4 * time.Second)})
		if err != nil || decision.Decision.Outcome != workflowruntime.ControlSelected || len(decision.Decision.Targets) != 1 || decision.Decision.Targets[0] != handlerID {
			t.Fatalf("DecideCatch() = %#v, %v", decision, err)
		}
	})
}

func TestUnsafePostSideEffectVerificationFailureCannotSilentlyRetry(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "verification-unsafe-retry")
	kinds := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("mcp", "v1")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectDestructive}
	kind.SpecValue.Idempotency = graph.IdempotencyKeyed
	kind.SpecValue.RetrySafety = stepkind.RetryRequiresIdempotency
	executions := 0
	kind.ExecuteFunc = func(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		if err := prepared.Invocation.Activity.RecordToolCall(context.Background(), verification.ToolCall{Server: "github", Tool: "branches.delete", Outcome: verification.ActivitySucceeded}); err != nil {
			t.Fatal(err)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}, nil
	}
	if registerErr := kinds.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	scheduler := &recordingScheduler{}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) },
		RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: scheduler},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{
		ID: node.ID.NodeID, Kind: "mcp", KindVersion: "v1", Config: graph.Config{},
		Retry:        &graph.RetryPolicy{Attempts: 2, On: []string{"verification_failed"}},
		Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"server": "github", "tool": "branches.delete", "count": 2}}}},
	}})
	if dispatchErr == nil || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Code != "verification_failed" || executions != 1 {
		t.Fatalf("Dispatch() = %#v, %v executions=%d", result, dispatchErr, executions)
	}
	scheduler.mu.Lock()
	scheduled := len(scheduler.scheduled)
	scheduler.mu.Unlock()
	if scheduled != 0 {
		t.Fatalf("unsafe verification failure scheduled %d retries", scheduled)
	}
}

func TestVerificationProviderAndMalformedResultsRemainDistinct(t *testing.T) {
	tests := []struct {
		name     string
		verifier verification.Verifier
		wantCode string
	}{
		{name: "provider", verifier: fixedVerifier{kind: "custom", err: &stepkind.ExecutionError{Code: "reviewer_unavailable", Message: "reviewer unavailable", Classification: stepkind.Retryable}}, wantCode: "verification_provider_failed"},
		{name: "malformed", verifier: fixedVerifier{kind: "custom", result: verification.CheckResult{Outcome: verification.CheckPassed, Code: "", Message: "missing code"}}, wantCode: "verification_result_invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, claim, node, now := dispatchFixture(t, "verification-"+test.name)
			kinds := stepkind.NewRegistry()
			if err := kinds.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
				t.Fatal(err)
			}
			verifiers := verification.NewRegistry()
			if err := verifiers.Register(test.verifier); err != nil {
				t.Fatal(err)
			}
			dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Verifiers: verifiers, Now: func() time.Time { return now.Add(3 * time.Second) }})
			if err != nil {
				t.Fatal(err)
			}
			result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: "custom", Config: graph.Config{}}}}}})
			if dispatchErr == nil || result.Attempt.Failure == nil || result.Attempt.Failure.Code != test.wantCode || result.Verification == nil || result.Verification.Report.Status != verification.ReportFailed {
				t.Fatalf("Dispatch() = %#v, %v", result, dispatchErr)
			}
		})
	}
}

func TestVerifierRequiredEvidenceIsEnforcedBeforeProviderCall(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "verification-required-evidence")
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
		t.Fatal(err)
	}
	calls := 0
	verifiers := verification.NewRegistry()
	if err := verifiers.Register(fixedVerifier{
		kind:     "requires_tool_call",
		required: []verification.ActivityKind{verification.ActivityToolCall},
		calls:    &calls,
		result:   verification.CheckResult{Outcome: verification.CheckPassed, Code: "provider_passed", Message: "provider passed"},
	}); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: kinds, Verifiers: verifiers, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim,
		Node: graph.Node{
			ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{},
			Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: "requires_tool_call", Config: graph.Config{}}}},
		},
	})
	if dispatchErr == nil || result.Attempt.Failure == nil || result.Attempt.Failure.Code != "verification_failed" || result.Verification == nil || calls != 0 {
		t.Fatalf("Dispatch() = %#v, %v calls=%d", result, dispatchErr, calls)
	}
	if len(result.Verification.Report.Checks) != 1 || result.Verification.Report.Checks[0].Code != "verification_evidence_missing" ||
		result.Verification.Report.Checks[0].Details["evidence_kind"] != string(verification.ActivityToolCall) {
		t.Fatalf("durable missing-evidence report = %#v", result.Verification.Report)
	}
}

func TestPreparerCannotObserveSwapOrFreezeRuntimeActivityRecorder(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "verification-prepare")
	kinds := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("fixture", "v1")
	kind.PrepareFunc = func(_ context.Context, invocation stepkind.Invocation) (stepkind.PreparedInvocation, error) {
		if invocation.Activity != nil {
			t.Fatal("Prepare received runtime activity recorder")
		}
		return stepkind.PreparedInvocation{Invocation: invocation}, nil
	}
	kind.ExecuteFunc = func(_ context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		if invocation.Invocation.Activity == nil {
			t.Fatal("Execute did not receive runtime activity recorder")
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}, nil
	}
	if err := kinds.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) }})
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckNoError, Config: graph.Config{}}}}}})
	if err != nil || result.Node.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("Dispatch() = %#v, %v", result, err)
	}

	store, claim, node, now = dispatchFixture(t, "verification-prepare-inject")
	kinds = stepkind.NewRegistry()
	kind = stepkindtest.NewLifecycleKind("fixture", "v1")
	kind.PrepareFunc = func(_ context.Context, invocation stepkind.Invocation) (stepkind.PreparedInvocation, error) {
		invocation.Activity = verification.NewActivityRecorder()
		return stepkind.PreparedInvocation{Invocation: invocation}, nil
	}
	if registerErr := kinds.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	dispatcher, _ = workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) }})
	result, err = dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckNoError, Config: graph.Config{}}}}}})
	if err == nil || result.Attempt.Failure == nil || result.Attempt.Failure.Code != "step_prepare_failed" {
		t.Fatalf("Dispatch(injected recorder) = %#v, %v", result, err)
	}
}

func TestFailedExecutorEvidenceRemainsProcessLocalAndExecutorFailureAuthoritative(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "verification-executor-failed")
	kinds := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("fixture", "v1")
	kind.ExecuteFunc = func(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		if err := prepared.Invocation.Activity.RecordToolCall(context.Background(), verification.ToolCall{Server: "fixture", Tool: "write", Outcome: verification.ActivityFailed}); err != nil {
			t.Fatal(err)
		}
		return stepkind.StepResult{}, &stepkind.ExecutionError{Code: "provider_failed", Message: "provider failed", Classification: stepkind.Retryable}
	}
	if err := kinds.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) }})
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "write", "outcome": string(verification.ActivityFailed)}}}}}})
	if err == nil || result.Attempt.Failure == nil || result.Attempt.Failure.Code != "provider_failed" || result.Verification != nil {
		t.Fatalf("Dispatch() = %#v, %v", result, err)
	}
	events, _ := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: node.ID.RunID})
	for _, event := range events {
		if event.Type == workflowruntime.EventNodeVerificationCompleted {
			t.Fatalf("failed executor unexpectedly created verification report: %#v", event)
		}
	}
}

func TestVerifiedExternalSuspensionRejectsUndurablePreSuspensionEvidence(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "verification-external-boundary")
	kinds := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("external", "v1")
	kind.ExecuteFunc = func(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		if err := prepared.Invocation.Activity.RecordToolCall(context.Background(), verification.ToolCall{Server: "fixture", Tool: "start", Outcome: verification.ActivitySucceeded}); err != nil {
			t.Fatal(err)
		}
		return stepkind.StepResult{Outcome: stepkind.StepExternal, External: &stepkind.ExternalOperationRef{Kind: "job", ID: "job-1"}}, nil
	}
	if err := kinds.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) }})
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "external", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckExpectedToolCall, Config: graph.Config{"tool": "start"}}}}}})
	if err == nil || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Failure == nil || result.Attempt.Failure.Code != "verification_evidence_not_durable" || result.External != nil {
		t.Fatalf("Dispatch() = %#v, %v", result, err)
	}
	recovered, recoverErr := store.RecoverExternalOperations(context.Background(), workflowruntime.ExternalOperationQuery{RunID: node.ID.RunID})
	if recoverErr != nil || len(recovered) != 0 {
		t.Fatalf("undurable activity created external work: %#v, %v", recovered, recoverErr)
	}
}

func TestDispatcherVerifierSnapshotIgnoresLateRegistryRegistration(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "verification-late-register")
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
		t.Fatal(err)
	}
	verifiers := verification.NewRegistry()
	if err := verifiers.Register(fixedVerifier{kind: "initial", result: verification.CheckResult{Outcome: verification.CheckPassed, Code: "passed", Message: "passed"}}); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Verifiers: verifiers, Now: func() time.Time { return now.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifiers.Register(fixedVerifier{kind: "late", result: verification.CheckResult{Outcome: verification.CheckPassed, Code: "passed", Message: "passed"}}); err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: "late", Config: graph.Config{}}}}}})
	if dispatchErr == nil || result.Attempt.ID.Number != 0 || result.Node.Status != workflowruntime.NodeReady {
		t.Fatalf("Dispatch(late verifier) = %#v, %v", result, dispatchErr)
	}
}

func TestVerificationPersistenceExactReplayConflictAndPartialFailure(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "verification-replay")
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
		t.Fatal(err)
	}
	dispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: kinds, Now: func() time.Time { return now.Add(3 * time.Second) }})
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckNoError, Config: graph.Config{}}}}}})
	if err != nil || result.Verification == nil {
		t.Fatalf("Dispatch() = %#v, %v", result, err)
	}
	original := result.Verification.Report
	replay, err := workflowruntime.PersistVerificationForTest(context.Background(), store, nil, nil, result.Attempt.ID, original, now.Add(10*time.Second))
	if err != nil || !replay.Replayed || replay.Ref != result.Verification.Ref || !reflect.DeepEqual(replay.Report, original) {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	replay.Report.Checks[0].Message = "mutated by caller"
	again, err := workflowruntime.PersistVerificationForTest(context.Background(), store, nil, nil, result.Attempt.ID, original, now.Add(11*time.Second))
	if err != nil || again.Report.Checks[0].Message != original.Checks[0].Message {
		t.Fatalf("defensive replay = %#v, %v", again, err)
	}
	conflict := original
	conflict.Checks = append([]verification.CheckResult(nil), original.Checks...)
	conflict.Checks[0].Message = "different durable decision"
	if _, err := workflowruntime.PersistVerificationForTest(context.Background(), store, nil, nil, result.Attempt.ID, conflict, now.Add(12*time.Second)); !errors.Is(err, workflowruntime.ErrVerificationConflict) {
		t.Fatalf("conflicting replay error = %v", err)
	}
	duplicate := result.Verification.Event
	if _, err := store.AppendEvent(context.Background(), workflowruntime.AppendEventRequest{RunID: duplicate.RunID, Invocation: duplicate.Invocation, Attempt: duplicate.Attempt, Type: duplicate.Type, OccurredAt: now.Add(13 * time.Second), Attributes: duplicate.Attributes, Values: duplicate.Values, Redaction: duplicate.Redaction, Retention: duplicate.Retention}); err != nil {
		t.Fatal(err)
	}
	if _, err := workflowruntime.PersistVerificationForTest(context.Background(), store, nil, nil, result.Attempt.ID, original, now.Add(14*time.Second)); !errors.Is(err, workflowruntime.ErrVerificationConflict) {
		t.Fatalf("duplicate event replay error = %v", err)
	}

	underlying, partialClaim, partialNode, partialNow := dispatchFixture(t, "verification-partial")
	partial := &verificationEventFailureStore{StateStore: underlying}
	partialDispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: partial, Registry: kinds, Now: func() time.Time { return partialNow.Add(3 * time.Second) }})
	partialResult, partialErr := partialDispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: partialClaim, Node: graph.Node{ID: partialNode.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{Kind: verification.CheckNoError, Config: graph.Config{}}}}}})
	if partialErr == nil || partialResult.Attempt.Failure == nil || partialResult.Attempt.Failure.Code != "verification_persistence_failed" || partial.saved == nil {
		t.Fatalf("partial Dispatch() = %#v, %v, saved=%#v", partialResult, partialErr, partial.saved)
	}
	if _, err := underlying.LoadValues(context.Background(), *partial.saved); err != nil {
		t.Fatalf("orphan verification value set is not discoverable: %v", err)
	}
	partialEvents, _ := underlying.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: partialNode.ID.RunID})
	for _, event := range partialEvents {
		if event.Type == workflowruntime.EventNodeVerificationCompleted {
			t.Fatalf("partial append created verification event: %#v", event)
		}
	}
}

type fixedVerifier struct {
	kind     string
	required []verification.ActivityKind
	calls    *int
	result   verification.CheckResult
	err      error
}

func (v fixedVerifier) Spec() verification.VerifierSpec {
	return verification.VerifierSpec{Kind: v.kind, Version: "v1", Mode: verification.ModeReviewer, ConfigSchema: graph.Schema{}, RequiredEvidence: append([]verification.ActivityKind(nil), v.required...)}
}
func (fixedVerifier) ValidateConfig(context.Context, graph.VerificationCheck) []diagnostic.Diagnostic {
	return nil
}
func (v fixedVerifier) Verify(context.Context, verification.Request) (verification.CheckResult, error) {
	if v.calls != nil {
		(*v.calls)++
	}
	return v.result, v.err
}

type verificationEventFailureStore struct {
	workflowruntime.StateStore
	saved *values.ValueSetRef
}

func (s *verificationEventFailureStore) SaveValues(ctx context.Context, request workflowruntime.SaveValuesRequest) (values.ValueSetRef, error) {
	ref, err := s.StateStore.SaveValues(ctx, request)
	if err == nil && request.Owner.Kind == "node-attempt-verification" {
		copyRef := ref
		s.saved = &copyRef
	}
	return ref, err
}

func (s *verificationEventFailureStore) AppendEvent(ctx context.Context, request workflowruntime.AppendEventRequest) (workflowruntime.Event, error) {
	if request.Type == workflowruntime.EventNodeVerificationCompleted {
		return workflowruntime.Event{}, errors.New("verification event unavailable")
	}
	return s.StateStore.AppendEvent(ctx, request)
}
