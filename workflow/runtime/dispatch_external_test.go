package runtime_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestStepDispatcherExecutesRegisteredSnapshotAndPersistsTypedOutputs(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-success")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("fixture", "v1")
	kind.SpecValue.ConfigSchema = objectSchema("enabled", "boolean")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	kind.ValidateConfigFunc = func(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
		config["enabled"] = false
		return nil
	}
	kind.ExecuteFunc = func(_ context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		if invocation.Invocation.Identity.RunID != "dispatch-success" || invocation.Invocation.Identity.Attempt != 1 ||
			invocation.Invocation.Config["enabled"] != true || invocation.Invocation.Inputs["input"].Inline != "hello" {
			t.Fatalf("invocation = %#v", invocation.Invocation)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "complete")}}, nil
	}
	if registerErr := registry.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	// A mutable adapter cannot rewrite the registered schema snapshot.
	kind.SpecValue.OutputSchema = graph.Schema{"not": graph.Schema{}}

	var retainedBefore, retainedAfter bool
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
		RetentionHook: workflowruntime.RetentionHookFuncs{
			Before: func(_ context.Context, plan workflowruntime.RetentionPlan) error {
				retainedBefore = len(plan.Groups) == 1 && plan.Groups[0].Class == values.RetentionRun
				return nil
			},
			After: func(_ context.Context, record workflowruntime.RetentionRecord) error {
				retainedAfter = record.Ref.ID != ""
				return nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim:          claim,
		Node:           graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{"enabled": true}},
		IdempotencyKey: "invoke-1", Target: "embedded",
		ExecutorAttributes: map[string]string{"pool": "test"},
	})
	if err != nil {
		t.Fatalf("Dispatch() = %#v, %v", result, err)
	}
	if result.Node.Status != workflowruntime.NodeSucceeded || result.Attempt.Status != workflowruntime.NodeSucceeded ||
		result.Outputs == nil || result.Result == nil || result.Attempt.Executor.Kind != "fixture" ||
		result.Attempt.Executor.Version != "v1" || result.Attempt.Executor.Target != "embedded" ||
		!retainedBefore || !retainedAfter {
		t.Fatalf("Dispatch() result = %#v", result)
	}
	persisted, err := store.LoadValues(context.Background(), *result.Outputs)
	if err != nil || persisted["result"].Inline != "complete" {
		t.Fatalf("LoadValues(outputs) = %#v, %v", persisted, err)
	}
}

func TestStepDispatcherFinishesAfterLeaseOnlyRenewalDuringExecution(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-renewed-lease")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("fixture", "v1")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	executing := make(chan struct{})
	resume := make(chan struct{})
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		close(executing)
		<-resume
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "renewed")}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	type dispatchOutcome struct {
		result workflowruntime.DispatchResult
		err    error
	}
	done := make(chan dispatchOutcome, 1)
	go func() {
		result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
			Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
		})
		done <- dispatchOutcome{result: result, err: dispatchErr}
	}()
	<-executing
	running, err := store.LoadNodeInvocation(context.Background(), node.ID)
	if err != nil || running.Status != workflowruntime.NodeRunning || running.Lease == nil {
		t.Fatalf("running node = %#v, %v", running, err)
	}
	renewed, err := store.RenewNodeLease(context.Background(), workflowruntime.RenewLeaseRequest{
		InvocationID: node.ID, Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation,
		Now: now.Add(30 * time.Minute), LeaseUntil: now.Add(2 * time.Hour),
	})
	if err != nil || !renewed.ExpiresAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("RenewNodeLease = %#v, %v", renewed, err)
	}
	afterRenew, err := store.LoadNodeInvocation(context.Background(), node.ID)
	if err != nil || afterRenew.Generation != running.Generation || !afterRenew.UpdatedAt.Equal(running.UpdatedAt) {
		t.Fatalf("renewal changed semantic node revision: before=%#v after=%#v err=%v", running, afterRenew, err)
	}
	close(resume)
	outcome := <-done
	if outcome.err != nil || outcome.result.Node.Status != workflowruntime.NodeSucceeded || outcome.result.Attempt.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("Dispatch after renewal = %#v, %v", outcome.result, outcome.err)
	}
}

func TestStepDispatcherValidatesConfigAndInputsBeforeStartingAttempt(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-validation")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("fixture", "v1")
	kind.SpecValue.ConfigSchema = objectSchema("enabled", "boolean")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim,
		Node:  graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{"enabled": "yes"}},
	})
	if !errors.Is(err, workflowruntime.ErrStepValidation) {
		t.Fatalf("Dispatch(invalid config) error = %v", err)
	}
	attempts, listErr := store.ListAttempts(context.Background(), node.ID)
	loaded, loadErr := store.LoadNodeInvocation(context.Background(), node.ID)
	if listErr != nil || loadErr != nil || len(attempts) != 0 || loaded.Status != workflowruntime.NodeReady ||
		loaded.Lease != nil || result.Node.Lease != nil {
		t.Fatalf("pre-start validation stranded claim: result=%#v attempts=%#v node=%#v errors=%v/%v", result, attempts, loaded, listErr, loadErr)
	}
}

func TestStepDispatcherReleasesClaimOnUnknownKindBeforeStartingAttempt(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-unknown-kind")
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: stepkind.NewRegistry(), Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "missing", KindVersion: "v1", Config: graph.Config{}},
	})
	var typed *workflowruntime.DispatchError
	if !errors.As(dispatchErr, &typed) || typed.Stage != workflowruntime.DispatchResolve ||
		!errors.Is(dispatchErr, stepkind.ErrUnknownStepKind) {
		t.Fatalf("unknown kind error = %T %v", dispatchErr, dispatchErr)
	}
	loaded, loadErr := store.LoadNodeInvocation(context.Background(), node.ID)
	attempts, listErr := store.ListAttempts(context.Background(), node.ID)
	if loadErr != nil || listErr != nil || loaded.Status != workflowruntime.NodeReady || loaded.Lease != nil ||
		result.Node.Lease != nil || len(attempts) != 0 {
		t.Fatalf("unknown kind stranded claim: result=%#v node=%#v attempts=%#v errors=%v/%v", result, loaded, attempts, loadErr, listErr)
	}
}

func TestStepDispatcherRequiresPinnedKindVersionBeforeStartingAttempt(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-unpinned")
	registry := stepkind.NewRegistry()
	if err := registry.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", Config: graph.Config{}},
	})
	if !errors.Is(err, workflowruntime.ErrInvalidDispatch) {
		t.Fatalf("Dispatch(unpinned kind) error = %v", err)
	}
	attempts, listErr := store.ListAttempts(context.Background(), node.ID)
	if listErr != nil || len(attempts) != 0 {
		t.Fatalf("unpinned dispatch started attempts: %#v, %v", attempts, listErr)
	}
}

func TestStepDispatcherClosesEveryStartedFailureAndDefaultsTerminal(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*stepkindtest.Kind, context.CancelFunc)
		wantStage  workflowruntime.DispatchStage
		wantStatus workflowruntime.NodeStatus
		wantCode   string
	}{
		{
			name: "execute retryable",
			configure: func(kind *stepkindtest.Kind, _ context.CancelFunc) {
				kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
					return stepkind.StepResult{}, &stepkind.ExecutionError{
						Code: "temporary", Message: "provider unavailable", Classification: stepkind.Retryable,
					}
				}
			},
			wantStage: workflowruntime.DispatchExecute, wantStatus: workflowruntime.NodeFailed, wantCode: "temporary",
		},
		{
			name: "invalid typed output",
			configure: func(kind *stepkindtest.Kind, _ context.CancelFunc) {
				kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
					return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", 42)}}, nil
				}
			},
			wantStage: workflowruntime.DispatchValidateOutput, wantStatus: workflowruntime.NodeFailed, wantCode: "step_result_invalid",
		},
		{
			name: "non-persistable output",
			configure: func(kind *stepkindtest.Kind, _ context.CancelFunc) {
				kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
					ephemeral, err := values.NewInline("process-only", values.Metadata{
						Producer:  values.Producer{Kind: "fixture", Reference: "node", Output: "result"},
						MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionNone,
					})
					if err != nil {
						t.Fatal(err)
					}
					return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": ephemeral}}, nil
				}
			},
			wantStage: workflowruntime.DispatchValidateOutput, wantStatus: workflowruntime.NodeFailed, wantCode: "step_result_invalid",
		},
		{
			name: "canceled execution",
			configure: func(kind *stepkindtest.Kind, cancel context.CancelFunc) {
				kind.ExecuteFunc = func(ctx context.Context, _ stepkind.PreparedInvocation) (stepkind.StepResult, error) {
					cancel()
					return stepkind.StepResult{}, ctx.Err()
				}
			},
			wantStage: workflowruntime.DispatchExecute, wantStatus: workflowruntime.NodeCanceled, wantCode: "step_execute_failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, claim, node, now := dispatchFixture(t, "failure-"+safeTestID(test.name))
			registry := stepkind.NewRegistry()
			kind := stepkindtest.NewNoopKind("fixture", "v1")
			kind.SpecValue.InputSchema = objectSchema("input", "string")
			kind.SpecValue.OutputSchema = objectSchema("result", "string")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			test.configure(kind, cancel)
			if err := registry.Register(kind); err != nil {
				t.Fatal(err)
			}
			dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
				Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
			})
			if err != nil {
				t.Fatal(err)
			}
			result, dispatchErr := dispatcher.Dispatch(ctx, workflowruntime.DispatchRequest{
				Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
			})
			var typed *workflowruntime.DispatchError
			if !errors.As(dispatchErr, &typed) || typed.Stage != test.wantStage {
				t.Fatalf("Dispatch() error = %T %v", dispatchErr, dispatchErr)
			}
			if !errors.Is(dispatchErr, workflowruntime.ErrStepExecution) {
				t.Fatalf("Dispatch() error does not unwrap ErrStepExecution: %v", dispatchErr)
			}
			if test.wantStage == workflowruntime.DispatchValidateOutput && !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) {
				t.Fatalf("output validation error does not unwrap ErrStepValidation: %v", dispatchErr)
			}
			if result.Node.Status != test.wantStatus || result.Node.Origin != workflowruntime.OriginExecuted || result.Attempt.Status != test.wantStatus ||
				result.Attempt.Failure == nil || result.Attempt.Failure.Code != test.wantCode || result.Node.Lease != nil {
				t.Fatalf("closed failure = %#v", result)
			}
			if test.name == "execute retryable" && !result.Attempt.Failure.Retryable {
				t.Fatal("retry classification was not persisted")
			}
		})
	}
}

func TestStepDispatcherInjectedDispositionCanReturnFailedAttemptToReady(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-retry")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{}, &stepkind.ExecutionError{Code: "retry", Message: "again", Classification: stepkind.Retryable}
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	called := false
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
		FailureDisposition: workflowruntime.FailureDispositionFunc(func(_ context.Context, request workflowruntime.FailureDispositionRequest) (workflowruntime.NodeStatus, error) {
			called = true
			if request.Status != workflowruntime.NodeFailed || !request.Failure.Retryable {
				t.Fatalf("disposition request = %#v", request)
			}
			return workflowruntime.NodeReady, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
	})
	if dispatchErr == nil || !called || result.Node.Status != workflowruntime.NodeReady || result.Node.Origin != "" || result.Attempt.Status != workflowruntime.NodeFailed {
		t.Fatalf("retry disposition = %#v, %v, called=%v", result, dispatchErr, called)
	}
}

func TestStepDispatcherValidatesAndPersistsArtifactAndSecretRefOutputs(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-reference-outputs")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewNoopKind("fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = graph.Schema{
		"type": "object", "required": []any{"report", "credential"},
		"properties": map[string]any{
			"report":     map[string]any{"$ref": "#/$defs/report"},
			"credential": map[string]any{"anyOf": []any{map[string]any{"type": "secret_ref"}, map[string]any{"type": "null"}}},
		},
		"additionalProperties": false,
		"$defs": map[string]any{"report": map[string]any{"allOf": []any{
			map[string]any{"type": "artifact"},
			map[string]any{"type": "object", "properties": map[string]any{"media_type": map[string]any{"const": "application/json"}}},
		}}},
	}
	artifact, err := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://reports/dispatch/result.json", Digest: values.SHA256Digest([]byte("result")),
		MediaType: "application/json", SizeBytes: 6,
		Producer:  values.Producer{Kind: "fixture", Reference: "node", Output: "report"},
		Redaction: values.RedactionPrivate, Retention: values.RetentionExternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	secretRef, _ := values.ParseSecretRef("secret://project/service#token")
	secret, err := values.NewSecretRef(secretRef, values.Metadata{
		Producer:  values.Producer{Kind: "fixture", Reference: "node", Output: "credential"},
		MediaType: "text/plain", Redaction: values.RedactionSecret, Retention: values.RetentionProject,
	})
	if err != nil {
		t.Fatal(err)
	}
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"report": artifact, "credential": secret}}, nil
	}
	if registerErr := registry.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
	})
	if dispatchErr != nil || result.Outputs == nil {
		t.Fatalf("Dispatch(reference outputs) = %#v, %v", result, dispatchErr)
	}
	persisted, err := store.LoadValues(context.Background(), *result.Outputs)
	if err != nil || persisted["report"].Type != values.TypeArtifact || persisted["credential"].Type != values.TypeSecretRef {
		t.Fatalf("persisted references = %#v, %v", persisted, err)
	}
}

func TestStepDispatcherMasksKnownSecretsBeforeFailureAndWarningPersistence(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-redaction")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	rawSecret := "token-123"
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{}, &stepkind.ExecutionError{
			Code: "provider-rejected", Message: "Bearer " + rawSecret + " rejected", Classification: stepkind.RetryPermanent,
			Details: map[string]string{"credential": rawSecret},
		}
	}
	kind.FinalizeFunc = func(context.Context, stepkind.Finalization) error {
		return errors.New("cleanup exposed " + rawSecret)
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	secretRef, _ := values.ParseSecretRef("secret://project/service#token")
	resolved, _ := values.NewResolvedSecret(secretRef, []byte(rawSecret))
	redactor, err := values.NewRedactor(resolved)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Redactor: redactor,
		Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
	})
	var rawExecution *stepkind.ExecutionError
	if !errors.As(dispatchErr, &rawExecution) || !strings.Contains(rawExecution.Message, rawSecret) {
		t.Fatalf("process-local cause lost raw detail: %T %v", dispatchErr, dispatchErr)
	}
	if result.Attempt.Failure == nil || strings.Contains(result.Attempt.Failure.Message, rawSecret) ||
		strings.Contains(result.Attempt.Failure.Details["credential"], rawSecret) ||
		len(result.Warnings) != 1 || strings.Contains(result.Warnings[0].Failure.Message, rawSecret) ||
		!strings.Contains(result.Warnings[0].Cause.Error(), rawSecret) {
		t.Fatalf("redaction boundary = %#v", result)
	}
	events, err := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: node.ID.RunID})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		for key, value := range event.Attributes {
			if strings.Contains(value, rawSecret) {
				t.Fatalf("persisted event %s[%s] leaked secret: %#v", event.Type, key, event)
			}
		}
	}
}

func TestStepDispatcherPersistsGenericMessagesForUnstructuredErrorsWithoutRedactor(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-generic-failure")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	rawSecret := "unregistered-secret"
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{}, errors.New("provider response included " + rawSecret)
	}
	kind.FinalizeFunc = func(context.Context, stepkind.Finalization) error {
		return errors.New("cleanup response included " + rawSecret)
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
	})
	if !strings.Contains(dispatchErr.Error(), rawSecret) || result.Attempt.Failure == nil ||
		result.Attempt.Failure.Message != "step execution failed" || strings.Contains(result.Attempt.Failure.Message, rawSecret) ||
		len(result.Warnings) != 1 || result.Warnings[0].Failure.Message != "step finalization failed" ||
		!strings.Contains(result.Warnings[0].Cause.Error(), rawSecret) {
		t.Fatalf("generic persistence boundary = %#v, %v", result, dispatchErr)
	}
	events, err := store.ListEvents(context.Background(), workflowruntime.EventQuery{RunID: node.ID.RunID})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		for key, value := range event.Attributes {
			if strings.Contains(value, rawSecret) {
				t.Fatalf("persisted event %s[%s] leaked unstructured error: %#v", event.Type, key, event)
			}
		}
	}
}

func TestStepDispatcherPrepareFailureClosesStartedAttempt(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-prepare")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.PrepareFunc = func(context.Context, stepkind.Invocation) (stepkind.PreparedInvocation, error) {
		return stepkind.PreparedInvocation{}, &stepkind.ExecutionError{
			Code: "prepare-unavailable", Message: "resource allocation failed", Classification: stepkind.Retryable,
		}
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
	})
	var typed *workflowruntime.DispatchError
	if !errors.As(dispatchErr, &typed) || typed.Stage != workflowruntime.DispatchPrepare ||
		result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Status != workflowruntime.NodeFailed ||
		result.Attempt.Failure == nil || result.Attempt.Failure.Code != "prepare-unavailable" {
		t.Fatalf("prepare failure = %#v, %v", result, dispatchErr)
	}
}

func TestStepDispatcherFinalizerReceivesProducedResultOnOutputFailure(t *testing.T) {
	tests := []struct {
		name      string
		payload   any
		wrapStore func(workflowruntime.StateStore) workflowruntime.StateStore
		stage     workflowruntime.DispatchStage
	}{
		{name: "validation", payload: 42, wrapStore: func(store workflowruntime.StateStore) workflowruntime.StateStore { return store }, stage: workflowruntime.DispatchValidateOutput},
		{name: "persistence", payload: "complete", wrapStore: func(store workflowruntime.StateStore) workflowruntime.StateStore {
			return &outputFailureStore{StateStore: store}
		}, stage: workflowruntime.DispatchPersistOutput},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, claim, node, now := dispatchFixture(t, "dispatch-finalize-"+test.name)
			registry := stepkind.NewRegistry()
			kind := stepkindtest.NewLifecycleKind("fixture", "v1")
			kind.SpecValue.InputSchema = objectSchema("input", "string")
			kind.SpecValue.OutputSchema = objectSchema("result", "string")
			produced := values.ValueSet{"result": dispatchValue(t, "result", test.payload)}
			kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
				return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: produced}, nil
			}
			var finalized stepkind.StepResult
			kind.FinalizeFunc = func(_ context.Context, finalization stepkind.Finalization) error {
				finalized = finalization.Result
				return nil
			}
			if err := registry.Register(kind); err != nil {
				t.Fatal(err)
			}
			dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
				Store: test.wrapStore(store), Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
			})
			if err != nil {
				t.Fatal(err)
			}
			result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
				Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
			})
			var typed *workflowruntime.DispatchError
			if !errors.As(dispatchErr, &typed) || typed.Stage != test.stage || result.Attempt.Status != workflowruntime.NodeFailed {
				t.Fatalf("Dispatch() = %#v, %v", result, dispatchErr)
			}
			if finalized.Outputs["result"].Digest != produced["result"].Digest {
				t.Fatalf("Finalize() result = %#v, want produced payload %#v", finalized, test.payload)
			}
		})
	}
}

func TestStepDispatcherRejectsPreparedInvocationIdentityOrPayloadMutation(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-prepare-mutation")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.PrepareFunc = func(_ context.Context, invocation stepkind.Invocation) (stepkind.PreparedInvocation, error) {
		invocation.Identity.Attempt = 2
		invocation.Config["injected"] = true
		return stepkind.PreparedInvocation{Invocation: invocation}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
	})
	var typed *workflowruntime.DispatchError
	if !errors.As(dispatchErr, &typed) || typed.Stage != workflowruntime.DispatchPrepare ||
		result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Status != workflowruntime.NodeFailed {
		t.Fatalf("prepared mutation = %#v, %v", result, dispatchErr)
	}
}

func TestStepDispatcherFinalizeFailureIsNonReversingStructuredWarning(t *testing.T) {
	store, claim, node, now := dispatchFixture(t, "dispatch-finalize")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("fixture", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.FinalizeFunc = func(context.Context, stepkind.Finalization) error { return errors.New("cleanup unavailable") }
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now.Add(3 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: claim, Node: graph.Node{ID: node.ID.NodeID, Kind: "fixture", KindVersion: "v1", Config: graph.Config{}},
	})
	if dispatchErr != nil || result.Node.Status != workflowruntime.NodeSucceeded || len(result.Warnings) != 1 ||
		result.Warnings[0].Event == nil || result.Warnings[0].Event.Type != workflowruntime.EventNodeFinalizeWarning {
		t.Fatalf("finalize warning = %#v, %v", result, dispatchErr)
	}
	loaded, err := store.LoadNodeInvocation(context.Background(), node.ID)
	if err != nil || loaded.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("finalize warning reversed terminal state: %#v, %v", loaded, err)
	}
}

func dispatchFixture(t *testing.T, run string) (*runtimetest.Store, workflowruntime.ReadyClaim, workflowruntime.NodeInvocationSnapshot, time.Time) {
	t.Helper()
	store := runtimetest.NewStore()
	now := time.Date(2026, time.August, 24, 14, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID(run)
	createRun(t, store, runID, now)
	inputRef, err := store.SaveValues(context.Background(), workflowruntime.SaveValuesRequest{
		Owner:  workflowruntime.ValueOwner{Kind: "node-inputs", RunID: runID},
		Values: values.ValueSet{"input": dispatchValue(t, "input", "hello")},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "node"}
	node, err := store.CreateNodeInvocation(context.Background(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodePending, Inputs: &inputRef, CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := store.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{
		InvocationID: id, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	claim, ok, err := queue.ClaimNext(context.Background(), workflowruntime.ReadyClaimRequest{
		Owner: "worker", Token: "token", IdempotencyKey: "claim-" + run,
		Now: now.Add(2 * time.Second), LeaseUntil: now.Add(time.Hour),
	})
	if err != nil || !ok {
		t.Fatalf("ClaimNext() = %#v, %v, %v", claim, ok, err)
	}
	claimed, err := store.LoadNodeInvocation(context.Background(), id)
	if err != nil || claimed.Generation <= ready.Snapshot.Generation {
		t.Fatalf("LoadNodeInvocation(claimed) = %#v, %v", claimed, err)
	}
	return store, claim, claimed, now
}

func objectSchema(name, valueType string) graph.Schema {
	return graph.Schema{
		"type": "object", "required": []any{name},
		"properties":           map[string]any{name: map[string]any{"type": valueType}},
		"additionalProperties": false,
	}
}

func dispatchValue(t *testing.T, output string, payload any) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{
		Producer:  values.Producer{Kind: "fixture", Reference: "node", Output: output},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func safeTestID(name string) string {
	result := make([]byte, 0, len(name))
	for i := range len(name) {
		if name[i] >= 'a' && name[i] <= 'z' {
			result = append(result, name[i])
		} else {
			result = append(result, '-')
		}
	}
	return string(result)
}

type outputFailureStore struct{ workflowruntime.StateStore }

func (s *outputFailureStore) SaveValues(ctx context.Context, request workflowruntime.SaveValuesRequest) (values.ValueSetRef, error) {
	if request.Owner.Kind == "node-attempt-outputs" {
		return values.ValueSetRef{}, errors.New("output store unavailable")
	}
	return s.StateStore.SaveValues(ctx, request)
}
