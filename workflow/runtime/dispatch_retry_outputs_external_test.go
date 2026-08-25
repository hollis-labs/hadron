package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestDispatcherSchedulesDurableRetryThenPreservesFinalTypedOutput(t *testing.T) {
	store, firstClaim, node, base := dispatchFixture(t, "dispatch-durable-retry")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("retry-kind", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = objectSchema("result", "string")
	kind.SpecValue.Effects = graph.EffectSet{graph.EffectRead}
	kind.SpecValue.Idempotency = graph.IdempotencyIntrinsic
	kind.SpecValue.RetrySafety = stepkind.RetrySafe
	executions := 0
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		executions++
		if executions == 1 {
			return stepkind.StepResult{}, &stepkind.ExecutionError{Code: "rate_limited", Message: "retry later", Classification: stepkind.Retryable}
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": dispatchValue(t, "result", "done")}}, nil
	}
	if registerErr := registry.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	now := base.Add(3 * time.Second)
	scheduler := &recordingScheduler{}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return now },
		RetryCoordinator: &workflowruntime.RetryCoordinator{Scheduler: scheduler},
	})
	if err != nil {
		t.Fatal(err)
	}
	graphNode := graph.Node{ID: node.ID.NodeID, Kind: "retry-kind", KindVersion: "v1", Retry: &graph.RetryPolicy{Attempts: 2, On: []string{"rate_limited"}, Backoff: graph.BackoffPolicy{Strategy: graph.BackoffFixed, InitialDelay: "10s"}}}
	first, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: firstClaim, Node: graphNode})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepExecution) || first.Node.Status != workflowruntime.NodeWaiting || first.Node.Lease != nil || first.Attempt.Status != workflowruntime.NodeFailed || executions != 1 {
		t.Fatalf("first Dispatch = %#v, %v executions=%d", first, dispatchErr, executions)
	}
	scheduler.mu.Lock()
	if len(scheduler.scheduled) != 1 || scheduler.scheduled[0].Kind != "node_retry" || !scheduler.scheduled[0].FireAt.Equal(now.Add(10*time.Second)) {
		t.Fatalf("scheduled activations = %#v", scheduler.scheduled)
	}
	activationID := string(scheduler.scheduled[0].ID)
	scheduler.mu.Unlock()
	activation, err := store.LoadRetryActivation(context.Background(), activationID)
	if err != nil {
		t.Fatal(err)
	}
	now = activation.FireAt
	activated, err := store.ActivateNodeRetry(context.Background(), workflowruntime.ActivateNodeRetryRequest{ActivationID: activation.ID, ExpectedActivationGeneration: activation.Generation, ExpectedNodeGeneration: first.Node.Generation, IdempotencyKey: "activate-dispatch-retry", Now: now})
	if err != nil || activated.Node.Status != workflowruntime.NodeReady {
		t.Fatalf("ActivateNodeRetry = %#v, %v", activated, err)
	}
	queue := workflowruntime.NewReadyQueueCoordinator(store, nil)
	now = now.Add(time.Second)
	secondClaim, ok, err := queue.ClaimNext(context.Background(), workflowruntime.ReadyClaimRequest{Owner: "worker-2", Token: "token-2", IdempotencyKey: "claim-dispatch-retry-2", Now: now, LeaseUntil: now.Add(time.Minute)})
	if err != nil || !ok {
		t.Fatalf("ClaimNext(retry) = %#v, %t, %v", secondClaim, ok, err)
	}
	now = now.Add(time.Second)
	second, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: secondClaim, Node: graphNode})
	if err != nil || second.Node.Status != workflowruntime.NodeSucceeded || second.Attempt.ID.Number != 2 || second.Outputs == nil || executions != 2 {
		t.Fatalf("second Dispatch = %#v, %v executions=%d", second, err, executions)
	}
	loaded, err := store.LoadValues(context.Background(), *second.Outputs)
	attempts, historyErr := store.ListAttempts(context.Background(), node.ID)
	if err != nil || historyErr != nil || loaded["result"].Inline != "done" || len(attempts) != 2 || attempts[0].Status != workflowruntime.NodeFailed || attempts[1].Status != workflowruntime.NodeSucceeded {
		t.Fatalf("typed output/history loaded=%#v attempts=%#v errors=%v/%v", loaded, attempts, err, historyErr)
	}
}

func TestDispatcherRetryDenialCannotBeBypassedByReadyDisposition(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-retry-denied")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("retry-denied-kind", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.RetrySafety = stepkind.RetrySafe
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{}, &stepkind.ExecutionError{Code: "temporary", Message: "retry later", Classification: stepkind.Retryable}
	}
	if registerErr := registry.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	dispositionCalled := false
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) },
		FailureDisposition: workflowruntime.FailureDispositionFunc(func(context.Context, workflowruntime.FailureDispositionRequest) (workflowruntime.NodeStatus, error) {
			dispositionCalled = true
			return workflowruntime.NodeReady, nil
		}),
		RetryCoordinator: &workflowruntime.RetryCoordinator{},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{
		ID: node.ID.NodeID, Kind: "retry-denied-kind", KindVersion: "v1",
		Retry: &graph.RetryPolicy{Attempts: 1, On: []string{"temporary"}},
	}})
	if !errors.Is(dispatchErr, workflowruntime.ErrStepExecution) || result.Node.Status != workflowruntime.NodeFailed || result.Attempt.Status != workflowruntime.NodeFailed || dispositionCalled {
		t.Fatalf("Dispatch(exhausted retry) = %#v, %v dispositionCalled=%t", result, dispatchErr, dispositionCalled)
	}
	recovered, recoverErr := store.RecoverRetryActivations(context.Background(), workflowruntime.RetryActivationQuery{RunID: node.ID.RunID})
	if recoverErr != nil || len(recovered) != 0 {
		t.Fatalf("denied retry persisted activation: %#v, %v", recovered, recoverErr)
	}
}

func TestDispatcherValidatesDeclaredNodeOutputsWithSource(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-declared-output")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("declared-output-kind", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = graph.Schema{}
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"other": dispatchValue(t, "other", "value")}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	source := &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "workflow.yaml", StartLine: 17, EndLine: 17}
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{
		ID: node.ID.NodeID, Kind: "declared-output-kind", KindVersion: "v1",
		Outputs: []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}, Source: source}},
	}})
	var outputErr *workflowruntime.NodeOutputValidationError
	if !errors.Is(err, workflowruntime.ErrStepValidation) || !errors.As(err, &outputErr) || outputErr.Output != "result" || outputErr.Source == nil || outputErr.Source.Locator != source.Locator || result.Attempt.Status != workflowruntime.NodeFailed {
		t.Fatalf("Dispatch(declared output) = %#v, %v outputErr=%#v", result, err, outputErr)
	}
	source.Locator = "mutated.yaml"
	if outputErr.Source.Locator != "workflow.yaml" {
		t.Fatalf("output diagnostic source aliased caller: %#v", outputErr.Source)
	}
}

func TestDispatcherProjectsDeclaredNodeOutputsAndExcludesUnselectedRawValues(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-projected-output")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("projected-output-kind", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = graph.Schema{}
	artifact := values.ArtifactRef{
		Store: "fixture", URI: "artifact://fixture/receipt", Digest: values.SHA256Digest([]byte("receipt")), MediaType: "application/json", SizeBytes: 7,
		Producer: values.Producer{Kind: "adapter", Reference: "raw", Output: "receipt"}, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	}
	artifactValue, err := values.NewArtifact(artifact)
	if err != nil {
		t.Fatal(err)
	}
	structured := dispatchValue(t, "structured", map[string]any{"id": "task-1"})
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{
			"structured": structured,
			"receipt":    artifactValue,
			"metadata":   dispatchValue(t, "metadata", map[string]any{"private": "not-public"}),
		}}, nil
	}
	if registerErr := registry.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{
		ID: node.ID.NodeID, Kind: "projected-output-kind", KindVersion: "v1",
		Outputs: []graph.OutputSpec{
			{Name: "result-json", Schema: graph.Schema{"type": "object"}, Value: &graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "outputs.structured"}}},
			{Name: "task-id", Schema: graph.Schema{"type": "string"}, Value: &graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "outputs.structured.id"}}},
			{Name: "artifact", Schema: graph.Schema{}, Value: &graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "outputs.receipt"}}},
		},
	}})
	if err != nil || result.Attempt.Status != workflowruntime.NodeSucceeded || result.Outputs == nil || result.Result == nil {
		t.Fatalf("Dispatch(projected output) = %#v, %v", result, err)
	}
	public, loadErr := store.LoadValues(context.Background(), *result.Outputs)
	if loadErr != nil || len(public) != 3 || public["task-id"].Inline != "task-1" {
		t.Fatalf("public outputs = %#v, %v", public, loadErr)
	}
	if _, exists := public["metadata"]; exists {
		t.Fatalf("unselected raw metadata was published: %#v", public)
	}
	if public["result-json"].Digest != structured.Digest || public["result-json"].Producer != structured.Producer {
		t.Fatalf("exact raw output passthrough lost envelope: %#v", public["result-json"])
	}
	if public["artifact"].Type != values.TypeArtifact || public["artifact"].Artifact.URI != artifact.URI || public["artifact"].Digest != artifactValue.Digest {
		t.Fatalf("artifact passthrough = %#v", public["artifact"])
	}
	if public["task-id"].Producer.Kind != "node_output" || public["task-id"].Producer.Output != "task-id" {
		t.Fatalf("computed output metadata = %#v", public["task-id"].Producer)
	}
	if len(result.Result.Outputs) != 3 || result.Result.Outputs["metadata"].Inline == nil {
		t.Fatalf("adapter result was rewritten instead of preserving raw outputs: %#v", result.Result)
	}
}

func TestDispatcherProjectionSchemaAndSecretFailuresPreserveOutputSource(t *testing.T) {
	for _, test := range []struct {
		name       string
		raw        func(*testing.T) values.ValueSet
		binding    *graph.Binding
		schema     graph.Schema
		wantSecret bool
	}{
		{
			name: "schema mismatch", schema: graph.Schema{"type": "string"},
			raw: func(t *testing.T) values.ValueSet {
				return values.ValueSet{"public": dispatchValue(t, "public", json.Number("7"))}
			},
		},
		{
			name: "secret derivation", schema: graph.Schema{"type": "string"}, wantSecret: true,
			binding: &graph.Binding{Kind: graph.BindingInterpolation, Interpolation: "prefix-{{ outputs.secret }}"},
			raw: func(t *testing.T) values.ValueSet {
				ref, err := values.ParseSecretRef("secret://project/output#token")
				if err != nil {
					t.Fatal(err)
				}
				secret, err := values.NewSecretRef(ref, values.Metadata{Producer: values.Producer{Kind: "adapter", Reference: "raw", Output: "secret"}, MediaType: "text/plain", Redaction: values.RedactionSecret, Retention: values.RetentionRun})
				if err != nil {
					t.Fatal(err)
				}
				return values.ValueSet{"secret": secret}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, claim, node, base := dispatchFixture(t, "dispatch-projection-"+test.name)
			registry := stepkind.NewRegistry()
			kind := stepkindtest.NewLifecycleKind("projection-failure-kind", "v1")
			kind.SpecValue.InputSchema = objectSchema("input", "string")
			kind.SpecValue.OutputSchema = graph.Schema{}
			kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
				return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: test.raw(t)}, nil
			}
			if err := registry.Register(kind); err != nil {
				t.Fatal(err)
			}
			dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
			if err != nil {
				t.Fatal(err)
			}
			source := &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "projection.workflow.yaml", StartLine: 27, EndLine: 27}
			result, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{
				ID: node.ID.NodeID, Kind: "projection-failure-kind", KindVersion: "v1",
				Outputs: []graph.OutputSpec{{Name: "public", Schema: test.schema, Value: test.binding, Source: source}},
			}})
			var outputErr *workflowruntime.NodeOutputValidationError
			if !errors.Is(dispatchErr, workflowruntime.ErrStepValidation) || !errors.As(dispatchErr, &outputErr) || outputErr.Source == nil || outputErr.Source.StartLine != 27 || result.Attempt.Status != workflowruntime.NodeFailed {
				t.Fatalf("Dispatch = %#v, %v outputErr=%#v", result, dispatchErr, outputErr)
			}
			if test.wantSecret && !errors.Is(dispatchErr, values.ErrSecretDerivation) {
				t.Fatalf("secret projection error = %v", dispatchErr)
			}
		})
	}
}

func TestDispatcherNodeOutputProjectionUsesDurableFanOutItemAndIndex(t *testing.T) {
	ctx := context.Background()
	store := inmemory.NewStore()
	base := time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)
	runID := workflowruntime.RunID("dispatch-fanout-projection")
	parent := invocationID(runID, "create")
	createRun(t, store, runID, base)
	createNode(t, store, parent, workflowruntime.NodePending, 0, base)
	forEach := graph.ForEachSpec{Items: graph.Expression{Text: `[{"title":"one"}]`}}
	expanded, err := (workflowruntime.FanOutCoordinator{Store: store}).Expand(ctx, workflowruntime.FanOutExpandCommand{
		Parent: parent, ExpectedParentGeneration: 1, Spec: forEach, At: base.Add(time.Second),
	})
	if err != nil || len(expanded.Children) != 1 {
		t.Fatalf("Expand = %#v, %v", expanded, err)
	}
	claim, acquired, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(ctx, workflowruntime.ReadyClaimRequest{
		RunID: runID, Owner: "worker", Token: "token", IdempotencyKey: "fanout-projection-claim", Now: base.Add(2 * time.Second), LeaseUntil: base.Add(time.Minute),
	})
	if err != nil || !acquired || claim.Candidate.InvocationID != expanded.Children[0].ID {
		t.Fatalf("ClaimNext = %#v, %t, %v", claim, acquired, err)
	}
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("fanout-projection-kind", "v1")
	kind.SpecValue.OutputSchema = graph.Schema{}
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"native": dispatchValue(t, "native", true)}}, nil
	}
	if registerErr := registry.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(ctx, workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{
		ID: parent.NodeID, Kind: "fanout-projection-kind", KindVersion: "v1", ForEach: &forEach,
		Outputs: []graph.OutputSpec{{Name: "label", Schema: graph.Schema{"type": "string"}, Value: &graph.Binding{Kind: graph.BindingInterpolation, Interpolation: "{{ item.title }}-{{ index }}"}}},
	}})
	if err != nil || result.Outputs == nil {
		t.Fatalf("Dispatch = %#v, %v", result, err)
	}
	outputs, err := store.LoadValues(ctx, *result.Outputs)
	if err != nil || outputs["label"].Inline != "one-0" {
		t.Fatalf("fan-out projected outputs = %#v, %v", outputs, err)
	}
}
