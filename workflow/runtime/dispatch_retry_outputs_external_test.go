package runtime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
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
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
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
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
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

func TestDispatcherRejectsUndeclaredNodeOutput(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "dispatch-undeclared-output")
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("undeclared-output-kind", "v1")
	kind.SpecValue.InputSchema = objectSchema("input", "string")
	kind.SpecValue.OutputSchema = graph.Schema{}
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{
			"result": dispatchValue(t, "result", "expected"),
			"extra":  dispatchValue(t, "extra", "unexpected"),
		}}, nil
	}
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{
		ID: node.ID.NodeID, Kind: "undeclared-output-kind", KindVersion: "v1",
		Outputs: []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}},
	}})
	var outputErr *workflowruntime.NodeOutputValidationError
	if !errors.Is(err, workflowruntime.ErrStepValidation) || !errors.As(err, &outputErr) || outputErr.Output != "extra" || result.Attempt.Status != workflowruntime.NodeFailed {
		t.Fatalf("Dispatch(undeclared output) = %#v, %v outputErr=%#v", result, err, outputErr)
	}
}
