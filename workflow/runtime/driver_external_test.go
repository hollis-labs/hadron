package runtime_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/runtimetest"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestNodeDriverDefersInputEvaluationUntilReadinessAndRouteSelection(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 20, 0, 0, 0, time.UTC)
	registry := driverRegistry(t)

	t.Run("dependency-pending", func(t *testing.T) {
		store := runtimetest.NewStore()
		createRun(t, store, "driver-pending", base)
		run, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "driver-pending", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base})
		if err != nil {
			t.Fatal(err)
		}
		createNode(t, store, invocationID(run.Snapshot.ID, "source"), workflowruntime.NodePending, 0, base)
		targetID := invocationID(run.Snapshot.ID, "target")
		createNode(t, store, targetID, workflowruntime.NodePending, 0, base)
		workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
			{ID: "source", Kind: "safe", KindVersion: "v1"},
			{ID: "target", Kind: "safe", KindVersion: "v1", Needs: []graph.Need{{Node: "source"}}, InputBindings: map[string]graph.Binding{"value": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.missing"}}}},
		}}
		plan := driverRecoveryPlan(t, run.Snapshot, workflow)
		result, err := (&workflowruntime.NodeDriver{Store: store, Inputs: store, Control: store, Registry: registry}).Drive(ctx, workflowruntime.DriveNodeRequest{Run: run.Snapshot, Plan: plan, InvocationID: targetID, Node: workflow.Nodes[1], ExpressionContext: values.ExpressionContext{}, At: base.Add(time.Second)})
		if err != nil || result.Binding != nil || result.Progressed.Snapshot.Status != workflowruntime.NodeBlocked || result.Progressed.Snapshot.Inputs != nil {
			t.Fatalf("pending driver = %#v, %v", result, err)
		}
	})

	t.Run("route-unselected", func(t *testing.T) {
		store := runtimetest.NewStore()
		createRun(t, store, "driver-route", base)
		run, transitionErr := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "driver-route", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base})
		if transitionErr != nil {
			t.Fatal(transitionErr)
		}
		for _, nodeID := range []string{"source", "selected", "target"} {
			createNode(t, store, invocationID(run.Snapshot.ID, nodeID), workflowruntime.NodePending, 0, base)
		}
		makeTerminalNode(t, store, invocationID(run.Snapshot.ID, "source"), workflowruntime.NodeSucceeded, base.Add(time.Second))
		workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
			{ID: "source", Kind: "safe", KindVersion: "v1", Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{{When: graph.Expression{Text: "true"}, Targets: []string{"selected"}}}, Default: []string{"target"}}},
			{ID: "selected", Kind: "safe", KindVersion: "v1"},
			{ID: "target", Kind: "safe", KindVersion: "v1", InputBindings: map[string]graph.Binding{"value": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.missing"}}}},
		}}
		if _, err := workflowruntime.NewControlFlowCoordinator(store, store, nil).DecideSwitch(ctx, workflowruntime.DecideSwitchRequest{Source: invocationID(run.Snapshot.ID, "source"), Node: workflow.Nodes[0], At: base.Add(2 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		plan := driverRecoveryPlan(t, run.Snapshot, workflow)
		result, err := (&workflowruntime.NodeDriver{Store: store, Inputs: store, Control: store, Registry: registry}).Drive(ctx, workflowruntime.DriveNodeRequest{Run: run.Snapshot, Plan: plan, InvocationID: invocationID(run.Snapshot.ID, "target"), Node: workflow.Nodes[2], ExpressionContext: values.ExpressionContext{}, At: base.Add(3 * time.Second)})
		if err != nil || result.Binding != nil || result.Progressed.Snapshot.Status != workflowruntime.NodeSkipped || result.Progressed.Snapshot.Inputs != nil {
			t.Fatalf("unselected route driver = %#v, %v", result, err)
		}
	})
}

func TestNodeDriverBindsCompilerScopedTypedInputsAndFailsClosedOnSchema(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 21, 0, 0, 0, time.UTC)
	registry := driverRegistry(t)
	store := runtimetest.NewStore()
	createRun(t, store, "driver-ready", base)
	run, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "driver-ready", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base})
	if err != nil {
		t.Fatal(err)
	}
	producerID := invocationID(run.Snapshot.ID, "producer")
	hiddenID := invocationID(run.Snapshot.ID, "hidden")
	consumerID := invocationID(run.Snapshot.ID, "consumer")
	for _, id := range []workflowruntime.NodeInvocationID{producerID, hiddenID} {
		createNode(t, store, id, workflowruntime.NodeReady, 0, base)
	}
	createNode(t, store, consumerID, workflowruntime.NodePending, 0, base)
	producerAttempt := startAttemptForRecovery(t, store, producerID, "safe", "v1", base.Add(time.Second), base.Add(time.Hour))
	value, err := values.NewInline("visible", values.Metadata{Producer: values.Producer{Kind: "test", Reference: "producer"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	producerIDCopy, attemptIDCopy := producerID, producerAttempt.Attempt.ID
	outputRef, err := store.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "attempt-output", RunID: run.Snapshot.ID, Invocation: &producerIDCopy, Attempt: &attemptIDCopy}, Values: values.ValueSet{"value": value}})
	if err != nil {
		t.Fatal(err)
	}
	finishSucceededForRecovery(t, store, producerAttempt, &outputRef, base.Add(2*time.Second))
	finishSucceededForRecovery(t, store, startAttemptForRecovery(t, store, hiddenID, "safe", "v1", base.Add(time.Second), base.Add(time.Hour)), nil, base.Add(2*time.Second))
	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{
		{ID: "producer", Kind: "safe", KindVersion: "v1"},
		{ID: "hidden", Kind: "safe", KindVersion: "v1"},
		{ID: "consumer", Kind: "safe", KindVersion: "v1", Needs: []graph.Need{{Node: "producer"}}, InputBindings: map[string]graph.Binding{"value": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.producer.outputs.value"}}}},
	}}
	plan := driverRecoveryPlan(t, run.Snapshot, workflow)
	expression, err := workflowruntime.BuildExpressionContext(ctx, store, store, workflow, run.Snapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	scoped, _, err := plan.Visibility.ScopeNodeContext("consumer", expression, values.ExpressionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, visible := scoped.Steps["hidden"]; visible || scoped.Steps["producer"].Outputs["value"].Inline != "visible" {
		t.Fatalf("compiler visibility = %#v", scoped.Steps)
	}
	result, err := (&workflowruntime.NodeDriver{Store: store, Inputs: store, Control: store, Registry: registry}).Drive(ctx, workflowruntime.DriveNodeRequest{Run: run.Snapshot, Plan: plan, InvocationID: consumerID, Node: workflow.Nodes[2], ExpressionContext: expression, At: base.Add(3 * time.Second)})
	if err != nil || result.Binding == nil || result.Progressed.Snapshot.Status != workflowruntime.NodeReady || result.Progressed.Snapshot.Inputs == nil {
		t.Fatalf("ready driver = %#v, %v", result, err)
	}
	bound, err := store.LoadValues(ctx, *result.Progressed.Snapshot.Inputs)
	if err != nil || bound["value"].Inline != "visible" || bound["value"].Redaction != values.RedactionPrivate {
		t.Fatalf("bound typed inputs = %#v, %v", bound, err)
	}

	invalidID := invocationID(run.Snapshot.ID, "invalid")
	createNode(t, store, invalidID, workflowruntime.NodePending, 0, base)
	invalidNode := graph.Node{ID: "invalid", Kind: "safe", KindVersion: "v1", InputBindings: map[string]graph.Binding{"value": {Kind: graph.BindingLiteral, Literal: jsonNumberValue(t, "9")}}}
	invalidGraph := graph.Graph{ID: "plan", Version: "v1", Nodes: append(append([]graph.Node(nil), workflow.Nodes...), invalidNode)}
	invalidPlan := driverRecoveryPlan(t, run.Snapshot, invalidGraph)
	invalidResult, invalidErr := (&workflowruntime.NodeDriver{Store: store, Inputs: store, Control: store, Registry: registry}).Drive(ctx, workflowruntime.DriveNodeRequest{Run: run.Snapshot, Plan: invalidPlan, InvocationID: invalidID, Node: invalidNode, ExpressionContext: expression, At: base.Add(4 * time.Second)})
	if invalidErr == nil || invalidResult.Binding != nil {
		t.Fatalf("schema-invalid unbound driver = %#v, %v", invalidResult, invalidErr)
	}
	invalidSnapshot, _ := store.LoadNodeInvocation(ctx, invalidID)
	if invalidSnapshot.Status != workflowruntime.NodePending || invalidSnapshot.Inputs != nil {
		t.Fatalf("schema-invalid node mutated = %#v", invalidSnapshot)
	}
	badValue := jsonNumberValue(t, "10")
	boundInvalid, err := store.BindNodeInputs(ctx, workflowruntime.BindNodeInputsRequest{InvocationID: invalidID, ExpectedGeneration: invalidSnapshot.Generation, IdempotencyKey: "prebound-invalid", Values: values.ValueSet{"value": badValue}, At: base.Add(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	_, invalidErr = (&workflowruntime.NodeDriver{Store: store, Inputs: store, Control: store, Registry: registry}).Drive(ctx, workflowruntime.DriveNodeRequest{Run: run.Snapshot, Plan: invalidPlan, InvocationID: invalidID, Node: invalidNode, ExpressionContext: expression, At: base.Add(6 * time.Second)})
	if invalidErr == nil {
		t.Fatal("already-bound schema-invalid inputs were accepted")
	}
	replay, err := store.BindNodeInputs(ctx, workflowruntime.BindNodeInputsRequest{InvocationID: invalidID, ExpectedGeneration: invalidSnapshot.Generation, IdempotencyKey: "prebound-invalid", Values: values.ValueSet{"value": badValue}, At: base.Add(5 * time.Second)})
	if err != nil || replay.Outcome != workflowruntime.IdempotencyReplayed || replay.Inputs != boundInvalid.Inputs {
		t.Fatalf("lost-response input replay = %#v, %v", replay, err)
	}
}

func TestInMemoryNodeInputBindingDefensivelyClonesIdempotencyIntent(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 22, 0, 0, 0, time.UTC)
	store := runtimetest.NewStore()
	createRun(t, store, "binding-clone", base)
	if _, err := store.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: "binding-clone", ExpectedGeneration: 1, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	id := invocationID("binding-clone", "work")
	createNode(t, store, id, workflowruntime.NodePending, 0, base)
	original, valueErr := values.NewInline("original", values.Metadata{Producer: values.Producer{Kind: "test", Reference: "binding"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if valueErr != nil {
		t.Fatal(valueErr)
	}
	callerValues := values.ValueSet{"value": original}
	request := workflowruntime.BindNodeInputsRequest{InvocationID: id, ExpectedGeneration: 1, IdempotencyKey: "binding-clone", Values: callerValues, At: base.Add(time.Second)}
	applied, bindErr := store.BindNodeInputs(ctx, request)
	if bindErr != nil || applied.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("BindNodeInputs = %#v, %v", applied, bindErr)
	}
	callerValues["value"] = jsonNumberValue(t, "99")
	if _, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: applied.Node.Generation, To: workflowruntime.NodeReady, At: base.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	request.Values = values.ValueSet{"value": original}
	replayed, replayErr := store.BindNodeInputs(ctx, request)
	if replayErr != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Inputs != applied.Inputs {
		t.Fatalf("defensive binding replay = %#v, %v", replayed, replayErr)
	}
}

func driverRegistry(t *testing.T) *stepkind.MemoryRegistry {
	t.Helper()
	kind := stepkindtest.NewNoopKind("safe", "v1")
	kind.SpecValue.InputSchema = graph.Schema{
		"type": "object", "additionalProperties": false, "required": []any{"value"},
		"properties": map[string]any{"value": map[string]any{"type": "string"}},
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(kind); err != nil {
		t.Fatal(err)
	}
	return registry
}

func driverRecoveryPlan(t *testing.T, run workflowruntime.RunSnapshot, workflow graph.Graph) workflowruntime.RecoveryPlan {
	t.Helper()
	plan := workflowcompile.ExecutionPlan{SchemaVersion: run.Plan.SchemaVersion, ID: run.Plan.ID, Digest: run.Plan.Digest, Graph: workflow}
	inferred := workflowcompile.InferValueDependencies(&plan, workflowcompile.DependencyOptions{})
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
		t.Fatalf("InferValueDependencies = %#v", inferred.Diagnostics)
	}
	return workflowruntime.RecoveryPlan{Ref: run.Plan, Plan: plan, Visibility: inferred.Visibility}
}

func jsonNumberValue(t *testing.T, number string) values.Value {
	t.Helper()
	value, err := values.NewInline(json.Number(number), values.Metadata{Producer: values.Producer{Kind: "test", Reference: "number"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
