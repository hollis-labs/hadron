package compile

import (
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestInferValueDependenciesRejectsNilPlanWithStructuredDiagnostic(t *testing.T) {
	t.Parallel()

	result := InferValueDependencies(nil, DependencyOptions{})
	if result.Plan != nil || len(result.Diagnostics) != 1 {
		t.Fatalf("nil result = %#v", result)
	}
	finding := result.Diagnostics[0]
	if finding.Code != CodeInvalidValidationInput || finding.Remediation == nil {
		t.Fatalf("nil diagnostic = %#v", finding)
	}
	if err := finding.Validate(); err != nil {
		t.Fatalf("nil diagnostic is invalid: %v", err)
	}
}

func TestInferValueDependenciesWalksEveryExpressionCarrier(t *testing.T) {
	t.Parallel()

	producerIDs := []string{
		"need-only", "edge-only", "existing-data", "binding", "condition",
		"collection", "switch-source", "transform-source", "node-output",
		"verifier-source", "memo-source", "idempotency-source", "interpolation-source",
		"workflow-output-source", "switch-target",
	}
	nodes := make([]graph.Node, 0, len(producerIDs)+1)
	for i, id := range producerIDs {
		nodes = append(nodes, graph.Node{ID: id, Kind: "fixture", Source: dependencySource(i+2, "steps", id)})
	}
	consumerSource := dependencySource(40, "steps", "consume")
	nodes = append(nodes, graph.Node{
		ID: "consume", Kind: "transform", Source: consumerSource,
		Needs: []graph.Need{{Node: "need-only", Kind: graph.EdgeControl, Source: dependencySource(41, "steps", "consume", "needs", "0")}},
		InputBindings: map[string]graph.Binding{
			"expression": dependencyExpressionBinding(42, "steps.binding.outputs.value", "steps", "consume", "with", "expression"),
			"interpolation": {
				Kind: graph.BindingInterpolation, Interpolation: "value={{ steps['interpolation-source'].outputs.value }}",
				Source: dependencySource(43, "steps", "consume", "with", "interpolation"),
			},
		},
		If: &graph.Expression{Text: "steps.condition.status == 'succeeded'", Source: dependencySource(44, "steps", "consume", "if")},
		ForEach: &graph.ForEachSpec{
			Items: graph.Expression{Text: "steps.collection.outputs.items", Source: dependencySource(45, "steps", "consume", "for_each", "items")},
		},
		Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{{
			When:    graph.Expression{Text: "steps['switch-source'].outputs.enabled", Source: dependencySource(46, "steps", "consume", "switch", "arms", "0", "when")},
			Targets: []string{"switch-target"},
		}}},
		Config: graph.Config{"nested": map[string]any{
			"mapped": "steps['transform-source'].outputs.value",
		}},
		Outputs: []graph.OutputSpec{{
			Name: "derived", Value: dependencyBinding(47, "steps['node-output'].outputs.value", "steps", "consume", "outputs", "derived"),
		}},
		Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{
			Kind: "expression-check", Config: graph.Config{"rule": "steps['verifier-source'].outputs.value"},
			Source: dependencySource(48, "steps", "consume", "verify", "checks", "0"),
		}}},
		Memoization: &graph.MemoizationSpec{
			Key: graph.Expression{Text: "steps['memo-source'].outputs.value", Source: dependencySource(49, "steps", "consume", "memoize", "key")},
		},
		Idempotency: &graph.IdempotencySpec{
			Mode: graph.IdempotencyKeyed,
			Key:  &graph.Expression{Text: "steps['idempotency-source'].outputs.value", Source: dependencySource(50, "steps", "consume", "idempotency", "key")},
		},
	})

	explicitDataSource := dependencySource(51, "edges", "existing-data")
	controlSource := dependencySource(52, "edges", "edge-only")
	plan := dependencyPlan(graph.Graph{
		ID: "all-carriers", Version: "v1", Nodes: nodes,
		Edges: []graph.Edge{
			{From: "edge-only", To: "consume", Kind: graph.EdgeControl, Source: controlSource},
			{From: "existing-data", To: "consume", Kind: graph.EdgeData, Source: explicitDataSource},
		},
		Outputs: []graph.OutputSpec{{
			Name: "result", Value: dependencyBinding(60, "steps['workflow-output-source'].outputs.value", "outputs", "result"),
		}},
	})
	originalJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}

	result := InferValueDependencies(plan, DependencyOptions{
		VerificationExtractors: map[string]VerificationExpressionExtractor{
			"expression-check": VerificationExpressionExtractorFunc(func(check graph.VerificationCheck) ([]graph.Expression, []diagnostic.Diagnostic) {
				return []graph.Expression{{Text: check.Config["rule"].(string)}}, nil
			}),
		},
	})
	if len(result.Diagnostics) != 0 {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if result.Plan == nil {
		t.Fatal("inferred plan is nil")
	}

	wantProducers := []string{
		"binding", "collection", "condition", "edge-only", "existing-data",
		"idempotency-source", "interpolation-source", "memo-source", "need-only",
		"node-output", "switch-source", "transform-source", "verifier-source",
	}
	if got := result.Visibility.Nodes["consume"]; !slices.Equal(got.Producers, wantProducers) || !got.FanOut {
		t.Fatalf("consume visibility = %#v, want producers %#v and fan-out", got, wantProducers)
	}
	if got := result.Visibility.WorkflowOutputs.Producers; !slices.Equal(got, []string{"workflow-output-source"}) {
		t.Fatalf("workflow output visibility = %#v", got)
	}
	if hasEdge(result.Plan.Graph.Edges, "workflow-output-source", "consume", graph.EdgeData) {
		t.Fatal("workflow output reference created an execution edge")
	}
	if !hasEdge(result.Plan.Graph.Edges, "edge-only", "consume", graph.EdgeControl) {
		t.Fatal("ordering-only explicit edge was not preserved")
	}
	if countEdges(result.Plan.Graph.Edges, "existing-data", "consume", graph.EdgeData) != 1 {
		t.Fatalf("explicit data edge was duplicated: %#v", result.Plan.Graph.Edges)
	}
	if got := findEdge(result.Plan.Graph.Edges, "existing-data", "consume", graph.EdgeData); !reflect.DeepEqual(got.Source, explicitDataSource) {
		t.Fatalf("explicit data source = %#v, want %#v", got.Source, explicitDataSource)
	}
	for _, producer := range wantProducers {
		if producer == "need-only" || producer == "edge-only" {
			continue
		}
		if !hasEdge(result.Plan.Graph.Edges, producer, "consume", graph.EdgeData) {
			t.Errorf("missing inferred data edge %s -> consume", producer)
		}
	}

	inferredKey := EdgeSourceKey("transform-source", "consume", graph.EdgeData)
	planSource, planOK := result.Plan.SourceMap.Edges[inferredKey]
	graphSourceRef, graphOK := result.Plan.Graph.SourceMap.Edges[inferredKey]
	if !planOK || !graphOK || !reflect.DeepEqual(planSource, graphSourceRef) {
		t.Fatalf("inferred edge source maps: plan=%#v/%t graph=%#v/%t", planSource, planOK, graphSourceRef, graphOK)
	}
	if !slices.Equal(planSource.Path, []string{"steps", "consume", "config", "nested", "mapped"}) {
		t.Fatalf("transform source path = %#v", planSource.Path)
	}
	if result.Plan.Graph.Digest == plan.Graph.Digest || result.Plan.Digest == plan.Digest {
		t.Fatal("inferred semantic edges did not update graph and plan digests")
	}
	afterJSON, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterJSON, originalJSON) {
		t.Fatal("InferValueDependencies mutated its input plan")
	}
}

func TestInferValueDependenciesReportsMisspellingsActivationLocalsCyclesAndReadiness(t *testing.T) {
	t.Parallel()

	cycleASource := dependencySource(10, "steps", "a", "with", "value")
	cycleBSource := dependencySource(20, "steps", "b", "with", "value")
	missingSource := dependencySource(30, "steps", "missing-consumer", "with", "value")
	itemsSource := dependencySource(40, "steps", "fan-out", "for_each", "items")
	activationSource := dependencySource(50, "activations", "schedule", "inputs", "payload")
	plan := dependencyPlan(graph.Graph{
		ID: "invalid-dependencies", Version: "v1",
		Nodes: []graph.Node{
			{ID: "a", Kind: "fixture", InputBindings: map[string]graph.Binding{"value": dependencyExpressionBindingAt(cycleASource, "steps.b.outputs.value")}, Source: dependencySource(8, "steps", "a")},
			{ID: "b", Kind: "fixture", InputBindings: map[string]graph.Binding{"value": dependencyExpressionBindingAt(cycleBSource, "steps.a.outputs.value")}, Source: dependencySource(18, "steps", "b")},
			{ID: "missing-consumer", Kind: "fixture", InputBindings: map[string]graph.Binding{"value": dependencyExpressionBindingAt(missingSource, "steps.fetc.outputs.value")}, Source: dependencySource(28, "steps", "missing-consumer")},
			{ID: "fan-out", Kind: "fixture", ForEach: &graph.ForEachSpec{Items: graph.Expression{Text: "item.tasks", Source: itemsSource}}, Source: dependencySource(38, "steps", "fan-out")},
			{ID: "readiness", Kind: "fixture", ReadyWhen: graph.ReadyRule("eventually"), Source: dependencySource(45, "steps", "readiness")},
		},
		Activations: []graph.ActivationDeclaration{{
			ID: "schedule", Kind: "cron", Source: dependencySource(49, "activations", "schedule"),
			Inputs: map[string]graph.Binding{"payload": dependencyExpressionBindingAt(activationSource, "steps.a.outputs.value")},
		}},
	})

	result := InferValueDependencies(plan, DependencyOptions{})
	if result.Plan != nil {
		t.Fatal("plan must be nil when inference reports errors")
	}
	for _, code := range []diagnostic.Code{
		CodeGraphCycle, CodeUnknownValueProducer, CodeUnavailableValueReference, CodeUnsupportedReadinessRule,
	} {
		if !hasDiagnosticCode(result.Diagnostics, code) {
			t.Errorf("missing diagnostic code %s: %#v", code, result.Diagnostics)
		}
	}
	assertDiagnosticAt(t, result.Diagnostics, CodeUnknownValueProducer, missingSource)
	assertDiagnosticAt(t, result.Diagnostics, CodeUnsupportedReadinessRule, dependencySource(45, "steps", "readiness"))
	for _, finding := range result.Diagnostics {
		if finding.Remediation == nil || strings.TrimSpace(finding.Remediation.Message) == "" {
			t.Errorf("diagnostic lacks remediation: %#v", finding)
		}
		if finding.Code == CodeGraphCycle && len(finding.Related) == 0 {
			t.Errorf("cycle lacks related producer/consumer source: %#v", finding)
		}
	}
	if got := result.Diagnostics; !sort.SliceIsSorted(got, func(i, j int) bool {
		return diagnosticSortKey(got[i]) < diagnosticSortKey(got[j])
	}) {
		t.Fatalf("diagnostics are not deterministic: %#v", got)
	}
}

func TestInferValueDependenciesDefersDynamicOptionalFanOutAndOpaqueVerification(t *testing.T) {
	t.Parallel()

	plan := dependencyPlan(graph.Graph{
		ID: "deferred", Version: "v1",
		Nodes: []graph.Node{
			{ID: "allowed", Kind: "fixture", Source: dependencySource(2, "steps", "allowed")},
			{ID: "conditional", Kind: "fixture", If: &graph.Expression{Text: "inputs.enabled", Source: dependencySource(5, "steps", "conditional", "if")}, Source: dependencySource(4, "steps", "conditional")},
			{ID: "fan-producer", Kind: "fixture", ForEach: &graph.ForEachSpec{Items: graph.Expression{Text: "inputs.items", Source: dependencySource(8, "steps", "fan-producer", "for_each")}}, Source: dependencySource(7, "steps", "fan-producer")},
			{ID: "branch", Kind: "fixture", Source: dependencySource(11, "steps", "branch")},
			{ID: "switch", Kind: "fixture", Switch: &graph.SwitchSpec{Arms: []graph.SwitchArm{{When: graph.Expression{Text: "inputs.enabled"}, Targets: []string{"branch"}}}}, Source: dependencySource(10, "steps", "switch")},
			{ID: "consume", Kind: "fixture", Needs: []graph.Need{{Node: "allowed", Kind: graph.EdgeControl}}, Source: dependencySource(20, "steps", "consume"), InputBindings: map[string]graph.Binding{
				"dynamic":     dependencyExpressionBinding(21, "steps[run.pick].status", "steps", "consume", "with", "dynamic"),
				"conditional": dependencyExpressionBinding(22, "steps.conditional.outputs.value", "steps", "consume", "with", "conditional"),
				"fan":         dependencyExpressionBinding(23, "steps['fan-producer'].items", "steps", "consume", "with", "fan"),
				"branch":      dependencyExpressionBinding(24, "steps.branch.outputs.value", "steps", "consume", "with", "branch"),
			}, Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{
				Kind: "opaque", Config: graph.Config{"expression": "steps.missing.outputs.value"}, Source: dependencySource(25, "steps", "consume", "verify", "checks", "0"),
			}}}},
			{ID: "fan-consumer", Kind: "fixture", ForEach: &graph.ForEachSpec{Items: graph.Expression{Text: "inputs.items"}}, Source: dependencySource(30, "steps", "fan-consumer"), InputBindings: map[string]graph.Binding{
				"item":  dependencyExpressionBinding(31, "item.name", "steps", "fan-consumer", "with", "item"),
				"index": dependencyExpressionBinding(32, "index", "steps", "fan-consumer", "with", "index"),
			}},
		},
	})

	result := InferValueDependencies(plan, DependencyOptions{})
	if len(result.Diagnostics) != 0 || result.Plan == nil {
		t.Fatalf("inference failed: plan=%#v diagnostics=%#v", result.Plan, result.Diagnostics)
	}
	wantReasons := map[DeferredDependencyReason]int{
		DeferredDynamicStep:        1,
		DeferredOptionalProducer:   3,
		DeferredOpaqueVerification: 1,
		DeferredFanOutItem:         1,
		DeferredFanOutIndex:        1,
	}
	gotReasons := make(map[DeferredDependencyReason]int)
	for _, deferred := range result.Deferred {
		gotReasons[deferred.Reason]++
		if deferred.Reason == DeferredDynamicStep && deferred.Reference != nil && len(deferred.Reference.Path) > 0 && deferred.Reference.Path[0] == "status" {
			if hasEdge(result.Plan.Graph.Edges, "status", "consume", graph.EdgeData) {
				t.Fatal("dynamic steps lookup inferred from the parser's remaining static path")
			}
		}
	}
	if !reflect.DeepEqual(gotReasons, wantReasons) {
		t.Fatalf("deferred reasons = %#v, want %#v; all=%#v", gotReasons, wantReasons, result.Deferred)
	}
	if !slices.Equal(result.Visibility.Nodes["consume"].Producers, []string{"allowed", "branch", "conditional", "fan-producer"}) {
		t.Fatalf("consume visibility = %#v", result.Visibility.Nodes["consume"])
	}
	if !sort.SliceIsSorted(result.Deferred, func(i, j int) bool {
		return deferredSortKey(result.Deferred[i]) < deferredSortKey(result.Deferred[j])
	}) {
		t.Fatalf("deferred dependencies are not deterministic: %#v", result.Deferred)
	}
}

func TestValueVisibilityPlanScopesAvailableContextAndPreservesBasePolicy(t *testing.T) {
	t.Parallel()

	plan := dependencyPlan(graph.Graph{
		ID: "visibility", Version: "v1",
		Nodes: []graph.Node{
			{ID: "allowed", Kind: "fixture"},
			{ID: "secret", Kind: "fixture"},
			{ID: "consume", Kind: "fixture", Needs: []graph.Need{{Node: "allowed", Kind: graph.EdgeControl}}, InputBindings: map[string]graph.Binding{
				"dynamic": dependencyExpressionBinding(10, "steps[run.pick].status", "steps", "consume", "with", "dynamic"),
			}},
			{ID: "fan", Kind: "fixture", ForEach: &graph.ForEachSpec{Items: graph.Expression{Text: "inputs.items"}}, InputBindings: map[string]graph.Binding{
				"item": dependencyExpressionBinding(20, "item", "steps", "fan", "with", "item"),
			}},
		},
	})
	result := InferValueDependencies(plan, DependencyOptions{})
	if len(result.Diagnostics) != 0 || result.Plan == nil {
		t.Fatalf("inference failed: %#v", result.Diagnostics)
	}
	item := values.Value{}
	index := 2
	available := values.ExpressionContext{
		Steps: map[string]values.StepContext{
			"allowed": {Status: "succeeded"},
			"secret":  {Status: "succeeded"},
		},
		Item: &item, Index: &index,
		Run: map[string]any{"pick": "secret"},
	}
	base := values.ExpressionOptions{AllowEnv: true, VisibleSteps: []string{"caller-value-must-be-replaced"}}
	scoped, options, err := result.Visibility.ScopeNodeContext("consume", available, base)
	if err != nil {
		t.Fatal(err)
	}
	if !options.AllowEnv || !slices.Equal(options.VisibleSteps, []string{"allowed"}) {
		t.Fatalf("scoped options = %#v", options)
	}
	if _, visible := scoped.Steps["secret"]; visible || len(scoped.Steps) != 1 {
		t.Fatalf("scoped steps = %#v", scoped.Steps)
	}
	if scoped.Item != nil || scoped.Index != nil {
		t.Fatal("non-fan-out context retained item/index")
	}
	_, err = values.NewExpressionEngine().EvaluateRaw(
		graph.Expression{Text: "steps[run.pick].status"}, scoped, options,
	)
	var expressionErr *values.ExpressionError
	if !errors.As(err, &expressionErr) || expressionErr.Diagnostic.Code != values.CodeExpressionUnresolved {
		t.Fatalf("hidden dynamic reference error = %#v", err)
	}

	fanScoped, _, err := result.Visibility.ScopeNodeContext("fan", available, base)
	if err != nil {
		t.Fatal(err)
	}
	if fanScoped.Item != &item || fanScoped.Index != &index {
		t.Fatal("fan-out context did not preserve item/index")
	}
	if _, _, err := result.Visibility.ScopeNodeContext("unknown", available, base); err == nil {
		t.Fatal("unknown node scope succeeded")
	}
}

func TestRootStepsAccessDefersToFilteredExplicitVisibility(t *testing.T) {
	t.Parallel()

	rootSource := dependencySource(10, "steps", "consume", "with", "visible")
	plan := dependencyPlan(graph.Graph{
		ID: "root-steps", Version: "v1",
		Nodes: []graph.Node{
			{ID: "allowed", Kind: "fixture", Source: dependencySource(2, "steps", "allowed")},
			{ID: "unrelated", Kind: "fixture", Source: dependencySource(4, "steps", "unrelated")},
			{
				ID: "consume", Kind: "fixture", Source: dependencySource(7, "steps", "consume"),
				Needs: []graph.Need{{Node: "allowed", Kind: graph.EdgeControl}},
				InputBindings: map[string]graph.Binding{
					"visible": dependencyExpressionBindingAt(rootSource, "steps"),
				},
			},
		},
	})
	result := InferValueDependencies(plan, DependencyOptions{})
	if len(result.Diagnostics) != 0 || result.Plan == nil {
		t.Fatalf("root steps inference failed: %#v", result.Diagnostics)
	}
	if len(result.Deferred) != 1 || result.Deferred[0].Reason != DeferredDynamicStep || result.Deferred[0].Reference == nil || len(result.Deferred[0].Reference.Path) != 0 {
		t.Fatalf("root steps deferred = %#v", result.Deferred)
	}
	if !slices.Equal(result.Visibility.Nodes["consume"].Producers, []string{"allowed"}) {
		t.Fatalf("root steps visibility = %#v", result.Visibility.Nodes["consume"])
	}

	scoped, options, err := result.Visibility.ScopeNodeContext(
		"consume",
		values.ExpressionContext{Steps: map[string]values.StepContext{
			"allowed":   {Status: "succeeded"},
			"unrelated": {Status: "succeeded"},
		}},
		values.ExpressionOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := scoped.Steps["unrelated"]; exists || len(scoped.Steps) != 1 {
		t.Fatalf("root steps scoped context = %#v", scoped.Steps)
	}
	evaluated, err := values.NewExpressionEngine().EvaluateRaw(
		graph.Expression{Text: "steps", Source: rootSource}, scoped, options,
	)
	if err != nil {
		t.Fatalf("evaluate root steps: %v", err)
	}
	stepMap, ok := evaluated.(map[string]any)
	if !ok || len(stepMap) != 1 || stepMap["allowed"] == nil {
		t.Fatalf("evaluated root steps = %#v", evaluated)
	}
}

func TestExplicitControlAndInferredDataEdgesShareEndpointsWithoutDuplication(t *testing.T) {
	t.Parallel()

	controlSource := dependencySource(8, "steps", "consumer", "needs", "0")
	dataSource := dependencySource(10, "steps", "consumer", "with", "payload")
	plan := dependencyPlan(graph.Graph{
		ID: "parallel-edges", Version: "v1",
		Nodes: []graph.Node{
			{ID: "producer", Kind: "fixture", Source: dependencySource(2, "steps", "producer")},
			{
				ID: "consumer", Kind: "fixture", Source: dependencySource(6, "steps", "consumer"),
				Needs: []graph.Need{{Node: "producer", Kind: graph.EdgeControl, Source: controlSource}},
				InputBindings: map[string]graph.Binding{
					"payload": dependencyExpressionBindingAt(dataSource, "steps.producer.outputs.value"),
				},
			},
		},
		Edges: []graph.Edge{{From: "producer", To: "consumer", Kind: graph.EdgeControl, Source: controlSource}},
	})

	first := InferValueDependencies(plan, DependencyOptions{})
	if len(first.Diagnostics) != 0 || first.Plan == nil {
		t.Fatalf("first inference failed: %#v", first.Diagnostics)
	}
	if countEdges(first.Plan.Graph.Edges, "producer", "consumer", graph.EdgeControl) != 1 ||
		countEdges(first.Plan.Graph.Edges, "producer", "consumer", graph.EdgeData) != 1 {
		t.Fatalf("parallel edges = %#v", first.Plan.Graph.Edges)
	}
	if got := findEdge(first.Plan.Graph.Edges, "producer", "consumer", graph.EdgeControl).Source; !reflect.DeepEqual(got, controlSource) {
		t.Fatalf("control source = %#v, want %#v", got, controlSource)
	}
	dataKey := EdgeSourceKey("producer", "consumer", graph.EdgeData)
	if got := first.Plan.SourceMap.Edges[dataKey]; !reflect.DeepEqual(got, *dataSource) {
		t.Fatalf("plan data source = %#v, want %#v", got, *dataSource)
	}
	if got := first.Plan.Graph.SourceMap.Edges[dataKey]; !reflect.DeepEqual(got, *dataSource) {
		t.Fatalf("graph data source = %#v, want %#v", got, *dataSource)
	}

	repeated := InferValueDependencies(first.Plan, DependencyOptions{})
	if len(repeated.Diagnostics) != 0 || repeated.Plan == nil {
		t.Fatalf("repeated inference failed: %#v", repeated.Diagnostics)
	}
	if countEdges(repeated.Plan.Graph.Edges, "producer", "consumer", graph.EdgeControl) != 1 ||
		countEdges(repeated.Plan.Graph.Edges, "producer", "consumer", graph.EdgeData) != 1 {
		t.Fatalf("repeated parallel edges = %#v", repeated.Plan.Graph.Edges)
	}
	if got := repeated.Plan.SourceMap.Edges[dataKey]; !reflect.DeepEqual(got, *dataSource) {
		t.Fatalf("repeated data source = %#v, want %#v", got, *dataSource)
	}
}

func TestVerificationExtractorsOwnOpaqueConfigAndDiagnosticLocations(t *testing.T) {
	t.Parallel()

	checkSource := dependencySource(12, "steps", "consumer", "verify", "checks", "0")
	baseGraph := graph.Graph{
		ID: "verification", Version: "v1",
		Nodes: []graph.Node{
			{ID: "producer", Kind: "fixture", Source: dependencySource(2, "steps", "producer")},
			{ID: "consumer", Kind: "fixture", Source: dependencySource(10, "steps", "consumer"), Verification: &graph.VerificationSpec{Checks: []graph.VerificationCheck{{
				Kind: "owned", Config: graph.Config{"rule": "steps.producer.outputs.value"}, Source: checkSource,
			}}}},
		},
	}

	deferred := InferValueDependencies(dependencyPlan(baseGraph), DependencyOptions{})
	if len(deferred.Diagnostics) != 0 || deferred.Plan == nil {
		t.Fatalf("opaque verification should defer, got %#v", deferred.Diagnostics)
	}
	if len(deferred.Deferred) != 1 || deferred.Deferred[0].Reason != DeferredOpaqueVerification || deferred.Deferred[0].Reference != nil {
		t.Fatalf("opaque deferred = %#v", deferred.Deferred)
	}
	if hasEdge(deferred.Plan.Graph.Edges, "producer", "consumer", graph.EdgeData) {
		t.Fatal("core lexically scanned opaque verification config")
	}

	extracted := InferValueDependencies(dependencyPlan(baseGraph), DependencyOptions{
		VerificationExtractors: map[string]VerificationExpressionExtractor{
			"owned": VerificationExpressionExtractorFunc(func(check graph.VerificationCheck) ([]graph.Expression, []diagnostic.Diagnostic) {
				return []graph.Expression{{Text: check.Config["rule"].(string)}}, nil
			}),
		},
	})
	if len(extracted.Diagnostics) != 0 || extracted.Plan == nil || !hasEdge(extracted.Plan.Graph.Edges, "producer", "consumer", graph.EdgeData) {
		t.Fatalf("extracted result = plan %#v diagnostics %#v", extracted.Plan, extracted.Diagnostics)
	}
	if got := findEdge(extracted.Plan.Graph.Edges, "producer", "consumer", graph.EdgeData).Source; !reflect.DeepEqual(got, checkSource) {
		t.Fatalf("extracted expression source = %#v, want check carrier %#v", got, checkSource)
	}

	failed := InferValueDependencies(dependencyPlan(baseGraph), DependencyOptions{
		VerificationExtractors: map[string]VerificationExpressionExtractor{
			"owned": VerificationExpressionExtractorFunc(func(graph.VerificationCheck) ([]graph.Expression, []diagnostic.Diagnostic) {
				return nil, []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Message: "bad verifier expression"}}
			}),
		},
	})
	if failed.Plan != nil || len(failed.Diagnostics) != 1 {
		t.Fatalf("failed extraction = %#v", failed)
	}
	finding := failed.Diagnostics[0]
	if finding.Code != CodeVerificationExpressionExtraction || !reflect.DeepEqual(finding.Source, checkSource) {
		t.Fatalf("extractor diagnostic = %#v", finding)
	}
}

func TestInferredDigestsAreStableAcrossMapOrderAndSourceRelocation(t *testing.T) {
	t.Parallel()

	makePlan := func(locator string, reverseConfig bool) *ExecutionPlan {
		config := graph.Config{"a": "steps.producer.outputs.a", "b": "steps.producer.outputs.b"}
		if reverseConfig {
			config = graph.Config{}
			config["b"] = "steps.producer.outputs.b"
			config["a"] = "steps.producer.outputs.a"
		}
		source := &graph.SourceRef{Format: graph.SourceWorkflow, Locator: locator, StartLine: 1, Path: []string{"workflow"}}
		return dependencyPlan(graph.Graph{
			ID: "stable", Version: "v1", Source: source,
			Nodes: []graph.Node{
				{ID: "producer", Kind: "fixture", Source: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: locator, StartLine: 3, Path: []string{"steps", "0"}}},
				{ID: "consumer", Kind: "transform", Config: config, Source: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: locator, StartLine: 6, Path: []string{"steps", "1"}}},
			},
		})
	}
	first := InferValueDependencies(makePlan("one.workflow.yaml", false), DependencyOptions{})
	second := InferValueDependencies(makePlan("relocated/two.workflow.yaml", true), DependencyOptions{})
	if len(first.Diagnostics) != 0 || len(second.Diagnostics) != 0 || first.Plan == nil || second.Plan == nil {
		t.Fatalf("inference failed: first=%#v second=%#v", first.Diagnostics, second.Diagnostics)
	}
	if first.Plan.Graph.Digest != second.Plan.Graph.Digest || first.Plan.Digest != second.Plan.Digest {
		t.Fatalf("relocation/map order changed digests: graph %q/%q plan %q/%q",
			first.Plan.Graph.Digest, second.Plan.Graph.Digest, first.Plan.Digest, second.Plan.Digest)
	}
	if !reflect.DeepEqual(first.Visibility, second.Visibility) {
		t.Fatalf("visibility differs: %#v / %#v", first.Visibility, second.Visibility)
	}
	repeated := InferValueDependencies(first.Plan, DependencyOptions{})
	if len(repeated.Diagnostics) != 0 || repeated.Plan == nil {
		t.Fatalf("repeated inference failed: %#v", repeated.Diagnostics)
	}
	if repeated.Plan.Graph.Digest != first.Plan.Graph.Digest || repeated.Plan.Digest != first.Plan.Digest {
		t.Fatalf("repeated inference changed digests: graph %q/%q plan %q/%q",
			repeated.Plan.Graph.Digest, first.Plan.Graph.Digest, repeated.Plan.Digest, first.Plan.Digest)
	}
}

func dependencyPlan(value graph.Graph) *ExecutionPlan {
	if value.Source == nil {
		value.Source = dependencySource(1, "workflow")
	}
	if value.SourceMap.Graph == nil {
		value.SourceMap.Graph = cloneSource(value.Source)
	}
	if value.SourceMap.Nodes == nil {
		value.SourceMap.Nodes = make(map[string]graph.SourceRef)
	}
	if value.SourceMap.Edges == nil {
		value.SourceMap.Edges = make(map[string]graph.SourceRef)
	}
	for _, node := range value.Nodes {
		if node.Source != nil {
			value.SourceMap.Nodes[node.ID] = *cloneSource(node.Source)
		}
	}
	return &ExecutionPlan{
		SchemaVersion: ExecutionPlanSchemaVersion,
		ID:            value.ID,
		Digest:        "sha256:before-inference",
		Definition:    graph.DefinitionRef{Kind: "workflow", ID: value.ID, Version: value.Version, Digest: "sha256:source"},
		SourceDigests: []SourceDigest{{Format: graph.SourceWorkflow, Digest: "sha256:source"}},
		Graph:         value,
		SourceMap:     value.SourceMap,
	}
}

func dependencySource(line int, path ...string) *graph.SourceRef {
	return &graph.SourceRef{
		Format: graph.SourceWorkflow, Locator: "workflow.yaml",
		StartLine: line, StartColumn: 3, Path: append([]string(nil), path...),
	}
}

func dependencyExpressionBinding(line int, text string, path ...string) graph.Binding {
	return dependencyExpressionBindingAt(dependencySource(line, path...), text)
}

func dependencyExpressionBindingAt(source *graph.SourceRef, text string) graph.Binding {
	return graph.Binding{
		Kind:       graph.BindingExpression,
		Expression: &graph.Expression{Text: text, Source: cloneSource(source)},
		Source:     cloneSource(source),
	}
}

func dependencyBinding(line int, text string, path ...string) *graph.Binding {
	binding := dependencyExpressionBinding(line, text, path...)
	return &binding
}

func hasEdge(edges []graph.Edge, from, to string, kind graph.EdgeKind) bool {
	return countEdges(edges, from, to, kind) != 0
}

func countEdges(edges []graph.Edge, from, to string, kind graph.EdgeKind) int {
	count := 0
	for _, edge := range edges {
		if edge.From == from && edge.To == to && edge.Kind == kind {
			count++
		}
	}
	return count
}

func findEdge(edges []graph.Edge, from, to string, kind graph.EdgeKind) graph.Edge {
	for _, edge := range edges {
		if edge.From == from && edge.To == to && edge.Kind == kind {
			return edge
		}
	}
	return graph.Edge{}
}

func hasDiagnosticCode(findings []diagnostic.Diagnostic, code diagnostic.Code) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}

func assertDiagnosticAt(t *testing.T, findings []diagnostic.Diagnostic, code diagnostic.Code, source *graph.SourceRef) {
	t.Helper()
	for _, finding := range findings {
		if finding.Code == code {
			if !reflect.DeepEqual(finding.Source, source) {
				t.Fatalf("diagnostic %s source = %#v, want %#v", code, finding.Source, source)
			}
			return
		}
	}
	t.Fatalf("diagnostic %s not found", code)
}
