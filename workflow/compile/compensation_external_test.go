package compile_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

type reversibleTestKind struct {
	*stepkindtest.Kind
	evidence func() stepkind.ReversibilityEvidence
	describe func(stepkind.ReversibilityRequest) stepkind.ReversibilityEvidence
}

type reversibleObservedTestKind struct{ *reversibleTestKind }

func (*reversibleObservedTestKind) Observe(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
	return stepkind.Observation{State: stepkind.ObservationPending}, nil
}

func (k *reversibleTestKind) DescribeReversibility(_ context.Context, request stepkind.ReversibilityRequest) (stepkind.ReversibilityEvidence, error) {
	if k.describe != nil {
		return k.describe(request), nil
	}
	return k.evidence(), nil
}

func TestCompensationValidationIsolatesMaliciousConfigMutation(t *testing.T) {
	forward := &reversibleTestKind{Kind: stepkindtest.NewNoopKind("mutating-effect", "v1")}
	forward.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	forward.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	forward.ValidateConfigFunc = func(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
		config["operation"] = "validator-mutated"
		config["nested"].(map[string]any)["value"] = "validator-mutated"
		return nil
	}
	var described []string
	forward.describe = func(request stepkind.ReversibilityRequest) stepkind.ReversibilityEvidence {
		described = append(described, request.Config["operation"].(string)+":"+request.Config["nested"].(map[string]any)["value"].(string))
		request.Config["operation"] = "provider-mutated"
		return stepkind.ReversibilityEvidence{Operation: "fixture.original", ReceiptSchema: graph.Schema{}}
	}
	handler := stepkindtest.NewNoopKind("undo", "v1")
	handler.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	handler.SpecValue.Idempotency = graph.IdempotencyIntrinsic
	workflow := validationGraph(
		graph.Node{ID: "apply", Kind: "mutating-effect", KindVersion: "v1", Config: graph.Config{"operation": "original", "nested": map[string]any{"value": "original"}}, Compensation: &graph.CompensationSpec{Handler: "undo"}},
		graph.Node{ID: "undo", Kind: "undo", KindVersion: "v1"},
	)
	workflow.Compensation = &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}
	if findings := workflowcompile.ValidateGraph(t.Context(), workflow, workflowcompile.ValidationOptions{StepKinds: validationRegistry(t, forward, handler)}); len(findings) != 0 {
		t.Fatalf("validation diagnostics = %#v", findings)
	}
	if workflow.Nodes[0].Config["operation"] != "original" || workflow.Nodes[0].Config["nested"].(map[string]any)["value"] != "original" {
		t.Fatalf("validator mutated authored graph config = %#v", workflow.Nodes[0].Config)
	}
	if fmt.Sprint(described) != "[original:original original:original]" {
		t.Fatalf("reversibility observed mutated config = %#v", described)
	}
}

func TestCompensationValidationRequiresDormantIdempotentHandlersAndTruthfulEvidence(t *testing.T) {
	forward := &reversibleTestKind{Kind: stepkindtest.NewNoopKind("effect", "v1")}
	forward.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	forward.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	forward.evidence = func() stepkind.ReversibilityEvidence {
		return stepkind.ReversibilityEvidence{Operation: "fixture.apply", ReceiptSchema: graph.Schema{"type": "object"}}
	}
	handler := stepkindtest.NewNoopKind("undo", "v1")
	handler.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	handler.SpecValue.Idempotency = graph.IdempotencyKeyed
	registry := validationRegistry(t, forward, handler)
	valid := validationGraph(
		graph.Node{ID: "apply", Kind: "effect", KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}},
		graph.Node{ID: "undo", Kind: "undo", KindVersion: "v1"},
	)
	valid.Compensation = &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}, Mode: graph.CompensationBestEffort}
	if findings := workflowcompile.ValidateGraph(t.Context(), valid, workflowcompile.ValidationOptions{StepKinds: registry}); len(findings) != 0 {
		t.Fatalf("valid compensation diagnostics = %#v", findings)
	}

	tests := []struct {
		name   string
		mutate func(graph.Graph) graph.Graph
		text   string
	}{
		{name: "self handler", mutate: func(g graph.Graph) graph.Graph { g.Nodes[0].Compensation.Handler = "apply"; return g }, text: "cannot compensate itself"},
		{name: "noncanonical handler id", mutate: func(g graph.Graph) graph.Graph { g.Nodes[0].Compensation.Handler = "Undo"; return g }, text: "not canonical"},
		{name: "handler with forward binding", mutate: func(g graph.Graph) graph.Graph {
			g.Nodes[1].InputBindings = map[string]graph.Binding{"value": {Kind: graph.BindingLiteral, Literal: "unsafe"}}
			return g
		}, text: "compensation expression root"},
		{name: "handler with memoization", mutate: func(g graph.Graph) graph.Graph {
			g.Nodes[1].Memoization = &graph.MemoizationSpec{Key: graph.Expression{Text: "inputs.key"}, MaxAge: "1h"}
			return g
		}, text: "memoization"},
		{name: "handler in forward graph", mutate: func(g graph.Graph) graph.Graph {
			g.Nodes[0].Needs = []graph.Need{{Node: "undo"}}
			return g
		}, text: "dormant handler"},
		{name: "workflow output reads handler", mutate: func(g graph.Graph) graph.Graph {
			g.Outputs = []graph.OutputSpec{{Name: "rollback", Schema: graph.Schema{}, Value: &graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.undo.outputs.result"}}}}
			return g
		}, text: "workflow output"},
		{name: "manual with finally", mutate: func(g graph.Graph) graph.Graph {
			g.Compensation.Triggers = []graph.CompensationTrigger{graph.CompensationManual}
			g.Nodes = append(g.Nodes, graph.Node{ID: "cleanup", Kind: "undo", KindVersion: "v1", Finally: &graph.FinallySpec{}})
			return g
		}, text: "incompatible with finally"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := workflowcompile.ValidateGraph(t.Context(), test.mutate(cloneCompensationGraph(t, valid)), workflowcompile.ValidationOptions{StepKinds: registry})
			if !hasCompensationFinding(findings, test.text) {
				t.Fatalf("diagnostics = %#v, want %q", findings, test.text)
			}
		})
	}
}

func TestCompensationValidationFailsClosedOnUnsupportedAndNondeterministicClaims(t *testing.T) {
	handler := stepkindtest.NewNoopKind("undo", "v1")
	handler.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	handler.SpecValue.Idempotency = graph.IdempotencyIntrinsic
	plain := stepkindtest.NewNoopKind("plain", "v1")
	plain.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	graphWith := func(kind string) graph.Graph {
		g := validationGraph(graph.Node{ID: "apply", Kind: kind, KindVersion: "v1", Compensation: &graph.CompensationSpec{Handler: "undo"}}, graph.Node{ID: "undo", Kind: "undo", KindVersion: "v1"})
		g.Compensation = &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationOnFailure}}
		return g
	}
	findings := workflowcompile.ValidateGraph(t.Context(), graphWith("plain"), workflowcompile.ValidationOptions{StepKinds: validationRegistry(t, plain, handler)})
	if !hasCompensationFinding(findings, "reversibility evidence") {
		t.Fatalf("unsupported diagnostics = %#v", findings)
	}
	noEffectHandler := stepkindtest.NewNoopKind("undo", "v1")
	noEffectHandler.SpecValue.Effects = graph.EffectSet{graph.EffectCompute}
	reversible := &reversibleTestKind{Kind: stepkindtest.NewNoopKind("effect", "v1")}
	reversible.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	reversible.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	reversible.evidence = func() stepkind.ReversibilityEvidence {
		return stepkind.ReversibilityEvidence{Operation: "fixture.effect", ReceiptSchema: graph.Schema{}}
	}
	findings = workflowcompile.ValidateGraph(t.Context(), graphWith("effect"), workflowcompile.ValidationOptions{StepKinds: validationRegistry(t, reversible, noEffectHandler)})
	if !hasCompensationFinding(findings, "material rollback effect") {
		t.Fatalf("no-effect handler diagnostics = %#v", findings)
	}
	external := &reversibleObservedTestKind{reversibleTestKind: &reversibleTestKind{Kind: stepkindtest.NewNoopKind("external-effect", "v1")}}
	external.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	external.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	external.SpecValue.Observation = stepkind.ObservationSpec{Mode: stepkind.ObservationPoll}
	external.evidence = func() stepkind.ReversibilityEvidence {
		return stepkind.ReversibilityEvidence{Operation: "fixture.external", ReceiptSchema: graph.Schema{}}
	}
	findings = workflowcompile.ValidateGraph(t.Context(), graphWith("external-effect"), workflowcompile.ValidationOptions{StepKinds: validationRegistry(t, external, handler)})
	if !hasCompensationFinding(findings, "external observation") {
		t.Fatalf("external compensation diagnostics = %#v", findings)
	}
	suspending := &reversibleTestKind{Kind: stepkindtest.NewNoopKind("suspending-effect", "v1")}
	suspending.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	suspending.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	suspending.SpecValue.CanSuspend = true
	suspending.evidence = func() stepkind.ReversibilityEvidence {
		return stepkind.ReversibilityEvidence{Operation: "fixture.suspending", ReceiptSchema: graph.Schema{}}
	}
	findings = workflowcompile.ValidateGraph(t.Context(), graphWith("suspending-effect"), workflowcompile.ValidationOptions{StepKinds: validationRegistry(t, suspending, handler)})
	if !hasCompensationFinding(findings, "suspension") {
		t.Fatalf("suspending compensation diagnostics = %#v", findings)
	}

	calls := 0
	nondeterministic := &reversibleTestKind{Kind: stepkindtest.NewNoopKind("drift", "v1")}
	nondeterministic.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	nondeterministic.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	nondeterministic.evidence = func() stepkind.ReversibilityEvidence {
		calls++
		return stepkind.ReversibilityEvidence{Operation: fmt.Sprintf("fixture.operation.%d", calls), ReceiptSchema: graph.Schema{}}
	}
	findings = workflowcompile.ValidateGraph(t.Context(), graphWith("drift"), workflowcompile.ValidationOptions{StepKinds: validationRegistry(t, nondeterministic, handler)})
	if !hasCompensationFinding(findings, "nondeterministic") {
		t.Fatalf("nondeterministic diagnostics = %#v", findings)
	}
}

func TestDependencyInferenceRejectsDormantHandlerAsForwardValueProducer(t *testing.T) {
	base := graph.Graph{ID: "dormant-dependency", Version: "v1", Nodes: []graph.Node{
		{ID: "apply", Compensation: &graph.CompensationSpec{Handler: "undo"}},
		{ID: "undo"},
		{ID: "consumer"},
	}}
	for _, test := range []struct {
		name   string
		mutate func(*graph.Graph)
	}{
		{name: "ordinary node binding", mutate: func(g *graph.Graph) {
			g.Nodes[2].InputBindings = map[string]graph.Binding{"value": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.undo.outputs.result"}}}
		}},
		{name: "workflow output", mutate: func(g *graph.Graph) {
			g.Outputs = []graph.OutputSpec{{Name: "result", Schema: graph.Schema{}, Value: &graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.undo.outputs.result"}}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			workflow := cloneCompensationGraph(t, base)
			test.mutate(&workflow)
			result := workflowcompile.InferValueDependencies(&workflowcompile.ExecutionPlan{SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion, ID: workflow.ID, Graph: workflow}, workflowcompile.DependencyOptions{})
			if result.Plan != nil || !hasCompensationFinding(result.Diagnostics, "dormant compensation handler") {
				t.Fatalf("dependency result = %#v", result)
			}
		})
	}
}

func hasCompensationFinding(findings []diagnostic.Diagnostic, text string) bool {
	for _, finding := range findings {
		if finding.Code == workflowcompile.CodeInvalidCompensation && strings.Contains(finding.Message, text) {
			return true
		}
	}
	return false
}

func cloneCompensationGraph(t *testing.T, input graph.Graph) graph.Graph {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var result graph.Graph
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
