package compile_test

import (
	"strings"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

func TestDurabilityNoneFailsClosedForHostDependentRequirements(t *testing.T) {
	registry := validationRegistry(t, stepkindtest.NewNoopKind("noop", "v1"))
	base := validationGraph(validationNode("work", "noop", "v1", 4))
	base.Durability = &graph.DurabilitySpec{Mode: graph.DurabilityNone}
	if findings := workflowcompile.ValidateGraph(t.Context(), base, workflowcompile.ValidationOptions{StepKinds: registry}); len(findings) != 0 {
		t.Fatalf("pure durability none diagnostics = %#v", findings)
	}

	cases := map[string]func(*graph.Graph){
		"graph target": func(value *graph.Graph) { value.Target.Capabilities = []string{"workspace"} },
		"node target":  func(value *graph.Graph) { value.Nodes[0].Target.Kinds = []string{"remote"} },
		"durable backoff": func(value *graph.Graph) {
			value.Nodes[0].Retry = &graph.RetryPolicy{Attempts: 2, Backoff: graph.BackoffPolicy{Strategy: graph.BackoffFixed, InitialDelay: "1s"}}
		},
		"incoherent override": func(value *graph.Graph) {
			value.Nodes[0].Durability = &graph.DurabilitySpec{Mode: graph.DurabilitySteps}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			value := base
			value.Nodes = append([]graph.Node(nil), base.Nodes...)
			mutate(&value)
			findings := workflowcompile.ValidateGraph(t.Context(), value, workflowcompile.ValidationOptions{StepKinds: registry})
			found := false
			for _, finding := range findings {
				found = found || finding.Code == workflowcompile.CodeInvalidDurability
			}
			if !found {
				t.Fatalf("diagnostics = %#v, want %s", findings, workflowcompile.CodeInvalidDurability)
			}
		})
	}
}

func TestCompileCanonicalContinueAsNewPolicy(t *testing.T) {
	plan := compileBytes(t, "reactor.workflow.yaml", []byte(`workflow:
  name: Reactor
  version: v1
  provenance:
    authority: project
on:
  event:
    type: project.changed
    source: project://fixture
inputs:
  - name: cursor
    type: string
outputs:
  cursor:
    type: string
    value: inputs.cursor
durability:
  mode: steps
  continue_as_new:
    max_events: 25
    carry: [cursor]
steps:
  - name: work
    kind: noop
`))
	if plan.Graph.Durability == nil || plan.Graph.Durability.Mode != graph.DurabilitySteps || plan.Graph.Durability.Extension.Version != "reactor/v1" || plan.Graph.Durability.Extension.Config["max_events"] != 25 {
		t.Fatalf("durability = %#v", plan.Graph.Durability)
	}
	if got := plan.Graph.Durability.Extension.Config["carry"].([]string); len(got) != 1 || got[0] != "cursor" {
		t.Fatalf("carry = %#v", got)
	}
	if encoded := string(stableJSON(t, plan)); !strings.Contains(encoded, `"version": "reactor/v1"`) {
		t.Fatalf("plan does not contain canonical extension: %s", encoded)
	}
}

func TestCompileContinueAsNewMaxEventsBoundsAllNamedWaitDeliveries(t *testing.T) {
	loaded := workflowcompile.LoadBytes("reactor-capacity.workflow.yaml", []byte(`workflow:
  name: Reactor Capacity
  version: v1
  provenance:
    authority: project
on:
  event:
    type: project.changed
    source: project://fixture
durability:
  mode: steps
  continue_as_new:
    max_events: 2
steps:
  - name: first
    kind_version: v1
    wait_for:
      event:
        type: project.changed
      correlation: project-42
      timeout: 1h
  - name: second
    kind_version: v1
    needs: [first]
    wait_for:
      signal: project.changed
      correlation: project-42
      timeout: 1h
`))
	result := workflowcompile.Compile(loaded.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile() = %#v", result)
	}
	findings := workflowcompile.ValidateGraph(t.Context(), result.Plan.Graph, workflowcompile.ValidationOptions{})
	found := false
	for _, finding := range findings {
		found = found || finding.Code == workflowcompile.CodeInvalidDurability && strings.Contains(finding.Message, "maximum 3 source delivery consumptions")
	}
	if !found {
		t.Fatalf("ValidateGraph() = %#v", findings)
	}
}

func TestCompileContinueAsNewRejectsDeliveryFreeAndReferenceCarryPlans(t *testing.T) {
	for name, source := range map[string]string{
		"no named wait": `workflow:
  name: Delivery Free Reactor
  version: v1
  provenance:
    authority: project
on:
  event:
    type: project.changed
durability:
  mode: steps
  continue_as_new:
    max_events: 1
steps:
  - name: work
    kind: noop
`,
		"artifact carry": `workflow:
  name: Reference Carry Reactor
  version: v1
  provenance:
    authority: project
on:
  event:
    type: project.changed
inputs:
  - name: cursor
    type: artifact
outputs:
  cursor:
    type: artifact
    value: inputs.cursor
durability:
  mode: steps
  continue_as_new:
    max_events: 2
    carry: [cursor]
steps:
  - name: next
    kind_version: v1
    wait_for:
      signal: project.changed
      correlation: project-42
      timeout: 1h
`,
	} {
		t.Run(name, func(t *testing.T) {
			loaded := workflowcompile.LoadBytes(name+".workflow.yaml", []byte(source))
			result := workflowcompile.Compile(loaded.Source)
			if result.Plan == nil || len(result.Diagnostics) != 0 {
				t.Fatalf("Compile() = %#v", result)
			}
			findings := workflowcompile.ValidateGraph(t.Context(), result.Plan.Graph, workflowcompile.ValidationOptions{})
			found := false
			for _, finding := range findings {
				found = found || finding.Code == workflowcompile.CodeInvalidDurability &&
					(strings.Contains(finding.Message, "at least one bounded named signal/event wait") || strings.Contains(finding.Message, "artifact or secret reference"))
			}
			if !found {
				t.Fatalf("ValidateGraph() = %#v", findings)
			}
		})
	}
}

func TestCompileContinueAsNewRequiresNonDefaultRequiredInputsInCarry(t *testing.T) {
	plan := compileBytes(t, "reactor-required-input.workflow.yaml", []byte(`workflow:
  name: Required Input Reactor
  version: v1
  provenance:
    authority: project
on:
  event:
    type: project.changed
inputs:
  - name: cursor
    type: string
    required: true
  - name: defaulted
    type: string
    required: true
    default: {literal: fallback}
  - name: optional
    type: string
outputs:
  cursor:
    type: string
    value: inputs.cursor
durability:
  mode: steps
  continue_as_new:
    max_events: 2
    carry: [cursor]
steps:
  - name: next
    kind_version: v1
    wait_for:
      event:
        type: project.changed
      correlation: project-42
      timeout: 1h
`))
	findings := workflowcompile.ValidateGraph(t.Context(), plan.Graph, workflowcompile.ValidationOptions{})
	for _, finding := range findings {
		if finding.Code == workflowcompile.CodeInvalidDurability {
			t.Fatalf("carried/defaulted/optional input diagnostics = %#v", findings)
		}
	}

	missing := plan.Graph
	durability := *missing.Durability
	extension := durability.Extension
	extension.Config = graph.Config{"max_events": 2, "carry": []string(nil)}
	durability.Extension = extension
	missing.Durability = &durability
	findings = workflowcompile.ValidateGraph(t.Context(), missing, workflowcompile.ValidationOptions{})
	found := false
	for _, finding := range findings {
		found = found || finding.Code == workflowcompile.CodeInvalidDurability && strings.Contains(finding.Message, `required workflow input "cursor" must be carried`)
	}
	if !found {
		t.Fatalf("missing required carry diagnostics = %#v", findings)
	}

	unsafeDefaults := []struct {
		name    string
		binding graph.Binding
	}{
		{name: "expression", binding: graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: `inputs.cursor`}}},
		{name: "interpolation", binding: graph.Binding{Kind: graph.BindingInterpolation, Interpolation: `{{ inputs.cursor }}`}},
		{name: "schema mismatch", binding: graph.Binding{Kind: graph.BindingLiteral, Literal: 42}},
	}
	for _, input := range []struct {
		index int
		name  string
	}{{index: 1, name: "defaulted"}, {index: 2, name: "optional"}} {
		for _, unsafe := range unsafeDefaults {
			t.Run(input.name+" "+unsafe.name+" default", func(t *testing.T) {
				candidate := plan.Graph
				candidate.Inputs = append([]graph.InputSpec(nil), plan.Graph.Inputs...)
				binding := unsafe.binding
				candidate.Inputs[input.index].Default = &binding
				candidateFindings := workflowcompile.ValidateGraph(t.Context(), candidate, workflowcompile.ValidationOptions{})
				found := false
				for _, finding := range candidateFindings {
					found = found || finding.Code == workflowcompile.CodeInvalidDurability && strings.Contains(finding.Message, `workflow input "`+input.name+`" has a continuation-unsafe default`)
				}
				if !found {
					t.Fatalf("unsafe default diagnostics = %#v", candidateFindings)
				}
			})
		}
	}
}
