package runtime_test

import (
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
)

func TestParseContinueAsNewRejectsNonConvergentPlansBeforeRecovery(t *testing.T) {
	valid := graph.Graph{Inputs: []graph.InputSpec{
		{Name: "cursor", Required: true, Schema: graph.Schema{"type": "string"}},
		{Name: "defaulted", Required: true, Default: &graph.Binding{Kind: graph.BindingLiteral, Literal: "fallback"}, Schema: graph.Schema{"type": "string"}},
		{Name: "optional", Schema: graph.Schema{"type": "string"}},
	},
		Outputs: []graph.OutputSpec{{Name: "cursor", Schema: graph.Schema{"type": "string"}}},
		Nodes:   []graph.Node{{ID: "next", Kind: "wait_for", Config: graph.Config{"signal": "project.changed"}}},
		Durability: &graph.DurabilitySpec{Mode: graph.DurabilitySteps, Extension: graph.Extension{Version: workflowruntime.ContinueAsNewExtensionVersion,
			Config: graph.Config{"max_events": 2, "carry": []string{"cursor"}}}}}
	if policy, ok, err := workflowruntime.ParseContinueAsNew(valid); err != nil || !ok || policy.MaxEvents != 2 {
		t.Fatalf("valid ParseContinueAsNew = %#v / %v, %v", policy, ok, err)
	}
	missingRequired := valid
	durability := *missingRequired.Durability
	extension := durability.Extension
	extension.Config = graph.Config{"max_events": 2, "carry": []string(nil)}
	durability.Extension = extension
	missingRequired.Durability = &durability
	if _, ok, err := workflowruntime.ParseContinueAsNew(missingRequired); err == nil || !ok || !strings.Contains(err.Error(), `required workflow input "cursor" is not carried`) {
		t.Fatalf("missing-required-input ParseContinueAsNew = ok %v, err %v", ok, err)
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
				candidate := valid
				candidate.Inputs = append([]graph.InputSpec(nil), valid.Inputs...)
				binding := unsafe.binding
				candidate.Inputs[input.index].Default = &binding
				if _, ok, err := workflowruntime.ParseContinueAsNew(candidate); err == nil || !ok || !strings.Contains(err.Error(), `workflow input "`+input.name+`" has a continuation-unsafe default`) {
					t.Fatalf("unsafe-default ParseContinueAsNew = ok %v, err %v", ok, err)
				}
			})
		}
	}

	deliveryFree := valid
	deliveryFree.Nodes = nil
	if _, ok, err := workflowruntime.ParseContinueAsNew(deliveryFree); err == nil || !ok {
		t.Fatalf("delivery-free ParseContinueAsNew = ok %v, err %v", ok, err)
	}

	for _, schema := range []graph.Schema{
		{"type": "artifact"},
		{"anyOf": []any{map[string]any{"type": "string"}, map[string]any{"type": "secret_ref"}}},
	} {
		referenceCarry := valid
		referenceCarry.Inputs = []graph.InputSpec{{Name: "cursor", Schema: schema}}
		referenceCarry.Outputs = []graph.OutputSpec{{Name: "cursor", Schema: schema}}
		if _, ok, err := workflowruntime.ParseContinueAsNew(referenceCarry); err == nil || !ok {
			t.Fatalf("reference carry ParseContinueAsNew(%#v) = ok %v, err %v", schema, ok, err)
		}
	}
}
