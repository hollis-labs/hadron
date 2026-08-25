package appworkflow

import (
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
)

func TestWorkflowToolNameIsBoundedASCIIAndCollisionsRemainVisible(t *testing.T) {
	for _, input := range []string{"team/review/αβ", strings.Repeat("long-segment/", 40)} {
		name := workflowToolName(input)
		if len(name) > MaximumWorkflowToolNameBytes {
			t.Fatalf("tool name length = %d", len(name))
		}
		for _, current := range name {
			if current >= 'A' && current <= 'Z' || current >= 'a' && current <= 'z' || current >= '0' && current <= '9' || current == '_' || current == '-' {
				continue
			}
			t.Fatalf("tool name %q contains non-ASCII protocol rune %q", name, current)
		}
	}
	if workflowToolName("team/a_b") != workflowToolName("team/a/b") {
		t.Fatal("collision fixture stopped exercising atomic collision refusal")
	}
}

func TestWorkflowIOSchemaRejectsNonliteralDefault(t *testing.T) {
	inputs := []graph.InputSpec{{
		Name: "unsafe", Schema: graph.Schema{"type": "string"},
		Default: &graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.other"}},
	}}
	if _, err := workflowIOSchema(inputs); err == nil {
		t.Fatal("nonliteral workflow default was published as an MCP JSON Schema default")
	}
}
