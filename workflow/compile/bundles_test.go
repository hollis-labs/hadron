package compile

import (
	"reflect"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
)

func TestPlanDigestIncludesBundledDefinitionContentButNotLocations(t *testing.T) {
	base := ExecutionPlan{
		SchemaVersion: ExecutionPlanSchemaVersion,
		ID:            "parent",
		Definition:    graph.DefinitionRef{Kind: "workflow", ID: "parent", Version: "v1", Digest: sourceDigest([]byte("parent"))},
		Graph:         graph.Graph{ID: "parent", Version: "v1", Digest: sourceDigest([]byte("parent-graph")), Nodes: []graph.Node{{ID: "call", Kind: "call"}}},
		BundledDefinitions: []ResolvedDefinition{{
			Definition: graph.DefinitionRef{Authority: "generated", Kind: "workflow", ID: "child", Locator: "generated:child", Version: "v1", Digest: sourceDigest([]byte("child"))},
			Graph:      graph.Graph{ID: "child", Version: "v1", Digest: sourceDigest([]byte("child")), Nodes: []graph.Node{{ID: "work", Kind: "noop"}}},
			InputBindings: map[string]graph.Binding{
				"value": {Kind: graph.BindingLiteral, Literal: "one", Source: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "one.workflow.yaml", StartLine: 4}},
			},
		}},
	}
	first, err := digestPlan(base)
	if err != nil {
		t.Fatal(err)
	}
	relocated := base
	relocated.BundledDefinitions = append([]ResolvedDefinition(nil), base.BundledDefinitions...)
	relocated.BundledDefinitions[0].Definition.Locator = "other-generated-locator"
	relocated.BundledDefinitions[0].InputBindings = map[string]graph.Binding{
		"value": {Kind: graph.BindingLiteral, Literal: "one", Source: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "relocated.workflow.yaml", StartLine: 40}},
	}
	second, err := digestPlan(relocated)
	if err != nil || first != second {
		t.Fatalf("relocation changed bundle digest: %s vs %s, %v", first, second, err)
	}
	changed := relocated
	changed.BundledDefinitions = append([]ResolvedDefinition(nil), relocated.BundledDefinitions...)
	changed.BundledDefinitions[0].InputBindings = map[string]graph.Binding{
		"value": {Kind: graph.BindingLiteral, Literal: "two"},
	}
	third, err := digestPlan(changed)
	if err != nil || third == first {
		t.Fatalf("semantic bundle change did not change plan digest: %s / %s, %v", first, third, err)
	}
	if reflect.DeepEqual(base, changed) {
		t.Fatal("test did not change bundle semantics")
	}
}
