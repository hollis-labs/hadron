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

func TestSemanticPlanFingerprintStripsBundledBindingLocationsButNotSourceDigests(t *testing.T) {
	base := ExecutionPlan{
		SchemaVersion: ExecutionPlanSchemaVersion,
		ID:            "parent",
		Definition:    graph.DefinitionRef{Kind: "workflow", ID: "parent", Version: "v1", Digest: sourceDigest([]byte("parent-source-a"))},
		SourceDigests: []SourceDigest{{Format: graph.SourceWorkflow, Digest: sourceDigest([]byte("parent-source-a"))}},
		Graph:         graph.Graph{ID: "parent", Version: "v1", Digest: sourceDigest([]byte("parent-graph")), Nodes: []graph.Node{{ID: "call", Kind: "call"}}},
		BundledDefinitions: []ResolvedDefinition{{
			Definition: graph.DefinitionRef{Authority: "generated", Kind: "workflow", ID: "child", Locator: "generated:child-a", Version: "v1", Digest: sourceDigest([]byte("child-source-a"))},
			Graph:      graph.Graph{ID: "child", Version: "v1", Digest: sourceDigest([]byte("child")), Nodes: []graph.Node{{ID: "work", Kind: "noop"}}},
			InputBindings: map[string]graph.Binding{
				"value": {Kind: graph.BindingLiteral, Literal: "one", Source: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "child-a.workflow.yaml", Path: []string{"steps", "0", "with", "value"}, StartLine: 4}},
			},
		}},
	}
	relocated := base
	relocated.Definition.Digest = sourceDigest([]byte("parent-source-b"))
	relocated.SourceDigests = []SourceDigest{{Format: graph.SourceWorkflow, Digest: sourceDigest([]byte("parent-source-b"))}}
	relocated.BundledDefinitions = append([]ResolvedDefinition(nil), base.BundledDefinitions...)
	relocated.BundledDefinitions[0].Definition = base.BundledDefinitions[0].Definition
	relocated.BundledDefinitions[0].Definition.Locator = "generated:child-b"
	relocated.BundledDefinitions[0].Definition.Digest = sourceDigest([]byte("child-source-b"))
	relocated.BundledDefinitions[0].InputBindings = map[string]graph.Binding{
		"value": {Kind: graph.BindingLiteral, Literal: "one", Source: &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "relocated/child-b.workflow.yaml", Path: []string{"definitions", "child", "inputs", "value"}, StartLine: 40}},
	}

	baseDigest, err := PlanDigest(base)
	if err != nil {
		t.Fatal(err)
	}
	relocatedDigest, err := PlanDigest(relocated)
	if err != nil {
		t.Fatal(err)
	}
	if baseDigest == relocatedDigest {
		t.Fatal("real plan digest did not retain exact source identity")
	}
	baseFingerprint, err := FingerprintPlanSemantics(base)
	if err != nil {
		t.Fatal(err)
	}
	relocatedFingerprint, err := FingerprintPlanSemantics(relocated)
	if err != nil {
		t.Fatal(err)
	}
	if baseFingerprint != relocatedFingerprint {
		t.Fatalf("relocated bundled binding changed semantic fingerprint: %#v vs %#v", baseFingerprint, relocatedFingerprint)
	}
}
