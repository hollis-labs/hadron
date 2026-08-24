package compile_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

type testExpander struct {
	name   string
	handle bool
	mutate bool
	result workflowcompile.NodeExpansion
}

func (e *testExpander) Name() string { return e.name }

func (e *testExpander) ExpandNode(request workflowcompile.NodeExpansionRequest) (workflowcompile.NodeExpansion, bool, []diagnostic.Diagnostic) {
	if e.mutate {
		if request.Node.Config == nil {
			request.Node.Config = graph.Config{}
		}
		request.Node.Config["mutated"] = true
		request.Graph.Nodes[0].ID = "mutated"
	}
	if request.Node.Kind != "sugar" || !e.handle {
		return workflowcompile.NodeExpansion{}, false, nil
	}
	return e.result, true, nil
}

func TestCompileWithOptionsSelectsExpandersDeterministicallyAndOwnsCopies(t *testing.T) {
	loaded := workflowcompile.LoadBytes("expand.workflow.yaml", []byte(`workflow: {id: expanded, version: v1}
steps:
  - id: prepare
    kind: noop
  - id: sugar
    needs: [prepare]
    kind: sugar
  - id: consume
    needs: [sugar]
    kind: noop
`))
	if len(loaded.Diagnostics) != 0 {
		t.Fatal(loaded.Diagnostics)
	}
	mutable := graph.Config{"large": json.Number("9007199254740993")}
	handler := &testExpander{name: "b-handler", handle: true, result: workflowcompile.NodeExpansion{
		EntryNodeID: "sugar-entry", ExitNodeID: "sugar",
		Nodes: []graph.Node{
			{ID: "sugar-entry", Kind: "noop", Config: mutable},
			{ID: "sugar", Kind: "noop"},
		},
		Edges: []graph.Edge{{From: "sugar-entry", To: "sugar", Kind: graph.EdgeData}},
	}}
	mutator := &testExpander{name: "a-unhandled", mutate: true}
	first := workflowcompile.CompileWithOptions(loaded.Source, workflowcompile.CompileOptions{NodeExpanders: []workflowcompile.NodeExpander{handler, mutator}})
	second := workflowcompile.CompileWithOptions(loaded.Source, workflowcompile.CompileOptions{NodeExpanders: []workflowcompile.NodeExpander{mutator, handler}})
	if first.Plan == nil || second.Plan == nil || !reflect.DeepEqual(first.Plan, second.Plan) {
		t.Fatalf("deterministic compile = %#v / %#v", first, second)
	}
	mutable["large"] = "mutated-after-return"
	entry := expansionNodeByID(t, first.Plan.Graph, "sugar-entry")
	if number, ok := entry.Config["large"].(json.Number); !ok || number.String() != "9007199254740993" {
		t.Fatalf("expander output was not defensively copied: %#v", entry.Config)
	}
	if !hasExpansionEdge(first.Plan.Graph.Edges, "prepare", "sugar-entry", graph.EdgeControl) ||
		!hasExpansionEdge(first.Plan.Graph.Edges, "sugar-entry", "sugar", graph.EdgeData) ||
		!hasExpansionEdge(first.Plan.Graph.Edges, "sugar", "consume", graph.EdgeControl) {
		t.Fatalf("entry/exit edge rewrite = %#v", first.Plan.Graph.Edges)
	}
	if source := first.Plan.SourceMap.Nodes["sugar-entry"]; !slices.Equal(source.Path, []string{"steps", "1"}) {
		t.Fatalf("replacement source map = %#v", source)
	}
}

func TestCompileWithOptionsRejectsAmbiguousOrCollidingExpansion(t *testing.T) {
	loaded := workflowcompile.LoadBytes("expand.workflow.yaml", []byte(`workflow: {id: expanded, version: v1}
steps:
  - id: occupied
    kind: noop
  - id: sugar
    kind: sugar
`))
	base := workflowcompile.NodeExpansion{EntryNodeID: "sugar", ExitNodeID: "sugar", Nodes: []graph.Node{{ID: "sugar", Kind: "noop"}}}
	tests := []struct {
		name      string
		expanders []workflowcompile.NodeExpander
		contains  string
	}{
		{"multiple handlers", []workflowcompile.NodeExpander{&testExpander{name: "z", handle: true, result: base}, &testExpander{name: "a", handle: true, result: base}}, "a, z"},
		{"duplicate names", []workflowcompile.NodeExpander{&testExpander{name: "same"}, &testExpander{name: "same"}}, "registered more than once"},
		{"collision", []workflowcompile.NodeExpander{&testExpander{name: "one", handle: true, result: workflowcompile.NodeExpansion{EntryNodeID: "occupied", ExitNodeID: "occupied", Nodes: []graph.Node{{ID: "occupied", Kind: "noop"}}}}}, "collides"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := workflowcompile.CompileWithOptions(loaded.Source, workflowcompile.CompileOptions{NodeExpanders: test.expanders})
			if result.Plan != nil || len(result.Diagnostics) == 0 || result.Diagnostics[0].Code != workflowcompile.CodeNodeExpansion || !strings.Contains(result.Diagnostics[0].Message, test.contains) {
				t.Fatalf("CompileWithOptions = %#v", result)
			}
		})
	}
	var typedNil *testExpander
	result := workflowcompile.CompileWithOptions(loaded.Source, workflowcompile.CompileOptions{NodeExpanders: []workflowcompile.NodeExpander{typedNil}})
	if result.Plan != nil || len(result.Diagnostics) != 1 || !strings.Contains(result.Diagnostics[0].Message, "must not be nil") {
		t.Fatalf("typed-nil expander = %#v", result)
	}
}

func expansionNodeByID(t *testing.T, value graph.Graph, id string) graph.Node {
	t.Helper()
	for _, node := range value.Nodes {
		if node.ID == id {
			return node
		}
	}
	t.Fatalf("node %q missing", id)
	return graph.Node{}
}

func hasExpansionEdge(edges []graph.Edge, from, to string, kind graph.EdgeKind) bool {
	for _, edge := range edges {
		if edge.From == from && edge.To == to && edge.Kind == kind {
			return true
		}
	}
	return false
}
