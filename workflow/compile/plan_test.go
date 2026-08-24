package compile_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

const representativeLocator = "testdata/representative.workflow.yaml"

func TestCompileRepresentativeWorkflow(t *testing.T) {
	plan := representativePlan(t)
	if plan.SchemaVersion != workflowcompile.ExecutionPlanSchemaVersion || plan.ID != "release-plan" {
		t.Fatalf("plan identity = version %q id %q", plan.SchemaVersion, plan.ID)
	}
	if plan.Definition.ID != "release-plan" || plan.Definition.Kind != "workflow" || plan.Definition.Version != "2.1.0" || plan.Definition.Locator != "" {
		t.Fatalf("definition = %+v", plan.Definition)
	}
	if plan.Provenance.Locator != representativeLocator || plan.Provenance.Authority != "project" || plan.Provenance.Digest != plan.SourceDigests[0].Digest {
		t.Fatalf("provenance = %+v", plan.Provenance)
	}
	if plan.Graph.ID != "release-plan" || plan.Graph.Namespace != "example-org" || plan.Graph.Version != "2.1.0" {
		t.Fatalf("graph identity = %q %q %q", plan.Graph.ID, plan.Graph.Namespace, plan.Graph.Version)
	}
	if len(plan.Graph.Inputs) != 2 || plan.Graph.Inputs[0].Name != "project-id" || plan.Graph.Inputs[1].Name != "tasks" {
		t.Fatalf("inputs = %+v", plan.Graph.Inputs)
	}
	if got := plan.Graph.Inputs[1].Schema; !reflect.DeepEqual(got, graph.Schema{"type": "array", "items": map[string]any{"type": "object"}}) {
		t.Fatalf("array shorthand schema = %#v", got)
	}
	if len(plan.Graph.Outputs) != 1 || plan.Graph.Outputs[0].Value == nil || plan.Graph.Outputs[0].Value.Kind != graph.BindingExpression {
		t.Fatalf("workflow outputs = %+v", plan.Graph.Outputs)
	}

	wantKinds := []string{"cmd", "mcp", "transform", "custom-summary", "call", "http"}
	if len(plan.Graph.Nodes) != len(wantKinds) {
		t.Fatalf("len(nodes) = %d, want %d", len(plan.Graph.Nodes), len(wantKinds))
	}
	for i, want := range wantKinds {
		if plan.Graph.Nodes[i].Kind != want {
			t.Errorf("nodes[%d].kind = %q, want %q", i, plan.Graph.Nodes[i].Kind, want)
		}
	}
	create := plan.Graph.Nodes[1]
	if create.ID != "create-items" || create.DisplayName != "Create Items" {
		t.Fatalf("normalized/display node identity = %q/%q", create.ID, create.DisplayName)
	}
	if create.ForEach == nil || create.ForEach.Items.Text != "inputs.tasks" || create.ForEach.MaxConcurrency != 4 {
		t.Fatalf("for_each = %+v", create.ForEach)
	}
	if create.Config["server"] != "torque" || create.Config["tool"] != "torque_task_create" {
		t.Fatalf("mcp_call config = %#v", create.Config)
	}
	if create.InputBindings["project"].Kind != graph.BindingExpression ||
		create.InputBindings["dry-run"].Kind != graph.BindingLiteral ||
		create.InputBindings["title"].Kind != graph.BindingInterpolation {
		t.Fatalf("typed bindings = %#v", create.InputBindings)
	}
	if create.Retry == nil || create.Retry.Attempts != 3 || create.Retry.Backoff.Strategy != graph.BackoffExponential ||
		create.Idempotency == nil || create.Idempotency.Mode != graph.IdempotencyKeyed {
		t.Fatalf("retry/idempotency = %+v / %+v", create.Retry, create.Idempotency)
	}
	if create.Timeout == nil || create.Timeout.ScheduleToClose != "5m" || len(create.Catch) != 1 {
		t.Fatalf("timeout/catch = %+v / %+v", create.Timeout, create.Catch)
	}
	if plan.Graph.Nodes[2].Finally == nil || !slices.Equal(plan.Graph.Nodes[2].Finally.Scope, []string{"create-items"}) {
		t.Fatalf("node finally = %+v", plan.Graph.Nodes[2].Finally)
	}
	if plan.Graph.Nodes[3].Switch == nil || len(plan.Graph.Nodes[3].Switch.Arms) != 1 {
		t.Fatalf("switch = %+v", plan.Graph.Nodes[3].Switch)
	}
	call := plan.Graph.Nodes[4].Call
	if call == nil || call.Definition.ID != "publisher" || call.Definition.Locator != "registry://workflows/publisher" || call.Mode != graph.CallRun {
		t.Fatalf("call = %+v", call)
	}
	if plan.Graph.Nodes[5].Finally == nil || plan.Graph.Nodes[5].Config["method"] != "DELETE" {
		t.Fatalf("root finally/http_call = %+v", plan.Graph.Nodes[5])
	}
	if len(plan.Graph.Edges) != 4 {
		t.Fatalf("explicit edges = %+v", plan.Graph.Edges)
	}
	for _, edge := range plan.Graph.Edges {
		if edge.Source == nil {
			t.Fatalf("edge lacks source: %+v", edge)
		}
	}
	if _, inferred := plan.SourceMap.Edges[workflowcompile.EdgeSourceKey("create-items", "summarize", graph.EdgeData)]; inferred {
		t.Fatal("compiler inferred a hidden data edge")
	}
}

func TestCompileSourceMapsExactDeclarations(t *testing.T) {
	plan := representativePlan(t)
	tests := []struct {
		name   string
		ref    *graph.SourceRef
		path   []string
		line   int
		column int
	}{
		{name: "graph", ref: plan.SourceMap.Graph, path: []string{"workflow"}, line: 2, column: 3},
		{name: "graph root carrier", ref: plan.Graph.Source, path: []string{"workflow"}, line: 2, column: 3},
		{name: "input", ref: plan.Graph.Inputs[0].Source, path: []string{"inputs", "0"}, line: 14, column: 5},
		{name: "output", ref: plan.Graph.Outputs[0].Source, path: []string{"outputs", "summary"}, line: 27, column: 5},
		{name: "output binding", ref: plan.Graph.Outputs[0].Value.Source, path: []string{"outputs", "summary", "value"}, line: 29, column: 7},
		{name: "output expression", ref: plan.Graph.Outputs[0].Value.Expression.Source, path: []string{"outputs", "summary", "value", "expression"}, line: 29, column: 19},
		{name: "node", ref: plan.Graph.Nodes[1].Source, path: []string{"steps", "1"}, line: 46, column: 5},
		{name: "need", ref: plan.Graph.Nodes[1].Needs[0].Source, path: []string{"steps", "1", "needs", "0"}, line: 48, column: 9},
		{name: "edge", ref: plan.Graph.Edges[0].Source, path: []string{"steps", "1", "needs", "0"}, line: 48, column: 9},
		{name: "if", ref: plan.Graph.Nodes[1].If.Source, path: []string{"steps", "1", "if"}, line: 51, column: 9},
		{name: "for_each", ref: plan.Graph.Nodes[1].ForEach.Items.Source, path: []string{"steps", "1", "for_each"}, line: 52, column: 15},
		{name: "with binding", ref: plan.Graph.Nodes[1].InputBindings["project"].Source, path: []string{"steps", "1", "with", "project"}, line: 61, column: 16},
		{name: "idempotency", ref: plan.Graph.Nodes[1].Idempotency.Key.Source, path: []string{"steps", "1", "retry", "idempotency_key"}, line: 78, column: 24},
		{name: "catch", ref: plan.Graph.Nodes[1].Catch[0].Source, path: []string{"steps", "1", "catch", "0"}, line: 82, column: 9},
		{name: "catch when", ref: plan.Graph.Nodes[1].Catch[0].When.Source, path: []string{"steps", "1", "catch", "0", "when"}, line: 83, column: 15},
		{name: "switch", ref: plan.Graph.Nodes[3].Switch.Arms[0].Source, path: []string{"steps", "3", "switch", "arms", "0"}, line: 110, column: 11},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.ref == nil {
				t.Fatal("source reference is nil")
			}
			if test.ref.Format != graph.SourceWorkflow || test.ref.Locator != representativeLocator ||
				test.ref.StartLine != test.line || test.ref.StartColumn != test.column || !slices.Equal(test.ref.Path, test.path) {
				t.Fatalf("source reference = %+v, want %s:%d:%d path %v", test.ref, representativeLocator, test.line, test.column, test.path)
			}
		})
	}
	if !reflect.DeepEqual(plan.SourceMap, plan.Graph.SourceMap) {
		t.Fatal("plan and graph full source maps diverge")
	}
	if got := plan.SourceMap.Nodes["create-items"]; !slices.Equal(got.Path, []string{"steps", "1"}) {
		t.Fatalf("compact node source key = %+v", got)
	}
	edgeKey := workflowcompile.EdgeSourceKey("prepare-request", "create-items", graph.EdgeData)
	if got := plan.SourceMap.Edges[edgeKey]; !slices.Equal(got.Path, []string{"steps", "1", "needs", "0"}) {
		t.Fatalf("compact edge source key = %+v", got)
	}
}

func TestCompileDigestsAreStableSensitiveAndRelocatable(t *testing.T) {
	data, err := os.ReadFile(representativeLocator)
	if err != nil {
		t.Fatal(err)
	}
	first := compileBytes(t, "/first/representative.workflow.yaml", data)
	second := compileBytes(t, "/second/representative.workflow.yaml", data)
	if first.Digest != second.Digest || first.Graph.Digest != second.Graph.Digest || first.SourceDigests[0] != second.SourceDigests[0] {
		t.Fatalf("relocation changed digests:\nfirst:  %+v\nsecond: %+v", first, second)
	}
	if first.Provenance.Locator == second.Provenance.Locator || first.SourceMap.Graph.Locator == second.SourceMap.Graph.Locator {
		t.Fatal("relocation-sensitive provenance/source map did not retain locator")
	}

	repeated := compileBytes(t, "/first/representative.workflow.yaml", data)
	if !reflect.DeepEqual(first, repeated) {
		t.Fatal("repeated compile was not deterministic")
	}

	commented := compileBytes(t, "/first/representative.workflow.yaml", append(append([]byte(nil), data...), []byte("\n# relevant source revision\n")...))
	if commented.SourceDigests[0].Digest == first.SourceDigests[0].Digest || commented.Digest == first.Digest {
		t.Fatal("source-only change did not change source and plan digests")
	}
	if commented.Graph.Digest != first.Graph.Digest {
		t.Fatal("source-only comment changed semantic graph digest")
	}

	changedData := bytes.Replace(data, []byte("source: adapter-semantic-value"), []byte("source: adapter-semantic-change"), 1)
	changed := compileBytes(t, "/first/representative.workflow.yaml", changedData)
	if changed.SourceDigests[0].Digest == first.SourceDigests[0].Digest || changed.Graph.Digest == first.Graph.Digest || changed.Digest == first.Digest {
		t.Fatal("semantic source change did not change all relevant digests")
	}
}

func TestCompilePreservesLargeJSONNumbersInSemanticDigests(t *testing.T) {
	const source = `workflow:
  name: precise-number
steps:
  - name: Preserve Number
    transform:
      large: %s
`
	first := compileBytes(t, "first.workflow.yaml", []byte(fmt.Sprintf(source, "9007199254740992")))
	second := compileBytes(t, "second.workflow.yaml", []byte(fmt.Sprintf(source, "9007199254740993")))

	for _, test := range []struct {
		name string
		plan workflowcompile.ExecutionPlan
		want string
	}{
		{name: "first", plan: first, want: "9007199254740992"},
		{name: "second", plan: second, want: "9007199254740993"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := test.plan.Graph.Nodes[0].Config["large"].(json.Number)
			if !ok || got.String() != test.want {
				t.Fatalf("config large = %#v (%T), want json.Number(%s)", test.plan.Graph.Nodes[0].Config["large"], test.plan.Graph.Nodes[0].Config["large"], test.want)
			}
		})
	}
	if first.Graph.Digest == second.Graph.Digest {
		t.Fatalf("adjacent large integers produced the same graph digest %q", first.Graph.Digest)
	}
}

func TestExplicitInterpolationPreservesLiteralWhitespaceThroughJSONRoundTrip(t *testing.T) {
	const source = `workflow:
  name: whitespace
steps:
  - name: Interpolate
    kind: custom
    with:
      message:
        interpolation: "  hello {{ inputs.name }}  "
`
	const want = "  hello {{ inputs.name }}  "
	plan := compileBytes(t, "whitespace.workflow.yaml", []byte(source))
	if got := plan.Graph.Nodes[0].InputBindings["message"].Interpolation; got != want {
		t.Fatalf("lowered interpolation = %q, want %q", got, want)
	}

	encoded := stableJSON(t, plan)
	var decoded workflowcompile.ExecutionPlan
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := decoded.Graph.Nodes[0].InputBindings["message"].Interpolation; got != want {
		t.Fatalf("round-tripped interpolation = %q, want %q", got, want)
	}
}

func TestExecutionPlanJSONRoundTripAndSnapshots(t *testing.T) {
	plan := representativePlan(t)
	encoded := stableJSON(t, plan)
	assertSnapshot(t, "representative.plan.json", encoded)
	assertSnapshot(t, "representative.source-map.json", stableJSON(t, plan.SourceMap))

	var decoded workflowcompile.ExecutionPlan
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("json.Unmarshal(ExecutionPlan) = %v", err)
	}
	if remarshal := stableJSON(t, decoded); !bytes.Equal(encoded, remarshal) {
		t.Fatalf("ExecutionPlan JSON roundtrip changed bytes")
	}
}

func TestRuntimeSemanticFieldsExcludeOriginalLocator(t *testing.T) {
	const locator = "/unique/source/location/representative.workflow.yaml"
	data, err := os.ReadFile(representativeLocator)
	if err != nil {
		t.Fatal(err)
	}
	plan := compileBytes(t, locator, data)
	encoded := stableJSON(t, plan)
	var document any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	removeLocationEnvelopes(document)
	semantic := stableJSON(t, document)
	if bytes.Contains(semantic, []byte(locator)) {
		t.Fatalf("original locator escaped provenance/source-map fields: %s", semantic)
	}
}

func TestLoweringDiagnosticsAreSourceMappedAndDeterministic(t *testing.T) {
	const invalid = `workflow:
  name: invalid
  unsupported: true
steps:
  - name: conflict
    cmd: echo one
    transform: {value: one}
  - name: ambiguous-binding
    kind: custom
    with:
      value:
        expression: inputs.value
        literal: fallback
`
	loaded := workflowcompile.LoadBytes("invalid.workflow.yaml", []byte(invalid))
	assertLoaded(t, loaded, "invalid.workflow.yaml")
	first := workflowcompile.Compile(loaded.Source)
	second := workflowcompile.Compile(loaded.Source)
	if first.Plan != nil || !reflect.DeepEqual(first.Diagnostics, second.Diagnostics) {
		t.Fatalf("lowering diagnostics are not deterministic: %#v / %#v", first, second)
	}
	want := []struct {
		code diagnostic.Code
		path []string
		line int
	}{
		{workflowcompile.CodeUnsupportedSourceField, []string{"workflow", "unsupported"}, 3},
		{workflowcompile.CodeInvalidWorkflowShape, []string{"steps", "0", "transform"}, 7},
		{workflowcompile.CodeInvalidBindingSource, []string{"steps", "1", "with", "value"}, 12},
	}
	if len(first.Diagnostics) != len(want) {
		t.Fatalf("diagnostics = %#v, want %d", first.Diagnostics, len(want))
	}
	for i, expected := range want {
		d := first.Diagnostics[i]
		if d.Code != expected.code || d.Source == nil || !slices.Equal(d.Source.Path, expected.path) || d.Source.StartLine != expected.line {
			t.Errorf("diagnostics[%d] = %+v", i, d)
		}
		if err := d.Validate(); err != nil {
			t.Errorf("diagnostics[%d].Validate() = %v", i, err)
		}
	}
}

func TestMissingWorkflowMarkerIsLoweringDiagnostic(t *testing.T) {
	loaded := workflowcompile.LoadBytes("missing.workflow.yaml", []byte("steps: []\n"))
	assertLoaded(t, loaded, "missing.workflow.yaml")
	result := workflowcompile.Compile(loaded.Source)
	if result.Plan != nil || len(result.Diagnostics) != 1 || result.Diagnostics[0].Code != workflowcompile.CodeInvalidWorkflowShape {
		t.Fatalf("Compile() = %+v", result)
	}
	if err := result.Diagnostics[0].Validate(); err != nil {
		t.Fatalf("diagnostic.Validate() = %v", err)
	}
}

func TestCompileDoesNotEvaluateExpressionsOrResolveDefinitions(t *testing.T) {
	const source = `workflow:
  name: inert
steps:
  - name: Child
    kind: call
    if: "not valid expression ("
    with:
      value:
        expression: missing.reference(
    call:
      definition:
        locator: does-not-exist.workflow.yaml
      mode: inline
`
	plan := compileBytes(t, "inert.workflow.yaml", []byte(source))
	node := plan.Graph.Nodes[0]
	if node.If == nil || node.If.Text != "not valid expression (" ||
		node.InputBindings["value"].Expression.Text != "missing.reference(" ||
		node.Call == nil || node.Call.Definition.Locator != "does-not-exist.workflow.yaml" || node.Call.Definition.Digest != "" {
		t.Fatalf("compiler interpreted or resolved inert source: %+v", node)
	}
	if len(plan.Graph.Edges) != 0 {
		t.Fatalf("compiler inferred edges from inert expressions: %+v", plan.Graph.Edges)
	}
}

func representativePlan(t *testing.T) workflowcompile.ExecutionPlan {
	t.Helper()
	loaded, err := workflowcompile.LoadFile(representativeLocator)
	if err != nil {
		t.Fatal(err)
	}
	assertLoaded(t, loaded, representativeLocator)
	result := workflowcompile.Compile(loaded.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile() = %+v", result)
	}
	return *result.Plan
}

func compileBytes(t *testing.T, locator string, data []byte) workflowcompile.ExecutionPlan {
	t.Helper()
	loaded := workflowcompile.LoadBytes(locator, data)
	assertLoaded(t, loaded, locator)
	result := workflowcompile.Compile(loaded.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile() = %+v", result)
	}
	return *result.Plan
}

func stableJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}

func assertSnapshot(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", "snapshots", name)
	if os.Getenv("UPDATE_WORKFLOW_SNAPSHOTS") == "1" {
		// #nosec G306 -- snapshots are non-sensitive repository artifacts.
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// #nosec G304 -- the snapshot path is a test-owned constant.
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("snapshot %s is stale; run UPDATE_WORKFLOW_SNAPSHOTS=1 go test ./workflow/compile", name)
	}
}

func removeLocationEnvelopes(value any) {
	switch typed := value.(type) {
	case map[string]any:
		delete(typed, "source")
		delete(typed, "source_map")
		delete(typed, "provenance")
		for _, child := range typed {
			removeLocationEnvelopes(child)
		}
	case []any:
		for _, child := range typed {
			removeLocationEnvelopes(child)
		}
	}
}
