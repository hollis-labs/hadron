package compile_test

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

var activeAcceptanceExamples = []string{
	"http-cmd-transform.workflow.yaml",
	"release-approval-gate.workflow.yaml",
	"torque-task-bulk-create.workflow.yaml",
}

type acceptanceCompilation struct {
	Plan       workflowcompile.ExecutionPlan
	Visibility workflowcompile.ValueVisibilityPlan
	Deferred   []workflowcompile.DeferredDependency
}

func TestEveryActiveWorkflowExampleCompilesInfersAndValidates(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "workflow", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	gotNames := make([]string, len(paths))
	for index, path := range paths {
		gotNames[index] = filepath.Base(path)
	}
	sort.Strings(gotNames)
	if !slices.Equal(gotNames, activeAcceptanceExamples) {
		t.Fatalf("active workflow examples = %v, want %v", gotNames, activeAcceptanceExamples)
	}

	for _, name := range activeAcceptanceExamples {
		t.Run(name, func(t *testing.T) {
			first := compileAcceptanceExample(t, name)
			second := compileAcceptanceExample(t, name)
			if !reflect.DeepEqual(first, second) {
				t.Fatal("repeated compile/inference result was not deterministic")
			}
			if first.Plan.Digest == "" || first.Plan.Graph.Digest == "" || first.Plan.SourceDigests[0].Digest == "" {
				t.Fatalf("compiled digests = plan %q graph %q source %#v", first.Plan.Digest, first.Plan.Graph.Digest, first.Plan.SourceDigests)
			}

			base := name[:len(name)-len(".workflow.yaml")]
			assertSnapshot(t, base+".execution-plan.json", stableJSON(t, first.Plan))
			assertSnapshot(t, base+".source-map.json", stableJSON(t, first.Plan.SourceMap))
		})
	}
}

func TestTorqueBulkCreateAcceptanceContract(t *testing.T) {
	result := compileAcceptanceExample(t, "torque-task-bulk-create.workflow.yaml")
	plan := result.Plan
	if plan.ID != "torque-task-bulk-create" || plan.Graph.Namespace != "torque" || len(plan.Graph.Inputs) != 2 {
		t.Fatalf("workflow identity/inputs = %q %q %#v", plan.ID, plan.Graph.Namespace, plan.Graph.Inputs)
	}
	tasks := inputByName(t, plan.Graph, "tasks")
	items, ok := tasks.Schema["items"].(map[string]any)
	if tasks.Schema["type"] != "array" || !ok || items["type"] != "object" {
		t.Fatalf("tasks schema = %#v", tasks.Schema)
	}

	create := nodeByID(t, plan.Graph, "create")
	if create.Kind != "mcp" || create.ForEach == nil || create.ForEach.Items.Text != "inputs.tasks" || create.ForEach.MaxConcurrency != 4 {
		t.Fatalf("create fan-out = kind %q, for_each %#v", create.Kind, create.ForEach)
	}
	if !slices.Equal(create.Effects, graph.EffectSet{graph.EffectMutate}) || create.Retry == nil || create.Retry.Attempts != 3 ||
		create.Retry.Backoff.Strategy != graph.BackoffExponential || create.Idempotency == nil ||
		create.Idempotency.Mode != graph.IdempotencyKeyed || create.Idempotency.Key == nil ||
		create.Idempotency.Key.Text != `inputs["project-id"] + ":" + item.title` {
		t.Fatalf("create retry/effects/idempotency = %#v / %#v / %#v", create.Effects, create.Retry, create.Idempotency)
	}
	arguments := create.Config["arguments"].(map[string]any)
	if create.Config["server"] != "torque" || create.Config["tool"] != "torque_task_create" ||
		arguments["title"] != "{{ item.title }}" || arguments["project_id"] != "{{ inputs['project-id'] }}" {
		t.Fatalf("MCP config = %#v", create.Config)
	}
	if schemaOfOutput(t, create.Outputs, "result-json")["type"] != "object" ||
		schemaOfOutput(t, create.Outputs, "task-id")["type"] != "string" ||
		outputByName(t, create.Outputs, "task-id").Value == nil {
		t.Fatalf("create outputs = %#v", create.Outputs)
	}

	summarize := nodeByID(t, plan.Graph, "summarize")
	if summarize.Kind != "transform" || summarize.Config["created"] != `map(steps.create.items, .outputs["result-json"].id)` ||
		summarize.Config["failed"] != `filter(steps.create.items, .status == "failed")` {
		t.Fatalf("summary transform = %#v", summarize.Config)
	}
	assertEdge(t, plan.Graph, "create", "summarize", graph.EdgeControl)
	assertEdge(t, plan.Graph, "create", "summarize", graph.EdgeData)
	dataKey := workflowcompile.EdgeSourceKey("create", "summarize", graph.EdgeData)
	if ref, exists := plan.SourceMap.Edges[dataKey]; !exists || !slices.Equal(ref.Path, []string{"steps", "1", "config", "count"}) {
		t.Fatalf("inferred summary edge source = %#v", ref)
	}
	if scope := result.Visibility.Nodes["create"]; !scope.FanOut || len(scope.Producers) != 0 {
		t.Fatalf("create visibility = %#v", scope)
	}
	if scope := result.Visibility.Nodes["summarize"]; scope.FanOut || !slices.Equal(scope.Producers, []string{"create"}) {
		t.Fatalf("summarize visibility = %#v", scope)
	}
	if len(result.Deferred) != 6 {
		t.Fatalf("fan-out deferred references = %#v", result.Deferred)
	}
	wantDeferred := map[workflowcompile.DeferredDependencyReason]int{
		workflowcompile.DeferredFanOutItem:       3,
		workflowcompile.DeferredOptionalProducer: 3,
	}
	for _, deferred := range result.Deferred {
		wantDeferred[deferred.Reason]--
	}
	for reason, remaining := range wantDeferred {
		if remaining != 0 {
			t.Errorf("deferred %s remaining count = %d; got %#v", reason, remaining, result.Deferred)
		}
	}
	for _, output := range []struct {
		name     string
		typeName any
	}{{"created", "array"}, {"failed", "array"}, {"count", "integer"}} {
		if got := schemaOfOutput(t, plan.Graph.Outputs, output.name)["type"]; got != output.typeName {
			t.Errorf("workflow output %s type = %#v", output.name, got)
		}
	}
}

func TestReleaseApprovalGateAcceptanceContract(t *testing.T) {
	result := compileAcceptanceExample(t, "release-approval-gate.workflow.yaml")
	approval := nodeByID(t, result.Plan.Graph, "approval")
	if approval.Kind != "human_gate" || approval.Config["timeout"] != "24h" || schemaOfOutput(t, approval.Outputs, "decision")["type"] != "string" {
		t.Fatalf("approval gate = %#v", approval)
	}
	decisionSchema := schemaOfOutput(t, approval.Outputs, "decision")
	if enum, ok := decisionSchema["enum"].([]any); !ok || !reflect.DeepEqual(enum, []any{"approve", "reject"}) {
		t.Fatalf("decision enum = %#v", decisionSchema["enum"])
	}
	decide := nodeByID(t, result.Plan.Graph, "decide")
	if binding := decide.InputBindings["decision"]; binding.Kind != graph.BindingExpression || binding.Expression == nil ||
		binding.Expression.Text != "steps.approval.outputs.decision" {
		t.Fatalf("typed decision binding = %#v", binding)
	}
	assertEdge(t, result.Plan.Graph, "approval", "decide", graph.EdgeControl)
	assertEdge(t, result.Plan.Graph, "approval", "decide", graph.EdgeData)
	if scope := result.Visibility.Nodes["decide"]; !slices.Equal(scope.Producers, []string{"approval"}) {
		t.Fatalf("decision visibility = %#v", scope)
	}
	if len(result.Deferred) != 0 {
		t.Fatalf("gate deferred dependencies = %#v", result.Deferred)
	}
}

func TestHTTPCommandTransformAcceptanceContract(t *testing.T) {
	result := compileAcceptanceExample(t, "http-cmd-transform.workflow.yaml")
	graphValue := result.Plan.Graph
	if nodeByID(t, graphValue, "fetch").Kind != "http" || nodeByID(t, graphValue, "extract").Kind != "cmd" ||
		nodeByID(t, graphValue, "summarize").Kind != "transform" {
		t.Fatalf("node kinds = %#v", graphValue.Nodes)
	}
	assertEdge(t, graphValue, "fetch", "extract", graph.EdgeControl)
	assertEdge(t, graphValue, "fetch", "extract", graph.EdgeData)
	assertEdge(t, graphValue, "extract", "summarize", graph.EdgeControl)
	assertEdge(t, graphValue, "extract", "summarize", graph.EdgeData)
	assertEdge(t, graphValue, "fetch", "summarize", graph.EdgeData)
	if scope := result.Visibility.Nodes["extract"]; !slices.Equal(scope.Producers, []string{"fetch"}) {
		t.Fatalf("extract visibility = %#v", scope)
	}
	if scope := result.Visibility.Nodes["summarize"]; !slices.Equal(scope.Producers, []string{"extract", "fetch"}) {
		t.Fatalf("summarize visibility = %#v", scope)
	}
	extract := nodeByID(t, graphValue, "extract")
	if schemaOfOutput(t, extract.Outputs, "records")["type"] != "array" {
		t.Fatalf("extract outputs = %#v", extract.Outputs)
	}
	if schemaOfOutput(t, graphValue.Outputs, "count")["type"] != "integer" || schemaOfOutput(t, graphValue.Outputs, "status")["type"] != "integer" {
		t.Fatalf("workflow outputs = %#v", graphValue.Outputs)
	}
}

func TestInvalidAcceptanceExampleDiagnosticSnapshot(t *testing.T) {
	const locator = "testdata/acceptance-invalid.workflow.yaml"
	loaded, err := workflowcompile.LoadFile(locator)
	if err != nil {
		t.Fatal(err)
	}
	assertLoaded(t, loaded, locator)
	first := workflowcompile.Compile(loaded.Source)
	second := workflowcompile.Compile(loaded.Source)
	if first.Plan != nil || !reflect.DeepEqual(first.Diagnostics, second.Diagnostics) || len(first.Diagnostics) != 1 {
		t.Fatalf("Compile() = %#v / %#v", first, second)
	}
	finding := first.Diagnostics[0]
	if finding.Code != workflowcompile.CodeUnsupportedSourceField || finding.Source == nil || finding.Source.StartLine != 12 ||
		finding.Source.StartColumn != 5 || !slices.Equal(finding.Source.Path, []string{"steps", "1", "depends_on"}) {
		t.Fatalf("diagnostic = %#v", finding)
	}
	if err := finding.Validate(); err != nil {
		t.Fatalf("diagnostic.Validate() = %v", err)
	}
	assertSnapshot(t, "acceptance-invalid.diagnostics.json", stableJSON(t, first.Diagnostics))
}

func compileAcceptanceExample(t *testing.T, name string) acceptanceCompilation {
	t.Helper()
	path := filepath.Join("..", "..", "examples", "workflow", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	locator := filepath.ToSlash(filepath.Join("examples", "workflow", name))
	loaded := workflowcompile.LoadBytes(locator, data)
	assertLoaded(t, loaded, locator)
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("Compile(%s) = %#v", name, compiled)
	}
	inferred := workflowcompile.InferValueDependencies(compiled.Plan, workflowcompile.DependencyOptions{})
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
		t.Fatalf("InferValueDependencies(%s) = %#v", name, inferred)
	}
	registry := acceptanceRegistry(t)
	if findings := workflowcompile.ValidatePlan(t.Context(), inferred.Plan, workflowcompile.ValidationOptions{StepKinds: registry}); len(findings) != 0 {
		t.Fatalf("ValidatePlan(%s) = %#v", name, findings)
	}
	return acceptanceCompilation{Plan: *inferred.Plan, Visibility: inferred.Visibility, Deferred: inferred.Deferred}
}

func acceptanceRegistry(t *testing.T) *stepkind.MemoryRegistry {
	t.Helper()
	registry := stepkind.NewRegistry()
	for _, name := range []string{"cmd", "http", "human_gate", "mcp", "transform"} {
		if err := registry.Register(stepkindtest.NewNoopKind(name, "v1")); err != nil {
			t.Fatalf("Register(%s) = %v", name, err)
		}
	}
	return registry
}

func nodeByID(t *testing.T, graphValue graph.Graph, id string) graph.Node {
	t.Helper()
	for _, node := range graphValue.Nodes {
		if node.ID == id {
			return node
		}
	}
	t.Fatalf("node %q not found in %#v", id, graphValue.Nodes)
	return graph.Node{}
}

func inputByName(t *testing.T, graphValue graph.Graph, name string) graph.InputSpec {
	t.Helper()
	for _, input := range graphValue.Inputs {
		if input.Name == name {
			return input
		}
	}
	t.Fatalf("input %q not found in %#v", name, graphValue.Inputs)
	return graph.InputSpec{}
}

func outputByName(t *testing.T, outputs []graph.OutputSpec, name string) graph.OutputSpec {
	t.Helper()
	for _, output := range outputs {
		if output.Name == name {
			return output
		}
	}
	t.Fatalf("output %q not found in %#v", name, outputs)
	return graph.OutputSpec{}
}

func schemaOfOutput(t *testing.T, outputs []graph.OutputSpec, name string) graph.Schema {
	t.Helper()
	return outputByName(t, outputs, name).Schema
}

func assertEdge(t *testing.T, graphValue graph.Graph, from, to string, kind graph.EdgeKind) {
	t.Helper()
	for _, edge := range graphValue.Edges {
		if edge.From == from && edge.To == to && edge.Kind == kind {
			return
		}
	}
	t.Fatalf("edge %s not found in %#v", workflowcompile.EdgeSourceKey(from, to, kind), graphValue.Edges)
}
