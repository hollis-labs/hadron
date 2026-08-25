package authoring_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/authoring"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	graphschema "github.com/hollis-labs/hadron/workflow/graph/schema"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
)

func TestBuilderIsImmutableAndUsesOrdinaryValidation(t *testing.T) {
	base := authoring.New("sdk-demo", "v1").Authority("project")
	withNode := base.Node(graph.Node{ID: "work", Kind: "fixture", KindVersion: "v1", Config: graph.Config{}})
	if len(base.Graph().Nodes) != 0 || len(withNode.Graph().Nodes) != 1 {
		t.Fatalf("builder mutation leaked: base=%#v next=%#v", base.Graph().Nodes, withNode.Graph().Nodes)
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
		t.Fatal(err)
	}
	result := withNode.Compile(t.Context(), authoring.CompileOptions{Validation: workflowcompile.ValidationOptions{StepKinds: registry}})
	if result.Plan == nil || len(result.Diagnostics) != 0 || result.Plan.Graph.Nodes[0].Kind != "fixture" {
		t.Fatalf("Compile() = %#v", result)
	}
	nested := graph.Config{"options": map[string]any{"enabled": true}}
	defensive := base.Node(graph.Node{ID: "defensive", Kind: "fixture", KindVersion: "v1", Config: nested})
	nested["options"].(map[string]any)["enabled"] = false
	exposed := defensive.Graph()
	exposed.Nodes[0].Config["options"].(map[string]any)["enabled"] = false
	if got := defensive.Graph().Nodes[0].Config["options"].(map[string]any)["enabled"]; got != true {
		t.Fatalf("nested builder state was mutable: %#v", got)
	}
	unsupported := base.Node(graph.Node{ID: "unsupported", Kind: "fixture", Config: graph.Config{"callback": func() {}}}).Compile(t.Context(), authoring.CompileOptions{})
	if unsupported.Plan != nil || !hasCode(unsupported.Diagnostics, authoring.CodeInvalidEnvelope) {
		t.Fatalf("non-JSON builder data did not fail closed: %#v", unsupported)
	}

	invalid := withNode.Node(graph.Node{ID: "missing-kind", Kind: "unknown", KindVersion: "v1", Config: graph.Config{}})
	failed := invalid.Compile(t.Context(), authoring.CompileOptions{Validation: workflowcompile.ValidationOptions{StepKinds: registry}})
	if failed.Plan != nil || !hasCode(failed.Diagnostics, workflowcompile.CodeUnknownStepKind) {
		t.Fatalf("invalid builder bypassed ordinary validation: %#v", failed)
	}
}

func TestEquivalentFrontEndsHaveSameCanonicalPlanAndDiagnostics(t *testing.T) {
	registry := stepkind.NewRegistry()
	if err := registry.Register(stepkindtest.NewNoopKind("fixture", "v1")); err != nil {
		t.Fatal(err)
	}
	const source = `workflow: {id: equivalent, version: v1}
steps:
  - {id: work, kind: fixture, kind_version: v1, config: {}}
`
	loaded := workflowcompile.LoadBytes("equivalent.workflow.yaml", []byte(source))
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("compile YAML = %#v", compiled.Diagnostics)
	}
	inferred := workflowcompile.InferValueDependencies(compiled.Plan, workflowcompile.DependencyOptions{})
	if inferred.Plan == nil {
		t.Fatalf("infer YAML = %#v", inferred.Diagnostics)
	}
	if findings := workflowcompile.ValidatePlan(context.Background(), inferred.Plan, workflowcompile.ValidationOptions{StepKinds: registry}); len(findings) != 0 {
		t.Fatalf("validate YAML = %#v", findings)
	}

	built := authoring.New("equivalent", "v1").Node(graph.Node{ID: "work", Kind: "fixture", KindVersion: "v1", Config: graph.Config{}}).
		Compile(t.Context(), authoring.CompileOptions{Validation: workflowcompile.ValidationOptions{StepKinds: registry}})
	if built.Plan == nil {
		t.Fatalf("compile builder = %#v", built.Diagnostics)
	}
	if built.Plan.Digest == inferred.Plan.Digest {
		t.Fatal("source-bound plan digests unexpectedly collapsed across front ends")
	}
	fixture, err := os.ReadFile("testdata/equivalent.graph.json")
	if err != nil {
		t.Fatal(err)
	}
	var authoredGraph graph.Graph
	if unmarshalErr := json.Unmarshal(fixture, &authoredGraph); unmarshalErr != nil {
		t.Fatal(unmarshalErr)
	}
	compileGraph := func(format graph.SourceFormat) authoring.Result {
		envelope := authoring.Envelope{
			SchemaID: authoring.EnvelopeSchemaID, SchemaVersion: authoring.EnvelopeSchemaVersion,
			MaterialSchemaID: graphschema.ID, MaterialSchemaVersion: graphschema.Version,
			Format: authoring.FormatGraphIR, Graph: &authoredGraph,
		}
		encoded, encodeErr := json.Marshal(envelope)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		decoded, diagnostics := authoring.DecodeEnvelope(encoded, authoring.Limits{})
		if len(diagnostics) != 0 {
			t.Fatalf("decode %s diagnostics = %#v", format, diagnostics)
		}
		return authoring.CompileEnvelope(t.Context(), decoded, format, authoring.CompileOptions{Validation: workflowcompile.ValidationOptions{StepKinds: registry}})
	}
	ui := compileGraph(graph.SourceUI)
	agent := compileGraph(graph.SourceAgent)
	for name, result := range map[string]authoring.Result{"go_builder": built, "typescript_ui": ui, "agent": agent} {
		if result.Plan == nil || len(result.Diagnostics) != 0 {
			t.Fatalf("%s compile = plan %#v diagnostics %#v", name, result.Plan, result.Diagnostics)
		}
	}

	want, err := workflowcompile.FingerprintPlanSemantics(*inferred.Plan)
	if err != nil {
		t.Fatal(err)
	}
	wantDocument, err := workflowcompile.CanonicalPlanSemantics(*inferred.Plan)
	if err != nil {
		t.Fatal(err)
	}
	for name, plan := range map[string]*workflowcompile.ExecutionPlan{"go_builder": built.Plan, "typescript_ui": ui.Plan, "agent": agent.Plan} {
		got, fingerprintErr := workflowcompile.FingerprintPlanSemantics(*plan)
		if fingerprintErr != nil {
			t.Fatal(fingerprintErr)
		}
		gotDocument, documentErr := workflowcompile.CanonicalPlanSemantics(*plan)
		if documentErr != nil {
			t.Fatal(documentErr)
		}
		if got != want || got.SchemaVersion == "" || got.SemanticDigest == "" || gotDocument.SchemaVersion != wantDocument.SchemaVersion || !bytes.Equal(gotDocument.CanonicalJSON, wantDocument.CanonicalJSON) {
			t.Fatalf("%s semantic output differs: YAML=%#v got=%#v\nYAML JSON=%s\ngot JSON=%s", name, want, got, wantDocument.CanonicalJSON, gotDocument.CanonicalJSON)
		}
	}
}

func TestDecodeEnvelopeRejectsUnknownSchemaDepthAndStructuralOverflow(t *testing.T) {
	value := authoring.New("decode-demo", "v1").Envelope()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, findings := authoring.DecodeEnvelope(encoded, authoring.Limits{}); decoded.Graph == nil || len(findings) != 0 {
		t.Fatalf("DecodeEnvelope(valid) = %#v, %#v", decoded, findings)
	}
	if _, findings := authoring.DecodeEnvelope(encoded, authoring.Limits{MaximumBytes: len(encoded) - 1}); !hasCode(findings, authoring.CodeInvalidEnvelope) {
		t.Fatalf("byte bound findings = %#v", findings)
	}

	unknown := strings.TrimSuffix(string(encoded), "}") + `,"unknown":true}`
	if _, findings := authoring.DecodeEnvelope([]byte(unknown), authoring.Limits{}); !hasCode(findings, authoring.CodeInvalidEnvelope) {
		t.Fatalf("unknown field findings = %#v", findings)
	}

	wrongSchema := value
	wrongSchema.MaterialSchemaID = "https://schemas.example.test/workflow/v2"
	wrong, _ := json.Marshal(wrongSchema)
	if _, findings := authoring.DecodeEnvelope(wrong, authoring.Limits{}); !hasCode(findings, authoring.CodeUnsupportedSchema) {
		t.Fatalf("unsupported schema findings = %#v", findings)
	}

	deep := []byte(`{"schema_id":"` + authoring.EnvelopeSchemaID + `","schema_version":"1","material_schema_id":"` + graphschema.ID + `","material_schema_version":"1","format":"graph_ir","graph":{"id":"deep","version":"v1","digest":"","nodes":[],"metadata":{"a":{"b":{"c":true}}}}}`)
	if _, findings := authoring.DecodeEnvelope(deep, authoring.Limits{MaximumDepth: 4}); !hasCode(findings, authoring.CodeInvalidEnvelope) {
		t.Fatalf("depth findings = %#v", findings)
	}

	overflow := authoring.New("large", "v1").Node(graph.Node{ID: "one", Kind: "fixture", Config: graph.Config{}}).
		Node(graph.Node{ID: "two", Kind: "fixture", Config: graph.Config{}}).Envelope()
	overflowJSON, _ := json.Marshal(overflow)
	if _, findings := authoring.DecodeEnvelope(overflowJSON, authoring.Limits{MaximumNodes: 1}); !hasCode(findings, authoring.CodeInvalidEnvelope) {
		t.Fatalf("node bound findings = %#v", findings)
	}
	edgeOverflow := authoring.New("edges", "v1").
		Edge(graph.Edge{From: "one", To: "two", Kind: graph.EdgeControl}).
		Edge(graph.Edge{From: "two", To: "three", Kind: graph.EdgeControl}).Envelope()
	edgeOverflowJSON, _ := json.Marshal(edgeOverflow)
	if _, findings := authoring.DecodeEnvelope(edgeOverflowJSON, authoring.Limits{MaximumEdges: 1}); !hasCode(findings, authoring.CodeInvalidEnvelope) {
		t.Fatalf("edge bound findings = %#v", findings)
	}
}

func TestCompileEnvelopeEnforcesWorkflowSourceStructuralBounds(t *testing.T) {
	compileSource := func(source string, limits authoring.Limits) authoring.Result {
		t.Helper()
		envelope := authoring.Envelope{
			SchemaID: authoring.EnvelopeSchemaID, SchemaVersion: authoring.EnvelopeSchemaVersion,
			MaterialSchemaID: authoring.WorkflowSourceSchemaID, MaterialSchemaVersion: authoring.WorkflowSourceSchemaVersion,
			Format: authoring.FormatWorkflowSource, Source: source,
		}
		encoded, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		decoded, findings := authoring.DecodeEnvelope(encoded, limits)
		if len(findings) != 0 {
			t.Fatalf("DecodeEnvelope() = %#v", findings)
		}
		return authoring.CompileEnvelope(t.Context(), decoded, graph.SourceWorkflow, authoring.CompileOptions{Limits: limits})
	}

	nodeOverflow := compileSource(`workflow: {id: node-overflow, version: v1}
steps:
  - {id: one, kind: fixture}
  - {id: two, kind: fixture}
`, authoring.Limits{MaximumNodes: 1})
	if nodeOverflow.Plan != nil || !hasCode(nodeOverflow.Diagnostics, authoring.CodeInvalidEnvelope) || !strings.Contains(nodeOverflow.Diagnostics[0].Message, "nodes 2/1") {
		t.Fatalf("workflow_source node overflow = %#v", nodeOverflow)
	}

	edgeOverflow := compileSource(`workflow: {id: edge-overflow, version: v1}
steps:
  - id: one
    kind: fixture
    outputs: {value: {type: string}}
  - id: two
    kind: fixture
    with: {one: {expression: steps.one.outputs.value}}
    outputs: {value: {type: string}}
  - id: three
    kind: fixture
    with:
      one: {expression: steps.one.outputs.value}
      two: {expression: steps.two.outputs.value}
`, authoring.Limits{MaximumEdges: 1})
	if edgeOverflow.Plan != nil || !hasCode(edgeOverflow.Diagnostics, authoring.CodeInvalidEnvelope) || !strings.Contains(edgeOverflow.Diagnostics[0].Message, "edges 3/1") {
		t.Fatalf("workflow_source edge overflow = %#v", edgeOverflow)
	}
}

func TestCompactDiagnosticsAreUTF8SafeAndExplicitlyBounded(t *testing.T) {
	oversized := strings.Repeat("🛰️", 700)
	path := make([]string, 40)
	for index := range path {
		path[index] = oversized
	}
	input := make([]diagnostic.Diagnostic, 70)
	for index := range input {
		input[index] = diagnostic.Diagnostic{
			Severity:    diagnostic.SeverityError,
			Code:        authoring.CodeInvalidEnvelope,
			Message:     oversized,
			Source:      &graph.SourceRef{Locator: oversized, Path: path},
			Remediation: &diagnostic.Remediation{Message: oversized},
		}
	}
	got := authoring.CompactDiagnostics(input)
	if len(got) != 64 || got[len(got)-1].Code != string(authoring.CodeDiagnosticsTruncated) || !strings.Contains(got[len(got)-1].Message, "7 additional") {
		t.Fatalf("compact finding bound/sentinel = len %d last %#v", len(got), got[len(got)-1])
	}
	first := got[0]
	for name, value := range map[string]string{"message": first.Message, "help": first.Help, "locator": first.Locator, "path": first.Path[0]} {
		if !utf8.ValidString(value) || !strings.HasSuffix(value, "…") {
			t.Fatalf("%s truncation is not visible UTF-8: %q", name, value)
		}
	}
	if len(first.Message) > 1024 || len(first.Help) > 1024 || len(first.Locator) > 512 || len(first.Path) != 32 || len(first.Path[0]) > 128 || first.Path[len(first.Path)-1] != "…" {
		t.Fatalf("compact field/path bounds = %#v", first)
	}
	if len(input[0].Message) != len(oversized) || len(input[0].Source.Path) != 40 {
		t.Fatal("compact projection mutated full Go diagnostics")
	}
}

func TestSourceSchemaNegotiationIsExactAndNeverContentDerived(t *testing.T) {
	for _, format := range []graph.SourceFormat{graph.SourceWorkflow, graph.SourceSDK, graph.SourceUI, graph.SourceAgent} {
		id, version, supported := authoring.SourceSchemaFor(format)
		if !supported || id == "" || version != "1" {
			t.Fatalf("SourceSchemaFor(%q) = %q, %q, %v", format, id, version, supported)
		}
	}
	for _, format := range []graph.SourceFormat{"", graph.SourceArchivedBlueprint, graph.SourceArchivedPipeline, "future"} {
		if id, version, supported := authoring.SourceSchemaFor(format); supported || id != "" || version != "" {
			t.Fatalf("unsupported SourceSchemaFor(%q) = %q, %q, %v", format, id, version, supported)
		}
	}
}

func hasCode(findings []diagnostic.Diagnostic, code diagnostic.Code) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}
