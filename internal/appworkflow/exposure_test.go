package appworkflow

import (
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
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

func TestExactExposurePlanAcceptsLegacyEmptyProvenanceAuthorityAfterHostNormalization(t *testing.T) {
	content := []byte("legacy registry source")
	sourceDigest := values.SHA256Digest(content)
	record := registry.WorkflowRecord{
		Name: "team/agent-demo", Namespace: "team", Version: "v1", Digest: sourceDigest,
		SourceFormat: graph.SourceWorkflow, Authority: "registry.test", TrustClass: "project",
		Provenance: graph.Provenance{Origin: "legacy-registry", Locator: "registry://team/agent-demo/v1", Digest: sourceDigest},
	}
	provenance := record.Provenance
	provenance.Authority = record.Authority
	provenance.Metadata = graph.Metadata{"trust_class": record.TrustClass}
	plan := compile.ExecutionPlan{
		SchemaVersion: compile.ExecutionPlanSchemaVersion, ID: "agent-demo",
		Definition: graph.DefinitionRef{Authority: record.Authority, Kind: "workflow", ID: "agent-demo", Locator: record.Provenance.Locator, Version: record.Version, Digest: record.Digest, Provenance: &provenance},
		Provenance: provenance, SourceDigests: []compile.SourceDigest{{Format: record.SourceFormat, Digest: record.Digest}},
		Graph: graph.Graph{ID: "agent-demo", Namespace: record.Namespace, Version: record.Version, Provenance: provenance},
	}
	graphDigest, err := compile.GraphDigest(plan.Graph)
	if err != nil {
		t.Fatal(err)
	}
	plan.Graph.Digest = graphDigest
	planDigest, err := compile.PlanDigest(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.Digest = planDigest
	record.PlanDigest = planDigest
	if exactErr := exactExposurePlan(record, &plan); exactErr != nil {
		t.Fatalf("normalized legacy exposure plan = %v", exactErr)
	}
	snapshot, err := hoststate.SealPlanSnapshot(hoststate.PlanSnapshot{
		SchemaVersion: hoststate.PlanSnapshotSchemaVersion, Plan: plan, SourceMap: plan.SourceMap,
		Compile: hoststate.UnavailableCompileDescriptor("legacy registry compiler metadata unavailable"),
		Source: &hoststate.SourceSnapshot{
			SchemaVersion: hoststate.SourceSnapshotSchemaVersion, Definition: plan.Definition,
			Format: record.SourceFormat, SourceSchemaID: "workflow.source", SourceSchemaVersion: "1",
			TrustClass: record.TrustClass, Digest: sourceDigest, Content: content,
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		},
	})
	if err != nil || snapshot.Validate() != nil {
		t.Fatalf("normalized legacy snapshot = %#v, %v", snapshot, err)
	}
}
