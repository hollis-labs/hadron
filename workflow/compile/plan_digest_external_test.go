package compile_test

import (
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
)

func TestPlanDigestRecomputesCanonicalRelocationStableIdentity(t *testing.T) {
	loaded := workflowcompile.LoadBytes("original.workflow.yaml", []byte(`workflow: {name: Digest Test, version: 1.0.0}
steps:
  - id: work
    kind: transform
    kind_version: v1
    config: {result: "'ok'"}
`))
	if len(loaded.Diagnostics) != 0 {
		t.Fatal(loaded.Diagnostics)
	}
	compiled := workflowcompile.Compile(loaded.Source)
	if len(compiled.Diagnostics) != 0 || compiled.Plan == nil {
		t.Fatal(compiled.Diagnostics)
	}
	digest, err := workflowcompile.PlanDigest(*compiled.Plan)
	if err != nil || digest != compiled.Plan.Digest {
		t.Fatalf("PlanDigest() = %q, %v; want %q", digest, err, compiled.Plan.Digest)
	}
	relocated := *compiled.Plan
	relocated.Provenance.Locator = "/different/root/workflow.yaml"
	relocated.Definition.Locator = "/different/root/workflow.yaml"
	relocated.SourceMap.Graph.Locator = "/different/root/workflow.yaml"
	relocatedDigest, err := workflowcompile.PlanDigest(relocated)
	if err != nil || relocatedDigest != digest {
		t.Fatalf("relocated PlanDigest() = %q, %v; want %q", relocatedDigest, err, digest)
	}
	tampered := *compiled.Plan
	tampered.Graph = compiled.Plan.Graph
	tampered.Graph.Nodes = append([]graph.Node(nil), compiled.Plan.Graph.Nodes...)
	tampered.Graph.Nodes[0].Config = graph.Config{"result": "'changed'"}
	tamperedDigest, err := workflowcompile.PlanDigest(tampered)
	if err != nil || tamperedDigest == digest {
		t.Fatalf("semantic tamper PlanDigest() = %q, %v", tamperedDigest, err)
	}
}
