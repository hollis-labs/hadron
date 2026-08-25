package hoststate

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestPlanSnapshotValidatesExactMaterialAndProjectsSafeMetadata(t *testing.T) {
	const secret = "source-private-marker"
	contents := []byte("workflow:\n  name: Snapshot Fixture\n  version: v1\nsteps:\n  - name: Echo\n    transform:\n      value: " + secret + "\n")
	loaded := workflowcompile.LoadBytes("snapshot.workflow.yaml", contents)
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("compile snapshot fixture = %#v", compiled)
	}
	plan := *compiled.Plan
	unsafeLocator := "https://user:password@example.test/workflow.yaml?token=secret-query#secret-fragment"
	plan.Definition.Authority = "project"
	plan.Definition.Locator = unsafeLocator
	provenance := plan.Provenance
	provenance.Authority = "project"
	provenance.Locator = unsafeLocator
	provenance.Parents = []graph.ProvenanceRef{{Authority: "registry", Locator: "https://key@example.test/parent?credential=hidden#raw"}}
	provenance.Parents = append(provenance.Parents, graph.ProvenanceRef{Authority: "legacy", Locator: "%zz-malformed-secret"})
	provenance.Metadata = graph.Metadata{"secret_payload": "must-not-render"}
	plan.Definition.Provenance = &provenance
	plan.Provenance = provenance
	setSourceMapLocator(&plan.SourceMap, unsafeLocator)
	setSourceMapLocator(&plan.Graph.SourceMap, unsafeLocator)
	plan.SourceMap.Inputs = map[string]graph.SourceRef{"relative": {Locator: "workflow.yaml?token=relative-secret#fragment-secret"}}
	plan.Graph.SourceMap.Inputs = map[string]graph.SourceRef{"relative": {Locator: "workflow.yaml?token=relative-secret#fragment-secret"}}
	plan.SourceMap.Outputs = map[string]graph.SourceRef{"credential": {Locator: "user:scheme-less-secret@example.test/path"}}
	plan.Graph.SourceMap.Outputs = map[string]graph.SourceRef{"credential": {Locator: "user:scheme-less-secret@example.test/path"}}
	plan.SourceMap.Activations = map[string]graph.SourceRef{"secret-ref": {Locator: "SeCrEt://vault/private-reference"}}
	plan.Graph.SourceMap.Activations = map[string]graph.SourceRef{"secret-ref": {Locator: "SeCrEt://vault/private-reference"}}
	plan.Digest, _ = workflowcompile.PlanDigest(plan)
	descriptor, err := NewCompileDescriptor("snapshot-test-v1", workflowcompile.DefaultMaxCallDepth, []stepkind.StepKindSpec{transform.New().Spec()}, nil, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := SealPlanSnapshot(PlanSnapshot{
		SchemaVersion: PlanSnapshotSchemaVersion, Plan: plan, SourceMap: plan.SourceMap,
		Compile: descriptor,
		Source: &SourceSnapshot{
			SchemaVersion: SourceSnapshotSchemaVersion, Definition: plan.Definition,
			Format: graph.SourceWorkflow, SourceSchemaID: "workflow.source", SourceSchemaVersion: "1",
			TrustClass: "project", Digest: values.SHA256Digest(contents), Content: contents,
			MovableAtResolution: true, Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		},
	})
	if err != nil || snapshot.Validate() != nil {
		t.Fatalf("valid snapshot = %#v, %v", snapshot, err)
	}
	metadata, err := snapshot.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{secret, "password", "secret-query", "secret-fragment", "credential=hidden", "relative-secret", "fragment-secret", "scheme-less-secret", "malformed-secret", "private-reference", "must-not-render"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("safe metadata contains %q: %s", forbidden, encoded)
		}
	}
	if !bytes.Contains(encoded, []byte("https://example.test/workflow.yaml")) || !metadata.Source.Available || metadata.Source.ContentBytes != len(contents) {
		t.Fatalf("safe metadata lost useful source identity: %s", encoded)
	}
	if !bytes.Contains(encoded, []byte(values.RedactedMarker)) {
		t.Fatalf("safe metadata did not mark secret locator redaction: %s", encoded)
	}
	metadata.SourceMap.Nodes["echo"] = graph.SourceRef{Locator: "mutated"}
	cloned, err := snapshot.Metadata()
	if err != nil || cloned.SourceMap.Nodes["echo"].Locator == "mutated" {
		t.Fatalf("metadata was not defensive: %#v, %v", cloned, err)
	}
	if !bytes.Contains(snapshot.Source.Content, []byte(secret)) {
		t.Fatal("journal-internal source content was not retained")
	}

	tampered := snapshot
	tampered.Source.Content = bytes.ReplaceAll(tampered.Source.Content, []byte(secret), []byte("changed"))
	if tampered.Validate() == nil {
		t.Fatal("tampered source content was accepted")
	}
}

func setSourceMapLocator(sourceMap *graph.SourceMap, locator string) {
	if sourceMap.Graph != nil {
		sourceMap.Graph.Locator = locator
	}
	for key, source := range sourceMap.Nodes {
		source.Locator = locator
		sourceMap.Nodes[key] = source
	}
}
