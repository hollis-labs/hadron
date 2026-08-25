package registry

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/workflow/authoring"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestLegacyCatalogRecordDefaultsToWorkflowSourceWithoutContentInference(t *testing.T) {
	source := []byte("workflow: {id: legacy, version: v1}\nsteps: [{id: work, kind: fixture, config: {}}]\n")
	digest := values.SHA256Digest(source)
	record := WorkflowRecord{
		Name: "team/legacy", Namespace: "team", Version: "v1", Digest: digest, Source: source,
		Authority: "project", TrustClass: "project", PublisherPrincipal: "principal:test",
		Provenance: graph.Provenance{Authority: "project", Origin: "workflow-file", Locator: "legacy.workflow.yaml", Revision: "v1", Digest: digest},
	}
	encoded, err := json.Marshal(workflowCatalogSnapshot{Records: []WorkflowRecord{record}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if writeErr := os.WriteFile(path, encoded, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	index, err := OpenWorkflowIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := index.ResolveWorkflow(context.Background(), WorkflowQuery{Name: "team/legacy", Version: "v1", Digest: digest})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Record.SourceFormat != graph.SourceWorkflow || resolved.Record.SourceSchemaID != authoring.WorkflowSourceSchemaID || resolved.Record.SourceSchemaVersion != authoring.WorkflowSourceSchemaVersion {
		t.Fatalf("legacy source negotiation = %#v", resolved.Record)
	}
	if resolved.Record.SourceDefinitionID() != "legacy" {
		t.Fatalf("legacy source-local definition ID = %q", resolved.Record.SourceDefinitionID())
	}
}

func TestQualifiedRegistryNameCarriesExactlyOneSourceLocalGraphID(t *testing.T) {
	valid := WorkflowRecord{
		Name: "team/agent-demo", Namespace: "team", Version: "v1", Source: []byte("workflow: {id: agent-demo, version: v1}\nsteps: []\n"),
		Authority: "project", TrustClass: "project", Provenance: graph.Provenance{Origin: "workflow-file", Locator: "round-trip.workflow.yaml"},
	}
	record, err := canonicalWorkflowRecord(valid)
	if err != nil || record.SourceDefinitionID() != "agent-demo" {
		t.Fatalf("canonical qualified record = %#v, %v", record, err)
	}
	for name, namespace := range map[string]string{
		"team/":       "team",
		"team/a/b":    "team",
		"other/local": "team",
	} {
		invalid := valid
		invalid.Name, invalid.Namespace = name, namespace
		if _, err := canonicalWorkflowRecord(invalid); !errors.Is(err, ErrInvalidWorkflow) {
			t.Fatalf("invalid qualified source ID %q in %q was accepted: %v", name, namespace, err)
		}
	}
}
