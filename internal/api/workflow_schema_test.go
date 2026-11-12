package api

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/internal/api/internal/workflowschema"
	graphschema "github.com/hollis-labs/libs/workflow/engine/graph/schema"
)

func TestGeneratedWorkflowSchemasAreCurrent(t *testing.T) {
	apiSchema, err := workflowschema.Generate()
	if err != nil {
		t.Fatal(err)
	}
	second, err := workflowschema.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(apiSchema, second) {
		t.Fatal("workflow API schema generation is not deterministic")
	}
	assertGeneratedFile(t, filepath.Join("schema", "workflow-api.schema.json"), apiSchema)

	typescript, err := workflowschema.GenerateTypeScript(graphschema.Document(), apiSchema)
	if err != nil {
		t.Fatal(err)
	}
	secondTypeScript, err := workflowschema.GenerateTypeScript(graphschema.Document(), apiSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(typescript, secondTypeScript) {
		t.Fatal("workflow TypeScript generation is not deterministic")
	}
	assertGeneratedFile(t, filepath.Join("schema", "workflow.ts"), typescript)
}

func TestAgentAuthoringSchemaExcludesHostOwnedIdentity(t *testing.T) {
	data, err := workflowschema.Generate()
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	definitions := document["$defs"].(map[string]any)
	request := definitions["AppworkflowAgentAuthoringRequest"].(map[string]any)
	properties := request["properties"].(map[string]any)
	if request["additionalProperties"] != false {
		t.Fatal("agent authoring request does not reject unknown spoof fields")
	}
	for _, field := range []string{"principal", "authority", "trust_class"} {
		if _, exists := properties[field]; exists {
			t.Fatalf("host-owned identity field %q leaked into generated agent input", field)
		}
	}
}

func assertGeneratedFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s is stale; run: go generate ./internal/api", path)
	}
}
