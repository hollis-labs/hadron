package graph_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/graph/internal/schemagen"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const schemaResource = "https://schemas.hollis-labs.dev/workflow/graph/v1/workflow.schema.json"

func TestGeneratedSchemaIsCurrent(t *testing.T) {
	generated, err := schemagen.Generate()
	if err != nil {
		t.Fatal(err)
	}
	regenerated, err := schemagen.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, regenerated) {
		t.Fatal("workflow graph schema generation is not deterministic")
	}
	committed, err := os.ReadFile(schemaPath(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, committed) {
		t.Fatal("workflow graph schema is stale; run: go generate ./workflow/graph")
	}
}

func TestGeneratedSchemaCompilesAndValidatesGraph(t *testing.T) {
	instance := graph.Graph{
		ID:      "schema-open-extension-fixture",
		Version: "1.0.0",
		Digest:  "sha256:schema-open-extension-fixture",
		Nodes: []graph.Node{{
			ID:   "extension",
			Kind: "future-registered-kind",
			Config: graph.Config{
				"adapter-owned": map[string]any{
					"enabled": true,
					"limit":   3,
				},
			},
		}},
		Activations: []graph.ActivationDeclaration{{
			ID:     "source-hook",
			Kind:   "webhook",
			Config: graph.Config{"path": "/hooks/source"},
			Provenance: graph.Provenance{
				Authority: "project",
				Origin:    "workflow-source",
				Digest:    "sha256:source",
			},
		}},
	}
	if err := compiledSchema(t).Validate(jsonValue(t, instance)); err != nil {
		t.Fatalf("generated schema rejects graph with registered-kind extension points: %v", err)
	}
}

func TestGeneratedSchemaRejectsClosedEnum(t *testing.T) {
	instance := graph.Graph{
		ID:      "schema-closed-enum-fixture",
		Version: "1.0.0",
		Digest:  "sha256:schema-closed-enum-fixture",
		Nodes: []graph.Node{{
			ID:   "child",
			Kind: "call",
			Call: &graph.CallSpec{
				Definition: graph.DefinitionRef{ID: "child"},
				Mode:       graph.CallMode("detached"),
			},
		}},
	}
	if err := compiledSchema(t).Validate(jsonValue(t, instance)); err == nil {
		t.Fatal("generated schema accepted unsupported call.mode \"detached\"")
	}
}

func TestGeneratedSchemaRejectsUndeclaredStructuralFields(t *testing.T) {
	tests := []struct {
		name     string
		instance map[string]any
	}{
		{
			name: "graph field",
			instance: map[string]any{
				"id":              "undeclared-graph-field",
				"version":         "1.0.0",
				"digest":          "sha256:undeclared-graph-field",
				"nodes":           []any{},
				"legacy_pipeline": map[string]any{},
			},
		},
		{
			name: "node field",
			instance: map[string]any{
				"id":      "undeclared-node-field",
				"version": "1.0.0",
				"digest":  "sha256:undeclared-node-field",
				"nodes": []any{map[string]any{
					"id":   "request",
					"kind": "future-registered-kind",
					"http": map[string]any{"url": "https://example.test"},
				}},
			},
		},
	}

	compiled := compiledSchema(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := compiled.Validate(test.instance); err == nil {
				t.Fatal("generated schema accepted an undeclared structural field")
			}
		})
	}
}

func TestGeneratedSchemaMetadataAndExtensionPoints(t *testing.T) {
	document := schemaDocument(t)
	boundaries := object(t, document, "x-workflow-boundaries")
	source := object(t, boundaries, "sourceAuthoring")
	if source["format"] != "workflow" || source["schema"] != "#/$defs/Graph" {
		t.Fatalf("source-authoring boundary = %#v", source)
	}
	preferredFiles, ok := source["preferredFiles"].([]any)
	if !ok || len(preferredFiles) != 2 || preferredFiles[0] != "*.workflow.yaml" || preferredFiles[1] != "workflow.yaml" {
		t.Fatalf("source-authoring preferred files = %#v", source["preferredFiles"])
	}
	plan := object(t, boundaries, "serializedExecutionPlan")
	if plan["component"] != "execution-plan" ||
		plan["schema"] != "https://schemas.hollis-labs.dev/workflow/plan/v1/execution-plan.schema.json" ||
		plan["graph"] != "#/$defs/Graph" {
		t.Fatalf("serialized-plan boundary = %#v", plan)
	}

	definitions := object(t, document, "$defs")
	node := object(t, definitions, "Node")
	properties := object(t, node, "properties")
	kind := object(t, properties, "kind")
	if _, closed := kind["enum"]; closed {
		t.Fatalf("Node.kind must remain open: %#v", kind)
	}
	if kind["x-workflow-extension-point"] != "registered-step-kind" {
		t.Fatalf("Node.kind extension metadata = %#v", kind)
	}
	nodeConfig := object(t, properties, "config")
	if nodeConfig["x-workflow-extension-point"] != "adapter-config" {
		t.Fatalf("Node.config extension metadata = %#v", nodeConfig)
	}
	config := object(t, definitions, "Config")
	if additional, exists := config["additionalProperties"]; !exists || additional == false {
		t.Fatalf("Config must remain adapter-opaque and open: %#v", config)
	}
	activation := object(t, definitions, "ActivationDeclaration")
	activationProperties := object(t, activation, "properties")
	provenance := object(t, activationProperties, "provenance")
	if provenance["$ref"] != "#/$defs/Provenance" {
		t.Fatalf("ActivationDeclaration.provenance schema = %#v", provenance)
	}
}

func compiledSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(schemaResource, schemaDocument(t)); err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.Compile(schemaResource)
	if err != nil {
		t.Fatalf("compile generated workflow graph schema: %v", err)
	}
	return compiled
}

func jsonValue(t *testing.T, value any) any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func schemaDocument(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile(schemaPath(t))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatalf("parse generated workflow graph schema: %v", err)
	}
	return document
}

func schemaPath(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate schema test")
	}
	return filepath.Join(filepath.Dir(filename), "schema", "workflow.schema.json")
}

func object(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v, want object", key, parent[key])
	}
	return value
}
