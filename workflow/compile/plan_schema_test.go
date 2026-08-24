package compile_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/compile/internal/planschema"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	planSchemaResource  = "https://schemas.hollis-labs.dev/workflow/plan/v1/execution-plan.schema.json"
	graphSchemaResource = "https://schemas.hollis-labs.dev/workflow/graph/v1/workflow.schema.json"
)

func TestGeneratedPlanSchemaIsCurrentAndDeterministic(t *testing.T) {
	generated, err := planschema.Generate()
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := planschema.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, repeated) {
		t.Fatal("execution-plan schema generation is not deterministic")
	}
	committed, err := os.ReadFile(planSchemaPath())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, committed) {
		t.Fatal("execution-plan schema is stale; run: go generate ./workflow/compile")
	}
}

func TestGeneratedPlanSchemaCompilesAndValidatesPlan(t *testing.T) {
	tests := []struct {
		name string
		plan any
	}{
		{name: "representative", plan: representativePlan(t)},
		{name: "activations", plan: activationPlan(t, activationLocator)},
		{name: "bundled definition", plan: bundledPlan(t)},
	}
	compiled := compiledPlanSchema(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := compiled.Validate(jsonDocument(t, stableJSON(t, test.plan))); err != nil {
				t.Fatalf("generated schema rejects compiled ExecutionPlan: %v", err)
			}
		})
	}
}

func bundledPlan(t *testing.T) *workflowcompile.ExecutionPlan {
	t.Helper()
	plan := representativePlan(t)
	plan.BundledDefinitions = []workflowcompile.ResolvedDefinition{{Definition: plan.Definition, Graph: plan.Graph}}
	return &plan
}

func TestGeneratedPlanSchemaRejectsUndeclaredEnvelopeField(t *testing.T) {
	plan := jsonDocument(t, stableJSON(t, representativePlan(t))).(map[string]any)
	plan["compiled_at"] = "2026-08-24T00:00:00Z"
	if err := compiledPlanSchema(t).Validate(plan); err == nil {
		t.Fatal("generated schema accepted undeclared compiled_at field")
	}
}

func TestGeneratedPlanSchemaReferencesGraphAuthority(t *testing.T) {
	document := planSchemaDocument(t)
	boundary, ok := document["x-workflow-boundary"].(map[string]any)
	if !ok || boundary["component"] != "execution-plan" || boundary["graphSchema"] != graphSchemaResource || boundary["version"] != "1" {
		t.Fatalf("execution-plan schema boundary = %#v", document["x-workflow-boundary"])
	}
	definitions, ok := document["$defs"].(map[string]any)
	if !ok {
		t.Fatalf("$defs = %#v", document["$defs"])
	}
	if _, duplicated := definitions["Graph"]; duplicated {
		t.Fatal("execution-plan schema duplicated graph.Graph")
	}
	plan, ok := definitions["ExecutionPlan"].(map[string]any)
	if !ok {
		t.Fatalf("ExecutionPlan definition = %#v", definitions["ExecutionPlan"])
	}
	properties := plan["properties"].(map[string]any)
	graphProperty := properties["graph"].(map[string]any)
	if graphProperty["$ref"] != graphSchemaResource+"#/$defs/Graph" {
		t.Fatalf("ExecutionPlan.graph schema = %#v", graphProperty)
	}
}

func compiledPlanSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(graphSchemaResource, graphSchemaDocument(t)); err != nil {
		t.Fatal(err)
	}
	if err := compiler.AddResource(planSchemaResource, planSchemaDocument(t)); err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.Compile(planSchemaResource)
	if err != nil {
		t.Fatalf("compile generated execution-plan schema: %v", err)
	}
	return compiled
}

func planSchemaDocument(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile(planSchemaPath())
	if err != nil {
		t.Fatal(err)
	}
	return jsonDocument(t, data).(map[string]any)
}

func graphSchemaDocument(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "graph", "schema", "workflow.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	return jsonDocument(t, data).(map[string]any)
}

func planSchemaPath() string {
	return filepath.Join("schema", "execution-plan.schema.json")
}

func jsonDocument(t *testing.T, data []byte) any {
	t.Helper()
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
