package values_test

import (
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestExpressionEngineExternalAPI(t *testing.T) {
	metadata := values.Metadata{
		Producer:  values.Producer{Kind: "workflow_input", Reference: "run-1", Output: "enabled"},
		MediaType: "application/json",
		Redaction: values.RedactionPublic,
		Retention: values.RetentionRun,
	}
	enabled, err := values.NewInline(true, metadata)
	if err != nil {
		t.Fatal(err)
	}
	context := values.ExpressionContext{
		Inputs: values.ValueSet{"enabled": enabled},
		Run:    map[string]any{"id": "run-1"},
	}
	engine := values.NewExpressionEngine()
	result, err := engine.EvaluateBool(
		graph.Expression{Text: `inputs.enabled && run.id == "run-1"`},
		context,
		values.ExpressionOptions{},
	)
	if err != nil {
		t.Fatalf("EvaluateBool failed: %v", err)
	}
	if !result {
		t.Fatal("EvaluateBool returned false")
	}

	interpolated, err := engine.Interpolate(
		`run={{ run.id }}`,
		&graph.SourceRef{Format: graph.SourceWorkflow, Locator: "workflow.yaml", StartLine: 3},
		context,
		values.ExpressionOptions{},
	)
	if err != nil {
		t.Fatalf("Interpolate failed: %v", err)
	}
	if interpolated != "run=run-1" {
		t.Fatalf("Interpolate = %q", interpolated)
	}
}
