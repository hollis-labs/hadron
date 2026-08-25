package values_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestValidateValueSchemaNestedLocalRefsEnumsNullAndNumericEdges(t *testing.T) {
	metadata := bindingTestMetadata("fixture", values.RedactionPrivate, values.RetentionRun)
	nested, err := values.NewInline([]any{
		map[string]any{"name": "one", "state": "ready"},
	}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	schema := graph.Schema{
		"type":  "array",
		"items": map[string]any{"$ref": "#/$defs/item"},
		"$defs": map[string]any{"item": map[string]any{
			"type": "object", "required": []any{"name", "state"},
			"additionalProperties": false,
			"properties": map[string]any{
				"name":  map[string]any{"type": "string"},
				"state": map[string]any{"enum": []any{"ready", nil}},
			},
		}},
	}
	if schemaErr := values.ValidateValueSchema(schema, nested); schemaErr != nil {
		t.Fatalf("nested local-ref schema rejected valid value: %v", schemaErr)
	}
	if schemaErr := values.ValidateSchema(schema); schemaErr != nil {
		t.Fatalf("declaration-only local-ref schema validation failed: %v", schemaErr)
	}

	invalidNested, err := values.NewInline([]any{
		map[string]any{"name": "one", "state": "unknown", "extra": true},
	}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if schemaErr := values.ValidateValueSchema(schema, invalidNested); !errors.Is(schemaErr, values.ErrSchemaMismatch) {
		t.Fatalf("nested schema error = %v, want ErrSchemaMismatch", schemaErr)
	}

	exact, err := values.NewInline(json.Number("9007199254740993"), metadata)
	if err != nil {
		t.Fatal(err)
	}
	numericSchema := graph.Schema{
		"type":    "integer",
		"minimum": json.Number("9007199254740993"),
		"maximum": json.Number("9007199254740993"),
	}
	if schemaErr := values.ValidateValueSchema(numericSchema, exact); schemaErr != nil {
		t.Fatalf("exact large integer rejected: %v", schemaErr)
	}
	adjacent, err := values.NewInline(json.Number("9007199254740992"), metadata)
	if err != nil {
		t.Fatal(err)
	}
	if schemaErr := values.ValidateValueSchema(numericSchema, adjacent); !errors.Is(schemaErr, values.ErrSchemaMismatch) {
		t.Fatalf("adjacent large integer error = %v, want ErrSchemaMismatch", schemaErr)
	}

	nullValue, err := values.NewInline(nil, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := values.ValidateValueSchema(graph.Schema{"type": []any{"string", "null"}, "enum": []any{"ok", nil}}, nullValue); err != nil {
		t.Fatalf("null enum rejected: %v", err)
	}
}

func TestValidateValueSchemaDoesNotRewriteLiteralTypeFields(t *testing.T) {
	metadata := bindingTestMetadata("fixture", values.RedactionPrivate, values.RetentionRun)
	literal := map[string]any{"type": "artifact", "label": "literal-data"}
	value, err := values.NewInline(literal, metadata)
	if err != nil {
		t.Fatal(err)
	}
	for _, schema := range []graph.Schema{
		{"type": "object", "const": literal},
		{"type": "object", "enum": []any{literal, map[string]any{"type": "other"}}},
	} {
		if err := values.ValidateSchema(schema); err != nil {
			t.Fatalf("literal-data schema did not compile: %v", err)
		}
		if err := values.ValidateValueSchema(schema, value); err != nil {
			t.Fatalf("literal type field was rewritten: %v", err)
		}
	}
}

func TestValidateValueSchemaDeniesExternalRefsAndRequiresArtifactType(t *testing.T) {
	metadata := bindingTestMetadata("fixture", values.RedactionPrivate, values.RetentionRun)
	inline, err := values.NewInline("value", metadata)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{
		"https://schemas.example.test/value.json",
		"file:///tmp/value.schema.json",
	} {
		if schemaErr := values.ValidateValueSchema(graph.Schema{"$ref": ref}, inline); !errors.Is(schemaErr, values.ErrInvalidSchema) {
			t.Fatalf("external ref %q error = %v, want ErrInvalidSchema", ref, schemaErr)
		}
		if schemaErr := values.ValidateSchema(graph.Schema{"$ref": ref}); !errors.Is(schemaErr, values.ErrInvalidSchema) {
			t.Fatalf("declaration-only external ref %q error = %v, want ErrInvalidSchema", ref, schemaErr)
		}
	}

	artifact, err := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://run/report.pdf",
		Digest: values.SHA256Digest([]byte("report")), MediaType: "application/pdf", SizeBytes: 6,
		Producer:  values.Producer{Kind: "node_output", Reference: "run-1/render", Output: "report"},
		Redaction: values.RedactionSecret, Retention: values.RetentionExternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := values.ValidateValueSchema(graph.Schema{"type": "artifact"}, artifact); err != nil {
		t.Fatalf("artifact schema rejected artifact: %v", err)
	}
	if err := values.ValidateSchema(graph.Schema{"type": "artifact"}); err != nil {
		t.Fatalf("declaration-only artifact schema validation failed: %v", err)
	}
	if err := values.ValidateValueSchema(graph.Schema{}, artifact); err != nil {
		t.Fatalf("open schema rejected artifact: %v", err)
	}
	if err := values.ValidateValueSchema(nil, artifact); err != nil {
		t.Fatalf("nil/open schema rejected artifact: %v", err)
	}
	localArtifactSchema := graph.Schema{
		"$ref":  "#/$defs/artifact",
		"$defs": map[string]any{"artifact": map[string]any{"type": "artifact"}},
	}
	if err := values.ValidateSchema(localArtifactSchema); err != nil {
		t.Fatalf("root-local artifact schema did not compile: %v", err)
	}
	if err := values.ValidateValueSchema(localArtifactSchema, artifact); err != nil {
		t.Fatalf("root-local artifact schema rejected artifact: %v", err)
	}
	if err := values.ValidateValueSchema(localArtifactSchema, inline); !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("root-local artifact schema accepted inline value: %v", err)
	}
	allOfArtifactSchema := graph.Schema{
		"allOf": []any{
			map[string]any{"$ref": "#/$defs/artifact"},
			map[string]any{"required": []any{"uri"}},
		},
		"$defs": map[string]any{"artifact": map[string]any{"type": "artifact"}},
	}
	if err := values.ValidateValueSchema(allOfArtifactSchema, artifact); err != nil {
		t.Fatalf("allOf local-ref artifact schema rejected artifact: %v", err)
	}
	if err := values.ValidateValueSchema(graph.Schema{"type": "object"}, artifact); !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("object schema accepted artifact: %v", err)
	}
	if err := values.ValidateValueSchema(graph.Schema{"required": []any{"uri"}}, artifact); !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("constrained schema without artifact type accepted artifact: %v", err)
	}
	if err := values.ValidateValueSchema(graph.Schema{"type": "artifact"}, inline); !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("artifact schema accepted inline string: %v", err)
	}
}

func TestEvaluateBindingPreservesExactValuePassthroughEnvelopes(t *testing.T) {
	secretRef, err := values.NewSecretRef(
		values.SecretRef("secret://project/api-token#value"),
		bindingTestMetadata("node-secret", values.RedactionSecret, values.RetentionProject),
	)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := values.NewArtifact(values.ArtifactRef{
		Store: "external", URI: "artifact://run/report.pdf",
		Digest: values.SHA256Digest([]byte("report")), MediaType: "application/pdf", SizeBytes: 6,
		Producer:  values.Producer{Kind: "node_output", Reference: "run-1/render", Output: "report"},
		Redaction: values.RedactionPrivate, Retention: values.RetentionExternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	context := values.ExpressionContext{Steps: map[string]values.StepContext{
		"render": {Outputs: values.ValueSet{"secret": secretRef, "report": artifact}, Status: "succeeded"},
	}, Outputs: values.ValueSet{"secret": secretRef, "report": artifact}}
	engine := values.NewExpressionEngine()
	computedMetadata := bindingTestMetadata("workflow-output", values.RedactionPrivate, values.RetentionRun)
	for name, want := range map[string]values.Value{"secret": secretRef, "report": artifact} {
		binding := graph.Binding{
			Kind:       graph.BindingExpression,
			Expression: &graph.Expression{Text: "steps.render.outputs." + name},
		}
		got, evaluationErr := engine.EvaluateBinding(binding, context, values.ExpressionOptions{VisibleSteps: []string{"render"}}, computedMetadata)
		if evaluationErr != nil {
			t.Fatalf("EvaluateBinding(%s): %v", name, evaluationErr)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("EvaluateBinding(%s) changed envelope:\ngot  %#v\nwant %#v", name, got, want)
		}
	}
	for name, want := range map[string]values.Value{"secret": secretRef, "report": artifact} {
		binding := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "outputs." + name}}
		got, evaluationErr := engine.EvaluateBinding(binding, context, values.ExpressionOptions{VisibleSteps: []string{}}, computedMetadata)
		if evaluationErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("EvaluateBinding(raw outputs.%s) = %#v, %v; want %#v", name, got, evaluationErr, want)
		}
	}

	_, err = engine.EvaluateBinding(graph.Binding{
		Kind:       graph.BindingExpression,
		Expression: &graph.Expression{Text: `string(steps.render.outputs.secret) + "-derived"`},
	}, context, values.ExpressionOptions{VisibleSteps: []string{"render"}}, computedMetadata)
	if !errors.Is(err, values.ErrSecretDerivation) {
		t.Fatalf("computed secret-ref binding error = %v, want ErrSecretDerivation", err)
	}
	_, err = engine.EvaluateBinding(graph.Binding{
		Kind: graph.BindingInterpolation, Interpolation: `token={{ outputs.secret }}`,
	}, context, values.ExpressionOptions{VisibleSteps: []string{}}, computedMetadata)
	if !errors.Is(err, values.ErrSecretDerivation) {
		t.Fatalf("computed raw-output secret-ref binding error = %v, want ErrSecretDerivation", err)
	}
}

func TestEvaluateBindingRejectsAmbiguousBindingShapes(t *testing.T) {
	metadata := bindingTestMetadata("output", values.RedactionPrivate, values.RetentionRun)
	tests := []graph.Binding{
		{Kind: graph.BindingLiteral, Literal: "value", Expression: &graph.Expression{Text: `"other"`}},
		{Kind: graph.BindingExpression, Literal: "value", Expression: &graph.Expression{Text: `"other"`}},
		{Kind: graph.BindingInterpolation, Literal: "value", Interpolation: "{{ inputs.value }}"},
	}
	for _, binding := range tests {
		if _, err := values.NewExpressionEngine().EvaluateBinding(binding, values.ExpressionContext{}, values.ExpressionOptions{}, metadata); err == nil {
			t.Fatalf("ambiguous binding accepted: %#v", binding)
		}
	}
}

func bindingTestMetadata(reference string, redaction values.RedactionClass, retention values.RetentionClass) values.Metadata {
	return values.Metadata{
		Producer:  values.Producer{Kind: "node_output", Reference: reference, Output: "value"},
		MediaType: "application/json", Redaction: redaction, Retention: retention,
	}
}
