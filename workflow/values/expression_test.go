package values

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
)

func TestExpressionEngineEvaluatesPredicatesBindingsAndFanOut(t *testing.T) {
	t.Parallel()

	engine := NewExpressionEngine()
	context := expressionTestContext(t)

	condition, err := engine.EvaluateBool(graph.Expression{
		Text: "inputs.enabled && steps.fetch.outputs.status_code == 200",
	}, context, ExpressionOptions{})
	if err != nil {
		t.Fatalf("EvaluateBool failed: %v", err)
	}
	if !condition {
		t.Fatal("if predicate evaluated false")
	}

	items, err := engine.EvaluateRaw(graph.Expression{Text: "inputs.tasks"}, context, ExpressionOptions{})
	if err != nil {
		t.Fatalf("for_each expression failed: %v", err)
	}
	wantItems := []any{
		map[string]any{"id": "a", "score": json.Number("1")},
		map[string]any{"id": "b", "score": json.Number("3")},
	}
	if !reflect.DeepEqual(items, wantItems) {
		t.Fatalf("for_each result = %#v, want %#v", items, wantItems)
	}

	bound, err := engine.EvaluateRaw(graph.Expression{
		Text: "steps.create.outputs.result.id",
	}, context, ExpressionOptions{})
	if err != nil {
		t.Fatalf("output binding failed: %v", err)
	}
	if bound != "created-1" {
		t.Fatalf("output binding = %#v, want created-1", bound)
	}
}

func TestExpressionEngineTransformMapFilterAndAggregate(t *testing.T) {
	t.Parallel()

	result, err := NewExpressionEngine().EvaluateRaw(graph.Expression{
		Text: `{ids: map(inputs.tasks, .id), high: map(filter(inputs.tasks, .score >= 2), .id), total: sum(map(inputs.tasks, .score))}`,
	}, expressionTestContext(t), ExpressionOptions{})
	if err != nil {
		t.Fatalf("transform expression failed: %v", err)
	}
	want := map[string]any{
		"ids":   []any{"a", "b"},
		"high":  []any{"b"},
		"total": json.Number("4"),
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("transform result = %#v, want %#v", result, want)
	}
}

func TestExpressionEngineItemIndexAndHostRoots(t *testing.T) {
	t.Parallel()

	context := expressionTestContext(t)
	item := expressionValue(t, map[string]any{"name": "task"})
	index := 3
	context.Item = &item
	context.Index = &index
	context.Run = map[string]any{"id": "run-9"}
	context.RunScope = map[string]any{"project": "hadron"}
	context.ExecutionTarget = map[string]any{"os": "linux"}

	result, err := NewExpressionEngine().EvaluateRaw(graph.Expression{
		Text: `{item: item.name, index: index, run: run.id, project: run_scope.project, target: execution_target.os}`,
	}, context, ExpressionOptions{})
	if err != nil {
		t.Fatalf("EvaluateRaw failed: %v", err)
	}
	want := map[string]any{
		"item": "task", "index": json.Number("3"), "run": "run-9", "project": "hadron", "target": "linux",
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("host-root result = %#v, want %#v", result, want)
	}
}

func TestExpressionEngineStepItemsAndArtifactReferences(t *testing.T) {
	t.Parallel()

	context := expressionTestContext(t)
	artifactRef := testArtifactRef()
	artifactRef.Redaction = RedactionPrivate
	artifact, err := NewArtifact(artifactRef)
	if err != nil {
		t.Fatal(err)
	}
	context.Steps["render"] = StepContext{
		Outputs: ValueSet{"report": artifact},
		Status:  "succeeded",
		Items: []StepContext{
			{Outputs: ValueSet{"name": expressionValue(t, "first")}, Status: "succeeded"},
			{Outputs: ValueSet{"name": expressionValue(t, "second")}, Status: "failed"},
		},
	}

	result, err := NewExpressionEngine().EvaluateRaw(graph.Expression{
		Text: `{failed: filter(steps.render.items, .status == "failed")[0].outputs.name, uri: steps.render.outputs.report.uri}`,
	}, context, ExpressionOptions{})
	if err != nil {
		t.Fatalf("EvaluateRaw failed: %v", err)
	}
	want := map[string]any{"failed": "second", "uri": "artifact://reports/run-1/report.pdf"}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("step-item/artifact result = %#v, want %#v", result, want)
	}
}

func TestExpressionEnginePreservesLargeJSONNumbers(t *testing.T) {
	t.Parallel()

	large := json.Number("123456789012345678901234567890.0001")
	context := ExpressionContext{Inputs: ValueSet{"large": expressionValue(t, large)}}
	engine := NewExpressionEngine()
	result, err := engine.EvaluateRaw(graph.Expression{Text: "inputs.large"}, context, ExpressionOptions{})
	if err != nil {
		t.Fatalf("EvaluateRaw failed: %v", err)
	}
	if result != large {
		t.Fatalf("large number = %#v, want %#v", result, large)
	}
	interpolated, err := engine.Interpolate("value={{ inputs.large }}", nil, context, ExpressionOptions{})
	if err != nil {
		t.Fatalf("Interpolate failed: %v", err)
	}
	if interpolated != "value="+large.String() {
		t.Fatalf("interpolation = %q", interpolated)
	}
}

func TestExpressionDiagnosticsPreserveSourceAndCategory(t *testing.T) {
	t.Parallel()

	source := graph.SourceRef{
		Format: graph.SourceWorkflow, Locator: "workflow.yaml",
		StartLine: 12, StartColumn: 8, EndLine: 12, EndColumn: 28,
		Path: []string{"nodes", "build", "if"},
	}
	context := expressionTestContext(t)
	tests := []struct {
		name string
		text string
		bool bool
		code diagnostic.Code
	}{
		{name: "syntax", text: "inputs.", code: CodeExpressionSyntax},
		{name: "unresolved", text: "inputs.missing", code: CodeExpressionUnresolved},
		{name: "type mismatch", text: `inputs.name + 1`, code: CodeExpressionType},
		{name: "boolean mismatch", text: `inputs.name`, bool: true, code: CodeExpressionType},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			expression := graph.Expression{Text: test.text, Source: &source}
			var err error
			if test.bool {
				_, err = NewExpressionEngine().EvaluateBool(expression, context, ExpressionOptions{})
			} else {
				_, err = NewExpressionEngine().EvaluateRaw(expression, context, ExpressionOptions{})
			}
			expressionErr := requireExpressionError(t, err, test.code)
			if expressionErr.Diagnostic.Source == nil || !reflect.DeepEqual(*expressionErr.Diagnostic.Source, source) {
				t.Fatalf("diagnostic source = %#v, want %#v", expressionErr.Diagnostic.Source, source)
			}
			if validationErr := expressionErr.Diagnostic.Validate(); validationErr != nil {
				t.Fatalf("diagnostic is invalid: %v", validationErr)
			}
		})
	}
}

func TestExpressionVisibilityDiagnosticIsDistinctFromUnresolved(t *testing.T) {
	t.Parallel()

	source := graph.SourceRef{Format: graph.SourceWorkflow, Locator: "workflow.yaml", StartLine: 4}
	context := expressionTestContext(t)
	engine := NewExpressionEngine()
	_, err := engine.EvaluateRaw(
		graph.Expression{Text: "steps.fetch.outputs.status_code", Source: &source},
		context,
		ExpressionOptions{VisibleSteps: []string{"create"}},
	)
	expressionErr := requireExpressionError(t, err, CodeExpressionInvisibleStep)
	if expressionErr.Diagnostic.Source == nil || !reflect.DeepEqual(*expressionErr.Diagnostic.Source, source) {
		t.Fatalf("diagnostic source = %#v, want %#v", expressionErr.Diagnostic.Source, source)
	}

	_, err = engine.EvaluateRaw(
		graph.Expression{Text: "steps.unknown.outputs.value", Source: &source},
		context,
		ExpressionOptions{VisibleSteps: []string{"create"}},
	)
	requireExpressionError(t, err, CodeExpressionUnresolved)
}

func TestExpressionEnvRequiresExplicitPolicyAndValues(t *testing.T) {
	t.Setenv("WORKFLOW_AMBIENT_ONLY", "must-not-leak")

	context := expressionTestContext(t)
	context.Env = ValueSet{"EXPLICIT": expressionValue(t, "supplied")}
	engine := NewExpressionEngine()
	source := graph.SourceRef{Format: graph.SourceWorkflow, Locator: "workflow.yaml", StartLine: 7}

	_, err := engine.EvaluateRaw(
		graph.Expression{Text: "env.EXPLICIT", Source: &source}, context, ExpressionOptions{},
	)
	expressionErr := requireExpressionError(t, err, CodeExpressionEnvDenied)
	if expressionErr.Diagnostic.Source == nil || !reflect.DeepEqual(*expressionErr.Diagnostic.Source, source) {
		t.Fatalf("env diagnostic source = %#v, want %#v", expressionErr.Diagnostic.Source, source)
	}

	result, err := engine.EvaluateRaw(
		graph.Expression{Text: "env.EXPLICIT"}, context, ExpressionOptions{AllowEnv: true},
	)
	if err != nil {
		t.Fatalf("explicit env evaluation failed: %v", err)
	}
	if result != "supplied" {
		t.Fatalf("explicit env = %#v", result)
	}

	_, err = engine.EvaluateRaw(
		graph.Expression{Text: "env.WORKFLOW_AMBIENT_ONLY"}, context, ExpressionOptions{AllowEnv: true},
	)
	requireExpressionError(t, err, CodeExpressionUnresolved)
}

func TestRawExpressionRejectsInterpolationMarkers(t *testing.T) {
	t.Parallel()

	_, err := NewExpressionEngine().EvaluateRaw(
		graph.Expression{Text: "{{ inputs.name }}"}, expressionTestContext(t), ExpressionOptions{},
	)
	requireExpressionError(t, err, CodeExpressionSyntax)
}

func TestInterpolationMultipleSegmentsAndDeterministicConversion(t *testing.T) {
	t.Parallel()

	context := expressionTestContext(t)
	index := 2
	context.Index = &index
	engine := NewExpressionEngine()
	result, err := engine.Interpolate(
		`name={{ inputs.name }} index={{ index }} active={{ inputs.enabled }}`,
		nil,
		context,
		ExpressionOptions{},
	)
	if err != nil {
		t.Fatalf("Interpolate failed: %v", err)
	}
	if result != "name=example index=2 active=true" {
		t.Fatalf("interpolation = %q", result)
	}

	result, err = engine.Interpolate(
		`object={{ string(inputs.object) }}`,
		nil,
		context,
		ExpressionOptions{},
	)
	if err != nil {
		t.Fatalf("explicit conversion failed: %v", err)
	}
	if result != `object={"a":1,"b":2}` {
		t.Fatalf("deterministic object interpolation = %q", result)
	}
}

func TestInterpolationRejectsMalformedAndNonStringableResults(t *testing.T) {
	t.Parallel()

	context := expressionTestContext(t)
	engine := NewExpressionEngine()
	for _, template := range []string{
		"before {{ inputs.name",
		"before inputs.name }}",
		"before {{   }} after",
		"before {{ {{ inputs.name }} }}",
	} {
		_, err := engine.Interpolate(template, nil, context, ExpressionOptions{})
		requireExpressionError(t, err, CodeInterpolation)
	}

	_, err := engine.Interpolate("tasks={{ inputs.tasks }}", nil, context, ExpressionOptions{})
	requireExpressionError(t, err, CodeInterpolation)
}

func TestInterpolationParsesCompositeLiteralsAndQuotedMarkers(t *testing.T) {
	t.Parallel()

	engine := NewExpressionEngine()
	result, err := engine.Interpolate(
		`{{ string({z: "}}", a: {nested: true}}) }}`,
		nil,
		expressionTestContext(t),
		ExpressionOptions{},
	)
	if err != nil {
		t.Fatalf("Interpolate failed: %v", err)
	}
	if result != `{"a":{"nested":true},"z":"}}"}` {
		t.Fatalf("composite interpolation = %q", result)
	}
}

func TestParseReferencesReturnsStableStructuralReferences(t *testing.T) {
	t.Parallel()

	references, err := ParseReferences(graph.Expression{
		Text: `steps.fetch.outputs.result.id == inputs.expected && env.FLAG == "yes" && steps[run.step_id].status == "ok"`,
	})
	if err != nil {
		t.Fatalf("ParseReferences failed: %v", err)
	}
	want := []Reference{
		{Root: "env", Path: []string{"FLAG"}},
		{Root: "inputs", Path: []string{"expected"}},
		{Root: "run", Path: []string{"step_id"}},
		{Root: "steps", Path: []string{"fetch", "outputs", "result", "id"}},
		{Root: "steps", Path: []string{"status"}, Dynamic: true},
	}
	if !reflect.DeepEqual(references, want) {
		t.Fatalf("references = %#v, want %#v", references, want)
	}
}

func TestParseInterpolationReferencesReturnsOnlyExpressionReferences(t *testing.T) {
	t.Parallel()

	source := &graph.SourceRef{Format: graph.SourceWorkflow, Locator: "workflow.yaml", StartLine: 9}
	references, err := ParseInterpolationReferences(
		`literal steps.hidden {{ steps.fetch.outputs.value }} / {{ inputs.name }} / {{ steps.fetch.status }}`,
		source,
	)
	if err != nil {
		t.Fatalf("ParseInterpolationReferences failed: %v", err)
	}
	want := []Reference{
		{Root: "inputs", Path: []string{"name"}},
		{Root: "steps", Path: []string{"fetch", "outputs", "value"}},
		{Root: "steps", Path: []string{"fetch", "status"}},
	}
	if !reflect.DeepEqual(references, want) {
		t.Fatalf("references = %#v, want %#v", references, want)
	}

	_, err = ParseInterpolationReferences("{{ steps.fetch.outputs.value", source)
	expressionErr := requireExpressionError(t, err, CodeInterpolation)
	if !reflect.DeepEqual(expressionErr.Diagnostic.Source, source) {
		t.Fatalf("source = %#v, want %#v", expressionErr.Diagnostic.Source, source)
	}
}

func TestParseReferencesDoesNotReportDynamicRootAsStaticProducer(t *testing.T) {
	t.Parallel()

	references, err := ParseReferences(graph.Expression{Text: "steps[run.step_id].status"})
	if err != nil {
		t.Fatal(err)
	}
	want := []Reference{
		{Root: "run", Path: []string{"step_id"}},
		{Root: "steps", Path: []string{"status"}, Dynamic: true},
	}
	if !reflect.DeepEqual(references, want) {
		t.Fatalf("references = %#v, want %#v", references, want)
	}
}

func TestExpressionProgramCacheIsPolicyAndShapeSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	engine := NewExpressionEngine()
	context := expressionTestContext(t)
	expression := graph.Expression{Text: "inputs.enabled && steps.fetch.status == 'succeeded'"}
	const workers = 100
	var wait sync.WaitGroup
	errorsByWorker := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := engine.EvaluateBool(expression, context, ExpressionOptions{})
			if err != nil {
				errorsByWorker <- err
				return
			}
			if !result {
				errorsByWorker <- errors.New("cached predicate evaluated false")
			}
		}()
	}
	wait.Wait()
	close(errorsByWorker)
	for err := range errorsByWorker {
		t.Error(err)
	}

	visible := ExpressionOptions{VisibleSteps: []string{"fetch"}}
	if _, err := engine.EvaluateBool(expression, context, visible); err != nil {
		t.Fatalf("visible cached evaluation failed: %v", err)
	}
	_, err := engine.EvaluateBool(expression, context, ExpressionOptions{VisibleSteps: []string{"create"}})
	requireExpressionError(t, err, CodeExpressionInvisibleStep)

	stringContext := expressionTestContext(t)
	stringContext.Inputs["enabled"] = expressionValue(t, "yes")
	_, err = engine.EvaluateBool(expression, stringContext, ExpressionOptions{})
	requireExpressionError(t, err, CodeExpressionType)
}

func TestExpressionProgramCacheTreatsStructurallyHeterogeneousArraysAsAny(t *testing.T) {
	t.Parallel()

	engine := NewExpressionEngine()
	large := json.Number("123456789012345678901234567890")
	forward := ExpressionContext{Inputs: ValueSet{
		"records": expressionValue(t, []any{
			map[string]any{"value": "match"},
			map[string]any{"value": large},
		}),
	}}
	reversed := ExpressionContext{Inputs: ValueSet{
		"records": expressionValue(t, []any{
			map[string]any{"value": large},
			map[string]any{"value": "match"},
		}),
	}}
	expression := graph.Expression{Text: `map(inputs.records, .value == "match")`}

	forwardResult, err := engine.EvaluateRaw(expression, forward, ExpressionOptions{})
	if err != nil {
		t.Fatalf("forward evaluation failed: %v", err)
	}
	if want := []any{true, false}; !reflect.DeepEqual(forwardResult, want) {
		t.Fatalf("forward result = %#v, want %#v", forwardResult, want)
	}

	reversedResult, err := engine.EvaluateRaw(expression, reversed, ExpressionOptions{})
	if err != nil {
		t.Fatalf("reversed evaluation failed after shared cache use: %v", err)
	}
	if want := []any{false, true}; !reflect.DeepEqual(reversedResult, want) {
		t.Fatalf("reversed result = %#v, want %#v", reversedResult, want)
	}
}

func TestExpressionResultRejectsUnsupportedNativeValues(t *testing.T) {
	t.Parallel()

	_, err := NewExpressionEngine().EvaluateRaw(
		graph.Expression{Text: `date("2026-08-24")`}, expressionTestContext(t), ExpressionOptions{},
	)
	requireExpressionError(t, err, CodeExpressionValue)
}

func TestExpressionEngineDisablesAmbientTimeAndProducesDeterministicResults(t *testing.T) {
	t.Parallel()

	engine := NewExpressionEngine()
	context := expressionTestContext(t)
	_, err := engine.EvaluateRaw(graph.Expression{Text: "now()"}, context, ExpressionOptions{})
	requireExpressionError(t, err, CodeExpressionUnresolved)

	expression := graph.Expression{Text: `{name: inputs.name, object: inputs.object, ids: map(inputs.tasks, .id)}`}
	first, err := engine.EvaluateRaw(expression, context, ExpressionOptions{})
	if err != nil {
		t.Fatalf("first evaluation failed: %v", err)
	}
	for iteration := 0; iteration < 20; iteration++ {
		result, evaluationErr := engine.EvaluateRaw(expression, context, ExpressionOptions{})
		if evaluationErr != nil {
			t.Fatalf("evaluation %d failed: %v", iteration, evaluationErr)
		}
		if !reflect.DeepEqual(result, first) {
			t.Fatalf("evaluation %d = %#v, want %#v", iteration, result, first)
		}
	}
}

func expressionTestContext(t *testing.T) ExpressionContext {
	t.Helper()
	return ExpressionContext{
		Inputs: ValueSet{
			"enabled": expressionValue(t, true),
			"name":    expressionValue(t, "example"),
			"object":  expressionValue(t, map[string]any{"b": 2, "a": 1}),
			"tasks": expressionValue(t, []any{
				map[string]any{"id": "a", "score": 1},
				map[string]any{"id": "b", "score": 3},
			}),
		},
		Steps: map[string]StepContext{
			"fetch": {
				Outputs: ValueSet{"status_code": expressionValue(t, 200)},
				Status:  "succeeded",
			},
			"create": {
				Outputs: ValueSet{"result": expressionValue(t, map[string]any{"id": "created-1"})},
				Status:  "succeeded",
			},
		},
	}
}

func expressionValue(t *testing.T, inline any) Value {
	t.Helper()
	value, err := NewInline(inline, testMetadata(RedactionPublic, RetentionRun))
	if err != nil {
		t.Fatalf("NewInline(%#v): %v", inline, err)
	}
	return value
}

func requireExpressionError(t *testing.T, err error, code diagnostic.Code) *ExpressionError {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", code)
	}
	var expressionErr *ExpressionError
	if !errors.As(err, &expressionErr) {
		t.Fatalf("error = %T %v, want *ExpressionError", err, err)
	}
	if expressionErr.Diagnostic.Code != code {
		t.Fatalf("diagnostic code = %s, want %s: %v", expressionErr.Diagnostic.Code, code, err)
	}
	return expressionErr
}
