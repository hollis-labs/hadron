package transform_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type expressionFixtures struct {
	Success []successFixture `json:"success"`
	Failure []failureFixture `json:"failure"`
}

type successFixture struct {
	Name     string         `json:"name"`
	Config   graph.Config   `json:"config"`
	Inputs   map[string]any `json:"inputs"`
	Expected map[string]any `json:"expected"`
}

type failureFixture struct {
	Name       string         `json:"name"`
	Config     graph.Config   `json:"config"`
	Inputs     map[string]any `json:"inputs"`
	Diagnostic string         `json:"diagnostic"`
}

func TestExecutorRegistersWithPureDeterministicMetadata(t *testing.T) {
	t.Parallel()

	executor := transform.New()
	registry := stepkind.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	registered, spec, err := stepkind.Resolve(registry, transform.Name, transform.Version)
	if err != nil || registered != executor {
		t.Fatalf("Resolve() = %#v, %#v, %v", registered, spec, err)
	}
	if !reflect.DeepEqual(spec.Effects, graph.EffectSet{graph.EffectCompute}) ||
		spec.Idempotency != graph.IdempotencyIntrinsic || spec.RetrySafety != stepkind.RetrySafe ||
		spec.Cancellation.Mode != stepkind.CancellationContext || spec.CanSuspend ||
		!spec.EmbeddedModeSupported || len(spec.RequiredCapabilities) != 0 {
		t.Fatalf("Spec() = %#v", spec)
	}
	if spec.ConfigSchema["type"] != "object" || spec.InputSchema["type"] != "object" || spec.OutputSchema["type"] != "object" {
		t.Fatalf("schemas = %#v / %#v / %#v", spec.ConfigSchema, spec.InputSchema, spec.OutputSchema)
	}

	// Direct Spec calls also return fresh schema/effect values.
	first := executor.Spec()
	first.ConfigSchema["mutated"] = true
	first.Effects[0] = graph.EffectDestructive
	second := executor.Spec()
	if second.ConfigSchema["mutated"] != nil || !reflect.DeepEqual(second.Effects, graph.EffectSet{graph.EffectCompute}) {
		t.Fatalf("Spec() leaked mutable state: %#v", second)
	}
}

func TestValidateConfigRejectsMalformedAndUnsupportedExpressionsDeterministically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config graph.Config
		want   string
	}{
		{name: "empty", config: graph.Config{}, want: "at least one"},
		{name: "non-normalized output", config: graph.Config{"Bad Name": "inputs.value"}, want: "normalized"},
		{name: "non-string", config: graph.Config{"result": json.Number("1")}, want: "must be a string"},
		{name: "empty expression", config: graph.Config{"result": "  "}, want: "syntax is invalid"},
		{name: "interpolation", config: graph.Config{"result": "{{ inputs.value }}"}, want: "syntax is invalid"},
		{name: "env", config: graph.Config{"result": "env.TOKEN"}, want: "env references"},
		{name: "now", config: graph.Config{"result": "now()"}, want: "now()"},
		{name: "adapter call", config: graph.Config{"result": "http(inputs.url)"}, want: "http()"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first := transform.New().ValidateConfig(t.Context(), test.config)
			second := transform.New().ValidateConfig(t.Context(), test.config)
			if len(first) != 1 || !reflect.DeepEqual(first, second) || first[0].Code != stepkind.CodeInvalidConfig ||
				!strings.Contains(first[0].Message, test.want) || first[0].Remediation == nil {
				t.Fatalf("ValidateConfig() = %#v / %#v", first, second)
			}
			if err := first[0].Validate(); err != nil {
				t.Fatalf("diagnostic.Validate() = %v", err)
			}
		})
	}

	config := graph.Config{"z-last": "inputs.value", "a-first": "inputs.value", "middle": 42}
	findings := transform.New().ValidateConfig(t.Context(), config)
	if len(findings) != 1 || !strings.Contains(findings[0].Message, `"middle"`) {
		t.Fatalf("sorted findings = %#v", findings)
	}
	multiple := transform.New().ValidateConfig(t.Context(), graph.Config{"z-last": 1, "a-first": 1})
	if len(multiple) != 2 || !strings.Contains(multiple[0].Message, `"a-first"`) ||
		!strings.Contains(multiple[1].Message, `"z-last"`) {
		t.Fatalf("multi-finding order = %#v", multiple)
	}
}

func TestExecuteFixtureExpressionsAreTypedPersistableExactAndDeterministic(t *testing.T) {
	t.Parallel()

	fixtures := loadFixtures(t)
	for _, fixture := range fixtures.Success {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			t.Parallel()
			invocation := fixtureInvocation(t, fixture.Config, fixture.Inputs)
			before := stableJSON(t, invocation)
			first, err := transform.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
			if err != nil {
				t.Fatalf("Execute() = %v (cause: %v)", err, errors.Unwrap(err))
			}
			second, err := transform.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
			if err != nil {
				t.Fatalf("Execute(repeated) = %v", err)
			}
			if first.Outcome != stepkind.StepCompleted {
				t.Fatalf("result outcome = %#v", first)
			}
			if validationErr := first.Validate(); validationErr != nil {
				t.Fatalf("result = %#v, Validate() = %v", first, validationErr)
			}
			if got := stableJSON(t, invocation); !bytes.Equal(got, before) {
				t.Fatalf("Execute mutated invocation:\ngot  %s\nwant %s", got, before)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("repeated result differs:\nfirst  %#v\nsecond %#v", first, second)
			}
			reordered := fixtureInvocation(t, reverseConfig(fixture.Config), reverseObject(fixture.Inputs))
			third, err := transform.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: reordered})
			if err != nil || !reflect.DeepEqual(first, third) {
				t.Fatalf("map-order result differs: %#v / %v", third, err)
			}
			gotDigest, err := values.DigestValueSet(first.Outputs)
			if err != nil {
				t.Fatal(err)
			}
			wantDigest, err := values.DigestValueSet(second.Outputs)
			if err != nil || gotDigest != wantDigest {
				t.Fatalf("digests = %q / %q, %v", gotDigest, wantDigest, err)
			}
			if got := unwrapOutputs(first.Outputs); !reflect.DeepEqual(got, fixture.Expected) {
				t.Fatalf("outputs = %#v, want %#v", got, fixture.Expected)
			}
			if large, exists := first.Outputs["large"]; exists {
				number, ok := large.Inline.(json.Number)
				if !ok || number.String() != "123456789012345678901234567890.0001" {
					t.Fatalf("large number = %T %#v", large.Inline, large.Inline)
				}
			}
			for name, output := range first.Outputs {
				if output.Producer != (values.Producer{Kind: "node_output", Reference: "run-1/summarize/iteration-2", Output: name}) ||
					output.Redaction != values.RedactionPrivate || output.Retention != values.RetentionRun ||
					output.MediaType != "application/json" {
					t.Errorf("output %q metadata = %#v", name, output)
				}
			}
		})
	}
}

func TestExecuteUsesScopedStepsAndFanOutItemContext(t *testing.T) {
	t.Parallel()

	contextValue := values.ExpressionContext{
		Steps: map[string]values.StepContext{
			"create": {
				Status: "succeeded",
				Items: []values.StepContext{
					{Status: "succeeded", Outputs: valueSet(t, "id", "task-1")},
					{Status: "failed", Outputs: valueSet(t, "id", "task-2")},
				},
			},
		},
	}
	item := inlineValue(t, "item", map[string]any{"title": "Current task"})
	index := 4
	contextValue.Item, contextValue.Index = &item, &index
	provider := transform.ContextProviderFunc(func(_ context.Context, invocation stepkind.Invocation) (values.ExpressionContext, error) {
		// Mutating the provider's invocation copy cannot rewrite authoritative
		// caller inputs used during evaluation.
		invocation.Inputs["label"] = inlineValue(t, "label", "mutated")
		return contextValue, nil
	})
	executor, err := transform.NewWithContextProvider(provider)
	if err != nil {
		t.Fatal(err)
	}
	invocation := fixtureInvocation(t, graph.Config{
		"created":      "map(steps.create.items, .outputs.id)",
		"failed-count": `len(filter(steps.create.items, .status == "failed"))`,
		"item-title":   "item.title",
		"item-index":   "index",
		"label":        "inputs.label",
	}, map[string]any{"label": "original"})
	result, err := executor.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"created": []any{"task-1", "task-2"}, "failed-count": json.Number("1"),
		"item-title": "Current task", "item-index": json.Number("4"), "label": "original",
	}
	if got := unwrapOutputs(result.Outputs); !reflect.DeepEqual(got, want) {
		t.Fatalf("fan-out outputs = %#v, want %#v", got, want)
	}
	if invocation.Inputs["label"].Inline != "original" {
		t.Fatalf("provider mutated caller invocation: %#v", invocation.Inputs)
	}
}

func TestContextProviderCannotExposeDispatcherPrivateRawOutputsRoot(t *testing.T) {
	t.Parallel()
	executor, err := transform.NewWithContextProvider(transform.ContextProviderFunc(func(context.Context, stepkind.Invocation) (values.ExpressionContext, error) {
		return values.ExpressionContext{Outputs: valueSet(t, "native", "must-not-be-visible")}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: fixtureInvocation(t,
		graph.Config{"result": "outputs.native"}, map[string]any{},
	)})
	assertExpressionFailure(t, err, values.CodeExpressionUnresolved, "result")
}

func TestDefaultContextAndExpressionFailuresAreStructured(t *testing.T) {
	t.Parallel()

	for _, fixture := range loadFixtures(t).Failure {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			t.Parallel()
			_, err := transform.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: fixtureInvocation(t, fixture.Config, fixture.Inputs)})
			assertExpressionFailure(t, err, diagnostic.Code(fixture.Diagnostic), "result")
		})
	}

	_, err := transform.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: fixtureInvocation(t,
		graph.Config{"created": "map(steps.create.items, .outputs.id)"}, map[string]any{},
	)})
	assertExpressionFailure(t, err, values.CodeExpressionUnresolved, "created")
}

func TestProviderFailuresAreCausePreservingAndMessageSafe(t *testing.T) {
	t.Parallel()

	secretCause := errors.New("resolved token should not be persisted")
	executor, err := transform.NewWithContextProvider(transform.ContextProviderFunc(
		func(context.Context, stepkind.Invocation) (values.ExpressionContext, error) {
			return values.ExpressionContext{}, secretCause
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: fixtureInvocation(t,
		graph.Config{"result": "inputs.value"}, map[string]any{"value": true},
	)})
	var typed *stepkind.ExecutionError
	if !errors.As(err, &typed) || typed.Code != transform.CodeContextUnavailable ||
		!errors.Is(err, secretCause) || strings.Contains(err.Error(), "token") ||
		typed.Classification != stepkind.Retryable {
		t.Fatalf("provider error = %T %#v / %v", err, typed, err)
	}
	if validationErr := typed.Validate(); validationErr != nil {
		t.Fatalf("ExecutionError.Validate() = %v", validationErr)
	}
}

func TestExecuteObservesCancellationAndIsConcurrentSafe(t *testing.T) {
	executor := transform.New()
	invocation := fixtureInvocation(t, graph.Config{"result": "len(inputs.items)"}, map[string]any{
		"items": []any{"a", "b", "c"},
	})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := executor.Execute(canceled, stepkind.PreparedInvocation{Invocation: invocation}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute(canceled) = %v", err)
	}

	const workers = 24
	var wait sync.WaitGroup
	wait.Add(workers)
	errorsSeen := make(chan error, workers)
	for range workers {
		go func() {
			defer wait.Done()
			result, err := executor.Execute(context.Background(), stepkind.PreparedInvocation{Invocation: invocation})
			if err == nil && result.Outputs["result"].Inline != json.Number("3") {
				err = errors.New("unexpected concurrent result")
			}
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestEvaluatedOutputsAreDefensiveCopies(t *testing.T) {
	t.Parallel()

	invocation := fixtureInvocation(t, graph.Config{"copy": "inputs.object"}, map[string]any{
		"object": map[string]any{"nested": []any{"original"}},
	})
	result, err := transform.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil {
		t.Fatal(err)
	}
	copyObject := result.Outputs["copy"].Inline.(map[string]any)
	copyObject["nested"].([]any)[0] = "changed"
	inputObject := invocation.Inputs["object"].Inline.(map[string]any)
	if inputObject["nested"].([]any)[0] != "original" {
		t.Fatalf("output aliases input: %#v", invocation.Inputs["object"])
	}
}

func TestOutputsRemainRuntimeSchemaValidatable(t *testing.T) {
	t.Parallel()

	executor := transform.New()
	result, err := executor.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: fixtureInvocation(t,
		graph.Config{"count": "len(inputs.items)", "names": "inputs.items"},
		map[string]any{"items": []any{"one", "two"}},
	)})
	if err != nil {
		t.Fatal(err)
	}
	if err := values.ValidateValueSetSchema(executor.Spec().OutputSchema, result.Outputs); err != nil {
		t.Fatalf("registered output schema rejected result: %v", err)
	}
	declared := graph.Schema{
		"type": "object", "required": []any{"count", "names"},
		"properties": map[string]any{
			"count": map[string]any{"type": "integer"},
			"names": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		},
		"additionalProperties": false,
	}
	if err := values.ValidateValueSetSchema(declared, result.Outputs); err != nil {
		t.Fatalf("declared output schema rejected result: %v", err)
	}
	wrong := graph.Schema{
		"type": "object", "properties": map[string]any{"count": map[string]any{"type": "string"}},
	}
	if err := values.ValidateValueSetSchema(wrong, result.Outputs); !errors.Is(err, values.ErrSchemaMismatch) {
		t.Fatalf("wrong declared schema error = %v", err)
	}
}

func TestNewWithContextProviderRejectsTypedNil(t *testing.T) {
	t.Parallel()

	var provider *nilProvider
	if _, err := transform.NewWithContextProvider(provider); err == nil {
		t.Fatal("NewWithContextProvider accepted typed nil")
	}
}

type nilProvider struct{}

func (*nilProvider) ExpressionContext(context.Context, stepkind.Invocation) (values.ExpressionContext, error) {
	return values.ExpressionContext{}, nil
}

func assertExpressionFailure(t *testing.T, err error, want diagnostic.Code, output string) {
	t.Helper()
	var execution *stepkind.ExecutionError
	if !errors.As(err, &execution) || execution.Code != transform.CodeExpressionFailed ||
		execution.Classification != stepkind.RetryPermanent || execution.Details["output"] != output ||
		execution.Details["expression"] != "config."+output {
		t.Fatalf("execution error = %T %#v / %v", err, execution, err)
	}
	var expression *values.ExpressionError
	if !errors.As(err, &expression) || expression.Diagnostic.Code != want || expression.Diagnostic.Source != nil {
		t.Fatalf("expression cause = %T %#v / %v", err, expression, err)
	}
	if validationErr := execution.Validate(); validationErr != nil {
		t.Fatalf("ExecutionError.Validate() = %v", validationErr)
	}
}

func fixtureInvocation(t *testing.T, config graph.Config, inputs map[string]any) stepkind.Invocation {
	t.Helper()
	set := make(values.ValueSet, len(inputs))
	for name, raw := range inputs {
		set[name] = inlineValue(t, name, raw)
	}
	return stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "summarize", Iteration: "iteration-2", Attempt: 1},
		Config:   config, Inputs: set,
	}
}

func inlineValue(t *testing.T, output string, raw any) values.Value {
	t.Helper()
	value, err := values.NewInline(raw, values.Metadata{
		Producer:  values.Producer{Kind: "fixture", Reference: "fixture-1", Output: output},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func valueSet(t *testing.T, name string, raw any) values.ValueSet {
	t.Helper()
	return values.ValueSet{name: inlineValue(t, name, raw)}
}

func loadFixtures(t *testing.T) expressionFixtures {
	t.Helper()
	encoded, err := os.ReadFile("testdata/expression-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var fixtures expressionFixtures
	if err := decoder.Decode(&fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures.Success) == 0 || len(fixtures.Failure) == 0 {
		t.Fatalf("empty fixtures = %#v", fixtures)
	}
	return fixtures
}

func unwrapOutputs(outputs values.ValueSet) map[string]any {
	names := make([]string, 0, len(outputs))
	for name := range outputs {
		names = append(names, name)
	}
	sort.Strings(names)
	unwrapped := make(map[string]any, len(outputs))
	for _, name := range names {
		unwrapped[name] = outputs[name].Inline
	}
	return unwrapped
}

func stableJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func reverseConfig(config graph.Config) graph.Config {
	names := make([]string, 0, len(config))
	for name := range config {
		names = append(names, name)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	reversed := make(graph.Config, len(config))
	for _, name := range names {
		reversed[name] = config[name]
	}
	return reversed
}

func reverseObject(input map[string]any) map[string]any {
	names := make([]string, 0, len(input))
	for name := range input {
		names = append(names, name)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	reversed := make(map[string]any, len(input))
	for _, name := range names {
		reversed[name] = input[name]
	}
	return reversed
}
