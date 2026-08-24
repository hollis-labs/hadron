package script_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	scriptadapter "github.com/hollis-labs/hadron/workflow/adapters/script"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestExecutorRegistersWithDeterministicCapabilityFreeContract(t *testing.T) {
	t.Parallel()

	executor := scriptadapter.New()
	registry := stepkind.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	registered, spec, err := stepkind.Resolve(registry, scriptadapter.Name, scriptadapter.Version)
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
	first := executor.Spec()
	first.ConfigSchema["mutated"] = true
	first.Effects[0] = graph.EffectDestructive
	second := executor.Spec()
	if second.ConfigSchema["mutated"] != nil || !reflect.DeepEqual(second.Effects, graph.EffectSet{graph.EffectCompute}) {
		t.Fatalf("Spec() leaked mutable metadata: %#v", second)
	}

	limits := executor.Limits()
	if limits != scriptadapter.DefaultResourceLimits() {
		t.Fatalf("Limits() = %#v", limits)
	}
	limits.MaxItems = 0
	if _, err := scriptadapter.NewWithLimits(limits); !errors.Is(err, scriptadapter.ErrInvalidLimits) {
		t.Fatalf("NewWithLimits() error = %v", err)
	}
}

func TestValidateConfigIsDeterministicSourceMappedAndLocalOnly(t *testing.T) {
	t.Parallel()

	executor := scriptadapter.New()
	tests := []struct {
		name   string
		config graph.Config
		want   string
		line   int
	}{
		{name: "nil", config: nil, want: "JSON object"},
		{name: "runtime", config: configFor(`function main(input) { return {}; }`), want: "runtime must be goja"},
		{name: "unknown", config: withConfig(configFor(`function main(input) { return {}; }`), "zzz", true), want: `unsupported field "zzz"`},
		{name: "entrypoint", config: withConfig(validConfig(`function main(input) { return {}; }`), "entrypoint", "not valid"), want: "JavaScript identifier"},
		{name: "missing schema", config: withoutConfig(validConfig(`function main(input) { return {}; }`), "input_schema"), want: "must declare input_schema"},
		{name: "external schema ref", config: withConfig(validConfig(`function main(input) { return {}; }`), "input_schema", map[string]any{"$ref": "https://example.test/schema"}), want: "not a valid local JSON Schema"},
		{name: "syntax", config: validConfig("\nfunction main(input) {\n  return {broken: };\n}"), want: "invalid JavaScript syntax", line: 3},
		{name: "default conflict", config: withConfig(validConfig(`export default function(input) { return {}; }`), "entrypoint", "main"), want: "entrypoint default"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first := executor.ValidateConfig(t.Context(), test.config)
			second := executor.ValidateConfig(t.Context(), test.config)
			if len(first) != 1 || !reflect.DeepEqual(first, second) || !strings.Contains(first[0].Message, test.want) {
				t.Fatalf("ValidateConfig() = %#v / %#v", first, second)
			}
			if err := first[0].Validate(); err != nil {
				t.Fatalf("Diagnostic.Validate() = %v", err)
			}
			if test.line > 0 && (first[0].Source == nil || first[0].Source.StartLine != test.line || !reflect.DeepEqual(first[0].Source.Path, []string{"config", "code"})) {
				t.Fatalf("source = %#v, want line %d", first[0].Source, test.line)
			}
		})
	}

	valid := validConfig(`export default function(input) { return {value: input.value}; }`)
	if findings := executor.ValidateConfig(t.Context(), valid); len(findings) != 0 {
		t.Fatalf("ValidateConfig(valid) = %#v", findings)
	}

	limits := scriptadapter.DefaultResourceLimits()
	limits.MaxSourceBytes = 32
	limited, err := scriptadapter.NewWithLimits(limits)
	if err != nil {
		t.Fatal(err)
	}
	findings := limited.ValidateConfig(t.Context(), validConfig(`function main(input) { return {value: true}; }`))
	if len(findings) != 1 || !strings.Contains(findings[0].Message, "max_source_bytes") {
		t.Fatalf("ValidateConfig(source limit) = %#v", findings)
	}
}

func TestExecuteExportedFunctionReturnsTypedExactPrivateOutputs(t *testing.T) {
	t.Parallel()

	config := graph.Config{
		"runtime": scriptadapter.RuntimeGoja,
		"code": `export default function(input) {
  return {
    title: input.title.trim().toLowerCase(),
    count: input.count + 1,
    exact: input.exact,
    nested: {items: input.items.map(function(item) { return item * 2; })}
  };
}`,
		"input_schema": map[string]any{
			"$ref": "#/$defs/input", "$defs": map[string]any{"input": map[string]any{
				"type": "object", "required": []any{"title", "count", "exact", "items"},
				"properties": map[string]any{
					"title": map[string]any{"type": "string"}, "count": map[string]any{"type": "integer"},
					"exact": map[string]any{"type": "number"}, "items": map[string]any{"type": "array"},
				}, "additionalProperties": false,
			}},
		},
		"output_schema": map[string]any{
			"allOf": []any{
				map[string]any{"type": "object", "required": []any{"title", "count", "exact", "nested"}},
				map[string]any{"properties": map[string]any{"count": map[string]any{"const": json.Number("3")}}},
			},
		},
	}
	inputs := valueSet(t, map[string]any{
		"title": "  HELLO  ", "count": json.Number("2"), "exact": json.Number("123456.125"),
		"items": []any{json.Number("2"), json.Number("4")},
	}, values.RedactionPublic)
	invocation := invocationWith(config, inputs)
	before := stableJSON(t, invocation)

	first, err := scriptadapter.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil {
		t.Fatalf("Execute() = %v (cause: %v)", err, errors.Unwrap(err))
	}
	second, err := scriptadapter.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("Execute(repeated) = %#v, %v", second, err)
	}
	if first.Outcome != stepkind.StepCompleted || first.Validate() != nil {
		t.Fatalf("result = %#v", first)
	}
	if got := stableJSON(t, invocation); !bytes.Equal(got, before) {
		t.Fatalf("invocation mutated:\ngot  %s\nwant %s", got, before)
	}
	assertInline(t, first.Outputs["title"], "hello")
	assertInline(t, first.Outputs["count"], json.Number("3"))
	assertInline(t, first.Outputs["exact"], json.Number("123456.125"))
	assertInline(t, first.Outputs["nested"], map[string]any{"items": []any{json.Number("4"), json.Number("8")}})
	for name, output := range first.Outputs {
		if output.Redaction != values.RedactionPrivate || output.Retention != values.RetentionRun ||
			output.Producer != (values.Producer{Kind: "node_output", Reference: "run-1/script-node/item-2", Output: name}) {
			t.Errorf("output %q metadata = %#v", name, output)
		}
	}

	first.Outputs["nested"].Inline.(map[string]any)["changed"] = true
	third, err := scriptadapter.New().Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := third.Outputs["nested"].Inline.(map[string]any)["changed"]; exists {
		t.Fatal("output mutation leaked into later execution")
	}
}

func TestSandboxDeniesAmbientModulesAndBulkAllocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code string
		want string
	}{
		{name: "filesystem module", code: `function main(input) { return {x: require("fs")}; }`, want: "module loading"},
		{name: "static module", code: `import fs from "fs"; function main(input) { return {}; }`, want: "invalid JavaScript syntax"},
		{name: "network", code: `function main(input) { return {x: fetch(input.url)}; }`, want: "network access"},
		{name: "secret helper", code: `function main(input) { return {x: hadron.secret("token")}; }`, want: "Hadron host access"},
		{name: "clock", code: `function main(input) { return {x: Date.now()}; }`, want: "clock access"},
		{name: "random", code: `function main(input) { return {x: Math.random()}; }`, want: "random access"},
		{name: "constructor escape", code: `function main(input) { return {x: ({}).constructor.constructor("return this")()}; }`, want: "dynamic code"},
		{name: "bulk string", code: `function main(input) { return {x: "x".repeat(1000000)}; }`, want: "bulk allocation"},
		{name: "bulk array call", code: `function main(input) { return {x: Array(1000000)}; }`, want: "bulk allocation"},
		{name: "bulk bigint", code: `function main(input) { return {x: 2n ** 1000000n}; }`, want: "bulk allocation"},
		{name: "regexp", code: `function main(input) { return {x: /x+/.test(input.x)}; }`, want: "regular expression"},
		{name: "regexp constructor", code: `function main(input) { return {x: RegExp("x+").test(input.x)}; }`, want: "regular expression"},
		{name: "new", code: `function main(input) { return {x: new Array(100)}; }`, want: "object construction"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			findings := scriptadapter.New().ValidateConfig(t.Context(), validConfig(test.code))
			if len(findings) != 1 || !strings.Contains(findings[0].Message, test.want) || findings[0].Source == nil {
				t.Fatalf("ValidateConfig() = %#v", findings)
			}
		})
	}

	// Computed member names evade lexical spelling but not the runtime sandbox.
	for _, code := range []string{
		`function main(input) { var key = "ran" + "dom"; return {x: Math[key]()}; }`,
		`function main(input) { var key = "con" + "structor"; return {x: ({})[key]("return this")()}; }`,
		`function main(input) { var key = "re" + "peat"; return {x: "x"[key](1000000)}; }`,
		`function main(input) { var key = "__pro" + "to__"; return {x: ({})[key].toString()}; }`,
		`function main(input) { var key = "proto" + "type"; Object[key].polluted = true; return {}; }`,
	} {
		_, err := scriptadapter.New().Execute(t.Context(), prepared(validConfig(code), emptyValues()))
		assertExecutionCode(t, err, scriptadapter.CodeExecutionFailed)
	}
}

func TestTimeoutCancellationStackAndStructuralLimits(t *testing.T) {
	t.Parallel()

	limits := scriptadapter.DefaultResourceLimits()
	limits.WallTime = 15 * time.Millisecond
	executor, err := scriptadapter.NewWithLimits(limits)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(t.Context(), prepared(validConfig(`function main(input) { for (;;) {} }`), emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeExecutionTimeout)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout does not unwrap context deadline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() {
		<-started
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	close(started)
	_, err = scriptadapter.New().Execute(ctx, prepared(validConfig(`function main(input) { for (;;) {} }`), emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeExecutionCanceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation does not unwrap context.Canceled: %v", err)
	}

	stackLimits := scriptadapter.DefaultResourceLimits()
	stackLimits.MaxCallStack = 8
	stackExecutor, _ := scriptadapter.NewWithLimits(stackLimits)
	_, err = stackExecutor.Execute(t.Context(), prepared(validConfig(`function deep() { return deep(); } function main(input) { return deep(); }`), emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)

	inputLimits := scriptadapter.DefaultResourceLimits()
	inputLimits.MaxInputBytes = 16
	inputExecutor, _ := scriptadapter.NewWithLimits(inputLimits)
	_, err = inputExecutor.Execute(t.Context(), prepared(validConfig(`function main(input) { return {}; }`), valueSet(t, map[string]any{"text": strings.Repeat("x", 32)}, values.RedactionPrivate)))
	assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)

	outputLimits := scriptadapter.DefaultResourceLimits()
	outputLimits.MaxOutputBytes = 16
	outputExecutor, _ := scriptadapter.NewWithLimits(outputLimits)
	_, err = outputExecutor.Execute(t.Context(), prepared(validConfig(`function main(input) { return {text: "abcdefghijklmnopqrstuvwxyz"}; }`), emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)

	depthLimits := scriptadapter.DefaultResourceLimits()
	depthLimits.MaxDepth = 3
	depthExecutor, _ := scriptadapter.NewWithLimits(depthLimits)
	_, err = depthExecutor.Execute(t.Context(), prepared(validConfig(`function main(input) { return {a: {b: {c: true}}}; }`), emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)

	itemLimits := scriptadapter.DefaultResourceLimits()
	itemLimits.MaxItems = 2
	itemExecutor, _ := scriptadapter.NewWithLimits(itemLimits)
	_, err = itemExecutor.Execute(t.Context(), prepared(validConfig(`function main(input) { return {items: [1, 2]}; }`), emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)

	stringLimits := scriptadapter.DefaultResourceLimits()
	stringLimits.MaxStringBytes = 4
	stringExecutor, _ := scriptadapter.NewWithLimits(stringLimits)
	_, err = stringExecutor.Execute(t.Context(), prepared(validConfig(`function main(input) { return {}; }`), valueSet(t, map[string]any{"text": "12345"}, values.RedactionPrivate)))
	assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)
}

func TestSchemasExactNumbersClassificationAndSafeErrors(t *testing.T) {
	t.Parallel()

	large := valueSet(t, map[string]any{"number": json.Number("9007199254740993")}, values.RedactionPrivate)
	_, err := scriptadapter.New().Execute(t.Context(), prepared(validConfig(`function main(input) { return {number: input.number}; }`), large))
	assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)
	if !errors.Is(err, scriptadapter.ErrUnsafeJSONNumber) {
		t.Fatalf("unsafe number cause = %v", err)
	}
	for _, number := range []json.Number{"0.10000000000000000000000000000000001", "1.0000000000000000000000000001e-20"} {
		inputs := valueSet(t, map[string]any{"number": number}, values.RedactionPrivate)
		_, err = scriptadapter.New().Execute(t.Context(), prepared(validConfig(`function main(input) { return {number: input.number}; }`), inputs))
		assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)
		if !errors.Is(err, scriptadapter.ErrUnsafeJSONNumber) {
			t.Fatalf("unsafe fractional number %s cause = %v", number, err)
		}
	}
	for _, number := range []json.Number{"1e-400", "1e-2000000000", json.Number(strings.Repeat("9", 129))} {
		inputs := valueSet(t, map[string]any{"number": number}, values.RedactionPrivate)
		_, err = scriptadapter.New().Execute(t.Context(), prepared(validConfig(`function main(input) { return {number: input.number}; }`), inputs))
		assertExecutionCode(t, err, scriptadapter.CodeResourceLimit)
	}

	_, err = scriptadapter.New().Execute(t.Context(), prepared(validConfig(`function main(input) { return {number: 9007199254740993}; }`), emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeOutputInvalid)

	inputMismatch := validConfig(`function main(input) { throw "must-not-run"; }`)
	inputMismatch["input_schema"] = map[string]any{"type": "object", "required": []any{"required"}}
	_, err = scriptadapter.New().Execute(t.Context(), prepared(inputMismatch, emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeInputInvalid)

	outputMismatch := validConfig(`function main(input) { return {count: 1}; }`)
	outputMismatch["output_schema"] = map[string]any{
		"type": "object", "properties": map[string]any{"count": map[string]any{"type": "string"}},
	}
	_, err = scriptadapter.New().Execute(t.Context(), prepared(outputMismatch, emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeOutputInvalid)

	secretText := "raw-super-secret"
	_, err = scriptadapter.New().Execute(t.Context(), prepared(validConfig("function main(input) {\n  throw \""+secretText+"\";\n}"), emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeExecutionFailed)
	structured := new(stepkind.ExecutionError)
	if !errors.As(err, &structured) {
		t.Fatalf("error = %T %v", err, err)
	}
	encoded, marshalErr := json.Marshal(structured)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(err.Error(), secretText) || strings.Contains(string(encoded), secretText) || structured.Details["line"] != "2" {
		t.Fatalf("persisted error leaked source or lost mapping: %v / %s / %#v", err, encoded, structured.Details)
	}

	secretRef, _ := values.ParseSecretRef("secret://project/service#token")
	secret, _ := values.NewSecretRef(secretRef, metadata("input", values.RedactionSecret))
	_, err = scriptadapter.New().Execute(t.Context(), prepared(validConfig(`function main(input) { return {}; }`), values.ValueSet{"credential": secret}))
	assertExecutionCode(t, err, scriptadapter.CodeCapabilityDenied)
	if strings.Contains(err.Error(), string(secretRef)) {
		t.Fatalf("secret reference leaked in error: %v", err)
	}

	artifactURI := "artifact://private/raw-token-value"
	artifact, artifactErr := values.NewArtifact(values.ArtifactRef{
		Store: "test", URI: artifactURI, Digest: values.SHA256Digest([]byte("artifact bytes")),
		MediaType: "application/octet-stream", SizeBytes: 14,
		Producer:  metadata("artifact-input", values.RedactionPrivate).Producer,
		Redaction: values.RedactionPrivate, Retention: values.RetentionExternal,
	})
	if artifactErr != nil {
		t.Fatal(artifactErr)
	}
	_, err = scriptadapter.New().Execute(t.Context(), prepared(validConfig(`function main(input) { return {}; }`), values.ValueSet{"document": artifact}))
	assertExecutionCode(t, err, scriptadapter.CodeCapabilityDenied)
	if strings.Contains(err.Error(), artifactURI) {
		t.Fatalf("artifact reference leaked in error: %v", err)
	}
}

func TestOutputMustBeExactPlainJSON(t *testing.T) {
	t.Parallel()

	tests := []string{
		`function main(input) { return undefined; }`,
		`function main(input) { return {missing: undefined}; }`,
		`function main(input) { return {number: NaN}; }`,
		`function main(input) { var x = {}; x.self = x; return x; }`,
		`function main(input) { return {callable: function() {}}; }`,
		`function main(input) { return [1, 2, 3]; }`,
		`function main(input) { return {"Bad Name": true}; }`,
	}
	for _, code := range tests {
		_, err := scriptadapter.New().Execute(t.Context(), prepared(validConfig(code), emptyValues()))
		assertExecutionCode(t, err, scriptadapter.CodeOutputInvalid)
	}

	mutation := validConfig(`function main(input) { input.nested.value = 2; return {}; }`)
	inputs := valueSet(t, map[string]any{"nested": map[string]any{"value": json.Number("1")}}, values.RedactionPrivate)
	before := stableJSON(t, inputs)
	_, err := scriptadapter.New().Execute(t.Context(), prepared(mutation, inputs))
	assertExecutionCode(t, err, scriptadapter.CodeExecutionFailed)
	if got := stableJSON(t, inputs); !bytes.Equal(got, before) {
		t.Fatalf("frozen input mutation reached caller: %s / %s", got, before)
	}
}

func TestExecutorIsConcurrentAndTypedNilFailsClosed(t *testing.T) {
	t.Parallel()

	executor := scriptadapter.New()
	config := validConfig(`function main(input) { return {value: input.value + 1}; }`)
	const workers = 32
	inputs := make([]values.ValueSet, workers)
	for worker := range inputs {
		inputs[worker] = valueSet(t, map[string]any{"value": json.Number(fmt.Sprint(worker))}, values.RedactionPrivate)
	}
	var wait sync.WaitGroup
	errorsByWorker := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := executor.Execute(context.Background(), prepared(config, inputs[worker]))
			if err == nil {
				want := json.Number(fmt.Sprint(worker + 1))
				if !reflect.DeepEqual(result.Outputs["value"].Inline, want) {
					err = fmt.Errorf("worker %d output = %#v", worker, result.Outputs["value"].Inline)
				}
			}
			errorsByWorker <- err
		}()
	}
	wait.Wait()
	close(errorsByWorker)
	for err := range errorsByWorker {
		if err != nil {
			t.Fatal(err)
		}
	}

	var nilExecutor *scriptadapter.Executor
	_, err := nilExecutor.Execute(context.Background(), prepared(config, emptyValues()))
	assertExecutionCode(t, err, scriptadapter.CodeRuntimeUnavailable)
}

func validConfig(code string) graph.Config {
	return graph.Config{
		"runtime":       scriptadapter.RuntimeGoja,
		"code":          code,
		"input_schema":  map[string]any{"type": "object"},
		"output_schema": map[string]any{"type": "object"},
	}
}

func configFor(code string) graph.Config {
	return graph.Config{"code": code, "input_schema": map[string]any{}, "output_schema": map[string]any{}}
}

func withConfig(config graph.Config, key string, value any) graph.Config {
	result := cloneConfig(config)
	result[key] = value
	return result
}

func withoutConfig(config graph.Config, key string) graph.Config {
	result := cloneConfig(config)
	delete(result, key)
	return result
}

func cloneConfig(config graph.Config) graph.Config {
	encoded, _ := json.Marshal(config)
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result graph.Config
	_ = decoder.Decode(&result)
	return result
}

func invocationWith(config graph.Config, inputs values.ValueSet) stepkind.Invocation {
	return stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "script-node", Iteration: "item-2", Attempt: 1},
		Config:   config, Inputs: inputs,
	}
}

func prepared(config graph.Config, inputs values.ValueSet) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: invocationWith(config, inputs)}
}

func emptyValues() values.ValueSet { return values.ValueSet{} }

func valueSet(t *testing.T, payload map[string]any, redaction values.RedactionClass) values.ValueSet {
	t.Helper()
	result := make(values.ValueSet, len(payload))
	for name, inline := range payload {
		value, err := values.NewInline(inline, metadata("input-"+name, redaction))
		if err != nil {
			t.Fatal(err)
		}
		result[name] = value
	}
	return result
}

func metadata(reference string, redaction values.RedactionClass) values.Metadata {
	return values.Metadata{
		Producer: values.Producer{Kind: "fixture", Reference: reference}, MediaType: "application/json",
		Redaction: redaction, Retention: values.RetentionRun,
	}
}

func assertExecutionCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", code)
	}
	var structured *stepkind.ExecutionError
	if !errors.As(err, &structured) || structured.Code != code {
		t.Fatalf("error = %T %v, want ExecutionError code %s", err, err, code)
	}
	if validationErr := structured.Validate(); validationErr != nil {
		t.Fatalf("ExecutionError.Validate() = %v", validationErr)
	}
}

func assertInline(t *testing.T, value values.Value, want any) {
	t.Helper()
	if !reflect.DeepEqual(value.Inline, want) {
		t.Fatalf("inline = %T %#v, want %T %#v", value.Inline, value.Inline, want, want)
	}
}

func stableJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
