package offline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/offline"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const pureSource = `workflow:
  name: Offline Double
  version: 1.0.0
inputs:
  - name: value
    type: integer
    required: true
steps:
  - id: double
    kind: transform
    kind_version: v1
    with:
      value:
        expression: inputs.value
    config:
      result: inputs.value * 2
    outputs:
      result:
        type: integer
outputs:
  result:
    type: integer
    value:
      expression: steps.double.outputs.result
`

func TestBuildIsCanonicalBoundedAndDefensivelyOwned(t *testing.T) {
	plan, registry := purePlan(t)
	first, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI})
	if err != nil || len(first.Diagnostics) != 0 || first.Manifest == nil {
		t.Fatalf("Build() = %#v, %v", first, err)
	}
	second, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI})
	if err != nil || !bytes.Equal(first.Bytes, second.Bytes) {
		t.Fatalf("rebuild differs: %v", err)
	}
	if first.Manifest.PlanDigest != plan.Digest || first.Manifest.BuildDigest == plan.Digest || len(first.Manifest.Inputs) != 1 || len(first.Manifest.Outputs) != 1 {
		t.Fatalf("manifest metadata = %#v", first.Manifest)
	}
	plan.Graph.Inputs[0].Name = "mutated"
	if first.Manifest.Inputs[0].Name != "value" || first.Manifest.Plan.Graph.Inputs[0].Name != "value" {
		t.Fatal("manifest aliases caller plan")
	}
	parsed, err := offline.ParseManifest(first.Bytes)
	if err != nil || parsed.BuildDigest != first.Manifest.BuildDigest {
		t.Fatalf("ParseManifest() = %#v, %v", parsed, err)
	}
	tampered := append([]byte(nil), first.Bytes...)
	tampered[len(tampered)-2] ^= 1
	if _, err := offline.ParseManifest(tampered); err == nil {
		t.Fatal("ParseManifest accepted tampering")
	}
}

func TestParseManifestRejectsInternallyInconsistentAndMalformedEnvelopes(t *testing.T) {
	plan, registry := purePlan(t)
	built := buildPlan(t, plan, registry)
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"input projection", func(root map[string]any) { root["inputs"].([]any)[0].(map[string]any)["name"] = "wrong" }},
		{"plan content", func(root map[string]any) { root["plan"].(map[string]any)["id"] = "wrong" }},
		{"visibility", func(root map[string]any) { root["visibility"].(map[string]any)["nodes"] = map[string]any{} }},
		{"kind coverage", func(root map[string]any) { root["step_kinds"] = []any{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var root map[string]any
			if err := decodeNumberJSON(built.Bytes, &root); err != nil {
				t.Fatal(err)
			}
			test.mutate(root)
			root["build_digest"] = ""
			unsigned, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			root["build_digest"] = values.SHA256Digest(unsigned)
			forged, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := offline.ParseManifest(forged); err == nil {
				t.Fatal("ParseManifest accepted internally inconsistent envelope")
			}
		})
	}
	unknown := append([]byte(nil), built.Bytes[:len(built.Bytes)-1]...)
	unknown = append(unknown, []byte(`,"unknown":true}`)...)
	for _, malformed := range [][]byte{nil, []byte(`{`), []byte(`[]`), unknown, append([]byte(" \n"), built.Bytes...)} {
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("ParseManifest panicked for %q: %v", malformed, recovered)
				}
			}()
			if _, err := offline.ParseManifest(malformed); err == nil {
				t.Fatalf("ParseManifest accepted malformed envelope %q", malformed)
			}
		}()
	}
}

func TestBuildRejectsUnsupportedKindsEffectsAndMissingFunctionalBindings(t *testing.T) {
	tests := []struct {
		name    string
		spec    stepkind.StepKindSpec
		binding *offline.ExternalBinding
		catalog offline.BindingCatalog
		code    diagnostic.Code
	}{
		{name: "embedded", spec: testSpec("custom", false, false, graph.EffectSet{graph.EffectCompute}), code: offline.CodeEmbeddedUnsupported},
		{name: "unsafe", spec: testSpec("custom", true, false, graph.EffectSet{graph.EffectMutate}), code: offline.CodeBindingRequired},
		{name: "wait", spec: testSpec("custom", true, true, graph.EffectSet{graph.EffectRead}), code: offline.CodeWaitServiceRequired},
		{name: "binding-without-bridge", spec: testSpec("custom", true, true, graph.EffectSet{graph.EffectRead}), binding: &offline.ExternalBinding{NodeID: "work", Kind: "custom", Version: "v1", Driver: "remote"}, code: offline.CodeBindingInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := customPlan(t, "custom")
			registry := stepkind.NewRegistry()
			if err := registry.Register(&staticKind{spec: test.spec}); err != nil {
				t.Fatal(err)
			}
			var bindings []offline.ExternalBinding
			if test.binding != nil {
				bindings = append(bindings, *test.binding)
			}
			result, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI, Bindings: bindings, BindingCatalog: test.catalog})
			if err != nil {
				t.Fatal(err)
			}
			if !hasCode(result.Diagnostics, test.code) {
				t.Fatalf("diagnostics = %#v, want %s", result.Diagnostics, test.code)
			}
		})
	}

	plan := customPlan(t, "mcp")
	registry := stepkind.NewRegistry()
	spec := testSpec("mcp", true, false, graph.EffectSet{graph.EffectMutate})
	spec.RequiredCapabilities = []string{"mcp.client"}
	if err := registry.Register(&staticKind{spec: spec}); err != nil {
		t.Fatal(err)
	}
	binding := offline.ExternalBinding{NodeID: "work", Kind: "mcp", Version: "v1", Driver: offline.DriverRemoteDaemonHTTP, Effects: graph.EffectSet{graph.EffectRead}, Capabilities: []string{"mcp.client"}, Config: graph.Config{"endpoint": "https://daemon.example.test/execute"}}
	catalog := offline.BindingCatalogFunc(func(context.Context, offline.ExternalBinding, graph.Node, stepkind.StepKindSpec) (offline.BindingDescription, error) {
		return offline.BindingDescription{EffectiveEffects: graph.EffectSet{graph.EffectRead}, Capabilities: []string{"mcp.client"}}, nil
	})
	rejectedFloor, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI, Bindings: []offline.ExternalBinding{binding}, BindingCatalog: catalog})
	if err != nil || !hasCode(rejectedFloor.Diagnostics, offline.CodeUnsupportedEffect) {
		t.Fatalf("unsafe MCP effect floor diagnostics = %#v, %v", rejectedFloor.Diagnostics, err)
	}
	executionRegistry, err := offline.AdaptExecutionRegistry(*plan, registry, []offline.ExternalBinding{binding})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: executionRegistry, SourceRegistry: registry, Mode: offline.ModeCLI, Bindings: []offline.ExternalBinding{binding}, BindingCatalog: catalog})
	if err != nil || len(accepted.Diagnostics) != 0 {
		t.Fatalf("adapted read-only MCP Build() = %#v, %v", accepted, err)
	}
	binding.Config["credential"] = "resolved-secret"
	if _, err := offline.AdaptExecutionRegistry(*plan, registry, []offline.ExternalBinding{binding}); err == nil {
		t.Fatal("AdaptExecutionRegistry accepted literal secret material")
	}
}

func TestExecuteUsesRuntimeAndIsConcurrentDeterministic(t *testing.T) {
	plan, registry := purePlan(t)
	built, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI})
	if err != nil || len(built.Diagnostics) != 0 {
		t.Fatalf("Build() = %#v, %v", built, err)
	}
	const workers = 20
	results := make(chan any, workers)
	failures := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, executeErr := offline.Execute(context.Background(), *built.Manifest, offline.ExecuteOptions{Registry: registry, Inputs: map[string]any{"value": json.Number("21")}})
			if executeErr != nil {
				failures <- executeErr
				return
			}
			object, objectErr := offline.OutputObject(result.Outputs)
			if objectErr != nil {
				failures <- objectErr
				return
			}
			results <- object["result"]
		}()
	}
	group.Wait()
	close(results)
	close(failures)
	for failure := range failures {
		t.Fatal(failure)
	}
	for value := range results {
		if fmt.Sprint(value) != "42" {
			t.Fatalf("output = %#v", value)
		}
	}
}

func TestRemoteExecutionProfilesRemainNodeScopedForSharedKind(t *testing.T) {
	plan := compileSource(t, `workflow: {name: Profile Isolation, version: 1.0.0}
steps:
  - {id: reader, kind: remote, kind_version: v1, config: {}}
  - {id: computer, kind: remote, kind_version: v1, config: {}}
`)
	registry := stepkind.NewRegistry()
	spec := testSpec("remote", false, false, graph.EffectSet{graph.EffectCompute})
	spec.RequiredCapabilities = []string{"remote.execute"}
	if err := registry.Register(&staticKind{spec: spec}); err != nil {
		t.Fatal(err)
	}
	bindings := []offline.ExternalBinding{
		{NodeID: "reader", Kind: "remote", Version: "v1", Driver: offline.DriverRemoteDaemonHTTP, Config: graph.Config{"endpoint": "https://daemon.example.test/execute"}, Effects: graph.EffectSet{graph.EffectRead}, Capabilities: []string{"remote.execute"}},
		{NodeID: "computer", Kind: "remote", Version: "v1", Driver: offline.DriverRemoteDaemonHTTP, Config: graph.Config{"endpoint": "https://daemon.example.test/execute"}, Effects: graph.EffectSet{graph.EffectCompute}, Capabilities: []string{"remote.execute"}},
	}
	execution, err := offline.AdaptExecutionRegistry(*plan, registry, bindings)
	if err != nil {
		t.Fatal(err)
	}
	built, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: execution, SourceRegistry: registry, Mode: offline.ModeCLI, Bindings: bindings})
	if err != nil || len(built.Diagnostics) != 0 {
		t.Fatalf("Build() = %#v, %v", built.Diagnostics, err)
	}
	profiles := map[string]offline.RemoteExecutionProfile{}
	for _, resolved := range built.Manifest.Bindings {
		profiles[resolved.Binding.NodeID] = resolved.ExecutionProfile
	}
	if !reflect.DeepEqual(profiles["reader"].Effects, graph.EffectSet{graph.EffectRead}) || !reflect.DeepEqual(profiles["computer"].Effects, graph.EffectSet{graph.EffectCompute}) {
		t.Fatalf("node profiles bled across shared kind: %#v", profiles)
	}
	actual := built.Manifest.StepKinds[0].Effects
	if !reflect.DeepEqual(actual, graph.EffectSet{graph.EffectCompute, graph.EffectRead}) {
		t.Fatalf("proxy spec did not retain safe union: %#v", actual)
	}
	destructive := stepkind.NewRegistry()
	destructiveSpec := testSpec("remote", false, false, graph.EffectSet{graph.EffectDestructive})
	destructiveSpec.RequiredCapabilities = []string{"remote.execute"}
	if err := destructive.Register(&staticKind{spec: destructiveSpec}); err != nil {
		t.Fatal(err)
	}
	if _, err := offline.AdaptExecutionRegistry(*plan, destructive, bindings); err == nil {
		t.Fatal("custom destructive source kind was narrowed by a remote binding")
	}
}

func TestGeneratedCLISurfaceAndMCPExposeTypedSingleTool(t *testing.T) {
	plan, registry := purePlan(t)
	cli, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI})
	if err != nil || len(cli.Diagnostics) != 0 {
		t.Fatal(err, cli.Diagnostics)
	}
	var stdout bytes.Buffer
	if runErr := offline.RunCLI(t.Context(), *cli.Manifest, registry, []string{"--value", "7"}, &stdout); runErr != nil {
		t.Fatal(runErr)
	}
	var output map[string]any
	if decodeErr := decodeNumberJSON(stdout.Bytes(), &output); decodeErr != nil || fmt.Sprint(output["result"]) != "14" {
		t.Fatalf("stdout = %s, %v", stdout.String(), decodeErr)
	}

	mcp, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeMCPServer, ToolName: "offline-double"})
	if err != nil || len(mcp.Diagnostics) != 0 {
		t.Fatal(err, mcp.Diagnostics)
	}
	requests := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"offline-double","arguments":{"value":9}}}`,
	}, "\n") + "\n"
	stdout.Reset()
	if err := offline.ServeMCP(t.Context(), *mcp.Manifest, registry, strings.NewReader(requests), &stdout); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("responses = %q", lines)
	}
	var listed struct {
		Result struct {
			Tools []map[string]any `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &listed); err != nil || len(listed.Result.Tools) != 1 || listed.Result.Tools[0]["name"] != "offline-double" || listed.Result.Tools[0]["outputSchema"] == nil {
		t.Fatalf("tools/list = %s", lines[1])
	}
	var called map[string]any
	if err := decodeNumberJSON([]byte(lines[2]), &called); err != nil {
		t.Fatal(err)
	}
	structured := called["result"].(map[string]any)["structuredContent"].(map[string]any)
	if fmt.Sprint(structured["result"]) != "18" {
		t.Fatalf("tools/call = %s", lines[2])
	}
}

func TestServeMCPBoundsEachFrameAndReturnsOnlyStableSafeFailures(t *testing.T) {
	plan, registry := purePlan(t)
	built, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeMCPServer, ToolName: "offline-double"})
	if err != nil || len(built.Diagnostics) != 0 {
		t.Fatal(err, built.Diagnostics)
	}
	padding := strings.Repeat("x", 1<<20)
	var input strings.Builder
	input.WriteString("{malformed}\n")
	for index := 0; index < 9; index++ {
		fmt.Fprintf(&input, `{"jsonrpc":"2.0","id":%d,"method":"initialize","params":{"padding":"%s"}}`+"\n", index+1, padding)
	}
	input.WriteString(`{"jsonrpc":"2.0","id":20,"method":"tools/call","params":{"name":"offline-double","arguments":{}}}` + "\n")
	var output bytes.Buffer
	if err := offline.ServeMCP(t.Context(), *built.Manifest, registry, strings.NewReader(input.String()), &output); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 11 || !strings.Contains(lines[0], "parse error") || !strings.Contains(lines[10], "workflow input validation failed") || strings.Contains(lines[10], "required") {
		t.Fatalf("MCP framed responses = count %d last %q", len(lines), lines[len(lines)-1])
	}
	oversized := strings.Repeat("x", offline.MaximumRPCBytes+1) + "\n"
	if err := offline.ServeMCP(t.Context(), *built.Manifest, registry, strings.NewReader(oversized), io.Discard); err == nil {
		t.Fatal("ServeMCP accepted oversized frame")
	}
}

func TestExecuteDrivesControlFinalizersFanOutAndRetriesThroughRuntime(t *testing.T) {
	t.Run("switch-finally", func(t *testing.T) {
		plan := compileSource(t, `workflow: {name: Offline Control, version: 1.0.0}
inputs:
  - {name: choose, type: boolean, required: true}
steps:
  - id: choose
    kind: transform
    kind_version: v1
    config: {result: "'selected'"}
    outputs: {result: {type: string}}
    switch:
      arms: [{when: inputs.choose, targets: [selected]}]
      default: [other]
  - id: selected
    kind: transform
    kind_version: v1
    config: {result: "'done'"}
    outputs: {result: {type: string}}
  - id: other
    kind: transform
    kind_version: v1
    config: {result: "'other'"}
    outputs: {result: {type: string}}
finally:
  - id: cleanup
    kind: transform
    kind_version: v1
    config: {result: "'clean'"}
    outputs: {result: {type: string}}
outputs:
  result: {type: string, value: {expression: steps.selected.outputs.result}}
`)
		registry := stepkind.NewRegistry()
		if err := registry.Register(&expressionKind{}); err != nil {
			t.Fatal(err)
		}
		built := buildPlan(t, plan, registry)
		result, err := offline.Execute(t.Context(), *built.Manifest, offline.ExecuteOptions{Registry: registry, Inputs: map[string]any{"choose": true}})
		if err != nil || result.Run.Status != "succeeded" || result.Run.Outputs == nil {
			t.Fatalf("Execute(control) = %#v, %v", result, err)
		}
	})

	t.Run("fan-out", func(t *testing.T) {
		plan := compileSource(t, `workflow: {name: Offline Fanout, version: 1.0.0}
inputs:
  - name: items
    schema: {type: array, items: {type: string}}
    required: true
steps:
  - id: fan
    kind: transform
    kind_version: v1
    for_each: inputs.items
    concurrency: 1
    config: {result: item}
    outputs: {result: {type: string}}
`)
		registry := stepkind.NewRegistry()
		if err := registry.Register(&expressionKind{}); err != nil {
			t.Fatal(err)
		}
		built := buildPlan(t, plan, registry)
		result, err := offline.Execute(t.Context(), *built.Manifest, offline.ExecuteOptions{Registry: registry, Inputs: map[string]any{"items": []any{"a", "b"}}})
		if err != nil || result.Run.Status != "succeeded" {
			t.Fatalf("Execute(fan-out) = %#v, %v", result, err)
		}
	})

	t.Run("retry", func(t *testing.T) {
		plan := compileSource(t, `workflow: {name: Offline Retry, version: 1.0.0}
steps:
  - id: flaky
    kind: flaky
    kind_version: v1
    config: {}
    outputs: {result: {type: string}}
    retry:
      attempts: 2
      backoff: {strategy: fixed, initial_delay: 1ms}
      on: [temporary]
outputs:
  result: {type: string, value: {expression: steps.flaky.outputs.result}}
`)
		registry := stepkind.NewRegistry()
		kind := &flakyKind{}
		if err := registry.Register(kind); err != nil {
			t.Fatal(err)
		}
		built := buildPlan(t, plan, registry)
		result, err := offline.Execute(t.Context(), *built.Manifest, offline.ExecuteOptions{Registry: registry})
		if err != nil || result.Run.Status != "succeeded" || kind.calls != 2 {
			t.Fatalf("Execute(retry) = %#v, %v calls=%d", result, err, kind.calls)
		}
	})
}

func compileSource(t *testing.T, source string) *compile.ExecutionPlan {
	t.Helper()
	loaded := compile.LoadBytes("offline-control.workflow.yaml", []byte(source))
	if len(loaded.Diagnostics) != 0 {
		t.Fatal(loaded.Diagnostics)
	}
	compiled := compile.Compile(loaded.Source)
	if len(compiled.Diagnostics) != 0 {
		t.Fatal(compiled.Diagnostics)
	}
	return compiled.Plan
}

func buildPlan(t *testing.T, plan *compile.ExecutionPlan, registry stepkind.Registry) offline.BuildResult {
	t.Helper()
	built, err := offline.Build(t.Context(), plan, offline.BuildOptions{Registry: registry, Mode: offline.ModeCLI})
	if err != nil || len(built.Diagnostics) != 0 {
		t.Fatalf("Build() = %#v, %v", built.Diagnostics, err)
	}
	return built
}

type flakyKind struct{ calls int }

func (*flakyKind) Spec() stepkind.StepKindSpec {
	return testSpec("flaky", true, false, graph.EffectSet{graph.EffectCompute})
}
func (*flakyKind) ValidateConfig(context.Context, graph.Config) []diagnostic.Diagnostic { return nil }
func (k *flakyKind) Execute(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	k.calls++
	if k.calls == 1 {
		return stepkind.StepResult{}, &stepkind.ExecutionError{Code: "temporary", Message: "temporary", Classification: stepkind.Retryable}
	}
	value, err := values.NewInline("done", values.Metadata{Producer: values.Producer{Kind: "test", Reference: prepared.Invocation.Identity.NodeID, Output: "result"}, MediaType: "application/json", Redaction: values.RedactionPublic, Retention: values.RetentionRun})
	if err != nil {
		return stepkind.StepResult{}, err
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": value}}, nil
}

func purePlan(t *testing.T) (*compile.ExecutionPlan, *stepkind.MemoryRegistry) {
	t.Helper()
	loaded := compile.LoadBytes("offline.workflow.yaml", []byte(pureSource))
	if len(loaded.Diagnostics) != 0 {
		t.Fatal(loaded.Diagnostics)
	}
	compiled := compile.Compile(loaded.Source)
	if len(compiled.Diagnostics) != 0 {
		t.Fatal(compiled.Diagnostics)
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(&expressionKind{}); err != nil {
		t.Fatal(err)
	}
	return compiled.Plan, registry
}

func customPlan(t *testing.T, kind string) *compile.ExecutionPlan {
	t.Helper()
	source := `workflow: {name: Custom, version: 1.0.0}
steps:
  - id: work
    kind: ` + kind + `
    kind_version: v1
    config: {}
`
	loaded := compile.LoadBytes("custom.workflow.yaml", []byte(source))
	if len(loaded.Diagnostics) != 0 {
		t.Fatal(loaded.Diagnostics)
	}
	compiled := compile.Compile(loaded.Source)
	if len(compiled.Diagnostics) != 0 {
		t.Fatal(compiled.Diagnostics)
	}
	return compiled.Plan
}

type staticKind struct{ spec stepkind.StepKindSpec }

func (k *staticKind) Spec() stepkind.StepKindSpec                                        { return k.spec }
func (*staticKind) ValidateConfig(context.Context, graph.Config) []diagnostic.Diagnostic { return nil }
func (*staticKind) Execute(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	return stepkind.StepResult{}, errors.New("not executed")
}

// expressionKind is an extraction-boundary test fixture, not an adapter
// implementation. Concrete adapter reconstruction is covered by
// internal/offlinebuild integration tests.
type expressionKind struct{}

func (*expressionKind) Spec() stepkind.StepKindSpec {
	return testSpec("transform", true, false, graph.EffectSet{graph.EffectCompute})
}

func (*expressionKind) ValidateConfig(context.Context, graph.Config) []diagnostic.Diagnostic {
	return nil
}

func (*expressionKind) Execute(_ context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	contextValue := values.ExpressionContext{Inputs: prepared.Invocation.Inputs}
	if item, ok := prepared.Invocation.Inputs["item"]; ok {
		contextValue.Item = &item
	}
	if index, ok := prepared.Invocation.Inputs["index"]; ok {
		parsed, err := strconv.Atoi(fmt.Sprint(index.Inline))
		if err != nil {
			return stepkind.StepResult{}, err
		}
		contextValue.Index = &parsed
	}
	names := make([]string, 0, len(prepared.Invocation.Config))
	for name := range prepared.Invocation.Config {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make(values.ValueSet, len(names))
	engine := values.NewExpressionEngine()
	for _, name := range names {
		expression, ok := prepared.Invocation.Config[name].(string)
		if !ok {
			return stepkind.StepResult{}, fmt.Errorf("test transform expression %q is not a string", name)
		}
		native, err := engine.EvaluateRaw(graph.Expression{Text: expression}, contextValue, values.ExpressionOptions{})
		if err != nil {
			return stepkind.StepResult{}, err
		}
		value, err := values.NewInline(native, values.Metadata{
			Producer:  values.Producer{Kind: "test.transform", Reference: prepared.Invocation.Identity.NodeID, Output: name},
			MediaType: "application/json", Redaction: values.RedactionPublic, Retention: values.RetentionRun,
		})
		if err != nil {
			return stepkind.StepResult{}, err
		}
		result[name] = value
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: result}, nil
}

func testSpec(name string, embedded, suspend bool, effects graph.EffectSet) stepkind.StepKindSpec {
	return stepkind.StepKindSpec{Name: name, Version: "v1", ConfigSchema: graph.Schema{"type": "object"}, InputSchema: graph.Schema{"type": "object"}, OutputSchema: graph.Schema{"type": "object"}, Effects: effects, Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe, Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationContext}, Observation: stepkind.ObservationSpec{Mode: stepkind.ObservationNone}, CanSuspend: suspend, EmbeddedModeSupported: embedded}
}

func hasCode(findings []diagnostic.Diagnostic, code diagnostic.Code) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}
func decodeNumberJSON(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(output)
}

var _ stepkind.StepKind = (*staticKind)(nil)
var _ stepkind.StepKind = (*expressionKind)(nil)
