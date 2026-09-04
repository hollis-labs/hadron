package offlinebuild_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/offlinebuild"
	"github.com/hollis-labs/hadron/internal/persistence"
	mcpadapter "github.com/hollis-labs/go-workflow/adapters/mcp"
	"github.com/hollis-labs/go-workflow/adapters/transform"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/offline"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
)

const source = `workflow:
  name: Built Echo
  version: 1.0.0
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: echo
    kind: transform
    kind_version: v1
    with:
      message:
        expression: inputs.message
    config:
      result: inputs.message
    outputs:
      result:
        type: string
outputs:
  result:
    type: string
    value:
      expression: steps.echo.outputs.result
`

func TestBuildExecutableCLIIsReproducibleAndRuns(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "echo.workflow.yaml")
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	builder, err := offlinebuild.NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	firstPath, secondPath := filepath.Join(directory, "echo-one"), filepath.Join(directory, "echo-two")
	first, err := builder.BuildExecutable(t.Context(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeCLI, OutputPath: firstPath})
	if err != nil || len(first.Diagnostics) != 0 {
		t.Fatalf("first BuildExecutable() = %#v, %v", first, err)
	}
	second, err := builder.BuildExecutable(t.Context(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeCLI, OutputPath: secondPath})
	if err != nil || len(second.Diagnostics) != 0 {
		t.Fatalf("second BuildExecutable() = %#v, %v", second, err)
	}
	firstBytes, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(firstBytes) != sha256.Sum256(secondBytes) || first.Manifest.BuildDigest != second.Manifest.BuildDigest {
		t.Fatal("same source and bindings produced different artifacts")
	}
	command := exec.Command(firstPath, "--message", "hello")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if decodeErr := json.Unmarshal(output, &result); decodeErr != nil || result["result"] != "hello" {
		t.Fatalf("artifact output = %s, %v", output, decodeErr)
	}
	help, err := exec.Command(firstPath, "--help").CombinedOutput()
	if err != nil || !strings.Contains(string(help), "--message <string> (required)") {
		t.Fatalf("artifact help = %v: %s", err, help)
	}
}

func TestBuildExecutableIsCallerCWDIndependentAndPublishesAtomically(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "echo.workflow.yaml")
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	builder, err := offlinebuild.NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	originalCWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	foreign := t.TempDir()
	if chdirErr := os.Chdir(foreign); chdirErr != nil {
		t.Fatal(chdirErr)
	}
	t.Cleanup(func() { _ = os.Chdir(originalCWD) })
	outputPath := filepath.Join(directory, "atomic-echo")
	if writeErr := os.WriteFile(outputPath, []byte("old-artifact"), 0o755); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, rebuildErr := builder.BuildExecutable(t.Context(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeCLI, OutputPath: outputPath}); rebuildErr != nil {
		t.Fatal(rebuildErr)
	}
	var group sync.WaitGroup
	errs := make(chan error, 2)
	for index := 0; index < 2; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, buildErr := builder.BuildExecutable(context.Background(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeCLI, OutputPath: outputPath})
			errs <- buildErr
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	result, err := exec.Command(outputPath, "--message", "atomic").CombinedOutput()
	if err != nil || !strings.Contains(string(result), "atomic") {
		t.Fatalf("published artifact = %v: %s", err, result)
	}
	matches, err := filepath.Glob(filepath.Join(directory, ".atomic-echo.tmp-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary publications = %#v, %v", matches, err)
	}
}

func TestBuildExecutableMCPServerExposesExactlyOneWorkflowTool(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "echo.workflow.yaml")
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	builder, err := offlinebuild.NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	artifact := filepath.Join(directory, "echo-mcp")
	result, err := builder.BuildExecutable(t.Context(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeMCPServer, ToolName: "echo-workflow", OutputPath: artifact})
	if err != nil || len(result.Diagnostics) != 0 {
		t.Fatalf("BuildExecutable() = %#v, %v", result, err)
	}
	requests := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo-workflow","arguments":{"message":"hello-mcp"}}}`,
	}, "\n") + "\n"
	command := exec.Command(artifact)
	command.Stdin = strings.NewReader(requests)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("artifact: %v: %s", err, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("responses = %q", lines)
	}
	var listed struct {
		Result struct {
			Tools []json.RawMessage `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &listed); err != nil || len(listed.Result.Tools) != 1 {
		t.Fatalf("tools/list = %s, %v", lines[1], err)
	}
	if !strings.Contains(lines[2], "hello-mcp") {
		t.Fatalf("tools/call = %s", lines[2])
	}
}

func TestCompileRejectsUnpinnedAndUnsupportedSources(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "bad.workflow.yaml")
	unpinned := strings.Replace(source, "    kind_version: v1\n", "", 1)
	if err := os.WriteFile(sourcePath, []byte(unpinned), 0o600); err != nil {
		t.Fatal(err)
	}
	builder, err := offlinebuild.NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	result, err := builder.Compile(t.Context(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeCLI})
	if err != nil || len(result.Diagnostics) == 0 || result.Diagnostics[0].Code != offline.CodeUnknownKind {
		t.Fatalf("Compile(unpinned) = %#v, %v", result, err)
	}
}

func TestOfflineInMemoryExecutionMatchesSQLiteHostedRuntimeContracts(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "echo.workflow.yaml")
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	builder, err := offlinebuild.NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	built, err := builder.Compile(t.Context(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeCLI})
	if err != nil || len(built.Diagnostics) != 0 {
		t.Fatalf("Compile() = %#v, %v", built, err)
	}
	registry := stepkind.NewRegistry()
	if registerErr := registry.Register(transform.New()); registerErr != nil {
		t.Fatal(registerErr)
	}
	fixed := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	options := offline.ExecuteOptions{Registry: registry, RegistryBinder: offline.RuntimeRegistryBinderFunc(offlinebuild.BindRuntimeRegistry), Inputs: map[string]any{"message": "parity"}, Now: func() time.Time { return fixed }}
	inMemory, err := offline.Execute(t.Context(), *built.Manifest, options)
	if err != nil {
		t.Fatal(err)
	}
	database, err := persistence.Open(filepath.Join(directory, "hosted.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	state, err := persistence.NewWorkflowStateStore(database)
	if err != nil {
		t.Fatal(err)
	}
	hosted, err := offline.ExecuteWithStore(t.Context(), *built.Manifest, options, state)
	if err != nil {
		t.Fatal(err)
	}
	inMemoryOutput, err := offline.OutputObject(inMemory.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	hostedOutput, err := offline.OutputObject(hosted.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inMemoryOutput, hostedOutput) || inMemory.Run.Status != hosted.Run.Status {
		t.Fatalf("in-memory = %#v, hosted = %#v", inMemory, hosted)
	}
}

const remoteKindsSource = `workflow: {name: Bound Remote Kinds, version: 1.0.0}
steps:
  - id: mcp
    kind: mcp
    kind_version: v1
    config:
      server: sample
      tool: echo
      arguments: {message: hello}
      expected_result: structured
  - id: llm
    kind: llm
    kind_version: v1
    needs: [mcp]
    config:
      profile: sample
      messages: [{role: user, content: hello}]
      output_schema: {type: string}
`

func TestBuildExecutableRunsBoundMCPAndLLMThroughExactRemoteDriver(t *testing.T) {
	var callsMu sync.Mutex
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() { _ = request.Body.Close() }()
		var remote struct {
			Kind string `json:"kind"`
		}
		if err := json.NewDecoder(request.Body).Decode(&remote); err != nil {
			http.Error(writer, "invalid", http.StatusBadRequest)
			return
		}
		callsMu.Lock()
		calls[remote.Kind]++
		callsMu.Unlock()
		outputs := values.ValueSet{}
		switch remote.Kind {
		case "mcp":
			outputs["structured"] = inlineValue(t, "structured", map[string]any{"answer": "mcp-ok"})
			outputs["tool_metadata"] = inlineValue(t, "tool_metadata", map[string]any{})
		case "llm":
			outputs["output"] = inlineValue(t, "output", "llm-ok")
			outputs["raw_text"] = inlineValue(t, "raw_text", `"llm-ok"`)
			outputs["tool_calls"] = inlineValue(t, "tool_calls", []any{})
			outputs["usage"] = inlineValue(t, "usage", map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2, "cost_microunits": 0, "requests": 1, "tool_calls": 0})
			outputs["stop_reason"] = inlineValue(t, "stop_reason", "completed")
			outputs["audit"] = inlineValue(t, "audit", map[string]any{"profile": "sample", "provider": "remote", "model": "sample", "binding_id": "offline", "binding_revision": "v1", "binding_attributes": map[string]any{}, "provider_calls": []any{map[string]any{"request_id": "request-1", "revision": "v1", "attributes": map[string]any{}}}})
		default:
			http.Error(writer, "invalid", http.StatusBadRequest)
			return
		}
		stepResult := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}
		if err := stepResult.Validate(); err != nil {
			t.Errorf("fake remote result: %v", err)
		}
		if remote.Kind == "mcp" {
			if err := values.ValidateValueSetSchema((&mcpadapter.Kind{}).Spec().OutputSchema, outputs); err != nil {
				t.Errorf("fake MCP schema: %v", err)
			}
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"result": stepResult})
	}))
	defer server.Close()

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "remote.workflow.yaml")
	if err := os.WriteFile(sourcePath, []byte(remoteKindsSource), 0o600); err != nil {
		t.Fatal(err)
	}
	builder, err := offlinebuild.NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	bindings := []offline.ExternalBinding{
		{NodeID: "mcp", Kind: "mcp", Version: "v1", Driver: offline.DriverRemoteDaemonHTTP, Config: graph.Config{"endpoint": server.URL}, Effects: graph.EffectSet{graph.EffectRead}, Capabilities: []string{"mcp.client"}},
		{NodeID: "llm", Kind: "llm", Version: "v1", Driver: offline.DriverRemoteDaemonHTTP, Config: graph.Config{"endpoint": server.URL}, Effects: graph.EffectSet{graph.EffectCompute}, Capabilities: []string{"llm.provider"}},
	}
	artifact := filepath.Join(directory, "remote")
	result, err := builder.BuildExecutable(t.Context(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeCLI, Bindings: bindings, OutputPath: artifact})
	if err != nil || len(result.Diagnostics) != 0 {
		t.Fatalf("BuildExecutable() = %#v, %v", result.Diagnostics, err)
	}
	testRegistry := stepkind.NewRegistry()
	if registerErr := offlinebuild.RegisterManifestKinds(testRegistry, *result.Manifest); registerErr != nil {
		t.Fatal(registerErr)
	}
	if _, executeErr := offline.Execute(t.Context(), *result.Manifest, offline.ExecuteOptions{Registry: testRegistry, RegistryBinder: offline.RuntimeRegistryBinderFunc(offlinebuild.BindRuntimeRegistry)}); executeErr != nil {
		var failure *offline.RunFailureError
		if errors.As(executeErr, &failure) {
			t.Fatalf("in-process bound execute: %+v", failure.Failure)
		}
		t.Fatalf("in-process bound execute: %#v", executeErr)
	}
	output, err := exec.Command(artifact).CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "{}" {
		t.Fatalf("bound artifact = %v: %s", err, output)
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if calls["mcp"] != 2 || calls["llm"] != 2 { // in-process plus generated executable
		t.Fatalf("remote calls = %#v", calls)
	}
}

func inlineValue(t *testing.T, output string, value any) values.Value {
	t.Helper()
	result, err := values.NewInline(value, values.Metadata{
		Producer:  values.Producer{Kind: "offline.remote", Reference: "test", Output: output},
		MediaType: "application/json", Redaction: values.RedactionPublic, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestBuildExecutableRemoteWaitRecoversThroughDurableOperationIdentity(t *testing.T) {
	var complete atomic.Bool
	var posts atomic.Int32
	var expectedProfile atomic.Value
	observing := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost:
			posts.Add(1)
			var envelope struct {
				Profile offline.RemoteExecutionProfile `json:"execution_profile"`
			}
			if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil || envelope.Profile.NodeID != "pause" || envelope.Profile.Kind != "sleep" {
				t.Errorf("remote POST profile = %#v, %v", envelope.Profile, err)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"pending": map[string]any{"operation_id": "sleep-operation"}})
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/operations/sleep-operation"):
			if request.Header.Get("X-Hadron-Execution-Profile") != expectedProfile.Load().(string) {
				t.Errorf("observation profile header = %q", request.Header.Get("X-Hadron-Execution-Profile"))
			}
			select {
			case observing <- struct{}{}:
			default:
			}
			if !complete.Load() {
				<-request.Context().Done()
				return
			}
			outputs := values.ValueSet{
				"woke_at":   inlineValue(t, "woke_at", "2026-08-24T12:00:00Z"),
				"resume":    inlineValue(t, "resume", map[string]any{"source": "remote-daemon"}),
				"timed_out": inlineValue(t, "timed_out", false),
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"result": stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "sleep.workflow.yaml")
	sleepSource := `workflow: {name: Remote Sleep, version: 1.0.0}
steps:
  - id: pause
    kind: sleep
    kind_version: v1
    config: {duration: 1s}
`
	if err := os.WriteFile(sourcePath, []byte(sleepSource), 0o600); err != nil {
		t.Fatal(err)
	}
	builder, err := offlinebuild.NewDefault()
	if err != nil {
		t.Fatal(err)
	}
	binding := offline.ExternalBinding{NodeID: "pause", Kind: "sleep", Version: "v1", Driver: offline.DriverRemoteDaemonHTTP, Config: graph.Config{"endpoint": server.URL}, Effects: graph.EffectSet{graph.EffectRead}, Capabilities: []string{"wait.resume"}}
	artifact := filepath.Join(directory, "sleep")
	result, err := builder.BuildExecutable(t.Context(), offlinebuild.Request{SourcePath: sourcePath, Mode: offline.ModeCLI, Bindings: []offline.ExternalBinding{binding}, OutputPath: artifact})
	if err != nil || len(result.Diagnostics) != 0 {
		t.Fatalf("BuildExecutable() = %#v, %v", result.Diagnostics, err)
	}
	profileJSON, err := json.Marshal(result.Manifest.Bindings[0].ExecutionProfile)
	if err != nil {
		t.Fatal(err)
	}
	expectedProfile.Store(values.SHA256Digest(profileJSON))
	first := exec.Command(artifact)
	if startErr := first.Start(); startErr != nil {
		t.Fatal(startErr)
	}
	select {
	case <-observing:
	case <-time.After(5 * time.Second):
		_ = first.Process.Kill()
		t.Fatal("generated artifact did not begin remote observation")
	}
	if killErr := first.Process.Kill(); killErr != nil {
		t.Fatal(killErr)
	}
	_ = first.Wait()
	complete.Store(true)
	output, err := exec.Command(artifact).CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "{}" || posts.Load() < 2 {
		t.Fatalf("restarted remote wait artifact = %v: %s, posts=%d", err, output, posts.Load())
	}
}
