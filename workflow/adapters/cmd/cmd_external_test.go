package cmd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cmdadapter "github.com/hollis-labs/hadron/workflow/adapters/cmd"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestCmdRegistersWithConservativeMetadataAndDescription(t *testing.T) {
	config := validConfig()
	description, findings := cmdadapter.DescribeConfig(config)
	if hasErrors(findings) {
		t.Fatalf("DescribeConfig findings = %#v", findings)
	}
	if !equalEffects(description.DeclaredEffects, graph.EffectSet{graph.EffectCompute}) ||
		!equalEffects(description.ConservativeEffects, graph.EffectSet{graph.EffectDestructive}) {
		t.Fatalf("description effects = declared %#v conservative %#v", description.DeclaredEffects, description.ConservativeEffects)
	}
	if !equalStrings(description.RequiredCapabilities, []string{cmdadapter.CapabilityProcessExecute}) ||
		!equalStrings(description.DeclaredCapabilities, []string{cmdadapter.CapabilityProcessExecute}) {
		t.Fatalf("description capabilities = %#v / %#v", description.RequiredCapabilities, description.DeclaredCapabilities)
	}
	if description.Idempotency != graph.IdempotencyNone || description.RetrySafety != stepkind.RetryUnsupported {
		t.Fatalf("description retry metadata = %q / %q", description.Idempotency, description.RetrySafety)
	}

	executor := mustExecutor(t, successfulPolicy(), cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, _ io.Writer) (cmdadapter.ProcessResult, error) {
		_, _ = io.WriteString(stdout, "ok")
		return processSuccess(request), nil
	}), nil, nil, nil)
	registry := stepkind.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	spec := executor.Spec()
	if spec.Name != cmdadapter.Name || spec.Version != cmdadapter.Version ||
		!equalEffects(spec.Effects, graph.EffectSet{graph.EffectDestructive}) ||
		spec.Idempotency != graph.IdempotencyNone || spec.RetrySafety != stepkind.RetryUnsupported ||
		spec.EmbeddedModeSupported {
		t.Fatalf("Spec() = %#v", spec)
	}
	// Every metadata read is defensive.
	spec.ConfigSchema["mutated"] = true
	if _, exists := executor.Spec().ConfigSchema["mutated"]; exists {
		t.Fatal("Spec returned shared config schema")
	}
	description.Arguments = append(description.Arguments, "mutated")
	again, _ := cmdadapter.DescribeConfig(config)
	if len(again.Arguments) != 3 {
		t.Fatalf("DescribeConfig returned shared arguments: %#v", again.Arguments)
	}
}

func TestValidateConfigFailsClosedAndWarnsForCompatibility(t *testing.T) {
	executor := mustExecutor(t, successfulPolicy(), noOpRunner(), nil, nil, nil)
	tests := []struct {
		name   string
		mutate func(graph.Config)
		part   string
	}{
		{"missing cwd", func(c graph.Config) { delete(c, "cwd") }, "config.cwd"},
		{"unknown", func(c graph.Config) { c["shell"] = true }, "config.shell"},
		{"literal env", func(c graph.Config) { c["env"] = map[string]any{"TOKEN": "plaintext"} }, "config.env.TOKEN"},
		{"missing process capability", func(c graph.Config) { c["capabilities"] = []any{"filesystem.read"} }, "process.execute"},
		{"invalid effect", func(c graph.Config) { c["effects"] = []any{"harmless"} }, "config.effects"},
		{"two executable fields", func(c graph.Config) { c["executable"] = "/tool" }, "exactly one"},
		{"reserved capture", func(c graph.Config) {
			c["capture"] = map[string]any{"stdout": map[string]any{"name": "exit_code", "parse": "text"}}
		}, "collides"},
		{"strong event overflow", func(c graph.Config) {
			c["capture"] = map[string]any{"stdout": map[string]any{"as": "event_stream", "max_bytes": values.MaximumInlineLimit + 1}}
		}, "operational ceiling"},
		{"non-string parser", func(c graph.Config) {
			c["capture"] = map[string]any{"stdout": map[string]any{"name": "out", "parse": true}}
		}, "parse must be a string"},
		{"invalid media type", func(c graph.Config) {
			c["capture"] = map[string]any{"stdout": map[string]any{"name": "out", "media_type": "not a media type"}}
		}, "valid media type"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validConfig()
			test.mutate(config)
			findings := executor.ValidateConfig(context.Background(), config)
			if !hasErrors(findings) || !diagnosticsContain(findings, test.part) {
				t.Fatalf("findings = %#v, want error containing %q", findings, test.part)
			}
		})
	}

	compatibility := validConfig()
	compatibility["capture"] = map[string]any{
		"stdout": map[string]any{"parse": "set-output", "compatibility": true},
	}
	findings := executor.ValidateConfig(context.Background(), compatibility)
	if hasErrors(findings) || len(findings) != 1 || findings[0].Severity != diagnostic.SeverityWarning {
		t.Fatalf("compatibility findings = %#v", findings)
	}
	twoStreams := validConfig()
	twoStreams["capture"] = map[string]any{
		"stdout": map[string]any{"parse": "set-output", "compatibility": true},
		"stderr": map[string]any{"parse": "set-output", "compatibility": true},
	}
	if findings := executor.ValidateConfig(context.Background(), twoStreams); !hasErrors(findings) || !diagnosticsContain(findings, "exactly one stream") {
		t.Fatalf("two-stream findings = %#v", findings)
	}
}

func TestExecuteUsesArgvWithoutShellAndNoAmbientState(t *testing.T) {
	config := validConfig()
	config["arguments"] = []any{"$(touch /tmp/must-not-run)", "*", "a b"}
	config["capture"] = map[string]any{
		"stdout": map[string]any{"name": "record", "parse": "json"},
		"stderr": map[string]any{"name": "warnings", "parse": "lines"},
	}
	var seen cmdadapter.ProcessRequest
	runner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, stderr io.Writer) (cmdadapter.ProcessResult, error) {
		seen = request
		if _, err := io.WriteString(stdout, `{"n":9007199254740993123456789}`); err != nil {
			return cmdadapter.ProcessResult{}, err
		}
		if _, err := io.WriteString(stderr, "first\r\nsecond\n"); err != nil {
			return cmdadapter.ProcessResult{}, err
		}
		return processSuccess(request), nil
	})
	executor := mustExecutor(t, successfulPolicy(), runner, nil, nil, nil)
	result, err := executor.Execute(context.Background(), invocation(config))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if seen.Executable != "/resolved/tool" || seen.CWD != "/resolved/workspace" || len(seen.Environment) != 0 ||
		!equalStrings(seen.Arguments, []string{"$(touch /tmp/must-not-run)", "*", "a b"}) {
		t.Fatalf("process request = %#v", seen)
	}
	record := result.Outputs["record"].Inline.(map[string]any)
	if got := record["n"].(json.Number); got != json.Number("9007199254740993123456789") {
		t.Fatalf("exact number = %q", got)
	}
	warnings := result.Outputs["warnings"].Inline.([]any)
	if fmt.Sprint(warnings) != "[first second]" || result.Outputs["exit_code"].Inline != json.Number("0") {
		t.Fatalf("outputs = %#v", result.Outputs)
	}
	if err := values.ValidateValueSetSchema(executor.Spec().OutputSchema, result.Outputs); err != nil {
		t.Fatalf("runtime-facing output schema rejected result: %v", err)
	}
}

func TestSecretsAreBoundaryOnlyAndAllStreamsAreRedacted(t *testing.T) {
	config := validConfig()
	config["env"] = map[string]any{"TOKEN": "secret://vault/token"}
	config["capture"] = map[string]any{
		"stdout": map[string]any{"name": "log", "parse": "text", "max_bytes": 128},
		"stderr": map[string]any{"as": "event_stream", "max_bytes": 128},
	}
	ref, _ := values.ParseSecretRef("secret://vault/token")
	resolved, _ := values.NewResolvedSecret(ref, []byte("topsecret"))
	resolver := values.SecretResolverFunc(func(_ context.Context, got values.SecretRef) (*values.ResolvedSecret, error) {
		if got != ref {
			t.Fatalf("resolved ref = %q", got)
		}
		return resolved, nil
	})
	var seen cmdadapter.ProcessRequest
	runner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, stderr io.Writer) (cmdadapter.ProcessResult, error) {
		seen = request
		if len(request.Environment) != 1 || string(request.Environment[0].Value) != "topsecret" {
			t.Fatalf("process environment = %#v", request.Environment)
		}
		for _, chunk := range []string{"before-top", "sec", "ret-after"} {
			if _, err := io.WriteString(stdout, chunk); err != nil {
				return cmdadapter.ProcessResult{}, err
			}
		}
		for _, chunk := range []string{"event-top", "secret-end"} {
			if _, err := io.WriteString(stderr, chunk); err != nil {
				return cmdadapter.ProcessResult{}, err
			}
		}
		return processSuccess(request), nil
	})
	var eventBytes bytes.Buffer
	events := cmdadapter.EventSinkFunc(func(_ context.Context, event cmdadapter.OperationalEvent) error {
		eventBytes.Write(event.Payload)
		return nil
	})
	executor := mustExecutor(t, successfulPolicy(), runner, resolver, nil, events)
	result, err := executor.Execute(context.Background(), invocation(config))
	if err != nil {
		t.Fatal(err)
	}
	log := result.Outputs["log"].Inline.(string)
	if log != "before-[REDACTED]-after" || eventBytes.String() != "event-[REDACTED]-end" {
		t.Fatalf("redacted output/event = %q / %q", log, eventBytes.String())
	}
	if strings.Contains(log+eventBytes.String(), "topsecret") {
		t.Fatal("resolved secret leaked")
	}
	if len(resolved.Bytes()) != 0 {
		t.Fatal("resolved secret was not forgotten")
	}
	if len(seen.Environment) != 1 || !allZero(seen.Environment[0].Value) {
		t.Fatalf("runner request environment was not zeroed: %#v", seen.Environment)
	}
}

func TestArtifactCaptureReceivesOnlyRedactedBoundedBytes(t *testing.T) {
	config := validConfig()
	config["env"] = map[string]any{"TOKEN": "secret://vault/token"}
	config["capture"] = map[string]any{
		"stdout": map[string]any{"as": "artifact", "name": "report", "max_bytes": 128, "media_type": "text/plain"},
	}
	ref, _ := values.ParseSecretRef("secret://vault/token")
	resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
		return values.NewResolvedSecret(ref, []byte("topsecret"))
	})
	var captured []byte
	var sinkValue values.Value
	artifact := cmdadapter.ArtifactSinkFunc(func(_ context.Context, request cmdadapter.ArtifactCapture, source io.Reader) (values.Value, error) {
		var err error
		captured, err = io.ReadAll(source)
		if err != nil {
			return values.Value{}, err
		}
		if request.MaxBytes != 128 || request.Name != "report" || request.Metadata.Redaction != values.RedactionPrivate || request.Metadata.Retention != values.RetentionRun {
			t.Fatalf("artifact request = %#v", request)
		}
		ref := values.ArtifactRef{
			Store: "fixture", URI: "artifact://fixture/report", Digest: values.SHA256Digest(captured),
			MediaType: request.Metadata.MediaType, SizeBytes: int64(len(captured)), Producer: request.Metadata.Producer,
			Redaction: request.Metadata.Redaction, Retention: request.Metadata.Retention,
		}
		sinkValue, err = values.NewArtifact(ref)
		return sinkValue, err
	})
	runner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, _ io.Writer) (cmdadapter.ProcessResult, error) {
		_, err := io.WriteString(stdout, "topsecret report")
		return processSuccess(request), err
	})
	executor := mustExecutor(t, successfulPolicy(), runner, resolver, artifact, nil)
	result, err := executor.Execute(context.Background(), invocation(config))
	if err != nil {
		t.Fatal(err)
	}
	if string(captured) != "[REDACTED] report" || result.Outputs["report"].Type != values.TypeArtifact ||
		strings.Contains(string(captured), "topsecret") {
		t.Fatalf("artifact bytes/output = %q / %#v", captured, result.Outputs["report"])
	}
	sinkValue.Artifact.URI = "artifact://fixture/mutated-after-return"
	if result.Outputs["report"].Artifact.URI != "artifact://fixture/report" {
		t.Fatal("artifact output retained sink-owned pointer")
	}
}

func TestCaptureFailuresKeepBothPipesDrainingAndAreTyped(t *testing.T) {
	t.Run("event sink", func(t *testing.T) {
		config := validConfig()
		config["capture"] = map[string]any{
			"stdout": map[string]any{"as": "event_stream", "max_bytes": 1024},
			"stderr": map[string]any{"name": "stderr", "parse": "text", "max_bytes": 1024},
		}
		var stdoutWritten, stderrWritten bool
		runner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, stderr io.Writer) (cmdadapter.ProcessResult, error) {
			content := bytes.Repeat([]byte("x"), 512)
			n, err := stdout.Write(content)
			stdoutWritten = err == nil && n == len(content)
			n, err = stderr.Write(content)
			stderrWritten = err == nil && n == len(content)
			return processSuccess(request), nil
		})
		executor := mustExecutor(t, successfulPolicy(), runner, nil, nil, cmdadapter.EventSinkFunc(func(context.Context, cmdadapter.OperationalEvent) error {
			return errors.New("event failed with secret://vault/not-safe")
		}))
		_, err := executor.Execute(context.Background(), invocation(config))
		assertExecutionCode(t, err, cmdadapter.CodeEventFailed)
		if !stdoutWritten || !stderrWritten || strings.Contains(err.Error(), "secret://") {
			t.Fatalf("pipes/error = %v/%v/%v", stdoutWritten, stderrWritten, err)
		}
	})

	for _, mode := range []string{"output", "event_stream"} {
		t.Run("truncated "+mode, func(t *testing.T) {
			config := validConfig()
			capture := map[string]any{"as": mode, "max_bytes": 4}
			if mode == "output" {
				capture["name"], capture["parse"] = "small", "text"
			}
			config["capture"] = map[string]any{"stdout": capture}
			var fullWrite bool
			runner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, _ io.Writer) (cmdadapter.ProcessResult, error) {
				content := []byte("hostile-unbounded-output")
				n, err := stdout.Write(content)
				fullWrite = err == nil && n == len(content)
				return processSuccess(request), nil
			})
			executor := mustExecutor(t, successfulPolicy(), runner, nil, nil, cmdadapter.EventSinkFunc(func(context.Context, cmdadapter.OperationalEvent) error { return nil }))
			_, err := executor.Execute(context.Background(), invocation(config))
			assertExecutionCode(t, err, cmdadapter.CodeOutputTruncated)
			if !fullWrite {
				t.Fatal("collector stopped draining after overflow")
			}
		})
	}
}

func TestRawByteBoundDoesNotLeakAPartialLongSecret(t *testing.T) {
	const material = "a-very-long-secret-material"
	config := validConfig()
	config["env"] = map[string]any{"TOKEN": "secret://vault/token"}
	config["capture"] = map[string]any{"stdout": map[string]any{"as": "event_stream", "max_bytes": 12}}
	ref, _ := values.ParseSecretRef("secret://vault/token")
	resolver := values.SecretResolverFunc(func(context.Context, values.SecretRef) (*values.ResolvedSecret, error) {
		return values.NewResolvedSecret(ref, []byte(material))
	})
	var observed bytes.Buffer
	events := cmdadapter.EventSinkFunc(func(_ context.Context, event cmdadapter.OperationalEvent) error {
		observed.Write(event.Payload)
		return nil
	})
	runner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, _ io.Writer) (cmdadapter.ProcessResult, error) {
		for _, part := range []string{material[:8], material[8:17], material[17:]} {
			written, err := io.WriteString(stdout, part)
			if err != nil || written != len(part) {
				return cmdadapter.ProcessResult{}, fmt.Errorf("write secret chunk: %w", err)
			}
		}
		return processSuccess(request), nil
	})
	executor := mustExecutor(t, successfulPolicy(), runner, resolver, nil, events)
	_, err := executor.Execute(context.Background(), invocation(config))
	assertExecutionCode(t, err, cmdadapter.CodeOutputTruncated)
	if observed.Len() != 0 || strings.Contains(observed.String(), material) {
		t.Fatalf("long secret escaped raw bound: %q", observed.String())
	}
}

func TestCompatibilityIsSelectedStreamOnlyAndCollisionSafe(t *testing.T) {
	config := validConfig()
	config["capture"] = map[string]any{
		"stdout": map[string]any{"parse": "set-output", "compatibility": true},
		"stderr": map[string]any{"name": "stderr", "parse": "text"},
	}
	runner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, stderr io.Writer) (cmdadapter.ProcessResult, error) {
		_, _ = io.WriteString(stdout, "noise\n::set-output alpha=one\n")
		_, _ = io.WriteString(stderr, "::set-output ignored=two\n")
		return processSuccess(request), nil
	})
	executor := mustExecutor(t, successfulPolicy(), runner, nil, nil, nil)
	result, err := executor.Execute(context.Background(), invocation(config))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outputs["alpha"].Producer.Kind != "compatibility_set_output" || result.Outputs["alpha"].Inline != "one" {
		t.Fatalf("compatibility output = %#v", result.Outputs["alpha"])
	}
	if _, exists := result.Outputs["ignored"]; exists || result.Outputs["stderr"].Inline != "::set-output ignored=two\n" {
		t.Fatalf("non-selected stream was scanned: %#v", result.Outputs)
	}

	for _, stdout := range []string{
		"::set-output exit_code=forbidden\n",
		"::set-output stderr=collision\n",
		"::set-output alpha=one\n::set-output alpha=two\n",
	} {
		collisionRunner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, output, _ io.Writer) (cmdadapter.ProcessResult, error) {
			_, _ = io.WriteString(output, stdout)
			return processSuccess(request), nil
		})
		collisionExecutor := mustExecutor(t, successfulPolicy(), collisionRunner, nil, nil, nil)
		_, err := collisionExecutor.Execute(context.Background(), invocation(config))
		assertExecutionCode(t, err, cmdadapter.CodeParseFailed)
	}
}

func TestPolicyNonzeroAndArtifactFailuresAreSafeAndStable(t *testing.T) {
	t.Run("policy", func(t *testing.T) {
		called := false
		executor := mustExecutor(t, cmdadapter.PolicyFunc(func(context.Context, cmdadapter.ConfigDescription) (cmdadapter.ResolvedCommand, error) {
			return cmdadapter.ResolvedCommand{}, errors.New("denied /private/path secret://vault/ref")
		}), cmdadapter.ProcessRunnerFunc(func(context.Context, cmdadapter.ProcessRequest, io.Writer, io.Writer) (cmdadapter.ProcessResult, error) {
			called = true
			return cmdadapter.ProcessResult{}, nil
		}), nil, nil, nil)
		_, err := executor.Execute(context.Background(), invocation(validConfig()))
		assertExecutionCode(t, err, cmdadapter.CodePolicyDenied)
		if called || strings.Contains(err.Error(), "/private") || strings.Contains(err.Error(), "secret://") {
			t.Fatalf("policy boundary leaked or launched: %v", err)
		}
	})

	t.Run("invalid resolution", func(t *testing.T) {
		executor := mustExecutor(t, cmdadapter.PolicyFunc(func(_ context.Context, description cmdadapter.ConfigDescription) (cmdadapter.ResolvedCommand, error) {
			return cmdadapter.ResolvedCommand{
				Executable: "relative", Arguments: description.Arguments, CWD: "/clean",
				EffectiveEffects: graph.EffectSet{graph.EffectCompute}, EffectiveCapabilities: []string{cmdadapter.CapabilityProcessExecute}, Sandbox: description.SandboxExpectation,
			}, nil
		}), noOpRunner(), nil, nil, nil)
		_, err := executor.Execute(context.Background(), invocation(validConfig()))
		assertExecutionCode(t, err, cmdadapter.CodePolicyDenied)
	})

	t.Run("nonzero", func(t *testing.T) {
		executor := mustExecutor(t, successfulPolicy(), cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, _, stderr io.Writer) (cmdadapter.ProcessResult, error) {
			_, _ = io.WriteString(stderr, "unsafe secret://vault/ref")
			return cmdadapter.ProcessResult{ExitCode: 7, EnforcedSandbox: request.Sandbox}, nil
		}), nil, nil, nil)
		_, err := executor.Execute(context.Background(), invocation(validConfig()))
		assertExecutionCode(t, err, cmdadapter.CodeNonZeroExit)
		var executionError *stepkind.ExecutionError
		if !errors.As(err, &executionError) || executionError.Details["exit_code"] != "7" || strings.Contains(err.Error(), "unsafe") {
			t.Fatalf("nonzero error = %#v", err)
		}
	})

	t.Run("artifact", func(t *testing.T) {
		config := validConfig()
		config["capture"] = map[string]any{"stdout": map[string]any{"as": "artifact", "name": "report", "max_bytes": 20}}
		executor := mustExecutor(t, successfulPolicy(), cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, _ io.Writer) (cmdadapter.ProcessResult, error) {
			_, _ = io.WriteString(stdout, "content")
			return processSuccess(request), nil
		}), nil, cmdadapter.ArtifactSinkFunc(func(context.Context, cmdadapter.ArtifactCapture, io.Reader) (values.Value, error) {
			return values.Value{}, errors.New("artifact /private/path failed")
		}), nil)
		_, err := executor.Execute(context.Background(), invocation(config))
		assertExecutionCode(t, err, cmdadapter.CodeArtifactFailed)
		if strings.Contains(err.Error(), "/private") {
			t.Fatalf("artifact error leaked cause: %v", err)
		}
	})
}

func TestCancellationDuringPolicySecretAndProcessIsTyped(t *testing.T) {
	t.Run("policy cancellation", func(t *testing.T) {
		started := make(chan struct{})
		executor := mustExecutor(t, cmdadapter.PolicyFunc(func(ctx context.Context, _ cmdadapter.ConfigDescription) (cmdadapter.ResolvedCommand, error) {
			close(started)
			<-ctx.Done()
			return cmdadapter.ResolvedCommand{}, ctx.Err()
		}), noOpRunner(), nil, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-started; cancel() }()
		_, err := executor.Execute(ctx, invocation(validConfig()))
		assertExecutionCode(t, err, cmdadapter.CodeCanceled)
	})

	t.Run("secret cancellation", func(t *testing.T) {
		config := validConfig()
		config["env"] = map[string]any{"TOKEN": "secret://vault/token"}
		started := make(chan struct{})
		resolver := values.SecretResolverFunc(func(ctx context.Context, _ values.SecretRef) (*values.ResolvedSecret, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		executor := mustExecutor(t, successfulPolicy(), noOpRunner(), resolver, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-started; cancel() }()
		_, err := executor.Execute(ctx, invocation(config))
		assertExecutionCode(t, err, cmdadapter.CodeCanceled)
	})

	t.Run("timeout", func(t *testing.T) {
		config := validConfig()
		config["timeout"] = "5ms"
		runner := cmdadapter.ProcessRunnerFunc(func(ctx context.Context, _ cmdadapter.ProcessRequest, _, _ io.Writer) (cmdadapter.ProcessResult, error) {
			<-ctx.Done()
			return cmdadapter.ProcessResult{}, ctx.Err()
		})
		executor := mustExecutor(t, successfulPolicy(), runner, nil, nil, nil)
		_, err := executor.Execute(context.Background(), invocation(config))
		assertExecutionCode(t, err, cmdadapter.CodeTimeout)
	})

	t.Run("process cancellation", func(t *testing.T) {
		started := make(chan struct{})
		runner := cmdadapter.ProcessRunnerFunc(func(ctx context.Context, _ cmdadapter.ProcessRequest, _, _ io.Writer) (cmdadapter.ProcessResult, error) {
			close(started)
			<-ctx.Done()
			return cmdadapter.ProcessResult{}, ctx.Err()
		})
		executor := mustExecutor(t, successfulPolicy(), runner, nil, nil, nil)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { <-started; cancel() }()
		_, err := executor.Execute(ctx, invocation(validConfig()))
		assertExecutionCode(t, err, cmdadapter.CodeCanceled)
	})
}

func TestStoreArtifactSinkUsesWorkflowStoreAndFixesImmutableCaptureMetadata(t *testing.T) {
	content := []byte("artifact-content")
	capture := cmdadapter.ArtifactCapture{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "node", Attempt: 1},
		Stream:   cmdadapter.StreamStdout, Name: "report", MaxBytes: 64,
		Metadata: values.Metadata{
			Producer:  values.Producer{Kind: "node_output", Reference: "run-1/node", Output: "report"},
			MediaType: "text/plain", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		},
	}
	store := &recordingArtifactStore{}
	factory := cmdadapter.ArtifactPutRequestFactoryFunc(func(_ context.Context, got cmdadapter.ArtifactCapture) (values.ArtifactPutRequest, error) {
		if got.Name != capture.Name {
			t.Fatalf("factory capture = %#v", got)
		}
		return values.ArtifactPutRequest{
			Store: "fixture", Owner: values.ArtifactOwner{Scope: values.ArtifactOwnerRun, ID: "run-1"},
			Metadata: values.Metadata{
				Producer: values.Producer{Kind: "wrong", Reference: "wrong"}, MediaType: "application/json",
				Redaction: values.RedactionPublic, Retention: values.RetentionProject,
			},
			MaxBytes: 1, CreatedAt: time.Unix(100, 0),
			Access: values.ArtifactAccess{Principal: "test", RunID: "run-1", At: time.Unix(100, 0)},
		}, nil
	})
	sink := cmdadapter.StoreArtifactSink{Store: store, Requests: factory}
	value, err := sink.CaptureArtifact(context.Background(), capture, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if store.request.Metadata != capture.Metadata || store.request.MaxBytes != capture.MaxBytes || !bytes.Equal(store.content, content) {
		t.Fatalf("store request/content = %#v / %q", store.request, store.content)
	}
	if value.Type != values.TypeArtifact || value.Artifact == nil || value.Producer != capture.Metadata.Producer {
		t.Fatalf("artifact value = %#v", value)
	}
}

func TestConcurrentExecutionIsDeterministicAndDefensive(t *testing.T) {
	config := validConfig()
	config["capture"] = map[string]any{"stdout": map[string]any{"name": "data", "parse": "json"}}
	policy := cmdadapter.PolicyFunc(func(_ context.Context, description cmdadapter.ConfigDescription) (cmdadapter.ResolvedCommand, error) {
		if len(description.Arguments) != 3 {
			t.Errorf("policy description was mutated: %#v", description.Arguments)
		}
		return resolvedFrom(description), nil
	})
	runner := cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, stdout, _ io.Writer) (cmdadapter.ProcessResult, error) {
		_, err := io.WriteString(stdout, `{"stable":true}`)
		return processSuccess(request), err
	})
	executor := mustExecutor(t, policy, runner, nil, nil, nil)

	const workers = 24
	digests := make(chan string, workers)
	errorsC := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := executor.Execute(context.Background(), invocation(config))
			if err != nil {
				errorsC <- err
				return
			}
			digests <- result.Outputs["data"].Digest
		}()
	}
	group.Wait()
	close(errorsC)
	close(digests)
	for err := range errorsC {
		t.Errorf("Execute() error = %v", err)
	}
	var first string
	for digest := range digests {
		if first == "" {
			first = digest
		} else if digest != first {
			t.Fatalf("nondeterministic digest %q != %q", digest, first)
		}
	}
}

func TestOSProcessRunnerIsDirectAndRejectsUnattestedIsolation(t *testing.T) {
	path, err := exec.LookPath("printf")
	if err != nil {
		t.Skip("printf is unavailable")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Clean(path)
	request := cmdadapter.ProcessRequest{
		Executable: path, Arguments: []string{"%s", "$(not-a-shell)"}, CWD: t.TempDir(),
		Sandbox: cmdadapter.SandboxSpec{Profile: cmdadapter.SandboxDirect},
	}
	var stdout, stderr bytes.Buffer
	result, err := (cmdadapter.OSProcessRunner{}).Run(context.Background(), request, &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 || stdout.String() != "$(not-a-shell)" || stderr.Len() != 0 || result.EnforcedSandbox != request.Sandbox {
		t.Fatalf("Run() = %#v, stdout %q, stderr %q", result, stdout.String(), stderr.String())
	}
	request.Sandbox.Profile = "container"
	if _, err := (cmdadapter.OSProcessRunner{}).Run(context.Background(), request, io.Discard, io.Discard); !errors.Is(err, cmdadapter.ErrProcessFailed) {
		t.Fatalf("strong sandbox error = %v", err)
	}
}

func TestOSProcessRunnerUsesOnlyExplicitEnvironmentAndStopsOnCancellation(t *testing.T) {
	envPath, err := exec.LookPath("env")
	if err != nil {
		t.Skip("env is unavailable")
	}
	envPath, err = filepath.Abs(envPath)
	if err != nil {
		t.Fatal(err)
	}
	request := cmdadapter.ProcessRequest{
		Executable: filepath.Clean(envPath), CWD: t.TempDir(),
		Environment: []cmdadapter.EnvironmentVariable{{Name: "ONLY", Value: []byte("present")}},
		Sandbox:     cmdadapter.SandboxSpec{Profile: cmdadapter.SandboxDirect},
	}
	var output bytes.Buffer
	if _, runErr := (cmdadapter.OSProcessRunner{}).Run(context.Background(), request, &output, io.Discard); runErr != nil {
		t.Fatalf("env Run() error = %v", runErr)
	}
	if output.String() != "ONLY=present\n" {
		t.Fatalf("direct runner inherited environment: %q", output.String())
	}

	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("sleep is unavailable")
	}
	sleepPath, err = filepath.Abs(sleepPath)
	if err != nil {
		t.Fatal(err)
	}
	request.Executable = filepath.Clean(sleepPath)
	request.Arguments = []string{"30"}
	request.Environment = nil
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = (cmdadapter.OSProcessRunner{}).Run(ctx, request, io.Discard, io.Discard)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("canceled Run() = %v after %s", err, time.Since(started))
	}
}

func validConfig() graph.Config {
	return graph.Config{
		"command":      "tool",
		"arguments":    []any{"alpha", "beta", "gamma"},
		"cwd":          "workspace",
		"timeout":      "2s",
		"effects":      []any{"compute"},
		"capabilities": []any{cmdadapter.CapabilityProcessExecute},
		"sandbox":      map[string]any{"profile": cmdadapter.SandboxDirect},
	}
}

func invocation(config graph.Config) stepkind.PreparedInvocation {
	return stepkind.PreparedInvocation{Invocation: stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: "run-1", NodeID: "node", Attempt: 1},
		Config:   config, Inputs: values.ValueSet{},
	}}
}

func successfulPolicy() cmdadapter.Policy {
	return cmdadapter.PolicyFunc(func(_ context.Context, description cmdadapter.ConfigDescription) (cmdadapter.ResolvedCommand, error) {
		return resolvedFrom(description), nil
	})
}

func resolvedFrom(description cmdadapter.ConfigDescription) cmdadapter.ResolvedCommand {
	return cmdadapter.ResolvedCommand{
		Executable: "/resolved/tool", Arguments: append([]string(nil), description.Arguments...), CWD: "/resolved/workspace",
		EffectiveEffects:      graph.EffectSet{graph.EffectCompute},
		EffectiveCapabilities: []string{cmdadapter.CapabilityProcessExecute}, Sandbox: description.SandboxExpectation,
	}
}

func noOpRunner() cmdadapter.ProcessRunner {
	return cmdadapter.ProcessRunnerFunc(func(_ context.Context, request cmdadapter.ProcessRequest, _, _ io.Writer) (cmdadapter.ProcessResult, error) {
		return processSuccess(request), nil
	})
}

func processSuccess(request cmdadapter.ProcessRequest) cmdadapter.ProcessResult {
	return cmdadapter.ProcessResult{ExitCode: 0, EnforcedSandbox: request.Sandbox}
}

func mustExecutor(
	t *testing.T,
	policy cmdadapter.Policy,
	runner cmdadapter.ProcessRunner,
	secrets values.SecretResolver,
	artifacts cmdadapter.ArtifactSink,
	events cmdadapter.EventSink,
) *cmdadapter.Executor {
	t.Helper()
	executor, err := cmdadapter.New(cmdadapter.Options{Policy: policy, Runner: runner, Secrets: secrets, Artifacts: artifacts, Events: events})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return executor
}

func assertExecutionCode(t *testing.T, err error, code string) {
	t.Helper()
	var executionError *stepkind.ExecutionError
	if !errors.As(err, &executionError) || executionError.Code != code {
		t.Fatalf("error = %#v, want execution code %q", err, code)
	}
	if validationErr := executionError.Validate(); validationErr != nil {
		t.Fatalf("ExecutionError.Validate() = %v", validationErr)
	}
}

func hasErrors(findings []diagnostic.Diagnostic) bool {
	for _, finding := range findings {
		if finding.Severity == diagnostic.SeverityError {
			return true
		}
	}
	return false
}

func diagnosticsContain(findings []diagnostic.Diagnostic, part string) bool {
	for _, finding := range findings {
		if strings.Contains(finding.Message, part) {
			return true
		}
	}
	return false
}

func equalEffects(left, right graph.EffectSet) bool {
	return equalStrings(effectStrings(left), effectStrings(right))
}

func effectStrings(effects graph.EffectSet) []string {
	result := make([]string, len(effects))
	for index, effect := range effects {
		result[index] = string(effect)
	}
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func allZero(content []byte) bool {
	for _, value := range content {
		if value != 0 {
			return false
		}
	}
	return true
}

type recordingArtifactStore struct {
	request values.ArtifactPutRequest
	content []byte
}

func (s *recordingArtifactStore) Put(_ context.Context, request values.ArtifactPutRequest, source io.Reader) (values.ArtifactMetadata, error) {
	content, err := io.ReadAll(source)
	if err != nil {
		return values.ArtifactMetadata{}, err
	}
	s.request = request
	s.content = append([]byte(nil), content...)
	ref := values.ArtifactRef{
		Store: request.Store, URI: "artifact://fixture/report", Digest: values.SHA256Digest(content),
		MediaType: request.Metadata.MediaType, SizeBytes: int64(len(content)), Producer: request.Metadata.Producer,
		Redaction: request.Metadata.Redaction, Retention: request.Metadata.Retention,
	}
	return values.ArtifactMetadata{Ref: ref, Owner: request.Owner, CreatedAt: request.CreatedAt}, nil
}

func (*recordingArtifactStore) Open(context.Context, values.ArtifactAccess, values.ArtifactRef) (values.ArtifactReadCloser, error) {
	return nil, errors.New("unused")
}

func (*recordingArtifactStore) Stat(context.Context, values.ArtifactAccess, values.ArtifactRef) (values.ArtifactMetadata, error) {
	return values.ArtifactMetadata{}, errors.New("unused")
}

func (*recordingArtifactStore) Delete(context.Context, values.ArtifactDeleteRequest) (values.ArtifactCleanupResult, error) {
	return values.ArtifactCleanupResult{}, errors.New("unused")
}

func (*recordingArtifactStore) Cleanup(context.Context, values.ArtifactCleanupRequest) (values.ArtifactCleanupResult, error) {
	return values.ArtifactCleanupResult{}, errors.New("unused")
}
