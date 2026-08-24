package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestEventCollectorReturnsFullWritesAndReplaysClose(t *testing.T) {
	sinkFailure := errors.New("sink failed")
	collector, err := newStreamCollector(
		context.Background(), stepkind.InvocationIdentity{}, StreamStdout,
		&CaptureConfig{Mode: CaptureEvent, MaxBytes: 32}, nil,
		EventSinkFunc(func(context.Context, OperationalEvent) error { return sinkFailure }),
	)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("continue-draining-after-failure")
	if written, writeErr := collector.Write(content); writeErr != nil || written != len(content) {
		t.Fatalf("Write() = %d, %v; want %d, nil", written, writeErr, len(content))
	}
	if written, writeErr := collector.Write(content); writeErr != nil || written != len(content) {
		t.Fatalf("second Write() = %d, %v; want %d, nil", written, writeErr, len(content))
	}
	first := collector.close()
	second := collector.close()
	if !errors.Is(first.err, sinkFailure) || !errors.Is(second.err, sinkFailure) {
		t.Fatalf("close errors = %v, %v; want sink failure", first.err, second.err)
	}
}

func TestEventCollectorBoundsTotalEmissionAndKeepsDraining(t *testing.T) {
	var payload strings.Builder
	collector, err := newStreamCollector(
		context.Background(), stepkind.InvocationIdentity{}, StreamStdout,
		&CaptureConfig{Mode: CaptureEvent, MaxBytes: 4}, nil,
		EventSinkFunc(func(_ context.Context, event OperationalEvent) error {
			payload.Write(event.Payload)
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("unbounded-child-output")
	if written, writeErr := collector.Write(content); writeErr != nil || written != len(content) {
		t.Fatalf("Write() = %d, %v; want %d, nil", written, writeErr, len(content))
	}
	if written, writeErr := collector.Write(content); writeErr != nil || written != len(content) {
		t.Fatalf("second Write() = %d, %v; want %d, nil", written, writeErr, len(content))
	}
	result := collector.close()
	if !result.overflow || payload.String() != "unbo" {
		t.Fatalf("result = %#v, payload = %q", result, payload.String())
	}
}

func TestKnownSecretIsMaskedAcrossWriterChunksBeforeEventSink(t *testing.T) {
	ref, err := values.ParseSecretRef("secret://vault/token")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := values.NewResolvedSecret(ref, []byte("topsecret"))
	if err != nil {
		t.Fatal(err)
	}
	redactor, err := values.NewRedactor(secret)
	if err != nil {
		t.Fatal(err)
	}
	var payload strings.Builder
	collector, err := newStreamCollector(
		context.Background(), stepkind.InvocationIdentity{}, StreamStderr,
		&CaptureConfig{Mode: CaptureEvent, MaxBytes: 128}, nil,
		EventSinkFunc(func(_ context.Context, event OperationalEvent) error {
			payload.Write(event.Payload)
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	writer := redactor.Writer(collector)
	for _, chunk := range []string{"before-top", "sec", "ret-after"} {
		if written, writeErr := writer.Write([]byte(chunk)); writeErr != nil || written != len(chunk) {
			t.Fatalf("Write(%q) = %d, %v", chunk, written, writeErr)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	result := collector.close()
	if result.err != nil || payload.String() != "before-[REDACTED]-after" || strings.Contains(payload.String(), "topsecret") {
		t.Fatalf("payload = %q, result = %#v", payload.String(), result)
	}
}

func TestOutputParsersAreDeterministicAndExact(t *testing.T) {
	decoded, err := parseOutput(ParseJSON, []byte(`{"n":9007199254740993123456789}`))
	if err != nil {
		t.Fatal(err)
	}
	number := decoded.(map[string]any)["n"].(json.Number)
	if number != json.Number("9007199254740993123456789") {
		t.Fatalf("number = %q", number)
	}
	if _, trailingErr := parseOutput(ParseJSON, []byte(`{} {}`)); !errors.Is(trailingErr, ErrParseFailed) {
		t.Fatalf("trailing JSON error = %v", trailingErr)
	}

	lines, err := parseOutput(ParseLines, []byte("alpha\r\nbeta\n"))
	if err != nil {
		t.Fatal(err)
	}
	wantLines := []any{"alpha", "beta"}
	if !reflect.DeepEqual(lines, wantLines) {
		t.Fatalf("lines = %#v, want %#v", lines, wantLines)
	}

	kv, err := parseOutput(ParseKV, []byte("alpha=1\nbeta=two=parts\n"))
	if err != nil {
		t.Fatal(err)
	}
	wantKV := map[string]any{"alpha": "1", "beta": "two=parts"}
	if !reflect.DeepEqual(kv, wantKV) {
		t.Fatalf("kv = %#v, want %#v", kv, wantKV)
	}
	for _, malformed := range []string{"missing", "alpha=1\nalpha=2\n", "Bad=1\n"} {
		if _, err := parseOutput(ParseKV, []byte(malformed)); !errors.Is(err, ErrParseFailed) {
			t.Fatalf("parseKV(%q) error = %v", malformed, err)
		}
	}
}

func TestSetOutputRejectsDuplicatesAndMalformedNames(t *testing.T) {
	parsed, err := parseSetOutput([]byte("noise\n::set-output alpha=one\n"))
	if err != nil || parsed["alpha"] != "one" {
		t.Fatalf("parseSetOutput = %#v, %v", parsed, err)
	}
	for _, content := range []string{
		"::set-output alpha=one\n::set-output alpha=two\n",
		"::set-output Invalid=value\n",
		"::set-output missing-equals\n",
	} {
		if _, err := parseSetOutput([]byte(content)); !errors.Is(err, ErrParseFailed) {
			t.Fatalf("parseSetOutput(%q) error = %v", content, err)
		}
	}
}
