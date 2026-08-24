package diagnostic

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/workflow/graph"
)

func TestJSONRoundTrip(t *testing.T) {
	t.Parallel()

	diagnostic := Diagnostic{
		Severity: SeverityError,
		Code:     CodeUnresolvedReference,
		Message:  "node deploy references steps.build.outputs.version without a declared dependency",
		Source: &graph.SourceRef{
			Format:      graph.SourceWorkflow,
			Locator:     "workflow.yaml",
			StartLine:   17,
			StartColumn: 20,
			EndLine:     18,
			EndColumn:   12,
			Path:        []string{"steps", "1", "with", "build_version"},
		},
		Related: []RelatedReference{{
			Message: "output is declared here",
			Source: graph.SourceRef{
				Format:      graph.SourceWorkflow,
				Locator:     "workflow.yaml",
				StartLine:   8,
				StartColumn: 3,
				EndLine:     10,
				EndColumn:   1,
				Path:        []string{"steps", "0", "outputs", "version"},
			},
		}},
		Remediation: &Remediation{
			Message:         "Declare build as a dependency before using its outputs.",
			SuggestedSyntax: "needs: [build]",
			Documentation:   "https://docs.example.test/workflows/dependencies",
		},
	}

	encoded, err := EncodeJSON(diagnostic)
	if err != nil {
		t.Fatalf("EncodeJSON() error = %v", err)
	}
	decoded, err := DecodeJSON(encoded)
	if err != nil {
		t.Fatalf("DecodeJSON() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, diagnostic) {
		t.Fatalf("round trip mismatch:\n got: %#v\nwant: %#v", decoded, diagnostic)
	}

	var transport map[string]any
	if err := json.Unmarshal(encoded, &transport); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	for _, field := range []string{"severity", "code", "message", "source", "related", "remediation"} {
		if _, ok := transport[field]; !ok {
			t.Errorf("transport JSON missing %q", field)
		}
	}
}

func TestReservedPrefixes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		prefix Prefix
		domain Domain
	}{
		{PrefixSource, DomainSourceValidation},
		{PrefixReference, DomainSourceValidation},
		{PrefixLegacy, DomainSourceValidation},
		{PrefixOutput, DomainSourceValidation},
		{PrefixValue, DomainValues},
		{PrefixArtifact, DomainValues},
		{PrefixPolicy, DomainPolicy},
		{PrefixEffect, DomainEffects},
		{PrefixWait, DomainWaits},
		{PrefixPersistence, DomainPersistence},
		{PrefixHost, DomainHostIntegration},
	}
	for _, tc := range cases {
		t.Run(string(tc.prefix), func(t *testing.T) {
			code, err := NewCode(tc.prefix, 1)
			if err != nil {
				t.Fatalf("NewCode() error = %v", err)
			}
			if got, want := code, Code("HADR-"+string(tc.prefix)+"-001"); got != want {
				t.Fatalf("NewCode() = %q, want %q", got, want)
			}
			if got, err := code.Domain(); err != nil || got != tc.domain {
				t.Fatalf("Code.Domain() = %q, %v; want %q, nil", got, err, tc.domain)
			}
		})
	}
}

func TestCodeValidation(t *testing.T) {
	t.Parallel()

	for _, code := range []Code{CodeUnresolvedReference, CodeUnsafeEffectRetry, CodeArchivedOutputShim, "HADR-HOST-999"} {
		if err := code.Validate(); err != nil {
			t.Errorf("Code(%q).Validate() error = %v", code, err)
		}
	}
	for _, code := range []Code{"", "HADR-UNKNOWN-001", "HADR-REF-000", "HADR-REF-01", "HADR-REF-+01", "hadr-ref-001"} {
		if err := code.Validate(); err == nil {
			t.Errorf("Code(%q).Validate() unexpectedly succeeded", code)
		}
	}
}

func TestSeverityValidation(t *testing.T) {
	t.Parallel()

	for _, severity := range []Severity{SeverityInfo, SeverityWarning, SeverityError} {
		if !severity.Valid() {
			t.Errorf("Severity(%q).Valid() = false", severity)
		}
	}
	if Severity("fatal").Valid() {
		t.Error("Severity(\"fatal\").Valid() = true")
	}
}

func TestExampleDiagnostics(t *testing.T) {
	t.Parallel()

	source := graph.SourceRef{Format: graph.SourceWorkflow, Locator: "workflow.yaml"}
	examples := []struct {
		name     string
		value    Diagnostic
		severity Severity
		code     Code
	}{
		{"unresolved reference", UnresolvedReference(source, "deploy", "steps.build.outputs.version", "build"), SeverityError, CodeUnresolvedReference},
		{"unsafe effect retry", UnsafeEffectRetry(source, "delete"), SeverityError, CodeUnsafeEffectRetry},
		{"archived output shim", ArchivedOutputShim(source, "build"), SeverityWarning, CodeArchivedOutputShim},
	}
	for _, example := range examples {
		t.Run(example.name, func(t *testing.T) {
			if err := example.value.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if example.value.Severity != example.severity || example.value.Code != example.code {
				t.Fatalf("severity/code = %q/%q, want %q/%q", example.value.Severity, example.value.Code, example.severity, example.code)
			}
			if example.value.Remediation == nil || example.value.Remediation.SuggestedSyntax == "" {
				t.Fatal("example is missing suggested target syntax")
			}
		})
	}
}

func TestSourceMappedValidationFixture(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("testdata", "unresolved-reference.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	diagnostic, err := DecodeJSON(data)
	if err != nil {
		t.Fatalf("DecodeJSON() error = %v", err)
	}
	if diagnostic.Source == nil {
		t.Fatal("fixture source is nil")
	}
	if diagnostic.Source.Format != graph.SourceWorkflow {
		t.Errorf("source format = %q, want %q", diagnostic.Source.Format, graph.SourceWorkflow)
	}
	if diagnostic.Source.StartLine == 0 || diagnostic.Source.EndLine <= diagnostic.Source.StartLine {
		t.Errorf("source line range = %d-%d, want a multi-line range", diagnostic.Source.StartLine, diagnostic.Source.EndLine)
	}
	wantPath := []string{"steps", "1", "with", "build_version"}
	if !reflect.DeepEqual(diagnostic.Source.Path, wantPath) {
		t.Errorf("source path = %#v, want %#v", diagnostic.Source.Path, wantPath)
	}
	if _, err := os.Stat(filepath.Join("testdata", filepath.Base(diagnostic.Source.Locator))); err != nil {
		t.Errorf("mapped YAML fixture: %v", err)
	}
}

func TestMalformedTransportDiagnosticsAreRejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		payload string
		field   string
	}{
		{"severity", `{"severity":"fatal","code":"HADR-REF-001","message":"bad"}`, "severity"},
		{"code", `{"severity":"error","code":"REF-001","message":"bad"}`, "code"},
		{"message", `{"severity":"error","code":"HADR-REF-001","message":" "}`, "message"},
		{"source format", `{"severity":"error","code":"HADR-REF-001","message":"bad","source":{"format":"yaml","locator":"workflow.yaml"}}`, "source.format"},
		{"source locator", `{"severity":"error","code":"HADR-REF-001","message":"bad","source":{"format":"workflow","locator":""}}`, "source.locator"},
		{"source range", `{"severity":"error","code":"HADR-REF-001","message":"bad","source":{"format":"workflow","locator":"workflow.yaml","start_line":4,"end_line":2}}`, "source.end_line"},
		{"orphaned start column", `{"severity":"error","code":"HADR-REF-001","message":"bad","source":{"format":"workflow","locator":"workflow.yaml","start_column":4}}`, "source.start_column"},
		{"orphaned end column", `{"severity":"error","code":"HADR-REF-001","message":"bad","source":{"format":"workflow","locator":"workflow.yaml","end_column":4}}`, "source.end_column"},
		{"related source", `{"severity":"error","code":"HADR-REF-001","message":"bad","related":[{"source":{"format":"workflow"}}]}`, "related[0].source.locator"},
		{"remediation", `{"severity":"error","code":"HADR-REF-001","message":"bad","remediation":{"message":""}}`, "remediation.message"},
		{"unknown field", `{"severity":"error","code":"HADR-REF-001","message":"bad","detail":"unknown"}`, "unknown field"},
		{"trailing value", `{"severity":"error","code":"HADR-REF-001","message":"bad"} {}`, "multiple JSON values"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeJSON([]byte(tc.payload))
			if err == nil {
				t.Fatal("DecodeJSON() unexpectedly succeeded")
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("DecodeJSON() error = %q, want field %q", err, tc.field)
			}
		})
	}
}

func TestValidationErrorIsStructured(t *testing.T) {
	t.Parallel()

	diagnostic := Diagnostic{Severity: "fatal", Code: "bad"}
	err := diagnostic.Validate()
	if err == nil {
		t.Fatal("Validate() unexpectedly succeeded")
	}
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("Validate() error type = %T, want *ValidationError", err)
	}
}
