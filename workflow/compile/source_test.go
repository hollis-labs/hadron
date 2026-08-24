package compile_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"gopkg.in/yaml.v3"
)

func TestLoadFileNamedWorkflow(t *testing.T) {
	result, err := compile.LoadFile("testdata/valid.workflow.yaml")
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	assertLoaded(t, result, "testdata/valid.workflow.yaml")

	if result.Source.Document.Kind != yaml.DocumentNode {
		t.Fatalf("Document.Kind = %v, want DocumentNode", result.Source.Document.Kind)
	}
	if got := string(result.Source.Bytes()); !strings.Contains(got, "{{ unresolved.value }}") {
		t.Fatalf("Bytes() did not preserve interpolation source: %q", got)
	}
}

func TestLoadFileDirectoryDefault(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "workflow.yaml")
	if err := os.WriteFile(path, []byte("workflow:\n  name: default\nsteps: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := compile.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile() error = %v", err)
	}
	assertLoaded(t, result, path)
}

func TestLoadBytesPreservesASTWithoutEvaluationOrResolution(t *testing.T) {
	const source = `workflow:
  name: memory
steps:
  - id: untouched
    if: "not valid expression ("
    call:
      definition:
        locator: does-not-exist.workflow.yaml
    config:
      interpolation: "{{ missing.output }}"
`
	result := compile.LoadBytes("memory.workflow.yaml", []byte(source))
	assertLoaded(t, result, "memory.workflow.yaml")

	assertScalarValue(t, result.Source, []string{"steps", "0", "if"}, "not valid expression (")
	assertScalarValue(t, result.Source, []string{"steps", "0", "call", "definition", "locator"}, "does-not-exist.workflow.yaml")
	assertScalarValue(t, result.Source, []string{"steps", "0", "config", "interpolation"}, "{{ missing.output }}")
}

func TestSourceLocationsPreserveSemanticPathsAndCoordinates(t *testing.T) {
	result, err := compile.LoadFile("testdata/valid.workflow.yaml")
	if err != nil {
		t.Fatal(err)
	}
	assertLoaded(t, result, "testdata/valid.workflow.yaml")

	tests := []struct {
		path   []string
		line   int
		column int
	}{
		{path: nil, line: 1, column: 1},
		{path: []string{"workflow"}, line: 2, column: 3},
		{path: []string{"workflow", "name"}, line: 2, column: 9},
		{path: []string{"steps"}, line: 4, column: 3},
		{path: []string{"steps", "0"}, line: 4, column: 5},
		{path: []string{"steps", "0", "id"}, line: 4, column: 9},
		{path: []string{"steps", "0", "call", "definition", "locator"}, line: 9, column: 18},
	}
	for _, test := range tests {
		ref, ok := result.Source.Location(test.path...)
		if !ok {
			t.Errorf("Location(%v) was not found", test.path)
			continue
		}
		if ref.Format != graph.SourceWorkflow || ref.Locator != "testdata/valid.workflow.yaml" {
			t.Errorf("Location(%v) identity = %q, %q", test.path, ref.Format, ref.Locator)
		}
		if ref.StartLine != test.line || ref.StartColumn != test.column {
			t.Errorf("Location(%v) = %d:%d, want %d:%d", test.path, ref.StartLine, ref.StartColumn, test.line, test.column)
		}
		if !slices.Equal(ref.Path, test.path) {
			t.Errorf("Location(%v).Path = %v", test.path, ref.Path)
		}
		if ref.EndLine != 0 || ref.EndColumn != 0 {
			t.Errorf("Location(%v) has unsupported derived end coordinates: %+v", test.path, ref)
		}
	}

	locations := result.Source.Locations()
	wantPrefix := [][]string{nil, {"workflow"}, {"workflow", "name"}, {"steps"}, {"steps", "0"}}
	if len(locations) < len(wantPrefix) {
		t.Fatalf("Locations() returned %d entries", len(locations))
	}
	for i, want := range wantPrefix {
		if !slices.Equal(locations[i].Path, want) {
			t.Errorf("Locations()[%d].Path = %v, want %v", i, locations[i].Path, want)
		}
	}

	locations[1].Path[0] = "mutated"
	ref, _ := result.Source.Location("workflow")
	if !slices.Equal(ref.Path, []string{"workflow"}) {
		t.Fatalf("Locations() exposed mutable internal path: %v", ref.Path)
	}
	original := result.Source.Bytes()
	original[0] = 'X'
	if result.Source.Bytes()[0] == 'X' {
		t.Fatal("Bytes() exposed mutable internal bytes")
	}
}

func TestMalformedYAMLIsStructuredDiagnostic(t *testing.T) {
	result, err := compile.LoadFile("testdata/malformed.workflow.yaml")
	if err != nil {
		t.Fatalf("LoadFile() operational error = %v", err)
	}
	d := assertSingleDiagnostic(t, result, compile.CodeMalformedSource)
	if d.Source.StartLine == 0 {
		t.Fatalf("malformed diagnostic has no parser-provided line: %+v", d.Source)
	}
	if d.Source.StartColumn != 0 {
		t.Fatalf("malformed diagnostic invented column %d", d.Source.StartColumn)
	}
}

func TestMultipleDocumentsAreRejected(t *testing.T) {
	result, err := compile.LoadFile("testdata/multiple.workflow.yaml")
	if err != nil {
		t.Fatalf("LoadFile() operational error = %v", err)
	}
	d := assertSingleDiagnostic(t, result, compile.CodeMultipleSourceDocuments)
	if d.Source.StartLine != 4 || d.Source.StartColumn != 1 {
		t.Fatalf("multiple-document source = %d:%d, want 4:1", d.Source.StartLine, d.Source.StartColumn)
	}
}

func TestUnsupportedFilenamesAreRejectedBeforeParsing(t *testing.T) {
	for _, locator := range []string{
		"workflow.yml",
		"generic.yaml",
		"named.workflow.yml",
		"pipeline.yaml",
		"blueprint.yaml",
		".workflow.yaml",
		"",
	} {
		t.Run(strings.ReplaceAll(locator, "/", "_"), func(t *testing.T) {
			result := compile.LoadBytes(locator, []byte("not: [valid"))
			d := assertSingleDiagnostic(t, result, compile.CodeUnsupportedSourceName)
			if err := d.Validate(); err != nil {
				t.Fatalf("diagnostic.Validate() = %v", err)
			}
		})
	}
}

func TestReadFailureIsOrdinaryError(t *testing.T) {
	result, err := compile.LoadFile(filepath.Join(t.TempDir(), "missing.workflow.yaml"))
	if err == nil {
		t.Fatal("LoadFile() error = nil, want filesystem error")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("LoadFile() error = %v, want fs.ErrNotExist", err)
	}
	if result.Source != nil || len(result.Diagnostics) != 0 {
		t.Fatalf("operational failure result = %+v", result)
	}
}

func TestLegacyRootShapesAreRejected(t *testing.T) {
	tests := []struct {
		file   string
		format graph.SourceFormat
		path   []string
		line   int
	}{
		{file: "testdata/blueprint.workflow.yaml", format: graph.SourceArchivedBlueprint, path: []string{"blueprint"}, line: 2},
		{file: "testdata/pipeline.workflow.yaml", format: graph.SourceArchivedPipeline, path: []string{"stages"}, line: 3},
	}
	for _, test := range tests {
		t.Run(test.file, func(t *testing.T) {
			result, err := compile.LoadFile(test.file)
			if err != nil {
				t.Fatalf("LoadFile() operational error = %v", err)
			}
			d := assertSingleDiagnostic(t, result, compile.CodeLegacySource)
			if d.Source.Format != test.format || d.Source.StartLine != test.line || d.Source.StartColumn != 1 || !slices.Equal(d.Source.Path, test.path) {
				t.Fatalf("legacy source reference = %+v", d.Source)
			}
			if d.Remediation == nil || d.Remediation.Documentation == "" || !strings.Contains(d.Remediation.Message, "rewrite") {
				t.Fatalf("legacy remediation = %+v", d.Remediation)
			}
		})
	}
}

func TestLegacyDetectionRequiresActualRootShape(t *testing.T) {
	for name, source := range map[string]string{
		"workflow marker wins": "workflow: {name: current}\nmeta: {}\nstages: []\n",
		"blueprint scalar":     "blueprint: just-data\n",
		"nested blueprint":     "workflow: {name: current}\nmetadata:\n  blueprint: {}\n",
	} {
		t.Run(name, func(t *testing.T) {
			result := compile.LoadBytes("shape.workflow.yaml", []byte(source))
			assertLoaded(t, result, "shape.workflow.yaml")
		})
	}
}

func TestRootStagesSequenceIsLegacyPipeline(t *testing.T) {
	result := compile.LoadBytes("stages.workflow.yaml", []byte("stages: []\n"))
	d := assertSingleDiagnostic(t, result, compile.CodeLegacySource)
	if d.Source.Format != graph.SourceArchivedPipeline || !slices.Equal(d.Source.Path, []string{"stages"}) {
		t.Fatalf("legacy source reference = %+v", d.Source)
	}
}

func TestAmbiguousMappingsProduceDeterministicDiagnostics(t *testing.T) {
	const source = `workflow:
  name: first
  name: second
steps:
  - id: one
    config:
      value: first
      value: second
`
	first := compile.LoadBytes("duplicate.workflow.yaml", []byte(source))
	second := compile.LoadBytes("duplicate.workflow.yaml", []byte(source))
	if first.Source != nil || second.Source != nil {
		t.Fatal("ambiguous source was accepted")
	}
	if !reflect.DeepEqual(first.Diagnostics, second.Diagnostics) {
		t.Fatalf("diagnostics are not deterministic:\nfirst:  %#v\nsecond: %#v", first.Diagnostics, second.Diagnostics)
	}
	if len(first.Diagnostics) != 2 {
		t.Fatalf("len(Diagnostics) = %d, want 2", len(first.Diagnostics))
	}
	wantPaths := [][]string{{"workflow", "name"}, {"steps", "0", "config", "value"}}
	for i, d := range first.Diagnostics {
		if d.Code != compile.CodeAmbiguousSourceShape || !slices.Equal(d.Source.Path, wantPaths[i]) {
			t.Errorf("Diagnostics[%d] = %+v", i, d)
		}
		if len(d.Related) != 1 || d.Related[0].Message != "first declaration" {
			t.Errorf("Diagnostics[%d].Related = %+v", i, d.Related)
		}
		if err := d.Validate(); err != nil {
			t.Errorf("Diagnostics[%d].Validate() = %v", i, err)
		}
	}
}

func TestNonStringMappingKeyIsAmbiguous(t *testing.T) {
	result := compile.LoadBytes("key.workflow.yaml", []byte("workflow: {}\ntrue: value\n"))
	d := assertSingleDiagnostic(t, result, compile.CodeAmbiguousSourceShape)
	if d.Source.StartLine != 2 || d.Source.StartColumn != 1 {
		t.Fatalf("ambiguous key location = %+v", d.Source)
	}
}

func assertLoaded(t *testing.T, result compile.Result, locator string) {
	t.Helper()
	if result.Source == nil {
		t.Fatalf("Source = nil; Diagnostics = %+v", result.Diagnostics)
	}
	if len(result.Diagnostics) != 0 {
		t.Fatalf("Diagnostics = %+v", result.Diagnostics)
	}
	if result.Source.Locator != locator {
		t.Fatalf("Source.Locator = %q, want %q", result.Source.Locator, locator)
	}
}

func assertSingleDiagnostic(t *testing.T, result compile.Result, code diagnostic.Code) diagnostic.Diagnostic {
	t.Helper()
	if result.Source != nil {
		t.Fatalf("Source = %+v, want nil", result.Source)
	}
	if len(result.Diagnostics) != 1 {
		t.Fatalf("Diagnostics = %+v, want one", result.Diagnostics)
	}
	d := result.Diagnostics[0]
	if d.Code != code {
		t.Fatalf("Diagnostic.Code = %q, want %q", d.Code, code)
	}
	if err := d.Validate(); err != nil {
		t.Fatalf("Diagnostic.Validate() = %v", err)
	}
	return d
}

func assertScalarValue(t *testing.T, source *compile.Source, path []string, want string) {
	t.Helper()
	node, ok := source.Node(path...)
	if !ok {
		t.Fatalf("Node(%v) was not found", path)
	}
	if node.Kind != yaml.ScalarNode || node.Value != want {
		t.Fatalf("Node(%v) = kind %v value %q, want scalar %q", path, node.Kind, node.Value, want)
	}
}
