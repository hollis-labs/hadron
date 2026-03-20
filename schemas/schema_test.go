package schemas_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

// schemaDir returns the absolute path to the schemas/ directory.
func schemaDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(thisFile)
}

// repoRoot returns the repository root (parent of schemas/).
func repoRoot(t *testing.T) string {
	t.Helper()
	return filepath.Dir(schemaDir(t))
}

// loadSchema compiles a JSON Schema file and returns the compiled schema.
func loadSchema(t *testing.T, schemaFile string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	sch, err := c.Compile(schemaFile)
	if err != nil {
		t.Fatalf("failed to compile schema %s: %v", schemaFile, err)
	}
	return sch
}

// yamlToJSONInterface reads a YAML file and converts it to an interface{}
// suitable for JSON Schema validation (via JSON round-trip to normalise types).
func yamlToJSONInterface(t *testing.T, path string) interface{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	var raw interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to parse YAML %s: %v", path, err)
	}
	// YAML produces map[string]interface{} but also map[interface{}]interface{};
	// round-trip through JSON to normalise.
	normalised := normalizeYAML(raw)
	jsonBytes, err := json.Marshal(normalised)
	if err != nil {
		t.Fatalf("failed to marshal to JSON %s: %v", path, err)
	}
	var result interface{}
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		t.Fatalf("failed to unmarshal JSON %s: %v", path, err)
	}
	return result
}

// normalizeYAML recursively converts map[interface{}]interface{} (from gopkg.in/yaml.v3)
// to map[string]interface{} so it can be marshalled to JSON.
func normalizeYAML(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, v2 := range val {
			out[k] = normalizeYAML(v2)
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, v2 := range val {
			key, _ := k.(string)
			out[key] = normalizeYAML(v2)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, v2 := range val {
			out[i] = normalizeYAML(v2)
		}
		return out
	default:
		return v
	}
}

// collectYAMLFiles walks a directory and returns all .yaml/.yml file paths.
func collectYAMLFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".yaml" || ext == ".yml" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("failed to walk %s: %v", dir, err)
	}
	return files
}

// isPipelineFile returns true if the YAML file looks like a pipeline spec
// (has top-level "stages" key) rather than a blueprint.
func isPipelineFile(t *testing.T, path string) bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return false
	}
	_, hasStages := raw["stages"]
	_, hasMeta := raw["meta"]
	return hasStages || hasMeta
}

// ─── Blueprint validation ────────────────────────────────────────────

func TestBlueprintExamplesMatchSchema(t *testing.T) {
	root := repoRoot(t)
	schemaPath := filepath.Join(schemaDir(t), "blueprint-v0.4.schema.json")
	sch := loadSchema(t, schemaPath)

	// Collect blueprint YAML files from examples/ and testdata/blueprints/
	var blueprintFiles []string

	// examples/ — skip pipeline files and pipeline sub-directories' pipeline.yaml
	examplesDir := filepath.Join(root, "examples")
	for _, f := range collectYAMLFiles(t, examplesDir) {
		if isPipelineFile(t, f) {
			continue
		}
		blueprintFiles = append(blueprintFiles, f)
	}

	// testdata/blueprints/
	testBPDir := filepath.Join(root, "testdata", "blueprints")
	blueprintFiles = append(blueprintFiles, collectYAMLFiles(t, testBPDir)...)

	if len(blueprintFiles) == 0 {
		t.Fatal("no blueprint YAML files found to validate")
	}

	for _, f := range blueprintFiles {
		rel, _ := filepath.Rel(root, f)
		t.Run(rel, func(t *testing.T) {
			doc := yamlToJSONInterface(t, f)
			if err := sch.Validate(doc); err != nil {
				t.Errorf("schema validation failed for %s:\n%v", rel, err)
			}
		})
	}
}

func TestPipelineExamplesMatchSchema(t *testing.T) {
	root := repoRoot(t)
	schemaPath := filepath.Join(schemaDir(t), "pipeline-v2.schema.json")
	sch := loadSchema(t, schemaPath)

	// Collect pipeline YAML files from examples/ and testdata/pipelines/
	var pipelineFiles []string

	// examples/ — only pipeline files
	examplesDir := filepath.Join(root, "examples")
	for _, f := range collectYAMLFiles(t, examplesDir) {
		if isPipelineFile(t, f) {
			pipelineFiles = append(pipelineFiles, f)
		}
	}

	// testdata/pipelines/
	testPipeDir := filepath.Join(root, "testdata", "pipelines")
	pipelineFiles = append(pipelineFiles, collectYAMLFiles(t, testPipeDir)...)

	if len(pipelineFiles) == 0 {
		t.Fatal("no pipeline YAML files found to validate")
	}

	for _, f := range pipelineFiles {
		rel, _ := filepath.Rel(root, f)
		t.Run(rel, func(t *testing.T) {
			doc := yamlToJSONInterface(t, f)
			if err := sch.Validate(doc); err != nil {
				t.Errorf("schema validation failed for %s:\n%v", rel, err)
			}
		})
	}
}

// ─── Negative tests: schema rejects invalid blueprints ───────────────

func TestSchemaRejectsInvalidBlueprint(t *testing.T) {
	schemaPath := filepath.Join(schemaDir(t), "blueprint-v0.4.schema.json")
	sch := loadSchema(t, schemaPath)

	root := repoRoot(t)
	invalidDir := filepath.Join(root, "testdata", "invalid", "blueprints")
	files := collectYAMLFiles(t, invalidDir)

	if len(files) == 0 {
		t.Fatal("no invalid blueprint YAML files found")
	}

	for _, f := range files {
		rel, _ := filepath.Rel(root, f)
		t.Run(rel, func(t *testing.T) {
			doc := yamlToJSONInterface(t, f)
			if err := sch.Validate(doc); err == nil {
				t.Errorf("expected schema validation to FAIL for %s, but it passed", rel)
			}
		})
	}
}

// ─── Negative tests: schema rejects invalid pipelines ────────────────

func TestSchemaRejectsInvalidPipeline(t *testing.T) {
	schemaPath := filepath.Join(schemaDir(t), "pipeline-v2.schema.json")
	sch := loadSchema(t, schemaPath)

	root := repoRoot(t)
	invalidDir := filepath.Join(root, "testdata", "invalid", "pipelines")
	files := collectYAMLFiles(t, invalidDir)

	if len(files) == 0 {
		t.Fatal("no invalid pipeline YAML files found")
	}

	for _, f := range files {
		rel, _ := filepath.Rel(root, f)
		t.Run(rel, func(t *testing.T) {
			doc := yamlToJSONInterface(t, f)
			if err := sch.Validate(doc); err == nil {
				t.Errorf("expected schema validation to FAIL for %s, but it passed", rel)
			}
		})
	}
}
