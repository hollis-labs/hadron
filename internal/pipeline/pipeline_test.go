package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseAndValidate(t *testing.T) {
	spec, err := ParseBytes([]byte(`
meta:
  name: demo
stages:
  - name: one
    blueprint_path: ./a.yaml
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(spec.Stages) != 1 {
		t.Fatalf("expected 1 stage")
	}
	if !spec.ShouldStopOnFail() {
		t.Fatalf("default stop_on_fail should be true")
	}

	_, err = ParseBytes([]byte(`
meta:
  name: bad
stages:
  - name: missing-path
`))
	if err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestParse_JSONAndJSONC(t *testing.T) {
	jsonSpec := []byte(`{
  "meta": { "name": "json-pipeline" },
  "stages": [{ "name": "one", "blueprint_path": "./a.yaml" }]
}`)
	if _, err := ParseBytes(jsonSpec); err != nil {
		t.Fatalf("parse json pipeline: %v", err)
	}

	jsoncSpec := []byte(`{
  "meta": { "name": "jsonc-pipeline", },
  "stages": [
    { "name": "one", "blueprint_path": "./a.yaml", },
  ],
}`)
	if _, err := ParseBytes(jsoncSpec); err != nil {
		t.Fatalf("parse jsonc pipeline: %v", err)
	}
}

func TestParseFile_JSONCByExtension(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pipeline.jsonc")
	src := `{
  "meta": { "name": "file-pipeline" },
  "stages": [{ "name": "one", "blueprint_path": "./a.yaml" }],
}`
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("write pipeline jsonc: %v", err)
	}
	spec, err := ParseFile(path)
	if err != nil {
		t.Fatalf("parse file jsonc pipeline: %v", err)
	}
	if spec.Meta.Name != "file-pipeline" {
		t.Fatalf("unexpected pipeline name: %s", spec.Meta.Name)
	}
}
