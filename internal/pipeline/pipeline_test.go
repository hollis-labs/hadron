package pipeline

import (
	"os"
	"path/filepath"
	"strings"
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

func TestStage_DependsOn(t *testing.T) {
	spec, err := ParseBytes([]byte(`
meta:
  name: deps-test
stages:
  - name: build
    blueprint_path: ./build.yaml
  - name: test
    blueprint_path: ./test.yaml
    depends_on:
      - build
  - name: deploy
    blueprint_path: ./deploy.yaml
    depends_on:
      - build
      - test
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(spec.Stages[0].DependsOn) != 0 {
		t.Fatalf("build stage should have no depends_on")
	}
	if len(spec.Stages[1].DependsOn) != 1 || spec.Stages[1].DependsOn[0] != "build" {
		t.Fatalf("test stage depends_on mismatch: %v", spec.Stages[1].DependsOn)
	}
	if len(spec.Stages[2].DependsOn) != 2 {
		t.Fatalf("deploy stage should depend on 2 stages, got %d", len(spec.Stages[2].DependsOn))
	}
}

func TestStage_DependsOn_V1Compat(t *testing.T) {
	spec, err := ParseBytes([]byte(`
meta:
  name: v1
stages:
  - name: one
    blueprint_path: ./a.yaml
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if spec.Stages[0].DependsOn != nil {
		t.Fatalf("v1 pipeline should have nil depends_on, got %v", spec.Stages[0].DependsOn)
	}
}

func TestStage_Position(t *testing.T) {
	spec, err := ParseBytes([]byte(`
meta:
  name: pos-test
stages:
  - name: one
    blueprint_path: ./a.yaml
    position:
      x: 100.5
      y: 200.75
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	pos := spec.Stages[0].Position
	if pos == nil {
		t.Fatalf("position should not be nil")
	}
	if pos.X != 100.5 {
		t.Fatalf("expected x=100.5, got %f", pos.X)
	}
	if pos.Y != 200.75 {
		t.Fatalf("expected y=200.75, got %f", pos.Y)
	}
}

func TestStage_Position_V1Compat(t *testing.T) {
	spec, err := ParseBytes([]byte(`
meta:
  name: v1
stages:
  - name: one
    blueprint_path: ./a.yaml
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if spec.Stages[0].Position != nil {
		t.Fatalf("v1 pipeline should have nil position")
	}
}

func TestStage_Outputs(t *testing.T) {
	spec, err := ParseBytes([]byte(`
meta:
  name: outputs-test
stages:
  - name: build
    blueprint_path: ./build.yaml
    outputs:
      artifact_path: "{{ steps.build.path }}"
      version: "{{ steps.build.version }}"
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	out := spec.Stages[0].Outputs
	if len(out) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(out))
	}
	if out["artifact_path"] != "{{ steps.build.path }}" {
		t.Fatalf("unexpected artifact_path value: %s", out["artifact_path"])
	}
	if out["version"] != "{{ steps.build.version }}" {
		t.Fatalf("unexpected version value: %s", out["version"])
	}
}

func TestStage_Outputs_V1Compat(t *testing.T) {
	spec, err := ParseBytes([]byte(`
meta:
  name: v1
stages:
  - name: one
    blueprint_path: ./a.yaml
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if spec.Stages[0].Outputs != nil {
		t.Fatalf("v1 pipeline should have nil outputs")
	}
}

func TestStage_Outputs_InvalidKey(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: bad-outputs
stages:
  - name: one
    blueprint_path: ./a.yaml
    outputs:
      "123bad": "val"
`))
	if err == nil {
		t.Fatalf("expected validation error for invalid output key")
	}
	if !strings.Contains(err.Error(), "invalid key") {
		t.Fatalf("expected 'invalid key' in error, got: %v", err)
	}
}

func TestDAG_ValidLinearChain(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: linear
stages:
  - name: A
    blueprint_path: ./a.yaml
  - name: B
    blueprint_path: ./b.yaml
    depends_on: [A]
  - name: C
    blueprint_path: ./c.yaml
    depends_on: [B]
`))
	if err != nil {
		t.Fatalf("valid DAG A→B→C should pass: %v", err)
	}
}

func TestDAG_ValidFanOut(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: fanout
stages:
  - name: A
    blueprint_path: ./a.yaml
  - name: B
    blueprint_path: ./b.yaml
    depends_on: [A]
  - name: C
    blueprint_path: ./c.yaml
    depends_on: [A]
  - name: D
    blueprint_path: ./d.yaml
    depends_on: [B, C]
`))
	if err != nil {
		t.Fatalf("valid fan-out DAG should pass: %v", err)
	}
}

func TestDAG_UnknownReference(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: bad-ref
stages:
  - name: A
    blueprint_path: ./a.yaml
    depends_on: [nonexistent]
`))
	if err == nil {
		t.Fatalf("expected error for unknown stage reference")
	}
	if !strings.Contains(err.Error(), `depends on unknown stage "nonexistent"`) {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestDAG_SelfReference(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: self-ref
stages:
  - name: A
    blueprint_path: ./a.yaml
    depends_on: [A]
`))
	if err == nil {
		t.Fatalf("expected error for self-reference")
	}
	if !strings.Contains(err.Error(), `depends on itself`) {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestDAG_DirectCycle(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: direct-cycle
stages:
  - name: A
    blueprint_path: ./a.yaml
    depends_on: [B]
  - name: B
    blueprint_path: ./b.yaml
    depends_on: [A]
`))
	if err == nil {
		t.Fatalf("expected error for direct cycle")
	}
	if !strings.Contains(err.Error(), "cycle detected") {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestDAG_TransitiveCycle(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: transitive-cycle
stages:
  - name: A
    blueprint_path: ./a.yaml
    depends_on: [C]
  - name: B
    blueprint_path: ./b.yaml
    depends_on: [A]
  - name: C
    blueprint_path: ./c.yaml
    depends_on: [B]
`))
	if err == nil {
		t.Fatalf("expected error for transitive cycle")
	}
	if !strings.Contains(err.Error(), "cycle detected") {
		t.Fatalf("wrong error: %v", err)
	}
}

func TestDAG_V1NoDependsOn(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: v1-compat
stages:
  - name: one
    blueprint_path: ./a.yaml
  - name: two
    blueprint_path: ./b.yaml
`))
	if err != nil {
		t.Fatalf("v1 pipeline with no depends_on should pass: %v", err)
	}
}

func TestStage_Outputs_InvalidKey_SpecialChars(t *testing.T) {
	_, err := ParseBytes([]byte(`
meta:
  name: bad-outputs
stages:
  - name: one
    blueprint_path: ./a.yaml
    outputs:
      "has spaces": "val"
`))
	if err == nil {
		t.Fatalf("expected validation error for output key with spaces")
	}
}
