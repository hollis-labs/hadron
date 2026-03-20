package blueprint

import (
	"os"
	"testing"
)

// ─── Parse Tests ──────────────────────────────────────────────────────────────

func TestParse_ValidYAML(t *testing.T) {
	src := []byte(`
version: "0.4"
blueprint:
  name: my-app
steps:
  - section: Bootstrap
    tasks:
      - name: hello
        cmd: echo hello
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("ParseBytes yaml: %v", err)
	}
	if bp.Spec.Name != "my-app" {
		t.Fatalf("expected name my-app, got %q", bp.Spec.Name)
	}
}

func TestParse_ValidJSONC(t *testing.T) {
	src := []byte(`{
  // comment
  "version": "0.4",
  "blueprint": { "name": "jsonc-app" },
  "steps": [
    {
      "section": "Bootstrap",
      "tasks": [
        { "name": "greet", "cmd": "echo hi" },
      ],
    }
  ],
}`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("ParseBytes jsonc: %v", err)
	}
	if bp.Spec.Name != "jsonc-app" {
		t.Fatalf("expected jsonc-app, got %q", bp.Spec.Name)
	}
}

func TestParse_V02Compat(t *testing.T) {
	src := []byte(`
version: "0.2"
blueprint:
  name: compat-app
steps:
  - section: Work
    tasks:
      - name: step1
        cmd: echo ok
        condition: "true"
        continueOnError: true
        retryDelay: 5
        timeout: 30
        onSuccess:
          - type: cmd
            value: echo done
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("parse v0.2 compat: %v", err)
	}
	task := bp.Steps[0].Steps[0]
	if task.If != "true" {
		t.Fatalf("expected condition→if='true', got %q", task.If)
	}
	if !task.ContinueOnError {
		t.Fatalf("expected continueOnError→continue_on_error=true")
	}
	if task.RetryDelaySecs != 5 {
		t.Fatalf("expected retryDelay→retry_delay_seconds=5, got %d", task.RetryDelaySecs)
	}
	if task.TimeoutSeconds != 30 {
		t.Fatalf("expected timeout→timeout_seconds=30, got %d", task.TimeoutSeconds)
	}
	if len(task.OnSuccess) != 1 || task.OnSuccess[0].Type != "cmd" {
		t.Fatalf("expected onSuccess→on_success with type cmd, got %+v", task.OnSuccess)
	}
}

func TestParse_AllFieldTypes(t *testing.T) {
	src := []byte(`
version: "0.4"
blueprint:
  name: full-app
  slug: full-app
  title: Full App
  author: test
  license: MIT
  tags: [go, test]
inputs:
  - name: app_name
    type: string
    required: true
project:
  type: app
  name: myapp
  php_version: "8.3"
  node: true
  vars:
    key: value
packages:
  composer: ["laravel/framework"]
  composerDev: ["phpunit/phpunit"]
  npm: ["vite"]
  npmDev: ["typescript"]
git:
  init: true
  visibility: private
stubs:
  enabled: true
  search_paths: [./stubs]
tools:
  install:
    homebrew: [git]
imports: []
hooks:
  before_run:
    - name: check
      cmd: echo checking
steps:
  - section: Main
    tasks:
      - name: go
        cmd: echo go
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("parse all fields: %v", err)
	}
	if bp.Spec.License != "MIT" {
		t.Fatalf("expected license MIT, got %q", bp.Spec.License)
	}
	if bp.Packages.Composer == nil || len(bp.Packages.Composer.Require) == 0 {
		t.Fatalf("expected composer.require populated")
	}
	if bp.Packages.Composer.RequireDev == nil || len(bp.Packages.Composer.RequireDev) == 0 {
		t.Fatalf("expected composer.require_dev populated from composerDev compat")
	}
	if !bp.Git.Init {
		t.Fatalf("expected git.init=true")
	}
	if !bp.Stubs.Enabled {
		t.Fatalf("expected stubs.enabled=true")
	}
}

// ─── Validation Tests ─────────────────────────────────────────────────────────

func TestValidate_MissingName(t *testing.T) {
	bp := &Blueprint{
		Steps: []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for missing blueprint.name")
	}
}

func TestValidate_EmptySteps(t *testing.T) {
	bp := &Blueprint{
		Spec: BlueprintInfo{Name: "app"},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for empty steps")
	}
}

func TestValidate_SectionNoTasks(t *testing.T) {
	bp := &Blueprint{
		Spec:  BlueprintInfo{Name: "app"},
		Steps: []Section{{Section: "Empty"}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for section with no tasks")
	}
}

func TestValidate_TaskNeitherCmdNorCall(t *testing.T) {
	bp := &Blueprint{
		Spec:  BlueprintInfo{Name: "app"},
		Steps: []Section{{Section: "Main", Steps: []Step{{Name: "bad"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for task without cmd or call")
	}
}

func TestValidate_DuplicateInputName(t *testing.T) {
	bp := &Blueprint{
		Spec: BlueprintInfo{Name: "app"},
		Inputs: []Input{
			{Name: "x", Type: "string"},
			{Name: "x", Type: "string"},
		},
		Steps: []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for duplicate input name")
	}
}

func TestValidate_InvalidInputType(t *testing.T) {
	bp := &Blueprint{
		Spec:   BlueprintInfo{Name: "app"},
		Inputs: []Input{{Name: "x", Type: "custom"}},
		Steps:  []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for invalid input type")
	}
}

func TestValidate_InvalidRegexPattern(t *testing.T) {
	bp := &Blueprint{
		Spec:   BlueprintInfo{Name: "app"},
		Inputs: []Input{{Name: "x", Type: "string", Pattern: "[invalid"}},
		Steps:  []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for invalid regex pattern")
	}
}

func TestValidate_MinGreaterThanMax(t *testing.T) {
	minV := 10.0
	maxV := 5.0
	bp := &Blueprint{
		Spec:   BlueprintInfo{Name: "app"},
		Inputs: []Input{{Name: "x", Type: "number", Min: &minV, Max: &maxV}},
		Steps:  []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for min > max")
	}
}

func TestValidate_MinLengthGreaterThanMaxLength(t *testing.T) {
	minL := 10
	maxL := 5
	bp := &Blueprint{
		Spec:   BlueprintInfo{Name: "app"},
		Inputs: []Input{{Name: "x", Type: "string", MinLength: &minL, MaxLength: &maxL}},
		Steps:  []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for min_length > max_length")
	}
}

func TestValidate_DuplicateImportAlias(t *testing.T) {
	bp := &Blueprint{
		Spec: BlueprintInfo{Name: "app"},
		Imports: []Import{
			{Path: "a.yaml", Alias: "shared"},
			{Path: "b.yaml", Alias: "shared"},
		},
		Steps: []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for duplicate import alias")
	}
}

func TestValidate_RetryNegative(t *testing.T) {
	bp := &Blueprint{
		Spec:  BlueprintInfo{Name: "app"},
		Steps: []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo", Retry: -1}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for negative retry")
	}
}

func TestValidate_ValidBlueprintAllOptionalAbsent(t *testing.T) {
	bp := &Blueprint{
		Spec:  BlueprintInfo{Name: "minimal"},
		Steps: []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo ok"}}}},
	}
	if err := Validate(bp); err != nil {
		t.Fatalf("expected no error for valid minimal blueprint: %v", err)
	}
}

// ─── Template Rendering Tests ──────────────────────────────────────────────────

func TestRenderForExecution_InputsInCmd(t *testing.T) {
	bp := &Blueprint{
		Spec: BlueprintInfo{Name: "app"},
		Steps: []Section{{Section: "Main", Steps: []Step{
			{Name: "t", Cmd: "echo {{ index .inputs \"name\" }}"},
		}}},
	}
	ctx := map[string]any{
		"inputs": map[string]any{"name": "world"},
	}
	out, err := RenderForExecution(bp, ctx)
	if err != nil {
		t.Fatalf("RenderForExecution: %v", err)
	}
	if out.Steps[0].Steps[0].Cmd != "echo world" {
		t.Fatalf("expected 'echo world', got %q", out.Steps[0].Steps[0].Cmd)
	}
}

func TestRenderForExecution_ProjectVarsInDir(t *testing.T) {
	bp := &Blueprint{
		Spec: BlueprintInfo{Name: "app"},
		Steps: []Section{{Section: "Main", Steps: []Step{
			{Name: "t", Cmd: "echo", Dir: `{{ index (index .project "vars") "app_name" }}`},
		}}},
	}
	ctx := map[string]any{
		"project": map[string]any{
			"vars": map[string]any{"app_name": "myapp"},
		},
	}
	out, err := RenderForExecution(bp, ctx)
	if err != nil {
		t.Fatalf("RenderForExecution dir: %v", err)
	}
	if out.Steps[0].Steps[0].Dir != "myapp" {
		t.Fatalf("expected 'myapp', got %q", out.Steps[0].Steps[0].Dir)
	}
}

func TestRenderForExecution_EnvFunc(t *testing.T) {
	t.Setenv("TEST_HAD_HOME", "/test/home")
	bp := &Blueprint{
		Spec: BlueprintInfo{Name: "app"},
		Env:  map[string]string{"MY_HOME": `{{ env "TEST_HAD_HOME" }}`},
		Steps: []Section{{Section: "Main", Steps: []Step{
			{Name: "t", Cmd: "echo"},
		}}},
	}
	ctx := map[string]any{}
	out, err := RenderForExecution(bp, ctx)
	if err != nil {
		t.Fatalf("RenderForExecution env: %v", err)
	}
	if out.Env["MY_HOME"] != "/test/home" {
		t.Fatalf("expected '/test/home', got %q", out.Env["MY_HOME"])
	}
}

func TestRenderForExecution_ReadFileSizeGuard(t *testing.T) {
	// Create a temp file > 1MB
	f, err := os.CreateTemp(t.TempDir(), "big*.txt")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()
	// Write 2MB
	chunk := make([]byte, 1024)
	for i := 0; i < 2*1024; i++ {
		if _, werr := f.Write(chunk); werr != nil {
			t.Fatalf("write: %v", werr)
		}
	}
	_ = f.Close()

	bp := &Blueprint{
		Spec: BlueprintInfo{Name: "app"},
		Steps: []Section{{Section: "Main", Steps: []Step{
			{Name: "t", Cmd: `{{ readFile "` + f.Name() + `" }}`},
		}}},
	}
	ctx := map[string]any{}
	if _, err := RenderForExecution(bp, ctx); err == nil {
		t.Fatal("expected error for readFile > 1MB")
	}
}

// ─── Input Normalization Tests ─────────────────────────────────────────────────

func TestNormalizeInputs_RequiredMissing(t *testing.T) {
	bp := &Blueprint{
		Inputs: []Input{{Name: "x", Type: "string", Required: true}},
	}
	if _, err := NormalizeInputs(bp, map[string]any{}); err == nil {
		t.Fatal("expected error for missing required input")
	}
}

func TestNormalizeInputs_UnknownInput(t *testing.T) {
	bp := &Blueprint{
		Inputs: []Input{{Name: "x", Type: "string"}},
	}
	if _, err := NormalizeInputs(bp, map[string]any{"x": "ok", "unknown": "val"}); err == nil {
		t.Fatal("expected error for unknown input")
	}
}

func TestNormalizeInputs_DefaultApplied(t *testing.T) {
	bp := &Blueprint{
		Inputs: []Input{{Name: "x", Type: "string", Default: "default-val"}},
	}
	out, err := NormalizeInputs(bp, map[string]any{})
	if err != nil {
		t.Fatalf("NormalizeInputs: %v", err)
	}
	if out["x"] != "default-val" {
		t.Fatalf("expected default-val, got %v", out["x"])
	}
}

func TestNormalizeInputs_EnumViolation(t *testing.T) {
	bp := &Blueprint{
		Inputs: []Input{{Name: "x", Type: "string", Enum: []any{"a", "b"}}},
	}
	if _, err := NormalizeInputs(bp, map[string]any{"x": "c"}); err == nil {
		t.Fatal("expected error for enum violation")
	}
}

func TestNormalizeInputs_PatternMismatch(t *testing.T) {
	bp := &Blueprint{
		Inputs: []Input{{Name: "x", Type: "string", Pattern: `^\d+$`}},
	}
	if _, err := NormalizeInputs(bp, map[string]any{"x": "abc"}); err == nil {
		t.Fatal("expected error for pattern mismatch")
	}
}

func TestNormalizeInputs_MinViolation(t *testing.T) {
	minV := 5.0
	bp := &Blueprint{
		Inputs: []Input{{Name: "x", Type: "number", Min: &minV}},
	}
	if _, err := NormalizeInputs(bp, map[string]any{"x": 3.0}); err == nil {
		t.Fatal("expected error for min violation")
	}
}

func TestNormalizeInputs_MaxViolation(t *testing.T) {
	maxV := 5.0
	bp := &Blueprint{
		Inputs: []Input{{Name: "x", Type: "number", Max: &maxV}},
	}
	if _, err := NormalizeInputs(bp, map[string]any{"x": 10.0}); err == nil {
		t.Fatal("expected error for max violation")
	}
}

// ─── Compat Tests ─────────────────────────────────────────────────────────────

func TestCompat_ConditionParsedAsIf(t *testing.T) {
	src := []byte(`
version: "0.2"
blueprint:
  name: compat
steps:
  - section: Main
    tasks:
      - name: t
        cmd: echo
        condition: "true"
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if bp.Steps[0].Steps[0].If != "true" {
		t.Fatalf("expected condition→if, got %q", bp.Steps[0].Steps[0].If)
	}
}

func TestCompat_ContinueOnErrorCamelCase(t *testing.T) {
	src := []byte(`
version: "0.2"
blueprint:
  name: compat
steps:
  - section: Main
    tasks:
      - name: t
        cmd: echo
        continueOnError: true
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !bp.Steps[0].Steps[0].ContinueOnError {
		t.Fatalf("expected continueOnError→continue_on_error=true")
	}
}

func TestValidate_InvalidRetryBackoff(t *testing.T) {
	bp := &Blueprint{
		Spec:  BlueprintInfo{Name: "app"},
		Steps: []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo", RetryBackoff: "random"}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for invalid retry_backoff value")
	}
}

func TestValidate_ValidRetryBackoffValues(t *testing.T) {
	for _, val := range []string{"", "fixed", "exponential", "linear"} {
		bp := &Blueprint{
			Spec:  BlueprintInfo{Name: "app"},
			Steps: []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo", RetryBackoff: val}}}},
		}
		if err := Validate(bp); err != nil {
			t.Fatalf("expected no error for retry_backoff=%q: %v", val, err)
		}
	}
}

func TestValidate_NegativeRetryMaxDelay(t *testing.T) {
	bp := &Blueprint{
		Spec:  BlueprintInfo{Name: "app"},
		Steps: []Section{{Section: "Main", Steps: []Step{{Name: "t", Cmd: "echo", RetryMaxDelay: -1}}}},
	}
	if err := Validate(bp); err == nil {
		t.Fatal("expected error for negative retry_max_delay_seconds")
	}
}

func TestParse_RetryBackoffYAML(t *testing.T) {
	src := []byte(`
version: "0.4"
blueprint:
  name: backoff-app
steps:
  - section: Main
    tasks:
      - name: t
        cmd: echo
        retry: 3
        retry_delay_seconds: 1
        retry_backoff: exponential
        retry_max_delay_seconds: 10
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	step := bp.Steps[0].Steps[0]
	if step.RetryBackoff != "exponential" {
		t.Fatalf("expected retry_backoff=exponential, got %q", step.RetryBackoff)
	}
	if step.RetryMaxDelay != 10 {
		t.Fatalf("expected retry_max_delay_seconds=10, got %d", step.RetryMaxDelay)
	}
}

func TestParse_RetryBackoffCamelCaseCompat(t *testing.T) {
	src := []byte(`
version: "0.4"
blueprint:
  name: backoff-compat
steps:
  - section: Main
    tasks:
      - name: t
        cmd: echo
        retryBackoff: linear
        retryMaxDelay: 5
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	step := bp.Steps[0].Steps[0]
	if step.RetryBackoff != "linear" {
		t.Fatalf("expected retryBackoff→retry_backoff=linear, got %q", step.RetryBackoff)
	}
	if step.RetryMaxDelay != 5 {
		t.Fatalf("expected retryMaxDelay→retry_max_delay_seconds=5, got %d", step.RetryMaxDelay)
	}
}

func TestCompat_RetryDelayCamelCase(t *testing.T) {
	src := []byte(`
version: "0.2"
blueprint:
  name: compat
steps:
  - section: Main
    tasks:
      - name: t
        cmd: echo
        retryDelay: 3
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if bp.Steps[0].Steps[0].RetryDelaySecs != 3 {
		t.Fatalf("expected retryDelay→retry_delay_seconds=3, got %d", bp.Steps[0].Steps[0].RetryDelaySecs)
	}
}

func TestCompat_ComposerShorthand(t *testing.T) {
	src := []byte(`
version: "0.2"
blueprint:
  name: compat
packages:
  composer: ["laravel/framework"]
steps:
  - section: Main
    tasks:
      - name: t
        cmd: echo
`)
	bp, err := ParseBytes(src)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if bp.Packages.Composer == nil || len(bp.Packages.Composer.Require) == 0 {
		t.Fatalf("expected composer shorthand → composer.require")
	}
	if bp.Packages.Composer.Require[0] != "laravel/framework" {
		t.Fatalf("unexpected require: %v", bp.Packages.Composer.Require)
	}
}

// ─── Integration Test ─────────────────────────────────────────────────────────

func TestIntegration_ReferenceBlueprint(t *testing.T) {
	const refPath = "../../../reference-only/nanite-spec-v0.2/reference-blueprint.yaml"
	if _, err := os.Stat(refPath); os.IsNotExist(err) {
		t.Skip("reference blueprint not available on this machine")
	}
	bp, err := ParseFile(refPath)
	if err != nil {
		t.Fatalf("parse reference blueprint: %v", err)
	}
	if bp.Spec.Name == "" && bp.Spec.Slug == "" {
		t.Fatal("reference blueprint has no name or slug")
	}
	if len(bp.Steps) == 0 {
		t.Fatal("reference blueprint has no steps")
	}
}
