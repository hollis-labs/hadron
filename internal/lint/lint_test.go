package lint

import (
	"os"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/pipeline"
)

// helper to parse a blueprint from YAML string.
func mustParseBlueprint(t *testing.T, yamlStr string) *blueprint.Blueprint {
	t.Helper()
	bp, err := blueprint.ParseBytes([]byte(yamlStr))
	if err != nil {
		t.Fatalf("failed to parse test blueprint: %v", err)
	}
	return bp
}

func mustParsePipeline(t *testing.T, yamlStr string) *pipeline.Spec {
	t.Helper()
	spec, err := pipeline.ParseBytes([]byte(yamlStr))
	if err != nil {
		t.Fatalf("failed to parse test pipeline: %v", err)
	}
	return spec
}

func hasRule(issues []Issue, rule string) bool {
	for _, i := range issues {
		if i.Rule == rule {
			return true
		}
	}
	return false
}

func countRule(issues []Issue, rule string) int {
	n := 0
	for _, i := range issues {
		if i.Rule == rule {
			n++
		}
	}
	return n
}

func hasSeverity(issues []Issue, sev Severity) bool {
	for _, i := range issues {
		if i.Severity == sev {
			return true
		}
	}
	return false
}

// ── Blueprint tests ───────────────────────────────────────────────────────────

func TestUnusedInput(t *testing.T) {
	raw := `
version: "0.4"
blueprint:
  name: test-unused
inputs:
  - name: used_input
    type: string
    description: "used"
  - name: unused_input
    type: string
    description: "unused"
steps:
  - section: Test
    tasks:
      - name: use-it
        cmd: echo "{{ .inputs.used_input }}"
`
	bp := mustParseBlueprint(t, raw)
	issues := LintBlueprint(bp, "test.yaml", []byte(raw))

	if !hasRule(issues, "unused-input") {
		t.Error("expected unused-input warning")
	}
	// The used one should NOT be flagged.
	for _, i := range issues {
		if i.Rule == "unused-input" && strings.Contains(i.Message, `"used_input"`) && !strings.Contains(i.Message, "unused") {
			t.Error("used_input should not be flagged as unused")
		}
	}
}

func TestUnreferencedImport(t *testing.T) {
	raw := `
version: "0.4"
blueprint:
  name: test-import
imports:
  - path: ./other.yaml
    alias: other
  - path: ./used.yaml
    alias: used
steps:
  - section: Test
    tasks:
      - name: call-used
        call: used
`
	bp := mustParseBlueprint(t, raw)
	issues := LintBlueprint(bp, "test.yaml", []byte(raw))

	if !hasRule(issues, "unreferenced-import") {
		t.Error("expected unreferenced-import warning for alias 'other'")
	}
	for _, i := range issues {
		if i.Rule == "unreferenced-import" && strings.Contains(i.Message, `"used"`) && !strings.Contains(i.Message, "other") {
			t.Error("alias 'used' should not be flagged as unreferenced")
		}
	}
}

func TestNoTimeout(t *testing.T) {
	raw := `
version: "0.4"
blueprint:
  name: test-timeout
steps:
  - section: Test
    tasks:
      - name: with-timeout
        cmd: echo "fast"
        timeout_seconds: 10
      - name: without-timeout
        cmd: echo "risky"
`
	bp := mustParseBlueprint(t, raw)
	issues := LintBlueprint(bp, "test.yaml", []byte(raw))

	if !hasRule(issues, "no-timeout") {
		t.Error("expected no-timeout warning")
	}
	// Count: only the step without timeout should be flagged.
	count := countRule(issues, "no-timeout")
	if count != 1 {
		t.Errorf("expected 1 no-timeout issue, got %d", count)
	}
}

func TestMissingDescription(t *testing.T) {
	raw := `
version: "0.4"
blueprint:
  name: test-desc
inputs:
  - name: has_desc
    type: string
    description: "I have one"
  - name: no_desc
    type: string
steps:
  - section: Test
    tasks:
      - name: do-thing
        cmd: echo "{{ .inputs.has_desc }} {{ .inputs.no_desc }}"
`
	bp := mustParseBlueprint(t, raw)
	issues := LintBlueprint(bp, "test.yaml", []byte(raw))

	if !hasRule(issues, "missing-description") {
		t.Error("expected missing-description warning")
	}
	count := countRule(issues, "missing-description")
	if count != 1 {
		t.Errorf("expected 1 missing-description, got %d", count)
	}
}

func TestDuplicateStepName(t *testing.T) {
	raw := `
version: "0.4"
blueprint:
  name: test-dup
steps:
  - section: Test
    tasks:
      - name: do-thing
        cmd: echo "first"
      - name: do-thing
        cmd: echo "second"
`
	bp := mustParseBlueprint(t, raw)
	issues := LintBlueprint(bp, "test.yaml", []byte(raw))

	if !hasRule(issues, "duplicate-step-name") {
		t.Error("expected duplicate-step-name error")
	}
	if !hasSeverity(issues, SeverityError) {
		t.Error("duplicate-step-name should be an error severity")
	}
}

func TestTemplateSyntaxError(t *testing.T) {
	// We pass raw content with a bad template but a valid blueprint structure.
	// The blueprint parser won't catch this because it renders lazily.
	raw := `
version: "0.4"
blueprint:
  name: test-tpl
steps:
  - section: Test
    tasks:
      - name: good-cmd
        cmd: echo "hello"
`
	bp := mustParseBlueprint(t, raw)

	// Inject a bad template expression into the raw content for lint scanning.
	rawBad := strings.Replace(raw, `echo "hello"`, `echo "{{ .inputs.bad.. }}"`, 1)
	issues := LintBlueprint(bp, "test.yaml", []byte(rawBad))

	if !hasRule(issues, "template-syntax") {
		t.Error("expected template-syntax error")
	}
	if !hasSeverity(issues, SeverityError) {
		t.Error("template-syntax should be an error severity")
	}
}

func TestCleanBlueprint(t *testing.T) {
	raw := `
version: "0.4"
blueprint:
  name: clean-bp
inputs:
  - name: greeting
    type: string
    description: "A greeting message"
steps:
  - section: Greet
    tasks:
      - name: say-hello
        cmd: echo "{{ .inputs.greeting }}"
        timeout_seconds: 30
`
	bp := mustParseBlueprint(t, raw)
	issues := LintBlueprint(bp, "test.yaml", []byte(raw))

	// Filter out only errors and warnings (info is acceptable).
	var significant []Issue
	for _, i := range issues {
		if i.Severity == SeverityError || i.Severity == SeverityWarning {
			significant = append(significant, i)
		}
	}
	if len(significant) > 0 {
		t.Errorf("expected clean blueprint, got %d issues:", len(significant))
		for _, i := range significant {
			t.Errorf("  [%s] %s: %s", i.Severity, i.Rule, i.Message)
		}
	}
}

// ── Pipeline tests ────────────────────────────────────────────────────────────

func TestOrphanStage(t *testing.T) {
	raw := `
meta:
  name: test-orphan
stages:
  - name: build
    blueprint_path: build.yaml
  - name: test
    blueprint_path: test.yaml
    depends_on:
      - build
  - name: orphan
    blueprint_path: orphan.yaml
`
	spec := mustParsePipeline(t, raw)
	issues := LintPipeline(spec, "pipeline.yaml", []byte(raw))

	if !hasRule(issues, "orphan-stage") {
		t.Error("expected orphan-stage warning for 'orphan' stage")
	}
	// build and test should not be orphans.
	for _, i := range issues {
		if i.Rule == "orphan-stage" && !strings.Contains(i.Message, `"orphan"`) {
			t.Errorf("unexpected orphan-stage for: %s", i.Message)
		}
	}
}

func TestOrphanStageAllV1(t *testing.T) {
	// When no stage has depends_on, no orphan warnings should fire (all v1 style).
	raw := `
meta:
  name: test-v1
stages:
  - name: build
    blueprint_path: build.yaml
  - name: test
    blueprint_path: test.yaml
`
	spec := mustParsePipeline(t, raw)
	issues := LintPipeline(spec, "pipeline.yaml", []byte(raw))

	if hasRule(issues, "orphan-stage") {
		t.Error("should not flag orphan stages when all v1 style (no depends_on)")
	}
}

func TestUnreachableStage(t *testing.T) {
	raw := `
meta:
  name: test-unreachable
stages:
  - name: build
    blueprint_path: build.yaml
  - name: maybe-deploy
    blueprint_path: deploy.yaml
    depends_on:
      - build
    if: '{{ eq .stages.build.status "success" }}'
  - name: notify
    blueprint_path: notify.yaml
    depends_on:
      - maybe-deploy
`
	spec := mustParsePipeline(t, raw)
	issues := LintPipeline(spec, "pipeline.yaml", []byte(raw))

	if !hasRule(issues, "unreachable-stage") {
		t.Error("expected unreachable-stage warning for 'notify' depending on conditional 'maybe-deploy'")
	}
}

// ── Existing examples should have no errors ───────────────────────────────────

func TestExamplesNoErrors(t *testing.T) {
	examples := []string{
		"../../examples/hello-hadron.yaml",
		"../../examples/hooks-demo.yaml",
		"../../examples/dev-cleanup.yaml",
		"../../examples/parameterized.yaml",
		"../../examples/laravel-app.yaml",
	}

	for _, path := range examples {
		t.Run(path, func(t *testing.T) {
			bp, err := blueprint.ParseFile(path)
			if err != nil {
				t.Skipf("cannot parse %s: %v (skip)", path, err)
				return
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("cannot read %s: %v", path, err)
			}
			issues := LintBlueprint(bp, path, raw)
			for _, i := range issues {
				if i.Severity == SeverityError {
					t.Errorf("error in %s: [%s] %s", path, i.Rule, i.Message)
				}
			}
		})
	}
}

func TestExamplePipelinesNoErrors(t *testing.T) {
	pipelines := []string{
		"../../examples/pipeline-v2-dag/pipeline.yaml",
	}

	for _, path := range pipelines {
		t.Run(path, func(t *testing.T) {
			spec, err := pipeline.ParseFile(path)
			if err != nil {
				t.Skipf("cannot parse %s: %v (skip)", path, err)
				return
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("cannot read %s: %v", path, err)
			}
			issues := LintPipeline(spec, path, raw)
			for _, i := range issues {
				if i.Severity == SeverityError {
					t.Errorf("error in %s: [%s] %s", path, i.Rule, i.Message)
				}
			}
		})
	}
}
