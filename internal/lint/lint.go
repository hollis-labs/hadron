package lint

import (
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/pipeline"
)

// Severity classifies the importance of a lint issue.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

// Issue represents a single lint finding.
type Issue struct {
	File     string   `json:"file"`
	Line     int      `json:"line,omitempty"` // 0 if unknown
	Severity Severity `json:"severity"`
	Rule     string   `json:"rule"`
	Message  string   `json:"message"`
}

// templateExprRe matches Go template expressions {{ ... }}.
var templateExprRe = regexp.MustCompile(`\{\{.*?\}\}`)

// inputRefRe matches {{ .inputs.<name> }} references (possibly with function calls around them).
var inputRefRe = regexp.MustCompile(`\.inputs\.([a-zA-Z][a-zA-Z0-9_\-]*)`)

// templateFuncMap mirrors the blueprint template engine's functions so we can
// parse templates without executing them.
var lintFuncMap = template.FuncMap{
	"upper":    func(s string) string { return s },
	"lower":    func(s string) string { return s },
	"trim":     func(s string) string { return s },
	"replace":  func(old, replacement, s string) string { return s },
	"split":    func(sep, s string) []string { return nil },
	"join":     func(sep string, a []string) string { return "" },
	"basename": func(s string) string { return s },
	"dirname":  func(s string) string { return s },
	"ext":      func(s string) string { return s },
	"env":      func(key string, def ...string) string { return "" },
	"readFile": func(path string) (string, error) { return "", nil },
	"default":  func(def, val any) any { return val },
	"ternary":  func(cond bool, t, f any) any { return t },
	"json":     func(v any) string { return "" },
	"eq":       func(a, b any) bool { return false },
	"ne":       func(a, b any) bool { return false },
	"lt":       func(a, b any) bool { return false },
	"le":       func(a, b any) bool { return false },
	"gt":       func(a, b any) bool { return false },
	"ge":       func(a, b any) bool { return false },
	"and":      func(a, b any) any { return a },
	"or":       func(a, b any) any { return a },
	"not":      func(a any) bool { return false },
}

// LintBlueprint runs all blueprint lint rules and returns any issues found.
func LintBlueprint(bp *blueprint.Blueprint, path string, rawContent []byte) []Issue {
	if bp == nil {
		return nil
	}
	var issues []Issue

	issues = append(issues, checkTemplateSyntax(path, rawContent)...)
	issues = append(issues, checkDuplicateStepNames(bp, path)...)
	issues = append(issues, checkUnusedInputs(bp, path, rawContent)...)
	issues = append(issues, checkUnreferencedImports(bp, path)...)
	issues = append(issues, checkNoTimeout(bp, path)...)
	issues = append(issues, checkMissingDescription(bp, path)...)

	return issues
}

// LintPipeline runs all pipeline-specific lint rules and returns any issues found.
func LintPipeline(spec *pipeline.Spec, path string, rawContent []byte) []Issue {
	if spec == nil {
		return nil
	}
	var issues []Issue

	issues = append(issues, checkOrphanStage(spec, path)...)
	issues = append(issues, checkUnreachableStage(spec, path)...)

	return issues
}

// ── Blueprint rules ───────────────────────────────────────────────────────────

// checkTemplateSyntax finds template expressions that fail to parse.
func checkTemplateSyntax(path string, rawContent []byte) []Issue {
	var issues []Issue
	raw := string(rawContent)

	// Build a set of line numbers that are YAML comments so we can skip them.
	lines := strings.Split(raw, "\n")
	commentLines := map[int]bool{}
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			commentLines[i+1] = true // 1-based
		}
	}

	// Find all template expressions in the raw content and try to parse them.
	matches := templateExprRe.FindAllStringIndex(raw, -1)
	for _, loc := range matches {
		line := lineNumber(raw, loc[0])
		if commentLines[line] {
			continue // skip expressions in YAML comments
		}
		expr := raw[loc[0]:loc[1]]
		_, err := template.New("lint").Funcs(lintFuncMap).Parse(expr)
		if err != nil {
			issues = append(issues, Issue{
				File:     path,
				Line:     line,
				Severity: SeverityError,
				Rule:     "template-syntax",
				Message:  fmt.Sprintf("invalid template expression: %s — %v", expr, err),
			})
		}
	}

	return issues
}

// checkDuplicateStepNames detects two steps with the same name within a section.
func checkDuplicateStepNames(bp *blueprint.Blueprint, path string) []Issue {
	var issues []Issue

	for _, sec := range bp.Steps {
		seen := map[string]bool{}
		for _, step := range sec.Steps {
			name := strings.TrimSpace(step.Name)
			if name == "" {
				continue
			}
			if seen[name] {
				issues = append(issues, Issue{
					File:     path,
					Severity: SeverityError,
					Rule:     "duplicate-step-name",
					Message:  fmt.Sprintf("duplicate step name %q in section %q", name, sec.Section),
				})
			}
			seen[name] = true
		}
	}

	return issues
}

// checkUnusedInputs finds inputs declared but never referenced in template expressions.
func checkUnusedInputs(bp *blueprint.Blueprint, path string, rawContent []byte) []Issue {
	var issues []Issue
	raw := string(rawContent)

	// Collect all input names referenced via .inputs.<name> anywhere in the file.
	refMatches := inputRefRe.FindAllStringSubmatch(raw, -1)
	referenced := map[string]bool{}
	for _, m := range refMatches {
		referenced[m[1]] = true
	}

	for _, inp := range bp.Inputs {
		if !referenced[inp.Name] {
			issues = append(issues, Issue{
				File:     path,
				Severity: SeverityWarning,
				Rule:     "unused-input",
				Message:  fmt.Sprintf("input %q is declared but never referenced in any template expression", inp.Name),
			})
		}
	}

	return issues
}

// checkUnreferencedImports finds imports whose alias is never used in a call: field.
func checkUnreferencedImports(bp *blueprint.Blueprint, path string) []Issue {
	var issues []Issue

	if len(bp.Imports) == 0 {
		return nil
	}

	// Collect all call: values from steps.
	callTargets := map[string]bool{}
	for _, sec := range bp.Steps {
		for _, step := range sec.Steps {
			call := strings.TrimSpace(step.Call)
			if call != "" {
				callTargets[call] = true
			}
		}
	}

	for _, imp := range bp.Imports {
		alias := strings.TrimSpace(imp.Alias)
		if alias == "" {
			// No alias means it's inlined via LoadWithImports, not callable.
			continue
		}
		if !callTargets[alias] {
			issues = append(issues, Issue{
				File:     path,
				Severity: SeverityWarning,
				Rule:     "unreferenced-import",
				Message:  fmt.Sprintf("import alias %q is declared but no step uses call: %s", alias, alias),
			})
		}
	}

	return issues
}

// checkNoTimeout flags steps with a cmd but no timeout_seconds.
func checkNoTimeout(bp *blueprint.Blueprint, path string) []Issue {
	var issues []Issue

	for _, sec := range bp.Steps {
		for _, step := range sec.Steps {
			cmd := strings.TrimSpace(step.Cmd)
			if cmd == "" {
				cmd = strings.TrimSpace(step.Run)
			}
			if cmd == "" {
				continue // call-only step, no cmd
			}
			if step.TimeoutSeconds == 0 {
				issues = append(issues, Issue{
					File:     path,
					Severity: SeverityWarning,
					Rule:     "no-timeout",
					Message:  fmt.Sprintf("step %q has a cmd but no timeout_seconds (risky for long-running commands)", step.Name),
				})
			}
		}
	}

	return issues
}

// checkMissingDescription flags inputs with no description field.
func checkMissingDescription(bp *blueprint.Blueprint, path string) []Issue {
	var issues []Issue

	for _, inp := range bp.Inputs {
		if strings.TrimSpace(inp.Description) == "" {
			issues = append(issues, Issue{
				File:     path,
				Severity: SeverityWarning,
				Rule:     "missing-description",
				Message:  fmt.Sprintf("input %q has no description (hurts discoverability for agents/users)", inp.Name),
			})
		}
	}

	return issues
}

// ── Pipeline rules ────────────────────────────────────────────────────────────

// checkOrphanStage detects stages that have no depends_on and are not depended
// on by any other stage, but only when other stages DO have depends_on (mixed style).
func checkOrphanStage(spec *pipeline.Spec, path string) []Issue {
	var issues []Issue

	// Check if any stage uses depends_on.
	hasDeps := false
	for _, st := range spec.Stages {
		if len(st.DependsOn) > 0 {
			hasDeps = true
			break
		}
	}
	if !hasDeps {
		return nil // all v1 style, no orphan detection needed
	}

	// Build sets: stages that depend on something, and stages depended upon.
	dependsOnSomething := map[string]bool{}
	dependedUpon := map[string]bool{}
	for _, st := range spec.Stages {
		if len(st.DependsOn) > 0 {
			dependsOnSomething[st.Name] = true
			for _, dep := range st.DependsOn {
				dependedUpon[dep] = true
			}
		}
	}

	for _, st := range spec.Stages {
		if !dependsOnSomething[st.Name] && !dependedUpon[st.Name] {
			issues = append(issues, Issue{
				File:     path,
				Severity: SeverityWarning,
				Rule:     "orphan-stage",
				Message:  fmt.Sprintf("stage %q has no depends_on and is not depended on by any other stage (orphan in DAG)", st.Name),
			})
		}
	}

	return issues
}

// checkUnreachableStage detects stages that depend on a stage with an if:
// condition, making them potentially unreachable if that condition evaluates false.
func checkUnreachableStage(spec *pipeline.Spec, path string) []Issue {
	var issues []Issue

	// Build a map of stage name → whether it has an if: condition.
	hasCondition := map[string]bool{}
	for _, st := range spec.Stages {
		if strings.TrimSpace(st.If) != "" {
			hasCondition[st.Name] = true
		}
	}

	for _, st := range spec.Stages {
		for _, dep := range st.DependsOn {
			if hasCondition[dep] {
				issues = append(issues, Issue{
					File:     path,
					Severity: SeverityWarning,
					Rule:     "unreachable-stage",
					Message:  fmt.Sprintf("stage %q depends on %q which has an if: condition — could be skipped, making %q potentially unreachable", st.Name, dep, st.Name),
				})
			}
		}
	}

	return issues
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// lineNumber returns the 1-based line number for the byte offset in s.
func lineNumber(s string, offset int) int {
	return strings.Count(s[:offset], "\n") + 1
}
