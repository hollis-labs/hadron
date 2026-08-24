package verification

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// CheckNoError is the structural completion check. It passes only after the
	// executor returned a valid completed result and output validation reached
	// the verification boundary. Executor/provider failures remain authoritative
	// attempt failures and do not manufacture a verification report.
	CheckNoError          = "no_error"
	CheckOutputSchema     = "output_schema"
	CheckPredicate        = "predicate"
	CheckExpectedToolCall = "expected_tool_call"
	CheckTests            = "tests"
	CheckLint             = "lint"
	BuiltinVersion        = "v1"
)

type builtinVerifier struct {
	kind   string
	engine *values.ExpressionEngine
}

// NewDefaultRegistry returns all deterministic core verifiers. It performs no
// host/provider I/O and never reads the ambient environment.
func NewDefaultRegistry() *MemoryRegistry {
	registry := NewRegistry()
	engine := values.NewExpressionEngine()
	for _, kind := range []string{CheckNoError, CheckOutputSchema, CheckPredicate, CheckExpectedToolCall, CheckTests, CheckLint} {
		if err := registry.Register(&builtinVerifier{kind: kind, engine: engine}); err != nil {
			panic(err)
		}
	}
	return registry
}

func (v *builtinVerifier) Spec() VerifierSpec {
	spec := VerifierSpec{Kind: v.kind, Version: BuiltinVersion, Mode: ModeDeterministic, ConfigSchema: graph.Schema{"type": "object"}}
	switch v.kind {
	case CheckNoError:
		spec.ConfigSchema = objectSchema(nil, nil)
	case CheckOutputSchema:
		spec.ConfigSchema = objectSchema(map[string]any{"schema": map[string]any{"type": "object"}}, nil)
	case CheckPredicate:
		spec.ConfigSchema = objectSchema(map[string]any{"expression": map[string]any{"type": "string", "minLength": json.Number("1")}}, []any{"expression"})
	case CheckExpectedToolCall:
		spec.ConfigSchema = objectSchema(map[string]any{
			"server":  map[string]any{"type": "string", "minLength": json.Number("1")},
			"tool":    map[string]any{"type": "string", "minLength": json.Number("1")},
			"count":   map[string]any{"type": "integer", "minimum": json.Number("1")},
			"outcome": map[string]any{"type": "string", "enum": []any{string(ActivitySucceeded), string(ActivityFailed), string(ActivitySkipped)}},
		}, []any{"tool"})
		spec.RequiredEvidence = []ActivityKind{ActivityToolCall}
	case CheckTests, CheckLint:
		spec.ConfigSchema = objectSchema(map[string]any{
			"required":      map[string]any{"type": "array", "minItems": json.Number("1"), "items": map[string]any{"type": "string", "minLength": json.Number("1")}, "uniqueItems": true},
			"allow_skipped": map[string]any{"type": "boolean"},
		}, []any{"required"})
		if v.kind == CheckTests {
			spec.RequiredEvidence = []ActivityKind{ActivityTest}
		} else {
			spec.RequiredEvidence = []ActivityKind{ActivityLint}
		}
	}
	return spec
}

func objectSchema(properties map[string]any, required []any) graph.Schema {
	if properties == nil {
		properties = map[string]any{}
	}
	result := graph.Schema{"type": "object", "additionalProperties": false, "properties": properties}
	if len(required) != 0 {
		result["required"] = required
	}
	return result
}

func (v *builtinVerifier) ValidateConfig(_ context.Context, check graph.VerificationCheck) []diagnostic.Diagnostic {
	err := v.validateConfig(check)
	if err == nil {
		return nil
	}
	return []diagnostic.Diagnostic{{
		Severity: diagnostic.SeverityError, Code: CodeInvalidCheck, Message: err.Error(), Source: cloneSource(check.Source),
		Remediation: &diagnostic.Remediation{Message: "Update the verification check to satisfy its registered verifier contract."},
	}}
}

func (v *builtinVerifier) validateConfig(check graph.VerificationCheck) error {
	if check.Config == nil {
		check.Config = graph.Config{}
	}
	switch v.kind {
	case CheckNoError:
		return requireKeys(check.Config)
	case CheckOutputSchema:
		if err := requireKeys(check.Config, "schema"); err != nil {
			return err
		}
		if raw, exists := check.Config["schema"]; exists {
			schema, ok := configuredSchema(raw)
			if !ok {
				return errorsf("output_schema schema must be an object")
			}
			if err := values.ValidateSchema(schema); err != nil {
				return fmt.Errorf("output_schema schema: %w", err)
			}
		}
		return nil
	case CheckPredicate:
		if err := requireKeys(check.Config, "expression"); err != nil {
			return err
		}
		expression, ok := check.Config["expression"].(string)
		if !ok || strings.TrimSpace(expression) == "" || expression != strings.TrimSpace(expression) {
			return errorsf("predicate expression is required without surrounding whitespace")
		}
		_, err := values.ParseReferences(graph.Expression{Text: expression, Source: check.Source})
		return err
	case CheckExpectedToolCall:
		if err := requireKeys(check.Config, "server", "tool", "count", "outcome"); err != nil {
			return err
		}
		tool, ok := check.Config["tool"].(string)
		if !ok || validateText("expected tool", tool, true) != nil {
			return errorsf("expected_tool_call tool is required")
		}
		if server, exists := check.Config["server"]; exists {
			text, ok := server.(string)
			if !ok || validateText("expected server", text, true) != nil {
				return errorsf("expected_tool_call server must be a non-empty string")
			}
		}
		if count, exists := check.Config["count"]; exists {
			parsed, ok := exactPositiveInt(count)
			if !ok || parsed < 1 {
				return errorsf("expected_tool_call count must be a positive integer")
			}
		}
		if outcome, exists := check.Config["outcome"]; exists {
			text, ok := outcome.(string)
			if !ok || !ActivityOutcome(text).Valid() {
				return errorsf("expected_tool_call outcome is unsupported")
			}
		}
		return nil
	case CheckTests, CheckLint:
		if err := requireKeys(check.Config, "required", "allow_skipped"); err != nil {
			return err
		}
		required, ok := stringList(check.Config["required"])
		if !ok || len(required) == 0 {
			return errorsf("%s required must be a non-empty unique string array", v.kind)
		}
		seen := map[string]struct{}{}
		for _, name := range required {
			if validateText(v.kind+" name", name, true) != nil {
				return errorsf("%s required contains an invalid name", v.kind)
			}
			if _, dup := seen[name]; dup {
				return errorsf("%s required contains duplicate %q", v.kind, name)
			}
			seen[name] = struct{}{}
		}
		if allow, exists := check.Config["allow_skipped"]; exists {
			if _, ok := allow.(bool); !ok {
				return errorsf("%s allow_skipped must be boolean", v.kind)
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrUnknownVerifier, v.kind)
	}
}

func (v *builtinVerifier) Verify(ctx context.Context, request Request) (CheckResult, error) {
	if ctx == nil {
		return CheckResult{}, errorsf("verification context is required")
	}
	if err := ctx.Err(); err != nil {
		return CheckResult{}, err
	}
	if err := v.validateConfig(request.Check); err != nil {
		return CheckResult{}, err
	}
	result := CheckResult{Kind: v.kind, Version: BuiltinVersion, Outcome: CheckPassed, Code: "verification_passed", Message: "verification check passed", Source: cloneSource(request.Check.Source)}
	switch v.kind {
	case CheckNoError:
		return result, nil
	case CheckOutputSchema:
		schema := request.OutputSchema
		if raw, exists := request.Check.Config["schema"]; exists {
			schema, _ = configuredSchema(raw)
		}
		if err := values.ValidateValueSetSchema(schema, request.Outputs); err != nil {
			// Schema validator errors may render the rejected value. Keep that
			// process-local and persist only the stable verifier decision.
			//nolint:nilerr // A schema mismatch is a verifier rejection, not provider failure.
			return failed(result, "verification_output_schema_failed", "outputs do not satisfy the verification schema", nil), nil
		}
	case CheckPredicate:
		expression := graph.Expression{Text: request.Check.Config["expression"].(string), Source: cloneSource(request.Check.Source)}
		passed, err := v.engine.EvaluateBool(expression, values.ExpressionContext{Inputs: request.Outputs}, values.ExpressionOptions{AllowEnv: false})
		if err != nil {
			return CheckResult{}, err
		}
		if !passed {
			return failed(result, "verification_predicate_failed", "output predicate evaluated to false", nil), nil
		}
	case CheckExpectedToolCall:
		server, _ := request.Check.Config["server"].(string)
		tool := request.Check.Config["tool"].(string)
		count := 1
		if raw, ok := request.Check.Config["count"]; ok {
			count, _ = exactPositiveInt(raw)
		}
		outcome := ActivitySucceeded
		if raw, ok := request.Check.Config["outcome"].(string); ok {
			outcome = ActivityOutcome(raw)
		}
		actual := 0
		for _, activity := range request.Evidence {
			if activity.Kind == ActivityToolCall && activity.ToolCall != nil && activity.ToolCall.Tool == tool && (server == "" || activity.ToolCall.Server == server) && activity.ToolCall.Outcome == outcome {
				actual++
			}
		}
		if actual != count {
			return failed(result, "verification_tool_call_missing", "literal tool-call evidence did not match the expectation", map[string]string{"actual": fmt.Sprint(actual), "expected": fmt.Sprint(count), "tool": tool}), nil
		}
	case CheckTests:
		return verifyNamedEvidence(result, request, ActivityTest)
	case CheckLint:
		return verifyNamedEvidence(result, request, ActivityLint)
	}
	return result, nil
}

func configuredSchema(value any) (graph.Schema, bool) {
	switch typed := value.(type) {
	case graph.Schema:
		return typed, true
	case map[string]any:
		return graph.Schema(typed), true
	default:
		return nil, false
	}
}

func verifyNamedEvidence(result CheckResult, request Request, kind ActivityKind) (CheckResult, error) {
	required, _ := stringList(request.Check.Config["required"])
	sort.Strings(required)
	allowSkipped, _ := request.Check.Config["allow_skipped"].(bool)
	observed := make(map[string]ActivityOutcome)
	counts := make(map[string]int)
	for _, activity := range request.Evidence {
		if activity.Kind == kind {
			if kind == ActivityTest && activity.Test != nil {
				observed[activity.Test.Name] = activity.Test.Outcome
				counts[activity.Test.Name]++
			}
			if kind == ActivityLint && activity.Lint != nil {
				observed[activity.Lint.Name] = activity.Lint.Outcome
				counts[activity.Lint.Name]++
			}
		}
	}
	for _, name := range required {
		if counts[name] > 1 {
			return failed(result, "verification_evidence_ambiguous", fmt.Sprintf("required %s evidence %q was recorded more than once", kind, name), map[string]string{"name": name, "count": fmt.Sprint(counts[name])}), nil
		}
		outcome, exists := observed[name]
		if !exists {
			return failed(result, "verification_evidence_missing", fmt.Sprintf("required %s evidence %q is missing", kind, name), map[string]string{"name": name}), nil
		}
		if outcome != ActivitySucceeded && (!allowSkipped || outcome != ActivitySkipped) {
			return failed(result, "verification_evidence_failed", fmt.Sprintf("required %s evidence %q did not succeed", kind, name), map[string]string{"name": name, "outcome": string(outcome)}), nil
		}
	}
	return result, nil
}

func failed(result CheckResult, code, message string, details map[string]string) CheckResult {
	result.Outcome = CheckFailed
	result.Code = code
	result.Message = message
	result.Details = details
	return result
}
func requireKeys(config graph.Config, allowed ...string) error {
	set := map[string]struct{}{}
	for _, key := range allowed {
		set[key] = struct{}{}
	}
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := set[key]; !ok {
			return errorsf("verification config contains unsupported field %q", key)
		}
	}
	return nil
}
func exactPositiveInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, typed > 0
	case int64:
		return int(typed), typed > 0 && int64(int(typed)) == typed
	case json.Number:
		parsed, err := typed.Int64()
		return int(parsed), err == nil && parsed > 0 && int64(int(parsed)) == parsed
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || typed > float64(math.MaxInt) {
			return 0, false
		}
		parsed := int(typed)
		return parsed, typed == float64(parsed) && parsed > 0
	default:
		return 0, false
	}
}
func stringList(value any) ([]string, bool) {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...), true
	case []any:
		result := make([]string, len(typed))
		for i, v := range typed {
			text, ok := v.(string)
			if !ok {
				return nil, false
			}
			result[i] = text
		}
		return result, true
	default:
		return nil, false
	}
}
func errorsf(format string, args ...any) error { return fmt.Errorf(format, args...) }
func cloneSource(source *graph.SourceRef) *graph.SourceRef {
	if source == nil {
		return nil
	}
	cloned := *source
	cloned.Path = append([]string(nil), source.Path...)
	return &cloned
}
