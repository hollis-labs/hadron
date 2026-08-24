package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

// Options injects every boundary that can authorize, contact, or observe an
// LLM provider. Redactor masks known resolved secrets from final/stream output;
// secret references themselves are never resolved by this adapter.
type Options struct {
	Policy   Policy
	Provider Provider
	Tools    ToolHost
	Stream   StreamSink
	Redactor *values.Redactor
}

// Kind is the immutable provider-neutral llm@v1 step-kind implementation.
type Kind struct {
	policy   Policy
	provider Provider
	tools    ToolHost
	stream   StreamSink
	redactor *values.Redactor
}

// New constructs llm@v1. A ToolHost is optional until configuration requests
// tools; Policy and Provider are always required.
func New(options Options) (*Kind, error) {
	if nilInterface(options.Policy) || nilInterface(options.Provider) {
		return nil, fmt.Errorf("%w: policy and provider are required", ErrInvalidOptions)
	}
	return &Kind{policy: options.Policy, provider: options.Provider, tools: options.Tools, stream: options.Stream, redactor: options.Redactor}, nil
}

// Register constructs and registers llm@v1.
func Register(registry stepkind.Registry, options Options) (*Kind, error) {
	if nilInterface(registry) {
		return nil, fmt.Errorf("%w: registry is required", ErrInvalidOptions)
	}
	kind, err := New(options)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(kind); err != nil {
		return nil, err
	}
	return kind, nil
}

// Spec returns the closed static contract. Configuration-specific output
// schema validation is additionally enforced inside Execute.
func (*Kind) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: KindName, Version: KindVersion,
		ConfigSchema: configSchema(), InputSchema: graph.Schema{"type": "object"}, OutputSchema: outputSchema(),
		Effects:              graph.EffectSet{graph.EffectCompute, graph.EffectRead, graph.EffectMaterialize, graph.EffectMutate, graph.EffectDestructive},
		RequiredCapabilities: []string{"llm.provider"},
		Idempotency:          graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation:          stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:           stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		EmbeddedModeSupported: true,
	}
}

func configSchema() graph.Schema {
	message := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"role"}, "properties": map[string]any{
		"role": map[string]any{"type": "string", "enum": []any{"user", "assistant"}}, "content": map[string]any{"type": "string", "minLength": json.Number("1")}, "input": map[string]any{"type": "string", "minLength": json.Number("1")},
	}, "oneOf": []any{map[string]any{"required": []any{"content"}}, map[string]any{"required": []any{"input"}}}}
	budget := map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{
		"max_input_bytes":     map[string]any{"type": "integer", "minimum": json.Number("1"), "maximum": json.Number(strconv.FormatInt(maximumConfiguredBytes, 10))},
		"max_output_bytes":    map[string]any{"type": "integer", "minimum": json.Number("1"), "maximum": json.Number(strconv.FormatInt(maximumConfiguredBytes, 10))},
		"max_total_tokens":    map[string]any{"type": "integer", "minimum": json.Number("1"), "maximum": json.Number(strconv.FormatInt(maximumConfiguredTokens, 10))},
		"max_cost_microunits": map[string]any{"type": "integer", "minimum": json.Number("0")},
		"max_tool_calls":      map[string]any{"type": "integer", "minimum": json.Number("0"), "maximum": json.Number(strconv.Itoa(maximumToolCalls))},
	}}
	return graph.Schema{"type": "object", "additionalProperties": false, "required": []any{"profile", "messages", "output_schema"}, "properties": map[string]any{
		"profile": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("256")}, "provider": map[string]any{"type": "string", "maxLength": json.Number("256")}, "model": map[string]any{"type": "string", "maxLength": json.Number("256")},
		"system": map[string]any{"type": "string", "maxLength": json.Number(strconv.FormatInt(defaultMaxInputBytes, 10))}, "messages": map[string]any{"type": "array", "minItems": json.Number("1"), "maxItems": json.Number(strconv.Itoa(maximumMessages)), "items": message},
		"context_inputs": map[string]any{"type": "array", "maxItems": json.Number(strconv.Itoa(maximumContextInputs)), "uniqueItems": true, "items": map[string]any{"type": "string"}}, "tools": map[string]any{"type": "array", "maxItems": json.Number(strconv.Itoa(maximumDeclaredTools)), "uniqueItems": true, "items": map[string]any{"type": "string", "pattern": `^[A-Za-z0-9_.-]{1,128}$`}},
		"max_tool_iterations": map[string]any{"type": "integer", "minimum": json.Number("0"), "maximum": json.Number(strconv.Itoa(maximumToolCalls))}, "output_schema": map[string]any{"type": "object"}, "repair": map[string]any{"type": "string", "enum": []any{"fail", "once"}}, "budget": budget, "timeout": map[string]any{"type": "string", "minLength": json.Number("1")}, "stream": map[string]any{"type": "boolean"},
	}}
}

func outputSchema() graph.Schema {
	usage := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"input_tokens", "output_tokens", "total_tokens", "cost_microunits", "requests", "tool_calls"}, "properties": map[string]any{
		"input_tokens": map[string]any{"type": "integer", "minimum": json.Number("0")}, "output_tokens": map[string]any{"type": "integer", "minimum": json.Number("0")}, "total_tokens": map[string]any{"type": "integer", "minimum": json.Number("0")}, "cost_microunits": map[string]any{"type": "integer", "minimum": json.Number("0")}, "requests": map[string]any{"type": "integer", "minimum": json.Number("1")}, "tool_calls": map[string]any{"type": "integer", "minimum": json.Number("0")},
	}}
	attributes := map[string]any{"type": "object", "maxProperties": json.Number(strconv.Itoa(MaximumProvenanceAttributeCount)), "propertyNames": map[string]any{"maxLength": json.Number("128")}, "additionalProperties": map[string]any{"type": "string", "maxLength": json.Number("512")}}
	providerCall := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"request_id", "revision", "attributes"}, "properties": map[string]any{"request_id": map[string]any{"type": "string"}, "revision": map[string]any{"type": "string"}, "attributes": attributes}}
	audit := map[string]any{"type": "object", "additionalProperties": false, "required": []any{"profile", "provider", "model", "binding_id", "binding_revision", "binding_attributes", "provider_calls"}, "properties": map[string]any{
		"profile": map[string]any{"type": "string"}, "provider": map[string]any{"type": "string"}, "model": map[string]any{"type": "string"}, "binding_id": map[string]any{"type": "string"}, "binding_revision": map[string]any{"type": "string"}, "binding_attributes": attributes, "provider_calls": map[string]any{"type": "array", "minItems": json.Number("1"), "items": providerCall},
	}}
	return graph.Schema{"type": "object", "additionalProperties": false, "required": []any{OutputValue, OutputRawText, OutputToolCalls, OutputUsage, OutputStop, OutputAudit}, "properties": map[string]any{
		OutputValue: map[string]any{}, OutputRawText: map[string]any{"type": "string"},
		OutputToolCalls: map[string]any{"type": "array", "items": map[string]any{"type": "object", "additionalProperties": false, "required": []any{"sequence", "tool", "outcome"}, "properties": map[string]any{"sequence": map[string]any{"type": "integer"}, "tool": map[string]any{"type": "string"}, "outcome": map[string]any{"type": "string", "enum": []any{"succeeded", "failed"}}}}},
		OutputUsage:     usage, OutputStop: map[string]any{"type": "string", "enum": []any{"completed", "length", "filtered"}}, OutputAudit: audit,
	}}
}

// ValidateConfig reports deterministic declaration findings without invoking
// policy, tool, or provider I/O.
func (*Kind) ValidateConfig(ctx context.Context, input graph.Config) []diagnostic.Diagnostic {
	_, err := parseConfig(input)
	if err == nil && ctx != nil {
		err = ctx.Err()
	}
	if err == nil {
		return nil
	}
	return []diagnostic.Diagnostic{{Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig, Message: "invalid LLM step configuration: " + err.Error()}}
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	current := reflect.ValueOf(value)
	switch current.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return current.IsNil()
	default:
		return false
	}
}
