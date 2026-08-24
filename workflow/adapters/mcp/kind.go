package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

const (
	defaultTimeout      = 30 * time.Second
	defaultInlineLimit  = values.DefaultInlineLimit
	defaultMaxArtifacts = 64
	maximumTimeout      = 24 * time.Hour
)

var conservativeEffects = graph.EffectSet{
	graph.EffectRead,
	graph.EffectMaterialize,
	graph.EffectMutate,
	graph.EffectDestructive,
}

// ExpectedResult selects the required MCP result content category.
type ExpectedResult string

const (
	ExpectedAny        ExpectedResult = "any"
	ExpectedStructured ExpectedResult = "structured"
	ExpectedText       ExpectedResult = "text"
	ExpectedResource   ExpectedResult = "resource"
	ExpectedArtifact   ExpectedResult = "artifact"
)

func (e ExpectedResult) valid() bool {
	switch e {
	case ExpectedAny, ExpectedStructured, ExpectedText, ExpectedResource, ExpectedArtifact:
		return true
	default:
		return false
	}
}

type config struct {
	Server         string
	Tool           string
	Arguments      map[string]any
	Timeout        time.Duration
	IdempotencyKey string
	Expected       ExpectedResult
}

// Kind is the registered MCP step-kind implementation.
type Kind struct {
	client       Client
	descriptor   Descriptor
	secrets      values.SecretResolver
	artifacts    ArtifactSink
	inlineLimit  int64
	maxArtifacts int
}

// New constructs a configured MCP step kind.
func New(options Options) (*Kind, error) {
	if nilInterface(options.Client) || nilInterface(options.Descriptor) {
		return nil, fmt.Errorf("%w: client and descriptor are required", ErrInvalidOptions)
	}
	inlineLimit := options.InlineLimit
	if inlineLimit == 0 {
		inlineLimit = defaultInlineLimit
	}
	if inlineLimit < 1 || inlineLimit > values.MaximumInlineLimit {
		return nil, fmt.Errorf("%w: inline limit must be between 1 and %d", ErrInvalidOptions, values.MaximumInlineLimit)
	}
	maxArtifacts := options.MaxArtifacts
	if maxArtifacts == 0 {
		maxArtifacts = defaultMaxArtifacts
	}
	if maxArtifacts < 1 || maxArtifacts > 1024 {
		return nil, fmt.Errorf("%w: max artifacts must be between 1 and 1024", ErrInvalidOptions)
	}
	return &Kind{
		client: options.Client, descriptor: options.Descriptor, secrets: options.Secrets,
		artifacts: options.Artifacts, inlineLimit: inlineLimit, maxArtifacts: maxArtifacts,
	}, nil
}

// Register constructs and registers mcp@v1.
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

// Spec returns the immutable conservative contract for dynamic MCP tools.
func (*Kind) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: KindName, Version: KindVersion,
		ConfigSchema: configSchema(), InputSchema: graph.Schema{}, OutputSchema: outputSchema(),
		Effects:              append(graph.EffectSet(nil), conservativeEffects...),
		RequiredCapabilities: []string{"mcp.client"},
		Idempotency:          graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation:          stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:           stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		EmbeddedModeSupported: true,
	}
}

func outputSchema() graph.Schema {
	return graph.Schema{
		"type":                 "object",
		"required":             []any{OutputMetadata},
		"additionalProperties": true,
		"properties": map[string]any{
			OutputStructured: map[string]any{},
			OutputText:       map[string]any{},
			OutputResources:  map[string]any{"type": "array"},
			OutputMetadata:   map[string]any{"type": "object"},
		},
	}
}

func configSchema() graph.Schema {
	return graph.Schema{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []any{"server", "tool", "arguments"},
		"properties": map[string]any{
			"server":          map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("512")},
			"tool":            map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("128"), "pattern": `^[A-Za-z0-9_-]+$`},
			"arguments":       map[string]any{"type": "object"},
			"timeout":         map[string]any{"type": "string", "minLength": json.Number("1")},
			"idempotency_key": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("512")},
			"expected_result": map[string]any{"type": "string", "enum": []any{"any", "structured", "text", "resource", "artifact"}},
		},
	}
}

// ValidateConfig reports deterministic adapter-specific validation findings.
func (k *Kind) ValidateConfig(ctx context.Context, input graph.Config) []diagnostic.Diagnostic {
	_, err := parseConfig(input)
	if err == nil && ctx != nil {
		err = ctx.Err()
	}
	if err == nil {
		return nil
	}
	return []diagnostic.Diagnostic{{
		Severity: diagnostic.SeverityError,
		Code:     stepkind.CodeInvalidConfig,
		Message:  "invalid MCP step configuration: " + err.Error(),
	}}
}

// DescribeConfig resolves annotations and returns the deterministic policy
// projection for config before execution. Untrusted or absent annotations do
// not narrow the static fail-safe behavior.
func (k *Kind) DescribeConfig(ctx context.Context, input graph.Config) (ConfigDescription, error) {
	if ctx == nil {
		return ConfigDescription{}, fmt.Errorf("%w: context is required", ErrInvalidConfig)
	}
	parsed, err := parseConfig(input)
	if err != nil {
		return ConfigDescription{}, err
	}
	descriptor, err := k.descriptor.DescribeTool(ctx, parsed.Server, parsed.Tool)
	if err != nil {
		return ConfigDescription{}, fmt.Errorf("describe MCP tool %q on server %q: %w", parsed.Tool, parsed.Server, err)
	}
	if descriptor.Server != parsed.Server || descriptor.Tool != parsed.Tool {
		return ConfigDescription{}, fmt.Errorf("descriptor identity does not match requested MCP tool")
	}
	return describe(parsed, descriptor), nil
}

func describe(parsed config, descriptor ToolDescriptor) ConfigDescription {
	description := ConfigDescription{
		Server: parsed.Server, Tool: parsed.Tool, Annotations: cloneAnnotations(descriptor.Annotations),
		AnnotationsTrusted: descriptor.Trusted,
		Effects:            append(graph.EffectSet(nil), conservativeEffects...),
		Idempotency:        graph.IdempotencyNone,
		RetrySafety:        stepkind.RetryUnsupported,
	}
	if parsed.IdempotencyKey != "" {
		description.Idempotency = graph.IdempotencyKeyed
		description.RetrySafety = stepkind.RetryRequiresIdempotency
	}
	if !descriptor.Trusted {
		return description
	}
	annotations := descriptor.Annotations
	switch {
	case annotations.ReadOnlyHint != nil && *annotations.ReadOnlyHint &&
		annotations.DestructiveHint != nil && !*annotations.DestructiveHint:
		description.Effects = graph.EffectSet{graph.EffectRead}
	case annotations.ReadOnlyHint != nil && !*annotations.ReadOnlyHint &&
		annotations.DestructiveHint != nil && *annotations.DestructiveHint:
		description.Effects = graph.EffectSet{graph.EffectDestructive}
	case annotations.ReadOnlyHint != nil && !*annotations.ReadOnlyHint &&
		annotations.DestructiveHint != nil && !*annotations.DestructiveHint:
		description.Effects = graph.EffectSet{graph.EffectMutate}
	}
	if annotations.IdempotentHint != nil && *annotations.IdempotentHint {
		description.Idempotency = graph.IdempotencyIntrinsic
		description.RetrySafety = stepkind.RetrySafe
	}
	return description
}

// Execute resolves secrets at the immediate client boundary and returns only
// persist-safe typed values.
func (k *Kind) Execute(ctx context.Context, invocation stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, permanent("mcp_invalid_invocation", "MCP invocation is invalid", errors.New("context is required"), nil)
	}
	parsed, err := parseConfig(invocation.Invocation.Config)
	if err != nil {
		return stepkind.StepResult{}, permanent("mcp_invalid_config", "MCP step configuration is invalid", err, nil)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.StepResult{}, classifyCallFailure("mcp_canceled", "MCP tool call was canceled", contextErr, parsed)
	}
	description, err := k.DescribeConfig(ctx, invocation.Invocation.Config)
	if err != nil {
		return stepkind.StepResult{}, classifyCallFailure("mcp_descriptor_error", "MCP tool descriptor is unavailable", err, parsed)
	}
	idempotencyKey := parsed.IdempotencyKey
	if invocation.Invocation.IdempotencyKey != "" {
		if idempotencyKey != "" && idempotencyKey != invocation.Invocation.IdempotencyKey {
			return stepkind.StepResult{}, permanent("mcp_idempotency_conflict", "MCP idempotency declarations conflict", nil, safeDetails(parsed))
		}
		idempotencyKey = invocation.Invocation.IdempotencyKey
	}
	arguments, resolved, err := resolveArguments(ctx, k.secrets, parsed.Arguments)
	if err != nil {
		return stepkind.StepResult{}, permanent("mcp_secret_resolution", "MCP argument secret resolution failed", err, safeDetails(parsed))
	}
	defer forgetSecrets(resolved)
	redactor, err := values.NewRedactor(resolved...)
	if err != nil {
		return stepkind.StepResult{}, permanent("mcp_secret_redaction", "MCP secret redaction setup failed", err, safeDetails(parsed))
	}
	callCtx, cancel := context.WithTimeout(ctx, parsed.Timeout)
	defer cancel()
	result, callErr := k.client.ExecuteTool(callCtx, CallRequest{
		Server: parsed.Server, Tool: parsed.Tool, Arguments: arguments, IdempotencyKey: idempotencyKey,
	})
	forgetArgumentSecrets(arguments)
	activityOutcome := verification.ActivitySucceeded
	if callErr != nil || callCtx.Err() != nil || result.IsError {
		activityOutcome = verification.ActivityFailed
	}
	if invocation.Invocation.Activity != nil {
		if activityErr := invocation.Invocation.Activity.RecordToolCall(context.WithoutCancel(callCtx), verification.ToolCall{
			Server: parsed.Server, Tool: parsed.Tool, Outcome: activityOutcome,
		}); activityErr != nil {
			return stepkind.StepResult{}, permanent("mcp_activity_recording", "MCP tool activity could not be recorded", activityErr, safeDetails(parsed))
		}
	}
	if callErr != nil {
		var resultErr *ResultError
		if errors.As(callErr, &resultErr) {
			return stepkind.StepResult{}, permanent("mcp_invalid_result", "MCP tool returned an invalid result", resultErr, safeDetails(parsed))
		}
		return stepkind.StepResult{}, classifyCallFailure("mcp_transport_error", "MCP transport failed", callErr, parsed)
	}
	if callCtx.Err() != nil {
		return stepkind.StepResult{}, classifyCallFailure("mcp_timeout", "MCP tool call did not complete", callCtx.Err(), parsed)
	}
	if result.IsError {
		return stepkind.StepResult{}, permanent("mcp_tool_error", "MCP tool reported an error", nil, safeDetails(parsed))
	}
	outputs, counts, err := k.mapResult(ctx, invocation.Invocation.Identity, parsed, description, result, redactor)
	if err != nil {
		return stepkind.StepResult{}, permanent("mcp_invalid_result", "MCP tool returned an invalid result", err, safeDetails(parsed))
	}
	if err := validateExpected(parsed.Expected, counts); err != nil {
		return stepkind.StepResult{}, permanent("mcp_unexpected_result", "MCP tool returned an unexpected result shape", err, safeDetails(parsed))
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
}

func parseConfig(input graph.Config) (config, error) {
	if input == nil {
		return config{}, fmt.Errorf("%w: config must be an object", ErrInvalidConfig)
	}
	server, ok := input["server"].(string)
	if !ok || validateStableText("server", server, true) != nil || len(server) > 512 {
		return config{}, fmt.Errorf("%w: server must be a stable string of at most 512 bytes", ErrInvalidConfig)
	}
	tool, ok := input["tool"].(string)
	if !ok || validateToolName(tool) != nil {
		return config{}, fmt.Errorf("%w: tool must match [A-Za-z0-9_-]{1,128}", ErrInvalidConfig)
	}
	arguments, ok := input["arguments"].(map[string]any)
	if !ok || arguments == nil {
		return config{}, fmt.Errorf("%w: arguments must be an object", ErrInvalidConfig)
	}
	if _, validationErr := values.DigestInline(arguments); validationErr != nil {
		return config{}, fmt.Errorf("%w: arguments: %w", ErrInvalidConfig, validationErr)
	}
	clonedArguments, cloneErr := cloneJSONObject(arguments)
	if cloneErr != nil {
		return config{}, fmt.Errorf("%w: arguments: %w", ErrInvalidConfig, cloneErr)
	}
	if secretErr := validateSecretReferences(clonedArguments); secretErr != nil {
		return config{}, fmt.Errorf("%w: arguments: %w", ErrInvalidConfig, secretErr)
	}
	timeout := defaultTimeout
	if raw, exists := input["timeout"]; exists {
		timeoutText, isString := raw.(string)
		if !isString {
			return config{}, fmt.Errorf("%w: timeout must be a duration string", ErrInvalidConfig)
		}
		parsedTimeout, parseErr := time.ParseDuration(timeoutText)
		if parseErr != nil || parsedTimeout <= 0 || parsedTimeout > maximumTimeout {
			return config{}, fmt.Errorf("%w: timeout must be positive and no greater than %s", ErrInvalidConfig, maximumTimeout)
		}
		timeout = parsedTimeout
	}
	idempotencyKey := ""
	if raw, exists := input["idempotency_key"]; exists {
		idempotencyKey, ok = raw.(string)
		if !ok || validateStableText("idempotency_key", idempotencyKey, true) != nil || len(idempotencyKey) > 512 {
			return config{}, fmt.Errorf("%w: idempotency_key must be a stable string of at most 512 bytes", ErrInvalidConfig)
		}
	}
	expected := ExpectedAny
	if raw, exists := input["expected_result"]; exists {
		text, ok := raw.(string)
		expected = ExpectedResult(text)
		if !ok || !expected.valid() {
			return config{}, fmt.Errorf("%w: unsupported expected_result", ErrInvalidConfig)
		}
	}
	allowed := map[string]struct{}{
		"server": {}, "tool": {}, "arguments": {}, "timeout": {}, "idempotency_key": {}, "expected_result": {},
	}
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := allowed[key]; !ok {
			return config{}, fmt.Errorf("%w: unsupported field %q", ErrInvalidConfig, key)
		}
	}
	return config{Server: server, Tool: tool, Arguments: clonedArguments, Timeout: timeout, IdempotencyKey: idempotencyKey, Expected: expected}, nil
}

func validateToolName(value string) error {
	if len(value) < 1 || len(value) > 128 {
		return ErrInvalidConfig
	}
	for _, current := range value {
		if (current >= 'a' && current <= 'z') || (current >= 'A' && current <= 'Z') ||
			(current >= '0' && current <= '9') || current == '_' || current == '-' {
			continue
		}
		return ErrInvalidConfig
	}
	return nil
}

func cloneJSONObject(input map[string]any) (map[string]any, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var output map[string]any
	if err := decoder.Decode(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func cloneAnnotations(input ToolAnnotations) ToolAnnotations {
	result := input
	result.ReadOnlyHint = cloneBool(input.ReadOnlyHint)
	result.DestructiveHint = cloneBool(input.DestructiveHint)
	result.IdempotentHint = cloneBool(input.IdempotentHint)
	result.OpenWorldHint = cloneBool(input.OpenWorldHint)
	return result
}

func cloneBool(input *bool) *bool {
	if input == nil {
		return nil
	}
	result := *input
	return &result
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

func safeDetails(parsed config) map[string]string {
	return map[string]string{"server": parsed.Server, "tool": parsed.Tool}
}

func permanent(code, message string, cause error, details map[string]string) error {
	return &stepkind.ExecutionError{
		Code: code, Message: message, Classification: stepkind.RetryPermanent,
		Details: details, Cause: cause,
	}
}

func classifyCallFailure(code, message string, cause error, parsed config) error {
	classification := stepkind.ClassifyError(cause)
	if classification == stepkind.RetryUnspecified {
		switch {
		case errors.Is(cause, context.DeadlineExceeded):
			classification = stepkind.Retryable
		case errors.Is(cause, context.Canceled):
			classification = stepkind.RetryPermanent
		default:
			classification = stepkind.RetryPermanent
		}
	}
	return &stepkind.ExecutionError{
		Code: code, Message: message, Classification: classification,
		Details: safeDetails(parsed), Cause: cause,
	}
}
