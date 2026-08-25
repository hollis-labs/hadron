package generatedapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type runtimeConfig struct {
	timeout          string
	maxResponseBytes int64
}

// Spec returns fresh policy-visible metadata for this generated operation.
func (k *Kind) Spec() stepkind.StepKindSpec {
	if k == nil || k.operation == nil {
		return stepkind.StepKindSpec{}
	}
	description := cloneDescription(k.operation.description)
	idempotency := graph.IdempotencyKeyed
	retry := stepkind.RetryRequiresIdempotency
	if generatedSafeMethod(description.Method) {
		idempotency, retry = graph.IdempotencyIntrinsic, stepkind.RetrySafe
	}
	return stepkind.StepKindSpec{
		Name: description.Name, Version: description.Version,
		ConfigSchema: description.ConfigSchema, InputSchema: description.InputSchema, OutputSchema: description.OutputSchema,
		Effects: description.Effects, RequiredCapabilities: description.RequiredCapabilities,
		Idempotency: idempotency, RetrySafety: retry,
		Cancellation:          stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:           stepkind.ObservationSpec{Mode: stepkind.ObservationNone},
		EmbeddedModeSupported: true,
	}
}

// ValidateConfig validates only bounded execution controls. Request identity,
// destination, method, auth, schemas, effects, and capabilities are generated
// constants and have no runtime override fields.
func (k *Kind) ValidateConfig(ctx context.Context, config graph.Config) []diagnostic.Diagnostic {
	if k == nil || k.operation == nil {
		return []diagnostic.Diagnostic{{
			Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig,
			Message: "generated API operation is unavailable",
		}}
	}
	_, err := parseRuntimeConfig(config)
	if err == nil && ctx != nil {
		err = ctx.Err()
	}
	if err == nil {
		return nil
	}
	return []diagnostic.Diagnostic{{
		Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig,
		Message: "invalid generated API step configuration: " + err.Error(),
	}}
}

// Execute maps typed operation inputs to a fixed HTTP declaration and invokes
// http@v1. Credential references remain opaque until that adapter resolves its
// auth configuration at the existing secret boundary.
func (k *Kind) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil || k == nil || k.operation == nil {
		return stepkind.StepResult{}, invalidInvocation("generated_api_invalid_invocation", "Generated API invocation is invalid", ErrInvalidInvocation)
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, invalidInvocation("generated_api_invalid_invocation", "Generated API invocation is invalid", err)
	}
	runtime, err := parseRuntimeConfig(prepared.Invocation.Config)
	if err != nil {
		return stepkind.StepResult{}, invalidInvocation("generated_api_invalid_config", "Generated API step configuration is invalid", err)
	}
	if validationErr := values.ValidateValueSetSchema(k.operation.description.InputSchema, prepared.Invocation.Inputs); validationErr != nil {
		return stepkind.StepResult{}, invalidInvocation("generated_api_invalid_inputs", "Generated API inputs do not satisfy the generated schema", validationErr)
	}
	safeMethod := generatedSafeMethod(k.operation.description.Method)
	if !safeMethod && prepared.Invocation.IdempotencyKey == "" {
		return stepkind.StepResult{}, invalidInvocation(
			"generated_api_idempotency_required", "Generated API write requires a runtime-bound idempotency key", nil,
		)
	}
	httpConfig, err := k.operation.httpConfig(prepared.Invocation.Inputs, runtime, prepared.Invocation.IdempotencyKey)
	if err != nil {
		return stepkind.StepResult{}, invalidInvocation("generated_api_invalid_inputs", "Generated API inputs could not be mapped safely", err)
	}
	description, err := k.operation.http.DescribeConfig(ctx, httpConfig)
	if err != nil {
		return stepkind.StepResult{}, invalidInvocation("generated_api_policy_description", "Generated API request policy is unavailable", err)
	}
	generated := k.operation.description
	wantIdempotency, wantRetry := graph.IdempotencyKeyed, stepkind.RetryRequiresIdempotency
	if safeMethod {
		wantIdempotency, wantRetry = graph.IdempotencyIntrinsic, stepkind.RetrySafe
	}
	if description.Method != generated.Method || description.Origin != generated.Origin ||
		!sameEffects(description.DeclaredEffects, generated.Effects) ||
		!effectsContained(description.EffectiveEffects, generated.Effects) ||
		!sameStrings(description.DeclaredCapabilities, generated.RequiredCapabilities) ||
		description.EffectiveIdempotency != wantIdempotency || description.EffectiveRetrySafety != wantRetry {
		return stepkind.StepResult{}, invalidInvocation(
			"generated_api_policy_mismatch", "Generated API request exceeds its policy-visible contract", nil,
		)
	}
	delegated := prepared
	delegated.Invocation.Config = httpConfig
	delegated.Invocation.Inputs = values.ValueSet{}
	result, err := k.operation.http.Execute(ctx, delegated)
	if err != nil {
		return stepkind.StepResult{}, err
	}
	if result.Outcome != stepkind.StepCompleted || values.ValidateValueSetSchema(generated.OutputSchema, result.Outputs) != nil {
		return stepkind.StepResult{}, invalidInvocation(
			"generated_api_invalid_output", "Generated API response does not satisfy the generated schema", nil,
		)
	}
	return result, nil
}

func generatedSafeMethod(method string) bool {
	return method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions
}

func sameEffects(left, right graph.EffectSet) bool {
	return len(left) == len(right) && effectsContained(left, right) && effectsContained(right, left)
}

func parseRuntimeConfig(input graph.Config) (runtimeConfig, error) {
	if input == nil {
		input = graph.Config{}
	}
	if _, err := values.DigestInline(map[string]any(input)); err != nil {
		return runtimeConfig{}, fmt.Errorf("%w: config must be native unambiguous JSON", ErrInvalidInvocation)
	}
	result := runtimeConfig{timeout: "30s", maxResponseBytes: defaultMaxResponse}
	for name, value := range input {
		switch name {
		case "timeout":
			text, ok := value.(string)
			duration, err := time.ParseDuration(text)
			if !ok || err != nil || duration <= 0 || duration > 24*time.Hour {
				return runtimeConfig{}, fmt.Errorf("%w: timeout must be between 1ns and 24h", ErrInvalidInvocation)
			}
			result.timeout = text
		case "max_response_bytes":
			parsed, err := boundedInteger(value, 1, defaultMaxResponse)
			if err != nil {
				return runtimeConfig{}, fmt.Errorf("%w: max_response_bytes is invalid", ErrInvalidInvocation)
			}
			result.maxResponseBytes = parsed
		default:
			return runtimeConfig{}, fmt.Errorf("%w: unknown config field %q", ErrInvalidInvocation, name)
		}
	}
	return result, nil
}

func boundedInteger(value any, minimum, maximum int64) (int64, error) {
	var text string
	switch typed := value.(type) {
	case json.Number:
		text = string(typed)
	case int:
		text = strconv.FormatInt(int64(typed), 10)
	case int64:
		text = strconv.FormatInt(typed, 10)
	case uint64:
		text = strconv.FormatUint(typed, 10)
	default:
		return 0, ErrInvalidInvocation
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, ErrInvalidInvocation
	}
	return parsed, nil
}

func (o *operation) httpConfig(inputs values.ValueSet, runtime runtimeConfig, idempotencyKey string) (graph.Config, error) {
	parsedServer, parseErr := url.Parse(o.server)
	if parseErr != nil {
		return nil, parseErr
	}
	pathSegments := strings.Split(strings.TrimPrefix(o.description.PathTemplate, "/"), "/")
	parametersByKey := make(map[string]parameter, len(o.parameters))
	for _, parameter := range o.parameters {
		parametersByKey[parameterKey(parameter.Location, parameter.SourceName)] = parameter
	}
	headers := make(map[string]any)
	query := make(url.Values)
	for index, segment := range pathSegments {
		if strings.HasPrefix(segment, "{") {
			name := segment[1 : len(segment)-1]
			pathParameter := parametersByKey[parameterKey("path", name)]
			text, scalarErr := inlineScalar(inputs[pathParameter.InputName])
			if scalarErr != nil {
				return nil, fmt.Errorf("path input %q is invalid", pathParameter.InputName)
			}
			escaped, escapeErr := escapePathValue(text)
			if escapeErr != nil {
				return nil, fmt.Errorf("path input %q is invalid", pathParameter.InputName)
			}
			pathSegments[index] = escaped
		}
	}
	for _, parameter := range o.parameters {
		if parameter.Location == "path" {
			continue
		}
		value, exists := inputs[parameter.InputName]
		if !exists {
			continue
		}
		switch parameter.Location {
		case "query":
			if parameter.Array {
				items, ok := value.Inline.([]any)
				if !ok {
					return nil, fmt.Errorf("query input %q is invalid", parameter.InputName)
				}
				for _, item := range items {
					text, scalarErr := scalarText(item)
					if scalarErr != nil {
						return nil, fmt.Errorf("query input %q is invalid", parameter.InputName)
					}
					query.Add(parameter.SourceName, text)
				}
			} else {
				text, scalarErr := inlineScalar(value)
				if scalarErr != nil {
					return nil, fmt.Errorf("query input %q is invalid", parameter.InputName)
				}
				query.Set(parameter.SourceName, text)
			}
		case "header":
			text, scalarErr := inlineScalar(value)
			if scalarErr != nil {
				return nil, fmt.Errorf("header input %q is invalid", parameter.InputName)
			}
			headers[http.CanonicalHeaderKey(parameter.SourceName)] = text
		}
	}
	parsedServer.Path = "/" + strings.Join(pathSegments, "/")
	parsedServer.RawPath = parsedServer.Path
	decodedPath, pathErr := url.PathUnescape(parsedServer.RawPath)
	if pathErr != nil {
		return nil, pathErr
	}
	parsedServer.Path = decodedPath
	parsedServer.RawQuery = query.Encode()
	config := graph.Config{
		"method": o.description.Method, "url": parsedServer.String(),
		"timeout": runtime.timeout, "max_response_bytes": json.Number(strconv.FormatInt(runtime.maxResponseBytes, 10)),
		"inline_limit":           json.Number(strconv.FormatInt(runtime.maxResponseBytes, 10)),
		"expected_status":        statusValues(o.description.SuccessStatuses),
		"expected_content_types": []any{"application/json"},
		"expected_json_schema":   cloneSchema(o.responseSchema),
		"redirects":              map[string]any{"mode": "deny"},
		"effects":                effectValues(o.description.Effects),
		"capabilities":           stringValues(o.description.RequiredCapabilities),
	}
	if len(headers) != 0 {
		config["headers"] = headers
	}
	if idempotencyKey != "" {
		config["idempotency_key"] = idempotencyKey
	}
	if o.bodyInput != "" {
		if body, exists := inputs[o.bodyInput]; exists {
			config["body"] = cloneValue(body.Inline)
		}
	}
	if o.credential != nil {
		value := inputs[o.credential.InputName]
		if value.Type != values.TypeSecretRef || value.SecretRef == nil {
			return nil, fmt.Errorf("credential input %q is invalid", o.credential.InputName)
		}
		auth := map[string]any{"type": o.credential.Kind, "secret_ref": string(*value.SecretRef)}
		switch o.credential.Kind {
		case "header":
			auth["header"] = o.credential.Header
		case "basic":
			auth["username"] = o.credential.Username
		}
		config["auth"] = auth
	}
	return config, nil
}

func inlineScalar(value values.Value) (string, error) {
	if value.Type == values.TypeSecretRef || value.Type == values.TypeArtifact {
		return "", ErrInvalidInvocation
	}
	return scalarText(value.Inline)
}

func scalarText(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case json.Number:
		return string(typed), nil
	case bool:
		return strconv.FormatBool(typed), nil
	default:
		return "", ErrInvalidInvocation
	}
}

func statusValues(input []int) []any {
	result := make([]any, len(input))
	for index, value := range input {
		result[index] = json.Number(strconv.Itoa(value))
	}
	return result
}

func effectValues(input graph.EffectSet) []any {
	result := make([]any, len(input))
	for index, value := range input {
		result[index] = string(value)
	}
	return result
}

func stringValues(input []string) []any {
	result := make([]any, len(input))
	for index, value := range input {
		result[index] = value
	}
	return result
}

func sameStrings(left, right []string) bool {
	leftCopy, rightCopy := sortedStrings(left), sortedStrings(right)
	if len(leftCopy) != len(rightCopy) {
		return false
	}
	for index := range leftCopy {
		if leftCopy[index] != rightCopy[index] {
			return false
		}
	}
	return true
}

var _ stepkind.StepKind = (*Kind)(nil)
