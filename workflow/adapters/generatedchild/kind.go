package generatedchild

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

type Executor struct {
	processor  Processor
	authorizer Authorizer
	registrar  Registrar
	resolver   workflowcompile.DefinitionResolver
}

func New(options Options) (*Executor, error) {
	if nilInterface(options.Processor) || nilInterface(options.Authorizer) || nilInterface(options.Registrar) || nilInterface(options.Resolver) {
		return nil, fmt.Errorf("%w: processor, authorizer, registrar, and durable resolver are required", ErrInvalidOptions)
	}
	return &Executor{processor: options.Processor, authorizer: options.Authorizer, registrar: options.Registrar, resolver: options.Resolver}, nil
}

func Register(registry stepkind.Registry, options Options) (*Executor, error) {
	if nilInterface(registry) {
		return nil, fmt.Errorf("%w: registry is required", ErrInvalidOptions)
	}
	executor, err := New(options)
	if err != nil {
		return nil, err
	}
	if err := registry.Register(executor); err != nil {
		return nil, err
	}
	return executor, nil
}

func (e *Executor) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: KindName, Version: KindVersion,
		ConfigSchema: graph.Schema{
			"type": "object", "additionalProperties": false, "required": []any{"format", "input", "authority"},
			"properties": map[string]any{
				"format":    map[string]any{"type": "string", "enum": []any{string(FormatWorkflowSource), string(FormatGraphIR)}},
				"input":     map[string]any{"type": "string", "minLength": 1},
				"authority": map[string]any{"type": "string", "minLength": 1},
			},
		},
		InputSchema: graph.Schema{"type": "object"},
		OutputSchema: graph.Schema{
			"type": "object", "additionalProperties": false, "required": []any{OutputDefinition},
			"properties": map[string]any{OutputDefinition: definitionSchema()},
		},
		Effects:              []graph.Effect{graph.EffectRead, graph.EffectCompute, graph.EffectMaterialize, graph.EffectMutate},
		RequiredCapabilities: []string{"workflow.definition.generate", "workflow.definition.register"},
		Idempotency:          graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationNone}, EmbeddedModeSupported: false,
	}
}

func (e *Executor) ValidateConfig(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
	_, findings := parseConfig(config)
	return findings
}

func (e *Executor) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil || e == nil || nilInterface(e.processor) || nilInterface(e.authorizer) || nilInterface(e.registrar) || nilInterface(e.resolver) {
		return stepkind.StepResult{}, executionError(CodeInvalidInvocation, "generated child invocation is invalid", stepkind.RetryPermanent, ErrInvalidOptions)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	invocation := prepared.Invocation
	if err := invocation.Validate(); err != nil {
		return stepkind.StepResult{}, executionError(CodeInvalidInvocation, "generated child invocation is invalid", stepkind.RetryPermanent, err)
	}
	config, findings := parseConfig(invocation.Config)
	if len(findings) != 0 || strings.TrimSpace(invocation.IdempotencyKey) == "" {
		return stepkind.StepResult{}, executionError(CodeInvalidInvocation, "generated child invocation is invalid", stepkind.RetryPermanent, ErrInvalidMaterial)
	}
	material, exists := invocation.Inputs[config.input]
	if !exists {
		return stepkind.StepResult{}, executionError(CodeInvalidInvocation, "generated child material input is missing", stepkind.RetryPermanent, ErrInvalidMaterial)
	}
	processed, processErr := e.processor.ProcessGenerated(ctx, ProcessRequest{Format: config.format, Value: material, Authority: config.authority, Identity: invocation.Identity})
	if processErr != nil {
		return stepkind.StepResult{}, contextOrExecution(ctx, CodeValidationFailed, "generated child validation failed", stepkind.RetryPermanent, processErr)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	request := AuthorizationRequest{
		Identity: invocation.Identity, Definition: processed.Definition.Definition,
		NodeCount: len(processed.Definition.Graph.Nodes), EdgeCount: len(processed.Definition.Graph.Edges),
		Effects:              append(graph.EffectSet(nil), processed.Policy.Effects...),
		RequiredCapabilities: append([]string(nil), processed.Policy.RequiredCapabilities...),
		ConfigDigests:        cloneStringMap(processed.Policy.ConfigDigests),
	}
	decision, authorizationErr := e.authorizer.AuthorizeGenerated(ctx, request)
	if authorizationErr != nil {
		return stepkind.StepResult{}, contextOrExecution(ctx, CodePolicyDenied, "generated child authorization failed", stepkind.RetryPermanent, authorizationErr)
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := decision.Validate(); err != nil || !decision.Allow {
		return stepkind.StepResult{}, executionError(CodePolicyDenied, "generated child authorization denied", stepkind.RetryPermanent, ErrPolicyDenied)
	}
	persisted, outcome, err := e.registrar.RegisterGenerated(context.WithoutCancel(ctx), RegistrationRequest{
		Definition: processed.Definition, Policy: clonePolicy(processed.Policy), Authorization: decision, IdempotencyKey: invocation.IdempotencyKey,
	})
	if err != nil {
		classification := stepkind.Retryable
		if errors.Is(err, ErrRegistrationConflict) {
			classification = stepkind.RetryPermanent
		}
		return stepkind.StepResult{}, executionError(CodeRegistration, "generated child registration failed", classification, err)
	}
	if !outcome.Valid() || !definitionEqual(processed.Definition, persisted) {
		return stepkind.StepResult{}, executionError(CodeRegistration, "generated child registration returned conflicting material", stepkind.RetryPermanent, ErrRegistrationConflict)
	}
	resolved, err := e.resolver.ResolveDefinition(context.WithoutCancel(ctx), persisted.Definition)
	if err != nil || !definitionEqual(persisted, resolved) {
		return stepkind.StepResult{}, executionError(CodeRegistration, "generated child durable resolution disagrees with registration", stepkind.RetryPermanent, ErrRegistrationConflict)
	}
	definitionValue, err := definitionObject(persisted.Definition)
	if err != nil {
		return stepkind.StepResult{}, executionError(CodeRegistration, "generated child output is invalid", stepkind.RetryPermanent, err)
	}
	output, err := values.NewInline(definitionValue, values.Metadata{
		Producer:  values.Producer{Kind: KindName, Reference: invocation.Identity.RunID + "/" + invocation.Identity.NodeID, Output: OutputDefinition},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		return stepkind.StepResult{}, executionError(CodeRegistration, "generated child output is invalid", stepkind.RetryPermanent, err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{OutputDefinition: output}}, nil
}

func definitionObject(ref graph.DefinitionRef) (map[string]any, error) {
	encoded, err := json.Marshal(ref)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func clonePolicy(input PolicySummary) PolicySummary {
	return PolicySummary{Effects: append(graph.EffectSet(nil), input.Effects...), RequiredCapabilities: append([]string(nil), input.RequiredCapabilities...), ConfigDigests: cloneStringMap(input.ConfigDigests)}
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

type parsedConfig struct {
	format    MaterialFormat
	input     string
	authority string
}

func parseConfig(config graph.Config) (parsedConfig, []diagnostic.Diagnostic) {
	if config == nil {
		return parsedConfig{}, []diagnostic.Diagnostic{configFinding("config", "must be an object")}
	}
	allowed := map[string]struct{}{"format": {}, "input": {}, "authority": {}}
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var findings []diagnostic.Diagnostic
	for _, key := range keys {
		if _, ok := allowed[key]; !ok {
			findings = append(findings, configFinding("config."+key, "is unsupported"))
		}
	}
	stringField := func(name string) string {
		value, ok := config[name].(string)
		if !ok || !stableText(value, maxStableBytes) {
			findings = append(findings, configFinding("config."+name, "must be stable non-empty text"))
			return ""
		}
		return value
	}
	parsed := parsedConfig{format: MaterialFormat(stringField("format")), input: stringField("input"), authority: stringField("authority")}
	if parsed.format != "" && !parsed.format.Valid() {
		findings = append(findings, configFinding("config.format", "must be workflow_source or graph_ir"))
	}
	if parsed.input != "" {
		if err := graph.ValidateID(parsed.input); err != nil {
			findings = append(findings, configFinding("config.input", "must be a normalized input name"))
		}
	}
	return parsed, findings
}

func definitionSchema() graph.Schema {
	text := map[string]any{"type": "string", "minLength": 1}
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"authority", "kind", "id", "version", "digest", "provenance"},
		"properties": map[string]any{
			"authority": text, "kind": map[string]any{"const": "workflow"}, "id": text, "locator": map[string]any{"type": "string"},
			"version": text, "digest": text, "provenance": map[string]any{"type": "object"},
		},
	}
}

func configFinding(path, message string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Severity: diagnostic.SeverityError, Code: "HADR-SOURCE-026", Message: path + " " + message, Remediation: &diagnostic.Remediation{Message: "Use the closed generated_child@v1 configuration."}}
}

func executionError(code, message string, classification stepkind.RetryClassification, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Cause: cause}
}

func contextOrExecution(ctx context.Context, code, message string, classification stepkind.RetryClassification, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return executionError(code, message, classification, cause)
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
