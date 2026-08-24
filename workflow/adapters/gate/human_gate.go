package gateadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	workflowgate "github.com/hollis-labs/hadron/workflow/gate"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const (
	Name                = "human_gate"
	Version             = "v1"
	CapabilityGate      = "gate.respond"
	CapabilityWait      = "wait.resume"
	CodeInvalidGate     = "human_gate_invalid"
	CodeAuthorityFailed = "human_gate_authority_failed"
	CodePayloadFailed   = "human_gate_payload_failed"
	CodeContinuation    = "human_gate_continuation_invalid"
	maximumTimeout      = 365 * 24 * time.Hour
)

// Options supplies application-owned gate policy, private payload storage,
// and a deterministic clock. Authority and Payloads are required.
type Options struct {
	Authority workflowgate.AuthorityResolver
	Payloads  workflowgate.PayloadStore
	Now       func() time.Time
}

// Executor implements human_gate@v1. It is concurrency-safe when injected
// collaborators are concurrency-safe.
type Executor struct {
	authority workflowgate.AuthorityResolver
	payloads  workflowgate.PayloadStore
	now       func() time.Time
}

// New constructs a fail-closed human gate executor.
func New(options Options) (*Executor, error) {
	if nilInterface(options.Authority) || nilInterface(options.Payloads) {
		return nil, fmt.Errorf("human gate authority resolver and payload store are required")
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Executor{authority: options.Authority, payloads: options.Payloads, now: now}, nil
}

// Register constructs and registers human_gate@v1.
func Register(registry stepkind.Registry, options Options) (*Executor, error) {
	if nilInterface(registry) {
		return nil, fmt.Errorf("step-kind registry is required")
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

// Spec returns immutable human_gate@v1 metadata. timed_out is const false on
// successful continuation; runtime timeout is a typed failed attempt and never
// fabricated as a successful adapter result.
func (*Executor) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: Name, Version: Version,
		ConfigSchema: configSchema(), InputSchema: graph.Schema{"type": "object"}, OutputSchema: outputSchema(),
		Effects: graph.EffectSet{graph.EffectRead}, RequiredCapabilities: []string{CapabilityGate, CapabilityWait},
		Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationNone}, CanSuspend: true, EmbeddedModeSupported: false,
	}
}

// ValidateConfig reports deterministic config paths. An optional non-blocking
// gate is valid shared vocabulary but rejected here until W07-T09 lowers it to
// ordinary graph readiness semantics.
func (*Executor) ValidateConfig(_ context.Context, input graph.Config) []diagnostic.Diagnostic {
	_, findings := parseConfig(input, "validation-placeholder")
	return findings
}

// Execute stores private presentation payload, resolves app-owned authority,
// and suspends initially. An accepted continuation returns typed decision and
// resume metadata without exposing the resume credential.
func (e *Executor) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, permanent(CodeInvalidGate, "human gate invocation is invalid", errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidGate, "human gate invocation is invalid", err)
	}
	defaultCorrelation := invocationCorrelation(prepared.Invocation.Identity)
	config, findings := parseConfig(prepared.Invocation.Config, defaultCorrelation)
	if len(findings) != 0 {
		return stepkind.StepResult{}, permanent(CodeInvalidGate, "human gate configuration is invalid", errors.New(findings[0].Message))
	}
	expectedID := gateWaitID(prepared.Invocation.Identity, config.checkpoint.Correlation)
	if prepared.Invocation.Continuation != nil {
		return e.continueGate(prepared.Invocation, config, expectedID)
	}
	if e == nil || nilInterface(e.authority) || nilInterface(e.payloads) || e.now == nil {
		return stepkind.StepResult{}, permanent(CodeInvalidGate, "human gate execution boundary is unavailable", errors.New("executor is not initialized"))
	}
	authority, err := e.authority.ResolveGateAuthority(ctx, workflowgate.AuthorizationRequest{
		RunID: prepared.Invocation.Identity.RunID, NodeID: prepared.Invocation.Identity.NodeID,
		Iteration: prepared.Invocation.Identity.Iteration, Attempt: prepared.Invocation.Identity.Attempt,
		Checkpoint: workflowgate.CloneCheckpoint(config.checkpoint),
	})
	if err != nil {
		if ctx.Err() != nil {
			return stepkind.StepResult{}, ctx.Err()
		}
		return stepkind.StepResult{}, permanent(CodeAuthorityFailed, "human gate authority resolution failed", err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.StepResult{}, contextErr
	}
	authority.Attributes = cloneStringMap(authority.Attributes)
	if validationErr := authority.Validate(); validationErr != nil {
		return stepkind.StepResult{}, permanent(CodeAuthorityFailed, "human gate authority is invalid", validationErr)
	}
	payload, err := e.payloads.StoreGatePayload(ctx, workflowgate.PayloadRequest{
		RunID: prepared.Invocation.Identity.RunID, NodeID: prepared.Invocation.Identity.NodeID,
		Iteration: prepared.Invocation.Identity.Iteration, Attempt: prepared.Invocation.Identity.Attempt,
		Checkpoint: workflowgate.CloneCheckpoint(config.checkpoint),
	})
	if err != nil {
		if ctx.Err() != nil {
			return stepkind.StepResult{}, ctx.Err()
		}
		return stepkind.StepResult{}, retryable(CodePayloadFailed, "human gate payload storage failed", err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.StepResult{}, contextErr
	}
	if err := payload.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(CodePayloadFailed, "human gate payload reference is invalid", err)
	}
	current := e.now()
	if current.IsZero() {
		return stepkind.StepResult{}, permanent(CodeInvalidGate, "human gate clock is invalid", errors.New("zero time"))
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.StepResult{}, contextErr
	}
	record := workflowwait.Record{
		Kind: workflowwait.KindGate, Correlation: config.checkpoint.Correlation,
		Deadline: current.UTC().Add(config.timeout), Payload: &payload,
		ResumeSchema: config.checkpoint.ResumeSchema, Visibility: workflowwait.VisibilityPrivate,
		Authority: authority, WakeSource: workflowwait.WakeGate, Status: workflowwait.StatusOpen,
	}
	if err := record.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(CodeInvalidGate, "human gate wait record is invalid", err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepWaiting, Wait: &stepkind.WaitResult{ID: expectedID, Record: record}}, nil
}

type parsedConfig struct {
	checkpoint workflowgate.Checkpoint
	timeout    time.Duration
}

func (e *Executor) continueGate(invocation stepkind.Invocation, config parsedConfig, expectedID string) (stepkind.StepResult, error) {
	continuation := invocation.Continuation
	if err := continuation.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate continuation is invalid", err)
	}
	record := continuation.Record
	if continuation.ID != expectedID || record.Kind != workflowwait.KindGate || record.WakeSource != workflowwait.WakeGate ||
		record.Correlation != config.checkpoint.Correlation || record.ResumeSchema.Digest != config.checkpoint.ResumeSchema.Digest || record.Payload == nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate continuation does not match invocation", errors.New("identity or immutable record mismatch"))
	}
	payload, ok := continuation.Values["resume"]
	if !ok || payload.Type != values.TypeObject || payload.Inline == nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate continuation payload is invalid", errors.New("resume object is required"))
	}
	if payload.Redaction != values.RedactionPrivate && payload.Redaction != values.RedactionSecret ||
		payload.Retention != values.RetentionRun && payload.Retention != values.RetentionProject && payload.Retention != values.RetentionExternal {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate continuation payload classification is invalid", errors.New("resume payload must be at least private/run classified"))
	}
	if err := values.ValidateValueSchema(record.ResumeSchema.Schema, payload); err != nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate continuation payload schema is invalid", err)
	}
	object, ok := payload.Inline.(map[string]any)
	if !ok {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate continuation payload is invalid", errors.New("resume value is not an object"))
	}
	decision, ok := object["decision"].(string)
	if !ok {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate decision is invalid", errors.New("decision is not a string"))
	}
	optionKind := workflowgate.OptionDecision
	found := false
	for _, option := range config.checkpoint.Options {
		if option.ID == decision {
			optionKind, found = option.Kind, true
			if optionKind == "" {
				optionKind = workflowgate.OptionDecision
			}
			break
		}
	}
	if !found {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate decision is invalid", errors.New("decision is not a configured option"))
	}
	outputs := values.ValueSet{}
	var err error
	outputs["decision"], err = values.NewInline(decision, metadata(invocation.Identity, "decision"))
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate decision output is invalid", err)
	}
	outputs["skipped"], err = values.NewInline(optionKind == workflowgate.OptionSkip, metadata(invocation.Identity, "skipped"))
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate skip output is invalid", err)
	}
	resolution := record.Resolution
	if resolution == nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate resolution is missing", errors.New("resolution is required"))
	}
	resume := map[string]any{
		"wait_id": expectedID, "status": "resumed", "source": string(resolution.Source),
		"correlation": record.Correlation, "resolved_at": resolution.ResolvedAt.UTC().Format(time.RFC3339Nano),
		"responder": map[string]any{"kind": resolution.Responder.Kind, "reference": resolution.Responder.Reference, "attributes": cloneStringMap(resolution.Responder.Attributes)},
	}
	outputs["resume"], err = values.NewInline(resume, metadata(invocation.Identity, "resume"))
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate resume output is invalid", err)
	}
	outputs["timed_out"], err = values.NewInline(false, metadata(invocation.Identity, "timed_out"))
	if err != nil {
		return stepkind.StepResult{}, permanent(CodeContinuation, "human gate timeout output is invalid", err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
}

func parseConfig(input graph.Config, defaultCorrelation string) (parsedConfig, []diagnostic.Diagnostic) {
	object, err := cloneConfig(input)
	if err != nil {
		return parsedConfig{}, []diagnostic.Diagnostic{finding("config", "must be a JSON-compatible object")}
	}
	var findings []diagnostic.Diagnostic
	allowed := map[string]struct{}{
		"prompt": {}, "options": {}, "environment": {}, "policy_subject": {}, "correlation": {},
		"timeout": {}, "optional": {}, "blocking": {}, "escalations": {},
	}
	for _, key := range sortedKeys(object) {
		if _, ok := allowed[key]; !ok {
			findings = append(findings, finding("config."+key, "is not supported by human_gate@v1"))
		}
	}
	prompt := requiredString(object["prompt"], "config.prompt", 16<<10, &findings)
	options := parseOptions(object["options"], &findings)
	subject := parseSubject(object, &findings)
	correlation := optionalString(object["correlation"], "config.correlation", 4096, &findings)
	if correlation == "" {
		correlation = defaultCorrelation
	}
	timeout := duration(object["timeout"], "config.timeout", &findings)
	behavior := workflowgate.Behavior{Optional: boolean(object["optional"], false, "config.optional", &findings), Blocking: boolean(object["blocking"], true, "config.blocking", &findings)}
	escalations := parseEscalations(object["escalations"], &findings)
	resumeSchema, schemaErr := workflowwait.NewSchemaRef(decisionSchema(options))
	if schemaErr != nil {
		findings = append(findings, finding("config.options", "cannot construct decision schema"))
	}
	checkpoint := workflowgate.Checkpoint{
		Prompt: prompt, Options: options, ResumeSchema: resumeSchema, Subject: subject,
		Correlation: correlation, Behavior: behavior, Escalations: escalations,
	}
	if len(findings) == 0 {
		if err := checkpoint.Validate(); err != nil {
			findings = append(findings, finding("config", err.Error()))
		}
	}
	if behavior.Optional && !behavior.Blocking {
		findings = append(findings, finding("config.blocking", "non-blocking optional gates require W07-T09 graph lowering and cannot execute directly"))
	}
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].Message < findings[j].Message })
	return parsedConfig{checkpoint: checkpoint, timeout: timeout}, findings
}

func parseOptions(value any, findings *[]diagnostic.Diagnostic) []workflowgate.Option {
	raw, ok := value.([]any)
	if !ok || len(raw) == 0 {
		*findings = append(*findings, finding("config.options", "must be a non-empty array"))
		return nil
	}
	options := make([]workflowgate.Option, 0, len(raw))
	for index, item := range raw {
		path := fmt.Sprintf("config.options[%d]", index)
		object, ok := item.(map[string]any)
		if !ok {
			*findings = append(*findings, finding(path, "must be an object"))
			continue
		}
		for _, key := range sortedKeys(object) {
			if key != "id" && key != "label" && key != "kind" {
				*findings = append(*findings, finding(path+"."+key, "is not supported"))
			}
		}
		option := workflowgate.Option{
			ID:    requiredString(object["id"], path+".id", 128, findings),
			Label: requiredString(object["label"], path+".label", 512, findings),
			Kind:  workflowgate.OptionKind(optionalString(object["kind"], path+".kind", 32, findings)),
		}
		options = append(options, option)
	}
	return options
}

func parseSubject(object map[string]any, findings *[]diagnostic.Diagnostic) workflowgate.PolicySubject {
	environment, hasEnvironment := object["environment"]
	policy, hasPolicy := object["policy_subject"]
	if hasEnvironment == hasPolicy {
		*findings = append(*findings, finding("config", "must declare exactly one of environment or policy_subject"))
		return workflowgate.PolicySubject{}
	}
	if hasEnvironment {
		reference := requiredString(environment, "config.environment", 4096, findings)
		if strings.Contains(reference, "{{") || strings.Contains(reference, "}}") {
			*findings = append(*findings, finding("config.environment", "must be a static policy subject"))
		}
		return workflowgate.PolicySubject{Kind: workflowgate.PolicyEnvironment, Reference: reference}
	}
	return subjectObject(policy, "config.policy_subject", findings)
}

func subjectObject(value any, path string, findings *[]diagnostic.Diagnostic) workflowgate.PolicySubject {
	object, ok := value.(map[string]any)
	if !ok {
		*findings = append(*findings, finding(path, "must be an object"))
		return workflowgate.PolicySubject{}
	}
	for _, key := range sortedKeys(object) {
		if key != "kind" && key != "reference" && key != "attributes" {
			*findings = append(*findings, finding(path+"."+key, "is not supported"))
		}
	}
	attributes := map[string]string{}
	if raw := object["attributes"]; raw != nil {
		attributeObject, ok := raw.(map[string]any)
		if !ok {
			*findings = append(*findings, finding(path+".attributes", "must be an object of strings"))
		} else {
			for _, key := range sortedKeys(attributeObject) {
				attributes[key] = requiredString(attributeObject[key], path+".attributes."+key, 4096, findings)
			}
		}
	}
	return workflowgate.PolicySubject{
		Kind:      requiredString(object["kind"], path+".kind", 128, findings),
		Reference: requiredString(object["reference"], path+".reference", 4096, findings), Attributes: attributes,
	}
}

func parseEscalations(value any, findings *[]diagnostic.Diagnostic) []workflowgate.Escalation {
	if value == nil {
		return nil
	}
	raw, ok := value.([]any)
	if !ok {
		*findings = append(*findings, finding("config.escalations", "must be an array"))
		return nil
	}
	escalations := make([]workflowgate.Escalation, 0, len(raw))
	for index, item := range raw {
		path := fmt.Sprintf("config.escalations[%d]", index)
		object, ok := item.(map[string]any)
		if !ok {
			*findings = append(*findings, finding(path, "must be an object"))
			continue
		}
		for _, key := range sortedKeys(object) {
			if key != "after" && key != "subject" {
				*findings = append(*findings, finding(path+"."+key, "is not supported"))
			}
		}
		escalations = append(escalations, workflowgate.Escalation{
			After:   requiredString(object["after"], path+".after", 64, findings),
			Subject: subjectObject(object["subject"], path+".subject", findings),
		})
	}
	return escalations
}

func decisionSchema(options []workflowgate.Option) graph.Schema {
	allowed := make([]any, 0, len(options))
	for _, option := range options {
		if option.ID != "" {
			allowed = append(allowed, option.ID)
		}
	}
	return graph.Schema{
		"type": "object", "additionalProperties": false, "required": []any{"decision"},
		"properties": map[string]any{"decision": map[string]any{"type": "string", "enum": allowed}},
	}
}

func configSchema() graph.Schema {
	subject := map[string]any{
		"type": "object", "additionalProperties": false, "required": []any{"kind", "reference"},
		"properties": map[string]any{
			"kind":       map[string]any{"type": "string", "minLength": json.Number("1")},
			"reference":  map[string]any{"type": "string", "minLength": json.Number("1")},
			"attributes": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
		},
	}
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"prompt", "options", "timeout"},
		"oneOf":    []any{map[string]any{"required": []any{"environment"}}, map[string]any{"required": []any{"policy_subject"}}},
		"properties": map[string]any{
			"prompt": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number(fmt.Sprint(16 << 10))},
			"options": map[string]any{"type": "array", "minItems": json.Number("1"), "maxItems": json.Number("128"), "items": map[string]any{
				"type": "object", "additionalProperties": false, "required": []any{"id", "label"},
				"properties": map[string]any{
					"id":    map[string]any{"type": "string", "minLength": json.Number("1")},
					"label": map[string]any{"type": "string", "minLength": json.Number("1")},
					"kind":  map[string]any{"type": "string", "enum": []any{"decision", "skip"}},
				},
			}},
			"environment":    map[string]any{"type": "string", "minLength": json.Number("1")},
			"policy_subject": subject,
			"correlation":    map[string]any{"type": "string", "minLength": json.Number("1")},
			"timeout":        map[string]any{"type": "string", "minLength": json.Number("1")},
			"optional":       map[string]any{"type": "boolean"}, "blocking": map[string]any{"type": "boolean"},
			"escalations": map[string]any{"type": "array", "maxItems": json.Number("32"), "items": map[string]any{
				"type": "object", "additionalProperties": false, "required": []any{"after", "subject"},
				"properties": map[string]any{"after": map[string]any{"type": "string", "minLength": json.Number("1")}, "subject": subject},
			}},
		},
	}
}

func outputSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"decision", "skipped", "resume", "timed_out"},
		"properties": map[string]any{
			"decision": map[string]any{"type": "string"}, "skipped": map[string]any{"type": "boolean"},
			"resume": map[string]any{"type": "object"}, "timed_out": map[string]any{"const": false},
		},
	}
}

func duration(value any, path string, findings *[]diagnostic.Diagnostic) time.Duration {
	text := requiredString(value, path, 64, findings)
	parsed, err := time.ParseDuration(text)
	if err != nil || parsed <= 0 || parsed > maximumTimeout {
		*findings = append(*findings, finding(path, "must be a positive duration no greater than 8760h"))
		return 0
	}
	return parsed
}

func boolean(value any, fallback bool, path string, findings *[]diagnostic.Diagnostic) bool {
	if value == nil {
		return fallback
	}
	result, ok := value.(bool)
	if !ok {
		*findings = append(*findings, finding(path, "must be a boolean"))
		return fallback
	}
	return result
}

func requiredString(value any, path string, maximum int, findings *[]diagnostic.Diagnostic) string {
	text, ok := value.(string)
	if !ok || !stableText(text, maximum) {
		*findings = append(*findings, finding(path, "must be non-empty stable UTF-8"))
		return ""
	}
	return text
}

func optionalString(value any, path string, maximum int, findings *[]diagnostic.Diagnostic) string {
	if value == nil {
		return ""
	}
	return requiredString(value, path, maximum, findings)
}

func stableText(value string, maximum int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) || len(value) > maximum {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func cloneConfig(config graph.Config) (map[string]any, error) {
	if config == nil {
		return nil, errors.New("nil config")
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing config JSON")
	}
	return object, nil
}

func finding(path, reason string) diagnostic.Diagnostic {
	return diagnostic.Diagnostic{Severity: diagnostic.SeverityError, Code: stepkind.CodeInvalidConfig, Message: path + " " + reason}
}

func invocationCorrelation(identity stepkind.InvocationIdentity) string {
	correlation := identity.RunID + ":" + identity.NodeID
	if identity.Iteration != "" {
		correlation += ":" + identity.Iteration
	}
	return correlation
}

func gateWaitID(identity stepkind.InvocationIdentity, correlation string) string {
	seed := strings.Join([]string{identity.RunID, identity.NodeID, identity.Iteration, fmt.Sprint(identity.Attempt), Name, correlation}, "\x00")
	return "wait-" + strings.TrimPrefix(values.SHA256Digest([]byte(seed)), "sha256:")[:32]
}

func metadata(identity stepkind.InvocationIdentity, output string) values.Metadata {
	reference := identity.RunID + "/" + identity.NodeID
	if identity.Iteration != "" {
		reference += "/" + identity.Iteration
	}
	reference += fmt.Sprintf("/attempt-%d", identity.Attempt)
	return values.Metadata{Producer: values.Producer{Kind: Name, Reference: reference, Output: output}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
}

func permanent(code, message string, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.RetryPermanent, Cause: cause}
}

func retryable(code, message string, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.Retryable, Cause: cause}
}

func cloneStringMap(entries map[string]string) map[string]string {
	if entries == nil {
		return nil
	}
	copyEntries := make(map[string]string, len(entries))
	for key, value := range entries {
		copyEntries[key] = value
	}
	return copyEntries
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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
