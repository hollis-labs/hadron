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
	maximumConfigBytes  = 64 << 10
)

// DecisionSchemaMode selects whether a shared gate profile derives its resume
// schema from declared option IDs or requires an exact authored schema.
type DecisionSchemaMode string

const (
	DecisionSchemaDerived    DecisionSchemaMode = "derived"
	DecisionSchemaConfigured DecisionSchemaMode = "configured"
)

// Profile gives a named gate-like adapter its immutable step-kind vocabulary
// while keeping parsing, suspension, continuation, timeout, and redaction in
// one implementation. Adapter packages should publish a fixed profile rather
// than accepting profile data from workflow authors.
type Profile struct {
	Name                string
	Version             string
	Label               string
	RespondCapability   string
	WaitCapability      string
	InvalidCode         string
	AuthorityFailedCode string
	PayloadFailedCode   string
	ContinuationCode    string
	DecisionSchema      DecisionSchemaMode
}

// StepKindSpec returns the immutable static metadata described by p. Adapter
// constructors validate profiles before execution; callers defining a custom
// profile should use NewProfile rather than publishing unchecked metadata.
func (p Profile) StepKindSpec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: p.Name, Version: p.Version,
		ConfigSchema: configSchema(p), InputSchema: graph.Schema{"type": "object"}, OutputSchema: outputSchema(),
		Effects: graph.EffectSet{graph.EffectRead}, RequiredCapabilities: []string{p.RespondCapability, p.WaitCapability},
		Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationNone}, Memoization: stepkind.MemoizationDisabled,
		CanSuspend: true, EmbeddedModeSupported: false,
	}
}

func (p Profile) validate() error {
	if !profileIdentifier(p.Name) || !profileIdentifier(p.Version) || !profileIdentifier(p.RespondCapability) ||
		!profileIdentifier(p.WaitCapability) || !profileIdentifier(p.InvalidCode) || !profileIdentifier(p.AuthorityFailedCode) ||
		!profileIdentifier(p.PayloadFailedCode) || !profileIdentifier(p.ContinuationCode) {
		return fmt.Errorf("gate profile identifiers are invalid")
	}
	fields := []struct{ name, value string }{
		{"label", p.Label},
	}
	for _, field := range fields {
		if !stableText(field.value, 128) {
			return fmt.Errorf("gate profile %s is invalid", field.name)
		}
	}
	if p.DecisionSchema != DecisionSchemaDerived && p.DecisionSchema != DecisionSchemaConfigured {
		return fmt.Errorf("gate profile decision schema mode is invalid")
	}
	return nil
}

func humanGateProfile() Profile {
	return Profile{
		Name: Name, Version: Version, Label: "human gate", RespondCapability: CapabilityGate, WaitCapability: CapabilityWait,
		InvalidCode: CodeInvalidGate, AuthorityFailedCode: CodeAuthorityFailed, PayloadFailedCode: CodePayloadFailed,
		ContinuationCode: CodeContinuation, DecisionSchema: DecisionSchemaDerived,
	}
}

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
	profile   Profile
}

func (e *Executor) effectiveProfile() Profile {
	if e == nil || e.profile.Name == "" {
		return humanGateProfile()
	}
	return e.profile
}

// New constructs a fail-closed human gate executor.
func New(options Options) (*Executor, error) {
	return NewProfile(humanGateProfile(), options)
}

// NewProfile constructs a gate executor with one immutable adapter-owned
// profile. The profile is copied and validated during construction.
func NewProfile(profile Profile, options Options) (*Executor, error) {
	if nilInterface(options.Authority) || nilInterface(options.Payloads) {
		return nil, fmt.Errorf("gate authority resolver and payload store are required")
	}
	if err := profile.validate(); err != nil {
		return nil, err
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Executor{authority: options.Authority, payloads: options.Payloads, now: now, profile: profile}, nil
}

// Register constructs and registers human_gate@v1.
func Register(registry stepkind.Registry, options Options) (*Executor, error) {
	return RegisterProfile(registry, humanGateProfile(), options)
}

// RegisterProfile constructs and registers one immutable shared gate profile.
func RegisterProfile(registry stepkind.Registry, profile Profile, options Options) (*Executor, error) {
	if nilInterface(registry) {
		return nil, fmt.Errorf("step-kind registry is required")
	}
	executor, err := NewProfile(profile, options)
	if err != nil {
		return nil, err
	}
	if err := RegisterExecutor(registry, executor); err != nil {
		return nil, err
	}
	return executor, nil
}

// RegisterExecutor applies the shared typed-nil-safe registry admission used
// by thin profiled wrappers.
func RegisterExecutor(registry stepkind.Registry, executor stepkind.StepKind) error {
	if nilInterface(registry) {
		return fmt.Errorf("step-kind registry is required")
	}
	if nilInterface(executor) {
		return fmt.Errorf("gate executor is required")
	}
	return registry.Register(executor)
}

// Spec returns immutable human_gate@v1 metadata. timed_out is const false on
// successful continuation; runtime timeout is a typed failed attempt and never
// fabricated as a successful adapter result.
func (e *Executor) Spec() stepkind.StepKindSpec {
	return e.effectiveProfile().StepKindSpec()
}

// ValidateConfig reports deterministic config paths. An optional non-blocking
// gate is valid shared vocabulary but rejected here until W07-T09 lowers it to
// ordinary graph readiness semantics.
func (e *Executor) ValidateConfig(_ context.Context, input graph.Config) []diagnostic.Diagnostic {
	_, findings := parseConfig(e.effectiveProfile(), input, "validation-placeholder")
	return findings
}

// Execute stores private presentation payload, resolves app-owned authority,
// and suspends initially. An accepted continuation returns typed decision and
// resume metadata without exposing the resume credential.
func (e *Executor) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	profile := e.effectiveProfile()
	if ctx == nil {
		return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" invocation is invalid", errors.New("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" invocation is invalid", err)
	}
	defaultCorrelation := invocationCorrelation(prepared.Invocation.Identity)
	config, findings := parseConfig(profile, prepared.Invocation.Config, defaultCorrelation)
	if len(findings) != 0 {
		return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" configuration is invalid", errors.New(findings[0].Message))
	}
	expectedID := gateWaitID(profile.Name, prepared.Invocation.Identity, config.checkpoint.Correlation)
	if prepared.Invocation.Continuation != nil {
		return e.continueGate(prepared.Invocation, config, expectedID)
	}
	if config.triggerInput != "" {
		trigger, ok := prepared.Invocation.Inputs[config.triggerInput]
		if !ok || trigger.Type != values.TypeBoolean || trigger.Artifact != nil {
			return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" trigger input is invalid", errors.New("trigger input must be an inline boolean"))
		}
		triggered, ok := trigger.Inline.(bool)
		if !ok {
			return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" trigger input is invalid", errors.New("trigger input must be an inline boolean"))
		}
		if !triggered {
			if config.notTriggered != "proceed" {
				return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" non-triggered skip must be lowered to ordinary node readiness", errors.New("non-triggered skip reached executor"))
			}
			return nonTriggeredOutputs(profile, prepared.Invocation, config)
		}
	}
	if e == nil || nilInterface(e.authority) || nilInterface(e.payloads) || e.now == nil {
		return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" execution boundary is unavailable", errors.New("executor is not initialized"))
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
		return stepkind.StepResult{}, permanent(profile.AuthorityFailedCode, profile.Label+" authority resolution failed", err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.StepResult{}, contextErr
	}
	authority.Attributes = cloneStringMap(authority.Attributes)
	if validationErr := validateSafeAuthority(authority); validationErr != nil {
		return stepkind.StepResult{}, permanent(profile.AuthorityFailedCode, profile.Label+" authority is invalid", validationErr)
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
		return stepkind.StepResult{}, retryable(profile.PayloadFailedCode, profile.Label+" payload storage failed", err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.StepResult{}, contextErr
	}
	if err := payload.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(profile.PayloadFailedCode, profile.Label+" payload reference is invalid", err)
	}
	current := e.now()
	if current.IsZero() {
		return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" clock is invalid", errors.New("zero time"))
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
		return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" wait record is invalid", err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepWaiting, Wait: &stepkind.WaitResult{ID: expectedID, Record: record}}, nil
}

type parsedConfig struct {
	checkpoint      workflowgate.Checkpoint
	timeout         time.Duration
	triggerInput    string
	notTriggered    string
	defaultDecision string
}

func (e *Executor) continueGate(invocation stepkind.Invocation, config parsedConfig, expectedID string) (stepkind.StepResult, error) {
	profile := e.effectiveProfile()
	continuation := invocation.Continuation
	if err := continuation.Validate(); err != nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" continuation is invalid", err)
	}
	record := continuation.Record
	if continuation.ID != expectedID || record.Kind != workflowwait.KindGate || record.WakeSource != workflowwait.WakeGate ||
		record.Correlation != config.checkpoint.Correlation || record.ResumeSchema.Digest != config.checkpoint.ResumeSchema.Digest || record.Payload == nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" continuation does not match invocation", errors.New("identity or immutable record mismatch"))
	}
	payload, ok := continuation.Values["resume"]
	if !ok || payload.Type != values.TypeObject || payload.Inline == nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" continuation payload is invalid", errors.New("resume object is required"))
	}
	if payload.Redaction != values.RedactionPrivate && payload.Redaction != values.RedactionSecret ||
		payload.Retention != values.RetentionRun && payload.Retention != values.RetentionProject && payload.Retention != values.RetentionExternal {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" continuation payload classification is invalid", errors.New("resume payload must be at least private/run classified"))
	}
	if err := values.ValidateValueSchema(record.ResumeSchema.Schema, payload); err != nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" continuation payload schema is invalid", err)
	}
	object, ok := payload.Inline.(map[string]any)
	if !ok {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" continuation payload is invalid", errors.New("resume value is not an object"))
	}
	decision, ok := object["decision"].(string)
	if !ok {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" decision is invalid", errors.New("decision is not a string"))
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
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" decision is invalid", errors.New("decision is not a configured option"))
	}
	outputs := values.ValueSet{}
	var err error
	outputs["decision"], err = values.NewInline(decision, metadata(profile.Name, invocation.Identity, "decision"))
	if err != nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" decision output is invalid", err)
	}
	outputs["skipped"], err = values.NewInline(optionKind == workflowgate.OptionSkip, metadata(profile.Name, invocation.Identity, "skipped"))
	if err != nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" skip output is invalid", err)
	}
	if record.Resolution == nil || record.Resolution.Source != workflowwait.WakeGate {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" resolution is invalid", errors.New("gate resolution provenance is required"))
	}
	resolution := *record.Resolution
	resolution.Responder.Attributes = cloneStringMap(resolution.Responder.Attributes)
	if responderErr := validateSafeResponder(resolution.Responder); responderErr != nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" resolution responder is invalid", responderErr)
	}
	resume := map[string]any{
		"wait_id": expectedID, "status": "resumed", "source": string(resolution.Source),
		"correlation": record.Correlation, "resolved_at": resolution.ResolvedAt.UTC().Format(time.RFC3339Nano),
		"responder": map[string]any{"kind": resolution.Responder.Kind, "reference": resolution.Responder.Reference, "attributes": cloneStringMap(resolution.Responder.Attributes)},
	}
	outputs["resume"], err = values.NewInline(resume, metadata(profile.Name, invocation.Identity, "resume"))
	if err != nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" resume output is invalid", err)
	}
	outputs["timed_out"], err = values.NewInline(false, metadata(profile.Name, invocation.Identity, "timed_out"))
	if err != nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" timeout output is invalid", err)
	}
	outputs["triggered"], err = values.NewInline(true, metadata(profile.Name, invocation.Identity, "triggered"))
	if err != nil {
		return stepkind.StepResult{}, permanent(profile.ContinuationCode, profile.Label+" trigger output is invalid", err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
}

func nonTriggeredOutputs(profile Profile, invocation stepkind.Invocation, config parsedConfig) (stepkind.StepResult, error) {
	resume := map[string]any{"status": "not-triggered", "source": "not-triggered", "correlation": config.checkpoint.Correlation}
	outputValues := map[string]any{
		"decision": config.defaultDecision, "skipped": true, "resume": resume,
		"timed_out": false, "triggered": false,
	}
	outputs := make(values.ValueSet, len(outputValues))
	for _, name := range []string{"decision", "skipped", "resume", "timed_out", "triggered"} {
		value, err := values.NewInline(outputValues[name], metadata(profile.Name, invocation.Identity, name))
		if err != nil {
			return stepkind.StepResult{}, permanent(profile.InvalidCode, profile.Label+" non-triggered output is invalid", err)
		}
		outputs[name] = value
	}
	return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
}

func parseConfig(profile Profile, input graph.Config, defaultCorrelation string) (parsedConfig, []diagnostic.Diagnostic) {
	object, err := cloneConfig(input)
	if err != nil {
		return parsedConfig{}, []diagnostic.Diagnostic{finding("config", "must be a JSON-compatible object")}
	}
	var findings []diagnostic.Diagnostic
	allowed := map[string]struct{}{
		"prompt": {}, "options": {}, "environment": {}, "policy_subject": {}, "correlation": {},
		"timeout": {}, "optional": {}, "blocking": {}, "escalations": {},
		"trigger_input": {}, "not_triggered": {}, "default_decision": {},
	}
	if profile.DecisionSchema == DecisionSchemaConfigured {
		allowed["decision_schema"] = struct{}{}
	}
	for _, key := range sortedKeys(object) {
		if _, ok := allowed[key]; !ok {
			findings = append(findings, finding("config."+key, "is not supported by "+profile.Name+"@"+profile.Version))
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
	triggerInput := optionalString(object["trigger_input"], "config.trigger_input", 128, &findings)
	notTriggered := optionalString(object["not_triggered"], "config.not_triggered", 32, &findings)
	defaultDecision := optionalString(object["default_decision"], "config.default_decision", 128, &findings)
	escalations := parseEscalations(object["escalations"], &findings)
	resumeSchemaValue := decisionSchema(options)
	if profile.DecisionSchema == DecisionSchemaConfigured {
		configured, ok := object["decision_schema"].(map[string]any)
		if !ok || configured == nil {
			findings = append(findings, finding("config.decision_schema", "must be a local JSON Schema object"))
		} else {
			resumeSchemaValue = graph.Schema(configured)
		}
	}
	resumeSchema, schemaErr := workflowwait.NewSchemaRef(resumeSchemaValue)
	if schemaErr != nil {
		path := "config.options"
		if profile.DecisionSchema == DecisionSchemaConfigured {
			path = "config.decision_schema"
		}
		findings = append(findings, finding(path, "cannot construct a local decision schema"))
	}
	if profile.DecisionSchema == DecisionSchemaConfigured && schemaErr == nil {
		for index, option := range options {
			if !decisionSchemaAccepts(resumeSchema.Schema, option.ID) {
				findings = append(findings, finding(fmt.Sprintf("config.options[%d]", index), "is not represented by config.decision_schema"))
			}
		}
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
		if triggerInput == "" || notTriggered == "" {
			findings = append(findings, finding("config.blocking", "non-blocking gates require compiler-lowered trigger and disposition fields"))
		}
		if !profileIdentifier(triggerInput) {
			findings = append(findings, finding("config.trigger_input", "must name the compiler-lowered boolean trigger input"))
		}
		if notTriggered != "proceed" && notTriggered != "skip" {
			findings = append(findings, finding("config.not_triggered", "must be proceed or skip"))
		}
		if notTriggered == "proceed" {
			if defaultDecision == "" || !decisionSchemaAccepts(resumeSchema.Schema, defaultDecision) || !configuredOption(options, defaultDecision) {
				findings = append(findings, finding("config.default_decision", "must be a configured option accepted by the decision schema"))
			}
		} else if defaultDecision != "" {
			findings = append(findings, finding("config.default_decision", "is allowed only when not_triggered is proceed"))
		}
	} else if triggerInput != "" || notTriggered != "" || defaultDecision != "" {
		findings = append(findings, finding("config.trigger_input", "trigger fields require optional true and blocking false"))
	}
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].Message < findings[j].Message })
	return parsedConfig{checkpoint: checkpoint, timeout: timeout, triggerInput: triggerInput, notTriggered: notTriggered, defaultDecision: defaultDecision}, findings
}

func configuredOption(options []workflowgate.Option, decision string) bool {
	for _, option := range options {
		if option.ID == decision {
			return true
		}
	}
	return false
}

func parseOptions(value any, findings *[]diagnostic.Diagnostic) []workflowgate.Option {
	raw, ok := value.([]any)
	if !ok || len(raw) == 0 || len(raw) > 128 {
		*findings = append(*findings, finding("config.options", "must contain between 1 and 128 entries"))
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
		if !ok || len(attributeObject) > 32 {
			*findings = append(*findings, finding(path+".attributes", "must be a bounded object of strings"))
		} else {
			total := 0
			for _, key := range sortedKeys(attributeObject) {
				attributes[key] = requiredString(attributeObject[key], path+".attributes."+key, 1024, findings)
				total += len(key) + len(attributes[key])
			}
			if total > 8<<10 {
				*findings = append(*findings, finding(path+".attributes", "exceeds its total byte limit"))
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
	if !ok || len(raw) > 32 {
		*findings = append(*findings, finding("config.escalations", "must be an array of at most 32 entries"))
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

func decisionSchemaAccepts(schema graph.Schema, decision string) bool {
	value, err := values.NewInline(map[string]any{"decision": decision}, values.Metadata{
		Producer: values.Producer{Kind: "gate_config", Reference: "decision_schema"}, MediaType: "application/json",
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	return err == nil && values.ValidateValueSchema(schema, value) == nil
}

func configSchema(profile Profile) graph.Schema {
	subject := map[string]any{
		"type": "object", "additionalProperties": false, "required": []any{"kind", "reference"},
		"properties": map[string]any{
			"kind":       map[string]any{"type": "string", "minLength": json.Number("1")},
			"reference":  map[string]any{"type": "string", "minLength": json.Number("1")},
			"attributes": map[string]any{"type": "object", "maxProperties": json.Number("32"), "additionalProperties": map[string]any{"type": "string", "maxLength": json.Number("1024")}},
		},
	}
	schema := graph.Schema{
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
			"trigger_input":    map[string]any{"type": "string", "minLength": json.Number("1")},
			"not_triggered":    map[string]any{"type": "string", "enum": []any{"proceed", "skip"}},
			"default_decision": map[string]any{"type": "string", "minLength": json.Number("1")},
			"escalations": map[string]any{"type": "array", "maxItems": json.Number("32"), "items": map[string]any{
				"type": "object", "additionalProperties": false, "required": []any{"after", "subject"},
				"properties": map[string]any{"after": map[string]any{"type": "string", "minLength": json.Number("1")}, "subject": subject},
			}},
		},
	}
	if profile.DecisionSchema == DecisionSchemaConfigured {
		schema["required"] = append(schema["required"].([]any), "decision_schema")
		schema["properties"].(map[string]any)["decision_schema"] = map[string]any{"type": "object"}
	}
	return schema
}

func outputSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"decision", "skipped", "resume", "timed_out", "triggered"},
		"properties": map[string]any{
			"decision": map[string]any{"type": "string"}, "skipped": map[string]any{"type": "boolean"},
			"resume": map[string]any{"type": "object"}, "timed_out": map[string]any{"const": false}, "triggered": map[string]any{"type": "boolean"},
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

func profileIdentifier(value string) bool {
	if !stableText(value, 128) {
		return false
	}
	for index, current := range value {
		if current >= 'a' && current <= 'z' || current >= '0' && current <= '9' ||
			index > 0 && (current == '-' || current == '_' || current == '.') {
			continue
		}
		return false
	}
	return true
}

func validateSafeAuthority(authority workflowwait.ResponderAuthority) error {
	if err := authority.Validate(); err != nil {
		return err
	}
	return validateSafeIdentity("authority", authority.Kind, authority.Reference, authority.Attributes)
}

func validateSafeResponder(responder workflowwait.Responder) error {
	if err := responder.Validate(); err != nil {
		return err
	}
	return validateSafeIdentity("responder", responder.Kind, responder.Reference, responder.Attributes)
}

func validateSafeIdentity(label, kind, reference string, attributes map[string]string) error {
	if !profileIdentifier(kind) || !safeGateOpaque(kind, 128) || !safeGateOpaque(reference, 4096) {
		return fmt.Errorf("%s identity is not safe for durable gate metadata", label)
	}
	if len(attributes) > 32 {
		return fmt.Errorf("%s attributes exceed their entry limit", label)
	}
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	total := 0
	for _, key := range keys {
		if !profileIdentifier(key) || !safeGateOpaque(key, 128) || !safeGateOpaque(attributes[key], 1024) {
			return fmt.Errorf("%s attributes are not safe for durable gate metadata", label)
		}
		total += len(key) + len(attributes[key])
		if total > 8<<10 {
			return fmt.Errorf("%s attributes exceed their byte limit", label)
		}
	}
	return nil
}

func safeGateOpaque(value string, maximum int) bool {
	if !stableText(value, maximum) || strings.Contains(value, "://") || strings.ContainsAny(value, "?#@=") {
		return false
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{"secret", "token", "credential", "password", "authorization", "bearer", "cookie", "signature", "api_key", "apikey"} {
		if strings.Contains(lower, marker) {
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
	if len(encoded) > maximumConfigBytes {
		return nil, errors.New("config exceeds its byte limit")
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

func gateWaitID(kind string, identity stepkind.InvocationIdentity, correlation string) string {
	seed := strings.Join([]string{identity.RunID, identity.NodeID, identity.Iteration, fmt.Sprint(identity.Attempt), kind, correlation}, "\x00")
	return "wait-" + strings.TrimPrefix(values.SHA256Digest([]byte(seed)), "sha256:")[:32]
}

func metadata(kind string, identity stepkind.InvocationIdentity, output string) values.Metadata {
	reference := identity.RunID + "/" + identity.NodeID
	if identity.Iteration != "" {
		reference += "/" + identity.Iteration
	}
	reference += fmt.Sprintf("/attempt-%d", identity.Attempt)
	return values.Metadata{Producer: values.Producer{Kind: kind, Reference: reference, Output: output}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
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
