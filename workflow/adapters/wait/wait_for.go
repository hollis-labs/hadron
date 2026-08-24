package waitadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type waitForConfig struct {
	source        Source
	correlation   string
	timeout       time.Duration
	schema        graph.Schema
	callbackPath  string
	childRunInput string
	failChildRun  bool
}

// WaitFor implements wait_for@v1 for signals, callbacks, events, and child-run
// completion. Event uses the canonical signal wake path with durable event
// metadata; no source is polled.
type WaitFor struct{ base baseExecutor }

// NewWaitFor constructs a fail-closed external wait executor.
func NewWaitFor(options Options) (*WaitFor, error) {
	base, err := newBase(options)
	if err != nil {
		return nil, err
	}
	return &WaitFor{base: base}, nil
}

// Spec returns immutable wait_for@v1 metadata.
func (*WaitFor) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: WaitForName, Version: Version,
		ConfigSchema: waitForConfigSchema(), InputSchema: graph.Schema{"type": "object"}, OutputSchema: waitOutputSchema("payload"),
		Effects: graph.EffectSet{graph.EffectRead}, RequiredCapabilities: []string{CapabilityWait},
		Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationNone}, CanSuspend: true,
	}
}

// ValidateConfig reports deterministic source-addressable config paths.
func (*WaitFor) ValidateConfig(_ context.Context, config graph.Config) []diagnostic.Diagnostic {
	_, findings := parseWaitForConfig(config)
	return findings
}

// Execute returns a generic wait handoff initially and typed outputs only after
// the runtime supplies an accepted durable continuation.
func (e *WaitFor) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	config, findings := parseWaitForConfig(prepared.Invocation.Config)
	if hasErrors(findings) {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("%s", findings[0].Message))
	}
	config, err := resolveChildRunInput(config, prepared.Invocation.Inputs)
	if err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	wake := config.source.wakeSource()
	kind := config.source.waitKind()
	resumeSchema := config.schema
	if config.failChildRun {
		resumeSchema = childRunTerminalSchema(config.schema)
	}
	if prepared.Invocation.Continuation != nil {
		payload, _, continuationErr := continuationPayload(prepared.Invocation, WaitForName, kind, wake, config.correlation, resumeSchema)
		if continuationErr != nil {
			return stepkind.StepResult{}, executionError(CodeContinuation, "wait continuation is invalid", stepkind.RetryPermanent, continuationErr)
		}
		if config.failChildRun {
			payload, continuationErr = childRunSuccessPayload(payload)
			if continuationErr != nil {
				var terminal *childRunTerminalError
				if errors.As(continuationErr, &terminal) {
					classification := stepkind.RetryPermanent
					if terminal.retryable {
						classification = stepkind.Retryable
					}
					return stepkind.StepResult{}, executionError(CodeChildRunFailed, "child run completed unsuccessfully", classification, terminal)
				}
				return stepkind.StepResult{}, executionError(CodeContinuation, "child-run continuation is invalid", stepkind.RetryPermanent, continuationErr)
			}
		}
		outputs, outputErr := completionOutputs(prepared.Invocation.Identity, WaitForName, "payload", payload, prepared.Invocation.Continuation.Record, config.source.Kind)
		if outputErr != nil {
			return stepkind.StepResult{}, executionError(CodeContinuation, "wait continuation outputs are invalid", stepkind.RetryPermanent, outputErr)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
	}
	if e == nil || nilInterface(e.base.authority) || e.base.now == nil {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("wait executor is not initialized"))
	}
	authority, err := e.base.authorize(ctx, prepared.Invocation.Identity, config.source, config.correlation)
	if err != nil {
		return stepkind.StepResult{}, err
	}
	deadline, err := deadline(e.base.now, config.timeout)
	if err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.StepResult{}, contextErr
	}
	record, err := openRecord(kind, wake, config.correlation, deadline, resumeSchema, authority)
	if err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	resumeToken := ""
	if config.source.Kind == SourceCallback {
		if nilInterface(e.base.callbacks) {
			return stepkind.StepResult{}, executionError(CodeCallbackFailed, "callback wait materialization is unavailable", stepkind.RetryPermanent, fmt.Errorf("callback issuer is required"))
		}
		credential, issueErr := e.base.callbacks.IssueCallback(ctx, CallbackRequest{
			Identity: prepared.Invocation.Identity, Path: config.callbackPath, Correlation: config.correlation, ExpiresAt: deadline,
			IdempotencyKey: callbackIdempotencyKey(prepared.Invocation.Identity, config.callbackPath, config.correlation),
		})
		if issueErr != nil {
			return stepkind.StepResult{}, contextOr(ctx, CodeCallbackFailed, "callback wait materialization failed", stepkind.Retryable, issueErr)
		}
		if contextErr := ctx.Err(); contextErr != nil {
			credential.Token = ""
			return stepkind.StepResult{}, contextErr
		}
		urlValue, digest, credentialErr := validateCallbackCredential(credential, config.callbackPath)
		if credentialErr != nil {
			return stepkind.StepResult{}, executionError(CodeCallbackFailed, "callback credential is invalid", stepkind.RetryPermanent, credentialErr)
		}
		record.ResumeURL, record.ResumeTokenDigest, resumeToken = urlValue, digest, credential.Token
		if err := record.Validate(); err != nil {
			return stepkind.StepResult{}, executionError(CodeCallbackFailed, "callback wait record is invalid", stepkind.RetryPermanent, err)
		}
	}
	return stepkind.StepResult{Outcome: stepkind.StepWaiting, Wait: &stepkind.WaitResult{
		ID: waitID(prepared.Invocation.Identity, WaitForName, config.correlation), Record: record, ResumeToken: resumeToken,
	}}, nil
}

func callbackIdempotencyKey(identity stepkind.InvocationIdentity, path, correlation string) string {
	return waitID(identity, WaitForName+"-callback", path+"\x00"+correlation)
}

func parseWaitForConfig(input graph.Config) (waitForConfig, []diagnostic.Diagnostic) {
	object, err := cloneConfig(input)
	if err != nil {
		return waitForConfig{}, []diagnostic.Diagnostic{configFinding("config", "must be a JSON-compatible object")}
	}
	var findings []diagnostic.Diagnostic
	validateFields(object, map[string]struct{}{
		"signal": {}, "event": {}, "callback": {}, "child_run": {}, "correlation": {}, "timeout": {}, "payload_schema": {},
	}, "config.", &findings)
	configured := make([]SourceKind, 0, 1)
	for _, candidate := range []struct {
		name string
		kind SourceKind
	}{{"signal", SourceSignal}, {"event", SourceEvent}, {"callback", SourceCallback}, {"child_run", SourceChildRun}} {
		if _, ok := object[candidate.name]; ok {
			configured = append(configured, candidate.kind)
		}
	}
	if len(configured) != 1 {
		findings = append(findings, configFinding("config", "must declare exactly one of signal, event, callback, or child_run"))
	}
	parsed := waitForConfig{
		correlation: optionalString(object["correlation"], "config.correlation", &findings),
		timeout:     parseDuration(object["timeout"], "config.timeout", true, &findings),
		schema:      parseSchema(object["payload_schema"], "config.payload_schema", &findings),
	}
	if len(configured) == 1 {
		parsed.source.Kind = configured[0]
		switch configured[0] {
		case SourceSignal:
			parsed.source.Reference = requiredString(object["signal"], "config.signal", &findings)
		case SourceEvent:
			parsed.source = parseEvent(object["event"], &findings)
		case SourceCallback:
			parsed.source, parsed.callbackPath = parseCallback(object["callback"], &findings)
		case SourceChildRun:
			parsed.source, parsed.childRunInput, parsed.failChildRun = parseChildRun(object["child_run"], &findings)
		case SourceMessage, SourceTimer:
			findings = append(findings, configFinding("config", "contains an unsupported wait_for source"))
		}
	}
	if parsed.correlation == "" && parsed.source.Reference != "" {
		parsed.correlation = parsed.source.Reference
	}
	if (parsed.correlation == "" && parsed.childRunInput == "") || (parsed.correlation != "" && !stableText(parsed.correlation, 4096)) {
		findings = append(findings, configFinding("config.correlation", "is required and must be stable UTF-8"))
	}
	sortFindings(findings)
	return parsed, findings
}

func parseEvent(value any, findings *[]diagnostic.Diagnostic) Source {
	object, ok := value.(map[string]any)
	if !ok {
		*findings = append(*findings, configFinding("config.event", "must be an object"))
		return Source{Kind: SourceEvent}
	}
	validateFields(object, map[string]struct{}{"type": {}, "source": {}}, "config.event.", findings)
	reference := requiredString(object["type"], "config.event.type", findings)
	source := optionalString(object["source"], "config.event.source", findings)
	attributes := map[string]string{}
	if source != "" {
		attributes["source"] = source
	}
	return Source{Kind: SourceEvent, Reference: reference, Attributes: attributes}
}

func parseCallback(value any, findings *[]diagnostic.Diagnostic) (Source, string) {
	object, ok := value.(map[string]any)
	if !ok {
		*findings = append(*findings, configFinding("config.callback", "must be an object"))
		return Source{Kind: SourceCallback}, ""
	}
	validateFields(object, map[string]struct{}{"path": {}}, "config.callback.", findings)
	path := requiredString(object["path"], "config.callback.path", findings)
	if path != "" {
		if err := validateRootPath(path); err != nil {
			*findings = append(*findings, configFinding("config.callback.path", err.Error()))
		}
	}
	return Source{Kind: SourceCallback, Reference: path}, path
}

func parseChildRun(value any, findings *[]diagnostic.Diagnostic) (Source, string, bool) {
	object, ok := value.(map[string]any)
	if !ok {
		*findings = append(*findings, configFinding("config.child_run", "must be an object"))
		return Source{Kind: SourceChildRun}, "", false
	}
	validateFields(object, map[string]struct{}{"run_id": {}, "input": {}, "fail_on_unsuccessful": {}}, "config.child_run.", findings)
	failOnUnsuccessful := false
	if raw, exists := object["fail_on_unsuccessful"]; exists {
		var valid bool
		failOnUnsuccessful, valid = raw.(bool)
		if !valid {
			*findings = append(*findings, configFinding("config.child_run.fail_on_unsuccessful", "must be a boolean"))
		}
	}
	_, hasRunID := object["run_id"]
	_, hasInput := object["input"]
	if hasRunID == hasInput {
		*findings = append(*findings, configFinding("config.child_run", "must declare exactly one of run_id or input"))
		return Source{Kind: SourceChildRun}, "", failOnUnsuccessful
	}
	if hasRunID {
		return Source{Kind: SourceChildRun, Reference: requiredString(object["run_id"], "config.child_run.run_id", findings)}, "", failOnUnsuccessful
	}
	input := requiredString(object["input"], "config.child_run.input", findings)
	if input != "" {
		if err := graph.ValidateID(input); err != nil {
			*findings = append(*findings, configFinding("config.child_run.input", "must name a normalized invocation input"))
		}
	}
	return Source{Kind: SourceChildRun}, input, failOnUnsuccessful
}

func resolveChildRunInput(config waitForConfig, inputs values.ValueSet) (waitForConfig, error) {
	if config.childRunInput == "" {
		return config, nil
	}
	value, ok := inputs[config.childRunInput]
	if !ok {
		return waitForConfig{}, fmt.Errorf("child-run input %q is missing", config.childRunInput)
	}
	if err := values.ValidatePersistable(value); err != nil {
		return waitForConfig{}, fmt.Errorf("child-run input %q is not persistable", config.childRunInput)
	}
	if value.Redaction == values.RedactionSecret || value.Artifact != nil || value.SecretRef != nil || value.Type != values.TypeString {
		return waitForConfig{}, fmt.Errorf("child-run input %q must be an inline non-secret string", config.childRunInput)
	}
	runID, ok := value.Inline.(string)
	if !ok || !stableText(runID, 4096) {
		return waitForConfig{}, fmt.Errorf("child-run input %q must contain a stable run ID", config.childRunInput)
	}
	config.source.Reference = runID
	if config.correlation == "" {
		config.correlation = runID
	}
	return config, nil
}

func (e ChildRunTerminalEnvelope) Validate() error {
	switch e.Status {
	case ChildRunSucceeded:
		if e.Outputs == nil || e.Failure != nil {
			return fmt.Errorf("succeeded child run requires only outputs")
		}
	case ChildRunFailed, ChildRunCanceled, ChildRunTimedOut, ChildRunCrashed:
		if e.Outputs != nil || e.Failure == nil || !stableText(e.Failure.Code, 128) || !stableText(e.Failure.Message, 4096) {
			return fmt.Errorf("unsuccessful child run requires only safe failure metadata")
		}
	default:
		return fmt.Errorf("unsupported child-run terminal status %q", e.Status)
	}
	return nil
}

type childRunTerminalError struct {
	status    ChildRunTerminalStatus
	code      string
	retryable bool
}

func (e *childRunTerminalError) Error() string {
	return fmt.Sprintf("child run ended with status %s (%s)", e.status, e.code)
}

func childRunSuccessPayload(payload values.Value) (values.Value, error) {
	if payload.Artifact != nil || payload.SecretRef != nil || payload.Type != values.TypeObject {
		return values.Value{}, fmt.Errorf("child-run terminal payload must be an inline object")
	}
	encoded, err := json.Marshal(payload.Inline)
	if err != nil {
		return values.Value{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var terminal ChildRunTerminalEnvelope
	if err := decoder.Decode(&terminal); err != nil {
		return values.Value{}, fmt.Errorf("decode child-run terminal payload: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return values.Value{}, fmt.Errorf("child-run terminal payload contains trailing JSON")
	}
	if err := terminal.Validate(); err != nil {
		return values.Value{}, err
	}
	if terminal.Status != ChildRunSucceeded {
		return values.Value{}, &childRunTerminalError{status: terminal.Status, code: terminal.Failure.Code, retryable: terminal.Failure.Retryable}
	}
	return values.NewInline(terminal.Outputs, values.Metadata{
		Producer: payload.Producer, MediaType: payload.MediaType,
		Redaction: payload.Redaction, Retention: payload.Retention,
	})
}

func childRunTerminalSchema(outputs graph.Schema) graph.Schema {
	failure := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []any{"code", "message"},
		"properties": map[string]any{
			"code":      map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("128")},
			"message":   map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"retryable": map[string]any{"type": "boolean"},
		},
	}
	success := map[string]any{
		"type": "object", "additionalProperties": false, "required": []any{"status", "outputs"},
		"properties": map[string]any{
			"status": map[string]any{"const": string(ChildRunSucceeded)}, "outputs": outputs,
		},
	}
	unsuccessful := map[string]any{
		"type": "object", "additionalProperties": false, "required": []any{"status", "failure"},
		"properties": map[string]any{
			"status":  map[string]any{"enum": []any{string(ChildRunFailed), string(ChildRunCanceled), string(ChildRunTimedOut), string(ChildRunCrashed)}},
			"failure": failure,
		},
	}
	return graph.Schema{"oneOf": []any{success, unsuccessful}}
}

func (s Source) wakeSource() workflowwait.WakeSource {
	switch s.Kind {
	case SourceCallback:
		return workflowwait.WakeCallback
	case SourceChildRun:
		return workflowwait.WakeChildRun
	default:
		return workflowwait.WakeSignal
	}
}

func (s Source) waitKind() workflowwait.Kind {
	switch s.Kind {
	case SourceCallback:
		return workflowwait.KindCallback
	case SourceChildRun:
		return workflowwait.KindChildRun
	default:
		return workflowwait.KindSignal
	}
}

func waitForConfigSchema() graph.Schema {
	sourceObject := func(properties map[string]any, required ...any) map[string]any {
		return map[string]any{"type": "object", "additionalProperties": false, "required": required, "properties": properties}
	}
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"timeout"},
		"oneOf": []any{
			map[string]any{"required": []any{"signal"}}, map[string]any{"required": []any{"event"}},
			map[string]any{"required": []any{"callback"}}, map[string]any{"required": []any{"child_run"}},
		},
		"properties": map[string]any{
			"signal": map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"event": sourceObject(map[string]any{
				"type":   map[string]any{"type": "string", "minLength": json.Number("1")},
				"source": map[string]any{"type": "string", "minLength": json.Number("1")},
			}, "type"),
			"callback": sourceObject(map[string]any{"path": map[string]any{"type": "string", "pattern": `^/[^?#]*$`}}, "path"),
			"child_run": map[string]any{
				"type": "object", "additionalProperties": false,
				"oneOf": []any{map[string]any{"required": []any{"run_id"}}, map[string]any{"required": []any{"input"}}},
				"properties": map[string]any{
					"run_id":               map[string]any{"type": "string", "minLength": json.Number("1")},
					"input":                map[string]any{"type": "string", "minLength": json.Number("1")},
					"fail_on_unsuccessful": map[string]any{"type": "boolean"},
				},
			},
			"correlation":    map[string]any{"type": "string", "minLength": json.Number("1")},
			"timeout":        map[string]any{"type": "string", "minLength": json.Number("1")},
			"payload_schema": map[string]any{"type": "object"},
		},
	}
}

func waitOutputSchema(payloadName string) graph.Schema {
	properties := map[string]any{
		"resume":    map[string]any{"type": "object"},
		"timed_out": map[string]any{"const": false},
	}
	required := []any{"resume", "timed_out"}
	if payloadName != "" {
		properties[payloadName] = map[string]any{}
		required = append(required, payloadName)
	}
	return graph.Schema{"type": "object", "additionalProperties": false, "required": required, "properties": properties}
}
