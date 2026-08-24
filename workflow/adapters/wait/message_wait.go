package waitadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type messageConfig struct {
	substrate   string
	to          string
	correlation string
	timeout     time.Duration
	schema      graph.Schema
}

// MessageWait implements message_wait@v1 as a message-arrival resume, never a
// mailbox poll.
type MessageWait struct{ base baseExecutor }

// NewMessageWait constructs a fail-closed message wait executor.
func NewMessageWait(options Options) (*MessageWait, error) {
	base, err := newBase(options)
	if err != nil {
		return nil, err
	}
	return &MessageWait{base: base}, nil
}

// Register registers sleep@v1, wait_for@v1, and message_wait@v1 from the
// caller's perspective unless the supplied registry reports a duplicate after
// the first registration. Registries do not expose rollback; callers should
// register into a fresh registry during host assembly.
func Register(registry stepkind.Registry, options Options) (Registration, error) {
	if nilInterface(registry) {
		return Registration{}, fmt.Errorf("step-kind registry is required")
	}
	waitFor, err := NewWaitFor(options)
	if err != nil {
		return Registration{}, err
	}
	message, err := NewMessageWait(options)
	if err != nil {
		return Registration{}, err
	}
	sleep := NewSleep(options.Now)
	if err := registry.Register(sleep); err != nil {
		return Registration{}, err
	}
	if err := registry.Register(waitFor); err != nil {
		return Registration{}, err
	}
	if err := registry.Register(message); err != nil {
		return Registration{}, err
	}
	return Registration{Sleep: sleep, WaitFor: waitFor, MessageWait: message}, nil
}

// Spec returns immutable message_wait@v1 metadata.
func (*MessageWait) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: MessageWaitName, Version: Version,
		ConfigSchema: messageConfigSchema(), InputSchema: graph.Schema{"type": "object"}, OutputSchema: waitOutputSchema("message"),
		Effects: graph.EffectSet{graph.EffectRead}, RequiredCapabilities: []string{CapabilityWait, CapabilityMessage},
		Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationNone}, CanSuspend: true, EmbeddedModeSupported: false,
	}
}

// ValidateConfig reports deterministic source-addressable config paths.
func (*MessageWait) ValidateConfig(_ context.Context, input graph.Config) []diagnostic.Diagnostic {
	_, findings := parseMessageConfig(input)
	return findings
}

// Execute suspends for a message initially and returns the exact accepted
// message Value plus safe resume metadata after continuation.
func (e *MessageWait) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	config, findings := parseMessageConfig(prepared.Invocation.Config)
	if hasErrors(findings) {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("%s", findings[0].Message))
	}
	if prepared.Invocation.Continuation != nil {
		message, _, err := continuationPayload(prepared.Invocation, MessageWaitName, workflowwait.KindMessage, workflowwait.WakeMessage, config.correlation, config.schema)
		if err != nil {
			return stepkind.StepResult{}, executionError(CodeContinuation, "message wait continuation is invalid", stepkind.RetryPermanent, err)
		}
		outputs, err := completionOutputs(prepared.Invocation.Identity, MessageWaitName, "message", message, prepared.Invocation.Continuation.Record, SourceMessage)
		if err != nil {
			return stepkind.StepResult{}, executionError(CodeContinuation, "message wait outputs are invalid", stepkind.RetryPermanent, err)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
	}
	if e == nil || nilInterface(e.base.authority) || e.base.now == nil {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("message wait executor is not initialized"))
	}
	source := Source{Kind: SourceMessage, Reference: config.to, Attributes: map[string]string{"substrate": config.substrate}}
	authority, err := e.base.authorize(ctx, prepared.Invocation.Identity, source, config.correlation)
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
	record, err := openRecord(workflowwait.KindMessage, workflowwait.WakeMessage, config.correlation, deadline, config.schema, authority)
	if err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepWaiting, Wait: &stepkind.WaitResult{
		ID: waitID(prepared.Invocation.Identity, MessageWaitName, config.correlation), Record: record,
	}}, nil
}

func parseMessageConfig(input graph.Config) (messageConfig, []diagnostic.Diagnostic) {
	object, err := cloneConfig(input)
	if err != nil {
		return messageConfig{}, []diagnostic.Diagnostic{configFinding("config", "must be a JSON-compatible object")}
	}
	var findings []diagnostic.Diagnostic
	validateFields(object, map[string]struct{}{
		"substrate": {}, "to": {}, "correlation": {}, "correlation_id": {}, "timeout": {}, "payload_schema": {},
	}, "config.", &findings)
	if _, old := object["correlation_id"]; old {
		findings = append(findings, configFinding("config.correlation_id", "is legacy vocabulary; use correlation"))
	}
	parsed := messageConfig{
		substrate:   requiredString(object["substrate"], "config.substrate", &findings),
		to:          requiredString(object["to"], "config.to", &findings),
		correlation: requiredString(object["correlation"], "config.correlation", &findings),
		timeout:     parseDuration(object["timeout"], "config.timeout", true, &findings),
		schema:      parseSchema(object["payload_schema"], "config.payload_schema", &findings),
	}
	if parsed.to != "" {
		parsedURL, parseErr := url.Parse(parsed.to)
		if parseErr != nil || parsedURL.Scheme == "" || parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.Fragment != "" || strings.Contains(parsed.to, "{{") {
			findings = append(findings, configFinding("config.to", "must be a static canonical address URI without credentials, query, or fragment"))
		}
	}
	sortFindings(findings)
	return parsed, findings
}

func messageConfigSchema() graph.Schema {
	return graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []any{"substrate", "to", "correlation", "timeout"},
		"properties": map[string]any{
			"substrate":      map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"to":             map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"correlation":    map[string]any{"type": "string", "minLength": json.Number("1"), "maxLength": json.Number("4096")},
			"timeout":        map[string]any{"type": "string", "minLength": json.Number("1")},
			"payload_schema": map[string]any{"type": "object"},
		},
	}
}
