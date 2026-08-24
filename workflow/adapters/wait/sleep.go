package waitadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

// Sleep implements sleep@v1 as a durable successful timer. WakeAt is never a
// timeout Deadline; runtime schedules a distinct wait_wake activation.
type Sleep struct{ now func() time.Time }

// NewSleep constructs a timer executor. A nil clock uses UTC wall time.
func NewSleep(now func() time.Time) *Sleep {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Sleep{now: now}
}

// Spec returns immutable sleep@v1 metadata.
func (*Sleep) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: SleepName, Version: Version,
		ConfigSchema: graph.Schema{
			"type": "object", "additionalProperties": false, "required": []any{"duration"},
			"properties": map[string]any{"duration": map[string]any{"type": "string", "minLength": json.Number("1")}},
		},
		InputSchema: graph.Schema{"type": "object"}, OutputSchema: graph.Schema{
			"type": "object", "additionalProperties": false, "required": []any{"woke_at", "resume", "timed_out"},
			"properties": map[string]any{
				"woke_at": map[string]any{"type": "string"}, "resume": map[string]any{"type": "object"}, "timed_out": map[string]any{"const": false},
			},
		},
		Effects: graph.EffectSet{graph.EffectRead}, RequiredCapabilities: []string{CapabilityWait},
		Idempotency: graph.IdempotencyIntrinsic, RetrySafety: stepkind.RetrySafe,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationContext},
		Observation:  stepkind.ObservationSpec{Mode: stepkind.ObservationNone}, CanSuspend: true, EmbeddedModeSupported: true,
	}
}

// ValidateConfig reports a deterministic duration diagnostic.
func (*Sleep) ValidateConfig(_ context.Context, input graph.Config) []diagnostic.Diagnostic {
	_, findings := parseSleepConfig(input)
	return findings
}

// Execute returns StepWaiting initially and completes only from an authorized
// successful timer continuation.
func (e *Sleep) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if ctx == nil {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("context is required"))
	}
	if err := ctx.Err(); err != nil {
		return stepkind.StepResult{}, err
	}
	if err := prepared.Invocation.Validate(); err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	duration, findings := parseSleepConfig(prepared.Invocation.Config)
	if len(findings) != 0 {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("%s", findings[0].Message))
	}
	correlation := "timer:" + waitID(prepared.Invocation.Identity, SleepName, duration.String())
	resumeSchema := graph.Schema{
		"type": "object", "additionalProperties": false, "required": []any{"woke_at"},
		"properties": map[string]any{"woke_at": map[string]any{"type": "string"}},
	}
	if prepared.Invocation.Continuation != nil {
		payload, resolution, err := continuationPayload(prepared.Invocation, SleepName, workflowwait.KindTimer, workflowwait.WakeTimer, correlation, resumeSchema)
		if err != nil {
			return stepkind.StepResult{}, executionError(CodeContinuation, "sleep continuation is invalid", stepkind.RetryPermanent, err)
		}
		record := prepared.Invocation.Continuation.Record
		if record.WakeAt.IsZero() || resolution.Source != workflowwait.WakeTimer || resolution.Responder.Kind != "system" || resolution.Responder.Reference != "wait-timer" || !resolution.ResolvedAt.Equal(record.WakeAt) {
			return stepkind.StepResult{}, executionError(CodeContinuation, "sleep continuation provenance is invalid", stepkind.RetryPermanent, fmt.Errorf("successful timer requires exact wake_at and runtime timer provenance"))
		}
		object, ok := payload.Inline.(map[string]any)
		if !ok {
			return stepkind.StepResult{}, executionError(CodeContinuation, "sleep continuation payload is invalid", stepkind.RetryPermanent, fmt.Errorf("timer payload must be an object"))
		}
		wokeAt, ok := object["woke_at"].(string)
		if !ok || wokeAt != record.WakeAt.UTC().Format(time.RFC3339Nano) {
			return stepkind.StepResult{}, executionError(CodeContinuation, "sleep continuation payload is invalid", stepkind.RetryPermanent, fmt.Errorf("woke_at must exactly match the durable timer wake_at"))
		}
		outputs, err := completionOutputs(prepared.Invocation.Identity, SleepName, "", payload, record, SourceTimer)
		if err != nil {
			return stepkind.StepResult{}, executionError(CodeContinuation, "sleep outputs are invalid", stepkind.RetryPermanent, err)
		}
		outputs["woke_at"], err = values.NewInline(wokeAt, outputMetadata(prepared.Invocation.Identity, SleepName, "woke_at"))
		if err != nil {
			return stepkind.StepResult{}, executionError(CodeContinuation, "sleep outputs are invalid", stepkind.RetryPermanent, err)
		}
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: outputs}, nil
	}
	if e == nil || e.now == nil {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("sleep executor is not initialized"))
	}
	current := e.now()
	if current.IsZero() {
		return stepkind.StepResult{}, invalidInvocation(fmt.Errorf("sleep clock returned zero time"))
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return stepkind.StepResult{}, contextErr
	}
	wakeAt := current.UTC().Add(duration)
	record, err := openRecord(workflowwait.KindTimer, workflowwait.WakeTimer, correlation, time.Time{}, resumeSchema, workflowwait.ResponderAuthority{Kind: "system_timer", Reference: "runtime"})
	if err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	record.WakeAt = wakeAt
	if err := record.Validate(); err != nil {
		return stepkind.StepResult{}, invalidInvocation(err)
	}
	return stepkind.StepResult{Outcome: stepkind.StepWaiting, Wait: &stepkind.WaitResult{
		ID: waitID(prepared.Invocation.Identity, SleepName, correlation), Record: record,
	}}, nil
}

func parseSleepConfig(input graph.Config) (time.Duration, []diagnostic.Diagnostic) {
	object, err := cloneConfig(input)
	if err != nil {
		return 0, []diagnostic.Diagnostic{configFinding("config", "must be a JSON-compatible object")}
	}
	var findings []diagnostic.Diagnostic
	validateFields(object, map[string]struct{}{"duration": {}}, "config.", &findings)
	duration := parseDuration(object["duration"], "config.duration", true, &findings)
	sortFindings(findings)
	return duration, findings
}
