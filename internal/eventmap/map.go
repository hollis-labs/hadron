package eventmap

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hollis-labs/libs/harness/core/adapters/runtimeevents"
	"github.com/hollis-labs/libs/harness/core/eventstage"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
	workflowwait "github.com/hollis-labs/libs/workflow/engine/wait"
)

// Channel is the envelope source channel of every event this package maps.
const Channel runtimeevents.SourceChannel = "workflow_events"

// Full catalog payload schema ids.
const (
	schemaStateChanged = "urn:hollis-labs:events:state_changed@1"
	schemaStepExecuted = "urn:hollis-labs:events:step.executed@1"
	schemaGate         = "urn:hollis-labs:events:gate.evaluated@1"
)

// Kinds exported.
const (
	KindRunStateChanged = "run.state_changed"
	KindStepExecuted    = "step.executed"
	KindGateEvaluated   = "gate.evaluated"
)

// DefaultAllowlist is the strict per-kind payload allowlist the sanitize stage
// applies. The mapper builds payloads from the same fields; this is the second lock.
func DefaultAllowlist() Allowlist {
	return Allowlist{
		KindRunStateChanged: {"/entity/type", "/entity/id", "/from", "/to", "/trigger/kind", "/run_sequence"},
		KindStepExecuted:    {"/step_id", "/outcome", "/node_id", "/iteration", "/attempt", "/failure_code", "/run_sequence"},
		KindGateEvaluated:   {"/gate/id", "/gate/kind", "/subject/type", "/subject/id", "/result", "/blocking", "/run_sequence"},
	}
}

// Mapper implements persistence.OutboxMapper through the eventstage chain.
type Mapper struct {
	chain *eventstage.Chain[runtimeevents.Event]
}

// NewMapper builds the chain: Stamp (Hadron), Sanitize (the allowlist, or another
// PayloadSanitizer in its place), Classify (the embedded rules).
func NewMapper(sanitizer eventstage.PayloadSanitizer) (*Mapper, error) {
	if sanitizer == nil {
		sanitizer = DefaultAllowlist()
	}
	rules, err := eventstage.ParseRules([]byte(classifyRules))
	if err != nil {
		return nil, fmt.Errorf("eventmap: classify rules: %w", err)
	}
	if errs := rules.SelfTest(context.Background()); len(errs) > 0 {
		return nil, fmt.Errorf("eventmap: classify rules fail their own tests: %w", errs[0])
	}
	chain, err := eventstage.NewChain(eventstage.Config[runtimeevents.Event]{
		Stamp:    eventstage.Named("stamp/hadron", stamp),
		Sanitize: eventstage.RuntimeEventsSanitize(sanitizer, eventstage.ModeProducer),
		Classify: eventstage.RuntimeEventsClassify(rules),
	})
	if err != nil {
		return nil, err
	}
	return &Mapper{chain: chain}, nil
}

// stamp only fills what is missing and drops an event that could never be valid.
// It never guesses: Hadron has no session, project or task to stamp, so none is.
func stamp(_ context.Context, e *runtimeevents.Event) (eventstage.Outcome, error) {
	if e.Subject == "" && e.SessionID == "" {
		return eventstage.Outcome{Dropped: true, Note: "no subject"}, nil
	}
	if e.Source.Channel == "" {
		e.Source.Channel = Channel
		return eventstage.Outcome{Changed: true}, nil
	}
	return eventstage.Outcome{}, nil
}

// MapWorkflowEvent implements persistence.OutboxMapper.
func (m *Mapper) MapWorkflowEvent(ctx context.Context, ev workflowruntime.Event, generation, seq uint64) ([]byte, string, bool, error) {
	e, ok := build(ev)
	if !ok {
		return nil, "", false, nil
	}
	e.Sequence, e.Generation = seq, generation
	report := m.chain.Apply(ctx, &e)
	if !report.Emit() {
		return nil, "", false, nil
	}
	if err := e.Validate(); err != nil {
		return nil, "", false, fmt.Errorf("mapped envelope is invalid: %w", err)
	}
	out, err := json.Marshal(e)
	if err != nil {
		return nil, "", false, err
	}
	return out, e.ID, true, nil
}

// build maps one workflow event to an unsanitized envelope, or reports that the
// event is not exported.
func build(ev workflowruntime.Event) (runtimeevents.Event, bool) {
	run := string(ev.RunID)
	base := runtimeevents.Event{
		SchemaVersion: "1", ID: runtimeevents.NewEventID(), Time: ev.OccurredAt.UTC(), Subject: "msg://workflow_run/hadron/" + run,
		Source: runtimeevents.Source{Channel: Channel, Confidence: runtimeevents.ConfidenceDerived},
		Corr:   &runtimeevents.Corr{CorrelationID: run},
	}
	runSequence := ev.Sequence
	switch ev.Type {
	case workflowruntime.EventRunStatusChanged:
		to := ev.Attributes["to_status"]
		if to == "" {
			return base, false
		}
		payload := map[string]any{
			"entity": map[string]string{"type": "workflow_run", "id": run}, "to": to,
			// A status change in the engine is the system moving the run; the event
			// does not say who asked, and Hadron does not guess.
			"trigger": map[string]string{"kind": "system"}, "run_sequence": runSequence,
		}
		if from := ev.Attributes["from_status"]; from != "" {
			payload["from"] = from
		}
		base.Kind = KindRunStateChanged
		base.Class = &runtimeevents.Class{Category: "state", Severity: runSeverity(to), PayloadSchema: schemaStateChanged}
		base.Payload = mustJSON(payload)
	case workflowruntime.EventNodeAttemptFinished:
		if ev.Invocation == nil || ev.Attempt == nil {
			return base, false
		}
		stepID := ev.Invocation.NodeID
		if ev.Invocation.Iteration != "" {
			stepID += "/" + ev.Invocation.Iteration
		}
		outcome := attemptOutcome(ev.Attributes["attempt_status"])
		payload := map[string]any{
			"step_id": stepID, "outcome": outcome, "node_id": ev.Invocation.NodeID, "attempt": ev.Attempt.Number, "run_sequence": runSequence,
		}
		if ev.Invocation.Iteration != "" {
			payload["iteration"] = ev.Invocation.Iteration
		}
		if code := ev.Attributes["failure_code"]; code != "" {
			payload["failure_code"] = code
		}
		base.Kind = KindStepExecuted
		base.Class = &runtimeevents.Class{Category: "step", Severity: stepSeverity(outcome), PayloadSchema: schemaStepExecuted}
		base.Payload = mustJSON(payload)
	case workflowruntime.EventWaitSuspended:
		if ev.Attributes["kind"] != string(workflowwait.KindGate) || ev.Attributes["wait_id"] == "" {
			return base, false
		}
		base.Kind = KindGateEvaluated
		base.Class = &runtimeevents.Class{Category: "state", Severity: "notice", PayloadSchema: schemaGate}
		base.Payload = mustJSON(map[string]any{
			"gate":    map[string]string{"id": ev.Attributes["wait_id"], "kind": "approval"},
			"subject": map[string]string{"type": "workflow_run", "id": run},
			"result":  "blocked", "blocking": true, "run_sequence": runSequence,
		})
	default:
		return base, false
	}
	return base, true
}

func runSeverity(to string) string {
	switch to {
	case string(workflowruntime.RunFailed):
		return "error"
	case string(workflowruntime.RunCanceled):
		return "notice"
	}
	return "info"
}

func attemptOutcome(status string) string {
	switch status {
	case "succeeded":
		return "ok"
	case "canceled", "skipped":
		return "skipped"
	}
	return "failed"
}

func stepSeverity(outcome string) string {
	if outcome == "failed" {
		return "warning"
	}
	return "info"
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic("eventmap: marshal " + strconv.Quote(fmt.Sprintf("%T", v)) + ": " + err.Error())
	}
	return b
}

// classifyRules labels the exported events. The catalog fills the category, and a
// rule may not set one for a cataloged kind.
const classifyRules = `{
  "schema": "eventstage.rules/v1",
  "id": "hadron-workflow-events",
  "version": "2026-10-16.1",
  "rules": [
    {"id": "run-failed", "match": {"kind": ["run.state_changed"], "payload": [{"path": "/to", "equals": "failed"}]}, "set": {"labels": ["run-failed"]}},
    {"id": "run-finished", "match": {"kind": ["run.state_changed"], "payload": [{"path": "/to", "in": ["succeeded", "failed", "canceled"]}]}, "set": {"labels": ["run-terminal"]}},
    {"id": "step-failed", "match": {"kind": ["step.executed"], "payload": [{"path": "/outcome", "equals": "failed"}]}, "set": {"labels": ["step-failed"]}},
    {"id": "gate-blocked", "match": {"kind": ["gate.evaluated"], "payload": [{"path": "/result", "equals": "blocked"}]}, "set": {"labels": ["gate-blocked"]}}
  ],
  "tests": [
    {"name": "failed run", "event": {"kind": "run.state_changed", "payload": {"to": "failed"}}, "want": {"category": "state", "labels": ["run-failed", "run-terminal"]}},
    {"name": "running run", "event": {"kind": "run.state_changed", "payload": {"to": "running"}}, "want": {"category": "state"}},
    {"name": "failed step", "event": {"kind": "step.executed", "payload": {"outcome": "failed"}}, "want": {"category": "step", "labels": ["step-failed"]}},
    {"name": "blocked gate", "event": {"kind": "gate.evaluated", "payload": {"result": "blocked"}}, "want": {"category": "state", "labels": ["gate-blocked"]}}
  ]
}`
