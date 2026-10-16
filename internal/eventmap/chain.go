package eventmap

// This file is the whole eventstage wiring of the Hadron producer, kept small on
// purpose: Tether and the other producers copy it. The chain runs at the one choke
// point (the transaction that appends the event): Stamp, Sanitize, Classify, then the
// caller persists the envelope.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hollis-labs/libs/harness/core/adapters/runtimeevents"
	"github.com/hollis-labs/libs/harness/core/eventstage"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

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
		// A stage dropped an event that build selected: that is a failure to
		// record it, not a choice not to, so it is reported rather than hidden.
		return nil, "", false, &SkipError{Class: "dropped", Err: report.Err()}
	}
	if err := e.Validate(); err != nil {
		return nil, "", false, &SkipError{Class: "invalid_envelope", Err: err}
	}
	out, err := json.Marshal(e)
	if err != nil {
		return nil, "", false, &SkipError{Class: "marshal", Err: err}
	}
	return out, e.ID, true, nil
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
    {"id": "gate-blocked", "match": {"kind": ["gate.evaluated"], "payload": [{"path": "/result", "equals": "blocked"}]}, "set": {"labels": ["gate-blocked"]}}
  ],
  "tests": [
    {"name": "failed run", "event": {"kind": "run.state_changed", "payload": {"to": "failed"}}, "want": {"category": "state", "labels": ["run-failed", "run-terminal"]}},
    {"name": "running run", "event": {"kind": "run.state_changed", "payload": {"to": "running"}}, "want": {"category": "state"}},
    {"name": "blocked gate", "event": {"kind": "gate.evaluated", "payload": {"result": "blocked"}}, "want": {"category": "state", "labels": ["gate-blocked"]}}
  ]
}`

// SkipError is a mapping failure with a class the outbox records. The workflow
// event it was mapped from is not affected: recording never rejects a mutation.
type SkipError struct {
	Class string
	Err   error
}

func (e *SkipError) Error() string     { return "eventmap: " + e.Class + ": " + fmt.Sprint(e.Err) }
func (e *SkipError) Unwrap() error     { return e.Err }
func (e *SkipError) SkipClass() string { return e.Class }
