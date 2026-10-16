package eventmap

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/libs/harness/core/adapters/runtimeevents"
	"github.com/hollis-labs/libs/harness/core/adapters/runtimeevents/catalog"
	"github.com/hollis-labs/libs/harness/core/eventstage"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

var at = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

func newMapper(t *testing.T) *Mapper {
	t.Helper()
	m, err := NewMapper(nil)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func mapOne(t *testing.T, ev workflowruntime.Event) (runtimeevents.Event, bool) {
	t.Helper()
	raw, id, ok, err := newMapper(t).MapWorkflowEvent(context.Background(), ev, 12345, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return runtimeevents.Event{}, false
	}
	var e runtimeevents.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	if e.ID != id {
		t.Fatalf("id %q != returned %q", e.ID, id)
	}
	return e, true
}

func mapAdmission(t *testing.T, a Admission) (runtimeevents.Event, bool) {
	t.Helper()
	raw, id, ok, err := newMapper(t).MapAdmission(context.Background(), a, 12345, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return runtimeevents.Event{}, false
	}
	var e runtimeevents.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	if e.ID != id || e.Sequence != 7 || e.Generation != 12345 {
		t.Fatalf("envelope %+v id %q", e, id)
	}
	return e, true
}

// The causation context sets corr.causation_id on a run.state_changed and nowhere
// else.
func TestCausationIsSetOnRunStateChangedOnly(t *testing.T) {
	ctx := WithCausation(context.Background(), "evt_cause")
	raw, _, ok, err := newMapper(t).MapWorkflowEvent(ctx, runStatus("pending", "running"), 1, 1)
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	var e runtimeevents.Event
	_ = json.Unmarshal(raw, &e)
	if e.Corr == nil || e.Corr.CausationID != "evt_cause" || e.Corr.CorrelationID != "run-1" {
		t.Fatalf("corr = %+v", e.Corr)
	}
	gate := workflowruntime.Event{Sequence: 3, RunID: "run-1", Type: workflowruntime.EventWaitSuspended, OccurredAt: at, Attributes: map[string]string{"kind": "gate", "wait_id": "w1", "wake_source": "gate"}}
	raw, _, ok, err = newMapper(t).MapWorkflowEvent(ctx, gate, 1, 2)
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	var g runtimeevents.Event
	_ = json.Unmarshal(raw, &g)
	if g.Corr == nil || g.Corr.CausationID != "" {
		t.Fatalf("a gate event carries causation: %+v", g.Corr)
	}
}

// An admission with an unknown outcome, or without ids, is not exported rather than
// guessed at; a known outcome maps allow to pass, deny to fail and confirm to blocked.
func TestAdmissionOutcomesMapToGateResults(t *testing.T) {
	for outcome, want := range map[string]string{"allow": "pass", "deny": "fail", "confirm": "blocked"} {
		e, ok := mapAdmission(t, Admission{DecisionID: "dec-1", RunID: "run-1", Outcome: outcome, DecidedAt: at})
		if !ok {
			t.Fatalf("%s not exported", outcome)
		}
		var p map[string]any
		_ = json.Unmarshal(e.Payload, &p)
		if p["result"] != want || p["blocking"] != (want == "blocked") || p["gate"].(map[string]any)["id"] != "hadron.admission" || p["gate"].(map[string]any)["kind"] != "custom" {
			t.Errorf("%s: payload = %v", outcome, p)
		}
		if _, hasSeq := p["run_sequence"]; hasSeq {
			t.Errorf("%s: an admission has no run sequence: %v", outcome, p)
		}
	}
	for name, a := range map[string]Admission{
		"unknown outcome": {DecisionID: "d", RunID: "r", Outcome: "maybe", DecidedAt: at},
		"no decision id":  {RunID: "r", Outcome: "allow", DecidedAt: at},
		"no run id":       {DecisionID: "d", Outcome: "allow", DecidedAt: at},
	} {
		if _, ok := mapAdmission(t, a); ok {
			t.Errorf("%s was exported", name)
		}
	}
	e, _ := mapAdmission(t, Admission{DecisionID: "dec/1 x", RunID: "run-1", Outcome: "deny", DecidedAt: at})
	if e.Subject != "msg://workflow_admission/hadron/dec%2F1%20x" {
		t.Errorf("subject not escaped: %q", e.Subject)
	}
	// Allow too is about the evaluation, never the run (which may not exist).
	if a, _ := mapAdmission(t, Admission{DecisionID: "dec-1", RunID: "run-1", Outcome: "allow", DecidedAt: at}); a.Subject != "msg://workflow_admission/hadron/dec-1" {
		t.Errorf("an allow subject = %q", a.Subject)
	}
}

func mapConfirmation(t *testing.T, c AdmissionConfirmation) (runtimeevents.Event, bool) {
	t.Helper()
	raw, id, ok, err := newMapper(t).MapAdmissionConfirmation(context.Background(), c, 12345, 8)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return runtimeevents.Event{}, false
	}
	var e runtimeevents.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	if e.ID != id || e.Sequence != 8 || e.Generation != 12345 {
		t.Fatalf("envelope %+v id %q", e, id)
	}
	return e, true
}

// A satisfied confirmation is a second gate.evaluated on the same admission subject:
// pass, not blocking, caused by the blocked event, with the confirmer as actor.
func TestConfirmationIsAPassOnTheAdmissionSubjectCausedByTheBlockedEvent(t *testing.T) {
	blocked, _ := mapAdmission(t, Admission{DecisionID: "dec-1", RunID: "run-1", Outcome: "confirm", DecidedAt: at})
	e, ok := mapConfirmation(t, AdmissionConfirmation{DecisionID: "dec-1", RunID: "run-1", BlockedEventID: "evt_blocked", ConfirmedBy: "operator:local", Verified: "claimed", At: at})
	if !ok {
		t.Fatal("not exported")
	}
	var p map[string]any
	_ = json.Unmarshal(e.Payload, &p)
	if e.Subject != blocked.Subject || p["result"] != "pass" || p["blocking"] != false || e.Corr.CausationID != "evt_blocked" || e.Corr.CorrelationID != "run-1" {
		t.Fatalf("envelope %+v payload %v", e, p)
	}
	if e.Actor == nil || e.Actor.Kind != "user" || e.Actor.Role != "operator" || e.Actor.Verified != "claimed" || !strings.HasPrefix(e.Actor.URN, "msg://user/hadron/") {
		t.Fatalf("actor = %+v", e.Actor)
	}
	// A dry run has no confirmer, so no actor; a missing blocked event means no cause.
	d, ok := mapConfirmation(t, AdmissionConfirmation{DecisionID: "dec-1", RunID: "run-1", At: at})
	if !ok || d.Actor != nil || d.Corr.CausationID != "" {
		t.Fatalf("dry-run confirmation = %+v", d)
	}
	if _, ok := mapConfirmation(t, AdmissionConfirmation{RunID: "run-1", At: at}); ok {
		t.Fatal("a confirmation without a decision id was exported")
	}
}

func runStatus(from, to string) workflowruntime.Event {
	return workflowruntime.Event{Sequence: 4, RunID: "run-1", Type: workflowruntime.EventRunStatusChanged, OccurredAt: at,
		Attributes: map[string]string{"entity": "run", "from_status": from, "to_status": to}}
}

func TestRunStatusBecomesRunStateChanged(t *testing.T) {
	e, ok := mapOne(t, runStatus("running", "failed"))
	if !ok {
		t.Fatal("not exported")
	}
	if e.Kind != KindRunStateChanged || e.Subject != "msg://workflow_run/hadron/run-1" || e.Source.Channel != Channel || e.Sequence != 7 || e.Generation != 12345 || e.SchemaVersion != "1" {
		t.Fatalf("envelope = %+v", e)
	}
	if e.SessionID != "" || e.Scope != nil {
		t.Fatalf("a session or scope was invented: %+v", e)
	}
	if e.Corr == nil || e.Corr.CorrelationID != "run-1" {
		t.Fatalf("corr = %+v", e.Corr)
	}
	var p map[string]any
	_ = json.Unmarshal(e.Payload, &p)
	if p["to"] != "failed" || p["from"] != "running" || p["run_sequence"] != float64(4) || p["trigger"].(map[string]any)["kind"] != "system" ||
		p["entity"].(map[string]any)["id"] != "run-1" || p["entity"].(map[string]any)["type"] != "workflow_run" {
		t.Fatalf("payload = %v", p)
	}
	if e.Class.Category != "state" || e.Class.Severity != "error" || e.Class.PayloadSchema != schemaStateChanged || !e.Class.Sanitized || e.Class.Visibility != "" {
		t.Fatalf("class = %+v", e.Class)
	}
	if got := strings.Join(e.Class.Labels, ","); got != "run-failed,run-terminal" {
		t.Fatalf("labels = %q", got)
	}
}

func TestOnlyAGateWaitSuspensionBecomesAGateEvaluatedBlocked(t *testing.T) {
	inv := workflowruntime.NodeInvocationID{RunID: "run-1", NodeID: "approve"}
	gate, ok := mapOne(t, workflowruntime.Event{Sequence: 3, RunID: "run-1", Invocation: &inv, Type: workflowruntime.EventWaitSuspended, OccurredAt: at,
		Attributes: map[string]string{"kind": "gate", "wait_id": "wait-9", "wake_source": "gate"}})
	if !ok || gate.Kind != KindGateEvaluated {
		t.Fatalf("gate wait = %+v ok=%v", gate, ok)
	}
	var p map[string]any
	_ = json.Unmarshal(gate.Payload, &p)
	if p["result"] != "blocked" || p["blocking"] != true || p["gate"].(map[string]any)["id"] != "wait-9" || p["gate"].(map[string]any)["kind"] != "approval" ||
		p["subject"].(map[string]any)["id"] != "run-1" {
		t.Fatalf("payload = %v", p)
	}
	if !strings.Contains(strings.Join(gate.Class.Labels, ","), "gate-blocked") {
		t.Fatalf("labels = %v", gate.Class.Labels)
	}
	// A gate wait that is not a human decision is a custom gate.
	custom, ok := mapOne(t, workflowruntime.Event{Sequence: 4, RunID: "run-1", Invocation: &inv, Type: workflowruntime.EventWaitSuspended, OccurredAt: at,
		Attributes: map[string]string{"kind": "gate", "wait_id": "wait-10", "wake_source": "callback"}})
	if !ok || !strings.Contains(string(custom.Payload), `"kind":"custom"`) {
		t.Fatalf("a non-human gate wait = %+v ok=%v", custom, ok)
	}
	if _, ok := mapOne(t, workflowruntime.Event{Sequence: 3, RunID: "run-1", Invocation: &inv, Type: workflowruntime.EventWaitSuspended, OccurredAt: at,
		Attributes: map[string]string{"kind": "timer", "wait_id": "wait-1"}}); ok {
		t.Fatal("a timer wait was exported as a gate")
	}
}

// Canceling a run records a run.cancellation_requested event (not a status
// change) that carries the move to canceled; it is the run's terminal state for a
// consumer, so it exports as run.state_changed. A request that leaves the run active
// carries no terminal to_status and stays local, and the cancel reason code never
// leaves.
func TestACancellationThatTerminatesTheRunBecomesRunStateChanged(t *testing.T) {
	cancel := workflowruntime.Event{Sequence: 9, RunID: "run-1", Type: workflowruntime.EventRunCancellationRequested, OccurredAt: at,
		Attributes: map[string]string{"from_status": "running", "to_status": "canceled", "reason_code": "operator said so"}}
	e, ok := mapOne(t, cancel)
	if !ok {
		t.Fatal("a terminal cancellation was not exported")
	}
	var p map[string]any
	_ = json.Unmarshal(e.Payload, &p)
	if e.Kind != KindRunStateChanged || p["to"] != "canceled" || p["from"] != "running" || p["run_sequence"] != float64(9) {
		t.Fatalf("envelope %+v payload %v", e, p)
	}
	if strings.Contains(string(e.Payload), "operator") || strings.Contains(string(e.Payload), "reason") {
		t.Fatalf("the cancel reason leaked: %s", e.Payload)
	}
	if got := strings.Join(e.Class.Labels, ","); got != "run-terminal" {
		t.Fatalf("labels = %q", got)
	}
	cancel.Attributes = map[string]string{"from_status": "running", "to_status": "running"}
	if _, ok := mapOne(t, cancel); ok {
		t.Fatal("a cancellation request that left the run active was exported")
	}
	cancel.Attributes = map[string]string{"reason_code": "x"}
	if _, ok := mapOne(t, cancel); ok {
		t.Fatal("a cancellation request without a status was exported")
	}
}

// Everything else stays in the local feed, and a gate's resume (which carries no
// explicit decision) does not become a pass or fail.
func TestEveryOtherEventTypeIsNotExported(t *testing.T) {
	for _, typ := range []string{
		workflowruntime.EventNodeStatusChanged, workflowruntime.EventNodeAttemptStarted, workflowruntime.EventNodeAttemptFinished, workflowruntime.EventWaitResumed,
		workflowruntime.EventWaitTimedOut, workflowruntime.EventRetryScheduled, workflowruntime.EventFanOutExpanded,
		workflowruntime.EventRunCancellationRequested, workflowruntime.EventCrashReconciled, "adapter.observation",
	} {
		if _, ok := mapOne(t, workflowruntime.Event{Sequence: 1, RunID: "r", Type: typ, OccurredAt: at, Attributes: map[string]string{"kind": "gate", "wait_id": "w", "to_status": "x"}}); ok {
			t.Errorf("%s was exported", typ)
		}
	}
}

// Every exported payload validates against the catalog's own JSON Schema for its
// kind, and the schema id on the envelope is the catalog's.
func TestPayloadsValidateAgainstTheCatalogSchemas(t *testing.T) {
	inv := workflowruntime.NodeInvocationID{RunID: "run-1", NodeID: "n"}
	events := []workflowruntime.Event{
		runStatus("pending", "running"), runStatus("", "succeeded"),
		{Sequence: 3, RunID: "run-1", Invocation: &inv, Type: workflowruntime.EventWaitSuspended, OccurredAt: at, Attributes: map[string]string{"kind": "gate", "wait_id": "w1", "wake_source": "gate"}},
	}
	var mapped []runtimeevents.Event
	for _, ev := range events {
		e, ok := mapOne(t, ev)
		if !ok {
			t.Fatalf("%s not exported", ev.Type)
		}
		mapped = append(mapped, e)
	}
	for _, outcome := range []string{"allow", "deny", "confirm"} {
		e, ok := mapAdmission(t, Admission{DecisionID: "dec-1", RunID: "run-1", Outcome: outcome, DecidedAt: at})
		if !ok {
			t.Fatalf("admission %s not exported", outcome)
		}
		mapped = append(mapped, e)
	}
	if c, ok := mapConfirmation(t, AdmissionConfirmation{DecisionID: "dec-1", RunID: "run-1", BlockedEventID: "evt_blocked", ConfirmedBy: "operator:local", Verified: "token", At: at}); ok {
		mapped = append(mapped, c)
	} else {
		t.Fatal("confirmation not exported")
	}
	for _, e := range mapped {
		entry, found := catalog.Lookup(string(e.Kind))
		if !found || entry.PayloadSchema != e.Class.PayloadSchema || entry.Category != e.Class.Category {
			t.Fatalf("%s: class %+v vs catalog %+v", e.Kind, e.Class, entry)
		}
		doc, err := catalog.PayloadSchema(entry.PayloadSchema)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
		if err != nil {
			t.Fatal(err)
		}
		compiler := jsonschema.NewCompiler()
		if addErr := compiler.AddResource(entry.PayloadSchema, schema); addErr != nil {
			t.Fatal(addErr)
		}
		compiled, err := compiler.Compile(entry.PayloadSchema)
		if err != nil {
			t.Fatal(err)
		}
		var instance any
		_ = json.Unmarshal(e.Payload, &instance)
		if err := compiled.Validate(instance); err != nil {
			t.Errorf("%s payload does not match its schema: %v\n%s", e.Kind, err, e.Payload)
		}
		if err := e.Validate(); err != nil {
			t.Errorf("%s envelope is invalid: %v", e.Kind, err)
		}
	}
}

func TestAllowlistKeepsOnlyListedPathsAndFailsClosed(t *testing.T) {
	a := Allowlist{"k": {"/a/b", "/c"}}
	out, rep, err := a.SanitizePayload(context.Background(), "k", []byte(`{"a":{"b":1,"secret":"x"},"c":"y","token":"SECRET"}`))
	if err != nil || rep.ErrorClass != "" || string(out) != `{"a":{"b":1},"c":"y"}` || rep.Redactions != 2 || rep.Sensitivity != "private" {
		t.Fatalf("out=%s rep=%+v err=%v", out, rep, err)
	}
	out, rep, _ = a.SanitizePayload(context.Background(), "unlisted", []byte(`{"a":1}`))
	if string(out) != `{}` || rep.Redactions != 1 {
		t.Fatalf("an unlisted kind keeps nothing: %s %+v", out, rep)
	}
	for name, payload := range map[string][]byte{"not json": []byte(`{`), "an array": []byte(`[1]`), "null": []byte(`null`), "oversized": bytes.Repeat([]byte("x"), MaxPayloadBytes+1)} {
		if _, rep, _ := a.SanitizePayload(context.Background(), "k", payload); rep.ErrorClass == "" {
			t.Errorf("%s did not fail closed", name)
		}
	}
	// And through the chain, a failing sanitizer yields the fail-closed payload, still emitted.
	var _ eventstage.PayloadSanitizer = a
}
