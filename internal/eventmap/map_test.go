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

func TestAttemptFinishedBecomesStepExecuted(t *testing.T) {
	inv := workflowruntime.NodeInvocationID{RunID: "run-1", NodeID: "build", Iteration: "item-3"}
	att := workflowruntime.AttemptID{Invocation: inv, Number: 2}
	for status, want := range map[string]string{"succeeded": "ok", "failed": "failed", "canceled": "skipped", "timed_out": "failed"} {
		e, ok := mapOne(t, workflowruntime.Event{Sequence: 9, RunID: "run-1", Invocation: &inv, Attempt: &att, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: at,
			Attributes: map[string]string{"attempt_status": status, "failure_code": "boom", "executor_target": "https://SECRET-host", "executor_kind": "script"}})
		if !ok || e.Kind != KindStepExecuted {
			t.Fatalf("%s: %+v", status, e)
		}
		var p map[string]any
		_ = json.Unmarshal(e.Payload, &p)
		if p["outcome"] != want || p["step_id"] != "build/item-3" || p["node_id"] != "build" || p["attempt"] != float64(2) || p["failure_code"] != "boom" || p["run_sequence"] != float64(9) {
			t.Fatalf("%s: payload = %v", status, p)
		}
		if strings.Contains(string(e.Payload), "SECRET") || strings.Contains(string(e.Payload), "script") {
			t.Fatalf("executor details leaked: %s", e.Payload)
		}
		if e.Class.Category != "step" || e.Class.PayloadSchema != schemaStepExecuted {
			t.Fatalf("class = %+v", e.Class)
		}
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
	if _, ok := mapOne(t, workflowruntime.Event{Sequence: 3, RunID: "run-1", Invocation: &inv, Type: workflowruntime.EventWaitSuspended, OccurredAt: at,
		Attributes: map[string]string{"kind": "timer", "wait_id": "wait-1"}}); ok {
		t.Fatal("a timer wait was exported as a gate")
	}
}

// Everything else stays in the local feed, and a gate's resume (which carries no
// explicit decision) does not become a pass or fail.
func TestEveryOtherEventTypeIsNotExported(t *testing.T) {
	for _, typ := range []string{
		workflowruntime.EventNodeStatusChanged, workflowruntime.EventNodeAttemptStarted, workflowruntime.EventWaitResumed,
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
	att := workflowruntime.AttemptID{Invocation: inv, Number: 1}
	events := []workflowruntime.Event{
		runStatus("pending", "running"), runStatus("", "succeeded"),
		{Sequence: 2, RunID: "run-1", Invocation: &inv, Attempt: &att, Type: workflowruntime.EventNodeAttemptFinished, OccurredAt: at, Attributes: map[string]string{"attempt_status": "succeeded"}},
		{Sequence: 3, RunID: "run-1", Invocation: &inv, Type: workflowruntime.EventWaitSuspended, OccurredAt: at, Attributes: map[string]string{"kind": "gate", "wait_id": "w1"}},
	}
	for _, ev := range events {
		e, ok := mapOne(t, ev)
		if !ok {
			t.Fatalf("%s not exported", ev.Type)
		}
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
