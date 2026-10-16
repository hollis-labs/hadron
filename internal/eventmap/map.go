package eventmap

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/hollis-labs/libs/harness/core/adapters/runtimeevents"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
	workflowwait "github.com/hollis-labs/libs/workflow/engine/wait"
)

// Channel is the envelope source channel of every event this package maps.
const Channel runtimeevents.SourceChannel = "workflow_events"

// Full catalog payload schema ids.
const (
	schemaStateChanged = "urn:hollis-labs:events:state_changed@1"
	schemaGate         = "urn:hollis-labs:events:gate.evaluated@1"
)

// ChannelAdmission is the source channel of the admission policy decisions, which
// the host store persists outside workflow_events.
const ChannelAdmission runtimeevents.SourceChannel = "workflow_admission"

// Kinds exported.
const (
	KindRunStateChanged = "run.state_changed"
	KindGateEvaluated   = "gate.evaluated"
)

// DefaultAllowlist is the strict per-kind payload allowlist the sanitize stage
// applies. The mapper builds payloads from the same fields; this is the second lock.
func DefaultAllowlist() Allowlist {
	return Allowlist{
		KindRunStateChanged: {"/entity/type", "/entity/id", "/from", "/to", "/trigger/kind", "/run_sequence"},
		KindGateEvaluated:   {"/gate/id", "/gate/kind", "/subject/type", "/subject/id", "/result", "/blocking", "/run_sequence"},
	}
}

// build maps one workflow event to an unsanitized envelope, or reports that the
// event is not exported.
func build(ev workflowruntime.Event) (runtimeevents.Event, bool) {
	run := string(ev.RunID)
	base := runtimeevents.Event{
		SchemaVersion: "1", ID: runtimeevents.NewEventID(), Time: ev.OccurredAt.UTC(), Subject: "msg://workflow_run/hadron/" + url.PathEscape(run),
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
	case workflowruntime.EventWaitSuspended:
		if ev.Attributes["kind"] != string(workflowwait.KindGate) || ev.Attributes["wait_id"] == "" {
			return base, false
		}
		base.Kind = KindGateEvaluated
		base.Class = &runtimeevents.Class{Category: "state", Severity: "notice", PayloadSchema: schemaGate}
		base.Payload = mustJSON(map[string]any{
			"gate":    map[string]string{"id": ev.Attributes["wait_id"], "kind": gateKind(ev.Attributes["wake_source"])},
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

// gateKind is the gate.evaluated gate kind of a gate wait: a wait woken by a gate
// response is a human decision (approval); any other gate wait is custom.
func gateKind(wakeSource string) string {
	if wakeSource == string(workflowwait.WakeGate) {
		return "approval"
	}
	return "custom"
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic("eventmap: marshal " + strconv.Quote(fmt.Sprintf("%T", v)) + ": " + err.Error())
	}
	return b
}

// Admission is what the mapper needs to know of a persisted admission policy
// evaluation: the decision, never its facts, reason or attributes.
type Admission struct {
	// DecisionID is the evaluation's id (hoststate PolicyDecision.ID).
	DecisionID string
	// RunID is the run the start would create.
	RunID string
	// Outcome is the policy outcome: allow, deny or confirm.
	Outcome string
	// DecidedAt is when the decision was made.
	DecidedAt time.Time
}

// AdmissionGateID is the gate id of the admission policy evaluation.
const AdmissionGateID = "hadron.admission"

// buildAdmission maps an admission decision to gate.evaluated (gate hadron.admission,
// kind custom): allow is pass, deny is fail, confirm is blocked. An allow is followed
// by the run it admits, so its subject is that run. A deny creates no run, and a
// confirm may never, so those are about the evaluation itself. The reason and the
// attributes of the decision are free text and never leave.
func buildAdmission(a Admission) (runtimeevents.Event, bool) {
	var result string
	switch a.Outcome {
	case "allow":
		result = "pass"
	case "deny":
		result = "fail"
	case "confirm":
		result = "blocked"
	default:
		return runtimeevents.Event{}, false
	}
	if a.DecisionID == "" || a.RunID == "" {
		return runtimeevents.Event{}, false
	}
	subjectType, subjectID, subject := "workflow_admission", a.DecisionID, "msg://workflow_admission/hadron/"+url.PathEscape(a.DecisionID)
	if a.Outcome == "allow" {
		subjectType, subjectID, subject = "workflow_run", a.RunID, "msg://workflow_run/hadron/"+url.PathEscape(a.RunID)
	}
	e := runtimeevents.Event{
		SchemaVersion: "1", ID: runtimeevents.NewEventID(), Time: a.DecidedAt.UTC(), Subject: subject,
		Source: runtimeevents.Source{Channel: ChannelAdmission, Confidence: runtimeevents.ConfidenceDerived},
		Corr:   &runtimeevents.Corr{CorrelationID: a.RunID},
		Kind:   KindGateEvaluated,
		Class:  &runtimeevents.Class{Category: "state", Severity: "notice", PayloadSchema: schemaGate},
	}
	e.Payload = mustJSON(map[string]any{
		"gate":    map[string]string{"id": AdmissionGateID, "kind": "custom"},
		"subject": map[string]string{"type": subjectType, "id": subjectID},
		"result":  result, "blocking": result == "blocked",
	})
	return e, true
}
