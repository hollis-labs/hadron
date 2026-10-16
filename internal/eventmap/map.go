package eventmap

import (
	"context"
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
	case workflowruntime.EventRunStatusChanged, workflowruntime.EventRunCancellationRequested:
		to := ev.Attributes["to_status"]
		if to == "" {
			return base, false
		}
		// Cancellation moves the run to canceled and records that as a
		// cancellation request rather than a status change; it is the run's terminal
		// state for a consumer. A request that leaves the run active carries no
		// to_status and is not exported. The cancel reason code stays local.
		if ev.Type == workflowruntime.EventRunCancellationRequested && !workflowruntime.RunStatus(to).Terminal() {
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

// ServiceActorURN is Hadron's own actor, for an event no person caused.
const ServiceActorURN = "msg://service/hadron"

// AdmissionGateID is the gate id of the admission policy evaluation.
const AdmissionGateID = "hadron.admission"

// admissionSubject is the one place that decides what an admission event is about.
// Every admission result, allow included, is about the evaluation: a subject must
// name something that exists, and a run may not (a dry run, a confirm). The run an
// allow goes on to create points back at this event through corr.causation_id on its
// first run.state_changed (see WithCausation).
func admissionSubject(a Admission) (subjectType, subjectID, subjectURN string) {
	return "workflow_admission", a.DecisionID, "msg://workflow_admission/hadron/" + url.PathEscape(a.DecisionID)
}

// buildAdmission maps an admission decision to gate.evaluated (gate hadron.admission,
// kind custom): allow is pass, deny is fail, confirm is blocked. An allow is followed
// by a run, which later cites this event as its cause. The reason and the attributes
// of the decision are free text and never leave.
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
	subjectType, subjectID, subject := admissionSubject(a)
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

type causationKey struct{}

// WithCausation returns a context that makes the mapper set corr.causation_id on the
// run.state_changed it builds to eventID: the id of the admission event that caused
// the run. The caller decides when it applies (the run's first state change).
func WithCausation(ctx context.Context, eventID string) context.Context {
	return context.WithValue(ctx, causationKey{}, eventID)
}

func causationFrom(ctx context.Context) string {
	id, _ := ctx.Value(causationKey{}).(string)
	return id
}

// AdmissionConfirmation is a confirm decision that was then satisfied: the start was
// confirmed by a caller (or is a dry run, which proceeds without confirmation).
type AdmissionConfirmation struct {
	// DecisionID is the evaluation's id; the subject is the same as the blocked event's.
	DecisionID string
	// RunID is the run of the start, kept for correlation.
	RunID string
	// BlockedEventID is the id of the exported blocked event this answers; empty when
	// that event was not exported.
	BlockedEventID string
	// ActorURN, ActorKind, ActorRole and Verified say who confirmed and how Hadron
	// knows (token or claimed). Every exported event has an actor: when ActorURN is
	// empty the mapper uses Hadron's own service actor, claimed.
	ActorURN, ActorKind, ActorRole, Verified string
	// At is when the confirmed start was recorded.
	At time.Time
}

// buildConfirmation maps a satisfied confirm decision to a second gate.evaluated on
// the same admission subject: result pass, blocking false, caused by the blocked
// event, with the confirmer (or Hadron's service actor) as the actor.
func buildConfirmation(c AdmissionConfirmation) (runtimeevents.Event, bool) {
	if c.DecisionID == "" || c.RunID == "" {
		return runtimeevents.Event{}, false
	}
	_, _, subject := admissionSubject(Admission{DecisionID: c.DecisionID})
	e := runtimeevents.Event{
		SchemaVersion: "1", ID: runtimeevents.NewEventID(), Time: c.At.UTC(), Subject: subject,
		Source: runtimeevents.Source{Channel: ChannelAdmission, Confidence: runtimeevents.ConfidenceDerived},
		Corr:   &runtimeevents.Corr{CorrelationID: c.RunID, CausationID: c.BlockedEventID},
		Kind:   KindGateEvaluated,
		Class:  &runtimeevents.Class{Category: "state", Severity: "notice", PayloadSchema: schemaGate},
	}
	e.Actor = &runtimeevents.Actor{URN: c.ActorURN, Kind: c.ActorKind, Role: c.ActorRole, Verified: c.Verified}
	if c.ActorURN == "" {
		e.Actor = &runtimeevents.Actor{URN: ServiceActorURN, Kind: "service", Role: "system", Verified: "claimed"}
	}
	e.Payload = mustJSON(map[string]any{
		"gate":    map[string]string{"id": AdmissionGateID, "kind": "custom"},
		"subject": map[string]string{"type": "workflow_admission", "id": c.DecisionID},
		"result":  "pass", "blocking": false,
	})
	return e, true
}
