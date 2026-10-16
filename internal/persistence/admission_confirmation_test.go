package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/eventmap"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

// confirmStart returns a confirm decision's evaluation and the start record that
// follows it (CW-0250).
func confirmStart(t *testing.T, name string) (hoststate.PolicyEvaluation, hoststate.StartRecord) {
	t.Helper()
	record := workflowHostStartFixture(t, name)
	record.Decision.Outcome = hoststate.PolicyConfirm
	record.Decision.Reason = "SECRET free-text reason that must never leave"
	record.Facts.ConfirmationAdvised = true
	// An identity the operator credential authenticated over HTTP (verified by token).
	record.Identity.SourceAuthority, record.Identity.Trust = "http", "local"
	record.Facts.Identity = record.Identity
	evaluation := hoststate.PolicyEvaluation{StartKey: record.StartKey, RequestDigest: record.RequestDigest, Facts: record.Facts, Decision: record.Decision}
	return evaluation, record
}

type confirmationEnvelope struct {
	Kind    string `json:"kind"`
	Subject string `json:"subject"`
	Corr    struct {
		CausationID string `json:"causation_id"`
	} `json:"corr"`
	Actor *struct {
		URN      string `json:"urn"`
		Kind     string `json:"kind"`
		Role     string `json:"role"`
		Verified string `json:"verified"`
	} `json:"actor"`
	Payload struct {
		Gate    map[string]string `json:"gate"`
		Subject map[string]string `json:"subject"`
		Result  string            `json:"result"`
		Block   bool              `json:"blocking"`
	} `json:"payload"`
}

func decodeConfirmation(t *testing.T, e OutboxEntry) confirmationEnvelope {
	t.Helper()
	var env confirmationEnvelope
	if err := json.Unmarshal(e.Envelope, &env); err != nil {
		t.Fatal(err)
	}
	return env
}

func firstStateChangeCause(t *testing.T, state *WorkflowStateStore, runID string, after uint64) string {
	t.Helper()
	entries, err := state.EventOutboxAfter(t.Context(), after, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if string(e.RunID) != runID {
			continue
		}
		var env struct {
			Kind string `json:"kind"`
			Corr struct {
				CausationID string `json:"causation_id"`
			} `json:"corr"`
		}
		if err := json.Unmarshal(e.Envelope, &env); err != nil {
			t.Fatal(err)
		}
		if env.Kind == "run.state_changed" {
			return env.Corr.CausationID
		}
	}
	t.Fatalf("no run.state_changed for %s in %+v", runID, entries)
	return ""
}

// A confirm decision exports `blocked`. A start that then satisfies it exports a second
// gate.evaluated on the same admission subject, in the transaction that records the
// start: result pass, blocking false, caused by the blocked event, with the confirmer
// as actor, at key (decision id, 1). The run's first state change cites the PASS.
func TestAConfirmedStartExportsThePassAndTheRunCitesIt(t *testing.T) {
	store, host := admissionHost(t, "confirmed", true)
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	enableOutbox(t, state)
	evaluation, record := confirmStart(t, "confirmed")
	if _, _, recordErr := host.RecordPolicyEvaluation(t.Context(), evaluation); recordErr != nil {
		t.Fatal(recordErr)
	}
	entries, err := state.EventOutboxAfter(t.Context(), 0, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("after the confirm decision: %+v, %v; want only the blocked event", entries, err)
	}
	blocked := entries[0]
	if env := decodeConfirmation(t, blocked); env.Payload.Result != "blocked" || !env.Payload.Block {
		t.Fatalf("blocked event = %s", blocked.Envelope)
	}

	record.Confirmation = &hoststate.StartConfirmation{Confirmed: true, ConfirmedBy: "operator:local"}
	if _, outcome, startErr := host.RecordStart(t.Context(), record); startErr != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("RecordStart = %q, %v", outcome, startErr)
	}
	entries, err = state.EventOutboxAfter(t.Context(), blocked.Seq, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("after the confirmed start: %+v, %v; want one more event", entries, err)
	}
	pass := entries[0]
	if string(pass.RunID) != evaluation.Decision.ID || pass.RunSequence != 1 {
		t.Fatalf("pass key = (%q, %d); want (decision id, 1)", pass.RunID, pass.RunSequence)
	}
	env := decodeConfirmation(t, pass)
	if env.Kind != "gate.evaluated" || env.Subject != "msg://workflow_admission/hadron/"+evaluation.Decision.ID ||
		env.Payload.Gate["id"] != "hadron.admission" || env.Payload.Result != "pass" || env.Payload.Block ||
		env.Payload.Subject["type"] != "workflow_admission" || env.Payload.Subject["id"] != evaluation.Decision.ID {
		t.Fatalf("pass envelope = %s", pass.Envelope)
	}
	if env.Corr.CausationID != blocked.EventID {
		t.Fatalf("pass causation = %q; want the blocked event %q", env.Corr.CausationID, blocked.EventID)
	}
	// Today's operator principal ("operator:local") is not a msg:// URN, so even over
	// the authenticated HTTP path the confirmer is claimed, never token.
	if env.Actor == nil || env.Actor.Kind != "user" || env.Actor.Role != "operator" || env.Actor.Verified != "claimed" || env.Actor.URN != "msg://user/hadron/operator:local" {
		t.Fatalf("actor = %+v; want the claimed operator", env.Actor)
	}

	// The run that follows cites the pass, not the blocked event.
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, string(record.Run.ID), base)
	if _, err = state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	if got := firstStateChangeCause(t, state, string(record.Run.ID), pass.Seq); got != pass.EventID {
		t.Fatalf("the run cites %q; want the pass %q", got, pass.EventID)
	}

	// A replay of the same start exports nothing more.
	if _, outcome, replayErr := host.RecordStart(t.Context(), record); replayErr != nil || outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("replay = %q, %v", outcome, replayErr)
	}
	if got := workflowRowCount(t, store, "workflow_event_outbox"); got != 3 {
		t.Fatalf("outbox has %d rows after the replay; want blocked, pass and the run's state change", got)
	}
}

// A confirm decision whose start is never confirmed stays `blocked`; an allow decision
// has no second event.
func TestNoPassWithoutAConfirmation(t *testing.T) {
	store, host := admissionHost(t, "unconfirmed", true)
	evaluation, record := confirmStart(t, "unconfirmed")
	if _, _, err := host.RecordPolicyEvaluation(t.Context(), evaluation); err != nil {
		t.Fatal(err)
	}
	if got := workflowRowCount(t, store, "workflow_event_outbox"); got != 1 {
		t.Fatalf("%d rows for an unconfirmed decision", got)
	}
	// A start for a decision that needed no confirmation exports no pass.
	allow := workflowHostStartFixture(t, "allowed")
	if _, _, err := host.RecordPolicyEvaluation(t.Context(), hoststate.PolicyEvaluation{StartKey: allow.StartKey, RequestDigest: allow.RequestDigest, Facts: allow.Facts, Decision: allow.Decision}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := host.RecordStart(t.Context(), allow); err != nil {
		t.Fatal(err)
	}
	// An unconfirmed confirm start is not exported either (the service refuses it
	// before recording, so this only guards the store).
	record.Confirmation = nil
	if _, _, err := host.RecordStart(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if got := workflowRowCount(t, store, "workflow_event_outbox"); got != 2 {
		t.Fatalf("%d rows; want only the two admission events", got)
	}
}

// A confirm dry run proceeds without confirmation: it still exports the pass, with no
// actor, and the dry run's run links to nothing.
func TestAConfirmDryRunExportsThePassWithNoRunLink(t *testing.T) {
	store, host := admissionHost(t, "dry", true)
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	enableOutbox(t, state)
	evaluation, record := confirmStart(t, "dry")
	if _, _, recordErr := host.RecordPolicyEvaluation(t.Context(), evaluation); recordErr != nil {
		t.Fatal(recordErr)
	}
	record.DryRun = true
	if _, _, startErr := host.RecordStart(t.Context(), record); startErr != nil {
		t.Fatal(startErr)
	}
	entries, err := state.EventOutboxAfter(t.Context(), 0, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries = %+v, %v; want blocked then pass", entries, err)
	}
	env := decodeConfirmation(t, entries[1])
	// Every exported event has an actor: a dry run has no confirmer, so Hadron itself.
	if env.Payload.Result != "pass" || env.Actor == nil || env.Actor.URN != "msg://service/hadron" || env.Actor.Kind != "service" || env.Actor.Verified != "claimed" || env.Corr.CausationID != entries[0].EventID {
		t.Fatalf("dry-run pass = %s", entries[1].Envelope)
	}
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, string(record.Run.ID), base)
	if _, err = state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	if got := firstStateChangeCause(t, state, string(record.Run.ID), entries[1].Seq); got != "" {
		t.Fatalf("a dry run's run cites %q; want no link", got)
	}
}

type confirmationFailingMapper struct{ failingMapper }

func (confirmationFailingMapper) MapAdmissionConfirmation(context.Context, eventmap.AdmissionConfirmation, uint64, uint64) ([]byte, string, bool, error) {
	return nil, "", false, errors.New("injected confirmation mapper failure")
}

// Recording never rejects a valid mutation: when the pass cannot be produced the
// start still commits, the counter is not advanced and a skipped row sits at
// (decision id, 1).
func TestAConfirmationEnvelopeFailureNeverRejectsTheStart(t *testing.T) {
	store, host := admissionHost(t, "failconfirm", true)
	evaluation, record := confirmStart(t, "failconfirm")
	if _, _, err := host.RecordPolicyEvaluation(t.Context(), evaluation); err != nil {
		t.Fatal(err)
	}
	var last int64
	if err := store.DB().QueryRow(`SELECT last_seq FROM workflow_event_outbox_counter`).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if err := host.EnableEventOutbox(t.Context(), confirmationFailingMapper{}); err != nil {
		t.Fatal(err)
	}
	record.Confirmation = &hoststate.StartConfirmation{Confirmed: true, ConfirmedBy: "operator:local"}
	if _, outcome, err := host.RecordStart(t.Context(), record); err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("a confirmation envelope failure rejected the start: %q, %v", outcome, err)
	}
	if _, err := host.LoadStart(t.Context(), record.Run.ID); err != nil {
		t.Fatalf("the start was not committed: %v", err)
	}
	var after int64
	if err := store.DB().QueryRow(`SELECT last_seq FROM workflow_event_outbox_counter`).Scan(&after); err != nil || after != last {
		t.Fatalf("the counter moved %d -> %d (%v)", last, after, err)
	}
	var seq int64
	var class string
	if err := store.DB().QueryRow(`SELECT run_sequence, error_class FROM workflow_event_outbox_skipped WHERE run_id = ? AND repaired_at IS NULL`, evaluation.Decision.ID).Scan(&seq, &class); err != nil || seq != 1 || class != "mapper_error" {
		t.Fatalf("skipped row = %d %q, %v", seq, class, err)
	}
}

// A confirmer who is a msg:// URN and was authenticated by Hadron's HTTP path is the
// actor, verified by token; the pass carries that URN end to end.
func TestAVerifiedURNConfirmerIsTheTokenActor(t *testing.T) {
	_, host := admissionHost(t, "token", true)
	evaluation, record := confirmStart(t, "token")
	if _, _, err := host.RecordPolicyEvaluation(t.Context(), evaluation); err != nil {
		t.Fatal(err)
	}
	record.Confirmation = &hoststate.StartConfirmation{Confirmed: true, ConfirmedBy: "msg://user/local/chris"}
	if _, _, err := host.RecordStart(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	entries, err := host.state.EventOutboxAfter(t.Context(), 0, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries = %+v, %v", entries, err)
	}
	env := decodeConfirmation(t, entries[1])
	if env.Actor == nil || env.Actor.URN != "msg://user/local/chris" || env.Actor.Kind != "user" || env.Actor.Verified != "token" {
		t.Fatalf("actor = %+v", env.Actor)
	}
}

// The actor rule, in one place: token only for an authenticated msg:// URN; everything
// else is claimed; no confirmer means Hadron's service actor.
func TestConfirmerActorRule(t *testing.T) {
	type want struct{ urn, kind, verified string }
	for name, test := range map[string]struct {
		confirmer, source string
		want              want
	}{
		"authenticated URN":          {"msg://user/local/chris", "http", want{"msg://user/local/chris", "user", "token"}},
		"authenticated agent URN":    {"msg://agent/agent-mux/agt_1", "http", want{"msg://agent/agent-mux/agt_1", "agent", "token"}},
		"URN without authentication": {"msg://user/local/chris", "test", want{"msg://user/hadron/msg:%2F%2Fuser%2Flocal%2Fchris", "user", "claimed"}},
		"operator principal":         {"operator:local", "http", want{"msg://user/hadron/operator:local", "user", "claimed"}},
		"unknown URN authority":      {"msg://thing/x", "http", want{"msg://user/hadron/msg:%2F%2Fthing%2Fx", "user", "claimed"}},
		"no confirmer":               {"", "http", want{"msg://service/hadron", "service", "claimed"}},
	} {
		t.Run(name, func(t *testing.T) {
			record := hoststate.StartRecord{}
			record.Identity.SourceAuthority, record.Identity.Trust = test.source, "local" // trust alone never makes token
			if test.confirmer != "" {
				record.Confirmation = &hoststate.StartConfirmation{Confirmed: true, ConfirmedBy: test.confirmer}
			}
			urn, kind, _, verified := confirmerOf(record)
			if urn != test.want.urn || kind != test.want.kind || verified != test.want.verified {
				t.Fatalf("confirmerOf = %q %q %q; want %+v", urn, kind, verified, test.want)
			}
		})
	}
}

// A start whose decision row is first created by RecordStart (no prior evaluation, as
// for a call-started child) exports its admission event there, once, at (decision id,
// 0); the run cites it. CW-0256.
func TestADecisionCreatedByRecordStartIsExportedOnceAndTheRunCitesIt(t *testing.T) {
	store, host := admissionHost(t, "startonly", true)
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	enableOutbox(t, state)
	record := workflowHostStartFixture(t, "startonly") // an allow decision
	if _, outcome, startErr := host.RecordStart(t.Context(), record); startErr != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("RecordStart = %q, %v", outcome, startErr)
	}
	entries, err := state.EventOutboxAfter(t.Context(), 0, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries = %+v, %v; want the one admission event", entries, err)
	}
	env := decodeConfirmation(t, entries[0])
	if string(entries[0].RunID) != record.Decision.ID || entries[0].RunSequence != 0 || env.Payload.Result != "pass" ||
		env.Subject != "msg://workflow_admission/hadron/"+record.Decision.ID || env.Payload.Gate["id"] != "hadron.admission" {
		t.Fatalf("admission envelope = %s", entries[0].Envelope)
	}

	// Neither a replay of the start nor a late evaluation of the same decision exports again.
	if _, outcome, replayErr := host.RecordStart(t.Context(), record); replayErr != nil || outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("replay = %q, %v", outcome, replayErr)
	}
	if _, outcome, evalErr := host.RecordPolicyEvaluation(t.Context(), hoststate.PolicyEvaluation{StartKey: record.StartKey, RequestDigest: record.RequestDigest, Facts: record.Facts, Decision: record.Decision}); evalErr != nil || outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("late evaluation = %q, %v", outcome, evalErr)
	}
	if got := workflowRowCount(t, store, "workflow_event_outbox"); got != 1 {
		t.Fatalf("%d rows after the replay and the late evaluation; want 1", got)
	}

	// The run it admits cites it.
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, string(record.Run.ID), base)
	if _, err = state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	if got := firstStateChangeCause(t, state, string(record.Run.ID), entries[0].Seq); got != entries[0].EventID {
		t.Fatalf("the run cites %q; want the admission event %q", got, entries[0].EventID)
	}
}

// A confirm decision created and confirmed in RecordStart alone exports blocked, then
// the pass caused by it, in that order and once each.
func TestAConfirmCreatedAndConfirmedInRecordStartExportsBlockedThenPass(t *testing.T) {
	store, host := admissionHost(t, "startconfirm", true)
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	_, record := confirmStart(t, "startconfirm")
	record.Confirmation = &hoststate.StartConfirmation{Confirmed: true, ConfirmedBy: "msg://user/local/chris"}
	if _, _, startErr := host.RecordStart(t.Context(), record); startErr != nil {
		t.Fatal(startErr)
	}
	entries, err := state.EventOutboxAfter(t.Context(), 0, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries = %+v, %v; want blocked then pass", entries, err)
	}
	blocked, pass := decodeConfirmation(t, entries[0]), decodeConfirmation(t, entries[1])
	if entries[0].RunSequence != 0 || entries[1].RunSequence != 1 || blocked.Payload.Result != "blocked" || pass.Payload.Result != "pass" || pass.Corr.CausationID != entries[0].EventID {
		t.Fatalf("blocked = %s\npass = %s", entries[0].Envelope, entries[1].Envelope)
	}
}

// An admission envelope failure in RecordStart never rejects the start: it is skipped
// at (decision id, 0) like any other.
func TestAnAdmissionFailureInRecordStartNeverRejectsTheStart(t *testing.T) {
	store, host := admissionHost(t, "startfail", false)
	if err := host.EnableEventOutbox(t.Context(), admissionFailingMapper{}); err != nil {
		t.Fatal(err)
	}
	record := workflowHostStartFixture(t, "startfail")
	if _, outcome, err := host.RecordStart(t.Context(), record); err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("RecordStart = %q, %v", outcome, err)
	}
	var seq int64
	if err := store.DB().QueryRow(`SELECT run_sequence FROM workflow_event_outbox_skipped WHERE run_id = ? AND repaired_at IS NULL`, record.Decision.ID).Scan(&seq); err != nil || seq != 0 {
		t.Fatalf("skipped row = %d, %v", seq, err)
	}
}
