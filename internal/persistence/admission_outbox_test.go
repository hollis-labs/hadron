package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/eventmap"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

func admissionHost(t *testing.T, name string, enable bool) (*Store, *WorkflowHostStore) {
	t.Helper()
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), name+".db"))
	host, err := NewWorkflowHostStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if enable {
		mapper, err := eventmap.NewMapper(nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := host.EnableEventOutbox(t.Context(), mapper); err != nil {
			t.Fatal(err)
		}
	}
	return store, host
}

func admissionEvaluation(t *testing.T, name string, outcome hoststate.PolicyOutcome) hoststate.PolicyEvaluation {
	t.Helper()
	record := workflowHostStartFixture(t, name)
	record.Decision.Outcome = outcome
	record.Decision.Reason = "SECRET free-text reason that must never leave"
	return hoststate.PolicyEvaluation{StartKey: record.StartKey, RequestDigest: record.RequestDigest, Facts: record.Facts, Decision: record.Decision}
}

type admissionEnvelope struct {
	Kind    string `json:"kind"`
	Subject string `json:"subject"`
	Source  struct {
		Channel string `json:"channel"`
	} `json:"source"`
	Payload struct {
		Gate    map[string]string `json:"gate"`
		Subject map[string]string `json:"subject"`
		Result  string            `json:"result"`
		Block   bool              `json:"blocking"`
	} `json:"payload"`
}

// The admission policy decision is persisted by the host store in its own
// transaction, outside workflow_events. With the outbox on, the same transaction
// writes a gate.evaluated (hadron.admission): allow is pass, deny is fail, confirm
// is blocked, and all three are about the evaluation. The decision's reason and
// attributes never leave.
func TestAdmissionDecisionIsExportedInItsOwnTransaction(t *testing.T) {
	for _, test := range []struct {
		outcome  hoststate.PolicyOutcome
		result   string
		blocking bool
	}{
		{hoststate.PolicyAllow, "pass", false},
		{hoststate.PolicyDeny, "fail", false},
		{hoststate.PolicyConfirm, "blocked", true},
	} {
		t.Run(string(test.outcome), func(t *testing.T) {
			store, host := admissionHost(t, string(test.outcome), true)
			evaluation := admissionEvaluation(t, "adm-"+string(test.outcome), test.outcome)
			if _, outcome, err := host.RecordPolicyEvaluation(t.Context(), evaluation); err != nil || outcome != workflowruntime.IdempotencyApplied {
				t.Fatalf("RecordPolicyEvaluation = %q, %v", outcome, err)
			}
			entries, err := host.state.EventOutboxAfter(t.Context(), 0, 10)
			if err != nil || len(entries) != 1 {
				t.Fatalf("outbox = %+v, %v; want one entry", entries, err)
			}
			entry := entries[0]
			if string(entry.RunID) != evaluation.Decision.ID || entry.RunSequence != 0 {
				t.Fatalf("outbox key = (%q, %d); want (decision id, 0)", entry.RunID, entry.RunSequence)
			}
			var env admissionEnvelope
			if err := json.Unmarshal(entry.Envelope, &env); err != nil {
				t.Fatal(err)
			}
			// Every admission result, allow included, is about the evaluation (architect
			// ruling hadron_admission_event_subject_2026_10_16): the run may not exist.
			wantSubject := "msg://workflow_admission/hadron/" + evaluation.Decision.ID
			wantSubjectID := evaluation.Decision.ID
			if env.Kind != "gate.evaluated" || env.Subject != wantSubject || env.Source.Channel != "workflow_admission" ||
				env.Payload.Gate["id"] != "hadron.admission" || env.Payload.Gate["kind"] != "custom" ||
				env.Payload.Result != test.result || env.Payload.Block != test.blocking ||
				env.Payload.Subject["type"] != "workflow_admission" || env.Payload.Subject["id"] != wantSubjectID {
				t.Fatalf("envelope = %s", entry.Envelope)
			}
			if strings.Contains(string(entry.Envelope), "SECRET") {
				t.Fatalf("the decision reason leaked: %s", entry.Envelope)
			}
			// The decision itself is stored with it.
			if _, err := host.LoadPolicyEvaluation(t.Context(), evaluation.Decision.ID); err != nil {
				t.Fatal(err)
			}

			// A replay of the same start key writes nothing more.
			if _, outcome, err := host.RecordPolicyEvaluation(t.Context(), evaluation); err != nil || outcome != workflowruntime.IdempotencyReplayed {
				t.Fatalf("replay = %q, %v", outcome, err)
			}
			if got := workflowRowCount(t, store, "workflow_event_outbox"); got != 1 {
				t.Fatalf("a replay exported again: %d rows", got)
			}
		})
	}
}

// With the outbox off (the default), the host store writes no envelope.
func TestAdmissionDecisionWritesNoEnvelopeWhenTheOutboxIsOff(t *testing.T) {
	store, host := admissionHost(t, "off", false)
	if _, _, err := host.RecordPolicyEvaluation(t.Context(), admissionEvaluation(t, "adm-off", hoststate.PolicyDeny)); err != nil {
		t.Fatal(err)
	}
	if got := workflowRowCount(t, store, "workflow_event_outbox"); got != 0 {
		t.Fatalf("%d envelopes with the outbox off", got)
	}
}

// Recording never rejects a valid mutation: when the admission envelope cannot be
// produced, the decision still commits, the counter is not advanced and a skipped row
// says which decision and why.
type admissionFailingMapper struct{ failingMapper }

func (admissionFailingMapper) MapAdmission(context.Context, eventmap.Admission, uint64, uint64) ([]byte, string, bool, error) {
	return nil, "", false, errors.New("injected admission mapper failure")
}

func TestAnAdmissionEnvelopeFailureNeverRejectsTheDecision(t *testing.T) {
	store, host := admissionHost(t, "fail", false)
	if err := host.EnableEventOutbox(t.Context(), admissionFailingMapper{}); err != nil {
		t.Fatal(err)
	}
	evaluation := admissionEvaluation(t, "adm-fail", hoststate.PolicyDeny)
	if _, outcome, err := host.RecordPolicyEvaluation(t.Context(), evaluation); err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("an envelope failure rejected the decision: %q, %v", outcome, err)
	}
	if _, err := host.LoadPolicyEvaluation(t.Context(), evaluation.Decision.ID); err != nil {
		t.Fatalf("the decision was not committed: %v", err)
	}
	if got := workflowRowCount(t, store, "workflow_event_outbox"); got != 0 {
		t.Fatalf("a failed envelope left %d rows", got)
	}
	var last int64
	if err := store.DB().QueryRow(`SELECT last_seq FROM workflow_event_outbox_counter`).Scan(&last); err != nil || last != 0 {
		t.Fatalf("the counter advanced: %d, %v", last, err)
	}
	var class string
	var seq int64
	if err := store.DB().QueryRow(`SELECT run_sequence, error_class FROM workflow_event_outbox_skipped WHERE run_id = ? AND repaired_at IS NULL`, evaluation.Decision.ID).Scan(&seq, &class); err != nil || seq != 0 || class != "mapper_error" {
		t.Fatalf("skipped row = %d %q, %v", seq, class, err)
	}
	if host.EventOutboxSkipped() != 1 {
		t.Fatalf("skipped counter = %d", host.EventOutboxSkipped())
	}
}

// The workflow-event outbox and the host store draw from one counter, so sequence
// numbers stay contiguous across both producers in one database.
func TestAdmissionAndWorkflowEventsShareOneSequence(t *testing.T) {
	store, host := admissionHost(t, "shared", true)
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	enableOutbox(t, state)
	run := createWorkflowTestRun(t, state, "shared", workflowTestTime())
	if _, _, recordErr := host.RecordPolicyEvaluation(t.Context(), admissionEvaluation(t, "adm-shared", hoststate.PolicyConfirm)); recordErr != nil {
		t.Fatal(recordErr)
	}
	if _, transitionErr := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: workflowTestTime()}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	entries, err := state.EventOutboxAfter(t.Context(), 0, 10)
	if err != nil || len(entries) != 2 || entries[0].Seq != 1 || entries[1].Seq != 2 {
		t.Fatalf("entries = %+v, %v; want sequences 1 and 2", entries, err)
	}
	if kinds := envelopeKinds(t, entries); kinds[0] != "gate.evaluated" || kinds[1] != "run.state_changed" {
		t.Fatalf("kinds = %v", kinds)
	}
}

// The run an allow goes on to create cites the admission event as its cause: its
// FIRST run.state_changed carries corr.causation_id = the admission event's id. Later
// state changes, a run with no exported admission, and a denied evaluation carry none.
func TestTheRunAnAllowCreatesCitesTheAdmissionEventAsItsCause(t *testing.T) {
	store, host := admissionHost(t, "cause", true)
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	enableOutbox(t, state)

	evaluation := admissionEvaluation(t, "adm-cause", hoststate.PolicyAllow)
	evaluation.Decision.RunID, evaluation.Facts.RunID = "caused-run", "caused-run"
	if _, _, recordErr := host.RecordPolicyEvaluation(t.Context(), evaluation); recordErr != nil {
		t.Fatal(recordErr)
	}
	admission, err := state.EventOutboxAfter(t.Context(), 0, 10)
	if err != nil || len(admission) != 1 {
		t.Fatalf("admission entries = %+v, %v", admission, err)
	}

	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "caused-run", base)
	running, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunSucceeded, At: base.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}

	other := createWorkflowTestRun(t, state, "uncaused-run", base)
	if _, err = state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: other.ID, ExpectedGeneration: other.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}

	entries, err := state.EventOutboxAfter(t.Context(), admission[0].Seq, 10)
	if err != nil || len(entries) != 3 {
		t.Fatalf("run entries = %+v, %v; want 3", entries, err)
	}
	causation := func(e OutboxEntry) string {
		var env struct {
			Corr struct {
				CausationID string `json:"causation_id"`
			} `json:"corr"`
		}
		if err := json.Unmarshal(e.Envelope, &env); err != nil {
			t.Fatal(err)
		}
		return env.Corr.CausationID
	}
	if got := causation(entries[0]); got != admission[0].EventID {
		t.Fatalf("first state change causation = %q; want the admission event %q", got, admission[0].EventID)
	}
	if got := causation(entries[1]); got != "" {
		t.Fatalf("a later state change carries causation %q", got)
	}
	if got := causation(entries[2]); got != "" {
		t.Fatalf("a run with no admission carries causation %q", got)
	}
}
