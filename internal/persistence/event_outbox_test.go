package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/eventmap"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

func outboxStore(t *testing.T, path string) (*Store, *WorkflowStateStore) {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, err := NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	return store, state
}

func enableOutbox(t *testing.T, state *WorkflowStateStore) {
	t.Helper()
	mapper, err := eventmap.NewMapper(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.EnableEventOutbox(context.Background(), mapper); err != nil {
		t.Fatal(err)
	}
}

func envelopeKinds(t *testing.T, entries []OutboxEntry) []string {
	t.Helper()
	out := make([]string, len(entries))
	for i, e := range entries {
		var env struct {
			Kind     string `json:"kind"`
			Sequence uint64 `json:"sequence"`
		}
		if err := json.Unmarshal(e.Envelope, &env); err != nil {
			t.Fatal(err)
		}
		if env.Sequence != e.Seq {
			t.Fatalf("envelope sequence %d != outbox sequence %d", env.Sequence, e.Seq)
		}
		out[i] = env.Kind
	}
	return out
}

func TestTheOutboxIsEmptyUnlessEnabled(t *testing.T) {
	store, state := outboxStore(t, filepath.Join(t.TempDir(), "off.db"))
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "off", base)
	if _, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	if n := workflowRowCount(t, store, "workflow_event_outbox"); n != 0 {
		t.Fatalf("outbox rows with the exporter off: %d", n)
	}
	if n := workflowRowCount(t, store, "workflow_events"); n == 0 {
		t.Fatal("test setup: no workflow events were written")
	}
}

func TestEnvelopesAreWrittenInTheSameTransactionAsTheirEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "on.db")
	store, state := outboxStore(t, path)
	enableOutbox(t, state)
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "on", base)
	running, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base})
	if err != nil {
		t.Fatal(err)
	}
	node := createWorkflowTestNode(t, state, run.ID, "work", base)
	finishWorkflowControlNode(t, state, node, workflowruntime.NodeFailed, workflowruntime.Failure{Code: "boom", Message: "boom"}, base)
	if _, err = state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunFailed, At: base.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}

	entries, err := state.EventOutboxAfter(t.Context(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	// node.* events (the finished attempt) are not exported.
	want := []string{"run.state_changed", "run.state_changed"}
	if got := envelopeKinds(t, entries); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("kinds = %v, want %v", got, want)
	}
	seen := map[string]bool{}
	for i, e := range entries {
		if e.Seq != uint64(i+1) || e.RunID != run.ID || seen[e.EventID] {
			t.Fatalf("entry %d = %+v", i, e)
		}
		seen[e.EventID] = true
		// The run sequence in the payload is the workflow_events sequence it came from.
		var env struct {
			Payload struct {
				RunSequence uint64 `json:"run_sequence"`
			} `json:"payload"`
			Generation uint64 `json:"generation"`
		}
		_ = json.Unmarshal(e.Envelope, &env)
		if env.Payload.RunSequence != e.RunSequence || env.Payload.RunSequence == 0 {
			t.Fatalf("entry %d: payload run_sequence %d, column %d", i, env.Payload.RunSequence, e.RunSequence)
		}
		var typ string
		if typeErr := store.DB().QueryRow(`SELECT event_type FROM workflow_events WHERE run_id = ? AND sequence = ?`, string(run.ID), int64(e.RunSequence)).Scan(&typ); typeErr != nil || typ != "run.status_changed" {
			t.Fatalf("entry %d points at a %q event (%v)", i, typ, typeErr)
		}
	}

	// A restart keeps the generation: the envelopes of one database share it.
	var gen1 struct {
		Generation uint64 `json:"generation"`
	}
	_ = json.Unmarshal(entries[0].Envelope, &gen1)
	_, reopened := outboxStore(t, path)
	enableOutbox(t, reopened)
	number, err := reopened.StoreGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := generationNumber(number); gen1.Generation != want || want == 0 {
		t.Fatalf("generation %d, want %d", gen1.Generation, want)
	}
}

// Recording never rejects a valid mutation (platform rule, CW-0149): when the
// envelope cannot be produced or stored, the workflow event still commits, the
// envelope work is rolled back to the savepoint, the counter is not advanced, and a
// skipped row says which event and why. Only a database error in the outer
// transaction fails the write.
func TestAnEnvelopeFailureNeverRejectsTheWorkflowMutation(t *testing.T) {
	for name, test := range map[string]struct {
		mapper OutboxMapper
		class  string
	}{
		"mapper error":     {failingMapper{}, "mapper_error"},
		"classed mapping":  {classedFailingMapper{class: "invalid_envelope"}, "invalid_envelope"},
		"dropped by stage": {classedFailingMapper{class: "dropped"}, "dropped"},
		"mapper panic":     {panickingMapper{}, "mapper_panic"},
		"insert failure":   {nil, "insert_error"},
	} {
		t.Run(name, func(t *testing.T) {
			store, state := outboxStore(t, filepath.Join(t.TempDir(), "skip.db"))
			enableOutbox(t, state)
			base := workflowTestTime()
			run := createWorkflowTestRun(t, state, "skip", base)
			running, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base})
			if err != nil {
				t.Fatal(err)
			}
			good := workflowRowCount(t, store, "workflow_event_outbox")
			if good != 1 {
				t.Fatalf("test setup: %d outbox rows after the first transition", good)
			}
			events := workflowRowCount(t, store, "workflow_events")

			realMapper := state.outbox.mapper
			state.outbox.mapper = test.mapper
			if test.mapper == nil {
				var firstID string
				if err = store.DB().QueryRow(`SELECT event_id FROM workflow_event_outbox`).Scan(&firstID); err != nil {
					t.Fatal(err)
				}
				state.outbox.mapper = duplicateIDMapper{id: firstID}
			}
			failed, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunFailed, At: base.Add(time.Minute)})
			if err != nil {
				t.Fatalf("an envelope failure rejected the workflow mutation: %v", err)
			}
			after, err := state.LoadRun(t.Context(), run.ID)
			if err != nil || after.Status != workflowruntime.RunFailed {
				t.Fatalf("the run did not move: %+v, %v", after, err)
			}
			if got := workflowRowCount(t, store, "workflow_events"); got != events+1 {
				t.Fatalf("workflow events %d -> %d: the event was not committed", events, got)
			}
			if got := workflowRowCount(t, store, "workflow_event_outbox"); got != good {
				t.Fatalf("a failed envelope left %d outbox rows (was %d)", got, good)
			}
			var counter int64
			if err := store.DB().QueryRow(`SELECT last_seq FROM workflow_event_outbox_counter`).Scan(&counter); err != nil || counter != int64(good) {
				t.Fatalf("the counter advanced for a skipped event: %d (%v)", counter, err)
			}
			var gotClass, created string
			var repaired sql.NullString
			var runSeq int64
			if err := store.DB().QueryRow(`SELECT run_sequence, error_class, created_at, repaired_at FROM workflow_event_outbox_skipped WHERE run_id = ?`, string(run.ID)).Scan(&runSeq, &gotClass, &created, &repaired); err != nil {
				t.Fatalf("no skipped row: %v", err)
			}
			if gotClass != test.class || created == "" || repaired.Valid || failed.Event == nil || uint64(runSeq) != failed.Event.Sequence {
				t.Fatalf("skipped row = seq %d class %q repaired_at %v (must be NULL: the row is open for repair); want seq %d class %q", runSeq, gotClass, repaired, failed.Event.Sequence, test.class)
			}
			var open int
			if err := store.DB().QueryRow(`SELECT COUNT(*) FROM workflow_event_outbox_skipped WHERE repaired_at IS NULL`).Scan(&open); err != nil || open != 1 {
				t.Fatalf("open skipped rows = %d (%v); want 1", open, err)
			}
			if state.EventOutboxSkipped() != 1 {
				t.Fatalf("skipped counter = %d", state.EventOutboxSkipped())
			}
			// With a working mapper again, the next exported event takes the very next
			// sequence number: the skipped one left no gap in the outbox.
			state.outbox.mapper = realMapper
			next := createWorkflowTestRun(t, state, "after-skip", base.Add(2*time.Minute))
			if _, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: next.ID, ExpectedGeneration: next.Generation, To: workflowruntime.RunRunning, At: base.Add(2 * time.Minute)}); err != nil {
				t.Fatal(err)
			}
			var maxSeq int64
			if err := store.DB().QueryRow(`SELECT MAX(outbox_seq) FROM workflow_event_outbox`).Scan(&maxSeq); err != nil || maxSeq != int64(good)+1 || workflowRowCount(t, store, "workflow_event_outbox") != good+1 {
				t.Fatalf("the next event did not take the next sequence: max %d (%v)", maxSeq, err)
			}
		})
	}
}

// A refused transition writes nothing anywhere, and a database error in the outer
// transaction (here the skipped table is gone) still fails the whole write.
func TestOuterTransactionErrorsStillFailTheWrite(t *testing.T) {
	store, state := outboxStore(t, filepath.Join(t.TempDir(), "outer.db"))
	enableOutbox(t, state)
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "outer", base)
	events, outbox := workflowRowCount(t, store, "workflow_events"), workflowRowCount(t, store, "workflow_event_outbox")
	if _, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation + 5, To: workflowruntime.RunRunning, At: base}); err == nil {
		t.Fatal("a stale transition was accepted")
	}
	if workflowRowCount(t, store, "workflow_events") != events || workflowRowCount(t, store, "workflow_event_outbox") != outbox {
		t.Fatal("a refused transition left rows behind")
	}

	if _, err := store.DB().Exec(`DROP TABLE workflow_event_outbox_skipped`); err != nil {
		t.Fatal(err)
	}
	state.outbox.mapper = failingMapper{}
	if _, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err == nil {
		t.Fatal("the write succeeded although the skip could not be recorded")
	}
	after, err := state.LoadRun(t.Context(), run.ID)
	if err != nil || after.Status != workflowruntime.RunPending {
		t.Fatalf("the run moved although the write failed: %+v, %v", after, err)
	}
}

type failingMapper struct{}

func (failingMapper) MapWorkflowEvent(context.Context, workflowruntime.Event, uint64, uint64) ([]byte, string, bool, error) {
	return nil, "", false, errors.New("injected mapper failure")
}

type classedFailure struct{ class string }

func (e classedFailure) Error() string     { return "injected " + e.class }
func (e classedFailure) SkipClass() string { return e.class }

type classedFailingMapper struct{ class string }

func (m classedFailingMapper) MapWorkflowEvent(context.Context, workflowruntime.Event, uint64, uint64) ([]byte, string, bool, error) {
	return nil, "", false, classedFailure(m)
}

type panickingMapper struct{}

func (panickingMapper) MapWorkflowEvent(context.Context, workflowruntime.Event, uint64, uint64) ([]byte, string, bool, error) {
	panic("injected mapper panic")
}

// duplicateIDMapper maps fine but reuses an event id the first transition already
// stored, so the outbox insert violates the unique key.
type duplicateIDMapper struct{ id string }

func (m duplicateIDMapper) MapWorkflowEvent(ctx context.Context, ev workflowruntime.Event, generation, seq uint64) ([]byte, string, bool, error) {
	return []byte(`{"kind":"run.state_changed"}`), m.id, true, nil
}

// Trimming the tail of the outbox must never make a sequence number come back.
func TestOutboxSequencesAreNeverReusedAfterTrimming(t *testing.T) {
	store, state := outboxStore(t, filepath.Join(t.TempDir(), "trim.db"))
	enableOutbox(t, state)
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "trim", base)
	running, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB().Exec(`DELETE FROM workflow_event_outbox`); err != nil {
		t.Fatal(err)
	}
	if _, err = state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunFailed, At: base.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	entries, err := state.EventOutboxAfter(t.Context(), 0, 0)
	if err != nil || len(entries) != 1 || entries[0].Seq != 2 {
		t.Fatalf("entries = %+v, %v: the sequence came back after the tail was trimmed", entries, err)
	}
}

// The envelope generation must be exact in every JSON reader: nonzero and below 2^53.
func TestGenerationNumberIsJSONSafe(t *testing.T) {
	for _, hex := range []string{strings.Repeat("f", 32), "0123456789abcdef0123456789abcdef", strings.Repeat("1", 32)} {
		n, err := generationNumber(hex)
		if err != nil || n == 0 || n >= 1<<53 {
			t.Errorf("%s -> %d, %v", hex, n, err)
		}
	}
	if _, err := generationNumber(strings.Repeat("0", 32)); err == nil {
		t.Error("a zero generation was accepted")
	}
	if _, err := generationNumber("short"); err == nil {
		t.Error("a short generation was accepted")
	}
}

// A canceled run exports its terminal state. The engine records a cancel as
// run.cancellation_requested (the run goes to canceled in the same transaction),
// so the outbox must carry a run.state_changed to canceled for it, and nothing of
// the cancel reason.
func TestACanceledRunExportsItsTerminalState(t *testing.T) {
	store, state := outboxStore(t, filepath.Join(t.TempDir(), "cancel.db"))
	enableOutbox(t, state)
	base := workflowTestTime()
	fixture := prepareWorkflowSQLiteWait(t, state, "cancel-export", base, time.Hour)
	suspended, err := (workflowruntime.WaitCoordinator{Store: state}).Suspend(t.Context(), workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token})
	if err != nil {
		t.Fatal(err)
	}
	run, err := state.LoadRun(t.Context(), suspended.Node.ID.RunID)
	if err != nil {
		t.Fatal(err)
	}
	before := workflowRowCount(t, store, "workflow_event_outbox")
	reason := workflowruntime.Failure{Code: "user_canceled", Message: "SECRET operator note"}
	result, err := state.RequestRunCancellation(t.Context(), workflowruntime.RequestRunCancellationRequest{RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "cancel-export", Reason: reason, At: base.Add(5 * time.Second)})
	if err != nil || result.Run.Status != workflowruntime.RunCanceled {
		t.Fatalf("RequestRunCancellation = %#v, %v", result, err)
	}
	entries, err := state.EventOutboxAfter(t.Context(), uint64(before), 10) // #nosec G115 -- a small count
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].RunID != run.ID {
		t.Fatalf("outbox after cancel = %+v; want one entry for the run", entries)
	}
	if kinds := envelopeKinds(t, entries); kinds[0] != "run.state_changed" {
		t.Fatalf("kinds = %v", kinds)
	}
	var env struct {
		Payload map[string]any `json:"payload"`
	}
	if err := json.Unmarshal(entries[0].Envelope, &env); err != nil {
		t.Fatal(err)
	}
	if env.Payload["to"] != "canceled" || env.Payload["from"] == nil {
		t.Fatalf("payload = %v", env.Payload)
	}
	if strings.Contains(string(entries[0].Envelope), "SECRET") || strings.Contains(string(entries[0].Envelope), "user_canceled") {
		t.Fatalf("the cancel reason leaked into the envelope: %s", entries[0].Envelope)
	}
	if state.EventOutboxSkipped() != 0 {
		t.Fatalf("%d events were skipped", state.EventOutboxSkipped())
	}
}
