package persistence

import (
	"context"
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
	want := []string{"run.state_changed", "step.executed", "run.state_changed"}
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
		if typeErr := store.DB().QueryRow(`SELECT event_type FROM workflow_events WHERE run_id = ? AND sequence = ?`, string(run.ID), int64(e.RunSequence)).Scan(&typ); typeErr != nil || (typ != "run.status_changed" && typ != "node.attempt_finished") {
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

// A failure while writing the envelope must undo the event with it, and a
// transition that fails for another reason must leave no envelope behind.
func TestAnEnvelopeAndItsEventCommitOrRollBackTogether(t *testing.T) {
	store, state := outboxStore(t, filepath.Join(t.TempDir(), "atomic.db"))
	enableOutbox(t, state)
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "atomic", base)
	events, outbox := workflowRowCount(t, store, "workflow_events"), workflowRowCount(t, store, "workflow_event_outbox")

	// Stale generation: the transition is refused, nothing is written anywhere.
	if _, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation + 5, To: workflowruntime.RunRunning, At: base}); err == nil {
		t.Fatal("a stale transition was accepted")
	}
	if workflowRowCount(t, store, "workflow_events") != events || workflowRowCount(t, store, "workflow_event_outbox") != outbox {
		t.Fatal("a refused transition left rows behind")
	}

	// The mapper fails: the whole transition rolls back, status included.
	state.outbox.mapper = failingMapper{}
	if _, err := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err == nil {
		t.Fatal("a failing mapper did not fail the transition")
	}
	after, err := state.LoadRun(t.Context(), run.ID)
	if err != nil || after.Status != workflowruntime.RunPending || after.Generation != run.Generation {
		t.Fatalf("the run moved although its envelope could not be written: %+v, %v", after, err)
	}
	if workflowRowCount(t, store, "workflow_events") != events || workflowRowCount(t, store, "workflow_event_outbox") != outbox {
		t.Fatal("a failed envelope left rows behind")
	}
}

type failingMapper struct{}

func (failingMapper) MapWorkflowEvent(context.Context, workflowruntime.Event, uint64, uint64) ([]byte, string, bool, error) {
	return nil, "", false, errors.New("injected mapper failure")
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
