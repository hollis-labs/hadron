package persistence

import (
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/testdb"
	"github.com/hollis-labs/libs/workflow/engine/compile"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
	"github.com/hollis-labs/libs/workflow/engine/values"
)

func TestStoreGenerationIsStableAcrossReopenAndDiffersBetweenDatabases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.db")
	first := mustWorkflowStateStore(t, path)
	generation, err := first.StoreGeneration(t.Context())
	if err != nil || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(generation) {
		t.Fatalf("generation = %q, %v", generation, err)
	}
	reopened, err := mustWorkflowStateStore(t, path).StoreGeneration(t.Context())
	if err != nil || reopened != generation {
		t.Fatalf("generation changed across reopen: %q -> %q (%v)", generation, reopened, err)
	}
	other, err := mustWorkflowStateStore(t, filepath.Join(t.TempDir(), "b.db")).StoreGeneration(t.Context())
	if err != nil || other == generation {
		t.Fatalf("two databases share generation %q (%v)", other, err)
	}
}

func TestEventHighWaterTracksAppendsPerRun(t *testing.T) {
	state := mustWorkflowStateStore(t, filepath.Join(t.TempDir(), "hw.db"))
	base := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	ref := workflowruntime.PlanRef{ID: "hw", Version: "v1", Digest: values.SHA256Digest([]byte("hw")), SchemaVersion: compile.ExecutionPlanSchemaVersion}
	if err := state.RecordPlan(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	for _, id := range []workflowruntime.RunID{"hw-a", "hw-b"} {
		if _, _, err := state.CreateRun(t.Context(), workflowruntime.CreateRunRequest{ID: id, Plan: ref, Status: workflowruntime.RunPending, StartIdempotencyKey: string(id), CreatedAt: base}); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := state.EventHighWater(t.Context(), "hw-a"); err != nil || got != 0 {
		t.Fatalf("empty run high water = %d, %v", got, err)
	}
	for range 3 {
		if _, err := state.AppendEvent(t.Context(), workflowruntime.AppendEventRequest{RunID: "hw-a", Type: "adapter.observation", OccurredAt: base, Redaction: values.RedactionPublic, Retention: values.RetentionRun}); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := state.EventHighWater(t.Context(), "hw-a"); err != nil || got != 3 {
		t.Fatalf("high water = %d, %v", got, err)
	}
	if got, err := state.EventHighWater(t.Context(), "hw-b"); err != nil || got != 0 {
		t.Fatalf("another run's events moved this run's high water: %d, %v", got, err)
	}
}

func TestEventsChangedWakesAfterACommitAcrossStoresOnOneDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wake.db")
	db, err := testdb.Open(path, Open)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	reader, err := NewWorkflowStateStore(db)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWorkflowStateStore(db)
	if err != nil {
		t.Fatal(err)
	}
	changed := reader.EventsChanged()
	select {
	case <-changed:
		t.Fatal("woken before any write")
	default:
	}
	ref := workflowruntime.PlanRef{ID: "wake", Version: "v1", Digest: values.SHA256Digest([]byte("wake")), SchemaVersion: compile.ExecutionPlanSchemaVersion}
	if err := writer.RecordPlan(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(2 * time.Second):
		t.Fatal("a commit through another store did not wake the reader")
	}
	select {
	case <-reader.EventsChanged():
		t.Fatal("a fresh channel was already closed")
	default:
	}
}
