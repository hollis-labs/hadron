package rundiagnostics

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/libs/workflow/engine/compile"
	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
	"github.com/hollis-labs/libs/workflow/engine/values"
)

// A run with more events than one page must be readable to the end: the
// previous read returned only the first page, so later events were unreachable.
func TestEventsPagesThroughARunLongerThanOnePage(t *testing.T) {
	database, err := persistence.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	state, err := persistence.NewWorkflowStateStore(database)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	ref := workflowruntime.PlanRef{ID: "events-plan", Version: "v1", Digest: values.SHA256Digest([]byte("events-plan")), SchemaVersion: compile.ExecutionPlanSchemaVersion}
	if recordErr := state.RecordPlan(t.Context(), ref); recordErr != nil {
		t.Fatal(recordErr)
	}
	for _, id := range []workflowruntime.RunID{"long-run", "other-run"} {
		if _, _, createErr := state.CreateRun(t.Context(), workflowruntime.CreateRunRequest{ID: id, Plan: ref, Status: workflowruntime.RunPending, StartIdempotencyKey: string(id) + "-start", CreatedAt: base}); createErr != nil {
			t.Fatal(createErr)
		}
	}
	const total, page = 25, 10
	for i := range total {
		if _, appendErr := state.AppendEvent(t.Context(), workflowruntime.AppendEventRequest{RunID: "long-run", Type: "adapter.observation", OccurredAt: base.Add(time.Duration(i) * time.Second), Attributes: map[string]string{"n": fmt.Sprint(i)}, Redaction: values.RedactionPublic, Retention: values.RetentionRun}); appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	// An event of another run must never appear in this run's pages.
	if _, appendErr := state.AppendEvent(t.Context(), workflowruntime.AppendEventRequest{RunID: "other-run", Type: "adapter.observation", OccurredAt: base, Redaction: values.RedactionPublic, Retention: values.RetentionRun}); appendErr != nil {
		t.Fatal(appendErr)
	}

	service := Service{State: state}
	var got []uint64
	var after uint64
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("paging did not terminate")
		}
		result, pageErr := service.Events(t.Context(), EventsQuery{RunID: "long-run", After: after, Limit: page})
		if pageErr != nil {
			t.Fatal(pageErr)
		}
		for _, event := range result.Events {
			got = append(got, event.Sequence)
			if event.RunID != "long-run" {
				t.Fatalf("event of run %q in long-run's page", event.RunID)
			}
		}
		if len(result.Events) > 0 {
			after = result.Events[len(result.Events)-1].Sequence
		}
		if !result.Truncated {
			break
		}
		if len(result.Events) != page {
			t.Fatalf("a truncated page returned %d events, want %d", len(result.Events), page)
		}
	}
	if len(got) != total {
		t.Fatalf("read %d events, want %d: %v", len(got), total, got)
	}
	for i, sequence := range got {
		if sequence != uint64(i+1) {
			t.Fatalf("events not contiguous and ordered at %d: %v", i, got)
		}
	}

	// Caught up: an empty page that does not move the position.
	result, err := service.Events(t.Context(), EventsQuery{RunID: "long-run", After: after, Limit: page})
	if err != nil || len(result.Events) != 0 || result.Truncated {
		t.Fatalf("caught-up read = %+v, %v", result, err)
	}
	// A position beyond the end is also empty, not an error.
	if result, err = service.Events(t.Context(), EventsQuery{RunID: "long-run", After: 1000, Limit: page}); err != nil || len(result.Events) != 0 {
		t.Fatalf("read past the end = %+v, %v", result, err)
	}
}

func TestEventsRejectsBadQueriesAndMissingRuns(t *testing.T) {
	database, err := persistence.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	state, err := persistence.NewWorkflowStateStore(database)
	if err != nil {
		t.Fatal(err)
	}
	service := Service{State: state}
	if _, err := service.Events(t.Context(), EventsQuery{}); !errors.Is(err, ErrInvalidGraphQuery) {
		t.Fatalf("empty run id: %v", err)
	}
	if _, err := service.Events(t.Context(), EventsQuery{RunID: "r", Limit: maximumEventLimit + 1}); !errors.Is(err, ErrInvalidGraphQuery) {
		t.Fatalf("oversized limit: %v", err)
	}
	if _, err := service.Events(t.Context(), EventsQuery{RunID: "missing"}); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("missing run: %v", err)
	}
	if _, err := (Service{}).Events(t.Context(), EventsQuery{RunID: "r"}); !errors.Is(err, ErrInvalidGraphQuery) {
		t.Fatalf("no state reader: %v", err)
	}
}
