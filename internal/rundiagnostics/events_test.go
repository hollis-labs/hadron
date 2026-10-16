package rundiagnostics

import (
	"context"
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

// eventsFixture is a real SQLite store with one run and a helper to append.
type eventsFixture struct {
	t     *testing.T
	state *persistence.WorkflowStateStore
	base  time.Time
}

func newEventsFixture(t *testing.T, runs ...workflowruntime.RunID) *eventsFixture {
	t.Helper()
	database, err := persistence.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	state, err := persistence.NewWorkflowStateStore(database)
	if err != nil {
		t.Fatal(err)
	}
	f := &eventsFixture{t: t, state: state, base: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)}
	ref := workflowruntime.PlanRef{ID: "events-plan", Version: "v1", Digest: values.SHA256Digest([]byte("events-plan")), SchemaVersion: compile.ExecutionPlanSchemaVersion}
	if recordErr := state.RecordPlan(t.Context(), ref); recordErr != nil {
		t.Fatal(recordErr)
	}
	for _, id := range runs {
		if _, _, createErr := state.CreateRun(t.Context(), workflowruntime.CreateRunRequest{ID: id, Plan: ref, Status: workflowruntime.RunPending, StartIdempotencyKey: string(id) + "-start", CreatedAt: f.base}); createErr != nil {
			t.Fatal(createErr)
		}
	}
	return f
}

func (f *eventsFixture) append(run workflowruntime.RunID, n int) {
	f.t.Helper()
	for range n {
		if _, err := f.state.AppendEvent(f.t.Context(), workflowruntime.AppendEventRequest{RunID: run, Type: "adapter.observation", OccurredAt: f.base, Redaction: values.RedactionPublic, Retention: values.RetentionRun}); err != nil {
			f.t.Error(err)
			return
		}
	}
}

func TestEventsWaitReturnsWhenAnEventArrivesAndNotBefore(t *testing.T) {
	f := newEventsFixture(t, "wait-run")
	service := Service{State: f.state}

	started := time.Now()
	result, err := service.Events(t.Context(), EventsQuery{RunID: "wait-run", Wait: 150 * time.Millisecond})
	if err != nil || len(result.Events) != 0 || time.Since(started) < 100*time.Millisecond {
		t.Fatalf("an idle wait should run its course: events=%d err=%v after %s", len(result.Events), err, time.Since(started))
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		f.append("wait-run", 1)
	}()
	started = time.Now()
	result, err = service.Events(t.Context(), EventsQuery{RunID: "wait-run", Wait: 10 * time.Second})
	if err != nil || len(result.Events) != 1 || result.Events[0].Sequence != 1 {
		t.Fatalf("wait result = %+v, %v", result, err)
	}
	if took := time.Since(started); took > 3*time.Second {
		t.Fatalf("the wait was not woken by the write: %s", took)
	}
}

func TestEventsWaitIsCancelledByItsContext(t *testing.T) {
	f := newEventsFixture(t, "cancel-run")
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := (Service{State: f.state}).Events(ctx, EventsQuery{RunID: "cancel-run", Wait: 20 * time.Second})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 3*time.Second {
		t.Fatalf("err = %v after %s", err, time.Since(started))
	}
}

func TestEventsDoNotWaitOnAFinishedRunAndSayItIsFinished(t *testing.T) {
	f := newEventsFixture(t, "done-run")
	for _, to := range []workflowruntime.RunStatus{workflowruntime.RunRunning, workflowruntime.RunSucceeded} {
		run, err := f.state.LoadRun(t.Context(), "done-run")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: "done-run", ExpectedGeneration: run.Generation, To: to, At: f.base.Add(time.Minute)}); err != nil {
			t.Fatalf("transition to %s: %v", to, err)
		}
	}
	service := Service{State: f.state}
	all, err := service.Events(t.Context(), EventsQuery{RunID: "done-run"})
	if err != nil || len(all.Events) == 0 || !all.Terminal {
		t.Fatalf("events of a finished run: %+v, %v", all, err)
	}
	last := all.Events[len(all.Events)-1].Sequence

	started := time.Now()
	tail, err := service.Events(t.Context(), EventsQuery{RunID: "done-run", After: last, Wait: 10 * time.Second})
	if err != nil || len(tail.Events) != 0 || !tail.Terminal || tail.Truncated {
		t.Fatalf("tail of a finished run: %+v, %v", tail, err)
	}
	if took := time.Since(started); took > 3*time.Second {
		t.Fatalf("waited %s on a finished run", took)
	}
}

func TestEventsReportGapsInsteadOfGuessing(t *testing.T) {
	f := newEventsFixture(t, "gap-run")
	f.append("gap-run", 3)
	service := Service{State: f.state}

	ahead, err := service.Events(t.Context(), EventsQuery{RunID: "gap-run", After: 10})
	if err != nil || ahead.Gap == nil || ahead.Gap.Reason != GapCursorAhead || ahead.Gap.HighWater != 3 || ahead.Gap.EarliestAvailable != 1 || len(ahead.Events) != 0 {
		t.Fatalf("cursor ahead: %+v, %v", ahead, err)
	}
	caughtUp, err := service.Events(t.Context(), EventsQuery{RunID: "gap-run", After: 3})
	if err != nil || caughtUp.Gap != nil || len(caughtUp.Events) != 0 {
		t.Fatalf("caught up at the high water is not a gap: %+v, %v", caughtUp, err)
	}
	generation := ahead.Generation
	if len(generation) != 32 {
		t.Fatalf("generation = %q", generation)
	}
	other, err := service.Events(t.Context(), EventsQuery{RunID: "gap-run", After: 1, Generation: "0123456789abcdef0123456789abcdef"})
	if err != nil || other.Gap == nil || other.Gap.Reason != GapGenerationChanged || len(other.Events) != 0 {
		t.Fatalf("other generation: %+v, %v", other, err)
	}
	same, err := service.Events(t.Context(), EventsQuery{RunID: "gap-run", After: 1, Generation: generation})
	if err != nil || same.Gap != nil || len(same.Events) != 2 {
		t.Fatalf("same generation: %+v, %v", same, err)
	}
}

func TestEventsRejectAWaitOverTheLimit(t *testing.T) {
	f := newEventsFixture(t, "limit-run")
	if _, err := (Service{State: f.state}).Events(t.Context(), EventsQuery{RunID: "limit-run", Wait: time.Minute}); !errors.Is(err, ErrInvalidGraphQuery) {
		t.Fatalf("err = %v", err)
	}
}

// Followers started at different points and a writer appending throughout
// must each see every event exactly once, in order.
func TestEventsFollowersSeeEveryEventOnceUnderConcurrentWrites(t *testing.T) {
	f := newEventsFixture(t, "follow-run")
	service := Service{State: f.state}
	const total = 60
	follow := func(start uint64) []uint64 {
		var got []uint64
		after := start
		deadline := time.Now().Add(20 * time.Second)
		for uint64(len(got))+start < total && time.Now().Before(deadline) {
			result, err := service.Events(t.Context(), EventsQuery{RunID: "follow-run", After: after, Limit: 7, Wait: 500 * time.Millisecond})
			if err != nil {
				t.Error(err)
				return got
			}
			for _, e := range result.Events {
				got = append(got, e.Sequence)
				after = e.Sequence
			}
		}
		return got
	}
	results := make(chan []uint64, 3)
	for _, start := range []uint64{0, 10, 30} {
		go func() { results <- follow(start) }()
	}
	for range total {
		f.append("follow-run", 1)
	}
	for range 3 {
		got := <-results
		for i := 1; i < len(got); i++ {
			if got[i] != got[i-1]+1 {
				t.Fatalf("follower lost or repeated an event: %v", got)
			}
		}
		if len(got) == 0 || got[len(got)-1] != total {
			t.Fatalf("follower stopped early: %v", got)
		}
	}
}
