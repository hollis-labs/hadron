package scheduler

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

type fakeStore struct {
	mu       sync.Mutex
	due      []persistence.ScheduleRecord
	claimed  map[string]bool
	claimCnt int
	setNext  map[string]time.Time
}

func (f *fakeStore) ListDueSchedules(_ context.Context, _ time.Time, _ int) ([]persistence.ScheduleRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]persistence.ScheduleRecord, len(f.due))
	copy(out, f.due)
	return out, nil
}

func (f *fakeStore) ClaimAndUpdateScheduleRun(_ context.Context, id string, _ time.Time, _ time.Time, _ time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimed == nil {
		f.claimed = map[string]bool{}
	}
	if f.claimed[id] {
		return false, nil
	}
	f.claimed[id] = true
	f.claimCnt++
	return true, nil
}

func (f *fakeStore) SetScheduleNextRun(_ context.Context, id string, nextRun time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setNext == nil {
		f.setNext = map[string]time.Time{}
	}
	f.setNext[id] = nextRun
	return nil
}

func (f *fakeStore) DisableSchedule(_ context.Context, _ string) error {
	return nil
}

type fakeRunner struct {
	mu   sync.Mutex
	reqs []execution.Request
}

func (f *fakeRunner) Enqueue(_ context.Context, req execution.Request) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return nil
}

func TestValidateCron(t *testing.T) {
	if err := ValidateCron("*/5 * * * *"); err != nil {
		t.Fatalf("expected valid cron: %v", err)
	}
	if err := ValidateCron("bad"); err == nil {
		t.Fatalf("expected invalid cron error")
	}
}

func TestTickDispatchesDueScheduleOnce(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &fakeStore{due: []persistence.ScheduleRecord{{
		ID:            "sch-1",
		BlueprintPath: "./bp.yaml",
		CronExpr:      "* * * * *",
		NextRunAt:     sql.NullString{String: now.Format(time.RFC3339), Valid: true},
	}}}
	runner := &fakeRunner{}
	eng := New(store, runner)

	if err := eng.TickNow(context.Background()); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	if err := eng.TickNow(context.Background()); err != nil {
		t.Fatalf("tick 2 failed: %v", err)
	}

	runner.mu.Lock()
	count := len(runner.reqs)
	runner.mu.Unlock()
	if count != 1 {
		t.Fatalf("expected 1 dispatch, got %d", count)
	}
}

type failOnceRunner struct {
	mu      sync.Mutex
	failed  bool
	success []execution.Request
}

func (f *failOnceRunner) Enqueue(_ context.Context, req execution.Request) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.failed && req.BlueprintPath == "./bp-fail-once.yaml" {
		f.failed = true
		return sql.ErrNoRows
	}
	f.success = append(f.success, req)
	return nil
}

func TestTick_RequeuesWhenEnqueueFails(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &fakeStore{due: []persistence.ScheduleRecord{{
		ID:            "sch-err",
		WorkspaceID:   "alpha",
		BlueprintPath: "./bp-fail-once.yaml",
		CronExpr:      "* * * * *",
		NextRunAt:     sql.NullString{String: now.Format(time.RFC3339), Valid: true},
	}}}
	runner := &failOnceRunner{}
	eng := New(store, runner)

	if err := eng.tick(context.Background(), now); err != nil {
		t.Fatalf("tick failed: %v", err)
	}

	store.mu.Lock()
	requeued, ok := store.setNext["sch-err"]
	store.mu.Unlock()
	if !ok {
		t.Fatalf("expected schedule to be requeued after enqueue error")
	}
	if !requeued.Equal(now) {
		t.Fatalf("expected next_run reset to %s, got %s", now.Format(time.RFC3339), requeued.Format(time.RFC3339))
	}
}

func TestTick_DispatchesAcrossWorkspaces(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := &fakeStore{due: []persistence.ScheduleRecord{
		{
			ID:            "sch-a",
			WorkspaceID:   "alpha",
			BlueprintPath: "./a.yaml",
			CronExpr:      "* * * * *",
			NextRunAt:     sql.NullString{String: now.Format(time.RFC3339), Valid: true},
		},
		{
			ID:            "sch-b",
			WorkspaceID:   "beta",
			BlueprintPath: "./b.yaml",
			CronExpr:      "* * * * *",
			NextRunAt:     sql.NullString{String: now.Format(time.RFC3339), Valid: true},
		},
	}}
	runner := &fakeRunner{}
	eng := New(store, runner)
	if err := eng.tick(context.Background(), now); err != nil {
		t.Fatalf("tick failed: %v", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.reqs) != 2 {
		t.Fatalf("expected 2 dispatches, got %d", len(runner.reqs))
	}
	if runner.reqs[0].WorkspaceID == runner.reqs[1].WorkspaceID {
		t.Fatalf("expected dispatches for distinct workspaces")
	}
}

func TestStartStop(t *testing.T) {
	eng := New(&fakeStore{}, &fakeRunner{})
	eng.Start()
	st := eng.Status()
	if !st.Running {
		t.Fatalf("expected running=true")
	}
	eng.Stop()
	st = eng.Status()
	if st.Running {
		t.Fatalf("expected running=false")
	}
}
