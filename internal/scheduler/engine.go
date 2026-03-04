package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/robfig/cron/v3"
)

type Store interface {
	ListDueSchedules(ctx context.Context, now time.Time, limit int) ([]persistence.ScheduleRecord, error)
	ClaimAndUpdateScheduleRun(ctx context.Context, id string, expectedNext time.Time, lastRun time.Time, nextRun time.Time) (bool, error)
	SetScheduleNextRun(ctx context.Context, id string, nextRun time.Time) error
	DisableSchedule(ctx context.Context, id string) error
}

type Runner interface {
	Enqueue(ctx context.Context, req execution.Request) error
}

type Status struct {
	Running      bool      `json:"running"`
	LastTickAt   time.Time `json:"last_tick_at"`
	Dispatches   int64     `json:"dispatches"`
	WorkerErrors int64     `json:"worker_errors"`
}

type Engine struct {
	store   Store
	runner  Runner
	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}
	status  Status
}

func New(store Store, runner Runner) *Engine {
	return &Engine{store: store, runner: runner}
}

func ValidateCron(expr string) error {
	_, err := cron.ParseStandard(strings.TrimSpace(expr))
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}
	return nil
}

func NextRun(expr string, from time.Time) (time.Time, error) {
	sched, err := cron.ParseStandard(strings.TrimSpace(expr))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse cron expression: %w", err)
	}
	return sched.Next(from), nil
}

func (e *Engine) Start() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running {
		return
	}
	e.running = true
	e.stopCh = make(chan struct{})
	e.doneCh = make(chan struct{})
	go e.loop()
}

func (e *Engine) Stop() {
	e.mu.Lock()
	if !e.running {
		e.mu.Unlock()
		return
	}
	stopCh := e.stopCh
	doneCh := e.doneCh
	e.running = false
	e.mu.Unlock()

	close(stopCh)
	<-doneCh
}

func (e *Engine) TickNow(ctx context.Context) error {
	return e.tick(ctx, time.Now().UTC())
}

func (e *Engine) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.status
	st.Running = e.running
	return st
}

func (e *Engine) loop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	defer close(e.doneCh)

	for {
		select {
		case <-ticker.C:
			_ = e.tick(context.Background(), time.Now().UTC())
		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) tick(ctx context.Context, now time.Time) error {
	e.mu.Lock()
	e.status.LastTickAt = now
	e.mu.Unlock()

	due, err := e.store.ListDueSchedules(ctx, now, 100)
	if err != nil {
		e.bumpWorkerErrors()
		return err
	}

	for _, sch := range due {
		if !sch.NextRunAt.Valid {
			continue
		}
		expectedNext, err := time.Parse(time.RFC3339, sch.NextRunAt.String)
		if err != nil {
			e.bumpWorkerErrors()
			continue
		}

		// One-time schedule: cron_expr is empty; disable after firing
		isOneTime := strings.TrimSpace(sch.CronExpr) == ""
		var next time.Time
		if isOneTime {
			// Use a far-future time as the "next" — schedule will be disabled after enqueue
			next = now.Add(100 * 365 * 24 * time.Hour)
		} else {
			next, err = NextRun(sch.CronExpr, now)
			if err != nil {
				e.bumpWorkerErrors()
				continue
			}
		}

		claimed, err := e.store.ClaimAndUpdateScheduleRun(ctx, sch.ID, expectedNext, now, next)
		if err != nil {
			e.bumpWorkerErrors()
			continue
		}
		if !claimed {
			continue
		}

		runID := fmt.Sprintf("sched-%s-%d", sch.ID, now.Unix())
		err = e.runner.Enqueue(ctx, execution.Request{
			WorkspaceID:   sch.WorkspaceID,
			RunID:         runID,
			BlueprintPath: sch.BlueprintPath,
			Inputs:        map[string]any{},
		})
		if err != nil {
			_ = e.store.SetScheduleNextRun(ctx, sch.ID, expectedNext)
			if strings.Contains(strings.ToLower(err.Error()), "unique") || err == sql.ErrNoRows {
				continue
			}
			e.bumpWorkerErrors()
			continue
		}
		e.bumpDispatches()

		// Disable one-time schedules after successful dispatch
		if isOneTime {
			_ = e.store.DisableSchedule(ctx, sch.ID)
		}
	}
	return nil
}

func (e *Engine) bumpDispatches() {
	e.mu.Lock()
	e.status.Dispatches++
	e.mu.Unlock()
}

func (e *Engine) bumpWorkerErrors() {
	e.mu.Lock()
	e.status.WorkerErrors++
	e.mu.Unlock()
}
