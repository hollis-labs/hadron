package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

// eventSignal wakes readers waiting for new events. It is a hint: a waiter
// always re-reads the database, which stays the only source of truth, so a
// missed or spurious wake costs a read, never an event.
type eventSignal struct {
	mu sync.Mutex
	ch chan struct{}
}

func (e *eventSignal) channel() <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ch == nil {
		e.ch = make(chan struct{})
	}
	return e.ch
}

// broadcast wakes every current waiter. Later waiters get a fresh channel.
func (e *eventSignal) broadcast() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ch != nil {
		close(e.ch)
		e.ch = nil
	}
}

// signals shares one eventSignal between every WorkflowStateStore opened on the
// same database handle, so a write through one store wakes readers of another.
var signals sync.Map // *sql.DB -> *eventSignal

func signalFor(db *sql.DB) *eventSignal {
	if existing, ok := signals.Load(db); ok {
		return existing.(*eventSignal)
	}
	created, _ := signals.LoadOrStore(db, &eventSignal{})
	return created.(*eventSignal)
}

// EventsChanged returns a channel that is closed after the next committed
// write on this database. Take it before reading, then read, then wait on it,
// so a write between the read and the wait is not missed. Writers in another
// process do not wake it; callers also poll.
func (s *WorkflowStateStore) EventsChanged() <-chan struct{} {
	return signalFor(s.db).channel()
}

// EventHighWater returns the last event sequence allocated for a run, or zero
// when the run has no events.
func (s *WorkflowStateStore) EventHighWater(ctx context.Context, runID workflowruntime.RunID) (uint64, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return 0, err
	}
	var last int64
	err := s.db.QueryRowContext(ctx, `SELECT last_sequence FROM workflow_event_sequences WHERE run_id = ?`, string(runID)).Scan(&last)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load workflow event high water: %w", err)
	}
	if last < 0 {
		return 0, workflowInvalid(errors.New("negative event high water"))
	}
	return uint64(last), nil
}

// StoreGeneration returns the identity of this database's event log. It
// changes only when a different database is created, never on restart.
func (s *WorkflowStateStore) StoreGeneration(ctx context.Context) (string, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return "", err
	}
	var generation string
	if err := s.db.QueryRowContext(ctx, `SELECT generation FROM store_generation WHERE id = 1`).Scan(&generation); err != nil {
		return "", fmt.Errorf("load store generation: %w", err)
	}
	return generation, nil
}
