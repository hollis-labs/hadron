package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/telemetry"
)

// NewManager creates a Manager and starts its worker goroutines.
// tel may be nil, in which case structured JSONL telemetry is skipped.
func NewManager(store RunStore, settings SettingsValidator, workers int, logDir string, tel *telemetry.Logger) *Manager {
	if workers <= 0 {
		workers = 1
	}
	m := &Manager{
		store:    store,
		settings: settings,
		workers:  workers,
		queue:    make(chan Request, 128),
		logDir:   logDir,
		tel:      tel,
		subs:     make(map[int]chan Event),
		active:   make(map[string]context.CancelFunc),
	}
	for i := 0; i < workers; i++ {
		m.wg.Add(1)
		go m.worker()
	}
	return m
}

// Subscribe returns a channel that receives all events and a cancel func.
func (m *Manager) Subscribe(buffer int) (<-chan Event, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan Event, buffer)
	m.subMu.Lock()
	id := m.nextSubID
	m.nextSubID++
	m.subs[id] = ch
	m.subMu.Unlock()

	cancel := func() {
		m.subMu.Lock()
		if c, ok := m.subs[id]; ok {
			delete(m.subs, id)
			close(c)
		}
		m.subMu.Unlock()
	}
	return ch, cancel
}

// Close drains the queue, waits for workers, and closes all subscriber channels.
func (m *Manager) Close() {
	if m.closed.CompareAndSwap(false, true) {
		close(m.queue)
		m.wg.Wait()
		m.subMu.Lock()
		for id, ch := range m.subs {
			delete(m.subs, id)
			close(ch)
		}
		m.subMu.Unlock()
	}
}

// Cancel requests cancellation of an in-progress run.
func (m *Manager) Cancel(runID string) bool {
	if runID == "" {
		return false
	}
	m.activeMu.Lock()
	cancel, ok := m.active[runID]
	m.activeMu.Unlock()
	if !ok {
		return false
	}
	cancel()
	m.emit(runID, "", "", "canceled", "run cancellation requested")
	return true
}

// Enqueue persists the run record and adds it to the work queue.
func (m *Manager) Enqueue(ctx context.Context, req Request) error {
	if req.RunID == "" {
		return fmt.Errorf("run id is required")
	}
	if req.BlueprintPath == "" {
		return fmt.Errorf("blueprint path is required")
	}
	if m.closed.Load() {
		return fmt.Errorf("manager is closed")
	}

	inputJSON := "{}"
	if len(req.Inputs) > 0 {
		b, err := json.Marshal(req.Inputs)
		if err != nil {
			return fmt.Errorf("marshal run inputs: %w", err)
		}
		inputJSON = string(b)
	}

	now := time.Now().UTC()
	workspaceID := req.WorkspaceID
	if workspaceID == "" {
		workspaceID = "default"
	}
	if err := m.store.CreateRun(ctx, persistence.RunRecord{
		ID:            req.RunID,
		WorkspaceID:   workspaceID,
		BlueprintPath: req.BlueprintPath,
		Status:        "queued",
		InputJSON:     inputJSON,
		CreatedAt:     now,
	}); err != nil {
		return err
	}
	m.emit(req.RunID, "", "", "queued", "run queued")

	select {
	case m.queue <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
