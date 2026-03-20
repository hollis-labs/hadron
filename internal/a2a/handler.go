package a2a

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// RunStore is the subset of persistence.Store needed by the A2A handler.
type RunStore interface {
	GetRun(ctx context.Context, id string) (persistence.RunRecord, error)
	ListRunEvents(ctx context.Context, runID string, limit int) ([]persistence.RunEventRecord, error)
}

// Runner enqueues blueprint runs.
type Runner interface {
	Enqueue(ctx context.Context, req execution.Request) error
}

// BlueprintResolver maps a skill name/slug to an absolute blueprint file path.
type BlueprintResolver interface {
	Resolve(name string) (string, error)
}

// Handler implements A2A task submission and retrieval.
type Handler struct {
	store    RunStore
	runner   Runner
	resolver BlueprintResolver

	mu    sync.RWMutex
	tasks map[string]string // a2a task ID -> hadron run ID

	seq atomic.Uint64
}

// NewHandler creates a new A2A handler.
func NewHandler(store RunStore, runner Runner, resolver BlueprintResolver) *Handler {
	return &Handler{
		store:    store,
		runner:   runner,
		resolver: resolver,
		tasks:    make(map[string]string),
	}
}

// SubmitTask resolves the skill to a blueprint, enqueues a run, and returns
// a task response with state "submitted".
func (h *Handler) SubmitTask(ctx context.Context, req TaskRequest) (*TaskResponse, error) {
	if req.Skill == "" {
		return nil, fmt.Errorf("skill is required")
	}

	// Resolve skill name to blueprint path.
	bpPath, err := h.resolver.Resolve(req.Skill)
	if err != nil {
		return nil, fmt.Errorf("unknown skill %q: %w", req.Skill, err)
	}

	// Generate task ID if not provided.
	taskID := req.ID
	if taskID == "" {
		taskID = h.nextTaskID()
	}

	// Generate a run ID and enqueue.
	runID := fmt.Sprintf("a2a-%s", taskID)
	if err := h.runner.Enqueue(ctx, execution.Request{
		RunID:         runID,
		WorkspaceID:   "default",
		BlueprintPath: bpPath,
		Inputs:        req.Input,
	}); err != nil {
		return nil, fmt.Errorf("enqueue run: %w", err)
	}

	// Store the mapping.
	h.mu.Lock()
	h.tasks[taskID] = runID
	h.mu.Unlock()

	return &TaskResponse{
		ID: taskID,
		Status: TaskStatus{
			State:   "submitted",
			Message: "task accepted and queued for execution",
		},
	}, nil
}

// GetTask returns the current status and result for a task.
func (h *Handler) GetTask(ctx context.Context, taskID string) (*TaskResponse, error) {
	h.mu.RLock()
	runID, ok := h.tasks[taskID]
	h.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("task not found")
	}

	rec, err := h.store.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("get run: %w", err)
	}

	resp := &TaskResponse{
		ID:     taskID,
		Status: mapRunStatus(rec),
	}

	// If completed, try to collect output from run events.
	if rec.Status == "success" {
		output := h.collectOutput(ctx, runID)
		resp.Result = &TaskResult{
			OutputType: "application/json",
			Output:     output,
		}
	}

	// If failed, include error in result.
	if rec.Status == "failed" && rec.ErrorMessage.Valid {
		resp.Result = &TaskResult{
			OutputType: "application/json",
			Output:     map[string]string{"error": rec.ErrorMessage.String},
		}
	}

	return resp, nil
}

// mapRunStatus translates Hadron run statuses to A2A task states.
func mapRunStatus(rec persistence.RunRecord) TaskStatus {
	switch rec.Status {
	case "queued":
		return TaskStatus{State: "submitted", Message: "task is queued"}
	case "running":
		return TaskStatus{State: "working", Message: "task is executing"}
	case "success":
		return TaskStatus{State: "completed", Message: "task completed successfully"}
	case "failed":
		msg := "task failed"
		if rec.ErrorMessage.Valid {
			msg = rec.ErrorMessage.String
		}
		return TaskStatus{State: "failed", Message: msg}
	case "canceled":
		return TaskStatus{State: "canceled", Message: "task was canceled"}
	default:
		return TaskStatus{State: "working", Message: "status: " + rec.Status}
	}
}

// collectOutput gathers the last log events from a completed run as output.
func (h *Handler) collectOutput(ctx context.Context, runID string) map[string]any {
	events, err := h.store.ListRunEvents(ctx, runID, 100)
	if err != nil {
		return map[string]any{"events_error": err.Error()}
	}

	logs := make([]string, 0, len(events))
	for _, ev := range events {
		if ev.Message.Valid && ev.Message.String != "" {
			logs = append(logs, ev.Message.String)
		}
	}

	return map[string]any{
		"run_id": runID,
		"logs":   logs,
	}
}

func (h *Handler) nextTaskID() string {
	n := h.seq.Add(1)
	return fmt.Sprintf("task-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}

// ── Convenience: InputJSON parsing helper ─────────────────────────────────────

// ParseInputJSON is a helper to parse the JSON input string from a RunRecord.
func ParseInputJSON(raw string) map[string]any {
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return map[string]any{}
	}
	return m
}

// Ensure sql.NullString is referenced (used in mapRunStatus via rec.ErrorMessage).
var _ = sql.NullString{}
