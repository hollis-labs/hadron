package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/hollis-labs/tiamat-otel/propagation"
)

// ── Store interfaces ──────────────────────────────────────────────────────────

type RunStore interface {
	CreateRun(ctx context.Context, rec persistence.RunRecord) error
	GetRun(ctx context.Context, id string) (persistence.RunRecord, error)
	ListRunsByWorkspaceFiltered(ctx context.Context, workspaceID string, limit int, cursorID string, createdAfter, createdBefore *time.Time) ([]persistence.RunRecord, error)
	ListRunEvents(ctx context.Context, runID string, limit int) ([]persistence.RunEventRecord, error)
	ListRunEventsFiltered(ctx context.Context, runID string, limit int, cursorID int64, createdAfter, createdBefore *time.Time) ([]persistence.RunEventRecord, error)
}

type ScheduleStore interface {
	GetSchedule(ctx context.Context, id string) (persistence.ScheduleRecord, error)
	ListSchedulesByWorkspace(ctx context.Context, workspaceID string) ([]persistence.ScheduleRecord, error)
	CreateSchedule(ctx context.Context, rec persistence.ScheduleRecord) error
	UpdateScheduleEnabledAndNext(ctx context.Context, id string, enabled bool, nextRun *time.Time) error
	UpdateScheduleFields(ctx context.Context, id string, name, cronExpr, blueprintPath string, enabled bool, nextRun *time.Time) error
	DeleteSchedule(ctx context.Context, id string) error
}

type PipelineStore interface {
	GetPipelineRun(ctx context.Context, id string) (persistence.PipelineRunRecord, error)
	ListPipelineRunsByWorkspace(ctx context.Context, workspaceID string, limit int) ([]persistence.PipelineRunRecord, error)
	ListPipelineStageRuns(ctx context.Context, pipelineRunID string) ([]persistence.PipelineStageRunRecord, error)
}

type WorkspaceStore interface {
	CreateWorkspace(ctx context.Context, id, name string) error
	GetWorkspace(ctx context.Context, id string) (persistence.WorkspaceRecord, error)
	ListWorkspaces(ctx context.Context) ([]persistence.WorkspaceRecord, error)
}

// ── Other interfaces ──────────────────────────────────────────────────────────

type Runner interface {
	Enqueue(ctx context.Context, req execution.Request) error
	Cancel(runID string) bool
}

type Scheduler interface {
	Start()
	Stop()
	Status() scheduler.Status
	TickNow(ctx context.Context) error
}

type PipelineRunner interface {
	Start(ctx context.Context, pipelineRunID, pipelinePath, workspaceID string) error
}

// ── Dependencies and Server ───────────────────────────────────────────────────

type Dependencies struct {
	Runs      RunStore
	Schedules ScheduleStore
	Pipelines PipelineStore
	Workspaces WorkspaceStore
	Runner    Runner
	Scheduler Scheduler
	Pipeline  PipelineRunner
}

type Server struct {
	httpServer  *http.Server
	handler     http.Handler
	deps        Dependencies
	runSeq      atomic.Uint64
	scheduleSeq atomic.Uint64
	pipelineSeq atomic.Uint64
}

// Handler returns the underlying HTTP handler (useful for testing with httptest).
func (s *Server) Handler() http.Handler {
	return s.handler
}

func NewServer(addr string, deps Dependencies) *Server {
	s := &Server{deps: deps}
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/health", s.handleHealth)

	// Workspaces
	mux.HandleFunc("/v1/workspaces", s.handleWorkspaces)
	mux.HandleFunc("/v1/workspaces/", s.handleWorkspaceByID)

	// Runs
	mux.HandleFunc("/v1/runs", s.handleRuns)
	mux.HandleFunc("/v1/runs/", s.handleRunByID)

	// Schedules
	mux.HandleFunc("/v1/schedules", s.handleSchedules)
	mux.HandleFunc("/v1/schedules/", s.handleScheduleByID)

	// Pipelines
	mux.HandleFunc("/v1/pipelines", s.handlePipelines)
	mux.HandleFunc("/v1/pipelines/", s.handlePipelineByID)

	// Blueprints
	mux.HandleFunc("/v1/blueprints/validate", s.handleBlueprintValidate)

	// 404 catch-all
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})

	s.handler = corsMiddleware(propagation.HTTPMiddleware(mux))
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.handler,
	}
	return s
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// corsMiddleware handles CORS preflight (OPTIONS) requests and sets
// permissive headers for local development. The Wails webview in production
// mode loads from a wails:// origin, so cross-origin requests to the daemon
// require proper CORS headers.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ── Health ────────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"version": "0.4.0",
		"service": "hadrond",
	})
}

// ── Workspaces ────────────────────────────────────────────────────────────────

func (s *Server) handleWorkspaces(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listWorkspaces(w, r)
	case http.MethodPost:
		s.createWorkspace(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleWorkspaceByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/workspaces/")
	if id == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	rec, err := s.deps.Workspaces.GetWorkspace(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "workspace not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toWorkspaceResponse(rec))
}

func (s *Server) listWorkspaces(w http.ResponseWriter, r *http.Request) {
	items, err := s.deps.Workspaces.ListWorkspaces(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, ws := range items {
		out = append(out, toWorkspaceResponse(ws))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "next_cursor": nil})
}

func (s *Server) createWorkspace(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	body.ID = strings.TrimSpace(body.ID)
	body.Name = strings.TrimSpace(body.Name)
	if body.ID == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}
	if err := s.deps.Workspaces.CreateWorkspace(r.Context(), body.ID, body.Name); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	rec, err := s.deps.Workspaces.GetWorkspace(r.Context(), body.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, toWorkspaceResponse(rec))
}

// ── Runs ──────────────────────────────────────────────────────────────────────

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listRuns(w, r)
	case http.MethodPost:
		s.createRun(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleRunByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/runs/")
	parts := strings.SplitN(path, "/", 2)
	runID := parts[0]
	if runID == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	sub := ""
	if len(parts) == 2 {
		sub = parts[1]
	}

	switch {
	case sub == "" && r.Method == http.MethodGet:
		s.getRun(w, r, runID)
	case sub == "" && r.Method == http.MethodDelete:
		s.cancelRun(w, r, runID)
	case sub == "events" && r.Method == http.MethodGet:
		s.listRunEvents(w, r, runID)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	wsID := q.Get("workspace_id")
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	cursor := q.Get("cursor")

	items, err := s.deps.Runs.ListRunsByWorkspaceFiltered(r.Context(), wsID, limit+1, cursor, nil, nil)
	if err != nil {
		if isInvalidCursor(err) {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var nextCursor any
	if len(items) > limit {
		items = items[:limit]
		nextCursor = items[len(items)-1].ID
	}

	out := make([]map[string]any, 0, len(items))
	for _, rec := range items {
		out = append(out, toRunResponse(rec))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "next_cursor": nextCursor})
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkspaceID   string         `json:"workspace_id"`
		BlueprintPath string         `json:"blueprint_path"`
		Inputs        map[string]any `json:"inputs"`
		DryRun        bool           `json:"dry_run"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.BlueprintPath == "" {
		writeError(w, http.StatusBadRequest, "blueprint_path is required")
		return
	}
	wsID := body.WorkspaceID
	if wsID == "" {
		wsID = "default"
	}

	// Validate blueprint exists + parse
	bp, err := blueprint.ParseFile(body.BlueprintPath)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid blueprint: "+err.Error())
		return
	}
	normalized, err := blueprint.NormalizeInputs(bp, body.Inputs)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "invalid inputs: "+err.Error())
		return
	}

	runID := s.nextRunID()
	if err := s.deps.Runner.Enqueue(r.Context(), execution.Request{
		RunID:         runID,
		WorkspaceID:   wsID,
		BlueprintPath: body.BlueprintPath,
		Inputs:        normalized,
		DryRun:        body.DryRun,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	rec, err := s.deps.Runs.GetRun(r.Context(), runID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, toRunResponse(rec))
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request, runID string) {
	rec, err := s.deps.Runs.GetRun(r.Context(), runID)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toRunResponse(rec))
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request, runID string) {
	if s.deps.Runner == nil {
		writeError(w, http.StatusServiceUnavailable, "runner unavailable")
		return
	}
	// Verify run exists
	rec, err := s.deps.Runs.GetRun(r.Context(), runID)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if rec.Status != "queued" && rec.Status != "running" {
		writeError(w, http.StatusConflict, "run is not in a cancellable state")
		return
	}
	s.deps.Runner.Cancel(runID)
	writeJSON(w, http.StatusOK, map[string]string{"run_id": runID, "status": "cancellation_requested"})
}

func (s *Server) listRunEvents(w http.ResponseWriter, r *http.Request, runID string) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 200
	}
	var cursorID int64
	if c := strings.TrimSpace(q.Get("cursor")); c != "" {
		cursorID, _ = strconv.ParseInt(c, 10, 64)
	}

	// Verify run exists
	if _, err := s.deps.Runs.GetRun(r.Context(), runID); err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items, err := s.deps.Runs.ListRunEventsFiltered(r.Context(), runID, limit+1, cursorID, nil, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var nextCursor any
	if len(items) > limit {
		items = items[:limit]
		nextCursor = strconv.FormatInt(items[len(items)-1].ID, 10)
	}

	out := make([]map[string]any, 0, len(items))
	for _, ev := range items {
		out = append(out, toRunEventResponse(ev))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "next_cursor": nextCursor})
}

// ── Schedules ─────────────────────────────────────────────────────────────────

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listSchedules(w, r)
	case http.MethodPost:
		s.createSchedule(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleScheduleByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/schedules/")
	if id == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.getSchedule(w, r, id)
	case http.MethodPatch:
		s.patchSchedule(w, r, id)
	case http.MethodDelete:
		s.deleteSchedule(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) listSchedules(w http.ResponseWriter, r *http.Request) {
	wsID := r.URL.Query().Get("workspace_id")
	items, err := s.deps.Schedules.ListSchedulesByWorkspace(r.Context(), wsID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, sc := range items {
		out = append(out, toScheduleResponse(sc))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "next_cursor": nil})
}

func (s *Server) createSchedule(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkspaceID   string `json:"workspace_id"`
		Name          string `json:"name"`
		BlueprintPath string `json:"blueprint_path"`
		CronExpr      string `json:"cron_expr"`
		RunAt         string `json:"run_at"` // RFC3339 for one-time schedules
		Enabled       *bool  `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.BlueprintPath == "" {
		writeError(w, http.StatusBadRequest, "blueprint_path is required")
		return
	}
	if body.CronExpr == "" && body.RunAt == "" {
		writeError(w, http.StatusBadRequest, "cron_expr or run_at is required")
		return
	}
	if body.CronExpr != "" {
		if err := scheduler.ValidateCron(body.CronExpr); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	wsID := body.WorkspaceID
	if wsID == "" {
		wsID = "default"
	}
	name := body.Name
	if name == "" {
		name = body.BlueprintPath
	}
	enabled := true
	if body.Enabled != nil {
		enabled = *body.Enabled
	}

	var nextRunStr sql.NullString
	if body.RunAt != "" {
		// One-time schedule: use run_at as next_run_at
		t, err := time.Parse(time.RFC3339, body.RunAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "run_at must be RFC3339")
			return
		}
		nextRunStr = sql.NullString{String: t.UTC().Format(time.RFC3339), Valid: true}
	} else {
		nextRun, err := scheduler.NextRun(body.CronExpr, time.Now())
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		nextRunStr = sql.NullString{String: nextRun.UTC().Format(time.RFC3339), Valid: true}
	}

	now := time.Now().UTC()
	schedID := s.nextScheduleID()
	rec := persistence.ScheduleRecord{
		ID:            schedID,
		WorkspaceID:   wsID,
		Name:          name,
		BlueprintPath: body.BlueprintPath,
		CronExpr:      body.CronExpr,
		Enabled:       enabled,
		CreatedAt:     now,
		UpdatedAt:     now,
		NextRunAt:     nextRunStr,
	}
	if err := s.deps.Schedules.CreateSchedule(r.Context(), rec); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	created, err := s.deps.Schedules.GetSchedule(r.Context(), schedID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, toScheduleResponse(created))
}

func (s *Server) getSchedule(w http.ResponseWriter, r *http.Request, id string) {
	rec, err := s.deps.Schedules.GetSchedule(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toScheduleResponse(rec))
}

func (s *Server) patchSchedule(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		Name          *string `json:"name"`
		CronExpr      *string `json:"cron_expr"`
		BlueprintPath *string `json:"blueprint_path"`
		Enabled       *bool   `json:"enabled"`
		NextRunAt     *string `json:"next_run_at"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	existing, err := s.deps.Schedules.GetSchedule(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Apply partial updates onto existing values
	name := existing.Name
	if body.Name != nil {
		name = *body.Name
	}
	cronExpr := existing.CronExpr
	if body.CronExpr != nil {
		if err := scheduler.ValidateCron(*body.CronExpr); err != nil {
			writeError(w, http.StatusBadRequest, "invalid cron: "+err.Error())
			return
		}
		cronExpr = *body.CronExpr
	}
	bpPath := existing.BlueprintPath
	if body.BlueprintPath != nil {
		bpPath = *body.BlueprintPath
	}
	enabled := existing.Enabled
	if body.Enabled != nil {
		enabled = *body.Enabled
	}

	// Recalculate next run if cron changed or explicitly set
	var nextRun *time.Time
	if body.CronExpr != nil && enabled {
		t, err := scheduler.NextRun(cronExpr, time.Now())
		if err == nil {
			nextRun = &t
		}
	}
	if body.NextRunAt != nil {
		t, err := time.Parse(time.RFC3339, *body.NextRunAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "next_run_at must be RFC3339")
			return
		}
		nextRun = &t
	}
	// If no next_run override and no cron change, preserve existing next_run
	if nextRun == nil && body.CronExpr == nil {
		if existing.NextRunAt.Valid {
			t, err := time.Parse(time.RFC3339, existing.NextRunAt.String)
			if err == nil {
				nextRun = &t
			}
		}
	}

	if err := s.deps.Schedules.UpdateScheduleFields(r.Context(), id, name, cronExpr, bpPath, enabled, nextRun); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	updated, err := s.deps.Schedules.GetSchedule(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toScheduleResponse(updated))
}

func (s *Server) deleteSchedule(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := s.deps.Schedules.GetSchedule(r.Context(), id); err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.deps.Schedules.DeleteSchedule(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── Pipelines ─────────────────────────────────────────────────────────────────

func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listPipelines(w, r)
	case http.MethodPost:
		s.createPipeline(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handlePipelineByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/pipelines/")
	parts := strings.SplitN(path, "/", 2)
	pipelineID := parts[0]
	if pipelineID == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	sub := ""
	if len(parts) == 2 {
		sub = parts[1]
	}

	switch {
	case sub == "" && r.Method == http.MethodGet:
		s.getPipeline(w, r, pipelineID)
	case sub == "stages" && r.Method == http.MethodGet:
		s.getPipelineStages(w, r, pipelineID)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	wsID := q.Get("workspace_id")
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	items, err := s.deps.Pipelines.ListPipelineRunsByWorkspace(r.Context(), wsID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, p := range items {
		out = append(out, toPipelineResponse(p))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "next_cursor": nil})
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkspaceID  string         `json:"workspace_id"`
		PipelinePath string         `json:"pipeline_path"`
		Inputs       map[string]any `json:"inputs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.PipelinePath == "" {
		writeError(w, http.StatusBadRequest, "pipeline_path is required")
		return
	}
	wsID := body.WorkspaceID
	if wsID == "" {
		wsID = "default"
	}
	if s.deps.Pipeline == nil {
		writeError(w, http.StatusServiceUnavailable, "pipeline runner unavailable")
		return
	}
	pipelineID := s.nextPipelineID()
	if err := s.deps.Pipeline.Start(r.Context(), pipelineID, body.PipelinePath, wsID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	rec, err := s.deps.Pipelines.GetPipelineRun(r.Context(), pipelineID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, toPipelineResponse(rec))
}

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request, id string) {
	rec, err := s.deps.Pipelines.GetPipelineRun(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "pipeline run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toPipelineResponse(rec))
}

func (s *Server) getPipelineStages(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := s.deps.Pipelines.GetPipelineRun(r.Context(), id); err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "pipeline run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	items, err := s.deps.Pipelines.ListPipelineStageRuns(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, st := range items {
		out = append(out, toPipelineStageResponse(st))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

// ── Blueprints ────────────────────────────────────────────────────────────────

func (s *Server) handleBlueprintValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4*1024*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read body")
		return
	}
	if len(body) == 0 {
		writeError(w, http.StatusBadRequest, "body is required")
		return
	}
	_, parseErr := blueprint.ParseBytes(body)
	if parseErr != nil {
		writeJSON(w, http.StatusOK, map[string]any{"valid": false, "error": parseErr.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"valid": true})
}

// ── Response helpers ──────────────────────────────────────────────────────────

func toWorkspaceResponse(ws persistence.WorkspaceRecord) map[string]any {
	return map[string]any{
		"id":         ws.ID,
		"name":       ws.Name,
		"created_at": ws.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": ws.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func toRunResponse(r persistence.RunRecord) map[string]any {
	return map[string]any{
		"id":             r.ID,
		"workspace_id":   r.WorkspaceID,
		"blueprint_path": r.BlueprintPath,
		"status":         r.Status,
		"input_json":     r.InputJSON,
		"error_message":  nullableString(r.ErrorMessage),
		"created_at":     r.CreatedAt.UTC().Format(time.RFC3339),
		"started_at":     nullableString(r.StartedAt),
		"ended_at":       nullableString(r.EndedAt),
	}
}

func toRunEventResponse(ev persistence.RunEventRecord) map[string]any {
	return map[string]any{
		"id":         ev.ID,
		"run_id":     ev.RunID,
		"step_name":  nullableString(ev.StepName),
		"event_type": ev.EventType,
		"message":    nullableString(ev.Message),
		"created_at": ev.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func toScheduleResponse(sc persistence.ScheduleRecord) map[string]any {
	return map[string]any{
		"id":             sc.ID,
		"workspace_id":   sc.WorkspaceID,
		"name":           sc.Name,
		"blueprint_path": sc.BlueprintPath,
		"cron_expr":      sc.CronExpr,
		"enabled":        sc.Enabled,
		"created_at":     sc.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":     sc.UpdatedAt.UTC().Format(time.RFC3339),
		"last_run_at":    nullableString(sc.LastRunAt),
		"next_run_at":    nullableString(sc.NextRunAt),
	}
}

func toPipelineResponse(p persistence.PipelineRunRecord) map[string]any {
	return map[string]any{
		"id":            p.ID,
		"workspace_id":  p.WorkspaceID,
		"pipeline_path": p.PipelinePath,
		"status":        p.Status,
		"error_message": nullableString(p.ErrorMessage),
		"created_at":    p.CreatedAt.UTC().Format(time.RFC3339),
		"started_at":    nullableString(p.StartedAt),
		"ended_at":      nullableString(p.EndedAt),
	}
}

func toPipelineStageResponse(st persistence.PipelineStageRunRecord) map[string]any {
	return map[string]any{
		"id":              st.ID,
		"workspace_id":    st.WorkspaceID,
		"pipeline_run_id": st.PipelineRunID,
		"stage_index":     st.StageIndex,
		"stage_name":      st.StageName,
		"run_id":          st.RunID,
		"status":          st.Status,
		"created_at":      st.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":      st.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

// ── ID generators ─────────────────────────────────────────────────────────────

func (s *Server) nextRunID() string {
	n := s.runSeq.Add(1)
	return fmt.Sprintf("run-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}

func (s *Server) nextScheduleID() string {
	n := s.scheduleSeq.Add(1)
	return fmt.Sprintf("sched-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}

func (s *Server) nextPipelineID() string {
	n := s.pipelineSeq.Add(1)
	return fmt.Sprintf("pl-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}

// ── Low-level helpers ─────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "no rows") ||
		fmt.Sprintf("%v", err) == "sql: no rows in result set"
}

func isInvalidCursor(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "invalid cursor")
}

func nullableString(s sql.NullString) any {
	if s.Valid {
		return s.String
	}
	return nil
}
