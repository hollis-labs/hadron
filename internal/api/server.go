package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hollis-labs/go-otel/propagation"
	"github.com/hollis-labs/hadron/internal/a2a"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/hollis-labs/hadron/internal/trigger"
)

// Handler returns the underlying HTTP handler (useful for testing with httptest).
func (s *Server) Handler() http.Handler {
	return s.handler
}

func NewServer(addr string, deps Dependencies) *Server {
	s := &Server{deps: deps}
	mux := http.NewServeMux()

	// Create trigger manager if trigger store is available
	if deps.Triggers != nil && deps.Runner != nil {
		s.triggerManager = trigger.New(deps.Triggers, deps.Runner)
	}

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

	// Triggers — CRUD for webhook trigger definitions
	mux.HandleFunc("/v1/triggers", s.handleWebhookTriggers)
	mux.HandleFunc("/v1/triggers/", s.handleWebhookTriggerByID)

	// Human gates
	mux.HandleFunc("/v1/human-gates/", s.handleHumanGateByID)

	// Messages
	mux.HandleFunc("/v1/messages", s.handleMessagesCollection)
	mux.HandleFunc("/v1/messages/inbox", s.handleMessagesInbox)
	mux.HandleFunc("/v1/messages/list", s.handleMessagesList)
	mux.HandleFunc("/v1/messages/thread/", s.handleMessagesThread)
	mux.HandleFunc("/v1/messages/", s.handleMessageByID)

	// Webhook catch-all — incoming webhook requests
	if s.triggerManager != nil {
		s.triggerManager.RegisterWebhookRoutes(mux)
	}

	// A2A Agent Card
	mux.HandleFunc("/.well-known/agent.json", s.handleAgentCard)

	// A2A Task endpoints
	if deps.Runner != nil && deps.Runs != nil {
		s.a2aHandler = a2a.NewHandler(deps.Runs, deps.Runner, &serverBlueprintResolver{s: s})
		mux.HandleFunc("/a2a/tasks", s.handleA2ATasks)
		mux.HandleFunc("/a2a/tasks/", s.handleA2ATaskByID)
	}

	// Blueprints
	mux.HandleFunc("/v1/blueprints/validate", s.handleBlueprintValidate)

	// 404 catch-all
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})

	s.handler = corsMiddleware(propagation.HTTPMiddleware(mux))
	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
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

// ── Human Gates ───────────────────────────────────────────────────────────────

func (s *Server) handleHumanGateByID(w http.ResponseWriter, r *http.Request) {
	if s.deps.HumanGates == nil {
		writeError(w, http.StatusServiceUnavailable, "human gates unavailable")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/human-gates/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "gate id is required")
		return
	}
	gateID := parts[0]
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		gate, err := s.deps.HumanGates.GetHumanGate(r.Context(), gateID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no rows") {
				writeError(w, http.StatusNotFound, "human gate not found")
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, toHumanGateResponse(gate))
		return
	}
	if len(parts) == 2 && parts[1] == "decision" {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var body struct {
			Decision string `json:"decision"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json body")
			return
		}
		decision := strings.TrimSpace(body.Decision)
		if decision == "" {
			writeError(w, http.StatusBadRequest, "decision is required")
			return
		}
		current, err := s.deps.HumanGates.GetHumanGate(r.Context(), gateID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || strings.Contains(err.Error(), "no rows") {
				writeError(w, http.StatusNotFound, "human gate not found")
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if current.Status != "waiting" {
			writeError(w, http.StatusConflict, "human gate is not waiting")
			return
		}
		if !humanGateDecisionAllowed(current.OptionsJSON, decision) {
			writeError(w, http.StatusBadRequest, "decision is not an allowed option")
			return
		}
		if submitErr := s.deps.HumanGates.SubmitHumanGateDecision(r.Context(), gateID, decision, time.Now().UTC()); submitErr != nil {
			if errors.Is(submitErr, sql.ErrNoRows) || strings.Contains(submitErr.Error(), "no rows") {
				writeError(w, http.StatusConflict, "human gate is not waiting or was not found")
				return
			}
			writeError(w, http.StatusInternalServerError, submitErr.Error())
			return
		}
		gate, err := s.deps.HumanGates.GetHumanGate(r.Context(), gateID)
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"id": gateID, "decision": decision, "status": "decided"})
			return
		}
		writeJSON(w, http.StatusOK, toHumanGateResponse(gate))
		return
	}
	writeError(w, http.StatusNotFound, "not found")
}

func humanGateDecisionAllowed(optionsJSON, decision string) bool {
	var options []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(optionsJSON), &options); err != nil {
		return false
	}
	for _, opt := range options {
		if opt.ID == decision {
			return true
		}
	}
	return false
}

// ── A2A Agent Card ────────────────────────────────────────────────────────────

func (s *Server) handleAgentCard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	dir := s.deps.BlueprintDir
	if dir == "" {
		writeError(w, http.StatusServiceUnavailable, "blueprint directory not configured")
		return
	}
	baseURL := "http://" + s.httpServer.Addr
	card, err := agentcard.FromDirectory(dir, baseURL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to build agent card: "+err.Error())
		return
	}
	data, err := card.JSON()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to marshal agent card: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
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
	case sub == "operations" && r.Method == http.MethodGet:
		s.listRunOperations(w, r, runID)
	case sub == "mcp-calls" && r.Method == http.MethodGet:
		s.listRunMCPCalls(w, r, runID)
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
	if enqueueErr := s.deps.Runner.Enqueue(r.Context(), execution.Request{
		RunID:         runID,
		WorkspaceID:   wsID,
		BlueprintPath: body.BlueprintPath,
		Inputs:        normalized,
		DryRun:        body.DryRun,
	}); enqueueErr != nil {
		writeError(w, http.StatusInternalServerError, enqueueErr.Error())
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

func (s *Server) listRunMCPCalls(w http.ResponseWriter, r *http.Request, runID string) {
	if _, err := s.deps.Runs.GetRun(r.Context(), runID); err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	events, err := s.deps.Runs.ListRunEvents(r.Context(), runID, 1000)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items := rundiagnostics.SummarizeMCPCalls(events)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"sequence":      item.Sequence,
			"step_name":     item.StepName,
			"server":        item.Server,
			"tool":          item.Tool,
			"transport":     item.Transport,
			"status":        item.Status,
			"retry_count":   item.RetryCount,
			"attempt_count": item.AttemptCount,
			"reused_client": item.ReusedClient,
			"health_probe":  item.HealthProbe,
			"reconnected":   item.Reconnected,
			"truncated":     item.Truncated,
			"result_json":   item.ResultJSON,
			"error_message": item.ErrorMessage,
			"started_at":    item.StartedAt,
			"finished_at":   item.FinishedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "count": len(out)})
}

func (s *Server) listRunOperations(w http.ResponseWriter, r *http.Request, runID string) {
	if _, err := s.deps.Runs.GetRun(r.Context(), runID); err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	events, err := s.deps.Runs.ListRunEvents(r.Context(), runID, 1000)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items := rundiagnostics.SummarizeOperations(events)
	q := r.URL.Query()
	kind := strings.TrimSpace(q.Get("kind"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	cursor := strings.TrimSpace(q.Get("cursor"))
	page, nextCursor, totalCount, err := rundiagnostics.FilterAndPageOperations(items, kind, limit, cursor)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid cursor")
		return
	}

	out := make([]map[string]any, 0, len(page))
	for _, item := range page {
		out = append(out, toOperationDiagnosticResponse(item))
	}
	var next any
	if nextCursor != "" {
		next = nextCursor
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":       out,
		"count":       len(out),
		"total_count": totalCount,
		"next_cursor": next,
	})
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
		if validateErr := scheduler.ValidateCron(*body.CronExpr); validateErr != nil {
			writeError(w, http.StatusBadRequest, "invalid cron: "+validateErr.Error())
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
		t, nextErr := scheduler.NextRun(cronExpr, time.Now())
		if nextErr == nil {
			nextRun = &t
		}
	}
	if body.NextRunAt != nil {
		t, parseErr := time.Parse(time.RFC3339, *body.NextRunAt)
		if parseErr != nil {
			writeError(w, http.StatusBadRequest, "next_run_at must be RFC3339")
			return
		}
		nextRun = &t
	}
	// If no next_run override and no cron change, preserve existing next_run
	if nextRun == nil && body.CronExpr == nil {
		if existing.NextRunAt.Valid {
			t, parseErr := time.Parse(time.RFC3339, existing.NextRunAt.String)
			if parseErr == nil {
				nextRun = &t
			}
		}
	}

	if updateErr := s.deps.Schedules.UpdateScheduleFields(r.Context(), id, name, cronExpr, bpPath, enabled, nextRun); updateErr != nil {
		writeError(w, http.StatusInternalServerError, updateErr.Error())
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
	case sub == "graph" && r.Method == http.MethodGet:
		s.getPipelineGraph(w, r, pipelineID)
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
	pipelineRun, err := s.deps.Pipelines.GetPipelineRun(r.Context(), id)
	if err != nil {
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

	// Try to load the pipeline spec for DAG metadata (depends_on, position, outputs).
	specStages := parsePipelineSpecStages(pipelineRun.PipelinePath)

	out := make([]map[string]any, 0, len(items))
	for _, st := range items {
		entry := toPipelineStageResponse(st)
		if spec, ok := specStages[st.StageName]; ok {
			entry["depends_on"] = spec.DependsOn
			if spec.Position != nil {
				entry["position"] = map[string]any{"x": spec.Position.X, "y": spec.Position.Y}
			} else {
				entry["position"] = nil
			}
			entry["outputs"] = spec.Outputs
		} else {
			entry["depends_on"] = nil
			entry["position"] = nil
			entry["outputs"] = nil
		}
		out = append(out, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) getPipelineGraph(w http.ResponseWriter, r *http.Request, id string) {
	pipelineRun, err := s.deps.Pipelines.GetPipelineRun(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "pipeline run not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Load stage run records for status.
	stageRuns, err := s.deps.Pipelines.ListPipelineStageRuns(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	statusMap := make(map[string]string, len(stageRuns))
	for _, sr := range stageRuns {
		statusMap[sr.StageName] = sr.Status
	}

	// Parse pipeline spec for DAG structure.
	spec, parseErr := pipeline.ParseFile(pipelineRun.PipelinePath)
	if parseErr != nil {
		writeError(w, http.StatusUnprocessableEntity, "cannot parse pipeline spec: "+parseErr.Error())
		return
	}

	nodes := make([]map[string]any, 0, len(spec.Stages))
	edges := make([]map[string]any, 0)

	for _, stage := range spec.Stages {
		status := statusMap[stage.Name]
		if status == "" {
			status = "pending"
		}

		var pos map[string]any
		if stage.Position != nil {
			pos = map[string]any{"x": stage.Position.X, "y": stage.Position.Y}
		}

		outputs := map[string]string{}
		if stage.Outputs != nil {
			outputs = stage.Outputs
		}

		nodes = append(nodes, map[string]any{
			"id":             stage.Name,
			"name":           stage.Name,
			"blueprint_path": stage.BlueprintPath,
			"position":       pos,
			"status":         status,
			"outputs":        outputs,
		})

		for _, dep := range stage.DependsOn {
			edges = append(edges, map[string]any{
				"source":    dep,
				"target":    stage.Name,
				"condition": stage.If,
			})
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"nodes": nodes,
		"edges": edges,
	})
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

// ── Webhook Triggers ──────────────────────────────────────────────────────────

func (s *Server) handleWebhookTriggers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listWebhookTriggers(w, r)
	case http.MethodPost:
		s.createWebhookTrigger(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleWebhookTriggerByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/triggers/")
	id = strings.TrimRight(id, "/")
	if id == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.getWebhookTrigger(w, r, id)
	case http.MethodDelete:
		s.deleteWebhookTrigger(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) listWebhookTriggers(w http.ResponseWriter, r *http.Request) {
	if s.deps.Triggers == nil {
		writeError(w, http.StatusServiceUnavailable, "triggers unavailable")
		return
	}
	items, err := s.deps.Triggers.ListTriggers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, t := range items {
		out = append(out, toTriggerResponse(t))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "next_cursor": nil})
}

func (s *Server) createWebhookTrigger(w http.ResponseWriter, r *http.Request) {
	if s.deps.Triggers == nil {
		writeError(w, http.StatusServiceUnavailable, "triggers unavailable")
		return
	}
	var body struct {
		Name          string            `json:"name"`
		Path          string            `json:"path"`
		BlueprintPath string            `json:"blueprint_path"`
		WorkspaceID   string            `json:"workspace_id"`
		Secret        string            `json:"secret"`
		ExtractInputs map[string]string `json:"extract_inputs"`
		OneShot       bool              `json:"one_shot"`
		TTLMinutes    int               `json:"ttl_minutes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if body.Path == "" {
		writeError(w, http.StatusBadRequest, "path is required")
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

	triggerID := s.nextTriggerID()

	rec := persistence.TriggerRecord{
		ID:            triggerID,
		Type:          "webhook",
		Name:          body.Name,
		Path:          body.Path,
		BlueprintPath: body.BlueprintPath,
		WorkspaceID:   wsID,
		Enabled:       true,
		OneShot:       body.OneShot,
	}
	if body.Secret != "" {
		rec.SecretHash = sql.NullString{String: body.Secret, Valid: true}
	}
	if body.ExtractInputs != nil {
		eiJSON, _ := json.Marshal(body.ExtractInputs)
		rec.ExtractInputs = sql.NullString{String: string(eiJSON), Valid: true}
	}
	if body.TTLMinutes > 0 {
		expires := time.Now().UTC().Add(time.Duration(body.TTLMinutes) * time.Minute)
		rec.TTLExpiresAt = sql.NullString{String: expires.Format(time.RFC3339), Valid: true}
	}

	if err := s.deps.Triggers.CreateTrigger(r.Context(), rec); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	created, err := s.deps.Triggers.GetTrigger(r.Context(), triggerID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, toTriggerResponse(created))
}

func (s *Server) getWebhookTrigger(w http.ResponseWriter, r *http.Request, id string) {
	if s.deps.Triggers == nil {
		writeError(w, http.StatusServiceUnavailable, "triggers unavailable")
		return
	}
	rec, err := s.deps.Triggers.GetTrigger(r.Context(), id)
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "trigger not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toTriggerResponse(rec))
}

func (s *Server) deleteWebhookTrigger(w http.ResponseWriter, r *http.Request, id string) {
	if s.deps.Triggers == nil {
		writeError(w, http.StatusServiceUnavailable, "triggers unavailable")
		return
	}
	if _, err := s.deps.Triggers.GetTrigger(r.Context(), id); err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "trigger not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.deps.Triggers.DeleteTrigger(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func toTriggerResponse(t persistence.TriggerRecord) map[string]any {
	resp := map[string]any{
		"id":             t.ID,
		"type":           t.Type,
		"name":           t.Name,
		"path":           t.Path,
		"blueprint_path": t.BlueprintPath,
		"workspace_id":   t.WorkspaceID,
		"enabled":        t.Enabled,
		"one_shot":       t.OneShot,
		"fired_count":    t.FiredCount,
		"created_at":     t.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at":     t.UpdatedAt.UTC().Format(time.RFC3339),
		"last_fired_at":  nullableString(t.LastFiredAt),
		"ttl_expires_at": nullableString(t.TTLExpiresAt),
	}
	if t.ExtractInputs.Valid {
		resp["extract_inputs"] = t.ExtractInputs.String
	}
	return resp
}

func (s *Server) nextTriggerID() string {
	n := s.triggerSeq.Add(1)
	return fmt.Sprintf("trig-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
}

// resolveBlueprintPath resolves a blueprint name to a file path within BlueprintDir.
// It searches for: name.yaml, name.yml, name/name.yaml, name/name.yml, and
// walks subdirectories matching by slug, spec name, or filename.
func (s *Server) resolveBlueprintPath(name string) (string, error) {
	dir := s.deps.BlueprintDir
	if dir == "" {
		return "", fmt.Errorf("blueprint directory not configured")
	}

	// Direct file match
	for _, ext := range []string{".yaml", ".yml"} {
		candidate := filepath.Join(dir, name+ext)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	// Subdirectory match: name/name.yaml
	for _, ext := range []string{".yaml", ".yml"} {
		candidate := filepath.Join(dir, name, name+ext)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	// Walk subdirectories looking for matching slug or spec name
	var found string
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || found != "" {
			return err
		}
		ext := filepath.Ext(path)
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}
		base := strings.TrimSuffix(filepath.Base(path), ext)
		if base == name {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	if found != "" {
		return found, nil
	}

	return "", fmt.Errorf("not found")
}

// ── A2A Tasks ─────────────────────────────────────────────────────────────────

// serverBlueprintResolver adapts Server.resolveBlueprintPath to the a2a.BlueprintResolver interface.
type serverBlueprintResolver struct {
	s *Server
}

func (r *serverBlueprintResolver) Resolve(name string) (string, error) {
	return r.s.resolveBlueprintPath(name)
}

func (s *Server) handleA2ATasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.a2aHandler == nil {
		writeError(w, http.StatusServiceUnavailable, "a2a tasks unavailable")
		return
	}

	var req a2a.TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Skill == "" {
		writeError(w, http.StatusBadRequest, "skill is required")
		return
	}

	resp, err := s.a2aHandler.SubmitTask(r.Context(), req)
	if err != nil {
		// Distinguish "unknown skill" from internal errors.
		if strings.Contains(err.Error(), "unknown skill") || strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleA2ATaskByID(w http.ResponseWriter, r *http.Request) {
	taskID := strings.TrimPrefix(r.URL.Path, "/a2a/tasks/")
	if taskID == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.a2aHandler == nil {
		writeError(w, http.StatusServiceUnavailable, "a2a tasks unavailable")
		return
	}

	resp, err := s.a2aHandler.GetTask(r.Context(), taskID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, resp)
}
