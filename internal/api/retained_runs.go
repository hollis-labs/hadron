package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/hollis-labs/hadron/internal/blueprint"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
)

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
