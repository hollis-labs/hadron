package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/scheduler"
)

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
