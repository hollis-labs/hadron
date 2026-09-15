package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/persistence"
)

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
