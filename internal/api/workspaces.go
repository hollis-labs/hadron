package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

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
