package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

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
