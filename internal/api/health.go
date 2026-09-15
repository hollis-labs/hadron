package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
)

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	version := strings.TrimSpace(s.deps.BuildVersion)
	if version == "" {
		version = "dev"
	}
	health := appworkflow.HealthStatus{}
	if s.deps.WorkflowHealth != nil {
		health = s.deps.WorkflowHealth.Health()
	}
	status, code := "not_ready", http.StatusServiceUnavailable
	if health.Started && health.Ready && !health.Recovering {
		status, code = "ready", http.StatusOK
	}
	writeJSON(w, code, struct {
		Status   string `json:"status"`
		Version  string `json:"version"`
		Service  string `json:"service"`
		Workflow struct {
			Started          bool      `json:"started"`
			Ready            bool      `json:"ready"`
			Recovering       bool      `json:"recovering"`
			LastRecoveryAt   time.Time `json:"last_recovery_at,omitempty"`
			RecoveryFailed   bool      `json:"recovery_failed"`
			IncompleteStarts int       `json:"incomplete_starts"`
		} `json:"workflow"`
	}{
		Status: status, Version: version, Service: "hadrond",
		Workflow: struct {
			Started          bool      `json:"started"`
			Ready            bool      `json:"ready"`
			Recovering       bool      `json:"recovering"`
			LastRecoveryAt   time.Time `json:"last_recovery_at,omitempty"`
			RecoveryFailed   bool      `json:"recovery_failed"`
			IncompleteStarts int       `json:"incomplete_starts"`
		}{health.Started, health.Ready, health.Recovering, health.LastRecoveryAt, health.LastRecoveryError != "", health.IncompleteStarts},
	})
}
