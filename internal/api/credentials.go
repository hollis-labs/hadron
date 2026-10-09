package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// CredentialRequestAuthenticator independently verifies an operator bearer;
// workflow.manage, cookies and anonymous loopback are not issuer authority.
type CredentialRequestAuthenticator interface {
	AuthenticateCredentialAdminRequest(*http.Request) (context.Context, error)
}

func (s *Server) handleCredentials(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.deps.WorkflowCredentials == nil || s.deps.CredentialAuth == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}
	ctx, err := s.deps.CredentialAuth.AuthenticateCredentialAdminRequest(r)
	if err != nil || ctx == nil {
		writeWorkflowOperationError(w, http.StatusUnauthorized, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnauthenticated})
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/v1/auth/credentials/")
	if action == "list" || action == "audit" {
		if r.Method != http.MethodGet {
			credentialInvalid(w, http.StatusMethodNotAllowed)
			return
		}
		query := r.URL.Query()
		if len(query) != 1 || len(query["principal_id"]) != 1 {
			credentialInvalid(w, http.StatusBadRequest)
			return
		}
		if action == "audit" {
			result, err := s.deps.WorkflowCredentials.CredentialAudit(ctx, query.Get("principal_id"))
			s.writeWorkflowLifecycleResult(w, result, err, false)
		} else {
			result, err := s.deps.WorkflowCredentials.ListCredentials(ctx, query.Get("principal_id"))
			s.writeWorkflowLifecycleResult(w, result, err, false)
		}
		return
	}
	if r.Method != http.MethodPost {
		credentialInvalid(w, http.StatusMethodNotAllowed)
		return
	}
	if r.URL.RawQuery != "" {
		credentialInvalid(w, http.StatusBadRequest)
		return
	}
	switch action {
	case "issue":
		var request hoststate.IssueCredentialRequest
		if !decodeCredentialRequest(w, r, &request) {
			return
		}
		result, err := s.deps.WorkflowCredentials.IssueCredential(ctx, request)
		s.writeWorkflowLifecycleResult(w, result, err, false)
	case "revoke":
		var request hoststate.RevokeCredentialRequest
		if !decodeCredentialRequest(w, r, &request) {
			return
		}
		result, err := s.deps.WorkflowCredentials.RevokeCredential(ctx, request)
		var refusal *hoststate.CredentialRefusal
		if errors.As(err, &refusal) {
			writeWorkflowJSON(w, http.StatusConflict, struct {
				appworkflow.WorkflowOperationError
				Credential hoststate.CredentialMetadata `json:"credential"`
			}{appworkflow.SafeWorkflowOperationError(err, nil), refusal.Credential})
			return
		}
		s.writeWorkflowLifecycleResult(w, result, err, false)
	default:
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
	}
}

func credentialInvalid(w http.ResponseWriter, status int) {
	writeWorkflowOperationError(w, status, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
}

// Decode the small flat contract without accepting duplicate fields, unknown
// fields, null durations, trailing values or lossy JSON number conversions.
func decodeCredentialRequest(w http.ResponseWriter, r *http.Request, out any) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8192))
	if err != nil || !utf8.Valid(raw) {
		credentialInvalid(w, http.StatusBadRequest)
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		credentialInvalid(w, http.StatusBadRequest)
		return false
	}
	fields := map[string]json.RawMessage{}
	for decoder.More() {
		key, keyErr := decoder.Token()
		name, ok := key.(string)
		if keyErr != nil || !ok {
			credentialInvalid(w, http.StatusBadRequest)
			return false
		}
		switch name {
		case "principal_id", "credential_id", "expected_generation", "idempotency_key", "overlap_seconds", "ttl_seconds":
		default:
			credentialInvalid(w, http.StatusBadRequest)
			return false
		}
		if _, duplicate := fields[name]; duplicate {
			credentialInvalid(w, http.StatusBadRequest)
			return false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			credentialInvalid(w, http.StatusBadRequest)
			return false
		}
		fields[name] = value
	}
	if closing, closeErr := decoder.Token(); closeErr != nil || closing != json.Delim('}') {
		credentialInvalid(w, http.StatusBadRequest)
		return false
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		credentialInvalid(w, http.StatusBadRequest)
		return false
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		credentialInvalid(w, http.StatusBadRequest)
		return false
	}
	strict := json.NewDecoder(bytes.NewReader(encoded))
	strict.DisallowUnknownFields()
	if strict.Decode(out) != nil {
		credentialInvalid(w, http.StatusBadRequest)
		return false
	}
	return true
}
