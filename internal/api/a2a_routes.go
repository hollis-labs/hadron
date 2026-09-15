package api

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/hadron/internal/a2a"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

func (s *Server) handleAgentCard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeWorkflowOperationError(w, http.StatusMethodNotAllowed, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return
	}
	if s.deps.AgentCard == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}
	baseURL := "http://" + s.httpServer.Addr
	card, err := s.deps.AgentCard.Card(r.Context(), baseURL)
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, false)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, card)
}

func (s *Server) handleA2ATasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeWorkflowOperationError(w, http.StatusMethodNotAllowed, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return
	}
	if s.deps.A2ATasks == nil || s.deps.WorkflowAuth == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}

	var req a2a.TaskRequest
	if !decodeWorkflowRequest(w, r, &req) || !workflowIdempotencyKey(w, r, &req.IdempotencyKey) {
		return
	}
	definition := req.Skill
	ctx, ok := s.authenticateA2A(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessRun, Definition: &definition}, true)
	if !ok {
		return
	}

	resp, err := s.deps.A2ATasks.SubmitTask(ctx, req)
	if err != nil {
		s.writeA2AFailure(w, err, true)
		return
	}
	status := http.StatusOK
	if resp.Outcome == workflowruntime.IdempotencyApplied || resp.Outcome == "" {
		status = http.StatusCreated
	}
	writeWorkflowJSON(w, status, resp)
}

func (s *Server) handleA2ATaskByID(w http.ResponseWriter, r *http.Request) {
	taskID, action, ok := a2aTaskPath(r)
	if !ok {
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
		return
	}
	if s.deps.A2ATasks == nil || s.deps.WorkflowAuth == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}
	operation := appworkflow.WorkflowAccessInspect
	switch action {
	case "cancel":
		operation = appworkflow.WorkflowAccessCancel
	case "resume":
		operation = appworkflow.WorkflowAccessResume
	}
	ctx, authenticated := s.authenticateA2A(w, r, appworkflow.WorkflowAccessIntent{Operation: operation}, false)
	if !authenticated {
		return
	}
	switch {
	case r.Method == http.MethodGet && action == "":
		response, err := s.deps.A2ATasks.GetTask(ctx, taskID)
		if err != nil {
			s.writeA2AFailure(w, err, false)
			return
		}
		writeWorkflowJSON(w, http.StatusOK, response)
	case r.Method == http.MethodPost && action == "cancel":
		var request a2a.CancelTaskRequest
		if !decodeWorkflowRequest(w, r, &request) || !workflowIdempotencyKey(w, r, &request.IdempotencyKey) {
			return
		}
		response, err := s.deps.A2ATasks.CancelTask(ctx, taskID, request)
		if err != nil {
			s.writeA2AFailure(w, err, false)
			return
		}
		writeWorkflowJSON(w, http.StatusOK, response)
	case r.Method == http.MethodPost && action == "resume":
		var request a2a.ResumeTaskRequest
		if !decodeWorkflowRequest(w, r, &request) || !workflowIdempotencyKey(w, r, &request.IdempotencyKey) {
			return
		}
		response, err := s.deps.A2ATasks.ResumeTask(ctx, taskID, request)
		if err != nil {
			s.writeA2AFailure(w, err, false)
			return
		}
		writeWorkflowJSON(w, http.StatusOK, response)
	default:
		writeWorkflowOperationError(w, http.StatusMethodNotAllowed, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
	}
}

func (s *Server) authenticateA2A(w http.ResponseWriter, r *http.Request, intent appworkflow.WorkflowAccessIntent, hideDenied bool) (context.Context, bool) {
	ctx, err := s.deps.WorkflowAuth.AuthenticateWorkflowRequest(r, intent)
	if err != nil || ctx == nil {
		operationError := appworkflow.SafeWorkflowOperationError(err, nil)
		if ctx == nil && err == nil {
			operationError.Code = appworkflow.WorkflowErrorCodeUnauthenticated
		}
		operationError = hideWorkflowDefinitionDenial(operationError, hideDenied)
		writeWorkflowOperationError(w, workflowHTTPStatus(operationError.Code), operationError)
		return nil, false
	}
	binding, err := (appworkflow.ContextIdentityProvider{}).BindIdentity(ctx, appworkflow.IdentityRequest{})
	if err != nil {
		writeWorkflowOperationError(w, http.StatusUnauthorized, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnauthenticated})
		return nil, false
	}
	binding.SourceAuthority = "a2a"
	ctx, err = appworkflow.WithAuthenticatedIdentity(ctx, binding)
	if err != nil {
		writeWorkflowOperationError(w, http.StatusUnauthorized, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnauthenticated})
		return nil, false
	}
	return ctx, true
}

func (s *Server) writeA2AFailure(w http.ResponseWriter, err error, hideDenied bool) {
	operationError := hideWorkflowDefinitionDenial(appworkflow.SafeWorkflowOperationError(err, nil), hideDenied)
	writeWorkflowOperationError(w, workflowHTTPStatus(operationError.Code), operationError)
}

func a2aTaskPath(request *http.Request) (string, string, bool) {
	path := strings.TrimPrefix(request.URL.EscapedPath(), "/a2a/tasks/")
	parts := strings.Split(path, "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		return "", "", false
	}
	taskID, err := url.PathUnescape(parts[0])
	if err != nil || hoststate.ValidateA2ATaskID(taskID) != nil {
		return "", "", false
	}
	action := ""
	if len(parts) == 2 {
		action = parts[1]
		if action != "cancel" && action != "resume" {
			return "", "", false
		}
	}
	return taskID, action, true
}
