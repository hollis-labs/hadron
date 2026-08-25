package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/workflow/graph"
)

func (s *Server) handleWorkflowLifecycle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeWorkflowOperationError(w, http.StatusMethodNotAllowed, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return
	}
	if s.deps.WorkflowLifecycle == nil || s.deps.WorkflowAuth == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/v1/workflows/lifecycle/")
	switch action {
	case "catalog/search":
		s.handleWorkflowCatalogSearch(w, r)
	case "catalog/inspect":
		s.handleWorkflowCatalogInspect(w, r)
	case "author/validate":
		s.handleWorkflowDraftValidate(w, r)
	case "author/scaffold":
		s.handleWorkflowDraftScaffold(w, r)
	case "author/test":
		s.handleWorkflowDraftTest(w, r)
	case "author/register":
		s.handleWorkflowDraftRegister(w, r)
	case "registry/package":
		s.handleWorkflowRegistryPackage(w, r)
	case "registry/publish", "registry/pin-version", "registry/unpin-version", "registry/clear-current":
		s.handleWorkflowRegistryMutation(w, r, action)
	case "exposure/inspect":
		s.handleWorkflowExposureInspect(w, r)
	case "exposure/pin-definition", "exposure/unpin-definition":
		s.handleWorkflowExposureMutation(w, r, action)
	default:
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
	}
}

func (s *Server) handleWorkflowCatalogSearch(w http.ResponseWriter, r *http.Request) {
	var request appworkflow.SearchWorkflowCatalogRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessCatalogSearch}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.WorkflowLifecycle.SearchWorkflowCatalog(ctx, request)
	s.writeWorkflowLifecycleResult(w, result, err, false)
}

func (s *Server) handleWorkflowCatalogInspect(w http.ResponseWriter, r *http.Request) {
	var request appworkflow.InspectWorkflowVersionRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, lifecycleIntent(appworkflow.WorkflowAccessCatalogInspect, request.Definition), true)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.WorkflowLifecycle.InspectWorkflowVersion(ctx, request)
	s.writeWorkflowLifecycleResult(w, result, err, true)
}

func (s *Server) handleWorkflowDraftValidate(w http.ResponseWriter, r *http.Request) {
	var request appworkflow.ValidateWorkflowDraftRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessAuthorValidate}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.WorkflowLifecycle.ValidateWorkflowDraft(ctx, request)
	s.writeWorkflowLifecycleResult(w, result, err, false)
}

func (s *Server) handleWorkflowDraftScaffold(w http.ResponseWriter, r *http.Request) {
	var request appworkflow.GenerateWorkflowContractRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessAuthorScaffold}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.WorkflowLifecycle.GenerateWorkflowContract(ctx, request)
	s.writeWorkflowLifecycleResult(w, result, err, false)
}

func (s *Server) handleWorkflowDraftTest(w http.ResponseWriter, r *http.Request) {
	var request appworkflow.TestWorkflowDraftRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessAuthorTest}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.WorkflowLifecycle.TestWorkflowDraft(ctx, request)
	s.writeWorkflowLifecycleResult(w, result, err, false)
}

func (s *Server) handleWorkflowDraftRegister(w http.ResponseWriter, r *http.Request) {
	var request appworkflow.RegisterWorkflowDraftRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessAuthorRegister}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.WorkflowLifecycle.RegisterWorkflowDraft(ctx, request)
	s.writeWorkflowLifecycleResult(w, result, err, false)
}

func (s *Server) handleWorkflowRegistryPackage(w http.ResponseWriter, r *http.Request) {
	var request appworkflow.PackageWorkflowVersionRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, lifecycleIntent(appworkflow.WorkflowAccessRegistryPackage, request.Definition), true)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.WorkflowLifecycle.PackageWorkflowVersion(ctx, request)
	s.writeWorkflowLifecycleResult(w, result, err, true)
}

func (s *Server) handleWorkflowRegistryMutation(w http.ResponseWriter, r *http.Request, action string) {
	var request appworkflow.MutateWorkflowVersionRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	operation := map[string]appworkflow.WorkflowAccessOperation{
		"registry/publish":       appworkflow.WorkflowAccessRegistryPublish,
		"registry/pin-version":   appworkflow.WorkflowAccessRegistryPin,
		"registry/unpin-version": appworkflow.WorkflowAccessRegistryUnpin,
		"registry/clear-current": appworkflow.WorkflowAccessRegistryClearCurrent,
	}[action]
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, lifecycleIntent(operation, request.Definition), true)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	var result appworkflow.WorkflowVersionDetail
	var err error
	switch action {
	case "registry/publish":
		result, err = s.deps.WorkflowLifecycle.PublishWorkflowVersion(ctx, request)
	case "registry/pin-version":
		result, err = s.deps.WorkflowLifecycle.PinRegistryVersion(ctx, request)
	case "registry/unpin-version":
		result, err = s.deps.WorkflowLifecycle.UnpinRegistryVersion(ctx, request)
	case "registry/clear-current":
		result, err = s.deps.WorkflowLifecycle.ClearWorkflowCurrentExact(ctx, request)
	}
	s.writeWorkflowLifecycleResult(w, result, err, true)
}

func (s *Server) handleWorkflowExposureInspect(w http.ResponseWriter, r *http.Request) {
	var request appworkflow.InspectWorkflowExposureRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessExposureInspect}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.WorkflowLifecycle.InspectWorkflowExposure(ctx, request)
	s.writeWorkflowLifecycleResult(w, result, err, false)
}

func (s *Server) handleWorkflowExposureMutation(w http.ResponseWriter, r *http.Request, action string) {
	var request appworkflow.MutateWorkflowExposureRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	operation := appworkflow.WorkflowAccessExposurePin
	if action == "exposure/unpin-definition" {
		operation = appworkflow.WorkflowAccessExposureUnpin
	}
	ctx, ok := s.authenticateWorkflowLifecycle(w, r, lifecycleIntent(operation, request.Definition), true)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	var result any
	var err error
	if action == "exposure/pin-definition" {
		result, err = s.deps.WorkflowLifecycle.PinWorkflowExposure(ctx, request)
	} else {
		result, err = s.deps.WorkflowLifecycle.UnpinWorkflowExposure(ctx, request)
	}
	s.writeWorkflowLifecycleResult(w, result, err, true)
}

func (s *Server) authenticateWorkflowLifecycle(w http.ResponseWriter, r *http.Request, intent appworkflow.WorkflowAccessIntent, hideDenied bool) (context.Context, bool) {
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
	return ctx, true
}

func (s *Server) writeWorkflowLifecycleResult(w http.ResponseWriter, result any, err error, hideDenied bool) {
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, hideDenied)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func lifecycleIntent(operation appworkflow.WorkflowAccessOperation, definition graph.DefinitionRef) appworkflow.WorkflowAccessIntent {
	return appworkflow.WorkflowAccessIntent{Operation: operation, Definition: &definition}
}
