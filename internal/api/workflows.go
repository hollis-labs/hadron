package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/trigger"
	"github.com/hollis-labs/go-workflow/graph"
)

const (
	maximumWorkflowHTTPRequestBytes  = 2 << 20
	maximumWorkflowHTTPResponseBytes = 8 << 20
	maximumWorkflowHTTPJSONDepth     = 128
	workflowHTTPSourceAuthority      = "http"
)

func (s *Server) handleWorkflowValidate(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflowPOST(w, r) {
		return
	}
	var request appworkflow.ValidateWorkflowRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	definition := request.Definition
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessValidate, Definition: &definition}, true)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.Workflows.ValidateWorkflow(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, true)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func (s *Server) handleWorkflowExplain(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflowPOST(w, r) {
		return
	}
	var request appworkflow.ExplainWorkflowRequest
	if !decodeWorkflowRequest(w, r, &request) || !workflowIdempotencyKey(w, r, &request.IdempotencyKey) {
		return
	}
	definition := request.Definition
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessExplain, Definition: &definition}, true)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.Workflows.ExplainWorkflow(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, &result, true)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func (s *Server) handleWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflowPOST(w, r) {
		return
	}
	var request appworkflow.RunWorkflowRequest
	if !decodeWorkflowRequest(w, r, &request) || !workflowIdempotencyKey(w, r, &request.IdempotencyKey) {
		return
	}
	definition := request.Definition
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessRun, Definition: &definition}, true)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.Workflows.RunWorkflow(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, &result, true)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func (s *Server) handleWorkflowActivationFire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeWorkflowOperationError(w, http.StatusMethodNotAllowed, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return
	}
	if s.deps.Workflows == nil || s.deps.WorkflowAuth == nil || s.deps.WorkflowActivations == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}
	escaped := strings.TrimPrefix(r.URL.EscapedPath(), "/v1/workflows/activations/")
	parts := strings.Split(escaped, "/")
	if len(parts) != 2 || parts[1] != "fire" {
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
		return
	}
	registrationID, err := url.PathUnescape(parts[0])
	if err != nil || graph.ValidateID(registrationID) != nil {
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
		return
	}
	var request appworkflow.FireWorkflowActivationRequest
	if !decodeWorkflowRequest(w, r, &request) {
		return
	}
	receivedAt := time.Now().UTC()
	if request.RegistrationID != registrationID || request.OccurredAt.IsZero() || request.OccurredAt.UTC().After(receivedAt) {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return
	}
	if !workflowIdempotencyKey(w, r, &request.IdempotencyKey) {
		return
	}
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessActivationFire}, false)
	if !ok {
		return
	}
	registration, err := s.deps.WorkflowActivations.LoadRegistration(ctx, registrationID)
	if err != nil {
		operationError := appworkflow.SafeWorkflowOperationError(err, nil)
		if operationError.Code != appworkflow.WorkflowErrorCodeNotFound {
			operationError = appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable}
		}
		writeWorkflowOperationError(w, workflowHTTPStatus(operationError.Code), operationError)
		return
	}
	definition, err := workflowActivationExposureDefinition(registration)
	if err != nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}
	ctx, ok = s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessActivationFire, Definition: &definition}, true)
	if !ok {
		return
	}
	result, err := s.deps.WorkflowActivations.Fire(ctx, trigger.ActivationEvent{
		RegistrationID: registrationID, IdempotencyKey: request.IdempotencyKey,
		OccurredAt: request.OccurredAt.UTC(), ReceivedAt: receivedAt, Payload: request.Payload,
		LogicalRunID: request.LogicalRunID, SourceRef: "http",
	})
	if err != nil {
		s.writeWorkflowFailure(w, err, &result.Start, true)
		return
	}
	projected, err := appworkflow.SafeFireWorkflowActivationResult(result)
	if err != nil {
		writeWorkflowOperationError(w, http.StatusInternalServerError, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInternal})
		return
	}
	writeWorkflowJSON(w, http.StatusOK, projected)
}

func workflowActivationExposureDefinition(registration hoststate.ActivationRegistration) (graph.DefinitionRef, error) {
	if registration.Derivation == nil {
		return graph.DefinitionRef{}, errors.New("activation exposure reference is unavailable")
	}
	exposure, err := appworkflow.DecodeWorkflowActivationExposureRef(registration.Principal.ExposureRef)
	if err != nil || exposure.ActivationID != registration.Derivation.TemplateID {
		return graph.DefinitionRef{}, errors.New("activation exposure reference is invalid")
	}
	ref := exposure.Definition
	if registration.Definition.Kind != appworkflow.DefinitionKindRegistry || ref.ID != registration.Definition.ID ||
		ref.Version != registration.Definition.Version || ref.Digest != registration.Definition.Digest {
		return graph.DefinitionRef{}, errors.New("activation exposure reference differs from its stored workflow")
	}
	return ref, nil
}

func (s *Server) handleWorkflowRunAction(w http.ResponseWriter, r *http.Request) {
	if !s.requireWorkflowPOST(w, r) {
		return
	}
	path := strings.TrimPrefix(r.URL.EscapedPath(), "/v1/workflows/runs/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
		return
	}
	decodedRunID, err := url.PathUnescape(parts[0])
	if err != nil || strings.TrimSpace(decodedRunID) == "" {
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
		return
	}
	runID, action := appworkflow.RunID(decodedRunID), parts[1]
	switch action {
	case "inspect":
		s.handleWorkflowInspect(w, r, runID)
	case "cancel":
		s.handleWorkflowCancel(w, r, runID)
	case "resume":
		s.handleWorkflowResume(w, r, runID)
	case "rerun":
		s.handleWorkflowRerun(w, r, runID)
	case "waits", "values", "events":
		s.handleWorkflowRead(w, r, runID, action)
	default:
		writeWorkflowOperationError(w, http.StatusNotFound, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound})
	}
}

func (s *Server) handleWorkflowInspect(w http.ResponseWriter, r *http.Request, runID appworkflow.RunID) {
	var request appworkflow.InspectWorkflowRunRequest
	if !decodeWorkflowRequest(w, r, &request) || !workflowRunIDMatches(w, runID, request.RunID) {
		return
	}
	display := request.Display
	intent := appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessInspect, RunID: runID, Display: &display}
	ctx, ok := s.authenticateWorkflow(w, r, intent, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.Workflows.InspectWorkflowRun(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, false)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func (s *Server) handleWorkflowCancel(w http.ResponseWriter, r *http.Request, runID appworkflow.RunID) {
	var request appworkflow.CancelWorkflowRunRequest
	if !decodeWorkflowRequest(w, r, &request) || !workflowRunIDMatches(w, runID, request.RunID) || !workflowIdempotencyKey(w, r, &request.IdempotencyKey) {
		return
	}
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessCancel, RunID: runID}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.Workflows.CancelWorkflowRun(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, false)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func (s *Server) handleWorkflowResume(w http.ResponseWriter, r *http.Request, runID appworkflow.RunID) {
	var request appworkflow.ResumeWorkflowRunRequest
	if !decodeWorkflowRequest(w, r, &request) || !workflowRunIDMatches(w, runID, request.RunID) || !workflowIdempotencyKey(w, r, &request.IdempotencyKey) {
		return
	}
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessResume, RunID: runID}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.Workflows.ResumeWorkflowRun(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, false)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func (s *Server) handleWorkflowRerun(w http.ResponseWriter, r *http.Request, runID appworkflow.RunID) {
	var request appworkflow.RerunWorkflowRequest
	if !decodeWorkflowRequest(w, r, &request) || !workflowRunIDMatches(w, runID, request.SourceRunID) || !workflowIdempotencyKey(w, r, &request.IdempotencyKey) {
		return
	}
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessRerun, RunID: runID}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	result, err := s.deps.Workflows.RerunWorkflow(ctx, request)
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, false)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func (s *Server) handleWorkflowRead(w http.ResponseWriter, r *http.Request, runID appworkflow.RunID, action string) {
	if s.deps.WorkflowReads == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return
	}
	var request appworkflow.WorkflowRunReadRequest
	if !decodeWorkflowRequest(w, r, &request) || !workflowRunIDMatches(w, runID, request.RunID) {
		return
	}
	operation := appworkflow.WorkflowAccessWaits
	switch action {
	case "values":
		operation = appworkflow.WorkflowAccessValues
	case "events":
		operation = appworkflow.WorkflowAccessEvents
	}
	display := request.Display
	ctx, ok := s.authenticateWorkflow(w, r, appworkflow.WorkflowAccessIntent{Operation: operation, RunID: runID, Display: &display}, false)
	if !ok {
		return
	}
	request.Identity = workflowHTTPIdentity(request.Identity)
	var result any
	var err error
	switch action {
	case "waits":
		result, err = s.deps.WorkflowReads.ListWorkflowWaits(ctx, request)
	case "values":
		result, err = s.deps.WorkflowReads.FetchWorkflowValues(ctx, request)
	case "events":
		result, err = s.deps.WorkflowReads.FetchWorkflowEvents(ctx, request)
	}
	if err != nil {
		s.writeWorkflowFailure(w, err, nil, false)
		return
	}
	writeWorkflowJSON(w, http.StatusOK, result)
}

func (s *Server) requireWorkflowPOST(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		writeWorkflowOperationError(w, http.StatusMethodNotAllowed, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	if s.deps.Workflows == nil || s.deps.WorkflowAuth == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return false
	}
	return true
}

func (s *Server) authenticateWorkflow(w http.ResponseWriter, r *http.Request, intent appworkflow.WorkflowAccessIntent, hideDenied bool) (context.Context, bool) {
	if s.deps.Workflows == nil || s.deps.WorkflowAuth == nil {
		writeWorkflowOperationError(w, http.StatusServiceUnavailable, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeUnavailable})
		return nil, false
	}
	ctx, err := s.deps.WorkflowAuth.AuthenticateWorkflowRequest(r, intent)
	if err != nil || ctx == nil {
		operationError := appworkflow.SafeWorkflowOperationError(err, nil)
		if ctx == nil && err == nil {
			operationError.Code = appworkflow.WorkflowErrorCodeUnauthenticated
		}
		operationError = hideWorkflowDefinitionDenial(operationError, hideDenied)
		status := workflowHTTPStatus(operationError.Code)
		writeWorkflowOperationError(w, status, operationError)
		return nil, false
	}
	return ctx, true
}

func workflowHTTPIdentity(input appworkflow.IdentityRequest) appworkflow.IdentityRequest {
	return appworkflow.IdentityRequest{SourceAuthority: workflowHTTPSourceAuthority, RunScope: input.RunScope, ExecutionTarget: input.ExecutionTarget}
}

func workflowRunIDMatches(w http.ResponseWriter, path, body appworkflow.RunID) bool {
	if path == "" || body == "" || path != body {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	return true
}

func workflowIdempotencyKey(w http.ResponseWriter, r *http.Request, body *string) bool {
	if len(r.Header.Values("Idempotency-Key")) > 1 {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	header := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	current := strings.TrimSpace(*body)
	if header != "" && current != "" && header != current {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	if current == "" {
		current = header
	}
	if current == "" {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	*body = current
	return true
}

func (s *Server) writeWorkflowFailure(w http.ResponseWriter, err error, result *appworkflow.StartRunResult, hideDenied bool) {
	operationError := hideWorkflowDefinitionDenial(appworkflow.SafeWorkflowOperationError(err, result), hideDenied)
	writeWorkflowOperationError(w, workflowHTTPStatus(operationError.Code), operationError)
}

func hideWorkflowDefinitionDenial(operationError appworkflow.WorkflowOperationError, hideDenied bool) appworkflow.WorkflowOperationError {
	if hideDenied && operationError.Code == appworkflow.WorkflowErrorCodePolicyDenied {
		return appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeNotFound}
	}
	return operationError
}

func workflowHTTPStatus(code string) int {
	switch code {
	case appworkflow.WorkflowErrorCodeUnauthenticated:
		return http.StatusUnauthorized
	case appworkflow.WorkflowErrorCodePolicyDenied:
		return http.StatusForbidden
	case appworkflow.WorkflowErrorCodeNotFound:
		return http.StatusNotFound
	case appworkflow.WorkflowErrorCodeInvalidRequest:
		return http.StatusBadRequest
	case appworkflow.WorkflowErrorCodeConfirmationRequired, appworkflow.WorkflowErrorCodePinRejected, appworkflow.WorkflowErrorCodeIdempotencyConflict, appworkflow.WorkflowErrorCodeActivationConflict:
		return http.StatusConflict
	case appworkflow.WorkflowErrorCodeDryRunUnsupported:
		return http.StatusUnprocessableEntity
	case appworkflow.WorkflowErrorCodeUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func decodeWorkflowRequest(w http.ResponseWriter, r *http.Request, destination any) bool {
	reader := http.MaxBytesReader(w, r.Body, maximumWorkflowHTTPRequestBytes)
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	value, err := decodeUniqueWorkflowJSON(decoder)
	if err != nil {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	if _, trailingErr := decoder.Token(); !errors.Is(trailingErr, io.EOF) {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	data, err := json.Marshal(value)
	if err != nil {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	typed := json.NewDecoder(bytes.NewReader(data))
	typed.UseNumber()
	typed.DisallowUnknownFields()
	if err := typed.Decode(destination); err != nil {
		writeWorkflowOperationError(w, http.StatusBadRequest, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodeInvalidRequest})
		return false
	}
	return true
}

func decodeUniqueWorkflowJSON(decoder *json.Decoder) (any, error) {
	return decodeUniqueWorkflowJSONDepth(decoder, 0)
}

func decodeUniqueWorkflowJSONDepth(decoder *json.Decoder, depth int) (any, error) {
	if depth > maximumWorkflowHTTPJSONDepth {
		return nil, errors.New("workflow JSON nesting exceeds the supported depth")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch delimiter := token.(type) {
	case json.Delim:
		switch delimiter {
		case '{':
			object := make(map[string]any)
			for decoder.More() {
				keyToken, keyErr := decoder.Token()
				if keyErr != nil {
					return nil, keyErr
				}
				key, ok := keyToken.(string)
				if !ok {
					return nil, errors.New("workflow JSON object key is not a string")
				}
				if _, duplicate := object[key]; duplicate {
					return nil, errors.New("workflow JSON contains duplicate object fields")
				}
				child, childErr := decodeUniqueWorkflowJSONDepth(decoder, depth+1)
				if childErr != nil {
					return nil, childErr
				}
				object[key] = child
			}
			end, endErr := decoder.Token()
			if endErr != nil || end != json.Delim('}') {
				return nil, errors.New("workflow JSON object is incomplete")
			}
			return object, nil
		case '[':
			array := make([]any, 0)
			for decoder.More() {
				child, childErr := decodeUniqueWorkflowJSONDepth(decoder, depth+1)
				if childErr != nil {
					return nil, childErr
				}
				array = append(array, child)
			}
			end, endErr := decoder.Token()
			if endErr != nil || end != json.Delim(']') {
				return nil, errors.New("workflow JSON array is incomplete")
			}
			return array, nil
		default:
			return nil, errors.New("workflow JSON has an unexpected delimiter")
		}
	default:
		return token, nil
	}
}

func writeWorkflowOperationError(w http.ResponseWriter, status int, response appworkflow.WorkflowOperationError) {
	writeWorkflowJSON(w, status, response)
}

func writeWorkflowJSON(w http.ResponseWriter, status int, response any) {
	data, err := json.Marshal(response)
	if err != nil || len(data)+1 > maximumWorkflowHTTPResponseBytes {
		data = []byte(`{"code":"internal_error"}`)
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(append(data, '\n')); err != nil {
		return
	}
}
