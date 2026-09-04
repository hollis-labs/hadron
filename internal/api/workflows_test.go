package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/internal/trigger"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

type workflowHTTPService struct {
	validate func(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error)
	explain  func(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error)
	run      func(context.Context, appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error)
	inspect  func(context.Context, appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error)
	cancel   func(context.Context, appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error)
	resume   func(context.Context, appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error)
	rerun    func(context.Context, appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error)
	waits    func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowWaitListResult, error)
	values   func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowValueListResult, error)
	events   func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error)
}

func (s *workflowHTTPService) ValidateWorkflow(ctx context.Context, request appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
	if s.validate != nil {
		return s.validate(ctx, request)
	}
	return appworkflow.ValidateWorkflowResult{}, nil
}
func (s *workflowHTTPService) ExplainWorkflow(ctx context.Context, request appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
	if s.explain != nil {
		return s.explain(ctx, request)
	}
	return appworkflow.StartRunResult{}, nil
}
func (s *workflowHTTPService) RunWorkflow(ctx context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
	if s.run != nil {
		return s.run(ctx, request)
	}
	return appworkflow.StartRunResult{}, nil
}
func (s *workflowHTTPService) InspectWorkflowRun(ctx context.Context, request appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
	if s.inspect != nil {
		return s.inspect(ctx, request)
	}
	return rundiagnostics.Result{}, nil
}
func (s *workflowHTTPService) CancelWorkflowRun(ctx context.Context, request appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error) {
	if s.cancel != nil {
		return s.cancel(ctx, request)
	}
	return appworkflow.CancelWorkflowRunResult{}, nil
}
func (s *workflowHTTPService) ResumeWorkflowRun(ctx context.Context, request appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
	if s.resume != nil {
		return s.resume(ctx, request)
	}
	return appworkflow.ResumeWorkflowRunResult{}, nil
}
func (s *workflowHTTPService) RerunWorkflow(ctx context.Context, request appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error) {
	if s.rerun != nil {
		return s.rerun(ctx, request)
	}
	return appworkflow.RerunWorkflowResult{}, nil
}
func (s *workflowHTTPService) ListWorkflowWaits(ctx context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowWaitListResult, error) {
	return s.waits(ctx, request)
}
func (s *workflowHTTPService) FetchWorkflowValues(ctx context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowValueListResult, error) {
	return s.values(ctx, request)
}
func (s *workflowHTTPService) FetchWorkflowEvents(ctx context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
	return s.events(ctx, request)
}

type workflowAuthKey struct{}

func workflowHTTPTestServer(t *testing.T, service *workflowHTTPService, authenticate api.WorkflowRequestAuthenticatorFunc) *httptest.Server {
	t.Helper()
	server := api.NewServer("", api.Dependencies{Workflows: service, WorkflowReads: service, WorkflowAuth: authenticate})
	return httptest.NewServer(server.Handler())
}

type workflowActivationHTTPService struct {
	registration hoststate.ActivationRegistration
	loadErr      error
	fireResult   appworkflow.ActivationStartResult
	fireErr      error
	loadCalls    int
	fireCalls    int
	lastEvent    trigger.ActivationEvent
	lastContext  context.Context
}

func (s *workflowActivationHTTPService) LoadRegistration(_ context.Context, id string) (hoststate.ActivationRegistration, error) {
	s.loadCalls++
	if s.loadErr != nil {
		return hoststate.ActivationRegistration{}, s.loadErr
	}
	if id != s.registration.ID {
		return hoststate.ActivationRegistration{}, workflowruntime.ErrNotFound
	}
	return s.registration, nil
}

func (s *workflowActivationHTTPService) Fire(ctx context.Context, event trigger.ActivationEvent) (appworkflow.ActivationStartResult, error) {
	s.fireCalls++
	s.lastContext = ctx
	s.lastEvent = event
	return s.fireResult, s.fireErr
}

func workflowActivationRegistration(t *testing.T, registryName, version string) hoststate.ActivationRegistration {
	t.Helper()
	digest := values.SHA256Digest([]byte(registryName + "@" + version))
	exposure, err := appworkflow.EncodeWorkflowActivationExposureRef(graph.DefinitionRef{
		Kind: appworkflow.DefinitionKindRegistry, ID: registryName, Version: version, Digest: digest,
	}, "external-hook")
	if err != nil {
		t.Fatal(err)
	}
	return hoststate.ActivationRegistration{
		ID: "external-hook",
		Definition: graph.DefinitionRef{
			Authority: "project", Kind: appworkflow.DefinitionKindRegistry, ID: registryName, Version: version, Digest: digest,
		},
		Principal:  hoststate.ActivationPrincipal{ExposureRef: exposure},
		Derivation: &hoststate.ActivationDerivation{TemplateID: "external-hook"},
	}
}

func workflowActivationHTTPTestServer(t *testing.T, activation *workflowActivationHTTPService, authenticate api.WorkflowRequestAuthenticatorFunc) *httptest.Server {
	t.Helper()
	workflow := &workflowHTTPService{}
	server := api.NewServer("", api.Dependencies{
		Workflows: workflow, WorkflowReads: workflow, WorkflowAuth: authenticate, WorkflowActivations: activation,
	})
	return httptest.NewServer(server.Handler())
}

func workflowPOST(t *testing.T, server *httptest.Server, path, body string, headers map[string]string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func workflowDecode[T any](t *testing.T, response *http.Response) T {
	t.Helper()
	defer func() { _ = response.Body.Close() }()
	var result T
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func allowWorkflowHTTP(request *http.Request, intent appworkflow.WorkflowAccessIntent) (context.Context, error) {
	if request.Header.Get("Authorization") != "Bearer allowed" {
		return nil, appworkflow.ErrWorkflowUnauthenticated
	}
	if intent.Definition != nil && intent.Definition.ID == "private/hidden" {
		return nil, appworkflow.ErrWorkflowHidden
	}
	if intent.RunID == "hidden-run" {
		return nil, appworkflow.ErrWorkflowHidden
	}
	if intent.Display != nil && intent.Display.RevealsPrivate() && request.Header.Get("X-Test-Private") != "allow" {
		return nil, appworkflow.ErrPolicyDenied
	}
	return context.WithValue(request.Context(), workflowAuthKey{}, "user:authenticated"), nil
}

func TestWorkflowHTTPAuthenticatesAndSanitizesIdentity(t *testing.T) {
	service := &workflowHTTPService{}
	service.validate = func(ctx context.Context, request appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		if ctx.Value(workflowAuthKey{}) != "user:authenticated" {
			t.Fatal("authenticated context was not forwarded")
		}
		if request.Identity.PrincipalHint != "" || request.Identity.SourceAuthority != "http" || request.Identity.Attributes != nil {
			t.Fatalf("transported identity was trusted: %#v", request.Identity)
		}
		if request.Identity.RunScope == nil || request.Identity.RunScope.ID != "team" {
			t.Fatalf("scope selector was not forwarded: %#v", request.Identity.RunScope)
		}
		return appworkflow.ValidateWorkflowResult{Definition: request.Definition}, nil
	}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	body := `{"definition":{"kind":"registry","id":"team/example","version":"v1"},"identity":{"principal_hint":"forged","source_authority":"forged","attributes":{"exposure_ref":"admin"},"run_scope":{"version":"v1","kind":"team","id":"team"}}}`
	response := workflowPOST(t, server, "/v1/workflows/validate", body, map[string]string{"Authorization": "Bearer allowed"})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d response=%#v", response.StatusCode, workflowDecode[appworkflow.WorkflowOperationError](t, response))
	}
	_ = response.Body.Close()
}

func TestWorkflowHTTPHiddenDefinitionMatchesNonexistentBeforeService(t *testing.T) {
	serviceCalls := 0
	service := &workflowHTTPService{validate: func(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		serviceCalls++
		return appworkflow.ValidateWorkflowResult{}, workflowruntime.ErrNotFound
	}, explain: func(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
		serviceCalls++
		return appworkflow.StartRunResult{}, workflowruntime.ErrNotFound
	}, run: func(context.Context, appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
		serviceCalls++
		return appworkflow.StartRunResult{}, workflowruntime.ErrNotFound
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	requests := []struct{ path, hidden, missing string }{
		{"/v1/workflows/validate", `{"definition":{"kind":"registry","id":"private/hidden","version":"v1"},"identity":{}}`, `{"definition":{"kind":"registry","id":"missing/notfound","version":"v1"},"identity":{}}`},
		{"/v1/workflows/explain", `{"run_id":"explain","definition":{"kind":"registry","id":"private/hidden","version":"v1"},"idempotency_key":"explain-key","identity":{}}`, `{"run_id":"explain","definition":{"kind":"registry","id":"missing/notfound","version":"v1"},"idempotency_key":"explain-key","identity":{}}`},
		{"/v1/workflows/runs", `{"run_id":"run","definition":{"kind":"registry","id":"private/hidden","version":"v1"},"idempotency_key":"run-key","identity":{}}`, `{"run_id":"run","definition":{"kind":"registry","id":"missing/notfound","version":"v1"},"idempotency_key":"run-key","identity":{}}`},
	}
	for _, test := range requests {
		hiddenResponse := workflowPOST(t, server, test.path, test.hidden, map[string]string{"Authorization": "Bearer allowed"})
		hiddenBody, hiddenErr := io.ReadAll(hiddenResponse.Body)
		_ = hiddenResponse.Body.Close()
		missingResponse := workflowPOST(t, server, test.path, test.missing, map[string]string{"Authorization": "Bearer allowed"})
		missingBody, missingErr := io.ReadAll(missingResponse.Body)
		_ = missingResponse.Body.Close()
		if hiddenErr != nil || missingErr != nil || hiddenResponse.StatusCode != http.StatusNotFound || missingResponse.StatusCode != http.StatusNotFound || !bytes.Equal(hiddenBody, missingBody) || string(hiddenBody) != `{"code":"not_found"}`+"\n" {
			t.Fatalf("%s hidden=%d %s missing=%d %s errors=%v/%v", test.path, hiddenResponse.StatusCode, hiddenBody, missingResponse.StatusCode, missingBody, hiddenErr, missingErr)
		}
	}
	if serviceCalls != len(requests) {
		t.Fatalf("hidden definitions crossed profile boundary: service calls=%d", serviceCalls)
	}
}

func TestWorkflowHTTPHiddenRunReadsMatchNonexistent(t *testing.T) {
	serviceCalls := 0
	missing := func(runID appworkflow.RunID) error {
		if runID == "hidden-run" {
			t.Fatal("hidden run crossed profile boundary")
		}
		serviceCalls++
		return workflowruntime.ErrNotFound
	}
	service := &workflowHTTPService{
		inspect: func(_ context.Context, request appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
			return rundiagnostics.Result{}, missing(request.RunID)
		},
		cancel: func(_ context.Context, request appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error) {
			return appworkflow.CancelWorkflowRunResult{}, missing(request.RunID)
		},
		resume: func(_ context.Context, request appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
			return appworkflow.ResumeWorkflowRunResult{}, missing(request.RunID)
		},
		rerun: func(_ context.Context, request appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error) {
			return appworkflow.RerunWorkflowResult{}, missing(request.SourceRunID)
		},
		waits: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowWaitListResult, error) {
			return appworkflow.WorkflowWaitListResult{}, missing(request.RunID)
		},
		values: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowValueListResult, error) {
			return appworkflow.WorkflowValueListResult{}, missing(request.RunID)
		},
		events: func(_ context.Context, request appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
			return appworkflow.WorkflowEventListResult{}, missing(request.RunID)
		},
	}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	tests := []struct {
		action      string
		hiddenBody  string
		missingBody string
	}{
		{"inspect", `{"run_id":"hidden-run","identity":{}}`, `{"run_id":"missing-run","identity":{}}`},
		{"cancel", `{"run_id":"hidden-run","idempotency_key":"cancel-key","identity":{}}`, `{"run_id":"missing-run","idempotency_key":"cancel-key","identity":{}}`},
		{"resume", `{"run_id":"hidden-run","wait_id":"wait-one","idempotency_key":"resume-key","identity":{}}`, `{"run_id":"missing-run","wait_id":"wait-one","idempotency_key":"resume-key","identity":{}}`},
		{"rerun", `{"source_run_id":"hidden-run","run_id":"rerun-new","idempotency_key":"rerun-key","identity":{}}`, `{"source_run_id":"missing-run","run_id":"rerun-new","idempotency_key":"rerun-key","identity":{}}`},
		{"waits", `{"run_id":"hidden-run","identity":{}}`, `{"run_id":"missing-run","identity":{}}`},
		{"values", `{"run_id":"hidden-run","identity":{}}`, `{"run_id":"missing-run","identity":{}}`},
		{"events", `{"run_id":"hidden-run","identity":{}}`, `{"run_id":"missing-run","identity":{}}`},
	}
	for _, test := range tests {
		hidden := workflowPOST(t, server, "/v1/workflows/runs/hidden-run/"+test.action, test.hiddenBody, map[string]string{"Authorization": "Bearer allowed"})
		hiddenBody, hiddenErr := io.ReadAll(hidden.Body)
		_ = hidden.Body.Close()
		missingResponse := workflowPOST(t, server, "/v1/workflows/runs/missing-run/"+test.action, test.missingBody, map[string]string{"Authorization": "Bearer allowed"})
		missingBody, missingErr := io.ReadAll(missingResponse.Body)
		_ = missingResponse.Body.Close()
		if hiddenErr != nil || missingErr != nil || hidden.StatusCode != http.StatusNotFound || missingResponse.StatusCode != http.StatusNotFound || !bytes.Equal(hiddenBody, missingBody) || string(hiddenBody) != `{"code":"not_found"}`+"\n" {
			t.Fatalf("%s hidden=%d %s missing=%d %s errors=%v/%v", test.action, hidden.StatusCode, hiddenBody, missingResponse.StatusCode, missingBody, hiddenErr, missingErr)
		}
	}
	if serviceCalls != len(tests) {
		t.Fatalf("hidden runs crossed profile boundary: service calls=%d", serviceCalls)
	}
}

func TestWorkflowHTTPPreservesEscapedOpaqueRunID(t *testing.T) {
	runID := appworkflow.RunID("source/run one")
	service := &workflowHTTPService{inspect: func(_ context.Context, request appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
		if request.RunID != runID {
			t.Fatalf("inspect run id = %q, want %q", request.RunID, runID)
		}
		return rundiagnostics.Result{}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()

	response := workflowPOST(t, server, "/v1/workflows/runs/"+url.PathEscape(string(runID))+"/inspect", `{"run_id":"source/run one","identity":{}}`, map[string]string{"Authorization": "Bearer allowed"})
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("escaped run id status=%d body=%s", response.StatusCode, body)
	}
}

func TestWorkflowHTTPUnauthenticatedIsSafe(t *testing.T) {
	service := &workflowHTTPService{validate: func(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		t.Fatal("unauthenticated request reached service")
		return appworkflow.ValidateWorkflowResult{}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	response := workflowPOST(t, server, "/v1/workflows/validate", `{"definition":{"kind":"registry","id":"team/example","version":"v1"},"identity":{}}`, nil)
	result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
	if response.StatusCode != http.StatusUnauthorized || result.Code != appworkflow.WorkflowErrorCodeUnauthenticated || result.Result != nil || len(result.Diagnostics) != 0 {
		t.Fatalf("status=%d result=%#v", response.StatusCode, result)
	}
}

func TestWorkflowHTTPRejectsAmbiguousRequestsAndRunMismatch(t *testing.T) {
	server := workflowHTTPTestServer(t, &workflowHTTPService{}, allowWorkflowHTTP)
	defer server.Close()
	tests := []struct{ path, body string }{
		{"/v1/workflows/validate", `{"definition":{"kind":"registry","id":"team/one","id":"team/two","version":"v1"},"identity":{}}`},
		{"/v1/workflows/validate", `{"definition":{"kind":"registry","id":"team/one","version":"v1"},"identity":{},"unknown":true}`},
		{"/v1/workflows/validate", `{"definition":{"kind":"registry","id":"team/one","version":"v1"},"identity":{}} {}`},
		{"/v1/workflows/validate", strings.Repeat("[", 130) + "null" + strings.Repeat("]", 130)},
		{"/v1/workflows/runs/path-run/inspect", `{"run_id":"body-run","identity":{}}`},
		{"/v1/workflows/runs/run-one/cancel", `{"run_id":"run-one","identity":{}}`},
		{"/v1/workflows/runs/run-one/rerun", `{"source_run_id":"run-one","run_id":"rerun-one","identity":{}}`},
	}
	for _, test := range tests {
		response := workflowPOST(t, server, test.path, test.body, map[string]string{"Authorization": "Bearer allowed"})
		result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
		if response.StatusCode != http.StatusBadRequest || result.Code != appworkflow.WorkflowErrorCodeInvalidRequest {
			t.Fatalf("%s status=%d result=%#v", test.path, response.StatusCode, result)
		}
	}
}

func TestWorkflowHTTPResumeIdempotencyAndHeaderAgreement(t *testing.T) {
	calls := 0
	service := &workflowHTTPService{resume: func(_ context.Context, request appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
		calls++
		if request.IdempotencyKey != "resume-key" {
			t.Fatalf("idempotency key=%q", request.IdempotencyKey)
		}
		outcome := workflowruntime.ResumeApplied
		if calls > 1 {
			outcome = workflowruntime.ResumeReplayed
		}
		return appworkflow.ResumeWorkflowRunResult{Outcome: outcome}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	body := `{"run_id":"run-one","wait_id":"wait-one","correlation":"corr","token":"token","wake_source":"gate","identity":{}}`
	for expected, outcome := range []workflowruntime.ResumeOutcome{workflowruntime.ResumeApplied, workflowruntime.ResumeReplayed} {
		response := workflowPOST(t, server, "/v1/workflows/runs/run-one/resume", body, map[string]string{"Authorization": "Bearer allowed", "Idempotency-Key": "resume-key"})
		result := workflowDecode[appworkflow.ResumeWorkflowRunResult](t, response)
		if response.StatusCode != http.StatusOK || result.Outcome != outcome || calls != expected+1 {
			t.Fatalf("status=%d result=%#v calls=%d", response.StatusCode, result, calls)
		}
	}
	conflict := workflowPOST(t, server, "/v1/workflows/runs/run-one/resume", strings.Replace(body, `"identity":{}`, `"idempotency_key":"body-key","identity":{}`, 1), map[string]string{"Authorization": "Bearer allowed", "Idempotency-Key": "header-key"})
	if conflict.StatusCode != http.StatusBadRequest {
		t.Fatalf("conflict status=%d", conflict.StatusCode)
	}
	_ = conflict.Body.Close()
}

func TestWorkflowHTTPPreservesStructuredStartRejections(t *testing.T) {
	service := &workflowHTTPService{run: func(_ context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
		if request.DryRun {
			result := appworkflow.StartRunResult{DryRun: true, Facts: hoststate.PolicyFacts{DryRunAvailable: false}}
			return result, appworkflow.ErrDryRunUnsupported
		}
		ref := values.ValueSetRef{ID: "inputs", Digest: values.SHA256Digest([]byte("inputs"))}
		result := appworkflow.StartRunResult{Bound: &workflowruntime.BoundRun{ID: request.RunID, InputsRef: ref}, Run: &workflowruntime.RunSnapshot{ID: request.RunID, Status: workflowruntime.RunCanceled, Inputs: &ref}, Phase: hoststate.StartPinsRejected}
		return result, appworkflow.ErrPolicyDenied
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	tests := []struct {
		body   string
		status int
		code   string
	}{
		{`{"run_id":"preview","definition":{"kind":"registry","id":"team/example","version":"v1"},"idempotency_key":"preview-key","dry_run":true,"identity":{}}`, http.StatusUnprocessableEntity, appworkflow.WorkflowErrorCodeDryRunUnsupported},
		{`{"run_id":"pinned","definition":{"kind":"registry","id":"team/example","version":"v1"},"idempotency_key":"pin-key","identity":{}}`, http.StatusConflict, appworkflow.WorkflowErrorCodePinRejected},
	}
	for _, test := range tests {
		response := workflowPOST(t, server, "/v1/workflows/runs", test.body, map[string]string{"Authorization": "Bearer allowed"})
		result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
		if response.StatusCode != test.status || result.Code != test.code || result.Result == nil {
			t.Fatalf("status=%d result=%#v", response.StatusCode, result)
		}
		if test.code == appworkflow.WorkflowErrorCodePinRejected && !result.Result.RejectedBeforeAdmission() {
			t.Fatalf("pin rejection lost terminal result: %#v", result.Result)
		}
	}
}

func TestWorkflowHTTPReadSurfacesAreRedactedAndPrivateRevealIsAuthorizedFirst(t *testing.T) {
	masked := values.RenderedValue{Type: values.TypeString, Payload: values.RedactedMarker, Producer: values.Producer{Kind: "test", Reference: "test"}, MediaType: "text/plain", Digest: values.SHA256Digest([]byte("secret")), Redaction: values.RedactionSecret, Retention: values.RetentionRun, Masked: true}
	valueRef := values.ValueSetRef{ID: "redacted-values", Digest: values.SHA256Digest([]byte("redacted-values"))}
	service := &workflowHTTPService{
		waits: func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowWaitListResult, error) {
			return appworkflow.WorkflowWaitListResult{RunID: "run-one", Waits: []appworkflow.WorkflowWaitListItem{}}, nil
		},
		values: func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowValueListResult, error) {
			return appworkflow.WorkflowValueListResult{RunID: "run-one", Values: []rundiagnostics.ValueSetDiagnostic{{Ref: valueRef, Roles: []string{"run.inputs"}, Values: values.RenderedValueSet{"password": masked}}}}, nil
		},
		events: func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
			return appworkflow.WorkflowEventListResult{RunID: "run-one", Events: []workflowruntime.RenderedEvent{{RunID: "run-one", Type: "secret.event", Masked: true, Redaction: values.RedactionSecret}}}, nil
		},
	}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	for _, action := range []string{"waits", "values", "events"} {
		response := workflowPOST(t, server, "/v1/workflows/runs/run-one/"+action, `{"run_id":"run-one","identity":{}}`, map[string]string{"Authorization": "Bearer allowed"})
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || bytes.Contains(body, []byte("password-value")) {
			t.Fatalf("%s status=%d body=%s error=%v", action, response.StatusCode, body, err)
		}
		if action == "values" && (!bytes.Contains(body, []byte(`"payload":"[REDACTED]"`)) || !bytes.Contains(body, []byte(`"masked":true`))) {
			t.Fatalf("values were not explicitly masked: %s", body)
		}
	}
	service.inspect = func(context.Context, appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
		t.Fatal("unauthorized private inspect reached service")
		return rundiagnostics.Result{}, nil
	}
	service.waits = func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowWaitListResult, error) {
		t.Fatal("unauthorized private waits reached service")
		return appworkflow.WorkflowWaitListResult{}, nil
	}
	service.values = func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowValueListResult, error) {
		t.Fatal("unauthorized private values reached service")
		return appworkflow.WorkflowValueListResult{}, nil
	}
	service.events = func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
		t.Fatal("unauthorized private events reached service")
		return appworkflow.WorkflowEventListResult{}, nil
	}
	for _, action := range []string{"inspect", "waits", "values", "events"} {
		denied := workflowPOST(t, server, "/v1/workflows/runs/run-one/"+action, `{"run_id":"run-one","identity":{},"display":{"private":"reveal"}}`, map[string]string{"Authorization": "Bearer allowed"})
		result := workflowDecode[appworkflow.WorkflowOperationError](t, denied)
		if denied.StatusCode != http.StatusForbidden || result.Code != appworkflow.WorkflowErrorCodePolicyDenied {
			t.Fatalf("%s status=%d result=%#v", action, denied.StatusCode, result)
		}
	}
}

func TestWorkflowHTTPCORSAllowsIdempotencyKey(t *testing.T) {
	server := workflowHTTPTestServer(t, &workflowHTTPService{}, allowWorkflowHTTP)
	defer server.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodOptions, server.URL+"/v1/workflows/runs", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Origin", server.URL)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if !strings.Contains(response.Header.Get("Access-Control-Allow-Headers"), "Idempotency-Key") {
		t.Fatalf("allow headers=%q", response.Header.Get("Access-Control-Allow-Headers"))
	}
}

func TestWorkflowHTTPBoundsRequestsAndResponses(t *testing.T) {
	service := &workflowHTTPService{validate: func(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
		return appworkflow.ValidateWorkflowResult{Diagnostics: nil, Definition: graphDefinitionWithLargeLocator()}, nil
	}}
	server := workflowHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	oversized := workflowPOST(t, server, "/v1/workflows/validate", strings.Repeat(" ", (2<<20)+1), map[string]string{"Authorization": "Bearer allowed"})
	requestError := workflowDecode[appworkflow.WorkflowOperationError](t, oversized)
	if oversized.StatusCode != http.StatusBadRequest || requestError.Code != appworkflow.WorkflowErrorCodeInvalidRequest {
		t.Fatalf("request status=%d result=%#v", oversized.StatusCode, requestError)
	}
	response := workflowPOST(t, server, "/v1/workflows/validate", `{"definition":{"kind":"registry","id":"team/example","version":"v1"},"identity":{}}`, map[string]string{"Authorization": "Bearer allowed"})
	responseError := workflowDecode[appworkflow.WorkflowOperationError](t, response)
	if response.StatusCode != http.StatusInternalServerError || responseError.Code != appworkflow.WorkflowErrorCodeInternal {
		t.Fatalf("response status=%d result=%#v", response.StatusCode, responseError)
	}
}

func TestWorkflowActivationHTTPAuthenticatesBeforeLookupThenAuthorizesExactDefinition(t *testing.T) {
	registration := workflowActivationRegistration(t, "team@alpha#?&/workflow-one", "v1@#/?&")
	activation := &workflowActivationHTTPService{
		registration: registration,
		fireResult: appworkflow.ActivationStartResult{Outcome: workflowruntime.IdempotencyApplied, Start: appworkflow.StartRunResult{
			Run: &workflowruntime.RunSnapshot{ID: "activation-run", Status: workflowruntime.RunPending}, Outcome: workflowruntime.IdempotencyApplied,
		}},
	}
	var intents []appworkflow.WorkflowAccessIntent
	authenticate := func(request *http.Request, intent appworkflow.WorkflowAccessIntent) (context.Context, error) {
		intents = append(intents, intent)
		if request.Header.Get("Authorization") != "Bearer allowed" {
			return nil, appworkflow.ErrWorkflowUnauthenticated
		}
		return context.WithValue(request.Context(), workflowAuthKey{}, "delivery:authorized"), nil
	}
	server := workflowActivationHTTPTestServer(t, activation, authenticate)
	defer server.Close()
	body := `{"registration_id":"external-hook","idempotency_key":"delivery-one","occurred_at":"2026-08-24T12:00:00Z","payload":{"message":"hello"}}`

	unauthenticated := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire", body, nil)
	unauthenticatedResult := workflowDecode[appworkflow.WorkflowOperationError](t, unauthenticated)
	if unauthenticated.StatusCode != http.StatusUnauthorized || unauthenticatedResult.Code != appworkflow.WorkflowErrorCodeUnauthenticated || activation.loadCalls != 0 {
		t.Fatalf("unauthenticated status=%d result=%#v load=%d", unauthenticated.StatusCode, unauthenticatedResult, activation.loadCalls)
	}

	response := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire", body, map[string]string{"Authorization": "Bearer allowed"})
	result := workflowDecode[appworkflow.FireWorkflowActivationResult](t, response)
	if response.StatusCode != http.StatusOK || result.Kind != appworkflow.WorkflowActivationFireDirect || result.Start == nil || result.Start.Run == nil || result.Start.Run.ID != "activation-run" || activation.loadCalls != 1 || activation.fireCalls != 1 {
		t.Fatalf("activation response status=%d result=%#v load=%d fire=%d", response.StatusCode, result, activation.loadCalls, activation.fireCalls)
	}
	if len(intents) != 3 || intents[1].Definition != nil || intents[2].Definition == nil ||
		intents[2].Definition.Kind != appworkflow.DefinitionKindRegistry || intents[2].Definition.ID != "team@alpha#?&/workflow-one" ||
		intents[2].Definition.Version != "v1@#/?&" || intents[2].Definition.Digest != registration.Definition.Digest {
		t.Fatalf("activation auth intents = %#v", intents)
	}
	if activation.lastContext.Value(workflowAuthKey{}) != "delivery:authorized" || activation.lastEvent.IdempotencyKey != "delivery-one" ||
		activation.lastEvent.SourceRef != "http" || activation.lastEvent.RegistrationID != "external-hook" {
		t.Fatalf("activation delivery = %#v context=%#v", activation.lastEvent, activation.lastContext)
	}
}

func TestWorkflowLifecycleInspectionProvidesExactActivationHandleForFire(t *testing.T) {
	registration := workflowActivationRegistration(t, "team/workflow-one", "v1")
	registration.ID = "derived-external-handle"
	definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: registration.Definition.ID, Version: registration.Definition.Version, Digest: registration.Definition.Digest}
	detail := appworkflow.WorkflowVersionDetail{Activations: []appworkflow.WorkflowActivationDescriptor{{
		TemplateID: registration.Derivation.TemplateID, Kind: string(registration.Source.Kind), RegistrationID: registration.ID,
		Enabled: true, Definition: definition,
	}}}
	lifecycle := &workflowLifecycleHTTPSpy{inspect: func(context.Context, appworkflow.InspectWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
		return detail, nil
	}}
	activation := &workflowActivationHTTPService{
		registration: registration,
		fireResult: appworkflow.ActivationStartResult{Outcome: workflowruntime.IdempotencyApplied, Start: appworkflow.StartRunResult{
			Run: &workflowruntime.RunSnapshot{ID: "activation-from-discovery", Status: workflowruntime.RunPending}, Outcome: workflowruntime.IdempotencyApplied,
		}},
	}
	server := httptest.NewServer(api.NewServer("", api.Dependencies{
		Workflows: &workflowHTTPService{}, WorkflowLifecycle: lifecycle, WorkflowActivations: activation,
		WorkflowAuth: api.WorkflowRequestAuthenticatorFunc(allowWorkflowHTTP),
	}).Handler())
	defer server.Close()
	inspectBody, err := json.Marshal(appworkflow.InspectWorkflowVersionRequest{Definition: definition})
	if err != nil {
		t.Fatal(err)
	}
	inspectedResponse := workflowPOST(t, server, "/v1/workflows/lifecycle/catalog/inspect", string(inspectBody), map[string]string{"Authorization": "Bearer allowed"})
	inspected := workflowDecode[appworkflow.WorkflowVersionDetail](t, inspectedResponse)
	if inspectedResponse.StatusCode != http.StatusOK || len(inspected.Activations) != 1 {
		t.Fatalf("activation discovery status=%d result=%#v", inspectedResponse.StatusCode, inspected)
	}
	handle := inspected.Activations[0]
	fireBody, err := json.Marshal(appworkflow.FireWorkflowActivationRequest{
		RegistrationID: handle.RegistrationID, IdempotencyKey: "discovered-fire", OccurredAt: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	firedResponse := workflowPOST(t, server, "/v1/workflows/activations/"+url.PathEscape(handle.RegistrationID)+"/fire", string(fireBody), map[string]string{"Authorization": "Bearer allowed"})
	fired := workflowDecode[appworkflow.FireWorkflowActivationResult](t, firedResponse)
	if firedResponse.StatusCode != http.StatusOK || fired.Start == nil || fired.Start.Run == nil || fired.Start.Run.ID != "activation-from-discovery" ||
		activation.lastEvent.RegistrationID != handle.RegistrationID {
		t.Fatalf("discovered activation fire status=%d result=%#v event=%#v", firedResponse.StatusCode, fired, activation.lastEvent)
	}
}

func TestWorkflowActivationHTTPHiddenDefinitionDoesNotFire(t *testing.T) {
	registration := workflowActivationRegistration(t, "private/workflow-one", "v1")
	activation := &workflowActivationHTTPService{registration: registration}
	authenticate := func(request *http.Request, intent appworkflow.WorkflowAccessIntent) (context.Context, error) {
		if request.Header.Get("Authorization") != "Bearer allowed" {
			return nil, appworkflow.ErrWorkflowUnauthenticated
		}
		if intent.Definition != nil {
			return nil, appworkflow.ErrWorkflowHidden
		}
		return request.Context(), nil
	}
	server := workflowActivationHTTPTestServer(t, activation, authenticate)
	defer server.Close()
	response := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire",
		`{"registration_id":"external-hook","idempotency_key":"delivery-hidden","occurred_at":"2026-08-24T12:00:00Z"}`,
		map[string]string{"Authorization": "Bearer allowed"})
	result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
	if response.StatusCode != http.StatusNotFound || result.Code != appworkflow.WorkflowErrorCodeNotFound || activation.loadCalls != 1 || activation.fireCalls != 0 {
		t.Fatalf("hidden activation status=%d result=%#v load=%d fire=%d", response.StatusCode, result, activation.loadCalls, activation.fireCalls)
	}
}

func TestWorkflowActivationHTTPAuthenticatedUnknownRegistrationStopsBeforeExactAuthorization(t *testing.T) {
	activation := &workflowActivationHTTPService{loadErr: workflowruntime.ErrNotFound}
	var intents []appworkflow.WorkflowAccessIntent
	authenticate := func(request *http.Request, intent appworkflow.WorkflowAccessIntent) (context.Context, error) {
		intents = append(intents, intent)
		return request.Context(), nil
	}
	server := workflowActivationHTTPTestServer(t, activation, authenticate)
	defer server.Close()
	response := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire",
		`{"registration_id":"external-hook","idempotency_key":"delivery-missing","occurred_at":"2026-08-24T12:00:00Z"}`,
		map[string]string{"Authorization": "Bearer allowed"})
	result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
	if response.StatusCode != http.StatusNotFound || result.Code != appworkflow.WorkflowErrorCodeNotFound ||
		activation.loadCalls != 1 || activation.fireCalls != 0 || len(intents) != 1 || intents[0].Definition != nil {
		t.Fatalf("unknown activation status=%d result=%#v load=%d fire=%d intents=%#v", response.StatusCode, result, activation.loadCalls, activation.fireCalls, intents)
	}
}

func TestWorkflowActivationHTTPRejectsMismatchedOrMalformedExposureBeforeFire(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*hoststate.ActivationRegistration)
	}{
		{name: "malformed", mutate: func(registration *hoststate.ActivationRegistration) {
			registration.Principal.ExposureRef = "workflow://ambiguous@v1#digest/activations/external-hook"
		}},
		{name: "template-substitution", mutate: func(registration *hoststate.ActivationRegistration) {
			registration.Derivation.TemplateID = "different-hook"
		}},
		{name: "source-substitution", mutate: func(registration *hoststate.ActivationRegistration) {
			registration.Definition.ID = "different-workflow"
		}},
		{name: "namespace-substitution", mutate: func(registration *hoststate.ActivationRegistration) {
			registration.Definition.ID = "other/workflow-one"
		}},
		{name: "duplicate-field", mutate: func(registration *hoststate.ActivationRegistration) {
			registration.Principal.ExposureRef += "&name=team%2Fother"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			registration := workflowActivationRegistration(t, "team/workflow-one", "v1")
			test.mutate(&registration)
			activation := &workflowActivationHTTPService{registration: registration}
			server := workflowActivationHTTPTestServer(t, activation, allowWorkflowHTTP)
			defer server.Close()
			response := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire",
				`{"registration_id":"external-hook","idempotency_key":"delivery-invalid","occurred_at":"2026-08-24T12:00:00Z"}`,
				map[string]string{"Authorization": "Bearer allowed"})
			result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
			if response.StatusCode != http.StatusServiceUnavailable || result.Code != appworkflow.WorkflowErrorCodeUnavailable || activation.fireCalls != 0 {
				t.Fatalf("invalid exposure status=%d result=%#v fire=%d", response.StatusCode, result, activation.fireCalls)
			}
		})
	}
}

func TestWorkflowActivationHTTPMapsMutationOutcomesAndExactReplay(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "disabled", err: appworkflow.ErrActivationSkipped, wantStatus: http.StatusConflict, wantCode: appworkflow.WorkflowErrorCodeActivationConflict},
		{name: "overlap", err: appworkflow.ErrActivationConflict, wantStatus: http.StatusConflict, wantCode: appworkflow.WorkflowErrorCodeActivationConflict},
		{name: "different-duplicate", err: workflowruntime.ErrIdempotencyConflict, wantStatus: http.StatusConflict, wantCode: appworkflow.WorkflowErrorCodeIdempotencyConflict},
		{name: "invalid", err: appworkflow.ErrInvalidActivation, wantStatus: http.StatusBadRequest, wantCode: appworkflow.WorkflowErrorCodeInvalidRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			activation := &workflowActivationHTTPService{registration: workflowActivationRegistration(t, "team/workflow-one", "v1"), fireErr: test.err}
			server := workflowActivationHTTPTestServer(t, activation, allowWorkflowHTTP)
			defer server.Close()
			response := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire",
				`{"registration_id":"external-hook","idempotency_key":"delivery-outcome","occurred_at":"2026-08-24T12:00:00Z"}`,
				map[string]string{"Authorization": "Bearer allowed"})
			result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
			if response.StatusCode != test.wantStatus || result.Code != test.wantCode || activation.fireCalls != 1 {
				t.Fatalf("status=%d result=%#v fire=%d", response.StatusCode, result, activation.fireCalls)
			}
		})
	}

	activation := &workflowActivationHTTPService{
		registration: workflowActivationRegistration(t, "team/workflow-one", "v1"),
		fireResult: appworkflow.ActivationStartResult{Outcome: workflowruntime.IdempotencyReplayed, Start: appworkflow.StartRunResult{
			Run: &workflowruntime.RunSnapshot{ID: "replayed-run", Status: workflowruntime.RunPending}, Outcome: workflowruntime.IdempotencyReplayed,
		}},
	}
	server := workflowActivationHTTPTestServer(t, activation, allowWorkflowHTTP)
	defer server.Close()
	for range 2 {
		response := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire",
			`{"registration_id":"external-hook","idempotency_key":"delivery-replay","occurred_at":"2026-08-24T12:00:00Z"}`,
			map[string]string{"Authorization": "Bearer allowed"})
		result := workflowDecode[appworkflow.FireWorkflowActivationResult](t, response)
		if response.StatusCode != http.StatusOK || result.Kind != appworkflow.WorkflowActivationFireDirect || result.Outcome != workflowruntime.IdempotencyReplayed || result.Start == nil || result.Start.Run == nil || result.Start.Run.ID != "replayed-run" {
			t.Fatalf("replay status=%d result=%#v", response.StatusCode, result)
		}
	}
}

func TestWorkflowActivationHTTPProjectsReactorDeliveryWithoutPrivateRequestState(t *testing.T) {
	activation := &workflowActivationHTTPService{
		registration: workflowActivationRegistration(t, "team/reactor-one", "v1"),
		fireResult: appworkflow.ActivationStartResult{Outcome: workflowruntime.IdempotencyReplayed, Reactor: &appworkflow.ReactorDeliveryResult{
			Outcome: workflowruntime.IdempotencyReplayed,
			Reactor: workflowruntime.ReactorSnapshot{
				Identity: workflowruntime.ReactorIdentity{ID: "reactor-safe"}, CurrentGeneration: 2,
				CurrentRunID: "reactor-run-2", Status: workflowruntime.ReactorWaiting,
			},
			Delivery: workflowruntime.ReactorDeliverySnapshot{RunID: "reactor-run-2", Status: workflowruntime.ReactorDeliveryApplied},
		}},
	}
	server := workflowActivationHTTPTestServer(t, activation, allowWorkflowHTTP)
	defer server.Close()
	response := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire",
		`{"registration_id":"external-hook","idempotency_key":"reactor-delivery","occurred_at":"2026-08-24T12:00:00Z","payload":{"secret":"private"}}`,
		map[string]string{"Authorization": "Bearer allowed"})
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result appworkflow.FireWorkflowActivationResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || result.Kind != appworkflow.WorkflowActivationFireReactor || result.Start != nil || result.Reactor == nil ||
		result.Reactor.ReactorID != "reactor-safe" || result.Reactor.RunID != "reactor-run-2" || result.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("reactor response status=%d result=%#v", response.StatusCode, result)
	}
	if bytes.Contains(body, []byte("private")) || bytes.Contains(body, []byte("reactor-delivery")) || bytes.Contains(body, []byte("payload")) {
		t.Fatalf("reactor response leaked private delivery state: %s", body)
	}
}

func TestWorkflowActivationHTTPRejectsPathBodyAndHeaderMismatch(t *testing.T) {
	activation := &workflowActivationHTTPService{registration: workflowActivationRegistration(t, "team/workflow-one", "v1")}
	server := workflowActivationHTTPTestServer(t, activation, allowWorkflowHTTP)
	defer server.Close()
	for _, test := range []struct {
		body    string
		headers map[string]string
	}{
		{body: `{"registration_id":"different-hook","idempotency_key":"delivery-one","occurred_at":"2026-08-24T12:00:00Z"}`, headers: map[string]string{"Authorization": "Bearer allowed"}},
		{body: `{"registration_id":"external-hook","idempotency_key":"body-key","occurred_at":"2026-08-24T12:00:00Z"}`, headers: map[string]string{"Authorization": "Bearer allowed", "Idempotency-Key": "header-key"}},
		{body: `{"registration_id":"external-hook","occurred_at":"2026-08-24T12:00:00Z"}`, headers: map[string]string{"Authorization": "Bearer allowed"}},
		{body: `{"registration_id":"external-hook","idempotency_key":"delivery-one","occurred_at":"0001-01-01T00:00:00Z"}`, headers: map[string]string{"Authorization": "Bearer allowed"}},
		{body: fmt.Sprintf(`{"registration_id":"external-hook","idempotency_key":"delivery-future","occurred_at":%q}`, time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)), headers: map[string]string{"Authorization": "Bearer allowed"}},
	} {
		response := workflowPOST(t, server, "/v1/workflows/activations/external-hook/fire", test.body, test.headers)
		result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
		if response.StatusCode != http.StatusBadRequest || result.Code != appworkflow.WorkflowErrorCodeInvalidRequest {
			t.Fatalf("mismatch status=%d result=%#v", response.StatusCode, result)
		}
	}
	if activation.loadCalls != 0 || activation.fireCalls != 0 {
		t.Fatalf("invalid request reached activation service load=%d fire=%d", activation.loadCalls, activation.fireCalls)
	}
}

func graphDefinitionWithLargeLocator() graph.DefinitionRef {
	return graph.DefinitionRef{Kind: "file", Locator: strings.Repeat("x", (8<<20)+1)}
}

func TestWorkflowHTTPHandlerHasNoWorkflowInternalShortcuts(t *testing.T) {
	for _, name := range []string{"workflows.go", "workflow_lifecycle.go"} {
		parsed, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range parsed.Imports {
			path := strings.Trim(imported.Path.Value, `"`)
			for _, forbidden := range []string{"/workflow/compile", "/workflow/runtime", "/workflow/storage", "/internal/persistence"} {
				if strings.Contains(path, forbidden) {
					t.Fatalf("%s imports forbidden shortcut %q", name, path)
				}
			}
		}
	}
}

func TestWorkflowHTTPUnavailableFailsClosed(t *testing.T) {
	service := &workflowHTTPService{
		validate: func(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
			t.Fatal("unavailable workflow service was called")
			return appworkflow.ValidateWorkflowResult{}, nil
		},
		values: func(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowValueListResult, error) {
			t.Fatal("unavailable workflow read service was called")
			return appworkflow.WorkflowValueListResult{}, nil
		},
	}
	tests := []api.Dependencies{
		{},
		{Workflows: service},
		{Workflows: service, WorkflowAuth: api.WorkflowRequestAuthenticatorFunc(allowWorkflowHTTP)},
	}
	paths := []string{"/v1/workflows/validate", "/v1/workflows/validate", "/v1/workflows/runs/run-one/values"}
	bodies := []string{`{}`, `{}`, `{"run_id":"run-one","identity":{}}`}
	for index, dependencies := range tests {
		server := httptest.NewServer(api.NewServer("", dependencies).Handler())
		response := workflowPOST(t, server, paths[index], bodies[index], map[string]string{"Authorization": "Bearer allowed"})
		result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
		server.Close()
		if response.StatusCode != http.StatusServiceUnavailable || result.Code != appworkflow.WorkflowErrorCodeUnavailable {
			t.Fatalf("case %d status=%d result=%#v", index, response.StatusCode, result)
		}
	}
}

var _ appworkflow.WorkflowOperations = (*workflowHTTPService)(nil)
var _ appworkflow.WorkflowRunReadOperations = (*workflowHTTPService)(nil)
