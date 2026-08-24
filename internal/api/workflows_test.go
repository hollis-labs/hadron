package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
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

func graphDefinitionWithLargeLocator() graph.DefinitionRef {
	return graph.DefinitionRef{Kind: "file", Locator: strings.Repeat("x", (8<<20)+1)}
}

func TestWorkflowHTTPHandlerHasNoWorkflowInternalShortcuts(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "workflows.go", nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range parsed.Imports {
		path := strings.Trim(imported.Path.Value, `"`)
		for _, forbidden := range []string{"/workflow/compile", "/workflow/runtime", "/workflow/storage", "/internal/persistence"} {
			if strings.Contains(path, forbidden) {
				t.Fatalf("workflow HTTP handler imports forbidden shortcut %q", path)
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
