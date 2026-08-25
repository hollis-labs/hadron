package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
)

type workflowLifecycleHTTPSpy struct {
	appworkflow.WorkflowLifecycleOperations
	search   func(context.Context, appworkflow.SearchWorkflowCatalogRequest) (appworkflow.WorkflowCatalogSearchResult, error)
	inspect  func(context.Context, appworkflow.InspectWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error)
	test     func(context.Context, appworkflow.TestWorkflowDraftRequest) (appworkflow.WorkflowContractTestResult, error)
	register func(context.Context, appworkflow.RegisterWorkflowDraftRequest) (appworkflow.WorkflowRegistrationResult, error)
	publish  func(context.Context, appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error)
}

func (s *workflowLifecycleHTTPSpy) SearchWorkflowCatalog(ctx context.Context, request appworkflow.SearchWorkflowCatalogRequest) (appworkflow.WorkflowCatalogSearchResult, error) {
	return s.search(ctx, request)
}

func (s *workflowLifecycleHTTPSpy) InspectWorkflowVersion(ctx context.Context, request appworkflow.InspectWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
	return s.inspect(ctx, request)
}

func (s *workflowLifecycleHTTPSpy) TestWorkflowDraft(ctx context.Context, request appworkflow.TestWorkflowDraftRequest) (appworkflow.WorkflowContractTestResult, error) {
	return s.test(ctx, request)
}

func (s *workflowLifecycleHTTPSpy) RegisterWorkflowDraft(ctx context.Context, request appworkflow.RegisterWorkflowDraftRequest) (appworkflow.WorkflowRegistrationResult, error) {
	return s.register(ctx, request)
}

func (s *workflowLifecycleHTTPSpy) PublishWorkflowVersion(ctx context.Context, request appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
	return s.publish(ctx, request)
}

func workflowLifecycleHTTPTestServer(t *testing.T, service appworkflow.WorkflowLifecycleOperations, auth api.WorkflowRequestAuthenticatorFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(api.NewServer("", api.Dependencies{WorkflowLifecycle: service, WorkflowAuth: auth}).Handler())
}

func TestWorkflowLifecycleHTTPAuthenticatesAndSanitizesIdentity(t *testing.T) {
	calls := 0
	service := &workflowLifecycleHTTPSpy{search: func(ctx context.Context, request appworkflow.SearchWorkflowCatalogRequest) (appworkflow.WorkflowCatalogSearchResult, error) {
		calls++
		if ctx.Value(workflowAuthKey{}) != "user:authenticated" {
			t.Fatal("authenticated context was not forwarded")
		}
		if request.Identity.PrincipalHint != "" || request.Identity.SourceAuthority != "http" || request.Identity.Attributes != nil {
			t.Fatalf("transported lifecycle identity was trusted: %#v", request.Identity)
		}
		if request.Identity.RunScope == nil || request.Identity.RunScope.ID != "team" {
			t.Fatalf("scope selector was lost: %#v", request.Identity.RunScope)
		}
		return appworkflow.WorkflowCatalogSearchResult{Matches: []appworkflow.WorkflowCatalogMatch{}, NextStep: "draft_validate"}, nil
	}}
	server := workflowLifecycleHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	response := workflowPOST(t, server, "/v1/workflows/lifecycle/catalog/search", `{"namespace":"team","query":"release","identity":{"principal_hint":"forged","source_authority":"forged","attributes":{"principal":"root"},"run_scope":{"version":"v1","kind":"team","id":"team"}}}`, map[string]string{"Authorization": "Bearer allowed"})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d result=%#v", response.StatusCode, workflowDecode[appworkflow.WorkflowOperationError](t, response))
	}
	_ = response.Body.Close()
	if calls != 1 {
		t.Fatalf("search calls=%d", calls)
	}
}

func TestWorkflowLifecycleHTTPStrictAndFailClosed(t *testing.T) {
	service := &workflowLifecycleHTTPSpy{
		search: func(context.Context, appworkflow.SearchWorkflowCatalogRequest) (appworkflow.WorkflowCatalogSearchResult, error) {
			t.Fatal("invalid request reached lifecycle service")
			return appworkflow.WorkflowCatalogSearchResult{}, nil
		},
		inspect: func(context.Context, appworkflow.InspectWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
			t.Fatal("hidden definition reached lifecycle service")
			return appworkflow.WorkflowVersionDetail{}, nil
		},
	}
	server := workflowLifecycleHTTPTestServer(t, service, allowWorkflowHTTP)
	for _, body := range []string{
		`{"query":"one","query":"two","identity":{}}`,
		`{"query":"one","identity":{},"unknown":true}`,
		`{"query":"one","identity":{}} {}`,
	} {
		response := workflowPOST(t, server, "/v1/workflows/lifecycle/catalog/search", body, map[string]string{"Authorization": "Bearer allowed"})
		result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
		if response.StatusCode != http.StatusBadRequest || result.Code != appworkflow.WorkflowErrorCodeInvalidRequest {
			t.Fatalf("body=%s status=%d result=%#v", body, response.StatusCode, result)
		}
	}
	hidden := workflowPOST(t, server, "/v1/workflows/lifecycle/catalog/inspect", `{"definition":{"kind":"registry","id":"private/hidden","version":"v1","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"identity":{}}`, map[string]string{"Authorization": "Bearer allowed"})
	hiddenResult := workflowDecode[appworkflow.WorkflowOperationError](t, hidden)
	if hidden.StatusCode != http.StatusNotFound || hiddenResult.Code != appworkflow.WorkflowErrorCodeNotFound {
		t.Fatalf("hidden status=%d result=%#v", hidden.StatusCode, hiddenResult)
	}
	server.Close()

	for _, dependencies := range []api.Dependencies{{WorkflowAuth: api.WorkflowRequestAuthenticatorFunc(allowWorkflowHTTP)}, {WorkflowLifecycle: service}} {
		unavailable := httptest.NewServer(api.NewServer("", dependencies).Handler())
		response := workflowPOST(t, unavailable, "/v1/workflows/lifecycle/catalog/search", `{}`, map[string]string{"Authorization": "Bearer allowed"})
		result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
		unavailable.Close()
		if response.StatusCode != http.StatusServiceUnavailable || result.Code != appworkflow.WorkflowErrorCodeUnavailable {
			t.Fatalf("unavailable status=%d result=%#v", response.StatusCode, result)
		}
	}
}

func TestWorkflowLifecycleHTTPSafePolicyAndContractFailures(t *testing.T) {
	testCalls, registerCalls, publishCalls := 0, 0, 0
	service := &workflowLifecycleHTTPSpy{
		test: func(context.Context, appworkflow.TestWorkflowDraftRequest) (appworkflow.WorkflowContractTestResult, error) {
			testCalls++
			return appworkflow.WorkflowContractTestResult{}, appworkflow.ErrContractTestFailed
		},
		register: func(context.Context, appworkflow.RegisterWorkflowDraftRequest) (appworkflow.WorkflowRegistrationResult, error) {
			registerCalls++
			return appworkflow.WorkflowRegistrationResult{}, appworkflow.ErrNamespaceUnauthorized
		},
		publish: func(context.Context, appworkflow.MutateWorkflowVersionRequest) (appworkflow.WorkflowVersionDetail, error) {
			publishCalls++
			return appworkflow.WorkflowVersionDetail{}, appworkflow.ErrNamespaceUnauthorized
		},
	}
	server := workflowLifecycleHTTPTestServer(t, service, allowWorkflowHTTP)
	defer server.Close()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tests := []struct {
		path, body, code string
		status           int
	}{
		{"/v1/workflows/lifecycle/author/test", `{"draft":{"envelope":{},"id":"demo","version":"v1","namespace":"team"},"suite":{"schema_version":"1","cases":[]},"identity":{}}`, appworkflow.WorkflowErrorCodeInvalidRequest, http.StatusBadRequest},
		{"/v1/workflows/lifecycle/author/register", `{"draft":{"envelope":{},"id":"demo","version":"v1","namespace":"team"},"suite":{"schema_version":"1","cases":[]},"identity":{}}`, appworkflow.WorkflowErrorCodePolicyDenied, http.StatusForbidden},
		{"/v1/workflows/lifecycle/registry/publish", `{"definition":{"kind":"registry","id":"team/demo","version":"v1","digest":"` + digest + `"},"identity":{}}`, appworkflow.WorkflowErrorCodeNotFound, http.StatusNotFound},
	}
	for _, test := range tests {
		response := workflowPOST(t, server, test.path, test.body, map[string]string{"Authorization": "Bearer allowed"})
		result := workflowDecode[appworkflow.WorkflowOperationError](t, response)
		if response.StatusCode != test.status || result.Code != test.code {
			t.Fatalf("%s status=%d result=%#v", test.path, response.StatusCode, result)
		}
	}
	if testCalls != 1 || registerCalls != 1 || publishCalls != 1 {
		t.Fatalf("service calls test=%d register=%d publish=%d", testCalls, registerCalls, publishCalls)
	}
}

var _ appworkflow.WorkflowLifecycleOperations = (*workflowLifecycleHTTPSpy)(nil)
