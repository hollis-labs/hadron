package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/hollis-labs/hadron/internal/a2a"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type a2aHTTPContextKey struct{}

type fakeA2AHTTPService struct {
	submits []a2a.TaskRequest
	gets    []string
	cancels []a2a.CancelTaskRequest
	resumes []a2a.ResumeTaskRequest
}

func (f *fakeA2AHTTPService) SubmitTask(ctx context.Context, request a2a.TaskRequest) (*a2a.TaskResponse, error) {
	if ctx.Value(a2aHTTPContextKey{}) != "authenticated" {
		return nil, errors.New("missing authenticated context")
	}
	f.submits = append(f.submits, request)
	return &a2a.TaskResponse{ID: request.ID, RunID: "run-one", Definition: request.Skill, Outcome: workflowruntime.IdempotencyApplied, Available: true, Status: a2a.TaskStatus{State: "working"}}, nil
}

func (f *fakeA2AHTTPService) GetTask(ctx context.Context, taskID string) (*a2a.TaskResponse, error) {
	if ctx.Value(a2aHTTPContextKey{}) != "authenticated" {
		return nil, errors.New("missing authenticated context")
	}
	f.gets = append(f.gets, taskID)
	return &a2a.TaskResponse{ID: taskID, RunID: "run-one", Available: true, Status: a2a.TaskStatus{State: "working"}}, nil
}

func (f *fakeA2AHTTPService) CancelTask(_ context.Context, taskID string, request a2a.CancelTaskRequest) (*a2a.TaskResponse, error) {
	f.gets = append(f.gets, taskID)
	f.cancels = append(f.cancels, request)
	return &a2a.TaskResponse{ID: taskID, RunID: "run-one", Available: true, Status: a2a.TaskStatus{State: "canceled"}}, nil
}

func (f *fakeA2AHTTPService) ResumeTask(_ context.Context, taskID string, request a2a.ResumeTaskRequest) (*a2a.ResumeTaskResponse, error) {
	f.gets = append(f.gets, taskID)
	f.resumes = append(f.resumes, request)
	return &a2a.ResumeTaskResponse{Resume: appworkflow.ResumeWorkflowRunResult{Outcome: workflowruntime.ResumeApplied}}, nil
}

type fakeAgentCardProvider struct{}

func (fakeAgentCardProvider) Card(context.Context, string) (*agentcard.AgentCard, error) {
	return &agentcard.AgentCard{Name: "Hadron Workflows", Skills: []agentcard.Skill{}}, nil
}

func TestA2ARoutesAreUnconditionalAndFailClosedWithoutComposition(t *testing.T) {
	server := httptest.NewServer(api.NewServer("", api.Dependencies{}).Handler())
	t.Cleanup(server.Close)
	for _, request := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/a2a/tasks", `{}`},
		{http.MethodGet, "/a2a/tasks/task-one", ``},
		{http.MethodPost, "/a2a/tasks/task-one/cancel", `{}`},
		{http.MethodPost, "/a2a/tasks/task-one/resume", `{}`},
		{http.MethodGet, "/.well-known/agent.json", ``},
	} {
		response := doA2AHTTP(t, server.URL+request.path, request.method, request.body, "")
		assertA2AHTTPError(t, response, http.StatusServiceUnavailable, appworkflow.WorkflowErrorCodeUnavailable)
	}
}

func TestA2AHTTPAuthenticatesExactStartAndRejectsTransportedIdentity(t *testing.T) {
	service := &fakeA2AHTTPService{}
	var intents []appworkflow.WorkflowAccessIntent
	auth := api.WorkflowRequestAuthenticatorFunc(func(request *http.Request, intent appworkflow.WorkflowAccessIntent) (context.Context, error) {
		intents = append(intents, intent)
		return context.WithValue(request.Context(), a2aHTTPContextKey{}, "authenticated"), nil
	})
	server := httptest.NewServer(api.NewServer("", api.Dependencies{A2ATasks: service, WorkflowAuth: auth, AgentCard: fakeAgentCardProvider{}}).Handler())
	t.Cleanup(server.Close)
	digest := values.SHA256Digest([]byte("workflow"))
	body := `{"id":"task-one","skill":{"kind":"registry","id":"team/workflow","version":"v1","digest":"` + digest + `"},"input":{"number":9007199254740993},"idempotency_key":"start-one","confirmed":true}`
	response := doA2AHTTP(t, server.URL+"/a2a/tasks", http.MethodPost, body, "start-one")
	if response.StatusCode != http.StatusCreated {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("start status=%d body=%s", response.StatusCode, data)
	}
	_ = response.Body.Close()
	if len(service.submits) != 1 || service.submits[0].Input["number"] != json.Number("9007199254740993") || len(intents) != 1 || intents[0].Operation != appworkflow.WorkflowAccessRun || intents[0].Definition == nil || intents[0].Definition.Digest != digest {
		t.Fatalf("submit=%#v intents=%#v", service.submits, intents)
	}
	for _, forbidden := range []string{"principal", "principal_hint", "identity", "attributes", "source_authority"} {
		bad := stringsReplaceLastObject(body, `,"`+forbidden+`":"forged"`)
		response := doA2AHTTP(t, server.URL+"/a2a/tasks", http.MethodPost, bad, "start-one")
		assertA2AHTTPError(t, response, http.StatusBadRequest, appworkflow.WorkflowErrorCodeInvalidRequest)
	}
	if len(service.submits) != 1 {
		t.Fatal("transported identity reached A2A service")
	}
}

func TestA2AHTTPHidesDefinitionDenialAndBindsMutationKeys(t *testing.T) {
	service := &fakeA2AHTTPService{}
	deny := true
	auth := api.WorkflowRequestAuthenticatorFunc(func(request *http.Request, _ appworkflow.WorkflowAccessIntent) (context.Context, error) {
		if deny {
			return nil, appworkflow.ErrPolicyDenied
		}
		return context.WithValue(request.Context(), a2aHTTPContextKey{}, "authenticated"), nil
	})
	server := httptest.NewServer(api.NewServer("", api.Dependencies{A2ATasks: service, WorkflowAuth: auth}).Handler())
	t.Cleanup(server.Close)
	digest := values.SHA256Digest([]byte("workflow"))
	start := `{"id":"hidden","skill":{"kind":"registry","id":"team/hidden","version":"v1","digest":"` + digest + `"},"idempotency_key":"start-hidden"}`
	response := doA2AHTTP(t, server.URL+"/a2a/tasks", http.MethodPost, start, "start-hidden")
	assertA2AHTTPError(t, response, http.StatusNotFound, appworkflow.WorkflowErrorCodeNotFound)
	if len(service.submits) != 0 {
		t.Fatal("hidden start reached service")
	}

	deny = false
	response = doA2AHTTP(t, server.URL+"/a2a/tasks/task-one/cancel", http.MethodPost, `{"idempotency_key":"body-key"}`, "header-key")
	assertA2AHTTPError(t, response, http.StatusBadRequest, appworkflow.WorkflowErrorCodeInvalidRequest)
	payload, err := values.NewInline("approve", values.Metadata{Producer: values.Producer{Kind: "a2a", Reference: "task-one"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	resume := a2a.ResumeTaskRequest{WaitID: "wait-one", Correlation: "correlation-one", WakeSource: workflowwait.WakeGate, Payload: payload, IdempotencyKey: "resume-one"}
	encoded, err := json.Marshal(resume)
	if err != nil {
		t.Fatal(err)
	}
	response = doA2AHTTP(t, server.URL+"/a2a/tasks/task-one/resume", http.MethodPost, string(encoded), "resume-one")
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("resume status=%d body=%s", response.StatusCode, data)
	}
	_ = response.Body.Close()
	if len(service.resumes) != 1 || !reflect.DeepEqual(service.resumes[0], resume) {
		t.Fatalf("resume = %#v", service.resumes)
	}
	resume.IdempotencyKey = ""
	encoded, _ = json.Marshal(resume)
	response = doA2AHTTP(t, server.URL+"/a2a/tasks/task-one/resume", http.MethodPost, string(encoded), "")
	assertA2AHTTPError(t, response, http.StatusBadRequest, appworkflow.WorkflowErrorCodeInvalidRequest)
}

func doA2AHTTP(t *testing.T, target, method, body, key string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, target, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func assertA2AHTTPError(t *testing.T, response *http.Response, status int, code string) {
	t.Helper()
	defer func() { _ = response.Body.Close() }()
	var envelope appworkflow.WorkflowOperationError
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status || envelope.Code != code {
		t.Fatalf("status=%d envelope=%#v", response.StatusCode, envelope)
	}
}

func stringsReplaceLastObject(input, field string) string {
	return string(append([]byte(input[:len(input)-1]), append([]byte(field), '}')...))
}

var _ = graph.DefinitionRef{}
