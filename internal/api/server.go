// Package api is Hadron's HTTP surface for workflows, on the Substrate host:
// routes live under /api/workflows/, are gated by the scopes in Scopes, and
// fail with the flat {code, message, details?} body the frontend reads.
//
// Handlers hold no authentication code. The framework's auth module verifies
// the token and enforces the route scope; the WorkflowRequestAuthenticator then
// turns the verified principal into the identity the workflow host binds.
package api

import (
	"context"
	"net/http"

	"github.com/hollis-labs/substrate/modules/httphost"

	"github.com/hollis-labs/hadron/internal/a2a"
	activationpkg "github.com/hollis-labs/hadron/internal/activation"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	appworkflowhoststate "github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// WorkflowRequestAuthenticator binds the authenticated caller to the request's
// context. Handlers never synthesize a principal from request JSON.
type WorkflowRequestAuthenticator interface {
	AuthenticateWorkflowRequest(*http.Request, appworkflow.WorkflowAccessIntent) (context.Context, error)
}

// WorkflowRequestAuthenticatorFunc adapts a function.
type WorkflowRequestAuthenticatorFunc func(*http.Request, appworkflow.WorkflowAccessIntent) (context.Context, error)

func (f WorkflowRequestAuthenticatorFunc) AuthenticateWorkflowRequest(request *http.Request, intent appworkflow.WorkflowAccessIntent) (context.Context, error) {
	return f(request, intent)
}

// WorkflowActivationService loads an activation registration (to authorize its
// exact workflow) and delivers a credential-free event to it.
type WorkflowActivationService interface {
	LoadRegistration(context.Context, string) (appworkflowhoststate.ActivationRegistration, error)
	Fire(context.Context, activationpkg.ActivationEvent) (appworkflow.ActivationStartResult, error)
}

// A2ATaskService is the A2A task surface over workflow runs.
type A2ATaskService interface {
	SubmitTask(context.Context, a2a.TaskRequest) (*a2a.TaskResponse, error)
	GetTask(context.Context, string) (*a2a.TaskResponse, error)
	CancelTask(context.Context, string, a2a.CancelTaskRequest) (*a2a.TaskResponse, error)
	ResumeTask(context.Context, string, a2a.ResumeTaskRequest) (*a2a.ResumeTaskResponse, error)
}

// AgentCardProvider builds the A2A discovery document for a base URL.
type AgentCardProvider interface {
	Card(context.Context, string) (*agentcard.AgentCard, error)
}

// Dependencies are the runtime services the handlers call.
type Dependencies struct {
	A2ATasks            A2ATaskService
	AgentCard           AgentCardProvider
	Workflows           appworkflow.WorkflowOperations
	WorkflowReads       appworkflow.WorkflowRunReadOperations
	WorkflowLifecycle   appworkflow.WorkflowLifecycleOperations
	WorkflowAuth        WorkflowRequestAuthenticator
	WorkflowActivations WorkflowActivationService
}

// Server serves the workflow routes.
type Server struct {
	deps    Dependencies
	handler http.Handler
}

// NewServer builds the route mux. addr is unused and kept out of the signature:
// the Substrate host owns the listener.
func NewServer(deps Dependencies) *Server {
	s := &Server{deps: deps}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/workflows/validate", s.handleWorkflowValidate)
	mux.HandleFunc("/api/workflows/explain", s.handleWorkflowExplain)
	mux.HandleFunc("/api/workflows/runs", s.handleWorkflowRuns)
	mux.HandleFunc("/api/workflows/runs/", s.handleWorkflowRunAction)
	mux.HandleFunc("/api/workflows/activations/", s.handleWorkflowActivationFire)
	mux.HandleFunc("/api/workflows/lifecycle/", s.handleWorkflowLifecycle)
	s.handler = rejectPathTraversal(mux)
	return s
}

// Mounts are the A2A routes, which a protocol fixes outside /api/: the agent
// card and the task endpoints. They are guarded and scoped like the API routes
// (see RegisterScopes); the card is not public because it lists the published
// workflows and their schemas.
func (s *Server) Mounts() []httphost.Mount {
	return []httphost.Mount{
		{Pattern: "GET /.well-known/agent.json", Handler: http.HandlerFunc(s.handleAgentCard)},
		{Pattern: "/a2a/tasks", Handler: http.HandlerFunc(s.handleA2ATasks)},
		{Pattern: "/a2a/tasks/", Handler: http.HandlerFunc(s.handleA2ATaskByID)},
	}
}

// Handler is the mux, for httphost.Options.Routes and for tests.
func (s *Server) Handler() http.Handler { return s.handler }

func rejectPathTraversal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, segment := range splitPath(r.URL.Path) {
			if segment == ".." {
				http.NotFound(w, r)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func splitPath(p string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(p); i++ {
		if i == len(p) || p[i] == '/' {
			out = append(out, p[start:i])
			start = i + 1
		}
	}
	return out
}
