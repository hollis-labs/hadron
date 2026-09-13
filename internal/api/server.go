package api

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hollis-labs/go-otel/propagation"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/trigger"
)

// Handler returns the underlying HTTP handler (useful for testing with httptest).
func (s *Server) Handler() http.Handler {
	return s.handler
}

func NewServer(addr string, deps Dependencies) *Server {
	s := &Server{deps: deps}
	mux := http.NewServeMux()

	// The old trigger manager dispatches execution.Request and is mounted only
	// by explicit archive/reference composition.
	if deps.EnableLegacyRuntime && deps.Triggers != nil && deps.Runner != nil {
		s.triggerManager = trigger.New(deps.Triggers, deps.Runner)
	}

	mux.HandleFunc("/v1/health", s.handleHealth)
	mux.HandleFunc("/v1/workflows/validate", s.handleWorkflowValidate)
	mux.HandleFunc("/v1/workflows/explain", s.handleWorkflowExplain)
	mux.HandleFunc("/v1/workflows/runs", s.handleWorkflowRuns)
	mux.HandleFunc("/v1/workflows/runs/", s.handleWorkflowRunAction)
	mux.HandleFunc("/v1/workflows/activations/", s.handleWorkflowActivationFire)
	mux.HandleFunc("/v1/workflows/lifecycle/", s.handleWorkflowLifecycle)

	// Workspaces
	mux.HandleFunc("/v1/workspaces", s.handleWorkspaces)
	mux.HandleFunc("/v1/workspaces/", s.handleWorkspaceByID)

	if deps.EnableLegacyRuntime {
		// Archived blueprint/pipeline routes. These are deliberately absent from
		// production and carry no forward compatibility promise.
		mux.HandleFunc("/v1/runs", s.handleRuns)
		mux.HandleFunc("/v1/runs/", s.handleRunByID)
		mux.HandleFunc("/v1/schedules", s.handleSchedules)
		mux.HandleFunc("/v1/schedules/", s.handleScheduleByID)
		mux.HandleFunc("/v1/pipelines", s.handlePipelines)
		mux.HandleFunc("/v1/pipelines/", s.handlePipelineByID)
		mux.HandleFunc("/v1/triggers", s.handleWebhookTriggers)
		mux.HandleFunc("/v1/triggers/", s.handleWebhookTriggerByID)
		mux.HandleFunc("/v1/human-gates/", s.handleHumanGateByID)
		mux.HandleFunc("/v1/messages", s.handleMessagesCollection)
		mux.HandleFunc("/v1/messages/inbox", s.handleMessagesInbox)
		mux.HandleFunc("/v1/messages/list", s.handleMessagesList)
		mux.HandleFunc("/v1/messages/thread/", s.handleMessagesThread)
		mux.HandleFunc("/v1/messages/", s.handleMessageByID)
		if s.triggerManager != nil {
			s.triggerManager.RegisterWebhookRoutes(mux)
		}
	}

	// A2A Agent Card
	mux.HandleFunc("/.well-known/agent.json", s.handleAgentCard)

	// A2A Task endpoints
	mux.HandleFunc("/a2a/tasks", s.handleA2ATasks)
	mux.HandleFunc("/a2a/tasks/", s.handleA2ATaskByID)

	if deps.EnableLegacyRuntime {
		mux.HandleFunc("/v1/blueprints/validate", s.handleBlueprintValidate)
	}

	// Browser operator UI. API-looking paths remain structured API 404s so a
	// mistyped workflow route can never be hidden by the SPA fallback.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if deps.WebUI != nil && !isAPIPath(r.URL.Path) {
			deps.WebUI.ServeHTTP(w, r)
			return
		}
		writeError(w, http.StatusNotFound, "not found")
	})

	s.handler = corsMiddleware(propagation.HTTPMiddleware(rejectPathTraversal(mux)))
	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func rejectPathTraversal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, segment := range strings.Split(r.URL.Path, "/") {
			if segment == ".." {
				http.NotFound(w, r)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func isAPIPath(requestPath string) bool {
	for _, prefix := range []string{"/v1", "/a2a", "/.well-known"} {
		if requestPath == prefix || strings.HasPrefix(requestPath, prefix+"/") {
			return true
		}
	}
	return false
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// corsMiddleware permits only same-origin browser requests. The production UI
// is served by this daemon; wildcard CORS would turn loopback operator identity
// into a cross-site request capability.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			parsed, err := url.Parse(origin)
			expectedScheme := "http"
			if r.TLS != nil {
				expectedScheme = "https"
			}
			if err != nil || parsed.Scheme != expectedScheme || !strings.EqualFold(parsed.Host, r.Host) || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
				writeJSON(w, http.StatusForbidden, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodePolicyDenied})
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key")
		}

		if r.Method == http.MethodOptions {
			if origin == "" {
				writeJSON(w, http.StatusForbidden, appworkflow.WorkflowOperationError{Code: appworkflow.WorkflowErrorCodePolicyDenied})
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
