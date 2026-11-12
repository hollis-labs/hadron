package runtime

import (
	"context"
	"net/http"

	libauth "github.com/hollis-labs/libs/security/auth"

	"github.com/hollis-labs/hadron/internal/a2a"
	activationpkg "github.com/hollis-labs/hadron/internal/activation"
	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/appworkflow"
)

// Host returns the workflow host.
func (r *Runtime) Host() *appworkflow.Host { return r.host }

// Operations returns the operator service (runs, events, replay, diagnostics).
func (r *Runtime) Operations() *appworkflow.WorkflowOperator { return r.operations }

// Exposure returns the exposure service.
func (r *Runtime) Exposure() *appworkflow.WorkflowExposureService { return r.exposure }

// Lifecycle returns the workflow lifecycle service (authoring, registration, exposure).
func (r *Runtime) Lifecycle() *appworkflow.WorkflowLifecycleService { return r.lifecycle }

// A2A returns the A2A task handler.
func (r *Runtime) A2A() *a2a.Handler { return r.a2a }

// AgentCard returns the A2A discovery card builder over the published workflows.
func (r *Runtime) AgentCard() *agentcard.Builder { return r.card }

// Activations returns the external activation manager (load a registration, fire an event).
func (r *Runtime) Activations() activationpkg.ActivationManager { return r.externalActivations }

// RequestAuthenticator binds a request's verified principal to the workflow
// host's identity for the transport named by Source ("http", "mcp"). Mode
// returns the auth dial currently in effect.
type RequestAuthenticator struct {
	Source string
	Mode   func() libauth.Mode
}

// AuthenticateWorkflowRequest implements api.WorkflowRequestAuthenticator.
func (a RequestAuthenticator) AuthenticateWorkflowRequest(r *http.Request, _ appworkflow.WorkflowAccessIntent) (context.Context, error) {
	return Authenticate(r.Context(), a.Mode(), a.Source)
}
