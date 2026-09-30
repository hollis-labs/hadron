package appworkflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// Setting confirmed satisfies a Confirm decision only for an identity that
// holds workflow.confirm. An agent's token (exposure or MCP principal) that
// sets confirmed is refused, not treated as the operator's confirmation.
func TestConfirmRequiresWorkflowConfirmGrant(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyConfirm, time.Hour, nil)
	fixture.setPolicy(func(hoststate.PolicyFacts) hoststate.PolicyDecision {
		return hoststate.PolicyDecision{Outcome: hoststate.PolicyConfirm, Reason: "advised"}
	})
	agent := testIdentityBinding("service:agent", "mcp")
	agent.Grants = []string{"workflow.run"}
	host := hostWithFixedIdentity(t, fixture, agent)
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	caller := authenticatedContext(t.Context(), "service:agent")

	request := fixture.startRequest("run-agent-confirm", "key-agent-confirm", "service:agent")
	if _, err := host.StartRun(caller, request); !errors.Is(err, appworkflow.ErrConfirmationRequired) {
		t.Fatalf("unconfirmed agent start = %v, want ErrConfirmationRequired", err)
	}
	request.Confirmed = true
	if _, err := host.StartRun(caller, request); !errors.Is(err, appworkflow.ErrConfirmationNotPermitted) {
		t.Fatalf("self-confirmed agent start = %v, want ErrConfirmationNotPermitted", err)
	}
	if code := appworkflow.SafeWorkflowOperationError(appworkflow.ErrConfirmationNotPermitted, nil).Code; code != appworkflow.WorkflowErrorCodeConfirmationNotPermitted {
		t.Fatalf("error code = %q", code)
	}
}
