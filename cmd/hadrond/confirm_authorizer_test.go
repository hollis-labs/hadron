package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hollis-labs/libs/workflow/graph"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// Run ownership compares whole identity bindings, so the operator's binding
// must not change when confirm rights do: a run the operator started stays
// the operator's to inspect and cancel. The confirm right is decided by
// operatorConfirmAuthorizer at confirmation time instead.
func TestLocalOperatorBindingIsStableAcrossConfirmRights(t *testing.T) {
	if got := localWorkflowIdentity().Grants; !reflect.DeepEqual(got, []string{"workflow.manage", "workflow.run"}) {
		t.Fatalf("operator:local grants = %v; changing them changes the binding and locks the operator out of runs started before", got)
	}
	runtime, cfg, _ := newTestProductionWorkflowRuntime(t)
	if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), "call-parent.workflow.yaml"), []byte(productionCallParentSource), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	ctx := productionLocalContext(t)
	identity := appworkflow.IdentityRequest{SourceAuthority: "http"}
	definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "call-parent", Locator: "call-parent.workflow.yaml", Version: "v1"}

	// The operator confirms an effect-advised (call) workflow...
	started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "owned-run", IdempotencyKey: "owned-run", Definition: definition,
		Inputs: map[string]any{"message": "x"}, Confirmed: true, Identity: identity,
	})
	if err != nil || started.Run == nil {
		t.Fatalf("operator confirmed start = %#v, %v", started, err)
	}
	// ...and still owns it: inspect and cancel pass the ownership check.
	if _, err := runtime.operations.InspectWorkflowRun(ctx, appworkflow.InspectWorkflowRunRequest{RunID: started.Run.ID, Identity: identity}); err != nil {
		t.Fatalf("operator inspect of own run = %v", err)
	}
	if _, err := runtime.operations.CancelWorkflowRun(ctx, appworkflow.CancelWorkflowRunRequest{RunID: started.Run.ID, Identity: identity, IdempotencyKey: "cancel-owned", Reason: "test"}); err != nil {
		t.Fatalf("operator cancel of own run = %v", err)
	}
}

// Agents' credentials cannot confirm. A non-operator identity that can
// otherwise run the workflow gets ErrConfirmationNotPermitted, and so does an
// identity claiming the operator's principal without the operator
// credential's local trust. The MCP principal is refused as well (today it
// also lacks the call capability this workflow needs).
func TestNonOperatorConfirmIsNotPermitted(t *testing.T) {
	runtime, cfg, _ := newTestProductionWorkflowRuntime(t)
	if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), "call-parent.workflow.yaml"), []byte(productionCallParentSource), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "call-parent", Locator: "call-parent.workflow.yaml", Version: "v1"}

	exposure := localWorkflowIdentity()
	exposure.Principal, exposure.Trust = "agent:exposure", "trusted"
	impersonated := localWorkflowIdentity()
	impersonated.Trust = "trusted"
	for _, tc := range []struct {
		name    string
		binding hoststate.IdentityBinding
		want    []error
	}{
		{"exposure", exposure, []error{appworkflow.ErrConfirmationNotPermitted}},
		{"impersonated", impersonated, []error{appworkflow.ErrConfirmationNotPermitted}},
		{"mcp", workflowMCPPrincipal().Identity, []error{appworkflow.ErrConfirmationNotPermitted, appworkflow.ErrExecutionTarget}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, err := appworkflow.WithAuthenticatedIdentity(t.Context(), tc.binding)
			if err != nil {
				t.Fatal(err)
			}
			started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
				RunID: appworkflow.RunID("self-confirm-" + tc.name), IdempotencyKey: "self-confirm-" + tc.name, Definition: definition,
				Inputs: map[string]any{"message": "x"}, Confirmed: true, Identity: appworkflow.IdentityRequest{SourceAuthority: tc.binding.SourceAuthority},
			})
			if started.Run != nil {
				t.Fatalf("self-confirmed start created a run")
			}
			for _, want := range tc.want {
				if errors.Is(err, want) {
					return
				}
			}
			t.Fatalf("self-confirmed start = %v, want one of %v", err, tc.want)
		})
	}
}
