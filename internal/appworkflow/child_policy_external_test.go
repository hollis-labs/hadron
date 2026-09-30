package appworkflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	calladapter "github.com/hollis-labs/go-workflow/adapters/call"
	"github.com/hollis-labs/go-workflow/adapters/transform"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

func TestHostStartChildRunAppliesAndPersistsStartPolicy(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	root, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:one"), fixture.startRequest("child-policy-root", "child-policy-root", "user:one"))
	if err != nil || root.Run == nil || root.Decision.Outcome != hoststate.PolicyAllow {
		t.Fatalf("root start = %#v, %v", root, err)
	}

	outcomes := map[workflowruntime.RunID]hoststate.PolicyOutcome{
		"deny-child": hoststate.PolicyDeny, "confirm-child": hoststate.PolicyConfirm, "allow-child": hoststate.PolicyAllow,
	}
	var calls atomic.Int32
	var observed atomic.Value
	gated, err := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: fixture.journal,
		Definitions: definitionProvider{plan: fixture.plan, calls: &fixture.definitionCalls},
		Identity: identityProviderFunc(func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
			return hoststate.IdentityBinding{}, errors.New("child policy must not rebind identity")
		}),
		Policy: appworkflow.PolicyEvaluatorFunc(func(_ context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			calls.Add(1)
			observed.Store(facts)
			return hoststate.PolicyDecision{Outcome: outcomes[facts.RunID], Reason: "child fixture policy"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
		Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		Clock:            appworkflow.ClockFunc(func() time.Time { return fixture.now }),
		RecoveryInterval: time.Hour, RecoveryBatchLimit: 1,
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	parent := workflowruntime.NodeInvocationID{RunID: root.Run.ID, NodeID: "echo"}
	request := func(id workflowruntime.RunID) calladapter.ChildRunRequest {
		child := childRecoveryRequest(t, compileFinalizerHostPlan(t), parent)
		child.ChildRunID, child.IdempotencyKey = id, string(id)+"-start"
		return child
	}

	_, err = gated.StartChildRun(t.Context(), request("deny-child"))
	var execution *stepkind.ExecutionError
	if !errors.As(err, &execution) || execution.Code != appworkflow.CodeChildRunPolicyDenied ||
		execution.Classification != stepkind.RetryPermanent || !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("denied child = %v", err)
	}
	if _, loadErr := fixture.state.LoadRun(t.Context(), "deny-child"); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		t.Fatalf("denied child run exists: %v", loadErr)
	}
	persisted, err := fixture.journal.LoadPolicyEvaluationByStartKey(t.Context(), "child-run:deny-child")
	if err != nil || persisted.Decision.Outcome != hoststate.PolicyDeny || persisted.Decision.RunID != "deny-child" ||
		persisted.Facts.Identity.Principal != "user:one" || persisted.Facts.Operation != "start" {
		t.Fatalf("persisted child decision = %#v, %v", persisted, err)
	}
	facts, _ := observed.Load().(hoststate.PolicyFacts)
	if facts.Plan.ID == "" || facts.NodeCount == 0 {
		t.Fatalf("child policy facts = %#v", facts)
	}
	before := calls.Load()
	if _, replayErr := gated.StartChildRun(t.Context(), request("deny-child")); !errors.Is(replayErr, appworkflow.ErrPolicyDenied) || calls.Load() != before {
		t.Fatalf("denied child replay = %v, policy calls %d -> %d", replayErr, before, calls.Load())
	}

	// A child that policy says needs confirmation cannot run under a root that
	// was allowed without it.
	if _, confirmErr := gated.StartChildRun(t.Context(), request("confirm-child")); !errors.Is(confirmErr, appworkflow.ErrConfirmationRequired) {
		t.Fatalf("unconfirmed child = %v", confirmErr)
	}

	// An allowed child passes the gate and reaches the journal, which applies
	// its own invariants (this fixture's parent node is not running).
	_, err = gated.StartChildRun(t.Context(), request("allow-child"))
	if errors.As(err, &execution) && execution.Code == appworkflow.CodeChildRunPolicyDenied {
		t.Fatalf("allowed child was refused by the gate: %v", err)
	}
	if err == nil || !errors.Is(err, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("allowed child did not reach the journal: %v", err)
	}
	if allowedDecision, loadErr := fixture.journal.LoadPolicyEvaluationByStartKey(t.Context(), "child-run:allow-child"); loadErr != nil || allowedDecision.Decision.Outcome != hoststate.PolicyAllow {
		t.Fatalf("allowed child decision = %#v, %v", allowedDecision, loadErr)
	}
}
