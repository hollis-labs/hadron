package appworkflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	calladapter "github.com/hollis-labs/go-workflow/adapters/call"
	"github.com/hollis-labs/go-workflow/adapters/transform"
	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/unattended"
)

// childAllowListHarness runs root starts on the fixture host and child-run
// gating on a second host that shares its journal and state, both with the
// production shape: every start needs confirmation unless the unattended
// allow-list waives it for that exact plan.
type childAllowListHarness struct {
	fixture *hostFixture
	gated   *appworkflow.Host
	plan    *workflowcompile.ExecutionPlan
}

func newChildAllowListHarness(t *testing.T, entries func(root, child hostPlanRef) []unattended.Entry) *childAllowListHarness {
	t.Helper()
	fixture := newHostFixture(t, hoststate.PolicyConfirm, time.Hour, nil)
	childPlan := compileFinalizerHostPlan(t)
	store, _ := writeAllowList(t, entries(
		hostPlanRef{ID: fixture.plan.ID, Digest: fixture.plan.Graph.Digest},
		hostPlanRef{ID: childPlan.ID, Digest: childPlan.Graph.Digest},
	)...)
	policy := allowListPolicy(store, func() time.Time { return fixture.now })
	fixture.setPolicy(policy)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	gated, err := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: fixture.journal,
		Definitions: definitionProvider{plan: fixture.plan, calls: &fixture.definitionCalls},
		Identity: identityProviderFunc(func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
			return hoststate.IdentityBinding{}, errors.New("child policy must not rebind identity")
		}),
		Policy: appworkflow.PolicyEvaluatorFunc(func(_ context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return policy(facts), nil
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
	return &childAllowListHarness{fixture: fixture, gated: gated, plan: childPlan}
}

type hostPlanRef struct{ ID, Digest string }

func allowEntry(id string, plan hostPlanRef, activation string) unattended.Entry {
	return unattended.Entry{
		ID: id, PlanID: plan.ID, Digest: plan.Digest, Scope: unattended.Scope{ActivationID: activation},
		Reason: id + " runs unattended", AddedBy: "local:operator", AddedAt: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC),
	}
}

// startRoot starts a root run and returns a child request parented to it.
func (h *childAllowListHarness) startRoot(t *testing.T, name string, confirmed bool, activation string) (calladapter.ChildRunRequest, hoststate.PolicyDecision) {
	t.Helper()
	request := h.fixture.startRequest("root-"+name, "root-"+name, "user:one")
	request.Confirmed = confirmed
	if activation != "" {
		request.Activation = &hoststate.ActivationBinding{ActivationID: activation, IdempotencyKey: "fire-" + name, OccurredAt: h.fixture.now}
	}
	root, err := h.fixture.host.StartRun(authenticatedContext(t.Context(), "user:one"), request)
	if err != nil || root.Run == nil {
		t.Fatalf("root %s start = %#v, %v", name, root, err)
	}
	child := childRecoveryRequest(t, h.plan, workflowruntime.NodeInvocationID{RunID: root.Run.ID, NodeID: "echo"})
	child.ChildRunID = workflowruntime.RunID("child-" + name)
	child.IdempotencyKey = "child-" + name + "-start"
	return child, root.Decision
}

// passedGate reports that the child cleared the policy gate: it reached the
// journal, which refuses it only because this fixture's parent node is not
// running.
func passedGate(err error) bool {
	var execution *stepkind.ExecutionError
	if errors.As(err, &execution) && execution.Code == appworkflow.CodeChildRunPolicyDenied {
		return false
	}
	return errors.Is(err, workflowruntime.ErrTransitionConflict)
}

func refusedForConfirmation(err error) bool {
	var execution *stepkind.ExecutionError
	return errors.As(err, &execution) && execution.Code == appworkflow.CodeChildRunPolicyDenied && errors.Is(err, appworkflow.ErrConfirmationRequired)
}

func TestUnattendedChildNeedsItsOwnEntry(t *testing.T) {
	t.Run("allow-listed root and child", func(t *testing.T) {
		h := newChildAllowListHarness(t, func(root, child hostPlanRef) []unattended.Entry {
			return []unattended.Entry{allowEntry("ua-root", root, ""), allowEntry("ua-child", child, "")}
		})
		child, rootDecision := h.startRoot(t, "listed", false, "")
		if rootDecision.Outcome != hoststate.PolicyAllow || rootDecision.Attributes[unattended.AttrEntry] != "ua-root" {
			t.Fatalf("root decision = %#v", rootDecision)
		}
		if _, err := h.gated.StartChildRun(t.Context(), child); !passedGate(err) {
			t.Fatalf("allow-listed child = %v", err)
		}
		persisted, err := h.fixture.journal.LoadPolicyEvaluationByStartKey(t.Context(), "child-run:"+string(child.ChildRunID))
		if err != nil || persisted.Decision.Outcome != hoststate.PolicyAllow || persisted.Decision.Attributes[unattended.AttrEntry] != "ua-child" {
			t.Fatalf("child decision should name the child's own entry: %#v, %v", persisted.Decision, err)
		}
	})
	t.Run("allow-listed root, unlisted child", func(t *testing.T) {
		h := newChildAllowListHarness(t, func(root, _ hostPlanRef) []unattended.Entry {
			return []unattended.Entry{allowEntry("ua-root", root, "")}
		})
		child, _ := h.startRoot(t, "unlisted-child", false, "")
		if _, err := h.gated.StartChildRun(t.Context(), child); !refusedForConfirmation(err) {
			t.Fatalf("unlisted child under allow-listed root = %v, want child_run_policy_denied (confirmation)", err)
		}
	})
	t.Run("human-confirmed root, unlisted child", func(t *testing.T) {
		h := newChildAllowListHarness(t, func(hostPlanRef, hostPlanRef) []unattended.Entry { return nil })
		child, rootDecision := h.startRoot(t, "confirmed", true, "")
		if rootDecision.Outcome != hoststate.PolicyConfirm {
			t.Fatalf("root decision = %#v, want confirm", rootDecision)
		}
		if _, err := h.gated.StartChildRun(t.Context(), child); !passedGate(err) {
			t.Fatalf("child under human-confirmed root = %v", err)
		}
	})
}

// A child inherits the activation that started its lineage, so an
// activation-scoped child entry covers only that activation's runs.
func TestUnattendedChildInheritsRootActivation(t *testing.T) {
	h := newChildAllowListHarness(t, func(root, child hostPlanRef) []unattended.Entry {
		return []unattended.Entry{allowEntry("ua-root", root, ""), allowEntry("ua-child-a", child, "act-a")}
	})
	fromA, _ := h.startRoot(t, "from-a", false, "act-a")
	if _, err := h.gated.StartChildRun(t.Context(), fromA); !passedGate(err) {
		t.Fatalf("child of activation act-a = %v", err)
	}
	persisted, err := h.fixture.journal.LoadPolicyEvaluationByStartKey(t.Context(), "child-run:"+string(fromA.ChildRunID))
	if err != nil || persisted.Facts.ActivationID != "act-a" || persisted.Decision.Attributes[unattended.AttrEntry] != "ua-child-a" {
		t.Fatalf("child facts/decision = %#v / %#v, %v", persisted.Facts.ActivationID, persisted.Decision, err)
	}
	fromB, _ := h.startRoot(t, "from-b", false, "act-b")
	if _, err := h.gated.StartChildRun(t.Context(), fromB); !refusedForConfirmation(err) {
		t.Fatalf("child of activation act-b = %v, want refused", err)
	}
}
