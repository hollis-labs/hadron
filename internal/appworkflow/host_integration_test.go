package appworkflow_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/artifacts"
	"github.com/hollis-labs/hadron/internal/persistence"
	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

func TestHostGraphNativeStartInspectExplainReplayAndActivation(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if startErr := fixture.host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-one", "key-one", "user:one")
	callerContext := authenticatedContext(t.Context(), "user:one")
	started, err := fixture.host.StartRun(callerContext, request)
	if err != nil || started.Run == nil || started.Run.Status != workflowruntime.RunRunning || started.Phase != hoststate.StartRunning {
		t.Fatalf("StartRun = %#v, %v", started, err)
	}
	if started.Facts.BlastRadius["compute"] != 1 || started.Facts.RequiredCapabilities == nil && len(started.Facts.TargetRequirements) == 0 {
		t.Fatalf("policy facts = %#v", started.Facts)
	}
	inspected, err := fixture.host.InspectRun(t.Context(), request.RunID)
	if err != nil || len(inspected.Nodes) != 1 || inspected.Nodes[0].Status != workflowruntime.NodeReady || len(inspected.Decisions) != 1 {
		t.Fatalf("InspectRun = %#v, %v", inspected, err)
	}
	explained, err := fixture.host.ExplainRun(t.Context(), request.RunID)
	if err != nil || explained.Decision.ID == "" || explained.DryRunTruth == "" {
		t.Fatalf("ExplainRun = %#v, %v", explained, err)
	}
	replayed, err := fixture.host.StartRun(callerContext, request)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Run == nil || replayed.Run.ID != started.Run.ID {
		t.Fatalf("StartRun replay = %#v, %v", replayed, err)
	}
	// The submitted hint is deliberately identical while the payload changes.
	// The host must authenticate before revealing that the key conflicts.
	foreign := request
	foreign.Inputs = map[string]any{"message": "changed"}
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:other"), foreign); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("cross-caller replay error = %v", err)
	}
	if _, err := fixture.host.StartRun(callerContext, foreign); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("same-caller divergent replay error = %v", err)
	}
	activation := workflowwait.Activation{ID: "timer-1", Kind: "test", RunID: string(request.RunID), NodeID: "echo", FireAt: fixture.now.Add(time.Hour), DedupKey: "timer-dedup"}
	if err := fixture.host.Schedule(t.Context(), activation); err != nil {
		t.Fatal(err)
	}
	if err := fixture.host.Cancel(t.Context(), activation.ID); err != nil {
		t.Fatal(err)
	}
	if fixture.scheduler.scheduled != 1 || fixture.scheduler.canceled != 1 {
		t.Fatalf("scheduler calls = %#v", fixture.scheduler)
	}
}

func TestHostAuthenticatesBeforeResolvingFirstStart(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-no-auth", "key-no-auth", "untrusted-hint")
	if _, err := fixture.host.StartRun(t.Context(), request); err == nil || fixture.definitionCalls.Load() != 0 {
		t.Fatalf("unauthenticated StartRun error=%v resolver_calls=%d", err, fixture.definitionCalls.Load())
	}
}

func TestHostConcurrentIdenticalStartConverges(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-concurrent", "key-concurrent", "user:concurrent")
	start := make(chan struct{})
	results := make(chan appworkflow.StartRunResult, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			result, err := fixture.host.StartRun(authenticatedContext(context.Background(), "user:concurrent"), request)
			results <- result
			errs <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent StartRun = %v", err)
		}
		if result := <-results; result.Run == nil || result.Run.Status != workflowruntime.RunRunning {
			t.Fatalf("concurrent result = %#v", result)
		}
	}
	if fixture.policyCalls.Load() < 1 || fixture.policyCalls.Load() > 2 {
		t.Fatalf("concurrent policy calls = %d", fixture.policyCalls.Load())
	}
}

func TestHostStartConvergesWhenCheckpointWinnerIsFarAhead(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	journal := &farAheadStartJournal{Journal: fixture.journal, state: fixture.state}
	host, newErr := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: journal,
		Definitions: definitionProvider{plan: fixture.plan},
		Identity: identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
			principal, _ := ctx.Value(authenticatedPrincipalKey{}).(string)
			return hoststate.IdentityBinding{Principal: principal, SourceAuthority: request.SourceAuthority, Trust: "trusted", Grants: []string{"workflow.run"}, RunScope: "scope:test", ExecutionTarget: "local"}, nil
		}),
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "fixture policy"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
		Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }), RecoveryInterval: time.Hour,
	})
	if newErr != nil {
		t.Fatal(newErr)
	}
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-far-ahead", "key-far-ahead", "user:far-ahead")
	started, err := host.StartRun(authenticatedContext(t.Context(), "user:far-ahead"), request)
	if err != nil || started.Run == nil || started.Run.Status != workflowruntime.RunRunning || started.Phase != hoststate.StartRunning {
		t.Fatalf("StartRun = %#v, %v", started, err)
	}
}

func TestHostStartRejectsConcurrentPolicyForDifferentResolvedPlan(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	journal := &differentPlanPolicyJournal{Journal: fixture.journal}
	host, newErr := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: journal,
		Definitions: definitionProvider{plan: fixture.plan},
		Identity: identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
			principal, _ := ctx.Value(authenticatedPrincipalKey{}).(string)
			return hoststate.IdentityBinding{Principal: principal, SourceAuthority: request.SourceAuthority, Trust: "trusted", Grants: []string{"workflow.run"}, RunScope: "scope:test", ExecutionTarget: "local"}, nil
		}),
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "fixture policy"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
		Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }), RecoveryInterval: time.Hour,
	})
	if newErr != nil {
		t.Fatal(newErr)
	}
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-policy-race", "key-policy-race", "user:policy-race")
	if _, err := host.StartRun(authenticatedContext(t.Context(), "user:policy-race"), request); err == nil || err.Error() != "persisted policy plan differs from resolved plan" {
		t.Fatalf("StartRun error = %v", err)
	}
	if _, err := fixture.state.LoadRun(t.Context(), request.RunID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("run should not be bound, error = %v", err)
	}
}

func TestHostConfirmationAcknowledgmentReusesDurablePolicyDecision(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyConfirm, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-confirm", "key-confirm", "user:confirm")
	callerContext := authenticatedContext(t.Context(), "user:confirm")
	first, err := fixture.host.StartRun(callerContext, request)
	if !errors.Is(err, appworkflow.ErrConfirmationRequired) || first.Decision.ID == "" {
		t.Fatalf("unconfirmed StartRun = %#v, %v", first, err)
	}
	request.Confirmed = true
	confirmed, err := fixture.host.StartRun(callerContext, request)
	if err != nil || confirmed.Run == nil || confirmed.Run.Status != workflowruntime.RunRunning || confirmed.Decision.ID != first.Decision.ID {
		t.Fatalf("confirmed StartRun = %#v, %v", confirmed, err)
	}
	replayed, err := fixture.host.StartRun(callerContext, request)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Decision.ID != first.Decision.ID {
		t.Fatalf("confirmed replay = %#v, %v", replayed, err)
	}
	if fixture.policyCalls.Load() != 1 || fixture.identityCalls.Load() != 3 {
		t.Fatalf("confirmation calls: policy=%d identity=%d", fixture.policyCalls.Load(), fixture.identityCalls.Load())
	}
	decisions, err := fixture.journal.ListPolicyDecisions(t.Context(), request.RunID)
	if err != nil || len(decisions) != 1 || decisions[0].ID != first.Decision.ID {
		t.Fatalf("confirmation decisions = %#v, %v", decisions, err)
	}
}

func TestHostCancellationReplaysOmittedTimeExactly(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	start := fixture.startRequest("run-cancel", "start-cancel", "user:cancel")
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:cancel"), start); err != nil {
		t.Fatal(err)
	}
	command := appworkflow.CancelRunRequest{RunID: start.RunID, IdempotencyKey: "cancel-command", Reason: "operator request"}
	first, failures, err := fixture.host.CancelRun(t.Context(), command)
	if err != nil || len(failures) != 0 || first.Outcome != workflowruntime.IdempotencyApplied || first.Run.Status != workflowruntime.RunCanceled {
		t.Fatalf("CancelRun = %#v, failures=%v, %v", first, failures, err)
	}
	replayed, failures, err := fixture.host.CancelRun(t.Context(), command)
	if err != nil || len(failures) != 0 || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Run.ID != first.Run.ID {
		t.Fatalf("CancelRun replay = %#v, failures=%v, %v", replayed, failures, err)
	}
	changed := command
	changed.Reason = "different reason"
	if _, _, changedErr := fixture.host.CancelRun(t.Context(), changed); !errors.Is(changedErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed cancellation reason = %v", changedErr)
	}
	changed = command
	changed.IdempotencyKey = "different-cancel-key"
	if _, _, changedErr := fixture.host.CancelRun(t.Context(), changed); !errors.Is(changedErr, workflowruntime.ErrInvalidTransition) {
		t.Fatalf("changed cancellation key = %v", changedErr)
	}
	events, err := fixture.state.ListEvents(t.Context(), workflowruntime.EventQuery{RunID: start.RunID})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Type == workflowruntime.EventRunCancellationRequested {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("cancellation event count = %d, events=%#v", count, events)
	}
}

func TestHostRestartRecoversJournaledCancellation(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	start := fixture.startRequest("run-cancel-recovery", "start-cancel-recovery", "user:cancel")
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:cancel"), start); err != nil {
		t.Fatal(err)
	}
	intent := hoststate.CancellationIntent{RunID: start.RunID, IdempotencyKey: "cancel-recovery", Reason: "restart recovery"}
	if _, outcome, err := fixture.journal.BindCancellation(t.Context(), hoststate.BindCancellationRequest{Intent: intent, DefaultAt: fixture.now}); err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("BindCancellation = %q, %v", outcome, err)
	}
	if err := fixture.host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	run, err := fixture.state.LoadRun(t.Context(), start.RunID)
	if err != nil || run.Status != workflowruntime.RunCanceled {
		t.Fatalf("recovered cancellation run = %#v, %v", run, err)
	}
	pending, err := fixture.journal.ListPendingCancellations(t.Context(), 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending cancellations = %#v, %v", pending, err)
	}
}

func TestHostCancellationBoundsCASChurnAndLeavesRecoveryWork(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	churning := &alwaysCASStateStore{StateStore: fixture.state}
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		principal, _ := ctx.Value(authenticatedPrincipalKey{}).(string)
		return hoststate.IdentityBinding{Principal: principal, SourceAuthority: request.SourceAuthority, Trust: "trusted", Grants: []string{"workflow.run"}, RunScope: "scope:test", ExecutionTarget: "local"}, nil
	})
	host, newErr := appworkflow.New(appworkflow.Options{
		State: churning, Journal: fixture.journal, Definitions: definitionProvider{plan: fixture.plan}, Identity: identity,
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "allow"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
		Activations: fixture.scheduler, Artifacts: fixture.artifacts, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }),
		RecoveryInterval: time.Hour, ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if newErr != nil {
		t.Fatal(newErr)
	}
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-cas-churn", "start-cas-churn", "user:churn")
	if _, startErr := host.StartRun(authenticatedContext(t.Context(), "user:churn"), request); startErr != nil {
		t.Fatal(startErr)
	}
	_, _, cancelErr := host.CancelRun(t.Context(), appworkflow.CancelRunRequest{RunID: request.RunID, IdempotencyKey: "cancel-cas-churn", Reason: "bounded churn"})
	if !errors.Is(cancelErr, workflowruntime.ErrCASMismatch) || churning.calls.Load() != 8 {
		t.Fatalf("bounded cancellation error=%v calls=%d", cancelErr, churning.calls.Load())
	}
	pending, pendingErr := fixture.journal.ListPendingCancellations(t.Context(), 0)
	if pendingErr != nil || len(pending) != 1 || pending[0].Intent.IdempotencyKey != "cancel-cas-churn" {
		t.Fatalf("recoverable cancellation = %#v, %v", pending, pendingErr)
	}
}

func TestHostPersistsDeniedPolicyAndReplaysAfterAuthenticatingCaller(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyDeny, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-denied", "key-denied", "user:denied")
	callerContext := authenticatedContext(t.Context(), "user:denied")
	first, err := fixture.host.StartRun(callerContext, request)
	if !errors.Is(err, appworkflow.ErrPolicyDenied) || first.Decision.ID == "" {
		t.Fatalf("denied StartRun = %#v, %v", first, err)
	}
	second, err := fixture.host.StartRun(callerContext, request)
	if !errors.Is(err, appworkflow.ErrPolicyDenied) || second.Decision.ID != first.Decision.ID {
		t.Fatalf("denied replay = %#v, %v", second, err)
	}
	if fixture.policyCalls.Load() != 1 || fixture.identityCalls.Load() != 2 {
		t.Fatalf("replay authorization calls: policy=%d identity=%d", fixture.policyCalls.Load(), fixture.identityCalls.Load())
	}
	if result, replayErr := fixture.host.StartRun(authenticatedContext(t.Context(), "user:other"), request); !errors.Is(replayErr, appworkflow.ErrPolicyDenied) || result.Decision.ID != "" || len(result.Facts.Identity.Principal) != 0 {
		t.Fatalf("cross-caller denied replay leaked result: %#v, %v", result, replayErr)
	}
	if _, loadErr := fixture.state.LoadRun(t.Context(), request.RunID); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		t.Fatalf("denied request created run: %v", loadErr)
	}
	evaluation, err := fixture.journal.LoadPolicyEvaluation(t.Context(), first.Decision.ID)
	if err != nil || evaluation.Facts.Identity.Principal != "user:denied" || evaluation.Decision.Outcome != hoststate.PolicyDeny {
		t.Fatalf("persisted denied evaluation = %#v, %v", evaluation, err)
	}
}

func TestHostStartupDrainsBatchesAndPeriodicRecoveryStaysReady(t *testing.T) {
	block := &periodicBlockHook{entered: make(chan struct{}, 1), release: make(chan struct{})}
	fixture := newHostFixture(t, hoststate.PolicyAllow, 10*time.Millisecond, block)
	for index := 0; index < 3; index++ {
		seedIncompleteStart(t, fixture, index)
	}
	if startErr := fixture.host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { close(block.release); _ = fixture.host.Shutdown(context.Background()) })
	for index := 0; index < 3; index++ {
		snapshot, err := fixture.journal.LoadStart(t.Context(), workflowruntime.RunID("seed-run-"+string(rune('a'+index))))
		if err != nil || snapshot.Phase != hoststate.StartRunning {
			t.Fatalf("seed[%d] = %#v, %v", index, snapshot, err)
		}
	}
	select {
	case <-block.entered:
	case <-time.After(time.Second):
		t.Fatal("periodic recovery did not enter")
	}
	health := fixture.host.Health()
	if !health.Ready || health.Recovering {
		t.Fatalf("health during periodic recovery = %#v", health)
	}
}

func TestHostStartupMaterializesPendingChildRunThroughRecoverySeam(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	parent := seedRunningCallParent(t, fixture)
	request := childRecoveryRequest(t, fixture.plan, parent.Node.ID)
	created, err := fixture.journal.StartChildRun(t.Context(), request)
	if err != nil || created.Run.Status != workflowruntime.RunPending {
		t.Fatalf("StartChildRun = %#v, %v", created, err)
	}
	if startErr := fixture.host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	child, err := fixture.state.LoadRun(t.Context(), request.ChildRunID)
	if err != nil || child.Status != workflowruntime.RunRunning || fixture.childCalls.Load() != 1 {
		t.Fatalf("recovered child = %#v, calls=%d, %v", child, fixture.childCalls.Load(), err)
	}
}

func TestHostStartDoesNotReportSuccessDuringFailingRecovery(t *testing.T) {
	hook := &startupFailureHook{entered: make(chan struct{}, 1), release: make(chan struct{})}
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, hook)
	first := make(chan error, 1)
	go func() { first <- fixture.host.Start(context.Background()) }()
	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("startup recovery did not enter")
	}
	if err := fixture.host.Start(t.Context()); !errors.Is(err, appworkflow.ErrHostNotReady) {
		t.Fatalf("concurrent Start = %v", err)
	}
	close(hook.release)
	if err := <-first; err == nil || fixture.host.Health().Started || fixture.host.Health().Ready {
		t.Fatalf("failed Start = %v, health=%#v", err, fixture.host.Health())
	}
}

func TestHostShutdownCancelsBlockedStartupWithoutReadyResurrection(t *testing.T) {
	hook := &startupFailureHook{entered: make(chan struct{}, 1), release: make(chan struct{})}
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, hook)
	startResult := make(chan error, 1)
	go func() { startResult <- fixture.host.Start(context.Background()) }()
	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("startup recovery did not enter")
	}
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- fixture.host.Shutdown(context.Background()) }()
	select {
	case err := <-startResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel startup recovery")
	}
	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatalf("Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not complete")
	}
	if health := fixture.host.Health(); health.Started || health.Ready || health.Recovering {
		t.Fatalf("health after startup shutdown = %#v", health)
	}
	if err := fixture.host.Shutdown(t.Context()); err != nil {
		t.Fatalf("idempotent Shutdown = %v", err)
	}
}

func TestHostShutdownCannotResurrectReadyAfterPeriodicRecovery(t *testing.T) {
	hook := &periodicIgnoringCancellationHook{entered: make(chan struct{}, 1), release: make(chan struct{})}
	fixture := newHostFixture(t, hoststate.PolicyAllow, 10*time.Millisecond, hook)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("periodic recovery did not enter")
	}
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- fixture.host.Shutdown(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for fixture.host.Health().Ready {
		if time.Now().After(deadline) {
			t.Fatal("Shutdown did not clear readiness")
		}
		time.Sleep(time.Millisecond)
	}
	close(hook.release)
	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatalf("Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not complete")
	}
	if health := fixture.host.Health(); health.Started || health.Ready || health.Recovering {
		t.Fatalf("health after periodic shutdown = %#v", health)
	}
}

func TestHostStartReplayAcceptsRunAdvancedPastRunning(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-complete-replay", "key-complete-replay", "user:complete")
	callerContext := authenticatedContext(t.Context(), "user:complete")
	started, err := fixture.host.StartRun(callerContext, request)
	if err != nil || started.Run == nil {
		t.Fatalf("StartRun = %#v, %v", started, err)
	}
	completed, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: request.RunID, ExpectedGeneration: started.Run.Generation, To: workflowruntime.RunSucceeded, At: started.Run.UpdatedAt.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := fixture.host.StartRun(callerContext, request)
	if err != nil || replayed.Run == nil || replayed.Run.Status != workflowruntime.RunSucceeded || replayed.Run.Generation != completed.Snapshot.Generation {
		t.Fatalf("completed StartRun replay = %#v, %v", replayed, err)
	}
}

func TestHostDoesNotReadyCatchSwitchOrFinallyTargetsAsRoots(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	*fixture.plan = *compileControlRoutePlan(t)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("run-control-routes", "key-control-routes", "user:routes")
	started, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:routes"), request)
	if err != nil || started.Run == nil {
		t.Fatalf("StartRun = %#v, %v", started, err)
	}
	inspected, err := fixture.host.InspectRun(t.Context(), request.RunID)
	if err != nil || len(inspected.Nodes) != 4 {
		t.Fatalf("InspectRun = %#v, %v", inspected, err)
	}
	statuses := make(map[string]workflowruntime.NodeStatus, len(inspected.Nodes))
	for _, node := range inspected.Nodes {
		statuses[node.ID.NodeID] = node.Status
	}
	if statuses["source"] != workflowruntime.NodeReady || statuses["catch-target"] != workflowruntime.NodePending || statuses["switch-target"] != workflowruntime.NodePending || statuses["cleanup"] != workflowruntime.NodePending {
		t.Fatalf("control-route initial statuses = %#v", statuses)
	}
}

type hostFixture struct {
	host            *appworkflow.Host
	store           *persistence.Store
	state           *persistence.WorkflowStateStore
	journal         *persistence.WorkflowHostStore
	plan            *workflowcompile.ExecutionPlan
	now             time.Time
	scheduler       *activationRecorder
	policyCalls     atomic.Int32
	identityCalls   atomic.Int32
	childCalls      atomic.Int32
	definitionCalls atomic.Int32
	artifacts       values.ArtifactStore
}

func newHostFixture(t *testing.T, outcome hoststate.PolicyOutcome, interval time.Duration, hook appworkflow.RecoveryHook) *hostFixture {
	t.Helper()
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	store, openErr := persistence.Open(filepath.Join(t.TempDir(), "host.db"))
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, _ := persistence.NewWorkflowStateStore(store)
	journal, _ := persistence.NewWorkflowHostStore(store)
	plan := compileHostPlan(t)
	scheduler := &activationRecorder{}
	artifactRoot, pathErr := filepath.EvalSymlinks(t.TempDir())
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	artifactStore, artifactErr := artifacts.New(artifactRoot, values.ArtifactAuthorizerFunc(func(context.Context, values.ArtifactAuthorization) error { return nil }), nil)
	if artifactErr != nil {
		t.Fatal(artifactErr)
	}
	fixture := &hostFixture{store: store, state: state, journal: journal, plan: plan, now: now, scheduler: scheduler, artifacts: artifactStore}
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		fixture.identityCalls.Add(1)
		principal, ok := ctx.Value(authenticatedPrincipalKey{}).(string)
		if !ok || principal == "" {
			return hoststate.IdentityBinding{}, errors.New("missing authenticated principal")
		}
		return hoststate.IdentityBinding{Principal: principal, SourceAuthority: request.SourceAuthority, Trust: "trusted", Grants: []string{"workflow.run"}, RunScope: "scope:test", ExecutionTarget: "local"}, nil
	})
	policy := appworkflow.PolicyEvaluatorFunc(func(_ context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
		fixture.policyCalls.Add(1)
		return hoststate.PolicyDecision{Outcome: outcome, Reason: "fixture policy"}, nil
	})
	hooks := []appworkflow.RecoveryHook(nil)
	if hook != nil {
		hooks = append(hooks, hook)
	}
	childMaterializer := childMaterializerFunc(func(ctx context.Context, request calladapter.ChildRunRequest) error {
		fixture.childCalls.Add(1)
		child, err := fixture.state.LoadRun(ctx, request.ChildRunID)
		if err != nil {
			return err
		}
		if child.Status != workflowruntime.RunPending {
			return nil
		}
		_, err = fixture.state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: child.ID, ExpectedGeneration: child.Generation, To: workflowruntime.RunRunning, At: child.UpdatedAt.Add(time.Nanosecond)})
		return err
	})
	host, hostErr := appworkflow.New(appworkflow.Options{State: state, Journal: journal, Definitions: definitionProvider{plan: plan, calls: &fixture.definitionCalls}, Identity: identity, Policy: policy, Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}}, Activations: scheduler, Artifacts: artifactStore, Clock: appworkflow.ClockFunc(func() time.Time { return now }), RecoveryInterval: interval, RecoveryBatchLimit: 1, RecoveryHooks: hooks, ChildRuns: childMaterializer})
	if hostErr != nil {
		t.Fatal(hostErr)
	}
	fixture.host = host
	return fixture
}

func seedRunningCallParent(t *testing.T, fixture *hostFixture) workflowruntime.StartNodeAttemptResult {
	t.Helper()
	bound, bindErr := workflowruntime.BindRun(t.Context(), fixture.state, workflowruntime.BindRunRequest{ID: "call-parent", Plan: fixture.plan, Inputs: map[string]any{"message": "parent"}, CreatedAt: fixture.now})
	if bindErr != nil || bound.Run == nil || len(bound.Diagnostics) != 0 {
		t.Fatalf("BindRun = %#v, %v", bound, bindErr)
	}
	if _, _, err := workflowruntime.StartBoundRun(t.Context(), fixture.state, *bound.Run, "call-parent-start"); err != nil {
		t.Fatal(err)
	}
	run, err := fixture.state.LoadRun(t.Context(), bound.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: fixture.now}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	inputs := bound.Run.InputsRef
	node, err := fixture.state.CreateNodeInvocation(t.Context(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: "call"}, Status: workflowruntime.NodePending, Inputs: &inputs, CreatedAt: fixture.now, UpdatedAt: fixture.now}})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := fixture.state.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "host-test", Token: "host-test-token", IdempotencyKey: "host-test-claim", Now: fixture.now, LeaseUntil: fixture.now.Add(time.Minute)})
	if err != nil || !claimed.Acquired || claimed.Lease == nil {
		t.Fatalf("ClaimNode = %#v, %v", claimed, err)
	}
	current, err := fixture.state.LoadNodeInvocation(t.Context(), node.ID)
	if err != nil {
		t.Fatal(err)
	}
	started, err := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: current.Generation, Claim: workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}, Executor: workflowruntime.ExecutorMetadata{Kind: "call", Version: "v1", Target: "local"}, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func childRecoveryRequest(t *testing.T, plan *workflowcompile.ExecutionPlan, parent workflowruntime.NodeInvocationID) calladapter.ChildRunRequest {
	t.Helper()
	provenance := plan.Provenance
	provenance.Digest = plan.Digest
	definition := graph.DefinitionRef{Authority: provenance.Authority, Kind: "workflow", ID: plan.ID, Locator: provenance.Locator, Version: plan.Graph.Version, Digest: plan.Digest, Provenance: &provenance}
	planRef := workflowruntime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion}
	resolvedGraph := plan.Graph
	resolvedGraph.Digest = plan.Digest
	resolvedGraph.Provenance = provenance
	rootDigest := values.SHA256Digest([]byte("recovery-root"))
	root := graph.DefinitionRef{Authority: "test", Kind: "workflow", ID: "recovery-root", Version: "v1", Digest: rootDigest}
	return calladapter.ChildRunRequest{
		Parent:     calladapter.CallSiteIdentity{RunID: string(parent.RunID), NodeID: parent.NodeID, Iteration: parent.Iteration},
		ChildRunID: "recovered-child", Definition: workflowcompile.ResolvedDefinition{Definition: definition, Graph: resolvedGraph},
		Plan: planRef, Inputs: values.ValueSet{"message": mustInline(t, "child")}, Lineage: []graph.DefinitionRef{root, definition},
		ParentClose: graph.ParentCloseCancel, IdempotencyKey: "recovered-child-start",
	}
}

func (f *hostFixture) startRequest(runID, key, principal string) appworkflow.StartRunRequest {
	return appworkflow.StartRunRequest{RunID: workflowruntime.RunID(runID), Definition: f.plan.Definition, Inputs: map[string]any{"message": "hello"}, IdempotencyKey: key, Identity: appworkflow.IdentityRequest{PrincipalHint: principal, SourceAuthority: "test"}}
}

type authenticatedPrincipalKey struct{}

func authenticatedContext(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, authenticatedPrincipalKey{}, principal)
}

func compileHostPlan(t *testing.T) *workflowcompile.ExecutionPlan {
	t.Helper()
	source := workflowcompile.LoadBytes("host-fixture.workflow.yaml", []byte(`workflow:
  name: Host Fixture
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - name: Echo
    transform:
      result: inputs.message
    with:
      message: inputs.message
    effects: [compute]
`))
	if source.Source == nil || len(source.Diagnostics) != 0 {
		t.Fatalf("LoadBytes = %#v", source)
	}
	result := workflowcompile.Compile(source.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile = %#v", result)
	}
	return result.Plan
}

func compileControlRoutePlan(t *testing.T) *workflowcompile.ExecutionPlan {
	t.Helper()
	source := workflowcompile.LoadBytes("host-control-routes.workflow.yaml", []byte(`workflow:
  name: Host Control Routes
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - name: Source
    transform:
      result: inputs.message
    catch:
      - errors: [failed]
        targets: [Catch Target]
    switch:
      arms:
        - when: inputs.message != ""
          targets: [Switch Target]
  - name: Catch Target
    transform:
      result: caught
  - name: Switch Target
    transform:
      result: switched
  - name: Cleanup
    transform:
      result: cleaned
    finally:
      scope: [Source]
`))
	if source.Source == nil || len(source.Diagnostics) != 0 {
		t.Fatalf("LoadBytes = %#v", source)
	}
	result := workflowcompile.Compile(source.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile = %#v", result)
	}
	return result.Plan
}

func seedIncompleteStart(t *testing.T, fixture *hostFixture, index int) {
	t.Helper()
	suffix := string(rune('a' + index))
	runID := workflowruntime.RunID("seed-run-" + suffix)
	inputs := values.ValueSet{"message": mustInline(t, "seed")}
	ref, err := fixture.state.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "seed", RunID: runID}, Values: inputs})
	if err != nil {
		t.Fatal(err)
	}
	planRef := workflowruntime.PlanRef{ID: fixture.plan.ID, Version: fixture.plan.Graph.Version, Digest: fixture.plan.Digest, SchemaVersion: fixture.plan.SchemaVersion}
	bound := workflowruntime.BoundRun{ID: runID, Plan: planRef, InputsRef: ref, CreatedAt: fixture.now, Provenance: fixture.plan.Provenance}
	identity := hoststate.IdentityBinding{Principal: "seed", SourceAuthority: "test", Trust: "trusted", Grants: []string{"workflow.run"}, RunScope: "scope:test", ExecutionTarget: "local"}
	facts, err := policyFactsForSeed(planRef, runID, identity)
	if err != nil {
		t.Fatal(err)
	}
	decision := hoststate.PolicyDecision{ID: "seed-decision-" + suffix, RunID: runID, Operation: "start", Outcome: hoststate.PolicyAllow, Reason: "seed", DecidedAt: fixture.now}
	digest := values.SHA256Digest([]byte("seed-request-" + suffix))
	record := hoststate.StartRecord{Run: bound, Plan: *fixture.plan, Requested: fixture.plan.Definition, StartKey: "seed-key-" + suffix, RequestDigest: digest, CallerInputHash: values.SHA256Digest([]byte("seed-input-" + suffix)), Identity: identity, Facts: facts, Decision: decision, RecordedAt: fixture.now}
	if _, _, err := fixture.journal.RecordStart(t.Context(), record); err != nil {
		t.Fatal(err)
	}
}

func policyFactsForSeed(plan workflowruntime.PlanRef, runID workflowruntime.RunID, identity hoststate.IdentityBinding) (hoststate.PolicyFacts, error) {
	facts := hoststate.PolicyFacts{Operation: "start", RunID: runID, Plan: plan, Identity: identity, Effects: graph.EffectSet{graph.EffectCompute}, NodeCount: 1, BlastRadius: map[string]int{"compute": 1}}
	return facts, facts.Validate()
}
func mustInline(t *testing.T, input any) values.Value {
	t.Helper()
	value, err := values.NewInline(input, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "fixture"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

type definitionProvider struct {
	plan  *workflowcompile.ExecutionPlan
	calls *atomic.Int32
}

func (p definitionProvider) ResolvePlan(context.Context, graph.DefinitionRef) (*workflowcompile.ExecutionPlan, error) {
	if p.calls != nil {
		p.calls.Add(1)
	}
	copyPlan := *p.plan
	return &copyPlan, nil
}
func (p definitionProvider) LoadPlan(_ context.Context, digest string) (*workflowcompile.ExecutionPlan, error) {
	if digest != p.plan.Digest {
		return nil, workflowruntime.ErrNotFound
	}
	copyPlan := *p.plan
	return &copyPlan, nil
}

type identityProviderFunc func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error)

func (f identityProviderFunc) BindIdentity(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
	return f(ctx, request)
}

type alwaysCASStateStore struct {
	workflowruntime.StateStore
	calls atomic.Int32
}

type farAheadStartJournal struct {
	hoststate.Journal
	state workflowruntime.StateStore
	once  atomic.Bool
}

func (j *farAheadStartJournal) AdvanceStart(ctx context.Context, request hoststate.AdvanceStartRequest) (hoststate.StartSnapshot, error) {
	if request.From != hoststate.StartRecorded || request.To != hoststate.StartRunCreated || !j.once.CompareAndSwap(false, true) {
		return j.Journal.AdvanceStart(ctx, request)
	}
	current, err := j.Journal.AdvanceStart(ctx, request)
	if err != nil {
		return hoststate.StartSnapshot{}, err
	}
	inputRef := current.Record.Run.InputsRef
	nodeID := workflowruntime.NodeInvocationID{RunID: current.Record.Run.ID, NodeID: current.Record.Plan.Graph.Nodes[0].ID}
	node, err := j.state.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: nodeID, Status: workflowruntime.NodePending, Inputs: &inputRef, CreatedAt: current.Record.Run.CreatedAt, UpdatedAt: current.Record.Run.CreatedAt}})
	if err != nil {
		return hoststate.StartSnapshot{}, err
	}
	if _, err = j.state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: current.UpdatedAt}); err != nil {
		return hoststate.StartSnapshot{}, err
	}
	run, err := j.state.LoadRun(ctx, current.Record.Run.ID)
	if err != nil {
		return hoststate.StartSnapshot{}, err
	}
	if _, err = j.state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: current.UpdatedAt}); err != nil {
		return hoststate.StartSnapshot{}, err
	}
	current, err = j.Journal.AdvanceStart(ctx, hoststate.AdvanceStartRequest{RunID: current.Record.Run.ID, ExpectedGeneration: current.Generation, From: current.Phase, To: hoststate.StartNodesMaterialized, At: current.UpdatedAt})
	if err != nil {
		return hoststate.StartSnapshot{}, err
	}
	return j.Journal.AdvanceStart(ctx, hoststate.AdvanceStartRequest{RunID: current.Record.Run.ID, ExpectedGeneration: current.Generation, From: current.Phase, To: hoststate.StartRunning, At: current.UpdatedAt})
}

type differentPlanPolicyJournal struct {
	hoststate.Journal
}

func (j *differentPlanPolicyJournal) RecordPolicyEvaluation(_ context.Context, evaluation hoststate.PolicyEvaluation) (hoststate.PolicyEvaluation, workflowruntime.IdempotencyOutcome, error) {
	evaluation.Facts.Plan.Digest = values.SHA256Digest([]byte("different resolved plan"))
	return evaluation, workflowruntime.IdempotencyReplayed, nil
}

func (s *alwaysCASStateStore) RequestRunCancellation(_ context.Context, request workflowruntime.RequestRunCancellationRequest) (workflowruntime.RequestRunCancellationResult, error) {
	s.calls.Add(1)
	return workflowruntime.RequestRunCancellationResult{}, &workflowruntime.CASMismatchError{Resource: "test cancellation churn", Expected: request.ExpectedGeneration, Actual: request.ExpectedGeneration + 1}
}

type childMaterializerFunc func(context.Context, calladapter.ChildRunRequest) error

func (f childMaterializerFunc) MaterializeChildRun(ctx context.Context, request calladapter.ChildRunRequest) error {
	return f(ctx, request)
}

type activationRecorder struct {
	mu                  sync.Mutex
	scheduled, canceled int
}

func (r *activationRecorder) Schedule(context.Context, workflowwait.Activation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scheduled++
	return nil
}
func (r *activationRecorder) Cancel(context.Context, workflowwait.ActivationID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.canceled++
	return nil
}

type periodicBlockHook struct {
	calls            atomic.Int32
	entered, release chan struct{}
}

type startupFailureHook struct {
	entered, release chan struct{}
}

type periodicIgnoringCancellationHook struct {
	calls            atomic.Int32
	entered, release chan struct{}
}

func (h *periodicIgnoringCancellationHook) RecoverWorkflow(context.Context, workflowruntime.RecoverySnapshot, time.Time) error {
	if h.calls.Add(1) == 1 {
		return nil
	}
	select {
	case h.entered <- struct{}{}:
	default:
	}
	<-h.release
	return nil
}

func (h *startupFailureHook) RecoverWorkflow(ctx context.Context, _ workflowruntime.RecoverySnapshot, _ time.Time) error {
	select {
	case h.entered <- struct{}{}:
	default:
	}
	select {
	case <-h.release:
		return errors.New("startup recovery failed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *periodicBlockHook) RecoverWorkflow(ctx context.Context, _ workflowruntime.RecoverySnapshot, _ time.Time) error {
	if h.calls.Add(1) == 1 {
		return nil
	}
	select {
	case h.entered <- struct{}{}:
	default:
	}
	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
