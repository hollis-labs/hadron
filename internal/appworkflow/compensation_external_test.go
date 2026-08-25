package appworkflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/hadron/workflow/values"
)

type hostReversibleKind struct{ *stepkindtest.Kind }

type hostStateWithoutCompensation struct {
	workflowruntime.StateStore
	workflowruntime.RecoveryStore
	workflowruntime.NodeInputStore
	workflowruntime.ControlFlowStore
	workflowruntime.RunPolicyStore
}

func (k *hostReversibleKind) DescribeReversibility(context.Context, stepkind.ReversibilityRequest) (stepkind.ReversibilityEvidence, error) {
	return stepkind.ReversibilityEvidence{Operation: "fixture.create", ReceiptSchema: graph.Schema{}}, nil
}

func TestHostRejectsCompensatedPlanBeforeStartMutationWithoutDurableExtension(t *testing.T) {
	plan, effect, undo := compensationHostPlan(t)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	state := hostStateWithoutCompensation{StateStore: fixture.state, RecoveryStore: fixture.state, NodeInputStore: fixture.state, ControlFlowStore: fixture.state, RunPolicyStore: fixture.state}
	host, err := appworkflow.New(appworkflow.Options{
		State: state, Journal: fixture.journal, Definitions: definitionProvider{plan: plan},
		Identity: identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
			principal, _ := ctx.Value(authenticatedPrincipalKey{}).(string)
			return testIdentityBinding(principal, request.SourceAuthority), nil
		}),
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "fixture policy"}, nil
		}),
		Kinds: []stepkind.StepKind{effect, undo}, RequiredKinds: []appworkflow.KindRef{{Name: "host-effect", Version: "v1"}, {Name: "host-undo", Version: "v1"}},
		Activations: fixture.scheduler, Artifacts: fixture.artifacts, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }), RecoveryInterval: time.Hour,
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	request := fixture.startRequest("compensation-no-store", "compensation-no-store-start", "user:compensation-owner")
	if _, err := host.StartRun(authenticatedContext(t.Context(), "user:compensation-owner"), request); !errors.Is(err, workflowruntime.ErrInvalidCompensation) {
		t.Fatalf("compensated start without store = %v", err)
	}
	if _, err := fixture.state.LoadRun(t.Context(), request.RunID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("rejected compensated start persisted run = %v", err)
	}
	if _, err := fixture.journal.LoadStartByKey(t.Context(), request.IdempotencyKey); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("rejected compensated start persisted journal = %v", err)
	}
}

func TestHostIssuesBoundedReplayProofForCollectingLedger(t *testing.T) {
	plan, effect, undo := compensationHostPlan(t)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	host := newCompensationHost(t, fixture, effect, undo, func(string) hoststate.PolicyOutcome { return hoststate.PolicyAllow })
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	ctx := authenticatedContext(t.Context(), "user:collecting-replay")
	request := fixture.startRequest("collecting-replay-source", "collecting-replay-start", "user:collecting-replay")
	started, err := host.StartRun(ctx, request)
	if err != nil || started.Run == nil {
		t.Fatalf("StartRun = %#v, %v", started, err)
	}
	dispatchCompensationNode(t, fixture, plan.Graph.Nodes[0], effect, undo, started.Run.ID, fixture.now.Add(21*time.Second))
	run, _ := fixture.state.LoadRun(ctx, started.Run.ID)
	if _, err := fixture.state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunSucceeded, At: fixture.now.Add(23 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	ledger, err := fixture.state.LoadCompensationLedger(ctx, started.Run.ID)
	if err != nil || ledger.Status != workflowruntime.CompensationCollecting || ledger.Outcome != "" {
		t.Fatalf("collecting ledger = %#v, %v", ledger, err)
	}
	var replayRequests []workflowruntime.ReplayRequest
	operator, err := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{
		Host: host,
		Diagnostics: graphInspectorFunc(func(context.Context, rundiagnostics.Query) (rundiagnostics.Result, error) {
			return rundiagnostics.Result{}, nil
		}),
		Replay: replayRunnerFunc(func(_ context.Context, replay workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error) {
			replayRequests = append(replayRequests, replay)
			return workflowruntime.BeginReplayResult{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	rerun := appworkflow.RerunWorkflowRequest{SourceRunID: started.Run.ID, RunID: "collecting-replay-target", FromNodeID: plan.Graph.Nodes[0].ID, IdempotencyKey: "collecting-replay-target", Identity: request.Identity}
	if _, err := operator.RerunWorkflow(ctx, rerun); !errors.Is(err, appworkflow.ErrPolicyDenied) || len(replayRequests) != 0 {
		t.Fatalf("unattested collecting replay calls=%d err=%v", len(replayRequests), err)
	}
	rerun.CompensationAttestation = "operator attests the exact collecting ledger"
	if _, err := operator.RerunWorkflow(ctx, rerun); err != nil || len(replayRequests) != 1 {
		t.Fatalf("attested collecting replay calls=%d err=%v", len(replayRequests), err)
	}
	authorization := replayRequests[0].CompensationAuthorization
	if authorization == nil || authorization.LedgerGeneration != ledger.Generation || authorization.LedgerOutcome != "" || authorization.Validate() != nil {
		t.Fatalf("collecting replay authorization = %#v", authorization)
	}
}

func TestHostCompensationPolicyCancelRetryAndReopenRecovery(t *testing.T) {
	plan, effect, undo := compensationHostPlan(t)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	allow := newCompensationHost(t, fixture, effect, undo, func(string) hoststate.PolicyOutcome { return hoststate.PolicyAllow })
	if err := allow.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = allow.Shutdown(t.Context()) })
	ctx := authenticatedContext(t.Context(), "user:compensation-owner")
	request := fixture.startRequest("host-compensation", "host-compensation-start", "user:compensation-owner")
	started, err := allow.StartRun(ctx, request)
	if err != nil || started.Run == nil || started.Run.Status != workflowruntime.RunRunning {
		t.Fatalf("StartRun = %#v, %v", started, err)
	}
	dispatchCompensationNode(t, fixture, plan.Graph.Nodes[0], effect, undo, started.Run.ID, fixture.now.Add(21*time.Second))
	run, err := fixture.state.LoadRun(t.Context(), started.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	succeeded, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunSucceeded, At: fixture.now.Add(23 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}

	denied := newCompensationHost(t, fixture, effect, undo, func(operation string) hoststate.PolicyOutcome {
		if operation == "compensate" {
			return hoststate.PolicyDeny
		}
		return hoststate.PolicyAllow
	})
	manualRequest := appworkflow.CompensateWorkflowRunRequest{RunID: run.ID, Identity: request.Identity, IdempotencyKey: "manual-rollback"}
	if _, err := denied.CompensateWorkflowRun(ctx, manualRequest); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("denied manual compensation = %v", err)
	}
	if ledger, err := fixture.state.LoadCompensationLedger(t.Context(), run.ID); err != nil || ledger.Status != workflowruntime.CompensationCollecting {
		t.Fatalf("denial changed collecting ledger = %#v, %v", ledger, err)
	}

	confirm := newCompensationHost(t, fixture, effect, undo, func(operation string) hoststate.PolicyOutcome {
		if operation == "compensate" {
			return hoststate.PolicyConfirm
		}
		return hoststate.PolicyAllow
	})
	if _, err := confirm.CompensateWorkflowRun(ctx, manualRequest); !errors.Is(err, appworkflow.ErrConfirmationRequired) {
		t.Fatalf("unconfirmed manual compensation = %v", err)
	}
	manualRequest.Confirmed = true
	manual, err := confirm.CompensateWorkflowRun(ctx, manualRequest)
	if err != nil || !manual.Present || manual.Ledger == nil || manual.Ledger.Trigger != graph.CompensationManual || len(manual.Entries) != 1 || manual.Entries[0].Status != workflowruntime.CompensationActive {
		t.Fatalf("confirmed manual compensation = %#v, %v", manual, err)
	}
	entryID := manual.Entries[0].ID

	cancelRequest := appworkflow.CancelWorkflowCompensationRequest{RunID: run.ID, Identity: request.Identity, IdempotencyKey: "cancel-rollback", Reason: "operator canceled rollback"}
	canceled, err := allow.CancelWorkflowCompensation(ctx, cancelRequest)
	if err != nil || canceled.Ledger == nil || canceled.Ledger.Outcome != workflowruntime.CompensationOutcomeCanceled || len(canceled.Entries) != 1 || canceled.Entries[0].Status != workflowruntime.CompensationCanceled {
		t.Fatalf("CancelWorkflowCompensation = %#v, %v", canceled, err)
	}
	manualReplay, err := allow.CompensateWorkflowRun(ctx, manualRequest)
	if err != nil || manualReplay.Ledger == nil || manualReplay.Ledger.Generation != canceled.Ledger.Generation || manualReplay.Ledger.Outcome != workflowruntime.CompensationOutcomeCanceled {
		t.Fatalf("post-progress manual replay = %#v, %v", manualReplay, err)
	}
	cancelReplay, err := allow.CancelWorkflowCompensation(ctx, cancelRequest)
	if err != nil || cancelReplay.Ledger == nil || cancelReplay.Ledger.Generation != canceled.Ledger.Generation || cancelReplay.Ledger.Outcome != workflowruntime.CompensationOutcomeCanceled {
		t.Fatalf("post-progress cancel replay = %#v, %v", cancelReplay, err)
	}
	changedCancel := cancelRequest
	changedCancel.Reason = "different cancellation intent"
	if _, err := allow.CancelWorkflowCompensation(ctx, changedCancel); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed cancel replay = %v", err)
	}
	immutable, err := fixture.state.LoadRun(t.Context(), run.ID)
	if err != nil || immutable.Status != workflowruntime.RunSucceeded || immutable.Generation != succeeded.Snapshot.Generation {
		t.Fatalf("cancellation mutated original run = %#v, %v", immutable, err)
	}
	var replayRequests []workflowruntime.ReplayRequest
	operator, err := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{
		Host: allow,
		Diagnostics: graphInspectorFunc(func(context.Context, rundiagnostics.Query) (rundiagnostics.Result, error) {
			return rundiagnostics.Result{}, nil
		}),
		Replay: replayRunnerFunc(func(_ context.Context, replay workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error) {
			replayRequests = append(replayRequests, replay)
			return workflowruntime.BeginReplayResult{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	rerun := appworkflow.RerunWorkflowRequest{SourceRunID: run.ID, RunID: "compensation-replay-target", FromNodeID: plan.Graph.Nodes[0].ID, IdempotencyKey: "compensation-replay", Identity: request.Identity}
	if _, err := operator.RerunWorkflow(ctx, rerun); !errors.Is(err, appworkflow.ErrPolicyDenied) || len(replayRequests) != 0 {
		t.Fatalf("unattested canceled replay calls=%d err=%v", len(replayRequests), err)
	}
	rerun.CompensationAttestation = strings.Repeat("x", 1025)
	if _, err := operator.RerunWorkflow(ctx, rerun); !errors.Is(err, appworkflow.ErrPolicyDenied) || len(replayRequests) != 0 {
		t.Fatalf("unbounded canceled replay calls=%d err=%v", len(replayRequests), err)
	}
	rerun.CompensationAttestation = "operator attests exact canceled rollback"
	if _, err := operator.RerunWorkflow(ctx, rerun); err != nil || len(replayRequests) != 1 {
		t.Fatalf("attested canceled replay calls=%d err=%v", len(replayRequests), err)
	}
	firstAuthorization := replayRequests[0].CompensationAuthorization
	if firstAuthorization == nil || firstAuthorization.LedgerGeneration != canceled.Ledger.Generation || firstAuthorization.LedgerOutcome != workflowruntime.CompensationOutcomeCanceled || values.ValidateDigest(firstAuthorization.Digest) != nil || strings.Contains(firstAuthorization.Digest, rerun.CompensationAttestation) {
		t.Fatalf("bounded replay authorization = %#v", firstAuthorization)
	}
	rerun.RunID, rerun.IdempotencyKey = "compensation-replay-target-two", "compensation-replay-two"
	if _, err := operator.RerunWorkflow(ctx, rerun); err != nil || len(replayRequests) != 2 || replayRequests[1].CompensationAuthorization == nil || replayRequests[1].CompensationAuthorization.Digest == firstAuthorization.Digest {
		t.Fatalf("target-bound replay authorizations = %#v, err=%v", replayRequests, err)
	}
	if _, err := allow.RetryWorkflowCompensation(ctx, appworkflow.RetryWorkflowCompensationRequest{RunID: run.ID, Identity: request.Identity, IdempotencyKey: "invalid-retry", Attestation: strings.Repeat("x", 1025)}); !errors.Is(err, appworkflow.ErrInvalidHost) {
		t.Fatalf("unbounded retry attestation = %v", err)
	}
	retryRequest := appworkflow.RetryWorkflowCompensationRequest{RunID: run.ID, Identity: request.Identity, IdempotencyKey: "retry-rollback", Attestation: "operator attests the exact indeterminate rollback"}
	retried, err := allow.RetryWorkflowCompensation(ctx, retryRequest)
	if err != nil || retried.Ledger == nil || retried.Ledger.Status != workflowruntime.CompensationFrozen || !retried.Truncated || len(retried.Ledger.Cycles) != 1 || retried.Ledger.Cycles[0].Attestation == "operator attests the exact indeterminate rollback" {
		t.Fatalf("RetryWorkflowCompensation = %#v, %v", retried, err)
	}

	// Simulate a crash after retry authorization but before activation. Host
	// startup must recover the same ledger and publish a new ordinary handler.
	reopenedDB, err := persistence.Open(fixture.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedDB.Close() })
	reopenedState, err := persistence.NewWorkflowStateStore(reopenedDB)
	if err != nil {
		t.Fatal(err)
	}
	reopenedJournal, err := persistence.NewWorkflowHostStore(reopenedDB)
	if err != nil {
		t.Fatal(err)
	}
	reopenedFixture := &hostFixture{
		dbPath: fixture.dbPath, store: reopenedDB, state: reopenedState, journal: reopenedJournal,
		plan: fixture.plan, now: fixture.now, scheduler: fixture.scheduler, artifacts: fixture.artifacts,
	}
	recoveredHost := newCompensationHost(t, reopenedFixture, effect, undo, func(string) hoststate.PolicyOutcome { return hoststate.PolicyAllow })
	if err := recoveredHost.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recoveredHost.Shutdown(t.Context()) })
	entries, err := reopenedState.ListCompensationEntries(t.Context(), run.ID)
	if err != nil || len(entries) != 1 || entries[0].ID != entryID || entries[0].Status != workflowruntime.CompensationActive || len(entries[0].History) != 1 {
		t.Fatalf("recovered retry activation = %#v, %v", entries, err)
	}
	dispatchCompensationNode(t, reopenedFixture, plan.Graph.Nodes[1], effect, undo, run.ID, fixture.now.Add(30*time.Second))

	// A second reopen is the crash point after handler commit but before ledger
	// seal. Production recovery must converge it without a caller replay.
	if err := recoveredHost.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	sealer := newCompensationHost(t, reopenedFixture, effect, undo, func(string) hoststate.PolicyOutcome { return hoststate.PolicyAllow })
	if err := sealer.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sealer.Shutdown(t.Context()) })
	inspected, err := sealer.InspectWorkflowCompensation(ctx, appworkflow.InspectWorkflowCompensationRequest{RunID: run.ID, Identity: request.Identity, Limit: 1})
	if err != nil || !inspected.Present || inspected.Ledger == nil || inspected.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded || len(inspected.Entries) != 1 || inspected.Entries[0].ID != entryID {
		t.Fatalf("recovered terminal inspection = %#v, %v", inspected, err)
	}
	for name, replay := range map[string]func() (appworkflow.WorkflowCompensationResult, error){
		"manual": func() (appworkflow.WorkflowCompensationResult, error) {
			return sealer.CompensateWorkflowRun(ctx, manualRequest)
		},
		"cancel": func() (appworkflow.WorkflowCompensationResult, error) {
			return sealer.CancelWorkflowCompensation(ctx, cancelRequest)
		},
		"retry": func() (appworkflow.WorkflowCompensationResult, error) {
			return sealer.RetryWorkflowCompensation(ctx, retryRequest)
		},
	} {
		replayed, replayErr := replay()
		if replayErr != nil || replayed.Ledger == nil || replayed.Ledger.Generation != inspected.Ledger.Generation || replayed.Ledger.Outcome != workflowruntime.CompensationOutcomeSucceeded {
			t.Fatalf("restart %s replay = %#v, %v", name, replayed, replayErr)
		}
	}
	changedRetry := retryRequest
	changedRetry.Attestation = "operator attests a different rollback state"
	if _, err := sealer.RetryWorkflowCompensation(ctx, changedRetry); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed retry replay = %v", err)
	}
	immutable, err = reopenedState.LoadRun(t.Context(), run.ID)
	if err != nil || immutable.Status != workflowruntime.RunSucceeded || immutable.Generation != succeeded.Snapshot.Generation {
		t.Fatalf("recovery mutated original run = %#v, %v", immutable, err)
	}
}

func compensationHostPlan(t *testing.T) (*workflowcompile.ExecutionPlan, *hostReversibleKind, *stepkindtest.Kind) {
	t.Helper()
	plan := compileHostPlan(t)
	effect := &hostReversibleKind{Kind: stepkindtest.NewNoopKind("host-effect", "v1")}
	effect.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	effect.SpecValue.Compensation = stepkind.CompensationReceiptRequired
	effect.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{"result": mustInline(t, "created")}, Compensation: &stepkind.CompensationReceipt{Operation: "fixture.create", Values: values.ValueSet{}}}, nil
	}
	undo := stepkindtest.NewNoopKind("host-undo", "v1")
	undo.SpecValue.Effects = graph.EffectSet{graph.EffectMutate}
	undo.SpecValue.Idempotency = graph.IdempotencyKeyed
	plan.Graph.Nodes[0].Kind, plan.Graph.Nodes[0].KindVersion, plan.Graph.Nodes[0].Config = "host-effect", "v1", graph.Config{}
	plan.Graph.Nodes[0].Compensation = &graph.CompensationSpec{Handler: "undo"}
	plan.Graph.Nodes = append(plan.Graph.Nodes, graph.Node{ID: "undo", Kind: "host-undo", KindVersion: "v1"})
	plan.Graph.Compensation = &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}, Mode: graph.CompensationBestEffort}
	var err error
	plan.Graph.Digest, err = workflowcompile.GraphDigest(plan.Graph)
	if err != nil {
		t.Fatal(err)
	}
	plan.Digest, err = workflowcompile.PlanDigest(*plan)
	if err != nil {
		t.Fatal(err)
	}
	return plan, effect, undo
}

func newCompensationHost(t *testing.T, fixture *hostFixture, effect *hostReversibleKind, undo *stepkindtest.Kind, outcome func(string) hoststate.PolicyOutcome) *appworkflow.Host {
	t.Helper()
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		principal, ok := ctx.Value(authenticatedPrincipalKey{}).(string)
		if !ok || principal == "" {
			return hoststate.IdentityBinding{}, errors.New("missing authenticated principal")
		}
		return testIdentityBinding(principal, request.SourceAuthority), nil
	})
	policy := appworkflow.PolicyEvaluatorFunc(func(_ context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
		return hoststate.PolicyDecision{Outcome: outcome(facts.Operation), Reason: "compensation fixture policy"}, nil
	})
	host, err := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: fixture.journal, Definitions: definitionProvider{plan: fixture.plan}, Identity: identity, Policy: policy,
		Kinds: []stepkind.StepKind{effect, undo}, RequiredKinds: []appworkflow.KindRef{{Name: "host-effect", Version: "v1"}, {Name: "host-undo", Version: "v1"}},
		Activations: fixture.scheduler, Artifacts: fixture.artifacts, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now.Add(20 * time.Second) }),
		RecoveryInterval: time.Hour, RecoveryBatchLimit: 1, ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func dispatchCompensationNode(t *testing.T, fixture *hostFixture, planNode graph.Node, effect *hostReversibleKind, undo *stepkindtest.Kind, runID workflowruntime.RunID, at time.Time) {
	t.Helper()
	registry := stepkind.NewRegistry()
	if err := registry.Register(effect); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(undo); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := workflowruntime.NewReadyQueueCoordinator(fixture.state, nil).ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{RunID: runID, Owner: "compensation-host-worker", Token: "compensation-host-token-" + planNode.ID, IdempotencyKey: "compensation-host-claim-" + planNode.ID, Now: at, LeaseUntil: at.Add(time.Hour)})
	if err != nil || !ok {
		t.Fatalf("ClaimNext(%s) = %#v, %t, %v", planNode.ID, claim, ok, err)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: fixture.state, Registry: registry, Now: func() time.Time { return at.Add(time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(t.Context(), workflowruntime.DispatchRequest{Claim: claim, Node: planNode, IdempotencyKey: "host-dispatch-" + planNode.ID})
	if err != nil || result.Node.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("Dispatch(%s) = %#v, %v", planNode.ID, result, err)
	}
}
