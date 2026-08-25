package appworkflow_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/artifacts"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	waitadapter "github.com/hollis-labs/hadron/workflow/adapters/wait"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
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
	if inspected.Plan.Source.Available || inspected.Plan.Compile.Available || inspected.Plan.SnapshotDigest == "" || inspected.Binding.Record.Snapshot != nil {
		t.Fatalf("fallback provider inspection metadata = %#v", inspected.Plan)
	}
	queried, err := fixture.host.QueryRun(callerContext, appworkflow.QueryRunRequest{Query: workflowruntime.RunStateQuery{RunID: request.RunID, Limit: 10}, Identity: request.Identity})
	if err != nil || queried.Run.ID != request.RunID || len(queried.Nodes) != 1 {
		t.Fatalf("QueryRun = %#v, %v", queried, err)
	}
	foreignQuery := request.Identity
	foreignQuery.PrincipalHint = "user:other"
	if _, queryErr := fixture.host.QueryRun(authenticatedContext(t.Context(), "user:other"), appworkflow.QueryRunRequest{Query: workflowruntime.RunStateQuery{RunID: request.RunID, Limit: 10}, Identity: foreignQuery}); !errors.Is(queryErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("foreign QueryRun = %v", queryErr)
	}
	explained, err := fixture.host.ExplainRun(t.Context(), request.RunID)
	if err != nil || explained.Decision.ID == "" || explained.DryRunTruth == "" || explained.Plan.SnapshotDigest != inspected.Plan.SnapshotDigest {
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

func TestHostExactPlanSnapshotsSurviveLocatorMutationDeletionAndSQLiteReopen(t *testing.T) {
	root := t.TempDir()
	const privateMarker = "private-source-snapshot-marker"
	source := []byte(`workflow:
  id: locator-snapshot
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: echo
    kind_version: v1
    transform:
      result: inputs.message
    with:
      message: inputs.message
    effects: [compute]
# ` + privateMarker + "\n")
	firstPath := filepath.Join(root, "first.workflow.yaml")
	secondPath := filepath.Join(root, "second.workflow.yaml")
	for _, path := range []string{firstPath, secondPath} {
		if err := os.WriteFile(path, source, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	resolver, resolverErr := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{root}, Authorizer: appworkflow.DefinitionAuthorizerFunc(func(context.Context, appworkflow.DefinitionAuthorization) error { return nil }),
		Compile: appworkflow.DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "plan-snapshot-integration-v1"},
	})
	if resolverErr != nil {
		t.Fatal(resolverErr)
	}
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		principal, _ := ctx.Value(authenticatedPrincipalKey{}).(string)
		if principal == "" {
			return hoststate.IdentityBinding{}, errors.New("missing authenticated principal")
		}
		return testIdentityBinding(principal, request.SourceAuthority), nil
	})
	policy := appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
		return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "snapshot integration allow"}, nil
	})
	newSnapshotHost := func(state workflowruntime.StateStore, journal *persistence.WorkflowHostStore, definitions appworkflow.DefinitionProvider) *appworkflow.Host {
		host, hostErr := appworkflow.New(appworkflow.Options{
			State: state, Journal: journal, Definitions: definitions, Identity: identity, Policy: policy,
			Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
			Activations: fixture.scheduler, Artifacts: fixture.artifacts, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }),
			RecoveryInterval: time.Hour, RecoveryBatchLimit: 10,
			ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
		})
		if hostErr != nil {
			t.Fatal(hostErr)
		}
		return host
	}
	host := newSnapshotHost(fixture.state, fixture.journal, resolver)
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	requests := []appworkflow.StartRunRequest{
		{RunID: "locator-first", IdempotencyKey: "locator-first-start", Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "locator-snapshot", Locator: "first.workflow.yaml", Version: "v1"}, Inputs: map[string]any{"message": "first"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}},
		{RunID: "locator-second", IdempotencyKey: "locator-second-start", Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "locator-snapshot", Locator: "second.workflow.yaml", Version: "v1"}, Inputs: map[string]any{"message": "second"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}},
	}
	plans := make([]hoststate.PlanSnapshotMetadata, 0, 2)
	for _, request := range requests {
		started, err := host.StartRun(authenticatedContext(t.Context(), "user:snapshot"), request)
		if err != nil || started.Run == nil {
			t.Fatalf("StartRun(%s) = %#v, %v", request.RunID, started, err)
		}
		inspected, err := host.InspectRun(t.Context(), request.RunID)
		if err != nil || !inspected.Plan.Source.Available || inspected.Binding.Record.Snapshot != nil {
			t.Fatalf("InspectRun(%s) = %#v, %v", request.RunID, inspected, err)
		}
		plans = append(plans, inspected.Plan)
	}
	if plans[0].Plan.Digest != plans[1].Plan.Digest || plans[0].SnapshotDigest == plans[1].SnapshotDigest || plans[0].Definition.Locator == plans[1].Definition.Locator {
		t.Fatalf("plan/snapshot locator identities = %#v", plans)
	}
	internal, internalErr := fixture.journal.LoadStart(t.Context(), requests[0].RunID)
	if internalErr != nil || internal.Record.Snapshot == nil || !bytes.Contains(internal.Record.Snapshot.Source.Content, []byte(privateMarker)) {
		t.Fatalf("internal exact source = %#v, %v", internal.Record.Snapshot, internalErr)
	}
	encodedInspect, _ := json.Marshal(plans[0])
	if bytes.Contains(encodedInspect, []byte(privateMarker)) {
		t.Fatalf("inspection exposed raw source: %s", encodedInspect)
	}

	firstNode, _ := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: requests[0].RunID, NodeID: "echo"})
	claim, claimErr := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: firstNode.ID, ExpectedClaimGeneration: firstNode.ClaimGeneration, Owner: "snapshot-test", Token: "snapshot-test-token", IdempotencyKey: "snapshot-test-claim", Now: fixture.now.Add(time.Second), LeaseUntil: fixture.now.Add(time.Minute)})
	if claimErr != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("claim exact-plan source node = %#v, %v", claim, claimErr)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	claimedNode, claimedNodeErr := fixture.state.LoadNodeInvocation(t.Context(), firstNode.ID)
	if claimedNodeErr != nil {
		t.Fatal(claimedNodeErr)
	}
	startedAttempt, attemptErr := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: firstNode.ID, ExpectedNodeGeneration: claimedNode.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, Inputs: firstNode.Inputs, At: fixture.now.Add(2 * time.Second)})
	if attemptErr != nil {
		t.Fatal(attemptErr)
	}
	if _, err := fixture.state.FinishNodeAttempt(t.Context(), workflowruntime.FinishNodeAttemptRequest{InvocationID: firstNode.ID, AttemptNumber: startedAttempt.Attempt.ID.Number, ExpectedNodeGeneration: startedAttempt.Node.Generation, ExpectedAttemptGeneration: startedAttempt.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: fixture.now.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	firstRun, _ := fixture.state.LoadRun(t.Context(), requests[0].RunID)
	if _, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: firstRun.ID, ExpectedGeneration: firstRun.Generation, To: workflowruntime.RunSucceeded, At: fixture.now.Add(4 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(firstPath, []byte("modified after accepted start"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{firstPath, secondPath} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	if err := host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, reopenErr := persistence.Open(fixture.dbPath)
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopenedState, _ := persistence.NewWorkflowStateStore(reopenedStore)
	reopenedJournal, _ := persistence.NewWorkflowHostStore(reopenedStore)
	var restartedResolutionCalls atomic.Int32
	restartedKinds := stepkind.NewRegistry()
	_ = restartedKinds.Register(transform.New())
	restartedResolver, restartedResolverErr := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{root}, Authorizer: appworkflow.DefinitionAuthorizerFunc(func(context.Context, appworkflow.DefinitionAuthorization) error {
			restartedResolutionCalls.Add(1)
			return nil
		}), Compile: appworkflow.DefinitionCompileOptions{StepKinds: restartedKinds, SemanticRevision: "plan-snapshot-integration-v1"},
	})
	if restartedResolverErr != nil {
		t.Fatal(restartedResolverErr)
	}
	restarted := newSnapshotHost(reopenedState, reopenedJournal, restartedResolver)
	if err := restarted.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Shutdown(context.Background()) })
	for index, request := range requests {
		inspected, err := restarted.InspectRun(t.Context(), request.RunID)
		if err != nil || inspected.Plan.SnapshotDigest != plans[index].SnapshotDigest || inspected.Plan.Definition.Locator != plans[index].Definition.Locator {
			t.Fatalf("reopened InspectRun(%s) = %#v, %v", request.RunID, inspected.Plan, err)
		}
		explained, err := restarted.ExplainRun(t.Context(), request.RunID)
		if err != nil || explained.Plan.SnapshotDigest != plans[index].SnapshotDigest {
			t.Fatalf("reopened ExplainRun(%s) = %#v, %v", request.RunID, explained, err)
		}
		replayed, err := restarted.StartRun(authenticatedContext(t.Context(), "user:snapshot"), request)
		if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed {
			t.Fatalf("reopened StartRun replay(%s) = %#v, %v", request.RunID, replayed, err)
		}
	}
	if restartedResolutionCalls.Load() != 0 {
		t.Fatalf("reopened inspection/start replay re-resolved deleted source %d times", restartedResolutionCalls.Load())
	}
	if _, err := restarted.StartRun(authenticatedContext(t.Context(), "user:foreign"), requests[0]); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("unauthorized durable start replay = %v", err)
	}

	recoveryPlans := appworkflow.PinnedRecoveryPlanSource{Roots: reopenedJournal, Children: reopenedJournal, State: reopenedState, Replays: reopenedState}
	replayService := &workflowruntime.ReplayService{
		Store: reopenedState, Replay: reopenedState, Inputs: reopenedState, Control: reopenedState, Plans: recoveryPlans, Registry: restartedKinds,
		Policy: workflowruntime.RepeatPolicyFunc(func(context.Context, workflowruntime.RepeatCandidate) (workflowruntime.RepeatPolicyDecision, error) {
			return workflowruntime.RepeatPolicyDecision{Allow: true, Code: "snapshot_test_approved", Reason: "test operator approved exact-plan rerun"}, nil
		}),
	}
	operator, operatorErr := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{
		Host: restarted, Diagnostics: graphInspectorFunc(func(context.Context, rundiagnostics.Query) (rundiagnostics.Result, error) {
			return rundiagnostics.Result{}, nil
		}), Replay: replayService,
	})
	if operatorErr != nil {
		t.Fatal(operatorErr)
	}
	if _, err := operator.RerunWorkflow(authenticatedContext(t.Context(), "user:foreign"), appworkflow.RerunWorkflowRequest{SourceRunID: requests[0].RunID, RunID: "locator-rerun-denied", FromNodeID: "echo", IdempotencyKey: "locator-rerun-denied", Identity: requests[0].Identity}); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("unauthorized exact-plan rerun = %v", err)
	}
	rerun, rerunErr := operator.RerunWorkflow(authenticatedContext(t.Context(), "user:snapshot"), appworkflow.RerunWorkflowRequest{SourceRunID: requests[0].RunID, RunID: "locator-rerun", FromNodeID: "echo", IdempotencyKey: "locator-rerun", Identity: requests[0].Identity})
	if rerunErr != nil || rerun.Outcome != workflowruntime.IdempotencyApplied || rerun.Run.Plan.Digest != plans[0].Plan.Digest || restartedResolutionCalls.Load() != 0 {
		t.Fatalf("authorized exact-plan rerun = %#v, %v resolutions=%d", rerun, rerunErr, restartedResolutionCalls.Load())
	}
}

func TestHostQueryRunReturnsOnlyTransportSafeProjection(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	start := fixture.startRequest("safe-query-run", "safe-query-start", "user:safe-query")
	ctx := authenticatedContext(t.Context(), "user:safe-query")
	if _, err := fixture.host.StartRun(ctx, start); err != nil {
		t.Fatal(err)
	}
	id := workflowruntime.NodeInvocationID{RunID: start.RunID, NodeID: fixture.plan.Graph.Nodes[0].ID}
	node, err := fixture.state.LoadNodeInvocation(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: id, ExpectedClaimGeneration: node.ClaimGeneration,
		Owner: "private-lease-owner", Token: "private-lease-token", IdempotencyKey: "safe-query-claim", Now: fixture.now, LeaseUntil: fixture.now.Add(time.Hour)})
	if err != nil || !claimed.Acquired || claimed.Lease == nil {
		t.Fatalf("ClaimNode = %#v, %v", claimed, err)
	}
	if _, appendErr := fixture.state.AppendEvent(t.Context(), workflowruntime.AppendEventRequest{RunID: start.RunID, Invocation: &id,
		Type: "private.event", OccurredAt: fixture.now, Attributes: map[string]string{"password": "private-event-secret"},
		Redaction: values.RedactionSecret, Retention: values.RetentionRun}); appendErr != nil {
		t.Fatal(appendErr)
	}
	queried, err := fixture.host.QueryRun(ctx, appworkflow.QueryRunRequest{Query: workflowruntime.RunStateQuery{RunID: start.RunID, Limit: 100}, Identity: start.Identity})
	if err != nil || len(queried.Nodes) != 1 || len(queried.Events) == 0 {
		t.Fatalf("QueryRun = %#v, %v", queried, err)
	}
	assertSafeQueryJSON(t, queried, "private-lease-owner", "private-lease-token", "private-event-secret", "password")

	proof := workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}
	claimedNode, err := fixture.state.LoadNodeInvocation(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	started, err := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: id,
		ExpectedNodeGeneration: claimedNode.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "transform", Version: "v1",
			Target: "private-target", Attributes: map[string]string{"credential": "private-executor-secret"}}, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{})
	if err != nil {
		t.Fatal(err)
	}
	resumeTokenDigest, err := workflowwait.DigestToken("private-resume-token")
	if err != nil {
		t.Fatal(err)
	}
	wait := workflowwait.Record{Kind: workflowwait.KindSignal, SignalName: "safe.signal", Correlation: "private-correlation",
		ResumeSchema: schema, ResumeTokenDigest: resumeTokenDigest, ResumeURL: "https://example.test/resume/safe",
		Visibility: workflowwait.VisibilitySecret, Authority: workflowwait.ResponderAuthority{Kind: "private-authority", Reference: "private-authority-reference",
			Attributes: map[string]string{"credential": "private-authority-secret"}}, WakeSource: workflowwait.WakeSignal, Status: workflowwait.StatusOpen}
	if _, suspendErr := (workflowruntime.WaitCoordinator{Store: fixture.state}).Suspend(t.Context(), workflowruntime.SuspendCommand{Request: workflowruntime.SuspendNodeWaitRequest{
		Wait:                   workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: "safe-query-wait"}, Invocation: id, Record: wait},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, At: fixture.now}, ResumeToken: "private-resume-token"}); suspendErr != nil {
		t.Fatal(suspendErr)
	}
	queried, err = fixture.host.QueryRun(ctx, appworkflow.QueryRunRequest{Query: workflowruntime.RunStateQuery{RunID: start.RunID, Limit: 100}, Identity: start.Identity})
	if err != nil || len(queried.Waits) != 1 || len(queried.Attempts) != 1 {
		t.Fatalf("QueryRun with wait = %#v, %v", queried, err)
	}
	assertSafeQueryJSON(t, queried, "private-correlation", "private-resume-token", resumeTokenDigest, "private-authority", "private-authority-reference",
		"private-authority-secret", "private-target", "private-executor-secret", "private-event-secret", "password")
}

func assertSafeQueryJSON(t *testing.T, view appworkflow.RunQueryView, forbidden ...string) {
	t.Helper()
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range forbidden {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("transport-safe query JSON exposed %q: %s", secret, encoded)
		}
	}
}

func TestHostRecoversChildTerminalThroughCanonicalWaitResume(t *testing.T) {
	t.Run("inline output and restart replay", func(t *testing.T) {
		fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
		wait, child := seedHostTerminalChildWait(t, fixture, "inline", values.ValueSet{"result": mustInline(t, "done")})
		if err := fixture.host.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		assertChildTerminalWait(t, fixture, wait.Ref.ID, "succeeded", "done")
		if err := fixture.host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		restarted := hostWithFixedIdentity(t, fixture, testIdentityBinding("seed", "test"))
		if err := restarted.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := restarted.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		assertSingleChildResume(t, fixture, wait.Ref.ID, child.ID)
	})

	t.Run("secret output converges to safe failure", func(t *testing.T) {
		fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
		ref, err := values.ParseSecretRef("secret://project/child-output")
		if err != nil {
			t.Fatal(err)
		}
		secret, err := values.NewSecretRef(ref, values.Metadata{Producer: values.Producer{Kind: "child", Reference: "secret"},
			MediaType: "text/plain", Redaction: values.RedactionSecret, Retention: values.RetentionRun})
		if err != nil {
			t.Fatal(err)
		}
		wait, child := seedHostTerminalChildWait(t, fixture, "secret", values.ValueSet{"result": secret})
		if err := fixture.host.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		assertChildTerminalWait(t, fixture, wait.Ref.ID, "child_output_not_inline", "")
		if err := fixture.host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		restarted := hostWithFixedIdentity(t, fixture, testIdentityBinding("seed", "test"))
		if err := restarted.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := restarted.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		assertSingleChildResume(t, fixture, wait.Ref.ID, child.ID)
	})

	t.Run("already resumed candidate is not duplicated", func(t *testing.T) {
		fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
		wait, child := seedHostTerminalChildWait(t, fixture, "already", values.ValueSet{"result": mustInline(t, "done")})
		payload := mustInline(t, map[string]any{"status": string(waitadapter.ChildRunSucceeded), "outputs": map[string]any{"result": "done"}})
		resumed, err := (workflowruntime.WaitCoordinator{Store: fixture.state}).Resume(t.Context(), workflowruntime.ResumeCommand{
			WaitID: wait.Ref.ID, Correlation: string(child.ID), WakeSource: workflowwait.WakeChildRun,
			Responder: workflowwait.Responder{Kind: "child_run", Reference: string(child.ID)}, Payload: payload,
			IdempotencyKey: "child-terminal:" + string(child.ID), ReceivedAt: fixture.now,
		})
		if err != nil || resumed.Outcome != workflowruntime.ResumeApplied {
			t.Fatalf("pre-recovery Resume = %#v, %v", resumed, err)
		}
		if err := fixture.host.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := fixture.host.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		assertSingleChildResume(t, fixture, wait.Ref.ID, child.ID)
	})

	t.Run("ambiguous open waits fail before limit one resumes either", func(t *testing.T) {
		fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
		wait, _ := seedHostTerminalChildWait(t, fixture, "ambiguous", values.ValueSet{"result": mustInline(t, "done")})
		duplicateID := workflowruntime.WaitID("child-wait-ambiguous-duplicate")
		if _, err := fixture.store.DB().Exec(`INSERT INTO workflow_waits(
wait_id,run_id,node_id,iteration,status,resume_values_ref_json,generation,created_at,updated_at,resolved_at,record_json,deadline)
SELECT ?,run_id,node_id,iteration,status,resume_values_ref_json,generation,created_at,updated_at,resolved_at,record_json,deadline
FROM workflow_waits WHERE wait_id=?`, duplicateID, wait.Ref.ID); err != nil {
			t.Fatal(err)
		}
		if err := fixture.host.Start(t.Context()); err == nil || !strings.Contains(err.Error(), "ambiguous open waits") {
			t.Fatalf("ambiguous child recovery = %v", err)
		}
		for _, waitID := range []workflowruntime.WaitID{wait.Ref.ID, duplicateID} {
			loaded, err := fixture.state.LoadWait(t.Context(), waitID)
			if err != nil || loaded.Status != workflowruntime.WaitOpen || loaded.Generation != 1 || loaded.ResumeValues != nil {
				t.Fatalf("ambiguous wait %s changed = %#v, %v", waitID, loaded, err)
			}
			var resumeRows int
			if err := fixture.store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_wait_resume_results WHERE wait_id=?`, waitID).Scan(&resumeRows); err != nil {
				t.Fatal(err)
			}
			if resumeRows != 0 {
				t.Fatalf("ambiguous wait %s recorded %d resume rows", waitID, resumeRows)
			}
		}
	})
}

func seedHostTerminalChildWait(t *testing.T, fixture *hostFixture, suffix string, outputs values.ValueSet) (workflowruntime.WaitSnapshot, workflowruntime.RunSnapshot) {
	t.Helper()
	parent := seedRunningCallParent(t, fixture)
	parentRun, err := fixture.state.LoadRun(t.Context(), parent.Node.ID.RunID)
	if err != nil {
		t.Fatal(err)
	}
	childID := workflowruntime.RunID("terminal-child-" + suffix)
	child, _, err := fixture.state.CreateRun(t.Context(), workflowruntime.CreateRunRequest{ID: childID, Plan: parentRun.Plan,
		Status: workflowruntime.RunPending, StartIdempotencyKey: "terminal-child-start-" + suffix, CreatedAt: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	running, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: child.ID,
		ExpectedGeneration: child.Generation, To: workflowruntime.RunRunning, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	outputRef, err := fixture.state.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "run_outputs", RunID: child.ID}, Values: outputs})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: child.ID,
		ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunSucceeded, Outputs: &outputRef, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	if recordErr := fixture.state.RecordChildRun(t.Context(), workflowruntime.ChildRunLink{ParentRunID: parent.Node.ID.RunID, Invocation: parent.Node.ID,
		ChildRunID: child.ID, Policy: graph.ParentCloseCancel, CreatedAt: fixture.now}); recordErr != nil {
		t.Fatal(recordErr)
	}
	if parent.Node.Lease == nil {
		t.Fatal("running parent lost its claim before suspension")
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{"type": "object"})
	if err != nil {
		t.Fatal(err)
	}
	record := workflowwait.Record{Kind: workflowwait.KindChildRun, Correlation: string(child.ID), ResumeSchema: schema,
		Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "child_run", Reference: string(child.ID)},
		WakeSource: workflowwait.WakeChildRun, Status: workflowwait.StatusOpen}
	suspended, err := (workflowruntime.WaitCoordinator{Store: fixture.state}).Suspend(t.Context(), workflowruntime.SuspendCommand{Request: workflowruntime.SuspendNodeWaitRequest{
		Wait:                   workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: workflowruntime.WaitID("child-wait-" + suffix)}, Invocation: parent.Node.ID, Record: record},
		ExpectedNodeGeneration: parent.Node.Generation, ExpectedAttemptGeneration: parent.Attempt.Generation,
		Claim: workflowruntime.ClaimProof{Owner: parent.Node.Lease.Owner, Token: parent.Node.Lease.Token, Generation: parent.Node.Lease.Generation}, At: fixture.now}})
	if err != nil {
		t.Fatal(err)
	}
	return suspended.Wait, completed.Snapshot
}

func assertChildTerminalWait(t *testing.T, fixture *hostFixture, waitID workflowruntime.WaitID, contains, output string) {
	t.Helper()
	wait, err := fixture.state.LoadWait(t.Context(), waitID)
	if err != nil || wait.Status != workflowruntime.WaitResumed || wait.Resolution == nil || wait.Resolution.Source != workflowwait.WakeChildRun || wait.ResumeValues == nil {
		t.Fatalf("child terminal wait = %#v, %v", wait, err)
	}
	set, err := fixture.state.LoadValues(t.Context(), *wait.ResumeValues)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(set[workflowruntime.ResumeValueName].Inline)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), contains) || output != "" && !strings.Contains(string(encoded), output) || strings.Contains(string(encoded), "secret://") {
		t.Fatalf("child terminal payload = %s", encoded)
	}
}

func assertSingleChildResume(t *testing.T, fixture *hostFixture, waitID workflowruntime.WaitID, childID workflowruntime.RunID) {
	t.Helper()
	var resultCount, idempotencyCount int
	if err := fixture.store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_wait_resume_results WHERE wait_id=?`, waitID).Scan(&resultCount); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_wait_resume_idempotency WHERE idempotency_key=?`, "child-terminal:"+string(childID)).Scan(&idempotencyCount); err != nil {
		t.Fatal(err)
	}
	if resultCount != 1 || idempotencyCount != 1 {
		t.Fatalf("child resume counts result=%d idempotency=%d", resultCount, idempotencyCount)
	}
}

func TestHostNamedSignalAndTrackedUpdateAuthorizeReplayAndConflict(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })

	start := fixture.startRequest("signal-run", "signal-start", "user:signal")
	owner := authenticatedContext(t.Context(), "user:signal")
	if _, err := fixture.host.StartRun(owner, start); err != nil {
		t.Fatal(err)
	}
	suspendHostNamedSignal(t, fixture, start.RunID, "signal-wait", "project.changed", "project-1")
	payload := mustInline(t, map[string]any{"sequence": 1})
	signal := appworkflow.SignalRunRequest{Selector: workflowruntime.SignalSelector{RunID: start.RunID, Name: "project.changed", Correlation: "project-1"}, Payload: payload,
		IdempotencyKey: "signal-delivery", Identity: start.Identity}
	foreign := signal
	foreign.Identity.PrincipalHint = "user:other"
	if _, err := fixture.host.SignalRun(authenticatedContext(t.Context(), "user:other"), foreign); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("foreign SignalRun = %v", err)
	}
	applied, err := fixture.host.SignalRun(owner, signal)
	if err != nil || applied.Outcome != workflowruntime.ResumeApplied || applied.Wait.Status != workflowruntime.WaitResumed {
		t.Fatalf("SignalRun = %#v, %v", applied, err)
	}
	replayed, err := fixture.host.SignalRun(owner, signal)
	if err != nil || replayed.Outcome != workflowruntime.ResumeReplayed || replayed.Wait.Ref.ID != applied.Wait.Ref.ID {
		t.Fatalf("SignalRun replay = %#v, %v", replayed, err)
	}
	conflict := signal
	conflict.Payload = mustInline(t, map[string]any{"sequence": 2})
	if _, conflictErr := fixture.host.SignalRun(owner, conflict); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("SignalRun conflict = %v", conflictErr)
	}

	updateStart := fixture.startRequest("update-run", "update-start", "user:update")
	updateOwner := authenticatedContext(t.Context(), "user:update")
	if _, startErr := fixture.host.StartRun(updateOwner, updateStart); startErr != nil {
		t.Fatal(startErr)
	}
	suspendHostNamedSignal(t, fixture, updateStart.RunID, "update-wait", "review.completed", "review-1")
	update := appworkflow.UpdateRunRequest{Selector: workflowruntime.SignalSelector{RunID: updateStart.RunID, Name: "review.completed", Correlation: "review-1"}, Payload: mustInline(t, "accepted"),
		IdempotencyKey: "tracked-update", Identity: updateStart.Identity}
	foreignUpdate := update
	foreignUpdate.Identity.PrincipalHint = "user:other"
	if _, foreignErr := fixture.host.UpdateRun(authenticatedContext(t.Context(), "user:other"), foreignUpdate); !errors.Is(foreignErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("foreign UpdateRun = %v", foreignErr)
	}
	updated, err := fixture.host.UpdateRun(updateOwner, update)
	if err != nil || updated.Status != workflowruntime.RunUpdateApplied || updated.Receipt == nil || updated.Receipt.Outcome != workflowruntime.ResumeApplied {
		t.Fatalf("UpdateRun = %#v, %v", updated, err)
	}
	updateReplay, err := fixture.host.UpdateRun(updateOwner, update)
	if err != nil || updateReplay.Status != workflowruntime.RunUpdateApplied || updateReplay.Generation != updated.Generation {
		t.Fatalf("UpdateRun replay = %#v, %v", updateReplay, err)
	}
	updateConflict := update
	updateConflict.Payload = mustInline(t, "changed")
	if _, err := fixture.host.UpdateRun(updateOwner, updateConflict); !errors.Is(err, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("UpdateRun conflict = %v", err)
	}
}

func TestHostTrackedUpdateClampsRegressedClockToSelectedWait(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := fixture.startRequest("update-clock-run", "update-clock-start", "user:update-clock")
	ctx := authenticatedContext(t.Context(), "user:update-clock")
	if _, err := fixture.host.StartRun(ctx, request); err != nil {
		t.Fatal(err)
	}
	wait := suspendHostNamedSignal(t, fixture, request.RunID, "update-clock-wait", "clock.updated", "clock-1")
	if shutdownErr := fixture.host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	regressed := hostWithFixedIdentityClock(t, fixture, testIdentityBinding("user:update-clock", request.Identity.SourceAuthority),
		appworkflow.ClockFunc(func() time.Time { return wait.UpdatedAt.Add(-time.Minute) }))
	if err := regressed.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = regressed.Shutdown(context.Background()) })
	updated, err := regressed.UpdateRun(ctx, appworkflow.UpdateRunRequest{Selector: workflowruntime.SignalSelector{RunID: request.RunID,
		Name: "clock.updated", Correlation: "clock-1"}, Payload: mustInline(t, "accepted"), IdempotencyKey: "update-clock-key", Identity: request.Identity})
	if err != nil || updated.Status != workflowruntime.RunUpdateApplied || !updated.Request.ReceivedAt.Equal(wait.UpdatedAt) {
		t.Fatalf("regressed-clock UpdateRun = %#v, %v", updated, err)
	}
	pending, err := fixture.state.RecoverRunUpdates(t.Context(), 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("regressed clock left unrecoverable update intents = %#v, %v", pending, err)
	}
}

func TestHostSignalRunClampsRegressedClockToSelectedWait(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if startErr := fixture.host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	request := fixture.startRequest("signal-clock-run", "signal-clock-start", "user:signal-clock")
	ctx := authenticatedContext(t.Context(), "user:signal-clock")
	if _, startErr := fixture.host.StartRun(ctx, request); startErr != nil {
		t.Fatal(startErr)
	}
	wait := suspendHostNamedSignal(t, fixture, request.RunID, "signal-clock-wait", "clock.signaled", "clock-signal-1")
	if shutdownErr := fixture.host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	regressed := hostWithFixedIdentityClock(t, fixture, testIdentityBinding("user:signal-clock", request.Identity.SourceAuthority),
		appworkflow.ClockFunc(func() time.Time { return wait.UpdatedAt.Add(-time.Minute) }))
	if startErr := regressed.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = regressed.Shutdown(context.Background()) })
	resumed, signalErr := regressed.SignalRun(ctx, appworkflow.SignalRunRequest{Selector: workflowruntime.SignalSelector{RunID: request.RunID,
		Name: "clock.signaled", Correlation: "clock-signal-1"}, Payload: mustInline(t, "accepted"), IdempotencyKey: "signal-clock-key", Identity: request.Identity})
	if signalErr != nil || resumed.Outcome != workflowruntime.ResumeApplied || !resumed.Wait.ResolvedAt.Equal(wait.UpdatedAt) {
		t.Fatalf("regressed-clock SignalRun = %#v, %v", resumed, signalErr)
	}
}

func TestHostRecoveryIgnoresWaitObserverFailureAfterTrackedUpdateSeals(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := fixture.startRequest("update-post-commit-run", "update-post-commit-start", "user:update-post-commit")
	ctx := authenticatedContext(t.Context(), "user:update-post-commit")
	if _, err := fixture.host.StartRun(ctx, request); err != nil {
		t.Fatal(err)
	}
	wait := suspendHostNamedSignal(t, fixture, request.RunID, "update-post-commit-wait", "observer.updated", "observer-1")
	payload := mustInline(t, "accepted")
	identity := testIdentityBinding("user:update-post-commit", request.Identity.SourceAuthority)
	responder := workflowwait.Responder{Kind: "hadron_identity", Reference: identity.Principal,
		Attributes: map[string]string{"source_authority": identity.SourceAuthority, "trust": identity.Trust}}
	pending, _, err := fixture.state.BeginRunUpdate(t.Context(), workflowruntime.BeginRunUpdateRequest{IdempotencyKey: "update-post-commit-key",
		Selector: workflowruntime.SignalSelector{RunID: request.RunID, Name: "observer.updated", Correlation: "observer-1"},
		WaitID:   wait.Ref.ID, Responder: responder, Payload: payload, ReceivedAt: wait.UpdatedAt})
	if err != nil || pending.Status != workflowruntime.RunUpdatePending {
		t.Fatalf("seed pending tracked update = %#v, %v", pending, err)
	}
	if shutdownErr := fixture.host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	restarted := hostWithFixedIdentityClock(t, fixture, identity, appworkflow.ClockFunc(func() time.Time { return fixture.now.Add(time.Minute) }),
		failingWaitMaterializer{err: errors.New("observer unavailable")})
	if startErr := restarted.Start(t.Context()); startErr != nil {
		t.Fatalf("recover post-commit tracked update = %v", startErr)
	}
	if shutdownErr := restarted.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	sealed, err := fixture.state.LoadRunUpdate(t.Context(), pending.Request.IdempotencyKey)
	resumed, waitErr := fixture.state.LoadWait(t.Context(), wait.Ref.ID)
	if err != nil || waitErr != nil || sealed.Status != workflowruntime.RunUpdateApplied || sealed.Receipt == nil ||
		resumed.Status != workflowruntime.WaitResumed || resumed.Resolution == nil || resumed.Resolution.IdempotencyKey != pending.Request.IdempotencyKey {
		t.Fatalf("post-commit tracked update convergence sealed=%#v wait=%#v errors=%v/%v", sealed, resumed, err, waitErr)
	}
}

func suspendHostNamedSignal(t *testing.T, fixture *hostFixture, runID workflowruntime.RunID, waitID workflowruntime.WaitID, name, correlation string) workflowruntime.WaitSnapshot {
	t.Helper()
	nodeID := workflowruntime.NodeInvocationID{RunID: runID, NodeID: fixture.plan.Graph.Nodes[0].ID}
	node, err := fixture.state.LoadNodeInvocation(t.Context(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: nodeID, ExpectedClaimGeneration: node.ClaimGeneration,
		Owner: "signal-worker", Token: "claim-" + string(waitID), IdempotencyKey: "claim-" + string(waitID), Now: fixture.now, LeaseUntil: fixture.now.Add(time.Hour)})
	if err != nil || !claimed.Acquired || claimed.Lease == nil {
		t.Fatalf("ClaimNode = %#v, %v", claimed, err)
	}
	proof := workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}
	claimedNode, err := fixture.state.LoadNodeInvocation(t.Context(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	started, err := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: nodeID, ExpectedNodeGeneration: claimedNode.Generation,
		Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: "transform", Version: "v1"}, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(graph.Schema{})
	if err != nil {
		t.Fatal(err)
	}
	wait := workflowwait.Record{Kind: workflowwait.KindSignal, SignalName: name, Correlation: correlation, ResumeSchema: schema,
		Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "policy", Reference: "run-owner"}, WakeSource: workflowwait.WakeSignal, Status: workflowwait.StatusOpen}
	suspended, err := (workflowruntime.WaitCoordinator{Store: fixture.state}).Suspend(t.Context(), workflowruntime.SuspendCommand{Request: workflowruntime.SuspendNodeWaitRequest{
		Wait: workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: waitID}, Invocation: nodeID, Record: wait}, ExpectedNodeGeneration: started.Node.Generation,
		ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, At: fixture.now}})
	if err != nil {
		t.Fatal(err)
	}
	return suspended.Wait
}

func TestHostDurabilityNoneUsesOrdinaryStartWithTypedOutputParityAndBoundedAudit(t *testing.T) {
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, compileNonDurableHostPlan(t))
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("non-durable-run", "non-durable-key", "user:none")
	ctx := authenticatedContext(t.Context(), "user:none")
	started, err := fixture.host.StartRun(ctx, request)
	if err != nil || started.Run == nil || started.Run.Status != workflowruntime.RunSucceeded || started.Durability != graph.DurabilityNone || started.Outputs["echo"].Inline != "hello" || len(started.InspectionLimitations) == 0 {
		t.Fatalf("StartRun durability none = %#v, %v", started, err)
	}
	secretRef, err := values.ParseSecretRef("secret://project/non-durable#token")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := values.NewSecretRef(secretRef, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "non-durable-secret"},
		MediaType: "text/plain", Redaction: values.RedactionSecret, Retention: values.RetentionProject})
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := values.NewArtifact(values.ArtifactRef{Store: "external", URI: "artifact://private/non-durable-output",
		Digest: values.SHA256Digest([]byte("artifact payload")), MediaType: "application/octet-stream", SizeBytes: 16,
		Producer: values.Producer{Kind: "test", Reference: "non-durable-artifact"}, Redaction: values.RedactionPrivate, Retention: values.RetentionExternal})
	if err != nil {
		t.Fatal(err)
	}
	transportValues := values.ValueSet{"echo": started.Outputs["echo"], "secret": secret, "artifact": artifact}
	started.Outputs = transportValues
	started.RenderedOutputs, err = values.RenderValueSet(transportValues, values.DisplayPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	encodedStart, err := json.Marshal(started)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"hello", "secret://project/non-durable", "artifact://private/non-durable-output"} {
		if strings.Contains(string(encodedStart), forbidden) {
			t.Fatalf("non-durable start JSON exposed %q: %s", forbidden, encodedStart)
		}
	}
	if !strings.Contains(string(encodedStart), values.RedactedMarker) || !strings.Contains(string(encodedStart), `"outputs"`) {
		t.Fatalf("non-durable start JSON omitted masked output projection: %s", encodedStart)
	}
	if _, loadErr := fixture.state.LoadRun(t.Context(), request.RunID); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		t.Fatalf("durability none wrote durable runtime run: %v", loadErr)
	}
	audit, err := fixture.journal.LoadNonDurableStart(t.Context(), request.RunID)
	if err != nil || audit.Outputs["echo"].Inline != started.Outputs["echo"].Inline {
		t.Fatalf("non-durable audit = %#v, %v", audit, err)
	}
	replayed, err := fixture.host.StartRun(ctx, request)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Outputs["echo"].Inline != "hello" {
		t.Fatalf("non-durable replay = %#v, %v", replayed, err)
	}
	queried, err := fixture.host.QueryRun(ctx, appworkflow.QueryRunRequest{Query: workflowruntime.RunStateQuery{RunID: request.RunID, Limit: 10}, Identity: request.Identity})
	if err != nil || queried.Run.ID != request.RunID || !queried.Run.HasOutputs || len(queried.Nodes) != 0 || len(queried.Events) != 0 {
		t.Fatalf("non-durable query = %#v, %v", queried, err)
	}
	assertSafeQueryJSON(t, queried, "hello", "secret://", "artifact://")
}

func TestHostAutomaticallyStartsExactFailureHandlerAndDurablyFencesRecursion(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := fixture.startRequest("failure-source", "failure-source-key", "user:failure")
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:failure"), request); err != nil {
		t.Fatal(err)
	}
	run, _ := fixture.state.LoadRun(t.Context(), request.RunID)
	failed, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunFailed, At: fixture.now.Add(time.Second)})
	if err != nil || failed.Snapshot.Status != workflowruntime.RunFailed {
		t.Fatalf("fail source run = %#v, %v", failed, err)
	}
	if shutdownErr := fixture.host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}

	handlerPlan := compileFailureHandlerPlan(t)
	provider := mapDefinitionProvider{plans: map[string]*workflowcompile.ExecutionPlan{fixture.plan.Digest: fixture.plan, fixture.plan.Definition.Digest: fixture.plan, handlerPlan.Digest: handlerPlan, handlerPlan.Definition.Digest: handlerPlan}}
	newHost := func() *appworkflow.Host {
		host, hostErr := appworkflow.New(appworkflow.Options{State: fixture.state, Journal: fixture.journal, Definitions: provider,
			Identity: identityProviderFunc(func(_ context.Context, identity appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
				return testIdentityBinding(identity.PrincipalHint, identity.SourceAuthority), nil
			}),
			Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
				return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "allow failure handler"}, nil
			}),
			Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}}, Activations: fixture.scheduler,
			Artifacts: fixture.artifacts, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now.Add(2 * time.Second) }), RecoveryInterval: time.Hour, RecoveryBatchLimit: 10,
			ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }), OnRunFailed: &appworkflow.FailureHandlerConfig{Definition: handlerPlan.Definition, MaximumDepth: 1}})
		if hostErr != nil {
			t.Fatal(hostErr)
		}
		return host
	}
	restarted := newHost()
	if startErr := restarted.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	handlerRunID := testFailureHandlerRunID(request.RunID, handlerPlan.Definition.Digest)
	handlerStart, err := fixture.journal.LoadStart(t.Context(), handlerRunID)
	if err != nil || handlerStart.Record.Requested.Digest != handlerPlan.Definition.Digest || handlerStart.Record.Facts.Plan.Digest != handlerPlan.Digest {
		t.Fatalf("failure handler start = %#v, %v", handlerStart, err)
	}
	handlerRun, _ := fixture.state.LoadRun(t.Context(), handlerRunID)
	if _, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: handlerRun.ID, ExpectedGeneration: handlerRun.Generation, To: workflowruntime.RunFailed, At: fixture.now.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	third := newHost()
	if err := third.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = third.Shutdown(context.Background()) })
	recursiveRunID := testFailureHandlerRunID(handlerRunID, handlerPlan.Definition.Digest)
	if _, err := fixture.journal.LoadStart(t.Context(), recursiveRunID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("recursive handler was not suppressed: %v", err)
	}
}

func TestHostFailureHandlerUsesNormalDenyAndConfirmationPolicy(t *testing.T) {
	for _, outcome := range []hoststate.PolicyOutcome{hoststate.PolicyDeny, hoststate.PolicyConfirm} {
		t.Run(string(outcome), func(t *testing.T) {
			fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
			if err := fixture.host.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			request := fixture.startRequest("failure-policy-"+string(outcome), "failure-policy-key-"+string(outcome), "user:failure-policy")
			if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:failure-policy"), request); err != nil {
				t.Fatal(err)
			}
			run, err := fixture.state.LoadRun(t.Context(), request.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunFailed, At: fixture.now.Add(time.Second)}); err != nil {
				t.Fatal(err)
			}
			if err := fixture.host.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}

			handlerPlan := compileFailureHandlerPlan(t)
			provider := mapDefinitionProvider{plans: map[string]*workflowcompile.ExecutionPlan{fixture.plan.Digest: fixture.plan, fixture.plan.Definition.Digest: fixture.plan, handlerPlan.Digest: handlerPlan, handlerPlan.Definition.Digest: handlerPlan}}
			newHost := func(policyOutcome hoststate.PolicyOutcome) *appworkflow.Host {
				host, hostErr := appworkflow.New(appworkflow.Options{State: fixture.state, Journal: fixture.journal, Definitions: provider,
					Identity: identityProviderFunc(func(_ context.Context, identity appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
						return testIdentityBinding(identity.PrincipalHint, identity.SourceAuthority), nil
					}),
					Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
						return hoststate.PolicyDecision{Outcome: policyOutcome, Reason: "failure policy fixture"}, nil
					}),
					Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}}, Activations: fixture.scheduler,
					Artifacts: fixture.artifacts, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now.Add(2 * time.Second) }), RecoveryInterval: time.Hour, RecoveryBatchLimit: 10,
					ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }), OnRunFailed: &appworkflow.FailureHandlerConfig{Definition: handlerPlan.Definition, MaximumDepth: 2}})
				if hostErr != nil {
					t.Fatal(hostErr)
				}
				return host
			}
			denied := newHost(outcome)
			if err := denied.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := denied.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			handlerRunID := testFailureHandlerRunID(request.RunID, handlerPlan.Definition.Digest)
			if _, err := fixture.journal.LoadStart(t.Context(), handlerRunID); !errors.Is(err, workflowruntime.ErrNotFound) {
				t.Fatalf("denied handler bypassed policy: %v", err)
			}
			var status, resultJSON string
			if err := fixture.store.DB().QueryRow(`SELECT status,result_json FROM workflow_failure_hooks WHERE source_run_id=?`, request.RunID).Scan(&status, &resultJSON); err != nil || status != string(hoststate.FailureHookFailed) || len(resultJSON) == 0 || len(resultJSON) > 4096 {
				t.Fatalf("terminal failure hook status=%q result=%q err=%v", status, resultJSON, err)
			}

			// A later permissive restart must retain the terminal deny/confirm
			// result rather than silently bypassing it.
			permissive := newHost(hoststate.PolicyAllow)
			if err := permissive.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = permissive.Shutdown(context.Background()) })
			if _, err := fixture.journal.LoadStart(t.Context(), handlerRunID); !errors.Is(err, workflowruntime.ErrNotFound) {
				t.Fatalf("terminal policy result was bypassed after restart: %v", err)
			}
		})
	}
}

func TestPinnedRecoveryPlanFollowsReplayOfReplayAcrossSQLiteRestart(t *testing.T) {
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, compilePinnedReplayHostPlan(t))
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("replay-root", "replay-root-start", "user:replay")
	started, startErr := fixture.host.StartRun(authenticatedContext(t.Context(), "user:replay"), request)
	if startErr != nil || started.Run == nil {
		t.Fatalf("StartRun = %#v, %v", started, startErr)
	}
	nodeID := fixture.plan.Graph.Nodes[0].ID
	rootNode, loadErr := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: request.RunID, NodeID: nodeID})
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if _, err := fixture.state.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: rootNode.ID, ExpectedGeneration: rootNode.Generation, To: workflowruntime.NodeSkipped, At: fixture.now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	rootRun, _ := fixture.state.LoadRun(t.Context(), request.RunID)
	if _, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: rootRun.ID, ExpectedGeneration: rootRun.Generation, To: workflowruntime.RunSucceeded, At: fixture.now.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	registry := stepkind.NewRegistry()
	if err := registry.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	plans := appworkflow.PinnedRecoveryPlanSource{Roots: fixture.journal, Children: fixture.journal, State: fixture.state, Replays: fixture.state}
	replay := workflowruntime.ReplayService{Store: fixture.state, Replay: fixture.state, Inputs: fixture.state, Control: fixture.state, Plans: plans, Registry: registry}
	first, replayErr := replay.Rerun(t.Context(), workflowruntime.ReplayRequest{SourceRunID: request.RunID, RunID: "replay-one", FromNodeID: nodeID, IdempotencyKey: "replay-one", At: fixture.now.Add(3 * time.Second)})
	if replayErr != nil || first.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("first replay = %#v, %v", first, replayErr)
	}
	firstNode, _ := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: first.Run.ID, NodeID: nodeID})
	if _, err := fixture.state.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: firstNode.ID, ExpectedGeneration: firstNode.Generation, To: workflowruntime.NodeSkipped, At: fixture.now.Add(4 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	firstRun, _ := fixture.state.LoadRun(t.Context(), first.Run.ID)
	if _, err := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: firstRun.ID, ExpectedGeneration: firstRun.Generation, To: workflowruntime.RunSucceeded, At: fixture.now.Add(5 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	second, replayErr := replay.Rerun(t.Context(), workflowruntime.ReplayRequest{SourceRunID: first.Run.ID, RunID: "replay-two", FromNodeID: nodeID, IdempotencyKey: "replay-two", At: fixture.now.Add(6 * time.Second)})
	if replayErr != nil || second.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("replay of replay = %#v, %v", second, replayErr)
	}
	if _, err := fixture.journal.LoadStart(t.Context(), second.Run.ID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("replay target unexpectedly has root start record: %v", err)
	}
	if _, err := fixture.journal.LoadChildRunRequest(t.Context(), second.Run.ID); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("replay target unexpectedly has child start record: %v", err)
	}
	var databasePath string
	if err := fixture.store.DB().QueryRow(`SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&databasePath); err != nil {
		t.Fatal(err)
	}
	if err := fixture.host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedStore, openErr := persistence.Open(databasePath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopenedState, stateErr := persistence.NewWorkflowStateStore(reopenedStore)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	reopenedJournal, err := persistence.NewWorkflowHostStore(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	reopenedPlans := appworkflow.PinnedRecoveryPlanSource{Roots: reopenedJournal, Children: reopenedJournal, State: reopenedState, Replays: reopenedState}
	secondRun, err := reopenedState.LoadRun(t.Context(), second.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := reopenedPlans.LoadRecoveryPlan(t.Context(), secondRun)
	if err != nil || pinned.Ref != secondRun.Plan || pinned.Plan.Graph.ID != fixture.plan.Graph.ID {
		t.Fatalf("reopened pinned replay plan = %#v, %v", pinned, err)
	}
	explained, err := (workflowruntime.ExplainService{Store: reopenedState, Control: reopenedState, Replay: reopenedState, Plans: reopenedPlans}).Explain(t.Context(), second.Run.ID, fixture.now.Add(7*time.Second))
	if err != nil || explained.Replay == nil || explained.Replay.SourceRunID != first.Run.ID || len(explained.Invocations) != 1 {
		t.Fatalf("reopened replay explanation = %#v, %v", explained, err)
	}

	cycle := replayPlanOverride{ReplayStore: reopenedState, provenance: workflowruntime.ReplayProvenance{RunID: second.Run.ID, SourceRunID: second.Run.ID, FromNodeID: nodeID, PlanDigest: secondRun.Plan.Digest, IdempotencyKey: "cycle", CreatedAt: fixture.now}}
	cyclePlans := reopenedPlans
	cyclePlans.Replays = cycle
	if _, err := cyclePlans.LoadRecoveryPlan(t.Context(), secondRun); !errors.Is(err, appworkflow.ErrInvalidHost) {
		t.Fatalf("replay provenance cycle = %v", err)
	}
	differentPlan := workflowruntime.PlanRef{ID: "other", Version: "v1", Digest: values.SHA256Digest([]byte("other")), SchemaVersion: secondRun.Plan.SchemaVersion}
	if _, _, err := reopenedState.CreateRun(t.Context(), workflowruntime.CreateRunRequest{ID: "other-source", Plan: differentPlan, Status: workflowruntime.RunPending, StartIdempotencyKey: "other-source", CreatedAt: fixture.now}); err != nil {
		t.Fatal(err)
	}
	mismatch := replayPlanOverride{ReplayStore: reopenedState, provenance: workflowruntime.ReplayProvenance{RunID: second.Run.ID, SourceRunID: "other-source", FromNodeID: nodeID, PlanDigest: secondRun.Plan.Digest, IdempotencyKey: "mismatch", CreatedAt: fixture.now}}
	mismatchPlans := reopenedPlans
	mismatchPlans.Replays = mismatch
	if _, err := mismatchPlans.LoadRecoveryPlan(t.Context(), secondRun); !errors.Is(err, appworkflow.ErrInvalidHost) {
		t.Fatalf("replay source-plan mismatch = %v", err)
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

func TestHostRejectsMalformedIdentityRequestBeforeCollaborators(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	tests := map[string]func(*appworkflow.IdentityRequest){
		"principal credential": func(request *appworkflow.IdentityRequest) { request.PrincipalHint = "Bearer rejected-value" },
		"authority query": func(request *appworkflow.IdentityRequest) {
			request.SourceAuthority = "https://identity.invalid/source?mode=public"
		},
		"attribute credential": func(request *appworkflow.IdentityRequest) {
			request.Attributes = map[string]string{"api-key": "rejected-value"}
		},
		"attribute userinfo": func(request *appworkflow.IdentityRequest) {
			request.Attributes = map[string]string{"source": "https://user@example.invalid/path"}
		},
		"workspace alias": func(request *appworkflow.IdentityRequest) {
			request.Attributes = map[string]string{"workspace.id": "rejected-value"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			suffix := strings.ReplaceAll(name, " ", "-")
			request := fixture.startRequest("invalid-"+suffix, "key-"+suffix, "user:valid")
			mutate(&request.Identity)
			_, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:valid"), request)
			if err == nil || strings.Contains(err.Error(), "rejected-value") {
				t.Fatalf("StartRun error = %v", err)
			}
		})
	}
	if fixture.identityCalls.Load() != 0 || fixture.definitionCalls.Load() != 0 || fixture.policyCalls.Load() != 0 {
		t.Fatalf("malformed requests reached collaborators: identity=%d definition=%d policy=%d", fixture.identityCalls.Load(), fixture.definitionCalls.Load(), fixture.policyCalls.Load())
	}
}

func TestHostScopeTargetSelectorsAndPolicyFactsAreDistinct(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	request := fixture.startRequest("scope-target", "scope-target-key", "user:scope-target")
	fixture.mutatePolicyInput.Store(true)
	request.Identity.RunScope = &hoststate.RunScopeSelector{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "test"}
	request.Identity.ExecutionTarget = &hoststate.ExecutionTargetSelector{
		Version:      hoststate.ScopeTargetVersionV1,
		ID:           "local-default",
		Kinds:        []hoststate.ExecutionTargetKind{hoststate.ExecutionTargetLocal},
		SandboxModes: []hoststate.SandboxMode{hoststate.SandboxHostDefault},
	}
	started, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:scope-target"), request)
	if err != nil || started.Run == nil || started.Facts.RunScope.ID != "test" || started.Facts.ExecutionTarget == nil || started.Facts.ExecutionTarget.ID != "local-default" {
		t.Fatalf("StartRun = %#v, %v", started, err)
	}
	fixture.policyMu.Lock()
	observed := fixture.observedFacts
	fixture.policyMu.Unlock()
	if observed.RunScope.ID != "test" || observed.ExecutionTarget == nil || observed.ExecutionTarget.ID != "local-default" || observed.RunScope.ID == observed.ExecutionTarget.ID {
		t.Fatalf("policy facts did not preserve distinct scope/target: %#v", observed)
	}
	observed.RunScope.Attributes["cost_center"] = "mutated-policy-input"
	observed.ExecutionTarget.Labels["region"] = "mutated-policy-input"
	persisted, err := fixture.journal.LoadStart(t.Context(), started.Run.ID)
	if err != nil || persisted.Record.Facts.RunScope.Attributes["cost_center"] != "research" || persisted.Record.Facts.ExecutionTarget.Labels["region"] != "local" {
		t.Fatalf("policy input alias changed durable facts: %#v, %v", persisted.Record.Facts, err)
	}
	replayed, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:scope-target"), request)
	if err != nil || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Facts.RunScope.Attributes["cost_center"] != "research" || replayed.Facts.ExecutionTarget.Labels["region"] != "local" {
		t.Fatalf("policy facts replay = %#v, %v", replayed, err)
	}
	encoded, err := json.Marshal(request.Identity)
	if err != nil || !strings.Contains(string(encoded), `"run_scope"`) || !strings.Contains(string(encoded), `"execution_target"`) || strings.Contains(string(encoded), "workspace_id") {
		t.Fatalf("identity request JSON = %s, %v", encoded, err)
	}

	mismatch := fixture.startRequest("scope-target-mismatch", "scope-target-mismatch-key", "user:scope-target")
	mismatch.Identity.RunScope = &hoststate.RunScopeSelector{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "other"}
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:scope-target"), mismatch); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("scope selector mismatch = %v", err)
	}
	mismatch = fixture.startRequest("target-mismatch", "target-mismatch-key", "user:scope-target")
	mismatch.Identity.ExecutionTarget = &hoststate.ExecutionTargetSelector{Version: hoststate.ScopeTargetVersionV1, ID: "other"}
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:scope-target"), mismatch); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("target selector mismatch = %v", err)
	}
}

func TestHostRejectsMalformedExecutionRequirementsBeforePolicy(t *testing.T) {
	tests := map[string]graph.ExecutionTargetRequirements{
		"duplicate capabilities": {Capabilities: []string{"compute", "compute"}},
		"unsorted capabilities":  {Capabilities: []string{"network", "compute"}},
		"unknown kind":           {Kinds: []string{"container"}},
		"credential label":       {Labels: map[string]string{"authorization": "masked"}},
		"unknown constraint":     {Constraints: graph.Config{"region": "central"}},
	}
	for name, requirement := range tests {
		t.Run(name, func(t *testing.T) {
			plan := compileHostPlan(t)
			plan.Graph.Target = requirement
			fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
			if err := fixture.host.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
			request := fixture.startRequest("invalid-target", "invalid-target-"+strings.ReplaceAll(name, " ", "-"), "user:target")
			_, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:target"), request)
			if !errors.Is(err, appworkflow.ErrExecutionTarget) {
				t.Fatalf("StartRun error = %v", err)
			}
			if fixture.policyCalls.Load() != 0 {
				t.Fatalf("malformed target requirements reached policy: %d", fixture.policyCalls.Load())
			}
		})
	}
}

func TestHostExecutionTargetIsOptionalUntilPlanRequiresComputeBinding(t *testing.T) {
	optionalFixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	withoutTarget := testIdentityBinding("user:optional", "test")
	withoutTarget.ExecutionTarget = nil
	optionalHost := hostWithFixedIdentity(t, optionalFixture, withoutTarget)
	if err := optionalHost.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = optionalHost.Shutdown(context.Background()) })
	optionalRequest := optionalFixture.startRequest("optional-target", "optional-target-key", "user:optional")
	optional, err := optionalHost.StartRun(authenticatedContext(t.Context(), "user:optional"), optionalRequest)
	if err != nil || optional.Run == nil || optional.Facts.ExecutionTarget != nil {
		t.Fatalf("optional target start = %#v, %v", optional, err)
	}

	requiredPlan := compileHostPlan(t)
	requiredPlan.Graph.Target.Capabilities = []string{"compute"}
	requiredPlan.Graph.Digest, _ = workflowcompile.GraphDigest(requiredPlan.Graph)
	requiredPlan.Digest, _ = workflowcompile.PlanDigest(*requiredPlan)
	requiredFixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, requiredPlan)
	requiredHost := hostWithFixedIdentity(t, requiredFixture, withoutTarget)
	if startErr := requiredHost.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = requiredHost.Shutdown(context.Background()) })
	requiredRequest := requiredFixture.startRequest("required-target", "required-target-key", "user:optional")
	if _, startErr := requiredHost.StartRun(authenticatedContext(t.Context(), "user:optional"), requiredRequest); !errors.Is(startErr, appworkflow.ErrExecutionTarget) || requiredFixture.policyCalls.Load() != 0 {
		t.Fatalf("required target start = %v policy_calls=%d", startErr, requiredFixture.policyCalls.Load())
	}

	boundFixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, requiredPlan)
	bound := testIdentityBinding("user:bound", "test")
	bound.ExecutionTarget.Readiness.CheckedAt = bound.ExecutionTarget.Readiness.CheckedAt.In(time.FixedZone("provider-offset", 3600))
	boundHost := hostWithFixedIdentity(t, boundFixture, bound)
	if startErr := boundHost.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = boundHost.Shutdown(context.Background()) })
	boundRequest := boundFixture.startRequest("bound-target", "bound-target-key", "user:bound")
	started, err := boundHost.StartRun(authenticatedContext(t.Context(), "user:bound"), boundRequest)
	if err != nil || started.Run == nil || started.Facts.ExecutionTarget == nil || started.Facts.ExecutionTarget.ID != "local-default" {
		t.Fatalf("bound target start = %#v, %v", started, err)
	}
	persisted, err := boundFixture.journal.LoadStart(t.Context(), started.Run.ID)
	if err != nil || persisted.Record.Identity.ExecutionTarget == nil || persisted.Record.Identity.ExecutionTarget.ID != "local-default" || persisted.Record.Identity.ExecutionTarget.Readiness.CheckedAt.Location() != time.UTC || persisted.Record.Facts.RunScope.ID != "test" {
		t.Fatalf("persisted target binding = %#v, %v", persisted, err)
	}
}

func TestHostUsesDefinitionResolverFrozenVerifierCatalogForStartAndDispatch(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "verified-host.workflow.yaml")
	if err := os.WriteFile(sourcePath, []byte(`workflow:
  id: verified-host
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: echo
    transform:
      result: inputs.message
    with:
      message: inputs.message
    verify:
      - type: custom_review
`), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := verification.VerifierSpec{
		Kind: "custom_review", Version: "v1", Mode: verification.ModeReviewer,
		ConfigSchema: graph.Schema{"type": "object", "additionalProperties": false},
	}
	authoritative := &hostTestVerifier{
		spec: spec,
		result: verification.CheckResult{
			Outcome: verification.CheckPassed, Code: "custom_review_passed", Message: "custom review passed",
		},
	}
	resolverCatalog := verification.NewRegistry()
	if err := resolverCatalog.Register(authoritative); err != nil {
		t.Fatal(err)
	}
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	resolver, resolverErr := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{root},
		Authorizer: appworkflow.DefinitionAuthorizerFunc(func(context.Context, appworkflow.DefinitionAuthorization) error {
			return nil
		}),
		Compile: appworkflow.DefinitionCompileOptions{
			StepKinds: kinds, Verifiers: resolverCatalog, SemanticRevision: "host-verifier-v1",
		},
	})
	if resolverErr != nil {
		t.Fatal(resolverErr)
	}
	definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "verified-host", Locator: sourcePath, Version: "v1"}
	plan, resolveErr := resolver.ResolvePlan(t.Context(), definition)
	if resolveErr != nil || plan.Graph.Nodes[0].Verification == nil || authoritative.validationCalls.Load() == 0 {
		t.Fatalf("ResolvePlan() = %#v, %v validation_calls=%d", plan, resolveErr, authoritative.validationCalls.Load())
	}

	// A same-spec implementation supplied directly to Host is only a parity
	// assertion. DefinitionProvider's frozen implementation remains execution
	// authority so same-spec/different-behavior implementations cannot diverge.
	supplied := &hostTestVerifier{
		spec: spec,
		result: verification.CheckResult{
			Outcome: verification.CheckFailed, Code: "must_not_execute", Message: "supplied implementation must not execute",
		},
	}
	suppliedCatalog := verification.NewRegistry()
	if err := suppliedCatalog.Register(supplied); err != nil {
		t.Fatal(err)
	}
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		principal, _ := ctx.Value(authenticatedPrincipalKey{}).(string)
		if principal == "" {
			return hoststate.IdentityBinding{}, errors.New("missing authenticated principal")
		}
		return testIdentityBinding(principal, request.SourceAuthority), nil
	})
	options := appworkflow.Options{
		State: fixture.state, Journal: fixture.journal, Definitions: resolver, Identity: identity,
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "allow verified workflow"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
		Verifiers: suppliedCatalog, Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }), RecoveryInterval: time.Hour,
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	}
	var typedNil *verification.MemoryRegistry
	typedNilOptions := options
	typedNilOptions.Verifiers = typedNil
	if _, err := appworkflow.New(typedNilOptions); !errors.Is(err, appworkflow.ErrInvalidHost) {
		t.Fatalf("typed-nil Host verifier catalog error = %v", err)
	}
	mismatchedCatalog := verification.NewRegistry()
	mismatchedSpec := spec
	mismatchedSpec.RequiredEvidence = []verification.ActivityKind{verification.ActivityToolCall}
	if err := mismatchedCatalog.Register(&hostTestVerifier{spec: mismatchedSpec}); err != nil {
		t.Fatal(err)
	}
	mismatchedOptions := options
	mismatchedOptions.Verifiers = mismatchedCatalog
	if _, err := appworkflow.New(mismatchedOptions); !errors.Is(err, appworkflow.ErrInvalidHost) {
		t.Fatalf("mismatched Host/definition verifier catalog error = %v", err)
	}

	host, hostErr := appworkflow.New(options)
	if hostErr != nil {
		t.Fatal(hostErr)
	}
	if resolvedVerifier, ok := host.Verifiers().Lookup(spec.Kind); !ok || resolvedVerifier != authoritative {
		t.Fatalf("Host verifier implementation = %#v, %v", resolvedVerifier, ok)
	}
	if _, err := workflowruntime.NewExternalOperationCoordinator(workflowruntime.ExternalOperationOptions{
		Store: fixture.state, Registry: host.Registry(), Verifiers: host.Verifiers(), Now: func() time.Time { return fixture.now },
	}); err != nil {
		t.Fatalf("construct external coordinator from Host catalog seam: %v", err)
	}
	if err := resolverCatalog.Register(&hostTestVerifier{spec: verification.VerifierSpec{
		Kind: "late_review", Version: "v1", Mode: verification.ModeReviewer,
		ConfigSchema: graph.Schema{"type": "object"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := host.Verifiers().Lookup("late_review"); ok {
		t.Fatal("late source registration changed Host verifier snapshot")
	}
	listed := host.Verifiers().List()
	listed[0].ConfigSchema["mutated"] = true
	if _, exists := host.Verifiers().List()[0].ConfigSchema["mutated"]; exists {
		t.Fatal("Host verifier specs were not defensive copies")
	}
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	started, startErr := host.StartRun(authenticatedContext(t.Context(), "user:verified"), appworkflow.StartRunRequest{
		RunID: "verified-host-run", Definition: definition, Inputs: map[string]any{"message": "hello"},
		IdempotencyKey: "verified-host-start", Identity: appworkflow.IdentityRequest{PrincipalHint: "user:verified", SourceAuthority: "test"},
	})
	if startErr != nil || started.Run == nil || started.Run.Status != workflowruntime.RunRunning {
		t.Fatalf("StartRun() = %#v, %v", started, startErr)
	}
	queue := workflowruntime.NewReadyQueueCoordinator(fixture.state, nil)
	claim, acquired, claimErr := queue.ClaimNext(t.Context(), workflowruntime.ReadyClaimRequest{
		Owner: "verified-worker", Token: "verified-token", IdempotencyKey: "verified-claim",
		Now: fixture.now.Add(time.Second), LeaseUntil: fixture.now.Add(time.Minute),
	})
	if claimErr != nil || !acquired {
		t.Fatalf("ClaimNext() = %#v, %v, %v", claim, acquired, claimErr)
	}
	node := plan.Graph.Nodes[0]
	node.KindVersion = transform.Version
	dispatched, dispatchErr := host.Dispatcher().Dispatch(t.Context(), workflowruntime.DispatchRequest{
		Claim: claim, Node: node, IdempotencyKey: "verified-operation",
	})
	if dispatchErr != nil || dispatched.Node.Status != workflowruntime.NodeSucceeded || dispatched.Verification == nil ||
		dispatched.Verification.Report.Status != verification.ReportPassed {
		t.Fatalf("Dispatch() = %#v, %v", dispatched, dispatchErr)
	}
	if authoritative.verifyCalls.Load() != 1 || supplied.verifyCalls.Load() != 0 || supplied.validationCalls.Load() != 0 {
		t.Fatalf("verifier calls authoritative=%d supplied_validate=%d supplied_verify=%d", authoritative.verifyCalls.Load(), supplied.validationCalls.Load(), supplied.verifyCalls.Load())
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
			return testIdentityBinding(principal, request.SourceAuthority), nil
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
			return testIdentityBinding(principal, request.SourceAuthority), nil
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

func TestHostCancellationWithFinalizersFencesProgressesAndReplays(t *testing.T) {
	plan := compileFinalizerHostPlan(t)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	start := fixture.startRequest("run-finalizer-cancel", "start-finalizer-cancel", "user:cancel")
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:cancel"), start); err != nil {
		t.Fatal(err)
	}
	command := appworkflow.CancelRunRequest{RunID: start.RunID, IdempotencyKey: "cancel-with-finalizer", Reason: "operator request"}
	first, failures, err := fixture.host.CancelRun(t.Context(), command)
	if err != nil || len(failures) != 0 || first.Outcome != workflowruntime.IdempotencyApplied || !first.Run.Status.Active() {
		t.Fatalf("CancelRun with finalizer = %#v failures=%v, %v", first, failures, err)
	}
	work, _ := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: start.RunID, NodeID: "echo"})
	cleanupID := workflowruntime.NodeInvocationID{RunID: start.RunID, NodeID: "cleanup"}
	cleanup, _ := fixture.state.LoadNodeInvocation(t.Context(), cleanupID)
	if work.Status != workflowruntime.NodeCanceled || cleanup.Status != workflowruntime.NodePending {
		t.Fatalf("cancellation fence = work %#v cleanup %#v", work, cleanup)
	}
	replayed, failures, err := fixture.host.CancelRun(t.Context(), command)
	if err != nil || len(failures) != 0 || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Run.Generation != first.Run.Generation {
		t.Fatalf("finalizer cancellation replay = %#v failures=%v, %v", replayed, failures, err)
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(fixture.state, fixture.state, nil)
	progress, err := coordinator.ProgressFinally(t.Context(), plan.Graph, cleanupID, values.ExpressionContext{}, values.ExpressionOptions{}, fixture.now.Add(time.Second))
	if err != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("ProgressFinally = %#v, %v", progress, err)
	}
	skipped, err := fixture.state.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: cleanupID, ExpectedGeneration: progress.Snapshot.Generation, To: workflowruntime.NodeSkipped, At: fixture.now.Add(2 * time.Second)})
	if err != nil || skipped.Snapshot.Status != workflowruntime.NodeSkipped {
		t.Fatalf("skip cleanup = %#v, %v", skipped, err)
	}
	completed, intent, err := coordinator.ReconcileRunCompletion(t.Context(), plan.Graph, start.RunID, "cancel-with-finalizer", fixture.now.Add(3*time.Second))
	if err != nil || completed.Status != workflowruntime.RunCanceled || intent == nil || intent.Status != workflowruntime.TerminalIntentCompleted {
		t.Fatalf("complete canceled cleanup = run %#v intent %#v, %v", completed, intent, err)
	}
}

func TestHostFinalizerCancellationReconcilesRunningAttemptBeforeCleanup(t *testing.T) {
	plan := compileFinalizerHostPlan(t)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	var canceled atomic.Int32
	identity := identityProviderFunc(func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		return testIdentityBinding("test", "test"), nil
	})
	host, hostErr := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: fixture.journal, Definitions: definitionProvider{plan: plan}, Identity: identity,
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "allow"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}}, Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }), RecoveryInterval: time.Hour,
		Cancellation: &workflowruntime.CancellationCoordinator{Attempts: hostAttemptCancelerFunc(func(_ context.Context, attempt workflowruntime.AttemptSnapshot) error {
			if attempt.ID.Invocation.NodeID != "echo" {
				t.Fatalf("CancelAttempt attempt = %#v", attempt.ID)
			}
			canceled.Add(1)
			return nil
		})},
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if hostErr != nil {
		t.Fatal(hostErr)
	}
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	start := fixture.startRequest("run-finalizer-running-cancel", "start-finalizer-running-cancel", "test")
	if _, err := host.StartRun(t.Context(), start); err != nil {
		t.Fatal(err)
	}
	workID := workflowruntime.NodeInvocationID{RunID: start.RunID, NodeID: "echo"}
	work, err := fixture.state.LoadNodeInvocation(t.Context(), workID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: workID, ExpectedClaimGeneration: work.ClaimGeneration, Owner: "host-cancel", Token: "host-cancel-token", IdempotencyKey: "host-cancel-claim", Now: fixture.now, LeaseUntil: fixture.now.Add(time.Minute)})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("ClaimNode = %#v, %v", claim, err)
	}
	work, err = fixture.state.LoadNodeInvocation(t.Context(), workID)
	if err != nil {
		t.Fatal(err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: workID, ExpectedNodeGeneration: work.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}

	result, failures, err := host.CancelRun(t.Context(), appworkflow.CancelRunRequest{RunID: start.RunID, IdempotencyKey: "cancel-running-with-finalizer", Reason: "operator request", At: fixture.now.Add(time.Second)})
	if err != nil || len(failures) != 0 || result.Run.Status != workflowruntime.RunRunning || canceled.Load() != 1 {
		t.Fatalf("CancelRun = %#v failures=%v canceled=%d, %v", result, failures, canceled.Load(), err)
	}
	resolvedAttempt, err := fixture.state.LoadAttempt(t.Context(), started.Attempt.ID)
	if err != nil || resolvedAttempt.Status != workflowruntime.NodeCanceled || resolvedAttempt.FinishedAt.IsZero() {
		t.Fatalf("resolved attempt = %#v, %v", resolvedAttempt, err)
	}
	resolvedNode, err := fixture.state.LoadNodeInvocation(t.Context(), workID)
	if err != nil || resolvedNode.Status != workflowruntime.NodeCanceled {
		t.Fatalf("resolved node = %#v, %v", resolvedNode, err)
	}
	pending, err := fixture.state.RecoverCancellationIntents(t.Context(), workflowruntime.CancellationIntentQuery{RunID: start.RunID})
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending cancellation intents = %#v, %v", pending, err)
	}
	cleanupID := workflowruntime.NodeInvocationID{RunID: start.RunID, NodeID: "cleanup"}
	progress, err := workflowruntime.NewControlFlowCoordinator(fixture.state, fixture.state, nil).ProgressFinally(t.Context(), plan.Graph, cleanupID, values.ExpressionContext{}, values.ExpressionOptions{}, fixture.now.Add(2*time.Second))
	if err != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("ProgressFinally after reconciled attempt = %#v, %v", progress, err)
	}
}

func TestHostFinalizerCancellationFailsClosedWithoutControlFlowStore(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	plan := compileFinalizerHostPlan(t)
	*fixture.plan = *plan
	stateOnly := &stateStoreOnly{StateStore: fixture.state}
	identity := identityProviderFunc(func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		return testIdentityBinding("test", "test"), nil
	})
	host, err := appworkflow.New(appworkflow.Options{
		State: stateOnly, Journal: fixture.journal, Definitions: definitionProvider{plan: plan}, Identity: identity,
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "allow"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}}, Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }), RecoveryInterval: time.Hour,
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if host != nil || !errors.Is(err, appworkflow.ErrInvalidHost) {
		t.Fatalf("missing recovery/control stores Host = %#v, %v", host, err)
	}
}

func TestHostCancellationLoadsPinnedChildStartGraphAndPreservesChildCleanup(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	rootRequest := fixture.startRequest("run-parent-cancel", "start-parent-cancel", "user:parent")
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:parent"), rootRequest); err != nil {
		t.Fatal(err)
	}
	parent := workflowruntime.NodeInvocationID{RunID: rootRequest.RunID, NodeID: "echo"}
	parentNode, _ := fixture.state.LoadNodeInvocation(t.Context(), parent)
	claimed, claimErr := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: parent, ExpectedClaimGeneration: parentNode.ClaimGeneration, Owner: "host-tree-test", Token: "host-tree-token", IdempotencyKey: "host-tree-claim", Now: fixture.now, LeaseUntil: fixture.now.Add(time.Minute)})
	if claimErr != nil || !claimed.Acquired || claimed.Lease == nil {
		t.Fatalf("claim parent call = %#v, %v", claimed, claimErr)
	}
	parentNode, _ = fixture.state.LoadNodeInvocation(t.Context(), parent)
	proof := workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}
	started, startErr := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: parent, ExpectedNodeGeneration: parentNode.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, At: fixture.now})
	if startErr != nil {
		t.Fatal(startErr)
	}
	childPlan := compileFinalizerHostPlan(t)
	childRequest := childRecoveryRequest(t, childPlan, parent)
	childRequest.ChildRunID = "cancel-child-with-cleanup"
	childRequest.IdempotencyKey = "cancel-child-with-cleanup-start"
	child, childErr := fixture.journal.StartChildRun(t.Context(), childRequest)
	if childErr != nil || child.Run.Status != workflowruntime.RunPending {
		t.Fatalf("StartChildRun = %#v, %v", child, childErr)
	}
	if _, err := fixture.state.FinishNodeAttempt(t.Context(), workflowruntime.FinishNodeAttemptRequest{InvocationID: parent, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: fixture.now.Add(time.Nanosecond)}); err != nil {
		t.Fatal(err)
	}
	childBase := child.Run.UpdatedAt
	running, transitionErr := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: child.Run.ID, ExpectedGeneration: child.Run.Generation, To: workflowruntime.RunRunning, At: childBase.Add(time.Nanosecond)})
	if transitionErr != nil {
		t.Fatal(transitionErr)
	}
	for _, node := range childPlan.Graph.Nodes {
		if _, err := fixture.state.CreateNodeInvocation(t.Context(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: workflowruntime.NodeInvocationID{RunID: child.Run.ID, NodeID: node.ID}, Status: workflowruntime.NodePending, CreatedAt: childBase.Add(time.Nanosecond), UpdatedAt: childBase.Add(time.Nanosecond)}}); err != nil {
			t.Fatal(err)
		}
	}
	result, failures, cancelErr := fixture.host.CancelRun(t.Context(), appworkflow.CancelRunRequest{RunID: rootRequest.RunID, IdempotencyKey: "cancel-parent-tree", Reason: "parent canceled", At: childBase.Add(time.Second)})
	if cancelErr != nil || len(failures) != 0 || result.Run.Status != workflowruntime.RunCanceled {
		t.Fatalf("CancelRun tree = %#v failures=%v, %v", result, failures, cancelErr)
	}
	childAfter, _ := fixture.state.LoadRun(t.Context(), child.Run.ID)
	workID := workflowruntime.NodeInvocationID{RunID: child.Run.ID, NodeID: "echo"}
	cleanupID := workflowruntime.NodeInvocationID{RunID: child.Run.ID, NodeID: "cleanup"}
	work, _ := fixture.state.LoadNodeInvocation(t.Context(), workID)
	cleanup, _ := fixture.state.LoadNodeInvocation(t.Context(), cleanupID)
	if childAfter.Generation != running.Snapshot.Generation+1 || !childAfter.Status.Active() || work.Status != workflowruntime.NodeCanceled || cleanup.Status != workflowruntime.NodePending {
		t.Fatalf("child cleanup fence = run %#v work %#v cleanup %#v", childAfter, work, cleanup)
	}
	coordinator := workflowruntime.NewControlFlowCoordinator(fixture.state, fixture.state, nil)
	progress, progressErr := coordinator.ProgressFinally(t.Context(), childPlan.Graph, cleanupID, values.ExpressionContext{}, values.ExpressionOptions{}, childBase.Add(2*time.Second))
	if progressErr != nil || progress.Snapshot.Status != workflowruntime.NodeReady {
		t.Fatalf("child ProgressFinally = %#v, %v", progress, progressErr)
	}
	if _, err := fixture.state.TransitionNode(t.Context(), workflowruntime.NodeTransitionRequest{InvocationID: cleanupID, ExpectedGeneration: progress.Snapshot.Generation, To: workflowruntime.NodeSkipped, At: childBase.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	completed, intent, completionErr := coordinator.ReconcileRunCompletion(t.Context(), childPlan.Graph, child.Run.ID, "cancel-tree:"+string(child.Run.ID), childBase.Add(4*time.Second))
	if completionErr != nil || completed.Status != workflowruntime.RunCanceled || intent == nil || intent.Status != workflowruntime.TerminalIntentCompleted {
		t.Fatalf("child cleanup completion = run %#v intent %#v, %v", completed, intent, completionErr)
	}
}

func TestHostCancellationRefreshesEntireTreeAfterCASReachabilityChange(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	state := &injectTreeCASStore{StateStore: fixture.state, ControlFlowStore: fixture.state, RecoveryStore: fixture.state, NodeInputStore: fixture.state, RunPolicyStore: fixture.state}
	host, hostErr := appworkflow.New(appworkflow.Options{
		State: state, Journal: fixture.journal, Definitions: definitionProvider{plan: fixture.plan},
		Identity: identityProviderFunc(func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
			return testIdentityBinding("test", "test"), nil
		}),
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "allow"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}}, Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }), RecoveryInterval: time.Hour,
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if hostErr != nil {
		t.Fatal(hostErr)
	}
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	root := fixture.startRequest("run-tree-refresh", "start-tree-refresh", "test")
	if _, err := host.StartRun(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	parent := workflowruntime.NodeInvocationID{RunID: root.RunID, NodeID: "echo"}
	parentNode, _ := fixture.state.LoadNodeInvocation(t.Context(), parent)
	claim, err := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: parent, ExpectedClaimGeneration: parentNode.ClaimGeneration, Owner: "tree-refresh", Token: "tree-refresh-token", IdempotencyKey: "tree-refresh-claim", Now: fixture.now, LeaseUntil: fixture.now.Add(time.Minute)})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("claim refresh parent = %#v, %v", claim, err)
	}
	parentNode, _ = fixture.state.LoadNodeInvocation(t.Context(), parent)
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: parent, ExpectedNodeGeneration: parentNode.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	childPlan := compileFinalizerHostPlan(t)
	childRequest := childRecoveryRequest(t, childPlan, parent)
	childRequest.ChildRunID = "refresh-child"
	childRequest.IdempotencyKey = "refresh-child-start"
	var externalAttempt workflowruntime.AttemptID
	state.inject = func() error {
		child, startErr := fixture.journal.StartChildRun(context.Background(), childRequest)
		if startErr != nil {
			return startErr
		}
		if _, finishErr := fixture.state.FinishNodeAttempt(context.Background(), workflowruntime.FinishNodeAttemptRequest{InvocationID: parent, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: fixture.now.Add(time.Nanosecond)}); finishErr != nil {
			return finishErr
		}
		at := child.Run.UpdatedAt.Add(time.Nanosecond)
		if _, transitionErr := fixture.state.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: child.Run.ID, ExpectedGeneration: child.Run.Generation, To: workflowruntime.RunRunning, At: at}); transitionErr != nil {
			return transitionErr
		}
		for _, node := range childPlan.Graph.Nodes {
			if _, createErr := fixture.state.CreateNodeInvocation(context.Background(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: workflowruntime.NodeInvocationID{RunID: child.Run.ID, NodeID: node.ID}, Status: workflowruntime.NodePending, CreatedAt: at, UpdatedAt: at}}); createErr != nil {
				return createErr
			}
		}
		workID := workflowruntime.NodeInvocationID{RunID: child.Run.ID, NodeID: "echo"}
		work, loadErr := fixture.state.LoadNodeInvocation(context.Background(), workID)
		if loadErr != nil {
			return loadErr
		}
		ready, readyErr := fixture.state.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: workID, ExpectedGeneration: work.Generation, To: workflowruntime.NodeReady, At: at.Add(time.Nanosecond)})
		if readyErr != nil {
			return readyErr
		}
		childClaim, claimErr := fixture.state.ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{InvocationID: workID, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "child-external", Token: "child-external-token", IdempotencyKey: "child-external-claim", Now: at.Add(2 * time.Nanosecond), LeaseUntil: at.Add(time.Minute)})
		if claimErr != nil || !childClaim.Acquired || childClaim.Lease == nil {
			return fmt.Errorf("claim child external: %#v: %w", childClaim, claimErr)
		}
		work, loadErr = fixture.state.LoadNodeInvocation(context.Background(), workID)
		if loadErr != nil {
			return loadErr
		}
		childProof := workflowruntime.ClaimProof{Owner: childClaim.Lease.Owner, Token: childClaim.Lease.Token, Generation: childClaim.Lease.Generation}
		childStarted, startErr := fixture.state.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{InvocationID: workID, ExpectedNodeGeneration: work.Generation, Claim: childProof, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, At: at.Add(2 * time.Nanosecond)})
		if startErr != nil {
			return startErr
		}
		externalAttempt = childStarted.Attempt.ID
		_, suspendErr := fixture.state.SuspendExternalOperation(context.Background(), workflowruntime.SuspendExternalOperationRequest{
			Operation:              workflowruntime.ExternalOperationSnapshot{Attempt: externalAttempt, Ref: stepkind.ExternalOperationRef{Kind: "test-job", ID: "refresh-job"}, Invocation: stepkind.Invocation{Identity: stepkind.InvocationIdentity{RunID: string(child.Run.ID), NodeID: workID.NodeID, Attempt: externalAttempt.Number}, Config: graph.Config{}, Inputs: values.ValueSet{}, IdempotencyKey: "refresh-job-execute"}, Status: stepkind.ObservationPending},
			ExpectedNodeGeneration: childStarted.Node.Generation, ExpectedAttemptGeneration: childStarted.Attempt.Generation, Claim: childProof, At: at.Add(3 * time.Nanosecond),
		})
		if suspendErr != nil {
			return suspendErr
		}
		return nil
	}
	result, failures, err := host.CancelRun(t.Context(), appworkflow.CancelRunRequest{RunID: root.RunID, IdempotencyKey: "tree-refresh-cancel", Reason: "cancel", At: time.Now().UTC().Add(time.Hour)})
	if err != nil || len(failures) != 1 || !errors.Is(failures[0], workflowruntime.ErrCancellationUnsupported) || result.Run.Status != workflowruntime.RunCanceled || state.calls.Load() != 2 {
		t.Fatalf("refreshed cancellation = %#v failures=%v calls=%d, %v", result, failures, state.calls.Load(), err)
	}
	intent, err := fixture.state.LoadTerminalIntent(t.Context(), childRequest.ChildRunID)
	if err != nil || intent.Status != workflowruntime.TerminalIntentPending || len(intent.Finalizers) != 1 {
		t.Fatalf("refreshed child intent = %#v, %v", intent, err)
	}
	operation, err := fixture.state.LoadExternalOperation(t.Context(), externalAttempt)
	if err != nil || operation.Status != stepkind.ObservationPending || operation.CancelRequestedAt.IsZero() {
		t.Fatalf("recoverable child external cancellation = %#v, %v", operation, err)
	}
	if _, err := workflowruntime.NewControlFlowCoordinator(fixture.state, fixture.state, nil).ProgressFinally(t.Context(), childPlan.Graph, workflowruntime.NodeInvocationID{RunID: childRequest.ChildRunID, NodeID: "cleanup"}, values.ExpressionContext{}, values.ExpressionOptions{}, time.Now().UTC().Add(2*time.Hour)); !errors.Is(err, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("child cleanup ignored pending external cancellation = %v", err)
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

func TestHostRestartMaterializesPendingChildBeforeCancellationTree(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	root := fixture.startRequest("run-recover-child-tree", "start-recover-child-tree", "test")
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "test"), root); err != nil {
		t.Fatal(err)
	}
	parentID := workflowruntime.NodeInvocationID{RunID: root.RunID, NodeID: "echo"}
	parent, loadErr := fixture.state.LoadNodeInvocation(t.Context(), parentID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	claim, claimErr := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: parentID, ExpectedClaimGeneration: parent.ClaimGeneration, Owner: "child-recovery", Token: "child-recovery-token", IdempotencyKey: "child-recovery-claim", Now: fixture.now, LeaseUntil: fixture.now.Add(time.Minute)})
	if claimErr != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("ClaimNode = %#v, %v", claim, claimErr)
	}
	parent, loadErr = fixture.state.LoadNodeInvocation(t.Context(), parentID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, startErr := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: parentID, ExpectedNodeGeneration: parent.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, At: fixture.now})
	if startErr != nil {
		t.Fatal(startErr)
	}
	childPlan := compileFinalizerHostPlan(t)
	childRequest := childRecoveryRequest(t, childPlan, parentID)
	childRequest.ChildRunID = "unmaterialized-child-with-cleanup"
	childRequest.IdempotencyKey = "unmaterialized-child-start"
	child, childErr := fixture.journal.StartChildRun(t.Context(), childRequest)
	if childErr != nil || child.Run.Status != workflowruntime.RunPending {
		t.Fatalf("StartChildRun = %#v, %v", child, childErr)
	}
	if _, err := fixture.state.FinishNodeAttempt(t.Context(), workflowruntime.FinishNodeAttemptRequest{InvocationID: parentID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, At: fixture.now.Add(time.Nanosecond)}); err != nil {
		t.Fatal(err)
	}
	cancellationAt := child.Run.UpdatedAt.Add(time.Second)
	if _, _, err := fixture.journal.BindCancellation(t.Context(), hoststate.BindCancellationRequest{Intent: hoststate.CancellationIntent{RunID: root.RunID, IdempotencyKey: "recover-child-tree-cancel", Reason: "restart cancellation", RequestedAt: cancellationAt}, DefaultAt: cancellationAt}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	var materialized atomic.Int32
	identity := identityProviderFunc(func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		return testIdentityBinding("test", "test"), nil
	})
	restarted, hostErr := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: fixture.journal, Definitions: definitionProvider{plan: fixture.plan}, Identity: identity,
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "allow"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}}, Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		Clock: appworkflow.ClockFunc(func() time.Time { return cancellationAt.Add(time.Second) }), RecoveryInterval: time.Hour,
		ChildRuns: childMaterializerFunc(func(ctx context.Context, request calladapter.ChildRunRequest) error {
			materialized.Add(1)
			run, loadErr := fixture.state.LoadRun(ctx, request.ChildRunID)
			if loadErr != nil {
				return loadErr
			}
			at := run.UpdatedAt.Add(time.Nanosecond)
			for _, node := range request.Definition.Graph.Nodes {
				if _, createErr := fixture.state.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: node.ID}, Status: workflowruntime.NodePending, CreatedAt: at, UpdatedAt: at}}); createErr != nil {
					return createErr
				}
			}
			_, transitionErr := fixture.state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: at})
			return transitionErr
		}),
	})
	if hostErr != nil {
		t.Fatal(hostErr)
	}
	if err := restarted.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Shutdown(context.Background()) })
	if materialized.Load() != 1 {
		t.Fatalf("child materialization calls = %d", materialized.Load())
	}
	rootAfter, err := fixture.state.LoadRun(t.Context(), root.RunID)
	if err != nil || rootAfter.Status != workflowruntime.RunCanceled {
		t.Fatalf("recovered root = %#v, %v", rootAfter, err)
	}
	childAfter, err := fixture.state.LoadRun(t.Context(), childRequest.ChildRunID)
	if err != nil || !childAfter.Status.Active() {
		t.Fatalf("recovered child = %#v, %v", childAfter, err)
	}
	intent, err := fixture.state.LoadTerminalIntent(t.Context(), childRequest.ChildRunID)
	if err != nil || intent.Status != workflowruntime.TerminalIntentPending || len(intent.Finalizers) != 1 {
		t.Fatalf("recovered child intent = %#v, %v", intent, err)
	}
	cleanup, err := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: childRequest.ChildRunID, NodeID: "cleanup"})
	if err != nil || cleanup.Status != workflowruntime.NodeReady || cleanup.Inputs == nil {
		t.Fatalf("preserved child cleanup = %#v, %v", cleanup, err)
	}
	pending, err := fixture.journal.ListPendingCancellations(t.Context(), 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending host cancellations = %#v, %v", pending, err)
	}
}

func TestHostCancellationBoundsCASChurnAndLeavesRecoveryWork(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	churning := &alwaysCASStateStore{StateStore: fixture.state, ControlFlowStore: fixture.state, RecoveryStore: fixture.state, NodeInputStore: fixture.state, RunPolicyStore: fixture.state}
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		principal, _ := ctx.Value(authenticatedPrincipalKey{}).(string)
		return testIdentityBinding(principal, request.SourceAuthority), nil
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
	host              *appworkflow.Host
	dbPath            string
	store             *persistence.Store
	state             *persistence.WorkflowStateStore
	journal           *persistence.WorkflowHostStore
	plan              *workflowcompile.ExecutionPlan
	now               time.Time
	scheduler         *activationRecorder
	policyCalls       atomic.Int32
	mutatePolicyInput atomic.Bool
	identityCalls     atomic.Int32
	childCalls        atomic.Int32
	definitionCalls   atomic.Int32
	artifacts         values.ArtifactStore
	policyMu          sync.Mutex
	observedFacts     hoststate.PolicyFacts
}

func newHostFixture(t *testing.T, outcome hoststate.PolicyOutcome, interval time.Duration, hook appworkflow.RecoveryHook) *hostFixture {
	return newHostFixtureWithPlan(t, outcome, interval, hook, compileHostPlan(t))
}

func newHostFixtureWithPlan(t *testing.T, outcome hoststate.PolicyOutcome, interval time.Duration, hook appworkflow.RecoveryHook, plan *workflowcompile.ExecutionPlan) *hostFixture {
	t.Helper()
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "host.db")
	store, openErr := persistence.Open(dbPath)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, _ := persistence.NewWorkflowStateStore(store)
	journal, _ := persistence.NewWorkflowHostStore(store)
	scheduler := &activationRecorder{}
	artifactRoot, pathErr := filepath.EvalSymlinks(t.TempDir())
	if pathErr != nil {
		t.Fatal(pathErr)
	}
	artifactStore, artifactErr := artifacts.New(artifactRoot, values.ArtifactAuthorizerFunc(func(context.Context, values.ArtifactAuthorization) error { return nil }), nil)
	if artifactErr != nil {
		t.Fatal(artifactErr)
	}
	fixture := &hostFixture{dbPath: dbPath, store: store, state: state, journal: journal, plan: plan, now: now, scheduler: scheduler, artifacts: artifactStore}
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		fixture.identityCalls.Add(1)
		principal, ok := ctx.Value(authenticatedPrincipalKey{}).(string)
		if !ok || principal == "" {
			return hoststate.IdentityBinding{}, errors.New("missing authenticated principal")
		}
		return testIdentityBinding(principal, request.SourceAuthority), nil
	})
	policy := appworkflow.PolicyEvaluatorFunc(func(_ context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
		fixture.policyCalls.Add(1)
		fixture.policyMu.Lock()
		fixture.observedFacts = facts
		fixture.policyMu.Unlock()
		if fixture.mutatePolicyInput.Load() {
			facts.Identity.RunScope.Attributes["cost_center"] = "mutated-in-policy"
			facts.RunScope.Attributes["cost_center"] = "mutated-in-policy"
			facts.Identity.ExecutionTarget.Labels["region"] = "mutated-in-policy"
			facts.ExecutionTarget.Labels["region"] = "mutated-in-policy"
		}
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
	host, hostErr := appworkflow.New(appworkflow.Options{State: state, Journal: &snapshotRequiredJournal{WorkflowHostStore: journal}, Definitions: definitionProvider{plan: plan, calls: &fixture.definitionCalls}, Identity: identity, Policy: policy, Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}}, Activations: scheduler, Artifacts: artifactStore, Clock: appworkflow.ClockFunc(func() time.Time { return now }), RecoveryInterval: interval, RecoveryBatchLimit: 1, RecoveryHooks: hooks, ChildRuns: childMaterializer})
	if hostErr != nil {
		t.Fatal(hostErr)
	}
	fixture.host = host
	return fixture
}

// snapshotRequiredJournal proves that Host seals a snapshot before handing a
// start to any journal. The embedded SQLite store still provides the optional
// durable host journal capabilities exercised by the integration suite.
type snapshotRequiredJournal struct {
	*persistence.WorkflowHostStore
}

func (j *snapshotRequiredJournal) RecordStart(ctx context.Context, record hoststate.StartRecord) (hoststate.StartSnapshot, workflowruntime.IdempotencyOutcome, error) {
	if record.Snapshot == nil {
		return hoststate.StartSnapshot{}, "", errors.New("host handed journal a start without a plan snapshot")
	}
	return j.WorkflowHostStore.RecordStart(ctx, record)
}

func hostWithFixedIdentity(t *testing.T, fixture *hostFixture, binding hoststate.IdentityBinding) *appworkflow.Host {
	return hostWithFixedIdentityClock(t, fixture, binding, appworkflow.ClockFunc(func() time.Time { return fixture.now }))
}

func hostWithFixedIdentityClock(t *testing.T, fixture *hostFixture, binding hoststate.IdentityBinding, clock appworkflow.Clock,
	materializers ...workflowwait.Materializer,
) *appworkflow.Host {
	t.Helper()
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		fixture.identityCalls.Add(1)
		principal, ok := ctx.Value(authenticatedPrincipalKey{}).(string)
		if !ok || principal == "" {
			return hoststate.IdentityBinding{}, errors.New("missing authenticated principal")
		}
		result := binding.Clone()
		result.Principal = principal
		result.SourceAuthority = request.SourceAuthority
		return result, nil
	})
	policy := appworkflow.PolicyEvaluatorFunc(func(_ context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
		fixture.policyCalls.Add(1)
		fixture.policyMu.Lock()
		fixture.observedFacts = facts
		fixture.policyMu.Unlock()
		return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "fixture policy"}, nil
	})
	var waits *workflowruntime.WaitCoordinator
	if len(materializers) > 1 {
		t.Fatal("at most one wait materializer is supported")
	}
	if len(materializers) == 1 {
		waits = &workflowruntime.WaitCoordinator{Store: fixture.state, Scheduler: fixture.scheduler, Materializer: materializers[0]}
	}
	host, err := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: fixture.journal,
		Definitions: definitionProvider{plan: fixture.plan, calls: &fixture.definitionCalls}, Identity: identity, Policy: policy,
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
		Activations: fixture.scheduler, Waits: waits, Artifacts: fixture.artifacts, Clock: clock,
		RecoveryInterval: time.Hour, RecoveryBatchLimit: 1,
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	return host
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
	identity := testIdentityBinding("seed", "test")
	facts, err := policyFactsForSeed(bound.Run.Plan, bound.Run.ID, identity)
	if err != nil {
		t.Fatal(err)
	}
	record := hoststate.StartRecord{Run: *bound.Run, Plan: *fixture.plan, Requested: fixture.plan.Definition, StartKey: "call-parent-start", RequestDigest: values.SHA256Digest([]byte("call-parent-request")), CallerInputHash: values.SHA256Digest([]byte("call-parent-input")), Identity: identity, Facts: facts, Decision: hoststate.PolicyDecision{ID: "call-parent-decision", RunID: bound.Run.ID, Operation: "start", Outcome: hoststate.PolicyAllow, Reason: "seed", DecidedAt: fixture.now}, RecordedAt: fixture.now}
	start, _, err := fixture.journal.RecordStart(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []hoststate.StartPhase{hoststate.StartRunCreated, hoststate.StartNodesMaterialized, hoststate.StartPinsBound, hoststate.StartRunning} {
		start, err = fixture.journal.AdvanceStart(t.Context(), hoststate.AdvanceStartRequest{RunID: bound.Run.ID, ExpectedGeneration: start.Generation, From: start.Phase, To: phase, At: fixture.now})
		if err != nil {
			t.Fatal(err)
		}
	}
	run, err := fixture.state.LoadRun(t.Context(), bound.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := fixture.state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: fixture.now}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	inputs := bound.Run.InputsRef
	node, err := fixture.state.CreateNodeInvocation(t.Context(), workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: workflowruntime.NodeInvocationID{RunID: run.ID, NodeID: "echo"}, Status: workflowruntime.NodePending, Inputs: &inputs, CreatedAt: fixture.now, UpdatedAt: fixture.now}})
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
	started, err := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: current.Generation, Claim: workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func childRecoveryRequest(t *testing.T, plan *workflowcompile.ExecutionPlan, parent workflowruntime.NodeInvocationID) calladapter.ChildRunRequest {
	t.Helper()
	childDigest, err := workflowcompile.GraphDigest(plan.Graph)
	if err != nil {
		t.Fatal(err)
	}
	provenance := plan.Graph.Provenance
	provenance.Digest = childDigest
	definition := graph.DefinitionRef{Authority: provenance.Authority, Kind: "workflow", ID: plan.ID, Locator: provenance.Locator, Version: plan.Graph.Version, Digest: childDigest, Provenance: &provenance}
	planRef := workflowruntime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: childDigest, SchemaVersion: plan.SchemaVersion}
	resolvedGraph := plan.Graph
	resolvedGraph.Digest = childDigest
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

type hostAttemptCancelerFunc func(context.Context, workflowruntime.AttemptSnapshot) error

func (f hostAttemptCancelerFunc) CancelAttempt(ctx context.Context, attempt workflowruntime.AttemptSnapshot) error {
	return f(ctx, attempt)
}

type replayPlanOverride struct {
	workflowruntime.ReplayStore
	provenance workflowruntime.ReplayProvenance
}

func (s replayPlanOverride) LoadReplayProvenance(context.Context, workflowruntime.RunID) (workflowruntime.ReplayProvenance, error) {
	return s.provenance, nil
}

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
	return inferHostPlan(t, result.Plan)
}

func compileNonDurableHostPlan(t *testing.T) *workflowcompile.ExecutionPlan {
	t.Helper()
	source := workflowcompile.LoadBytes("host-non-durable.workflow.yaml", []byte(`workflow:
  name: Host Non Durable Fixture
  version: v1
durability: none
inputs:
  - name: message
    type: string
    required: true
outputs:
  echo:
    type: string
    value: steps.echo.outputs.result
steps:
  - name: Echo
    kind_version: v1
    transform:
      result: inputs.message
    with:
      message: inputs.message
    effects: [compute]
`))
	if source.Source == nil || len(source.Diagnostics) != 0 {
		t.Fatalf("LoadBytes non-durable = %#v", source)
	}
	result := workflowcompile.Compile(source.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile non-durable = %#v", result)
	}
	return inferHostPlan(t, result.Plan)
}

func compileFailureHandlerPlan(t *testing.T) *workflowcompile.ExecutionPlan {
	t.Helper()
	source := workflowcompile.LoadBytes("failure-handler.workflow.yaml", []byte(`workflow:
  name: Failure Handler
  version: v1
steps:
  - name: Record
    transform:
      result: handled
    effects: [compute]
`))
	if source.Source == nil || len(source.Diagnostics) != 0 {
		t.Fatalf("LoadBytes failure handler = %#v", source)
	}
	result := workflowcompile.Compile(source.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile failure handler = %#v", result)
	}
	plan := inferHostPlan(t, result.Plan)
	plan.Definition.Authority = "project"
	plan.Definition.Kind = "workflow"
	plan.Digest, _ = workflowcompile.PlanDigest(*plan)
	return plan
}

func testFailureHandlerRunID(source workflowruntime.RunID, digest string) workflowruntime.RunID {
	sum := sha256.Sum256([]byte(string(source) + "\x00" + digest))
	return workflowruntime.RunID("failure-handler-" + hex.EncodeToString(sum[:]))
}

func compilePinnedReplayHostPlan(t *testing.T) *workflowcompile.ExecutionPlan {
	t.Helper()
	source := workflowcompile.LoadBytes("host-pinned-replay.workflow.yaml", []byte(`workflow:
  name: Host Pinned Replay Fixture
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - name: Echo
    kind_version: v1
    transform:
      result: inputs.message
    with:
      message: inputs.message
    effects: [compute]
`))
	if source.Source == nil || len(source.Diagnostics) != 0 {
		t.Fatalf("LoadBytes pinned replay = %#v", source)
	}
	result := workflowcompile.Compile(source.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile pinned replay = %#v", result)
	}
	return inferHostPlan(t, result.Plan)
}

func compileFinalizerHostPlan(t *testing.T) *workflowcompile.ExecutionPlan {
	t.Helper()
	source := workflowcompile.LoadBytes("host-finalizer.workflow.yaml", []byte(`workflow:
  name: Host Finalizer Fixture
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
finally:
  - name: Cleanup
    transform:
      result: cleaned
`))
	if source.Source == nil || len(source.Diagnostics) != 0 {
		t.Fatalf("LoadBytes finalizer = %#v", source)
	}
	result := workflowcompile.Compile(source.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile finalizer = %#v", result)
	}
	return inferHostPlan(t, result.Plan)
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
	return inferHostPlan(t, result.Plan)
}

func inferHostPlan(t *testing.T, plan *workflowcompile.ExecutionPlan) *workflowcompile.ExecutionPlan {
	t.Helper()
	result := workflowcompile.InferValueDependencies(plan, workflowcompile.DependencyOptions{})
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("InferValueDependencies = %#v", result)
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
	identity := testIdentityBinding("seed", "test")
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
	facts := hoststate.PolicyFacts{Operation: "start", RunID: runID, Plan: plan, Identity: identity, RunScope: identity.RunScope, ExecutionTarget: identity.ExecutionTarget, Effects: graph.EffectSet{graph.EffectCompute}, NodeCount: 1, BlastRadius: map[string]int{"compute": 1}}
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

type mapDefinitionProvider struct {
	plans map[string]*workflowcompile.ExecutionPlan
}

func (p mapDefinitionProvider) ResolvePlan(_ context.Context, ref graph.DefinitionRef) (*workflowcompile.ExecutionPlan, error) {
	plan, ok := p.plans[ref.Digest]
	if !ok {
		return nil, workflowruntime.ErrNotFound
	}
	copyPlan := *plan
	return &copyPlan, nil
}

func (p mapDefinitionProvider) LoadPlan(_ context.Context, digest string) (*workflowcompile.ExecutionPlan, error) {
	plan, ok := p.plans[digest]
	if !ok {
		return nil, workflowruntime.ErrNotFound
	}
	copyPlan := *plan
	return &copyPlan, nil
}

type hostTestVerifier struct {
	spec            verification.VerifierSpec
	result          verification.CheckResult
	validationCalls atomic.Int32
	verifyCalls     atomic.Int32
}

func (v *hostTestVerifier) Spec() verification.VerifierSpec { return v.spec }

func (v *hostTestVerifier) ValidateConfig(context.Context, graph.VerificationCheck) []diagnostic.Diagnostic {
	v.validationCalls.Add(1)
	return nil
}

func (v *hostTestVerifier) Verify(context.Context, verification.Request) (verification.CheckResult, error) {
	v.verifyCalls.Add(1)
	return v.result, nil
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
	workflowruntime.ControlFlowStore
	workflowruntime.RecoveryStore
	workflowruntime.NodeInputStore
	workflowruntime.RunPolicyStore
	calls atomic.Int32
}

type stateStoreOnly struct{ workflowruntime.StateStore }

type injectTreeCASStore struct {
	workflowruntime.StateStore
	workflowruntime.ControlFlowStore
	workflowruntime.RecoveryStore
	workflowruntime.NodeInputStore
	workflowruntime.RunPolicyStore
	inject func() error
	calls  atomic.Int32
}

func (s *injectTreeCASStore) RequestRunCancellationWithFinalizers(ctx context.Context, request workflowruntime.RequestRunCancellationWithFinalizersRequest) (workflowruntime.RequestRunCancellationWithFinalizersResult, error) {
	if s.calls.Add(1) == 1 {
		if s.inject != nil {
			if err := s.inject(); err != nil {
				return workflowruntime.RequestRunCancellationWithFinalizersResult{}, err
			}
		}
		return workflowruntime.RequestRunCancellationWithFinalizersResult{}, workflowruntime.ErrCASMismatch
	}
	return s.ControlFlowStore.RequestRunCancellationWithFinalizers(ctx, request)
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
	current, err = j.Journal.AdvanceStart(ctx, hoststate.AdvanceStartRequest{RunID: current.Record.Run.ID, ExpectedGeneration: current.Generation, From: current.Phase, To: hoststate.StartPinsBound, At: current.UpdatedAt})
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

func (s *alwaysCASStateStore) RequestRunCancellationWithFinalizers(_ context.Context, request workflowruntime.RequestRunCancellationWithFinalizersRequest) (workflowruntime.RequestRunCancellationWithFinalizersResult, error) {
	s.calls.Add(1)
	return workflowruntime.RequestRunCancellationWithFinalizersResult{}, &workflowruntime.CASMismatchError{Resource: "test cancellation-tree churn", Expected: request.Cancellation.ExpectedGeneration, Actual: request.Cancellation.ExpectedGeneration + 1}
}

func testIdentityBinding(principal, authority string) hoststate.IdentityBinding {
	checkedAt := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	target := hoststate.ExecutionTarget{
		Version: hoststate.ScopeTargetVersionV1, ID: "local-default", Kind: hoststate.ExecutionTargetLocal,
		Capabilities: []string{"compute"}, Labels: map[string]string{"region": "local"}, Sandbox: hoststate.SandboxPolicy{Mode: hoststate.SandboxHostDefault},
		Readiness:  hoststate.TargetReadiness{State: hoststate.TargetReady, CheckedAt: checkedAt},
		Provenance: hoststate.TargetProvenance{Authority: "hadron", Reference: "local-default", Attributes: map[string]string{"pool": "default"}},
	}
	return hoststate.IdentityBinding{
		Principal: principal, SourceAuthority: authority, Trust: "trusted", Grants: []string{"workflow.run"},
		RunScope:        hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "test", Attributes: map[string]string{"cost_center": "research"}},
		ExecutionTarget: &target,
	}
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
