package appworkflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

type graphInspectorFunc func(context.Context, rundiagnostics.Query) (rundiagnostics.Result, error)

func (f graphInspectorFunc) Inspect(ctx context.Context, query rundiagnostics.Query) (rundiagnostics.Result, error) {
	return f(ctx, query)
}

type replayRunnerFunc func(context.Context, workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error)

func (f replayRunnerFunc) Rerun(ctx context.Context, request workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error) {
	return f(ctx, request)
}

func TestWorkflowOperatorRunAccessIsAuthenticatedAndDelegable(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.host.Shutdown(context.Background()) })
	ownerContext := authenticatedContext(t.Context(), "user:owner")
	request := fixture.startRequest("operator-owned-run", "operator-owned-start", "ignored-hint")
	if _, err := fixture.host.StartRun(ownerContext, request); err != nil {
		t.Fatal(err)
	}

	inspections := 0
	inspector := graphInspectorFunc(func(_ context.Context, query rundiagnostics.Query) (rundiagnostics.Result, error) {
		inspections++
		return rundiagnostics.Result{Run: rundiagnostics.RunDiagnostic{ID: query.RunID}}, nil
	})
	replays := 0
	replay := replayRunnerFunc(func(context.Context, workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error) {
		replays++
		return workflowruntime.BeginReplayResult{}, nil
	})
	strict, constructorErr := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{Host: fixture.host, Diagnostics: inspector, Replay: replay})
	if constructorErr != nil {
		t.Fatal(constructorErr)
	}
	approverContext := authenticatedContext(t.Context(), "user:approver")
	inspectRequest := appworkflow.InspectWorkflowRunRequest{RunID: request.RunID, Identity: appworkflow.IdentityRequest{PrincipalHint: "user:owner", SourceAuthority: "test"}}
	if _, inspectErr := strict.InspectWorkflowRun(approverContext, inspectRequest); !errors.Is(inspectErr, appworkflow.ErrPolicyDenied) || inspections != 0 {
		t.Fatalf("strict inspect calls=%d error=%v", inspections, inspectErr)
	}
	if _, cancelErr := strict.CancelWorkflowRun(approverContext, appworkflow.CancelWorkflowRunRequest{RunID: request.RunID, Identity: inspectRequest.Identity, IdempotencyKey: "forged-cancel"}); !errors.Is(cancelErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("forged cancel error=%v", cancelErr)
	}
	if _, rerunErr := strict.RerunWorkflow(approverContext, appworkflow.RerunWorkflowRequest{SourceRunID: request.RunID, RunID: "forged-rerun", FromNodeID: fixture.plan.Graph.Nodes[0].ID, IdempotencyKey: "forged-rerun", Identity: inspectRequest.Identity}); !errors.Is(rerunErr, appworkflow.ErrPolicyDenied) || replays != 0 {
		t.Fatalf("forged rerun calls=%d error=%v", replays, rerunErr)
	}
	ownerAcrossTransport := inspectRequest.Identity
	ownerAcrossTransport.SourceAuthority = "different-operator-transport"
	if _, rerunErr := strict.RerunWorkflow(ownerContext, appworkflow.RerunWorkflowRequest{SourceRunID: request.RunID, RunID: "cross-transport-rerun", FromNodeID: fixture.plan.Graph.Nodes[0].ID, IdempotencyKey: "cross-transport-rerun", Identity: ownerAcrossTransport}); !errors.Is(rerunErr, appworkflow.ErrPolicyDenied) || replays != 0 {
		t.Fatalf("different identity envelope rerun calls=%d error=%v", replays, rerunErr)
	}
	if _, rerunErr := strict.RerunWorkflow(ownerContext, appworkflow.RerunWorkflowRequest{SourceRunID: request.RunID, RunID: "owned-rerun", FromNodeID: fixture.plan.Graph.Nodes[0].ID, IdempotencyKey: "owned-rerun", Identity: inspectRequest.Identity}); rerunErr != nil || replays != 1 {
		t.Fatalf("exact owner rerun calls=%d error=%v", replays, rerunErr)
	}

	delegated, constructorErr := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{Host: fixture.host, Diagnostics: inspector, Replay: replay, RunAccess: appworkflow.RunOperationAuthorizerFunc(func(_ context.Context, authorization appworkflow.RunOperationAuthorization) error {
		if authorization.Operation != appworkflow.RunOperationInspect || authorization.Caller.Principal != "user:approver" || authorization.Owner.Principal != "user:owner" || authorization.Display == nil || authorization.Display.RevealsPrivate() {
			return appworkflow.ErrPolicyDenied
		}
		return nil
	})})
	if constructorErr != nil {
		t.Fatal(constructorErr)
	}
	result, inspectErr := delegated.InspectWorkflowRun(approverContext, inspectRequest)
	if inspectErr != nil || result.Run.ID != request.RunID || inspections != 1 {
		t.Fatalf("delegated inspect=%#v calls=%d error=%v", result, inspections, inspectErr)
	}
	privateRequest := inspectRequest
	privateRequest.Display.Private = values.PrivateDisplayReveal
	if _, inspectErr := delegated.InspectWorkflowRun(approverContext, privateRequest); !errors.Is(inspectErr, appworkflow.ErrPolicyDenied) || inspections != 1 {
		t.Fatalf("delegated private inspect calls=%d error=%v", inspections, inspectErr)
	}
	if _, err := delegated.InspectWorkflowRun(context.Background(), inspectRequest); err == nil {
		t.Fatal("run ID authorized an unauthenticated caller")
	}
}

func TestWorkflowOperatorValidateAndExplainDoNotAdmitRunningWork(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	host := newOperatorHost(t, fixture, nil)
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	operator, constructorErr := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{Host: host, Diagnostics: graphInspectorFunc(func(context.Context, rundiagnostics.Query) (rundiagnostics.Result, error) {
		return rundiagnostics.Result{}, nil
	}), Replay: replayRunnerFunc(func(context.Context, workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error) {
		return workflowruntime.BeginReplayResult{}, nil
	})})
	if constructorErr != nil {
		t.Fatal(constructorErr)
	}
	ctx := authenticatedContext(t.Context(), "user:validator")
	identity := appworkflow.IdentityRequest{PrincipalHint: "untrusted-hint", SourceAuthority: "test"}
	validated, err := operator.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: fixture.plan.Definition, Identity: identity})
	if err != nil || validated.Plan == nil || len(validated.Diagnostics) != 0 {
		t.Fatalf("validate=%#v error=%v", validated, err)
	}
	if incomplete, listErr := fixture.journal.ListIncompleteStarts(t.Context(), 0); listErr != nil || len(incomplete) != 0 {
		t.Fatalf("validation starts=%#v error=%v", incomplete, listErr)
	}

	explained, err := operator.ExplainWorkflow(ctx, appworkflow.ExplainWorkflowRequest{RunID: "explain-only", Definition: fixture.plan.Definition, Inputs: map[string]any{"message": "hello"}, IdempotencyKey: "explain-only-key", Identity: identity})
	if err != nil || !explained.DryRun || explained.Phase != hoststate.StartDryRunComplete || !explained.Facts.DryRunAvailable {
		t.Fatalf("explain=%#v error=%v", explained, err)
	}
	start, err := fixture.journal.LoadStart(t.Context(), "explain-only")
	if err != nil || start.Phase != hoststate.StartDryRunComplete || !start.Record.DryRun {
		t.Fatalf("explain audit=%#v error=%v", start, err)
	}
	if _, err := fixture.state.LoadRun(t.Context(), "explain-only"); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("dry-run admitted runtime state: %v", err)
	}
	if nodes, err := fixture.state.ListRunInvocations(t.Context(), "explain-only"); !errors.Is(err, workflowruntime.ErrNotFound) || len(nodes) != 0 {
		t.Fatalf("dry-run nodes=%#v error=%v", nodes, err)
	}
}

func TestWorkflowOperatorResumeUsesAuthenticatedResponderNotRunOwner(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	var mu sync.Mutex
	var responders []string
	authorizer := responderAuthorizerFunc(func(_ context.Context, request workflowwait.AuthorizationRequest) error {
		mu.Lock()
		responders = append(responders, request.Responder.Reference)
		mu.Unlock()
		if request.Responder.Kind != "principal" || request.Responder.Reference != "user:approver" {
			return appworkflow.ErrPolicyDenied
		}
		return nil
	})
	waits := &workflowruntime.WaitCoordinator{Store: fixture.state, Scheduler: fixture.scheduler, Authorizer: authorizer}
	host := newOperatorHost(t, fixture, waits)
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	firstToken := "operator-resume-token-one"
	firstWait := seedOperatorWait(t, host, fixture, waits, "operator-resume-one", "wait-operator-one", firstToken)
	operator, err := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{Host: host, Diagnostics: graphInspectorFunc(func(context.Context, rundiagnostics.Query) (rundiagnostics.Result, error) {
		return rundiagnostics.Result{}, nil
	}), Replay: replayRunnerFunc(func(context.Context, workflowruntime.ReplayRequest) (workflowruntime.BeginReplayResult, error) {
		return workflowruntime.BeginReplayResult{}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	payload := mustInline(t, "approved")
	resumed, err := operator.ResumeWorkflowRun(authenticatedContext(t.Context(), "user:approver"), appworkflow.ResumeWorkflowRunRequest{RunID: "operator-resume-one", Identity: appworkflow.IdentityRequest{PrincipalHint: "user:owner", SourceAuthority: "test"}, WaitID: firstWait, Correlation: "correlation-" + string(firstWait), Token: firstToken, WakeSource: workflowwait.WakeGate, Payload: payload, IdempotencyKey: "resume-one"})
	if err != nil || resumed.Outcome != workflowruntime.ResumeApplied {
		t.Fatalf("approver resume=%#v error=%v", resumed, err)
	}
	encoded, err := json.Marshal(resumed)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"resume_token", "payload", "lease", "events", firstToken} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("safe resume result exposed %q: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(string(encoded), `"outcome":"applied"`) || !strings.Contains(string(encoded), `"id":"`+string(firstWait)+`"`) {
		t.Fatalf("safe resume result omitted identifiers/status: %s", encoded)
	}

	secondToken := "operator-resume-token-two"
	secondWait := seedOperatorWait(t, host, fixture, waits, "operator-resume-two", "wait-operator-two", secondToken)
	_, forgedErr := operator.ResumeWorkflowRun(authenticatedContext(t.Context(), "user:intruder"), appworkflow.ResumeWorkflowRunRequest{RunID: "operator-resume-two", Identity: appworkflow.IdentityRequest{PrincipalHint: "user:approver", SourceAuthority: "test"}, WaitID: secondWait, Correlation: "correlation-" + string(secondWait), Token: secondToken, WakeSource: workflowwait.WakeGate, Payload: payload, IdempotencyKey: "resume-two"})
	if !errors.Is(forgedErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("forged responder error=%v", forgedErr)
	}
	_, mismatchErr := operator.ResumeWorkflowRun(authenticatedContext(t.Context(), "user:approver"), appworkflow.ResumeWorkflowRunRequest{RunID: "different-run", Identity: appworkflow.IdentityRequest{SourceAuthority: "test"}, WaitID: secondWait, Correlation: "correlation-" + string(secondWait), Token: secondToken, WakeSource: workflowwait.WakeGate, Payload: payload, IdempotencyKey: "resume-mismatch"})
	if !errors.Is(mismatchErr, appworkflow.ErrPolicyDenied) {
		t.Fatalf("wait/run mismatch error=%v", mismatchErr)
	}
	mu.Lock()
	got := append([]string(nil), responders...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "user:approver" || got[1] != "user:intruder" {
		t.Fatalf("authenticated responders=%v", got)
	}
}

func TestHostPinnedStartCrashReplayConvergesBeforeAdmission(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	journal := &pinCheckpointCrashJournal{Journal: fixture.journal, runID: "pin-target"}
	var observedAuthority workflowruntime.ReuseAuthority
	host := newOperatorHostConfigured(t, fixture, journal, nil, workflowruntime.ReuseAuthorizerFunc(func(_ context.Context, candidate workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
		observedAuthority = candidate.Authority
		return workflowruntime.ReusePolicyDecision{Allow: true, Code: "operator_test_allow", Reason: "operator test permits explicit pins"}, nil
	}))
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	sourceRequest := fixture.startRequest("pin-source", "pin-source-start", "user:developer")
	if _, err := host.StartRun(authenticatedContext(t.Context(), "user:developer"), sourceRequest); err != nil {
		t.Fatal(err)
	}
	outputRef := completeProjectOutput(t, fixture, sourceRequest.RunID)
	targetRequest := fixture.startRequest("pin-target", "pin-target-start", "user:developer")
	targetRequest.Pins = []hoststate.StartPin{{NodeID: fixture.plan.Graph.Nodes[0].ID, Outputs: outputRef}}
	partial, startErr := host.StartRun(authenticatedContext(t.Context(), "user:developer"), targetRequest)
	if startErr == nil || partial.Phase != hoststate.StartNodesMaterialized || partial.Run == nil || partial.Run.Status != workflowruntime.RunPending {
		t.Fatalf("partial start=%#v error=%v", partial, startErr)
	}
	targetID := workflowruntime.NodeInvocationID{RunID: targetRequest.RunID, NodeID: fixture.plan.Graph.Nodes[0].ID}
	bound, loadPinErr := fixture.state.LoadPin(t.Context(), targetID)
	if loadPinErr != nil || bound.Outputs != outputRef || bound.Authority.Principal != "user:developer" {
		t.Fatalf("partial pin=%#v error=%v", bound, loadPinErr)
	}
	if observedAuthority.Attributes["trust"] != "trusted" || observedAuthority.Attributes["target_id"] != "local-default" || observedAuthority.Attributes["target_kind"] != "local" || observedAuthority.Attributes["identity.extension.exposure_ref"] != "operator-cli" || observedAuthority.Attributes["grants"] != `["workflow.run"]` || observedAuthority.Attributes["identity_digest"] == "" || observedAuthority.Attributes["target_digest"] == "" {
		t.Fatalf("pin authority omitted immutable policy facts: %#v", observedAuthority)
	}
	if err := host.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	restarted := newOperatorHost(t, fixture, nil)
	if err := restarted.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Shutdown(context.Background()) })
	converged, replayErr := restarted.StartRun(authenticatedContext(t.Context(), "user:developer"), targetRequest)
	if replayErr != nil || converged.Outcome != workflowruntime.IdempotencyReplayed || converged.Phase != hoststate.StartRunning || converged.Run == nil || converged.Run.Status != workflowruntime.RunRunning {
		t.Fatalf("converged start=%#v error=%v", converged, replayErr)
	}
	changed := targetRequest
	changed.Pins = []hoststate.StartPin{{NodeID: fixture.plan.Graph.Nodes[0].ID, Outputs: values.ValueSetRef{ID: outputRef.ID, Digest: values.SHA256Digest([]byte("changed"))}}}
	if _, changedErr := restarted.StartRun(authenticatedContext(t.Context(), "user:developer"), changed); !errors.Is(changedErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed pin replay error=%v", changedErr)
	}
	if err := restarted.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	denying := newOperatorHostConfigured(t, fixture, fixture.journal, nil, workflowruntime.ReuseAuthorizerFunc(func(context.Context, workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
		return workflowruntime.ReusePolicyDecision{Allow: false, Code: "pin_denied", Reason: "operator is not permitted to reuse this output"}, nil
	}))
	if err := denying.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = denying.Shutdown(context.Background()) })
	deniedRequest := fixture.startRequest("pin-denied", "pin-denied-start", "user:developer")
	deniedRequest.Pins = []hoststate.StartPin{{NodeID: fixture.plan.Graph.Nodes[0].ID, Outputs: outputRef}}
	denied, deniedErr := denying.StartRun(authenticatedContext(t.Context(), "user:developer"), deniedRequest)
	if !errors.Is(deniedErr, workflowruntime.ErrReuseDenied) || !denied.RejectedBeforeAdmission() {
		t.Fatalf("denied start=%#v error=%v", denied, deniedErr)
	}
	node, loadNodeErr := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: deniedRequest.RunID, NodeID: fixture.plan.Graph.Nodes[0].ID})
	if loadNodeErr != nil || node.Status != workflowruntime.NodeCanceled {
		t.Fatalf("denied node=%#v error=%v", node, loadNodeErr)
	}
	claim, claimErr := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: node.ClaimGeneration, Owner: "rejected-pin-worker", Token: "rejected-pin-claim", IdempotencyKey: "rejected-pin-claim", Now: fixture.now, LeaseUntil: fixture.now.Add(time.Hour)})
	if claimErr != nil || claim.Acquired || claim.Lease != nil {
		t.Fatalf("rejected pin remained claimable: %#v error=%v", claim, claimErr)
	}
	if incomplete, listErr := fixture.journal.ListIncompleteStarts(t.Context(), 0); listErr != nil || len(incomplete) != 0 {
		t.Fatalf("denied start remained recoverable: %#v error=%v", incomplete, listErr)
	}
	if shutdownErr := denying.Shutdown(context.Background()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	rejectedRestart := newOperatorHost(t, fixture, nil)
	if restartErr := rejectedRestart.Start(t.Context()); restartErr != nil {
		t.Fatalf("restart after rejected pins: %v", restartErr)
	}
	t.Cleanup(func() { _ = rejectedRestart.Shutdown(context.Background()) })
	restartedRun, loadRunErr := fixture.state.LoadRun(t.Context(), deniedRequest.RunID)
	if loadRunErr != nil || restartedRun.Status != workflowruntime.RunCanceled {
		t.Fatalf("restarted rejected run=%#v error=%v", restartedRun, loadRunErr)
	}
}

func TestHostMultiPinSecondDenialLeavesTerminalNonAdmittedRun(t *testing.T) {
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, compileTwoPinHostPlan(t))
	nodeIDs := make([]string, 0, len(fixture.plan.Graph.Nodes))
	for _, node := range fixture.plan.Graph.Nodes {
		nodeIDs = append(nodeIDs, node.ID)
	}
	sort.Strings(nodeIDs)
	if len(nodeIDs) != 2 {
		t.Fatalf("fixture nodes=%v", nodeIDs)
	}
	var authorized []string
	host := newOperatorHostConfigured(t, fixture, fixture.journal, nil, workflowruntime.ReuseAuthorizerFunc(func(_ context.Context, candidate workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
		authorized = append(authorized, candidate.Target.NodeID)
		if candidate.Target.NodeID == nodeIDs[1] {
			return workflowruntime.ReusePolicyDecision{Allow: false, Code: "second_pin_denied", Reason: "second pin is not permitted"}, nil
		}
		return workflowruntime.ReusePolicyDecision{Allow: true, Code: "first_pin_allowed", Reason: "first pin is permitted"}, nil
	}))
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	sourceRequest := fixture.startRequest("multi-pin-source", "multi-pin-source-start", "user:developer")
	if _, err := host.StartRun(authenticatedContext(t.Context(), "user:developer"), sourceRequest); err != nil {
		t.Fatal(err)
	}
	outputRefs := make(map[string]values.ValueSetRef, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		outputRefs[nodeID] = completeProjectNodeOutput(t, fixture, sourceRequest.RunID, nodeID)
	}
	targetRequest := fixture.startRequest("multi-pin-target", "multi-pin-target-start", "user:developer")
	// Deliberately submit reverse order. The immutable request and authorization
	// pass canonicalize pins by node ID before the first durable binding.
	targetRequest.Pins = []hoststate.StartPin{
		{NodeID: nodeIDs[1], Outputs: outputRefs[nodeIDs[1]]},
		{NodeID: nodeIDs[0], Outputs: outputRefs[nodeIDs[0]]},
	}
	denied, deniedErr := host.StartRun(authenticatedContext(t.Context(), "user:developer"), targetRequest)
	if !errors.Is(deniedErr, workflowruntime.ErrReuseDenied) || !denied.RejectedBeforeAdmission() {
		t.Fatalf("denied=%#v error=%v", denied, deniedErr)
	}
	if len(authorized) != 2 || authorized[0] != nodeIDs[0] || authorized[1] != nodeIDs[1] {
		t.Fatalf("pin authorization order=%v", authorized)
	}
	if _, err := fixture.state.LoadPin(t.Context(), workflowruntime.NodeInvocationID{RunID: targetRequest.RunID, NodeID: nodeIDs[0]}); err != nil {
		t.Fatalf("first durable pin: %v", err)
	}
	if _, err := fixture.state.LoadPin(t.Context(), workflowruntime.NodeInvocationID{RunID: targetRequest.RunID, NodeID: nodeIDs[1]}); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("denied second pin exists: %v", err)
	}
	for _, nodeID := range nodeIDs {
		node, err := fixture.state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: targetRequest.RunID, NodeID: nodeID})
		if err != nil || node.Status != workflowruntime.NodeCanceled || node.Lease != nil {
			t.Fatalf("terminal node %s=%#v error=%v", nodeID, node, err)
		}
		claim, claimErr := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: node.ClaimGeneration, Owner: "rejected-multi-pin-worker", Token: "rejected-multi-pin-" + nodeID, IdempotencyKey: "rejected-multi-pin-" + nodeID, Now: fixture.now, LeaseUntil: fixture.now.Add(time.Hour)})
		if claimErr != nil || claim.Acquired || claim.Lease != nil {
			t.Fatalf("rejected node %s claim=%#v error=%v", nodeID, claim, claimErr)
		}
	}
	events, listEventsErr := fixture.state.ListEvents(t.Context(), workflowruntime.EventQuery{RunID: targetRequest.RunID})
	if listEventsErr != nil {
		t.Fatal(listEventsErr)
	}
	var runCancellationEvents, nodeCancellationEvents int
	for _, event := range events {
		switch event.Type {
		case workflowruntime.EventRunCancellationRequested:
			runCancellationEvents++
		case workflowruntime.EventNodeStatusChanged:
			if event.Attributes["to_status"] == string(workflowruntime.NodeCanceled) {
				nodeCancellationEvents++
			}
		}
	}
	if runCancellationEvents != 1 || nodeCancellationEvents != len(nodeIDs) {
		t.Fatalf("cancellation events run=%d nodes=%d all=%#v", runCancellationEvents, nodeCancellationEvents, events)
	}
	if incomplete, listErr := fixture.journal.ListIncompleteStarts(t.Context(), 0); listErr != nil || len(incomplete) != 0 {
		t.Fatalf("rejected start incomplete=%#v error=%v", incomplete, listErr)
	}
	if shutdownErr := host.Shutdown(context.Background()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}

	restarted := newOperatorHost(t, fixture, nil)
	if restartErr := restarted.Start(t.Context()); restartErr != nil {
		t.Fatalf("restart after partial pin rejection: %v", restartErr)
	}
	t.Cleanup(func() { _ = restarted.Shutdown(context.Background()) })
	inspect, err := restarted.InspectRun(t.Context(), targetRequest.RunID)
	if err != nil || inspect.Run.Status != workflowruntime.RunCanceled || len(inspect.Nodes) != len(nodeIDs) {
		t.Fatalf("inspect rejected run=%#v error=%v", inspect, err)
	}
	replayed, replayErr := restarted.StartRun(authenticatedContext(t.Context(), "user:developer"), targetRequest)
	if !errors.Is(replayErr, workflowruntime.ErrReuseDenied) || replayed.Outcome != workflowruntime.IdempotencyReplayed || replayed.Phase != hoststate.StartPinsRejected || replayed.Run == nil || replayed.Run.Status != workflowruntime.RunCanceled {
		t.Fatalf("replay rejected run=%#v error=%v", replayed, replayErr)
	}
}

func compileTwoPinHostPlan(t *testing.T) *workflowcompile.ExecutionPlan {
	t.Helper()
	source := workflowcompile.LoadBytes("host-two-pin.workflow.yaml", []byte(`workflow:
  name: Host Two Pin Fixture
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - name: Alpha
    transform:
      result: inputs.message
    with:
      message: inputs.message
    effects: [compute]
  - name: Zulu
    transform:
      result: inputs.message
    with:
      message: inputs.message
    effects: [compute]
`))
	if source.Source == nil || len(source.Diagnostics) != 0 {
		t.Fatalf("LoadBytes two pin = %#v", source)
	}
	result := workflowcompile.Compile(source.Source)
	if result.Plan == nil || len(result.Diagnostics) != 0 {
		t.Fatalf("Compile two pin = %#v", result)
	}
	return inferHostPlan(t, result.Plan)
}

func newOperatorHost(t *testing.T, fixture *hostFixture, waits *workflowruntime.WaitCoordinator) *appworkflow.Host {
	return newOperatorHostWithJournal(t, fixture, fixture.journal, waits)
}

func newOperatorHostWithJournal(t *testing.T, fixture *hostFixture, journal hoststate.Journal, waits *workflowruntime.WaitCoordinator) *appworkflow.Host {
	return newOperatorHostConfigured(t, fixture, journal, waits, workflowruntime.ReuseAuthorizerFunc(func(context.Context, workflowruntime.ReuseCandidate) (workflowruntime.ReusePolicyDecision, error) {
		return workflowruntime.ReusePolicyDecision{Allow: true, Code: "operator_test_allow", Reason: "operator test permits explicit pins"}, nil
	}))
}

func newOperatorHostConfigured(t *testing.T, fixture *hostFixture, journal hoststate.Journal, waits *workflowruntime.WaitCoordinator, reuse workflowruntime.ReuseAuthorizer) *appworkflow.Host {
	t.Helper()
	identity := identityProviderFunc(func(ctx context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
		principal, ok := ctx.Value(authenticatedPrincipalKey{}).(string)
		if !ok || principal == "" {
			return hoststate.IdentityBinding{}, errors.New("missing authenticated principal")
		}
		binding := testIdentityBinding(principal, request.SourceAuthority)
		binding.Extension = map[string]string{"exposure_ref": "operator-cli"}
		return binding, nil
	})
	policy := appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
		return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "operator test"}, nil
	})
	host, err := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: journal, Definitions: definitionProvider{plan: fixture.plan}, Identity: identity, Policy: policy,
		Kinds: []stepkind.StepKind{transform.New()}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
		Activations: fixture.scheduler, Waits: waits, Artifacts: fixture.artifacts, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now }),
		DryRun:           dryRunSupportFunc(func(context.Context, stepkind.StepKindSpec) (bool, error) { return true, nil }),
		RecoveryInterval: time.Hour, RecoveryBatchLimit: 1, ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
		ReuseAuthorizer: reuse,
	})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func seedOperatorWait(t *testing.T, host *appworkflow.Host, fixture *hostFixture, waits *workflowruntime.WaitCoordinator, runID, waitID, token string) workflowruntime.WaitID {
	t.Helper()
	request := fixture.startRequest(runID, "start-"+runID, "user:owner")
	if _, err := host.StartRun(authenticatedContext(t.Context(), "user:owner"), request); err != nil {
		t.Fatal(err)
	}
	nodeID := workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(runID), NodeID: fixture.plan.Graph.Nodes[0].ID}
	node, err := fixture.state.LoadNodeInvocation(t.Context(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: nodeID, ExpectedClaimGeneration: node.ClaimGeneration, Owner: "operator-wait-worker", Token: "claim-" + waitID, IdempotencyKey: "claim-" + waitID, Now: fixture.now, LeaseUntil: fixture.now.Add(time.Hour)})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("claim=%#v error=%v", claim, err)
	}
	node, _ = fixture.state.LoadNodeInvocation(t.Context(), nodeID)
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: nodeID, ExpectedNodeGeneration: node.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	schema, err := workflowwait.NewSchemaRef(map[string]any{"type": "string"})
	if err != nil {
		t.Fatal(err)
	}
	tokenDigest, err := workflowwait.DigestToken(token)
	if err != nil {
		t.Fatal(err)
	}
	waitRef := workflowruntime.WaitID(waitID)
	_, err = waits.Suspend(t.Context(), workflowruntime.SuspendCommand{Request: workflowruntime.SuspendNodeWaitRequest{Wait: workflowruntime.WaitSnapshot{Ref: workflowruntime.WaitRef{ID: waitRef}, Invocation: nodeID, Record: workflowwait.Record{Kind: workflowwait.KindGate, Correlation: "correlation-" + waitID, ResumeSchema: schema, ResumeTokenDigest: tokenDigest, Visibility: workflowwait.VisibilityPrivate, Authority: workflowwait.ResponderAuthority{Kind: "principal", Reference: "user:approver"}, WakeSource: workflowwait.WakeGate, Status: workflowwait.StatusOpen}}, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, At: fixture.now}, ResumeToken: token})
	if err != nil {
		t.Fatal(err)
	}
	return waitRef
}

type responderAuthorizerFunc func(context.Context, workflowwait.AuthorizationRequest) error

func (f responderAuthorizerFunc) AuthorizeResume(ctx context.Context, request workflowwait.AuthorizationRequest) error {
	return f(ctx, request)
}

type dryRunSupportFunc func(context.Context, stepkind.StepKindSpec) (bool, error)

func (f dryRunSupportFunc) SupportsDryRun(ctx context.Context, spec stepkind.StepKindSpec) (bool, error) {
	return f(ctx, spec)
}

type pinCheckpointCrashJournal struct {
	hoststate.Journal
	runID workflowruntime.RunID
	once  bool
}

func (j *pinCheckpointCrashJournal) AdvanceStart(ctx context.Context, request hoststate.AdvanceStartRequest) (hoststate.StartSnapshot, error) {
	if request.RunID == j.runID && request.From == hoststate.StartNodesMaterialized && request.To == hoststate.StartPinsBound && !j.once {
		j.once = true
		return hoststate.StartSnapshot{}, errors.New("injected crash after durable pin binding")
	}
	return j.Journal.AdvanceStart(ctx, request)
}

func completeProjectOutput(t *testing.T, fixture *hostFixture, runID workflowruntime.RunID) values.ValueSetRef {
	t.Helper()
	return completeProjectNodeOutput(t, fixture, runID, fixture.plan.Graph.Nodes[0].ID)
}

func completeProjectNodeOutput(t *testing.T, fixture *hostFixture, runID workflowruntime.RunID, graphNodeID string) values.ValueSetRef {
	t.Helper()
	nodeID := workflowruntime.NodeInvocationID{RunID: runID, NodeID: graphNodeID}
	node, err := fixture.state.LoadNodeInvocation(t.Context(), nodeID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.state.ClaimNode(t.Context(), workflowruntime.ClaimNodeRequest{InvocationID: nodeID, ExpectedClaimGeneration: node.ClaimGeneration, Owner: "pin-source-worker-" + graphNodeID, Token: "pin-source-token-" + graphNodeID, IdempotencyKey: "pin-source-claim-" + graphNodeID, Now: fixture.now, LeaseUntil: fixture.now.Add(time.Hour)})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("claim=%#v error=%v", claim, err)
	}
	node, _ = fixture.state.LoadNodeInvocation(t.Context(), nodeID)
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := fixture.state.StartNodeAttempt(t.Context(), workflowruntime.StartNodeAttemptRequest{InvocationID: nodeID, ExpectedNodeGeneration: node.Generation, Claim: proof, Executor: workflowruntime.ExecutorMetadata{Kind: transform.Name, Version: transform.Version, Target: "local"}, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	value, err := values.NewInline("pinned-"+graphNodeID, values.Metadata{Producer: values.Producer{Kind: "node", Reference: string(runID) + ":" + nodeID.NodeID, Output: "result"}, MediaType: "application/json", Redaction: values.RedactionPublic, Retention: values.RetentionProject})
	if err != nil {
		t.Fatal(err)
	}
	invocation := nodeID
	outputRef, err := fixture.state.SaveValues(t.Context(), workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "node-outputs", RunID: runID, Invocation: &invocation}, Values: values.ValueSet{"result": value}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.state.FinishNodeAttempt(t.Context(), workflowruntime.FinishNodeAttemptRequest{InvocationID: nodeID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: workflowruntime.NodeSucceeded, NextNodeStatus: workflowruntime.NodeSucceeded, Outputs: &outputRef, At: fixture.now})
	if err != nil {
		t.Fatal(err)
	}
	return outputRef
}
