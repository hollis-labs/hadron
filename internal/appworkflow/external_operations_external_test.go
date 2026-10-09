package appworkflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	calladapter "github.com/hollis-labs/libs/workflow/adapters/call"
	"github.com/hollis-labs/libs/workflow/adapters/transform"
	"github.com/hollis-labs/libs/workflow/graph"
	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
	"github.com/hollis-labs/libs/workflow/stepkind"
	"github.com/hollis-labs/libs/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/libs/workflow/values"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
)

// TestHostCancellationCancelsSuspendedExternalStep proves Host wires its
// ExternalOperationCoordinator into cancellation: an explicit-cancel external
// step is canceled through its adapter instead of failing with
// runtime.ErrCancellationUnsupported.
func TestHostCancellationCancelsSuspendedExternalStep(t *testing.T) {
	fixture := newHostFixture(t, hoststate.PolicyAllow, time.Hour, nil)
	if err := fixture.host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := fixture.startRequest("external-cancel-run", "external-cancel-run", "user:one")
	if _, err := fixture.host.StartRun(authenticatedContext(t.Context(), "user:one"), request); err != nil {
		t.Fatal(err)
	}
	if err := fixture.host.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}

	external := stepkindtest.NewLifecycleKind("external-kind", "v1")
	var cancels atomic.Int32
	external.CancelFunc = func(_ context.Context, ref stepkind.ExternalOperationRef) error {
		if ref.ID != "external-cancel-job" {
			return errors.New("unexpected external ref")
		}
		cancels.Add(1)
		return nil
	}
	external.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
		if cancels.Load() == 0 {
			return stepkind.Observation{State: stepkind.ObservationPending}, nil
		}
		failure := &stepkind.ExecutionError{Code: "remote-canceled", Message: "remote job canceled", Classification: stepkind.RetryPermanent}
		return stepkind.Observation{State: stepkind.ObservationCanceled, Failure: failure}, nil
	}
	host, err := appworkflow.New(appworkflow.Options{
		State: fixture.state, Journal: fixture.journal,
		Definitions: definitionProvider{plan: fixture.plan, calls: &fixture.definitionCalls},
		Identity: identityProviderFunc(func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
			return testIdentityBinding("user:one", "test"), nil
		}),
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "fixture"}, nil
		}),
		Kinds: []stepkind.StepKind{transform.New(), external}, RequiredKinds: []appworkflow.KindRef{{Name: transform.Name, Version: transform.Version}},
		Activations: fixture.scheduler, Artifacts: fixture.artifacts,
		RecoveryInterval: time.Hour, RecoveryBatchLimit: 1,
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })

	attempt := suspendHostExternalOperation(t, fixture, request.RunID, "echo", "external-kind", "external-cancel-job")
	coordinator := host.ExternalOperations()
	if coordinator == nil {
		t.Fatal("Host did not expose its external operation coordinator")
	}
	pending, err := coordinator.Recover(t.Context(), workflowruntime.ExternalOperationQuery{RunID: request.RunID})
	if err != nil || len(pending) != 1 || pending[0].Attempt != attempt {
		t.Fatalf("pending external operations = %#v, %v", pending, err)
	}
	if observed, reconcileErr := coordinator.Reconcile(t.Context(), attempt); reconcileErr != nil || observed.Operation.Status != stepkind.ObservationPending {
		t.Fatalf("Reconcile(pending) = %#v, %v", observed, reconcileErr)
	}

	result, failures, err := host.CancelRun(t.Context(), appworkflow.CancelRunRequest{
		RunID: request.RunID, IdempotencyKey: "external-cancel", Reason: "operator request", At: time.Now().UTC().Add(time.Hour),
	})
	for _, failure := range failures {
		if errors.Is(failure, workflowruntime.ErrCancellationUnsupported) {
			t.Fatalf("external cancellation is unsupported: %v", failures)
		}
	}
	if err != nil || len(failures) != 0 || cancels.Load() != 1 {
		t.Fatalf("CancelRun = %#v failures=%v cancels=%d, %v", result, failures, cancels.Load(), err)
	}
	operation, err := fixture.state.LoadExternalOperation(t.Context(), attempt)
	if err != nil || operation.Status != stepkind.ObservationCanceled || operation.CancelRequestedAt.IsZero() {
		t.Fatalf("external operation after cancel = %#v, %v", operation, err)
	}
	node, err := fixture.state.LoadNodeInvocation(t.Context(), attempt.Invocation)
	if err != nil || node.Status != workflowruntime.NodeCanceled {
		t.Fatalf("external node after cancel = %#v, %v", node, err)
	}
}

// suspendHostExternalOperation drives one ready node into a suspended
// external operation executed by kindName, as the dispatcher would.
func suspendHostExternalOperation(t *testing.T, fixture *hostFixture, runID workflowruntime.RunID, nodeID, kindName, jobID string) workflowruntime.AttemptID {
	t.Helper()
	ctx := t.Context()
	id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: nodeID}
	node, err := fixture.state.LoadNodeInvocation(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	at := node.UpdatedAt.Add(time.Millisecond)
	claim, err := fixture.state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{
		InvocationID: id, ExpectedClaimGeneration: node.ClaimGeneration, Owner: "external-worker", Token: "external-token",
		IdempotencyKey: "external-claim-" + jobID, Now: at, LeaseUntil: at.Add(time.Hour),
	})
	if err != nil || !claim.Acquired || claim.Lease == nil {
		t.Fatalf("claim %s = %#v, %v", nodeID, claim, err)
	}
	node, err = fixture.state.LoadNodeInvocation(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	proof := workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	started, err := fixture.state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: id, ExpectedNodeGeneration: node.Generation, Claim: proof,
		Executor: workflowruntime.ExecutorMetadata{Kind: kindName, Version: "v1", Target: "local"}, At: at.Add(time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.state.SuspendExternalOperation(ctx, workflowruntime.SuspendExternalOperationRequest{
		Operation: workflowruntime.ExternalOperationSnapshot{
			Attempt: started.Attempt.ID, Ref: stepkind.ExternalOperationRef{Kind: "job", ID: jobID},
			Invocation: stepkind.Invocation{
				Identity: stepkind.InvocationIdentity{RunID: string(runID), NodeID: nodeID, Attempt: started.Attempt.ID.Number},
				Config:   graph.Config{}, Inputs: values.ValueSet{}, IdempotencyKey: jobID + "-execute",
			},
			Status: stepkind.ObservationPending,
		},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: proof, At: at.Add(2 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	return started.Attempt.ID
}
