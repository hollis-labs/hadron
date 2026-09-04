package persistence

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
)

func TestWorkflowServicesReopenTwoHandleCASAndChronology(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "services.db")
	firstDB, first := openWorkflowStateTest(t, path)
	started, _ := prepareWorkflowSQLiteRunning(t, first, "service-store", workflowTestTime())
	base := started.Attempt.StartedAt
	invocation := workflowServiceInvocation(started.Attempt.ID, "service-key", nil)
	intent, err := first.PrepareServiceStart(ctx, workflowruntime.PrepareServiceStartRequest{
		Service: workflowruntime.ServiceSnapshot{
			Start: started.Attempt.ID, Invocation: invocation, Status: workflowruntime.ServiceLaunching,
		},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, At: base.Add(time.Second),
	})
	if err != nil || intent.Status != workflowruntime.ServiceLaunching {
		t.Fatalf("PrepareServiceStart = %#v, %v", intent, err)
	}

	secondDB, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	second, err := NewWorkflowStateStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := second.LoadService(ctx, started.Node.ID)
	if err != nil || reopened.Generation != intent.Generation || reopened.Invocation.IdempotencyKey != "service-key" {
		t.Fatalf("reopened launch intent = %#v, %v", reopened, err)
	}

	request := workflowruntime.RecoverServiceStartRequest{
		Start: intent.Start, Ref: stepkind.ExternalOperationRef{Kind: "fixture", ID: "service-1"},
		ExpectedServiceGeneration: intent.Generation, ExpectedNodeGeneration: started.Node.Generation,
		ExpectedAttemptGeneration: started.Attempt.Generation, At: base.Add(2 * time.Second),
	}
	type recoveryResult struct {
		result workflowruntime.SuspendServiceStartResult
		err    error
	}
	results := make(chan recoveryResult, 2)
	var group sync.WaitGroup
	for _, state := range []*WorkflowStateStore{first, second} {
		group.Add(1)
		go func(state *WorkflowStateStore) {
			defer group.Done()
			result, recoverErr := state.RecoverServiceStart(ctx, request)
			results <- recoveryResult{result: result, err: recoverErr}
		}(state)
	}
	group.Wait()
	close(results)
	var recovered workflowruntime.SuspendServiceStartResult
	var applied, conflicted int
	for result := range results {
		switch {
		case result.err == nil:
			applied++
			recovered = result.result
		case errors.Is(result.err, workflowruntime.ErrCASMismatch):
			conflicted++
		default:
			t.Fatalf("RecoverServiceStart race error = %v", result.err)
		}
	}
	if applied != 1 || conflicted != 1 || recovered.Service.Status != workflowruntime.ServiceStarting {
		t.Fatalf("recovery CAS applied=%d conflicted=%d result=%#v", applied, conflicted, recovered)
	}

	outputs := workflowTestValues(t, "ready")
	outputRef, err := first.SaveValues(ctx, workflowruntime.SaveValuesRequest{
		Owner: workflowruntime.ValueOwner{Kind: "node-attempt-outputs", RunID: recovered.Node.ID.RunID, Invocation: &recovered.Node.ID, Attempt: &recovered.Attempt.ID}, Values: outputs,
	})
	if err != nil {
		t.Fatal(err)
	}
	readyAt := base.Add(3 * time.Second)
	ready, err := first.ApplyServiceReady(ctx, workflowruntime.ApplyServiceReadyRequest{
		Start: recovered.Attempt.ID, ExpectedServiceGeneration: recovered.Service.Generation,
		ExpectedNodeGeneration: recovered.Node.Generation, ExpectedAttemptGeneration: recovered.Attempt.Generation,
		Ready: true, Outputs: &outputRef, ObservedAt: readyAt, HeartbeatAt: readyAt, At: readyAt,
	})
	if err != nil || ready.Service.Status != workflowruntime.ServiceReady || !ready.Service.ReadyAt.Equal(readyAt) {
		t.Fatalf("ApplyServiceReady = %#v, %v", ready, err)
	}

	heartbeatAt := base.Add(4 * time.Second)
	heartbeat, err := second.ApplyServiceHeartbeat(ctx, workflowruntime.ApplyServiceHeartbeatRequest{
		Start: ready.Node.ID, ExpectedServiceGeneration: ready.Service.Generation,
		ObservedAt: heartbeatAt, HeartbeatAt: heartbeatAt, At: heartbeatAt,
	})
	if err != nil || !heartbeat.LastHeartbeatAt.Equal(heartbeatAt) {
		t.Fatalf("ApplyServiceHeartbeat = %#v, %v", heartbeat, err)
	}
	before := heartbeat
	for name, mutate := range map[string]func(*workflowruntime.ApplyServiceHeartbeatRequest){
		"observation": func(value *workflowruntime.ApplyServiceHeartbeatRequest) {
			value.ObservedAt = readyAt.Add(-time.Second)
		},
		"heartbeat": func(value *workflowruntime.ApplyServiceHeartbeatRequest) {
			value.HeartbeatAt = readyAt.Add(-time.Second)
		},
		"mutation": func(value *workflowruntime.ApplyServiceHeartbeatRequest) { value.At = heartbeatAt.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := workflowruntime.ApplyServiceHeartbeatRequest{Start: ready.Node.ID, ExpectedServiceGeneration: before.Generation, ObservedAt: heartbeatAt, HeartbeatAt: heartbeatAt, At: heartbeatAt}
			mutate(&candidate)
			if _, applyErr := first.ApplyServiceHeartbeat(ctx, candidate); !errors.Is(applyErr, workflowruntime.ErrInvalidRecord) {
				t.Fatalf("regressive heartbeat error = %v", applyErr)
			}
			after, loadErr := first.LoadService(ctx, ready.Node.ID)
			if loadErr != nil || after.Generation != before.Generation || !after.LastHeartbeatAt.Equal(before.LastHeartbeatAt) || !after.LastObservedAt.Equal(before.LastObservedAt) {
				t.Fatalf("rejected heartbeat mutated service: %#v, %v", after, loadErr)
			}
		})
	}

	teardown := prepareWorkflowSQLiteNodeRunning(t, first, ready.Node.ID.RunID, "node-teardown", base.Add(5*time.Second))
	teardownInvocation := workflowServiceInvocation(teardown.Attempt.ID, "service-stop-key", &stepkind.ServiceBinding{
		Phase:           stepkind.ServiceTeardown,
		StartInvocation: stepkind.InvocationIdentity{RunID: string(started.Node.ID.RunID), NodeID: started.Node.ID.NodeID, Attempt: started.Attempt.ID.Number},
		Ref:             before.Ref,
	})
	stopping, err := first.SuspendServiceTeardown(ctx, workflowruntime.SuspendServiceTeardownRequest{
		Start: ready.Node.ID, Teardown: teardown.Attempt.ID, Invocation: teardownInvocation,
		ExpectedServiceGeneration: before.Generation, ExpectedNodeGeneration: teardown.Node.Generation,
		ExpectedAttemptGeneration: teardown.Attempt.Generation, Claim: teardown.claim, At: base.Add(8 * time.Second),
	})
	if err != nil || stopping.Service.Status != workflowruntime.ServiceStopping {
		t.Fatalf("SuspendServiceTeardown = %#v, %v", stopping, err)
	}
	regressiveStop := workflowruntime.ApplyServiceStopRequest{
		Start: ready.Node.ID, ExpectedServiceGeneration: stopping.Service.Generation,
		ExpectedNodeGeneration: stopping.Node.Generation, ExpectedAttemptGeneration: stopping.Attempt.Generation,
		ObservedAt: base.Add(7 * time.Second), At: base.Add(7 * time.Second),
	}
	if _, err := second.ApplyServiceStop(ctx, regressiveStop); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("regressive ApplyServiceStop error = %v", err)
	}
	if after, err := first.LoadService(ctx, ready.Node.ID); err != nil || after.Generation != stopping.Service.Generation {
		t.Fatalf("rejected stop mutated service: %#v, %v", after, err)
	}

	_ = firstDB
}

type workflowServiceRunning struct {
	workflowruntime.StartNodeAttemptResult
	claim workflowruntime.ClaimProof
}

func prepareWorkflowSQLiteNodeRunning(t *testing.T, state *WorkflowStateStore, runID workflowruntime.RunID, nodeID string, base time.Time) workflowServiceRunning {
	t.Helper()
	node := createWorkflowTestNode(t, state, runID, nodeID, base)
	ready, err := state.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := state.ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "worker", Token: "token-" + nodeID, IdempotencyKey: "claim-" + nodeID, Now: base.Add(time.Second), LeaseUntil: base.Add(time.Hour)})
	if err != nil || !claimed.Acquired {
		t.Fatalf("ClaimNode(%s) = %#v, %v", nodeID, claimed, err)
	}
	claim := workflowruntime.ClaimProof{Owner: claimed.Lease.Owner, Token: claimed.Lease.Token, Generation: claimed.Lease.Generation}
	claimedNode, _ := state.LoadNodeInvocation(context.Background(), node.ID)
	started, err := state.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: claimedNode.Generation, Claim: claim, Executor: workflowTestExecutor(), At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	return workflowServiceRunning{StartNodeAttemptResult: started, claim: claim}
}

func workflowServiceInvocation(attempt workflowruntime.AttemptID, key string, service *stepkind.ServiceBinding) stepkind.Invocation {
	return stepkind.Invocation{
		Identity: stepkind.InvocationIdentity{RunID: string(attempt.Invocation.RunID), NodeID: attempt.Invocation.NodeID, Iteration: attempt.Invocation.Iteration, Attempt: attempt.Number},
		Config:   graphConfig("fixture"), Inputs: values.ValueSet{}, Service: service, IdempotencyKey: key,
	}
}

func graphConfig(provider string) map[string]any {
	return map[string]any{"provider": provider, "config": map[string]any{}}
}
