package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
)

func TestBuildSchedulerRequirementsKeepsDimensionsIndependent(t *testing.T) {
	requirements, err := workflowruntime.BuildSchedulerRequirements("run-a", workflowruntime.SchedulerLimits{
		Workers: 8, PerRun: 2,
		Effects:      map[graph.Effect]int{graph.EffectRead: 4, graph.EffectMutate: 1},
		Capabilities: map[string]int{"network": 3, "gpu": 1},
		Named:        map[string]int{"shared-db": 2},
	}, workflowruntime.SchedulerDemand{
		Effects:      graph.EffectSet{graph.EffectRead, graph.EffectMutate, graph.EffectRead},
		Capabilities: []string{"network", "gpu", "network"},
		Concurrency:  []graph.ConcurrencyClaim{{Resource: "shared-db", Amount: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []workflowruntime.SchedulerResourceKind{
		workflowruntime.SchedulerResourceCapability,
		workflowruntime.SchedulerResourceCapability,
		workflowruntime.SchedulerResourceKey,
		workflowruntime.SchedulerResourceEffect,
		workflowruntime.SchedulerResourceEffect,
		workflowruntime.SchedulerResourceRun,
		workflowruntime.SchedulerResourceWorker,
	}
	if len(requirements) != len(want) {
		t.Fatalf("requirements = %#v", requirements)
	}
	for index := range want {
		if requirements[index].Resource.Kind != want[index] {
			t.Fatalf("requirements[%d] kind = %q, want %q", index, requirements[index].Resource.Kind, want[index])
		}
	}
	if requirements[2].Units != 2 || requirements[2].Limit != 2 {
		t.Fatalf("named requirement = %#v", requirements[2])
	}

	_, err = workflowruntime.BuildSchedulerRequirements("run-a", workflowruntime.SchedulerLimits{Workers: 1}, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "missing"}}})
	if !errors.Is(err, workflowruntime.ErrInvalidSchedulerResource) {
		t.Fatalf("missing named definition = %v", err)
	}
	_, err = workflowruntime.BuildSchedulerRequirements("run-a", workflowruntime.SchedulerLimits{Workers: 1, Named: map[string]int{"shared": 1}}, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "shared"}, {Resource: "shared"}}})
	if !errors.Is(err, workflowruntime.ErrInvalidSchedulerResource) {
		t.Fatalf("duplicate named claim = %v", err)
	}
}

func TestSchedulerAdmissionIsAtomicInspectableAndLeakFree(t *testing.T) {
	ctx := context.Background()
	store := inmemory.NewStore()
	base := time.Date(2026, 8, 24, 20, 0, 0, 123, time.UTC)
	first := invocationID("resource-run-a", "first")
	second := invocationID("resource-run-b", "second")
	independent := invocationID("resource-run-c", "independent")
	for _, id := range []workflowruntime.NodeInvocationID{first, second, independent} {
		createNode(t, store, id, workflowruntime.NodeReady, 0, base)
	}
	limits := workflowruntime.SchedulerLimits{
		Workers: 4, PerRun: 2,
		Effects: map[graph.Effect]int{graph.EffectRead: 1},
		Named:   map[string]int{"shared-db": 1},
	}
	contended := resourceRequirements(t, first.RunID, limits, workflowruntime.SchedulerDemand{
		Effects: graph.EffectSet{graph.EffectRead}, Concurrency: []graph.ConcurrencyClaim{{Resource: "shared-db"}},
	})
	firstResult, err := store.AdmitNode(ctx, schedulerAdmission(first, 0, "first", contended, base.Add(time.Second), base.Add(time.Minute)))
	if err != nil || !firstResult.Claim.Acquired || firstResult.Claim.Lease == nil {
		t.Fatalf("first admission = %#v, %v", firstResult, err)
	}
	firstReplay := schedulerAdmission(first, 0, "first", contended, base.Add(time.Second).In(time.FixedZone("equivalent", 9*60*60)), base.Add(time.Minute).In(time.FixedZone("equivalent", 9*60*60)))
	replayed, err := store.AdmitNode(ctx, firstReplay)
	if err != nil || !replayed.Claim.Acquired || !replayed.Claim.Replayed {
		t.Fatalf("semantic-time admission replay = %#v, %v", replayed, err)
	}
	changed := firstReplay
	changed.Requirements = append([]workflowruntime.SchedulerResourceRequirement(nil), changed.Requirements...)
	for index := range changed.Requirements {
		if changed.Requirements[index].Resource.Kind == workflowruntime.SchedulerResourceKey {
			changed.Requirements[index].Limit++
		}
	}
	if _, replayErr := store.AdmitNode(ctx, changed); !errors.Is(replayErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed admission replay = %v", replayErr)
	}
	secondRequirements := resourceRequirements(t, second.RunID, limits, workflowruntime.SchedulerDemand{
		Effects: graph.EffectSet{graph.EffectRead}, Concurrency: []graph.ConcurrencyClaim{{Resource: "shared-db"}},
	})
	blocked, err := store.AdmitNode(ctx, schedulerAdmission(second, 0, "second-blocked", secondRequirements, base.Add(2*time.Second), base.Add(time.Minute)))
	if err != nil || blocked.Claim.Acquired || len(blocked.Blocked) != 2 || blocked.Blocked[0].Kind != workflowruntime.SchedulerResourceKey || blocked.Blocked[1].Kind != workflowruntime.SchedulerResourceEffect {
		t.Fatalf("blocked admission = %#v, %v", blocked, err)
	}
	independentRequirements := resourceRequirements(t, independent.RunID, limits, workflowruntime.SchedulerDemand{})
	independentResult, err := store.AdmitNode(ctx, schedulerAdmission(independent, 0, "independent", independentRequirements, base.Add(3*time.Second), base.Add(time.Minute)))
	if err != nil || !independentResult.Claim.Acquired {
		t.Fatalf("independent admission = %#v, %v", independentResult, err)
	}
	state, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(3 * time.Second)})
	if err != nil || len(state.Holders) != 6 || len(state.Waiters) != 1 || state.Waiters[0].Invocation != second {
		t.Fatalf("resource state = %#v, %v", state, err)
	}

	firstLease := firstResult.Claim.Lease
	renewed, err := store.RenewNodeLease(ctx, workflowruntime.RenewLeaseRequest{
		InvocationID: first, Owner: firstLease.Owner, Token: firstLease.Token,
		Generation: firstLease.Generation, Now: base.Add(4 * time.Second), LeaseUntil: base.Add(2 * time.Minute),
	})
	if err != nil || !renewed.ExpiresAt.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("renew = %#v, %v", renewed, err)
	}
	state, err = store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: first.RunID, Now: base.Add(4 * time.Second)})
	if err != nil || len(state.Holders) != len(contended) {
		t.Fatalf("renewed holders = %#v, %v", state, err)
	}
	for _, holder := range state.Holders {
		if !holder.ExpiresAt.Equal(renewed.ExpiresAt) || holder.ClaimGeneration != renewed.Generation {
			t.Fatalf("holder not renewed atomically = %#v", holder)
		}
	}
	if releaseErr := store.ReleaseNodeClaim(ctx, workflowruntime.ReleaseClaimRequest{InvocationID: first, Owner: firstLease.Owner, Token: firstLease.Token, Generation: firstLease.Generation, Now: base.Add(5 * time.Second)}); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	state, err = store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(5 * time.Second)})
	if err != nil || len(state.Waiters) != 0 {
		t.Fatalf("released/unblocked diagnostics = %#v, %v", state, err)
	}
	secondResult, err := store.AdmitNode(ctx, schedulerAdmission(second, 0, "second-acquired", secondRequirements, base.Add(6*time.Second), base.Add(3*time.Minute)))
	if err != nil || !secondResult.Claim.Acquired {
		t.Fatalf("second admission after release = %#v, %v", secondResult, err)
	}
	secondNode, _ := store.LoadNodeInvocation(ctx, second)
	proof := proofFromLease(*secondResult.Claim.Lease)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: second, ExpectedNodeGeneration: secondNode.Generation, Claim: proof, Executor: testExecutor(), At: base.Add(7 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: second, ExpectedGeneration: started.Node.Generation, To: workflowruntime.NodeWaiting, Claim: &proof, At: base.Add(8 * time.Second)})
	if err != nil || waiting.Snapshot.Lease != nil {
		t.Fatalf("suspend = %#v, %v", waiting, err)
	}
	state, err = store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: second.RunID, Now: base.Add(8 * time.Second)})
	if err != nil || len(state.Holders) != 0 {
		t.Fatalf("suspension leaked holders = %#v, %v", state, err)
	}
}

func TestSchedulerAdmissionErrorRollsBackEveryInMemoryProjection(t *testing.T) {
	ctx := context.Background()
	store := inmemory.NewStore()
	base := time.Date(2026, 8, 24, 20, 20, 0, 0, time.UTC)
	id := invocationID("resource-rollback", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, base.Add(10*time.Second))

	initial := resourceRequirements(t, id.RunID, workflowruntime.SchedulerLimits{Workers: 1}, workflowruntime.SchedulerDemand{})
	failed := schedulerAdmission(id, 0, "rollback", initial, base.Add(5*time.Second), base.Add(time.Minute))
	if _, err := store.AdmitNode(ctx, failed); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("regressing admission = %v", err)
	}
	state, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: id.RunID, Now: base.Add(11 * time.Second)})
	if err != nil || len(state.Holders) != 0 || len(state.Waiters) != 0 {
		t.Fatalf("failed admission residue = %#v, %v", state, err)
	}

	changed := resourceRequirements(t, id.RunID, workflowruntime.SchedulerLimits{Workers: 2}, workflowruntime.SchedulerDemand{})
	retry := schedulerAdmission(id, 0, "rollback", changed, base.Add(11*time.Second), base.Add(time.Minute))
	result, err := store.AdmitNode(ctx, retry)
	if err != nil || !result.Claim.Acquired || result.Claim.Replayed {
		t.Fatalf("post-rollback changed-limit admission = %#v, %v", result, err)
	}
	state, err = store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: id.RunID, Now: base.Add(11 * time.Second)})
	if err != nil || len(state.Holders) != 1 || len(state.Waiters) != 0 {
		t.Fatalf("post-rollback resource state = %#v, %v", state, err)
	}
}

func TestSchedulerAdmissionContentionAndFanOutOccupancy(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 20, 30, 0, 0, time.UTC)
	t.Run("global_and_cross_run", func(t *testing.T) {
		store := inmemory.NewStore()
		const contenders = 32
		ids := make([]workflowruntime.NodeInvocationID, contenders)
		for index := range ids {
			ids[index] = invocationID(workflowruntime.RunID(fmt.Sprintf("contention-run-%02d", index)), "work")
			createNode(t, store, ids[index], workflowruntime.NodeReady, 0, base)
		}
		limits := workflowruntime.SchedulerLimits{Workers: 8, Named: map[string]int{"singleton": 1}}
		var acquired atomic.Int64
		var unexpected atomic.Int64
		var wg sync.WaitGroup
		for index, id := range ids {
			wg.Add(1)
			go func(index int, id workflowruntime.NodeInvocationID) {
				defer wg.Done()
				requirements, requirementErr := workflowruntime.BuildSchedulerRequirements(id.RunID, limits, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "singleton"}}})
				if requirementErr != nil {
					unexpected.Add(1)
					return
				}
				result, err := store.AdmitNode(ctx, schedulerAdmission(id, 0, fmt.Sprintf("race-%02d", index), requirements, base.Add(time.Second), base.Add(time.Minute)))
				if err != nil {
					unexpected.Add(1)
					return
				}
				if result.Claim.Acquired {
					acquired.Add(1)
				}
			}(index, id)
		}
		wg.Wait()
		if acquired.Load() != 1 || unexpected.Load() != 0 {
			t.Fatalf("acquired=%d unexpected=%d", acquired.Load(), unexpected.Load())
		}
		state, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(time.Second)})
		if err != nil || len(state.Holders) != 2 || len(state.Waiters) != contenders-1 {
			t.Fatalf("contention diagnostics = holders %d waiters %d err %v", len(state.Holders), len(state.Waiters), err)
		}
	})

	t.Run("waiting_item_keeps_logical_slot", func(t *testing.T) {
		store := inmemory.NewStore()
		runID := workflowruntime.RunID("resource-fanout")
		parent := invocationID(runID, "bulk")
		createRun(t, store, runID, base)
		createNode(t, store, parent, workflowruntime.NodePending, 0, base)
		expanded, err := (workflowruntime.FanOutCoordinator{Store: store}).Expand(ctx, workflowruntime.FanOutExpandCommand{
			Parent: parent, ExpectedParentGeneration: 1,
			Spec: graph.ForEachSpec{Items: graph.Expression{Text: `["a", "b"]`}, MaxConcurrency: 1}, At: base.Add(time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
		limits := workflowruntime.SchedulerLimits{Workers: 4}
		requirements := resourceRequirements(t, runID, limits, workflowruntime.SchedulerDemand{})
		first := expanded.Children[0]
		admitted, err := store.AdmitNode(ctx, schedulerAdmission(first.ID, 0, "fanout-first", requirements, base.Add(2*time.Second), base.Add(time.Minute)))
		if err != nil || !admitted.Claim.Acquired {
			t.Fatalf("first fan-out admission = %#v, %v", admitted, err)
		}
		claimed, _ := store.LoadNodeInvocation(ctx, first.ID)
		proof := proofFromLease(*admitted.Claim.Lease)
		started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: first.ID, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: testExecutor(), Inputs: first.Inputs, At: base.Add(3 * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		if _, waitErr := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: first.ID, ExpectedGeneration: started.Node.Generation, To: workflowruntime.NodeWaiting, Claim: &proof, At: base.Add(4 * time.Second)}); waitErr != nil {
			t.Fatal(waitErr)
		}
		state, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: runID, Now: base.Add(4 * time.Second)})
		if err != nil || len(state.Holders) != 0 {
			t.Fatalf("waiting child worker holders = %#v, %v", state, err)
		}
		second := expanded.Children[1]
		blocked, err := store.AdmitNode(ctx, schedulerAdmission(second.ID, 0, "fanout-second", requirements, base.Add(5*time.Second), base.Add(time.Minute)))
		if err != nil || blocked.Claim.Acquired || len(blocked.Blocked) != 1 || blocked.Blocked[0].Kind != workflowruntime.SchedulerResourceFanOut {
			t.Fatalf("fan-out admission = %#v, %v", blocked, err)
		}
	})
}

func TestSchedulerResourcesReleaseOnRetryAndEveryAttemptTerminal(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 8, 24, 20, 45, 0, 0, time.UTC)
	for _, status := range []workflowruntime.NodeStatus{workflowruntime.NodeSucceeded, workflowruntime.NodeFailed, workflowruntime.NodeTimedOut, workflowruntime.NodeCrashed} {
		t.Run(string(status), func(t *testing.T) {
			store := inmemory.NewStore()
			id := invocationID(workflowruntime.RunID("release-"+status), "work")
			createNode(t, store, id, workflowruntime.NodeReady, 0, base)
			requirements := resourceRequirements(t, id.RunID, workflowruntime.SchedulerLimits{Workers: 2, Capabilities: map[string]int{"network": 1}}, workflowruntime.SchedulerDemand{Capabilities: []string{"network"}})
			admitted, err := store.AdmitNode(ctx, schedulerAdmission(id, 0, string(status), requirements, base.Add(time.Second), base.Add(time.Minute)))
			if err != nil || !admitted.Claim.Acquired {
				t.Fatalf("admission = %#v, %v", admitted, err)
			}
			claimed, _ := store.LoadNodeInvocation(ctx, id)
			proof := proofFromLease(*admitted.Claim.Lease)
			started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: testExecutor(), At: base.Add(2 * time.Second)})
			if err != nil {
				t.Fatal(err)
			}
			var failure *workflowruntime.Failure
			if status != workflowruntime.NodeSucceeded {
				failure = &workflowruntime.Failure{Code: "terminal_" + string(status), Message: "terminal resource release"}
				if status == workflowruntime.NodeTimedOut {
					failure.Details = map[string]string{"timeout_kind": string(workflowruntime.TimeoutExecution)}
				}
			}
			if _, finishErr := store.FinishNodeAttempt(ctx, workflowruntime.FinishNodeAttemptRequest{InvocationID: id, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: status, NextNodeStatus: status, Failure: failure, At: base.Add(3 * time.Second)}); finishErr != nil {
				t.Fatal(finishErr)
			}
			state, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: id.RunID, Now: base.Add(3 * time.Second)})
			if err != nil || len(state.Holders) != 0 {
				t.Fatalf("terminal holders = %#v, %v", state, err)
			}
		})
	}

	store := inmemory.NewStore()
	id := invocationID("release-retry", "work")
	createNode(t, store, id, workflowruntime.NodeReady, 0, base)
	requirements := resourceRequirements(t, id.RunID, workflowruntime.SchedulerLimits{Workers: 2}, workflowruntime.SchedulerDemand{})
	admitted, err := store.AdmitNode(ctx, schedulerAdmission(id, 0, "retry", requirements, base.Add(time.Second), base.Add(time.Minute)))
	if err != nil || !admitted.Claim.Acquired {
		t.Fatalf("retry admission = %#v, %v", admitted, err)
	}
	claimed, _ := store.LoadNodeInvocation(ctx, id)
	proof := proofFromLease(*admitted.Claim.Lease)
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: id, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: testExecutor(), At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	failure := workflowruntime.Failure{Code: "retryable", Message: "retry later", Retryable: true}
	scheduled, err := store.ScheduleNodeRetry(ctx, workflowruntime.ScheduleNodeRetryRequest{
		Activation:             workflowruntime.RetryActivationSnapshot{ID: "resource-retry", Attempt: started.Attempt.ID, Failure: failure, FireAt: base.Add(time.Minute), Status: workflowruntime.RetryScheduled},
		ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation,
		Claim: proof, AttemptStatus: workflowruntime.NodeFailed, At: base.Add(3 * time.Second),
	})
	if err != nil || scheduled.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("schedule retry = %#v, %v", scheduled, err)
	}
	state, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: id.RunID, Now: base.Add(3 * time.Second)})
	if err != nil || len(state.Holders) != 0 {
		t.Fatalf("retry delay holders = %#v, %v", state, err)
	}
}

func TestResourceReadyQueuePreservesFIFOAndFairnessHooks(t *testing.T) {
	ctx := context.Background()
	store := inmemory.NewStore()
	base := time.Date(2026, 8, 24, 21, 0, 0, 0, time.UTC)
	low := invocationID("fair-run-a", "low")
	high := invocationID("fair-run-b", "high")
	createNode(t, store, low, workflowruntime.NodeReady, 1, base.Add(-time.Minute))
	createNode(t, store, high, workflowruntime.NodeReady, 100, base)
	requirements := func(_ context.Context, candidate workflowruntime.ReadyCandidate) ([]workflowruntime.SchedulerResourceRequirement, error) {
		return workflowruntime.BuildSchedulerRequirements(candidate.InvocationID.RunID, workflowruntime.SchedulerLimits{Workers: 1}, workflowruntime.SchedulerDemand{})
	}
	fifo := workflowruntime.NewResourceReadyQueueCoordinator(store, nil, workflowruntime.SchedulerAdmissionPolicyFunc(requirements))
	claim, ok, err := fifo.ClaimNext(ctx, workflowruntime.ReadyClaimRequest{Owner: "fifo", Token: "fifo", IdempotencyKey: "fifo", Now: base.Add(time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || !ok || claim.Candidate.InvocationID != low {
		t.Fatalf("resource FIFO = %#v, %v, %v", claim, ok, err)
	}
	if releaseErr := fifo.Release(ctx, workflowruntime.ReleaseClaimRequest{InvocationID: low, Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation, Now: base.Add(2 * time.Second)}); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	priority := workflowruntime.ReadyQueuePolicyFunc(func(_ context.Context, candidates []workflowruntime.ReadyCandidate) ([]workflowruntime.NodeInvocationID, error) {
		sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Priority > candidates[j].Priority })
		ids := make([]workflowruntime.NodeInvocationID, len(candidates))
		for index := range candidates {
			ids[index] = candidates[index].InvocationID
		}
		return ids, nil
	})
	queue := workflowruntime.NewResourceReadyQueueCoordinator(store, priority, workflowruntime.SchedulerAdmissionPolicyFunc(requirements))
	claim, ok, err = queue.ClaimNext(ctx, workflowruntime.ReadyClaimRequest{Owner: "priority", Token: "priority", IdempotencyKey: "priority", Now: base.Add(3 * time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || !ok || claim.Candidate.InvocationID != high {
		t.Fatalf("resource priority = %#v, %v, %v", claim, ok, err)
	}
	if releaseErr := queue.Release(ctx, workflowruntime.ReleaseClaimRequest{InvocationID: high, Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation, Now: base.Add(4 * time.Second)}); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	perRun := workflowruntime.ReadyQueuePolicyFunc(func(_ context.Context, candidates []workflowruntime.ReadyCandidate) ([]workflowruntime.NodeInvocationID, error) {
		for _, candidate := range candidates {
			if candidate.InvocationID.RunID == high.RunID {
				return []workflowruntime.NodeInvocationID{candidate.InvocationID}, nil
			}
		}
		return nil, nil
	})
	queue = workflowruntime.NewResourceReadyQueueCoordinator(store, perRun, workflowruntime.SchedulerAdmissionPolicyFunc(requirements))
	claim, ok, err = queue.ClaimNext(ctx, workflowruntime.ReadyClaimRequest{Owner: "fair", Token: "fair", IdempotencyKey: "fair", Now: base.Add(5 * time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || !ok || claim.Candidate.InvocationID != high {
		t.Fatalf("resource per-run fairness = %#v, %v, %v", claim, ok, err)
	}
}

func TestResourceReadyQueueRejectsMissingAndTypedNilAdmission(t *testing.T) {
	now := time.Date(2026, 8, 24, 21, 30, 0, 0, time.UTC)
	request := workflowruntime.ReadyClaimRequest{Owner: "worker", Token: "token", IdempotencyKey: "nil-admission", Now: now, LeaseUntil: now.Add(time.Minute)}
	if _, _, err := workflowruntime.NewResourceReadyQueueCoordinator(inmemory.NewStore(), nil, nil).ClaimNext(context.Background(), request); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("nil admission = %v", err)
	}
	var typedNil workflowruntime.SchedulerAdmissionPolicyFunc
	if _, _, err := workflowruntime.NewResourceReadyQueueCoordinator(inmemory.NewStore(), nil, typedNil).ClaimNext(context.Background(), request); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("typed-nil admission = %v", err)
	}
}

func resourceRequirements(t *testing.T, runID workflowruntime.RunID, limits workflowruntime.SchedulerLimits, demand workflowruntime.SchedulerDemand) []workflowruntime.SchedulerResourceRequirement {
	t.Helper()
	requirements, err := workflowruntime.BuildSchedulerRequirements(runID, limits, demand)
	if err != nil {
		t.Fatal(err)
	}
	return requirements
}

func schedulerAdmission(id workflowruntime.NodeInvocationID, generation uint64, suffix string, requirements []workflowruntime.SchedulerResourceRequirement, now, until time.Time) workflowruntime.AdmitNodeRequest {
	return workflowruntime.AdmitNodeRequest{
		Claim: workflowruntime.ClaimNodeRequest{
			InvocationID: id, ExpectedClaimGeneration: generation, Owner: "worker-" + suffix, Token: "token-" + suffix,
			IdempotencyKey: "admit-" + suffix, Now: now, LeaseUntil: until,
		},
		Requirements: requirements, EnqueuedAt: now.Add(-time.Minute),
	}
}

func proofFromLease(lease workflowruntime.ClaimLease) workflowruntime.ClaimProof {
	return workflowruntime.ClaimProof{Owner: lease.Owner, Token: lease.Token, Generation: lease.Generation}
}
