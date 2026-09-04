package persistence

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

func TestWorkflowSQLiteSchedulerAdmissionTwoHandlesRestartAndExpiry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scheduler.db")
	firstStore, first := openWorkflowStateTest(t, path)
	base := workflowTestTime()
	const contenders = 20
	ids := make([]workflowruntime.NodeInvocationID, contenders)
	for index := range ids {
		run := createWorkflowTestRun(t, first, fmt.Sprintf("scheduler-run-%02d", index), base)
		node := createWorkflowTestNode(t, first, run.ID, "work", base)
		ready, err := first.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = ready.Snapshot.ID
	}
	secondStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, stateErr := NewWorkflowStateStore(secondStore)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	limits := workflowruntime.SchedulerLimits{Workers: contenders, Named: map[string]int{"cross-run": 1}}
	requests := make([]workflowruntime.AdmitNodeRequest, contenders)
	results := make([]workflowruntime.AdmitNodeResult, contenders)
	errs := make([]error, contenders)
	var wg sync.WaitGroup
	for index, id := range ids {
		requirements := workflowSchedulerRequirements(t, id.RunID, limits, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "cross-run"}}})
		requests[index] = workflowSchedulerAdmission(id, fmt.Sprintf("contender-%02d", index), requirements, base.Add(time.Second), base.Add(time.Minute))
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			stores := []*WorkflowStateStore{first, second}
			results[index], errs[index] = stores[index%len(stores)].AdmitNode(ctx, requests[index])
		}(index)
	}
	wg.Wait()
	winner := -1
	for index := range results {
		if errs[index] != nil {
			t.Fatalf("admission %d = %v", index, errs[index])
		}
		if results[index].Claim.Acquired {
			if winner >= 0 {
				t.Fatalf("multiple cross-run owners %d and %d", winner, index)
			}
			winner = index
		}
	}
	if winner < 0 {
		t.Fatal("no cross-run owner acquired")
	}
	state, err := second.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(time.Second)})
	if err != nil || len(state.Holders) != 2 || len(state.Waiters) != contenders-1 {
		t.Fatalf("two-handle scheduler state holders=%#v waiters=%d err=%v", state.Holders, len(state.Waiters), err)
	}
	for _, holder := range state.Holders {
		if holder.Invocation != ids[winner] {
			t.Fatalf("holder identity = %#v, winner %v", holder, ids[winner])
		}
	}

	replay, err := second.AdmitNode(ctx, requests[winner])
	if err != nil || !replay.Claim.Acquired || !replay.Claim.Replayed {
		t.Fatalf("cross-handle admission replay = %#v, %v", replay, err)
	}
	changed := requests[winner]
	changed.Requirements = append([]workflowruntime.SchedulerResourceRequirement(nil), changed.Requirements...)
	for index := range changed.Requirements {
		if changed.Requirements[index].Resource.Kind == workflowruntime.SchedulerResourceKey {
			changed.Requirements[index].Limit++
		}
	}
	if _, replayErr := first.AdmitNode(ctx, changed); !errors.Is(replayErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed admission replay = %v", replayErr)
	}
	winnerLease := results[winner].Claim.Lease
	renewed, err := first.RenewNodeLease(ctx, workflowruntime.RenewLeaseRequest{InvocationID: ids[winner], Owner: winnerLease.Owner, Token: winnerLease.Token, Generation: winnerLease.Generation, Now: base.Add(2 * time.Second), LeaseUntil: base.Add(2 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}

	if closeErr := firstStore.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, err := NewWorkflowStateStore(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	state, err = reopened.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(3 * time.Second)})
	if err != nil || len(state.Holders) != 2 {
		t.Fatalf("reopened holders = %#v, %v", state, err)
	}
	for _, holder := range state.Holders {
		if !holder.ExpiresAt.Equal(renewed.ExpiresAt) {
			t.Fatalf("reopened renewal = %#v", holder)
		}
	}
	state, err = reopened.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(3 * time.Minute)})
	if err != nil || len(state.Holders) != 0 {
		t.Fatalf("expired holders = %#v, %v", state, err)
	}
	replacementIndex := (winner + 1) % contenders
	replacementRequest := requests[replacementIndex]
	replacementRequest.Claim.IdempotencyKey += "-after-expiry"
	replacementRequest.Claim.Owner += "-after-expiry"
	replacementRequest.Claim.Token += "-after-expiry"
	replacementRequest.Claim.Now = base.Add(3 * time.Minute)
	replacementRequest.Claim.LeaseUntil = base.Add(4 * time.Minute)
	replacementRequest.EnqueuedAt = base
	replacement, err := reopened.AdmitNode(ctx, replacementRequest)
	if err != nil || !replacement.Claim.Acquired {
		t.Fatalf("post-expiry replacement = %#v, %v", replacement, err)
	}
	if releaseErr := reopened.ReleaseNodeClaim(ctx, workflowruntime.ReleaseClaimRequest{InvocationID: ids[replacementIndex], Owner: replacement.Claim.Lease.Owner, Token: replacement.Claim.Lease.Token, Generation: replacement.Claim.Lease.Generation, Now: base.Add(3*time.Minute + time.Second)}); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	state, err = reopened.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(3*time.Minute + time.Second)})
	if err != nil || len(state.Holders) != 0 {
		t.Fatalf("released replacement holders = %#v, %v", state, err)
	}
}

func TestWorkflowSQLiteSchedulerAtomicRollbackLimitConflictAndSuspension(t *testing.T) {
	ctx := context.Background()
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "scheduler-rollback.db"))
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "scheduler-rollback", base)
	node := createWorkflowTestNode(t, state, run.ID, "work", base)
	ready, transitionErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
	if transitionErr != nil {
		t.Fatal(transitionErr)
	}
	rollbackRequirements := workflowSchedulerRequirements(t, run.ID, workflowruntime.SchedulerLimits{Workers: 2, Named: map[string]int{"rollback-key": 1}}, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "rollback-key"}}})
	badCAS := workflowSchedulerAdmission(ready.Snapshot.ID, "bad-cas", rollbackRequirements, base.Add(time.Second), base.Add(time.Minute))
	badCAS.Claim.ExpectedClaimGeneration = 1
	if _, admissionErr := state.AdmitNode(ctx, badCAS); !errors.Is(admissionErr, workflowruntime.ErrCASMismatch) {
		t.Fatalf("bad admission CAS = %v", admissionErr)
	}
	afterBadCAS, _ := state.LoadNodeInvocation(ctx, node.ID)
	if afterBadCAS.Generation != ready.Snapshot.Generation || afterBadCAS.Lease != nil {
		t.Fatalf("failed admission changed node = %#v", afterBadCAS)
	}
	var rollbackDefinitions int
	if queryErr := store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_scheduler_resources WHERE resource_json LIKE '%rollback-key%'`).Scan(&rollbackDefinitions); queryErr != nil || rollbackDefinitions != 0 {
		t.Fatalf("failed admission definitions = %d, %v", rollbackDefinitions, queryErr)
	}

	admitted, err := state.AdmitNode(ctx, workflowSchedulerAdmission(node.ID, "good", rollbackRequirements, base.Add(2*time.Second), base.Add(time.Minute)))
	if err != nil || !admitted.Claim.Acquired {
		t.Fatalf("good admission = %#v, %v", admitted, err)
	}
	otherRun := createWorkflowTestRun(t, state, "scheduler-limit-conflict", base)
	otherNode := createWorkflowTestNode(t, state, otherRun.ID, "work", base)
	otherReady, _ := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: otherNode.ID, ExpectedGeneration: otherNode.Generation, To: workflowruntime.NodeReady, At: base})
	conflicting := workflowSchedulerRequirements(t, otherRun.ID, workflowruntime.SchedulerLimits{Workers: 2, Named: map[string]int{"rollback-key": 2}}, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "rollback-key"}}})
	if _, admissionErr := state.AdmitNode(ctx, workflowSchedulerAdmission(otherReady.Snapshot.ID, "limit-conflict", conflicting, base.Add(3*time.Second), base.Add(time.Minute))); !errors.Is(admissionErr, workflowruntime.ErrInvalidSchedulerResource) {
		t.Fatalf("durable limit conflict = %v", admissionErr)
	}
	otherAfter, _ := state.LoadNodeInvocation(ctx, otherNode.ID)
	if otherAfter.Lease != nil || otherAfter.Generation != otherReady.Snapshot.Generation {
		t.Fatalf("limit conflict changed node = %#v", otherAfter)
	}
	rollbackRun := createWorkflowTestRun(t, state, "scheduler-holder-rollback", base)
	rollbackNode := createWorkflowTestNode(t, state, rollbackRun.ID, "work", base)
	rollbackReady, _ := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: rollbackNode.ID, ExpectedGeneration: rollbackNode.Generation, To: workflowruntime.NodeReady, At: base})
	if _, triggerErr := store.DB().Exec(`CREATE TRIGGER scheduler_test_reject_holder BEFORE INSERT ON workflow_scheduler_holders BEGIN SELECT RAISE(ABORT, 'reject holder'); END`); triggerErr != nil {
		t.Fatal(triggerErr)
	}
	rollbackRequest := workflowSchedulerAdmission(rollbackNode.ID, "holder-rollback", workflowSchedulerRequirements(t, rollbackRun.ID, workflowruntime.SchedulerLimits{Workers: 2}, workflowruntime.SchedulerDemand{}), base.Add(3*time.Second), base.Add(time.Minute))
	if _, admissionErr := state.AdmitNode(ctx, rollbackRequest); admissionErr == nil {
		t.Fatal("forced holder failure succeeded")
	}
	if _, triggerErr := store.DB().Exec(`DROP TRIGGER scheduler_test_reject_holder`); triggerErr != nil {
		t.Fatal(triggerErr)
	}
	rollbackAfter, _ := state.LoadNodeInvocation(ctx, rollbackNode.ID)
	if rollbackAfter.Generation != rollbackReady.Snapshot.Generation || rollbackAfter.Lease != nil {
		t.Fatalf("holder insertion rollback changed node = %#v", rollbackAfter)
	}
	var rollbackHolders, rollbackIdempotency int
	if queryErr := store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_scheduler_holders WHERE run_id = ?`, rollbackRun.ID).Scan(&rollbackHolders); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_scheduler_admission_idempotency WHERE idempotency_key = ?`, rollbackRequest.Claim.IdempotencyKey).Scan(&rollbackIdempotency); queryErr != nil {
		t.Fatal(queryErr)
	}
	if rollbackHolders != 0 || rollbackIdempotency != 0 {
		t.Fatalf("holder insertion rollback left holders=%d idempotency=%d", rollbackHolders, rollbackIdempotency)
	}

	claimed, _ := state.LoadNodeInvocation(ctx, node.ID)
	proof := workflowruntime.ClaimProof{Owner: admitted.Claim.Lease.Owner, Token: admitted.Claim.Lease.Token, Generation: admitted.Claim.Lease.Generation}
	started, err := state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: claimed.Generation, Claim: proof, Executor: workflowTestExecutor(), At: base.Add(4 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: started.Node.Generation, To: workflowruntime.NodeWaiting, Claim: &proof, At: base.Add(5 * time.Second)})
	if err != nil || waiting.Snapshot.Lease != nil {
		t.Fatalf("waiting transition = %#v, %v", waiting, err)
	}
	resources, err := state.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: run.ID, Now: base.Add(5 * time.Second)})
	if err != nil || len(resources.Holders) != 0 {
		t.Fatalf("waiting transition leaked resources = %#v, %v", resources, err)
	}
}

func TestWorkflowSQLiteSchedulerAdmissionErrorRollbackParity(t *testing.T) {
	ctx := context.Background()
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "scheduler-error-rollback.db"))
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "scheduler-error-rollback", base.Add(10*time.Second))
	node := createWorkflowTestNode(t, state, run.ID, "work", base.Add(10*time.Second))
	ready, transitionErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base.Add(10 * time.Second)})
	if transitionErr != nil {
		t.Fatal(transitionErr)
	}
	initial := workflowSchedulerRequirements(t, run.ID, workflowruntime.SchedulerLimits{Workers: 1}, workflowruntime.SchedulerDemand{})
	failed := workflowSchedulerAdmission(ready.Snapshot.ID, "error-rollback", initial, base.Add(5*time.Second), base.Add(time.Minute))
	if _, admissionErr := state.AdmitNode(ctx, failed); !errors.Is(admissionErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SQLite regressing admission = %v", admissionErr)
	}
	for _, check := range []struct{ name, query string }{
		{name: "definitions", query: `SELECT COUNT(1) FROM workflow_scheduler_resources`},
		{name: "holders", query: `SELECT COUNT(1) FROM workflow_scheduler_holders`},
		{name: "waiters", query: `SELECT COUNT(1) FROM workflow_scheduler_waiters`},
		{name: "idempotency", query: `SELECT COUNT(1) FROM workflow_scheduler_admission_idempotency`},
	} {
		var count int
		if queryErr := store.DB().QueryRow(check.query).Scan(&count); queryErr != nil || count != 0 {
			t.Fatalf("SQLite failed admission left %s=%d, %v", check.name, count, queryErr)
		}
	}
	changed := workflowSchedulerRequirements(t, run.ID, workflowruntime.SchedulerLimits{Workers: 2}, workflowruntime.SchedulerDemand{})
	retry := workflowSchedulerAdmission(ready.Snapshot.ID, "error-rollback", changed, base.Add(11*time.Second), base.Add(time.Minute))
	result, admissionErr := state.AdmitNode(ctx, retry)
	if admissionErr != nil || !result.Claim.Acquired || result.Claim.Replayed {
		t.Fatalf("SQLite post-rollback changed-limit admission = %#v, %v", result, admissionErr)
	}
}

func TestWorkflowSQLiteSchedulerProjectionCorruptionFailsClosed(t *testing.T) {
	ctx := context.Background()
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "scheduler-corruption.db"))
	base := workflowTestTime()
	limits := workflowruntime.SchedulerLimits{Workers: 4, Named: map[string]int{"integrity-key": 1}}
	ids := make([]workflowruntime.NodeInvocationID, 2)
	for index := range ids {
		run := createWorkflowTestRun(t, state, fmt.Sprintf("scheduler-corruption-%d", index), base)
		node := createWorkflowTestNode(t, state, run.ID, "work", base)
		ready, transitionErr := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
		if transitionErr != nil {
			t.Fatal(transitionErr)
		}
		ids[index] = ready.Snapshot.ID
	}
	firstRequirements := workflowSchedulerRequirements(t, ids[0].RunID, limits, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "integrity-key"}}})
	first, admissionErr := state.AdmitNode(ctx, workflowSchedulerAdmission(ids[0], "integrity-first", firstRequirements, base.Add(time.Second), base.Add(time.Minute)))
	if admissionErr != nil || !first.Claim.Acquired {
		t.Fatalf("integrity first admission = %#v, %v", first, admissionErr)
	}
	secondRequirements := workflowSchedulerRequirements(t, ids[1].RunID, limits, workflowruntime.SchedulerDemand{Concurrency: []graph.ConcurrencyClaim{{Resource: "integrity-key"}}})
	second, admissionErr := state.AdmitNode(ctx, workflowSchedulerAdmission(ids[1], "integrity-second", secondRequirements, base.Add(2*time.Second), base.Add(time.Minute)))
	if admissionErr != nil || second.Claim.Acquired || len(second.Blocked) == 0 {
		t.Fatalf("integrity second admission = %#v, %v", second, admissionErr)
	}

	if _, updateErr := store.DB().Exec(`UPDATE workflow_scheduler_holders SET units = units + 1 WHERE run_id = ?`, ids[0].RunID); updateErr != nil {
		t.Fatal(updateErr)
	}
	if _, inspectErr := state.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(2 * time.Second)}); !errors.Is(inspectErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("holder projection corruption = %v", inspectErr)
	}
	if _, updateErr := store.DB().Exec(`UPDATE workflow_scheduler_holders SET units = units - 1 WHERE run_id = ?`, ids[0].RunID); updateErr != nil {
		t.Fatal(updateErr)
	}
	if _, updateErr := store.DB().Exec(`UPDATE workflow_scheduler_waiters SET priority = priority + 7 WHERE run_id = ?`, ids[1].RunID); updateErr != nil {
		t.Fatal(updateErr)
	}
	if _, inspectErr := state.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(2 * time.Second)}); !errors.Is(inspectErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("waiter projection corruption = %v", inspectErr)
	}
	if _, updateErr := store.DB().Exec(`UPDATE workflow_scheduler_waiters SET priority = priority - 7 WHERE run_id = ?`, ids[1].RunID); updateErr != nil {
		t.Fatal(updateErr)
	}
	if _, dropErr := store.DB().Exec(`DROP TRIGGER workflow_scheduler_resources_immutable`); dropErr != nil {
		t.Fatal(dropErr)
	}
	if _, updateErr := store.DB().Exec(`UPDATE workflow_scheduler_resources SET resource_json = '{"kind":"concurrency_key","name":"forged"}' WHERE resource_json LIKE '%integrity-key%'`); updateErr != nil {
		t.Fatal(updateErr)
	}
	if _, inspectErr := state.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{Now: base.Add(2 * time.Second)}); !errors.Is(inspectErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("resource projection corruption = %v", inspectErr)
	}
}

func TestWorkflowSQLiteFailFastZeroFinalizerRestartReplayAndCancellationRace(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "run-policy.db")
	store, state := openWorkflowStateTest(t, path)
	base := workflowTestTime()
	run := createWorkflowTestRun(t, state, "policy-zero", base)
	runningRun, transitionErr := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base})
	if transitionErr != nil {
		t.Fatal(transitionErr)
	}
	source := createWorkflowTestNode(t, state, run.ID, "source", base)
	running := createWorkflowTestNode(t, state, run.ID, "running", base)
	failedSource := finishWorkflowPolicyNode(t, state, source, workflowruntime.NodeFailed, base.Add(time.Second))
	runningStarted := startWorkflowPolicyNode(t, state, running, base.Add(2*time.Second), "running")
	workflow := graph.Graph{ID: run.Plan.ID, Version: run.Plan.Version, Completion: &graph.RunCompletionPolicy{Mode: graph.CompletionFailFast}, Nodes: []graph.Node{{ID: "source"}, {ID: "running"}}}
	coordinator := workflowruntime.NewRunPolicyCoordinator(state, state, state)
	result, policyErr := coordinator.HandleFailure(ctx, workflow, source.ID, "sqlite-zero-policy", base.Add(3*time.Second))
	if policyErr != nil || result.Disposition != workflowruntime.RunFailureFailFast || len(result.Intent.Finalizers) != 0 || len(result.Intents) != 1 || result.Run.Generation != runningRun.Snapshot.Generation+1 {
		t.Fatalf("SQLite zero-finalizer policy = %#v, %v", result, policyErr)
	}
	if result.Decision.SourceGeneration != failedSource.Node.Generation || result.Intents[0].Attempt == nil || *result.Intents[0].Attempt != runningStarted.Attempt.ID {
		t.Fatalf("SQLite policy binding = %#v", result)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, stateErr := NewWorkflowStateStore(reopenedStore)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	replay, replayErr := workflowruntime.NewRunPolicyCoordinator(reopened, reopened, reopened).HandleFailure(ctx, workflow, source.ID, "sqlite-zero-policy", base.Add(3*time.Second))
	if replayErr != nil || replay.Disposition != workflowruntime.RunFailureFailFast || replay.Decision != result.Decision {
		t.Fatalf("SQLite policy replay = %#v, %v", replay, replayErr)
	}
	if _, completionErr := reopened.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: replay.Run.Generation, ExpectedIntentGeneration: replay.Intent.Generation, At: base.Add(4 * time.Second)}); !errors.Is(completionErr, workflowruntime.ErrControlFlowPending) {
		t.Fatalf("SQLite policy completed before cancellation = %v", completionErr)
	}
	if _, resolutionErr := reopened.ResolveCancellationIntent(ctx, workflowruntime.ResolveCancellationIntentRequest{IntentID: replay.Intents[0].ID, ExpectedGeneration: replay.Intents[0].Generation, At: base.Add(5 * time.Second)}); resolutionErr != nil {
		t.Fatal(resolutionErr)
	}
	currentRun, _ := reopened.LoadRun(ctx, run.ID)
	currentIntent, _ := reopened.LoadTerminalIntent(ctx, run.ID)
	completed, completionErr := reopened.CompleteTerminalIntent(ctx, workflowruntime.CompleteTerminalIntentRequest{RunID: run.ID, ExpectedRunGeneration: currentRun.Generation, ExpectedIntentGeneration: currentIntent.Generation, At: base.Add(6 * time.Second)})
	if completionErr != nil || completed.Run.Status != workflowruntime.RunFailed {
		t.Fatalf("SQLite policy completion = %#v, %v", completed, completionErr)
	}

	raceRun := createWorkflowTestRun(t, reopened, "policy-cancel-race", base.Add(10*time.Second))
	if _, raceTransitionErr := reopened.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: raceRun.ID, ExpectedGeneration: raceRun.Generation, To: workflowruntime.RunRunning, At: base.Add(10 * time.Second)}); raceTransitionErr != nil {
		t.Fatal(raceTransitionErr)
	}
	raceSource := createWorkflowTestNode(t, reopened, raceRun.ID, "source", base.Add(10*time.Second))
	finishWorkflowPolicyNode(t, reopened, raceSource, workflowruntime.NodeFailed, base.Add(11*time.Second))
	raceGraph := graph.Graph{ID: raceRun.Plan.ID, Version: raceRun.Plan.Version, Completion: &graph.RunCompletionPolicy{Mode: graph.CompletionFailFast}, Nodes: []graph.Node{{ID: "source"}}}
	secondStore, secondOpenErr := Open(path)
	if secondOpenErr != nil {
		t.Fatal(secondOpenErr)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, _ := NewWorkflowStateStore(secondStore)
	type raceOutcome struct {
		kind string
		err  error
	}
	outcomes := make(chan raceOutcome, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, policyErr := workflowruntime.NewRunPolicyCoordinator(reopened, reopened, reopened).HandleFailure(ctx, raceGraph, raceSource.ID, "policy-race", base.Add(12*time.Second))
		outcomes <- raceOutcome{kind: "policy", err: policyErr}
	}()
	go func() {
		defer wg.Done()
		current, loadErr := second.LoadRun(ctx, raceRun.ID)
		if loadErr != nil {
			outcomes <- raceOutcome{kind: "cancel", err: loadErr}
			return
		}
		_, cancelErr := second.RequestRunCancellation(ctx, workflowruntime.RequestRunCancellationRequest{RunID: raceRun.ID, ExpectedGeneration: current.Generation, IdempotencyKey: "cancel-race", Reason: workflowruntime.Failure{Code: "user_cancel", Message: "cancel race"}, At: base.Add(12 * time.Second)})
		outcomes <- raceOutcome{kind: "cancel", err: cancelErr}
	}()
	wg.Wait()
	close(outcomes)
	successes := 0
	for outcome := range outcomes {
		if outcome.err == nil {
			successes++
			continue
		}
		if !errors.Is(outcome.err, workflowruntime.ErrCASMismatch) && !errors.Is(outcome.err, workflowruntime.ErrInvalidRecord) && !errors.Is(outcome.err, workflowruntime.ErrIdempotencyConflict) {
			t.Fatalf("unexpected %s race error = %v", outcome.kind, outcome.err)
		}
	}
	if successes != 1 {
		t.Fatalf("policy/cancellation race successes = %d", successes)
	}
	var policyRows, terminalRows, cancellationRows int
	if queryErr := reopenedStore.DB().QueryRow(`SELECT COUNT(1) FROM workflow_run_policy_decisions WHERE run_id = ?`, raceRun.ID).Scan(&policyRows); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := reopenedStore.DB().QueryRow(`SELECT COUNT(1) FROM workflow_terminal_intents WHERE run_id = ?`, raceRun.ID).Scan(&terminalRows); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := reopenedStore.DB().QueryRow(`SELECT COUNT(1) FROM workflow_run_cancellation_idempotency WHERE idempotency_key = 'cancel-race'`).Scan(&cancellationRows); queryErr != nil {
		t.Fatal(queryErr)
	}
	if policyRows+cancellationsAsDecision(cancellationRows) != 1 || terminalRows != policyRows {
		t.Fatalf("race persistence policy=%d terminal=%d cancellation=%d", policyRows, terminalRows, cancellationRows)
	}

	publicRun := createWorkflowTestRun(t, reopened, "public-empty-finalizers", base.Add(20*time.Second))
	publicRunning, publicRunErr := reopened.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: publicRun.ID, ExpectedGeneration: publicRun.Generation, To: workflowruntime.RunRunning, At: base.Add(20 * time.Second)})
	if publicRunErr != nil {
		t.Fatal(publicRunErr)
	}
	publicFailure := workflowruntime.Failure{Code: "public_empty", Message: "public terminal intents require finalizers"}
	publicError, valueErr := workflowruntime.NewRunFailureValue(publicRun.ID, workflowruntime.RunFailed, publicFailure)
	if valueErr != nil {
		t.Fatal(valueErr)
	}
	if _, beginErr := reopened.BeginTerminalIntent(ctx, workflowruntime.BeginTerminalIntentRequest{RunID: publicRun.ID, ExpectedRunGeneration: publicRunning.Snapshot.Generation, IntendedStatus: workflowruntime.RunFailed, Reason: &publicFailure, ErrorValues: values.ValueSet{"error": publicError}, IdempotencyKey: "public-empty-finalizers", At: base.Add(21 * time.Second)}); !errors.Is(beginErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("SQLite public empty terminal intent = %v", beginErr)
	}
	if _, loadErr := reopened.LoadTerminalIntent(ctx, publicRun.ID); !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		t.Fatalf("SQLite public empty terminal intent persisted = %v", loadErr)
	}

	if _, dropErr := reopenedStore.DB().Exec(`DROP TRIGGER workflow_run_policy_decisions_immutable`); dropErr != nil {
		t.Fatal(dropErr)
	}
	if _, updateErr := reopenedStore.DB().Exec(`UPDATE workflow_run_policy_decisions SET created_at = ? WHERE run_id = ?`, workflowTime(base.Add(99*time.Second)), run.ID); updateErr != nil {
		t.Fatal(updateErr)
	}
	if _, loadErr := reopened.LoadRunPolicyDecision(ctx, run.ID); !errors.Is(loadErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("run policy projection corruption = %v", loadErr)
	}
}

func workflowSchedulerRequirements(t *testing.T, runID workflowruntime.RunID, limits workflowruntime.SchedulerLimits, demand workflowruntime.SchedulerDemand) []workflowruntime.SchedulerResourceRequirement {
	t.Helper()
	requirements, err := workflowruntime.BuildSchedulerRequirements(runID, limits, demand)
	if err != nil {
		t.Fatal(err)
	}
	return requirements
}

func workflowSchedulerAdmission(id workflowruntime.NodeInvocationID, suffix string, requirements []workflowruntime.SchedulerResourceRequirement, now, until time.Time) workflowruntime.AdmitNodeRequest {
	return workflowruntime.AdmitNodeRequest{Claim: workflowruntime.ClaimNodeRequest{InvocationID: id, Owner: "worker-" + suffix, Token: "token-" + suffix, IdempotencyKey: "admit-" + suffix, Now: now, LeaseUntil: until}, Requirements: requirements, EnqueuedAt: now.Add(-time.Minute)}
}

func startWorkflowPolicyNode(t *testing.T, state *WorkflowStateStore, node workflowruntime.NodeInvocationSnapshot, at time.Time, suffix string) workflowruntime.StartNodeAttemptResult {
	t.Helper()
	ready, err := state.TransitionNode(context.Background(), workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: at})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := state.ClaimNode(context.Background(), workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "policy-" + suffix, Token: "policy-token-" + suffix, IdempotencyKey: "policy-claim-" + suffix + "-" + string(node.ID.RunID), Now: at, LeaseUntil: at.Add(time.Minute)})
	if err != nil || !claim.Acquired {
		t.Fatalf("policy claim = %#v, %v", claim, err)
	}
	claimed, _ := state.LoadNodeInvocation(context.Background(), node.ID)
	started, err := state.StartNodeAttempt(context.Background(), workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: claimed.Generation, Claim: workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}, Executor: workflowTestExecutor(), At: at})
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func finishWorkflowPolicyNode(t *testing.T, state *WorkflowStateStore, node workflowruntime.NodeInvocationSnapshot, status workflowruntime.NodeStatus, at time.Time) workflowruntime.FinishNodeAttemptResult {
	t.Helper()
	started := startWorkflowPolicyNode(t, state, node, at, "finish-"+node.ID.NodeID)
	failure := workflowruntime.Failure{Code: "policy_" + string(status), Message: "policy test failure"}
	proof := workflowruntime.ClaimProof{Owner: "policy-finish-" + node.ID.NodeID, Token: "policy-token-finish-" + node.ID.NodeID, Generation: started.Node.ClaimGeneration}
	result, err := state.FinishNodeAttempt(context.Background(), workflowruntime.FinishNodeAttemptRequest{InvocationID: node.ID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: status, NextNodeStatus: status, Failure: &failure, At: at})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func cancellationsAsDecision(count int) int {
	if count > 0 {
		return 1
	}
	return 0
}
