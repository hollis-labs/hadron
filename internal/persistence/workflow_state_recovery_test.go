package persistence

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/go-workflow/values"
)

func TestWorkflowSQLiteNodeInputBindingReplaySurvivesLaterProgressAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binding-replay.db")
	store, state := openWorkflowStateTest(t, path)
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "binding-replay", base)
	if _, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	node := createWorkflowTestNode(t, state, run.ID, "work", base)
	request := workflowruntime.BindNodeInputsRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, IdempotencyKey: "bind-replay", Values: workflowTestValues(t, "bound"), At: base.Add(time.Second)}
	bound, bindErr := state.BindNodeInputs(ctx, request)
	if bindErr != nil || bound.Outcome != workflowruntime.IdempotencyApplied || bound.Node.Inputs == nil {
		t.Fatalf("BindNodeInputs = %#v, %v", bound, bindErr)
	}
	if _, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: bound.Node.Generation, To: workflowruntime.NodeReady, At: base.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
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
	replay, err := reopened.BindNodeInputs(ctx, request)
	if err != nil || replay.Outcome != workflowruntime.IdempotencyReplayed || replay.Node.Status != workflowruntime.NodePending || replay.Inputs != *bound.Node.Inputs {
		t.Fatalf("later-state binding replay = %#v, %v", replay, err)
	}
	current, err := reopened.LoadNodeInvocation(ctx, node.ID)
	if err != nil || current.Status != workflowruntime.NodeReady || current.Inputs == nil || *current.Inputs != replay.Inputs {
		t.Fatalf("durable later node = %#v, %v", current, err)
	}
}

func TestWorkflowSQLiteCrashReconciliationReleasesSchedulerHoldersAtomically(t *testing.T) {
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "crash-holder.db"))
	_ = store
	ctx, base := context.Background(), workflowTestTime()
	run := createWorkflowTestRun(t, state, "crash-holder", base)
	if _, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	node := createWorkflowTestNode(t, state, run.ID, "work", base)
	ready, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
	if err != nil {
		t.Fatal(err)
	}
	requirements := workflowSchedulerRequirements(t, run.ID, workflowruntime.SchedulerLimits{Workers: 1}, workflowruntime.SchedulerDemand{})
	admitted, err := state.AdmitNode(ctx, workflowSchedulerAdmission(ready.Snapshot.ID, "crash", requirements, base.Add(time.Second), base.Add(2*time.Second)))
	if err != nil || !admitted.Claim.Acquired || admitted.Claim.Lease == nil {
		t.Fatalf("AdmitNode = %#v, %v", admitted, err)
	}
	current, _ := state.LoadNodeInvocation(ctx, node.ID)
	proof := workflowruntime.ClaimProof{Owner: admitted.Claim.Lease.Owner, Token: admitted.Claim.Lease.Token, Generation: admitted.Claim.Lease.Generation}
	started, err := state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: current.Generation, Claim: proof, Executor: workflowTestExecutor(), At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := state.ReconcileCrashedAttempt(ctx, workflowruntime.ReconcileCrashedAttemptRequest{Attempt: started.Attempt.ID, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, IdempotencyKey: "crash-holder", Decision: workflowruntime.CrashRecoveryDecision{Action: workflowruntime.CrashTerminal, Policy: workflowruntime.RepeatPolicyDecision{Code: "denied", Reason: "unsafe"}}, At: base.Add(2 * time.Second)})
	if err != nil || result.Node.Status != workflowruntime.NodeCrashed {
		t.Fatalf("ReconcileCrashedAttempt = %#v, %v", result, err)
	}
	resources, err := state.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: run.ID, Now: base.Add(2 * time.Second)})
	if err != nil || len(resources.Holders) != 0 {
		t.Fatalf("scheduler holders after crash = %#v, %v", resources.Holders, err)
	}
	var idempotencyRows int
	if err := state.db.QueryRow(`SELECT COUNT(1) FROM workflow_crash_recovery_idempotency WHERE idempotency_key = 'crash-holder'`).Scan(&idempotencyRows); err != nil || idempotencyRows != 1 {
		t.Fatalf("crash idempotency rows = %d, %v", idempotencyRows, err)
	}
}

func TestWorkflowSQLiteRestartRecoversDueWaitAndReadiesDownstreamInOnePass(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wait-recovery-pass.db")
	store, state := openWorkflowStateTest(t, path)
	ctx, base := context.Background(), workflowTestTime()
	fixture := prepareWorkflowSQLiteWait(t, state, "recovery-pass", base, 10*time.Second)
	run, loadErr := state.LoadRun(ctx, fixture.invocation.RunID)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if _, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	downstream := createWorkflowTestNode(t, state, run.ID, "downstream", base.Add(3*time.Second))
	if _, err := (workflowruntime.WaitCoordinator{Store: state}).Suspend(ctx, workflowruntime.SuspendCommand{Request: fixture.request, ResumeToken: fixture.token}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
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
	workflow := graph.Graph{ID: run.Plan.ID, Version: run.Plan.Version, Digest: run.Plan.Digest, Nodes: []graph.Node{
		{ID: fixture.invocation.NodeID, Kind: "test", KindVersion: "v1"},
		{ID: downstream.ID.NodeID, Kind: "test", KindVersion: "v1", ReadyWhen: graph.ReadyAllDone, Needs: []graph.Need{{Node: fixture.invocation.NodeID}}},
	}}
	registry := stepkind.NewRegistry()
	if err := registry.Register(stepkindtest.NewNoopKind("test", "v1")); err != nil {
		t.Fatal(err)
	}
	waits := &workflowruntime.WaitCoordinator{Store: reopened}
	coordinator := workflowruntime.RecoveryCoordinator{
		Store: reopened, Recovery: reopened, Inputs: reopened, Control: reopened,
		Plans: workflowPersistenceRecoveryPlans{graph: workflow}, Registry: registry, Waits: waits,
	}
	result, err := coordinator.Recover(ctx, workflowruntime.RecoveryRequest{Now: base.Add(time.Minute)})
	if err != nil || len(result.Waits.TimedOut) != 1 || result.Waits.TimedOut[0].Wait.Ref.ID != fixture.waitID {
		t.Fatalf("restart wait recovery = %#v, %v", result.Waits, err)
	}
	waitNode, waitErr := reopened.LoadNodeInvocation(ctx, fixture.invocation)
	downstreamNode, downstreamErr := reopened.LoadNodeInvocation(ctx, downstream.ID)
	if waitErr != nil || downstreamErr != nil || waitNode.Status != workflowruntime.NodeTimedOut || downstreamNode.Status != workflowruntime.NodeReady || downstreamNode.Inputs == nil {
		t.Fatalf("restart progression wait=%#v downstream=%#v errors=%v/%v", waitNode, downstreamNode, waitErr, downstreamErr)
	}
}

func TestWorkflowRecoveryMigrationFactsAreImmutableAndForeignKeyed(t *testing.T) {
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "recovery-migration.db"))
	var crashFKs, replayFKs int
	if err := store.DB().QueryRow(`SELECT COUNT(1) FROM pragma_foreign_key_list('workflow_crash_recovery_idempotency') WHERE "table" = 'workflow_attempts'`).Scan(&crashFKs); err != nil || crashFKs != 4 {
		t.Fatalf("crash recovery foreign keys = %d, %v", crashFKs, err)
	}
	if err := store.DB().QueryRow(`SELECT COUNT(1) FROM pragma_foreign_key_list('workflow_replay_provenance') WHERE "table" = 'workflow_runs'`).Scan(&replayFKs); err != nil || replayFKs != 2 {
		t.Fatalf("replay foreign keys = %d, %v", replayFKs, err)
	}
	ctx, base := context.Background(), workflowTestTime()
	state, stateErr := NewWorkflowStateStore(store)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	run := createWorkflowTestRun(t, state, "immutable-crash", base)
	if _, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base}); err != nil {
		t.Fatal(err)
	}
	node := createWorkflowTestNode(t, state, run.ID, "work", base)
	ready, _ := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: node.ID, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: base})
	claim, _ := state.ClaimNode(ctx, workflowruntime.ClaimNodeRequest{InvocationID: node.ID, ExpectedClaimGeneration: ready.Snapshot.ClaimGeneration, Owner: "worker", Token: "token", IdempotencyKey: "claim-immutable", Now: base, LeaseUntil: base.Add(time.Second)})
	current, _ := state.LoadNodeInvocation(ctx, node.ID)
	started, startErr := state.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{InvocationID: node.ID, ExpectedNodeGeneration: current.Generation, Claim: workflowruntime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}, Executor: workflowTestExecutor(), At: base})
	if startErr != nil {
		t.Fatal(startErr)
	}
	if _, err := state.ReconcileCrashedAttempt(ctx, workflowruntime.ReconcileCrashedAttemptRequest{Attempt: started.Attempt.ID, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, IdempotencyKey: "immutable-crash", Decision: workflowruntime.CrashRecoveryDecision{Action: workflowruntime.CrashTerminal, Policy: workflowruntime.RepeatPolicyDecision{Code: "terminal", Reason: "terminal"}}, At: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE workflow_crash_recovery_idempotency SET request_json = '{}' WHERE idempotency_key = 'immutable-crash'`); err == nil {
		t.Fatal("crash recovery immutable update succeeded")
	}
	if _, err := store.DB().Exec(`DELETE FROM workflow_crash_recovery_idempotency WHERE idempotency_key = 'immutable-crash'`); err == nil {
		t.Fatal("crash recovery immutable delete succeeded")
	}
	var rows int
	if err := store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_crash_recovery_idempotency WHERE idempotency_key = 'immutable-crash'`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("immutable crash row = %d, %v", rows, err)
	}
	if _, err := state.LoadReplayProvenance(ctx, "missing"); !errors.Is(err, workflowruntime.ErrNotFound) {
		t.Fatalf("missing replay provenance = %v", err)
	}
}

func TestWorkflowSQLiteReplayContendsReopensAndRejectsCorruptProjection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replay-reopen.db")
	store, state := openWorkflowStateTest(t, path)
	ctx, base := context.Background(), workflowTestTime()
	source := createWorkflowTestRun(t, state, "replay-source", base)
	running, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: source.ID, ExpectedGeneration: source.Generation, To: workflowruntime.RunRunning, At: base})
	if err != nil {
		t.Fatal(err)
	}
	upstream := createWorkflowTestNode(t, state, source.ID, "upstream", base)
	selected := createWorkflowTestNode(t, state, source.ID, "selected", base)
	upstreamResult, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: upstream.ID, ExpectedGeneration: upstream.Generation, To: workflowruntime.NodeSkipped, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	selectedResult, err := state.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: selected.ID, ExpectedGeneration: selected.Generation, To: workflowruntime.NodeSkipped, At: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := state.TransitionRun(ctx, workflowruntime.RunTransitionRequest{RunID: source.ID, ExpectedGeneration: running.Snapshot.Generation, To: workflowruntime.RunSucceeded, At: base.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	targetRun := workflowruntime.RunID("replay-target")
	request := workflowruntime.BeginReplayRequest{
		Provenance: workflowruntime.ReplayProvenance{
			RunID: targetRun, SourceRunID: source.ID, FromNodeID: "selected", PlanDigest: terminal.Snapshot.Plan.Digest,
			IdempotencyKey: "replay-reopen", CreatedAt: base.Add(3 * time.Second),
			Policy: []workflowruntime.ReplayNodePolicy{{Invocation: selected.ID, Decision: workflowruntime.RepeatPolicyDecision{Allow: true, Code: "safe", Reason: "test replay"}}},
		},
		Plan: terminal.Snapshot.Plan, Inputs: terminal.Snapshot.Inputs, ExpectedSourceGeneration: terminal.Snapshot.Generation,
		Nodes: []workflowruntime.ReplayNodeBinding{
			{Source: selectedResult.Snapshot, Target: workflowruntime.NodeInvocationID{RunID: targetRun, NodeID: "selected"}},
			{Source: upstreamResult.Snapshot, Target: workflowruntime.NodeInvocationID{RunID: targetRun, NodeID: "upstream"}, Reuse: true},
		},
	}
	offset := time.FixedZone("replay-offset", -7*60*60)
	request.Provenance.CreatedAt = request.Provenance.CreatedAt.In(offset)
	request.Nodes = append([]workflowruntime.ReplayNodeBinding(nil), request.Nodes...)
	for index := range request.Nodes {
		request.Nodes[index].Source.CreatedAt = request.Nodes[index].Source.CreatedAt.In(offset)
		request.Nodes[index].Source.UpdatedAt = request.Nodes[index].Source.UpdatedAt.In(offset)
	}
	secondStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	second, err := NewWorkflowStateStore(secondStore)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan workflowruntime.BeginReplayResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, adapter := range []*WorkflowStateStore{state, second} {
		wg.Add(1)
		go func(adapter *WorkflowStateStore) {
			defer wg.Done()
			result, callErr := adapter.BeginReplay(ctx, request)
			results <- result
			errs <- callErr
		}(adapter)
	}
	wg.Wait()
	close(results)
	close(errs)
	for callErr := range errs {
		if callErr != nil {
			t.Fatalf("concurrent BeginReplay: %v", callErr)
		}
	}
	applied, replayed := 0, 0
	for result := range results {
		switch result.Outcome {
		case workflowruntime.IdempotencyApplied:
			applied++
		case workflowruntime.IdempotencyReplayed:
			replayed++
		}
	}
	if applied != 1 || replayed != 1 {
		t.Fatalf("concurrent replay outcomes applied=%d replayed=%d", applied, replayed)
	}
	if request.Provenance.CreatedAt.Location() != offset || request.Nodes[0].Source.CreatedAt.Location() != offset {
		t.Fatal("SQLite replay canonicalization mutated caller-owned timestamps")
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
	provenance, err := reopened.LoadReplayProvenance(ctx, targetRun)
	if err != nil || provenance.SourceRunID != source.ID || provenance.PlanDigest != request.Plan.Digest {
		t.Fatalf("reopened replay provenance = %#v, %v", provenance, err)
	}
	nodes, err := reopened.ListRunInvocations(ctx, targetRun)
	if err != nil || len(nodes) != 2 || nodes[0].ID.NodeID != "selected" || nodes[0].Status != workflowruntime.NodePending || nodes[1].ID.NodeID != "upstream" || nodes[1].Status != workflowruntime.NodeSkipped {
		t.Fatalf("reopened replay nodes = %#v, %v", nodes, err)
	}
	exactRequest := request
	exactRequest.Provenance.CreatedAt = exactRequest.Provenance.CreatedAt.UTC()
	exactRequest.Nodes = append([]workflowruntime.ReplayNodeBinding(nil), exactRequest.Nodes...)
	for index := range exactRequest.Nodes {
		exactRequest.Nodes[index].Source.CreatedAt = exactRequest.Nodes[index].Source.CreatedAt.UTC()
		exactRequest.Nodes[index].Source.UpdatedAt = exactRequest.Nodes[index].Source.UpdatedAt.UTC()
	}
	exact, err := reopened.BeginReplay(ctx, exactRequest)
	if err != nil || exact.Outcome != workflowruntime.IdempotencyReplayed {
		t.Fatalf("reopened exact replay = %#v, %v", exact, err)
	}
	if _, err := store.DB().Exec(`UPDATE workflow_replay_provenance SET plan_digest = ? WHERE run_id = ?`, values.SHA256Digest([]byte("tampered")), targetRun); err == nil {
		t.Fatal("immutable replay projection update succeeded")
	}
	if _, err := store.DB().Exec(`DROP TRIGGER workflow_replay_provenance_reject_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE workflow_replay_provenance SET plan_digest = ? WHERE run_id = ?`, values.SHA256Digest([]byte("tampered")), targetRun); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.LoadReplayProvenance(ctx, targetRun); !errors.Is(err, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("corrupt replay projection = %v", err)
	}
}

func TestCanonicalWorkflowReplayRequestDeepCopiesNestedTimes(t *testing.T) {
	zone := time.FixedZone("nested-offset", 9*60*60)
	base := workflowTestTime().In(zone)
	rule := 0
	invocation := workflowruntime.NodeInvocationID{RunID: "source", NodeID: "fan"}
	item := invocation
	item.Iteration = workflowruntime.FanOutIteration(0)
	request := workflowruntime.BeginReplayRequest{
		Provenance: workflowruntime.ReplayProvenance{CreatedAt: base, Policy: []workflowruntime.ReplayNodePolicy{{Invocation: invocation, Attempt: &workflowruntime.AttemptID{Invocation: invocation, Number: 1}, Decision: workflowruntime.RepeatPolicyDecision{Code: "safe", Reason: "safe", Attributes: map[string]string{"owner": "caller"}}}}},
		Inputs:     &values.ValueSetRef{ID: "inputs", Digest: values.SHA256Digest([]byte("inputs"))},
		Nodes: []workflowruntime.ReplayNodeBinding{{
			Source:   workflowruntime.NodeInvocationSnapshot{ID: invocation, Lease: &workflowruntime.ClaimLease{ExpiresAt: base.Add(time.Hour)}, CreatedAt: base, UpdatedAt: base},
			Attempts: []workflowruntime.AttemptSnapshot{{ID: workflowruntime.AttemptID{Invocation: invocation, Number: 1}, StartedAt: base, FinishedAt: base.Add(time.Second), CreatedAt: base, UpdatedAt: base.Add(time.Second)}},
			Control:  []workflowruntime.ControlDecisionSnapshot{{ID: workflowruntime.ControlDecisionID{Source: invocation, Kind: workflowruntime.ControlSwitch}, RuleIndex: &rule, Targets: []workflowruntime.NodeInvocationID{item}, CreatedAt: base}},
		}},
		FanOuts: []workflowruntime.ReplayFanOutBinding{{
			Source: workflowruntime.FanOutSnapshot{Parent: invocation, CreatedAt: base, UpdatedAt: base},
			Target: workflowruntime.FanOutSnapshot{Parent: workflowruntime.NodeInvocationID{RunID: "target", NodeID: "fan"}, CreatedAt: base, UpdatedAt: base},
		}},
	}
	canonical := canonicalWorkflowReplayRequest(request)
	if request.Provenance.CreatedAt.Location() != zone || request.Nodes[0].Source.CreatedAt.Location() != zone || request.Nodes[0].Attempts[0].StartedAt.Location() != zone || request.Nodes[0].Control[0].CreatedAt.Location() != zone || request.FanOuts[0].Source.CreatedAt.Location() != zone {
		t.Fatal("canonical replay mutated caller-owned nested timestamps")
	}
	for name, timestamp := range map[string]time.Time{
		"provenance": canonical.Provenance.CreatedAt, "source": canonical.Nodes[0].Source.CreatedAt,
		"lease": canonical.Nodes[0].Source.Lease.ExpiresAt, "attempt": canonical.Nodes[0].Attempts[0].StartedAt,
		"control": canonical.Nodes[0].Control[0].CreatedAt, "fanout": canonical.FanOuts[0].Source.CreatedAt,
	} {
		if timestamp.Location() != time.UTC {
			t.Fatalf("canonical %s timestamp location = %s", name, timestamp.Location())
		}
	}
	canonical.Provenance.Policy[0].Decision.Attributes["owner"] = "canonical"
	canonical.Nodes[0].Source.Lease.Owner = "canonical"
	canonical.Nodes[0].Control[0].Targets[0].NodeID = "changed"
	if request.Provenance.Policy[0].Decision.Attributes["owner"] != "caller" || request.Nodes[0].Source.Lease.Owner != "" || request.Nodes[0].Control[0].Targets[0] != item {
		t.Fatal("canonical replay retained caller-owned nested aliases")
	}
	second := canonicalWorkflowReplayRequest(request)
	if !reflect.DeepEqual(canonicalWorkflowReplayRequest(second), second) {
		t.Fatal("canonical replay request is not idempotent")
	}
}

type workflowPersistenceRecoveryPlans struct{ graph graph.Graph }

func (s workflowPersistenceRecoveryPlans) LoadRecoveryPlan(_ context.Context, run workflowruntime.RunSnapshot) (workflowruntime.RecoveryPlan, error) {
	plan := workflowcompile.ExecutionPlan{SchemaVersion: run.Plan.SchemaVersion, ID: run.Plan.ID, Digest: run.Plan.Digest, Graph: s.graph}
	inferred := workflowcompile.InferValueDependencies(&plan, workflowcompile.DependencyOptions{})
	return workflowruntime.RecoveryPlan{Ref: run.Plan, Plan: plan, Visibility: inferred.Visibility}, nil
}
