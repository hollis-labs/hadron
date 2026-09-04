package persistence

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	calladapter "github.com/hollis-labs/go-workflow/adapters/call"
	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

func TestWorkflowHostJournalExactReplayCASAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	store, _ := openWorkflowStateTest(t, path)
	host, err := NewWorkflowHostStore(store)
	if err != nil {
		t.Fatal(err)
	}
	record := workflowHostStartFixture(t, "root")
	created, outcome, err := host.RecordStart(t.Context(), record)
	if err != nil || outcome != workflowruntime.IdempotencyApplied || created.Phase != hoststate.StartRecorded {
		t.Fatalf("RecordStart = %#v, %q, %v", created, outcome, err)
	}
	replayed, outcome, err := host.RecordStart(t.Context(), record)
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || !reflect.DeepEqual(replayed, created) {
		t.Fatalf("RecordStart replay = %#v, %q, %v", replayed, outcome, err)
	}
	created.Record.Identity.RunScope.Attributes["cost_center"] = "mutated-result"
	created.Record.Identity.ExecutionTarget.Labels["region"] = "mutated-result"
	record.Identity.RunScope.Attributes["cost_center"] = "mutated-caller"
	record.Identity.ExecutionTarget.Labels["region"] = "mutated-caller"
	immutable := workflowHostStartFixture(t, "root")
	loadedImmutable, loadErr := host.LoadStart(t.Context(), record.Run.ID)
	if loadErr != nil || !reflect.DeepEqual(loadedImmutable.Record.Identity, immutable.Identity) {
		t.Fatalf("start defensive persistence = %#v, %v", loadedImmutable.Record.Identity, loadErr)
	}
	record = immutable
	changed := record
	contradictory := record
	contradictory.Identity.Principal = "contradictory"
	if _, _, recordErr := host.RecordStart(t.Context(), contradictory); !errors.Is(recordErr, hoststate.ErrInvalidRecord) {
		t.Fatalf("contradictory start identity = %v", recordErr)
	}
	changed.Identity.Principal = "different"
	changed.Facts.Identity.Principal = "different"
	if _, _, recordErr := host.RecordStart(t.Context(), changed); !errors.Is(recordErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed start error = %v", recordErr)
	}
	for name, mutate := range map[string]func(*hoststate.StartRecord){
		"scope": func(candidate *hoststate.StartRecord) {
			candidate.Identity.RunScope.ID = "different-scope"
			candidate.Facts.Identity.RunScope.ID = "different-scope"
			candidate.Facts.RunScope.ID = "different-scope"
		},
		"target": func(candidate *hoststate.StartRecord) {
			candidate.Identity.ExecutionTarget.ID = "different-target"
			candidate.Facts.Identity.ExecutionTarget.ID = "different-target"
			candidate.Facts.ExecutionTarget.ID = "different-target"
		},
	} {
		t.Run("changed "+name, func(t *testing.T) {
			candidate := workflowHostStartFixture(t, "root")
			mutate(&candidate)
			if _, _, conflictErr := host.RecordStart(t.Context(), candidate); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
				t.Fatalf("RecordStart conflict = %v", conflictErr)
			}
		})
	}

	advanced, err := host.AdvanceStart(t.Context(), hoststate.AdvanceStartRequest{RunID: record.Run.ID, ExpectedGeneration: created.Generation, From: created.Phase, To: hoststate.StartRunCreated, At: created.UpdatedAt.Add(time.Second)})
	if err != nil || advanced.Generation != 2 {
		t.Fatalf("AdvanceStart = %#v, %v", advanced, err)
	}
	concurrentReplay, err := host.AdvanceStart(t.Context(), hoststate.AdvanceStartRequest{RunID: record.Run.ID, ExpectedGeneration: created.Generation, From: created.Phase, To: hoststate.StartRunCreated, At: created.UpdatedAt.Add(time.Second)})
	if err != nil || concurrentReplay.Generation != advanced.Generation {
		t.Fatalf("AdvanceStart replay = %#v, %v", concurrentReplay, err)
	}

	evaluation := hoststate.PolicyEvaluation{StartKey: record.StartKey, RequestDigest: record.RequestDigest, Facts: record.Facts, Decision: record.Decision}
	if loaded, loadErr := host.LoadPolicyEvaluation(t.Context(), record.Decision.ID); loadErr != nil || loaded.RequestDigest != evaluation.RequestDigest {
		t.Fatalf("LoadPolicyEvaluation = %#v, %v", loaded, loadErr)
	}
	if persisted, replayOutcome, policyErr := host.RecordPolicyEvaluation(t.Context(), evaluation); policyErr != nil || replayOutcome != workflowruntime.IdempotencyReplayed || persisted.StartKey != record.StartKey {
		t.Fatalf("RecordPolicyEvaluation replay = %#v, %q, %v", persisted, replayOutcome, policyErr)
	}

	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopenedStore, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })
	reopened, err := NewWorkflowHostStore(reopenedStore)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopened.LoadStart(t.Context(), record.Run.ID)
	if err != nil || loaded.Phase != hoststate.StartRunCreated || loaded.Generation != 2 || !reflect.DeepEqual(loaded.Record.Identity, immutable.Identity) || !reflect.DeepEqual(loaded.Record.Facts.RunScope, immutable.Facts.RunScope) || !reflect.DeepEqual(loaded.Record.Facts.ExecutionTarget, immutable.Facts.ExecutionTarget) {
		t.Fatalf("reopened LoadStart = %#v, %v", loaded, err)
	}
	var persistedJSON string
	if err := reopenedStore.DB().QueryRow(`SELECT request_json FROM workflow_host_starts WHERE run_id = ?`, record.Run.ID).Scan(&persistedJSON); err != nil || strings.Contains(persistedJSON, "workspace_id") || !strings.Contains(persistedJSON, `"run_scope"`) || !strings.Contains(persistedJSON, `"execution_target"`) {
		t.Fatalf("persisted graph-native start JSON = %q, %v", persistedJSON, err)
	}
	var version int
	if err := reopenedStore.DB().QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE version = 18`).Scan(&version); err != nil || version != 1 {
		t.Fatalf("migration 18 = %d, %v", version, err)
	}
}

func TestWorkflowHostPolicyEvaluationDefensiveScopeTargetPersistence(t *testing.T) {
	store, _ := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "policy-clone.db"))
	host, err := NewWorkflowHostStore(store)
	if err != nil {
		t.Fatal(err)
	}
	record := workflowHostStartFixture(t, "policy-clone")
	evaluation := hoststate.PolicyEvaluation{StartKey: record.StartKey, RequestDigest: record.RequestDigest, Facts: record.Facts, Decision: record.Decision}
	applied, outcome, err := host.RecordPolicyEvaluation(t.Context(), evaluation)
	if err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("RecordPolicyEvaluation = %#v, %q, %v", applied, outcome, err)
	}
	evaluation.Facts.Identity.RunScope.Attributes["cost_center"] = "mutated-caller"
	evaluation.Facts.Identity.ExecutionTarget.Labels["region"] = "mutated-caller"
	applied.Facts.RunScope.Attributes["cost_center"] = "mutated-result"
	applied.Facts.ExecutionTarget.Labels["region"] = "mutated-result"
	original := workflowHostStartFixture(t, "policy-clone")
	replayed, outcome, err := host.RecordPolicyEvaluation(t.Context(), hoststate.PolicyEvaluation{StartKey: original.StartKey, RequestDigest: original.RequestDigest, Facts: original.Facts, Decision: original.Decision})
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || replayed.Facts.RunScope.Attributes["cost_center"] != "research" || replayed.Facts.ExecutionTarget.Labels["region"] != "local" || replayed.Facts.Identity.RunScope.Attributes["cost_center"] != "research" {
		t.Fatalf("RecordPolicyEvaluation replay = %#v, %q, %v", replayed, outcome, err)
	}
	loaded, err := host.LoadPolicyEvaluation(t.Context(), original.Decision.ID)
	if err != nil || !reflect.DeepEqual(loaded, replayed) {
		t.Fatalf("LoadPolicyEvaluation = %#v, %v", loaded, err)
	}
}

func TestWorkflowHostCallResolutionAndChildStartAtomicReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "calls.db")
	store, state := openWorkflowStateTest(t, path)
	host, _ := NewWorkflowHostStore(store)
	started, _ := prepareWorkflowSQLiteRunning(t, state, "call-host", workflowTestTime())
	resolution := workflowCallResolutionFixture(t, started.Node.ID)
	persisted, outcome, err := host.RecordCallResolution(t.Context(), calladapter.RecordResolutionRequest{Record: resolution})
	if err != nil || outcome != calladapter.ResolutionApplied || persisted.Key != resolution.Key {
		t.Fatalf("RecordCallResolution = %#v, %q, %v", persisted, outcome, err)
	}
	persisted.Lineage[0].ID = "mutated"
	replayed, outcome, err := host.RecordCallResolution(t.Context(), calladapter.RecordResolutionRequest{Record: resolution})
	if err != nil || outcome != calladapter.ResolutionReplayed || replayed.Lineage[0].ID == "mutated" {
		t.Fatalf("resolution replay = %#v, %q, %v", replayed, outcome, err)
	}
	conflict := resolution
	conflict.InputDigest = values.SHA256Digest([]byte("changed"))
	if _, _, resolutionErr := host.RecordCallResolution(t.Context(), calladapter.RecordResolutionRequest{Record: conflict}); !errors.Is(resolutionErr, calladapter.ErrResolutionConflict) {
		t.Fatalf("resolution conflict = %v", resolutionErr)
	}
	events, _ := state.ListEvents(t.Context(), workflowruntime.EventQuery{RunID: started.Node.ID.RunID})
	if len(events) != 3 || events[2].Type != "call.definition_resolved" {
		t.Fatalf("resolution events = %#v", events)
	}

	request := workflowChildStartFixture(t, started.Node.ID, "child-one")
	invalidParent := request
	invalidParent.Parent.NodeID = "invalid node"
	if _, startErr := host.StartChildRun(t.Context(), invalidParent); startErr == nil {
		t.Fatal("invalid parent callsite was accepted")
	}
	invalidLineage := request
	invalidLineage.Lineage = append([]graph.DefinitionRef(nil), request.Lineage...)
	invalidLineage.Lineage[len(invalidLineage.Lineage)-1] = invalidLineage.Lineage[0]
	if _, startErr := host.StartChildRun(t.Context(), invalidLineage); startErr == nil {
		t.Fatal("mismatched child lineage was accepted")
	}
	invalidGraph := request
	invalidGraph.Definition.Graph.Digest = values.SHA256Digest([]byte("wrong-graph"))
	if _, startErr := host.StartChildRun(t.Context(), invalidGraph); startErr == nil {
		t.Fatal("mismatched resolved graph was accepted")
	}
	child, err := host.StartChildRun(t.Context(), request)
	if err != nil || child.Run.Status != workflowruntime.RunPending || child.Run.Inputs == nil {
		t.Fatalf("StartChildRun = %#v, %v", child, err)
	}
	wantInvocation := workflowruntime.NodeInvocationID{RunID: workflowruntime.RunID(request.Parent.RunID), NodeID: request.Parent.NodeID, Iteration: request.Parent.Iteration}
	if child.Link.ParentRunID != wantInvocation.RunID || child.Link.Invocation != wantInvocation || child.Link.ChildRunID != request.ChildRunID || child.Link.Policy != request.ParentClose || !child.Link.CreatedAt.Equal(child.Run.CreatedAt) {
		t.Fatalf("child link = %#v", child.Link)
	}
	child.Run.Status = workflowruntime.RunSucceeded
	replayedChild, err := host.StartChildRun(t.Context(), request)
	if err != nil || replayedChild.Run.Status != workflowruntime.RunPending {
		t.Fatalf("StartChildRun replay = %#v, %v", replayedChild, err)
	}
	changedRequest := request
	changedRequest.Inputs = workflowTestValues(t, "changed")
	if _, startErr := host.StartChildRun(t.Context(), changedRequest); !errors.Is(startErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("child conflict = %v", startErr)
	}
	pending, err := host.RecoverPendingChildRuns(t.Context(), 1)
	if err != nil || len(pending) != 1 || pending[0].ChildRunID != request.ChildRunID {
		t.Fatalf("RecoverPendingChildRuns = %#v, %v", pending, err)
	}
}

func TestWorkflowHostChildStartContentionAndCancellationFence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "contention.db")
	firstStore, firstState := openWorkflowStateTest(t, path)
	secondStore, openErr := Open(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	t.Cleanup(func() { _ = secondStore.Close() })
	first, _ := NewWorkflowHostStore(firstStore)
	second, _ := NewWorkflowHostStore(secondStore)
	hostNow := func() time.Time { return workflowTestTime().Add(4 * time.Hour) }
	first.now = hostNow
	second.now = hostNow
	started, _ := prepareWorkflowSQLiteRunning(t, firstState, "child-race", workflowTestTime())
	request := workflowChildStartFixture(t, started.Node.ID, "child-race")
	results := make(chan calladapter.ChildRunResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, host := range []*WorkflowHostStore{first, second} {
		wg.Add(1)
		go func(host *WorkflowHostStore) {
			defer wg.Done()
			result, startErr := host.StartChildRun(context.Background(), request)
			results <- result
			errs <- startErr
		}(host)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("contended child start: %v", err)
		}
	}
	for result := range results {
		if result.Run.ID != request.ChildRunID {
			t.Fatalf("contended result = %#v", result)
		}
	}
	var count int
	if queryErr := firstStore.DB().QueryRow(`SELECT COUNT(1) FROM workflow_runs WHERE run_id = ?`, request.ChildRunID).Scan(&count); queryErr != nil || count != 1 {
		t.Fatalf("child count = %d, %v", count, queryErr)
	}

	other, _ := prepareWorkflowSQLiteRunning(t, firstState, "cancel-fence", workflowTestTime().Add(time.Hour))
	run, _ := firstState.LoadRun(t.Context(), other.Node.ID.RunID)
	_, cancelRequestErr := firstState.RequestRunCancellation(t.Context(), workflowruntime.RequestRunCancellationRequest{RunID: run.ID, ExpectedGeneration: run.Generation, IdempotencyKey: "cancel-before-child", Reason: workflowruntime.Failure{Code: "test_cancel", Message: "test cancellation"}, At: workflowTestTime().Add(2 * time.Hour)})
	if cancelRequestErr != nil {
		t.Fatal(cancelRequestErr)
	}
	if _, startErr := first.StartChildRun(t.Context(), workflowChildStartFixture(t, other.Node.ID, "late-child")); !errors.Is(startErr, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("post-cancel child start = %v", startErr)
	}

	// Independent SQLite handles contend over the same parent lifecycle fence.
	// Either child creation wins and parent-close cancellation durably cancels
	// that child, or cancellation wins and no child is created.
	tracing, _ := prepareWorkflowSQLiteRunning(t, firstState, "cancel-race", workflowTestTime().Add(3*time.Hour))
	parent, err := firstState.LoadRun(t.Context(), tracing.Node.ID.RunID)
	if err != nil {
		t.Fatal(err)
	}
	childRequest := workflowChildStartFixture(t, tracing.Node.ID, "cancel-race-child")
	secondState, err := NewWorkflowStateStore(secondStore)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	childErr := make(chan error, 1)
	cancelErr := make(chan error, 1)
	go func() {
		<-start
		_, err := first.StartChildRun(context.Background(), childRequest)
		childErr <- err
	}()
	go func() {
		<-start
		_, err := secondState.RequestRunCancellation(context.Background(), workflowruntime.RequestRunCancellationRequest{
			RunID: parent.ID, ExpectedGeneration: parent.Generation,
			IdempotencyKey: "cancel-race-key",
			Reason:         workflowruntime.Failure{Code: "test_cancel", Message: "race cancellation"},
			At:             workflowTestTime().Add(6 * time.Hour),
		})
		cancelErr <- err
	}()
	close(start)
	childStartErr, cancellationErr := <-childErr, <-cancelErr
	if cancellationErr != nil {
		t.Fatalf("contended cancellation = %v", cancellationErr)
	}
	if childStartErr != nil && !errors.Is(childStartErr, workflowruntime.ErrTransitionConflict) {
		t.Fatalf("contended child start = %v", childStartErr)
	}
	childRun, loadErr := firstState.LoadRun(t.Context(), childRequest.ChildRunID)
	if childStartErr == nil {
		if loadErr != nil || childRun.Status != workflowruntime.RunCanceled {
			t.Fatalf("winning child after parent cancellation = %#v, %v", childRun, loadErr)
		}
	} else if !errors.Is(loadErr, workflowruntime.ErrNotFound) {
		t.Fatalf("losing child unexpectedly durable = %#v, %v", childRun, loadErr)
	}
}

func TestWorkflowHostCancellationPreparationSurvivesGenerationRace(t *testing.T) {
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "cancel-bind.db"))
	host, newErr := NewWorkflowHostStore(store)
	if newErr != nil {
		t.Fatal(newErr)
	}
	started, _ := prepareWorkflowSQLiteRunning(t, state, "host-cancel-bind", workflowTestTime())
	regressing := hoststate.CancellationIntent{RunID: started.Node.ID.RunID, IdempotencyKey: "host-cancel-regressing", Reason: "test old time", RequestedAt: workflowTestTime().Add(-time.Second)}
	if _, _, bindErr := host.BindCancellation(t.Context(), hoststate.BindCancellationRequest{Intent: regressing, DefaultAt: workflowTestTime().Add(3 * time.Second)}); !errors.Is(bindErr, hoststate.ErrInvalidRecord) {
		t.Fatalf("regressing explicit cancellation = %v", bindErr)
	}
	var regressingCount int
	if queryErr := store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_host_cancellations WHERE idempotency_key = ?`, regressing.IdempotencyKey).Scan(&regressingCount); queryErr != nil || regressingCount != 0 {
		t.Fatalf("regressing cancellation persisted = %d, %v", regressingCount, queryErr)
	}
	intent := hoststate.CancellationIntent{RunID: started.Node.ID.RunID, IdempotencyKey: "host-cancel-bind", Reason: "test bind"}
	binding, outcome, err := host.BindCancellation(t.Context(), hoststate.BindCancellationRequest{Intent: intent, DefaultAt: workflowTestTime().Add(3 * time.Second)})
	if err != nil || outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("BindCancellation = %#v, %q, %v", binding, outcome, err)
	}
	replayedBinding, outcome, err := host.BindCancellation(t.Context(), hoststate.BindCancellationRequest{Intent: intent, DefaultAt: workflowTestTime().Add(time.Hour)})
	if err != nil || outcome != workflowruntime.IdempotencyReplayed || !reflect.DeepEqual(replayedBinding, binding) {
		t.Fatalf("BindCancellation replay = %#v, %q, %v", replayedBinding, outcome, err)
	}
	stale, err := host.PrepareCancellation(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	run, err := state.LoadRun(t.Context(), intent.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := state.TransitionRun(t.Context(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: workflowTestTime().Add(4 * time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	if _, cancelErr := state.RequestRunCancellation(t.Context(), stale); !errors.Is(cancelErr, workflowruntime.ErrCASMismatch) {
		t.Fatalf("stale prepared cancellation = %v", cancelErr)
	}
	fresh, err := host.PrepareCancellation(t.Context(), binding)
	if err != nil || fresh.ExpectedGeneration == stale.ExpectedGeneration || fresh.At.Before(workflowTestTime().Add(4*time.Second)) {
		t.Fatalf("fresh prepared cancellation = %#v, %v", fresh, err)
	}
	applied, err := state.RequestRunCancellation(t.Context(), fresh)
	if err != nil || applied.Outcome != workflowruntime.IdempotencyApplied {
		t.Fatalf("prepared cancellation apply = %#v, %v", applied, err)
	}
	exact, err := host.PrepareCancellation(t.Context(), binding)
	if err != nil || !reflect.DeepEqual(exact, fresh) {
		t.Fatalf("prepared cancellation replay = %#v, %v", exact, err)
	}
	pending, err := host.ListPendingCancellations(t.Context(), 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending cancellations = %#v, %v", pending, err)
	}
}

func TestWorkflowHostCancellationRejectsDriftedCoreReplay(t *testing.T) {
	store, state := openWorkflowStateTest(t, filepath.Join(t.TempDir(), "cancel-drift.db"))
	host, err := NewWorkflowHostStore(store)
	if err != nil {
		t.Fatal(err)
	}
	started, _ := prepareWorkflowSQLiteRunning(t, state, "host-cancel-drift", workflowTestTime())
	intent := hoststate.CancellationIntent{RunID: started.Node.ID.RunID, IdempotencyKey: "host-cancel-drift", Reason: "exact reason"}
	binding, _, err := host.BindCancellation(t.Context(), hoststate.BindCancellationRequest{Intent: intent, DefaultAt: workflowTestTime().Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	drifted, err := host.PrepareCancellation(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	drifted.Reason.Retryable = true
	drifted.Reason.Details = map[string]string{"unsafe": "semantic drift"}
	if _, err := state.RequestRunCancellation(t.Context(), drifted); err != nil {
		t.Fatal(err)
	}
	if _, err := host.PrepareCancellation(t.Context(), binding); !errors.Is(err, hoststate.ErrConflict) {
		t.Fatalf("drifted core replay = %v", err)
	}
}

func workflowHostStartFixture(t *testing.T, suffix string) hoststate.StartRecord {
	t.Helper()
	at := workflowTestTime()
	plan := workflowcompile.ExecutionPlan{
		SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion,
		ID:            "plan-" + suffix,
		Definition: graph.DefinitionRef{
			Kind: "workflow", ID: "plan-" + suffix, Version: "v1",
			Digest: values.SHA256Digest([]byte("source-" + suffix)),
		},
		Graph: graph.Graph{ID: "plan-" + suffix, Version: "v1"},
	}
	plan.Graph.Digest, _ = workflowcompile.GraphDigest(plan.Graph)
	plan.Digest, _ = workflowcompile.PlanDigest(plan)
	planRef := workflowruntime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion}
	inputRef := values.ValueSetRef{ID: "values-000000000001", Digest: values.SHA256Digest([]byte("inputs"))}
	bound := workflowruntime.BoundRun{ID: workflowruntime.RunID("run-" + suffix), Plan: planRef, InputsRef: inputRef, CreatedAt: at, Provenance: graph.Provenance{Authority: "test", Locator: suffix + ".yaml", Digest: planRef.Digest}}
	identity := workflowHostIdentity()
	facts := hoststate.PolicyFacts{Operation: "start", RunID: bound.ID, Plan: planRef, Identity: identity, RunScope: identity.RunScope, ExecutionTarget: identity.ExecutionTarget, Effects: graph.EffectSet{graph.EffectCompute}, NodeCount: 1, BlastRadius: map[string]int{"compute": 1}}
	decision := hoststate.PolicyDecision{ID: "decision-" + suffix, RunID: bound.ID, Operation: "start", Outcome: hoststate.PolicyAllow, Reason: "test allow", DecidedAt: at}
	return hoststate.StartRecord{Run: bound, Plan: plan, Requested: graph.DefinitionRef{Kind: "workflow", ID: planRef.ID}, StartKey: "host-start-" + suffix, RequestDigest: values.SHA256Digest([]byte("request-" + suffix)), CallerInputHash: values.SHA256Digest([]byte("inputs-" + suffix)), Identity: identity, Facts: facts, Decision: decision, RecordedAt: at}
}

func workflowHostIdentity() hoststate.IdentityBinding {
	checkedAt := workflowTestTime()
	target := hoststate.ExecutionTarget{
		Version: hoststate.ScopeTargetVersionV1, ID: "local-default", Kind: hoststate.ExecutionTargetLocal,
		EnvironmentRefs: map[string]hoststate.TargetConfigReference{"locale": {Authority: "host-config", Name: "locale"}},
		Capabilities:    []string{"compute"}, Labels: map[string]string{"region": "local"},
		Sandbox:    hoststate.SandboxPolicy{Mode: hoststate.SandboxHostDefault},
		Readiness:  hoststate.TargetReadiness{State: hoststate.TargetReady, CheckedAt: checkedAt},
		Provenance: hoststate.TargetProvenance{Authority: "hadron", Reference: "local-default", Attributes: map[string]string{"pool": "default"}},
	}
	return hoststate.IdentityBinding{
		Principal: "user:test", SourceAuthority: "test", Trust: "trusted", Grants: []string{"workflow.run"},
		RunScope: hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "test", Attributes: map[string]string{"cost_center": "research"}}, ExecutionTarget: &target,
	}
}

func workflowCallResolutionFixture(t *testing.T, parent workflowruntime.NodeInvocationID) calladapter.ResolutionRecord {
	t.Helper()
	rootDigest := values.SHA256Digest([]byte("root"))
	childDigest := values.SHA256Digest([]byte("child"))
	inputDigest, _ := values.DigestValueSet(workflowTestValues(t, "hello"))
	root := graph.DefinitionRef{Kind: "workflow", ID: "root", Version: "v1", Digest: rootDigest}
	provenance := graph.Provenance{Authority: "test", Locator: "child.yaml", Digest: childDigest}
	child := graph.DefinitionRef{Authority: "test", Kind: "workflow", ID: "child", Locator: provenance.Locator, Version: "v1", Digest: childDigest, Provenance: &provenance}
	return calladapter.ResolutionRecord{Key: "resolution-" + parent.NodeID, Invocation: calladapter.CallSiteIdentity{RunID: string(parent.RunID), NodeID: parent.NodeID, Iteration: parent.Iteration}, Requested: graph.DefinitionRef{Kind: "workflow", ID: "child", Version: "v1"}, Resolved: child, InputDigest: inputDigest, Lineage: []graph.DefinitionRef{root, child}}
}

func workflowChildStartFixture(t *testing.T, parent workflowruntime.NodeInvocationID, childID string) calladapter.ChildRunRequest {
	t.Helper()
	plan := workflowTestPlan(childID)
	provenance := graph.Provenance{Authority: "test", Locator: childID + ".yaml", Digest: plan.Digest}
	definition := graph.DefinitionRef{Authority: "test", Kind: "workflow", ID: plan.ID, Locator: provenance.Locator, Version: plan.Version, Digest: plan.Digest, Provenance: &provenance}
	root := graph.DefinitionRef{Authority: "test", Kind: "workflow", ID: "root", Version: "v1", Digest: values.SHA256Digest([]byte("root-" + childID))}
	return calladapter.ChildRunRequest{Parent: calladapter.CallSiteIdentity{RunID: string(parent.RunID), NodeID: parent.NodeID, Iteration: parent.Iteration}, ChildRunID: workflowruntime.RunID(childID), Definition: workflowcompile.ResolvedDefinition{Definition: definition, Graph: graph.Graph{ID: plan.ID, Version: plan.Version, Digest: plan.Digest, Provenance: provenance}}, Plan: plan, Inputs: workflowTestValues(t, "hello"), Lineage: []graph.DefinitionRef{root, definition}, ParentClose: graph.ParentCloseCancel, IdempotencyKey: "child-key-" + childID}
}
