package appworkflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	calladapter "github.com/hollis-labs/hadron/workflow/adapters/call"
	"github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestPinnedChildRunMaterializerCreatesAndReplaysRunnableGraphState(t *testing.T) {
	fixture := newChildMaterializerFixture(t)
	materializer, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{
		State: fixture.store, Clock: ClockFunc(func() time.Time { return fixture.now.Add(time.Second) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if materializeErr := materializer.MaterializeChildRun(t.Context(), fixture.request); materializeErr != nil {
		t.Fatal(materializeErr)
	}
	if replayErr := materializer.MaterializeChildRun(t.Context(), fixture.request); replayErr != nil {
		t.Fatalf("exact replay: %v", replayErr)
	}

	run, err := fixture.store.LoadRun(t.Context(), fixture.request.ChildRunID)
	if err != nil || run.Status != runtime.RunRunning || run.Generation != 2 {
		t.Fatalf("run = %+v, %v", run, err)
	}
	root, err := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: "root"})
	if err != nil || root.Status != runtime.NodeReady || root.Inputs == nil {
		t.Fatalf("root = %+v, %v", root, err)
	}
	bound, err := fixture.store.LoadValues(t.Context(), *root.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	if got := bound["message"].Inline; got != "hello" {
		t.Fatalf("bound message = %#v", got)
	}
	dependent, err := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: "dependent"})
	if err != nil || dependent.Status != runtime.NodePending || dependent.Inputs != nil {
		t.Fatalf("dependent = %+v, %v", dependent, err)
	}

	changed := fixture.request
	changed.Inputs = cloneValueSetForTest(t, fixture.request.Inputs)
	changed.Inputs["message"] = inlineValueForTest(t, "changed")
	if err := materializer.MaterializeChildRun(t.Context(), changed); !errors.Is(err, runtime.ErrInvalidRecord) {
		t.Fatalf("changed request error = %v", err)
	}
}

func TestPinnedChildRunMaterializerKeepsCompensationHandlerDormantUntilChildLedgerActivatesIt(t *testing.T) {
	fixture := newChildMaterializerFixture(t)
	fixture.request.Definition.Graph.Compensation = &graph.CompensationPolicy{Triggers: []graph.CompensationTrigger{graph.CompensationManual}}
	rootDefinition := fixture.request.Definition.Graph.Nodes[0]
	rootDefinition.Compensation = &graph.CompensationSpec{Handler: "undo"}
	fixture.request.Definition.Graph.Nodes = []graph.Node{rootDefinition, {ID: "undo", Kind: "noop", KindVersion: "v1"}}
	fixture.request.Definition.Graph.Edges = nil
	materializer, materializerErr := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{State: fixture.store, Clock: ClockFunc(func() time.Time { return fixture.now.Add(time.Second) })})
	if materializerErr != nil {
		t.Fatal(materializerErr)
	}
	if err := materializer.MaterializeChildRun(t.Context(), fixture.request); err != nil {
		t.Fatal(err)
	}
	handlerID := runtime.NodeInvocationID{RunID: fixture.request.ChildRunID, NodeID: "undo"}
	if _, err := fixture.store.LoadNodeInvocation(t.Context(), handlerID); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("dormant child handler was materialized in forward phase: %v", err)
	}
	rootID := runtime.NodeInvocationID{RunID: fixture.request.ChildRunID, NodeID: "root"}
	root, err := fixture.store.LoadNodeInvocation(t.Context(), rootID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.store.ClaimNode(t.Context(), runtime.ClaimNodeRequest{InvocationID: rootID, ExpectedClaimGeneration: root.ClaimGeneration, Owner: "child-forward", Token: "child-forward-token", IdempotencyKey: "child-forward-claim", Now: fixture.now.Add(2 * time.Second), LeaseUntil: fixture.now.Add(time.Hour)})
	if err != nil || claim.Lease == nil {
		t.Fatalf("claim = %#v, %v", claim, err)
	}
	proof := runtime.ClaimProof{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Generation: claim.Lease.Generation}
	root, err = fixture.store.LoadNodeInvocation(t.Context(), rootID)
	if err != nil {
		t.Fatal(err)
	}
	started, err := fixture.store.StartNodeAttempt(t.Context(), runtime.StartNodeAttemptRequest{InvocationID: rootID, ExpectedNodeGeneration: root.Generation, Claim: proof, Executor: runtime.ExecutorMetadata{Kind: "noop", Version: "v1"}, Inputs: root.Inputs, At: fixture.now.Add(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	evidence := stepkind.ReversibilityEvidence{Operation: "fixture.child", ReceiptSchema: graph.Schema{}}
	if _, finishErr := fixture.store.FinishCompensableAttempt(t.Context(), runtime.FinishCompensableAttemptRequest{
		Finish:      runtime.FinishNodeAttemptRequest{InvocationID: rootID, AttemptNumber: started.Attempt.ID.Number, ExpectedNodeGeneration: started.Node.Generation, ExpectedAttemptGeneration: started.Attempt.Generation, Claim: proof, AttemptStatus: runtime.NodeSucceeded, NextNodeStatus: runtime.NodeSucceeded, At: fixture.now.Add(4 * time.Second)},
		Eligibility: runtime.CompensationEligibility{PlanDigest: fixture.request.Plan.Digest, HandlerNodeID: "undo", Evidence: evidence, Receipt: stepkind.CompensationReceipt{Operation: evidence.Operation, Values: values.ValueSet{}}},
	}); finishErr != nil {
		t.Fatal(finishErr)
	}
	run, err := fixture.store.LoadRun(t.Context(), fixture.request.ChildRunID)
	if err != nil {
		t.Fatal(err)
	}
	succeeded, err := fixture.store.TransitionRun(t.Context(), runtime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: runtime.RunSucceeded, At: fixture.now.Add(5 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	manual, err := fixture.store.BeginManualCompensation(t.Context(), runtime.BeginManualCompensationRequest{RunID: run.ID, PlanDigest: run.Plan.Digest, ExpectedRunGeneration: succeeded.Snapshot.Generation, OriginalStatus: runtime.RunSucceeded, IdempotencyKey: "child-manual", Authorization: values.SHA256Digest([]byte("child-manual-authorization")), At: fixture.now.Add(6 * time.Second)})
	if err != nil || manual.Ledger.Status != runtime.CompensationFrozen {
		t.Fatalf("manual = %#v, %v", manual, err)
	}
	progress, err := (runtime.CompensationCoordinator{Store: fixture.store, Compensation: fixture.store, Plans: childMaterializerPlanSource{graph: fixture.request.Definition.Graph}}).Progress(t.Context(), run.ID, fixture.now.Add(7*time.Second))
	if err != nil || len(progress.Activated) != 1 || progress.Activated[0].Node.ID.RunID != handlerID.RunID || progress.Activated[0].Node.ID.NodeID != handlerID.NodeID || progress.Activated[0].Node.ID.Iteration == "" || progress.Activated[0].Node.Phase != runtime.InvocationCompensation {
		t.Fatalf("ledger handler activation = %#v, %v", progress, err)
	}
}

func TestPinnedChildRunMaterializerRecoversPartialAmbiguousMaterialization(t *testing.T) {
	fixture := newChildMaterializerFixture(t)
	ambiguous := &ambiguousCreateStore{StateStore: fixture.store, cause: errors.New("node create response lost")}
	first, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{State: ambiguous, Clock: ClockFunc(func() time.Time { return fixture.now.Add(time.Second) })})
	if err != nil {
		t.Fatal(err)
	}
	if materializeErr := first.MaterializeChildRun(t.Context(), fixture.request); !errors.Is(materializeErr, ambiguous.cause) {
		t.Fatalf("ambiguous materialization error = %v", materializeErr)
	}
	pending, err := fixture.store.LoadRun(t.Context(), fixture.request.ChildRunID)
	if err != nil || pending.Status != runtime.RunPending {
		t.Fatalf("pending child = %+v, %v", pending, err)
	}

	recovered, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{State: fixture.store, Clock: ClockFunc(func() time.Time { return fixture.now.Add(2 * time.Second) })})
	if err != nil {
		t.Fatal(err)
	}
	if recoveryErr := recovered.MaterializeChildRun(t.Context(), fixture.request); recoveryErr != nil {
		t.Fatal(recoveryErr)
	}
	run, err := fixture.store.LoadRun(t.Context(), fixture.request.ChildRunID)
	if err != nil || run.Status != runtime.RunRunning {
		t.Fatalf("recovered run = %+v, %v", run, err)
	}
	for _, nodeID := range []string{"dependent", "root"} {
		if _, err := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: nodeID}); err != nil {
			t.Fatalf("load recovered node %s: %v", nodeID, err)
		}
	}
}

func TestPinnedChildRunMaterializerRecoversLostSaveValuesResponse(t *testing.T) {
	fixture := newChildMaterializerFixture(t)
	lost := &lostSaveValuesStore{StateStore: fixture.store, cause: errors.New("value-set response lost")}
	first, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{
		State: lost, Clock: ClockFunc(func() time.Time { return fixture.now.Add(time.Second) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if materializeErr := first.MaterializeChildRun(t.Context(), fixture.request); !errors.Is(materializeErr, lost.cause) {
		t.Fatalf("lost value-set response error = %v", materializeErr)
	}
	if _, loadErr := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: fixture.request.ChildRunID, NodeID: "root"}); !errors.Is(loadErr, runtime.ErrNotFound) {
		t.Fatalf("root unexpectedly materialized after lost response: %v", loadErr)
	}

	recovered, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{
		State: fixture.store, Clock: ClockFunc(func() time.Time { return fixture.now.Add(2 * time.Second) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if recoveryErr := recovered.MaterializeChildRun(t.Context(), fixture.request); recoveryErr != nil {
		t.Fatal(recoveryErr)
	}
	root, err := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: fixture.request.ChildRunID, NodeID: "root"})
	if err != nil || root.Status != runtime.NodeReady || root.Inputs == nil {
		t.Fatalf("recovered root = %+v, %v", root, err)
	}
	bound, err := fixture.store.LoadValues(t.Context(), *root.Inputs)
	if err != nil || !equalValueSets(bound, values.ValueSet{"message": inlineValueForTest(t, "hello")}) {
		t.Fatalf("recovered root inputs = %+v, %v", bound, err)
	}
}

func TestPinnedChildRunMaterializerConvergesWhenCanceledBeforeOrDuringMaterialization(t *testing.T) {
	t.Run("before", func(t *testing.T) {
		fixture := newChildMaterializerFixture(t)
		run, err := fixture.store.LoadRun(t.Context(), fixture.request.ChildRunID)
		if err != nil {
			t.Fatal(err)
		}
		if _, transitionErr := fixture.store.TransitionRun(t.Context(), runtime.RunTransitionRequest{
			RunID: run.ID, ExpectedGeneration: run.Generation, To: runtime.RunCanceled, At: fixture.now.Add(time.Second),
		}); transitionErr != nil {
			t.Fatal(transitionErr)
		}
		materializer, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{State: fixture.store})
		if err != nil {
			t.Fatal(err)
		}
		if materializeErr := materializer.MaterializeChildRun(t.Context(), fixture.request); materializeErr != nil {
			t.Fatal(materializeErr)
		}
		_, err = fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: "root"})
		if !errors.Is(err, runtime.ErrNotFound) {
			t.Fatalf("terminal child materialized work: %v", err)
		}
	})

	t.Run("midway", func(t *testing.T) {
		fixture := newChildMaterializerFixture(t)
		canceling := &cancelAfterCreateStore{StateStore: fixture.store, at: fixture.now.Add(time.Second)}
		materializer, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{State: canceling, Clock: ClockFunc(func() time.Time { return fixture.now.Add(2 * time.Second) })})
		if err != nil {
			t.Fatal(err)
		}
		if materializeErr := materializer.MaterializeChildRun(t.Context(), fixture.request); materializeErr != nil {
			t.Fatal(materializeErr)
		}
		run, err := fixture.store.LoadRun(t.Context(), fixture.request.ChildRunID)
		if err != nil || run.Status != runtime.RunCanceled {
			t.Fatalf("canceled run = %+v, %v", run, err)
		}
		_, rootErr := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: "root"})
		_, dependentErr := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: "dependent"})
		if (rootErr == nil) == (dependentErr == nil) {
			t.Fatalf("midway cancellation materialized root=%v dependent=%v; want exactly one", rootErr, dependentErr)
		}
		if err := materializer.MaterializeChildRun(t.Context(), fixture.request); err != nil {
			t.Fatal(err)
		}
		if rootErr != nil {
			_, rootErr = fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: "root"})
			if !errors.Is(rootErr, runtime.ErrNotFound) {
				t.Fatalf("terminal replay recreated root: %v", rootErr)
			}
		} else {
			_, dependentErr = fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: "dependent"})
			if !errors.Is(dependentErr, runtime.ErrNotFound) {
				t.Fatalf("terminal replay recreated dependent: %v", dependentErr)
			}
		}
	})

	t.Run("node creation fence", func(t *testing.T) {
		fixture := newChildMaterializerFixture(t)
		canceling := &cancelBeforeCreateStore{StateStore: fixture.store, at: fixture.now.Add(time.Second)}
		materializer, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{
			State: canceling, Clock: ClockFunc(func() time.Time { return fixture.now.Add(2 * time.Second) }),
		})
		if err != nil {
			t.Fatal(err)
		}
		if materializeErr := materializer.MaterializeChildRun(t.Context(), fixture.request); materializeErr != nil {
			t.Fatalf("terminal node-creation fence must converge: %v", materializeErr)
		}
		run, err := fixture.store.LoadRun(t.Context(), fixture.request.ChildRunID)
		if err != nil || run.Status != runtime.RunCanceled {
			t.Fatalf("fenced child = %+v, %v", run, err)
		}
		for _, nodeID := range []string{"dependent", "root"} {
			if _, loadErr := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: run.ID, NodeID: nodeID}); !errors.Is(loadErr, runtime.ErrNotFound) {
				t.Fatalf("fenced child created node %s: %v", nodeID, loadErr)
			}
		}
	})
}

func TestPinnedChildRunMaterializerLeavesControlFlowTargetsPending(t *testing.T) {
	fixture := newChildMaterializerFixture(t)
	fixture.request.Definition.Graph.Nodes = []graph.Node{
		{ID: "root", Kind: "noop", KindVersion: "v1"},
		{ID: "router", Kind: "noop", KindVersion: "v1",
			Catch: []graph.CatchRule{{Errors: []string{"temporary"}, Targets: []string{"handler"}}},
			Switch: &graph.SwitchSpec{
				Arms:    []graph.SwitchArm{{When: graph.Expression{Text: "true"}, Targets: []string{"branch"}}},
				Default: []string{"handler"},
			}},
		{ID: "handler", Kind: "noop", KindVersion: "v1"},
		{ID: "branch", Kind: "noop", KindVersion: "v1"},
		{ID: "cleanup", Kind: "noop", KindVersion: "v1", Finally: &graph.FinallySpec{}},
	}
	materializer, err := NewPinnedChildRunMaterializer(ChildRunMaterializerOptions{
		State: fixture.store, Clock: ClockFunc(func() time.Time { return fixture.now.Add(time.Second) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	if materializeErr := materializer.MaterializeChildRun(t.Context(), fixture.request); materializeErr != nil {
		t.Fatal(materializeErr)
	}
	for _, expectation := range []struct {
		id     string
		status runtime.NodeStatus
	}{
		{"root", runtime.NodeReady}, {"router", runtime.NodeReady},
		{"handler", runtime.NodePending}, {"branch", runtime.NodePending}, {"cleanup", runtime.NodePending},
	} {
		node, loadErr := fixture.store.LoadNodeInvocation(t.Context(), runtime.NodeInvocationID{RunID: fixture.request.ChildRunID, NodeID: expectation.id})
		if loadErr != nil || node.Status != expectation.status {
			t.Fatalf("node %s = %+v, %v; want %s", expectation.id, node, loadErr, expectation.status)
		}
	}
}

type childMaterializerFixture struct {
	store   *inmemory.Store
	request calladapter.ChildRunRequest
	now     time.Time
}

type childMaterializerPlanSource struct{ graph graph.Graph }

func (s childMaterializerPlanSource) LoadRecoveryPlan(_ context.Context, run runtime.RunSnapshot) (runtime.RecoveryPlan, error) {
	plan := compile.ExecutionPlan{SchemaVersion: run.Plan.SchemaVersion, ID: run.Plan.ID, Digest: run.Plan.Digest, Graph: s.graph}
	inferred := compile.InferValueDependencies(&plan, compile.DependencyOptions{})
	return runtime.RecoveryPlan{Ref: run.Plan, Plan: plan, Visibility: inferred.Visibility}, nil
}

func newChildMaterializerFixture(t *testing.T) childMaterializerFixture {
	t.Helper()
	store := inmemory.NewStore()
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	childDigest := values.SHA256Digest([]byte("child-graph-v1"))
	parentDigest := values.SHA256Digest([]byte("parent-graph-v1"))
	provenance := graph.Provenance{Authority: "registry.test", Origin: "publisher", Locator: "registry://child/v1/child.workflow.yaml", Revision: "v1", Digest: childDigest}
	child := graph.DefinitionRef{Authority: provenance.Authority, Kind: "workflow", ID: "child", Version: "v1", Digest: childDigest, Provenance: &provenance}
	resolved := compile.ResolvedDefinition{
		Definition: child,
		Graph: graph.Graph{
			ID: "child", Version: "v1", Digest: childDigest, Provenance: provenance,
			Nodes: []graph.Node{
				{ID: "root", Kind: "noop", KindVersion: "v1", InputBindings: map[string]graph.Binding{"message": {Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.message"}}}},
				{ID: "dependent", Kind: "noop", KindVersion: "v1", Needs: []graph.Need{{Node: "root", Kind: graph.EdgeControl}}},
			},
			Edges: []graph.Edge{{From: "root", To: "dependent", Kind: graph.EdgeControl}},
		},
	}
	plan := runtime.PlanRef{ID: "child", Version: "v1", Digest: childDigest, SchemaVersion: compile.ExecutionPlanSchemaVersion}
	if err := store.RecordPlan(t.Context(), plan); err != nil {
		t.Fatal(err)
	}
	inputs := values.ValueSet{"message": inlineValueForTest(t, "hello")}
	inputRef, err := store.SaveValues(t.Context(), runtime.SaveValuesRequest{Owner: runtime.ValueOwner{Kind: "child-run-inputs", RunID: "child-run"}, Values: inputs})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateRun(t.Context(), runtime.CreateRunRequest{
		ID: "child-run", Plan: plan, Status: runtime.RunPending, Inputs: &inputRef,
		StartIdempotencyKey: "child-start-key", CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	request := calladapter.ChildRunRequest{
		Parent:     calladapter.CallSiteIdentity{RunID: "parent-run", NodeID: "call-child"},
		ChildRunID: "child-run", Definition: resolved, Plan: plan, Inputs: inputs,
		Lineage: []graph.DefinitionRef{
			{Kind: "workflow", ID: "parent", Version: "v1", Digest: parentDigest},
			child,
		},
		ParentClose: graph.ParentCloseCancel, IdempotencyKey: "child-start-key",
	}
	return childMaterializerFixture{store: store, request: request, now: now}
}

func inlineValueForTest(t *testing.T, content any) values.Value {
	t.Helper()
	value, err := values.NewInline(content, values.Metadata{
		Producer:  values.Producer{Kind: "test", Reference: "fixture", Output: "value"},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func cloneValueSetForTest(t *testing.T, input values.ValueSet) values.ValueSet {
	t.Helper()
	result := make(values.ValueSet, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

type ambiguousCreateStore struct {
	runtime.StateStore
	once  sync.Once
	cause error
}

type lostSaveValuesStore struct {
	runtime.StateStore
	once  sync.Once
	cause error
}

func (s *lostSaveValuesStore) SaveValues(ctx context.Context, request runtime.SaveValuesRequest) (values.ValueSetRef, error) {
	ref, err := s.StateStore.SaveValues(ctx, request)
	if err != nil {
		return ref, err
	}
	failed := false
	s.once.Do(func() { failed = true })
	if failed {
		return values.ValueSetRef{}, s.cause
	}
	return ref, nil
}

func (s *ambiguousCreateStore) CreateNodeInvocation(ctx context.Context, request runtime.CreateNodeInvocationRequest) (runtime.NodeInvocationSnapshot, error) {
	created, err := s.StateStore.CreateNodeInvocation(ctx, request)
	if err != nil {
		return created, err
	}
	failed := false
	s.once.Do(func() { failed = true })
	if failed {
		return runtime.NodeInvocationSnapshot{}, s.cause
	}
	return created, nil
}

type cancelAfterCreateStore struct {
	runtime.StateStore
	once sync.Once
	at   time.Time
}

type cancelBeforeCreateStore struct {
	runtime.StateStore
	once sync.Once
	at   time.Time
}

func (s *cancelBeforeCreateStore) CreateNodeInvocation(ctx context.Context, request runtime.CreateNodeInvocationRequest) (runtime.NodeInvocationSnapshot, error) {
	var cancelErr error
	s.once.Do(func() {
		run, loadErr := s.LoadRun(ctx, request.Snapshot.ID.RunID)
		if loadErr != nil {
			cancelErr = loadErr
			return
		}
		_, cancelErr = s.TransitionRun(ctx, runtime.RunTransitionRequest{
			RunID: run.ID, ExpectedGeneration: run.Generation, To: runtime.RunCanceled, At: s.at,
		})
	})
	if cancelErr != nil {
		return runtime.NodeInvocationSnapshot{}, cancelErr
	}
	return s.StateStore.CreateNodeInvocation(ctx, request)
}

func (s *cancelAfterCreateStore) CreateNodeInvocation(ctx context.Context, request runtime.CreateNodeInvocationRequest) (runtime.NodeInvocationSnapshot, error) {
	created, err := s.StateStore.CreateNodeInvocation(ctx, request)
	if err != nil {
		return created, err
	}
	var cancelErr error
	s.once.Do(func() {
		run, loadErr := s.LoadRun(ctx, request.Snapshot.ID.RunID)
		if loadErr != nil {
			cancelErr = loadErr
			return
		}
		_, cancelErr = s.TransitionRun(ctx, runtime.RunTransitionRequest{
			RunID: run.ID, ExpectedGeneration: run.Generation, To: runtime.RunCanceled, At: s.at,
		})
	})
	if cancelErr != nil {
		return runtime.NodeInvocationSnapshot{}, cancelErr
	}
	return created, nil
}
