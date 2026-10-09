package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/libs/workflow/graph"
	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
	"github.com/hollis-labs/libs/workflow/runtime/inmemory"
	"github.com/hollis-labs/libs/workflow/stepkind"
	"github.com/hollis-labs/libs/workflow/stepkind/stepkindtest"
	"github.com/hollis-labs/libs/workflow/values"
)

func TestWorkflowExternalReconcilerDrivesPendingOperationToCompletion(t *testing.T) {
	store, attempt, registry, kind := suspendedInMemoryExternalOperation(t)
	var observations atomic.Int32
	kind.ObserveFunc = func(context.Context, stepkind.ExternalOperationRef) (stepkind.Observation, error) {
		if observations.Add(1) < 2 {
			return stepkind.Observation{State: stepkind.ObservationPending, Progress: map[string]string{"step": "running"}}, nil
		}
		result := stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}
		return stepkind.Observation{State: stepkind.ObservationSucceeded, Result: &result}, nil
	}
	coordinator, err := workflowruntime.NewExternalOperationCoordinator(workflowruntime.ExternalOperationOptions{Store: store, Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := newWorkflowExternalReconciler(coordinator, 10*time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Start()
	reconciler.Start() // idempotent
	deadline := time.Now().Add(5 * time.Second)
	for {
		operation, loadErr := store.LoadExternalOperation(t.Context(), attempt)
		if loadErr == nil && operation.Status == stepkind.ObservationSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("external operation = %#v, %v", operation, loadErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	node, err := store.LoadNodeInvocation(t.Context(), attempt.Invocation)
	if err != nil || node.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("external node = %#v, %v", node, err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := reconciler.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	settled := observations.Load()
	time.Sleep(50 * time.Millisecond)
	if observations.Load() != settled {
		t.Fatal("reconciler kept observing after Stop")
	}
	if err := reconciler.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop = %v", err)
	}
}

type blockingExternalDriver struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *blockingExternalDriver) Recover(context.Context, workflowruntime.ExternalOperationQuery) ([]workflowruntime.ExternalOperationSnapshot, error) {
	return []workflowruntime.ExternalOperationSnapshot{{Attempt: workflowruntime.AttemptID{Invocation: workflowruntime.NodeInvocationID{RunID: "run", NodeID: "node"}, Number: 1}}}, nil
}

func (d *blockingExternalDriver) Reconcile(ctx context.Context, _ workflowruntime.AttemptID) (workflowruntime.ExternalOperationResult, error) {
	d.once.Do(func() { close(d.entered) })
	<-d.release
	return workflowruntime.ExternalOperationResult{}, ctx.Err()
}

func TestWorkflowExternalReconcilerStopHonorsDeadlineThenDrains(t *testing.T) {
	driver := &blockingExternalDriver{entered: make(chan struct{}), release: make(chan struct{})}
	reconciler, err := newWorkflowExternalReconciler(driver, time.Hour, 1)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.Start()
	<-driver.entered
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := reconciler.Stop(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop with blocked reconcile = %v", err)
	}
	close(driver.release)
	if err := reconciler.Stop(context.Background()); err != nil {
		t.Fatalf("draining Stop = %v", err)
	}
	if _, err := newWorkflowExternalReconciler(nil, time.Second, 1); err == nil {
		t.Fatal("nil driver was accepted")
	}
	if _, err := newWorkflowExternalReconciler(driver, 0, 1); err == nil {
		t.Fatal("zero interval was accepted")
	}
}

func TestProductionRuntimeComposesExternalReconciler(t *testing.T) {
	runtime, _, _ := newTestProductionWorkflowRuntime(t)
	if runtime.external == nil || runtime.external.driver != runtime.host.ExternalOperations() ||
		runtime.external.interval != workflowExternalReconcileInterval || runtime.external.batch != workflowExternalReconcileBatch {
		t.Fatalf("external reconciler = %#v", runtime.external)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	runtime.external.mu.Lock()
	running := runtime.external.cancel != nil
	runtime.external.mu.Unlock()
	if running {
		t.Fatal("external reconciler still running after Shutdown")
	}
}

// suspendedInMemoryExternalOperation dispatches one node of an explicit
// external kind so the runtime persists a pending external operation.
func suspendedInMemoryExternalOperation(t *testing.T) (*inmemory.Store, workflowruntime.AttemptID, stepkind.Registry, *stepkindtest.LifecycleKind) {
	t.Helper()
	ctx := t.Context()
	store := inmemory.NewStore()
	// The dispatcher and coordinator use the wall clock, so the claim lease
	// must be anchored to it.
	now := time.Now().UTC().Add(-time.Minute)
	runID := workflowruntime.RunID("external-reconcile")
	plan := workflowruntime.PlanRef{ID: "external-plan", Version: "v1", Digest: values.SHA256Digest([]byte("external-plan")), SchemaVersion: "1"}
	if _, _, err := store.CreateRun(ctx, workflowruntime.CreateRunRequest{ID: runID, Plan: plan, Status: workflowruntime.RunPending, StartIdempotencyKey: "start-external", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	inputs, err := store.SaveValues(ctx, workflowruntime.SaveValuesRequest{Owner: workflowruntime.ValueOwner{Kind: "node-inputs", RunID: runID}, Values: values.ValueSet{}})
	if err != nil {
		t.Fatal(err)
	}
	id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "node"}
	node, err := store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{
		ID: id, Status: workflowruntime.NodePending, Inputs: &inputs, CreatedAt: now, UpdatedAt: now,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: id, ExpectedGeneration: node.Generation, To: workflowruntime.NodeReady, At: now.Add(time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	claim, ok, err := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(ctx, workflowruntime.ReadyClaimRequest{
		Owner: "worker", Token: "token", IdempotencyKey: "claim-external", Now: now.Add(2 * time.Second), LeaseUntil: now.Add(time.Hour),
	})
	if err != nil || !ok {
		t.Fatalf("ClaimNext = %#v, %v, %v", claim, ok, err)
	}
	registry := stepkind.NewRegistry()
	kind := stepkindtest.NewLifecycleKind("external-kind", "v1")
	kind.ExecuteFunc = func(context.Context, stepkind.PreparedInvocation) (stepkind.StepResult, error) {
		return stepkind.StepResult{Outcome: stepkind.StepExternal, External: &stepkind.ExternalOperationRef{Kind: "job", ID: "job-1"}}, nil
	}
	if registerErr := registry.Register(kind); registerErr != nil {
		t.Fatal(registerErr)
	}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.Dispatch(ctx, workflowruntime.DispatchRequest{Claim: claim, Node: graph.Node{ID: "node", Kind: "external-kind", KindVersion: "v1", Config: graph.Config{}}})
	if err != nil || result.External == nil || result.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("Dispatch(external) = %#v, %v", result, err)
	}
	return store, result.Attempt.ID, registry, kind
}
