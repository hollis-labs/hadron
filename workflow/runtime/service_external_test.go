package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

func TestServiceLaunchIntentReacquiresAfterCrashAndGuaranteedTeardown(t *testing.T) {
	store, claim, node, base := dispatchFixture(t, "service-crash-window")
	host := &durableServiceHost{references: make(map[string]stepkind.ExternalOperationRef)}
	registry := stepkind.NewRegistry()
	if err := registry.Register(runtimeServiceKind{host: host}); err != nil {
		t.Fatal(err)
	}
	failing := &failServiceSuspendStore{Store: store, err: errors.New("crash after provider start")}
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: failing, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	definition := graph.Node{
		ID: node.ID.NodeID, Kind: runtimeServiceKindName, KindVersion: runtimeServiceKindVersion,
		Config:      graph.Config{"provider": "fixture", "config": map[string]any{"port": 8080}},
		Service:     &graph.ServiceNodeSpec{TeardownNodes: []string{"node-teardown"}, HeartbeatTimeout: "1m"},
		Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed},
	}
	first, dispatchErr := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "service-start-key"})
	if dispatchErr == nil || first.Node.Status != workflowruntime.NodeRunning || first.Attempt.Status != workflowruntime.NodeRunning {
		t.Fatalf("ambiguous start result = %#v, %v", first, dispatchErr)
	}
	intent, err := store.LoadService(context.Background(), node.ID)
	if err != nil || intent.Status != workflowruntime.ServiceLaunching || intent.Ref.ID != "" || host.startCalls() != 1 {
		t.Fatalf("durable launch intent = %#v calls=%d err=%v", intent, host.startCalls(), err)
	}

	workflow := graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{definition}}
	recovery := workflowruntime.RecoveryCoordinator{
		Store: store, Recovery: store, Inputs: store, Control: store,
		Plans: staticRecoveryPlans{graph: workflow}, Registry: registry,
	}
	recovered, err := recovery.Recover(context.Background(), workflowruntime.RecoveryRequest{Now: base.Add(2 * time.Hour)})
	if err != nil || len(recovered.Services) != 1 || recovered.Services[0].Service.Status != workflowruntime.ServiceStarting || host.startCalls() != 2 {
		t.Fatalf("service launch recovery = %#v calls=%d err=%v", recovered.Services, host.startCalls(), err)
	}
	if host.distinctResources() != 1 {
		t.Fatalf("same-key recovery created %d resources", host.distinctResources())
	}
	coordinator, err := workflowruntime.NewServiceCoordinator(workflowruntime.ServiceCoordinatorOptions{
		Store: store, State: store, Registry: registry, Plans: staticRecoveryPlans{graph: workflow}, Control: store,
		Now: func() time.Time { return base.Add(2*time.Hour + time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	registered, _ := registry.Lookup(runtimeServiceKindName, runtimeServiceKindVersion)
	probe, probeErr := registered.(stepkind.ServiceController).ObserveService(context.Background(), recovered.Services[0].Service.Ref)
	if probeErr != nil || probe.Validate() != nil {
		t.Fatalf("service observation probe = %#v, %v", probe, probeErr)
	}
	beforeReady, _ := store.LoadService(context.Background(), node.ID)
	if beforeReady.Status != workflowruntime.ServiceStarting {
		t.Fatalf("service changed before readiness: %#v", beforeReady)
	}
	ready, err := coordinator.Reconcile(context.Background(), node.ID)
	if err != nil || ready.Service.Status != workflowruntime.ServiceReady || ready.Node == nil || ready.Node.Status != workflowruntime.NodeSucceeded {
		t.Fatalf("service readiness = %#v failure=%#v, %v", ready, ready.Service.Failure, err)
	}

	teardownID := workflowruntime.NodeInvocationID{RunID: node.ID.RunID, NodeID: "node-teardown"}
	createNode(t, store, teardownID, workflowruntime.NodeReady, 0, base.Add(2*time.Hour+2*time.Second))
	teardownClaim, acquired, claimErr := workflowruntime.NewReadyQueueCoordinator(store, nil).ClaimNext(context.Background(), workflowruntime.ReadyClaimRequest{Owner: "worker", Token: "teardown", IdempotencyKey: "claim-service-teardown", Now: base.Add(2*time.Hour + 3*time.Second), LeaseUntil: base.Add(3 * time.Hour)})
	if claimErr != nil || !acquired || teardownClaim.Candidate.InvocationID != teardownID {
		t.Fatalf("claim teardown = %#v, %t, %v", teardownClaim, acquired, claimErr)
	}
	teardownNode, _ := store.LoadNodeInvocation(context.Background(), teardownID)
	teardownDispatcher, _ := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(2*time.Hour + 4*time.Second) }})
	teardown, err := teardownDispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{
		Claim: teardownClaim,
		Node: graph.Node{ID: teardownNode.ID.NodeID, Kind: runtimeServiceKindName, KindVersion: runtimeServiceKindVersion,
			Config: definition.Config, Service: &graph.ServiceNodeSpec{TeardownOf: node.ID.NodeID}, Finally: &graph.FinallySpec{}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed}},
		IdempotencyKey: "service-stop-key",
	})
	if err != nil || teardown.Service == nil || teardown.Service.Status != workflowruntime.ServiceStopping || teardown.Node.Status != workflowruntime.NodeWaiting {
		t.Fatalf("service teardown suspension = %#v, %v", teardown, err)
	}
	stopCoordinator, err := workflowruntime.NewServiceCoordinator(workflowruntime.ServiceCoordinatorOptions{
		Store: store, State: store, Registry: registry, Plans: staticRecoveryPlans{graph: workflow}, Control: store,
		Now: func() time.Time { return base.Add(2*time.Hour + 5*time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := stopCoordinator.Reconcile(context.Background(), node.ID)
	if err != nil || stopped.Service.Status != workflowruntime.ServiceStopped || stopped.Node == nil || stopped.Node.Status != workflowruntime.NodeSucceeded || host.stopCalls() != 1 {
		t.Fatalf("service stop = %#v calls=%d err=%v", stopped, host.stopCalls(), err)
	}
}

func TestServiceReadinessCheckFailsThroughOrdinaryVerification(t *testing.T) {
	store, host, registry, definition, service, base := startServiceFixture(t, "service-ready-check", &graph.VerificationSpec{Checks: []graph.VerificationCheck{{
		Kind: verification.CheckTests, Config: graph.Config{"required": []any{"unit"}},
	}}})
	coordinator, err := workflowruntime.NewServiceCoordinator(workflowruntime.ServiceCoordinatorOptions{
		Store: store, State: store, Registry: registry, Plans: staticRecoveryPlans{graph: serviceGraph(definition)}, Control: store,
		Now: func() time.Time { return base.Add(4 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Reconcile(context.Background(), service.Start.Invocation)
	if err != nil || result.Service.Status != workflowruntime.ServiceFailed || result.Node == nil || result.Node.Status != workflowruntime.NodeFailed || host.stopCalls() != 0 {
		t.Fatalf("readiness verification failure = %#v, %v", result, err)
	}
}

func TestServiceHeartbeatAnchorDoesNotSlideAndFailureFencesRun(t *testing.T) {
	store, host, registry, definition, service, base := startServiceFixture(t, "service-heartbeat", nil)
	now := base.Add(4 * time.Second)
	coordinator, err := workflowruntime.NewServiceCoordinator(workflowruntime.ServiceCoordinatorOptions{
		Store: store, State: store, Registry: registry, Plans: staticRecoveryPlans{graph: serviceGraph(definition)}, Control: store,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := coordinator.Reconcile(context.Background(), service.Start.Invocation)
	if err != nil || ready.Service.Status != workflowruntime.ServiceReady {
		t.Fatalf("service ready = %#v, %v", ready, err)
	}
	anchor := ready.Service.LastHeartbeatAt
	host.setObservation(stepkind.ServiceObservation{State: stepkind.ServiceObservationStarting})
	now = anchor.Add(30 * time.Second)
	pending, err := coordinator.Reconcile(context.Background(), service.Start.Invocation)
	if err != nil || pending.Service.Status != workflowruntime.ServiceReady || !pending.Service.LastHeartbeatAt.Equal(anchor) || !pending.Service.LastObservedAt.Equal(now) {
		t.Fatalf("heartbeat-free observation slid anchor: %#v, %v", pending.Service, err)
	}
	if _, heartbeatErr := store.ApplyServiceHeartbeat(context.Background(), workflowruntime.ApplyServiceHeartbeatRequest{
		Start: pending.Service.Start.Invocation, ExpectedServiceGeneration: pending.Service.Generation,
		ObservedAt: now, HeartbeatAt: anchor.Add(-time.Second), At: now.Add(time.Second),
	}); !errors.Is(heartbeatErr, workflowruntime.ErrInvalidRecord) {
		t.Fatalf("in-memory store accepted a regressive heartbeat: %v", heartbeatErr)
	}
	now = anchor.Add(time.Minute + time.Second)
	failed, err := coordinator.Reconcile(context.Background(), service.Start.Invocation)
	if err != nil || failed.Service.Status != workflowruntime.ServiceFailed {
		t.Fatalf("heartbeat timeout = %#v, %v", failed, err)
	}
	intent, err := store.LoadTerminalIntent(context.Background(), service.Start.Invocation.RunID)
	if err != nil || intent.IntendedStatus != workflowruntime.RunFailed || len(intent.Finalizers) != 1 || intent.Finalizers[0].Invocation.NodeID != definition.ID+"-teardown" {
		t.Fatalf("heartbeat failure terminal intent = %#v, %v", intent, err)
	}
}

func TestGeneratedServiceTeardownIsGlobalFinalizer(t *testing.T) {
	start := graph.Node{ID: "database", Kind: runtimeServiceKindName, KindVersion: runtimeServiceKindVersion, Service: &graph.ServiceNodeSpec{TeardownNodes: []string{"database-teardown"}}}
	workflow := serviceGraph(start)
	scopes, err := workflowruntime.PlanFinalizerScopes(workflow, workflowruntime.RunID("service-global-finalizer"))
	if err != nil || len(scopes) != 1 || scopes[0].Invocation.NodeID != "database-teardown" || len(scopes[0].Scope) != 1 || scopes[0].Scope[0].NodeID != "database" {
		t.Fatalf("service finalizer scopes = %#v, %v", scopes, err)
	}
}

func startServiceFixture(t *testing.T, suffix string, readyCheck *graph.VerificationSpec) (*inmemory.Store, *durableServiceHost, *stepkind.MemoryRegistry, graph.Node, workflowruntime.ServiceSnapshot, time.Time) {
	t.Helper()
	store, claim, node, base := dispatchFixture(t, suffix)
	run, err := store.LoadRun(context.Background(), node.ID.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := store.TransitionRun(context.Background(), workflowruntime.RunTransitionRequest{RunID: run.ID, ExpectedGeneration: run.Generation, To: workflowruntime.RunRunning, At: base.Add(time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	host := &durableServiceHost{references: make(map[string]stepkind.ExternalOperationRef)}
	registry := stepkind.NewRegistry()
	if registerErr := registry.Register(runtimeServiceKind{host: host}); registerErr != nil {
		t.Fatal(registerErr)
	}
	definition := graph.Node{
		ID: node.ID.NodeID, Kind: runtimeServiceKindName, KindVersion: runtimeServiceKindVersion,
		Config: graph.Config{"provider": "fixture", "config": map[string]any{}}, Service: &graph.ServiceNodeSpec{
			TeardownNodes: []string{node.ID.NodeID + "-teardown"}, HeartbeatTimeout: "1m", ReadyCheck: readyCheck,
		}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed},
	}
	createNode(t, store, workflowruntime.NodeInvocationID{RunID: node.ID.RunID, NodeID: node.ID.NodeID + "-teardown"}, workflowruntime.NodePending, 0, base.Add(time.Second))
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{Store: store, Registry: registry, Now: func() time.Time { return base.Add(3 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	dispatched, err := dispatcher.Dispatch(context.Background(), workflowruntime.DispatchRequest{Claim: claim, Node: definition, IdempotencyKey: "service-key-" + suffix})
	if err != nil || dispatched.Service == nil || dispatched.Service.Status != workflowruntime.ServiceStarting {
		t.Fatalf("service dispatch = %#v, %v", dispatched, err)
	}
	return store, host, registry, definition, *dispatched.Service, base
}

func serviceGraph(start graph.Node) graph.Graph {
	teardown := graph.Node{
		ID: start.ID + "-teardown", Kind: start.Kind, KindVersion: start.KindVersion, Config: start.Config,
		Service: &graph.ServiceNodeSpec{TeardownOf: start.ID}, Finally: &graph.FinallySpec{}, Idempotency: &graph.IdempotencySpec{Mode: graph.IdempotencyKeyed},
	}
	return graph.Graph{ID: "plan", Version: "v1", Nodes: []graph.Node{start, teardown}}
}

type failServiceSuspendStore struct {
	*inmemory.Store
	err error
}

func (s *failServiceSuspendStore) SuspendServiceStart(context.Context, workflowruntime.SuspendServiceStartRequest) (workflowruntime.SuspendServiceStartResult, error) {
	return workflowruntime.SuspendServiceStartResult{}, s.err
}

const runtimeServiceKindName, runtimeServiceKindVersion = "service-fixture", "v1"

type runtimeServiceStartRequest struct {
	Provider, IdempotencyKey string
}

type runtimeServiceKind struct{ host *durableServiceHost }

func (runtimeServiceKind) Spec() stepkind.StepKindSpec {
	return stepkind.StepKindSpec{
		Name: runtimeServiceKindName, Version: runtimeServiceKindVersion,
		ConfigSchema: graph.Schema{"type": "object"}, InputSchema: graph.Schema{"type": "object"}, OutputSchema: graph.Schema{"type": "object"},
		Effects: graph.EffectSet{graph.EffectMaterialize}, Idempotency: graph.IdempotencyKeyed, RetrySafety: stepkind.RetryRequiresIdempotency,
		Cancellation: stepkind.CancellationSpec{Mode: stepkind.CancellationExplicit}, Observation: stepkind.ObservationSpec{Mode: stepkind.ObservationPoll, Heartbeat: true}, Lifecycle: stepkind.LifecycleSpec{Service: true},
	}
}
func (runtimeServiceKind) ValidateConfig(context.Context, graph.Config) []diagnostic.Diagnostic {
	return nil
}
func (k runtimeServiceKind) Execute(ctx context.Context, prepared stepkind.PreparedInvocation) (stepkind.StepResult, error) {
	if prepared.Invocation.Service != nil {
		if prepared.Invocation.Service.Absent {
			return stepkind.StepResult{Outcome: stepkind.StepCompleted, Outputs: values.ValueSet{}}, nil
		}
		ref := prepared.Invocation.Service.Ref
		return stepkind.StepResult{Outcome: stepkind.StepExternal, External: &ref}, nil
	}
	provider, _ := prepared.Invocation.Config["provider"].(string)
	ref, err := k.host.Start(ctx, runtimeServiceStartRequest{Provider: provider, IdempotencyKey: prepared.Invocation.IdempotencyKey})
	return stepkind.StepResult{Outcome: stepkind.StepExternal, External: &ref}, err
}
func (k runtimeServiceKind) ObserveService(ctx context.Context, ref stepkind.ExternalOperationRef) (stepkind.ServiceObservation, error) {
	return k.host.Observe(ctx, ref)
}
func (k runtimeServiceKind) RequestStop(ctx context.Context, ref stepkind.ExternalOperationRef, key string) error {
	return k.host.RequestStop(ctx, ref, key)
}

type durableServiceHost struct {
	mu          sync.Mutex
	references  map[string]stepkind.ExternalOperationRef
	starts      int
	stops       int
	stopped     bool
	observation *stepkind.ServiceObservation
}

func (h *durableServiceHost) Start(_ context.Context, request runtimeServiceStartRequest) (stepkind.ExternalOperationRef, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.starts++
	if ref, ok := h.references[request.IdempotencyKey]; ok {
		return ref, nil
	}
	ref := stepkind.ExternalOperationRef{Kind: "fixture-service", ID: "service-1", Metadata: map[string]string{"provider": request.Provider}}
	h.references[request.IdempotencyKey] = ref
	return ref, nil
}

func (h *durableServiceHost) Observe(context.Context, stepkind.ExternalOperationRef) (stepkind.ServiceObservation, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return stepkind.ServiceObservation{State: stepkind.ServiceObservationStopped}, nil
	}
	if h.observation != nil {
		return *h.observation, nil
	}
	return stepkind.ServiceObservation{State: stepkind.ServiceObservationReady, Heartbeat: true, Outputs: values.ValueSet{}}, nil
}

func (h *durableServiceHost) RequestStop(context.Context, stepkind.ExternalOperationRef, string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.stops++
	h.stopped = true
	return nil
}

func (h *durableServiceHost) startCalls() int { h.mu.Lock(); defer h.mu.Unlock(); return h.starts }
func (h *durableServiceHost) stopCalls() int  { h.mu.Lock(); defer h.mu.Unlock(); return h.stops }
func (h *durableServiceHost) distinctResources() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.references)
}

func (h *durableServiceHost) setObservation(observation stepkind.ServiceObservation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.observation = &observation
}
