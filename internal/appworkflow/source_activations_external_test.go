package appworkflow_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	calladapter "github.com/hollis-labs/go-workflow/adapters/call"
	waitadapter "github.com/hollis-labs/go-workflow/adapters/wait"
	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
)

func TestSourceActivationsMaterializeCompiledDeclarationsAndUseCommonHostStart(t *testing.T) {
	loaded, loadErr := workflowcompile.LoadFile("testdata/activations.workflow.yaml")
	if loadErr != nil || loaded.Source == nil || len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadFile = %#v, %v", loaded, loadErr)
	}
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("Compile = %#v", compiled)
	}
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, compiled.Plan)
	identity := testIdentityBinding("service:activation", "activation")
	identity.Extension = map[string]string{"exposure_ref": "source-activation-route"}
	store, storeErr := persistence.NewWorkflowActivationStore(fixture.store)
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	service := appworkflow.ActivationService{Store: store, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now })}
	exposures := make(map[string]string, len(compiled.Plan.Graph.Activations))
	for _, declaration := range compiled.Plan.Graph.Activations {
		exposures[declaration.ID] = identity.Extension["exposure_ref"]
	}
	result, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: compiled.Plan, Identity: identity, ExposureRefs: exposures, Enabled: true, At: fixture.now,
	})
	if err != nil || result.Outcome != runtime.IdempotencyApplied || len(result.Registrations) != len(compiled.Plan.Graph.Activations) {
		t.Fatalf("ReconcileSourcePlan = %#v, %v", result, err)
	}
	byTemplate := make(map[string]hoststate.ActivationRegistration, len(result.Registrations))
	for _, registration := range result.Registrations {
		if registration.Derivation == nil || registration.ID == registration.Derivation.TemplateID ||
			registration.Definition.Digest != compiled.Plan.Definition.Digest || registration.Derivation.PlanDigest != compiled.Plan.Digest {
			t.Fatalf("derived registration = %#v", registration)
		}
		byTemplate[registration.Derivation.TemplateID] = registration
	}
	replayed, replayErr := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: compiled.Plan, Identity: identity, ExposureRefs: exposures, Enabled: true, At: fixture.now.Add(time.Second),
	})
	if replayErr != nil || replayed.Outcome != runtime.IdempotencyReplayed || replayed.SourceGeneration != result.SourceGeneration {
		t.Fatalf("source activation replay = %#v, %v", replayed, replayErr)
	}
	if len(result.Registrations) != 6 {
		t.Fatalf("materialized source kinds = %d, want 6", len(result.Registrations))
	}
	for template, want := range map[string]hoststate.ActivationSourceKind{
		"incoming-hook": hoststate.ActivationSourceWebhook, "daily-schedule": hoststate.ActivationSourceSchedule,
		"agent-message": hoststate.ActivationSourceExternal, "inbox-files": hoststate.ActivationSourceFile,
		"project-event": hoststate.ActivationSourceExternal, "setup-callback": hoststate.ActivationSourceCallback,
	} {
		if got := byTemplate[template]; got.Source.Kind != want || got.Derivation.TemplateDigest == "" || got.Derivation.MaterializationDigest == "" {
			t.Errorf("materialized %s = %#v", template, got)
		}
	}
	if callback := byTemplate["setup-callback"]; !callback.Source.OneShot || callback.Source.Config["ttl"] != "15m" {
		t.Fatalf("one-shot TTL activation = %#v", callback)
	}
	exposures["agent-message"] = "mutated-exposure"
	identity.RunScope.Attributes["cost_center"] = "mutated-scope"
	for index := range compiled.Plan.Graph.Activations {
		if compiled.Plan.Graph.Activations[index].ID == "agent-message" {
			compiled.Plan.Graph.Activations[index].Config["to"] = "mutated-route"
		}
	}
	storedMessage, err := store.LoadActivation(t.Context(), byTemplate["agent-message"].ID)
	if err != nil || storedMessage.Principal.ExposureRef != "source-activation-route" ||
		storedMessage.RunScope.Attributes["cost_center"] != "research" || storedMessage.Source.Config["to"] != "msg://agent/hadron/bulk-create" {
		t.Fatalf("source materialization retained caller aliases = %#v, %v", storedMessage, err)
	}
}

func TestSourceActivationUsesCommonHostStartPath(t *testing.T) {
	loaded := workflowcompile.LoadBytes("source-start.workflow.yaml", []byte(`workflow:
  name: Source Start
  version: 1.0.0
  provenance:
    authority: project
on:
  message:
    name: Message
    to: msg://agent/hadron/source-start
    extract:
      message: message.text
inputs:
  - name: message
    type: string
steps:
  - name: Accept
    transform:
      expression: inputs.message
`))
	if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadBytes = %#v", loaded)
	}
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("Compile = %#v", compiled)
	}
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, compiled.Plan)
	identity := testIdentityBinding("service:activation", "activation")
	identity.Extension = map[string]string{"exposure_ref": "source-start-route"}
	host := hostWithFixedIdentity(t, fixture, identity)
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	store, err := persistence.NewWorkflowActivationStore(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	service := appworkflow.ActivationService{Host: host, Store: store, Clock: appworkflow.ClockFunc(func() time.Time { return fixture.now })}
	materialized, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: compiled.Plan, Identity: identity, ExposureRefs: map[string]string{"message": identity.Extension["exposure_ref"]},
		Enabled: true, At: fixture.now,
	})
	if err != nil || len(materialized.Registrations) != 1 {
		t.Fatalf("ReconcileSourcePlan = %#v, %v", materialized, err)
	}
	message := materialized.Registrations[0]
	request := appworkflow.ExternalActivationRequest{
		RegistrationID: message.ID, IdempotencyKey: "message-delivery", OccurredAt: fixture.now.Add(time.Minute), ReceivedAt: fixture.now.Add(time.Minute),
		Payload: map[string]any{"message": map[string]any{"text": "hello"}}, SourceRef: "message-source",
	}
	started, startErr := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), request)
	if startErr != nil || started.Start.Run == nil || started.Start.Run.Plan.Digest != compiled.Plan.Digest || started.Start.Outcome != runtime.IdempotencyApplied {
		t.Fatalf("ActivateExternal(source registration) = %#v, %v", started, startErr)
	}
	replayed, replayErr := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), request)
	if replayErr != nil || replayed.Outcome != runtime.IdempotencyReplayed || replayed.Start.Run == nil || replayed.Start.Run.ID != started.Start.Run.ID {
		t.Fatalf("ActivateExternal(source replay) = %#v, %v", replayed, replayErr)
	}
	changed := request
	changed.Payload = map[string]any{"message": map[string]any{"text": "changed"}}
	if _, err := service.ActivateExternal(authenticatedContext(t.Context(), "service:activation"), changed); !errors.Is(err, runtime.ErrIdempotencyConflict) {
		t.Fatalf("ActivateExternal(source conflict) = %v", err)
	}
}

func TestDerivedSourceReactorAdmissionHoldsCurrentAliasFenceThroughDelivery(t *testing.T) {
	correlationDigest, err := values.DigestInline("project-fenced")
	if err != nil {
		t.Fatal(err)
	}
	correlation := "reactor-correlation-" + correlationDigest[len("sha256:"):]
	plan := compileSourceActivationPlan(t, "source-reactor-current-fence.workflow.yaml", []byte(fmt.Sprintf(`workflow:
  name: Source Reactor Current Fence
  version: 1.0.0
  provenance:
    authority: project
on:
  event:
    name: Project Event
    type: project.changed
    source: project://fixture
    deduplication_key: event.project_id
    extract:
      cursor: event.cursor
inputs:
  - name: cursor
    type: string
outputs:
  cursor:
    type: string
    value: steps.await.outputs.payload.event.cursor
durability:
  mode: steps
  continue_as_new:
    max_events: 2
    carry: [cursor]
steps:
  - name: await
    kind_version: v1
    wait_for:
      event:
        type: project.changed
        source: project://fixture
      correlation: %s
      timeout: 24h
      payload_schema:
        type: object
`, correlation)))
	plan = inferHostPlan(t, plan)
	plan.Definition = graph.DefinitionRef{
		Kind: appworkflow.DefinitionKindRegistry, ID: plan.Graph.ID, Version: plan.Graph.Version,
		Digest: plan.Definition.Digest, Authority: plan.Definition.Authority,
	}
	plan.Digest, err = workflowcompile.PlanDigest(*plan)
	if err != nil {
		t.Fatal(err)
	}

	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	activationStore, err := persistence.NewWorkflowActivationStore(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	identity := testIdentityBinding("service:reactor-fence", "activation")
	identity.ExecutionTarget.Capabilities = append(identity.ExecutionTarget.Capabilities, waitadapter.CapabilityWait)
	identity.Extension = map[string]string{"exposure_ref": "source-reactor-fence-route"}
	clock := &reactorTestClock{at: fixture.now}
	host := newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, clock, hoststate.PolicyAllow)
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })

	materializer := appworkflow.ActivationService{Host: host, Store: activationStore, Clock: clock}
	materialized, err := materializer.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: plan, Identity: identity, ExposureRefs: map[string]string{"project-event": identity.Extension["exposure_ref"]},
		Enabled: true, At: clock.Now(),
	})
	if err != nil || len(materialized.Registrations) != 1 {
		t.Fatalf("ReconcileSourcePlan = %#v, %v", materialized, err)
	}
	registration := materialized.Registrations[0]
	fence := &lockingSourceActivationRegistry{resolution: hadronregistry.WorkflowResolution{
		Movable: true,
		Record: hadronregistry.WorkflowRecord{
			Name: registration.Definition.ID, Version: registration.Definition.Version, Digest: registration.Definition.Digest,
			Authority: registration.Definition.Authority, PlanDigest: registration.Derivation.PlanDigest,
		},
	}}
	recordReached, releaseRecord := make(chan struct{}), make(chan struct{})
	store := &blockingActivationEventStore{ActivationStore: activationStore, reached: recordReached, release: releaseRecord}
	service := appworkflow.ActivationService{Host: host, Store: store, Clock: clock, CurrentRegistry: fence, RequireCurrentFence: true}
	clock.Set(fixture.now.Add(time.Minute))
	request := appworkflow.ExternalActivationRequest{
		RegistrationID: registration.ID, IdempotencyKey: "reactor-current-fence", OccurredAt: clock.Now(), ReceivedAt: clock.Now(),
		SourceRef: "project-event-source", Payload: map[string]any{"event": map[string]any{"project_id": "project-fenced", "cursor": "cursor-0"}},
	}
	type activationOutcome struct {
		result appworkflow.ActivationStartResult
		err    error
	}
	activated := make(chan activationOutcome, 1)
	go func() {
		result, activateErr := service.ActivateExternal(authenticatedContext(context.Background(), identity.Principal), request)
		activated <- activationOutcome{result: result, err: activateErr}
	}()
	<-recordReached

	mutationStarted, mutationAcquired := make(chan struct{}), make(chan struct{})
	go func() {
		close(mutationStarted)
		fence.mutate(func() { close(mutationAcquired) })
	}()
	<-mutationStarted
	select {
	case <-mutationAcquired:
		t.Fatal("current mutation escaped the source-reactor admission fence")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseRecord)
	outcome := <-activated
	if outcome.err != nil || outcome.result.Reactor == nil || outcome.result.Reactor.Delivery.Status != runtime.ReactorDeliveryApplied {
		t.Fatalf("fenced source-reactor activation = %#v, %v", outcome.result, outcome.err)
	}
	select {
	case <-mutationAcquired:
	case <-time.After(time.Second):
		t.Fatal("current mutation did not proceed after reactor delivery released the fence")
	}
}

type lockingSourceActivationRegistry struct {
	gate       sync.RWMutex
	resolution hadronregistry.WorkflowResolution
}

func (r *lockingSourceActivationRegistry) WithSourceActivationCurrent(ctx context.Context, operation func(appworkflow.SourceActivationRegistry) error) error {
	r.gate.RLock()
	defer r.gate.RUnlock()
	return operation(r)
}

func (r *lockingSourceActivationRegistry) ResolveWorkflow(_ context.Context, query hadronregistry.WorkflowQuery) (hadronregistry.WorkflowResolution, error) {
	if query != (hadronregistry.WorkflowQuery{Name: r.resolution.Record.Name}) {
		return hadronregistry.WorkflowResolution{}, runtime.ErrNotFound
	}
	return r.resolution, nil
}

func (r *lockingSourceActivationRegistry) mutate(operation func()) {
	r.gate.Lock()
	defer r.gate.Unlock()
	operation()
}

type blockingActivationEventStore struct {
	hoststate.ActivationStore
	reached chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *blockingActivationEventStore) RecordActivationEvent(ctx context.Context, event hoststate.ActivationEvent) (gosched.Fire, runtime.IdempotencyOutcome, error) {
	s.once.Do(func() { close(s.reached) })
	<-s.release
	return s.ActivationStore.RecordActivationEvent(ctx, event)
}

func TestSourceEventActivationDrivesAuthoredWaitsExactlyOnceAcrossGenerations(t *testing.T) {
	correlationDigest, digestErr := values.DigestInline("project-42")
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	correlation := "reactor-correlation-" + correlationDigest[len("sha256:"):]
	plan := compileSourceActivationPlan(t, "source-reactor-authored-waits.workflow.yaml", []byte(fmt.Sprintf(`workflow:
  name: Source Reactor Authored Waits
  version: 1.0.0
  provenance:
    authority: project
on:
  event:
    name: Project Event
    type: project.changed
    source: project://fixture
    deduplication_key: event.project_id
    extract:
      cursor: event.cursor
inputs:
  - name: cursor
    type: string
outputs:
  cursor:
    type: string
    value: steps.awaitsecond.outputs.payload.event.cursor
durability:
  mode: steps
  continue_as_new:
    max_events: 3
    carry: [cursor]
steps:
  - name: awaitfirst
    kind_version: v1
    wait_for:
      event:
        type: project.changed
        source: project://fixture
      correlation: %s
      timeout: 24h
      payload_schema:
        type: object
  - name: awaitsecond
    kind_version: v1
    needs: [awaitfirst]
    wait_for:
      event:
        type: project.changed
        source: project://fixture
      correlation: %s
      timeout: 24h
      payload_schema:
        type: object
`, correlation, correlation)))
	plan = inferHostPlan(t, plan)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	activationStore, err := persistence.NewWorkflowActivationStore(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	identity := testIdentityBinding("service:reactor", "activation")
	identity.ExecutionTarget.Capabilities = append(identity.ExecutionTarget.Capabilities, waitadapter.CapabilityWait)
	identity.Extension = map[string]string{"exposure_ref": "source-reactor-route"}
	clock := &reactorTestClock{at: fixture.now}
	host := newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, clock, hoststate.PolicyAllow)
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	service := appworkflow.ActivationService{Host: host, Store: activationStore, Clock: clock}
	materialized, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{Plan: plan, Identity: identity,
		ExposureRefs: map[string]string{"project-event": identity.Extension["exposure_ref"]}, Enabled: true, At: clock.Now()})
	if err != nil || len(materialized.Registrations) != 1 {
		t.Fatalf("ReconcileSourcePlan = %#v, %v", materialized, err)
	}
	registration := materialized.Registrations[0]
	clock.Set(fixture.now.Add(time.Minute))
	firstRequest := appworkflow.ExternalActivationRequest{RegistrationID: registration.ID, IdempotencyKey: "reactor-event-1",
		OccurredAt: clock.Now(), ReceivedAt: clock.Now(), SourceRef: "project-event-source",
		Payload: map[string]any{"event": map[string]any{"project_id": "project-42", "cursor": "cursor-0"}}}
	first, err := service.ActivateExternal(authenticatedContext(t.Context(), identity.Principal), firstRequest)
	if err != nil || first.Reactor == nil || first.Reactor.Delivery.Status != runtime.ReactorDeliveryApplied ||
		!first.Reactor.Delivery.StartsGeneration || first.Reactor.Delivery.Receipt == nil ||
		first.Reactor.Delivery.Receipt.Kind != runtime.ReactorDeliveryStartedRun || first.Reactor.Reactor.EventCount != 1 {
		t.Fatalf("initial reactor activation = %#v, %v", first, err)
	}
	reactorID := first.Reactor.Reactor.Identity.ID
	firstRun := first.Reactor.Reactor.CurrentRunID
	if first.Reactor.Reactor.Identity.Correlation != correlation || first.Reactor.Reactor.Identity.RegistrationID != registration.ID ||
		first.Reactor.Reactor.Identity.Plan.Digest != plan.Digest || first.Reactor.Reactor.Identity.Provenance.Digest != plan.Graph.Provenance.Digest {
		t.Fatalf("source-derived reactor identity = %#v", first.Reactor.Reactor.Identity)
	}
	if inputs := loadReactorRunInputs(t, fixture.state, firstRun); inputs["cursor"].Inline != "cursor-0" {
		t.Fatalf("first activation was not consumed exactly once as start input: %#v", inputs)
	}

	secondRequest := reactorActivation(firstRequest, "reactor-event-2", "cursor-1", fixture.now.Add(2*time.Minute))
	thirdRequest := reactorActivation(firstRequest, "reactor-event-3", "cursor-2", fixture.now.Add(2*time.Minute))
	clock.Set(secondRequest.ReceivedAt)
	for _, pendingRequest := range []appworkflow.ExternalActivationRequest{secondRequest, thirdRequest} {
		pending, pendingErr := service.ActivateExternal(authenticatedContext(t.Context(), identity.Principal), pendingRequest)
		if pendingErr != nil || pending.Reactor == nil || pending.Reactor.Delivery.Status != runtime.ReactorDeliveryPending {
			t.Fatalf("pre-wait activation %s = %#v, %v", pendingRequest.IdempotencyKey, pending, pendingErr)
		}
	}
	if shutdownErr := host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}

	secondDB, err := persistence.Open(fixture.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	secondState, err := persistence.NewWorkflowStateStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	secondJournal, err := persistence.NewWorkflowHostStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	secondActivationStore, err := persistence.NewWorkflowActivationStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(fixture.now.Add(3 * time.Minute))
	host = newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, clock, hoststate.PolicyAllow)
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	secondHost := newAuthoredReactorHost(t, fixture, secondState, secondJournal, secondActivationStore, identity, clock, hoststate.PolicyAllow)
	if startErr := secondHost.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	service.Host = host
	secondService := appworkflow.ActivationService{Host: secondHost, Store: secondActivationStore, Clock: clock}
	for _, key := range []string{secondRequest.IdempotencyKey, thirdRequest.IdempotencyKey} {
		pending, loadErr := fixture.state.LoadReactorDelivery(t.Context(), reactorID, key)
		if loadErr != nil || pending.Status != runtime.ReactorDeliveryPending || pending.RunID != firstRun {
			t.Fatalf("pending delivery %s after reopen = %#v, %v", key, pending, loadErr)
		}
	}

	firstWait := dispatchAuthoredReactorNode(t, host, fixture.state, plan, firstRun, "awaitfirst", clock, "g1-first-open", runtime.NodeWaiting)
	if firstWait.Wait == nil || firstWait.Wait.SignalName != "project.changed" || firstWait.Wait.Correlation != correlation {
		t.Fatalf("authored first wait = %#v", firstWait.Wait)
	}
	clock.Set(fixture.now.Add(4 * time.Minute))
	type concurrentResult struct {
		request appworkflow.ExternalActivationRequest
		result  appworkflow.ActivationStartResult
		err     error
	}
	gate := make(chan struct{})
	results := make(chan concurrentResult, 2)
	candidates := []struct {
		service appworkflow.ActivationService
		request appworkflow.ExternalActivationRequest
	}{{service, secondRequest}, {secondService, thirdRequest}}
	for _, candidate := range candidates {
		go func(candidate struct {
			service appworkflow.ActivationService
			request appworkflow.ExternalActivationRequest
		}) {
			<-gate
			result, activateErr := candidate.service.ActivateExternal(authenticatedContext(context.Background(), identity.Principal), candidate.request)
			results <- concurrentResult{request: candidate.request, result: result, err: activateErr}
		}(candidate)
	}
	close(gate)
	for range 2 {
		concurrent := <-results
		if concurrent.err != nil || concurrent.result.Reactor == nil {
			t.Fatalf("concurrent delivery %s = %#v, %v", concurrent.request.IdempotencyKey, concurrent.result, concurrent.err)
		}
	}
	var appliedRequest, pendingRequest appworkflow.ExternalActivationRequest
	for _, candidate := range []appworkflow.ExternalActivationRequest{secondRequest, thirdRequest} {
		delivery, loadErr := fixture.state.LoadReactorDelivery(t.Context(), reactorID, candidate.IdempotencyKey)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		switch delivery.Status {
		case runtime.ReactorDeliveryApplied:
			if appliedRequest.IdempotencyKey != "" || delivery.Receipt == nil || delivery.Receipt.Kind != runtime.ReactorDeliveryResumedWait ||
				delivery.Receipt.Update == nil || delivery.Receipt.Update.WaitID != firstWait.Wait.Ref.ID {
				t.Fatalf("first applied delivery receipt = %#v", delivery)
			}
			appliedRequest = candidate
		case runtime.ReactorDeliveryPending:
			if pendingRequest.IdempotencyKey != "" || delivery.Receipt != nil {
				t.Fatalf("pending concurrent delivery = %#v", delivery)
			}
			pendingRequest = candidate
		default:
			t.Fatalf("concurrent delivery was silently closed/dropped = %#v", delivery)
		}
	}
	if appliedRequest.IdempotencyKey == "" || pendingRequest.IdempotencyKey == "" {
		t.Fatalf("concurrent split applied=%#v pending=%#v", appliedRequest, pendingRequest)
	}
	if reactor, loadErr := fixture.state.LoadReactor(t.Context(), reactorID); loadErr != nil || reactor.EventCount != 2 {
		t.Fatalf("only one concurrent delivery counted = %#v, %v", reactor, loadErr)
	}
	completeFirst := dispatchAuthoredReactorNode(t, host, fixture.state, plan, firstRun, "awaitfirst", clock, "g1-first-complete", runtime.NodeSucceeded)
	assertAuthoredWaitCompletion(t, fixture.state, completeFirst, activationCursor(t, appliedRequest))
	secondWait := dispatchAuthoredReactorNode(t, host, fixture.state, plan, firstRun, "awaitsecond", clock, "g1-second-open", runtime.NodeWaiting)
	clock.Set(fixture.now.Add(5 * time.Minute))
	pendingApplied, err := service.ActivateExternal(authenticatedContext(t.Context(), identity.Principal), pendingRequest)
	if err != nil || pendingApplied.Reactor == nil || pendingApplied.Reactor.Delivery.Status != runtime.ReactorDeliveryApplied ||
		pendingApplied.Reactor.Delivery.Receipt == nil || pendingApplied.Reactor.Delivery.Receipt.Update == nil ||
		pendingApplied.Reactor.Delivery.Receipt.Update.WaitID != secondWait.Wait.Ref.ID || pendingApplied.Reactor.Reactor.EventCount != 3 {
		t.Fatalf("pending delivery reached second authored wait = %#v, %v", pendingApplied, err)
	}
	completeSecond := dispatchAuthoredReactorNode(t, host, fixture.state, plan, firstRun, "awaitsecond", clock, "g1-second-complete", runtime.NodeSucceeded)
	terminalCursor := activationCursor(t, pendingRequest)
	assertAuthoredWaitCompletion(t, fixture.state, completeSecond, terminalCursor)
	again, err := service.ActivateExternal(authenticatedContext(t.Context(), identity.Principal), pendingRequest)
	if err != nil || again.Reactor == nil || again.Reactor.Reactor.EventCount != 3 || again.Reactor.Delivery.Status != runtime.ReactorDeliveryApplied {
		t.Fatalf("duplicate activation replay = %#v, %v", again, err)
	}
	changed := pendingRequest
	changed.Payload = map[string]any{"event": map[string]any{"project_id": "project-42", "cursor": "changed"}}
	if _, conflictErr := service.ActivateExternal(authenticatedContext(t.Context(), identity.Principal), changed); !errors.Is(conflictErr, runtime.ErrIdempotencyConflict) {
		t.Fatalf("changed duplicate activation = %v", conflictErr)
	}
	finalizeAuthoredReactorGeneration(t, fixture.state, plan, firstRun, terminalCursor, clock)
	if shutdownErr := host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	if shutdownErr := secondHost.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	clock.Set(fixture.now.Add(6 * time.Minute))
	host = newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, clock, hoststate.PolicyAllow)
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	service.Host = host
	continued, err := fixture.state.LoadReactor(t.Context(), reactorID)
	if err != nil || continued.CurrentGeneration != 2 || continued.Status != runtime.ReactorWaiting ||
		continued.Identity.Correlation != correlation || continued.Identity.Plan.Digest != plan.Digest {
		t.Fatalf("continued reactor = %#v, %v", continued, err)
	}
	if inputs := loadReactorRunInputs(t, fixture.state, continued.CurrentRunID); inputs["cursor"].Inline != terminalCursor {
		t.Fatalf("continued exact typed state = %#v", inputs)
	}

	for generation := uint64(2); generation <= 4; generation++ {
		current, loadErr := fixture.state.LoadReactor(t.Context(), reactorID)
		if loadErr != nil || current.CurrentGeneration != generation || current.EventCount != 0 {
			t.Fatalf("generation %d before delivery = %#v, %v", generation, current, loadErr)
		}
		for index, nodeID := range []string{"awaitfirst", "awaitsecond"} {
			opened := dispatchAuthoredReactorNode(t, host, fixture.state, plan, current.CurrentRunID, nodeID, clock,
				fmt.Sprintf("g%d-%d-open", generation, index), runtime.NodeWaiting)
			at := fixture.now.Add(time.Duration(generation*10+uint64(index)) * time.Minute)
			delivery := reactorActivation(firstRequest, fmt.Sprintf("reactor-event-%d-%d", generation, index+1),
				fmt.Sprintf("cursor-%d-%d", generation, index+1), at)
			clock.Set(at)
			result, deliveryErr := service.ActivateExternal(authenticatedContext(t.Context(), identity.Principal), delivery)
			if deliveryErr != nil || result.Reactor == nil || result.Reactor.Delivery.Status != runtime.ReactorDeliveryApplied ||
				result.Reactor.Delivery.Receipt == nil || result.Reactor.Delivery.Receipt.Update == nil || result.Reactor.Delivery.Receipt.Update.WaitID != opened.Wait.Ref.ID {
				t.Fatalf("generation %d delivery %d = %#v, %v", generation, index, result, deliveryErr)
			}
			completed := dispatchAuthoredReactorNode(t, host, fixture.state, plan, current.CurrentRunID, nodeID, clock,
				fmt.Sprintf("g%d-%d-complete", generation, index), runtime.NodeSucceeded)
			terminalCursor = activationCursor(t, delivery)
			assertAuthoredWaitCompletion(t, fixture.state, completed, terminalCursor)
		}
		finalizeAuthoredReactorGeneration(t, fixture.state, plan, current.CurrentRunID, terminalCursor, clock)
		if shutdownErr := host.Shutdown(t.Context()); shutdownErr != nil {
			t.Fatal(shutdownErr)
		}
		clock.Set(clock.Now().Add(time.Minute))
		host = newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, clock, hoststate.PolicyAllow)
		if startErr := host.Start(t.Context()); startErr != nil {
			t.Fatal(startErr)
		}
		service.Host = host
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()) })
	bounded, err := fixture.state.LoadReactor(t.Context(), reactorID)
	if err != nil || bounded.CurrentGeneration != 5 || bounded.EventCount != 0 {
		t.Fatalf("bounded reactor after long history = %#v, %v", bounded, err)
	}
	var generationRows, deliveryRows, maximumAttempts, maximumEvents int
	if queryErr := fixture.store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_reactor_generations WHERE reactor_id=?`, reactorID).Scan(&generationRows); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := fixture.store.DB().QueryRow(`SELECT COUNT(1) FROM workflow_reactor_deliveries WHERE reactor_id=?`, reactorID).Scan(&deliveryRows); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := fixture.store.DB().QueryRow(`SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(1) AS n FROM workflow_attempts GROUP BY run_id)`).Scan(&maximumAttempts); queryErr != nil {
		t.Fatal(queryErr)
	}
	if queryErr := fixture.store.DB().QueryRow(`SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(1) AS n FROM workflow_events GROUP BY run_id)`).Scan(&maximumEvents); queryErr != nil {
		t.Fatal(queryErr)
	}
	if generationRows != 5 || deliveryRows != 9 || maximumAttempts != 2 || maximumEvents > 40 {
		t.Fatalf("bounded histories generations=%d deliveries=%d max_attempts=%d max_events=%d", generationRows, deliveryRows, maximumAttempts, maximumEvents)
	}

	// Crash after the canonical update has resumed its exact wait but before
	// the reactor delivery receipt is sealed. Recovery must reuse that immutable
	// authorized update even if current policy now denies fresh updates.
	recoveryReactor, err := fixture.state.LoadReactor(t.Context(), reactorID)
	if err != nil {
		t.Fatal(err)
	}
	recoveryWait := dispatchAuthoredReactorNode(t, host, fixture.state, plan, recoveryReactor.CurrentRunID, "awaitfirst", clock,
		"recovery-policy-open", runtime.NodeWaiting)
	clock.Set(clock.Now().Add(time.Minute))
	recoveryPayload, err := values.NewInline(map[string]any{"event": map[string]any{"project_id": "project-42", "cursor": "recovery-cursor"}},
		values.Metadata{Producer: values.Producer{Kind: "reactor-activation", Reference: "reactor-recovery-delivery"}, MediaType: "application/json",
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	responder := workflowwait.Responder{Kind: "hadron_identity", Reference: identity.Principal,
		Attributes: map[string]string{"source_authority": identity.SourceAuthority, "trust": identity.Trust}}
	_, recoveryDelivery, _, err := fixture.state.BeginReactorDelivery(t.Context(), runtime.BeginReactorDeliveryRequest{Identity: recoveryReactor.Identity,
		InitialRunID: firstRun, ContinueAfterEvents: recoveryReactor.ContinueAfterEvents,
		Delivery: runtime.ReactorDeliveryRequest{ReactorID: reactorID, IdempotencyKey: "reactor-recovery-delivery", SignalName: "project.changed",
			Payload: recoveryPayload, Responder: responder, OccurredAt: clock.Now(), ReceivedAt: clock.Now()}, At: clock.Now()})
	if err != nil {
		t.Fatal(err)
	}
	claimedRecovery, err := fixture.state.ClaimReactorDelivery(t.Context(), runtime.ClaimReactorDeliveryRequest{ReactorID: reactorID,
		IdempotencyKey: recoveryDelivery.Request.IdempotencyKey, ExpectedGeneration: recoveryDelivery.Generation,
		WaitID: recoveryWait.Wait.Ref.ID, At: clock.Now()})
	if err != nil {
		t.Fatal(err)
	}
	selector := runtime.SignalSelector{RunID: recoveryReactor.CurrentRunID, Name: "project.changed", Correlation: correlation}
	updateKey := reactorUpdateKeyForTest(reactorID, recoveryDelivery.Request.IdempotencyKey, recoveryWait.Wait.Ref.ID)
	updated, err := host.UpdateRun(authenticatedContext(t.Context(), identity.Principal), appworkflow.UpdateRunRequest{Selector: selector,
		Payload: recoveryPayload, IdempotencyKey: updateKey,
		Identity: appworkflow.IdentityRequest{PrincipalHint: identity.Principal, SourceAuthority: identity.SourceAuthority}})
	if err != nil || updated.Status != runtime.RunUpdateApplied || updated.Receipt == nil || updated.Receipt.WaitID != recoveryWait.Wait.Ref.ID {
		t.Fatalf("crash-window canonical update = %#v, %v", updated, err)
	}
	stillApplying, err := fixture.state.LoadReactorDelivery(t.Context(), reactorID, recoveryDelivery.Request.IdempotencyKey)
	if err != nil || stillApplying.Status != runtime.ReactorDeliveryApplying || stillApplying.Generation != claimedRecovery.Generation {
		t.Fatalf("unsealed crash-window delivery = %#v, %v", stillApplying, err)
	}
	if shutdownErr := host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	clock.Set(clock.Now().Add(time.Minute))
	host = newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, clock, hoststate.PolicyDeny)
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatalf("restart after policy change = %v", startErr)
	}
	service.Host = host
	recoveredDelivery, err := fixture.state.LoadReactorDelivery(t.Context(), reactorID, recoveryDelivery.Request.IdempotencyKey)
	recoveredReactor, reactorErr := fixture.state.LoadReactor(t.Context(), reactorID)
	if err != nil || reactorErr != nil || recoveredDelivery.Status != runtime.ReactorDeliveryApplied || recoveredDelivery.Receipt == nil ||
		recoveredDelivery.Receipt.Update == nil || recoveredDelivery.Receipt.Update.WaitID != recoveryWait.Wait.Ref.ID || recoveredReactor.EventCount != 1 {
		t.Fatalf("policy-independent update recovery delivery=%#v reactor=%#v errors=%v/%v", recoveredDelivery, recoveredReactor, err, reactorErr)
	}

	// A resume observer can fail after the wait update commits. Recovery still
	// seals both the update and reactor receipt and must not keep Host unready.
	dispatchAuthoredReactorNode(t, host, fixture.state, plan, recoveredReactor.CurrentRunID, "awaitfirst", clock,
		"post-commit-first-complete", runtime.NodeSucceeded)
	postCommitWait := dispatchAuthoredReactorNode(t, host, fixture.state, plan, recoveredReactor.CurrentRunID, "awaitsecond", clock,
		"post-commit-second-open", runtime.NodeWaiting)
	clock.Set(clock.Now().Add(time.Minute))
	postCommitPayload, err := values.NewInline(map[string]any{"event": map[string]any{"project_id": "project-42", "cursor": "post-commit-cursor"}},
		values.Metadata{Producer: values.Producer{Kind: "reactor-activation", Reference: "reactor-post-commit"}, MediaType: "application/json",
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	_, postCommitDelivery, _, err := fixture.state.BeginReactorDelivery(t.Context(), runtime.BeginReactorDeliveryRequest{Identity: recoveredReactor.Identity,
		InitialRunID: firstRun, ContinueAfterEvents: recoveredReactor.ContinueAfterEvents,
		Delivery: runtime.ReactorDeliveryRequest{ReactorID: reactorID, IdempotencyKey: "reactor-post-commit", SignalName: "project.changed",
			Payload: postCommitPayload, Responder: responder, OccurredAt: clock.Now(), ReceivedAt: clock.Now()}, At: clock.Now()})
	if err != nil {
		t.Fatal(err)
	}
	postCommitClaim, err := fixture.state.ClaimReactorDelivery(t.Context(), runtime.ClaimReactorDeliveryRequest{ReactorID: reactorID,
		IdempotencyKey: postCommitDelivery.Request.IdempotencyKey, ExpectedGeneration: postCommitDelivery.Generation,
		WaitID: postCommitWait.Wait.Ref.ID, At: clock.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if shutdownErr := host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	clock.Set(clock.Now().Add(time.Minute))
	host = newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, clock, hoststate.PolicyDeny,
		failingWaitMaterializer{err: errors.New("observer unavailable")})
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatalf("post-commit reactor recovery = %v", startErr)
	}
	service.Host = host
	sealedPostCommit, err := fixture.state.LoadReactorDelivery(t.Context(), reactorID, postCommitDelivery.Request.IdempotencyKey)
	postCommitReactor, reactorErr := fixture.state.LoadReactor(t.Context(), reactorID)
	postCommitUpdate, updateErr := fixture.state.LoadRunUpdate(t.Context(), reactorUpdateKeyForTest(reactorID,
		postCommitDelivery.Request.IdempotencyKey, postCommitWait.Wait.Ref.ID))
	if err != nil || reactorErr != nil || updateErr != nil || sealedPostCommit.Status != runtime.ReactorDeliveryApplied ||
		sealedPostCommit.Generation != postCommitClaim.Generation+1 || postCommitUpdate.Status != runtime.RunUpdateApplied || postCommitReactor.EventCount != 2 {
		t.Fatalf("post-commit reactor convergence delivery=%#v update=%#v reactor=%#v errors=%v/%v/%v",
			sealedPostCommit, postCommitUpdate, postCommitReactor, err, updateErr, reactorErr)
	}

	// A terminal failure below max_events converges on restart: the reactor is
	// durably failed, queued work is explicitly closed, duplicate delivery
	// replays that receipt, and a new delivery is rejected without being stored.
	terminalPendingRequest := reactorActivation(firstRequest, "reactor-terminal-pending", "terminal-pending", clock.Now().Add(time.Minute))
	clock.Set(terminalPendingRequest.ReceivedAt)
	terminalPendingPayload, err := values.NewInline(terminalPendingRequest.Payload,
		values.Metadata{Producer: values.Producer{Kind: "reactor-activation", Reference: terminalPendingRequest.IdempotencyKey}, MediaType: "application/json",
			Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	_, terminalPending, _, err := fixture.state.BeginReactorDelivery(t.Context(), runtime.BeginReactorDeliveryRequest{Identity: postCommitReactor.Identity,
		InitialRunID: firstRun, ContinueAfterEvents: postCommitReactor.ContinueAfterEvents,
		Delivery: runtime.ReactorDeliveryRequest{ReactorID: reactorID, IdempotencyKey: terminalPendingRequest.IdempotencyKey, SignalName: "project.changed",
			Payload: terminalPendingPayload, Responder: responder, OccurredAt: terminalPendingRequest.OccurredAt, ReceivedAt: terminalPendingRequest.ReceivedAt}, At: clock.Now()})
	if err != nil || terminalPending.Status != runtime.ReactorDeliveryPending {
		t.Fatalf("terminal pending delivery = %#v, %v", terminalPending, err)
	}
	terminalRun, err := fixture.state.LoadRun(t.Context(), postCommitReactor.CurrentRunID)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(clock.Now().Add(time.Minute))
	failedRun, err := fixture.state.TransitionRun(t.Context(), runtime.RunTransitionRequest{RunID: terminalRun.ID,
		ExpectedGeneration: terminalRun.Generation, To: runtime.RunFailed, At: clock.Now()})
	if err != nil || failedRun.Snapshot.Status != runtime.RunFailed {
		t.Fatalf("terminal reactor run failure = %#v, %v", failedRun, err)
	}
	if shutdownErr := host.Shutdown(t.Context()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	clock.Set(clock.Now().Add(time.Minute))
	host = newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, clock, hoststate.PolicyAllow)
	if startErr := host.Start(t.Context()); startErr != nil {
		t.Fatalf("restart after terminal reactor failure = %v", startErr)
	}
	service.Host = host
	failedReactor, reactorErr := fixture.state.LoadReactor(t.Context(), reactorID)
	closedPending, deliveryErr := fixture.state.LoadReactorDelivery(t.Context(), reactorID, terminalPendingRequest.IdempotencyKey)
	if reactorErr != nil || deliveryErr != nil || failedReactor.Status != runtime.ReactorFailed || closedPending.Status != runtime.ReactorDeliveryClosed ||
		closedPending.Receipt == nil || closedPending.Receipt.Kind != runtime.ReactorDeliveryTerminalRun || closedPending.Receipt.RunStatus != runtime.RunFailed {
		t.Fatalf("terminal reactor recovery reactor=%#v delivery=%#v errors=%v/%v", failedReactor, closedPending, reactorErr, deliveryErr)
	}
	terminalReplay, err := (appworkflow.ReactorService{Host: host, Activations: activationStore, Store: fixture.state, Clock: clock}).Deliver(
		authenticatedContext(t.Context(), identity.Principal), appworkflow.ReactorDeliveryRequest{RegistrationID: registration.ID,
			IdempotencyKey: terminalPendingRequest.IdempotencyKey, Correlation: correlation, Payload: terminalPendingRequest.Payload,
			OccurredAt: terminalPendingRequest.OccurredAt, ReceivedAt: terminalPendingRequest.ReceivedAt})
	if err != nil || terminalReplay.Outcome != runtime.IdempotencyReplayed || terminalReplay.Delivery.Status != runtime.ReactorDeliveryClosed {
		t.Fatalf("closed terminal delivery replay = %#v, %v", terminalReplay, err)
	}
	lateRequest := reactorActivation(firstRequest, "reactor-terminal-late", "terminal-late", clock.Now().Add(time.Minute))
	if _, activationErr := service.ActivateExternal(authenticatedContext(t.Context(), identity.Principal), lateRequest); !errors.Is(activationErr, runtime.ErrReactorTerminal) {
		t.Fatalf("late terminal reactor activation = %v", activationErr)
	}
	if _, loadErr := fixture.state.LoadReactorDelivery(t.Context(), reactorID, lateRequest.IdempotencyKey); !errors.Is(loadErr, runtime.ErrNotFound) {
		t.Fatalf("late terminal activation persisted = %v", loadErr)
	}

	operator := registration
	operator.ID = "operator-reactor"
	operator.Authority = hoststate.ActivationAuthorityOperator
	operator.Derivation = nil
	operator.Generation = 1
	operator.CreatedAt, operator.UpdatedAt = clock.Now(), clock.Now()
	if _, _, registerErr := service.Register(t.Context(), operator); registerErr != nil {
		t.Fatal(registerErr)
	}
	_, err = (appworkflow.ReactorService{Host: host, Activations: activationStore, Store: fixture.state}).Deliver(t.Context(), appworkflow.ReactorDeliveryRequest{
		RegistrationID: operator.ID, IdempotencyKey: "operator-delivery", Correlation: correlation,
		Payload: map[string]any{"event": map[string]any{"project_id": "project-42"}}, OccurredAt: clock.Now(), ReceivedAt: clock.Now(),
	})
	if !errors.Is(err, appworkflow.ErrInvalidActivation) {
		t.Fatalf("operator reactor delivery = %v", err)
	}
}

func TestReactorGenerationStartOwnerAndDuplicateReplayAcrossTwoHandles(t *testing.T) {
	correlation := "reactor-start-race-correlation"
	plan := compileSourceActivationPlan(t, "source-reactor-start-race.workflow.yaml", []byte(fmt.Sprintf(`workflow:
  name: Source Reactor Start Race
  version: 1.0.0
  provenance:
    authority: project
on:
  event:
    name: Project Event
    type: project.changed
    source: project://fixture
    deduplication_key: event.project_id
    extract:
      cursor: event.cursor
inputs:
  - name: cursor
    type: string
outputs:
  cursor:
    type: string
    value: steps.awaitnext.outputs.payload.event.cursor
durability:
  mode: steps
  continue_as_new:
    max_events: 2
    carry: [cursor]
steps:
  - name: awaitnext
    kind_version: v1
    wait_for:
      event:
        type: project.changed
        source: project://fixture
      correlation: %s
      timeout: 24h
      payload_schema:
        type: object
`, correlation)))
	plan = inferHostPlan(t, plan)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	activationStore, err := persistence.NewWorkflowActivationStore(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	identity := testIdentityBinding("service:reactor-start-race", "activation")
	identity.ExecutionTarget.Capabilities = append(identity.ExecutionTarget.Capabilities, waitadapter.CapabilityWait)
	identity.Extension = map[string]string{"exposure_ref": "source-reactor-start-race-route"}
	firstClock := &reactorTestClock{at: fixture.now.Add(20 * time.Minute)}
	firstHost := newAuthoredReactorHost(t, fixture, fixture.state, fixture.journal, activationStore, identity, firstClock, hoststate.PolicyAllow)
	if startErr := firstHost.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	activationService := appworkflow.ActivationService{Host: firstHost, Store: activationStore, Clock: firstClock}
	materialized, err := activationService.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{Plan: plan, Identity: identity,
		ExposureRefs: map[string]string{"project-event": identity.Extension["exposure_ref"]}, Enabled: true, At: fixture.now})
	if err != nil || len(materialized.Registrations) != 1 {
		t.Fatalf("ReconcileSourcePlan = %#v, %v", materialized, err)
	}
	registration := materialized.Registrations[0]

	secondDB, err := persistence.Open(fixture.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondDB.Close() })
	secondState, err := persistence.NewWorkflowStateStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	secondJournal, err := persistence.NewWorkflowHostStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	secondActivationStore, err := persistence.NewWorkflowActivationStore(secondDB)
	if err != nil {
		t.Fatal(err)
	}
	secondClock := &reactorTestClock{at: fixture.now.Add(30 * time.Minute)}
	secondHost := newAuthoredReactorHost(t, fixture, secondState, secondJournal, secondActivationStore, identity, secondClock, hoststate.PolicyAllow)
	if startErr := secondHost.Start(t.Context()); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() {
		_ = firstHost.Shutdown(context.Background())
		_ = secondHost.Shutdown(context.Background())
	})

	firstAt := fixture.now.Add(10 * time.Minute)
	firstRequest := appworkflow.ReactorDeliveryRequest{RegistrationID: registration.ID, IdempotencyKey: "start-owner",
		Correlation: correlation, Payload: map[string]any{"event": map[string]any{"project_id": "project-42", "cursor": "owner-cursor"}},
		OccurredAt: firstAt, ReceivedAt: firstAt}
	laterRequest := appworkflow.ReactorDeliveryRequest{RegistrationID: registration.ID, IdempotencyKey: "later-delivery",
		Correlation: correlation, Payload: map[string]any{"event": map[string]any{"project_id": "project-42", "cursor": "later-cursor"}},
		OccurredAt: fixture.now.Add(5 * time.Minute), ReceivedAt: fixture.now.Add(5 * time.Minute)}
	beginReached, releaseBegin := make(chan struct{}), make(chan struct{})
	blockedStore := &blockingBeginReactorStore{ReactorStore: fixture.state, key: firstRequest.IdempotencyKey, reached: beginReached, release: releaseBegin}
	firstService := appworkflow.ReactorService{Host: firstHost, Activations: activationStore, Store: blockedStore, Clock: firstClock}
	secondService := appworkflow.ReactorService{Host: secondHost, Activations: secondActivationStore, Store: secondState, Clock: secondClock}

	type deliveryOutcome struct {
		result appworkflow.ReactorDeliveryResult
		err    error
	}
	ownerResult := make(chan deliveryOutcome, 1)
	go func() {
		result, deliverErr := firstService.Deliver(context.Background(), firstRequest)
		ownerResult <- deliveryOutcome{result: result, err: deliverErr}
	}()
	<-beginReached
	later, err := secondService.Deliver(t.Context(), laterRequest)
	if err != nil || later.Delivery.Status != runtime.ReactorDeliveryPending || later.Delivery.StartsGeneration {
		t.Fatalf("later delivery while starting = %#v, %v", later, err)
	}
	duplicateResult := make(chan deliveryOutcome, 1)
	go func() {
		result, deliverErr := secondService.Deliver(context.Background(), firstRequest)
		duplicateResult <- deliveryOutcome{result: result, err: deliverErr}
	}()
	duplicate := <-duplicateResult
	close(releaseBegin)
	owner := <-ownerResult
	for name, outcome := range map[string]deliveryOutcome{"owner": owner, "duplicate": duplicate} {
		if outcome.err != nil || outcome.result.Delivery.Status != runtime.ReactorDeliveryApplied || outcome.result.Delivery.Receipt == nil ||
			outcome.result.Delivery.Receipt.Kind != runtime.ReactorDeliveryStartedRun ||
			!outcome.result.Delivery.Receipt.ProcessedAt.Equal(firstRequest.ReceivedAt) {
			t.Fatalf("%s start delivery = %#v, %v", name, outcome.result, outcome.err)
		}
	}
	if owner.result.Outcome != runtime.IdempotencyApplied || duplicate.result.Outcome != runtime.IdempotencyReplayed {
		t.Fatalf("generation start outcomes owner=%s duplicate=%s", owner.result.Outcome, duplicate.result.Outcome)
	}
	reactorID, firstRun := owner.result.Reactor.Identity.ID, owner.result.Reactor.CurrentRunID
	if inputs := loadReactorRunInputs(t, fixture.state, firstRun); inputs["cursor"].Inline != "owner-cursor" {
		t.Fatalf("non-owner payload won generation start = %#v", inputs)
	}
	firstClock.Set(fixture.now.Add(40 * time.Minute))
	secondClock.Set(fixture.now.Add(40 * time.Minute))
	waiting := dispatchAuthoredReactorNode(t, firstHost, fixture.state, plan, firstRun, "awaitnext", firstClock, "start-race-open", runtime.NodeWaiting)
	laterApplied, err := secondService.Deliver(t.Context(), laterRequest)
	if err != nil || laterApplied.Delivery.Status != runtime.ReactorDeliveryApplied || laterApplied.Delivery.Receipt == nil ||
		laterApplied.Delivery.Receipt.Update == nil || laterApplied.Delivery.Receipt.Update.WaitID != waiting.Wait.Ref.ID || laterApplied.Reactor.EventCount != 2 {
		t.Fatalf("later delivery after generation start = %#v, %v", laterApplied, err)
	}
	completed := dispatchAuthoredReactorNode(t, firstHost, fixture.state, plan, firstRun, "awaitnext", firstClock, "start-race-complete", runtime.NodeSucceeded)
	assertAuthoredWaitCompletion(t, fixture.state, completed, "later-cursor")
	if stored, loadErr := fixture.state.LoadReactor(t.Context(), reactorID); loadErr != nil || stored.EventCount != 2 {
		t.Fatalf("generation start counted deliveries = %#v, %v", stored, loadErr)
	}
}

type blockingBeginReactorStore struct {
	runtime.ReactorStore
	key     string
	reached chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *blockingBeginReactorStore) BeginReactorDelivery(ctx context.Context, request runtime.BeginReactorDeliveryRequest) (runtime.ReactorSnapshot, runtime.ReactorDeliverySnapshot, runtime.IdempotencyOutcome, error) {
	reactor, delivery, outcome, err := s.ReactorStore.BeginReactorDelivery(ctx, request)
	if err == nil && request.Delivery.IdempotencyKey == s.key {
		s.once.Do(func() { close(s.reached) })
		<-s.release
	}
	return reactor, delivery, outcome, err
}

type reactorTestClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *reactorTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *reactorTestClock) Set(at time.Time) {
	c.mu.Lock()
	c.at = at.UTC()
	c.mu.Unlock()
}

func newAuthoredReactorHost(t *testing.T, fixture *hostFixture, state *persistence.WorkflowStateStore, journal *persistence.WorkflowHostStore,
	activationStore hoststate.ActivationStore, binding hoststate.IdentityBinding, clock *reactorTestClock, outcome hoststate.PolicyOutcome,
	materializers ...workflowwait.Materializer,
) *appworkflow.Host {
	t.Helper()
	waitFor, err := waitadapter.NewWaitFor(waitadapter.Options{Now: clock.Now, Authority: waitadapter.AuthorityResolverFunc(
		func(_ context.Context, request waitadapter.AuthorityRequest) (workflowwait.ResponderAuthority, error) {
			if request.Source.Kind != waitadapter.SourceEvent || request.Source.Reference != "project.changed" {
				return workflowwait.ResponderAuthority{}, errors.New("unexpected reactor wait authority request")
			}
			return workflowwait.ResponderAuthority{Kind: "hadron_identity", Reference: binding.Principal,
				Attributes: map[string]string{"source_authority": binding.SourceAuthority, "trust": binding.Trust}}, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	var waits *runtime.WaitCoordinator
	if len(materializers) > 1 {
		t.Fatal("at most one reactor wait materializer is supported")
	}
	if len(materializers) == 1 {
		waits = &runtime.WaitCoordinator{Store: state, Scheduler: fixture.scheduler, Materializer: materializers[0]}
	}
	host, err := appworkflow.New(appworkflow.Options{State: state, Journal: journal, Definitions: definitionProvider{plan: fixture.plan},
		Identity: identityProviderFunc(func(context.Context, appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
			return binding.Clone(), nil
		}),
		Policy: appworkflow.PolicyEvaluatorFunc(func(context.Context, hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			return hoststate.PolicyDecision{Outcome: outcome, Reason: "source reactor policy"}, nil
		}), Kinds: []stepkind.StepKind{waitFor}, RequiredKinds: []appworkflow.KindRef{{Name: waitadapter.WaitForName, Version: waitadapter.Version}},
		Activations: fixture.scheduler, ActivationStore: activationStore, Waits: waits, Artifacts: fixture.artifacts, Clock: clock,
		RecoveryInterval: time.Hour, RecoveryBatchLimit: 2,
		ChildRuns: childMaterializerFunc(func(context.Context, calladapter.ChildRunRequest) error { return nil })})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

type failingWaitMaterializer struct{ err error }

func (f failingWaitMaterializer) Materialize(context.Context, workflowwait.Materialization) error {
	return nil
}
func (f failingWaitMaterializer) Resolve(context.Context, workflowwait.Materialization) error {
	return f.err
}

func reactorActivation(base appworkflow.ExternalActivationRequest, key, cursor string, at time.Time) appworkflow.ExternalActivationRequest {
	base.IdempotencyKey, base.OccurredAt, base.ReceivedAt = key, at.UTC(), at.UTC()
	base.Payload = map[string]any{"event": map[string]any{"project_id": "project-42", "cursor": cursor}}
	return base
}

func activationCursor(t *testing.T, request appworkflow.ExternalActivationRequest) string {
	t.Helper()
	event, ok := request.Payload["event"].(map[string]any)
	if !ok {
		t.Fatalf("activation event = %#v", request.Payload)
	}
	cursor, ok := event["cursor"].(string)
	if !ok {
		t.Fatalf("activation cursor = %#v", event)
	}
	return cursor
}

func loadReactorRunInputs(t *testing.T, state *persistence.WorkflowStateStore, runID runtime.RunID) values.ValueSet {
	t.Helper()
	run, err := state.LoadRun(t.Context(), runID)
	if err != nil || run.Inputs == nil {
		t.Fatalf("load run inputs %s = %#v, %v", runID, run, err)
	}
	inputs, err := state.LoadValues(t.Context(), *run.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	return inputs
}

func dispatchAuthoredReactorNode(t *testing.T, host *appworkflow.Host, state *persistence.WorkflowStateStore, plan *workflowcompile.ExecutionPlan,
	runID runtime.RunID, nodeID string, clock *reactorTestClock, key string, want runtime.NodeStatus,
) runtime.DispatchResult {
	t.Helper()
	var node graph.Node
	for _, candidate := range plan.Graph.Nodes {
		if candidate.ID == nodeID {
			node = candidate
			break
		}
	}
	if node.ID == "" {
		t.Fatalf("plan node %s not found", nodeID)
	}
	clock.Set(clock.Now().Add(time.Second))
	queue := runtime.NewReadyQueueCoordinator(state, nil)
	claim, acquired, err := queue.ClaimNext(t.Context(), runtime.ReadyClaimRequest{RunID: runID, Owner: "reactor-worker-" + key,
		Token: "reactor-token-" + key, IdempotencyKey: "reactor-claim-" + key, Now: clock.Now(), LeaseUntil: clock.Now().Add(time.Minute)})
	if err == nil && !acquired {
		run, loadErr := state.LoadRun(t.Context(), runID)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		expression, expressionErr := runtime.BuildExpressionContext(t.Context(), state, state, plan.Graph, runID)
		if expressionErr != nil {
			t.Fatal(expressionErr)
		}
		inferred := workflowcompile.InferValueDependencies(plan, workflowcompile.DependencyOptions{})
		if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
			t.Fatalf("reactor recovery visibility = %#v", inferred.Diagnostics)
		}
		driver := runtime.NodeDriver{Store: state, Inputs: state, Control: state, Registry: host.Registry()}
		clock.Set(clock.Now().Add(time.Second))
		progressed, progressErr := driver.Drive(t.Context(), runtime.DriveNodeRequest{Run: run,
			Plan:         runtime.RecoveryPlan{Ref: run.Plan, Plan: *plan, Visibility: inferred.Visibility},
			InvocationID: runtime.NodeInvocationID{RunID: runID, NodeID: nodeID}, Node: node, ExpressionContext: expression, At: clock.Now()})
		if progressErr != nil || progressed.Progressed.Snapshot.Status != runtime.NodeReady {
			t.Fatalf("progress reactor node %s = %#v, %v", nodeID, progressed, progressErr)
		}
		claim, acquired, err = queue.ClaimNext(t.Context(), runtime.ReadyClaimRequest{RunID: runID, Owner: "reactor-worker-" + key,
			Token: "reactor-token-" + key, IdempotencyKey: "reactor-claim-" + key + "-progressed", Now: clock.Now(), LeaseUntil: clock.Now().Add(time.Minute)})
	}
	if err != nil || !acquired {
		t.Fatalf("claim reactor node %s = %#v, %v, %v", nodeID, claim, acquired, err)
	}
	if claim.Candidate.InvocationID.NodeID != nodeID {
		t.Fatalf("claimed reactor node = %s, want %s", claim.Candidate.InvocationID.NodeID, nodeID)
	}
	result, err := host.Dispatcher().Dispatch(t.Context(), runtime.DispatchRequest{Claim: claim, Node: node, IdempotencyKey: "reactor-dispatch-" + key})
	if err != nil || result.Node.Status != want {
		t.Fatalf("dispatch reactor node %s = %#v, %v; want %s", nodeID, result, err, want)
	}
	return result
}

func assertAuthoredWaitCompletion(t *testing.T, state *persistence.WorkflowStateStore, result runtime.DispatchResult, wantCursor string) {
	t.Helper()
	if result.Result == nil || result.Result.Outputs["payload"].Inline == nil {
		t.Fatalf("completed wait result = %#v", result)
	}
	payload, ok := result.Result.Outputs["payload"].Inline.(map[string]any)
	if !ok {
		t.Fatalf("completed wait payload = %#v", result.Result.Outputs["payload"])
	}
	event, ok := payload["event"].(map[string]any)
	if !ok || event["cursor"] != wantCursor {
		t.Fatalf("completed wait event = %#v, want cursor %s", payload, wantCursor)
	}
	attempts, err := state.ListAttempts(t.Context(), result.Node.ID)
	if err != nil || len(attempts) != 1 || attempts[0].Status != runtime.NodeSucceeded {
		t.Fatalf("durable wait attempt history = %#v, %v", attempts, err)
	}
}

func finalizeAuthoredReactorGeneration(t *testing.T, state *persistence.WorkflowStateStore, plan *workflowcompile.ExecutionPlan,
	runID runtime.RunID, wantCursor string, clock *reactorTestClock,
) {
	t.Helper()
	run, err := state.LoadRun(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Inputs == nil {
		t.Fatalf("reactor run %s has no typed inputs", runID)
	}
	expression, err := runtime.BuildExpressionContext(t.Context(), state, state, plan.Graph, runID)
	if err != nil {
		t.Fatal(err)
	}
	clock.Set(clock.Now().Add(time.Second))
	finalized, err := runtime.FinalizeRunOutputs(t.Context(), state, runtime.FinalizeRunRequest{BoundRun: runtime.BoundRun{ID: run.ID, Plan: run.Plan,
		InputsRef: *run.Inputs, CreatedAt: run.CreatedAt, Provenance: plan.Provenance}, Run: run, Plan: plan, Context: expression, Control: state, At: clock.Now()})
	if err != nil || len(finalized.Diagnostics) != 0 || finalized.Run.Status != runtime.RunSucceeded || finalized.Outputs["cursor"].Inline != wantCursor {
		t.Fatalf("finalize reactor generation %s = %#v, %v", runID, finalized, err)
	}
	events, err := state.ListEvents(t.Context(), runtime.EventQuery{RunID: runID})
	if err != nil || len(events) == 0 {
		t.Fatalf("reactor generation %s event history = %#v, %v", runID, events, err)
	}
}

func reactorUpdateKeyForTest(reactorID, deliveryKey string, waitID runtime.WaitID) string {
	digest := sha256.Sum256([]byte(reactorID + "\x00" + deliveryKey + "\x00" + string(waitID)))
	return "reactor-update-" + hex.EncodeToString(digest[:])
}

func TestSourceActivationMaterializationValidatesBeforeWriting(t *testing.T) {
	loaded, _ := workflowcompile.LoadFile("testdata/activations.workflow.yaml")
	compiled := workflowcompile.Compile(loaded.Source)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, compiled.Plan)
	store, _ := persistence.NewWorkflowActivationStore(fixture.store)
	service := appworkflow.ActivationService{Store: store}
	identity := testIdentityBinding("service:activation", "activation")
	exposures := map[string]string{}
	for _, declaration := range compiled.Plan.Graph.Activations {
		exposures[declaration.ID] = "exposure-" + declaration.ID
	}
	delete(exposures, compiled.Plan.Graph.Activations[0].ID)
	if _, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: compiled.Plan, Identity: identity, ExposureRefs: exposures, Enabled: true, At: fixture.now,
	}); err == nil {
		t.Fatal("missing exposure unexpectedly materialized source activations")
	}
	owner, _ := sourceOwnerForTest(compiled.Plan.Definition)
	registrations, err := store.ListDerivedActivations(t.Context(), owner)
	if err != nil || len(registrations) != 0 {
		t.Fatalf("failed validation changed durable activations: %#v, %v", registrations, err)
	}
	exposures[compiled.Plan.Graph.Activations[0].ID] = "exposure-" + compiled.Plan.Graph.Activations[0].ID

	for name, mutate := range map[string]func(*workflowcompile.ExecutionPlan){
		"missing": func(plan *workflowcompile.ExecutionPlan) { plan.SourceDigests = nil },
		"multiple": func(plan *workflowcompile.ExecutionPlan) {
			plan.SourceDigests = append(plan.SourceDigests, plan.SourceDigests[0])
		},
		"wrong-format": func(plan *workflowcompile.ExecutionPlan) {
			plan.SourceDigests[0].Format = graph.SourceArchivedBlueprint
		},
		"wrong-digest": func(plan *workflowcompile.ExecutionPlan) { plan.SourceDigests[0].Digest = plan.Graph.Digest },
	} {
		t.Run("source-digest-"+name, func(t *testing.T) {
			mutated := *compiled.Plan
			mutated.SourceDigests = append([]workflowcompile.SourceDigest(nil), compiled.Plan.SourceDigests...)
			mutate(&mutated)
			mutated.Digest, _ = workflowcompile.PlanDigest(mutated)
			if _, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
				Plan: &mutated, Identity: identity, ExposureRefs: exposures, Enabled: true, At: fixture.now,
			}); err == nil {
				t.Fatal("invalid source digest shape materialized")
			}
			if rows, err := store.ListDerivedActivations(t.Context(), owner); err != nil || len(rows) != 0 {
				t.Fatalf("invalid source digest changed rows = %#v, %v", rows, err)
			}
		})
	}
	targetMismatch := *compiled.Plan
	targetMismatch.Graph = compiled.Plan.Graph
	targetMismatch.Graph.Target = graph.ExecutionTargetRequirements{Capabilities: []string{"gpu"}}
	targetMismatch.Graph.Digest, _ = workflowcompile.GraphDigest(targetMismatch.Graph)
	targetMismatch.Digest, _ = workflowcompile.PlanDigest(targetMismatch)
	if _, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: &targetMismatch, Identity: identity, ExposureRefs: exposures, Enabled: true, At: fixture.now,
	}); err == nil {
		t.Fatal("unsatisfied execution target materialized")
	}
	if rows, loadErr := store.ListDerivedActivations(t.Context(), owner); loadErr != nil || len(rows) != 0 {
		t.Fatalf("target mismatch changed rows = %#v, %v", rows, loadErr)
	}
}

func TestSourceActivationZeroDeclarationLifecycleIsExplicitAndReplaySafe(t *testing.T) {
	zeroSource := []byte(`workflow:
  name: Zero Lifecycle
  version: v1
  provenance:
    authority: project
steps:
  - name: Accept
    transform:
      expression: "ok"
`)
	activeSource := []byte(`workflow:
  name: Zero Lifecycle
  version: v2
  provenance:
    authority: project
on:
  message:
    name: Message
    to: msg://agent/hadron/zero-lifecycle
steps:
  - name: Accept
    transform:
      expression: "ok"
`)
	retiredSource := []byte(`workflow:
  name: Zero Lifecycle
  version: v3
  provenance:
    authority: project
steps:
  - name: Accept
    transform:
      expression: "ok"
`)
	zeroPlan := compileSourceActivationPlan(t, "zero-v1.workflow.yaml", zeroSource)
	activePlan := compileSourceActivationPlan(t, "zero-v2.workflow.yaml", activeSource)
	retiredPlan := compileSourceActivationPlan(t, "zero-v3.workflow.yaml", retiredSource)
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, zeroPlan)
	store, _ := persistence.NewWorkflowActivationStore(fixture.store)
	service := appworkflow.ActivationService{Store: store}
	identity := testIdentityBinding("service:activation", "activation")
	zero, zeroErr := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: zeroPlan, Identity: identity, ExposureRefs: map[string]string{}, Enabled: true, At: fixture.now,
	})
	if zeroErr != nil || zero.Outcome != runtime.IdempotencyReplayed || zero.CurrentPlanDigest != "" || zero.SourceGeneration != 0 {
		t.Fatalf("initial zero declaration plan = %#v, %v", zero, zeroErr)
	}
	owner, _ := sourceOwnerForTest(zeroPlan.Definition)
	if rows, loadErr := store.ListDerivedActivations(t.Context(), owner); loadErr != nil || len(rows) != 0 {
		t.Fatalf("zero declaration rows = %#v, %v", rows, loadErr)
	}
	active, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: activePlan, Identity: identity, ExposureRefs: map[string]string{"message": "message-exposure"}, Enabled: true, At: fixture.now.Add(time.Second),
	})
	if err != nil || active.Outcome != runtime.IdempotencyApplied || active.SourceGeneration != 1 || len(active.Registrations) != 1 {
		t.Fatalf("first active declaration plan = %#v, %v", active, err)
	}
	retired, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: retiredPlan, Identity: identity, ExposureRefs: map[string]string{}, ExpectedCurrentPlanDigest: activePlan.Digest,
		Enabled: true, At: fixture.now.Add(2 * time.Second),
	})
	if err != nil || retired.Outcome != runtime.IdempotencyApplied || retired.CurrentPlanDigest != retiredPlan.Digest || len(retired.Registrations) != 0 {
		t.Fatalf("active to zero declaration plan = %#v, %v", retired, err)
	}
	replayed, err := service.ReconcileSourcePlan(t.Context(), appworkflow.SourceActivationRequest{
		Plan: retiredPlan, Identity: identity, ExposureRefs: map[string]string{}, ExpectedCurrentPlanDigest: retiredPlan.Digest,
		Enabled: true, At: fixture.now.Add(3 * time.Second),
	})
	if err != nil || replayed.Outcome != runtime.IdempotencyReplayed || replayed.SourceGeneration != retired.SourceGeneration {
		t.Fatalf("zero declaration replay = %#v, %v", replayed, err)
	}
}

func sourceOwnerForTest(definition graph.DefinitionRef) (string, error) {
	return values.DigestInline([]any{definition.Authority, definition.Kind, definition.ID, definition.Locator})
}
