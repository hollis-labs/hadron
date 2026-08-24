package appworkflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/values"
)

func TestSourceActivationsMaterializeCompiledDeclarationsAndUseCommonHostStart(t *testing.T) {
	loaded, loadErr := workflowcompile.LoadFile("../../workflow/compile/testdata/activations.workflow.yaml")
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
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
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

func TestSourceActivationMaterializationValidatesBeforeWriting(t *testing.T) {
	loaded, _ := workflowcompile.LoadFile("../../workflow/compile/testdata/activations.workflow.yaml")
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
		"wrong-format": func(plan *workflowcompile.ExecutionPlan) { plan.SourceDigests[0].Format = graph.SourceSDK },
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
