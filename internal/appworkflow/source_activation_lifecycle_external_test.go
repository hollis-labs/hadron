package appworkflow_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/runtime"
)

func TestSourceActivationLifecycleReconcilesRegistryUpdatesDisableAndRemove(t *testing.T) {
	firstSource := []byte(`workflow:
  name: Lifecycle
  version: v1
  provenance:
    authority: project
on:
  message:
    name: Message
    to: msg://agent/hadron/lifecycle
steps:
  - name: Accept
    transform:
      expression: "ok"
`)
	secondSource := []byte(`workflow:
  name: Lifecycle
  version: v2
  provenance:
    authority: project
on:
  event:
    name: Event
    type: lifecycle.updated
steps:
  - name: Accept
    transform:
      expression: "ok"
`)
	firstPlan := compileSourceActivationPlan(t, "lifecycle-v1.workflow.yaml", firstSource)
	secondPlan := compileSourceActivationPlan(t, "lifecycle-v2.workflow.yaml", secondSource)
	registryPath := filepath.Join(t.TempDir(), "workflow-catalog.json")
	catalog, err := hadronregistry.OpenWorkflowIndex(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	registryName := "team/" + firstPlan.ID

	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, firstPlan)
	store, err := persistence.NewWorkflowActivationStore(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	operatorIdentity := testIdentityBinding("service:operator", "operator")
	operatorIdentity.Extension = map[string]string{"exposure_ref": "operator-route"}
	operator := activationHostRegistration(t, fixture, operatorIdentity, "operator-route")
	operator.Authority = hoststate.ActivationAuthorityOperator
	if _, _, registerErr := store.RegisterActivation(t.Context(), operator); registerErr != nil {
		t.Fatal(registerErr)
	}
	identity := testIdentityBinding("service:activation", "activation")
	failingStore := &failOnceDerivedActivationStore{ActivationStore: store}
	registrar := &qualifiedSourceActivationRegistrar{catalog: catalog, records: map[string]hadronregistry.WorkflowRecord{
		firstPlan.Definition.Version:  sourceActivationRecord(registryName, "team", firstPlan, firstSource),
		secondPlan.Definition.Version: sourceActivationRecord(registryName, "team", secondPlan, secondSource),
	}}
	coordinator := appworkflow.SourceActivationRegistrationCoordinator{
		Registrar: registrar, Catalog: catalog, Activations: appworkflow.ActivationService{Store: failingStore},
	}
	firstRequest := appworkflow.RegisteredSourceActivationRequest{RegistryName: registryName, Materialization: appworkflow.SourceActivationRequest{
		Plan: firstPlan, Identity: identity, ExposureRefs: map[string]string{"message": "message-exposure"}, Enabled: true, At: fixture.now,
	}}
	partial, partialErr := coordinator.RegisterCurrent(t.Context(), appworkflow.SourceActivationRegistrationRequest{
		Registration: sourceActivationContractRequest(firstPlan), Materialization: firstRequest.Materialization,
	})
	if partialErr == nil || partial.Record.Name != registryName {
		t.Fatalf("post-catalog reconciliation failure = %#v, %v", partial, partialErr)
	}
	owner, _ := sourceOwnerForTest(firstPlan.Definition)
	if rows, loadErr := store.ListDerivedActivations(t.Context(), owner); loadErr != nil || len(rows) != 0 {
		t.Fatalf("failed first reconciliation changed rows = %#v, %v", rows, loadErr)
	}
	coordinator.Activations.Store = store
	firstResult, err := coordinator.RegisterCurrent(t.Context(), appworkflow.SourceActivationRegistrationRequest{
		Registration: sourceActivationContractRequest(firstPlan), Materialization: firstRequest.Materialization,
	})
	first := firstResult.Activations
	if err != nil || first.Outcome != runtime.IdempotencyApplied || len(first.Registrations) != 1 || !first.Registrations[0].Enabled {
		t.Fatalf("RegisterCurrent(v1 recovery) = %#v, %v", firstResult, err)
	}
	if replay, replayErr := coordinator.RegisterCurrent(t.Context(), appworkflow.SourceActivationRegistrationRequest{
		Registration: sourceActivationContractRequest(firstPlan), Materialization: firstRequest.Materialization,
	}); replayErr != nil || replay.Activations.Outcome != runtime.IdempotencyReplayed {
		t.Fatalf("RegisterCurrent(v1 replay) = %#v, %v", replay, replayErr)
	}

	secondRequest := appworkflow.RegisteredSourceActivationRequest{RegistryName: registryName, Materialization: appworkflow.SourceActivationRequest{
		Plan: secondPlan, Identity: identity, ExposureRefs: map[string]string{"event": "event-exposure"},
		ExpectedCurrentPlanDigest: firstPlan.Digest, Enabled: true, At: fixture.now.Add(time.Second),
	}}
	secondResult, err := coordinator.RegisterCurrent(t.Context(), appworkflow.SourceActivationRegistrationRequest{
		Registration: sourceActivationContractRequest(secondPlan), Materialization: secondRequest.Materialization,
	})
	second := secondResult.Activations
	if err != nil || second.Outcome != runtime.IdempotencyApplied || second.SourceGeneration != 2 || len(second.Registrations) != 1 {
		t.Fatalf("RegisterCurrent(v2) = %#v, %v", secondResult, err)
	}
	if _, staleRemoveErr := coordinator.Remove(t.Context(), registryName, appworkflow.SourceActivationRetireRequest{
		Definition: firstPlan.Definition, ExpectedCurrentPlanDigest: firstPlan.Digest, At: fixture.now.Add(2 * time.Second),
	}, firstPlan); !errors.Is(staleRemoveErr, hadronregistry.ErrWorkflowConflict) {
		t.Fatalf("stale current alias removal = %v", staleRemoveErr)
	}
	if current, currentErr := catalog.ResolveWorkflow(t.Context(), hadronregistry.WorkflowQuery{Name: registryName}); currentErr != nil || current.Record.Version != secondPlan.Definition.Version {
		t.Fatalf("stale removal changed current = %#v, %v", current, currentErr)
	}
	secondRequest.Materialization.ExpectedCurrentPlanDigest = secondPlan.Digest
	secondRequest.Materialization.At = fixture.now.Add(3 * time.Second)
	disabled, err := coordinator.Disable(t.Context(), secondRequest)
	if err != nil || disabled.Outcome != runtime.IdempotencyApplied || len(disabled.Registrations) != 1 || disabled.Registrations[0].Enabled {
		t.Fatalf("OnDisabled = %#v, %v", disabled, err)
	}
	coordinator.Catalog = &failOnceCurrentAliasRemovalCatalog{SourceActivationCatalog: catalog}
	partialRemoval, partialRemovalErr := coordinator.Remove(t.Context(), registryName, appworkflow.SourceActivationRetireRequest{
		Definition: secondPlan.Definition, ExpectedCurrentPlanDigest: secondPlan.Digest, At: fixture.now.Add(4 * time.Second),
	}, secondPlan)
	if partialRemovalErr == nil || partialRemoval.Outcome != "" {
		t.Fatalf("post-alias removal interruption = %#v, %v", partialRemoval, partialRemovalErr)
	}
	if _, currentErr := catalog.ResolveWorkflow(t.Context(), hadronregistry.WorkflowQuery{Name: registryName}); !errors.Is(currentErr, hadronregistry.ErrWorkflowNotFound) {
		t.Fatalf("interrupted removal retained current alias = %v", currentErr)
	}
	rowsAfterAliasRemoval, loadErr := store.ListDerivedActivations(t.Context(), owner)
	if loadErr != nil || len(rowsAfterAliasRemoval) != 2 {
		t.Fatalf("interrupted removal retired activation rows = %#v, %v", rowsAfterAliasRemoval, loadErr)
	}
	var secondStillOperational bool
	for _, registration := range rowsAfterAliasRemoval {
		if registration.Derivation.PlanDigest == secondPlan.Digest {
			secondStillOperational = !registration.Derivation.Retired
		}
	}
	if !secondStillOperational {
		t.Fatal("interrupted removal retired the current activation before replay")
	}
	coordinator.Catalog = catalog
	removed, err := coordinator.Remove(t.Context(), registryName, appworkflow.SourceActivationRetireRequest{
		Definition: secondPlan.Definition, ExpectedCurrentPlanDigest: secondPlan.Digest, At: fixture.now.Add(5 * time.Second),
	}, secondPlan)
	if err != nil || removed.Outcome != runtime.IdempotencyApplied || removed.CurrentPlanDigest != "" || len(removed.Registrations) != 0 {
		t.Fatalf("OnRemoved = %#v, %v", removed, err)
	}
	storedOperator, err := store.LoadActivation(t.Context(), operator.ID)
	if err != nil || !storedOperator.Enabled || storedOperator.Generation != operator.Generation || storedOperator.Authority != hoststate.ActivationAuthorityOperator {
		t.Fatalf("operator after source lifecycle = %#v, %v", storedOperator, err)
	}
	firstRequest.Materialization.ExpectedCurrentPlanDigest = firstPlan.Digest
	firstRequest.Materialization.At = fixture.now.Add(6 * time.Second)
	lifecycle := appworkflow.SourceActivationLifecycle{Registry: catalog, Activations: appworkflow.ActivationService{Store: store}}
	if _, staleErr := lifecycle.OnRegistered(t.Context(), firstRequest); !errors.Is(staleErr, runtime.ErrIdempotencyConflict) {
		t.Fatalf("stale source resurrection = %v", staleErr)
	}

	reopened, err := hadronregistry.OpenWorkflowIndex(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.Catalog = reopened
	if _, removeErr := coordinator.Remove(t.Context(), registryName, appworkflow.SourceActivationRetireRequest{
		Definition: secondPlan.Definition, ExpectedCurrentPlanDigest: secondPlan.Digest, At: fixture.now.Add(7 * time.Second),
	}, secondPlan); removeErr != nil {
		t.Fatalf("reopened lifecycle replay = %v", removeErr)
	}
	if exact, exactErr := reopened.ResolveWorkflow(t.Context(), hadronregistry.WorkflowQuery{
		Name: registryName, Version: secondPlan.Definition.Version, Digest: secondPlan.Definition.Digest,
	}); exactErr != nil || exact.Record.PlanDigest != secondPlan.Digest {
		t.Fatalf("removal erased immutable source history = %#v, %v", exact, exactErr)
	}
	if _, currentErr := reopened.ResolveWorkflow(t.Context(), hadronregistry.WorkflowQuery{Name: registryName}); !errors.Is(currentErr, hadronregistry.ErrWorkflowNotFound) {
		t.Fatalf("reopened removal replay restored current alias = %v", currentErr)
	}
}

func TestSourceActivationLifecycleRejectsUnregisteredOrMismatchedPlanBeforeWrites(t *testing.T) {
	source := []byte(`workflow:
  name: Unregistered
  version: v1
  provenance:
    authority: project
on:
  webhook:
    name: Hook
    path: /hooks/unregistered
steps:
  - name: Accept
    transform:
      expression: "ok"
`)
	plan := compileSourceActivationPlan(t, "unregistered.workflow.yaml", source)
	catalog := hadronregistry.NewWorkflowIndex()
	fixture := newHostFixtureWithPlan(t, hoststate.PolicyAllow, time.Hour, nil, plan)
	store, _ := persistence.NewWorkflowActivationStore(fixture.store)
	lifecycle := appworkflow.SourceActivationLifecycle{Registry: catalog, Activations: appworkflow.ActivationService{Store: store}}
	request := appworkflow.RegisteredSourceActivationRequest{RegistryName: "team/" + plan.ID, Materialization: appworkflow.SourceActivationRequest{
		Plan: plan, Identity: testIdentityBinding("service:activation", "activation"),
		ExposureRefs: map[string]string{"hook": "hook-exposure"}, Enabled: true, At: fixture.now,
	}}
	owner, _ := sourceOwnerForTest(plan.Definition)
	registrar := &qualifiedSourceActivationRegistrar{catalog: catalog, records: map[string]hadronregistry.WorkflowRecord{
		plan.Definition.Version: sourceActivationRecord(request.RegistryName, "team", plan, source),
	}}
	coordinator := appworkflow.SourceActivationRegistrationCoordinator{Registrar: registrar, Catalog: catalog, Activations: lifecycle.Activations}
	invalidRegistration := request.Materialization
	invalidRegistration.ExposureRefs = nil
	if _, registrationErr := coordinator.RegisterCurrent(t.Context(), appworkflow.SourceActivationRegistrationRequest{
		Registration: sourceActivationContractRequest(plan), Materialization: invalidRegistration,
	}); registrationErr == nil {
		t.Fatal("invalid activation materialization committed its catalog record")
	}
	if registrar.calls.Load() != 0 {
		t.Fatal("invalid activation materialization reached the qualification registrar")
	}
	if _, resolveErr := catalog.ResolveWorkflow(t.Context(), hadronregistry.WorkflowQuery{
		Name: request.RegistryName, Version: plan.Definition.Version, Digest: plan.Definition.Digest,
	}); !errors.Is(resolveErr, hadronregistry.ErrWorkflowNotFound) {
		t.Fatalf("invalid materialization changed catalog = %v", resolveErr)
	}
	registrar.failure = errors.New("qualification failed")
	if _, registrationErr := coordinator.RegisterCurrent(t.Context(), appworkflow.SourceActivationRegistrationRequest{
		Registration: sourceActivationContractRequest(plan), Materialization: request.Materialization,
	}); registrationErr == nil {
		t.Fatal("qualification failure was accepted")
	}
	if registrar.calls.Load() != 1 {
		t.Fatalf("qualification registrar calls = %d", registrar.calls.Load())
	}
	if _, resolveErr := catalog.ResolveWorkflow(t.Context(), hadronregistry.WorkflowQuery{
		Name: request.RegistryName, Version: plan.Definition.Version, Digest: plan.Definition.Digest,
	}); !errors.Is(resolveErr, hadronregistry.ErrWorkflowNotFound) {
		t.Fatalf("qualification failure changed catalog = %v", resolveErr)
	}
	if rows, loadErr := store.ListDerivedActivations(t.Context(), owner); loadErr != nil || len(rows) != 0 {
		t.Fatalf("qualification failure changed activations = %#v, %v", rows, loadErr)
	}
	if _, err := lifecycle.OnRegistered(t.Context(), request); err == nil {
		t.Fatal("unregistered plan materialized")
	}
	if rows, err := store.ListDerivedActivations(t.Context(), owner); err != nil || len(rows) != 0 {
		t.Fatalf("unregistered plan changed rows = %#v, %v", rows, err)
	}
	registerSourceActivationPlan(t, catalog, request.RegistryName, "team", plan, source, true)
	corrupt := *plan
	corrupt.SourceDigests = append([]workflowcompile.SourceDigest(nil), plan.SourceDigests...)
	corrupt.SourceDigests[0].Digest = plan.Graph.Digest
	request.Materialization.Plan = &corrupt
	if _, err := lifecycle.OnRegistered(t.Context(), request); err == nil {
		t.Fatal("plan with mismatched source digest materialized")
	}
	if rows, err := store.ListDerivedActivations(t.Context(), owner); err != nil || len(rows) != 0 {
		t.Fatalf("corrupt plan changed rows = %#v, %v", rows, err)
	}
}

func compileSourceActivationPlan(t *testing.T, locator string, source []byte) *workflowcompile.ExecutionPlan {
	t.Helper()
	loaded := workflowcompile.LoadBytes(locator, source)
	if loaded.Source == nil || len(loaded.Diagnostics) != 0 {
		t.Fatalf("LoadBytes(%s) = %#v", locator, loaded)
	}
	compiled := workflowcompile.Compile(loaded.Source)
	if compiled.Plan == nil || len(compiled.Diagnostics) != 0 {
		t.Fatalf("Compile(%s) = %#v", locator, compiled)
	}
	return compiled.Plan
}

func sourceActivationRecord(name, namespace string, plan *workflowcompile.ExecutionPlan, source []byte) hadronregistry.WorkflowRecord {
	provenance := plan.Provenance
	if provenance.Origin == "" {
		provenance.Origin = "workflow-source"
	}
	return hadronregistry.WorkflowRecord{
		Name: name, Namespace: namespace, Version: plan.Definition.Version, Digest: plan.Definition.Digest, Source: source,
		Authority: plan.Definition.Authority, TrustClass: "project", Provenance: provenance, PlanDigest: plan.Digest,
		RegisteredAt: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
	}
}

func sourceActivationContractRequest(plan *workflowcompile.ExecutionPlan) appworkflow.RegisterWorkflowRequest {
	return appworkflow.RegisterWorkflowRequest{
		Definition: plan.Definition, Namespace: "team", Principal: "principal:activation", MakeCurrent: true,
	}
}

func registerSourceActivationPlan(t *testing.T, catalog *hadronregistry.WorkflowIndex, name, namespace string, plan *workflowcompile.ExecutionPlan, source []byte, current bool) {
	t.Helper()
	if _, err := catalog.RegisterWorkflow(t.Context(), sourceActivationRecord(name, namespace, plan, source), current); err != nil {
		t.Fatal(err)
	}
}

type failOnceDerivedActivationStore struct {
	hoststate.ActivationStore
	failed atomic.Bool
}

type qualifiedSourceActivationRegistrar struct {
	catalog *hadronregistry.WorkflowIndex
	records map[string]hadronregistry.WorkflowRecord
	failure error
	calls   atomic.Int32
}

func (r *qualifiedSourceActivationRegistrar) Register(ctx context.Context, request appworkflow.RegisterWorkflowRequest) (hadronregistry.WorkflowRecord, error) {
	r.calls.Add(1)
	if r.failure != nil {
		return hadronregistry.WorkflowRecord{}, r.failure
	}
	record, exists := r.records[request.Definition.Version]
	if !exists {
		return hadronregistry.WorkflowRecord{}, errors.New("qualified record missing")
	}
	return r.catalog.RegisterWorkflow(ctx, record, request.MakeCurrent)
}

type failOnceCurrentAliasRemovalCatalog struct {
	appworkflow.SourceActivationCatalog
	failed atomic.Bool
}

func (c *failOnceCurrentAliasRemovalCatalog) RemoveCurrentWorkflowExact(ctx context.Context, query hadronregistry.WorkflowQuery) error {
	if err := c.SourceActivationCatalog.RemoveCurrentWorkflowExact(ctx, query); err != nil {
		return err
	}
	if c.failed.CompareAndSwap(false, true) {
		return errors.New("simulated process loss after current-alias removal")
	}
	return nil
}

func (s *failOnceDerivedActivationStore) ReconcileDerivedActivations(ctx context.Context, request hoststate.ActivationReconcileRequest) (hoststate.ActivationReconcileResult, error) {
	if s.failed.CompareAndSwap(false, true) {
		return hoststate.ActivationReconcileResult{}, errors.New("simulated post-catalog process loss")
	}
	return s.ActivationStore.ReconcileDerivedActivations(ctx, request)
}
