package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gosched "github.com/hollis-labs/go-scheduler"
	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/mcpadapter"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/internal/trigger"
	"github.com/hollis-labs/hadron/workflow/adapters/transform"
	"github.com/hollis-labs/hadron/workflow/authoring"
	workflowcompile "github.com/hollis-labs/hadron/workflow/compile"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	workflowruntime "github.com/hollis-labs/hadron/workflow/runtime"
	"github.com/hollis-labs/hadron/workflow/runtime/inmemory"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
	"github.com/mark3labs/mcp-go/mcp"
)

func TestProductionWorkflowRuntimeExecutesPinnedGraphAndStopsCleanly(t *testing.T) {
	runtime, cfg, _ := newTestProductionWorkflowRuntime(t)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://127.0.0.1:8095/v1/workflows/runs", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.RemoteAddr = "127.0.0.1:43123"
	request.Host = "127.0.0.1:8095"
	ctx, err := runtime.auth.AuthenticateWorkflowRequest(request, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessRun})
	if err != nil {
		t.Fatal(err)
	}
	started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: "production-transform-one", IdempotencyKey: "production-transform-one",
		Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "production-transform", Locator: "production-transform.workflow.yaml", Version: "v1"},
		Inputs:     map[string]any{"message": "hello"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	})
	if err != nil || started.Run == nil {
		validated, validationErr := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "production-transform", Locator: "production-transform.workflow.yaml", Version: "v1"}, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"}})
		t.Fatalf("RunWorkflow = %#v, %v; validation=%#v, %v", started, err, validated, validationErr)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		inspected, inspectErr := runtime.operations.InspectWorkflowRun(ctx, appworkflow.InspectWorkflowRunRequest{RunID: started.Run.ID, Identity: appworkflow.IdentityRequest{SourceAuthority: "http"}})
		if inspectErr == nil && inspected.Run.Status == workflowruntime.RunSucceeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not succeed: %#v, %v", inspected, inspectErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	if health := runtime.host.Health(); health.Started || health.Ready {
		t.Fatalf("health after shutdown = %#v", health)
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "workflow-contract-attestor.key")); err != nil {
		t.Fatalf("attestor key: %v", err)
	}
}

func TestProductionHTTPDurableBearerRebindsWorkflowAndLifecycleIdentity(t *testing.T) {
	runtime, _, _ := newTestProductionWorkflowRuntime(t)
	const token = "workflow-http-bearer-token"
	managementCtx, err := appworkflow.WithAuthenticatedIdentity(t.Context(), localWorkflowIdentity())
	if err != nil {
		t.Fatal(err)
	}
	detail := registerProductionActivationDraft(managementCtx, t, runtime.lifecycle, productionActivationDraft(t, "http-bearer", "v1"))
	if err := runtime.BootstrapMCP(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	server := httptest.NewServer(api.NewServer("", api.Dependencies{
		Workflows: runtime.operations, WorkflowReads: runtime.operations,
		WorkflowLifecycle: runtime.lifecycle, WorkflowAuth: runtime.auth,
		WorkflowActivations: runtime.externalActivations,
		A2ATasks:            runtime.a2a, AgentCard: runtime.card, WorkflowHealth: runtime.host,
	}).Handler())
	defer server.Close()

	post := func(path string, body any, credential, idempotencyKey string) *http.Response {
		t.Helper()
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+path, bytes.NewReader(encoded))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+credential)
		if idempotencyKey != "" {
			request.Header.Set("Idempotency-Key", idempotencyKey)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}

	search := post("/v1/workflows/lifecycle/catalog/search", appworkflow.SearchWorkflowCatalogRequest{
		Namespace: "team", Limit: 1, Identity: appworkflow.IdentityRequest{PrincipalHint: "forged", SourceAuthority: "forged", Attributes: map[string]string{"principal": "root"}},
	}, token, "")
	if search.StatusCode != http.StatusOK {
		defer func() { _ = search.Body.Close() }()
		var operationError appworkflow.WorkflowOperationError
		_ = json.NewDecoder(search.Body).Decode(&operationError)
		t.Fatalf("bearer lifecycle search status=%d result=%#v", search.StatusCode, operationError)
	}
	_ = search.Body.Close()

	runRequest := appworkflow.RunWorkflowRequest{
		RunID: "production-http-bearer", IdempotencyKey: "production-http-bearer",
		Definition: detail.Descriptor.Definition,
		Inputs:     map[string]any{"message": "hello"},
		Identity:   appworkflow.IdentityRequest{PrincipalHint: "forged", SourceAuthority: "forged", Attributes: map[string]string{"principal": "root"}},
	}
	runResponse := post("/v1/workflows/runs", runRequest, token, runRequest.IdempotencyKey)
	defer func() { _ = runResponse.Body.Close() }()
	var started appworkflow.StartRunResult
	if err := json.NewDecoder(runResponse.Body).Decode(&started); err != nil {
		t.Fatal(err)
	}
	if runResponse.StatusCode != http.StatusOK || started.Run == nil || started.Facts.Identity.Principal != workflowMCPPrincipalID ||
		started.Facts.Identity.SourceAuthority != "http" {
		t.Fatalf("bearer workflow run status=%d result=%#v", runResponse.StatusCode, started)
	}

	unknown := post("/v1/workflows/lifecycle/catalog/search", appworkflow.SearchWorkflowCatalogRequest{Namespace: "team", Limit: 1}, "unknown-workflow-token", "")
	defer func() { _ = unknown.Body.Close() }()
	var denied appworkflow.WorkflowOperationError
	if err := json.NewDecoder(unknown.Body).Decode(&denied); err != nil {
		t.Fatal(err)
	}
	if unknown.StatusCode != http.StatusUnauthorized || denied.Code != appworkflow.WorkflowErrorCodeUnauthenticated {
		t.Fatalf("unknown bearer status=%d result=%#v", unknown.StatusCode, denied)
	}
}

func TestProductionLifecycleActivationsUseStableRegistryIdentityAcrossRestartAndCurrentMoves(t *testing.T) {
	firstRuntime, cfg, store := newTestProductionWorkflowRuntime(t)
	base := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Second)
	now := base
	firstRuntime.sourceActivations.Activations.Clock = appworkflow.ClockFunc(func() time.Time { return now })
	ctx, identityErr := appworkflow.WithAuthenticatedIdentity(t.Context(), localWorkflowIdentity())
	if identityErr != nil {
		t.Fatal(identityErr)
	}

	v1Draft := productionActivationDraft(t, "lifecycle-activation", "v1")
	v1 := registerProductionActivationDraft(ctx, t, firstRuntime.lifecycle, v1Draft)
	now = base.Add(time.Hour)
	v2Draft := productionActivationDraft(t, "lifecycle-activation", "v2")
	v2 := registerProductionActivationDraft(ctx, t, firstRuntime.lifecycle, v2Draft)
	// Moving current back to an older immutable record must use the durable
	// activation projection as its time floor, not the record's RegisteredAt.
	now = base.Add(-time.Hour)
	v1Again := registerProductionActivationDraft(ctx, t, firstRuntime.lifecycle, v1Draft)
	if v1Again.Descriptor.Definition != v1.Descriptor.Definition {
		t.Fatalf("v1 replay identity = %#v, want %#v", v1Again.Descriptor.Definition, v1.Descriptor.Definition)
	}
	current, resolveErr := firstRuntime.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: "team/lifecycle-activation"})
	if resolveErr != nil || current.Record.Version != "v1" || current.Record.Digest != v1.Descriptor.Definition.Digest {
		t.Fatalf("current after move back = %#v, %v", current, resolveErr)
	}
	inspected, inspectErr := firstRuntime.lifecycle.InspectWorkflowVersion(ctx, appworkflow.InspectWorkflowVersionRequest{Definition: v1Again.Descriptor.Definition})
	if inspectErr != nil || !reflect.DeepEqual(inspected.Activations, v1Again.Activations) || len(inspected.Activations) != 2 {
		t.Fatalf("registration/inspection activation discovery = register %#v inspect %#v, %v", v1Again.Activations, inspected.Activations, inspectErr)
	}
	var scheduleDescriptor, externalDescriptor appworkflow.WorkflowActivationDescriptor
	for _, descriptor := range inspected.Activations {
		if descriptor.Definition != v1Again.Descriptor.Definition || descriptor.Retired || !descriptor.Enabled {
			t.Fatalf("public activation descriptor = %#v", descriptor)
		}
		switch descriptor.TemplateID {
		case "scheduled":
			scheduleDescriptor = descriptor
		case "external":
			externalDescriptor = descriptor
		}
	}
	if scheduleDescriptor.RegistrationID == "" || externalDescriptor.RegistrationID == "" {
		t.Fatalf("public activation discovery = %#v", inspected.Activations)
	}
	ownerKey, digestErr := values.DigestInline([]any{"hadron", appworkflow.DefinitionKindRegistry, "team/lifecycle-activation", ""})
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	rows, listErr := firstRuntime.activationStore.ListDerivedActivations(ctx, ownerKey)
	if listErr != nil || len(rows) != 4 {
		t.Fatalf("stable owner rows = %d, %v", len(rows), listErr)
	}
	var scheduleRegistration hoststate.ActivationRegistration
	for _, registration := range rows {
		if registration.Derivation == nil || registration.Derivation.SourceOwnerKey != ownerKey ||
			registration.Definition.Kind != appworkflow.DefinitionKindRegistry || registration.Definition.ID != "team/lifecycle-activation" {
			t.Fatalf("derived registry binding = %#v", registration)
		}
		if registration.Derivation.PlanDigest == v2.Descriptor.Evidence.PlanDigest && !registration.Derivation.Retired {
			t.Fatalf("v2 registration remained active after moving current to v1: %#v", registration)
		}
		if registration.Derivation.PlanDigest != v1.Descriptor.Evidence.PlanDigest || registration.Derivation.Retired || !registration.Enabled {
			continue
		}
		switch registration.Source.Kind {
		case hoststate.ActivationSourceSchedule:
			scheduleRegistration = registration
		default:
		}
	}
	if scheduleRegistration.ID == "" {
		t.Fatalf("active v1 schedule registration = %#v", scheduleRegistration)
	}
	if scheduleRegistration.ID != scheduleDescriptor.RegistrationID {
		t.Fatalf("public schedule activation handle differs from durable projection: public=%#v durable=%#v", scheduleDescriptor, scheduleRegistration)
	}

	// A fresh composition has no AuthoringSourceStager entry. Both ingress
	// paths therefore prove the persisted exact registry definition is the
	// executable source of truth after restart.
	restarted, runtimeErr := newProductionWorkflowRuntime(store, cfg, 1)
	if runtimeErr != nil {
		t.Fatal(runtimeErr)
	}
	if startErr := restarted.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = restarted.Shutdown(context.Background()) })
	v2Record, inspectV2Err := restarted.catalog.InspectWorkflow(ctx, registry.WorkflowQuery{
		Name: v2.Descriptor.Definition.ID, Version: v2.Descriptor.Definition.Version, Digest: v2.Descriptor.Definition.Digest,
	})
	if inspectV2Err != nil {
		t.Fatal(inspectV2Err)
	}
	if _, registerErr := restarted.catalog.RegisterWorkflow(ctx, v2Record, true); registerErr != nil {
		t.Fatal(registerErr)
	}
	if stale, staleErr := restarted.externalActivations.Fire(ctx, trigger.ActivationEvent{
		RegistrationID: externalDescriptor.RegistrationID, IdempotencyKey: "production-stale-v1",
		OccurredAt: base.Add(2 * time.Hour), ReceivedAt: time.Now().UTC(), SourceRef: "production-test",
	}); !errors.Is(staleErr, appworkflow.ErrActivationSkipped) || stale.Start.Run != nil {
		t.Fatalf("stale durable activation after current commit = %#v, %v", stale, staleErr)
	}
	v1Record, inspectV1Err := restarted.catalog.InspectWorkflow(ctx, registry.WorkflowQuery{
		Name: v1.Descriptor.Definition.ID, Version: v1.Descriptor.Definition.Version, Digest: v1.Descriptor.Definition.Digest,
	})
	if inspectV1Err != nil {
		t.Fatal(inspectV1Err)
	}
	if _, registerErr := restarted.catalog.RegisterWorkflow(ctx, v1Record, true); registerErr != nil {
		t.Fatal(registerErr)
	}
	deliveryIdentity := localWorkflowIdentity()
	deliveryIdentity.Principal = "operator:delegated-delivery"
	deliveryCtx, bindErr := appworkflow.WithAuthenticatedIdentity(t.Context(), deliveryIdentity)
	if bindErr != nil {
		t.Fatal(bindErr)
	}
	external, fireErr := restarted.externalActivations.Fire(deliveryCtx, trigger.ActivationEvent{
		RegistrationID: externalDescriptor.RegistrationID, IdempotencyKey: "production-external-v1",
		OccurredAt: base.Add(2 * time.Hour), ReceivedAt: time.Now().UTC(), SourceRef: "production-test",
	})
	if fireErr != nil || external.Start.Run == nil || external.Start.Run.Plan.Digest != v1.Descriptor.Evidence.PlanDigest {
		t.Fatalf("restarted external activation = %#v, %v", external, fireErr)
	}

	due, dueErr := restarted.activationStore.ListDueSchedules(ctx, time.Now().UTC(), 10)
	if dueErr != nil || len(due) != 1 || due[0].ID != scheduleRegistration.ID {
		t.Fatalf("due production schedule = %#v, %v", due, dueErr)
	}
	scheduledAt := due[0].NextRun
	if tickErr := restarted.activation.source.TickNow(context.Background()); tickErr != nil {
		t.Fatal(tickErr)
	}
	dispatch, loadErr := restarted.activationStore.LoadActivationDispatch(ctx, gosched.DeriveFireID(scheduleRegistration.ID, scheduledAt))
	if loadErr != nil || dispatch.Status != hoststate.ActivationDispatchStarted || dispatch.PhysicalRunID == "" {
		t.Fatalf("background schedule dispatch = %#v, %v", dispatch, loadErr)
	}
}

func TestProductionLifecycleClearCurrentRecoversAfterAliasCommitAndHandlesNoActivations(t *testing.T) {
	ctx, bindErr := appworkflow.WithAuthenticatedIdentity(t.Context(), localWorkflowIdentity())
	if bindErr != nil {
		t.Fatal(bindErr)
	}
	t.Run("retirement-recovery", func(t *testing.T) {
		runtime, _, _ := newTestProductionWorkflowRuntime(t)
		detail := registerProductionActivationDraft(ctx, t, runtime.lifecycle, productionActivationDraft(t, "clear-recovery", "v1"))
		failing := &failOnceProductionActivationStore{ActivationStore: runtime.sourceActivations.Activations.Store}
		runtime.sourceActivations.Activations.Store = failing
		request := appworkflow.MutateWorkflowVersionRequest{Definition: detail.Descriptor.Definition}
		if _, clearErr := runtime.lifecycle.ClearWorkflowCurrentExact(ctx, request); clearErr == nil {
			t.Fatal("clear unexpectedly survived the injected post-alias retirement failure")
		}
		if _, resolveErr := runtime.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: detail.Descriptor.Definition.ID}); !errors.Is(resolveErr, registry.ErrWorkflowNotFound) {
			t.Fatalf("failed retirement retained current alias: %v", resolveErr)
		}
		runtime.sourceActivations.Activations.Store = failing.ActivationStore
		if _, clearErr := runtime.lifecycle.ClearWorkflowCurrentExact(ctx, request); clearErr != nil {
			t.Fatalf("clear recovery = %v", clearErr)
		}
		ownerKey, digestErr := values.DigestInline([]any{"hadron", appworkflow.DefinitionKindRegistry, detail.Descriptor.Definition.ID, ""})
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		rows, listErr := runtime.activationStore.ListDerivedActivations(ctx, ownerKey)
		if listErr != nil || len(rows) != 2 {
			t.Fatalf("recovered historical activation rows = %d, %v", len(rows), listErr)
		}
		for _, registration := range rows {
			if registration.Enabled || registration.Derivation == nil || !registration.Derivation.Retired || registration.Derivation.CurrentPlanDigest != "" {
				t.Fatalf("recovered clear projection = %#v", registration)
			}
		}
	})

	t.Run("zero-activation", func(t *testing.T) {
		runtime, _, _ := newTestProductionWorkflowRuntime(t)
		draft := productionActivationDraft(t, "clear-empty", "v1")
		var envelope authoring.Envelope
		if decodeErr := json.Unmarshal(draft.Envelope, &envelope); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		envelope.Graph.Activations = nil
		var encodeErr error
		draft.Envelope, encodeErr = json.Marshal(envelope)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		detail := registerProductionActivationDraft(ctx, t, runtime.lifecycle, draft)
		if _, clearErr := runtime.lifecycle.ClearWorkflowCurrentExact(ctx, appworkflow.MutateWorkflowVersionRequest{Definition: detail.Descriptor.Definition}); clearErr != nil {
			t.Fatalf("zero-activation clear = %v", clearErr)
		}
		if _, resolveErr := runtime.catalog.ResolveWorkflow(ctx, registry.WorkflowQuery{Name: detail.Descriptor.Definition.ID}); !errors.Is(resolveErr, registry.ErrWorkflowNotFound) {
			t.Fatalf("zero-activation current alias remained: %v", resolveErr)
		}
	})
}

type failOnceProductionActivationStore struct {
	hoststate.ActivationStore
	failed bool
}

func (s *failOnceProductionActivationStore) ReconcileDerivedActivations(ctx context.Context, request hoststate.ActivationReconcileRequest) (hoststate.ActivationReconcileResult, error) {
	if !s.failed {
		s.failed = true
		return hoststate.ActivationReconcileResult{}, errors.New("injected activation retirement interruption")
	}
	return s.ActivationStore.ReconcileDerivedActivations(ctx, request)
}

func productionActivationDraft(t *testing.T, id, version string) appworkflow.WorkflowDraft {
	t.Helper()
	binding := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.message"}}
	output := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.echo.outputs.result"}}
	envelope := authoring.New(id, version).
		Input(graph.InputSpec{Name: "message", Schema: graph.Schema{"type": "string"}, Required: true}).
		Node(graph.Node{
			ID: "echo", Kind: transform.Name, KindVersion: "v1", Config: graph.Config{"result": "inputs.message"},
			InputBindings: map[string]graph.Binding{"message": binding},
			Outputs:       []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}},
			Effects:       graph.EffectSet{graph.EffectCompute},
		}).
		Output(graph.OutputSpec{Name: "result", Schema: graph.Schema{"type": "string"}, Value: &output}).Envelope()
	envelope.Graph.Source.Format = graph.SourceAgent
	envelope.Graph.SourceMap.Graph.Format = graph.SourceAgent
	literal := func(value string) graph.Binding { return graph.Binding{Kind: graph.BindingLiteral, Literal: value} }
	envelope.Graph.Activations = []graph.ActivationDeclaration{
		{ID: "scheduled", Kind: "schedule", Config: graph.Config{"cron": "* * * * *"}, Inputs: map[string]graph.Binding{"message": literal("scheduled")}},
		{ID: "external", Kind: "event", Config: graph.Config{"type": "production.external"}, Inputs: map[string]graph.Binding{"message": literal("external")}},
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return appworkflow.WorkflowDraft{Envelope: encoded, ID: id, Version: version, Namespace: "team"}
}

func registerProductionActivationDraft(ctx context.Context, t *testing.T, lifecycle *appworkflow.WorkflowLifecycleService, draft appworkflow.WorkflowDraft) appworkflow.WorkflowVersionDetail {
	t.Helper()
	scaffold, err := lifecycle.GenerateWorkflowContract(ctx, appworkflow.GenerateWorkflowContractRequest{Draft: draft})
	if err != nil || scaffold.Scaffold == nil || scaffold.Validation.Plan == nil || len(scaffold.Scaffold.Cases) == 0 {
		t.Fatalf("activation scaffold = %#v, %v", scaffold, err)
	}
	input, err := values.NewInline("hello", values.Metadata{Producer: values.Producer{Kind: "contract-input", Reference: scaffold.Validation.Plan.ID, Output: "message"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	output, err := values.NewInline("hello", values.Metadata{Producer: values.Producer{Kind: "contract-output", Reference: scaffold.Validation.Plan.ID, Output: "result"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	caseOne := &scaffold.Scaffold.Cases[0]
	caseOne.Name, caseOne.Editable = "echo", false
	caseOne.Inputs, caseOne.ExpectedOutputs = values.ValueSet{"message": input}, values.ValueSet{"result": output}
	if len(caseOne.Mocks) != 1 {
		t.Fatalf("activation scaffold mocks = %#v", caseOne.Mocks)
	}
	caseOne.Mocks[0].ExpectedInputs = values.ValueSet{"message": input}
	caseOne.Mocks[0].ExpectedInputsEditable = false
	caseOne.Mocks[0].Results = []appworkflow.ContractMockResult{{Outputs: values.ValueSet{"result": output}}}
	registered, err := lifecycle.RegisterWorkflowDraft(ctx, appworkflow.RegisterWorkflowDraftRequest{Draft: draft, Suite: *scaffold.Scaffold, MakeCurrent: true})
	if err != nil || registered.Detail == nil {
		t.Fatalf("activation register = %#v, %v", registered, err)
	}
	return *registered.Detail
}

func TestWorkflowHTTPLocalIdentityRejectsCrossOriginAndDNSRebinding(t *testing.T) {
	runtime, _, _ := newTestProductionWorkflowRuntime(t)
	base, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://localhost:8095/v1/workflows/validate", nil)
	if err != nil {
		t.Fatal(err)
	}
	base.RemoteAddr = "127.0.0.1:40000"
	base.Host = "localhost:8095"
	if _, err := runtime.auth.AuthenticateWorkflowRequest(base, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessValidate}); err != nil {
		t.Fatalf("loopback CLI auth = %v", err)
	}
	crossOrigin := base.Clone(t.Context())
	crossOrigin.Header.Set("Origin", "http://evil.example")
	if _, err := runtime.auth.AuthenticateWorkflowRequest(crossOrigin, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessValidate}); !errors.Is(err, appworkflow.ErrPolicyDenied) {
		t.Fatalf("cross-origin auth error = %v", err)
	}
	rebinding := base.Clone(t.Context())
	rebinding.Host = "evil.example"
	if _, err := runtime.auth.AuthenticateWorkflowRequest(rebinding, appworkflow.WorkflowAccessIntent{Operation: appworkflow.WorkflowAccessValidate}); !errors.Is(err, appworkflow.ErrWorkflowUnauthenticated) {
		t.Fatalf("DNS-rebinding auth error = %v", err)
	}
}

func TestWorkflowInstanceNonceIsRestartUnique(t *testing.T) {
	first, err := workflowNonce()
	if err != nil {
		t.Fatal(err)
	}
	second, err := workflowNonce()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || len(first) != 32 || len(second) != 32 {
		t.Fatalf("nonces = %q, %q", first, second)
	}
}

func TestWorkflowContractAttestorConcurrentOpenPublishesOnePrivateKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attestor.key")
	const contenders = 32
	start := make(chan struct{})
	keys := make(chan []byte, contenders)
	errs := make(chan error, contenders)
	var group sync.WaitGroup
	for range contenders {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			attestor, err := openWorkflowContractAttestor(path)
			if err != nil {
				errs <- err
				return
			}
			keys <- append([]byte(nil), attestor.key...)
		}()
	}
	close(start)
	group.Wait()
	close(keys)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent attestor open: %v", err)
	}
	var want []byte
	for key := range keys {
		if want == nil {
			want = key
			continue
		}
		if !reflect.DeepEqual(key, want) {
			t.Fatalf("attestor keys differ: %x != %x", key, want)
		}
	}
	if len(want) != 32 {
		t.Fatalf("attestor key length = %d", len(want))
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("attestor key metadata = %#v, %v", info, err)
	}
}

func TestWorkflowContractAttestorRejectsUnsafeKeyAndLockFiles(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{name: "symlink-key", setup: func(t *testing.T, path string) {
			t.Helper()
			target := path + ".target"
			if err := os.WriteFile(target, make([]byte, 32), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "permissive-key", setup: func(t *testing.T, path string) {
			t.Helper()
			if err := os.WriteFile(path, make([]byte, 32), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink-lock", setup: func(t *testing.T, path string) {
			t.Helper()
			target := path + ".lock.target"
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path+".lock"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "permissive-lock", setup: func(t *testing.T, path string) {
			t.Helper()
			if err := os.WriteFile(path+".lock", nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "attestor.key")
			test.setup(t, path)
			if _, err := openWorkflowContractAttestor(path); err == nil {
				t.Fatal("unsafe attestor material was accepted")
			}
		})
	}
}

func TestProductionWorkflowCatalogMultiOpenSerializesWritesAndRefreshesReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow-catalog.json")
	first, err := openProductionWorkflowCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := openProductionWorkflowCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	const records = 24
	start := make(chan struct{})
	errCh := make(chan error, records)
	var group sync.WaitGroup
	for index := range records {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			catalog := first
			if index%2 == 1 {
				catalog = second
			}
			name := "team/concurrent-" + strconv.Itoa(index)
			_, err := catalog.RegisterWorkflow(t.Context(), qualifiedProductionRecord(name, "team", "v1", "workflow:"+name), false)
			errCh <- err
		}(index)
	}
	close(start)
	group.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent register: %v", err)
		}
	}
	for _, catalog := range []*productionWorkflowCatalog{first, second} {
		found, err := catalog.SearchWorkflows(t.Context(), "team", "concurrent")
		if err != nil || len(found) != records {
			t.Fatalf("refreshed catalog search = %d records, %v", len(found), err)
		}
	}
}

func TestProductionWorkflowCatalogCurrentFenceSerializesAliasMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow-catalog.json")
	catalog, openErr := openProductionWorkflowCatalog(path)
	if openErr != nil {
		t.Fatal(openErr)
	}
	v1Input := qualifiedProductionRecord("team/fenced", "team", "v1", "workflow:fenced-v1")
	v2 := qualifiedProductionRecord("team/fenced", "team", "v2", "workflow:fenced-v2")
	v1, registerErr := catalog.RegisterWorkflow(t.Context(), v1Input, true)
	if registerErr != nil {
		t.Fatal(registerErr)
	}

	fenceEntered := make(chan struct{})
	releaseFence := make(chan struct{})
	fenceDone := make(chan error, 1)
	go func() {
		fenceDone <- catalog.WithSourceActivationCurrent(context.Background(), func(view appworkflow.SourceActivationRegistry) error {
			current, resolveErr := view.ResolveWorkflow(context.Background(), registry.WorkflowQuery{Name: v1.Name})
			if resolveErr != nil || !current.Movable || current.Record.Digest != v1.Digest {
				return fmt.Errorf("fenced current = %#v: %w", current, resolveErr)
			}
			close(fenceEntered)
			<-releaseFence
			return nil
		})
	}()
	select {
	case <-fenceEntered:
	case fenceErr := <-fenceDone:
		t.Fatalf("enter current fence: %v", fenceErr)
	}

	mutationStarted := make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		close(mutationStarted)
		_, registerErr := catalog.RegisterWorkflow(context.Background(), v2, true)
		mutationDone <- registerErr
	}()
	<-mutationStarted
	select {
	case mutationErr := <-mutationDone:
		t.Fatalf("current mutation escaped the activation admission fence: %v", mutationErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFence)
	if fenceErr := <-fenceDone; fenceErr != nil {
		t.Fatal(fenceErr)
	}
	if mutationErr := <-mutationDone; mutationErr != nil {
		t.Fatal(mutationErr)
	}
	current, resolveErr := catalog.ResolveWorkflow(t.Context(), registry.WorkflowQuery{Name: v1.Name})
	if resolveErr != nil || current.Record.Version != v2.Version || !bytes.Equal(current.Record.Source, v2.Source) {
		t.Fatalf("current after serialized mutation = %#v, %v", current, resolveErr)
	}
}

func TestProductionWorkflowCatalogRejectsUnsafeLockFiles(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{name: "symlink", setup: func(t *testing.T, lockPath string) {
			t.Helper()
			target := lockPath + ".target"
			if err := os.WriteFile(target, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, lockPath); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "permissive", setup: func(t *testing.T, lockPath string) {
			t.Helper()
			if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "workflow-catalog.json")
			test.setup(t, path+".lock")
			if _, err := openProductionWorkflowCatalog(path); err == nil {
				t.Fatal("unsafe catalog lock was accepted")
			}
		})
	}
}

func TestProductionWorkflowPolicyConfirmsAdvisedAndDeniesInvalidFacts(t *testing.T) {
	identity := localWorkflowIdentity()
	facts := hoststate.PolicyFacts{
		Operation: "start", RunID: "policy-run",
		Plan:            workflowruntime.PlanRef{ID: "policy", Version: "v1", Digest: values.SHA256Digest([]byte("policy")), SchemaVersion: "1"},
		Identity:        identity,
		RunScope:        identity.RunScope,
		ExecutionTarget: identity.ExecutionTarget,
		Effects:         graph.EffectSet{graph.EffectCompute},
		NodeCount:       1,
		BlastRadius:     map[string]int{"nodes": 1},
	}
	decision, err := productionWorkflowPolicy(t.Context(), facts)
	if err != nil || decision.Outcome != hoststate.PolicyAllow {
		t.Fatalf("allow policy = %#v, %v", decision, err)
	}
	facts.ConfirmationAdvised = true
	decision, err = productionWorkflowPolicy(t.Context(), facts)
	if err != nil || decision.Outcome != hoststate.PolicyConfirm {
		t.Fatalf("confirmation policy = %#v, %v", decision, err)
	}
	facts.Identity.Principal = ""
	decision, err = productionWorkflowPolicy(t.Context(), facts)
	if err != nil || decision.Outcome != hoststate.PolicyDeny {
		t.Fatalf("invalid facts policy = %#v, %v", decision, err)
	}
}

func TestProductionWorkflowKindBoundaryIsExactAndUnavailableKindsFailClosed(t *testing.T) {
	runtime, cfg, _ := newTestProductionWorkflowRuntime(t)
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })

	var got []appworkflow.KindRef
	for _, spec := range runtime.host.Registry().List() {
		got = append(got, appworkflow.KindRef{Name: spec.Name, Version: spec.Version})
	}
	want := productionWorkflowKindBoundary()
	sort.Slice(got, func(i, j int) bool {
		if got[i].Name == got[j].Name {
			return got[i].Version < got[j].Version
		}
		return got[i].Name < got[j].Name
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("production kinds = %#v, want %#v", got, want)
	}

	const unsupportedSource = `workflow:
  id: unavailable-http
  version: v1
steps:
  - id: fetch
    kind: http
    kind_version: v1
    with:
      url: '"https://example.invalid"'
`
	if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), "unavailable-http.workflow.yaml"), []byte(unsupportedSource), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, err := appworkflow.WithAuthenticatedIdentity(t.Context(), localWorkflowIdentity())
	if err != nil {
		t.Fatal(err)
	}
	validated, err := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{
		Definition: graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: "unavailable-http", Locator: "unavailable-http.workflow.yaml", Version: "v1"},
	})
	if err != nil || validated.Plan != nil || !hasWorkflowDiagnostic(validated.Diagnostics, workflowcompile.CodeUnknownStepKind) {
		t.Fatalf("unavailable kind validation = %#v, %v", validated, err)
	}

	record, err := runtime.catalog.RegisterWorkflow(t.Context(), qualifiedProductionRecord("team/unavailable-http", "team", "v1", unsupportedSource), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, pinErr := runtime.catalog.PinWorkflow(t.Context(), registry.WorkflowQuery{Name: record.Name, Version: record.Version, Digest: record.Digest}); pinErr != nil {
		t.Fatal(pinErr)
	}
	if _, publishErr := runtime.catalog.PublishWorkflow(t.Context(), registry.WorkflowQuery{Name: record.Name, Version: record.Version, Digest: record.Digest}); publishErr != nil {
		t.Fatal(publishErr)
	}
	if bootstrapErr := runtime.BootstrapMCP(t.Context(), "workflow-test-token"); bootstrapErr != nil {
		t.Fatal(bootstrapErr)
	}
	sessionCtx, session, err := runtime.exposure.ResolveSession(t.Context(), "unavailable-kind", "workflow-test-token")
	if err != nil || !session.Authenticated {
		t.Fatalf("resolve session = %#v, %v", session, err)
	}
	ref := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: record.Name, Version: record.Version, Digest: record.Digest}
	if _, err := runtime.exposure.Describe(sessionCtx, session, ref, "run"); err == nil {
		t.Fatal("unavailable production kind was advertised by exact exposure")
	}
	if listed, err := runtime.exposure.Search(sessionCtx, session, "unavailable-http", 10); err != nil || len(listed) != 0 {
		t.Fatalf("unavailable production kind catalog = %#v, %v", listed, err)
	}
}

func TestProductionMCPBootstrapPreservesMutableProfileAcrossRestart(t *testing.T) {
	runtime, cfg, store := newTestProductionWorkflowRuntime(t)
	const token = "workflow-restart-token"
	if err := runtime.BootstrapMCP(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	managementCtx, err := appworkflow.WithAuthenticatedIdentity(t.Context(), localWorkflowIdentity())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := runtime.exposure.GetProfile(managementCtx, workflowMCPProfileID)
	if err != nil {
		t.Fatal(err)
	}
	profile.Record.Namespaces = []string{"team"}
	profile.Record.Display = values.DisplayPolicy{Private: values.PrivateDisplayReveal}
	updated, err := runtime.exposure.PutProfile(managementCtx, profile.Record, profile.Generation)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := newProductionWorkflowRuntime(store, cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	if bootstrapErr := restarted.BootstrapMCP(t.Context(), token); bootstrapErr != nil {
		t.Fatal(bootstrapErr)
	}
	after, err := restarted.exposure.GetProfile(managementCtx, workflowMCPProfileID)
	if err != nil || !reflect.DeepEqual(after.Record, updated.Record) || after.Generation != updated.Generation {
		t.Fatalf("restarted profile = %#v, %v; want %#v", after, err, updated)
	}
	if err := restarted.BootstrapMCP(t.Context(), "different-workflow-token"); err == nil {
		t.Fatal("different token unexpectedly rebound the fixed MCP principal")
	}
}

func TestProductionMCPAdapterUsesSharedWorkflowComposition(t *testing.T) {
	runtime, _, _ := newTestProductionWorkflowRuntime(t)
	const token = "workflow-mcp-call-token"
	if err := runtime.BootstrapMCP(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	adapter := mcpadapter.New(nil, nil, nil, nil, token, nil,
		mcpadapter.WithWorkflowOnly(),
		mcpadapter.WithWorkflowServices(runtime.exposure, runtime.operations, runtime.operations, runtime.operations),
		mcpadapter.WithWorkflowLifecycle(runtime.lifecycle),
	)
	result := adapter.CallTool(t.Context(), "hadron_workflows_search", map[string]any{"query": "", "limit": 1})
	if result == nil || result.IsError {
		t.Fatalf("production workflow MCP search = %#v", result)
	}
	if health := adapter.CallTool(t.Context(), "hadron_health", nil); health == nil || len(health.Content) != 1 || !strings.Contains(health.Content[0].(mcp.TextContent).Text, `"code":"not_found"`) {
		t.Fatalf("workflow-only MCP exposed independent health = %#v", health)
	}
}

func TestProductionResourceAdmissionEnforcesFanOutOccupancy(t *testing.T) {
	ctx := t.Context()
	base := time.Date(2026, 8, 25, 18, 0, 0, 0, time.UTC)
	store := inmemory.NewStore()
	runID := workflowruntime.RunID("production-fanout")
	ref := workflowruntime.PlanRef{ID: "production-fanout", Version: "v1", Digest: values.SHA256Digest([]byte("production-fanout")), SchemaVersion: workflowcompile.ExecutionPlanSchemaVersion}
	if _, _, err := store.CreateRun(ctx, workflowruntime.CreateRunRequest{ID: runID, Plan: ref, Status: workflowruntime.RunPending, StartIdempotencyKey: "production-fanout", CreatedAt: base}); err != nil {
		t.Fatal(err)
	}
	parent := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "bulk"}
	if _, err := store.CreateNodeInvocation(ctx, workflowruntime.CreateNodeInvocationRequest{Snapshot: workflowruntime.NodeInvocationSnapshot{ID: parent, Status: workflowruntime.NodePending, CreatedAt: base, UpdatedAt: base}}); err != nil {
		t.Fatal(err)
	}
	expanded, err := (workflowruntime.FanOutCoordinator{Store: store}).Expand(ctx, workflowruntime.FanOutExpandCommand{
		Parent: parent, ExpectedParentGeneration: 1,
		Spec: graph.ForEachSpec{Items: graph.Expression{Text: `["first", "second"]`}, MaxConcurrency: 1}, At: base.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := workflowcompile.ExecutionPlan{
		SchemaVersion: ref.SchemaVersion, ID: ref.ID, Digest: ref.Digest,
		Graph: graph.Graph{ID: ref.ID, Version: ref.Version, Nodes: []graph.Node{{ID: parent.NodeID, Kind: "transform", KindVersion: "v1"}}},
	}
	recovery := workflowruntime.RecoveryPlan{Ref: ref, Plan: plan, Visibility: workflowcompile.ValueVisibilityPlan{Nodes: map[string]workflowcompile.ValueScope{parent.NodeID: {FanOut: true}}}}
	kinds := stepkind.NewRegistry()
	if registerErr := kinds.Register(newProductionTransformKind()); registerErr != nil {
		t.Fatal(registerErr)
	}
	queue := workflowruntime.NewResourceReadyQueueCoordinator(store, nil, productionSchedulerAdmission{state: store, plans: fixedProductionRecoveryPlan{plan: recovery}, kinds: kinds, workers: 4})
	first, acquired, err := queue.ClaimNext(ctx, workflowruntime.ReadyClaimRequest{Owner: "worker-one", Token: "token-one", IdempotencyKey: "claim-one", Now: base.Add(2 * time.Second), LeaseUntil: base.Add(time.Minute)})
	if err != nil || !acquired || first.Candidate.InvocationID != expanded.Children[0].ID {
		t.Fatalf("first fan-out claim = %#v, %v, %v", first, acquired, err)
	}
	claimed, err := store.LoadNodeInvocation(ctx, first.Candidate.InvocationID)
	if err != nil {
		t.Fatal(err)
	}
	proof := workflowruntime.ClaimProof{Owner: first.Lease.Owner, Token: first.Lease.Token, Generation: first.Lease.Generation}
	started, err := store.StartNodeAttempt(ctx, workflowruntime.StartNodeAttemptRequest{
		InvocationID: claimed.ID, ExpectedNodeGeneration: claimed.Generation, Claim: proof,
		Executor: workflowruntime.ExecutorMetadata{Kind: "transform", Version: "v1"}, Inputs: claimed.Inputs, At: base.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, transitionErr := store.TransitionNode(ctx, workflowruntime.NodeTransitionRequest{InvocationID: claimed.ID, ExpectedGeneration: started.Node.Generation, To: workflowruntime.NodeWaiting, Claim: &proof, At: base.Add(4 * time.Second)}); transitionErr != nil {
		t.Fatal(transitionErr)
	}
	if second, secondAcquired, secondErr := queue.ClaimNext(ctx, workflowruntime.ReadyClaimRequest{Owner: "worker-two", Token: "token-two", IdempotencyKey: "claim-two", Now: base.Add(5 * time.Second), LeaseUntil: base.Add(time.Minute)}); secondErr != nil || secondAcquired {
		t.Fatalf("second fan-out claim = %#v, %v, %v", second, secondAcquired, secondErr)
	}
	resources, err := store.InspectSchedulerResources(ctx, workflowruntime.SchedulerResourceQuery{RunID: runID, Now: base.Add(5 * time.Second)})
	if err != nil || len(resources.Waiters) != 1 || len(resources.Waiters[0].Blocked) != 1 || resources.Waiters[0].Blocked[0].Kind != workflowruntime.SchedulerResourceFanOut {
		t.Fatalf("fan-out scheduler resources = %#v, %v", resources, err)
	}
}

func TestWorkflowWorkerStopRetainsJoinStateAfterTimeout(t *testing.T) {
	workers := &workflowWorkers{}
	workers.cancel = func() {}
	workers.done = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := workers.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("timed stop = %v", err)
	}
	if workers.cancel == nil || workers.done == nil {
		t.Fatal("timed stop discarded live worker join state")
	}
	close(workers.done)
	if err := workers.Stop(context.Background()); err != nil || workers.cancel != nil || workers.done != nil {
		t.Fatalf("drained stop = %v, cancel=%v, done=%v", err, workers.cancel != nil, workers.done != nil)
	}
}

func TestWorkflowActivationBridgeZeroDelayPublicationAndStopJoin(t *testing.T) {
	bridge := newWorkflowActivationBridge()
	entered := make(chan struct{})
	release := make(chan struct{})
	bridge.beforeTimerDispatch = func() {
		close(entered)
		<-release
	}
	bridge.mu.Lock()
	bridge.accepting, bridge.dispatchReady = true, true
	activation := workflowwait.Activation{
		ID: "timer-stop", Kind: workflowruntime.ActivationWaitWake, RunID: "run", NodeID: "node", WaitID: "wait",
		FireAt: time.Now().UTC(), DedupKey: "timer-stop",
	}
	bridge.armLocked(activation, 0)
	bridge.mu.Unlock()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timer callback did not start")
	}
	stopped := make(chan struct{})
	go func() {
		bridge.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("bridge stop returned while callback was still running")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("bridge stop did not join the timer callback")
	}
}

func TestWorkflowActivationBridgeCanceledTimerCannotRearmAfterCallbackBegins(t *testing.T) {
	bridge := newWorkflowActivationBridge()
	activation := workflowwait.Activation{
		ID: "timer-canceled", Kind: workflowruntime.ActivationWaitWake, RunID: "run", NodeID: "node", WaitID: "wait",
		FireAt: time.Now().UTC(), DedupKey: "timer-canceled",
	}
	bridge.mu.Lock()
	bridge.accepting = true
	timer := &workflowActivationTimer{timer: time.NewTimer(time.Hour)}
	bridge.timers[activation.ID] = timer
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		bridge.fire(activation, timer)
		close(done)
	}()
	<-started
	delete(bridge.timers, activation.ID)
	bridge.mu.Unlock()
	<-done
	if _, resurrected := bridge.timers[activation.ID]; resurrected {
		t.Fatal("canceled timer callback rearmed itself")
	}
	if !timer.timer.Stop() {
		<-timer.timer.C
	}
}

func TestWorkflowDispatchIdempotencyIsDurableInvocationScoped(t *testing.T) {
	first := workflowruntime.NodeInvocationID{RunID: "run", NodeID: "node", Iteration: "0"}
	firstKey := workflowDispatchIdempotencyKey(first)
	if firstKey == "" || firstKey != workflowDispatchIdempotencyKey(first) {
		t.Fatal("dispatch key is not stable")
	}
	second := first
	second.Iteration = "1"
	if workflowDispatchIdempotencyKey(first) == workflowDispatchIdempotencyKey(second) {
		t.Fatal("dispatch key collided across durable iterations")
	}
}

type fixedProductionRecoveryPlan struct{ plan workflowruntime.RecoveryPlan }

func (f fixedProductionRecoveryPlan) LoadRecoveryPlan(_ context.Context, run workflowruntime.RunSnapshot) (workflowruntime.RecoveryPlan, error) {
	if run.Plan != f.plan.Ref {
		return workflowruntime.RecoveryPlan{}, errors.New("unexpected production recovery plan")
	}
	return f.plan, nil
}

func newProductionTransformKind() stepkind.StepKind { return transform.New() }

func hasWorkflowDiagnostic(findings []diagnostic.Diagnostic, code diagnostic.Code) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}

func qualifiedProductionRecord(name, namespace, version, source string) registry.WorkflowRecord {
	return registry.WorkflowRecord{
		Name: name, Namespace: namespace, Version: version, Source: []byte(source),
		Authority: "project", TrustClass: "project-owned",
		Provenance:          graph.Provenance{Origin: "workflow-source", Locator: "/project/workflow.yaml"},
		PlanDigest:          values.SHA256Digest([]byte("production-plan")),
		ContractSuiteDigest: values.SHA256Digest([]byte("production-suite")),
		ContractTestDigest:  values.SHA256Digest([]byte("production-tests")),
		TestsPassed:         true, PublisherPrincipal: "operator:local", RegisteredAt: time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC),
	}
}

func newTestProductionWorkflowRuntime(t *testing.T) (*productionWorkflowRuntime, *config.Config, *persistence.Store) {
	t.Helper()
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Addr: "127.0.0.1:0", DBPath: filepath.Join(root, "state", "hadron.db"), LogsDir: filepath.Join(root, "logs"), DataDir: filepath.Join(root, "data"), WorkspaceID: "default"}
	if ensureErr := cfg.Ensure(); ensureErr != nil {
		t.Fatal(ensureErr)
	}
	workflowRoot := workflowSourceRoot(cfg)
	if mkdirErr := os.MkdirAll(workflowRoot, 0o750); mkdirErr != nil {
		t.Fatal(mkdirErr)
	}
	source := []byte(`workflow:
  id: production-transform
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: echo
    kind_version: v1
    transform:
      result: inputs.message
    with:
      message: inputs.message
    outputs:
      result:
        type: string
    effects: [compute]
outputs:
  result:
    type: string
    value: steps.echo.outputs.result
`)
	if writeErr := os.WriteFile(filepath.Join(workflowRoot, "production-transform.workflow.yaml"), source, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	store, err := persistence.Open(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	runtime, err := newProductionWorkflowRuntime(store, cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, cfg, store
}
