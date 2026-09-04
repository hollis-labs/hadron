package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/internal/agentcard"
	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/mcpadapter"
	"github.com/hollis-labs/hadron/internal/persistence"
	hadronregistry "github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/go-workflow/adapters/transform"
	"github.com/hollis-labs/go-workflow/authoring"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
)

func TestWorkflowLifecycleCrossSurfaceFlywheel(t *testing.T) {
	lifecycle, mcpLifecycle, exposure, profile, token, cleanup := newWorkflowLifecycleE2E(t)
	defer cleanup()
	identity := appworkflow.IdentityRequest{SourceAuthority: "test", PrincipalHint: "transport-cannot-authorize"}

	missing, err := lifecycle.SearchWorkflowCatalog(t.Context(), appworkflow.SearchWorkflowCatalogRequest{Namespace: "team", Query: "echo", Identity: identity})
	if err != nil || len(missing.Matches) != 0 || missing.NextStep != "draft_validate" {
		t.Fatalf("missing-fit search = %#v, %v", missing, err)
	}
	draft := workflowLifecycleE2EDraft(t)
	validation, err := lifecycle.ValidateWorkflowDraft(t.Context(), appworkflow.ValidateWorkflowDraftRequest{Draft: draft, Identity: identity})
	if err != nil || validation.Plan == nil || len(validation.Diagnostics) != 0 {
		t.Fatalf("validate = %#v, %v", validation, err)
	}
	scaffold, err := lifecycle.GenerateWorkflowContract(t.Context(), appworkflow.GenerateWorkflowContractRequest{Draft: draft, Identity: identity})
	if err != nil || scaffold.Scaffold == nil || scaffold.Validation.Plan == nil {
		t.Fatalf("scaffold = %#v, %v", scaffold, err)
	}
	suite := workflowLifecycleE2ESuite(t, *scaffold.Scaffold, scaffold.Validation.Plan.ID)
	tested, err := lifecycle.TestWorkflowDraft(t.Context(), appworkflow.TestWorkflowDraftRequest{Draft: draft, Suite: suite, Identity: identity})
	if err != nil || tested.Evidence == nil || !tested.Evidence.Passed {
		t.Fatalf("test = %#v, %v", tested, err)
	}
	registered, err := lifecycle.RegisterWorkflowDraft(t.Context(), appworkflow.RegisterWorkflowDraftRequest{Draft: draft, Suite: suite, MakeCurrent: true, Identity: identity})
	if err != nil || registered.Detail == nil || registered.Evidence == nil || !registered.Evidence.Passed {
		t.Fatalf("register = %#v, %v", registered, err)
	}
	ref := registered.Detail.Descriptor.Definition
	packaged, err := lifecycle.PackageWorkflowVersion(t.Context(), appworkflow.PackageWorkflowVersionRequest{Definition: ref, Suite: suite, Identity: identity})
	if err != nil || packaged.Definition != ref || packaged.SizeBytes < 1 || values.ValidateDigest(packaged.Digest) != nil {
		t.Fatalf("package metadata = %#v, %v", packaged, err)
	}
	if _, pinErr := lifecycle.PinRegistryVersion(t.Context(), appworkflow.MutateWorkflowVersionRequest{Definition: ref, Identity: identity}); pinErr != nil {
		t.Fatal(pinErr)
	}
	published, err := lifecycle.PublishWorkflowVersion(t.Context(), appworkflow.MutateWorkflowVersionRequest{Definition: ref, Identity: identity})
	if err != nil || !published.Registry.Current || !published.Registry.RegistryPinned || !published.Registry.Published {
		t.Fatalf("published detail = %#v, %v", published, err)
	}
	pinned, err := lifecycle.PinWorkflowExposure(t.Context(), appworkflow.MutateWorkflowExposureRequest{ProfileID: profile.Record.ID, Definition: ref, ExpectedGeneration: profile.Generation, Identity: identity})
	if err != nil || len(pinned.Record.Pins) != 1 {
		t.Fatalf("profile pin = %#v, %v", pinned, err)
	}

	operations := completeWorkflowSpy()
	var invoked appworkflow.RunWorkflowRequest
	runCalls := 0
	operations.run = func(_ context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
		runCalls++
		invoked = request
		return appworkflow.StartRunResult{}, nil
	}
	operations.inspect = func(_ context.Context, request appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
		outputRef := values.ValueSetRef{ID: "surface-output", Digest: values.SHA256Digest([]byte("surface-output"))}
		return rundiagnostics.Result{
			SchemaVersion: "1", Run: rundiagnostics.RunDiagnostic{ID: request.RunID, Status: workflowruntime.RunSucceeded, Outputs: &outputRef},
			Values: []rundiagnostics.ValueSetDiagnostic{{Ref: outputRef, Roles: []string{"run.outputs"}, Values: values.RenderedValueSet{
				"result": {Type: values.TypeString, Payload: values.RedactedMarker, Producer: values.Producer{Kind: "workflow", Reference: "surface-echo", Output: "result"}, MediaType: "application/json", Digest: values.SHA256Digest([]byte("hello")), Redaction: values.RedactionPrivate, Retention: values.RetentionRun, Masked: true},
			}}},
		}, nil
	}
	adapter := mcpadapter.New(nil, nil, nil, nil, token, nil,
		mcpadapter.WithWorkflowServices(exposure, operations, nil, nil),
		mcpadapter.WithWorkflowLifecycle(mcpLifecycle),
	)
	mcpSearch := adapter.CallTool(t.Context(), "hadron_workflow_catalog_search", map[string]any{"namespace": "team", "query": "echo", "limit": 10})
	searchResult, ok := mcpSearch.StructuredContent.(appworkflow.WorkflowCatalogSearchResult)
	if mcpSearch.IsError || !ok || len(searchResult.Matches) != 1 || searchResult.Matches[0].Definition != ref {
		t.Fatalf("MCP catalog search = %#v", mcpSearch.StructuredContent)
	}
	load := adapter.CallTool(t.Context(), "hadron_workflows_load", map[string]any{"definitions": []string{ref.ID + "@" + ref.Version + "@" + ref.Digest}})
	if load.IsError {
		t.Fatalf("MCP lazy load = %#v", load.StructuredContent)
	}
	invocation := adapter.CallTool(t.Context(), published.Descriptor.ToolName, map[string]any{"message": "hello"})
	if invocation.IsError || invoked.Definition != ref || invoked.Inputs["message"] != "hello" {
		t.Fatalf("MCP generated invocation = %#v request=%#v", invocation.StructuredContent, invoked)
	}
	invocationJSON, err := json.Marshal(invocation.StructuredContent)
	if err != nil || !bytes.Contains(invocationJSON, []byte(`"run_id"`)) || !bytes.Contains(invocationJSON, []byte(`"status"`)) {
		t.Fatalf("MCP async output = %s, %v", invocationJSON, err)
	}
	var handle struct {
		RunID string `json:"run_id"`
	}
	if decodeErr := json.Unmarshal(invocationJSON, &handle); decodeErr != nil || handle.RunID == "" {
		t.Fatalf("MCP run handle = %#v, %v", handle, decodeErr)
	}
	inspectedRun := adapter.CallTool(t.Context(), "hadron_workflow_run_inspect", map[string]any{"run_id": handle.RunID})
	diagnostic, ok := inspectedRun.StructuredContent.(rundiagnostics.Result)
	if inspectedRun.IsError || !ok || len(diagnostic.Values) != 1 || !diagnostic.Values[0].Values["result"].Masked || diagnostic.Values[0].Values["result"].Payload != values.RedactedMarker || diagnostic.Values[0].Roles[0] != "run.outputs" {
		t.Fatalf("MCP typed redacted output = %#v", inspectedRun.StructuredContent)
	}

	cliDetail := workflowLifecycleE2ECLI(t, lifecycle, ref)
	httpDetail := workflowLifecycleE2EHTTP(t, lifecycle, ref)
	builder, err := agentcard.NewBuilder(exposure)
	if err != nil {
		t.Fatal(err)
	}
	card, err := builder.Card(t.Context(), "https://hadron.test")
	if err != nil || len(card.Skills) != 1 {
		t.Fatalf("agent card = %#v, %v", card, err)
	}
	skill := card.Skills[0]
	for surface, detail := range map[string]appworkflow.WorkflowVersionDetail{"direct": published, "cli": cliDetail, "http": httpDetail} {
		if detail.Descriptor.Definition != ref || detail.Descriptor.Evidence != published.Descriptor.Evidence || detail.Descriptor.OutputSchema["type"] != "object" {
			t.Fatalf("%s version/evidence/output diverged: %#v", surface, detail)
		}
	}
	if skill.Definition != ref || skill.Evidence != published.Descriptor.Evidence || !workflowLifecycleE2EJSONEqual(skill.OutputSchema, published.Descriptor.OutputSchema) {
		t.Fatalf("A2A skill diverged = %#v", skill)
	}
	generated, err := os.ReadFile(filepath.Join("..", "hadron-app", "frontend", "src", "api", "generated", "workflow.ts"))
	if err != nil || !bytes.Contains(generated, []byte("export type WorkflowVersionDetail")) || !bytes.Contains(generated, []byte("contract_test_digest")) || !bytes.Contains(generated, []byte("/v1/workflows/lifecycle/catalog/inspect")) {
		t.Fatalf("generated UI lifecycle contract is stale: %v", err)
	}

	unpinned, err := lifecycle.UnpinWorkflowExposure(t.Context(), appworkflow.MutateWorkflowExposureRequest{ProfileID: profile.Record.ID, Definition: ref, ExpectedGeneration: pinned.Generation, Identity: identity})
	if err != nil || len(unpinned.Record.Pins) != 0 {
		t.Fatalf("profile unpin = %#v, %v", unpinned, err)
	}
	removed := adapter.CallTool(t.Context(), published.Descriptor.ToolName, map[string]any{"message": "again"})
	removedJSON, marshalErr := json.Marshal(removed.Content)
	if marshalErr != nil || !bytes.Contains(removedJSON, []byte(`not_found`)) || runCalls != 1 {
		t.Fatalf("unpin retained generated MCP tool: content=%s calls=%d error=%v", removedJSON, runCalls, marshalErr)
	}
}

func newWorkflowLifecycleE2E(t *testing.T) (*appworkflow.WorkflowLifecycleService, *appworkflow.WorkflowLifecycleService, *appworkflow.WorkflowExposureService, hoststate.ExposureProfileSnapshot, string, func()) {
	t.Helper()
	stager := appworkflow.NewAuthoringSourceStager()
	catalog := hadronregistry.NewWorkflowIndex()
	kinds := stepkind.NewRegistry()
	if err := kinds.Register(transform.New()); err != nil {
		t.Fatal(err)
	}
	resolver, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{t.TempDir()}, Authoring: stager, Registry: catalog,
		Authorizer: appworkflow.DefinitionAuthorizerFunc(func(context.Context, appworkflow.DefinitionAuthorization) error { return nil }),
		Compile:    appworkflow.DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "lifecycle-cross-surface-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	contracts, err := appworkflow.NewContractRegistrationService(appworkflow.ContractRegistrationOptions{
		Definitions: resolver, StepKinds: kinds, Catalog: catalog,
		Authorizer: appworkflow.NamespaceAuthorizerFunc(func(context.Context, appworkflow.NamespaceAuthorization) error { return nil }),
		Attestor:   workflowLifecycleE2EAttestor{}, Policy: appworkflow.ContractTestPolicy{MinimumCases: 1, Repetitions: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := appworkflow.NewAgentAuthoringService(appworkflow.AgentAuthoringOptions{
		Stager: stager, Contracts: contracts,
		HostIdentity: appworkflow.AgentAuthoringHostIdentity{Authority: "host:cross-surface", TrustClass: "reviewed", Principal: "configured-not-authoritative"},
	})
	if err != nil {
		t.Fatal(err)
	}
	database, err := persistence.Open(filepath.Join(t.TempDir(), "workflow-lifecycle.db"))
	if err != nil {
		t.Fatal(err)
	}
	exposureStore, err := persistence.NewWorkflowExposureStore(database)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	exposure, err := appworkflow.NewWorkflowExposureService(appworkflow.WorkflowExposureOptions{Store: exposureStore, Catalog: catalog, Definitions: resolver, StepKinds: kinds})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	profile, err := exposure.PutProfile(t.Context(), hoststate.ExposureProfileRecord{ID: "lifecycle-e2e", MaxDirectTools: 2, SearchScope: hoststate.ExposureSearchPublic, LazyLoad: true}, 0)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	token := "workflow-lifecycle-e2e-token"
	binding := workflowLifecycleE2EBinding("principal:lifecycle-e2e", "mcp")
	if _, principalErr := exposure.PutPrincipal(t.Context(), appworkflow.PutMCPPrincipalRequest{Record: hoststate.MCPPrincipalRecord{ID: binding.Principal, ProfileID: profile.Record.ID, Identity: binding}, Token: token}); principalErr != nil {
		_ = database.Close()
		t.Fatal(principalErr)
	}
	lifecycle, err := appworkflow.NewWorkflowLifecycleService(appworkflow.WorkflowLifecycleOptions{Identity: workflowLifecycleE2EIdentity{}, Contracts: contracts, Authoring: agent, Exposure: exposure})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	mcpLifecycle, err := appworkflow.NewWorkflowLifecycleService(appworkflow.WorkflowLifecycleOptions{Identity: appworkflow.MCPIdentityProvider{}, Contracts: contracts, Authoring: agent, Exposure: exposure})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	return lifecycle, mcpLifecycle, exposure, profile, token, func() { _ = database.Close() }
}

type workflowLifecycleE2EIdentity struct{}

func (workflowLifecycleE2EIdentity) BindIdentity(_ context.Context, request appworkflow.IdentityRequest) (hoststate.IdentityBinding, error) {
	authority := request.SourceAuthority
	if authority == "" {
		authority = "test"
	}
	return workflowLifecycleE2EBinding("principal:lifecycle-e2e", authority), nil
}

func workflowLifecycleE2EBinding(principal, authority string) hoststate.IdentityBinding {
	return hoststate.IdentityBinding{
		Principal: principal, SourceAuthority: authority, Trust: "trusted",
		RunScope: hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeUser, ID: principal},
	}
}

type workflowLifecycleE2EAttestor struct{}

func (workflowLifecycleE2EAttestor) AttestContractReport(_ context.Context, digest string) (string, error) {
	return "e2e:" + digest, nil
}

func (workflowLifecycleE2EAttestor) VerifyContractReport(_ context.Context, digest, attestation string) error {
	if digest == "" || attestation != "e2e:"+digest {
		return errors.New("invalid lifecycle E2E attestation")
	}
	return nil
}

func workflowLifecycleE2EDraft(t *testing.T) appworkflow.WorkflowDraft {
	t.Helper()
	input := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "inputs.message"}}
	output := graph.Binding{Kind: graph.BindingExpression, Expression: &graph.Expression{Text: "steps.echo.outputs.result"}}
	envelope := authoring.New("surface-echo", "v1").
		Input(graph.InputSpec{Name: "message", Schema: graph.Schema{"type": "string"}, Required: true}).
		Node(graph.Node{ID: "echo", Kind: "transform", KindVersion: "v1", Config: graph.Config{"result": "inputs.message"}, InputBindings: map[string]graph.Binding{"message": input}, Outputs: []graph.OutputSpec{{Name: "result", Schema: graph.Schema{"type": "string"}}}, Effects: graph.EffectSet{graph.EffectCompute}}).
		Output(graph.OutputSpec{Name: "result", Schema: graph.Schema{"type": "string"}, Value: &output}).Envelope()
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return appworkflow.WorkflowDraft{Envelope: encoded, ID: "surface-echo", Version: "v1", Namespace: "team"}
}

func workflowLifecycleE2ESuite(t *testing.T, scaffold appworkflow.WorkflowContractSuite, planID string) appworkflow.WorkflowContractSuite {
	t.Helper()
	input, err := values.NewInline("hello", values.Metadata{Producer: values.Producer{Kind: "contract-input", Reference: planID, Output: "message"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	output, err := values.NewInline("hello", values.Metadata{Producer: values.Producer{Kind: "contract-output", Reference: planID, Output: "result"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	current := &scaffold.Cases[0]
	current.Name, current.Editable = "echo", false
	current.Inputs, current.ExpectedOutputs = values.ValueSet{"message": input}, values.ValueSet{"result": output}
	current.Mocks[0].ExpectedInputs = values.ValueSet{"message": input}
	current.Mocks[0].ExpectedInputsEditable = false
	current.Mocks[0].Results = []appworkflow.ContractMockResult{{Outputs: values.ValueSet{"result": output}}}
	return scaffold
}

func workflowLifecycleE2ECLI(t *testing.T, lifecycle appworkflow.WorkflowLifecycleOperations, ref graph.DefinitionRef) appworkflow.WorkflowVersionDetail {
	t.Helper()
	dependencies := testWorkflowDependencies(completeWorkflowSpy())
	dependencies.lifecycle = lifecycle
	command := buildWorkflowCmdWithDependencies(dependencies)
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"catalog", "inspect", ref.ID + "@" + ref.Version + "#" + ref.Digest, "--json"})
	if err := command.Execute(); err != nil {
		t.Fatalf("CLI inspect: %v\n%s", err, output.String())
	}
	var result appworkflow.WorkflowVersionDetail
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("CLI detail: %v\n%s", err, output.String())
	}
	return result
}

func workflowLifecycleE2EHTTP(t *testing.T, lifecycle appworkflow.WorkflowLifecycleOperations, ref graph.DefinitionRef) appworkflow.WorkflowVersionDetail {
	t.Helper()
	auth := api.WorkflowRequestAuthenticatorFunc(func(request *http.Request, _ appworkflow.WorkflowAccessIntent) (context.Context, error) {
		return request.Context(), nil
	})
	server := httptest.NewServer(api.NewServer("", api.Dependencies{WorkflowLifecycle: lifecycle, WorkflowAuth: auth}).Handler())
	defer server.Close()
	body, err := json.Marshal(appworkflow.InspectWorkflowVersionRequest{Definition: ref, Identity: appworkflow.IdentityRequest{SourceAuthority: "forged"}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/v1/workflows/lifecycle/catalog/inspect", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(response.Body)
		t.Fatalf("HTTP inspect status=%d body=%s", response.StatusCode, message)
	}
	var result appworkflow.WorkflowVersionDetail
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func workflowLifecycleE2EJSONEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

var _ appworkflow.IdentityProvider = workflowLifecycleE2EIdentity{}
var _ appworkflow.ContractReportAttestor = workflowLifecycleE2EAttestor{}
