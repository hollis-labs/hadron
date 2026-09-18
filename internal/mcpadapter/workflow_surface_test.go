package mcpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	gomcp "github.com/hollis-labs/go-mcp/server"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/go-workflow/diagnostic"
	"github.com/hollis-labs/go-workflow/graph"
	"github.com/hollis-labs/go-workflow/values"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// workflowConnect wires srv to a fresh in-memory client session, for the
// handful of tests below that need to observe real wire behavior (schema on
// the wire, tools/list_changed notifications, malformed-argument rejection)
// rather than calling handler methods directly.
func workflowConnect(t *testing.T, srv *gomcp.Server, opts *mcpsdk.ClientOptions) *mcpsdk.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	ss, err := srv.SDKServer().Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.0"}, opts)
	cs, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func workflowToolDefs(srv *gomcp.Server) map[string]gomcp.ToolDefinition {
	defs := make(map[string]gomcp.ToolDefinition)
	for _, def := range srv.ToolDefinitions() {
		defs[def.Name] = def
	}
	return defs
}

// TestWorkflowSurfacePinnedSchemasAndProfileRemoval covers pinned (direct)
// and lazy-loaded workflow tool mounting, the generated tool's schema and
// output-envelope contract, and mount removal on a profile-generation
// change -- against the single global mount described in workflowSurface's
// doc comment. Mark3labs-era per-session isolation (this test used to
// mount two DIFFERENT principals into two DIFFERENT concurrent sessions and
// assert neither leaked into the other) no longer has a code path to test:
// Hadron serves exactly one stdio connection per process, and the official
// SDK's tool registry is process-global by design, so there is only ever
// one mount to reason about.
func TestWorkflowSurfacePinnedSchemasAndProfileRemoval(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, operations, operations, operations))
	srv := adapter.newServer()

	if _, _, err := adapter.workflow.current(t.Context(), "token-a"); err != nil {
		t.Fatal(err)
	}

	defs := workflowToolDefs(srv)
	firstTool, ok := defs[exposure.alpha.ToolName]
	if !ok || firstTool.InputSchema == nil || firstTool.OutputSchema == nil {
		t.Fatalf("pinned tool = %#v", defs)
	}
	if !strings.Contains(firstTool.Description, "asynchronous durable run") || !strings.Contains(firstTool.Description, "outputs is optional") || !strings.Contains(firstTool.Description, "hadron_workflow_run_inspect") {
		t.Fatalf("pinned tool does not document its run-handle contract: %q", firstTool.Description)
	}

	result, callErr := srv.CallTool(t.Context(), exposure.alpha.ToolName, map[string]any{"message": "hello", "run_id": "workflow-input-not-control"})
	if callErr != nil {
		t.Fatalf("pinned invocation = %#v, %v", result, callErr)
	}
	handle, ok := result.(workflowInvocationResult)
	if !ok || handle.RunID == "" || handle.Status != "not_admitted" {
		t.Fatalf("pinned invocation handle = %#v", result)
	}
	outputSchema, ok := firstTool.OutputSchema.(json.RawMessage)
	if !ok {
		t.Fatalf("pinned output schema type = %T", firstTool.OutputSchema)
	}
	var outputDocument map[string]any
	if decodeErr := json.Unmarshal(outputSchema, &outputDocument); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	properties, ok := outputDocument["properties"].(map[string]any)
	if !ok {
		t.Fatalf("pinned output envelope schema = %#v", outputDocument)
	}
	gotOutputs, err := json.Marshal(properties["outputs"])
	if err != nil {
		t.Fatal(err)
	}
	wantOutputs, err := json.Marshal(exposure.alpha.OutputSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotOutputs, wantOutputs) {
		t.Fatalf("nested workflow output contract = %s, want %s", gotOutputs, wantOutputs)
	}
	compiler := jsonschema.NewCompiler()
	if resourceErr := compiler.AddResource("memory://workflow-run-handle.json", outputDocument); resourceErr != nil {
		t.Fatal(resourceErr)
	}
	compiled, err := compiler.Compile("memory://workflow-run-handle.json")
	if err != nil {
		t.Fatal(err)
	}
	encodedHandle, err := json.Marshal(handle)
	if err != nil {
		t.Fatal(err)
	}
	var handleDocument any
	if decodeErr := json.Unmarshal(encodedHandle, &handleDocument); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if validationErr := compiled.Validate(handleDocument); validationErr != nil {
		t.Fatalf("real pinned result violates advertised output schema: %v", validationErr)
	}
	operations.mu.Lock()
	if len(operations.runs) != 1 || operations.runs[0].Definition != exposure.alpha.Definition || operations.runs[0].Identity.SourceAuthority != "mcp" || operations.runs[0].Inputs["message"] != "hello" || operations.runs[0].Inputs["run_id"] != "workflow-input-not-control" || string(operations.runs[0].RunID) == "workflow-input-not-control" {
		t.Fatalf("run requests = %#v", operations.runs)
	}
	operations.mu.Unlock()

	metaResult, err := adapter.workflow.handleRun(t.Context(), map[string]any{"name": exposure.alpha.Name, "version": exposure.alpha.Version, "digest": exposure.alpha.Digest, "run_id": "meta-run", "idempotency_key": "meta-key"})
	if err != nil {
		t.Fatalf("meta invocation = %#v, %v", metaResult, err)
	}
	operations.mu.Lock()
	if len(operations.runs) != 2 || string(operations.runs[1].RunID) != "meta-run" || operations.runs[1].IdempotencyKey != "meta-key" || len(operations.runs[1].Inputs) != 0 || operations.runs[1].Definition != exposure.alpha.Definition {
		t.Fatalf("meta run request = %#v", operations.runs)
	}
	operations.mu.Unlock()

	loaded, err := adapter.workflow.handleLoad(t.Context(), map[string]any{"definitions": []string{"team/lazy@v1@" + exposure.lazy.Digest}})
	if err != nil {
		t.Fatalf("lazy load = %#v, %v", loaded, err)
	}
	if _, ok := workflowToolDefs(srv)[exposure.lazy.ToolName]; !ok {
		t.Fatal("lazy tool was not mounted")
	}

	exposure.setGeneration("token-a", 2, nil)
	if _, _, err := adapter.workflow.current(t.Context(), "token-a"); err != nil {
		t.Fatal(err)
	}
	if _, ok := workflowToolDefs(srv)[exposure.alpha.ToolName]; ok {
		t.Fatal("profile change retained stale mounts")
	}
	if _, ok := workflowToolDefs(srv)[exposure.lazy.ToolName]; ok {
		t.Fatal("profile change retained stale lazy mounts")
	}
}

func TestWorkflowOnlySurfaceContainsNoLegacyToolsHealthSkillsPromptsOrResources(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil,
		WithWorkflowOnly(),
		WithWorkflowServices(exposure, operations, operations, operations),
		WithWorkflowLifecycle(&fakeWorkflowLifecycle{}),
	)
	srv := adapter.newServer()
	for name := range workflowToolDefs(srv) {
		// newServer mounts the fake exposure's direct workflow tools (here,
		// "workflow_team_alpha" for token-a) synchronously, same as
		// production -- those are graph-native, not legacy, so they belong
		// alongside the hadron_workflow(s)_* meta-tools and hadron_skills.
		if name == "hadron_skills" || strings.HasPrefix(name, "hadron_workflow_") || strings.HasPrefix(name, "hadron_workflows_") || strings.HasPrefix(name, "workflow_") {
			continue
		}
		t.Fatalf("workflow-only tool %q is outside the graph-native surface", name)
	}
	if _, exists := workflowToolDefs(srv)["hadron_health"]; exists {
		t.Fatal("workflow-only MCP retained independent hard-coded health")
	}
	assertNoLegacyWorkflowSurfaceText(t, "server instructions", workflowServerInstructions)

	cs := workflowConnect(t, srv, nil)
	prompts, err := cs.ListPrompts(t.Context(), nil)
	if err != nil {
		t.Fatalf("prompts/list: %v", err)
	}
	if len(prompts.Prompts) != 0 {
		t.Fatalf("workflow-only prompts = %#v", prompts.Prompts)
	}
	resources, err := cs.ListResources(t.Context(), nil)
	if err != nil {
		t.Fatalf("resources/list: %v", err)
	}
	for _, resource := range resources.Resources {
		if !strings.HasPrefix(resource.URI, "workflow://") && !strings.HasPrefix(resource.URI, "hadron://workflows/") {
			t.Fatalf("workflow-only resource = %#v", resource)
		}
	}
	encoded, err := json.Marshal(resources.Resources)
	if err != nil {
		t.Fatal(err)
	}
	assertNoLegacyWorkflowSurfaceText(t, "resources/list", string(encoded))

	index, err := adapter.CallTool(t.Context(), "hadron_skills", nil)
	if err != nil {
		t.Fatalf("workflow skill index = %#v, %v", index, err)
	}
	indexText, ok := index.(map[string]any)
	if !ok {
		t.Fatalf("workflow skill index shape = %#v", index)
	}
	indexJSON, err := json.Marshal(indexText)
	if err != nil {
		t.Fatal(err)
	}
	assertNoLegacyWorkflowSurfaceText(t, "skill index", string(indexJSON))
	var catalog struct {
		Items []hadronSkillDoc `json:"items"`
	}
	if err := json.Unmarshal(indexJSON, &catalog); err != nil {
		t.Fatal(err)
	}
	wantSkills := []string{"start-here", "workflow-lifecycle", "run-inspection"}
	gotSkills := make([]string, len(catalog.Items))
	for index := range catalog.Items {
		gotSkills[index] = catalog.Items[index].Name
	}
	if !reflect.DeepEqual(gotSkills, wantSkills) {
		t.Fatalf("workflow skill names = %#v, want %#v", gotSkills, wantSkills)
	}
	advertised := workflowToolDefs(srv)
	for _, skill := range wantSkills {
		result, err := adapter.CallTool(t.Context(), "hadron_skills", map[string]any{"name": skill})
		if err != nil {
			t.Fatalf("workflow skill %q = %#v, %v", skill, result, err)
		}
		body, ok := result.(string)
		if !ok {
			t.Fatalf("workflow skill %q body shape = %#v", skill, result)
		}
		assertNoLegacyWorkflowSurfaceText(t, skill, body)
		for _, name := range workflowSkillToolNames(body) {
			if _, exists := advertised[name]; !exists {
				t.Fatalf("workflow skill %q advertises unregistered tool %q", skill, name)
			}
		}
	}
	if _, hiddenErr := adapter.CallTool(t.Context(), "hadron_skills", map[string]any{"name": "blueprint-discovery"}); hiddenErr == nil || !strings.Contains(hiddenErr.Error(), "skill_not_found") {
		t.Fatalf("legacy skill remained readable, err=%v", hiddenErr)
	}
}

func assertNoLegacyWorkflowSurfaceText(t *testing.T, surface, value string) {
	t.Helper()
	lower := strings.ToLower(value)
	for _, forbidden := range []string{"blueprint", "hadron_run_", "hadron_message_", "hadron_pipeline_", "hadron_schedule_", "hadron_trigger_", "hadron_workflow_run_values", "hadron_workflow_run_waits"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("%s contains legacy or nonexistent surface %q: %s", surface, forbidden, value)
		}
	}
}

func workflowSkillToolNames(body string) []string {
	var result []string
	for _, field := range strings.Fields(body) {
		name := strings.Trim(field, "`.,:;()[]")
		if strings.HasPrefix(name, "hadron_") {
			result = append(result, name)
		}
	}
	return result
}

func TestWorkflowSurfaceFailClosedErrorAndMetaCatalog(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, nil, nil, nil))
	args := map[string]any{"name": "team/alpha", "version": "v1", "digest": exposure.alpha.Digest}
	result, err := adapter.workflow.handleValidate(t.Context(), args)
	if result != nil || err == nil {
		t.Fatalf("uncomposed validate = %#v, %v", result, err)
	}
	var structuredErr budget.StructuredError
	if !errors.As(err, &structuredErr) {
		t.Fatalf("uncomposed validate error type = %#v", err)
	}
	envelope, ok := structuredErr.ToolErrorContent().(map[string]any)
	if !ok {
		t.Fatalf("error envelope = %#v", structuredErr.ToolErrorContent())
	}
	operationError, ok := envelope["error"].(appworkflow.WorkflowOperationError)
	if !ok || operationError.Code != appworkflow.WorkflowErrorCodeUnavailable {
		t.Fatalf("operation error = %#v", envelope["error"])
	}

	exposure.mu.Lock()
	exposure.describeErr = errors.New("database password=super-secret")
	exposure.mu.Unlock()
	adapter = New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}))
	result, err = adapter.workflow.handleDescribe(t.Context(), args)
	if result != nil || err == nil {
		t.Fatalf("hidden describe = %#v, %v", result, err)
	}
	if containsWorkflowTestText(err.Error(), "super-secret") {
		t.Fatalf("transport exposed raw dependency error: %s", err.Error())
	}
	var structuredErr2 budget.StructuredError
	if errors.As(err, &structuredErr2) {
		content, marshalErr := json.Marshal(structuredErr2.ToolErrorContent())
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if containsWorkflowTestText(string(content), "super-secret") {
			t.Fatalf("transport exposed raw dependency error in structured content: %s", content)
		}
	}
}

func TestWorkflowSurfaceRefreshFailureRemovesSameGenerationMounts(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}))
	srv := adapter.newServer()
	if _, _, err := adapter.workflow.current(t.Context(), "token-a"); err != nil {
		t.Fatal(err)
	}
	if _, mounted := workflowToolDefs(srv)[exposure.alpha.ToolName]; !mounted {
		t.Fatal("initial direct workflow was not mounted")
	}
	exposure.setDirectError(errors.New("catalog unavailable"))
	if _, _, err := adapter.workflow.current(t.Context(), "token-a"); err == nil {
		t.Fatal("same-generation direct refresh failure was ignored")
	}
	if _, mounted := workflowToolDefs(srv)[exposure.alpha.ToolName]; mounted {
		t.Fatal("refresh failure retained stale tools")
	}
}

func TestWorkflowSurfaceLoadingIdenticalDirectToolIsBudgetNeutral(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	exposure.setBudget("token-a", 1)
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}))
	srv := adapter.newServer()
	if _, _, err := adapter.workflow.current(t.Context(), "token-a"); err != nil {
		t.Fatal(err)
	}
	args := map[string]any{"definitions": []string{exposure.alpha.Name + "@" + exposure.alpha.Version + "@" + exposure.alpha.Digest}}
	result, err := adapter.workflow.handleLoad(t.Context(), args)
	if err != nil {
		t.Fatalf("idempotent direct load = %#v, %v", result, err)
	}
	defs := workflowToolDefs(srv)
	if len(defs) < 1 {
		t.Fatal("expected at least the direct tool to remain mounted")
	}
	if _, ok := defs[exposure.alpha.ToolName]; !ok {
		t.Fatalf("idempotent load changed direct mount: %#v", defs)
	}
}

func TestWorkflowMountChangeEmitsToolListChangedNotification(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}))
	// newServer computes the initial mount synchronously, before any client
	// can be listening for notifications -- so there is nothing to observe
	// for it specifically. What's testable here is a mount CHANGE after a
	// client has connected.
	srv := adapter.newServer()

	changed := make(chan struct{}, 8)
	_ = workflowConnect(t, srv, &mcpsdk.ClientOptions{
		ToolListChangedHandler: func(context.Context, *mcpsdk.ToolListChangedRequest) { changed <- struct{}{} },
	})

	exposure.setGeneration("token-a", 2, nil)
	if _, _, err := adapter.workflow.current(t.Context(), "token-a"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(2 * time.Second):
		t.Fatal("profile change emitted no tools list-changed notification")
	}
	if _, mounted := workflowToolDefs(srv)[exposure.alpha.ToolName]; mounted {
		t.Fatal("profile change retained a removed workflow tool")
	}
}

func TestWorkflowMetaReplaySafeAnnotations(t *testing.T) {
	for _, name := range []string{"hadron_workflows_load", "hadron_workflow_run_resume", "hadron_workflow_gate_submit", "hadron_workflow_message_submit", "hadron_workflow_signal"} {
		if behavior := workflowMetaBehavior(name); !behavior.idempotent {
			t.Fatalf("%s is not annotated idempotent", name)
		}
	}
	if behavior := workflowMetaBehavior("hadron_workflow_run"); behavior.idempotent {
		t.Fatal("run-by-reference was incorrectly annotated idempotent")
	}
}

func TestWorkflowMetaLimitsRequireExactIntegers(t *testing.T) {
	for _, name := range []string{"hadron_workflows_search", "hadron_workflow_catalog_search", "hadron_workflow_run_events", "hadron_workflow_run_subscribe"} {
		_, schema := workflowMetaToolSchema(name)
		properties, _ := schema["properties"].(map[string]any)
		property, ok := properties["limit"].(map[string]any)
		if !ok || property["type"] != "integer" {
			t.Fatalf("%s limit schema = %#v", name, property)
		}
	}

	if limit, err := exactWorkflowLimitArgument(nil, 20); err != nil || limit != 20 {
		t.Fatalf("absent limit = %d, %v", limit, err)
	}
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "fractional", value: 1.9},
		{name: "string", value: "2"},
		{name: "overflow", value: ^uint64(0)},
		{name: "null", value: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := map[string]any{"limit": test.value}
			if _, err := exactWorkflowLimitArgument(args, 20); err == nil {
				t.Fatalf("limit %#v was accepted", test.value)
			}
		})
	}
	limit, err := exactWorkflowLimitArgument(map[string]any{"limit": float64(1001)}, 100)
	if err != nil || boundedWorkflowLimit(limit) != 1000 {
		t.Fatalf("event limit clamp = %d, %v", limit, err)
	}

	exposure := newFakeWorkflowExposure()
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, operations, operations, operations))
	if result, callErr := adapter.workflow.handleSearch(t.Context(), map[string]any{"limit": 1.9}); result != nil || callErr == nil {
		t.Fatalf("fractional search limit = %#v, %v", result, callErr)
	}
	if result, callErr := adapter.workflow.handleEvents(t.Context(), map[string]any{"run_id": "run-one", "limit": 1.9}); result != nil || callErr == nil {
		t.Fatalf("fractional event limit = %#v, %v", result, callErr)
	}
}

func TestWorkflowLifecycleMetaSchemasAndExactGeneration(t *testing.T) {
	_, testSchema := workflowMetaToolSchema("hadron_workflow_author_test")
	_, registerSchema := workflowMetaToolSchema("hadron_workflow_author_register")
	testProperties := testSchema["properties"].(map[string]any)
	registerProperties := registerSchema["properties"].(map[string]any)
	if _, advertised := testProperties["make_current"]; advertised {
		t.Fatalf("author test advertised register-only make_current: %#v", testProperties)
	}
	if _, advertised := registerProperties["make_current"]; !advertised {
		t.Fatalf("author register omitted make_current: %#v", registerProperties)
	}
	draft, ok := testProperties["draft"].(map[string]any)
	if !ok || draft["description"] == "" {
		t.Fatal("authoring tools must describe the bounded draft contract")
	}
	draftProperties, ok := draft["properties"].(map[string]any)
	if !ok || draftProperties["envelope"] == nil || draftProperties["id"] == nil || draftProperties["version"] == nil || draftProperties["namespace"] == nil {
		t.Fatalf("draft schema is not agent-usable: %#v", draft)
	}
	if required, present := draft["required"].([]any); !present || !reflect.DeepEqual(required, []any{"envelope", "id", "version", "namespace"}) {
		t.Fatalf("draft nested required fields = %#v", draft["required"])
	}
	suite, ok := testProperties["suite"].(map[string]any)
	if !ok || suite["description"] == "" {
		t.Fatal("contract tools must describe the deterministic suite contract")
	}
	suiteProperties, ok := suite["properties"].(map[string]any)
	if !ok || suiteProperties["schema_version"] == nil || suiteProperties["cases"] == nil {
		t.Fatalf("suite schema is not agent-usable: %#v", suite)
	}
	if required, present := suite["required"].([]any); !present || !reflect.DeepEqual(required, []any{"schema_version", "cases"}) {
		t.Fatalf("suite nested required fields = %#v", suite["required"])
	}
	_, exposureSchema := workflowMetaToolSchema("hadron_workflow_exposure_pin_definition")
	generationSchema, ok := exposureSchema["properties"].(map[string]any)["expected_generation"].(map[string]any)
	if !ok || generationSchema["type"] != "integer" || generationSchema["minimum"] != 1 {
		t.Fatalf("expected_generation schema = %#v", generationSchema)
	}

	exposure := newFakeWorkflowExposure()
	lifecycle := &fakeWorkflowLifecycle{}
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil,
		WithWorkflowServices(exposure, operations, operations, operations),
		WithWorkflowLifecycle(lifecycle),
	)
	args := map[string]any{
		"profile_id": "profile:token-a", "name": exposure.alpha.Name,
		"version": exposure.alpha.Version, "digest": exposure.alpha.Digest,
		"expected_generation": 1.9,
	}
	result, err := adapter.workflow.handleLifecycleExposurePin(t.Context(), args)
	if result != nil || err == nil || lifecycle.pinCalls != 0 {
		t.Fatalf("fractional generation = %#v, %v calls=%d", result, err, lifecycle.pinCalls)
	}
	args["expected_generation"] = float64(1 << 53)
	result, err = adapter.workflow.handleLifecycleExposurePin(t.Context(), args)
	if result != nil || err == nil || lifecycle.pinCalls != 0 {
		t.Fatalf("unsafe generation = %#v, %v calls=%d", result, err, lifecycle.pinCalls)
	}
	if parsed, parseErr := parseWorkflowUint64(json.Number("18446744073709551615")); parseErr != nil || parsed != ^uint64(0) {
		t.Fatalf("maximum uint64 = %d, %v", parsed, parseErr)
	}
}

func TestWorkflowLifecycleCatalogSearchIsDistinctAndProfileFiltered(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	lifecycle := &fakeWorkflowLifecycle{search: func(_ context.Context, request appworkflow.SearchWorkflowCatalogRequest) (appworkflow.WorkflowCatalogSearchResult, error) {
		if request.Namespace != "team" || request.Query != "lazy" || request.Identity.SourceAuthority != "mcp" {
			t.Fatalf("catalog search request = %#v", request)
		}
		return appworkflow.WorkflowCatalogSearchResult{Matches: []appworkflow.WorkflowCatalogMatch{{
			Definition: exposure.lazy.Definition, Name: exposure.lazy.Name, Namespace: exposure.lazy.Namespace,
			Score: 90, RecommendedNext: "inspect_exact",
		}, {
			Definition: exposure.alpha.Definition, Name: exposure.alpha.Name, Namespace: exposure.alpha.Namespace,
			Score: 80, RecommendedNext: "inspect_exact",
		}}, NextStep: "inspect_exact"}, nil
	}}
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil,
		WithWorkflowLifecycle(lifecycle),
		WithWorkflowServices(exposure, operations, operations, operations),
	)
	args := map[string]any{"namespace": "team", "query": "lazy", "limit": 10}
	result, err := adapter.workflow.handleLifecycleCatalogSearch(t.Context(), args)
	if err != nil {
		t.Fatalf("catalog search = %#v, %v", result, err)
	}
	search, ok := result.(appworkflow.WorkflowCatalogSearchResult)
	if !ok || len(search.Matches) != 1 || search.Matches[0].Definition != exposure.lazy.Definition || search.NextStep != "inspect_exact" {
		t.Fatalf("profile-filtered catalog result = %#v", result)
	}
	if lifecycle.searchCalls != 1 {
		t.Fatalf("catalog search calls=%d", lifecycle.searchCalls)
	}
	searchDescription, _ := workflowMetaToolSchema("hadron_workflows_search")
	catalogDescription, _ := workflowMetaToolSchema("hadron_workflow_catalog_search")
	if searchDescription == catalogDescription {
		t.Fatal("session discovery and ranked lifecycle catalog search were conflated")
	}
}

func TestWorkflowRawMCPCallsRejectUnsafeNumbers(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, operations, operations, operations))
	srv := adapter.newServer()
	cs := workflowConnect(t, srv, nil)

	res, err := cs.CallTool(t.Context(), &mcpsdk.CallToolParams{
		Name: "hadron_workflow_run",
		Arguments: map[string]any{
			"name": exposure.alpha.Name, "version": exposure.alpha.Version, "digest": exposure.alpha.Digest,
			"inputs": map[string]any{"nested": []any{9007199254740993.0}},
		},
	})
	if err != nil {
		t.Fatalf("unsafe number was rejected outside the tool handler: %v", err)
	}
	if !res.IsError {
		t.Fatalf("unsafe number did not produce a handler error: %#v", res)
	}
	operations.mu.Lock()
	runCount := len(operations.runs)
	operations.mu.Unlock()
	if runCount != 0 {
		t.Fatalf("lossy raw MCP call reached RunWorkflow %d times", runCount)
	}

	res, err = cs.CallTool(t.Context(), &mcpsdk.CallToolParams{Name: "hadron_workflows_search", Arguments: map[string]any{"limit": 1.9}})
	if err != nil {
		t.Fatalf("fractional limit was rejected outside the tool handler: %v", err)
	}
	if !res.IsError {
		t.Fatalf("fractional limit did not produce a handler error: %#v", res)
	}
}

func TestWorkflowGeneratedInvocationIdentityIsRestartUnique(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	firstOperations := &fakeWorkflowOperations{}
	secondOperations := &fakeWorkflowOperations{}
	first := New(nil, nil, nil, nil, "token-a", nil, withWorkflowInstanceNonceForTest("instance-one"), WithWorkflowServices(exposure, firstOperations, firstOperations, firstOperations))
	second := New(nil, nil, nil, nil, "token-a", nil, withWorkflowInstanceNonceForTest("instance-two"), WithWorkflowServices(exposure, secondOperations, secondOperations, secondOperations))
	args := map[string]any{"message": "same invocation"}
	for _, surface := range []*workflowSurface{first.workflow, second.workflow} {
		result, err := surface.run(t.Context(), args, exposure.alpha.Definition, true)
		if err != nil {
			t.Fatalf("generated invocation = %#v, %v", result, err)
		}
	}
	firstOperations.mu.Lock()
	firstRun := firstOperations.runs[0]
	firstOperations.mu.Unlock()
	secondOperations.mu.Lock()
	secondRun := secondOperations.runs[0]
	secondOperations.mu.Unlock()
	if firstRun.RunID == secondRun.RunID || firstRun.IdempotencyKey == secondRun.IdempotencyKey || !strings.Contains(string(firstRun.RunID), "instance-one") || !strings.Contains(string(secondRun.RunID), "instance-two") {
		t.Fatalf("restart identities collided: first=%#v second=%#v", firstRun, secondRun)
	}
	if generated := New(nil, nil, nil, nil, "token-a", nil).workflowNonce; generated == "" {
		t.Fatal("production adapter generated no restart-unique workflow nonce")
	}
}

func TestWorkflowSurfaceResumePreservesIdempotencyAndTypedPayload(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, operations, operations, operations))
	payload, err := values.NewInline("approve", values.Metadata{Producer: values.Producer{Kind: "mcp", Reference: "session"}, MediaType: "text/plain", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	args := map[string]any{"run_id": "run-one", "wait_id": "wait-one", "correlation": "gate-one", "token": "one-time-token", "payload": payload, "idempotency_key": "resume-key"}
	for attempt := 0; attempt < 2; attempt++ {
		result, callErr := adapter.workflow.handleGate(t.Context(), args)
		if callErr != nil {
			t.Fatalf("resume attempt %d = %#v, %v", attempt, result, callErr)
		}
	}
	operations.mu.Lock()
	if len(operations.resumes) != 2 || operations.resumes[0].IdempotencyKey != "resume-key" || operations.resumes[1].IdempotencyKey != "resume-key" || operations.resumes[0].WakeSource != "gate" || operations.resumes[0].Token != "one-time-token" || operations.resumes[0].Payload.Inline != "approve" {
		t.Fatalf("resume requests = %#v", operations.resumes)
	}
	operations.mu.Unlock()

	messageArgs := map[string]any{"run_id": "run-two", "wait_id": "wait-two", "correlation": "message-one", "payload": payload}
	for attempt := 0; attempt < 2; attempt++ {
		credentialless, callErr := adapter.workflow.handleMessage(t.Context(), messageArgs)
		if callErr != nil {
			t.Fatalf("credentialless resume attempt %d = %#v, %v", attempt, credentialless, callErr)
		}
		if attempt == 1 {
			result, ok := credentialless.(appworkflow.ResumeWorkflowRunResult)
			if !ok || result.Outcome != appworkflow.WorkflowResumeReplayed {
				t.Fatalf("credentialless replay = %#v", credentialless)
			}
		}
	}
	operations.mu.Lock()
	if len(operations.resumes) != 4 || operations.resumes[2].Token != "" || operations.resumes[2].IdempotencyKey != "" || operations.resumes[2].WakeSource != "message" || operations.resumes[3].IdempotencyKey != "" {
		t.Fatalf("credentialless resume requests = %#v", operations.resumes)
	}
	operations.mu.Unlock()

	signalArgs := map[string]any{"run_id": "run-two", "name": "approved", "correlation": "signal-one", "payload": payload, "idempotency_key": "signal-key", "confirmed": true}
	signaled, signalErr := adapter.workflow.handleSignal(t.Context(), signalArgs)
	if signalErr != nil {
		t.Fatalf("typed signal = %#v, %v", signaled, signalErr)
	}
	operations.mu.Lock()
	defer operations.mu.Unlock()
	if len(operations.signals) != 1 || operations.signals[0].Payload.Inline != "approve" || operations.signals[0].IdempotencyKey != "signal-key" || !operations.signals[0].Confirmed {
		t.Fatalf("signal requests = %#v", operations.signals)
	}
}

func TestAdapterCanonicalizesConfiguredToken(t *testing.T) {
	if adapter := New(nil, nil, nil, nil, " non-reproducible ", nil); adapter.token != "" {
		t.Fatalf("adapter silently rewrote a non-canonical configured credential: %q", adapter.token)
	}
}

type fakeWorkflowExposure struct {
	mu          sync.Mutex
	generations map[string]uint64
	budgets     map[string]int
	direct      map[string][]appworkflow.WorkflowExposureDescriptor
	alpha       appworkflow.WorkflowExposureDescriptor
	beta        appworkflow.WorkflowExposureDescriptor
	lazy        appworkflow.WorkflowExposureDescriptor
	describeErr error
	directErr   error
}

func newFakeWorkflowExposure() *fakeWorkflowExposure {
	alpha := workflowTestDescriptor("team/alpha", graph.EffectCompute)
	beta := workflowTestDescriptor("team/beta", graph.EffectMutate)
	lazy := workflowTestDescriptor("team/lazy", graph.EffectRead)
	return &fakeWorkflowExposure{generations: map[string]uint64{"token-a": 1, "token-b": 1}, budgets: map[string]int{"token-a": 4, "token-b": 4}, direct: map[string][]appworkflow.WorkflowExposureDescriptor{"token-a": {alpha}, "token-b": {beta}}, alpha: alpha, beta: beta, lazy: lazy}
}

func (f *fakeWorkflowExposure) setGeneration(token string, generation uint64, direct []appworkflow.WorkflowExposureDescriptor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.generations[token], f.direct[token] = generation, direct
}

func (f *fakeWorkflowExposure) setDirectError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.directErr = err
}

func (f *fakeWorkflowExposure) setBudget(token string, budget int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.budgets[token] = budget
}

type workflowTestContextToken struct{}

func (f *fakeWorkflowExposure) ResolveSession(ctx context.Context, sessionID, token string) (context.Context, appworkflow.WorkflowExposureSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	generation, found := f.generations[token]
	if !found {
		return ctx, appworkflow.WorkflowExposureSession{SessionID: sessionID, Profile: hoststate.ExposureProfileRecord{ID: "default:meta-only", MaxDirectTools: 24, SearchScope: hoststate.ExposureSearchNone}}, nil
	}
	id := "principal:" + token
	session := appworkflow.WorkflowExposureSession{SessionID: sessionID, Authenticated: true, Principal: hoststate.MCPPrincipalSnapshot{Record: hoststate.MCPPrincipalRecord{ID: id}, Generation: generation}, Profile: hoststate.ExposureProfileRecord{ID: "profile:" + token, MaxDirectTools: f.budgets[token], SearchScope: hoststate.ExposureSearchAll, LazyLoad: true}, ProfileGeneration: generation}
	return context.WithValue(ctx, workflowTestContextToken{}, token), session, nil
}

func (f *fakeWorkflowExposure) DirectWorkflows(ctx context.Context, session appworkflow.WorkflowExposureSession) ([]appworkflow.WorkflowExposureDescriptor, error) {
	if !session.Authenticated {
		return []appworkflow.WorkflowExposureDescriptor{}, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.directErr != nil {
		return nil, f.directErr
	}
	return append([]appworkflow.WorkflowExposureDescriptor(nil), f.direct[ctx.Value(workflowTestContextToken{}).(string)]...), nil
}

func (f *fakeWorkflowExposure) Search(_ context.Context, _ appworkflow.WorkflowExposureSession, _ string, _ int) ([]appworkflow.WorkflowExposureSummary, error) {
	return []appworkflow.WorkflowExposureSummary{{Name: f.lazy.Name, Namespace: f.lazy.Namespace, Version: f.lazy.Version, Digest: f.lazy.Digest, Definition: f.lazy.Definition, Effects: f.lazy.Effects}}, nil
}

func (f *fakeWorkflowExposure) Load(_ context.Context, _ appworkflow.WorkflowExposureSession, refs []graph.DefinitionRef) ([]appworkflow.WorkflowExposureDescriptor, error) {
	result := make([]appworkflow.WorkflowExposureDescriptor, 0, len(refs))
	for _, ref := range refs {
		found := false
		for _, descriptor := range []appworkflow.WorkflowExposureDescriptor{f.alpha, f.beta, f.lazy} {
			if ref == descriptor.Definition {
				result = append(result, descriptor)
				found = true
				break
			}
		}
		if !found {
			return nil, appworkflow.ErrWorkflowHidden
		}
	}
	return result, nil
}

func (f *fakeWorkflowExposure) Describe(_ context.Context, _ appworkflow.WorkflowExposureSession, ref graph.DefinitionRef, _ string) (appworkflow.WorkflowExposureDescriptor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.describeErr != nil {
		return appworkflow.WorkflowExposureDescriptor{}, f.describeErr
	}
	for _, descriptor := range []appworkflow.WorkflowExposureDescriptor{f.alpha, f.beta, f.lazy} {
		if descriptor.Definition == ref {
			return descriptor, nil
		}
	}
	return appworkflow.WorkflowExposureDescriptor{}, appworkflow.ErrWorkflowHidden
}

func (*fakeWorkflowExposure) NamespaceCatalog(context.Context, appworkflow.WorkflowExposureSession) (map[string]int, error) {
	return map[string]int{"team": 3}, nil
}

func (*fakeWorkflowExposure) DisplayPolicy(_ context.Context, _ appworkflow.WorkflowExposureSession, requested values.DisplayPolicy) (values.DisplayPolicy, error) {
	if requested.RevealsPrivate() {
		return values.DisplayPolicy{}, appworkflow.ErrPolicyDenied
	}
	return values.DisplayPolicy{}, nil
}

func workflowTestDescriptor(name string, effect graph.Effect) appworkflow.WorkflowExposureDescriptor {
	digest := values.SHA256Digest([]byte(name))
	return appworkflow.WorkflowExposureDescriptor{ToolName: "workflow_" + stringsWorkflowTestReplace(name), Name: name, Namespace: "team", Version: "v1", Digest: digest, Definition: graph.DefinitionRef{Kind: "registry", ID: name, Version: "v1", Digest: digest}, InputSchema: graph.Schema{"type": "object", "additionalProperties": false, "properties": map[string]any{"message": map[string]any{"type": "string"}}, "required": []string{"message"}}, OutputSchema: graph.Schema{"type": "object", "properties": map[string]any{"result": map[string]any{"type": "string"}}}, Effects: graph.EffectSet{effect}}
}

type fakeWorkflowOperations struct {
	mu      sync.Mutex
	runs    []appworkflow.RunWorkflowRequest
	resumes []appworkflow.ResumeWorkflowRunRequest
	signals []appworkflow.SignalWorkflowRunRequest
}

type fakeWorkflowLifecycle struct {
	appworkflow.WorkflowLifecycleOperations
	search      func(context.Context, appworkflow.SearchWorkflowCatalogRequest) (appworkflow.WorkflowCatalogSearchResult, error)
	searchCalls int
	pinCalls    int
}

func (f *fakeWorkflowLifecycle) SearchWorkflowCatalog(ctx context.Context, request appworkflow.SearchWorkflowCatalogRequest) (appworkflow.WorkflowCatalogSearchResult, error) {
	f.searchCalls++
	if f.search == nil {
		return appworkflow.WorkflowCatalogSearchResult{}, nil
	}
	return f.search(ctx, request)
}

func (f *fakeWorkflowLifecycle) PinWorkflowExposure(_ context.Context, request appworkflow.MutateWorkflowExposureRequest) (hoststate.ExposureProfileSnapshot, error) {
	f.pinCalls++
	return hoststate.ExposureProfileSnapshot{Record: hoststate.ExposureProfileRecord{ID: request.ProfileID, MaxDirectTools: 4, SearchScope: hoststate.ExposureSearchAll}, Generation: request.ExpectedGeneration + 1}, nil
}

func (*fakeWorkflowOperations) ValidateWorkflow(context.Context, appworkflow.ValidateWorkflowRequest) (appworkflow.ValidateWorkflowResult, error) {
	return appworkflow.ValidateWorkflowResult{Diagnostics: []diagnostic.Diagnostic{}}, nil
}
func (*fakeWorkflowOperations) ExplainWorkflow(context.Context, appworkflow.ExplainWorkflowRequest) (appworkflow.StartRunResult, error) {
	return appworkflow.StartRunResult{}, nil
}
func (f *fakeWorkflowOperations) RunWorkflow(_ context.Context, request appworkflow.RunWorkflowRequest) (appworkflow.StartRunResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs = append(f.runs, request)
	return appworkflow.StartRunResult{}, nil
}
func (*fakeWorkflowOperations) InspectWorkflowRun(context.Context, appworkflow.InspectWorkflowRunRequest) (rundiagnostics.Result, error) {
	return rundiagnostics.Result{}, nil
}
func (*fakeWorkflowOperations) CancelWorkflowRun(context.Context, appworkflow.CancelWorkflowRunRequest) (appworkflow.CancelWorkflowRunResult, error) {
	return appworkflow.CancelWorkflowRunResult{}, nil
}
func (f *fakeWorkflowOperations) ResumeWorkflowRun(_ context.Context, request appworkflow.ResumeWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	outcome := appworkflow.WorkflowResumeApplied
	if len(f.resumes) != 0 {
		prior := f.resumes[len(f.resumes)-1]
		if request.IdempotencyKey != "" && prior.IdempotencyKey == request.IdempotencyKey || request.IdempotencyKey == "" && prior.RunID == request.RunID && prior.WaitID == request.WaitID && prior.Correlation == request.Correlation {
			outcome = appworkflow.WorkflowResumeReplayed
		}
	}
	f.resumes = append(f.resumes, request)
	return appworkflow.ResumeWorkflowRunResult{Outcome: outcome}, nil
}
func (*fakeWorkflowOperations) RerunWorkflow(context.Context, appworkflow.RerunWorkflowRequest) (appworkflow.RerunWorkflowResult, error) {
	return appworkflow.RerunWorkflowResult{}, nil
}
func (*fakeWorkflowOperations) ListWorkflowWaits(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowWaitListResult, error) {
	return appworkflow.WorkflowWaitListResult{}, nil
}
func (*fakeWorkflowOperations) FetchWorkflowValues(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowValueListResult, error) {
	return appworkflow.WorkflowValueListResult{}, nil
}
func (*fakeWorkflowOperations) FetchWorkflowEvents(context.Context, appworkflow.WorkflowRunReadRequest) (appworkflow.WorkflowEventListResult, error) {
	return appworkflow.WorkflowEventListResult{}, nil
}
func (f *fakeWorkflowOperations) SignalWorkflowRun(_ context.Context, request appworkflow.SignalWorkflowRunRequest) (appworkflow.ResumeWorkflowRunResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signals = append(f.signals, request)
	return appworkflow.ResumeWorkflowRunResult{}, nil
}

func stringsWorkflowTestReplace(value string) string {
	result := make([]rune, 0, len(value))
	for _, current := range value {
		if current == '/' {
			current = '_'
		}
		result = append(result, current)
	}
	return string(result)
}

func containsWorkflowTestText(value, search string) bool {
	for index := 0; index+len(search) <= len(value); index++ {
		if value[index:index+len(search)] == search {
			return true
		}
	}
	return false
}
