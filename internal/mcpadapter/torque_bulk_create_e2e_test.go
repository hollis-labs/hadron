package mcpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/artifacts"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/registry"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	calladapter "github.com/hollis-labs/go-workflow/adapters/call"
	workflowmcp "github.com/hollis-labs/go-workflow/adapters/mcp"
	"github.com/hollis-labs/go-workflow/adapters/transform"
	workflowcompile "github.com/hollis-labs/go-workflow/compile"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
	workflowwait "github.com/hollis-labs/go-workflow/wait"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// TestTorqueBulkCreateThroughPinnedWorkflowMCP is the Hadron-owned release
// proof for the graph-native Torque bulk-create workflow. The only simulated
// external dependency is a local protocol-compatible Torque MCP server.
func TestTorqueBulkCreateThroughPinnedWorkflowMCP(t *testing.T) {
	ctx := t.Context()
	fake := newTorqueCreateFake(t)
	trusted := newTrustedTorqueCaller(t, fake.url)
	kinds, mcpKind := torqueKinds(t, trusted, nil)

	root := filepath.Clean(filepath.Join("..", "..", "examples", "workflow"))
	source, err := os.ReadFile(filepath.Join(root, "torque-task-bulk-create.workflow.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	sourceRef := graph.DefinitionRef{
		Authority: "project", Kind: appworkflow.DefinitionKindFile, ID: "torque-task-bulk-create",
		Locator: "torque-task-bulk-create.workflow.yaml", Version: "1.0.0", Digest: values.SHA256Digest(source),
	}
	catalog, err := registry.OpenWorkflowIndex(filepath.Join(t.TempDir(), "workflow-catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := torqueResolver(t, root, catalog, kinds)
	contracts, err := appworkflow.NewContractRegistrationService(appworkflow.ContractRegistrationOptions{
		Definitions: resolver, StepKinds: kinds, Catalog: catalog,
		Authorizer: appworkflow.NamespaceAuthorizerFunc(func(_ context.Context, request appworkflow.NamespaceAuthorization) error {
			if request.Namespace != "torque" || request.Principal != "agent:torque/acceptance" {
				return errors.New("outside acceptance namespace")
			}
			return nil
		}),
		Attestor: torqueContractAttestor{},
		Policy:   appworkflow.ContractTestPolicy{MinimumCases: 1, Repetitions: 2, RequireEffectCoverage: true},
		Now:      func() time.Time { return torqueBaseTime },
	})
	if err != nil {
		t.Fatal(err)
	}
	validation, err := contracts.Validate(ctx, sourceRef)
	if err != nil || validation.Plan == nil || len(validation.Diagnostics) != 0 {
		t.Fatalf("validate Torque workflow = %#v, %v", validation, err)
	}
	plan := validation.Plan
	assertTorquePlan(t, plan, mcpKind)

	scaffold, err := contracts.GenerateContractScaffold(ctx, sourceRef)
	if err != nil {
		t.Fatal(err)
	}
	suite := torqueContractSuite(t, scaffold, plan)
	report, err := contracts.ExecuteContractTests(ctx, sourceRef, suite)
	if err != nil || !report.Passed || len(report.Cases) != 1 || !report.Cases[0].Passed {
		t.Fatalf("Torque contract report = %#v, %v", report, err)
	}
	record, err := contracts.Register(ctx, appworkflow.RegisterWorkflowRequest{
		Definition: sourceRef, Namespace: "torque", Principal: "agent:torque/acceptance", Report: report, MakeCurrent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	exact := registry.WorkflowQuery{Name: record.Name, Version: record.Version, Digest: record.Digest}
	if _, err = contracts.Pin(ctx, exact, "agent:torque/acceptance"); err != nil {
		t.Fatal(err)
	}
	if record.Name != "torque/torque-task-bulk-create" || record.PlanDigest != plan.Digest ||
		record.ContractSuiteDigest != report.SuiteDigest || record.ContractTestDigest != report.Digest ||
		record.Authority != "project" || record.Provenance.Digest != sourceRef.Digest {
		t.Fatalf("registered provenance = %#v", record)
	}

	dbPath := filepath.Join(t.TempDir(), "torque-e2e.db")
	store, state, journal, exposureStore := openTorqueStores(t, dbPath)
	artifactStore := torqueArtifactStore(t)
	exposure, token := configureTorqueExposure(t, exposureStore, catalog, resolver, kinds, record)
	registeredRef := graph.DefinitionRef{Kind: appworkflow.DefinitionKindRegistry, ID: record.Name, Version: record.Version, Digest: record.Digest}
	if _, resolveErr := resolver.ResolvePlan(ctx, registeredRef); resolveErr != nil {
		t.Fatalf("resolve registered Torque plan: %v", resolveErr)
	}
	clock := &torqueClock{current: torqueBaseTime.Add(time.Hour)}
	host := newTorqueHost(t, state, journal, resolver, kinds, trusted, artifactStore, clock)
	if startErr := host.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}

	planSource := appworkflow.PinnedRecoveryPlanSource{Roots: journal, State: state, Replays: state}
	diagnostics := rundiagnostics.Service{State: state, Plans: planSource, Control: state, Replay: state, Pins: state, Resources: state, Starts: journal}
	operator := torqueOperator(t, host, diagnostics, planSource, state)
	hadron := New(nil, nil, nil, nil, token, nil,
		withWorkflowInstanceNonceForTest("torque-e2e"),
		WithWorkflowServices(exposure, operator, operator, operator))
	hadronServer := server.NewTestServer(hadron.newServer())
	t.Cleanup(hadronServer.Close)
	transport := NewInternalCaller(&Adapter{}, WithExternalServers(map[string]ExternalServerConfig{
		"hadron-e2e": {Transport: "sse", URL: hadronServer.URL + "/sse", Headers: map[string]string{"Authorization": "Bearer " + token}},
	}))
	t.Cleanup(func() { _ = transport.Close() })

	description, session, err := exposure.ResolveSession(ctx, "preflight", token)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := exposure.DirectWorkflows(description, session)
	if err != nil || len(direct) != 1 {
		t.Fatalf("direct workflows = %#v, %v", direct, err)
	}
	descriptor := direct[0]
	if descriptor.ToolName != "workflow_torque_torque-task-bulk-create" || descriptor.Definition.Kind != appworkflow.DefinitionKindRegistry ||
		descriptor.Definition.ID != record.Name || descriptor.Definition.Version != record.Version || descriptor.Definition.Digest != record.Digest ||
		!reflect.DeepEqual(descriptor.Effects, graph.EffectSet{graph.EffectCompute, graph.EffectDestructive, graph.EffectMaterialize, graph.EffectMutate, graph.EffectRead}) {
		t.Fatalf("generated descriptor = %#v", descriptor)
	}
	assertTorqueExposureSchemas(t, descriptor)
	assertTorqueMountedToolSchemas(ctx, t, transport, "hadron-e2e", descriptor)

	tasks := []any{
		map[string]any{"title": "alpha", "description": "first"},
		map[string]any{"title": "beta", "description": "second"},
		map[string]any{"title": "retry-after-timeout", "description": "cached across timeout"},
		map[string]any{"title": "gamma", "description": "third"},
		map[string]any{"title": "terminal-failure", "description": "tolerated"},
		map[string]any{"title": "delta", "description": "fourth"},
	}
	started, err := transport.CallTool(ctx, "hadron-e2e", descriptor.ToolName, map[string]any{"project-id": "project-42", "tasks": tasks})
	if err != nil {
		t.Fatalf("MCP workflow start: %v", err)
	}
	handle := torqueResultMap(t, started)
	runID := workflowruntime.RunID(mustTorqueString(t, handle, "run_id"))
	if handle["status"] != string(workflowruntime.RunRunning) || runID == "" {
		t.Fatalf("workflow handle = %#v", handle)
	}

	startedRecord, err := journal.LoadStart(ctx, runID)
	if err != nil || startedRecord.Record.Plan.Digest != plan.Digest || startedRecord.Record.Plan.Definition.ID != plan.Definition.ID {
		t.Fatalf("durable pinned Torque start = %#v, %v", startedRecord, err)
	}
	recovery := torqueRecoveryPlan(t, &startedRecord.Record.Plan)
	runner := newTorqueRuntimeDriver(t, state, journal, host, recovery, trusted, clock)
	runner.driveUntilRetryWaiting(ctx, runID)
	if fake.uniqueCreates("project-42") != 5 {
		t.Fatalf("logical creates before restart = %d, want 5", fake.uniqueCreates("project-42"))
	}
	_ = transport.Close()
	hadronServer.Close()
	if shutdownErr := host.Shutdown(context.Background()); shutdownErr != nil {
		t.Fatal(shutdownErr)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}

	store, state, journal, exposureStore = openTorqueStores(t, dbPath)
	clock.advance(time.Hour)
	host = newTorqueHost(t, state, journal, resolver, kinds, trusted, artifactStore, clock)
	if startErr := host.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	t.Cleanup(func() { _ = host.Shutdown(context.Background()); _ = store.Close() })
	runner = newTorqueRuntimeDriver(t, state, journal, host, recovery, trusted, clock)
	runner.driveToTerminal(ctx, runID)
	reopenedExposure := newTorqueExposure(t, exposureStore, catalog, resolver, kinds)
	reopenedPlanSource := appworkflow.PinnedRecoveryPlanSource{Roots: journal, State: state, Replays: state}
	reopenedDiagnostics := rundiagnostics.Service{State: state, Plans: reopenedPlanSource, Control: state, Replay: state, Pins: state, Resources: state, Starts: journal}
	reopenedOperator := torqueOperator(t, host, reopenedDiagnostics, reopenedPlanSource, state)
	reopenedHadron := New(nil, nil, nil, nil, token, nil,
		withWorkflowInstanceNonceForTest("torque-e2e-reopened"),
		WithWorkflowServices(reopenedExposure, reopenedOperator, reopenedOperator, reopenedOperator))
	reopenedServer := server.NewTestServer(reopenedHadron.newServer())
	t.Cleanup(reopenedServer.Close)
	reopenedTransport := NewInternalCaller(&Adapter{}, WithExternalServers(map[string]ExternalServerConfig{
		"hadron-e2e-reopened": {Transport: "sse", URL: reopenedServer.URL + "/sse", Headers: map[string]string{"Authorization": "Bearer " + token}},
	}))
	t.Cleanup(func() { _ = reopenedTransport.Close() })

	run, err := state.LoadRun(ctx, runID)
	if err != nil || run.Status != workflowruntime.RunSucceeded || run.Outputs == nil {
		t.Fatalf("terminal run = %#v, %v", run, err)
	}
	outputs, err := state.LoadValues(ctx, *run.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	assertTorqueOutputs(t, outputs)
	assertTorqueDurableHistory(ctx, t, state, runID, fake)
	if fake.maxConcurrent() != 4 || fake.uniqueCreates("project-42") != 5 || fake.attempts("retry-after-timeout") != 2 || fake.attempts("terminal-failure") != 1 {
		t.Fatalf("fake observations max=%d unique=%d retry=%d failure=%d", fake.maxConcurrent(), fake.uniqueCreates("project-42"), fake.attempts("retry-after-timeout"), fake.attempts("terminal-failure"))
	}
	if violationErr := fake.protocolViolation(); violationErr != nil {
		t.Fatal(violationErr)
	}
	for _, task := range tasks {
		title := task.(map[string]any)["title"].(string)
		keys := fake.keyObservations(title)
		if len(keys) == 0 || keys[0] == "" {
			t.Fatalf("Torque item %q did not observe an MCP idempotency key: %#v", title, keys)
		}
		for _, key := range keys[1:] {
			if key != keys[0] {
				t.Fatalf("Torque item %q changed idempotency key across attempts: %#v", title, keys)
			}
		}
	}
	if keys := fake.keyObservations("retry-after-timeout"); len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("timeout retry did not reuse its exact pre-restart idempotency key: %#v", keys)
	}

	inspected, err := reopenedTransport.CallTool(ctx, "hadron-e2e-reopened", "hadron_workflow_run_inspect", map[string]any{"run_id": string(runID)})
	if err != nil {
		t.Fatal(err)
	}
	inspection := torqueResultMap(t, inspected)
	encoded, err := json.Marshal(inspection)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"project-42", "cached across timeout", "task-project-42"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("redacted MCP inspection leaked %q: %s", private, encoded)
		}
	}
	if !strings.Contains(string(encoded), values.RedactedMarker) || strings.Contains(string(encoded), "hadron/idempotencyKey") {
		t.Fatalf("inspection was not safely redacted: %s", encoded)
	}
}

func assertTorqueExposureSchemas(t *testing.T, descriptor appworkflow.WorkflowExposureDescriptor) {
	t.Helper()
	expectedInput := graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []string{"project-id", "tasks"},
		"properties": map[string]any{
			"project-id": map[string]any{"type": "string", "description": "Torque project receiving the tasks."},
			"tasks": map[string]any{
				"type": "array", "description": "Task records to create.",
				"items": map[string]any{
					"type": "object", "required": []any{"title"},
					"properties": map[string]any{
						"title": map[string]any{"type": "string"}, "description": map[string]any{"type": "string"},
					},
				},
			},
		},
	}
	expectedOutput := graph.Schema{
		"type": "object", "additionalProperties": false,
		"required": []string{"created", "failed", "count"},
		"properties": map[string]any{
			"created": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"failed":  map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
			"count":   map[string]any{"type": "integer"},
		},
	}
	assertTorqueJSONEqual(t, "generated input schema", descriptor.InputSchema, expectedInput)
	assertTorqueJSONEqual(t, "generated output schema", descriptor.OutputSchema, expectedOutput)
}

func assertTorqueMountedToolSchemas(ctx context.Context, t *testing.T, caller *InternalCaller, serverName string, descriptor appworkflow.WorkflowExposureDescriptor) {
	t.Helper()
	entry, _, err := caller.externalClient(ctx, normalizeServerName(serverName))
	if err != nil {
		t.Fatal(err)
	}
	listed, err := entry.client.ListTools(ctx, mcp.ListToolsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var mounted *mcp.Tool
	for index := range listed.Tools {
		if listed.Tools[index].Name == descriptor.ToolName {
			mounted = &listed.Tools[index]
			break
		}
	}
	if mounted == nil {
		t.Fatalf("first-class MCP tool %q was not mounted: %#v", descriptor.ToolName, listed)
	}
	annotations := mounted.Annotations
	if annotations.ReadOnlyHint == nil || *annotations.ReadOnlyHint || annotations.DestructiveHint == nil || !*annotations.DestructiveHint ||
		annotations.IdempotentHint == nil || *annotations.IdempotentHint || annotations.OpenWorldHint == nil || *annotations.OpenWorldHint {
		t.Fatalf("mounted MCP annotations = %#v", annotations)
	}
	var mountedInput any = mounted.InputSchema
	if len(mounted.RawInputSchema) != 0 {
		mountedInput = mounted.RawInputSchema
	}
	assertTorqueJSONEqual(t, "mounted MCP input schema", mountedInput, descriptor.InputSchema)
	var mountedOutput any = mounted.OutputSchema
	if len(mounted.RawOutputSchema) != 0 {
		mountedOutput = mounted.RawOutputSchema
	}
	encodedOutput, err := json.Marshal(mountedOutput)
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(encodedOutput)))
	decoder.UseNumber()
	if decodeErr := decoder.Decode(&output); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	properties, ok := output["properties"].(map[string]any)
	if !ok {
		t.Fatalf("mounted MCP output properties = %#v", output)
	}
	assertTorqueJSONEqual(t, "mounted MCP terminal output schema", properties["outputs"], descriptor.OutputSchema)
	if output["type"] != "object" || output["additionalProperties"] != false {
		t.Fatalf("mounted MCP run-handle schema = %#v", output)
	}
	required, err := json.Marshal(output["required"])
	if err != nil || string(required) != `["run_id","status"]` {
		t.Fatalf("mounted MCP run-handle required = %s, %v", required, err)
	}
}

func assertTorqueJSONEqual(t *testing.T, name string, got, want any) {
	t.Helper()
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var gotValue, wantValue any
	for _, item := range []struct {
		encoded []byte
		target  *any
	}{{gotJSON, &gotValue}, {wantJSON, &wantValue}} {
		decoder := json.NewDecoder(strings.NewReader(string(item.encoded)))
		decoder.UseNumber()
		if err := decoder.Decode(item.target); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("%s mismatch:\n got: %s\nwant: %s", name, gotJSON, wantJSON)
	}
}

var torqueBaseTime = time.Date(2026, time.August, 24, 18, 0, 0, 0, time.UTC)

type torqueClock struct {
	mu      sync.Mutex
	current time.Time
}

func (c *torqueClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = c.current.Add(time.Nanosecond)
	return c.current
}

func (c *torqueClock) advance(delta time.Duration) {
	c.mu.Lock()
	c.current = c.current.Add(delta)
	c.mu.Unlock()
}

type torqueContractAttestor struct{}

func (torqueContractAttestor) AttestContractReport(_ context.Context, digest string) (string, error) {
	return "torque-e2e:" + digest, nil
}

func (torqueContractAttestor) VerifyContractReport(_ context.Context, digest, attestation string) error {
	if digest == "" || attestation != "torque-e2e:"+digest {
		return errors.New("invalid Torque acceptance attestation")
	}
	return nil
}

type trustedTorqueCaller struct{ *InternalCaller }

func (c trustedTorqueCaller) DescribeTool(ctx context.Context, serverName, toolName string) (workflowmcp.ToolDescriptor, error) {
	description, err := c.InternalCaller.DescribeTool(ctx, serverName, toolName)
	if err != nil {
		return workflowmcp.ToolDescriptor{}, err
	}
	if serverName != "torque" || toolName != "torque_task_create" || description.Server != serverName || description.Tool != toolName {
		return workflowmcp.ToolDescriptor{}, errors.New("Torque descriptor authority mismatch")
	}
	description.Trusted = true
	return description, nil
}

func newTrustedTorqueCaller(t *testing.T, url string) trustedTorqueCaller {
	t.Helper()
	caller := NewInternalCaller(&Adapter{}, WithExternalServers(map[string]ExternalServerConfig{
		"torque": {Transport: "streamable_http", URL: url},
	}))
	t.Cleanup(func() { _ = caller.Close() })
	return trustedTorqueCaller{InternalCaller: caller}
}

func torqueKinds(t *testing.T, caller trustedTorqueCaller, provider transform.ContextProvider) (*stepkind.MemoryRegistry, *workflowmcp.Kind) {
	t.Helper()
	kinds := stepkind.NewRegistry()
	mcpKind, err := workflowmcp.Register(kinds, workflowmcp.Options{Client: caller, Descriptor: caller})
	if err != nil {
		t.Fatal(err)
	}
	var transformKind stepkind.StepKind = transform.New()
	if provider != nil {
		transformKind, err = transform.NewWithContextProvider(provider)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := kinds.Register(transformKind); err != nil {
		t.Fatal(err)
	}
	return kinds, mcpKind
}

func torqueResolver(t *testing.T, root string, catalog *registry.WorkflowIndex, kinds stepkind.Registry) *appworkflow.DefinitionResolver {
	t.Helper()
	resolver, err := appworkflow.NewDefinitionResolver(appworkflow.DefinitionResolverOptions{
		Roots: []string{root}, Registry: catalog,
		Authorizer: appworkflow.DefinitionAuthorizerFunc(func(context.Context, appworkflow.DefinitionAuthorization) error { return nil }),
		Compile:    appworkflow.DefinitionCompileOptions{StepKinds: kinds, SemanticRevision: "torque-e2e-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func assertTorquePlan(t *testing.T, plan *workflowcompile.ExecutionPlan, mcpKind *workflowmcp.Kind) {
	t.Helper()
	if plan.Graph.Namespace != "torque" || plan.Graph.Version != "1.0.0" || len(plan.Graph.Nodes) != 2 {
		t.Fatalf("compiled plan identity = %#v", plan.Graph)
	}
	create := plan.Graph.Nodes[0]
	if create.ID != "create" {
		create = plan.Graph.Nodes[1]
	}
	description, err := mcpKind.DescribeConfig(t.Context(), create.Config)
	if err != nil {
		t.Fatal(err)
	}
	if create.ForEach == nil || create.ForEach.MaxConcurrency != 4 || create.ForEach.Tolerate == nil || create.ForEach.Tolerate.Count != 1 ||
		create.Retry == nil || create.Retry.Attempts != 3 || create.Kind != workflowmcp.KindName ||
		!description.AnnotationsTrusted || description.Idempotency != graph.IdempotencyIntrinsic || !reflect.DeepEqual(description.Effects, graph.EffectSet{graph.EffectMutate}) {
		t.Fatalf("compiled create node/annotations = %#v / %#v", create, description)
	}
	arguments := create.Config["arguments"].(map[string]any)
	if arguments["project_id"] != "{{ inputs['project-id'] }}" || arguments["title"] != "{{ inputs.title }}" || arguments["description"] != "{{ inputs.description }}" {
		t.Fatalf("compiled MCP arguments = %#v", arguments)
	}
}

type torqueCreateFake struct {
	url string

	mu          sync.Mutex
	active      int
	peak        int
	started     int
	initialGate chan struct{}
	gateOnce    sync.Once
	byKey       map[string]torqueCreateOutcome
	callByTitle map[string]int
	keysByTitle map[string][]string
	violation   error
}

type torqueCreateOutcome struct {
	project     string
	title       string
	description string
	id          string
	failed      bool
}

func newTorqueCreateFake(t *testing.T) *torqueCreateFake {
	t.Helper()
	fake := &torqueCreateFake{
		initialGate: make(chan struct{}), byKey: make(map[string]torqueCreateOutcome),
		callByTitle: make(map[string]int), keysByTitle: make(map[string][]string),
	}
	readOnly, destructive, idempotent, openWorld := false, false, true, false
	tool := mcp.NewTool("torque_task_create",
		mcp.WithDescription("Create exactly one Torque task."),
		mcp.WithString("project_id", mcp.Required()),
		mcp.WithString("title", mcp.Required()),
		mcp.WithString("description"),
	)
	tool.Annotations = mcp.ToolAnnotation{
		Title: "Create Torque task", ReadOnlyHint: &readOnly, DestructiveHint: &destructive,
		IdempotentHint: &idempotent, OpenWorldHint: &openWorld,
	}
	mcpServer := server.NewMCPServer("torque-e2e-fake", "1.0.0", server.WithToolCapabilities(true))
	mcpServer.AddTool(tool, fake.create)
	testServer := server.NewTestStreamableHTTPServer(mcpServer, server.WithStateLess(true))
	fake.url = testServer.URL
	t.Cleanup(testServer.Close)
	return fake
}

func (f *torqueCreateFake) create(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	project := request.GetString("project_id", "")
	title := request.GetString("title", "")
	description := request.GetString("description", "")
	if project == "" || title == "" {
		return mcp.NewToolResultError("project_id and title are required"), nil
	}
	key := ""
	if request.Params.Meta != nil {
		key, _ = request.Params.Meta.AdditionalFields["hadron/idempotencyKey"].(string)
	}
	f.mu.Lock()
	f.active++
	f.started++
	gateParticipant := f.started <= 4
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	if f.active > f.peak {
		f.peak = f.active
	}
	if f.active == 4 {
		f.gateOnce.Do(func() { close(f.initialGate) })
	}
	f.callByTitle[title]++
	f.keysByTitle[title] = append(f.keysByTitle[title], key)
	call := f.callByTitle[title]
	if key == "" {
		f.violation = errors.New("Torque create call omitted hadron/idempotencyKey")
		f.mu.Unlock()
		return mcp.NewToolResultError("idempotency key is required"), nil
	}
	outcome, exists := f.byKey[key]
	if exists && (outcome.project != project || outcome.title != title) {
		f.violation = fmt.Errorf("idempotency key %q was replayed for conflicting intent", key)
		f.mu.Unlock()
		return mcp.NewToolResultError("idempotency key intent conflict"), nil
	}
	if !exists {
		outcome = torqueCreateOutcome{project: project, title: title, description: description, failed: title == "terminal-failure"}
		if !outcome.failed {
			outcome.id = "task-" + project + "-" + strings.ReplaceAll(title, " ", "-")
		}
		f.byKey[key] = outcome
	}
	f.mu.Unlock()
	if gateParticipant {
		select {
		case <-f.initialGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if title == "retry-after-timeout" && call == 1 {
		// The external logical create is already committed and keyed before the
		// response is delayed beyond the workflow call timeout. The durable retry
		// must reuse that same key/result after the Host and SQLite handle reopen.
		timer := time.NewTimer(400 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	} else {
		timer := time.NewTimer(35 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if outcome.failed {
		return mcp.NewToolResultError("fixture rejected one task"), nil
	}
	return &mcp.CallToolResult{
		StructuredContent: map[string]any{"id": outcome.id, "project_id": outcome.project, "title": outcome.title, "description": outcome.description},
		Content:           []mcp.Content{mcp.TextContent{Type: mcp.ContentTypeText, Text: `{"created":true}`}},
	}, nil
}

func (f *torqueCreateFake) attempts(title string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callByTitle[title]
}

func (f *torqueCreateFake) uniqueCreates(project string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := 0
	for _, outcome := range f.byKey {
		if outcome.project == project && !outcome.failed {
			result++
		}
	}
	return result
}

func (f *torqueCreateFake) keyObservations(title string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.keysByTitle[title]...)
}

func (f *torqueCreateFake) protocolViolation() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.violation
}

func (f *torqueCreateFake) maxConcurrent() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak
}

func torqueContractSuite(t *testing.T, scaffold appworkflow.WorkflowContractSuite, plan *workflowcompile.ExecutionPlan) appworkflow.WorkflowContractSuite {
	t.Helper()
	if len(scaffold.Cases) != 1 || len(scaffold.Cases[0].Mocks) != 2 {
		t.Fatalf("Torque scaffold = %#v", scaffold)
	}
	contractRunID := workflowruntime.RunID("contract-" + values.SHA256Digest([]byte(plan.Digest + "\x00contract-success"))[7:31])
	project := torqueInline(t, "project-contract", "contract-input", plan.ID, "project-id")
	tasks := torqueInline(t, []any{map[string]any{"title": "contract", "description": "qualification"}}, "contract-input", plan.ID, "tasks")
	created := torqueInline(t, []any{"task-contract"}, "contract-output", plan.ID, "created")
	failed := torqueInline(t, []any{}, "contract-output", plan.ID, "failed")
	count := torqueInline(t, json.Number("1"), "contract-output", plan.ID, "count")
	resultJSON := torqueInline(t, map[string]any{"id": "task-contract", "project_id": "project-contract", "title": "contract", "description": "qualification"}, "executor-mock", "create", workflowmcp.OutputStructured)
	metadata := torqueInline(t, map[string]any{"server": "torque", "tool": "torque_task_create"}, "executor-mock", "create", workflowmcp.OutputMetadata)

	current := &scaffold.Cases[0]
	current.Name, current.Editable = "contract-success", false
	current.Inputs = values.ValueSet{"project-id": project, "tasks": tasks}
	current.ExpectedOutputs = values.ValueSet{"created": created, "failed": failed, "count": count}
	current.ExpectedEffects = graph.EffectSet{graph.EffectCompute, graph.EffectDestructive, graph.EffectMaterialize, graph.EffectMutate, graph.EffectRead}
	current.ExpectedCalls = []appworkflow.ContractToolCall{{
		NodeID: "create", Kind: "torque", Name: "torque_task_create",
		Arguments: map[string]any{"project_id": "project-contract", "title": "contract", "description": "qualification"},
		Effect:    graph.EffectMutate, Outcome: "succeeded",
	}}
	for index := range current.Mocks {
		mock := &current.Mocks[index]
		mock.ExpectedInputsEditable = false
		switch mock.NodeID {
		case "create":
			iteration := workflowruntime.FanOutIteration(0)
			child := workflowruntime.NodeInvocationID{RunID: contractRunID, NodeID: "create", Iteration: iteration}
			itemMetadata := values.Metadata{Producer: values.Producer{Kind: "fanout-item", Reference: string(contractRunID) + "/create"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun}
			item, err := values.NewInline(map[string]any{"title": "contract", "description": "qualification"}, itemMetadata)
			if err != nil {
				t.Fatal(err)
			}
			ordinal, err := values.NewInline(json.Number("0"), itemMetadata)
			if err != nil {
				t.Fatal(err)
			}
			reference := torqueControlIdentity(child)
			title := torqueInline(t, "contract", "node_input", reference, "title")
			description := torqueInline(t, "qualification", "node_input", reference, "description")
			expected := values.ValueSet{"item": item, "index": ordinal, "project-id": project, "title": title, "description": description}
			mock.ExpectedInputs = values.ValueSet{}
			mock.Results = []appworkflow.ContractMockResult{{
				Iteration: iteration, Attempt: 1, ExpectedInputs: &expected,
				Outputs: values.ValueSet{workflowmcp.OutputStructured: resultJSON, workflowmcp.OutputMetadata: metadata},
				Calls:   append([]appworkflow.ContractToolCall(nil), current.ExpectedCalls...),
			}}
		case "summarize":
			mock.ExpectedInputs = values.ValueSet{}
			mock.Results = []appworkflow.ContractMockResult{{Attempt: 1, Outputs: values.ValueSet{"created": created, "failed": failed, "count": count}}}
		default:
			t.Fatalf("unexpected Torque mock %q", mock.NodeID)
		}
	}
	return scaffold
}

func torqueInline(t *testing.T, payload any, kind, reference, output string) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{
		Producer: values.Producer{Kind: kind, Reference: reference, Output: output}, MediaType: "application/json",
		Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func torqueControlIdentity(id workflowruntime.NodeInvocationID) string {
	return fmt.Sprintf("%d:%s%d:%s%d:%s", len(id.RunID), id.RunID, len(id.NodeID), id.NodeID, len(id.Iteration), id.Iteration)
}

func openTorqueStores(t *testing.T, path string) (*persistence.Store, *persistence.WorkflowStateStore, *persistence.WorkflowHostStore, *persistence.WorkflowExposureStore) {
	t.Helper()
	store, err := persistence.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := persistence.NewWorkflowHostStore(store)
	if err != nil {
		t.Fatal(err)
	}
	exposure, err := persistence.NewWorkflowExposureStore(store)
	if err != nil {
		t.Fatal(err)
	}
	return store, state, journal, exposure
}

func torqueArtifactStore(t *testing.T) values.ArtifactStore {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, err := artifacts.New(root, values.ArtifactAuthorizerFunc(func(context.Context, values.ArtifactAuthorization) error { return nil }), nil)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func configureTorqueExposure(t *testing.T, store *persistence.WorkflowExposureStore, catalog *registry.WorkflowIndex, resolver *appworkflow.DefinitionResolver, kinds stepkind.Registry, record registry.WorkflowRecord) (*appworkflow.WorkflowExposureService, string) {
	t.Helper()
	exposure := newTorqueExposure(t, store, catalog, resolver, kinds)
	profileID := "torque-e2e-profile"
	_, err := exposure.PutProfile(t.Context(), hoststate.ExposureProfileRecord{
		ID:             profileID,
		Pins:           []graph.DefinitionRef{{Kind: appworkflow.DefinitionKindRegistry, ID: record.Name, Version: record.Version, Digest: record.Digest}},
		MaxDirectTools: 1, SearchScope: hoststate.ExposureSearchNone,
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	token := "torque-e2e-local-token"
	target := hoststate.ExecutionTarget{
		Version: hoststate.ScopeTargetVersionV1, ID: "torque-e2e-local", Kind: hoststate.ExecutionTargetLocal,
		Capabilities: []string{"mcp.client"}, Sandbox: hoststate.SandboxPolicy{Mode: hoststate.SandboxHostDefault},
		Readiness:  hoststate.TargetReadiness{State: hoststate.TargetReady, CheckedAt: torqueBaseTime},
		Provenance: hoststate.TargetProvenance{Authority: "hadron", Reference: "torque-e2e-local"},
	}
	principal := hoststate.MCPPrincipalRecord{
		ID: "agent:torque/acceptance", ProfileID: profileID,
		Identity: hoststate.IdentityBinding{
			Principal: "agent:torque/acceptance", SourceAuthority: "mcp", Trust: "project",
			Grants:          []string{"workflow.run"},
			RunScope:        hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeProject, ID: "torque-acceptance"},
			ExecutionTarget: &target,
		},
	}
	if _, err = exposure.PutPrincipal(t.Context(), appworkflow.PutMCPPrincipalRequest{Record: principal, Token: token}); err != nil {
		t.Fatal(err)
	}
	return exposure, token
}

func newTorqueExposure(t *testing.T, store *persistence.WorkflowExposureStore, catalog *registry.WorkflowIndex, resolver *appworkflow.DefinitionResolver, kinds stepkind.Registry) *appworkflow.WorkflowExposureService {
	t.Helper()
	exposure, err := appworkflow.NewWorkflowExposureService(appworkflow.WorkflowExposureOptions{
		Store: store, Catalog: catalog, Definitions: resolver, StepKinds: kinds,
	})
	if err != nil {
		t.Fatal(err)
	}
	return exposure
}

type torqueActivationScheduler struct{}

func (torqueActivationScheduler) Schedule(context.Context, workflowwait.Activation) error { return nil }
func (torqueActivationScheduler) Cancel(context.Context, workflowwait.ActivationID) error { return nil }

type torqueChildRuns struct{}

func (torqueChildRuns) MaterializeChildRun(context.Context, calladapter.ChildRunRequest) error {
	return errors.New("Torque acceptance workflow unexpectedly materialized a child run")
}

type torqueRetryAuthorizer struct{}

func (torqueRetryAuthorizer) AuthorizeRetry(_ context.Context, request workflowruntime.RetryAuthorizationRequest) error {
	if request.Node.ID != "create" || request.IdempotencyKey == "" {
		return errors.New("retry is outside the exact Torque create node")
	}
	return nil
}

func newTorqueHost(t *testing.T, state *persistence.WorkflowStateStore, journal *persistence.WorkflowHostStore, resolver *appworkflow.DefinitionResolver, buildKinds stepkind.Registry, caller trustedTorqueCaller, artifactStore values.ArtifactStore, clock *torqueClock) *appworkflow.Host {
	t.Helper()
	mcpKind, ok := buildKinds.Lookup(workflowmcp.KindName, workflowmcp.KindVersion)
	if !ok {
		t.Fatal("compiled MCP kind is unavailable")
	}
	provider := transform.ContextProviderFunc(func(ctx context.Context, invocation stepkind.Invocation) (values.ExpressionContext, error) {
		start, err := journal.LoadStart(ctx, workflowruntime.RunID(invocation.Identity.RunID))
		if err != nil {
			return values.ExpressionContext{}, err
		}
		expression, err := workflowruntime.BuildExpressionContext(ctx, state, state, start.Record.Plan.Graph, workflowruntime.RunID(invocation.Identity.RunID))
		if err != nil {
			return values.ExpressionContext{}, err
		}
		inferred := workflowcompile.InferValueDependencies(&start.Record.Plan, resolver.RecoveryDependencyOptions())
		if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
			return values.ExpressionContext{}, errors.New("Torque visibility inference failed")
		}
		scoped, _, err := inferred.Visibility.ScopeNodeContext(invocation.Identity.NodeID, expression, values.ExpressionOptions{})
		return scoped, err
	})
	transformKind, err := transform.NewWithContextProvider(provider)
	if err != nil {
		t.Fatal(err)
	}
	host, err := appworkflow.New(appworkflow.Options{
		State: state, Journal: journal, Definitions: resolver, Identity: appworkflow.MCPIdentityProvider{},
		Policy: appworkflow.PolicyEvaluatorFunc(func(_ context.Context, facts hoststate.PolicyFacts) (hoststate.PolicyDecision, error) {
			if facts.Operation != "start" || facts.Identity.Principal != "agent:torque/acceptance" ||
				!reflect.DeepEqual(facts.Effects, graph.EffectSet{graph.EffectCompute, graph.EffectDestructive, graph.EffectMaterialize, graph.EffectMutate, graph.EffectRead}) ||
				!reflect.DeepEqual(facts.RequiredCapabilities, []string{"mcp.client"}) {
				return hoststate.PolicyDecision{}, errors.New("unexpected Torque policy facts")
			}
			return hoststate.PolicyDecision{Outcome: hoststate.PolicyAllow, Reason: "allow pinned Torque acceptance"}, nil
		}),
		Kinds:         []stepkind.StepKind{mcpKind, transformKind},
		RequiredKinds: []appworkflow.KindRef{{Name: workflowmcp.KindName, Version: workflowmcp.KindVersion}, {Name: transform.Name, Version: transform.Version}},
		Activations:   torqueActivationScheduler{}, Artifacts: artifactStore, Clock: clock,
		RecoveryInterval: time.Hour, RecoveryBatchLimit: 100,
		RecoveryRetryAuthorizer: torqueRetryAuthorizer{}, ChildRuns: torqueChildRuns{},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = caller // The exact caller is retained by the registered MCP kind.
	return host
}

func torqueOperator(t *testing.T, host *appworkflow.Host, diagnostics rundiagnostics.Service, plans appworkflow.PinnedRecoveryPlanSource, state *persistence.WorkflowStateStore) *appworkflow.WorkflowOperator {
	t.Helper()
	replay := &workflowruntime.ReplayService{Store: state, Replay: state, Inputs: state, Control: state, Plans: plans, Registry: host.Registry()}
	operator, err := appworkflow.NewWorkflowOperator(appworkflow.WorkflowOperatorOptions{Host: host, Diagnostics: diagnostics, Replay: replay})
	if err != nil {
		t.Fatal(err)
	}
	return operator
}

func torqueRecoveryPlan(t *testing.T, plan *workflowcompile.ExecutionPlan) workflowruntime.RecoveryPlan {
	t.Helper()
	inferred := workflowcompile.InferValueDependencies(plan, workflowcompile.DependencyOptions{})
	if inferred.Plan == nil || len(inferred.Diagnostics) != 0 {
		t.Fatalf("Torque recovery inference = %#v", inferred.Diagnostics)
	}
	result := workflowruntime.RecoveryPlan{
		Ref:  workflowruntime.PlanRef{ID: plan.ID, Version: plan.Graph.Version, Digest: plan.Digest, SchemaVersion: plan.SchemaVersion},
		Plan: *plan, Visibility: inferred.Visibility,
	}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	return result
}

type torqueRuntimeDriver struct {
	t        *testing.T
	state    *persistence.WorkflowStateStore
	journal  *persistence.WorkflowHostStore
	host     *appworkflow.Host
	plan     workflowruntime.RecoveryPlan
	clock    *torqueClock
	queue    *workflowruntime.ReadyQueueCoordinator
	dispatch *workflowruntime.StepDispatcher
	claims   int
}

func newTorqueRuntimeDriver(t *testing.T, state *persistence.WorkflowStateStore, journal *persistence.WorkflowHostStore, host *appworkflow.Host, plan workflowruntime.RecoveryPlan, _ trustedTorqueCaller, clock *torqueClock) *torqueRuntimeDriver {
	t.Helper()
	dispatcher, err := workflowruntime.NewStepDispatcher(workflowruntime.DispatcherOptions{
		Store: state, Registry: host.Registry(), Now: clock.Now, Verifiers: host.Verifiers(),
		RetryCoordinator: &workflowruntime.RetryCoordinator{
			Store: state, Scheduler: torqueActivationScheduler{},
			Evaluator: workflowruntime.RetryEvaluator{Authorizer: torqueRetryAuthorizer{}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &torqueRuntimeDriver{t: t, state: state, journal: journal, host: host, plan: plan, clock: clock, queue: workflowruntime.NewReadyQueueCoordinator(state, nil), dispatch: dispatcher}
}

func (d *torqueRuntimeDriver) driveUntilRetryWaiting(ctx context.Context, runID workflowruntime.RunID) {
	d.t.Helper()
	for pass := 0; pass < 50; pass++ {
		d.progressNodes(ctx, runID)
		d.progressFanOut(ctx, runID)
		claims := d.claimBatch(ctx, runID, 16)
		if len(claims) != 0 {
			d.dispatchBatch(ctx, claims)
		}
		fanOut, err := d.state.LoadFanOut(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "create"})
		if err == nil {
			waitingRetry := false
			settled := 0
			for _, item := range fanOut.Items {
				node, loadErr := d.state.LoadNodeInvocation(ctx, item.Invocation)
				if loadErr != nil {
					d.t.Fatal(loadErr)
				}
				if node.Status == workflowruntime.NodeWaiting && node.LatestAttempt == 1 {
					waitingRetry = true
				}
				if node.Status.Terminal() {
					settled++
				}
			}
			if waitingRetry && settled == len(fanOut.Items)-1 {
				activations, loadErr := d.state.RecoverRetryActivations(ctx, workflowruntime.RetryActivationQuery{RunID: runID})
				if loadErr != nil || len(activations) != 1 || activations[0].Attempt.Number != 1 {
					d.t.Fatalf("durable retry before restart = %#v, %v", activations, loadErr)
				}
				return
			}
		}
		if len(claims) == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	run, _ := d.state.LoadRun(ctx, runID)
	fanOut, fanOutErr := d.state.LoadFanOut(ctx, workflowruntime.NodeInvocationID{RunID: runID, NodeID: "create"})
	children := make([]workflowruntime.NodeInvocationSnapshot, 0, len(fanOut.Items))
	for _, item := range fanOut.Items {
		child, _ := d.state.LoadNodeInvocation(ctx, item.Invocation)
		children = append(children, child)
	}
	d.t.Fatalf("Torque runtime did not reach the restart fence: run=%#v fanout=%#v err=%v children=%#v", run, fanOut, fanOutErr, children)
}

func (d *torqueRuntimeDriver) driveToTerminal(ctx context.Context, runID workflowruntime.RunID) {
	d.t.Helper()
	for pass := 0; pass < 200; pass++ {
		run, err := d.state.LoadRun(ctx, runID)
		if err != nil {
			d.t.Fatal(err)
		}
		if run.Status.Terminal() {
			return
		}
		d.activateRetries(ctx, runID)
		d.progressNodes(ctx, runID)
		d.progressFanOut(ctx, runID)
		claims := d.claimBatch(ctx, runID, 16)
		if len(claims) != 0 {
			d.dispatchBatch(ctx, claims)
			continue
		}
		d.completeRun(ctx, runID)
	}
	d.t.Fatal("Torque runtime exceeded the bounded terminal driver")
}

func (d *torqueRuntimeDriver) progressNodes(ctx context.Context, runID workflowruntime.RunID) {
	run, err := d.state.LoadRun(ctx, runID)
	if err != nil {
		d.t.Fatal(err)
	}
	expression, err := workflowruntime.BuildExpressionContext(ctx, d.state, d.state, d.plan.Plan.Graph, runID)
	if err != nil {
		d.t.Fatal(err)
	}
	driver := workflowruntime.NodeDriver{Store: d.state, Inputs: d.state, Control: d.state, Registry: d.host.Registry()}
	for _, planned := range d.plan.Plan.Graph.Nodes {
		node := d.exactNode(planned.ID)
		id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: node.ID}
		before, loadErr := d.state.LoadNodeInvocation(ctx, id)
		if loadErr != nil {
			d.t.Fatal(loadErr)
		}
		if before.Status != workflowruntime.NodePending && before.Status != workflowruntime.NodeBlocked {
			continue
		}
		_, driveErr := driver.Drive(ctx, workflowruntime.DriveNodeRequest{Run: run, Plan: d.plan, InvocationID: id, Node: node, ExpressionContext: expression, At: d.clock.Now()})
		if driveErr != nil && !errors.Is(driveErr, workflowruntime.ErrControlFlowPending) {
			d.t.Fatalf("drive node %s: %v", node.ID, driveErr)
		}
	}
}

func (d *torqueRuntimeDriver) progressFanOut(ctx context.Context, runID workflowruntime.RunID) {
	node := d.exactNode("create")
	id := workflowruntime.NodeInvocationID{RunID: runID, NodeID: node.ID}
	snapshot, err := d.state.LoadNodeInvocation(ctx, id)
	if err != nil {
		d.t.Fatal(err)
	}
	coordinator := workflowruntime.FanOutCoordinator{Store: d.state}
	if snapshot.Status == workflowruntime.NodeReady {
		expression, buildErr := workflowruntime.BuildExpressionContext(ctx, d.state, d.state, d.plan.Plan.Graph, runID)
		if buildErr != nil {
			d.t.Fatal(buildErr)
		}
		scoped, options, scopeErr := d.plan.Visibility.ScopeNodeContext(node.ID, expression, values.ExpressionOptions{})
		if scopeErr != nil {
			d.t.Fatal(scopeErr)
		}
		if _, expandErr := coordinator.Expand(ctx, workflowruntime.FanOutExpandCommand{
			Parent: id, ExpectedParentGeneration: snapshot.Generation, Spec: *node.ForEach, InputBindings: node.InputBindings,
			ExpressionContext: scoped, ExpressionOptions: options, Priority: snapshot.Priority, At: d.clock.Now(),
		}); expandErr != nil && !errors.Is(expandErr, workflowruntime.ErrCASMismatch) {
			d.t.Fatal(expandErr)
		}
		return
	}
	if snapshot.Status != workflowruntime.NodeWaiting {
		return
	}
	if _, _, _, collectErr := coordinator.Collect(ctx, id, d.clock.Now()); collectErr != nil &&
		!errors.Is(collectErr, workflowruntime.ErrFanOutIncomplete) && !errors.Is(collectErr, workflowruntime.ErrCASMismatch) {
		d.t.Fatal(collectErr)
	}
}

func (d *torqueRuntimeDriver) claimBatch(ctx context.Context, runID workflowruntime.RunID, limit int) []workflowruntime.ReadyClaim {
	result := make([]workflowruntime.ReadyClaim, 0, limit)
	for len(result) < limit {
		d.claims++
		at := d.clock.Now()
		claim, ok, err := d.queue.ClaimNext(ctx, workflowruntime.ReadyClaimRequest{
			RunID: runID, Owner: fmt.Sprintf("torque-worker-%d", d.claims), Token: fmt.Sprintf("torque-lease-%d", d.claims),
			IdempotencyKey: fmt.Sprintf("torque-claim-%d", d.claims), Now: at, LeaseUntil: at.Add(time.Minute),
		})
		if err != nil {
			d.t.Fatal(err)
		}
		if !ok {
			break
		}
		result = append(result, claim)
	}
	return result
}

func (d *torqueRuntimeDriver) dispatchBatch(ctx context.Context, claims []workflowruntime.ReadyClaim) {
	var wg sync.WaitGroup
	errs := make(chan error, len(claims))
	for _, claim := range claims {
		claim := claim
		wg.Add(1)
		go func() {
			defer wg.Done()
			node := d.exactNode(claim.Candidate.InvocationID.NodeID)
			if node.ForEach != nil && claim.Candidate.InvocationID.Iteration == "" {
				errs <- errors.New("fan-out aggregate was claimed")
				return
			}
			key, err := d.idempotencyKey(ctx, node, claim.Candidate.InvocationID)
			if err != nil {
				errs <- err
				return
			}
			result, err := d.dispatch.Dispatch(ctx, workflowruntime.DispatchRequest{Claim: claim, Node: node, IdempotencyKey: key})
			if strings.Contains(key, "retry-after-timeout") && result.Node.LatestAttempt == 1 && result.Node.Status != workflowruntime.NodeWaiting {
				detail := fmt.Errorf("timeout retry was not scheduled: node=%#v executor=%#v", result.Node, result.Attempt.Executor)
				errs <- errors.Join(detail, err)
				return
			}
			if err != nil && result.Node.Generation == 0 {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		d.t.Fatal(err)
	}
}

func (d *torqueRuntimeDriver) idempotencyKey(ctx context.Context, node graph.Node, invocation workflowruntime.NodeInvocationID) (string, error) {
	if node.Idempotency == nil || node.Idempotency.Mode != graph.IdempotencyKeyed || node.Idempotency.Key == nil {
		return "", nil
	}
	available, err := workflowruntime.BuildExpressionContext(ctx, d.state, d.state, d.plan.Plan.Graph, invocation.RunID)
	if err != nil {
		return "", err
	}
	if invocation.Iteration != "" {
		fanOut, loadErr := d.state.LoadFanOut(ctx, workflowruntime.NodeInvocationID{RunID: invocation.RunID, NodeID: invocation.NodeID})
		if loadErr != nil {
			return "", loadErr
		}
		matched := false
		for _, item := range fanOut.Items {
			if item.Invocation != invocation {
				continue
			}
			inputs, valuesErr := d.state.LoadValues(ctx, item.Inputs)
			if valuesErr != nil {
				return "", valuesErr
			}
			itemValue, ok := inputs[fanOut.ItemName]
			if !ok {
				return "", errors.New("durable fan-out item is missing")
			}
			index := item.Index
			available.Item, available.Index, matched = &itemValue, &index, true
			break
		}
		if !matched {
			return "", errors.New("fan-out invocation is absent from its durable expansion")
		}
	}
	scoped, options, err := d.plan.Visibility.ScopeNodeContext(node.ID, available, values.ExpressionOptions{})
	if err != nil {
		return "", err
	}
	raw, err := values.NewExpressionEngine().EvaluateRaw(*node.Idempotency.Key, scoped, options)
	if err != nil {
		return "", err
	}
	key, ok := raw.(string)
	if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(key) != key {
		return "", errors.New("Torque idempotency expression did not produce canonical non-empty text")
	}
	return key, nil
}

func (d *torqueRuntimeDriver) activateRetries(ctx context.Context, runID workflowruntime.RunID) {
	activations, err := d.state.RecoverRetryActivations(ctx, workflowruntime.RetryActivationQuery{RunID: runID, DueBefore: d.clock.Now()})
	if err != nil {
		d.t.Fatal(err)
	}
	for _, activation := range activations {
		node, loadErr := d.state.LoadNodeInvocation(ctx, activation.Attempt.Invocation)
		if loadErr != nil {
			d.t.Fatal(loadErr)
		}
		if _, activateErr := d.state.ActivateNodeRetry(ctx, workflowruntime.ActivateNodeRetryRequest{
			ActivationID: activation.ID, ExpectedActivationGeneration: activation.Generation, ExpectedNodeGeneration: node.Generation,
			IdempotencyKey: "torque-recover:" + activation.ID, Now: activation.FireAt,
		}); activateErr != nil && !errors.Is(activateErr, workflowruntime.ErrCASMismatch) {
			d.t.Fatal(activateErr)
		}
	}
}

func (d *torqueRuntimeDriver) completeRun(ctx context.Context, runID workflowruntime.RunID) {
	run, err := d.state.LoadRun(ctx, runID)
	if err != nil {
		d.t.Fatal(err)
	}
	completed, _, err := workflowruntime.NewControlFlowCoordinator(d.state, d.state, nil).ReconcileRunCompletion(ctx, d.plan.Plan.Graph, runID, "torque-complete:"+string(runID), d.clock.Now())
	if errors.Is(err, workflowruntime.ErrControlFlowPending) {
		return
	}
	if !errors.Is(err, workflowruntime.ErrRunOutputsPending) {
		if err != nil && !errors.Is(err, workflowruntime.ErrCASMismatch) {
			d.t.Fatal(err)
		}
		return
	}
	start, err := d.journal.LoadStart(ctx, runID)
	if err != nil {
		d.t.Fatal(err)
	}
	expression, err := workflowruntime.BuildExpressionContext(ctx, d.state, d.state, d.plan.Plan.Graph, runID)
	if err != nil {
		d.t.Fatal(err)
	}
	finalized, err := workflowruntime.FinalizeRunOutputs(ctx, d.state, workflowruntime.FinalizeRunRequest{
		BoundRun: start.Record.Run, Run: completed, Plan: &d.plan.Plan, Context: expression, Control: d.state, At: d.clock.Now(),
	})
	if err != nil {
		d.t.Fatal(err)
	}
	if len(finalized.Diagnostics) != 0 {
		d.t.Fatalf("finalize Torque outputs = %#v", finalized.Diagnostics)
	}
	_ = run
}

func (d *torqueRuntimeDriver) exactNode(id string) graph.Node {
	d.t.Helper()
	node := torqueNode(d.plan.Plan.Graph, id)
	_, spec, err := stepkind.Resolve(d.host.Registry(), node.Kind, node.KindVersion)
	if err != nil {
		d.t.Fatal(err)
	}
	node.Kind, node.KindVersion = spec.Name, spec.Version
	return node
}

func torqueNode(workflow graph.Graph, id string) graph.Node {
	for _, node := range workflow.Nodes {
		if node.ID == id {
			return node
		}
	}
	return graph.Node{}
}

func torqueResultMap(t *testing.T, result any) map[string]any {
	t.Helper()
	wrapped, ok := result.(execution.MCPToolResult)
	if !ok {
		t.Fatalf("MCP result type = %T", result)
	}
	payload, ok := wrapped.Result.(map[string]any)
	if !ok {
		t.Fatalf("MCP structured result = %#v", wrapped.Result)
	}
	return payload
}

func mustTorqueString(t *testing.T, input map[string]any, name string) string {
	t.Helper()
	value, ok := input[name].(string)
	if !ok || value == "" {
		t.Fatalf("%s = %#v", name, input[name])
	}
	return value
}

func assertTorqueOutputs(t *testing.T, outputs values.ValueSet) {
	t.Helper()
	if len(outputs) != 3 {
		t.Fatalf("terminal output set = %#v", outputs)
	}
	count, ok := outputs["count"].Inline.(json.Number)
	if !ok || count.String() != "6" {
		t.Fatalf("count output = %#v", outputs["count"])
	}
	created, ok := outputs["created"].Inline.([]any)
	if !ok || len(created) != 5 {
		t.Fatalf("created output = %#v", outputs["created"])
	}
	want := map[string]bool{
		"task-project-42-alpha": true, "task-project-42-beta": true,
		"task-project-42-retry-after-timeout": true, "task-project-42-gamma": true,
		"task-project-42-delta": true,
	}
	for _, current := range created {
		if id, idOK := current.(string); !idOK || !want[id] {
			t.Fatalf("created IDs = %#v", created)
		} else {
			delete(want, id)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing created IDs = %#v", want)
	}
	failed, ok := outputs["failed"].Inline.([]any)
	if !ok || len(failed) != 1 {
		t.Fatalf("failed output = %#v", outputs["failed"])
	}
	projection, ok := failed[0].(map[string]any)
	if !ok || projection["status"] != string(workflowruntime.NodeFailed) {
		t.Fatalf("failed projection = %#v", failed[0])
	}
	errorValue, ok := projection["error"].(map[string]any)
	if !ok || errorValue["code"] != "mcp_tool_error" {
		t.Fatalf("failed error projection = %#v", projection["error"])
	}
	for name, value := range outputs {
		if value.Redaction != values.RedactionPrivate || value.Retention != values.RetentionRun || value.Producer.Output != name {
			t.Fatalf("output %s provenance = %#v", name, value)
		}
	}
}

func assertTorqueDurableHistory(ctx context.Context, t *testing.T, state *persistence.WorkflowStateStore, runID workflowruntime.RunID, fake *torqueCreateFake) {
	t.Helper()
	parent := workflowruntime.NodeInvocationID{RunID: runID, NodeID: "create"}
	fanOut, err := state.LoadFanOut(ctx, parent)
	if err != nil || fanOut.Status != workflowruntime.FanOutSucceeded || fanOut.MaxConcurrency != 4 || len(fanOut.Items) != 6 {
		t.Fatalf("durable fan-out = %#v, %v", fanOut, err)
	}
	results, err := state.LoadFanOutItemResults(ctx, parent)
	if err != nil || len(results) != 6 {
		t.Fatalf("durable item results = %#v, %v", results, err)
	}
	failed := 0
	for _, item := range results {
		attempts, loadErr := state.ListAttempts(ctx, item.Invocation)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		inputs, loadErr := state.LoadValues(ctx, fanOut.Items[item.Index].Inputs)
		if loadErr != nil || len(inputs) != 5 || inputs["item"].Producer.Kind != "fanout-item" || inputs["title"].Producer.Kind != "node_input" || inputs["project-id"].Inline != "project-42" {
			t.Fatalf("item %d inputs = %#v, %v", item.Index, inputs, loadErr)
		}
		title := fmt.Sprint(inputs["title"].Inline)
		switch item.Status {
		case workflowruntime.NodeSucceeded:
			if item.Outputs == nil || len(item.OutputValues) != 2 || item.OutputValues["task-id"].Producer.Kind != "node_output" ||
				item.OutputValues["result-json"].Producer.Kind != "mcp" || len(attempts) != fake.attempts(title) {
				t.Fatalf("succeeded item %s = %#v attempts=%#v", title, item, attempts)
			}
			if _, leaked := item.OutputValues[workflowmcp.OutputMetadata]; leaked {
				t.Fatalf("adapter-private MCP metadata became public: %#v", item.OutputValues)
			}
		case workflowruntime.NodeFailed:
			failed++
			if title != "terminal-failure" || item.Failure == nil || item.Failure.Code != "mcp_tool_error" || len(attempts) != 1 {
				t.Fatalf("failed item = %#v attempts=%#v", item, attempts)
			}
		default:
			t.Fatalf("nonterminal item after run completion = %#v", item)
		}
	}
	if failed != 1 {
		t.Fatalf("terminal item failure count = %d", failed)
	}
	events, err := state.ListEvents(ctx, workflowruntime.EventQuery{RunID: runID})
	if err != nil || len(events) < 20 {
		t.Fatalf("durable event history length = %d, %v", len(events), err)
	}
	foundRetrySchedule, foundRetryActivation, foundFanOutComplete := false, false, false
	for _, event := range events {
		switch event.Type {
		case workflowruntime.EventRetryScheduled:
			foundRetrySchedule = true
		case workflowruntime.EventRetryActivated:
			foundRetryActivation = true
		case workflowruntime.EventFanOutCompleted:
			foundFanOutComplete = true
		}
	}
	if !foundRetrySchedule || !foundRetryActivation || !foundFanOutComplete {
		t.Fatalf("missing durable retry/fan-out events schedule=%v activate=%v fanout=%v", foundRetrySchedule, foundRetryActivation, foundFanOutComplete)
	}
}
