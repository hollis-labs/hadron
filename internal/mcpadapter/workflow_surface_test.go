package mcpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/rundiagnostics"
	"github.com/hollis-labs/hadron/workflow/diagnostic"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

func TestWorkflowSurfacePinnedSchemasSessionIsolationAndProfileRemoval(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, operations, operations, operations))
	mcpServer := adapter.newServer()
	first := newWorkflowTestSession("session-a")
	second := newWorkflowTestSession("session-b")
	if err := mcpServer.RegisterSession(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := mcpServer.RegisterSession(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	firstContext := mcpServer.WithContext(t.Context(), first)
	secondContext := mcpServer.WithContext(t.Context(), second)
	if _, _, err := adapter.workflow.current(secondContext, second.SessionID(), "token-b"); err != nil {
		t.Fatal(err)
	}

	firstTools := first.GetSessionTools()
	secondTools := second.GetSessionTools()
	firstTool, ok := firstTools["workflow_team_alpha"]
	if !ok || firstTool.Tool.RawInputSchema == nil || firstTool.Tool.RawOutputSchema == nil {
		t.Fatalf("first pinned tool = %#v", firstTools)
	}
	if !strings.Contains(firstTool.Tool.Description, "asynchronous durable run") || !strings.Contains(firstTool.Tool.Description, "outputs is optional") || !strings.Contains(firstTool.Tool.Description, "hadron_workflow_run_inspect") {
		t.Fatalf("pinned tool does not document its run-handle contract: %q", firstTool.Tool.Description)
	}
	if _, leaked := firstTools["workflow_team_beta"]; leaked {
		t.Fatal("second principal's pinned tool leaked into first session")
	}
	if _, betaMounted := secondTools["workflow_team_beta"]; !betaMounted {
		t.Fatalf("second pinned tool = %#v", secondTools)
	}
	if _, leaked := secondTools["workflow_team_alpha"]; leaked {
		t.Fatal("first principal's pinned tool leaked into second session")
	}

	request := mcp.CallToolRequest{Header: http.Header{"Authorization": []string{"Bearer token-a"}}}
	request.Params.Name = firstTool.Tool.Name
	request.Params.Arguments = map[string]any{"message": "hello", "run_id": "workflow-input-not-control"}
	result, callErr := firstTool.Handler(firstContext, request)
	if callErr != nil || result.IsError {
		t.Fatalf("pinned invocation = %#v, %v", result, callErr)
	}
	handle, ok := result.StructuredContent.(workflowInvocationResult)
	if !ok || handle.RunID == "" || handle.Status != "not_admitted" {
		t.Fatalf("pinned invocation handle = %#v", result.StructuredContent)
	}
	var outputDocument map[string]any
	if decodeErr := json.Unmarshal(firstTool.Tool.RawOutputSchema, &outputDocument); decodeErr != nil {
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
	metaRun := mcp.CallToolRequest{Header: http.Header{"Authorization": []string{"Bearer token-a"}}}
	metaRun.Params.Arguments = map[string]any{"name": exposure.alpha.Name, "version": exposure.alpha.Version, "digest": exposure.alpha.Digest, "run_id": "meta-run", "idempotency_key": "meta-key"}
	metaResult, err := adapter.workflow.handleRun(firstContext, metaRun)
	if err != nil || metaResult.IsError {
		t.Fatalf("meta invocation = %#v, %v", metaResult, err)
	}
	operations.mu.Lock()
	if len(operations.runs) != 2 || string(operations.runs[1].RunID) != "meta-run" || operations.runs[1].IdempotencyKey != "meta-key" || len(operations.runs[1].Inputs) != 0 || operations.runs[1].Definition != exposure.alpha.Definition {
		t.Fatalf("meta run request = %#v", operations.runs)
	}
	operations.mu.Unlock()

	load := mcp.CallToolRequest{Header: http.Header{"Authorization": []string{"Bearer token-a"}}}
	load.Params.Arguments = map[string]any{"definitions": []string{"team/lazy@v1@" + exposure.lazy.Digest}}
	loaded, err := adapter.workflow.handleLoad(firstContext, load)
	if err != nil || loaded.IsError {
		t.Fatalf("lazy load = %#v, %v", loaded, err)
	}
	if _, ok := first.GetSessionTools()[exposure.lazy.ToolName]; !ok {
		t.Fatal("lazy tool was not mounted in requesting session")
	}
	if _, leaked := second.GetSessionTools()[exposure.lazy.ToolName]; leaked {
		t.Fatal("lazy tool leaked across MCP sessions")
	}

	exposure.setGeneration("token-a", 2, nil)
	if _, _, err := adapter.workflow.current(firstContext, first.SessionID(), "token-a"); err != nil {
		t.Fatal(err)
	}
	if len(first.GetSessionTools()) != 0 {
		t.Fatalf("profile change retained stale mounts: %#v", first.GetSessionTools())
	}
	if len(first.notifications) == 0 {
		t.Fatal("mount changes emitted no tools.listChanged notification")
	}
}

func TestWorkflowSurfaceFailClosedErrorAndMetaCatalog(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, nil, nil, nil))
	request := mcp.CallToolRequest{}
	request.Params.Arguments = map[string]any{"name": "team/alpha", "version": "v1", "digest": exposure.alpha.Digest}
	result, err := adapter.workflow.handleValidate(t.Context(), request)
	if err != nil || !result.IsError {
		t.Fatalf("uncomposed validate = %#v, %v", result, err)
	}
	envelope, ok := result.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("error envelope = %#v", result.StructuredContent)
	}
	operationError, ok := envelope["error"].(appworkflow.WorkflowOperationError)
	if !ok || operationError.Code != appworkflow.WorkflowErrorCodeUnavailable {
		t.Fatalf("operation error = %#v", envelope["error"])
	}

	exposure.mu.Lock()
	exposure.describeErr = errors.New("database password=super-secret")
	exposure.mu.Unlock()
	adapter = New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}))
	result, err = adapter.workflow.handleDescribe(t.Context(), request)
	if err != nil || !result.IsError {
		t.Fatalf("hidden describe = %#v, %v", result, err)
	}
	if text := result.Content[0].(mcp.TextContent).Text; containsWorkflowTestText(text, "super-secret") {
		t.Fatalf("transport exposed raw dependency error: %s", text)
	}
}

func TestWorkflowSurfaceRefreshFailureRemovesSameGenerationMounts(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}))
	mcpServer := adapter.newServer()
	session := newWorkflowTestSession("session-refresh")
	if err := mcpServer.RegisterSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	ctx := mcpServer.WithContext(t.Context(), session)
	if _, mounted := session.GetSessionTools()[exposure.alpha.ToolName]; !mounted {
		t.Fatal("initial direct workflow was not mounted")
	}
	exposure.setDirectError(errors.New("catalog unavailable"))
	if _, _, err := adapter.workflow.current(ctx, session.SessionID(), "token-a"); err == nil {
		t.Fatal("same-generation direct refresh failure was ignored")
	}
	if len(session.GetSessionTools()) != 0 {
		t.Fatalf("refresh failure retained stale tools: %#v", session.GetSessionTools())
	}
	if len(session.notifications) == 0 {
		t.Fatal("refresh failure removal emitted no tools.listChanged notification")
	}
}

func TestWorkflowSurfaceLoadingIdenticalDirectToolIsBudgetNeutral(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	exposure.setBudget("token-a", 1)
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}))
	mcpServer := adapter.newServer()
	session := newWorkflowTestSession("session-idempotent-load")
	if err := mcpServer.RegisterSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	request := mcp.CallToolRequest{Header: http.Header{"Authorization": []string{"Bearer token-a"}}}
	request.Params.Arguments = map[string]any{"definitions": []string{exposure.alpha.Name + "@" + exposure.alpha.Version + "@" + exposure.alpha.Digest}}
	result, err := adapter.workflow.handleLoad(mcpServer.WithContext(t.Context(), session), request)
	if err != nil || result.IsError {
		t.Fatalf("idempotent direct load = %#v, %v", result, err)
	}
	if tools := session.GetSessionTools(); len(tools) != 1 || tools[exposure.alpha.ToolName].Tool.Name != exposure.alpha.ToolName {
		t.Fatalf("idempotent load changed direct mount or budget: %#v", tools)
	}
}

func TestWorkflowSurfacePublishesSingleClientStdioMountWithoutSessionTools(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}, &fakeWorkflowOperations{}))
	mcpServer := adapter.newServer()
	stdio := newWorkflowTestPlainSession("stdio")
	if _, sessionTools := any(stdio).(server.SessionWithTools); sessionTools {
		t.Fatal("stdio regression fixture unexpectedly supports session-local tools")
	}
	if err := mcpServer.RegisterSession(t.Context(), stdio); err != nil {
		t.Fatal(err)
	}
	if mounted := mcpServer.ListTools()[exposure.alpha.ToolName]; mounted == nil {
		t.Fatal("single-client stdio mount was not published globally")
	}
	exposure.setGeneration("token-a", 2, nil)
	ctx := mcpServer.WithContext(t.Context(), stdio)
	if _, _, err := adapter.workflow.current(ctx, stdio.SessionID(), "token-a"); err != nil {
		t.Fatal(err)
	}
	if stale := mcpServer.ListTools()[exposure.alpha.ToolName]; stale != nil {
		t.Fatal("single-client stdio retained a removed workflow tool")
	}
	if len(stdio.notifications) == 0 {
		t.Fatal("single-client stdio mount changes emitted no tools.listChanged notification")
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
	for _, name := range []string{"hadron_workflows_search", "hadron_workflow_run_events", "hadron_workflow_run_subscribe"} {
		property, ok := workflowMetaTool(name).InputSchema.Properties["limit"].(map[string]any)
		if !ok || property["type"] != "integer" {
			t.Fatalf("%s limit schema = %#v", name, property)
		}
	}

	request := mcp.CallToolRequest{}
	if limit, err := exactWorkflowLimitArgument(request, 20); err != nil || limit != 20 {
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
			request.Params.Arguments = map[string]any{"limit": test.value}
			if _, err := exactWorkflowLimitArgument(request, 20); err == nil {
				t.Fatalf("limit %#v was accepted", test.value)
			}
		})
	}
	request.Params.Arguments = map[string]any{"limit": float64(1001)}
	limit, err := exactWorkflowLimitArgument(request, 100)
	if err != nil || boundedWorkflowLimit(limit) != 1000 {
		t.Fatalf("event limit clamp = %d, %v", limit, err)
	}

	exposure := newFakeWorkflowExposure()
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, operations, operations, operations))
	fractional := mcp.CallToolRequest{}
	fractional.Params.Arguments = map[string]any{"limit": 1.9}
	if result, callErr := adapter.workflow.handleSearch(t.Context(), fractional); callErr != nil || !result.IsError {
		t.Fatalf("fractional search limit = %#v, %v", result, callErr)
	}
	fractional.Params.Arguments = map[string]any{"run_id": "run-one", "limit": 1.9}
	if result, callErr := adapter.workflow.handleEvents(t.Context(), fractional); callErr != nil || !result.IsError {
		t.Fatalf("fractional event limit = %#v, %v", result, callErr)
	}
}

func TestWorkflowRawMCPCallsRejectUnsafeNumbers(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	operations := &fakeWorkflowOperations{}
	adapter := New(nil, nil, nil, nil, "token-a", nil, WithWorkflowServices(exposure, operations, operations, operations))
	mcpServer := adapter.newServer()
	session := newWorkflowTestSession("session-lossy-number")
	if err := mcpServer.RegisterSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	assertHandlerRejection := func(response mcp.JSONRPCMessage) {
		t.Helper()
		rpcResponse, ok := response.(mcp.JSONRPCResponse)
		if !ok {
			t.Fatalf("unsafe number was rejected outside the tool handler: %#v", response)
		}
		result, ok := rpcResponse.Result.(*mcp.CallToolResult)
		if !ok || !result.IsError {
			t.Fatalf("unsafe number did not produce a handler error: %#v", rpcResponse.Result)
		}
	}
	message := json.RawMessage(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"hadron_workflow_run","arguments":{"name":"` + exposure.alpha.Name + `","version":"` + exposure.alpha.Version + `","digest":"` + exposure.alpha.Digest + `","inputs":{"nested":[9007199254740993]}}}}`)
	response := mcpServer.HandleMessage(mcpServer.WithContext(t.Context(), session), message)
	assertHandlerRejection(response)
	operations.mu.Lock()
	runCount := len(operations.runs)
	operations.mu.Unlock()
	if runCount != 0 {
		t.Fatalf("lossy raw MCP call reached RunWorkflow %d times", runCount)
	}

	message = json.RawMessage(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"hadron_workflows_search","arguments":{"limit":1.9}}}`)
	response = mcpServer.HandleMessage(mcpServer.WithContext(t.Context(), session), message)
	assertHandlerRejection(response)
}

func TestWorkflowGeneratedInvocationIdentityIsRestartUnique(t *testing.T) {
	exposure := newFakeWorkflowExposure()
	firstOperations := &fakeWorkflowOperations{}
	secondOperations := &fakeWorkflowOperations{}
	first := New(nil, nil, nil, nil, "token-a", nil, withWorkflowInstanceNonceForTest("instance-one"), WithWorkflowServices(exposure, firstOperations, firstOperations, firstOperations))
	second := New(nil, nil, nil, nil, "token-a", nil, withWorkflowInstanceNonceForTest("instance-two"), WithWorkflowServices(exposure, secondOperations, secondOperations, secondOperations))
	request := mcp.CallToolRequest{}
	request.Params.Arguments = map[string]any{"message": "same invocation"}
	for _, surface := range []*workflowSurface{first.workflow, second.workflow} {
		result, err := surface.run(t.Context(), "stdio", request, exposure.alpha.Definition, true)
		if err != nil || result.IsError {
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
	request := mcp.CallToolRequest{}
	request.Params.Arguments = map[string]any{"run_id": "run-one", "wait_id": "wait-one", "correlation": "gate-one", "token": "one-time-token", "payload": payload, "idempotency_key": "resume-key"}
	for attempt := 0; attempt < 2; attempt++ {
		result, callErr := adapter.workflow.handleGate(t.Context(), request)
		if callErr != nil || result.IsError {
			t.Fatalf("resume attempt %d = %#v, %v", attempt, result, callErr)
		}
	}
	operations.mu.Lock()
	if len(operations.resumes) != 2 || operations.resumes[0].IdempotencyKey != "resume-key" || operations.resumes[1].IdempotencyKey != "resume-key" || operations.resumes[0].WakeSource != "gate" || operations.resumes[0].Token != "one-time-token" || operations.resumes[0].Payload.Inline != "approve" {
		t.Fatalf("resume requests = %#v", operations.resumes)
	}
	operations.mu.Unlock()

	request.Params.Arguments = map[string]any{"run_id": "run-two", "wait_id": "wait-two", "correlation": "message-one", "payload": payload}
	for attempt := 0; attempt < 2; attempt++ {
		credentialless, callErr := adapter.workflow.handleMessage(t.Context(), request)
		if callErr != nil || credentialless.IsError {
			t.Fatalf("credentialless resume attempt %d = %#v, %v", attempt, credentialless, callErr)
		}
		if attempt == 1 {
			result, ok := credentialless.StructuredContent.(appworkflow.ResumeWorkflowRunResult)
			if !ok || result.Outcome != appworkflow.WorkflowResumeReplayed {
				t.Fatalf("credentialless replay = %#v", credentialless.StructuredContent)
			}
		}
	}
	operations.mu.Lock()
	if len(operations.resumes) != 4 || operations.resumes[2].Token != "" || operations.resumes[2].IdempotencyKey != "" || operations.resumes[2].WakeSource != "message" || operations.resumes[3].IdempotencyKey != "" {
		t.Fatalf("credentialless resume requests = %#v", operations.resumes)
	}
	operations.mu.Unlock()

	signalRequest := mcp.CallToolRequest{}
	signalRequest.Params.Arguments = map[string]any{"run_id": "run-two", "name": "approved", "correlation": "signal-one", "payload": payload, "idempotency_key": "signal-key", "confirmed": true}
	signaled, signalErr := adapter.workflow.handleSignal(t.Context(), signalRequest)
	if signalErr != nil || signaled.IsError {
		t.Fatalf("typed signal = %#v, %v", signaled, signalErr)
	}
	operations.mu.Lock()
	defer operations.mu.Unlock()
	if len(operations.signals) != 1 || operations.signals[0].Payload.Inline != "approve" || operations.signals[0].IdempotencyKey != "signal-key" || !operations.signals[0].Confirmed {
		t.Fatalf("signal requests = %#v", operations.signals)
	}
}

func TestWorkflowTokenDoesNotFallBackAcrossExplicitAuthorization(t *testing.T) {
	for _, header := range []string{"Basic opaque", "Bearer ", "Bearer  leading", "Bearer trailing ", "Bearer line\nbreak", strings.Repeat("x", (16<<10)+1)} {
		if got := workflowToken(http.Header{"Authorization": []string{header}}, "privileged-fallback"); got != "" {
			t.Fatalf("explicit authorization %q fell back to %q", header, got)
		}
	}
	if got := workflowToken(nil, "stdio-token"); got != "stdio-token" {
		t.Fatalf("missing authorization lost stdio token: %q", got)
	}
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

type workflowTestSession struct {
	id            string
	mu            sync.Mutex
	initialized   bool
	tools         map[string]server.ServerTool
	notifications chan mcp.JSONRPCNotification
	level         mcp.LoggingLevel
}

type workflowTestPlainSession struct {
	id            string
	initialized   bool
	notifications chan mcp.JSONRPCNotification
}

func newWorkflowTestPlainSession(id string) *workflowTestPlainSession {
	return &workflowTestPlainSession{id: id, initialized: true, notifications: make(chan mcp.JSONRPCNotification, 32)}
}

func (s *workflowTestPlainSession) Initialize()       { s.initialized = true }
func (s *workflowTestPlainSession) Initialized() bool { return s.initialized }
func (s *workflowTestPlainSession) NotificationChannel() chan<- mcp.JSONRPCNotification {
	return s.notifications
}
func (s *workflowTestPlainSession) SessionID() string { return s.id }

func newWorkflowTestSession(id string) *workflowTestSession {
	return &workflowTestSession{id: id, initialized: true, tools: make(map[string]server.ServerTool), notifications: make(chan mcp.JSONRPCNotification, 32)}
}

func (s *workflowTestSession) Initialize()       { s.initialized = true }
func (s *workflowTestSession) Initialized() bool { return s.initialized }
func (s *workflowTestSession) NotificationChannel() chan<- mcp.JSONRPCNotification {
	return s.notifications
}
func (s *workflowTestSession) SessionID() string { return s.id }
func (s *workflowTestSession) GetSessionTools() map[string]server.ServerTool {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]server.ServerTool, len(s.tools))
	for name, tool := range s.tools {
		result[name] = tool
	}
	return result
}
func (s *workflowTestSession) SetSessionTools(tools map[string]server.ServerTool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tools = make(map[string]server.ServerTool, len(tools))
	for name, tool := range tools {
		s.tools[name] = tool
	}
}
func (s *workflowTestSession) SetLogLevel(level mcp.LoggingLevel) { s.level = level }
func (s *workflowTestSession) GetLogLevel() mcp.LoggingLevel      { return s.level }

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

var _ server.SessionWithTools = (*workflowTestSession)(nil)
var _ server.SessionWithLogging = (*workflowTestSession)(nil)
var _ server.ClientSession = (*workflowTestPlainSession)(nil)
