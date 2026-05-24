package mcpadapter_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/mcpadapter"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// ── Fakes ──────────────────────────────────────────────────────────────────────

type fakeRunner struct{}

func (f *fakeRunner) Enqueue(_ context.Context, _ execution.Request) error { return nil }
func (f *fakeRunner) Cancel(_ string) bool                                 { return false }

type fakePipelineRunner struct{}

func (f *fakePipelineRunner) Start(_ context.Context, _, _, _ string) error { return nil }

type fakeScheduler struct{}

func (f *fakeScheduler) Start()                          {}
func (f *fakeScheduler) Stop()                           {}
func (f *fakeScheduler) TickNow(_ context.Context) error { return nil }
func (f *fakeScheduler) Status() scheduler.Status        { return scheduler.Status{Running: true} }

// ── Helpers ───────────────────────────────────────────────────────────────────

func newTestStore(t *testing.T) *persistence.Store {
	t.Helper()
	store, err := persistence.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newTestAdapter(t *testing.T) *mcpadapter.Adapter {
	t.Helper()
	store := newTestStore(t)
	return mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "", nil)
}

func newBlueprintDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	content := strings.TrimSpace(`
blueprint:
  name: release-docs
  slug: release-docs
  title: Release Docs
  description: Build release notes and publish docs for a beta release.
  tags: [release, docs]
inputs:
  - name: version
    type: string
    required: true
    description: Release version
steps:
  - section: main
    tasks:
      - name: publish
        cmd: echo publish {{ .inputs.version }}
`)
	if err := os.WriteFile(filepath.Join(dir, "release-docs.yaml"), []byte(content), 0o644); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	return dir
}

// callTool invokes a registered tool and returns the text result.
func callTool(t *testing.T, adapter *mcpadapter.Adapter, toolName string, args map[string]any) map[string]any {
	t.Helper()
	result := adapter.CallTool(context.Background(), toolName, args)
	if result == nil {
		t.Fatalf("tool %s returned nil", toolName)
	}
	if len(result.Content) == 0 {
		t.Fatalf("tool %s returned empty content", toolName)
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("tool %s returned non-text content", toolName)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
		t.Fatalf("tool %s: unmarshal result: %v (raw: %s)", toolName, err, text.Text)
	}
	return out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestMCP_Health(t *testing.T) {
	adapter := newTestAdapter(t)
	result := adapter.CallTool(context.Background(), "hadron_health", nil)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Content) == 0 {
		t.Fatal("expected content in result")
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatal("expected text content")
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", out["status"])
	}
}

func TestMCP_BlueprintValidate_Valid(t *testing.T) {
	adapter := newTestAdapter(t)

	validContent := strings.TrimSpace(`
blueprint:
  name: test
steps:
  - section: main
    tasks:
      - name: greet
        cmd: echo hello
`)

	out := callTool(t, adapter, "hadron_blueprint_validate", map[string]any{
		"content": validContent,
	})
	if out["valid"] != true {
		t.Fatalf("expected valid=true, got %v", out)
	}
	if _, hasErr := out["error"]; hasErr {
		t.Fatalf("expected no error field, got %v", out)
	}
}

func TestMCP_HadronSkills_IndexAndBody(t *testing.T) {
	adapter := newTestAdapter(t)

	index := callTool(t, adapter, "hadron_skills", nil)
	items, ok := index["items"].([]any)
	if !ok || len(items) == 0 {
		t.Fatalf("expected skill index items, got %#v", index)
	}

	result := adapter.CallTool(context.Background(), "hadron_skills", map[string]any{"name": "start-here"})
	if result == nil || len(result.Content) == 0 {
		t.Fatal("expected hadron_skills text response")
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatal("expected text content from hadron_skills")
	}
	if !strings.Contains(text.Text, "hadron_blueprint_discover") {
		t.Fatalf("expected orientation body, got %q", text.Text)
	}
}

func TestMCP_BlueprintDiscoverAndSchema(t *testing.T) {
	store := newTestStore(t)
	dir := newBlueprintDir(t)
	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "", nil, mcpadapter.WithBlueprintDir(dir))

	discover := callTool(t, adapter, "hadron_blueprint_discover", map[string]any{
		"query": "release docs beta",
	})
	items, ok := discover["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one discover hit, got %#v", discover)
	}
	item, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected discover item object, got %#v", items[0])
	}
	if item["slug"] != "release-docs" {
		t.Fatalf("unexpected discover hit: %#v", item)
	}

	schema := callTool(t, adapter, "hadron_blueprint_schema", map[string]any{
		"blueprint_path": filepath.Join(dir, "release-docs.yaml"),
	})
	inputSchema, ok := schema["input_schema"].(map[string]any)
	if !ok {
		t.Fatalf("expected input_schema object, got %#v", schema)
	}
	required, ok := inputSchema["required"].([]any)
	if !ok || len(required) != 1 || required[0] != "version" {
		t.Fatalf("unexpected required schema fields: %#v", inputSchema)
	}
}

func TestMCP_BlueprintSearch_RequiresQuery(t *testing.T) {
	adapter := newTestAdapter(t)
	out := callTool(t, adapter, "hadron_blueprint_search", map[string]any{})
	if out["code"] != "validation_error" {
		t.Fatalf("expected validation_error, got %#v", out)
	}
}

func TestMCP_BlueprintGet_RejectsSymlinkEscape(t *testing.T) {
	store := newTestStore(t)
	blueprintDir := t.TempDir()
	outsideDir := t.TempDir()
	outsidePath := filepath.Join(outsideDir, "outside.yaml")
	content := strings.TrimSpace(`
blueprint:
  name: outside
steps:
  - section: main
    tasks:
      - name: noop
        cmd: echo outside
`)
	if err := os.WriteFile(outsidePath, []byte(content), 0o644); err != nil {
		t.Fatalf("write outside blueprint: %v", err)
	}
	linkPath := filepath.Join(blueprintDir, "linked.yaml")
	if err := os.Symlink(outsidePath, linkPath); err != nil {
		t.Skipf("symlink not available: %v", err)
	}

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "", nil, mcpadapter.WithBlueprintDir(blueprintDir))
	out := callTool(t, adapter, "hadron_blueprint_get", map[string]any{"blueprint_path": linkPath})
	if out["code"] != "validation_error" {
		t.Fatalf("expected validation_error for symlink escape, got %#v", out)
	}
}

func TestMCP_MessageSendGetAndInbox(t *testing.T) {
	store := newTestStore(t)
	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "internal", mcpadapter.AllScopes())

	sent := callTool(t, adapter, "hadron_message_send", map[string]any{
		"substrate":     "local_mailbox",
		"kind":          "notice",
		"from":          "msg://service/hadron/operator",
		"to":            "msg://agent/hadron/reviewer-1",
		"thread_id":     "review-123",
		"payload_json":  `{"approved":true}`,
		"metadata_json": `{"correlation_id":"review-123"}`,
	})
	messageID, _ := sent["id"].(string)
	if messageID == "" {
		t.Fatalf("expected message id in send response: %#v", sent)
	}

	got := callTool(t, adapter, "hadron_message_get", map[string]any{"message_id": messageID})
	if got["thread_id"] != "review-123" {
		t.Fatalf("unexpected message_get response: %#v", got)
	}

	inbox := callTool(t, adapter, "hadron_messages_inbox", map[string]any{
		"substrate":      "local_mailbox",
		"to":             "msg://agent/hadron/reviewer-1",
		"correlation_id": "review-123",
	})
	if inbox["count"] != float64(1) {
		t.Fatalf("unexpected inbox count: %#v", inbox)
	}

	consumed := callTool(t, adapter, "hadron_message_consume", map[string]any{"message_id": messageID})
	if consumed["consumed_at"] == nil {
		t.Fatalf("expected consumed_at after consume: %#v", consumed)
	}
}

func TestMCP_BlueprintValidate_Invalid(t *testing.T) {
	adapter := newTestAdapter(t)

	out := callTool(t, adapter, "hadron_blueprint_validate", map[string]any{
		"content": "not_a_blueprint: true",
	})
	if out["valid"] != false {
		t.Fatalf("expected valid=false, got %v", out)
	}
	if out["error"] == nil || out["error"] == "" {
		t.Fatalf("expected error message, got %v", out)
	}
}

func TestMCP_HumanGateGet(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := store.CreateHumanGate(context.Background(), persistence.HumanGateRecord{
		ID:          "gate-1",
		WorkspaceID: "default",
		RunID:       "run-1",
		StepName:    "approval",
		Prompt:      "Approve deploy?",
		OptionsJSON: `[{"id":"approve","label":"Approve"},{"id":"deny","label":"Deny"}]`,
		Status:      "waiting",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("create human gate: %v", err)
	}

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "", nil)
	out := callTool(t, adapter, "hadron_human_gate_get", map[string]any{"gate_id": "gate-1"})

	if out["id"] != "gate-1" {
		t.Fatalf("expected gate id, got %v", out["id"])
	}
	if out["status"] != "waiting" {
		t.Fatalf("expected waiting status, got %v", out["status"])
	}
	options, ok := out["options"].([]any)
	if !ok || len(options) != 2 {
		t.Fatalf("expected 2 options, got %T %#v", out["options"], out["options"])
	}
}

func TestMCP_HumanGateSubmit(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := store.CreateHumanGate(context.Background(), persistence.HumanGateRecord{
		ID:          "gate-2",
		WorkspaceID: "default",
		RunID:       "run-2",
		StepName:    "approval",
		Prompt:      "Ship it?",
		OptionsJSON: `[{"id":"approve","label":"Approve"},{"id":"deny","label":"Deny"}]`,
		Status:      "waiting",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("create human gate: %v", err)
	}

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "token", []string{mcpadapter.ScopeHumanGateWrite})
	out := callTool(t, adapter, "hadron_human_gate_submit", map[string]any{
		"gate_id":  "gate-2",
		"decision": "approve",
	})

	if out["status"] != "decided" {
		t.Fatalf("expected decided status, got %v", out["status"])
	}
	if out["decision"] != "approve" {
		t.Fatalf("expected approve decision, got %v", out["decision"])
	}

	rec, err := store.GetHumanGate(context.Background(), "gate-2")
	if err != nil {
		t.Fatalf("reload human gate: %v", err)
	}
	if rec.Status != "decided" {
		t.Fatalf("expected decided record status, got %s", rec.Status)
	}
	if !rec.Decision.Valid || rec.Decision.String != "approve" {
		t.Fatalf("expected approve decision record, got %#v", rec.Decision)
	}
}

func TestMCP_HumanGateSubmitRejectsInvalidDecision(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := store.CreateHumanGate(context.Background(), persistence.HumanGateRecord{
		ID:          "gate-3",
		WorkspaceID: "default",
		RunID:       "run-3",
		StepName:    "approval",
		Prompt:      "Continue?",
		OptionsJSON: `[{"id":"approve","label":"Approve"}]`,
		Status:      "waiting",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("create human gate: %v", err)
	}

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "token", []string{mcpadapter.ScopeHumanGateWrite})
	out := callTool(t, adapter, "hadron_human_gate_submit", map[string]any{
		"gate_id":  "gate-3",
		"decision": "deny",
	})

	if out["code"] != "validation_error" {
		t.Fatalf("expected validation_error, got %v", out)
	}

	rec, err := store.GetHumanGate(context.Background(), "gate-3")
	if err != nil {
		t.Fatalf("reload human gate: %v", err)
	}
	if rec.Status != "waiting" {
		t.Fatalf("expected waiting status after rejected decision, got %s", rec.Status)
	}
}

func TestMCP_HumanGateSubmitRequiresScope(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := store.CreateHumanGate(context.Background(), persistence.HumanGateRecord{
		ID:          "gate-4",
		WorkspaceID: "default",
		RunID:       "run-4",
		StepName:    "approval",
		Prompt:      "Approve?",
		OptionsJSON: `[{"id":"approve","label":"Approve"}]`,
		Status:      "waiting",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("create human gate: %v", err)
	}

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "token", []string{mcpadapter.ScopeRunWrite})
	out := callTool(t, adapter, "hadron_human_gate_submit", map[string]any{
		"gate_id":  "gate-4",
		"decision": "approve",
	})
	if out["code"] != "insufficient_scope" {
		t.Fatalf("expected insufficient_scope, got %v", out)
	}

	rec, err := store.GetHumanGate(context.Background(), "gate-4")
	if err != nil {
		t.Fatalf("reload human gate: %v", err)
	}
	if rec.Status != "waiting" {
		t.Fatalf("expected waiting status after scope denial, got %s", rec.Status)
	}
}

func TestMCP_HumanGateGetNotFound(t *testing.T) {
	adapter := newTestAdapter(t)
	out := callTool(t, adapter, "hadron_human_gate_get", map[string]any{"gate_id": "missing"})
	if out["code"] != "not_found" {
		t.Fatalf("expected not_found, got %v", out)
	}
}

func TestMCP_HumanGateSubmitAlreadyDecided(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := store.CreateHumanGate(context.Background(), persistence.HumanGateRecord{
		ID:          "gate-5",
		WorkspaceID: "default",
		RunID:       "run-5",
		StepName:    "approval",
		Prompt:      "Approve?",
		OptionsJSON: `[{"id":"approve","label":"Approve"}]`,
		Status:      "decided",
		Decision:    sql.NullString{String: "approve", Valid: true},
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("create human gate: %v", err)
	}

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "token", []string{mcpadapter.ScopeHumanGateWrite})
	out := callTool(t, adapter, "hadron_human_gate_submit", map[string]any{
		"gate_id":  "gate-5",
		"decision": "approve",
	})
	if out["code"] != "conflict" {
		t.Fatalf("expected conflict, got %v", out)
	}
}

func TestInternalCaller_HadronHealth(t *testing.T) {
	store := newTestStore(t)
	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "internal", mcpadapter.AllScopes())
	caller := mcpadapter.NewInternalCaller(adapter)

	result, err := caller.CallTool(context.Background(), "hadron", "hadron_health", nil)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	wrapped, ok := result.(execution.MCPToolResult)
	if !ok {
		t.Fatalf("expected MCPToolResult, got %T", result)
	}
	payload, ok := wrapped.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", wrapped.Result)
	}
	if wrapped.Metadata.Transport != "in_process" {
		t.Fatalf("unexpected metadata: %#v", wrapped.Metadata)
	}
	if payload["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", payload["status"])
	}
}

func TestInternalCaller_UnknownServer(t *testing.T) {
	store := newTestStore(t)
	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "internal", mcpadapter.AllScopes())
	caller := mcpadapter.NewInternalCaller(adapter)

	_, err := caller.CallTool(context.Background(), "torque", "torque_runs_list", nil)
	if err == nil || !strings.Contains(err.Error(), `mcp server "torque" is not configured`) {
		t.Fatalf("expected unknown server error, got %v", err)
	}
}

func TestInternalCaller_ToolError(t *testing.T) {
	store := newTestStore(t)
	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "internal", mcpadapter.AllScopes())
	caller := mcpadapter.NewInternalCaller(adapter)

	_, err := caller.CallTool(context.Background(), "hadron", "hadron_human_gate_submit", map[string]any{
		"gate_id":  "missing",
		"decision": "approve",
	})
	if err == nil || !strings.Contains(err.Error(), "human gate not found") {
		t.Fatalf("expected tool error, got %v", err)
	}
}

func TestInternalCaller_ExternalStdioServer(t *testing.T) {
	store := newTestStore(t)
	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "internal", mcpadapter.AllScopes())
	caller := mcpadapter.NewInternalCaller(adapter, mcpadapter.WithExternalServers(map[string]mcpadapter.ExternalServerConfig{
		"fake": {
			Transport: "stdio",
			Command:   os.Args[0],
			Args:      []string{"-test.run=TestHelperProcessMCPServer", "--"},
			Env:       map[string]string{"GO_WANT_HELPER_PROCESS_MCP": "1"},
		},
	}))
	defer func() { _ = caller.Close() }()

	result, err := caller.CallTool(context.Background(), "fake", "echo_json", map[string]any{
		"name": "hadron",
	})
	if err != nil {
		t.Fatalf("call external tool: %v", err)
	}
	wrapped, ok := result.(execution.MCPToolResult)
	if !ok {
		t.Fatalf("expected MCPToolResult, got %T", result)
	}
	payload, ok := wrapped.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", wrapped.Result)
	}
	if wrapped.Metadata.Transport != "stdio" || wrapped.Metadata.AttemptCount != 1 {
		t.Fatalf("unexpected metadata: %#v", wrapped.Metadata)
	}
	if payload["echo"] != "hadron" {
		t.Fatalf("expected echoed payload, got %#v", payload)
	}
	if payload["server"] != "fake-helper" {
		t.Fatalf("expected helper server marker, got %#v", payload)
	}
}

func TestInternalCaller_ExternalStreamableHTTPServer(t *testing.T) {
	store := newTestStore(t)
	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "internal", mcpadapter.AllScopes())

	mcpServer := server.NewMCPServer("http-helper", "1.0.0", server.WithToolCapabilities(true))
	mcpServer.AddTool(mcp.NewTool("echo_json",
		mcp.WithString("name", mcp.Required()),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		payload, _ := json.Marshal(map[string]any{
			"echo":   req.GetString("name", ""),
			"server": "http-helper",
		})
		return mcp.NewToolResultText(string(payload)), nil
	})
	testServer := server.NewTestStreamableHTTPServer(mcpServer, server.WithStateLess(true))
	defer testServer.Close()

	caller := mcpadapter.NewInternalCaller(adapter, mcpadapter.WithExternalServers(map[string]mcpadapter.ExternalServerConfig{
		"fake-http": {
			Transport: "streamable_http",
			URL:       testServer.URL,
		},
	}))
	defer func() { _ = caller.Close() }()

	result, err := caller.CallTool(context.Background(), "fake-http", "echo_json", map[string]any{"name": "hadron"})
	if err != nil {
		t.Fatalf("call external streamable_http tool: %v", err)
	}
	wrapped, ok := result.(execution.MCPToolResult)
	if !ok {
		t.Fatalf("expected MCPToolResult, got %T", result)
	}
	payload, ok := wrapped.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", wrapped.Result)
	}
	if wrapped.Metadata.Transport != "streamable_http" {
		t.Fatalf("unexpected metadata: %#v", wrapped.Metadata)
	}
	if payload["echo"] != "hadron" || payload["server"] != "http-helper" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestInternalCaller_ExternalSSEServer(t *testing.T) {
	store := newTestStore(t)
	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "internal", mcpadapter.AllScopes())

	mcpServer := server.NewMCPServer("sse-helper", "1.0.0", server.WithToolCapabilities(true))
	mcpServer.AddTool(mcp.NewTool("echo_json",
		mcp.WithString("name", mcp.Required()),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		payload, _ := json.Marshal(map[string]any{
			"echo":   req.GetString("name", ""),
			"server": "sse-helper",
		})
		return mcp.NewToolResultText(string(payload)), nil
	})
	testServer := server.NewTestServer(mcpServer)
	defer testServer.Close()

	caller := mcpadapter.NewInternalCaller(adapter, mcpadapter.WithExternalServers(map[string]mcpadapter.ExternalServerConfig{
		"fake-sse": {
			Transport: "sse",
			URL:       testServer.URL + "/sse",
		},
	}))
	defer func() { _ = caller.Close() }()

	result, err := caller.CallTool(context.Background(), "fake-sse", "echo_json", map[string]any{"name": "hadron"})
	if err != nil {
		t.Fatalf("call external sse tool: %v", err)
	}
	wrapped, ok := result.(execution.MCPToolResult)
	if !ok {
		t.Fatalf("expected MCPToolResult, got %T", result)
	}
	payload, ok := wrapped.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", wrapped.Result)
	}
	if wrapped.Metadata.Transport != "sse" {
		t.Fatalf("unexpected metadata: %#v", wrapped.Metadata)
	}
	if payload["echo"] != "hadron" || payload["server"] != "sse-helper" {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestMCP_RunMCPCalls(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC()
	if err := store.CreateRun(context.Background(), persistence.RunRecord{
		ID:            "run-mcp-1",
		WorkspaceID:   "default",
		BlueprintPath: "test.yaml",
		Status:        "success",
		InputJSON:     "{}",
		CreatedAt:     now,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	appendEvent := func(eventType, message string, at time.Time) {
		t.Helper()
		if err := store.AppendRunEvent(context.Background(), persistence.RunEventRecord{
			RunID:     "run-mcp-1",
			StepName:  sql.NullString{String: "echo", Valid: true},
			EventType: eventType,
			Message:   sql.NullString{String: message, Valid: message != ""},
			CreatedAt: at,
		}); err != nil {
			t.Fatalf("append event %s: %v", eventType, err)
		}
	}
	appendEvent("mcp_call_start", "fake.echo_json", now)
	appendEvent("mcp_call_transport", `{"server":"fake","tool":"echo_json","transport":"streamable_http","reused_client":true}`, now.Add(time.Millisecond))
	appendEvent("mcp_call_retry", `{"server":"fake","tool":"echo_json","transport":"streamable_http","retry_count":1,"attempt_count":2}`, now.Add(2*time.Millisecond))
	appendEvent("mcp_call_result", `{"server":"fake","tool":"echo_json","transport":"streamable_http","result_json":"{\"ok\":true}","retry_count":1,"attempt_count":2}`, now.Add(3*time.Millisecond))

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "", nil)
	out := callTool(t, adapter, "hadron_run_mcp_calls", map[string]any{"run_id": "run-mcp-1"})
	items, ok := out["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected 1 item, got %#v", out["items"])
	}
	item := items[0].(map[string]any)
	if item["transport"] != "streamable_http" || item["retry_count"] != float64(1) || item["tool"] != "echo_json" {
		t.Fatalf("unexpected item: %#v", item)
	}
}

func TestMCP_RunOperations(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC()
	if err := store.CreateRun(context.Background(), persistence.RunRecord{
		ID:            "run-ops-1",
		WorkspaceID:   "default",
		BlueprintPath: "test.yaml",
		Status:        "success",
		InputJSON:     "{}",
		CreatedAt:     now,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	appendEvent := func(step, eventType, message string, at time.Time) {
		t.Helper()
		if err := store.AppendRunEvent(context.Background(), persistence.RunEventRecord{
			RunID:     "run-ops-1",
			StepName:  sql.NullString{String: step, Valid: true},
			EventType: eventType,
			Message:   sql.NullString{String: message, Valid: message != ""},
			CreatedAt: at,
		}); err != nil {
			t.Fatalf("append event %s: %v", eventType, err)
		}
	}
	appendEvent("fetch", "http_call_start", "GET http://127.0.0.1:7777/health", now)
	appendEvent("fetch", "http_call_response", `{"status_code":200,"duration_ms":12,"body_json":"{\"ok\":true}"}`, now.Add(time.Millisecond))
	appendEvent("wait", "message_wait_start", `{"substrate":"tether","to":"mailbox://agent/replies","correlation_id":"corr-123","timeout_ms":2000}`, now.Add(2*time.Millisecond))
	appendEvent("wait", "message_wait_poll", "no matching message", now.Add(3*time.Millisecond))
	appendEvent("wait", "message_wait_reply", `{"message_id":"msg-1","body_json":"{\"approved\":true}"}`, now.Add(4*time.Millisecond))
	appendEvent("launch", "agent_launch_start", `{"substrate":"tether","launch_id":"launch-1","logical_agent_id":"agent-1"}`, now.Add(5*time.Millisecond))
	appendEvent("launch", "agent_launch_result", `{"substrate":"tether","launch_id":"launch-1","result_json":"{\"session_id\":\"sess-1\"}"}`, now.Add(6*time.Millisecond))

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "", nil)
	out := callTool(t, adapter, "hadron_run_operations", map[string]any{
		"run_id": "run-ops-1",
		"kind":   "message_wait",
		"limit":  1,
	})
	items, ok := out["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected 1 item, got %#v", out["items"])
	}
	item := items[0].(map[string]any)
	if item["kind"] != "message_wait" || item["message_id"] != "msg-1" || item["poll_count"] != float64(1) {
		t.Fatalf("unexpected message_wait item: %#v", item)
	}
	if out["total_count"] != float64(1) || out["next_cursor"] != nil {
		t.Fatalf("unexpected envelope: %#v", out)
	}
}

func TestMCP_RunOperationsPagination(t *testing.T) {
	store := newTestStore(t)
	now := time.Now().UTC()
	if err := store.CreateRun(context.Background(), persistence.RunRecord{
		ID:            "run-ops-page-1",
		WorkspaceID:   "default",
		BlueprintPath: "test.yaml",
		Status:        "success",
		InputJSON:     "{}",
		CreatedAt:     now,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	for i, step := range []string{"a", "b", "c"} {
		at := now.Add(time.Duration(i) * time.Millisecond)
		if err := store.AppendRunEvent(context.Background(), persistence.RunEventRecord{
			RunID:     "run-ops-page-1",
			StepName:  sql.NullString{String: step, Valid: true},
			EventType: "http_call_start",
			Message:   sql.NullString{String: "GET http://127.0.0.1:7777/health", Valid: true},
			CreatedAt: at,
		}); err != nil {
			t.Fatalf("append start: %v", err)
		}
		if err := store.AppendRunEvent(context.Background(), persistence.RunEventRecord{
			RunID:     "run-ops-page-1",
			StepName:  sql.NullString{String: step, Valid: true},
			EventType: "http_call_response",
			Message:   sql.NullString{String: `{"status_code":200}`, Valid: true},
			CreatedAt: at.Add(time.Microsecond),
		}); err != nil {
			t.Fatalf("append response: %v", err)
		}
	}

	adapter := mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "", nil)
	out := callTool(t, adapter, "hadron_run_operations", map[string]any{
		"run_id": "run-ops-page-1",
		"kind":   "http_call",
		"limit":  2,
	})
	items, ok := out["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 items, got %#v", out["items"])
	}
	cursor, _ := out["next_cursor"].(string)
	if cursor == "" || out["total_count"] != float64(3) {
		t.Fatalf("unexpected first page envelope: %#v", out)
	}

	out2 := callTool(t, adapter, "hadron_run_operations", map[string]any{
		"run_id": "run-ops-page-1",
		"kind":   "http_call",
		"limit":  2,
		"cursor": cursor,
	})
	items2, ok := out2["items"].([]any)
	if !ok || len(items2) != 1 || out2["next_cursor"] != nil {
		t.Fatalf("unexpected second page: %#v", out2)
	}
}

func TestHelperProcessMCPServer(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS_MCP") != "1" {
		return
	}
	s := server.NewMCPServer("fake-helper", "1.0.0", server.WithToolCapabilities(true))
	s.AddTool(mcp.NewTool("echo_json",
		mcp.WithString("name", mcp.Required()),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		payload, _ := json.Marshal(map[string]any{
			"echo":   req.GetString("name", ""),
			"server": "fake-helper",
		})
		return mcp.NewToolResultText(string(payload)), nil
	})
	if err := server.ServeStdio(s); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
