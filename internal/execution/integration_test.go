package execution_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging"
	"github.com/hollis-labs/hadron/internal/agentsubstrate"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/mcpadapter"
	"github.com/hollis-labs/hadron/internal/messagesubstrate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func openStore(t *testing.T) *persistence.Store {
	t.Helper()
	store, err := persistence.Open(filepath.Join(t.TempDir(), "hadron.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func writeBlueprintFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "blueprint.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	return path
}

func writeExecutableScript(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "script.sh")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

func waitRunStatus(t *testing.T, store *persistence.Store, runID, want string) persistence.RunRecord {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := store.GetRun(context.Background(), runID)
		if err == nil && rec.Status == want {
			return rec
		}
		time.Sleep(50 * time.Millisecond)
	}
	rec, _ := store.GetRun(context.Background(), runID)
	t.Fatalf("timed out waiting for run %s status=%s, got %s", runID, want, rec.Status)
	return persistence.RunRecord{}
}

func getRunEvents(t *testing.T, store *persistence.Store, runID string) []persistence.RunEventRecord {
	t.Helper()
	events, err := store.ListRunEvents(context.Background(), runID, 200)
	if err != nil {
		t.Fatalf("list run events: %v", err)
	}
	return events
}

func waitHumanGateID(t *testing.T, store *persistence.Store, runID string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		events := getRunEvents(t, store, runID)
		for _, e := range events {
			if e.EventType != "human_gate_waiting" || !e.Message.Valid {
				continue
			}
			if idx := strings.Index(e.Message.String, `"gate_id":"`); idx >= 0 {
				rest := e.Message.String[idx+len(`"gate_id":"`):]
				if end := strings.Index(rest, `"`); end >= 0 {
					return rest[:end]
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for human gate id")
	return ""
}

// ─── Test 1: hello-world blueprint ───────────────────────────────────────────

func TestIntegration_HelloWorld(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: hello-world
steps:
  - section: main
    tasks:
      - name: greet
        cmd: echo "hello hadron"
`)

	runID := "test-hw-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	found := false
	for _, e := range events {
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "hello hadron") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected 'hello hadron' in run events; got %d events", len(events))
	}
}

// ─── Test 2: retry on failure ─────────────────────────────────────────────────

func TestIntegration_RetryOnFailure(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: retry-test
steps:
  - section: main
    tasks:
      - name: fails-always
        cmd: exit 1
        retry: 2
`)

	runID := "test-retry-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "failed")
	if rec.Status != "failed" {
		t.Fatalf("expected failed, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	retryCount := 0
	for _, e := range events {
		if e.EventType == "step_retry" {
			retryCount++
		}
	}
	if retryCount < 2 {
		t.Fatalf("expected at least 2 retry events, got %d", retryCount)
	}
}

// ─── Test 3: condition skip ───────────────────────────────────────────────────

func TestIntegration_ConditionSkip(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: condition-skip
steps:
  - section: main
    tasks:
      - name: should-skip
        cmd: echo "should not run"
        if: "false"
      - name: should-run
        cmd: echo "did run"
`)

	runID := "test-cond-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	skipped := false
	for _, e := range events {
		if e.EventType == "step_skipped_condition" {
			skipped = true
			break
		}
	}
	if !skipped {
		t.Fatalf("expected step_skipped_condition event; got events: %v", eventTypes(events))
	}
}

// ─── Test 4: dry run ──────────────────────────────────────────────────────────

func TestIntegration_DryRun(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	touchPath := filepath.Join(t.TempDir(), "hadron-dry-run-test")
	bpPath := writeBlueprintFile(t, fmt.Sprintf(`
blueprint:
  name: dry-run-test
steps:
  - section: main
    tasks:
      - name: create-file
        cmd: touch %s
`, touchPath))

	runID := "test-dry-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
		DryRun:        true,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success (dry run), got %s", rec.Status)
	}

	if _, err := os.Stat(touchPath); err == nil {
		t.Fatalf("file should NOT have been created in dry-run mode")
	}

	events := getRunEvents(t, store, runID)
	found := false
	for _, e := range events {
		if e.EventType == "dry_run" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected dry_run event; got %v", eventTypes(events))
	}
}

func TestIntegration_HTTPCall(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("X-Test") != "hadron" {
			t.Fatalf("expected X-Test header, got %q", r.Header.Get("X-Test"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"source":"httptest"}`))
	}))
	defer srv.Close()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, fmt.Sprintf(`
blueprint:
  name: http-call
steps:
  - section: main
    tasks:
      - name: probe
        http_call:
          method: POST
          url: %q
          timeout_seconds: 5
          headers:
            X-Test: hadron
          body_json:
            ping: true
`, srv.URL))

	runID := "test-http-call-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawStart, sawResponse, sawStatusOutput, sawBodyJSONOutput bool
	for _, e := range events {
		if e.EventType == "http_call_start" {
			sawStart = true
		}
		if e.EventType == "http_call_response" && e.Message.Valid && strings.Contains(e.Message.String, `"status_code":200`) {
			sawResponse = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output status_code=200") {
			sawStatusOutput = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output body_json=") && strings.Contains(e.Message.String, `"ok":true`) {
			sawBodyJSONOutput = true
		}
	}
	if !sawStart || !sawResponse || !sawStatusOutput || !sawBodyJSONOutput {
		t.Fatalf("missing http_call events: start=%v response=%v status_output=%v body_json_output=%v", sawStart, sawResponse, sawStatusOutput, sawBodyJSONOutput)
	}
}

func TestIntegration_MCPCall(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	caller := &fakeMCPCaller{
		result: map[string]any{
			"count": 2,
			"items": []any{"run-1", "run-2"},
		},
	}
	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetMCPCaller(caller)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: mcp-call
inputs:
  - name: workspace_id
    type: string
steps:
  - section: main
    tasks:
      - name: list-runs
        mcp_call:
          server: torque
          tool: torque_runs_list
          arguments:
            workspace_id: "{{ .inputs.workspace_id }}"
            limit: 50
`)

	runID := "test-mcp-call-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{"workspace_id": "default"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
	if caller.server != "torque" || caller.tool != "torque_runs_list" {
		t.Fatalf("unexpected MCP call target: %s.%s", caller.server, caller.tool)
	}
	if caller.arguments["workspace_id"] != "default" {
		t.Fatalf("expected rendered workspace_id, got %+v", caller.arguments)
	}

	events := getRunEvents(t, store, runID)
	var sawStart, sawResult, sawOutput bool
	for _, e := range events {
		if e.EventType == "mcp_call_start" {
			sawStart = true
		}
		if e.EventType == "mcp_call_result" && e.Message.Valid && strings.Contains(e.Message.String, `\"count\":2`) {
			sawResult = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output result_json=") && strings.Contains(e.Message.String, `"run-1"`) {
			sawOutput = true
		}
	}
	if !sawStart || !sawResult || !sawOutput {
		t.Fatalf("missing mcp_call events: start=%v result=%v output=%v", sawStart, sawResult, sawOutput)
	}
}

func TestIntegration_MCPCallLocalHadron(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	internalAdapter := mcpadapter.New(store, mgr, nil, nil, "internal", mcpadapter.AllScopes())
	mgr.SetMCPCaller(mcpadapter.NewInternalCaller(internalAdapter))
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: mcp-call-local-hadron
steps:
  - section: main
    tasks:
      - name: health
        mcp_call:
          server: hadron
          tool: hadron_health
`)

	runID := "test-mcp-call-local-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawStart, sawTransport, sawResult, sawOutput bool
	for _, e := range events {
		if e.EventType == "mcp_call_start" && e.Message.Valid && strings.Contains(e.Message.String, "hadron.hadron_health") {
			sawStart = true
		}
		if e.EventType == "mcp_call_transport" && e.Message.Valid && strings.Contains(e.Message.String, `"transport":"in_process"`) {
			sawTransport = true
		}
		if e.EventType == "mcp_call_result" && e.Message.Valid && strings.Contains(e.Message.String, `\"status\":\"ok\"`) {
			sawResult = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output result_json=") && strings.Contains(e.Message.String, `"service":"hadron-mcp"`) {
			sawOutput = true
		}
	}
	if !sawStart || !sawTransport || !sawResult || !sawOutput {
		t.Fatalf("missing local mcp_call events: start=%v transport=%v result=%v output=%v", sawStart, sawTransport, sawResult, sawOutput)
	}
}

func TestIntegration_MCPCallObservabilityEvents(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetMCPCaller(&fakeMCPCaller{
		result: execution.MCPToolResult{
			Result: map[string]any{"ok": true},
			Metadata: execution.MCPCallMetadata{
				Server:       "fake-http",
				Transport:    "streamable_http",
				ReusedClient: true,
				HealthProbe:  true,
				Reconnected:  true,
				RetryCount:   1,
				AttemptCount: 2,
			},
		},
	})
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: mcp-call-observability
steps:
  - section: main
    tasks:
      - name: echo
        mcp_call:
          server: fake-http
          tool: echo_json
`)

	runID := "test-mcp-call-observability-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawTransport, sawRetry, sawReconnect, sawResult bool
	for _, e := range events {
		if e.EventType == "mcp_call_transport" && e.Message.Valid && strings.Contains(e.Message.String, `"transport":"streamable_http"`) && strings.Contains(e.Message.String, `"reused_client":true`) {
			sawTransport = true
		}
		if e.EventType == "mcp_call_retry" && e.Message.Valid && strings.Contains(e.Message.String, `"retry_count":1`) && strings.Contains(e.Message.String, `"attempt_count":2`) {
			sawRetry = true
		}
		if e.EventType == "mcp_call_reconnect" && e.Message.Valid && strings.Contains(e.Message.String, `"transport":"streamable_http"`) {
			sawReconnect = true
		}
		if e.EventType == "mcp_call_result" && e.Message.Valid && strings.Contains(e.Message.String, `"retry_count":1`) && strings.Contains(e.Message.String, `"reconnected":true`) {
			sawResult = true
		}
	}
	if !sawTransport || !sawRetry || !sawReconnect || !sawResult {
		t.Fatalf("missing observability events: transport=%v retry=%v reconnect=%v result=%v", sawTransport, sawRetry, sawReconnect, sawResult)
	}
}

func TestIntegration_MCPCallExternalStdio(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	internalAdapter := mcpadapter.New(store, mgr, nil, nil, "internal", mcpadapter.AllScopes())
	caller := mcpadapter.NewInternalCaller(internalAdapter, mcpadapter.WithExternalServers(map[string]mcpadapter.ExternalServerConfig{
		"fake": {
			Transport: "stdio",
			Command:   os.Args[0],
			Args:      []string{"-test.run=TestHelperProcessExternalMCPServer", "--"},
			Env:       map[string]string{"GO_WANT_HELPER_PROCESS_EXTERNAL_MCP": "1"},
		},
	}))
	mgr.SetMCPCaller(caller)
	defer func() { _ = caller.Close() }()
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: mcp-call-external-stdio
steps:
  - section: main
    tasks:
      - name: echo
        mcp_call:
          server: fake
          tool: echo_json
          arguments:
            name: hadron
`)

	runID := "test-mcp-call-external-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawStart, sawTransport, sawResult, sawOutput bool
	for _, e := range events {
		if e.EventType == "mcp_call_start" && e.Message.Valid && strings.Contains(e.Message.String, "fake.echo_json") {
			sawStart = true
		}
		if e.EventType == "mcp_call_transport" && e.Message.Valid && strings.Contains(e.Message.String, `"transport":"stdio"`) {
			sawTransport = true
		}
		if e.EventType == "mcp_call_result" && e.Message.Valid && strings.Contains(e.Message.String, `\"echo\":\"hadron\"`) {
			sawResult = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output result_json=") && strings.Contains(e.Message.String, `"server":"fake-helper"`) {
			sawOutput = true
		}
	}
	if !sawStart || !sawTransport || !sawResult || !sawOutput {
		t.Fatalf("missing external mcp_call events: start=%v transport=%v result=%v output=%v", sawStart, sawTransport, sawResult, sawOutput)
	}
}

func TestIntegration_MCPCallErrorContinueOnError(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetMCPCaller(&fakeMCPCaller{err: fmt.Errorf("tool failed")})
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: mcp-call-continue
steps:
  - section: main
    tasks:
      - name: failing-tool
        continue_on_error: true
        mcp_call:
          server: torque
          tool: torque_runs_list
      - name: next
        cmd: echo continued
`)

	runID := "test-mcp-call-continue-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawError, sawContinued bool
	for _, e := range events {
		if e.EventType == "mcp_call_error" && e.Message.Valid && strings.Contains(e.Message.String, "tool failed") {
			sawError = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "continued") {
			sawContinued = true
		}
	}
	if !sawError || !sawContinued {
		t.Fatalf("missing continue_on_error evidence: error=%v continued=%v", sawError, sawContinued)
	}
}

func TestIntegration_MCPCallErrorFailsRun(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetMCPCaller(&fakeMCPCaller{err: fmt.Errorf("tool failed")})
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: mcp-call-fails
steps:
  - section: main
    tasks:
      - name: failing-tool
        mcp_call:
          server: torque
          tool: torque_runs_list
`)

	runID := "test-mcp-call-fails-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "failed")
	if rec.Status != "failed" {
		t.Fatalf("expected failed, got %s", rec.Status)
	}
}

func TestIntegration_MessageWaitReply(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	source := &fakeMessageSource{
		replyAfter: 2,
		message: &execution.Message{
			ID:       "msg-1",
			Body:     `{"severity":"high"}`,
			BodyJSON: map[string]any{"severity": "high"},
		},
	}
	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetMessageSource(source)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: message-wait
inputs:
  - name: reply_mailbox
    type: string
  - name: correlation_id
    type: string
steps:
  - section: main
    tasks:
      - name: wait
        message_wait:
          substrate: tether
          to: "{{ .inputs.reply_mailbox }}"
          correlation_id: "{{ .inputs.correlation_id }}"
          timeout_seconds: 2
`)

	runID := "test-message-wait-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs: map[string]any{
			"reply_mailbox":  "mailbox://agent/replies",
			"correlation_id": "corr-123",
		},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
	if source.query.To != "mailbox://agent/replies" || source.query.CorrelationID != "corr-123" {
		t.Fatalf("unexpected query: %+v", source.query)
	}

	events := getRunEvents(t, store, runID)
	var sawStart, sawPoll, sawReply, sawMessageIDOutput, sawBodyJSONOutput bool
	for _, e := range events {
		if e.EventType == "message_wait_start" {
			sawStart = true
		}
		if e.EventType == "message_wait_poll" {
			sawPoll = true
		}
		if e.EventType == "message_wait_reply" && e.Message.Valid && strings.Contains(e.Message.String, `"message_id":"msg-1"`) {
			sawReply = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output message_id=msg-1") {
			sawMessageIDOutput = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output body_json=") && strings.Contains(e.Message.String, `"severity":"high"`) {
			sawBodyJSONOutput = true
		}
	}
	if !sawStart || !sawPoll || !sawReply || !sawMessageIDOutput || !sawBodyJSONOutput {
		t.Fatalf("missing message_wait events: start=%v poll=%v reply=%v id_output=%v body_json_output=%v", sawStart, sawPoll, sawReply, sawMessageIDOutput, sawBodyJSONOutput)
	}
}

func TestIntegration_MessageWaitTimeout(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetMessageSource(&fakeMessageSource{})
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: message-wait-timeout
steps:
  - section: main
    tasks:
      - name: wait
        message_wait:
          substrate: tether
          to: mailbox://agent/replies
          correlation_id: corr-123
          timeout_seconds: 1
`)

	runID := "test-message-wait-timeout-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "failed")
	if rec.Status != "failed" {
		t.Fatalf("expected failed, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	found := false
	for _, e := range events {
		if e.EventType == "message_wait_timeout" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected message_wait_timeout event; got %v", eventTypes(events))
	}
}

func TestIntegration_MessageWaitWithLocalMessageSubstrate(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	messageService := messagesubstrate.New(store, map[string]settings.MessageSubstrateSetting{
		"local_mailbox": {
			Kind:      "go_messaging",
			Authority: "hadron",
		},
	})
	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetMessageSource(messageService)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: local-message-wait
steps:
  - section: main
    tasks:
      - name: wait
        message_wait:
          substrate: local_mailbox
          to: msg://agent/hadron/reviewer-1
          correlation_id: review-123
          timeout_seconds: 5
          poll_interval_seconds: 0
`)

	runID := "test-message-wait-local-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	go func() {
		time.Sleep(150 * time.Millisecond)
		_, _ = messageService.Send(context.Background(), "local_mailbox", messaging.Envelope{
			Kind:        messaging.MsgKindNotice,
			From:        messaging.Address{Kind: messaging.KindService, Authority: "hadron", ID: "operator"},
			To:          messaging.Address{Kind: messaging.KindAgent, Authority: "hadron", ID: "reviewer-1"},
			ThreadID:    "review-123",
			Payload:     []byte(`{"approved":true}`),
			Metadata:    map[string]string{"correlation_id": "review-123"},
			ContentType: "application/json",
		})
	}()

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawReply, sawBodyJSONOutput bool
	for _, e := range events {
		if e.EventType == "message_wait_reply" && e.Message.Valid && strings.Contains(e.Message.String, `"body_json"`) {
			sawReply = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output body_json=") && strings.Contains(e.Message.String, `"approved":true`) {
			sawBodyJSONOutput = true
		}
	}
	if !sawReply || !sawBodyJSONOutput {
		t.Fatalf("missing local message_wait outputs: reply=%v body_json=%v", sawReply, sawBodyJSONOutput)
	}
}

func TestIntegration_MessageWaitWithRemoteHTTPSubstrate(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	reply := messaging.Envelope{
		ID:        "msg-remote-1",
		Kind:      messaging.MsgKindNotice,
		From:      messaging.Address{Kind: messaging.KindService, Authority: "agent-mux", ID: "operator"},
		To:        messaging.Address{Kind: messaging.KindAgent, Authority: "agent-mux", ID: "reviewer-1"},
		ThreadID:  "review-123",
		Payload:   json.RawMessage(`{"approved":true}`),
		Metadata:  map[string]string{"correlation_id": "review-123"},
		CreatedAt: time.Now().UTC(),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/messages/inbox":
			if got := r.URL.Query().Get("to"); got != "msg://agent/agent-mux/reviewer-1" {
				t.Fatalf("to = %q", got)
			}
			if got := r.URL.Query().Get("thread_id"); got != "review-123" {
				t.Fatalf("thread_id = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"messages": []messaging.Envelope{reply}})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetMessageSource(messagesubstrate.New(store, map[string]settings.MessageSubstrateSetting{
		"remote_mailbox": {
			Kind:       "tether_http",
			BaseURL:    server.URL,
			Authority:  "agent-mux",
			NotifyWake: true,
		},
	}))
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: wait-remote-message
steps:
  - section: main
    tasks:
      - name: wait-for-reply
        message_wait:
          substrate: remote_mailbox
          to: msg://agent/agent-mux/reviewer-1
          correlation_id: review-123
          timeout_seconds: 5
          poll_interval_seconds: 0
`)

	runID := "test-message-wait-remote-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
}

func TestIntegration_AgentLaunchThenMessageWaitUsingCallbackContract(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	dataDir := t.TempDir()
	scriptPath := writeExecutableScript(t, "#!/bin/sh\nsleep 1\n")
	launcher := agentsubstrate.NewLauncher(dataDir, map[string]settings.AgentSubstrateSettings{
		"local_runtime": {
			Kind:           "go_agent_runtime",
			Provider:       "codex",
			Runtime:        "subprocess",
			Command:        scriptPath,
			Authority:      "hadron",
			WorkingDirMode: "blueprint_dir",
			Boot: settings.AgentBootSettings{
				CallbacksProfile: "shared",
				PlantNativeFiles: true,
			},
		},
	})
	defer func() { _ = launcher.Close() }()

	messageService := messagesubstrate.New(store, map[string]settings.MessageSubstrateSetting{
		"local_mailbox": {Kind: "go_messaging", Authority: "hadron"},
	})

	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetAgentLauncher(launcher)
	mgr.SetMessageSource(messageService)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: launch-then-wait
steps:
  - section: main
    tasks:
      - name: launch
        agent_launch:
          substrate: local_runtime
          launch_id: review-sprint
          logical_agent_id: reviewer-1
          prompt_append: reply on the mailbox
      - name: wait
        message_wait:
          substrate: local_mailbox
          to: msg://agent/hadron/reviewer-1
          correlation_id: review-123
          timeout_seconds: 5
          poll_interval_seconds: 0
`)

	runID := "test-agent-launch-wait-callback-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			events, err := store.ListRunEvents(context.Background(), runID, 200)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			var bootDir string
			for _, e := range events {
				if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output boot_dir=") {
					bootDir = strings.TrimPrefix(e.Message.String, "::set-output boot_dir=")
					break
				}
			}
			if bootDir != "" {
				data, err := os.ReadFile(filepath.Join(bootDir, "hadron", "callbacks.json"))
				if err == nil {
					var details map[string]any
					if json.Unmarshal(data, &details) == nil {
						to, _ := details["mailbox_urn"].(string)
						addr, err := messaging.ParseURN(to)
						if err != nil {
							return
						}
						_, _ = messageService.Send(context.Background(), "local_mailbox", messaging.Envelope{
							Kind:        messaging.MsgKindNotice,
							From:        messaging.Address{Kind: messaging.KindService, Authority: "hadron", ID: "operator"},
							To:          addr,
							ThreadID:    "review-123",
							Payload:     json.RawMessage(`{"approved":true,"source":"callback-contract"}`),
							Metadata:    map[string]string{"correlation_id": "review-123"},
							ContentType: "application/json",
						})
						return
					}
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
}

func TestIntegration_AgentLaunch(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	launcher := &fakeAgentLauncher{
		result: execution.AgentLaunchResult{
			SessionID: "session-1",
			Mailbox:   "mailbox://agent/replies",
			Handles: map[string]any{
				"run_id": "agent-run-1",
			},
		},
	}
	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetAgentLauncher(launcher)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: agent-launch
inputs:
  - name: state_artifact
    type: string
steps:
  - section: main
    tasks:
      - name: launch
        agent_launch:
          substrate: tether
          launch_id: torque-monitor-correlator
          logical_agent_id: torque-monitor-correlator
          prompt_append: |
            Read the injected monitor artifacts.
          injection:
            native_files:
              - rel_path: context/torque-state.json
                source: "{{ .inputs.state_artifact }}"
          metadata:
            workflow: monitor
`)

	runID := "test-agent-launch-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{"state_artifact": "/tmp/torque-state.json"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
	if launcher.req.Substrate != "tether" || launcher.req.LaunchID != "torque-monitor-correlator" {
		t.Fatalf("unexpected launch request: %+v", launcher.req)
	}
	if got := launcher.req.Injection.NativeFiles[0].Source; got != "/tmp/torque-state.json" {
		t.Fatalf("expected rendered injection source, got %q", got)
	}

	events := getRunEvents(t, store, runID)
	var sawStart, sawResult, sawSessionOutput, sawMailboxOutput bool
	for _, e := range events {
		if e.EventType == "agent_launch_start" && e.Message.Valid && strings.Contains(e.Message.String, `"launch_id":"torque-monitor-correlator"`) {
			sawStart = true
		}
		if e.EventType == "agent_launch_result" && e.Message.Valid && strings.Contains(e.Message.String, `\"session_id\":\"session-1\"`) {
			sawResult = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output session_id=session-1") {
			sawSessionOutput = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output mailbox_urn=mailbox://agent/replies") {
			sawMailboxOutput = true
		}
	}
	if !sawStart || !sawResult || !sawSessionOutput || !sawMailboxOutput {
		t.Fatalf("missing agent_launch events: start=%v result=%v session=%v mailbox=%v", sawStart, sawResult, sawSessionOutput, sawMailboxOutput)
	}
}

func TestIntegration_AgentLaunchMissingLauncherFailsRun(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: agent-launch-missing-launcher
steps:
  - section: main
    tasks:
      - name: launch
        agent_launch:
          substrate: tether
          launch_id: launch-1
          logical_agent_id: agent-1
`)

	runID := "test-agent-launch-missing-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "failed")
	if rec.Status != "failed" {
		t.Fatalf("expected failed, got %s", rec.Status)
	}
}

func TestIntegration_AgentLaunchWithRuntimeLauncher(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	dataDir := t.TempDir()
	scriptPath := writeExecutableScript(t, "#!/bin/sh\nexit 0\n")
	launcher := agentsubstrate.NewLauncher(dataDir, map[string]settings.AgentSubstrateSettings{
		"local_runtime": {
			Kind:           "go_agent_runtime",
			Provider:       "codex",
			Runtime:        "subprocess",
			Command:        scriptPath,
			Authority:      "hadron",
			WorkingDirMode: "blueprint_dir",
			Boot: settings.AgentBootSettings{
				PlantNativeFiles: true,
			},
		},
	})
	defer func() { _ = launcher.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetAgentLauncher(launcher)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: runtime-agent-launch
steps:
  - section: main
    tasks:
      - name: launch
        agent_launch:
          substrate: local_runtime
          launch_id: reviewer
          logical_agent_id: reviewer-1
          prompt_append: |
            Review the injected file.
          injection:
            native_files:
              - rel_path: context/task.txt
                source: "inspect this file"
`)

	runID := "test-agent-launch-runtime-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawSessionURN, sawProvider, sawBootDir bool
	for _, e := range events {
		if e.EventType != "log" || !e.Message.Valid {
			continue
		}
		msg := e.Message.String
		if strings.Contains(msg, "::set-output session_urn=msg://session/hadron/") {
			sawSessionURN = true
		}
		if strings.Contains(msg, "::set-output provider=codex") {
			sawProvider = true
		}
		if strings.Contains(msg, "::set-output boot_dir=") {
			sawBootDir = true
		}
	}
	if !sawSessionURN || !sawProvider || !sawBootDir {
		t.Fatalf("missing runtime launcher outputs: session_urn=%v provider=%v boot_dir=%v", sawSessionURN, sawProvider, sawBootDir)
	}
}

func TestIntegration_AgentLaunchTimeout(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	launcher := &fakeAgentLauncher{blockUntilCtx: true}
	mgr := execution.NewManager(store, nil, 1, "", nil)
	mgr.SetAgentLauncher(launcher)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: agent-launch-timeout
steps:
  - section: main
    tasks:
      - name: launch
        timeout_seconds: 1
        agent_launch:
          substrate: tether
          launch_id: launch-1
          logical_agent_id: agent-1
`)

	runID := "test-agent-launch-timeout-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "failed")
	if rec.Status != "failed" {
		t.Fatalf("expected failed, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawDeadline bool
	for _, e := range events {
		if e.EventType == "agent_launch_error" && e.Message.Valid && strings.Contains(e.Message.String, "context deadline exceeded") {
			sawDeadline = true
			break
		}
	}
	if !sawDeadline {
		t.Fatalf("expected context deadline exceeded in agent_launch_error, got %v", eventTypes(events))
	}
}

func TestIntegration_HumanGateDecision(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: human-gate
steps:
  - section: main
    tasks:
      - name: approve
        human_gate:
          prompt: "Approve remediation?"
          options:
            - id: approve
              label: Approve
            - id: deny
              label: Deny
          timeout_seconds: 5
`)

	runID := "test-human-gate-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	gateID := waitHumanGateID(t, store, runID)
	if err := store.SubmitHumanGateDecision(context.Background(), gateID, "approve", time.Now().UTC()); err != nil {
		t.Fatalf("submit decision: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	var sawWaiting, sawDecision, sawOutput bool
	for _, e := range events {
		if e.EventType == "human_gate_waiting" && e.Message.Valid && strings.Contains(e.Message.String, gateID) {
			sawWaiting = true
		}
		if e.EventType == "human_gate_decision" && e.Message.Valid && strings.Contains(e.Message.String, `"decision":"approve"`) {
			sawDecision = true
		}
		if e.EventType == "log" && e.Message.Valid && strings.Contains(e.Message.String, "::set-output decision=approve") {
			sawOutput = true
		}
	}
	if !sawWaiting || !sawDecision || !sawOutput {
		t.Fatalf("missing human_gate events: waiting=%v decision=%v output=%v", sawWaiting, sawDecision, sawOutput)
	}
}

func TestIntegration_HumanGateTimeout(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: human-gate-timeout
steps:
  - section: main
    tasks:
      - name: approve
        human_gate:
          prompt: "Approve remediation?"
          options:
            - id: approve
              label: Approve
          timeout_seconds: 1
`)

	runID := "test-human-gate-timeout-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "failed")
	if rec.Status != "failed" {
		t.Fatalf("expected failed, got %s", rec.Status)
	}
	events := getRunEvents(t, store, runID)
	found := false
	for _, e := range events {
		if e.EventType == "human_gate_timeout" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected human_gate_timeout event; got %v", eventTypes(events))
	}
}

// ─── Test 5: on_fail hook ─────────────────────────────────────────────────────

func TestIntegration_OnFailHook(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()

	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	bpPath := writeBlueprintFile(t, `
blueprint:
  name: on-fail-hook
steps:
  - section: main
    tasks:
      - name: fails
        cmd: exit 1
        continue_on_error: true
        on_fail:
          - type: cmd
            value: echo "failed-hook"
`)

	runID := "test-hook-001"
	if err := mgr.Enqueue(context.Background(), execution.Request{
		RunID:         runID,
		BlueprintPath: bpPath,
		Inputs:        map[string]any{},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	rec := waitRunStatus(t, store, runID, "success")
	if rec.Status != "success" {
		t.Fatalf("expected success (continue_on_error), got %s", rec.Status)
	}

	events := getRunEvents(t, store, runID)
	found := false
	for _, e := range events {
		if e.EventType == "hook_output" && e.Message.Valid && strings.Contains(e.Message.String, "failed-hook") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected 'failed-hook' in hook_output events; got %v", eventTypes(events))
	}
}

func eventTypes(events []persistence.RunEventRecord) []string {
	out := make([]string, len(events))
	for i, e := range events {
		msg := ""
		if e.Message.Valid {
			msg = e.Message.String
		}
		out[i] = fmt.Sprintf("%s:%s", e.EventType, msg)
	}
	return out
}

type fakeMCPCaller struct {
	server    string
	tool      string
	arguments map[string]any
	result    any
	err       error
}

func (f *fakeMCPCaller) CallTool(_ context.Context, server, tool string, arguments map[string]any) (any, error) {
	f.server = server
	f.tool = tool
	f.arguments = arguments
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

type fakeMessageSource struct {
	query      execution.MessageQuery
	polls      int
	replyAfter int
	message    *execution.Message
	err        error
}

func (f *fakeMessageSource) PollMessage(_ context.Context, query execution.MessageQuery) (*execution.Message, error) {
	f.query = query
	f.polls++
	if f.err != nil {
		return nil, f.err
	}
	if f.message != nil && f.polls >= f.replyAfter {
		return f.message, nil
	}
	return nil, nil
}

type fakeAgentLauncher struct {
	req           execution.AgentLaunchRequest
	result        execution.AgentLaunchResult
	err           error
	blockUntilCtx bool
}

func (f *fakeAgentLauncher) LaunchAgent(ctx context.Context, req execution.AgentLaunchRequest) (execution.AgentLaunchResult, error) {
	f.req = req
	if f.blockUntilCtx {
		<-ctx.Done()
		return execution.AgentLaunchResult{}, ctx.Err()
	}
	if f.err != nil {
		return execution.AgentLaunchResult{}, f.err
	}
	return f.result, nil
}

func TestHelperProcessExternalMCPServer(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS_EXTERNAL_MCP") != "1" {
		return
	}
	s := server.NewMCPServer("fake-helper", "1.0.0", server.WithToolCapabilities(true))
	s.AddTool(mcp.NewTool("echo_json",
		mcp.WithString("name", mcp.Required()),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		payload := fmt.Sprintf(`{"echo":%q,"server":"fake-helper"}`, req.GetString("name", ""))
		return mcp.NewToolResultText(payload), nil
	})
	if err := server.ServeStdio(s); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
