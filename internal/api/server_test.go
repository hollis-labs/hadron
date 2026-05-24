package api_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/messagesubstrate"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/hollis-labs/hadron/internal/settings"
)

// ── Test helpers ──────────────────────────────────────────────────────────────

func openTestStore(t *testing.T) *persistence.Store {
	t.Helper()
	store, err := persistence.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	store := openTestStore(t)
	mgr := execution.NewManager(store, nil, 1, "", nil)
	t.Cleanup(mgr.Close)

	sched := scheduler.New(store, mgr)
	pipelineRunner := pipeline.NewRunner(store, mgr)
	messageService := messagesubstrate.New(store, map[string]settings.MessageSubstrateSetting{
		"local_mailbox": {Kind: "go_messaging", Authority: "hadron"},
	})

	srv := api.NewServer("", api.Dependencies{
		Runs:       store,
		Schedules:  store,
		Pipelines:  store,
		Workspaces: store,
		HumanGates: store,
		Messages:   messageService,
		Runner:     mgr,
		Scheduler:  sched,
		Pipeline:   pipelineRunner,
	})

	return httptest.NewServer(srv.Handler())
}

func doRequest(t *testing.T, ts *httptest.Server, method, path string, body any) *http.Response {
	t.Helper()
	var reqBody bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&reqBody).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req, err := http.NewRequest(method, ts.URL+path, &reqBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response, out any) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func sqlNullString(v string) sql.NullString {
	return sql.NullString{String: v, Valid: v != ""}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestHealth(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp := doRequest(t, ts, "GET", "/v1/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result map[string]any
	decodeJSON(t, resp, &result)
	if result["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", result["status"])
	}
}

func TestCreateWorkspace(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp := doRequest(t, ts, "POST", "/v1/workspaces", map[string]string{"id": "test-ws", "name": "Test"})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	var ws map[string]any
	decodeJSON(t, resp, &ws)
	if ws["id"] != "test-ws" {
		t.Fatalf("expected id=test-ws, got %v", ws["id"])
	}

	// Verify it appears in list
	listResp := doRequest(t, ts, "GET", "/v1/workspaces", nil)
	var list map[string]any
	decodeJSON(t, listResp, &list)
	items, _ := list["items"].([]any)
	found := false
	for _, item := range items {
		if m, ok := item.(map[string]any); ok && m["id"] == "test-ws" {
			found = true
		}
	}
	if !found {
		t.Fatal("workspace not found in list")
	}
}

func TestMessagesSendGetAndInbox(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	sendBody := map[string]any{
		"substrate": "local_mailbox",
		"kind":      "notice",
		"from":      "msg://service/hadron/operator",
		"to":        "msg://agent/hadron/reviewer-1",
		"thread_id": "review-123",
		"payload":   map[string]any{"approved": true},
		"metadata":  map[string]string{"correlation_id": "review-123"},
	}
	resp := doRequest(t, ts, "POST", "/v1/messages", sendBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	var created map[string]any
	decodeJSON(t, resp, &created)
	messageID, _ := created["id"].(string)
	if messageID == "" {
		t.Fatal("expected message id")
	}

	resp = doRequest(t, ts, "GET", "/v1/messages/"+messageID, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var fetched map[string]any
	decodeJSON(t, resp, &fetched)
	if fetched["thread_id"] != "review-123" {
		t.Fatalf("expected thread_id review-123, got %v", fetched["thread_id"])
	}

	resp = doRequest(t, ts, "GET", "/v1/messages/inbox?substrate=local_mailbox&to=msg://agent/hadron/reviewer-1&correlation_id=review-123", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var inbox map[string]any
	decodeJSON(t, resp, &inbox)
	if inbox["count"] != float64(1) {
		t.Fatalf("expected count=1, got %v", inbox["count"])
	}
	msgs, _ := inbox["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 inbox message, got %d", len(msgs))
	}
}

func TestRunMCPCallsEndpoint(t *testing.T) {
	store := openTestStore(t)
	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	sched := scheduler.New(store, mgr)
	pipelineRunner := pipeline.NewRunner(store, mgr)
	ts := httptest.NewServer(api.NewServer("", api.Dependencies{
		Runs:       store,
		Schedules:  store,
		Pipelines:  store,
		Workspaces: store,
		HumanGates: store,
		Runner:     mgr,
		Scheduler:  sched,
		Pipeline:   pipelineRunner,
	}).Handler())
	defer ts.Close()

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
	appendEvent := func(eventType, message string, step string, at time.Time) {
		t.Helper()
		if err := store.AppendRunEvent(context.Background(), persistence.RunEventRecord{
			RunID:     "run-mcp-1",
			StepName:  sqlNullString(step),
			EventType: eventType,
			Message:   sqlNullString(message),
			CreatedAt: at,
		}); err != nil {
			t.Fatalf("append event %s: %v", eventType, err)
		}
	}
	appendEvent("mcp_call_start", "fake.echo_json", "echo", now)
	appendEvent("mcp_call_transport", `{"server":"fake","tool":"echo_json","transport":"streamable_http","reused_client":true,"health_probe":true}`, "echo", now.Add(time.Millisecond))
	appendEvent("mcp_call_retry", `{"server":"fake","tool":"echo_json","transport":"streamable_http","retry_count":1,"attempt_count":2}`, "echo", now.Add(2*time.Millisecond))
	appendEvent("mcp_call_reconnect", `{"server":"fake","tool":"echo_json","transport":"streamable_http","health_probe":true}`, "echo", now.Add(3*time.Millisecond))
	appendEvent("mcp_call_result", `{"server":"fake","tool":"echo_json","transport":"streamable_http","result_json":"{\"ok\":true}","retry_count":1,"attempt_count":2,"reconnected":true}`, "echo", now.Add(4*time.Millisecond))

	resp := doRequest(t, ts, "GET", "/v1/runs/run-mcp-1/mcp-calls", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]any
	decodeJSON(t, resp, &body)
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected 1 item, got %#v", body["items"])
	}
	item := items[0].(map[string]any)
	if item["transport"] != "streamable_http" || item["retry_count"] != float64(1) || item["reconnected"] != true {
		t.Fatalf("unexpected item: %#v", item)
	}
}

func TestRunOperationsEndpoint(t *testing.T) {
	store := openTestStore(t)
	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	sched := scheduler.New(store, mgr)
	pipelineRunner := pipeline.NewRunner(store, mgr)
	ts := httptest.NewServer(api.NewServer("", api.Dependencies{
		Runs:       store,
		Schedules:  store,
		Pipelines:  store,
		Workspaces: store,
		HumanGates: store,
		Runner:     mgr,
		Scheduler:  sched,
		Pipeline:   pipelineRunner,
	}).Handler())
	defer ts.Close()

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
			StepName:  sqlNullString(step),
			EventType: eventType,
			Message:   sqlNullString(message),
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

	resp := doRequest(t, ts, "GET", "/v1/runs/run-ops-1/operations?kind=message_wait&limit=1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]any
	decodeJSON(t, resp, &body)
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected 1 item, got %#v", body["items"])
	}
	if body["total_count"] != float64(1) || body["next_cursor"] != nil {
		t.Fatalf("unexpected paging envelope: %#v", body)
	}
	item := items[0].(map[string]any)
	if item["kind"] != "message_wait" || item["message_id"] != "msg-1" || item["poll_count"] != float64(1) {
		t.Fatalf("unexpected message_wait item: %#v", item)
	}
}

func TestRunOperationsEndpointPagination(t *testing.T) {
	store := openTestStore(t)
	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	sched := scheduler.New(store, mgr)
	pipelineRunner := pipeline.NewRunner(store, mgr)
	ts := httptest.NewServer(api.NewServer("", api.Dependencies{
		Runs:       store,
		Schedules:  store,
		Pipelines:  store,
		Workspaces: store,
		HumanGates: store,
		Runner:     mgr,
		Scheduler:  sched,
		Pipeline:   pipelineRunner,
	}).Handler())
	defer ts.Close()

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
			StepName:  sqlNullString(step),
			EventType: "http_call_start",
			Message:   sqlNullString("GET http://127.0.0.1:7777/health"),
			CreatedAt: at,
		}); err != nil {
			t.Fatalf("append start: %v", err)
		}
		if err := store.AppendRunEvent(context.Background(), persistence.RunEventRecord{
			RunID:     "run-ops-page-1",
			StepName:  sqlNullString(step),
			EventType: "http_call_response",
			Message:   sqlNullString(`{"status_code":200}`),
			CreatedAt: at.Add(time.Microsecond),
		}); err != nil {
			t.Fatalf("append response: %v", err)
		}
	}

	resp := doRequest(t, ts, "GET", "/v1/runs/run-ops-page-1/operations?kind=http_call&limit=2", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]any
	decodeJSON(t, resp, &body)
	items, ok := body["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 items, got %#v", body["items"])
	}
	cursor, _ := body["next_cursor"].(string)
	if cursor == "" || body["total_count"] != float64(3) {
		t.Fatalf("unexpected first page envelope: %#v", body)
	}

	resp2 := doRequest(t, ts, "GET", "/v1/runs/run-ops-page-1/operations?kind=http_call&limit=2&cursor="+cursor, nil)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}
	var body2 map[string]any
	decodeJSON(t, resp2, &body2)
	items2, ok := body2["items"].([]any)
	if !ok || len(items2) != 1 || body2["next_cursor"] != nil {
		t.Fatalf("unexpected second page: %#v", body2)
	}
}

func TestRunOperationsEndpointRejectsInvalidCursor(t *testing.T) {
	store := openTestStore(t)
	mgr := execution.NewManager(store, nil, 1, "", nil)
	defer mgr.Close()

	sched := scheduler.New(store, mgr)
	pipelineRunner := pipeline.NewRunner(store, mgr)
	ts := httptest.NewServer(api.NewServer("", api.Dependencies{
		Runs:       store,
		Schedules:  store,
		Pipelines:  store,
		Workspaces: store,
		HumanGates: store,
		Runner:     mgr,
		Scheduler:  sched,
		Pipeline:   pipelineRunner,
	}).Handler())
	defer ts.Close()
	now := time.Now().UTC()
	if err := store.CreateRun(context.Background(), persistence.RunRecord{
		ID:            "run-ops-bad-cursor",
		WorkspaceID:   "default",
		BlueprintPath: "test.yaml",
		Status:        "success",
		InputJSON:     "{}",
		CreatedAt:     now,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	resp := doRequest(t, ts, "GET", "/v1/runs/run-ops-bad-cursor/operations?cursor=bad", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestCreateRun(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	// Write a temp blueprint
	bpPath := filepath.Join(t.TempDir(), "test.yaml")
	bpContent := "blueprint:\n  name: smoke-test\nsteps:\n  - section: main\n    tasks:\n      - name: greet\n        cmd: echo hello\n"
	if err := os.WriteFile(bpPath, []byte(bpContent), 0o644); err != nil {
		t.Fatal(err)
	}

	resp := doRequest(t, ts, "POST", "/v1/runs", map[string]any{
		"blueprint_path": bpPath,
		"workspace_id":   "default",
		"inputs":         map[string]any{},
	})
	if resp.StatusCode != http.StatusAccepted {
		var errResp map[string]any
		decodeJSON(t, resp, &errResp)
		t.Fatalf("expected 202, got %d: %v", resp.StatusCode, errResp)
	}
	var run map[string]any
	decodeJSON(t, resp, &run)
	runID, _ := run["id"].(string)
	if runID == "" {
		t.Fatal("missing run id in response")
	}

	// GET the run
	getResp := doRequest(t, ts, "GET", "/v1/runs/"+runID, nil)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}
	var runGet map[string]any
	decodeJSON(t, getResp, &runGet)
	if runGet["id"] != runID {
		t.Fatalf("expected id=%s, got %v", runID, runGet["id"])
	}
}

func TestBlueprintValidate_Valid(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	validYAML := []byte("blueprint:\n  name: valid-test\nsteps:\n  - section: main\n    tasks:\n      - name: greet\n        cmd: echo hello\n")

	resp, err := http.Post(ts.URL+"/v1/blueprints/validate", "application/yaml", bytes.NewReader(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result map[string]any
	decodeJSON(t, resp, &result)
	if result["valid"] != true {
		t.Fatalf("expected valid=true, got %v", result)
	}
}

func TestBlueprintValidate_Invalid(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	invalidYAML := []byte("not_valid: yaml_content_without_blueprint_key")

	resp, err := http.Post(ts.URL+"/v1/blueprints/validate", "application/yaml", bytes.NewReader(invalidYAML))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var result map[string]any
	decodeJSON(t, resp, &result)
	if result["valid"] != false {
		t.Fatalf("expected valid=false, got %v", result)
	}
	if result["error"] == "" || result["error"] == nil {
		t.Fatalf("expected error message, got %v", result)
	}
}

func TestCreateSchedule(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp := doRequest(t, ts, "POST", "/v1/schedules", map[string]any{
		"blueprint_path": "/tmp/test.yaml",
		"cron_expr":      "0 * * * *",
		"name":           "test-sched",
		"enabled":        true,
	})
	if resp.StatusCode != http.StatusCreated {
		var errResp map[string]any
		decodeJSON(t, resp, &errResp)
		t.Fatalf("expected 201, got %d: %v", resp.StatusCode, errResp)
	}
	var sc map[string]any
	decodeJSON(t, resp, &sc)
	schedID, _ := sc["id"].(string)
	if schedID == "" {
		t.Fatal("missing schedule id")
	}

	// Patch: disable
	patchResp := doRequest(t, ts, "PATCH", "/v1/schedules/"+schedID, map[string]any{"enabled": false})
	if patchResp.StatusCode != http.StatusOK {
		var errResp map[string]any
		decodeJSON(t, patchResp, &errResp)
		t.Fatalf("expected 200 on patch, got %d: %v", patchResp.StatusCode, errResp)
	}
	var patched map[string]any
	decodeJSON(t, patchResp, &patched)
	if patched["enabled"] != false {
		t.Fatalf("expected enabled=false, got %v", patched["enabled"])
	}
}

func newTestServerWithTriggers(t *testing.T, bpDir string) *httptest.Server {
	t.Helper()
	store := openTestStore(t)
	mgr := execution.NewManager(store, nil, 1, "", nil)
	t.Cleanup(mgr.Close)

	sched := scheduler.New(store, mgr)
	pipelineRunner := pipeline.NewRunner(store, mgr)

	srv := api.NewServer("", api.Dependencies{
		Runs:         store,
		Schedules:    store,
		Pipelines:    store,
		Workspaces:   store,
		Triggers:     store,
		HumanGates:   store,
		Runner:       mgr,
		Scheduler:    sched,
		Pipeline:     pipelineRunner,
		BlueprintDir: bpDir,
	})

	return httptest.NewServer(srv.Handler())
}

func TestTriggerByName(t *testing.T) {
	bpDir := t.TempDir()
	bpContent := "blueprint:\n  name: smoke-test\nsteps:\n  - section: main\n    tasks:\n      - name: greet\n        cmd: echo hello\n"
	bpPath := filepath.Join(bpDir, "smoke-test.yaml")
	if err := os.WriteFile(bpPath, []byte(bpContent), 0o644); err != nil {
		t.Fatal(err)
	}

	ts := newTestServerWithTriggers(t, bpDir)
	defer ts.Close()

	// Create a webhook trigger
	resp := doRequest(t, ts, "POST", "/v1/triggers", map[string]any{
		"name":           "smoke",
		"path":           "smoke-test",
		"blueprint_path": bpPath,
	})
	if resp.StatusCode != http.StatusCreated {
		var errResp map[string]any
		decodeJSON(t, resp, &errResp)
		t.Fatalf("expected 201, got %d: %v", resp.StatusCode, errResp)
	}
	var trigger map[string]any
	decodeJSON(t, resp, &trigger)
	trigID, _ := trigger["id"].(string)
	if trigID == "" {
		t.Fatal("missing trigger id")
	}

	// Fire the webhook
	resp = doRequest(t, ts, "POST", "/hooks/smoke-test", map[string]any{})
	if resp.StatusCode != http.StatusAccepted {
		var errResp map[string]any
		decodeJSON(t, resp, &errResp)
		t.Fatalf("expected 202, got %d: %v", resp.StatusCode, errResp)
	}
	var run map[string]any
	decodeJSON(t, resp, &run)
	runID, _ := run["run_id"].(string)
	if runID == "" {
		t.Fatal("missing run_id in webhook response")
	}
}

func TestTriggerNotFound(t *testing.T) {
	bpDir := t.TempDir()
	ts := newTestServerWithTriggers(t, bpDir)
	defer ts.Close()

	resp := doRequest(t, ts, "POST", "/hooks/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestTriggerMethodNotAllowed(t *testing.T) {
	ts := newTestServerWithTriggers(t, "")
	defer ts.Close()

	resp := doRequest(t, ts, "GET", "/hooks/anything", nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestTriggerSubdirectory(t *testing.T) {
	bpDir := t.TempDir()
	subDir := filepath.Join(bpDir, "output")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bpContent := "blueprint:\n  name: md-to-pdf\nsteps:\n  - section: main\n    tasks:\n      - name: convert\n        cmd: echo converting\n"
	bpPath := filepath.Join(subDir, "md-to-pdf.yaml")
	if err := os.WriteFile(bpPath, []byte(bpContent), 0o644); err != nil {
		t.Fatal(err)
	}

	ts := newTestServerWithTriggers(t, bpDir)
	defer ts.Close()

	// Create trigger
	resp := doRequest(t, ts, "POST", "/v1/triggers", map[string]any{
		"name":           "md-to-pdf",
		"path":           "md-to-pdf",
		"blueprint_path": bpPath,
	})
	if resp.StatusCode != http.StatusCreated {
		var errResp map[string]any
		decodeJSON(t, resp, &errResp)
		t.Fatalf("expected 201, got %d: %v", resp.StatusCode, errResp)
	}

	// Fire webhook
	resp = doRequest(t, ts, "POST", "/hooks/md-to-pdf", map[string]any{})
	if resp.StatusCode != http.StatusAccepted {
		var errResp map[string]any
		decodeJSON(t, resp, &errResp)
		t.Fatalf("expected 202, got %d: %v", resp.StatusCode, errResp)
	}
}

func TestUnknownRoute(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp := doRequest(t, ts, "GET", "/v1/does-not-exist", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	var result map[string]any
	decodeJSON(t, resp, &result)
	if result["error"] == nil {
		t.Fatal("expected error field in 404 response")
	}
}

func TestHumanGateDecisionEndpoint(t *testing.T) {
	store := openTestStore(t)
	mgr := execution.NewManager(store, nil, 1, "", nil)
	t.Cleanup(mgr.Close)
	sched := scheduler.New(store, mgr)
	pipelineRunner := pipeline.NewRunner(store, mgr)
	srv := api.NewServer("", api.Dependencies{
		Runs:       store,
		Schedules:  store,
		Pipelines:  store,
		Workspaces: store,
		HumanGates: store,
		Runner:     mgr,
		Scheduler:  sched,
		Pipeline:   pipelineRunner,
	})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	now := time.Now().UTC()
	if err := store.CreateHumanGate(context.Background(), persistence.HumanGateRecord{
		ID:          "gate-api-1",
		WorkspaceID: "default",
		RunID:       "run-1",
		StepName:    "approve",
		Prompt:      "Approve?",
		OptionsJSON: `[{"id":"approve","label":"Approve"}]`,
		Status:      "waiting",
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("create gate: %v", err)
	}

	resp := doRequest(t, ts, "POST", "/v1/human-gates/gate-api-1/decision", map[string]any{"decision": "approve"})
	if resp.StatusCode != http.StatusOK {
		var errResp map[string]any
		decodeJSON(t, resp, &errResp)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, errResp)
	}
	var result map[string]any
	decodeJSON(t, resp, &result)
	if result["status"] != "decided" || result["decision"] != "approve" {
		t.Fatalf("unexpected response: %+v", result)
	}
}
