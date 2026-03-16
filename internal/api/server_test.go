package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/hollis-labs/hadron/internal/api"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/pipeline"
	"github.com/hollis-labs/hadron/internal/scheduler"
)

// ── Test helpers ──────────────────────────────────────────────────────────────

func openTestStore(t *testing.T) *persistence.Store {
	t.Helper()
	store, err := persistence.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
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
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
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

func newTestServerWithBlueprintDir(t *testing.T, bpDir string) *httptest.Server {
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
	if err := os.WriteFile(filepath.Join(bpDir, "smoke-test.yaml"), []byte(bpContent), 0o644); err != nil {
		t.Fatal(err)
	}

	ts := newTestServerWithBlueprintDir(t, bpDir)
	defer ts.Close()

	resp := doRequest(t, ts, "POST", "/v1/triggers/smoke-test", map[string]any{
		"inputs": map[string]any{},
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
		t.Fatal("missing run id in trigger response")
	}
	if run["status"] != "queued" {
		t.Fatalf("expected status=queued, got %v", run["status"])
	}
}

func TestTriggerNotFound(t *testing.T) {
	bpDir := t.TempDir()
	ts := newTestServerWithBlueprintDir(t, bpDir)
	defer ts.Close()

	resp := doRequest(t, ts, "POST", "/v1/triggers/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestTriggerMethodNotAllowed(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	resp := doRequest(t, ts, "GET", "/v1/triggers/anything", nil)
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
	if err := os.WriteFile(filepath.Join(subDir, "md-to-pdf.yaml"), []byte(bpContent), 0o644); err != nil {
		t.Fatal(err)
	}

	ts := newTestServerWithBlueprintDir(t, bpDir)
	defer ts.Close()

	resp := doRequest(t, ts, "POST", "/v1/triggers/md-to-pdf", map[string]any{
		"inputs": map[string]any{},
	})
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
