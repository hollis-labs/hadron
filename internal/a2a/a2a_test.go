package a2a_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/a2a"
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

// writeBlueprintFile creates a minimal blueprint YAML file in the given dir.
func writeBlueprintFile(t *testing.T, dir, name string) string {
	t.Helper()
	content := `version: "0.4"
blueprint:
  name: ` + name + `
  slug: ` + name + `
  title: Test Blueprint
  description: A test blueprint for A2A
inputs:
  - name: message
    type: string
    required: false
    default: hello
steps:
  - section: main
    tasks:
      - name: echo
        cmd: echo "done"
`
	path := filepath.Join(dir, name+".yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	return path
}

func newTestServerWithBlueprints(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	store := openTestStore(t)
	mgr := execution.NewManager(store, nil, 2, "", nil)
	t.Cleanup(mgr.Close)

	bpDir := filepath.Join(t.TempDir(), "blueprints")
	if err := os.MkdirAll(bpDir, 0o755); err != nil {
		t.Fatalf("mkdir blueprints: %v", err)
	}

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

	return httptest.NewServer(srv.Handler()), bpDir
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

func TestSubmitTask_ValidSkill(t *testing.T) {
	ts, bpDir := newTestServerWithBlueprints(t)
	defer ts.Close()

	writeBlueprintFile(t, bpDir, "greet")

	resp := doRequest(t, ts, "POST", "/a2a/tasks", a2a.TaskRequest{
		Skill: "greet",
		Input: map[string]any{"message": "world"},
	})
	if resp.StatusCode != http.StatusCreated {
		var errResp map[string]any
		decodeJSON(t, resp, &errResp)
		t.Fatalf("expected 201, got %d: %v", resp.StatusCode, errResp)
	}

	var result a2a.TaskResponse
	decodeJSON(t, resp, &result)

	if result.ID == "" {
		t.Fatal("expected non-empty task ID")
	}
	if result.Status.State != "submitted" {
		t.Fatalf("expected state=submitted, got %s", result.Status.State)
	}
}

func TestSubmitTask_UnknownSkill(t *testing.T) {
	ts, _ := newTestServerWithBlueprints(t)
	defer ts.Close()

	resp := doRequest(t, ts, "POST", "/a2a/tasks", a2a.TaskRequest{
		Skill: "nonexistent-skill",
	})
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	var errResp map[string]any
	decodeJSON(t, resp, &errResp)
	if errResp["error"] == nil || errResp["error"] == "" {
		t.Fatal("expected error message in response")
	}
}

func TestSubmitTask_MissingSkill(t *testing.T) {
	ts, _ := newTestServerWithBlueprints(t)
	defer ts.Close()

	resp := doRequest(t, ts, "POST", "/a2a/tasks", a2a.TaskRequest{})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestGetTask_ReturnsStatus(t *testing.T) {
	ts, bpDir := newTestServerWithBlueprints(t)
	defer ts.Close()

	writeBlueprintFile(t, bpDir, "status-check")

	// Submit a task.
	submitResp := doRequest(t, ts, "POST", "/a2a/tasks", a2a.TaskRequest{
		Skill: "status-check",
		Input: map[string]any{"message": "test"},
	})
	if submitResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", submitResp.StatusCode)
	}
	var submitted a2a.TaskResponse
	decodeJSON(t, submitResp, &submitted)

	// Get the task — should return a valid state.
	getResp := doRequest(t, ts, "GET", "/a2a/tasks/"+submitted.ID, nil)
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}
	var got a2a.TaskResponse
	decodeJSON(t, getResp, &got)

	if got.ID != submitted.ID {
		t.Fatalf("expected ID=%s, got %s", submitted.ID, got.ID)
	}
	validStates := map[string]bool{
		"submitted": true, "working": true, "completed": true, "failed": true, "canceled": true,
	}
	if !validStates[got.Status.State] {
		t.Fatalf("unexpected state: %s", got.Status.State)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	ts, _ := newTestServerWithBlueprints(t)
	defer ts.Close()

	resp := doRequest(t, ts, "GET", "/a2a/tasks/does-not-exist", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestSubmitTask_MethodNotAllowed(t *testing.T) {
	ts, _ := newTestServerWithBlueprints(t)
	defer ts.Close()

	resp := doRequest(t, ts, "GET", "/a2a/tasks", nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestGetTask_MethodNotAllowed(t *testing.T) {
	ts, _ := newTestServerWithBlueprints(t)
	defer ts.Close()

	resp := doRequest(t, ts, "DELETE", "/a2a/tasks/some-id", nil)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestTaskLifecycle_SubmitAndWaitForCompletion(t *testing.T) {
	ts, bpDir := newTestServerWithBlueprints(t)
	defer ts.Close()

	writeBlueprintFile(t, bpDir, "lifecycle")

	// Submit.
	submitResp := doRequest(t, ts, "POST", "/a2a/tasks", a2a.TaskRequest{
		ID:    "my-custom-id",
		Skill: "lifecycle",
		Input: map[string]any{"message": "lifecycle test"},
	})
	if submitResp.StatusCode != http.StatusCreated {
		var errResp map[string]any
		decodeJSON(t, submitResp, &errResp)
		t.Fatalf("expected 201, got %d: %v", submitResp.StatusCode, errResp)
	}
	var submitted a2a.TaskResponse
	decodeJSON(t, submitResp, &submitted)

	if submitted.ID != "my-custom-id" {
		t.Fatalf("expected ID=my-custom-id, got %s", submitted.ID)
	}

	// Poll until completed or timeout.
	deadline := time.Now().Add(10 * time.Second)
	var finalState string
	for time.Now().Before(deadline) {
		getResp := doRequest(t, ts, "GET", "/a2a/tasks/"+submitted.ID, nil)
		if getResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", getResp.StatusCode)
		}
		var got a2a.TaskResponse
		decodeJSON(t, getResp, &got)

		finalState = got.Status.State
		if finalState == "completed" || finalState == "failed" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The blueprint is a simple echo command — it should complete.
	if finalState != "completed" {
		// Log the error details for debugging.
		getDebug := doRequest(t, ts, "GET", "/a2a/tasks/"+submitted.ID, nil)
		var debugResp a2a.TaskResponse
		decodeJSON(t, getDebug, &debugResp)
		t.Fatalf("expected completed, got %s: %+v", finalState, debugResp)
	}

	// Verify the completed response includes a result.
	getResp := doRequest(t, ts, "GET", "/a2a/tasks/"+submitted.ID, nil)
	var completed a2a.TaskResponse
	decodeJSON(t, getResp, &completed)

	if completed.Result == nil {
		t.Fatal("expected result on completed task")
	}
	if completed.Result.OutputType != "application/json" {
		t.Fatalf("expected outputType=application/json, got %s", completed.Result.OutputType)
	}
}

func TestSubmitTask_WithClientProvidedID(t *testing.T) {
	ts, bpDir := newTestServerWithBlueprints(t)
	defer ts.Close()

	writeBlueprintFile(t, bpDir, "custom-id")

	resp := doRequest(t, ts, "POST", "/a2a/tasks", a2a.TaskRequest{
		ID:    "client-provided-123",
		Skill: "custom-id",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var result a2a.TaskResponse
	decodeJSON(t, resp, &result)

	if result.ID != "client-provided-123" {
		t.Fatalf("expected ID=client-provided-123, got %s", result.ID)
	}
}
