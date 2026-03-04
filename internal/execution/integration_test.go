package execution_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
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
		if e.EventType == "task_retry" {
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
		if e.EventType == "task_skipped_condition" {
			skipped = true
			break
		}
	}
	if !skipped {
		t.Fatalf("expected task_skipped_condition event; got events: %v", eventTypes(events))
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
