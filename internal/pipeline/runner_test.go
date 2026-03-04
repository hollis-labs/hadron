package pipeline

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

func TestRunner_ExecutesStagesInOrder(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 1, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "simple-success.yaml")
	if err := r.Start(context.Background(), "pl-test-001", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-test-001", "success")
	if rec.Status != "success" {
		t.Fatalf("expected success, got %s", rec.Status)
	}
	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-test-001")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}
	if len(stages) != 2 {
		t.Fatalf("expected 2 stage runs, got %d", len(stages))
	}
	if stages[0].StageIndex != 0 || stages[1].StageIndex != 1 {
		t.Fatalf("unexpected stage order: %+v", stages)
	}
}

func TestRunner_StopOnFailDefault(t *testing.T) {
	store := openStore(t)
	defer func() { _ = store.Close() }()
	execMgr := execution.NewManager(store, nil, 1, "", nil)
	defer execMgr.Close()
	r := NewRunner(store, execMgr)

	path := filepath.Join("..", "..", "testdata", "pipelines", "stop-on-fail.yaml")
	if err := r.Start(context.Background(), "pl-test-002", path, "default"); err != nil {
		t.Fatalf("start pipeline: %v", err)
	}

	rec := waitPipelineStatus(t, store, "pl-test-002", "failed")
	if !rec.ErrorMessage.Valid || !strings.Contains(rec.ErrorMessage.String, "stop_on_fail") {
		t.Fatalf("expected stop_on_fail error, got %+v", rec.ErrorMessage)
	}
	stages, err := store.ListPipelineStageRuns(context.Background(), "pl-test-002")
	if err != nil {
		t.Fatalf("list stage runs: %v", err)
	}
	if len(stages) != 1 {
		t.Fatalf("expected 1 stage run due to stop-on-fail, got %d", len(stages))
	}
}

func openStore(t *testing.T) *persistence.Store {
	t.Helper()
	store, err := persistence.Open(filepath.Join(t.TempDir(), "pipeline.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func waitPipelineStatus(t *testing.T, store *persistence.Store, id, want string) persistence.PipelineRunRecord {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := store.GetPipelineRun(context.Background(), id)
		if err == nil && rec.Status == want {
			return rec
		}
		time.Sleep(60 * time.Millisecond)
	}
	rec, err := store.GetPipelineRun(context.Background(), id)
	if err != nil {
		t.Fatalf("wait pipeline status get error: %v", err)
	}
	t.Fatalf("timed out waiting for pipeline status %s, got %s", want, rec.Status)
	return persistence.PipelineRunRecord{}
}
