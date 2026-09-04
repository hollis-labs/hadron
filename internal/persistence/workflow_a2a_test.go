package persistence

import (
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/go-workflow/graph"
	workflowruntime "github.com/hollis-labs/go-workflow/runtime"
	"github.com/hollis-labs/go-workflow/values"
)

func TestWorkflowA2ATaskStoreReplayConflictReopenAndTwoHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a2a.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	firstTasks, err := NewWorkflowA2ATaskStore(first)
	if err != nil {
		t.Fatal(err)
	}
	secondTasks, err := NewWorkflowA2ATaskStore(second)
	if err != nil {
		t.Fatal(err)
	}
	correlation := workflowA2ATestCorrelation()
	var wg sync.WaitGroup
	results := make(chan workflowruntime.IdempotencyOutcome, 2)
	errorsOut := make(chan error, 2)
	for _, tasks := range []*WorkflowA2ATaskStore{firstTasks, secondTasks} {
		wg.Add(1)
		go func(tasks *WorkflowA2ATaskStore) {
			defer wg.Done()
			_, outcome, putErr := tasks.PutA2ATaskCorrelation(t.Context(), correlation)
			results <- outcome
			errorsOut <- putErr
		}(tasks)
	}
	wg.Wait()
	close(results)
	close(errorsOut)
	for putErr := range errorsOut {
		if putErr != nil {
			t.Fatal(putErr)
		}
	}
	counts := map[workflowruntime.IdempotencyOutcome]int{}
	for outcome := range results {
		counts[outcome]++
	}
	if counts[workflowruntime.IdempotencyApplied] != 1 || counts[workflowruntime.IdempotencyReplayed] != 1 {
		t.Fatalf("concurrent outcomes = %#v", counts)
	}
	changed := correlation
	changed.RequestDigest = values.SHA256Digest([]byte("different"))
	if _, _, conflictErr := secondTasks.PutA2ATaskCorrelation(t.Context(), changed); !errors.Is(conflictErr, workflowruntime.ErrIdempotencyConflict) {
		t.Fatalf("changed intent error = %v", conflictErr)
	}
	if closeErr := first.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if closeErr := second.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedTasks, err := NewWorkflowA2ATaskStore(reopened)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := reopenedTasks.GetA2ATaskCorrelation(t.Context(), correlation.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	correlation.CreatedAt = loaded.CreatedAt
	if !reflect.DeepEqual(loaded, correlation) {
		t.Fatalf("reopened correlation = %#v, want %#v", loaded, correlation)
	}
}

func TestWorkflowA2ATaskCorrelationsAreAppendOnly(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "a2a.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	tasks, err := NewWorkflowA2ATaskStore(store)
	if err != nil {
		t.Fatal(err)
	}
	correlation := workflowA2ATestCorrelation()
	if _, _, putErr := tasks.PutA2ATaskCorrelation(t.Context(), correlation); putErr != nil {
		t.Fatal(putErr)
	}
	if _, updateErr := store.DB().Exec(`UPDATE workflow_a2a_tasks SET owner_json = ? WHERE task_id = ?`, `{}`, correlation.TaskID); updateErr == nil {
		t.Fatal("direct A2A correlation update succeeded")
	}
	if _, deleteErr := store.DB().Exec(`DELETE FROM workflow_a2a_tasks WHERE task_id = ?`, correlation.TaskID); deleteErr == nil {
		t.Fatal("direct A2A correlation delete succeeded")
	}
	loaded, err := tasks.GetA2ATaskCorrelation(t.Context(), correlation.TaskID)
	if err != nil || !reflect.DeepEqual(loaded, correlation) {
		t.Fatalf("append-only correlation changed: %#v, %v", loaded, err)
	}
}

func workflowA2ATestCorrelation() hoststate.A2ATaskCorrelation {
	return hoststate.A2ATaskCorrelation{
		TaskID: "task-persisted", RunID: "a2a-persisted-run",
		Definition:    graph.DefinitionRef{Kind: "registry", ID: "team/persisted", Version: "v1", Digest: values.SHA256Digest([]byte("workflow"))},
		RequestDigest: values.SHA256Digest([]byte("request")), IdempotencyKey: "start-key", HostStartKey: "a2a-start-persisted",
		Owner: hoststate.IdentityBinding{
			Principal: "principal:owner", SourceAuthority: "a2a", Trust: "local", Grants: []string{"workflow.run"},
			RunScope: hoststate.RunScope{Version: hoststate.ScopeTargetVersionV1, Kind: hoststate.RunScopeUser, ID: "owner"},
		},
		CreatedAt: time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC),
	}
}
