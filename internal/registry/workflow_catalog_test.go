package registry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/workflow/graph"
)

func TestWorkflowCatalogPersistsQualificationPinsAndPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow-catalog.json")
	index, err := OpenWorkflowIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	record := qualifiedRecord("team/orders", "team", "v1", "workflow:\n  name: orders\n")
	registered, err := index.RegisterWorkflow(t.Context(), record, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, pinErr := index.PinWorkflow(t.Context(), WorkflowQuery{Name: record.Name}); !errors.Is(pinErr, ErrInvalidWorkflow) {
		t.Fatalf("PinWorkflow(mutable) = %v", pinErr)
	}
	if _, pinErr := index.PinWorkflow(t.Context(), WorkflowQuery{Name: record.Name, Version: record.Version, Digest: registered.Digest}); pinErr != nil {
		t.Fatal(pinErr)
	}
	if _, publishErr := index.PublishWorkflow(t.Context(), WorkflowQuery{Name: record.Name, Version: record.Version, Digest: registered.Digest}); publishErr != nil {
		t.Fatal(publishErr)
	}
	// Publication is operational state, and an idempotent service retry
	// preserves the original registration timestamp rather than conflicting on
	// a later wall clock.
	replay := record
	replay.RegisteredAt = replay.RegisteredAt.Add(time.Hour)
	replayed, replayErr := index.RegisterWorkflow(t.Context(), replay, false)
	if replayErr != nil || !replayed.RegisteredAt.Equal(record.RegisteredAt) {
		t.Fatalf("RegisterWorkflow(exact replay after publish) = %v", replayErr)
	}

	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("catalog mode = %v, %v", info.Mode().Perm(), err)
	}
	reopened, err := OpenWorkflowIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := reopened.ResolvePinnedWorkflow(t.Context(), record.Name)
	if err != nil || pinned.Record.Version != "v1" || !pinned.Record.Published {
		t.Fatalf("reopened pinned = %#v, %v", pinned, err)
	}
	current, err := reopened.ResolveWorkflow(t.Context(), WorkflowQuery{Name: record.Name})
	if err != nil || current.Record.Authority != "project" || current.Record.Namespace != "team" {
		t.Fatalf("current transferred authority = %#v, %v", current, err)
	}
}

func TestWorkflowCatalogPinsExactVersionWhenSourceDigestIsShared(t *testing.T) {
	index := NewWorkflowIndex()
	first := qualifiedRecord("team/shared", "team", "v1", "workflow:\n  name: shared\n")
	second := qualifiedRecord("team/shared", "team", "v2", string(first.Source))
	first, err := index.RegisterWorkflow(t.Context(), first, false)
	if err != nil {
		t.Fatal(err)
	}
	second, err = index.RegisterWorkflow(t.Context(), second, false)
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest != second.Digest {
		t.Fatal("fixture does not share source digest")
	}
	if _, pinErr := index.PinWorkflow(t.Context(), WorkflowQuery{Name: first.Name, Version: second.Version, Digest: second.Digest}); pinErr != nil {
		t.Fatal(pinErr)
	}
	resolved, err := index.ResolvePinnedWorkflow(t.Context(), first.Name)
	if err != nil || resolved.Record.Version != "v2" {
		t.Fatalf("ResolvePinnedWorkflow() = %#v, %v", resolved, err)
	}
	if unpinErr := index.UnpinWorkflowExact(t.Context(), WorkflowQuery{Name: first.Name, Version: first.Version, Digest: first.Digest}); !errors.Is(unpinErr, ErrWorkflowConflict) {
		t.Fatalf("UnpinWorkflowExact(stale) = %v", unpinErr)
	}
	if stillPinned, resolveErr := index.ResolvePinnedWorkflow(t.Context(), first.Name); resolveErr != nil || stillPinned.Record.Version != "v2" {
		t.Fatalf("stale exact unpin changed pin = %#v, %v", stillPinned, resolveErr)
	}
	if unpinErr := index.UnpinWorkflowExact(t.Context(), WorkflowQuery{Name: second.Name, Version: second.Version, Digest: second.Digest}); unpinErr != nil {
		t.Fatalf("UnpinWorkflowExact() = %v", unpinErr)
	}
}

func TestWorkflowCatalogPersistenceFailureCommitPoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow-catalog.json")
	index, err := OpenWorkflowIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	pre := qualifiedRecord("team/pre", "team", "v1", "workflow:\n  name: pre\n")
	index.beforeRename = func() error { return errors.New("pre-rename") }
	if _, registerErr := index.RegisterWorkflow(t.Context(), pre, false); registerErr == nil {
		t.Fatal("pre-rename failure was accepted")
	}
	if _, resolveErr := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: pre.Name, Version: pre.Version}); !errors.Is(resolveErr, ErrWorkflowNotFound) {
		t.Fatalf("pre-commit memory rollback = %v", resolveErr)
	}
	index.beforeRename = nil

	post := qualifiedRecord("team/post", "team", "v1", "workflow:\n  name: post\n")
	index.afterRename = func() error { return errors.New("post-rename") }
	if _, registerErr := index.RegisterWorkflow(t.Context(), post, false); registerErr == nil {
		t.Fatal("post-rename warning was not reported")
	}
	if _, resolveErr := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: post.Name, Version: post.Version}); resolveErr != nil {
		t.Fatalf("post-commit memory rolled back: %v", resolveErr)
	}
	index.afterRename = nil
	reopened, err := OpenWorkflowIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ResolveWorkflow(t.Context(), WorkflowQuery{Name: post.Name, Version: post.Version}); err != nil {
		t.Fatalf("post-commit disk missing: %v", err)
	}
}

func TestWorkflowCatalogRejectsCorruptionDuplicatesAndPublicFiles(t *testing.T) {
	directory := t.TempDir()
	for name, data := range map[string][]byte{
		"corrupt.json":   []byte(`{"records":[]} trailing`),
		"duplicate.json": duplicateCatalogBytes(t),
	} {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenWorkflowIndex(path); err == nil {
			t.Fatalf("OpenWorkflowIndex(%s) accepted invalid catalog", name)
		}
	}
	publicPath := filepath.Join(directory, "public.json")
	if err := os.WriteFile(publicPath, []byte(`{"records":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWorkflowIndex(publicPath); err == nil {
		t.Fatal("OpenWorkflowIndex accepted public permissions")
	}
}

func TestWorkflowCatalogConcurrentExactReplayAndDefensiveReads(t *testing.T) {
	index := NewWorkflowIndex()
	record := qualifiedRecord("team/concurrent", "team", "v1", "workflow:\n  name: concurrent\n")
	const workers = 24
	errorsByWorker := make([]error, workers)
	var group sync.WaitGroup
	for worker := range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			resolved, err := index.RegisterWorkflow(t.Context(), record, true)
			errorsByWorker[worker] = err
			if len(resolved.Source) != 0 {
				resolved.Source[0] = 'x'
			}
		}()
	}
	group.Wait()
	for worker, err := range errorsByWorker {
		if err != nil {
			t.Fatalf("worker %d exact replay = %v", worker, err)
		}
	}
	resolved, err := index.ResolveWorkflow(t.Context(), WorkflowQuery{Name: record.Name})
	if err != nil || string(resolved.Record.Source) != string(record.Source) {
		t.Fatalf("resolved defensive record = %#v, %v", resolved, err)
	}
	all, err := index.SearchWorkflows(t.Context(), "team", "concurrent")
	if err != nil || len(all) != 1 {
		t.Fatalf("concurrent catalog contents = %#v, %v", all, err)
	}
}

func qualifiedRecord(name, namespace, version, source string) WorkflowRecord {
	return WorkflowRecord{
		Name: name, Namespace: namespace, Version: version, Source: []byte(source),
		Authority: "project", TrustClass: "project-owned",
		Provenance:          graph.Provenance{Origin: "workflow-source", Locator: "/project/workflow.yaml"},
		PlanDigest:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ContractSuiteDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ContractTestDigest:  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		TestsPassed:         true, PublisherPrincipal: "principal:test",
		RegisteredAt: time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC),
	}
}

func duplicateCatalogBytes(t *testing.T) []byte {
	t.Helper()
	record, err := canonicalWorkflowRecord(qualifiedRecord("team/duplicate", "team", "v1", "workflow:\n  name: duplicate\n"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(workflowCatalogSnapshot{Records: []WorkflowRecord{record, record}})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
