package trigger

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// ── Mock Store ────────────────────────────────────────────────────────────────

type mockStore struct {
	mu            sync.Mutex
	triggers      map[string]*persistence.TriggerRecord
	triggerByPath map[string]string // path → id
	firedIDs      []string
	deletedIDs    []string
	deletedCh     chan struct{}
}

func newMockStore() *mockStore {
	return &mockStore{
		triggers:      make(map[string]*persistence.TriggerRecord),
		triggerByPath: make(map[string]string),
	}
}

func (s *mockStore) addTrigger(t persistence.TriggerRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.triggers[t.ID] = &t
	s.triggerByPath[t.Path] = t.ID
}

func (s *mockStore) GetTriggerByPath(_ context.Context, path string) (persistence.TriggerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.triggerByPath[path]
	if !ok {
		return persistence.TriggerRecord{}, fmt.Errorf("get trigger by path: sql: no rows in result set")
	}
	t, ok := s.triggers[id]
	if !ok || !t.Enabled {
		return persistence.TriggerRecord{}, fmt.Errorf("get trigger by path: sql: no rows in result set")
	}
	return *t, nil
}

func (s *mockStore) UpdateTriggerFired(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.firedIDs = append(s.firedIDs, id)
	if t, ok := s.triggers[id]; ok {
		t.FiredCount++
	}
	return nil
}

func (s *mockStore) DeleteTrigger(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedIDs = append(s.deletedIDs, id)
	if t, ok := s.triggers[id]; ok {
		delete(s.triggerByPath, t.Path)
		delete(s.triggers, id)
	}
	if s.deletedCh != nil {
		close(s.deletedCh)
		s.deletedCh = nil
	}
	return nil
}

func (s *mockStore) ListWebhookTriggers(_ context.Context) ([]persistence.TriggerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []persistence.TriggerRecord
	for _, t := range s.triggers {
		if t.Enabled && t.Type == "webhook" {
			out = append(out, *t)
		}
	}
	return out, nil
}

func (s *mockStore) ListFileWatchTriggers(_ context.Context) ([]persistence.TriggerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []persistence.TriggerRecord
	for _, t := range s.triggers {
		if t.Enabled && t.Type == "file_watch" {
			out = append(out, *t)
		}
	}
	return out, nil
}

func (s *mockStore) DeleteExpiredTriggers(_ context.Context, now time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	for id, t := range s.triggers {
		if t.TTLExpiresAt.Valid && t.TTLExpiresAt.String != "" {
			expires, err := time.Parse(time.RFC3339, t.TTLExpiresAt.String)
			if err == nil && expires.Before(now) {
				delete(s.triggerByPath, t.Path)
				delete(s.triggers, id)
				count++
			}
		}
	}
	return count, nil
}

func (s *mockStore) trigger(id string) (persistence.TriggerRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.triggers[id]
	if !ok {
		return persistence.TriggerRecord{}, false
	}
	return *t, true
}

func (s *mockStore) deletedIDsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deletedIDs...)
}

func (s *mockStore) waitForDeleted(id string, timeout time.Duration) ([]string, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		s.mu.Lock()
		for _, deletedID := range s.deletedIDs {
			if deletedID == id {
				out := append([]string(nil), s.deletedIDs...)
				s.mu.Unlock()
				return out, true
			}
		}
		if s.deletedCh == nil {
			s.deletedCh = make(chan struct{})
		}
		ch := s.deletedCh
		s.mu.Unlock()

		select {
		case <-ch:
		case <-deadline.C:
			return s.deletedIDsSnapshot(), false
		}
	}
}

func (s *mockStore) firedIDsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.firedIDs...)
}

// ── Mock Runner ───────────────────────────────────────────────────────────────

type mockRunner struct {
	mu             sync.Mutex
	enqueued       []execution.Request
	enqueuedCh     chan struct{}
	enqueueStarted chan struct{}
	releaseEnqueue chan struct{}
}

func (r *mockRunner) Enqueue(_ context.Context, req execution.Request) error {
	r.mu.Lock()
	r.enqueued = append(r.enqueued, req)
	if r.enqueuedCh != nil {
		close(r.enqueuedCh)
		r.enqueuedCh = nil
	}
	started := r.enqueueStarted
	release := r.releaseEnqueue
	r.enqueueStarted = nil
	r.mu.Unlock()

	if started != nil {
		close(started)
	}
	if release != nil {
		<-release
	}
	return nil
}

func (r *mockRunner) enqueuedSnapshot() []execution.Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]execution.Request(nil), r.enqueued...)
}

func (r *mockRunner) waitForEnqueued(n int, timeout time.Duration) ([]execution.Request, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		r.mu.Lock()
		if len(r.enqueued) >= n {
			out := append([]execution.Request(nil), r.enqueued...)
			r.mu.Unlock()
			return out, true
		}
		if r.enqueuedCh == nil {
			r.enqueuedCh = make(chan struct{})
		}
		ch := r.enqueuedCh
		r.mu.Unlock()

		select {
		case <-ch:
		case <-deadline.C:
			return r.enqueuedSnapshot(), false
		}
	}
}

// ── Helper ────────────────────────────────────────────────────────────────────

func makeSignature(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func baseTrigger(id, path, bpPath string) persistence.TriggerRecord {
	return persistence.TriggerRecord{
		ID:            id,
		Type:          "webhook",
		Name:          "Test Trigger",
		Path:          path,
		BlueprintPath: bpPath,
		WorkspaceID:   "default",
		Enabled:       true,
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestWebhookNoSecret_RunEnqueued(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	trig := baseTrigger("trig-1", "deploy", "/bp/deploy.yaml")
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	body := []byte(`{"ref": "main"}`)
	req := httptest.NewRequest(http.MethodPost, "/hooks/deploy", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	enqueued := runner.enqueuedSnapshot()
	if len(enqueued) != 1 {
		t.Fatalf("expected 1 enqueued run, got %d", len(enqueued))
	}
	if enqueued[0].BlueprintPath != "/bp/deploy.yaml" {
		t.Errorf("wrong blueprint path: %s", enqueued[0].BlueprintPath)
	}
	if enqueued[0].WorkspaceID != "default" {
		t.Errorf("wrong workspace: %s", enqueued[0].WorkspaceID)
	}

	var resp map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["run_id"] == nil || resp["run_id"] == "" {
		t.Error("response missing run_id")
	}
}

func TestWebhookValidSecret_RunEnqueued(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	secret := "my-webhook-secret"
	trig := baseTrigger("trig-2", "build", "/bp/build.yaml")
	trig.SecretHash = sql.NullString{String: secret, Valid: true}
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	body := []byte(`{"branch": "develop"}`)
	sig := makeSignature(body, secret)
	req := httptest.NewRequest(http.MethodPost, "/hooks/build", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hub-Signature-256", sig)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	if len(runner.enqueuedSnapshot()) != 1 {
		t.Fatal("expected 1 enqueued run")
	}
}

func TestWebhookInvalidSecret_401(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	trig := baseTrigger("trig-3", "secure", "/bp/secure.yaml")
	trig.SecretHash = sql.NullString{String: "correct-secret", Valid: true}
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	body := []byte(`{}`)
	wrongSig := makeSignature(body, "wrong-secret")
	req := httptest.NewRequest(http.MethodPost, "/hooks/secure", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", wrongSig)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", w.Code, w.Body.String())
	}
	if len(runner.enqueuedSnapshot()) != 0 {
		t.Error("should not have enqueued a run")
	}
}

func TestWebhookMissingSignature_401(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	trig := baseTrigger("trig-4", "protected", "/bp/protected.yaml")
	trig.SecretHash = sql.NullString{String: "a-secret", Valid: true}
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	body := []byte(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/hooks/protected", bytes.NewReader(body))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestInputExtractionFromBody(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	trig := baseTrigger("trig-5", "extract", "/bp/extract.yaml")
	trig.ExtractInputs = sql.NullString{
		String: `{"repo": "body.repository.full_name", "branch": "body.ref"}`,
		Valid:  true,
	}
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	body := []byte(`{"repository": {"full_name": "org/repo"}, "ref": "refs/heads/main"}`)
	req := httptest.NewRequest(http.MethodPost, "/hooks/extract", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	enqueued := runner.enqueuedSnapshot()
	if len(enqueued) != 1 {
		t.Fatal("expected 1 enqueued run")
	}
	inputs := enqueued[0].Inputs
	if inputs["repo"] != "org/repo" {
		t.Errorf("expected repo=org/repo, got %v", inputs["repo"])
	}
	if inputs["branch"] != "refs/heads/main" {
		t.Errorf("expected branch=refs/heads/main, got %v", inputs["branch"])
	}
}

func TestInputExtractionFromHeaderAndQuery(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	trig := baseTrigger("trig-6", "hq", "/bp/hq.yaml")
	trig.ExtractInputs = sql.NullString{
		String: `{"event_type": "header.X-GitHub-Event", "env": "query.environment"}`,
		Valid:  true,
	}
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/hooks/hq?environment=staging", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "push")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	enqueued := runner.enqueuedSnapshot()
	inputs := enqueued[0].Inputs
	if inputs["event_type"] != "push" {
		t.Errorf("expected event_type=push, got %v", inputs["event_type"])
	}
	if inputs["env"] != "staging" {
		t.Errorf("expected env=staging, got %v", inputs["env"])
	}
}

func TestOneShotTrigger_DeletedAfterFiring(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	trig := baseTrigger("trig-7", "oneshot", "/bp/oneshot.yaml")
	trig.OneShot = true
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/hooks/oneshot", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	deletedIDs := store.deletedIDsSnapshot()
	if len(deletedIDs) != 1 || deletedIDs[0] != "trig-7" {
		t.Errorf("expected trigger trig-7 to be deleted, got %v", deletedIDs)
	}
}

func TestWebhookNotFound(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/hooks/nonexistent", bytes.NewReader([]byte(`{}`)))
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestWebhookMethodNotAllowed(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/hooks/something", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestFiredCountUpdated(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	trig := baseTrigger("trig-8", "counter", "/bp/counter.yaml")
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	// Fire twice
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/hooks/counter", bytes.NewReader([]byte(`{}`)))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusAccepted {
			t.Fatalf("fire %d: expected 202, got %d", i+1, w.Code)
		}
	}

	firedIDs := store.firedIDsSnapshot()
	if len(firedIDs) != 2 {
		t.Errorf("expected 2 fired updates, got %d", len(firedIDs))
	}
	enqueued := runner.enqueuedSnapshot()
	if len(enqueued) != 2 {
		t.Errorf("expected 2 enqueued runs, got %d", len(enqueued))
	}
}

func TestDotPathAccess(t *testing.T) {
	data := map[string]any{
		"repository": map[string]any{
			"owner": map[string]any{
				"login": "octocat",
			},
			"full_name": "octocat/hello-world",
		},
		"ref": "refs/heads/main",
	}

	tests := []struct {
		path     string
		expected any
	}{
		{"ref", "refs/heads/main"},
		{"repository.full_name", "octocat/hello-world"},
		{"repository.owner.login", "octocat"},
		{"nonexistent", nil},
		{"repository.nonexistent", nil},
	}

	for _, tt := range tests {
		got := dotPathAccess(data, tt.path)
		if got != tt.expected {
			t.Errorf("dotPathAccess(%q) = %v, want %v", tt.path, got, tt.expected)
		}
	}
}

// ── File-Watch Trigger Tests ──────────────────────────────────────────────────

func TestFileWatchTrigger_StoredCorrectly(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	_ = New(store, runner)

	trig := persistence.TriggerRecord{
		ID:              "fw-1",
		Type:            "file_watch",
		Name:            "Config Watcher",
		Path:            `["/tmp/configs"]`,
		BlueprintPath:   "/bp/reload.yaml",
		WorkspaceID:     "default",
		Enabled:         true,
		DebounceSeconds: 3,
	}
	store.addTrigger(trig)

	got, ok := store.trigger("fw-1")
	if !ok {
		t.Fatal("expected stored trigger fw-1")
	}
	if got.Type != "file_watch" {
		t.Errorf("expected type=file_watch, got %s", got.Type)
	}
	if got.DebounceSeconds != 3 {
		t.Errorf("expected debounce=3, got %d", got.DebounceSeconds)
	}
	if got.Path != `["/tmp/configs"]` {
		t.Errorf("expected JSON paths, got %s", got.Path)
	}
}

func TestFileWatchTrigger_FileCreated_RunEnqueued(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	tmpDir := t.TempDir()

	trig := persistence.TriggerRecord{
		ID:              "fw-2",
		Type:            "file_watch",
		Name:            "Dir Watcher",
		Path:            `["` + tmpDir + `"]`,
		BlueprintPath:   "/bp/deploy.yaml",
		WorkspaceID:     "default",
		Enabled:         true,
		DebounceSeconds: 1,
	}
	store.addTrigger(trig)

	mgr.StartFileWatchers()
	t.Cleanup(mgr.StopFileWatchers)

	// Create a file in watched directory
	testFile := filepath.Join(tmpDir, "new-config.yaml")
	if err := os.WriteFile(testFile, []byte("test"), 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	enqueued, ok := runner.waitForEnqueued(1, 3*time.Second)
	mgr.StopFileWatchers()
	if !ok {
		t.Fatal("expected at least 1 enqueued run after file creation, got 0")
	}
	req := enqueued[0]
	if req.BlueprintPath != "/bp/deploy.yaml" {
		t.Errorf("wrong blueprint path: %s", req.BlueprintPath)
	}
	if req.Inputs["changed_file"] == nil {
		t.Error("expected changed_file input")
	}
	if req.Inputs["event_type"] == nil {
		t.Error("expected event_type input")
	}
}

func TestFileWatchTrigger_Debounce_OnlyOneRun(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	tmpDir := t.TempDir()

	trig := persistence.TriggerRecord{
		ID:              "fw-3",
		Type:            "file_watch",
		Name:            "Debounce Test",
		Path:            `["` + tmpDir + `"]`,
		BlueprintPath:   "/bp/debounce.yaml",
		WorkspaceID:     "default",
		Enabled:         true,
		DebounceSeconds: 1,
	}
	store.addTrigger(trig)

	mgr.StartFileWatchers()
	t.Cleanup(mgr.StopFileWatchers)

	// Rapid file changes — should be debounced into one run
	for i := 0; i < 5; i++ {
		testFile := filepath.Join(tmpDir, fmt.Sprintf("file-%d.yaml", i))
		if err := os.WriteFile(testFile, []byte(fmt.Sprintf("content-%d", i)), 0o644); err != nil {
			t.Fatalf("failed to write test file %d: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	_, ok := runner.waitForEnqueued(1, 3*time.Second)
	mgr.StopFileWatchers()
	if !ok {
		t.Fatal("expected 1 enqueued run after debounced file changes, got 0")
	}
	enqueued := runner.enqueuedSnapshot()
	if len(enqueued) != 1 {
		t.Errorf("expected exactly 1 enqueued run (debounced), got %d", len(enqueued))
	}
}

func TestFileWatchTrigger_OneShot_DeletedAfterFire(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	tmpDir := t.TempDir()

	trig := persistence.TriggerRecord{
		ID:              "fw-4",
		Type:            "file_watch",
		Name:            "One-shot Watcher",
		Path:            `["` + tmpDir + `"]`,
		BlueprintPath:   "/bp/oneshot.yaml",
		WorkspaceID:     "default",
		Enabled:         true,
		OneShot:         true,
		DebounceSeconds: 1,
	}
	store.addTrigger(trig)

	mgr.StartFileWatchers()
	t.Cleanup(mgr.StopFileWatchers)

	testFile := filepath.Join(tmpDir, "trigger-file.txt")
	if err := os.WriteFile(testFile, []byte("fire"), 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	if _, ok := runner.waitForEnqueued(1, 3*time.Second); !ok {
		mgr.StopFileWatchers()
		t.Fatal("expected at least 1 enqueued run")
	}
	deletedIDs, ok := store.waitForDeleted("fw-4", 3*time.Second)
	mgr.StopFileWatchers()
	if !ok {
		t.Fatal("expected trigger fw-4 to be deleted")
	}
	if len(deletedIDs) != 1 || deletedIDs[0] != "fw-4" {
		t.Errorf("expected trigger fw-4 to be deleted, got %v", deletedIDs)
	}
}

func TestFileWatchTrigger_StopWaitsForInFlightEnqueue(t *testing.T) {
	store := newMockStore()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseEnqueue := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}

	runner := &mockRunner{
		enqueueStarted: started,
		releaseEnqueue: release,
	}
	mgr := New(store, runner)

	tmpDir := t.TempDir()
	trig := persistence.TriggerRecord{
		ID:              "fw-stop",
		Type:            "file_watch",
		Name:            "Stop Waiter",
		Path:            `["` + tmpDir + `"]`,
		BlueprintPath:   "/bp/stop.yaml",
		WorkspaceID:     "default",
		Enabled:         true,
		DebounceSeconds: 1,
	}
	store.addTrigger(trig)

	mgr.StartFileWatchers()
	t.Cleanup(mgr.StopFileWatchers)
	t.Cleanup(releaseEnqueue)

	testFile := filepath.Join(tmpDir, "stop-waits.txt")
	if err := os.WriteFile(testFile, []byte("fire"), 0o644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("expected enqueue to start")
	}

	stopped := make(chan struct{})
	go func() {
		mgr.StopFileWatchers()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("StopFileWatchers returned while Enqueue was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	releaseEnqueue()

	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("StopFileWatchers did not return after in-flight Enqueue completed")
	}
}

// ── TTL Cleanup Tests ─────────────────────────────────────────────────────────

func TestCleanExpiredTriggers_RemovesExpired(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	// Expired trigger
	expired := persistence.TriggerRecord{
		ID:            "trig-expired",
		Type:          "webhook",
		Name:          "Expired",
		Path:          "expired-hook",
		BlueprintPath: "/bp/expired.yaml",
		WorkspaceID:   "default",
		Enabled:       true,
		TTLExpiresAt:  sql.NullString{String: time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339), Valid: true},
	}
	store.addTrigger(expired)

	// Non-expired trigger
	active := persistence.TriggerRecord{
		ID:            "trig-active",
		Type:          "webhook",
		Name:          "Active",
		Path:          "active-hook",
		BlueprintPath: "/bp/active.yaml",
		WorkspaceID:   "default",
		Enabled:       true,
		TTLExpiresAt:  sql.NullString{String: time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339), Valid: true},
	}
	store.addTrigger(active)

	ctx := context.Background()
	n, err := mgr.CleanExpiredTriggers(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 expired trigger deleted, got %d", n)
	}

	// Active trigger should still exist
	if _, ok := store.trigger("trig-active"); !ok {
		t.Error("active trigger should not have been deleted")
	}
	// Expired trigger should be gone
	if _, ok := store.trigger("trig-expired"); ok {
		t.Error("expired trigger should have been deleted")
	}
}

func TestCleanExpiredTriggers_NoExpired(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	active := persistence.TriggerRecord{
		ID:            "trig-active2",
		Type:          "webhook",
		Name:          "Active",
		Path:          "active-hook",
		BlueprintPath: "/bp/active.yaml",
		WorkspaceID:   "default",
		Enabled:       true,
	}
	store.addTrigger(active)

	ctx := context.Background()
	n, err := mgr.CleanExpiredTriggers(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 expired triggers deleted, got %d", n)
	}
}

func TestOneShotWithTTL_DeletedOnFire_NotWaitForTTL(t *testing.T) {
	store := newMockStore()
	runner := &mockRunner{}
	mgr := New(store, runner)

	trig := baseTrigger("trig-os-ttl", "oneshot-ttl", "/bp/os-ttl.yaml")
	trig.OneShot = true
	trig.TTLExpiresAt = sql.NullString{String: time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339), Valid: true}
	store.addTrigger(trig)

	mux := http.NewServeMux()
	mgr.RegisterWebhookRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/hooks/oneshot-ttl", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	deletedIDs := store.deletedIDsSnapshot()
	if len(deletedIDs) != 1 || deletedIDs[0] != "trig-os-ttl" {
		t.Errorf("expected trigger deleted on fire (not waiting for TTL), got %v", deletedIDs)
	}
}
