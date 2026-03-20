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
	"testing"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// ── Mock Store ────────────────────────────────────────────────────────────────

type mockStore struct {
	triggers      map[string]*persistence.TriggerRecord
	triggerByPath map[string]string // path → id
	firedIDs      []string
	deletedIDs    []string
}

func newMockStore() *mockStore {
	return &mockStore{
		triggers:      make(map[string]*persistence.TriggerRecord),
		triggerByPath: make(map[string]string),
	}
}

func (s *mockStore) addTrigger(t persistence.TriggerRecord) {
	s.triggers[t.ID] = &t
	s.triggerByPath[t.Path] = t.ID
}

func (s *mockStore) GetTriggerByPath(_ context.Context, path string) (persistence.TriggerRecord, error) {
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
	s.firedIDs = append(s.firedIDs, id)
	if t, ok := s.triggers[id]; ok {
		t.FiredCount++
	}
	return nil
}

func (s *mockStore) DeleteTrigger(_ context.Context, id string) error {
	s.deletedIDs = append(s.deletedIDs, id)
	if t, ok := s.triggers[id]; ok {
		delete(s.triggerByPath, t.Path)
		delete(s.triggers, id)
	}
	return nil
}

func (s *mockStore) ListWebhookTriggers(_ context.Context) ([]persistence.TriggerRecord, error) {
	var out []persistence.TriggerRecord
	for _, t := range s.triggers {
		if t.Enabled && t.Type == "webhook" {
			out = append(out, *t)
		}
	}
	return out, nil
}

// ── Mock Runner ───────────────────────────────────────────────────────────────

type mockRunner struct {
	enqueued []execution.Request
}

func (r *mockRunner) Enqueue(_ context.Context, req execution.Request) error {
	r.enqueued = append(r.enqueued, req)
	return nil
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
	if len(runner.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued run, got %d", len(runner.enqueued))
	}
	if runner.enqueued[0].BlueprintPath != "/bp/deploy.yaml" {
		t.Errorf("wrong blueprint path: %s", runner.enqueued[0].BlueprintPath)
	}
	if runner.enqueued[0].WorkspaceID != "default" {
		t.Errorf("wrong workspace: %s", runner.enqueued[0].WorkspaceID)
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
	if len(runner.enqueued) != 1 {
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
	if len(runner.enqueued) != 0 {
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
	if len(runner.enqueued) != 1 {
		t.Fatal("expected 1 enqueued run")
	}
	inputs := runner.enqueued[0].Inputs
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
	inputs := runner.enqueued[0].Inputs
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
	if len(store.deletedIDs) != 1 || store.deletedIDs[0] != "trig-7" {
		t.Errorf("expected trigger trig-7 to be deleted, got %v", store.deletedIDs)
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

	if len(store.firedIDs) != 2 {
		t.Errorf("expected 2 fired updates, got %d", len(store.firedIDs))
	}
	if len(runner.enqueued) != 2 {
		t.Errorf("expected 2 enqueued runs, got %d", len(runner.enqueued))
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
