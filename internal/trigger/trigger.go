package trigger

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// Store is the persistence interface required by the trigger manager.
type Store interface {
	GetTriggerByPath(ctx context.Context, path string) (persistence.TriggerRecord, error)
	UpdateTriggerFired(ctx context.Context, id string) error
	DeleteTrigger(ctx context.Context, id string) error
	ListWebhookTriggers(ctx context.Context) ([]persistence.TriggerRecord, error)
	ListFileWatchTriggers(ctx context.Context) ([]persistence.TriggerRecord, error)
	DeleteExpiredTriggers(ctx context.Context, now time.Time) (int64, error)
}

// Runner enqueues blueprint runs.
type Runner interface {
	Enqueue(ctx context.Context, req execution.Request) error
}

// Manager handles webhook trigger routing, file-watch triggers, and TTL cleanup.
type Manager struct {
	store  Store
	runner Runner
	seq    atomic.Uint64

	// file watcher state
	mu          sync.Mutex
	watchers    map[string]*fileWatcher // trigger ID → watcher
	watchCancel context.CancelFunc
	watchCtx    context.Context

	// TTL cleanup state
	ttlCancel context.CancelFunc
}

type fileWatcher struct {
	watcher  *fsnotify.Watcher
	cancel   context.CancelFunc
	trigID   string
	debounce time.Duration
}

// New creates a new trigger Manager.
func New(store Store, runner Runner) *Manager {
	return &Manager{
		store:    store,
		runner:   runner,
		watchers: make(map[string]*fileWatcher),
	}
}

// StartFileWatchers queries all file_watch triggers and starts goroutines to watch them.
func (m *Manager) StartFileWatchers() {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	m.watchCtx = ctx
	m.watchCancel = cancel

	triggers, err := m.store.ListFileWatchTriggers(ctx)
	if err != nil {
		log.Printf("trigger: failed to list file_watch triggers: %v", err)
		return
	}

	for _, t := range triggers {
		m.startWatcherLocked(t)
	}
}

// startWatcherLocked starts a single file watcher goroutine. Must hold m.mu.
func (m *Manager) startWatcherLocked(trigger persistence.TriggerRecord) {
	if _, exists := m.watchers[trigger.ID]; exists {
		return
	}

	// Parse paths from the trigger's Path field (JSON-encoded list).
	var paths []string
	if err := json.Unmarshal([]byte(trigger.Path), &paths); err != nil {
		// Fall back to treating as a single path.
		paths = []string{trigger.Path}
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("trigger: failed to create fsnotify watcher for %s: %v", trigger.ID, err)
		return
	}

	for _, p := range paths {
		if err := watcher.Add(p); err != nil {
			log.Printf("trigger: failed to watch path %q for trigger %s: %v", p, trigger.ID, err)
		}
	}

	debounce := time.Duration(trigger.DebounceSeconds) * time.Second
	if debounce <= 0 {
		debounce = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(m.watchCtx)
	fw := &fileWatcher{
		watcher:  watcher,
		cancel:   cancel,
		trigID:   trigger.ID,
		debounce: debounce,
	}
	m.watchers[trigger.ID] = fw

	go m.runFileWatcher(ctx, fw, trigger)
}

// runFileWatcher processes fsnotify events for a single trigger with debouncing.
func (m *Manager) runFileWatcher(ctx context.Context, fw *fileWatcher, trigger persistence.TriggerRecord) {
	defer func() { _ = fw.watcher.Close() }()

	// Parse event filter if present in extract_inputs.
	var eventFilter map[string]bool
	if trigger.ExtractInputs.Valid && trigger.ExtractInputs.String != "" {
		var cfg map[string]string
		if err := json.Unmarshal([]byte(trigger.ExtractInputs.String), &cfg); err == nil {
			if events, ok := cfg["events"]; ok {
				eventFilter = make(map[string]bool)
				for _, e := range strings.Split(events, ",") {
					eventFilter[strings.TrimSpace(e)] = true
				}
			}
		}
	}

	var debounceTimer *time.Timer
	var lastEvent fsnotify.Event

	for {
		select {
		case <-ctx.Done():
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return
		case event, ok := <-fw.watcher.Events:
			if !ok {
				return
			}
			eventType := fsEventType(event.Op)
			if eventFilter != nil && !eventFilter[eventType] {
				continue
			}
			lastEvent = event
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.AfterFunc(fw.debounce, func() {
				m.fireFileWatch(trigger, lastEvent)
			})
		case err, ok := <-fw.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("trigger: fsnotify error for %s: %v", fw.trigID, err)
		}
	}
}

// fireFileWatch enqueues a blueprint run for a file-watch trigger event.
func (m *Manager) fireFileWatch(trigger persistence.TriggerRecord, event fsnotify.Event) {
	ctx := context.Background()

	inputs := map[string]any{
		"changed_file": event.Name,
		"event_type":   fsEventType(event.Op),
	}

	n := m.seq.Add(1)
	runID := fmt.Sprintf("fwatch-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
	if err := m.runner.Enqueue(ctx, execution.Request{
		RunID:         runID,
		WorkspaceID:   trigger.WorkspaceID,
		BlueprintPath: trigger.BlueprintPath,
		Inputs:        inputs,
	}); err != nil {
		log.Printf("trigger: failed to enqueue file_watch run for %s: %v", trigger.ID, err)
		return
	}

	_ = m.store.UpdateTriggerFired(ctx, trigger.ID)

	if trigger.OneShot {
		_ = m.store.DeleteTrigger(ctx, trigger.ID)
		m.removeWatcher(trigger.ID)
	}
}

// removeWatcher stops and removes a file watcher by trigger ID.
func (m *Manager) removeWatcher(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fw, ok := m.watchers[id]; ok {
		fw.cancel()
		delete(m.watchers, id)
	}
}

// AddFileWatcher adds and starts a new file watcher for a trigger at runtime.
func (m *Manager) AddFileWatcher(trigger persistence.TriggerRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.watchCtx == nil {
		// Not started yet; StartFileWatchers will pick it up.
		return
	}
	m.startWatcherLocked(trigger)
}

// StopFileWatchers stops all file watchers.
func (m *Manager) StopFileWatchers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.watchCancel != nil {
		m.watchCancel()
	}
	for id, fw := range m.watchers {
		fw.cancel()
		delete(m.watchers, id)
	}
}

// CleanExpiredTriggers deletes triggers where ttl_expires_at < now.
func (m *Manager) CleanExpiredTriggers(ctx context.Context) (int64, error) {
	return m.store.DeleteExpiredTriggers(ctx, time.Now())
}

// StartTTLCleanup starts a background goroutine that cleans expired triggers every interval.
func (m *Manager) StartTTLCleanup(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	m.ttlCancel = cancel

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n, err := m.CleanExpiredTriggers(ctx); err != nil {
					log.Printf("trigger: TTL cleanup error: %v", err)
				} else if n > 0 {
					log.Printf("trigger: cleaned %d expired trigger(s)", n)
				}
			}
		}
	}()
}

// StopTTLCleanup stops the TTL cleanup goroutine.
func (m *Manager) StopTTLCleanup() {
	if m.ttlCancel != nil {
		m.ttlCancel()
	}
}

// fsEventType converts an fsnotify Op to a human-readable event type string.
func fsEventType(op fsnotify.Op) string {
	switch {
	case op.Has(fsnotify.Create):
		return "create"
	case op.Has(fsnotify.Write):
		return "modify"
	case op.Has(fsnotify.Remove):
		return "delete"
	case op.Has(fsnotify.Rename):
		return "rename"
	case op.Has(fsnotify.Chmod):
		return "chmod"
	default:
		return "unknown"
	}
}

// RegisterWebhookRoutes registers the catch-all /hooks/ handler on the mux.
func (m *Manager) RegisterWebhookRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/hooks/", m.handleWebhookCatchAll)
}

func (m *Manager) handleWebhookCatchAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	hookPath := strings.TrimPrefix(r.URL.Path, "/hooks/")
	hookPath = strings.TrimRight(hookPath, "/")
	if hookPath == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "hook path is required"})
		return
	}

	trigger, err := m.store.GetTriggerByPath(r.Context(), hookPath)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "webhook not found"})
		return
	}

	m.HandleWebhook(&trigger, w, r)
}

// HandleWebhook processes an incoming webhook request for a specific trigger.
func (m *Manager) HandleWebhook(trigger *persistence.TriggerRecord, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Read body
	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024)) // 10MB limit
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	// Validate secret if configured
	if trigger.SecretHash.Valid && trigger.SecretHash.String != "" {
		sig := r.Header.Get("X-Hub-Signature-256")
		if !validateSignature(body, trigger.SecretHash.String, sig) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid signature"})
			return
		}
	}

	// Extract inputs
	inputs := map[string]any{}
	if trigger.ExtractInputs.Valid && trigger.ExtractInputs.String != "" {
		extracted, err := extractInputs(trigger.ExtractInputs.String, body, r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "input extraction failed: " + err.Error()})
			return
		}
		inputs = extracted
	}

	// Enqueue run
	n := m.seq.Add(1)
	runID := fmt.Sprintf("hook-%s-%04d", time.Now().UTC().Format("20060102-150405"), n)
	if err := m.runner.Enqueue(ctx, execution.Request{
		RunID:         runID,
		WorkspaceID:   trigger.WorkspaceID,
		BlueprintPath: trigger.BlueprintPath,
		Inputs:        inputs,
	}); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Update fired count
	_ = m.store.UpdateTriggerFired(ctx, trigger.ID)

	// One-shot: delete after firing
	if trigger.OneShot {
		_ = m.store.DeleteTrigger(ctx, trigger.ID)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{"run_id": runID})
}

// validateSignature checks the HMAC-SHA256 signature (GitHub X-Hub-Signature-256 convention).
// secret is the raw secret string (not hashed). sig is "sha256=<hex>".
func validateSignature(body []byte, secret string, sig string) bool {
	if sig == "" {
		return false
	}
	if !strings.HasPrefix(sig, "sha256=") {
		return false
	}
	sigHex := strings.TrimPrefix(sig, "sha256=")
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := mac.Sum(nil)

	return hmac.Equal(sigBytes, expected)
}

// extractInputs parses the extract_inputs JSON config and extracts values from the request.
// Config format: {"input_name": "body.field.path"} or {"input_name": "header.X-Custom"} or {"input_name": "query.param"}
func extractInputs(configJSON string, body []byte, r *http.Request) (map[string]any, error) {
	var config map[string]string
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		return nil, fmt.Errorf("invalid extract_inputs config: %w", err)
	}

	// Parse body as JSON (if any)
	var bodyData map[string]any
	if len(body) > 0 {
		_ = json.Unmarshal(body, &bodyData) // ok if it fails — body might not be JSON
	}

	inputs := map[string]any{}
	for inputName, sourcePath := range config {
		parts := strings.SplitN(sourcePath, ".", 2)
		if len(parts) < 2 {
			continue
		}
		source := parts[0]
		field := parts[1]

		switch source {
		case "body":
			if bodyData != nil {
				val := dotPathAccess(bodyData, field)
				if val != nil {
					inputs[inputName] = val
				}
			}
		case "header":
			if v := r.Header.Get(field); v != "" {
				inputs[inputName] = v
			}
		case "query":
			if v := r.URL.Query().Get(field); v != "" {
				inputs[inputName] = v
			}
		}
	}

	return inputs, nil
}

// dotPathAccess accesses a nested map value using dot notation.
// e.g., "repository.full_name" on {"repository": {"full_name": "foo/bar"}} returns "foo/bar".
func dotPathAccess(data map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = data
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current, ok = m[part]
		if !ok {
			return nil
		}
	}
	return current
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
