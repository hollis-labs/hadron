package trigger

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// Store is the persistence interface required by the trigger manager.
type Store interface {
	GetTriggerByPath(ctx context.Context, path string) (persistence.TriggerRecord, error)
	UpdateTriggerFired(ctx context.Context, id string) error
	DeleteTrigger(ctx context.Context, id string) error
	ListWebhookTriggers(ctx context.Context) ([]persistence.TriggerRecord, error)
}

// Runner enqueues blueprint runs.
type Runner interface {
	Enqueue(ctx context.Context, req execution.Request) error
}

// Manager handles webhook trigger routing and execution.
type Manager struct {
	store  Store
	runner Runner
	seq    atomic.Uint64
}

// New creates a new trigger Manager.
func New(store Store, runner Runner) *Manager {
	return &Manager{store: store, runner: runner}
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
