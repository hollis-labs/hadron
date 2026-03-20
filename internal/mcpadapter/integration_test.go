package mcpadapter_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/mcpadapter"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/scheduler"
	"github.com/mark3labs/mcp-go/mcp"
)

// ── Fakes ──────────────────────────────────────────────────────────────────────

type fakeRunner struct{}

func (f *fakeRunner) Enqueue(_ context.Context, _ execution.Request) error { return nil }
func (f *fakeRunner) Cancel(_ string) bool                                 { return false }

type fakePipelineRunner struct{}

func (f *fakePipelineRunner) Start(_ context.Context, _, _, _ string) error { return nil }

type fakeScheduler struct{}

func (f *fakeScheduler) Start()                          {}
func (f *fakeScheduler) Stop()                           {}
func (f *fakeScheduler) TickNow(_ context.Context) error { return nil }
func (f *fakeScheduler) Status() scheduler.Status        { return scheduler.Status{Running: true} }

// ── Helpers ───────────────────────────────────────────────────────────────────

func newTestStore(t *testing.T) *persistence.Store {
	t.Helper()
	store, err := persistence.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestAdapter(t *testing.T) *mcpadapter.Adapter {
	t.Helper()
	store := newTestStore(t)
	return mcpadapter.New(store, &fakeRunner{}, &fakeScheduler{}, &fakePipelineRunner{}, "", nil)
}

// callTool invokes a registered tool and returns the text result.
func callTool(t *testing.T, adapter *mcpadapter.Adapter, toolName string, args map[string]any) map[string]any {
	t.Helper()
	result := adapter.CallTool(context.Background(), toolName, args)
	if result == nil {
		t.Fatalf("tool %s returned nil", toolName)
	}
	if len(result.Content) == 0 {
		t.Fatalf("tool %s returned empty content", toolName)
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("tool %s returned non-text content", toolName)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
		t.Fatalf("tool %s: unmarshal result: %v (raw: %s)", toolName, err, text.Text)
	}
	return out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestMCP_Health(t *testing.T) {
	adapter := newTestAdapter(t)
	result := adapter.CallTool(context.Background(), "hadron_health", nil)
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Content) == 0 {
		t.Fatal("expected content in result")
	}
	text, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatal("expected text content")
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(text.Text), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", out["status"])
	}
}

func TestMCP_BlueprintValidate_Valid(t *testing.T) {
	adapter := newTestAdapter(t)

	validContent := strings.TrimSpace(`
blueprint:
  name: test
steps:
  - section: main
    tasks:
      - name: greet
        cmd: echo hello
`)

	out := callTool(t, adapter, "hadron_blueprint_validate", map[string]any{
		"content": validContent,
	})
	if out["valid"] != true {
		t.Fatalf("expected valid=true, got %v", out)
	}
	if _, hasErr := out["error"]; hasErr {
		t.Fatalf("expected no error field, got %v", out)
	}
}

func TestMCP_BlueprintValidate_Invalid(t *testing.T) {
	adapter := newTestAdapter(t)

	out := callTool(t, adapter, "hadron_blueprint_validate", map[string]any{
		"content": "not_a_blueprint: true",
	})
	if out["valid"] != false {
		t.Fatalf("expected valid=false, got %v", out)
	}
	if out["error"] == nil || out["error"] == "" {
		t.Fatalf("expected error message, got %v", out)
	}
}
