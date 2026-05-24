package mcpadapter

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/mark3labs/mcp-go/mcp"
)

func TestNewServer_ToolAnnotations(t *testing.T) {
	store, err := persistence.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	adapter := New(store, nil, nil, nil, "", nil)
	srv := adapter.newServer()

	broker := srv.GetTool("hadron_blueprint_broker")
	if broker == nil {
		t.Fatal("expected hadron_blueprint_broker tool")
	}
	if broker.Tool.Annotations.ReadOnlyHint == nil || !*broker.Tool.Annotations.ReadOnlyHint {
		t.Fatalf("expected hadron_blueprint_broker to be read-only: %#v", broker.Tool.Annotations)
	}
	if broker.Tool.Annotations.IdempotentHint == nil || !*broker.Tool.Annotations.IdempotentHint {
		t.Fatalf("expected hadron_blueprint_broker to be idempotent: %#v", broker.Tool.Annotations)
	}

	runEnqueue := srv.GetTool("hadron_run_enqueue")
	if runEnqueue == nil {
		t.Fatal("expected hadron_run_enqueue tool")
	}
	if runEnqueue.Tool.Annotations.ReadOnlyHint == nil || *runEnqueue.Tool.Annotations.ReadOnlyHint {
		t.Fatalf("expected hadron_run_enqueue to be mutating: %#v", runEnqueue.Tool.Annotations)
	}
	if runEnqueue.Tool.Annotations.DestructiveHint == nil || *runEnqueue.Tool.Annotations.DestructiveHint {
		t.Fatalf("expected hadron_run_enqueue to be non-destructive: %#v", runEnqueue.Tool.Annotations)
	}
}

func TestHandlePromptPickBlueprint(t *testing.T) {
	adapter := &Adapter{}
	result, err := adapter.handlePromptPickBlueprint(context.Background(), mcp.GetPromptRequest{
		Params: mcp.GetPromptParams{
			Arguments: map[string]string{"task": "prepare a beta release"},
		},
	})
	if err != nil {
		t.Fatalf("handlePromptPickBlueprint: %v", err)
	}
	if result == nil || len(result.Messages) != 2 {
		t.Fatalf("unexpected prompt result: %#v", result)
	}
	if msg, ok := result.Messages[0].Content.(mcp.TextContent); !ok || !strings.Contains(msg.Text, "hadron_blueprint_broker") {
		t.Fatalf("unexpected first prompt message: %#v", result.Messages[0])
	}
}

func TestHandleBlueprintSchemaResource(t *testing.T) {
	store, err := persistence.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	dir := t.TempDir()
	content := strings.TrimSpace(`
blueprint:
  name: release-docs
  slug: release-docs
  title: Release Docs
  description: Build release notes.
inputs:
  - name: version
    type: string
    required: true
steps:
  - section: main
    tasks:
      - name: publish
        cmd: echo publish
`)
	path := dir + "/release-docs.yaml"
	if writeErr := os.WriteFile(path, []byte(content), 0o644); writeErr != nil {
		t.Fatalf("write blueprint: %v", writeErr)
	}

	adapter := New(store, nil, nil, nil, "", nil, WithBlueprintDir(dir))
	items, err := adapter.handleBlueprintSchemaResource(context.Background(), mcp.ReadResourceRequest{
		Params: mcp.ReadResourceParams{URI: "hadron://blueprints/release-docs/input-schema"},
	})
	if err != nil {
		t.Fatalf("handleBlueprintSchemaResource: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one resource content, got %#v", items)
	}
	text, ok := items[0].(mcp.TextResourceContents)
	if !ok || !strings.Contains(text.Text, "\"version\"") {
		t.Fatalf("unexpected resource content: %#v", items[0])
	}
}

func TestBlueprintCompletions(t *testing.T) {
	store, err := persistence.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	dir := t.TempDir()
	content := strings.TrimSpace(`
blueprint:
  name: release-docs
  slug: release-docs
  title: Release Docs
  tags: [release, docs]
steps:
  - section: main
    tasks:
      - name: publish
        cmd: echo publish
`)
	if writeErr := os.WriteFile(dir+"/release-docs.yaml", []byte(content), 0o644); writeErr != nil {
		t.Fatalf("write blueprint: %v", writeErr)
	}

	adapter := New(store, nil, nil, nil, "", nil, WithBlueprintDir(dir))

	tagCompletion, err := adapter.CompletePromptArgument(context.Background(), "hadron_pick_blueprint", mcp.CompleteArgument{
		Name:  "tag",
		Value: "re",
	}, mcp.CompleteContext{})
	if err != nil {
		t.Fatalf("CompletePromptArgument: %v", err)
	}
	if len(tagCompletion.Values) != 1 || tagCompletion.Values[0] != "release" {
		t.Fatalf("unexpected tag completions: %#v", tagCompletion)
	}

	refCompletion, err := adapter.CompleteResourceArgument(context.Background(), "hadron://blueprints/{blueprint_ref}/input-schema", mcp.CompleteArgument{
		Name:  "blueprint_ref",
		Value: "rel",
	}, mcp.CompleteContext{})
	if err != nil {
		t.Fatalf("CompleteResourceArgument: %v", err)
	}
	if len(refCompletion.Values) == 0 || refCompletion.Values[0] != "release-docs" {
		t.Fatalf("unexpected blueprint ref completions: %#v", refCompletion)
	}
}
