package mcpadapter

import (
	"context"
	"os"
	"strings"
	"testing"

	gomcp "github.com/hollis-labs/go-mcp/server"
	"github.com/hollis-labs/hadron/internal/persistence"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestNewServer_ToolAnnotations(t *testing.T) {
	store, err := persistence.Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	adapter := New(store, nil, nil, nil, "", nil)
	srv := adapter.newServer()

	defs := map[string]gomcp.ToolDefinition{}
	for _, def := range srv.ToolDefinitions() {
		defs[def.Name] = def
	}

	broker, ok := defs["hadron_blueprint_broker"]
	if !ok {
		t.Fatal("expected hadron_blueprint_broker tool")
	}
	if !broker.Annotations.ReadOnlyHint {
		t.Fatalf("expected hadron_blueprint_broker to be read-only: %#v", broker.Annotations)
	}
	if !broker.Annotations.IdempotentHint {
		t.Fatalf("expected hadron_blueprint_broker to be idempotent: %#v", broker.Annotations)
	}

	runEnqueue, ok := defs["hadron_run_enqueue"]
	if !ok {
		t.Fatal("expected hadron_run_enqueue tool")
	}
	if runEnqueue.Annotations.ReadOnlyHint {
		t.Fatalf("expected hadron_run_enqueue to be mutating: %#v", runEnqueue.Annotations)
	}
	if runEnqueue.Annotations.DestructiveHint {
		t.Fatalf("expected hadron_run_enqueue to be non-destructive: %#v", runEnqueue.Annotations)
	}
}

func TestHandlePromptPickBlueprint(t *testing.T) {
	adapter := &Adapter{}
	result, err := adapter.handlePromptPickBlueprint(context.Background(), &mcpsdk.GetPromptRequest{
		Params: &mcpsdk.GetPromptParams{
			Arguments: map[string]string{"task": "prepare a beta release"},
		},
	})
	if err != nil {
		t.Fatalf("handlePromptPickBlueprint: %v", err)
	}
	if result == nil || len(result.Messages) != 2 {
		t.Fatalf("unexpected prompt result: %#v", result)
	}
	if msg, ok := result.Messages[0].Content.(*mcpsdk.TextContent); !ok || !strings.Contains(msg.Text, "hadron_blueprint_broker") {
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
	result, err := adapter.handleBlueprintSchemaResource(context.Background(), &mcpsdk.ReadResourceRequest{
		Params: &mcpsdk.ReadResourceParams{URI: "hadron://blueprints/release-docs/input-schema"},
	})
	if err != nil {
		t.Fatalf("handleBlueprintSchemaResource: %v", err)
	}
	if result == nil || len(result.Contents) != 1 {
		t.Fatalf("expected one resource content, got %#v", result)
	}
	if !strings.Contains(result.Contents[0].Text, "\"version\"") {
		t.Fatalf("unexpected resource content: %#v", result.Contents[0])
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

	tagCompletion, err := adapter.handleCompletion(context.Background(), &mcpsdk.CompleteRequest{
		Params: &mcpsdk.CompleteParams{
			Ref:      &mcpsdk.CompleteReference{Type: "ref/prompt", Name: "hadron_pick_blueprint"},
			Argument: mcpsdk.CompleteParamsArgument{Name: "tag", Value: "re"},
		},
	})
	if err != nil {
		t.Fatalf("handleCompletion (prompt): %v", err)
	}
	if len(tagCompletion.Completion.Values) != 1 || tagCompletion.Completion.Values[0] != "release" {
		t.Fatalf("unexpected tag completions: %#v", tagCompletion.Completion)
	}

	refCompletion, err := adapter.handleCompletion(context.Background(), &mcpsdk.CompleteRequest{
		Params: &mcpsdk.CompleteParams{
			Ref:      &mcpsdk.CompleteReference{Type: "ref/resource", URI: "hadron://blueprints/{blueprint_ref}/input-schema"},
			Argument: mcpsdk.CompleteParamsArgument{Name: "blueprint_ref", Value: "rel"},
		},
	})
	if err != nil {
		t.Fatalf("handleCompletion (resource): %v", err)
	}
	if len(refCompletion.Completion.Values) == 0 || refCompletion.Completion.Values[0] != "release-docs" {
		t.Fatalf("unexpected blueprint ref completions: %#v", refCompletion.Completion)
	}
}
