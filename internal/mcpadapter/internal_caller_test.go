package mcpadapter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	workflowmcp "github.com/hollis-labs/hadron/workflow/adapters/mcp"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
)

type fakeExternalClient struct {
	pingErr   error
	callErr   error
	result    *mcp.CallToolResult
	tools     []mcp.Tool
	listErr   error
	listNil   bool
	pingCalls int
	callCalls int
	request   mcp.CallToolRequest
}

func (f *fakeExternalClient) ListTools(_ context.Context, _ mcp.ListToolsRequest) (*mcp.ListToolsResult, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.listNil {
		return nil, nil
	}
	return &mcp.ListToolsResult{Tools: append([]mcp.Tool(nil), f.tools...)}, nil
}

func (f *fakeExternalClient) CallTool(_ context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	f.callCalls++
	f.request = request
	if f.callErr != nil {
		return nil, f.callErr
	}
	if f.result != nil {
		return f.result, nil
	}
	return mcp.NewToolResultText(`{"ok":true}`), nil
}

func TestInternalCallerWorkflowBridgeConvertsContentAndDescriptor(t *testing.T) {
	readOnly, destructive, idempotent := true, false, true
	external := &fakeExternalClient{
		tools: []mcp.Tool{{
			Name: "inspect", Annotations: mcp.ToolAnnotation{
				Title: "Inspect", ReadOnlyHint: &readOnly,
				DestructiveHint: &destructive, IdempotentHint: &idempotent,
			},
		}},
		result: &mcp.CallToolResult{
			StructuredContent: map[string]any{"large": json.Number("9007199254740993")},
			Content: []mcp.Content{
				mcp.TextContent{Type: mcp.ContentTypeText, Text: "hello"},
				mcp.ImageContent{Type: mcp.ContentTypeImage, Data: base64.StdEncoding.EncodeToString([]byte("image")), MIMEType: "image/png"},
				mcp.ResourceLink{Type: "resource_link", URI: "resource://fixture/item", Name: "item", MIMEType: "text/plain"},
				mcp.EmbeddedResource{Type: mcp.ContentTypeResource, Resource: mcp.BlobResourceContents{
					URI: "resource://fixture/blob", MIMEType: "application/octet-stream", Blob: base64.StdEncoding.EncodeToString([]byte("blob")),
				}},
			},
		},
	}
	caller := NewInternalCaller(&Adapter{})
	caller.servers["fixture"] = ExternalServerConfig{Transport: "stdio", Command: "unused"}
	caller.clientFactory = func(context.Context, ExternalServerConfig) (externalClient, error) { return external, nil }

	description, err := caller.DescribeTool(t.Context(), "fixture", "inspect")
	if err != nil {
		t.Fatalf("DescribeTool() error = %v", err)
	}
	if description.Server != "fixture" || description.Tool != "inspect" || description.Trusted ||
		description.Annotations.ReadOnlyHint == nil || !*description.Annotations.ReadOnlyHint {
		t.Fatalf("description = %#v", description)
	}
	result, err := caller.ExecuteTool(t.Context(), workflowmcp.CallRequest{
		Server: "fixture", Tool: "inspect", Arguments: map[string]any{"query": "hello"}, IdempotencyKey: "call-key",
	})
	if err != nil {
		t.Fatalf("ExecuteTool() error = %v", err)
	}
	if !result.HasStructured || result.Structured.(map[string]any)["large"] != json.Number("9007199254740993") || len(result.Content) != 4 {
		t.Fatalf("result = %#v", result)
	}
	if string(result.Content[1].Data) != "image" || result.Content[2].Kind != workflowmcp.ContentResourceLink || string(result.Content[3].Data) != "blob" {
		t.Fatalf("content = %#v", result.Content)
	}
	if external.request.Params.Meta == nil || external.request.Params.Meta.AdditionalFields["hadron/idempotencyKey"] != "call-key" {
		t.Fatalf("call meta = %#v", external.request.Params.Meta)
	}
	result.Content[1].Data[0] = 'X'
	if external.result.Content[1].(mcp.ImageContent).Data != base64.StdEncoding.EncodeToString([]byte("image")) {
		t.Fatal("workflow result mutated SDK result")
	}
}

func TestInternalCallerWorkflowBridgeClassifiesMalformedResult(t *testing.T) {
	external := &fakeExternalClient{result: &mcp.CallToolResult{Content: []mcp.Content{
		mcp.ImageContent{Type: mcp.ContentTypeImage, Data: "not-base64", MIMEType: "image/png"},
	}}}
	caller := NewInternalCaller(&Adapter{})
	caller.servers["fixture"] = ExternalServerConfig{Transport: "stdio", Command: "unused"}
	caller.clientFactory = func(context.Context, ExternalServerConfig) (externalClient, error) { return external, nil }
	_, err := caller.ExecuteTool(t.Context(), workflowmcp.CallRequest{Server: "fixture", Tool: "broken", Arguments: map[string]any{}})
	var resultErr *workflowmcp.ResultError
	if !errors.As(err, &resultErr) || err.Error() != "MCP result conversion failed" {
		t.Fatalf("ExecuteTool() error = %#v", err)
	}
}

func TestInternalCallerDescribeToolPreservesConfiguredAliasAndRejectsNilList(t *testing.T) {
	caller := NewInternalCaller(&Adapter{})
	description, err := caller.DescribeTool(t.Context(), "self", "hadron_health")
	if err != nil {
		t.Fatalf("DescribeTool(self) error = %v", err)
	}
	if description.Server != "self" || description.Tool != "hadron_health" {
		t.Fatalf("description = %#v", description)
	}

	external := &fakeExternalClient{listNil: true}
	caller.servers["fixture"] = ExternalServerConfig{Transport: "stdio", Command: "unused"}
	caller.clientFactory = func(context.Context, ExternalServerConfig) (externalClient, error) { return external, nil }
	if _, err := caller.DescribeTool(t.Context(), "fixture", "missing"); err == nil {
		t.Fatal("DescribeTool accepted nil tool list")
	}
}

func TestInternalCallerWorkflowRetryRequiresIdempotencyKey(t *testing.T) {
	tests := []struct {
		name          string
		idempotency   string
		wantFactories int
		wantCalls     []int
		wantError     bool
	}{
		{name: "unkeyed fails once", wantFactories: 1, wantCalls: []int{1}, wantError: true},
		{name: "keyed reconnects", idempotency: "stable-key", wantFactories: 2, wantCalls: []int{1, 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first := &fakeExternalClient{callErr: transport.ErrTransportClosed}
			second := &fakeExternalClient{result: mcp.NewToolResultText("ok")}
			clients := []externalClient{first, second}
			factoryCalls := 0
			caller := NewInternalCaller(&Adapter{})
			caller.servers["fixture"] = ExternalServerConfig{Transport: "stdio", Command: "unused"}
			caller.clientFactory = func(context.Context, ExternalServerConfig) (externalClient, error) {
				factoryCalls++
				current := clients[0]
				clients = clients[1:]
				return current, nil
			}
			_, err := caller.ExecuteTool(t.Context(), workflowmcp.CallRequest{
				Server: "fixture", Tool: "mutate", Arguments: map[string]any{}, IdempotencyKey: test.idempotency,
			})
			if (err != nil) != test.wantError {
				t.Fatalf("ExecuteTool() error = %v", err)
			}
			calls := []int{first.callCalls}
			if factoryCalls == 2 {
				calls = append(calls, second.callCalls)
			}
			if factoryCalls != test.wantFactories || !reflect.DeepEqual(calls, test.wantCalls) {
				t.Fatalf("factories/calls = %d/%v", factoryCalls, calls)
			}
			if test.idempotency == "" {
				if first.request.Params.Meta != nil {
					t.Fatalf("unkeyed request meta = %#v", first.request.Params.Meta)
				}
			} else if first.request.Params.Meta == nil || first.request.Params.Meta.AdditionalFields["hadron/idempotencyKey"] != test.idempotency ||
				second.request.Params.Meta == nil || second.request.Params.Meta.AdditionalFields["hadron/idempotencyKey"] != test.idempotency {
				t.Fatalf("keyed retry did not retain key: %#v / %#v", first.request.Params.Meta, second.request.Params.Meta)
			}
		})
	}
}

func (f *fakeExternalClient) Ping(_ context.Context) error {
	f.pingCalls++
	return f.pingErr
}

func (f *fakeExternalClient) Close() error { return nil }

func TestInternalCallerRetriesRecoverableCallError(t *testing.T) {
	first := &fakeExternalClient{callErr: transport.ErrTransportClosed}
	second := &fakeExternalClient{result: mcp.NewToolResultText(`{"ok":true}`)}
	clients := []externalClient{first, second}
	factoryCalls := 0

	caller := NewInternalCaller(&Adapter{})
	caller.servers["fake"] = ExternalServerConfig{Transport: "stdio", Command: "unused"}
	caller.clientFactory = func(ctx context.Context, cfg ExternalServerConfig) (externalClient, error) {
		_ = ctx
		_ = cfg
		factoryCalls++
		if len(clients) == 0 {
			return nil, errors.New("no clients left")
		}
		client := clients[0]
		clients = clients[1:]
		return client, nil
	}

	result, err := caller.CallTool(context.Background(), "fake", "echo_json", nil)
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	wrapped, ok := result.(execution.MCPToolResult)
	if !ok {
		t.Fatalf("expected MCPToolResult, got %T", result)
	}
	payload, ok := wrapped.Result.(map[string]any)
	if !ok || payload["ok"] != true {
		t.Fatalf("unexpected payload: %#v", result)
	}
	if wrapped.Metadata.RetryCount != 1 || wrapped.Metadata.AttemptCount != 2 || !wrapped.Metadata.Reconnected {
		t.Fatalf("unexpected metadata: %#v", wrapped.Metadata)
	}
	if factoryCalls != 2 {
		t.Fatalf("expected 2 factory calls, got %d", factoryCalls)
	}
	if first.callCalls != 1 {
		t.Fatalf("expected first client to be used once, got %d", first.callCalls)
	}
	if second.callCalls != 1 {
		t.Fatalf("expected second client to be used once, got %d", second.callCalls)
	}
}

func TestInternalCallerReconnectsOnFailedHealthProbe(t *testing.T) {
	stale := &fakeExternalClient{pingErr: transport.ErrTransportClosed}
	fresh := &fakeExternalClient{result: mcp.NewToolResultText(`{"ok":true}`)}
	factoryCalls := 0

	caller := NewInternalCaller(&Adapter{})
	caller.servers["fake"] = ExternalServerConfig{Transport: "streamable_http", URL: "http://example.invalid"}
	caller.clientFactory = func(ctx context.Context, cfg ExternalServerConfig) (externalClient, error) {
		_ = ctx
		_ = cfg
		factoryCalls++
		if factoryCalls == 1 {
			return stale, nil
		}
		return fresh, nil
	}

	entry, _, err := caller.externalClient(context.Background(), "fake")
	if err != nil {
		t.Fatalf("externalClient: %v", err)
	}
	entry.lastProbe = time.Now().UTC().Add(-externalClientProbeInterval - time.Second)

	result, err := caller.CallTool(context.Background(), "fake", "echo_json", nil)
	if err != nil {
		t.Fatalf("CallTool returned error: %v", err)
	}
	wrapped, ok := result.(execution.MCPToolResult)
	if !ok {
		t.Fatalf("expected MCPToolResult, got %T", result)
	}
	payload, ok := wrapped.Result.(map[string]any)
	if !ok || payload["ok"] != true {
		t.Fatalf("unexpected payload: %#v", result)
	}
	if !wrapped.Metadata.HealthProbe || !wrapped.Metadata.Reconnected {
		t.Fatalf("expected health probe reconnect metadata, got %#v", wrapped.Metadata)
	}
	if stale.pingCalls != 1 {
		t.Fatalf("expected stale client ping once, got %d", stale.pingCalls)
	}
	if stale.callCalls != 0 {
		t.Fatalf("expected stale client not to be used for tool call, got %d", stale.callCalls)
	}
	if fresh.callCalls != 1 {
		t.Fatalf("expected fresh client to service tool call, got %d", fresh.callCalls)
	}
	if factoryCalls != 2 {
		t.Fatalf("expected 2 factory calls, got %d", factoryCalls)
	}
}
