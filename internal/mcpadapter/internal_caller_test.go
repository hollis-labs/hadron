package mcpadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
)

type fakeExternalClient struct {
	pingErr   error
	callErr   error
	result    *mcp.CallToolResult
	pingCalls int
	callCalls int
}

func (f *fakeExternalClient) CallTool(_ context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	f.callCalls++
	if f.callErr != nil {
		return nil, f.callErr
	}
	if f.result != nil {
		return f.result, nil
	}
	return mcp.NewToolResultText(`{"ok":true}`), nil
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
