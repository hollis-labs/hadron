package mcpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hollis-labs/hadron/internal/execution"
	mcpclient "github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
)

const externalClientProbeInterval = 30 * time.Second

type InternalCaller struct {
	hadron        *Adapter
	servers       map[string]ExternalServerConfig
	clients       map[string]*externalClientEntry
	clientsMu     sync.Mutex
	clientFactory externalClientFactory
}

type ExternalServerConfig struct {
	Transport      string
	Command        string
	Args           []string
	Env            map[string]string
	URL            string
	Headers        map[string]string
	TimeoutSeconds int
}

type InternalCallerOption func(*InternalCaller)

type externalClient interface {
	CallTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)
	Ping(ctx context.Context) error
	Close() error
}

type externalClientEntry struct {
	client    externalClient
	transport string
	lastProbe time.Time
}

type externalClientFactory func(ctx context.Context, cfg ExternalServerConfig) (externalClient, error)

func WithExternalServers(servers map[string]ExternalServerConfig) InternalCallerOption {
	return func(c *InternalCaller) {
		if len(servers) == 0 {
			return
		}
		c.servers = make(map[string]ExternalServerConfig, len(servers))
		for name, cfg := range servers {
			c.servers[normalizeServerName(name)] = cfg
		}
	}
}

func NewInternalCaller(hadron *Adapter, opts ...InternalCallerOption) *InternalCaller {
	c := &InternalCaller{
		hadron:        hadron,
		servers:       map[string]ExternalServerConfig{},
		clients:       map[string]*externalClientEntry{},
		clientFactory: newExternalClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *InternalCaller) CallTool(ctx context.Context, serverName, toolName string, arguments map[string]any) (any, error) {
	if c == nil || c.hadron == nil {
		return nil, fmt.Errorf("internal MCP caller is not configured")
	}
	if !isLocalHadronServer(serverName) {
		return c.callExternalTool(ctx, serverName, toolName, arguments)
	}
	result := c.hadron.CallTool(ctx, toolName, arguments)
	if result == nil {
		return nil, fmt.Errorf("mcp tool %q returned no result", toolName)
	}
	payload, err := decodeToolResult(result)
	if err != nil {
		return nil, err
	}
	return execution.MCPToolResult{
		Result: payload,
		Metadata: execution.MCPCallMetadata{
			Server:       normalizeServerName(serverName),
			Transport:    "in_process",
			AttemptCount: 1,
		},
	}, nil
}

func isLocalHadronServer(name string) bool {
	switch normalizeServerName(name) {
	case "hadron", "local", "self":
		return true
	default:
		return false
	}
}

func normalizeServerName(name string) string {
	return strings.TrimSpace(strings.ToLower(name))
}

func (c *InternalCaller) callExternalTool(ctx context.Context, serverName, toolName string, arguments map[string]any) (any, error) {
	name := normalizeServerName(serverName)
	entry, reusedClient, err := c.externalClient(ctx, name)
	if err != nil {
		return nil, err
	}
	metadata := execution.MCPCallMetadata{
		Server:       name,
		Transport:    entry.transport,
		ReusedClient: reusedClient,
		AttemptCount: 1,
	}
	entry, healthProbed, reconnected, err := c.ensureHealthy(ctx, name, entry)
	if err != nil {
		return nil, fmt.Errorf("mcp_call %s.%s: %w", serverName, toolName, err)
	}
	metadata.HealthProbe = healthProbed
	metadata.Reconnected = reconnected

	for attempt := 0; attempt < 2; attempt++ {
		result, err := entry.client.CallTool(ctx, mcp.CallToolRequest{
			Params: mcp.CallToolParams{
				Name:      toolName,
				Arguments: arguments,
			},
		})
		if err == nil {
			payload, decodeErr := decodeToolResult(result)
			if decodeErr != nil {
				return nil, decodeErr
			}
			return execution.MCPToolResult{
				Result:   payload,
				Metadata: metadata,
			}, nil
		}
		if attempt == 0 && isRecoverableExternalClientError(err) && ctx.Err() == nil {
			metadata.RetryCount++
			metadata.AttemptCount++
			metadata.Reconnected = true
			c.invalidateExternalClient(name)
			entry, _, err = c.externalClient(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("mcp_call %s.%s: %w", serverName, toolName, err)
			}
			continue
		}
		return nil, fmt.Errorf("mcp_call %s.%s: %w", serverName, toolName, err)
	}
	return nil, fmt.Errorf("mcp_call %s.%s: exhausted retries", serverName, toolName)
}

func (c *InternalCaller) externalClient(ctx context.Context, name string) (*externalClientEntry, bool, error) {
	c.clientsMu.Lock()
	defer c.clientsMu.Unlock()

	if entry := c.clients[name]; entry != nil {
		return entry, true, nil
	}
	cfg, ok := c.servers[name]
	if !ok {
		return nil, false, fmt.Errorf("mcp server %q is not configured", name)
	}
	client, err := c.clientFactory(ctx, cfg)
	if err != nil {
		return nil, false, err
	}
	entry := &externalClientEntry{
		client:    client,
		transport: normalizeServerName(cfg.Transport),
		lastProbe: time.Now().UTC(),
	}
	if entry.transport == "" {
		entry.transport = "stdio"
	}
	c.clients[name] = entry
	return entry, false, nil
}

func newExternalClient(ctx context.Context, cfg ExternalServerConfig) (externalClient, error) {
	transportName := normalizeServerName(cfg.Transport)
	if transportName == "" {
		transportName = "stdio"
	}
	switch transportName {
	case "stdio":
		if strings.TrimSpace(cfg.Command) == "" {
			return nil, fmt.Errorf("mcp stdio server command is required")
		}
		client, err := mcpclient.NewStdioMCPClient(cfg.Command, flattenEnv(cfg.Env), cfg.Args...)
		if err != nil {
			return nil, fmt.Errorf("start mcp stdio server %q: %w", cfg.Command, err)
		}
		initReq := mcp.InitializeRequest{}
		initReq.Params.ClientInfo = mcp.Implementation{Name: "hadron", Version: "dev"}
		if _, err := client.Initialize(ctx, initReq); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("initialize mcp server %q: %w", cfg.Command, err)
		}
		return client, nil
	case "streamable_http", "http":
		if strings.TrimSpace(cfg.URL) == "" {
			return nil, fmt.Errorf("mcp %s server url is required", transportName)
		}
		opts := make([]transport.StreamableHTTPCOption, 0, 2)
		if len(cfg.Headers) > 0 {
			opts = append(opts, transport.WithHTTPHeaders(cloneStringMap(cfg.Headers)))
		}
		if cfg.TimeoutSeconds > 0 {
			opts = append(opts, transport.WithHTTPBasicClient(&http.Client{Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second}))
		}
		client, err := mcpclient.NewStreamableHttpClient(cfg.URL, opts...)
		if err != nil {
			return nil, fmt.Errorf("start mcp streamable_http server %q: %w", cfg.URL, err)
		}
		if err := client.Start(ctx); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("start mcp streamable_http client %q: %w", cfg.URL, err)
		}
		initReq := mcp.InitializeRequest{}
		initReq.Params.ClientInfo = mcp.Implementation{Name: "hadron", Version: "dev"}
		if _, err := client.Initialize(ctx, initReq); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("initialize mcp server %q: %w", cfg.URL, err)
		}
		return client, nil
	case "sse":
		if strings.TrimSpace(cfg.URL) == "" {
			return nil, fmt.Errorf("mcp sse server url is required")
		}
		opts := make([]transport.ClientOption, 0, 2)
		if len(cfg.Headers) > 0 {
			opts = append(opts, transport.WithHeaders(cloneStringMap(cfg.Headers)))
		}
		if cfg.TimeoutSeconds > 0 {
			opts = append(opts, transport.WithHTTPClient(&http.Client{Timeout: time.Duration(cfg.TimeoutSeconds) * time.Second}))
		}
		client, err := mcpclient.NewSSEMCPClient(cfg.URL, opts...)
		if err != nil {
			return nil, fmt.Errorf("start mcp sse server %q: %w", cfg.URL, err)
		}
		if err := client.Start(ctx); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("start mcp sse client %q: %w", cfg.URL, err)
		}
		initReq := mcp.InitializeRequest{}
		initReq.Params.ClientInfo = mcp.Implementation{Name: "hadron", Version: "dev"}
		if _, err := client.Initialize(ctx, initReq); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("initialize mcp server %q: %w", cfg.URL, err)
		}
		return client, nil
	default:
		return nil, fmt.Errorf("mcp transport %q is not supported", cfg.Transport)
	}
}

func flattenEnv(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	out := make([]string, 0, len(env))
	for k, v := range env {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out = append(out, k+"="+v)
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (c *InternalCaller) invalidateExternalClient(name string) {
	c.clientsMu.Lock()
	defer c.clientsMu.Unlock()
	if entry := c.clients[name]; entry != nil {
		_ = entry.client.Close()
		delete(c.clients, name)
	}
}

func (c *InternalCaller) Close() error {
	if c == nil {
		return nil
	}
	c.clientsMu.Lock()
	defer c.clientsMu.Unlock()
	var firstErr error
	for name, entry := range c.clients {
		if err := entry.client.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close mcp client %q: %w", name, err)
		}
		delete(c.clients, name)
	}
	return firstErr
}

func (c *InternalCaller) ensureHealthy(ctx context.Context, name string, entry *externalClientEntry) (*externalClientEntry, bool, bool, error) {
	if entry == nil || entry.client == nil {
		return nil, false, false, fmt.Errorf("mcp client is not initialized")
	}
	if entry.transport == "stdio" {
		return entry, false, false, nil
	}
	if time.Since(entry.lastProbe) < externalClientProbeInterval {
		return entry, false, false, nil
	}
	if err := entry.client.Ping(ctx); err != nil {
		if isRecoverableExternalClientError(err) && ctx.Err() == nil {
			c.invalidateExternalClient(name)
			replacement, _, openErr := c.externalClient(ctx, name)
			if openErr != nil {
				return nil, true, false, openErr
			}
			return replacement, true, true, nil
		}
		return nil, true, false, err
	}
	entry.lastProbe = time.Now().UTC()
	return entry, true, false, nil
}

func isRecoverableExternalClientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, transport.ErrTransportClosed) || errors.Is(err, transport.ErrSessionTerminated) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "transport closed") ||
		strings.Contains(msg, "session terminated") ||
		strings.Contains(msg, "connection lost")
}

func decodeToolResult(result *mcp.CallToolResult) (any, error) {
	if result.IsError {
		msg := decodeToolErrorMessage(result)
		if msg == "" {
			msg = "MCP tool returned an error"
		}
		return nil, fmt.Errorf("%s", msg)
	}
	if result.StructuredContent != nil {
		return result.StructuredContent, nil
	}
	texts := extractTextContent(result.Content)
	switch len(texts) {
	case 0:
		return map[string]any{}, nil
	case 1:
		payload := decodeTextPayload(texts[0])
		if msg, ok := payloadErrorMessage(payload); ok {
			return nil, fmt.Errorf("%s", msg)
		}
		return payload, nil
	default:
		out := make([]any, 0, len(texts))
		for _, text := range texts {
			payload := decodeTextPayload(text)
			if msg, ok := payloadErrorMessage(payload); ok {
				return nil, fmt.Errorf("%s", msg)
			}
			out = append(out, payload)
		}
		return out, nil
	}
}

func decodeToolErrorMessage(result *mcp.CallToolResult) string {
	if result == nil {
		return ""
	}
	texts := extractTextContent(result.Content)
	for _, text := range texts {
		var payload map[string]any
		if err := json.Unmarshal([]byte(text), &payload); err == nil {
			if message, ok := payload["message"].(string); ok && strings.TrimSpace(message) != "" {
				return message
			}
		}
		if strings.TrimSpace(text) != "" {
			return text
		}
	}
	return ""
}

func extractTextContent(contents []mcp.Content) []string {
	out := make([]string, 0, len(contents))
	for _, content := range contents {
		if text, ok := content.(mcp.TextContent); ok {
			out = append(out, text.Text)
		}
	}
	return out
}

func decodeTextPayload(text string) any {
	var parsed any
	if err := json.Unmarshal([]byte(text), &parsed); err == nil {
		return parsed
	}
	return text
}

func payloadErrorMessage(payload any) (string, bool) {
	m, ok := payload.(map[string]any)
	if !ok {
		return "", false
	}
	code, ok := m["code"].(string)
	if !ok || strings.TrimSpace(code) == "" {
		return "", false
	}
	message, ok := m["message"].(string)
	if !ok || strings.TrimSpace(message) == "" {
		return "", false
	}
	return message, true
}
