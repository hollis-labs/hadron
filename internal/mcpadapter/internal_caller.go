package mcpadapter

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hollis-labs/go-otel/propagation"
	"github.com/hollis-labs/hadron/internal/execution"
	workflowmcp "github.com/hollis-labs/hadron/workflow/adapters/mcp"
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

var (
	_ workflowmcp.Client     = (*InternalCaller)(nil)
	_ workflowmcp.Descriptor = (*InternalCaller)(nil)
)

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
	ListTools(ctx context.Context, request mcp.ListToolsRequest) (*mcp.ListToolsResult, error)
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
	result, metadata, err := c.callToolResult(ctx, serverName, toolName, arguments, "", true)
	if err != nil {
		return nil, err
	}
	payload, err := decodeToolResult(result)
	if err != nil {
		return nil, err
	}
	return execution.MCPToolResult{
		Result:   payload,
		Metadata: metadata,
	}, nil
}

// ExecuteTool implements the SDK-neutral workflow MCP client bridge while the
// legacy CallTool method above preserves blueprint execution behavior.
func (c *InternalCaller) ExecuteTool(ctx context.Context, request workflowmcp.CallRequest) (workflowmcp.CallResult, error) {
	result, metadata, err := c.callToolResult(ctx, request.Server, request.Tool, request.Arguments, request.IdempotencyKey, request.IdempotencyKey != "")
	if err != nil {
		return workflowmcp.CallResult{}, &workflowmcp.TransportError{
			Retryable: isRecoverableExternalClientError(err) || errors.Is(err, context.DeadlineExceeded),
			Cause:     err,
		}
	}
	converted, err := workflowCallResult(result, metadata)
	if err != nil {
		return workflowmcp.CallResult{}, &workflowmcp.ResultError{Cause: err}
	}
	return converted, nil
}

// DescribeTool implements the SDK-neutral pre-execution descriptor bridge.
// MCP servers self-assert annotations, so this bridge never marks them trusted;
// a host policy wrapper may do so after independent approval.
func (c *InternalCaller) DescribeTool(ctx context.Context, serverName, toolName string) (workflowmcp.ToolDescriptor, error) {
	if c == nil || c.hadron == nil {
		return workflowmcp.ToolDescriptor{}, fmt.Errorf("internal MCP caller is not configured")
	}
	name := normalizeServerName(serverName)
	var tool mcp.Tool
	if isLocalHadronServer(name) {
		registered := c.hadron.newServer().ListTools()
		entry := registered[toolName]
		if entry == nil {
			return workflowmcp.ToolDescriptor{}, fmt.Errorf("mcp tool %q is not registered on server %q", toolName, name)
		}
		tool = entry.Tool
	} else {
		entry, _, err := c.externalClient(ctx, name)
		if err != nil {
			return workflowmcp.ToolDescriptor{}, err
		}
		entry, _, _, err = c.ensureHealthy(ctx, name, entry)
		if err != nil {
			return workflowmcp.ToolDescriptor{}, err
		}
		listed, err := entry.client.ListTools(ctx, mcp.ListToolsRequest{})
		if err != nil {
			return workflowmcp.ToolDescriptor{}, err
		}
		if listed == nil {
			return workflowmcp.ToolDescriptor{}, fmt.Errorf("mcp server %q returned no tool descriptor list", name)
		}
		for _, candidate := range listed.Tools {
			if candidate.Name == toolName {
				tool = candidate
				break
			}
		}
		if tool.Name == "" {
			return workflowmcp.ToolDescriptor{}, fmt.Errorf("mcp tool %q is not registered on server %q", toolName, name)
		}
	}
	return workflowmcp.ToolDescriptor{
		Server: serverName, Tool: tool.Name, Trusted: false,
		Annotations: workflowToolAnnotations(tool.Annotations),
	}, nil
}

func (c *InternalCaller) callToolResult(ctx context.Context, serverName, toolName string, arguments map[string]any, idempotencyKey string, allowRetry bool) (*mcp.CallToolResult, execution.MCPCallMetadata, error) {
	if c == nil || c.hadron == nil {
		return nil, execution.MCPCallMetadata{}, fmt.Errorf("internal MCP caller is not configured")
	}
	if !isLocalHadronServer(serverName) {
		return c.callExternalToolResult(ctx, serverName, toolName, arguments, idempotencyKey, allowRetry)
	}
	request := mcp.CallToolRequest{}
	request.Params.Name = toolName
	request.Params.Arguments = cloneAnyMap(arguments)
	if idempotencyKey != "" {
		request.Params.Meta = &mcp.Meta{AdditionalFields: map[string]any{"hadron/idempotencyKey": idempotencyKey}}
	}
	handler := c.hadron.buildHandlerMap()[toolName]
	if handler == nil {
		return toolError("not_found", "unknown tool: "+toolName), execution.MCPCallMetadata{
			Server: normalizeServerName(serverName), Transport: "in_process", AttemptCount: 1,
		}, nil
	}
	result, err := handler(ctx, request)
	if err != nil {
		result = toolError("internal_error", err.Error())
	}
	if result == nil {
		return nil, execution.MCPCallMetadata{}, fmt.Errorf("mcp tool %q returned no result", toolName)
	}
	return result, execution.MCPCallMetadata{
		Server: normalizeServerName(serverName), Transport: "in_process", AttemptCount: 1,
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

func (c *InternalCaller) callExternalToolResult(ctx context.Context, serverName, toolName string, arguments map[string]any, idempotencyKey string, allowRetry bool) (*mcp.CallToolResult, execution.MCPCallMetadata, error) {
	name := normalizeServerName(serverName)
	entry, reusedClient, err := c.externalClient(ctx, name)
	if err != nil {
		return nil, execution.MCPCallMetadata{}, err
	}
	metadata := execution.MCPCallMetadata{
		Server:       name,
		Transport:    entry.transport,
		ReusedClient: reusedClient,
		AttemptCount: 1,
	}
	entry, healthProbed, reconnected, err := c.ensureHealthy(ctx, name, entry)
	if err != nil {
		return nil, metadata, fmt.Errorf("mcp_call %s.%s: %w", serverName, toolName, err)
	}
	metadata.HealthProbe = healthProbed
	metadata.Reconnected = reconnected

	for attempt := 0; attempt < 2; attempt++ {
		callArguments := cloneAnyMap(arguments)
		callArguments = propagation.InjectMCP(ctx, callArguments)
		request := mcp.CallToolRequest{Params: mcp.CallToolParams{Name: toolName, Arguments: callArguments}}
		if idempotencyKey != "" {
			request.Params.Meta = &mcp.Meta{AdditionalFields: map[string]any{"hadron/idempotencyKey": idempotencyKey}}
		}
		result, err := entry.client.CallTool(ctx, request)
		if err == nil {
			if result == nil {
				return nil, metadata, fmt.Errorf("mcp tool %q returned no result", toolName)
			}
			return result, metadata, nil
		}
		if attempt == 0 && allowRetry && isRecoverableExternalClientError(err) && ctx.Err() == nil {
			metadata.RetryCount++
			metadata.AttemptCount++
			metadata.Reconnected = true
			c.invalidateExternalClient(name)
			entry, _, err = c.externalClient(ctx, name)
			if err != nil {
				return nil, metadata, fmt.Errorf("mcp_call %s.%s: %w", serverName, toolName, err)
			}
			continue
		}
		return nil, metadata, fmt.Errorf("mcp_call %s.%s: %w", serverName, toolName, err)
	}
	return nil, metadata, fmt.Errorf("mcp_call %s.%s: exhausted retries", serverName, toolName)
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

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
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

func workflowCallResult(result *mcp.CallToolResult, metadata execution.MCPCallMetadata) (workflowmcp.CallResult, error) {
	if result == nil {
		return workflowmcp.CallResult{}, fmt.Errorf("MCP tool returned no result")
	}
	converted := workflowmcp.CallResult{
		IsError: result.IsError,
		Transport: workflowmcp.TransportMetadata{
			Transport: metadata.Transport, AttemptCount: metadata.AttemptCount,
			RetryCount: metadata.RetryCount, Reconnected: metadata.Reconnected,
		},
	}
	if result.StructuredContent != nil {
		structured, err := workflowJSON(result.StructuredContent)
		if err != nil {
			return workflowmcp.CallResult{}, fmt.Errorf("MCP structured content is not JSON-compatible: %w", err)
		}
		converted.HasStructured = true
		converted.Structured = structured
	}
	converted.Content = make([]workflowmcp.Content, 0, len(result.Content))
	for index, content := range result.Content {
		mapped, err := workflowContent(content)
		if err != nil {
			return workflowmcp.CallResult{}, fmt.Errorf("MCP content[%d]: %w", index, err)
		}
		converted.Content = append(converted.Content, mapped)
	}
	return converted, nil
}

func workflowContent(content mcp.Content) (workflowmcp.Content, error) {
	switch current := content.(type) {
	case mcp.TextContent:
		return workflowmcp.Content{Kind: workflowmcp.ContentText, Text: current.Text}, nil
	case *mcp.TextContent:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil text content")
		}
		return workflowmcp.Content{Kind: workflowmcp.ContentText, Text: current.Text}, nil
	case mcp.ImageContent:
		data, err := base64.StdEncoding.DecodeString(current.Data)
		if err != nil {
			return workflowmcp.Content{}, fmt.Errorf("decode image data: %w", err)
		}
		return workflowmcp.Content{Kind: workflowmcp.ContentImage, Data: data, MediaType: current.MIMEType}, nil
	case *mcp.ImageContent:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil image content")
		}
		return workflowContent(*current)
	case mcp.AudioContent:
		data, err := base64.StdEncoding.DecodeString(current.Data)
		if err != nil {
			return workflowmcp.Content{}, fmt.Errorf("decode audio data: %w", err)
		}
		return workflowmcp.Content{Kind: workflowmcp.ContentAudio, Data: data, MediaType: current.MIMEType}, nil
	case *mcp.AudioContent:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil audio content")
		}
		return workflowContent(*current)
	case mcp.ResourceLink:
		return workflowmcp.Content{
			Kind: workflowmcp.ContentResourceLink, URI: current.URI, Name: current.Name,
			Description: current.Description, MediaType: current.MIMEType,
		}, nil
	case *mcp.ResourceLink:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil resource link")
		}
		return workflowContent(*current)
	case mcp.EmbeddedResource:
		return workflowResource(current.Resource)
	case *mcp.EmbeddedResource:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil embedded resource")
		}
		return workflowResource(current.Resource)
	default:
		return workflowmcp.Content{}, fmt.Errorf("unsupported content type %T", content)
	}
}

func workflowResource(resource mcp.ResourceContents) (workflowmcp.Content, error) {
	switch current := resource.(type) {
	case mcp.TextResourceContents:
		return workflowmcp.Content{
			Kind: workflowmcp.ContentResourceText, URI: current.URI,
			Text: current.Text, MediaType: current.MIMEType,
		}, nil
	case *mcp.TextResourceContents:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil text resource")
		}
		return workflowResource(*current)
	case mcp.BlobResourceContents:
		data, err := base64.StdEncoding.DecodeString(current.Blob)
		if err != nil {
			return workflowmcp.Content{}, fmt.Errorf("decode resource blob: %w", err)
		}
		return workflowmcp.Content{
			Kind: workflowmcp.ContentResourceBlob, URI: current.URI,
			Data: data, MediaType: current.MIMEType,
		}, nil
	case *mcp.BlobResourceContents:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil blob resource")
		}
		return workflowResource(*current)
	default:
		return workflowmcp.Content{}, fmt.Errorf("unsupported resource type %T", resource)
	}
}

func workflowJSON(input any) (any, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var output any
	if err := decoder.Decode(&output); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON documents")
		}
		return nil, err
	}
	return output, nil
}

func workflowToolAnnotations(input mcp.ToolAnnotation) workflowmcp.ToolAnnotations {
	return workflowmcp.ToolAnnotations{
		Title: input.Title, ReadOnlyHint: cloneBoolPointer(input.ReadOnlyHint),
		DestructiveHint: cloneBoolPointer(input.DestructiveHint),
		IdempotentHint:  cloneBoolPointer(input.IdempotentHint),
		OpenWorldHint:   cloneBoolPointer(input.OpenWorldHint),
	}
}

func cloneBoolPointer(input *bool) *bool {
	if input == nil {
		return nil
	}
	output := *input
	return &output
}
