package mcpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/hollis-labs/go-mcp/budget"
	"github.com/hollis-labs/go-mcp/compat"
	"github.com/hollis-labs/go-otel/propagation"
	workflowmcp "github.com/hollis-labs/go-workflow/adapters/mcp"
	"github.com/hollis-labs/hadron/internal/execution"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// InternalCaller's external-MCP-client role (connecting out to third-party
// MCP servers over stdio/streamable_http/SSE, with reconnect and
// health-probing) has no go-mcp equivalent: go-mcp is server-only by
// current design. It is ported here directly against the official SDK's
// Client/ClientSession, per Chrispian's direction -- a shared go-mcp client
// package covering this same reconnect/health-probe shape (Tether has its
// own ~475-line version of the identical problem) is tracked separately as
// CW-20260918-0013, not built as a prerequisite for this port.

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

// externalClient is satisfied directly by *mcpsdk.ClientSession; declared
// narrowly so tests can substitute a fake.
type externalClient interface {
	CallTool(ctx context.Context, params *mcpsdk.CallToolParams) (*mcpsdk.CallToolResult, error)
	ListTools(ctx context.Context, params *mcpsdk.ListToolsParams) (*mcpsdk.ListToolsResult, error)
	Ping(ctx context.Context, params *mcpsdk.PingParams) error
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
	if isLocalHadronServer(name) {
		for _, def := range c.hadron.newServer().ToolDefinitions() {
			if def.Name != toolName {
				continue
			}
			readOnly, destructive := def.Annotations.ReadOnlyHint, def.Annotations.DestructiveHint
			idempotent, openWorld := def.Annotations.IdempotentHint, def.Annotations.OpenWorldHint
			return workflowmcp.ToolDescriptor{
				Server: serverName, Tool: def.Name, Trusted: false,
				Annotations: workflowmcp.ToolAnnotations{
					Title:           def.Title,
					ReadOnlyHint:    &readOnly,
					DestructiveHint: &destructive,
					IdempotentHint:  &idempotent,
					OpenWorldHint:   &openWorld,
				},
			}, nil
		}
		return workflowmcp.ToolDescriptor{}, fmt.Errorf("mcp tool %q is not registered on server %q", toolName, name)
	}
	entry, _, err := c.externalClient(ctx, name)
	if err != nil {
		return workflowmcp.ToolDescriptor{}, err
	}
	entry, _, _, err = c.ensureHealthy(ctx, name, entry)
	if err != nil {
		return workflowmcp.ToolDescriptor{}, err
	}
	listed, err := entry.client.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		return workflowmcp.ToolDescriptor{}, err
	}
	if listed == nil {
		return workflowmcp.ToolDescriptor{}, fmt.Errorf("mcp server %q returned no tool descriptor list", name)
	}
	for _, candidate := range listed.Tools {
		if candidate.Name != toolName {
			continue
		}
		return workflowmcp.ToolDescriptor{
			Server: serverName, Tool: candidate.Name, Trusted: false,
			Annotations: workflowToolAnnotations(candidate.Title, candidate.Annotations),
		}, nil
	}
	return workflowmcp.ToolDescriptor{}, fmt.Errorf("mcp tool %q is not registered on server %q", toolName, name)
}

func (c *InternalCaller) callToolResult(ctx context.Context, serverName, toolName string, arguments map[string]any, idempotencyKey string, allowRetry bool) (*mcpsdk.CallToolResult, execution.MCPCallMetadata, error) {
	if c == nil || c.hadron == nil {
		return nil, execution.MCPCallMetadata{}, fmt.Errorf("internal MCP caller is not configured")
	}
	if !isLocalHadronServer(serverName) {
		return c.callExternalToolResult(ctx, serverName, toolName, arguments, idempotencyKey, allowRetry)
	}
	value, err := c.hadron.CallTool(ctx, toolName, cloneAnyMap(arguments))
	metadata := execution.MCPCallMetadata{
		Server: normalizeServerName(serverName), Transport: "in_process", AttemptCount: 1,
	}
	if err != nil {
		return toolErrorResult(err), metadata, nil
	}
	if text, ok := value.(string); ok {
		return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: text}}}, metadata, nil
	}
	data, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return nil, metadata, marshalErr
	}
	return &mcpsdk.CallToolResult{
		Content:           []mcpsdk.Content{&mcpsdk.TextContent{Text: string(data)}},
		StructuredContent: value,
	}, metadata, nil
}

// toolErrorResult mirrors adaptHandler's own error-content shape (see
// go-mcp/server's marshaledResult, unexported there) for the in-process
// path, which bypasses adaptHandler entirely via Adapter.CallTool's direct
// dispatch: a *budget.ToolError or budget.StructuredError keeps its full
// structured shape, any other error falls back to its plain Error() string.
func toolErrorResult(err error) *mcpsdk.CallToolResult {
	var toolErr *budget.ToolError
	if errors.As(err, &toolErr) {
		return marshaledErrorResult(toolErr)
	}
	var structuredErr budget.StructuredError
	if errors.As(err, &structuredErr) {
		return marshaledErrorResult(structuredErr.ToolErrorContent())
	}
	data, marshalErr := json.Marshal(map[string]string{"message": err.Error()})
	if marshalErr != nil {
		data = []byte(`{"message":"internal error"}`)
	}
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: string(data)}},
		IsError: true,
	}
}

func marshaledErrorResult(v any) *mcpsdk.CallToolResult {
	data, err := json.Marshal(v)
	if err != nil {
		return &mcpsdk.CallToolResult{
			Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: `{"message":"internal error"}`}},
			IsError: true,
		}
	}
	return &mcpsdk.CallToolResult{
		Content:           []mcpsdk.Content{&mcpsdk.TextContent{Text: string(data)}},
		StructuredContent: v,
		IsError:           true,
	}
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

func (c *InternalCaller) callExternalToolResult(ctx context.Context, serverName, toolName string, arguments map[string]any, idempotencyKey string, allowRetry bool) (*mcpsdk.CallToolResult, execution.MCPCallMetadata, error) {
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
		params := &mcpsdk.CallToolParams{Name: toolName, Arguments: callArguments}
		if idempotencyKey != "" {
			params.Meta = mcpsdk.Meta{"hadron/idempotencyKey": idempotencyKey}
		}
		result, err := entry.client.CallTool(ctx, params)
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
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "hadron", Version: "dev"}, nil)
	switch transportName {
	case "stdio":
		if strings.TrimSpace(cfg.Command) == "" {
			return nil, fmt.Errorf("mcp stdio server command is required")
		}
		cmd := exec.CommandContext(ctx, cfg.Command, cfg.Args...) // #nosec G204 -- operator-configured MCP server command.
		if env := flattenEnv(cfg.Env); env != nil {
			cmd.Env = append(cmd.Environ(), env...)
		}
		cs, err := client.Connect(ctx, &mcpsdk.CommandTransport{Command: cmd}, nil)
		if err != nil {
			return nil, fmt.Errorf("start mcp stdio server %q: %w", cfg.Command, err)
		}
		return cs, nil
	case "streamable_http", "http":
		if strings.TrimSpace(cfg.URL) == "" {
			return nil, fmt.Errorf("mcp %s server url is required", transportName)
		}
		httpClient := headeredHTTPClient(cfg.Headers, cfg.TimeoutSeconds)
		cs, err := client.Connect(ctx, &mcpsdk.StreamableClientTransport{Endpoint: cfg.URL, HTTPClient: httpClient}, nil)
		if err != nil {
			return nil, fmt.Errorf("start mcp streamable_http server %q: %w", cfg.URL, err)
		}
		return cs, nil
	case "sse":
		if strings.TrimSpace(cfg.URL) == "" {
			return nil, fmt.Errorf("mcp sse server url is required")
		}
		httpClient := headeredHTTPClient(cfg.Headers, cfg.TimeoutSeconds)
		// compat.NewSSEClientTransport rewrites the server's relative
		// "endpoint" event to an absolute URL and sanitizes the stream; see
		// go-mcp's compat package. Known issue tracked separately
		// (CW-20260917-0032): its keepalive-skip path can stall Read in some
		// gateway-shaped streams.
		cs, err := client.Connect(ctx, compat.NewSSEClientTransport(cfg.URL, httpClient), nil)
		if err != nil {
			return nil, fmt.Errorf("start mcp sse server %q: %w", cfg.URL, err)
		}
		return cs, nil
	default:
		return nil, fmt.Errorf("mcp transport %q is not supported", cfg.Transport)
	}
}

// headeredHTTPClient builds an *http.Client that injects static headers on
// every request, for the streamable_http/sse transports' optional Headers
// config -- mcpsdk's client transports take an *http.Client, not a headers
// map, so this is the seam for it.
func headeredHTTPClient(headers map[string]string, timeoutSeconds int) *http.Client {
	client := &http.Client{}
	if timeoutSeconds > 0 {
		client.Timeout = time.Duration(timeoutSeconds) * time.Second
	}
	if len(headers) > 0 {
		client.Transport = &staticHeaderRoundTripper{headers: cloneStringMap(headers), base: http.DefaultTransport}
	}
	return client
}

type staticHeaderRoundTripper struct {
	headers map[string]string
	base    http.RoundTripper
}

func (t *staticHeaderRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	for k, v := range t.headers {
		cloned.Header.Set(k, v)
	}
	return t.base.RoundTrip(cloned)
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
	if err := entry.client.Ping(ctx, &mcpsdk.PingParams{}); err != nil {
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
	if errors.Is(err, mcpsdk.ErrConnectionClosed) || errors.Is(err, mcpsdk.ErrSessionMissing) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "transport closed") ||
		strings.Contains(msg, "connection closed") ||
		strings.Contains(msg, "session terminated") ||
		strings.Contains(msg, "session not found") ||
		strings.Contains(msg, "connection lost")
}

func decodeToolResult(result *mcpsdk.CallToolResult) (any, error) {
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

func decodeToolErrorMessage(result *mcpsdk.CallToolResult) string {
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

func extractTextContent(contents []mcpsdk.Content) []string {
	out := make([]string, 0, len(contents))
	for _, content := range contents {
		if text, ok := content.(*mcpsdk.TextContent); ok {
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

func workflowCallResult(result *mcpsdk.CallToolResult, metadata execution.MCPCallMetadata) (workflowmcp.CallResult, error) {
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

func workflowContent(content mcpsdk.Content) (workflowmcp.Content, error) {
	switch current := content.(type) {
	case *mcpsdk.TextContent:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil text content")
		}
		return workflowmcp.Content{Kind: workflowmcp.ContentText, Text: current.Text}, nil
	case *mcpsdk.ImageContent:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil image content")
		}
		return workflowmcp.Content{Kind: workflowmcp.ContentImage, Data: bytes.Clone(current.Data), MediaType: current.MIMEType}, nil
	case *mcpsdk.AudioContent:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil audio content")
		}
		return workflowmcp.Content{Kind: workflowmcp.ContentAudio, Data: bytes.Clone(current.Data), MediaType: current.MIMEType}, nil
	case *mcpsdk.ResourceLink:
		if current == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil resource link")
		}
		return workflowmcp.Content{
			Kind: workflowmcp.ContentResourceLink, URI: current.URI, Name: current.Name,
			Description: current.Description, MediaType: current.MIMEType,
		}, nil
	case *mcpsdk.EmbeddedResource:
		if current == nil || current.Resource == nil {
			return workflowmcp.Content{}, fmt.Errorf("nil embedded resource")
		}
		return workflowResource(current.Resource)
	default:
		return workflowmcp.Content{}, fmt.Errorf("unsupported content type %T", content)
	}
}

func workflowResource(resource *mcpsdk.ResourceContents) (workflowmcp.Content, error) {
	if resource == nil {
		return workflowmcp.Content{}, fmt.Errorf("nil resource contents")
	}
	if resource.Blob != nil {
		return workflowmcp.Content{
			Kind: workflowmcp.ContentResourceBlob, URI: resource.URI,
			Data: bytes.Clone(resource.Blob), MediaType: resource.MIMEType,
		}, nil
	}
	return workflowmcp.Content{
		Kind: workflowmcp.ContentResourceText, URI: resource.URI,
		Text: resource.Text, MediaType: resource.MIMEType,
	}, nil
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

func workflowToolAnnotations(title string, input *mcpsdk.ToolAnnotations) workflowmcp.ToolAnnotations {
	if input == nil {
		return workflowmcp.ToolAnnotations{Title: title}
	}
	readOnly, idempotent := input.ReadOnlyHint, input.IdempotentHint
	return workflowmcp.ToolAnnotations{
		Title:           title,
		ReadOnlyHint:    &readOnly,
		DestructiveHint: cloneBoolPointer(input.DestructiveHint),
		IdempotentHint:  &idempotent,
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
