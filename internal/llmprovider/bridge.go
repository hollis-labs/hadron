package llmprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	llmcontracts "github.com/hollis-labs/go-llm-contracts"
	llmtypes "github.com/hollis-labs/go-llm-types"
	providers "github.com/hollis-labs/go-providers/provider"

	workflowllm "github.com/hollis-labs/hadron/workflow/adapters/llm"
	"github.com/hollis-labs/hadron/workflow/values"
)

var (
	// ErrInvalidBridge reports a missing registry, context, provider binding, or
	// otherwise malformed bridge request.
	ErrInvalidBridge = errors.New("invalid Hadron LLM provider bridge request")
	// ErrUnsupported reports a requested behavior that go-llm-contracts cannot
	// represent or account for truthfully.
	ErrUnsupported = errors.New("unsupported go-llm-contracts capability")
	// ErrUpstreamResult reports an ambiguous or malformed upstream event stream.
	ErrUpstreamResult = errors.New("invalid go-llm-contracts provider result")
)

// Bridge resolves the provider selected by a host-approved ProviderBinding
// through a concurrency-safe go-providers registry.
type Bridge struct {
	registry              *providers.Registry
	proposalOnlyProviders map[string]llmcontracts.Provider
}

// Options contains host assertions that cannot be inferred from the installed
// go-llm-contracts capability flags.
type Options struct {
	// ProposalOnlyToolProviders names providers that are known to return tool
	// proposals without executing tools internally. Do not list the stock
	// go-providers PTY/Subprocess CLI bridges: those CLIs manage tools inside the
	// provider process and cannot uphold the workflow exact-allowlist boundary.
	// New binds each assertion to the exact registered provider instance and
	// later registry replacement revokes the assertion.
	ProposalOnlyToolProviders []string
}

// New creates a Hadron-owned go-llm-contracts bridge. Providers are selected
// by ProviderBinding.Provider; profiles and credentials remain host concerns.
func New(registry *providers.Registry, provided ...Options) (*Bridge, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: provider registry is required", ErrInvalidBridge)
	}
	if len(provided) > 1 {
		return nil, fmt.Errorf("%w: at most one options value is accepted", ErrInvalidBridge)
	}
	bridge := &Bridge{registry: registry, proposalOnlyProviders: make(map[string]llmcontracts.Provider)}
	if len(provided) == 0 {
		return bridge, nil
	}
	for _, name := range provided[0].ProposalOnlyToolProviders {
		if !stableIdentifier(name) {
			return nil, fmt.Errorf("%w: invalid proposal-only provider name", ErrInvalidBridge)
		}
		if _, duplicate := bridge.proposalOnlyProviders[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate proposal-only provider %q", ErrInvalidBridge, name)
		}
		upstream, found := registry.Get(name)
		if !found || nilInterface(upstream) || reflect.ValueOf(upstream).Kind() != reflect.Pointer {
			return nil, fmt.Errorf("%w: proposal-only provider %q must be a registered identity-bearing pointer", ErrInvalidBridge, name)
		}
		bridge.proposalOnlyProviders[name] = upstream
	}
	return bridge, nil
}

// Complete converts one neutral call to the adopted go-llm-contracts stream
// protocol. StreamChat is used for both workflow delivery modes because it is
// the only upstream API that can carry tool use, usage, and stop facts in one
// turn. A nil receiver suppresses live delivery without changing provider-side
// transport or tool semantics.
func (b *Bridge) Complete(ctx context.Context, request workflowllm.ProviderRequest, receiver workflowllm.StreamReceiver) (workflowllm.ProviderResponse, error) {
	if b == nil || b.registry == nil || ctx == nil {
		return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: bridge, registry, and context are required", ErrInvalidBridge))
	}
	if err := ctx.Err(); err != nil {
		return workflowllm.ProviderResponse{}, err
	}
	if !stableIdentifier(request.Binding.Provider) || !stableIdentifier(request.Binding.Model) {
		return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: provider and model bindings are required", ErrInvalidBridge))
	}
	if request.Budget.MaxCostMicrounits > 0 {
		return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: go-llm-contracts does not report exact cost", ErrUnsupported))
	}
	upstream, found := b.registry.Get(request.Binding.Provider)
	if !found || nilInterface(upstream) {
		return workflowllm.ProviderResponse{}, &workflowllm.ProviderError{Kind: workflowllm.ProviderUnavailable, Retryable: false, Cause: fmt.Errorf("%w: provider is not registered", ErrInvalidBridge)}
	}

	trustedProvider, asserted := b.proposalOnlyProviders[request.Binding.Provider]
	proposalOnly := asserted && sameProvider(trustedProvider, upstream)
	chat, err := convertRequest(request, upstream.Capabilities(), proposalOnly)
	if err != nil {
		return workflowllm.ProviderResponse{}, rejected(err)
	}
	stream, err := upstream.StreamChat(ctx, chat)
	if err != nil {
		return workflowllm.ProviderResponse{}, classifyUpstream(ctx, err)
	}
	if stream == nil {
		return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: provider returned a nil event stream", ErrUpstreamResult))
	}
	if nilInterface(receiver) {
		receiver = nil
	}
	return consumeStream(ctx, stream, receiver, request.Budget.MaxOutputBytes, request.Budget.MaxToolCalls, toolNames(request.Tools))
}

func convertRequest(request workflowllm.ProviderRequest, capabilities llmtypes.ProviderCapabilities, proposalOnly bool) (llmtypes.ChatRequest, error) {
	if request.Budget.MaxInputBytes < 1 || request.Budget.MaxOutputBytes < 1 || request.Budget.MaxTotalTokens < 1 || request.Budget.MaxToolCalls < 0 {
		return llmtypes.ChatRequest{}, fmt.Errorf("%w: invalid remaining budget", ErrInvalidBridge)
	}
	if capabilities.MaxTokens < 0 {
		return llmtypes.ChatRequest{}, fmt.Errorf("%w: provider reported a negative token capability", ErrUpstreamResult)
	}
	if err := values.ValidateSchema(request.OutputSchema); err != nil {
		return llmtypes.ChatRequest{}, fmt.Errorf("%w: output schema: %w", ErrInvalidBridge, err)
	}
	if len(request.Tools) != 0 && !proposalOnly {
		return llmtypes.ChatRequest{}, fmt.Errorf("%w: provider is not trusted as proposal-only for tools", ErrUnsupported)
	}
	if len(request.Tools) != 0 && (!capabilities.SupportsToolCalling || !capabilities.SupportsStreamJSON) {
		return llmtypes.ChatRequest{}, fmt.Errorf("%w: provider cannot expose structured tool-use events", ErrUnsupported)
	}

	chat := llmtypes.ChatRequest{Model: request.Binding.Model}
	seenNonSystem := false
	for index, message := range request.Messages {
		switch message.Role {
		case "system":
			if seenNonSystem || chat.SystemPrompt != "" || message.Content == "" || hasToolFields(message) {
				return llmtypes.ChatRequest{}, fmt.Errorf("%w: messages[%d] is not one leading system message", ErrInvalidBridge, index)
			}
			chat.SystemPrompt = message.Content
		case "user", "assistant", "tool":
			seenNonSystem = true
			converted, err := convertMessage(message)
			if err != nil {
				return llmtypes.ChatRequest{}, fmt.Errorf("%w: messages[%d]: %w", ErrInvalidBridge, index, err)
			}
			chat.Messages = append(chat.Messages, converted)
		default:
			return llmtypes.ChatRequest{}, fmt.Errorf("%w: messages[%d] has unsupported role", ErrInvalidBridge, index)
		}
	}
	if len(chat.Messages) == 0 {
		return llmtypes.ChatRequest{}, fmt.Errorf("%w: at least one non-system message is required", ErrInvalidBridge)
	}

	contextJSON, err := canonicalContext(request.Context)
	if err != nil {
		return llmtypes.ChatRequest{}, err
	}
	if contextJSON != "" {
		chat.SlotBlocks = append(chat.SlotBlocks, llmtypes.SlotBlock{Name: "workflow_context", Content: "Workflow typed context (JSON value envelopes):\n" + contextJSON})
	}
	schemaJSON, err := canonicalJSON(request.OutputSchema)
	if err != nil {
		return llmtypes.ChatRequest{}, fmt.Errorf("%w: output schema cannot be encoded", ErrInvalidBridge)
	}
	schemaInstruction := "Return only JSON that satisfies this JSON Schema:\n" + schemaJSON
	if request.Repair {
		schemaInstruction = "Repair the prior invalid output. " + schemaInstruction
	}
	chat.SlotBlocks = append(chat.SlotBlocks, llmtypes.SlotBlock{Name: "workflow_output_schema", Content: schemaInstruction})

	seenTools := make(map[string]struct{}, len(request.Tools))
	strict := true
	for index, definition := range request.Tools {
		if !stableIdentifier(definition.Name) {
			return llmtypes.ChatRequest{}, fmt.Errorf("%w: tools[%d] has invalid name", ErrInvalidBridge, index)
		}
		if _, duplicate := seenTools[definition.Name]; duplicate {
			return llmtypes.ChatRequest{}, fmt.Errorf("%w: duplicate tool %q", ErrInvalidBridge, definition.Name)
		}
		seenTools[definition.Name] = struct{}{}
		if !utf8.ValidString(definition.Description) || values.ValidateSchema(definition.InputSchema) != nil {
			return llmtypes.ChatRequest{}, fmt.Errorf("%w: tools[%d] has invalid metadata", ErrInvalidBridge, index)
		}
		schema, err := cloneObject(definition.InputSchema)
		if err != nil {
			return llmtypes.ChatRequest{}, fmt.Errorf("%w: tools[%d] schema cannot be cloned", ErrInvalidBridge, index)
		}
		chat.Tools = append(chat.Tools, llmtypes.ToolDefinition{Name: definition.Name, Description: definition.Description, InputSchema: schema, Strict: &strict})
	}
	chat.MaxTokens = maximumTokens(request.Budget.MaxTotalTokens, capabilities.MaxTokens)
	return chat, nil
}

func convertMessage(message workflowllm.Message) (llmtypes.ChatMessage, error) {
	switch message.Role {
	case "user":
		if hasToolFields(message) {
			return llmtypes.ChatMessage{}, errors.New("user message must contain only text")
		}
		return llmtypes.ChatMessage{Role: "user", Content: message.Content}, nil
	case "assistant":
		if message.ToolRequest == nil {
			if message.Tool != "" || message.ToolCallID != "" || message.ToolResult != nil {
				return llmtypes.ChatMessage{}, errors.New("assistant message must contain text or one tool request")
			}
			return llmtypes.ChatMessage{Role: "assistant", Content: message.Content}, nil
		}
		request := message.ToolRequest
		if message.Content != "" || message.ToolResult != nil || !stableIdentifier(request.ID) || !stableIdentifier(request.Name) || request.Arguments == nil || message.ToolCallID != request.ID || message.Tool != request.Name {
			return llmtypes.ChatMessage{}, errors.New("assistant tool request is inconsistent")
		}
		arguments, err := cloneObject(request.Arguments)
		if err != nil {
			return llmtypes.ChatMessage{}, errors.New("assistant tool arguments are invalid")
		}
		return llmtypes.ChatMessage{Role: "assistant", ContentBlocks: []llmtypes.ContentBlock{{Type: "tool_use", ID: request.ID, Name: request.Name, Input: &arguments}}}, nil
	case "tool":
		if message.Content != "" || message.ToolRequest != nil || !stableIdentifier(message.ToolCallID) || !stableIdentifier(message.Tool) {
			return llmtypes.ChatMessage{}, errors.New("tool result identity is invalid")
		}
		content, err := canonicalJSON(message.ToolResult)
		if err != nil {
			return llmtypes.ChatMessage{}, errors.New("tool result is not exact JSON")
		}
		return llmtypes.ChatMessage{Role: "user", ContentBlocks: []llmtypes.ContentBlock{{Type: "tool_result", Name: message.Tool, ToolUseID: message.ToolCallID, Content: content}}}, nil
	default:
		return llmtypes.ChatMessage{}, errors.New("unsupported role")
	}
}

func consumeStream(ctx context.Context, stream <-chan llmtypes.StreamEvent, receiver workflowllm.StreamReceiver, maximumOutputBytes int64, maximumToolCalls int, allowedTools map[string]struct{}) (workflowllm.ProviderResponse, error) {
	var text strings.Builder
	var usage *llmtypes.Usage
	var toolRequests []workflowllm.ToolRequest
	seenToolIDs := make(map[string]struct{})
	var outputBytes int64
	seenSession := false
	done := false
	for {
		select {
		case <-ctx.Done():
			return workflowllm.ProviderResponse{}, ctx.Err()
		case event, open := <-stream:
			if !open {
				if !done || usage == nil {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: stream closed without one done and usage event", ErrUpstreamResult))
				}
				return buildResponse(text.String(), toolRequests, *usage, receiver != nil)
			}
			if done {
				return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: event followed terminal done", ErrUpstreamResult))
			}
			switch event.Type {
			case llmtypes.EventDelta:
				if event.ToolUse != nil || event.Usage != nil || event.Error != "" || event.Content == "" || !utf8.ValidString(event.Content) || len(toolRequests) != 0 {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: malformed or mixed text delta", ErrUpstreamResult))
				}
				if err := consumeOutputBytes(&outputBytes, int64(len(event.Content)), maximumOutputBytes); err != nil {
					return workflowllm.ProviderResponse{}, rejected(err)
				}
				text.WriteString(event.Content)
				if receiver != nil {
					if err := receiver.Receive(ctx, workflowllm.StreamChunk{Text: event.Content}); err != nil {
						return workflowllm.ProviderResponse{}, &workflowllm.ProviderError{Kind: workflowllm.ProviderInfrastructure, Retryable: true, Cause: err}
					}
				}
			case llmtypes.EventToolUse:
				if text.Len() != 0 || event.ToolUse == nil || event.Usage != nil || event.Error != "" || event.Content != "" {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: malformed or mixed tool-use event", ErrUpstreamResult))
				}
				if len(toolRequests) >= maximumToolCalls {
					return workflowllm.ProviderResponse{}, rejected(workflowllm.ErrBudgetExceeded)
				}
				encoded, err := json.Marshal(event.ToolUse)
				if err != nil {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: tool use cannot be encoded", ErrUpstreamResult))
				}
				if limitErr := consumeOutputBytes(&outputBytes, int64(len(encoded)), maximumOutputBytes); limitErr != nil {
					return workflowllm.ProviderResponse{}, rejected(limitErr)
				}
				tool, err := convertToolUse(*event.ToolUse)
				if err != nil {
					return workflowllm.ProviderResponse{}, rejected(err)
				}
				if _, allowed := allowedTools[tool.Name]; !allowed {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: provider proposed an unrequested tool", ErrUpstreamResult))
				}
				if _, duplicate := seenToolIDs[tool.ID]; duplicate {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: provider reused a tool identity", ErrUpstreamResult))
				}
				seenToolIDs[tool.ID] = struct{}{}
				toolRequests = append(toolRequests, tool)
			case llmtypes.EventUsage:
				if usage != nil || event.Usage == nil || event.ToolUse != nil || event.Error != "" || event.Content != "" {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: malformed or duplicate usage event", ErrUpstreamResult))
				}
				usageCopy := *event.Usage
				usage = &usageCopy
			case llmtypes.EventDone:
				if event.ToolUse != nil || event.Usage != nil || event.Error != "" || event.Content != "" {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: malformed done event", ErrUpstreamResult))
				}
				done = true
			case llmtypes.EventError:
				if err := ctx.Err(); err != nil {
					return workflowllm.ProviderResponse{}, err
				}
				return workflowllm.ProviderResponse{}, &workflowllm.ProviderError{Kind: workflowllm.ProviderInfrastructure, Retryable: true, Cause: ErrUpstreamResult}
			case llmtypes.EventThinking:
				if event.ThinkingBlock == nil || (event.ThinkingBlock.Thinking == "" && event.ThinkingBlock.Signature == "") || !utf8.ValidString(event.ThinkingBlock.Thinking) || !utf8.ValidString(event.ThinkingBlock.Signature) {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: malformed thinking event", ErrUpstreamResult))
				}
				thinkingBytes, ok := checkedAddInt(len(event.ThinkingBlock.Thinking), len(event.ThinkingBlock.Signature))
				if !ok {
					return workflowllm.ProviderResponse{}, rejected(workflowllm.ErrBudgetExceeded)
				}
				if err := consumeOutputBytes(&outputBytes, int64(thinkingBytes), maximumOutputBytes); err != nil {
					return workflowllm.ProviderResponse{}, rejected(err)
				}
				// Thinking content and signatures are intentionally discarded. They
				// are neither delivered nor written to audit metadata.
			case llmtypes.EventSessionID:
				if seenSession || !stableIdentifier(event.SessionID) || len(event.SessionID) > 4096 {
					return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: malformed provider session identity", ErrUpstreamResult))
				}
				seenSession = true
				// Validate then discard resumable provider session IDs. Neither the
				// raw ID nor a reusable derivative enters durable workflow audit.
			default:
				return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: unknown event type %q", ErrUpstreamResult, event.Type))
			}
		}
	}
}

func buildResponse(raw string, tools []workflowllm.ToolRequest, usage llmtypes.Usage, delivered bool) (workflowllm.ProviderResponse, error) {
	mappedUsage, err := convertUsage(usage)
	if err != nil {
		return workflowllm.ProviderResponse{}, rejected(err)
	}
	stop, err := convertStopReason(usage.StopReason, len(tools) != 0)
	if err != nil {
		return workflowllm.ProviderResponse{}, rejected(err)
	}
	if (len(tools) != 0) != (stop == workflowllm.StopTool) || (len(tools) != 0 && raw != "") {
		return workflowllm.ProviderResponse{}, rejected(fmt.Errorf("%w: stop reason and response content disagree", ErrUpstreamResult))
	}
	delivery := "suppressed"
	if delivered {
		delivery = "streamed"
	}
	return workflowllm.ProviderResponse{
		RawText:      raw,
		ToolRequests: cloneToolRequests(tools),
		Usage:        mappedUsage,
		StopReason:   stop,
		Audit: workflowllm.ProviderAudit{Attributes: map[string]string{
			"bridge":    "go-llm-contracts",
			"delivery":  delivery,
			"transport": "stream_chat",
		}},
	}, nil
}

func convertToolUse(block llmtypes.ToolUseBlock) (workflowllm.ToolRequest, error) {
	if !stableIdentifier(block.ID) || !stableIdentifier(block.Name) || block.Input == nil {
		return workflowllm.ToolRequest{}, fmt.Errorf("%w: malformed tool use", ErrUpstreamResult)
	}
	arguments, err := cloneObject(block.Input)
	if err != nil {
		return workflowllm.ToolRequest{}, fmt.Errorf("%w: tool arguments are not exact JSON", ErrUpstreamResult)
	}
	return workflowllm.ToolRequest{ID: block.ID, Name: block.Name, Arguments: arguments}, nil
}

func convertUsage(usage llmtypes.Usage) (workflowllm.Usage, error) {
	for _, value := range []int{usage.InputTokens, usage.OutputTokens, usage.CacheCreationTokens, usage.CacheReadTokens} {
		if value < 0 {
			return workflowllm.Usage{}, fmt.Errorf("%w: negative token usage", ErrUpstreamResult)
		}
	}
	input, ok := checkedAddInt(usage.InputTokens, usage.CacheCreationTokens)
	if !ok {
		return workflowllm.Usage{}, fmt.Errorf("%w: token usage overflow", ErrUpstreamResult)
	}
	input, ok = checkedAddInt(input, usage.CacheReadTokens)
	if !ok {
		return workflowllm.Usage{}, fmt.Errorf("%w: token usage overflow", ErrUpstreamResult)
	}
	total, ok := checkedAddInt(input, usage.OutputTokens)
	if !ok {
		return workflowllm.Usage{}, fmt.Errorf("%w: token usage overflow", ErrUpstreamResult)
	}
	return workflowllm.Usage{InputTokens: int64(input), OutputTokens: int64(usage.OutputTokens), TotalTokens: int64(total)}, nil
}

func convertStopReason(input string, hasTools bool) (workflowllm.StopReason, error) {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "":
		if hasTools {
			return workflowllm.StopTool, nil
		}
		return workflowllm.StopCompleted, nil
	case "end_turn", "stop", "stop_sequence", "completed", "done":
		return workflowllm.StopCompleted, nil
	case "max_tokens", "length":
		return workflowllm.StopLength, nil
	case "tool", "tool_use", "tool_calls":
		return workflowllm.StopTool, nil
	case "content_filter", "filtered", "safety":
		return workflowllm.StopFiltered, nil
	default:
		return "", fmt.Errorf("%w: unknown stop reason", ErrUnsupported)
	}
}

func toolNames(input []workflowllm.ToolDefinition) map[string]struct{} {
	result := make(map[string]struct{}, len(input))
	for _, definition := range input {
		result[definition.Name] = struct{}{}
	}
	return result
}

func consumeOutputBytes(current *int64, addition, maximum int64) error {
	if current == nil || addition < 0 || maximum < 1 || *current < 0 || addition > math.MaxInt64-*current || *current+addition > maximum {
		return workflowllm.ErrBudgetExceeded
	}
	*current += addition
	return nil
}

func canonicalContext(contextValues values.ValueSet) (string, error) {
	if len(contextValues) == 0 {
		return "", nil
	}
	keys := make([]string, 0, len(contextValues))
	for name := range contextValues {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, name := range keys {
		value := contextValues[name]
		if value.Type == values.TypeSecretRef || value.Redaction == values.RedactionSecret {
			return "", fmt.Errorf("%w: context value %q is secret", ErrInvalidBridge, name)
		}
	}
	encoded, err := json.Marshal(contextValues)
	if err != nil {
		return "", fmt.Errorf("%w: context values cannot be encoded", ErrInvalidBridge)
	}
	return string(encoded), nil
}

func canonicalJSON(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	var normalized any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if decodeErr := decoder.Decode(&normalized); decodeErr != nil {
		return "", decodeErr
	}
	encoded, err = json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func cloneObject[M ~map[string]any](input M) (map[string]any, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, firstError(err, errors.New("object is nil"))
	}
	return result, nil
}

func cloneToolRequests(input []workflowllm.ToolRequest) []workflowllm.ToolRequest {
	if input == nil {
		return nil
	}
	result := make([]workflowllm.ToolRequest, len(input))
	for index := range input {
		result[index] = input[index]
		result[index].Arguments, _ = cloneObject(input[index].Arguments)
	}
	return result
}

func maximumTokens(remaining int64, providerMaximum int) int {
	if remaining > int64(math.MaxInt) {
		remaining = int64(math.MaxInt)
	}
	result := int(remaining)
	if providerMaximum > 0 && providerMaximum < result {
		result = providerMaximum
	}
	return result
}

func checkedAddInt(left, right int) (int, bool) {
	if left < 0 || right < 0 || right > math.MaxInt-left {
		return 0, false
	}
	return left + right, true
}

func stableIdentifier(value string) bool {
	return value != "" && utf8.ValidString(value) && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\t")
}

func hasToolFields(message workflowllm.Message) bool {
	return message.ToolCallID != "" || message.Tool != "" || message.ToolRequest != nil || message.ToolResult != nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func sameProvider(left, right llmcontracts.Provider) bool {
	if nilInterface(left) || nilInterface(right) || reflect.TypeOf(left) != reflect.TypeOf(right) {
		return false
	}
	leftValue, rightValue := reflect.ValueOf(left), reflect.ValueOf(right)
	return leftValue.Kind() == reflect.Pointer && rightValue.Kind() == reflect.Pointer && leftValue.Pointer() == rightValue.Pointer()
}

func rejected(cause error) error {
	return &workflowllm.ProviderError{Kind: workflowllm.ProviderRejected, Retryable: false, Cause: cause}
}

func classifyUpstream(ctx context.Context, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if errors.Is(cause, llmcontracts.ErrRequestExceedsRateBudget) {
		return &workflowllm.ProviderError{Kind: workflowllm.ProviderRejected, Retryable: false, Cause: cause}
	}
	return &workflowllm.ProviderError{Kind: workflowllm.ProviderInfrastructure, Retryable: true, Cause: cause}
}

func firstError(primary, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}

var _ workflowllm.Provider = (*Bridge)(nil)
