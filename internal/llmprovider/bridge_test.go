package llmprovider_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	llmcontracts "github.com/hollis-labs/go-llm-contracts"
	llmtypes "github.com/hollis-labs/go-llm-types"
	providers "github.com/hollis-labs/go-providers/provider"

	"github.com/hollis-labs/hadron/internal/llmprovider"
	workflowllm "github.com/hollis-labs/hadron/workflow/adapters/llm"
	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/values"
)

type fakeProvider struct {
	capabilities llmtypes.ProviderCapabilities
	stream       func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error)
	complete     atomic.Int64
}

func (f *fakeProvider) StreamChat(ctx context.Context, request llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
	return f.stream(ctx, request)
}

func (f *fakeProvider) Complete(context.Context, llmtypes.ChatRequest) (string, error) {
	f.complete.Add(1)
	return "", errors.New("Complete must not be used")
}

func (f *fakeProvider) Capabilities() llmtypes.ProviderCapabilities { return f.capabilities }

type equalValueProvider struct{ marker int }

func (equalValueProvider) StreamChat(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
	return events(
		llmtypes.StreamEvent{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "call", Name: "read", Input: map[string]any{}}},
		llmtypes.StreamEvent{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{InputTokens: 1, OutputTokens: 1, StopReason: "tool_use"}},
		llmtypes.StreamEvent{Type: llmtypes.EventDone},
	), nil
}

func (equalValueProvider) Complete(context.Context, llmtypes.ChatRequest) (string, error) {
	return "", errors.New("Complete must not be used")
}

func (equalValueProvider) Capabilities() llmtypes.ProviderCapabilities {
	return llmtypes.ProviderCapabilities{SupportsToolCalling: true, SupportsStreamJSON: true}
}

type receiver struct {
	mu     sync.Mutex
	chunks []string
}

func (r *receiver) Receive(_ context.Context, chunk workflowllm.StreamChunk) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.chunks = append(r.chunks, chunk.Text)
	return nil
}

func TestBridgeConvertsSystemMessagesContextSchemaTextUsageAndStop(t *testing.T) {
	var captured llmtypes.ChatRequest
	upstream := &fakeProvider{
		capabilities: llmtypes.ProviderCapabilities{MaxTokens: 512},
		stream: func(_ context.Context, request llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
			captured = request
			return events(
				llmtypes.StreamEvent{Type: llmtypes.EventDelta, Content: "hello"},
				llmtypes.StreamEvent{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{InputTokens: 7, OutputTokens: 2, CacheCreationTokens: 3, CacheReadTokens: 5, StopReason: "end_turn"}},
				llmtypes.StreamEvent{Type: llmtypes.EventDone},
			), nil
		},
	}
	bridge := newBridge(t, "direct", upstream)
	contextValue := mustInline(t, map[string]any{"exact": json.Number("9007199254740993")})
	request := validRequest()
	request.Messages = []workflowllm.Message{{Role: "system", Content: "system exact"}, {Role: "user", Content: "question"}, {Role: "assistant", Content: "prior"}}
	request.Context = values.ValueSet{"record": contextValue}
	request.Budget.MaxTotalTokens = 900

	response, err := bridge.Complete(t.Context(), request, nil)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if response.RawText != "hello" || response.StopReason != workflowllm.StopCompleted {
		t.Fatalf("response = %#v", response)
	}
	if response.Usage.InputTokens != 15 || response.Usage.OutputTokens != 2 || response.Usage.TotalTokens != 17 || response.Usage.CostMicrounits != 0 {
		t.Fatalf("usage = %#v", response.Usage)
	}
	if response.Audit.Attributes["bridge"] != "go-llm-contracts" || response.Audit.Attributes["delivery"] != "suppressed" || response.Audit.Attributes["transport"] != "stream_chat" {
		t.Fatalf("audit = %#v", response.Audit)
	}
	if captured.Model != "model-v1" || captured.SystemPrompt != "system exact" || captured.MaxTokens != 512 {
		t.Fatalf("captured request identity = %#v", captured)
	}
	if got := captured.Messages; !reflect.DeepEqual(got, []llmtypes.ChatMessage{{Role: "user", Content: "question"}, {Role: "assistant", Content: "prior"}}) {
		t.Fatalf("messages = %#v", got)
	}
	if len(captured.SlotBlocks) != 2 || captured.SlotBlocks[0].Name != "workflow_context" || captured.SlotBlocks[1].Name != "workflow_output_schema" {
		t.Fatalf("slots = %#v", captured.SlotBlocks)
	}
	contextJSON, marshalErr := json.Marshal(request.Context)
	if marshalErr != nil || !strings.HasSuffix(captured.SlotBlocks[0].Content, string(contextJSON)) {
		t.Fatalf("context slot = %q, marshal err = %v", captured.SlotBlocks[0].Content, marshalErr)
	}
	if !strings.HasSuffix(captured.SlotBlocks[1].Content, `{"additionalProperties":false,"properties":{"answer":{"type":"string"}},"required":["answer"],"type":"object"}`) {
		t.Fatalf("schema slot = %q", captured.SlotBlocks[1].Content)
	}
	if upstream.complete.Load() != 0 {
		t.Fatal("bridge used non-streaming Complete instead of the event-bearing stream contract")
	}
}

func TestBridgePreservesExactStructuredJSONAndStreamingDelivery(t *testing.T) {
	sink := &receiver{}
	upstream := &fakeProvider{stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
		return events(
			llmtypes.StreamEvent{Type: llmtypes.EventDelta, Content: `{"count":`},
			llmtypes.StreamEvent{Type: llmtypes.EventDelta, Content: `9007199254740993}`},
			llmtypes.StreamEvent{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{InputTokens: 2, OutputTokens: 4, StopReason: "stop"}},
			llmtypes.StreamEvent{Type: llmtypes.EventDone},
		), nil
	}}
	response, err := newBridge(t, "direct", upstream).Complete(t.Context(), validRequest(), sink)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if response.RawText != `{"count":9007199254740993}` || response.Audit.Attributes["delivery"] != "streamed" {
		t.Fatalf("response = %#v", response)
	}
	if !reflect.DeepEqual(sink.chunks, []string{`{"count":`, `9007199254740993}`}) {
		t.Fatalf("chunks = %#v", sink.chunks)
	}
}

func TestBridgeConvertsToolDefinitionsHistoryAndToolUseInEitherDeliveryMode(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("receiver-%t", streaming), func(t *testing.T) {
			var captured llmtypes.ChatRequest
			arguments := map[string]any{"count": json.Number("9007199254740993")}
			upstream := &fakeProvider{
				capabilities: llmtypes.ProviderCapabilities{SupportsToolCalling: true, SupportsStreamJSON: true},
				stream: func(_ context.Context, request llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
					captured = request
					return events(
						llmtypes.StreamEvent{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "call-2", Name: "lookup.read", Input: arguments}},
						llmtypes.StreamEvent{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{InputTokens: 10, OutputTokens: 1, StopReason: "tool_use"}},
						llmtypes.StreamEvent{Type: llmtypes.EventDone},
					), nil
				},
			}
			request := validRequest()
			request.Messages = []workflowllm.Message{
				{Role: "user", Content: "begin"},
				{Role: "assistant", Tool: "lookup.read", ToolCallID: "call-1", ToolRequest: &workflowllm.ToolRequest{ID: "call-1", Name: "lookup.read", Arguments: map[string]any{"id": json.Number("7")}}},
				{Role: "tool", Tool: "lookup.read", ToolCallID: "call-1", ToolResult: map[string]any{"ok": true, "id": json.Number("7")}},
			}
			request.Tools = []workflowllm.ToolDefinition{{Name: "lookup.read", Description: "Read one record", InputSchema: graph.Schema{"type": "object", "additionalProperties": false, "required": []any{"count"}, "properties": map[string]any{"count": map[string]any{"type": "integer"}}}}}
			var sink workflowllm.StreamReceiver
			if streaming {
				sink = &receiver{}
			}
			response, err := newToolBridge(t, "direct", upstream).Complete(t.Context(), request, sink)
			if err != nil {
				t.Fatalf("Complete() error = %v", err)
			}
			if response.StopReason != workflowllm.StopTool || len(response.ToolRequests) != 1 || response.ToolRequests[0].ID != "call-2" || response.ToolRequests[0].Name != "lookup.read" || response.ToolRequests[0].Arguments["count"] != json.Number("9007199254740993") {
				t.Fatalf("tool response = %#v", response)
			}
			if len(captured.Tools) != 1 || captured.Tools[0].Strict == nil || !*captured.Tools[0].Strict || captured.Tools[0].InputSchema["type"] != "object" {
				t.Fatalf("tool definition = %#v", captured.Tools)
			}
			if len(captured.Messages) != 3 || len(captured.Messages[1].ContentBlocks) != 1 || len(captured.Messages[2].ContentBlocks) != 1 {
				t.Fatalf("tool history = %#v", captured.Messages)
			}
			toolUse := captured.Messages[1].ContentBlocks[0]
			if toolUse.Type != "tool_use" || toolUse.ID != "call-1" || toolUse.Name != "lookup.read" || toolUse.Input == nil || (*toolUse.Input)["id"] != json.Number("7") {
				t.Fatalf("tool-use history = %#v", toolUse)
			}
			toolResult := captured.Messages[2].ContentBlocks[0]
			if captured.Messages[2].Role != "user" || toolResult.Type != "tool_result" || toolResult.Name != "lookup.read" || toolResult.ToolUseID != "call-1" || toolResult.Content != `{"id":7,"ok":true}` {
				t.Fatalf("tool-result history = %#v", captured.Messages[2])
			}
		})
	}
}

func TestBridgeCancellationAndErrorsAreClassifiedWithoutDurableRawText(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		upstream := &fakeProvider{stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
			cancel()
			return make(chan llmtypes.StreamEvent), nil
		}}
		_, err := newBridge(t, "direct", upstream).Complete(ctx, validRequest(), nil)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Complete() error = %v", err)
		}
	})

	t.Run("upstream infrastructure", func(t *testing.T) {
		raw := errors.New("Authorization: Bearer top-secret")
		upstream := &fakeProvider{stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) { return nil, raw }}
		_, err := newBridge(t, "direct", upstream).Complete(t.Context(), validRequest(), nil)
		assertProviderError(t, err, workflowllm.ProviderInfrastructure, true)
		if !errors.Is(err, raw) || strings.Contains(err.Error(), "top-secret") {
			t.Fatalf("raw cause contract = %v", err)
		}
	})

	t.Run("provider-local deadline is infrastructure while caller remains live", func(t *testing.T) {
		upstream := &fakeProvider{stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
			return nil, context.DeadlineExceeded
		}}
		_, err := newBridge(t, "direct", upstream).Complete(t.Context(), validRequest(), nil)
		assertProviderError(t, err, workflowllm.ProviderInfrastructure, true)
	})

	t.Run("rate budget rejection", func(t *testing.T) {
		upstream := &fakeProvider{stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
			return nil, llmcontracts.ErrRequestExceedsRateBudget
		}}
		_, err := newBridge(t, "direct", upstream).Complete(t.Context(), validRequest(), nil)
		assertProviderError(t, err, workflowllm.ProviderRejected, false)
	})

	t.Run("stream error content discarded", func(t *testing.T) {
		upstream := &fakeProvider{stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
			return events(llmtypes.StreamEvent{Type: llmtypes.EventError, Error: "token=top-secret"}), nil
		}}
		_, err := newBridge(t, "direct", upstream).Complete(t.Context(), validRequest(), nil)
		assertProviderError(t, err, workflowllm.ProviderInfrastructure, true)
		if strings.Contains(fmt.Sprintf("%+v", err), "top-secret") {
			t.Fatalf("stream error leaked raw content: %v", err)
		}
	})
}

func TestBridgeMapsEverySupportedStopReasonDeterministically(t *testing.T) {
	tests := []struct {
		upstream string
		want     workflowllm.StopReason
	}{
		{"end_turn", workflowllm.StopCompleted},
		{"stop_sequence", workflowllm.StopCompleted},
		{"max_tokens", workflowllm.StopLength},
		{"content_filter", workflowllm.StopFiltered},
		{"", workflowllm.StopCompleted},
	}
	for _, test := range tests {
		t.Run(test.upstream+"-maps-to-"+string(test.want), func(t *testing.T) {
			upstream := &fakeProvider{stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
				return events(
					llmtypes.StreamEvent{Type: llmtypes.EventDelta, Content: `"ok"`},
					llmtypes.StreamEvent{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{InputTokens: 1, OutputTokens: 1, StopReason: test.upstream}},
					llmtypes.StreamEvent{Type: llmtypes.EventDone},
				), nil
			}}
			response, err := newBridge(t, "direct", upstream).Complete(t.Context(), validRequest(), nil)
			if err != nil || response.StopReason != test.want {
				t.Fatalf("Complete() = %#v, %v", response, err)
			}
		})
	}
}

func TestBridgeFailsClosedForUnsupportedOrAmbiguousUpstreamContracts(t *testing.T) {
	tests := []struct {
		name    string
		request func() workflowllm.ProviderRequest
		caps    llmtypes.ProviderCapabilities
		events  []llmtypes.StreamEvent
	}{
		{
			name: "cost budget",
			request: func() workflowllm.ProviderRequest {
				request := validRequest()
				request.Budget.MaxCostMicrounits = 1
				return request
			},
		},
		{
			name: "tool boundary default denial despite advertised capabilities",
			request: func() workflowllm.ProviderRequest {
				request := validRequest()
				request.Tools = []workflowllm.ToolDefinition{{Name: "read", InputSchema: graph.Schema{"type": "object"}}}
				return request
			},
			caps: llmtypes.ProviderCapabilities{SupportsToolCalling: true, SupportsStreamJSON: true},
		},
		{
			name:    "missing usage",
			request: validRequest,
			events:  []llmtypes.StreamEvent{{Type: llmtypes.EventDelta, Content: `"ok"`}, {Type: llmtypes.EventDone}},
		},
		{
			name:    "mixed text and tool",
			request: validRequest,
			caps:    llmtypes.ProviderCapabilities{SupportsToolCalling: true, SupportsStreamJSON: true},
			events: []llmtypes.StreamEvent{
				{Type: llmtypes.EventDelta, Content: "preface"},
				{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "id", Name: "read", Input: map[string]any{}}},
			},
		},
		{
			name:    "unknown stop",
			request: validRequest,
			events: []llmtypes.StreamEvent{
				{Type: llmtypes.EventDelta, Content: `"ok"`},
				{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{StopReason: "future_reason"}},
				{Type: llmtypes.EventDone},
			},
		},
		{
			name:    "malformed thinking event",
			request: validRequest,
			events:  []llmtypes.StreamEvent{{Type: llmtypes.EventThinking}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			upstream := &fakeProvider{capabilities: test.caps, stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
				calls.Add(1)
				return events(test.events...), nil
			}}
			_, err := newBridge(t, "direct", upstream).Complete(t.Context(), test.request(), nil)
			assertProviderError(t, err, workflowllm.ProviderRejected, false)
			if test.name == "cost budget" || strings.HasPrefix(test.name, "tool boundary") {
				if calls.Load() != 0 {
					t.Fatalf("upstream called %d times", calls.Load())
				}
			}
		})
	}
}

func TestBridgeConsumesInstalledClaudeAndCodexInformationalSequencesSafely(t *testing.T) {
	upstream := &fakeProvider{stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
		return events(
			llmtypes.StreamEvent{Type: llmtypes.EventSessionID, SessionID: "resume-secret-session-id"},
			llmtypes.StreamEvent{Type: llmtypes.EventThinking, ThinkingBlock: &llmtypes.ThinkingBlock{Thinking: "private chain", Signature: "signed-private-chain"}},
			llmtypes.StreamEvent{Type: llmtypes.EventDelta, Content: `{"answer":"ok"}`},
			llmtypes.StreamEvent{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{InputTokens: 4, OutputTokens: 3}},
			llmtypes.StreamEvent{Type: llmtypes.EventDone},
		), nil
	}}
	response, err := newBridge(t, "direct", upstream).Complete(t.Context(), validRequest(), nil)
	if err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if response.StopReason != workflowllm.StopCompleted || response.RawText != `{"answer":"ok"}` {
		t.Fatalf("response = %#v", response)
	}
	audit := fmt.Sprintf("%#v", response.Audit)
	for _, forbidden := range []string{"resume-secret-session-id", "private chain", "signed-private-chain"} {
		if strings.Contains(audit, forbidden) {
			t.Fatalf("audit leaked %q: %s", forbidden, audit)
		}
	}
}

func TestBridgeBoundsSuppressedAndToolOutputBeforeAccumulation(t *testing.T) {
	tests := []struct {
		name   string
		limit  int64
		tools  bool
		events []llmtypes.StreamEvent
	}{
		{name: "single suppressed delta", limit: 8, events: []llmtypes.StreamEvent{{Type: llmtypes.EventDelta, Content: "123456789"}}},
		{name: "cumulative suppressed deltas", limit: 8, events: []llmtypes.StreamEvent{{Type: llmtypes.EventDelta, Content: "12345"}, {Type: llmtypes.EventDelta, Content: "6789"}}},
		{name: "tool input", limit: 32, tools: true, events: []llmtypes.StreamEvent{{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "call", Name: "read", Input: map[string]any{"payload": strings.Repeat("x", 128)}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			caps := llmtypes.ProviderCapabilities{}
			if test.tools {
				caps = llmtypes.ProviderCapabilities{SupportsToolCalling: true, SupportsStreamJSON: true}
			}
			upstream := &fakeProvider{capabilities: caps, stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
				return events(test.events...), nil
			}}
			request := validRequest()
			request.Budget.MaxOutputBytes = test.limit
			bridge := newBridge(t, "direct", upstream)
			if test.tools {
				request.Tools = []workflowllm.ToolDefinition{{Name: "read", InputSchema: graph.Schema{"type": "object"}}}
				bridge = newToolBridge(t, "direct", upstream)
			}
			_, err := bridge.Complete(t.Context(), request, nil)
			if !errors.Is(err, workflowllm.ErrBudgetExceeded) {
				t.Fatalf("Complete() error = %v", err)
			}
		})
	}
}

func TestBridgePreflightsToolEventCountBeforeEncoding(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	for _, test := range []struct {
		name     string
		limit    int
		events   []llmtypes.StreamEvent
		want     int
		budgeted bool
	}{
		{name: "zero", limit: 0, events: []llmtypes.StreamEvent{{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "call-1", Name: "read", Input: cyclic}}}, budgeted: true},
		{name: "exact", limit: 2, events: []llmtypes.StreamEvent{
			{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "call-1", Name: "read", Input: map[string]any{}}},
			{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "call-2", Name: "read", Input: map[string]any{}}},
			{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{InputTokens: 1, OutputTokens: 1, StopReason: "tool_use"}},
			{Type: llmtypes.EventDone},
		}, want: 2},
		{name: "one over", limit: 1, events: []llmtypes.StreamEvent{
			{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "call-1", Name: "read", Input: map[string]any{}}},
			{Type: llmtypes.EventToolUse, ToolUse: &llmtypes.ToolUseBlock{ID: "call-2", Name: "read", Input: cyclic}},
		}, budgeted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := &fakeProvider{
				capabilities: llmtypes.ProviderCapabilities{SupportsToolCalling: true, SupportsStreamJSON: true},
				stream: func(context.Context, llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
					return events(test.events...), nil
				},
			}
			request := validRequest()
			request.Budget.MaxToolCalls = test.limit
			request.Tools = []workflowllm.ToolDefinition{{Name: "read", InputSchema: graph.Schema{"type": "object"}}}
			response, err := newToolBridge(t, "direct", upstream).Complete(t.Context(), request, nil)
			if test.budgeted {
				if !errors.Is(err, workflowllm.ErrBudgetExceeded) {
					t.Fatalf("Complete() error = %v", err)
				}
				return
			}
			if err != nil || len(response.ToolRequests) != test.want {
				t.Fatalf("Complete() response/error = %#v, %v", response, err)
			}
		})
	}
}

func TestProposalOnlyToolAssertionIsBoundToExactRegisteredProvider(t *testing.T) {
	t.Run("pointer replacement", func(t *testing.T) {
		capabilities := llmtypes.ProviderCapabilities{SupportsToolCalling: true, SupportsStreamJSON: true}
		trusted := &fakeProvider{capabilities: capabilities}
		replacement := &fakeProvider{capabilities: capabilities}
		assertProviderReplacementRejected(t, trusted, replacement)
	})

	t.Run("equal-value pointer replacement", func(t *testing.T) {
		trusted := &equalValueProvider{marker: 1}
		replacement := &equalValueProvider{marker: 1}
		assertProviderReplacementRejected(t, trusted, replacement)
	})

	t.Run("value provider", func(t *testing.T) {
		registry := providers.NewRegistry()
		registry.Register("direct", equalValueProvider{marker: 1})
		if _, err := llmprovider.New(registry, llmprovider.Options{ProposalOnlyToolProviders: []string{"direct"}}); !errors.Is(err, llmprovider.ErrInvalidBridge) {
			t.Fatalf("value-provider trust error = %v", err)
		}
	})
}

func assertProviderReplacementRejected(t *testing.T, trusted, replacement llmcontracts.Provider) {
	t.Helper()
	registry := providers.NewRegistry()
	registry.Register("direct", trusted)
	bridge, err := llmprovider.New(registry, llmprovider.Options{ProposalOnlyToolProviders: []string{"direct"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registry.Register("direct", replacement)
	request := validRequest()
	request.Tools = []workflowllm.ToolDefinition{{Name: "read", InputSchema: graph.Schema{"type": "object"}}}
	_, err = bridge.Complete(t.Context(), request, nil)
	assertProviderError(t, err, workflowllm.ProviderRejected, false)
}

func TestBridgeConcurrentCallsRemainIsolated(t *testing.T) {
	upstream := &fakeProvider{stream: func(_ context.Context, request llmtypes.ChatRequest) (<-chan llmtypes.StreamEvent, error) {
		return events(
			llmtypes.StreamEvent{Type: llmtypes.EventDelta, Content: `"` + request.Model + `"`},
			llmtypes.StreamEvent{Type: llmtypes.EventUsage, Usage: &llmtypes.Usage{InputTokens: 1, OutputTokens: 1, StopReason: "stop"}},
			llmtypes.StreamEvent{Type: llmtypes.EventDone},
		), nil
	}}
	bridge := newBridge(t, "direct", upstream)
	var group sync.WaitGroup
	errorsCh := make(chan error, 32)
	for index := 0; index < 32; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			request := validRequest()
			request.Binding.Model = fmt.Sprintf("model-%d", index)
			response, err := bridge.Complete(t.Context(), request, nil)
			if err == nil && response.RawText != fmt.Sprintf(`"model-%d"`, index) {
				err = fmt.Errorf("response %q", response.RawText)
			}
			errorsCh <- err
		}(index)
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func validRequest() workflowllm.ProviderRequest {
	return workflowllm.ProviderRequest{
		Binding:  workflowllm.ProviderBinding{Profile: "default", Provider: "direct", Model: "model-v1", BindingID: "binding-1"},
		Messages: []workflowllm.Message{{Role: "user", Content: "question"}},
		OutputSchema: graph.Schema{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"answer"},
			"properties":           map[string]any{"answer": map[string]any{"type": "string"}},
		},
		Budget: workflowllm.Budget{MaxInputBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxTotalTokens: 1_000, MaxToolCalls: 8},
	}
}

func newBridge(t *testing.T, name string, upstream llmcontracts.Provider) *llmprovider.Bridge {
	t.Helper()
	registry := providers.NewRegistry()
	registry.Register(name, upstream)
	bridge, err := llmprovider.New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return bridge
}

func newToolBridge(t *testing.T, name string, upstream llmcontracts.Provider) *llmprovider.Bridge {
	t.Helper()
	registry := providers.NewRegistry()
	registry.Register(name, upstream)
	bridge, err := llmprovider.New(registry, llmprovider.Options{ProposalOnlyToolProviders: []string{name}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return bridge
}

func mustInline(t *testing.T, input any) values.Value {
	t.Helper()
	value, err := values.NewInline(input, values.Metadata{Producer: values.Producer{Kind: "fixture", Reference: "input"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatalf("NewInline() error = %v", err)
	}
	return value
}

func events(input ...llmtypes.StreamEvent) <-chan llmtypes.StreamEvent {
	stream := make(chan llmtypes.StreamEvent, len(input))
	for _, event := range input {
		stream <- event
	}
	close(stream)
	return stream
}

func assertProviderError(t *testing.T, err error, kind workflowllm.ProviderFailureKind, retryable bool) {
	t.Helper()
	var providerErr *workflowllm.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Kind != kind || providerErr.Retryable != retryable {
		t.Fatalf("error = %#v, want kind=%q retryable=%t", err, kind, retryable)
	}
}

var _ llmcontracts.Provider = (*fakeProvider)(nil)
var _ llmcontracts.Provider = equalValueProvider{}
var _ workflowllm.StreamReceiver = (*receiver)(nil)
