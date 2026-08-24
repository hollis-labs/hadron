package llm

import (
	"context"
	"errors"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

const (
	KindName    = "llm"
	KindVersion = "v1"

	// MaximumProvenanceAttributeCount bounds each durable binding/provider
	// metadata map independently.
	MaximumProvenanceAttributeCount = 64
	// MaximumProvenanceAttributeBytes bounds the aggregate UTF-8 key and value
	// bytes in each durable binding/provider metadata map.
	MaximumProvenanceAttributeBytes = 16 << 10

	OutputValue     = "output"
	OutputRawText   = "raw_text"
	OutputToolCalls = "tool_calls"
	OutputUsage     = "usage"
	OutputStop      = "stop_reason"
	OutputAudit     = "audit"
)

var (
	ErrInvalidOptions = errors.New("invalid LLM adapter options")
	ErrInvalidConfig  = errors.New("invalid LLM step config")
	ErrPolicyDenied   = errors.New("LLM policy denied")
	ErrInvalidResult  = errors.New("invalid LLM provider result")
	ErrToolDenied     = errors.New("LLM tool denied")
	ErrBudgetExceeded = errors.New("LLM budget exceeded")
)

// ProviderBinding is the host-approved provider identity used for one
// invocation. Every field is durable audit data and must be stable and
// non-secret. Credentials, bearer tokens, endpoints containing credentials,
// and resolved secret material are forbidden.
type ProviderBinding struct {
	Profile    string            `json:"profile"`
	Provider   string            `json:"provider"`
	Model      string            `json:"model"`
	BindingID  string            `json:"binding_id"`
	Revision   string            `json:"revision,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// PolicyRequest is the payload-free declaration authorized before prompts or
// typed inputs leave the process. It intentionally omits message contents.
type PolicyRequest struct {
	Profile   string   `json:"profile"`
	Provider  string   `json:"provider,omitempty"`
	Model     string   `json:"model,omitempty"`
	Tools     []string `json:"tools,omitempty"`
	Budget    Budget   `json:"budget"`
	Streaming bool     `json:"streaming,omitempty"`
}

// Policy binds a provider and applies host policy independently of workflow
// authoring. Denials should wrap ErrPolicyDenied.
type Policy interface {
	Authorize(context.Context, PolicyRequest) (ProviderBinding, error)
}

// Message is a provider-neutral conversational message. ToolResult is used
// only for adapter-created tool responses and is process-local provider input.
type Message struct {
	Role        string       `json:"role"`
	Content     string       `json:"content,omitempty"`
	ToolCallID  string       `json:"tool_call_id,omitempty"`
	Tool        string       `json:"tool,omitempty"`
	ToolRequest *ToolRequest `json:"tool_request,omitempty"`
	ToolResult  any          `json:"tool_result,omitempty"`
}

// ToolDefinition is trusted host metadata exposed to the selected provider.
type ToolDefinition struct {
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	InputSchema graph.Schema `json:"input_schema"`
}

// ToolRequest is untrusted model intent. ID is process-local correlation and
// never becomes literal audit evidence.
type ToolRequest struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

// ToolExecutionRequest carries the entire exact allowlist so the host tool
// boundary can enforce it independently of this adapter.
type ToolExecutionRequest struct {
	Allowed []string        `json:"allowed"`
	Request ToolRequest     `json:"request"`
	Binding ProviderBinding `json:"binding"`
}

// ToolExecutionResult is process-local provider context. Content must be
// JSON-compatible and must not contain unresolved or resolved secret material.
type ToolExecutionResult struct {
	Tool    string
	Content any
}

// ToolHost resolves trusted definitions and executes approved tools. Both
// methods must enforce that names match the supplied exact allowlist.
type ToolHost interface {
	ResolveTools(context.Context, []string) ([]ToolDefinition, error)
	ExecuteTool(context.Context, ToolExecutionRequest) (ToolExecutionResult, error)
}

// ProviderRequest is one bounded, process-local provider call. Messages and
// Context may contain private workflow data and must never be logged or
// persisted by provider bridges. Repair requests consume the same aggregate
// Budget and have tools disabled.
type ProviderRequest struct {
	Binding      ProviderBinding  `json:"binding"`
	Messages     []Message        `json:"messages"`
	Context      values.ValueSet  `json:"context,omitempty"`
	Tools        []ToolDefinition `json:"tools,omitempty"`
	OutputSchema graph.Schema     `json:"output_schema"`
	Budget       Budget           `json:"budget"`
	Repair       bool             `json:"repair,omitempty"`
}

// Usage is exact provider-reported accounting for one call or the aggregate
// typed output. CostMicrounits avoids floating-point currency.
type Usage struct {
	InputTokens    int64 `json:"input_tokens"`
	OutputTokens   int64 `json:"output_tokens"`
	TotalTokens    int64 `json:"total_tokens"`
	CostMicrounits int64 `json:"cost_microunits"`
	Requests       int64 `json:"requests"`
	ToolCalls      int64 `json:"tool_calls"`
}

// StopReason is the closed terminal provider outcome.
type StopReason string

const (
	StopCompleted StopReason = "completed"
	StopLength    StopReason = "length"
	StopTool      StopReason = "tool"
	StopFiltered  StopReason = "filtered"
)

func (s StopReason) Valid() bool {
	switch s {
	case StopCompleted, StopLength, StopTool, StopFiltered:
		return true
	default:
		return false
	}
}

// ProviderAudit is stable non-secret provider response provenance.
type ProviderAudit struct {
	RequestID  string            `json:"request_id,omitempty"`
	Revision   string            `json:"revision,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// ProviderResponse is one provider turn. ToolRequests and terminal output are
// mutually exclusive. Output is preferred; otherwise RawText is decoded as
// exact JSON for the configured output schema.
type ProviderResponse struct {
	HasOutput    bool          `json:"has_output,omitempty"`
	Output       any           `json:"output,omitempty"`
	RawText      string        `json:"raw_text,omitempty"`
	ToolRequests []ToolRequest `json:"tool_requests,omitempty"`
	Usage        Usage         `json:"usage"`
	StopReason   StopReason    `json:"stop_reason"`
	Audit        ProviderAudit `json:"audit"`
}

// StreamChunk is an ephemeral provider output fragment. It is never durable;
// the adapter applies its configured redactor before forwarding it.
type StreamChunk struct {
	Text string
}

// StreamReceiver is invoked by Provider only when streaming was requested.
type StreamReceiver interface {
	Receive(context.Context, StreamChunk) error
}

// StreamSink receives already-redacted, bounded process-local fragments.
type StreamSink interface {
	Receive(context.Context, StreamChunk) error
}

// Provider performs one provider-neutral completion. Concrete SDK clients,
// authentication, retries, and endpoint selection belong behind this seam.
type Provider interface {
	Complete(context.Context, ProviderRequest, StreamReceiver) (ProviderResponse, error)
}

// ProviderError supplies provider/infra classification without allowing its
// raw Cause text to become a persistent workflow failure.
type ProviderError struct {
	Kind      ProviderFailureKind
	Retryable bool
	Cause     error
}

// ProviderFailureKind keeps provider rejection distinct from transient
// provider service and host transport/infrastructure failures.
type ProviderFailureKind string

const (
	ProviderUnavailable    ProviderFailureKind = "provider_unavailable"
	ProviderRateLimited    ProviderFailureKind = "provider_rate_limited"
	ProviderRejected       ProviderFailureKind = "provider_rejected"
	ProviderInfrastructure ProviderFailureKind = "infrastructure"
)

func (k ProviderFailureKind) Valid() bool {
	switch k {
	case ProviderUnavailable, ProviderRateLimited, ProviderRejected, ProviderInfrastructure:
		return true
	default:
		return false
	}
}

func (e *ProviderError) Error() string { return "LLM provider call failed" }
func (e *ProviderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
func (e *ProviderError) RetryClassification() stepkind.RetryClassification {
	if e != nil && e.Retryable {
		return stepkind.Retryable
	}
	return stepkind.RetryPermanent
}

// ToolError supplies host tool classification without persisting raw content.
type ToolError struct {
	Retryable bool
	Cause     error
}

func (e *ToolError) Error() string { return "LLM tool execution failed" }
func (e *ToolError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
func (e *ToolError) RetryClassification() stepkind.RetryClassification {
	if e != nil && e.Retryable {
		return stepkind.Retryable
	}
	return stepkind.RetryPermanent
}

// ToolCallRecord is adapter-issued literal audit evidence. It intentionally
// excludes model-supplied IDs, arguments, tool results, and credentials.
type ToolCallRecord struct {
	Sequence int                          `json:"sequence"`
	Tool     string                       `json:"tool"`
	Outcome  verification.ActivityOutcome `json:"outcome"`
}
