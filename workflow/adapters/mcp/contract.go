package mcp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// KindName is the stable graph kind name registered by this adapter.
	KindName = "mcp"
	// KindVersion is the first public MCP adapter contract version.
	KindVersion = "v1"

	OutputStructured = "structured_content"
	OutputText       = "text_content"
	OutputResources  = "resource_content"
	OutputMetadata   = "tool_metadata"
)

var (
	ErrInvalidOptions = errors.New("invalid MCP adapter options")
	ErrInvalidConfig  = errors.New("invalid MCP step config")
	ErrInvalidResult  = errors.New("invalid MCP tool result")
)

// ContentKind is the adapter-neutral projection of MCP result content.
type ContentKind string

const (
	ContentText         ContentKind = "text"
	ContentImage        ContentKind = "image"
	ContentAudio        ContentKind = "audio"
	ContentResourceLink ContentKind = "resource_link"
	ContentResourceText ContentKind = "resource_text"
	ContentResourceBlob ContentKind = "resource_blob"
)

// Content is one MCP result item. Data contains decoded image, audio, or blob
// bytes and is process-local until captured by ArtifactSink. Metadata is
// deliberately omitted because arbitrary server metadata has no persistence-
// safety guarantee.
type Content struct {
	Kind        ContentKind
	Text        string
	Data        []byte
	URI         string
	Name        string
	Description string
	MediaType   string
}

// ToolAnnotations mirrors the standard MCP behavior hints without importing
// an SDK. Pointer fields preserve absent versus explicit false.
type ToolAnnotations struct {
	Title           string `json:"title,omitempty"`
	ReadOnlyHint    *bool  `json:"read_only_hint,omitempty"`
	DestructiveHint *bool  `json:"destructive_hint,omitempty"`
	IdempotentHint  *bool  `json:"idempotent_hint,omitempty"`
	OpenWorldHint   *bool  `json:"open_world_hint,omitempty"`
}

// ToolDescriptor is supplied by a host-approved descriptor source. Trusted is
// an explicit policy decision: remote self-asserted annotations must remain
// untrusted unless a host has independently approved them.
type ToolDescriptor struct {
	Server      string
	Tool        string
	Annotations ToolAnnotations
	Trusted     bool
}

// TransportMetadata contains stable, non-secret observations about one call.
// Attributes must be safe to persist and must never contain arguments,
// credentials, endpoint URLs, headers, raw result data, or bearer tokens.
type TransportMetadata struct {
	Transport    string
	AttemptCount int
	RetryCount   int
	Reconnected  bool
	Attributes   map[string]string
}

// CallRequest is the process-local call envelope. Arguments may contain
// resolved secret material only during Client.ExecuteTool. Implementations
// must not persist or log Arguments or IdempotencyKey.
type CallRequest struct {
	Server         string
	Tool           string
	Arguments      map[string]any
	IdempotencyKey string
}

// CallResult is the complete adapter-neutral MCP tool result. HasStructured
// distinguishes an explicitly present JSON null from an absent field.
type CallResult struct {
	HasStructured bool
	Structured    any
	Content       []Content
	IsError       bool
	Transport     TransportMetadata
}

// Client performs MCP calls. Protocol SDKs belong in host bridges, not in
// workflow engine core packages.
type Client interface {
	ExecuteTool(context.Context, CallRequest) (CallResult, error)
}

// Descriptor resolves tool annotations before execution so validation and
// policy do not depend on post-execution metadata.
type Descriptor interface {
	DescribeTool(context.Context, string, string) (ToolDescriptor, error)
}

// ArtifactRequest contains one already-redacted result body. Implementations
// must return a persistable TypeArtifact Value with the exact Producer,
// MediaType, Redaction, and Retention metadata supplied here.
type ArtifactRequest struct {
	Name     string
	Content  []byte
	Metadata values.Metadata
	RunID    string
}

// ArtifactSink materializes large, binary, or explicitly resource-backed MCP
// result content without prescribing a workflow artifact-store owner policy.
type ArtifactSink interface {
	CaptureArtifact(context.Context, ArtifactRequest) (values.Value, error)
}

// ArtifactSinkFunc adapts a function to ArtifactSink.
type ArtifactSinkFunc func(context.Context, ArtifactRequest) (values.Value, error)

func (f ArtifactSinkFunc) CaptureArtifact(ctx context.Context, request ArtifactRequest) (values.Value, error) {
	return f(ctx, request)
}

// Options injects every boundary that can touch MCP transport, secrets, or
// artifact bytes. InlineLimit bounds result values retained inline.
type Options struct {
	Client       Client
	Descriptor   Descriptor
	Secrets      values.SecretResolver
	Artifacts    ArtifactSink
	InlineLimit  int64
	MaxArtifacts int
}

// ConfigDescription is the deterministic pre-execution policy projection of
// one configuration. Annotations remain visible even when untrusted; only
// trusted annotations may narrow the conservative effects and safety hints.
type ConfigDescription struct {
	Server             string                `json:"server"`
	Tool               string                `json:"tool"`
	Annotations        ToolAnnotations       `json:"annotations"`
	AnnotationsTrusted bool                  `json:"annotations_trusted"`
	Effects            graph.EffectSet       `json:"effects"`
	Idempotency        graph.IdempotencyMode `json:"idempotency"`
	RetrySafety        stepkind.RetrySafety  `json:"retry_safety"`
}

// TransportError lets a client classify a transport failure without making
// the workflow adapter depend on a concrete SDK error type.
type TransportError struct {
	Retryable bool
	Cause     error
}

// ResultError marks a malformed protocol result after transport succeeded.
// Error intentionally omits Cause so raw server content never becomes a
// persistable or routinely logged error string.
type ResultError struct {
	Cause error
}

func (e *ResultError) Error() string { return "MCP result conversion failed" }

func (e *ResultError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *ResultError) RetryClassification() stepkind.RetryClassification {
	return stepkind.RetryPermanent
}

func (e *TransportError) Error() string {
	if e == nil || e.Cause == nil {
		return "MCP transport failed"
	}
	return e.Cause.Error()
}

func (e *TransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *TransportError) RetryClassification() stepkind.RetryClassification {
	if e != nil && e.Retryable {
		return stepkind.Retryable
	}
	return stepkind.RetryPermanent
}

func validateStableText(name, value string, required bool) error {
	if value == "" && !required {
		return nil
	}
	if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must be non-empty without surrounding whitespace", name)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must contain valid UTF-8", name)
	}
	for _, current := range value {
		if current < 0x20 || current == 0x7f {
			return fmt.Errorf("%s must not contain control characters", name)
		}
	}
	return nil
}
