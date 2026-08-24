package cmd

import (
	"context"
	"errors"
	"io"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// Name is the canonical registered command kind name.
	Name = "cmd"
	// Version is the immutable initial command contract version.
	Version = "v1"

	CodeInvalidInvocation = "cmd_invalid_invocation"
	CodePolicyDenied      = "cmd_policy_denied"
	CodeSecretFailed      = "cmd_secret_failed" //nolint:gosec // Stable error code, not credential material.
	CodeProcessFailed     = "cmd_process_failed"
	CodeNonZeroExit       = "cmd_nonzero_exit"
	CodeTimeout           = "cmd_timeout"
	CodeCanceled          = "cmd_canceled"
	CodeOutputTruncated   = "cmd_output_truncated"
	CodeParseFailed       = "cmd_parse_failed"
	CodeArtifactFailed    = "cmd_artifact_failed"
	CodeEventFailed       = "cmd_event_failed"
)

const (
	CapabilityProcessExecute = "process.execute"
	SandboxDirect            = "direct"
	SandboxNone              = "none"
)

var (
	ErrPolicyDenied    = errors.New("command policy denied")
	ErrProcessFailed   = errors.New("command process failed")
	ErrOutputTruncated = errors.New("command output exceeded configured bound")
	ErrParseFailed     = errors.New("command output parse failed")
)

// SandboxSpec is a named isolation expectation. direct and none explicitly
// mean that the adapter supplies no isolation. Other profiles require an
// injected runner that enforces and attests the exact authorized profile.
type SandboxSpec struct {
	Profile string `json:"profile"`
}

// ConfigDescription separates untrusted author expectations from the
// conservative metadata that policy must assume for an arbitrary executable.
type ConfigDescription struct {
	ConfiguredExecutable string
	Arguments            []string
	ConfiguredCWD        string
	EnvironmentNames     []string
	DeclaredEffects      graph.EffectSet
	DeclaredCapabilities []string
	SandboxExpectation   SandboxSpec
	ConservativeEffects  graph.EffectSet
	RequiredCapabilities []string
	Idempotency          graph.IdempotencyMode
	RetrySafety          stepkind.RetrySafety
}

// ResolvedCommand is the policy-authoritative launch description. Executable
// and CWD must be absolute, clean paths. Effective metadata is trusted only
// because it is returned by Policy, never because an author declared it.
type ResolvedCommand struct {
	Executable            string
	Arguments             []string
	CWD                   string
	EffectiveEffects      graph.EffectSet
	EffectiveCapabilities []string
	Sandbox               SandboxSpec
}

// Policy resolves and authorizes one structured launch description. It must
// not inspect shell-like substrings: arguments are already distinct values.
type Policy interface {
	AuthorizeCommand(context.Context, ConfigDescription) (ResolvedCommand, error)
}

// PolicyFunc adapts a function to Policy.
type PolicyFunc func(context.Context, ConfigDescription) (ResolvedCommand, error)

func (f PolicyFunc) AuthorizeCommand(ctx context.Context, description ConfigDescription) (ResolvedCommand, error) {
	if f == nil {
		return ResolvedCommand{}, ErrPolicyDenied
	}
	return f(ctx, description)
}

// EnvironmentVariable is short-lived process-boundary material. Value may
// contain a resolved secret and must never be logged, persisted, or retained.
type EnvironmentVariable struct {
	Name  string
	Value []byte
}

// ProcessRequest is a direct, already-authorized launch request. Runners must
// defensively copy data they retain, observe ctx, terminate the process before
// returning on cancellation, drain/close both streams, and enforce Sandbox.
type ProcessRequest struct {
	Executable  string
	Arguments   []string
	CWD         string
	Environment []EnvironmentVariable
	Sandbox     SandboxSpec
}

// ProcessResult attests the exit status and sandbox actually enforced. A
// runner returns non-zero exit codes as results, not as raw errors.
type ProcessResult struct {
	ExitCode        int
	EnforcedSandbox SandboxSpec
}

// ProcessRunner launches a command without a shell.
type ProcessRunner interface {
	Run(context.Context, ProcessRequest, io.Writer, io.Writer) (ProcessResult, error)
}

// ProcessRunnerFunc adapts a function to ProcessRunner.
type ProcessRunnerFunc func(context.Context, ProcessRequest, io.Writer, io.Writer) (ProcessResult, error)

func (f ProcessRunnerFunc) Run(ctx context.Context, request ProcessRequest, stdout, stderr io.Writer) (ProcessResult, error) {
	if f == nil {
		return ProcessResult{}, ErrProcessFailed
	}
	return f(ctx, request, stdout, stderr)
}

// ArtifactCapture describes already-redacted command bytes. ArtifactSink must
// stream and bound source to MaxBytes, preserve Metadata exactly, and return a
// persistable TypeArtifact value.
type ArtifactCapture struct {
	Identity stepkind.InvocationIdentity
	Stream   Stream
	Name     string
	MaxBytes int64
	Metadata values.Metadata
}

// ArtifactSink stores a redacted command stream as an immutable artifact.
type ArtifactSink interface {
	CaptureArtifact(context.Context, ArtifactCapture, io.Reader) (values.Value, error)
}

// ArtifactSinkFunc adapts a function to ArtifactSink.
type ArtifactSinkFunc func(context.Context, ArtifactCapture, io.Reader) (values.Value, error)

func (f ArtifactSinkFunc) CaptureArtifact(ctx context.Context, request ArtifactCapture, source io.Reader) (values.Value, error) {
	if f == nil {
		return values.Value{}, values.ErrArtifactInvalid
	}
	return f(ctx, request, source)
}

// ArtifactPutRequestFactory supplies host ownership, access, clock, and store
// data while the adapter fixes content metadata and hard byte bounds.
type ArtifactPutRequestFactory interface {
	ArtifactPutRequest(context.Context, ArtifactCapture) (values.ArtifactPutRequest, error)
}

// ArtifactPutRequestFactoryFunc adapts a function to ArtifactPutRequestFactory.
type ArtifactPutRequestFactoryFunc func(context.Context, ArtifactCapture) (values.ArtifactPutRequest, error)

func (f ArtifactPutRequestFactoryFunc) ArtifactPutRequest(ctx context.Context, capture ArtifactCapture) (values.ArtifactPutRequest, error) {
	if f == nil {
		return values.ArtifactPutRequest{}, values.ErrArtifactInvalid
	}
	return f(ctx, capture)
}

// StoreArtifactSink connects cmd capture to the workflow ArtifactStore without
// placing host ownership or authorization rules in this adapter.
type StoreArtifactSink struct {
	Store    values.ArtifactStore
	Requests ArtifactPutRequestFactory
}

func (s StoreArtifactSink) CaptureArtifact(ctx context.Context, capture ArtifactCapture, source io.Reader) (values.Value, error) {
	if ctx == nil || nilInterface(s.Store) || nilInterface(s.Requests) || source == nil || capture.MaxBytes <= 0 {
		return values.Value{}, values.ErrArtifactInvalid
	}
	if err := ctx.Err(); err != nil {
		return values.Value{}, err
	}
	request, err := s.Requests.ArtifactPutRequest(ctx, capture)
	if err != nil {
		return values.Value{}, err
	}
	request.Metadata = capture.Metadata
	request.MaxBytes = capture.MaxBytes
	metadata, err := s.Store.Put(ctx, request, source)
	if err != nil {
		return values.Value{}, err
	}
	value, err := values.NewArtifact(metadata.Ref)
	if err != nil {
		return values.Value{}, err
	}
	if value.Producer != capture.Metadata.Producer || value.MediaType != capture.Metadata.MediaType ||
		value.Redaction != capture.Metadata.Redaction || value.Retention != capture.Metadata.Retention ||
		value.Artifact.SizeBytes > capture.MaxBytes {
		return values.Value{}, values.ErrArtifactInvalid
	}
	return value, nil
}

// Stream identifies one process byte stream.
type Stream string

const (
	StreamStdout Stream = "stdout"
	StreamStderr Stream = "stderr"
)

// OperationalEvent is redacted, non-durable workflow-output data. Sequence is
// monotonic within one stream; cross-stream ordering is intentionally not data.
type OperationalEvent struct {
	Identity stepkind.InvocationIdentity
	Stream   Stream
	Sequence uint64
	Payload  []byte
}

// EventSink receives operational chunks. Implementations must be concurrency-
// safe and must not reinterpret events as typed workflow outputs.
type EventSink interface {
	EmitCommandEvent(context.Context, OperationalEvent) error
}

// EventSinkFunc adapts a function to EventSink.
type EventSinkFunc func(context.Context, OperationalEvent) error

func (f EventSinkFunc) EmitCommandEvent(ctx context.Context, event OperationalEvent) error {
	if f == nil {
		return nil
	}
	event.Payload = append([]byte(nil), event.Payload...)
	return f(ctx, event)
}
