package waitadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/hollis-labs/hadron/workflow/stepkind"
	workflowwait "github.com/hollis-labs/hadron/workflow/wait"
)

const (
	WaitForName       = "wait_for"
	MessageWaitName   = "message_wait"
	SleepName         = "sleep"
	Version           = "v1"
	CapabilityWait    = "wait.resume"
	CapabilityMessage = "message.receive"

	CodeInvalidInvocation = "wait_invalid_invocation"
	CodeAuthorityFailed   = "wait_authority_failed"
	CodeCallbackFailed    = "wait_callback_failed"
	CodeContinuation      = "wait_continuation_invalid"
	CodeChildRunFailed    = "wait_child_run_unsuccessful"
)

// ChildRunTerminalStatus is the closed status vocabulary accepted by a
// fail-on-unsuccessful child-run wait.
type ChildRunTerminalStatus string

const (
	ChildRunSucceeded ChildRunTerminalStatus = "succeeded"
	ChildRunFailed    ChildRunTerminalStatus = "failed"
	ChildRunCanceled  ChildRunTerminalStatus = "canceled"
	ChildRunTimedOut  ChildRunTerminalStatus = "timed_out"
	ChildRunCrashed   ChildRunTerminalStatus = "crashed"
)

// ChildRunFailure is safe terminal metadata supplied by the host wake bridge.
// Provider causes and raw logs must not enter this envelope.
type ChildRunFailure struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

// ChildRunTerminalEnvelope is the generic child-run wake payload. Successful
// deliveries carry typed JSON outputs; unsuccessful deliveries carry only
// safe failure metadata and are converted to ordinary step failure when the
// wait config opts into fail_on_unsuccessful.
type ChildRunTerminalEnvelope struct {
	Status  ChildRunTerminalStatus `json:"status"`
	Outputs map[string]any         `json:"outputs,omitempty"`
	Failure *ChildRunFailure       `json:"failure,omitempty"`
}

// SourceKind is the adapter-level external wake vocabulary. Event is lowered
// to the canonical signal wake source and tagged in durable authority metadata.
type SourceKind string

const (
	SourceSignal   SourceKind = "signal"
	SourceEvent    SourceKind = "event"
	SourceCallback SourceKind = "callback"
	SourceChildRun SourceKind = "child_run"
	SourceMessage  SourceKind = "message"
	SourceTimer    SourceKind = "timer"
)

// Valid reports whether s is a supported external wake kind.
func (s SourceKind) Valid() bool {
	switch s {
	case SourceSignal, SourceEvent, SourceCallback, SourceChildRun, SourceMessage, SourceTimer:
		return true
	default:
		return false
	}
}

// Source is the normalized source description supplied to host policy.
// Attributes contain stable non-secret identifiers only.
type Source struct {
	Kind       SourceKind        `json:"kind"`
	Reference  string            `json:"reference"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// AuthorityRequest supplies immutable invocation and wake details to an
// application-owned policy resolver. Implementations must be concurrency-safe.
type AuthorityRequest struct {
	Identity    stepkind.InvocationIdentity `json:"identity"`
	Source      Source                      `json:"source"`
	Correlation string                      `json:"correlation"`
}

// AuthorityResolver returns the responder authority persisted in the generic
// wait record. The runtime's ResponderAuthorizer enforces it on resume.
type AuthorityResolver interface {
	ResolveWaitAuthority(context.Context, AuthorityRequest) (workflowwait.ResponderAuthority, error)
}

// AuthorityResolverFunc adapts a function to AuthorityResolver.
type AuthorityResolverFunc func(context.Context, AuthorityRequest) (workflowwait.ResponderAuthority, error)

// ResolveWaitAuthority implements AuthorityResolver.
func (f AuthorityResolverFunc) ResolveWaitAuthority(ctx context.Context, request AuthorityRequest) (workflowwait.ResponderAuthority, error) {
	if f == nil {
		return workflowwait.ResponderAuthority{}, fmt.Errorf("wait authority resolver is unavailable")
	}
	return f(ctx, request)
}

// CallbackRequest asks a host to materialize one callback without exposing a
// host route or credential format to workflow core.
type CallbackRequest struct {
	Identity    stepkind.InvocationIdentity `json:"identity"`
	Path        string                      `json:"path"`
	Correlation string                      `json:"correlation"`
	ExpiresAt   time.Time                   `json:"expires_at"`
	// IdempotencyKey is derived from immutable invocation, path, and
	// correlation identity. ExpiresAt is a requested TTL, not part of identity.
	IdempotencyKey string `json:"idempotency_key"`
}

// CallbackCredential is process-local. Token is returned once in StepWaiting
// and only its digest is persisted. URL must not contain Token.
type CallbackCredential struct {
	URL   string `json:"url"`
	Token string `json:"-"`
}

// CallbackIssuer creates an application-owned one-shot callback endpoint. An
// implementation must be concurrency-safe and idempotent for the same
// immutable IdempotencyKey: retrying an invocation must return the same URL
// and token instead of creating another concurrently live endpoint, even when
// requested ExpiresAt advances. The host must extend that credential through
// the requested expiry (or fail); replacement is permitted only after it has
// expired. Issuers own expiry conflicts and TTL cleanup for a credential
// created before durable suspension.
type CallbackIssuer interface {
	IssueCallback(context.Context, CallbackRequest) (CallbackCredential, error)
}

// CallbackIssuerFunc adapts a function to CallbackIssuer.
type CallbackIssuerFunc func(context.Context, CallbackRequest) (CallbackCredential, error)

// IssueCallback implements CallbackIssuer.
func (f CallbackIssuerFunc) IssueCallback(ctx context.Context, request CallbackRequest) (CallbackCredential, error) {
	if f == nil {
		return CallbackCredential{}, fmt.Errorf("callback issuer is unavailable")
	}
	return f(ctx, request)
}

// Options supplies host-owned wait policy and callback construction. Now is
// injectable for deterministic durable deadlines.
type Options struct {
	Authority AuthorityResolver
	Callbacks CallbackIssuer
	Now       func() time.Time
}

// Registration is the registered wait-backed executor set implemented by this
// package.
type Registration struct {
	Sleep       *Sleep
	WaitFor     *WaitFor
	MessageWait *MessageWait
}
