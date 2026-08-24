package http

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"net/netip"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	KindName    = "http"
	KindVersion = "v1"

	OutputStatus   = "status"
	OutputHeaders  = "headers"
	OutputBody     = "body"
	OutputBodyJSON = "body_json"
	OutputMetadata = "request_metadata"
)

var (
	ErrInvalidOptions = errors.New("invalid HTTP adapter options")
	ErrInvalidConfig  = errors.New("invalid HTTP step config")
	ErrPolicyDenied   = errors.New("HTTP destination denied")
	ErrInvalidResult  = errors.New("invalid HTTP response")
)

// Resolver resolves every address considered for a destination. Implementors
// must return all answers; the adapter authorizes every answer before choosing
// one canonical address to pin for the actual dial.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

// RequestDeclaration is the non-secret pre-execution policy view. Path never
// includes a query or fragment. Effects and capabilities are author claims,
// not effective permissions.
type RequestDeclaration struct {
	Method            string          `json:"method"`
	Scheme            string          `json:"scheme"`
	Host              string          `json:"host"`
	Port              uint16          `json:"port"`
	Path              string          `json:"path"`
	Effects           graph.EffectSet `json:"effects,omitempty"`
	Capabilities      []string        `json:"capabilities,omitempty"`
	HasBody           bool            `json:"has_body"`
	HasSecretRefs     bool            `json:"has_secret_refs"`
	HasIdempotencyKey bool            `json:"has_idempotency_key"`
	RedirectMode      RedirectMode    `json:"redirect_mode"`
}

// PolicyDescription is a host's pre-execution interpretation. Only a trusted,
// valid description may narrow the adapter's conservative metadata.
type PolicyDescription struct {
	Trusted     bool                  `json:"trusted"`
	Effects     graph.EffectSet       `json:"effects,omitempty"`
	Idempotency graph.IdempotencyMode `json:"idempotency"`
	RetrySafety stepkind.RetrySafety  `json:"retry_safety"`
}

// RedirectContext describes one process-local redirect decision without the
// raw Location value or URL query.
type RedirectContext struct {
	Status         int
	PreviousOrigin string
	CrossOrigin    bool
	Method         string
	ProposedMethod string
	MethodRewrite  bool
}

// DestinationRequest is presented once for every resolved address of every
// hop. Host is normalized without brackets; Port is always explicit.
type DestinationRequest struct {
	Scheme   string
	Host     string
	Port     uint16
	Address  netip.Addr
	Path     string
	Hop      int
	Method   string
	Redirect *RedirectContext
}

// DestinationAuthorization contains narrow trusted decisions that cannot be
// inferred safely from the author configuration alone.
type DestinationAuthorization struct {
	AllowMethodRewrite bool
}

// Policy supplies both pre-execution metadata and per-address authorization.
// Returning nil from AuthorizeDestination means only that the destination is
// allowed; method rewriting still requires an explicit true decision.
type Policy interface {
	DescribeRequest(context.Context, RequestDeclaration) (PolicyDescription, error)
	AuthorizeDestination(context.Context, DestinationRequest) (DestinationAuthorization, error)
}

// Exchange is the process-local request handed to Transport. Request may
// contain resolved credentials. Transport implementations must dial exactly
// Destination.Address:Destination.Port and must not consult ambient proxies,
// credential stores, cookie jars, or a second DNS resolver.
type Exchange struct {
	Request     *nethttp.Request
	Destination DestinationRequest
}

// Transport performs exactly one HTTP exchange. It must not transparently
// retry or follow redirects.
type Transport interface {
	RoundTrip(context.Context, Exchange) (*nethttp.Response, error)
}

// ContextDialer is the narrow dial seam used by PinnedTransport.
type ContextDialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

// PinnedTransportOptions configures the secure default transport. TLSConfig is
// defensively cloned for each exchange; ServerName is always replaced by the
// approved logical host.
type PinnedTransportOptions struct {
	Dialer                 ContextDialer
	TLSConfig              *tls.Config
	MaxResponseHeaderBytes int64
}

// PinnedTransport is the default proxy-free, cookie-free, single-destination
// transport. Each RoundTrip owns a fresh net/http transport and connection.
type PinnedTransport struct {
	dialer                 ContextDialer
	tlsConfig              *tls.Config
	maxResponseHeaderBytes int64
}

// TransportFailure is a stable failure category supplied by custom transports.
type TransportFailure string

const (
	FailureDNS      TransportFailure = "dns"
	FailureConnect  TransportFailure = "connect"
	FailureTLS      TransportFailure = "tls"
	FailureProtocol TransportFailure = "protocol"
	FailureTimeout  TransportFailure = "timeout"
	FailureCanceled TransportFailure = "canceled"
)

func (f TransportFailure) valid() bool {
	switch f {
	case FailureDNS, FailureConnect, FailureTLS, FailureProtocol, FailureTimeout, FailureCanceled:
		return true
	default:
		return false
	}
}

// TransportError lets an injected transport retain a process-local cause
// while exposing only a stable category to persistence and retry policy.
type TransportError struct {
	Failure TransportFailure
	Cause   error
}

func (e *TransportError) Error() string {
	if e == nil || !e.Failure.valid() {
		return "HTTP transport failed"
	}
	return "HTTP " + string(e.Failure) + " failed"
}

func (e *TransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// ArtifactRequest contains one already-redacted response body. MaxBytes is the
// original configured response bound; sinks must not exceed it.
type ArtifactRequest struct {
	Name     string
	Content  []byte
	Metadata values.Metadata
	RunID    string
	MaxBytes int64
}

// ArtifactSink materializes large or binary response bodies.
type ArtifactSink interface {
	CaptureArtifact(context.Context, ArtifactRequest) (values.Value, error)
}

// ArtifactSinkFunc adapts a function to ArtifactSink.
type ArtifactSinkFunc func(context.Context, ArtifactRequest) (values.Value, error)

func (f ArtifactSinkFunc) CaptureArtifact(ctx context.Context, request ArtifactRequest) (values.Value, error) {
	return f(ctx, request)
}

// ObservationPhase is a closed operational event phase.
type ObservationPhase string

const (
	ObservationRequest  ObservationPhase = "request"
	ObservationResponse ObservationPhase = "response"
	ObservationRedirect ObservationPhase = "redirect"
	ObservationError    ObservationPhase = "error"
)

// Observation is operational metadata, never workflow output data. Origin has
// no path/query. Header values are already masked; body bytes are never present.
type Observation struct {
	Phase     ObservationPhase
	Origin    string
	Method    string
	Hop       int
	Status    int
	Headers   map[string][]string
	BodyBytes int64
	Code      string
}

// Observer receives defensive, sanitized operational observations. Errors are
// deliberately ignored and cannot change workflow execution.
type Observer interface {
	ObserveHTTP(context.Context, Observation) error
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Observation) error

func (f ObserverFunc) ObserveHTTP(ctx context.Context, observation Observation) error {
	return f(ctx, observation)
}

// Options injects every security- or persistence-sensitive boundary.
type Options struct {
	Resolver  Resolver
	Policy    Policy
	Transport Transport
	Secrets   values.SecretResolver
	Artifacts ArtifactSink
	Observer  Observer
	// MaxHeaderBytes bounds both resolved request and received response headers.
	MaxHeaderBytes int64
}

// ConfigDescription is the deterministic pre-execution policy contract. URL
// path/query, headers, body, credentials, and idempotency-key values are absent.
type ConfigDescription struct {
	Method                string                `json:"method"`
	Origin                string                `json:"origin"`
	DeclaredEffects       graph.EffectSet       `json:"declared_effects,omitempty"`
	DeclaredCapabilities  []string              `json:"declared_capabilities,omitempty"`
	IdempotencyKeyPresent bool                  `json:"idempotency_key_present"`
	Policy                PolicyDescription     `json:"policy"`
	EffectiveEffects      graph.EffectSet       `json:"effective_effects"`
	EffectiveIdempotency  graph.IdempotencyMode `json:"effective_idempotency"`
	EffectiveRetrySafety  stepkind.RetrySafety  `json:"effective_retry_safety"`
}

// NewPinnedTransport constructs the secure default transport.
func NewPinnedTransport(options PinnedTransportOptions) (*PinnedTransport, error) {
	dialer := options.Dialer
	if nilInterface(dialer) {
		dialer = &net.Dialer{Timeout: 30 * time.Second, KeepAlive: -1}
	}
	headerLimit := options.MaxResponseHeaderBytes
	if headerLimit == 0 {
		headerLimit = 256 << 10
	}
	if headerLimit < 1024 || headerLimit > 16<<20 {
		return nil, fmt.Errorf("%w: max response header bytes must be between 1 KiB and 16 MiB", ErrInvalidOptions)
	}
	var tlsConfig *tls.Config
	if options.TLSConfig != nil {
		if options.TLSConfig.InsecureSkipVerify {
			return nil, fmt.Errorf("%w: insecure TLS verification is not allowed", ErrInvalidOptions)
		}
		tlsConfig = options.TLSConfig.Clone()
	}
	return &PinnedTransport{dialer: dialer, tlsConfig: tlsConfig, maxResponseHeaderBytes: headerLimit}, nil
}

func validateStableText(name, value string) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must be non-empty UTF-8 without surrounding whitespace", name)
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return fmt.Errorf("%s must not contain control characters", name)
		}
	}
	return nil
}
