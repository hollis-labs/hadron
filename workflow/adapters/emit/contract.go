package emit

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	KindName    = "emit"
	KindVersion = "v1"

	CapabilityPublish = "event.publish"

	CodeInvalidInvocation = "emit_invalid_invocation"
	CodePolicyDenied      = "emit_policy_denied"
	CodePublishFailed     = "emit_publish_failed"
	CodeInvalidResult     = "emit_invalid_result"
)

const (
	maxStableTextBytes = 4096
	maxAttributes      = 32
	maxAttributeBytes  = 1024
	maxAttributesBytes = 8 << 10
)

var (
	ErrInvalidOptions       = errors.New("invalid emit adapter options")
	ErrInvalidConfig        = errors.New("invalid emit configuration")
	ErrInvalidEnvelope      = errors.New("invalid emit envelope")
	ErrInvalidResult        = errors.New("invalid emit publication result")
	ErrPublicationConflict  = errors.New("emit publication idempotency conflict")
	ErrPublicationRejected  = errors.New("emit publication rejected")
	ErrPublicationTransient = errors.New("emit publication temporarily unavailable")
)

// Destination is an application-neutral routing declaration. Kind identifies
// the host adapter/capability family and Reference is an opaque, non-secret
// route identity. Credentials and bearer URLs are forbidden in every field.
type Destination struct {
	Kind       string            `json:"kind"`
	Reference  string            `json:"reference"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// Validate checks the bounded transport-stable destination declaration.
func (d Destination) Validate() error {
	if err := stableIdentifier("destination kind", d.Kind); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEnvelope, err)
	}
	if err := safeOpaqueText("destination reference", d.Reference, maxStableTextBytes); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEnvelope, err)
	}
	if err := safeStringMap("destination attributes", d.Attributes); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEnvelope, err)
	}
	return nil
}

// Envelope is the immutable typed publication handed to a host Publisher.
// Payload may be inline, an ArtifactRef, or a SecretRef and retains its exact
// classification and producer provenance. Publishers must never log it.
type Envelope struct {
	ID             string       `json:"id"`
	Destination    Destination  `json:"destination"`
	EventType      string       `json:"event_type"`
	Correlation    string       `json:"correlation"`
	Payload        values.Value `json:"payload"`
	IdempotencyKey string       `json:"idempotency_key"`
}

// Validate checks the complete publication boundary without inspecting or
// rendering payload content.
func (e Envelope) Validate() error {
	if err := stableIdentifier("envelope id", e.ID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEnvelope, err)
	}
	if err := e.Destination.Validate(); err != nil {
		return err
	}
	if err := stableIdentifier("event type", e.EventType); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEnvelope, err)
	}
	if err := safeOpaqueText("correlation", e.Correlation, maxStableTextBytes); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEnvelope, err)
	}
	if err := e.Payload.Validate(); err != nil {
		return fmt.Errorf("%w: payload: %w", ErrInvalidEnvelope, err)
	}
	if err := stableText("idempotency key", e.IdempotencyKey, 512); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEnvelope, err)
	}
	return nil
}

// AuthorizationRequest contains only immutable identity and payload metadata;
// policy never receives inline payload or resolved secret material.
type AuthorizationRequest struct {
	Identity       stepkind.InvocationIdentity `json:"identity"`
	Destination    Destination                 `json:"destination"`
	EventType      string                      `json:"event_type"`
	Correlation    string                      `json:"correlation"`
	EnvelopeID     string                      `json:"envelope_id"`
	PayloadType    values.Type                 `json:"payload_type"`
	PayloadDigest  string                      `json:"payload_digest"`
	Redaction      values.RedactionClass       `json:"redaction"`
	Retention      values.RetentionClass       `json:"retention"`
	IdempotencyKey string                      `json:"idempotency_key"`
}

// Policy authorizes one exact destination and envelope before any publisher
// side effect. Implementations must be concurrency-safe and fail closed.
type Policy interface {
	AuthorizeEmit(context.Context, AuthorizationRequest) error
}

// PolicyFunc adapts a function to Policy.
type PolicyFunc func(context.Context, AuthorizationRequest) error

// AuthorizeEmit implements Policy.
func (f PolicyFunc) AuthorizeEmit(ctx context.Context, request AuthorizationRequest) error {
	if f == nil {
		return ErrPublicationRejected
	}
	return f(ctx, request)
}

// PublicationOutcome describes whether the host applied or exactly replayed
// the immutable publication.
type PublicationOutcome string

const (
	PublicationApplied  PublicationOutcome = "applied"
	PublicationReplayed PublicationOutcome = "replayed"
)

// Valid reports whether o belongs to the closed publication vocabulary.
func (o PublicationOutcome) Valid() bool {
	return o == PublicationApplied || o == PublicationReplayed
}

// PublicationResult contains bounded, non-secret receipt metadata only.
// Attributes must never contain payloads, credentials, raw transport errors,
// endpoint URLs, or secret destination identifiers.
type PublicationResult struct {
	EnvelopeID    string             `json:"envelope_id"`
	PublicationID string             `json:"publication_id"`
	Outcome       PublicationOutcome `json:"outcome"`
	PublishedAt   time.Time          `json:"published_at"`
	Attributes    map[string]string  `json:"attributes,omitempty"`
}

// Validate checks the safe durable receipt envelope.
func (r PublicationResult) Validate() error {
	if err := stableIdentifier("publication envelope id", r.EnvelopeID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	if err := safeOpaqueText("publication id", r.PublicationID, 512); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	if !r.Outcome.Valid() {
		return fmt.Errorf("%w: unsupported publication outcome", ErrInvalidResult)
	}
	if r.PublishedAt.IsZero() {
		return fmt.Errorf("%w: published_at is required", ErrInvalidResult)
	}
	if err := safeStringMap("publication attributes", r.Attributes); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	return nil
}

// Publisher performs the actual host-bound event/message publication.
// Implementations must be concurrency-safe and idempotent by IdempotencyKey:
// exact replay returns the same receipt with Outcome=replayed; a different
// envelope for an existing key returns ErrPublicationConflict.
type Publisher interface {
	Publish(context.Context, Envelope) (PublicationResult, error)
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(context.Context, Envelope) (PublicationResult, error)

// Publish implements Publisher.
func (f PublisherFunc) Publish(ctx context.Context, envelope Envelope) (PublicationResult, error) {
	if f == nil {
		return PublicationResult{}, ErrPublicationRejected
	}
	return f(ctx, envelope)
}

// ObservationPhase is a closed operational emit phase. Authorized never
// implies that a publication side effect occurred; the other phases are
// emitted only after authorization succeeds.
type ObservationPhase string

const (
	ObservationAuthorized ObservationPhase = "authorized"
	ObservationPublished  ObservationPhase = "published"
	ObservationFailed     ObservationPhase = "failed"
)

// Observation contains only bounded routing and payload digests. It never
// carries the destination reference, correlation, attributes, payload,
// idempotency key, credentials, or raw causes.
type Observation struct {
	Phase             ObservationPhase      `json:"phase"`
	EnvelopeID        string                `json:"envelope_id"`
	EventType         string                `json:"event_type"`
	DestinationKind   string                `json:"destination_kind"`
	DestinationDigest string                `json:"destination_digest"`
	PayloadType       values.Type           `json:"payload_type"`
	PayloadDigest     string                `json:"payload_digest"`
	Redaction         values.RedactionClass `json:"redaction"`
	Outcome           PublicationOutcome    `json:"outcome,omitempty"`
	Code              string                `json:"code,omitempty"`
}

// Observer receives defensive, sanitized operational observations. Errors
// are deliberately ignored and cannot change workflow execution.
type Observer interface {
	ObserveEmit(context.Context, Observation) error
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(context.Context, Observation) error

// ObserveEmit implements Observer.
func (f ObserverFunc) ObserveEmit(ctx context.Context, observation Observation) error {
	if f == nil {
		return nil
	}
	return f(ctx, observation)
}

// PublicationError provides safe retry classification while retaining a raw
// process-local cause. Error never formats Cause.
type PublicationError struct {
	Failure error
	Cause   error
}

func (e *PublicationError) Error() string {
	if e == nil {
		return "emit publication failed"
	}
	switch {
	case errors.Is(e.Failure, ErrPublicationConflict):
		return "emit publication conflicted"
	case errors.Is(e.Failure, ErrPublicationTransient):
		return "emit publication temporarily unavailable"
	default:
		return "emit publication rejected"
	}
}

func (e *PublicationError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return []error{e.Failure, e.Cause}
}

func (e *PublicationError) RetryClassification() stepkind.RetryClassification {
	if e != nil && errors.Is(e.Failure, ErrPublicationTransient) {
		return stepkind.Retryable
	}
	return stepkind.RetryPermanent
}

// Options supplies the application-owned execution boundaries. Observer is
// optional and fail-safe; Policy and Publisher are required.
type Options struct {
	Policy    Policy
	Publisher Publisher
	Observer  Observer
}

// ConfigDescription is a deterministic pre-execution policy projection.
type ConfigDescription struct {
	Destination          Destination           `json:"destination"`
	EventType            string                `json:"event_type"`
	RequiredCapabilities []string              `json:"required_capabilities"`
	Effects              graph.EffectSet       `json:"effects"`
	Idempotency          graph.IdempotencyMode `json:"idempotency"`
	RetrySafety          stepkind.RetrySafety  `json:"retry_safety"`
}

func stableIdentifier(field, value string) error {
	if err := stableText(field, value, 128); err != nil {
		return err
	}
	for index, current := range value {
		if current >= 'a' && current <= 'z' || current >= '0' && current <= '9' ||
			index > 0 && (current == '-' || current == '_' || current == '.' || current == '/' || current == ':') {
			continue
		}
		return fmt.Errorf("%s must use a normalized identifier", field)
	}
	return nil
}

func stableText(field, value string, maximum int) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must be non-empty stable UTF-8", field)
	}
	if len(value) > maximum {
		return fmt.Errorf("%s exceeds its byte limit", field)
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return fmt.Errorf("%s must not contain control characters", field)
		}
	}
	return nil
}

func stableStringMap(field string, entries map[string]string) error {
	if len(entries) > maxAttributes {
		return fmt.Errorf("%s exceeds its entry limit", field)
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	total := 0
	for _, key := range keys {
		if err := stableIdentifier(field+" key", key); err != nil {
			return err
		}
		value := entries[key]
		if err := stableText(field+" value", value, maxAttributeBytes); err != nil {
			return err
		}
		total += len(key) + len(value)
		if total > maxAttributesBytes {
			return fmt.Errorf("%s exceeds its total byte limit", field)
		}
	}
	return nil
}

func safeOpaqueText(field, value string, maximum int) error {
	if err := stableText(field, value, maximum); err != nil {
		return err
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{"secret", "token", "credential", "password", "authorization", "bearer", "cookie", "signature", "api_key", "apikey"} {
		if strings.Contains(lower, marker) {
			return fmt.Errorf("%s contains forbidden sensitive vocabulary", field)
		}
	}
	if strings.Contains(value, "://") || strings.ContainsAny(value, "?#@=") {
		return fmt.Errorf("%s must be an opaque non-URI identifier", field)
	}
	return nil
}

func safeStringMap(field string, entries map[string]string) error {
	if err := stableStringMap(field, entries); err != nil {
		return err
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := safeOpaqueText(field+" key", key, 128); err != nil {
			return err
		}
		if err := safeOpaqueText(field+" value", entries[key], maxAttributeBytes); err != nil {
			return err
		}
	}
	return nil
}
