package wait

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/values"
)

// Kind identifies the application-neutral reason an invocation is suspended.
type Kind string

const (
	KindGate     Kind = "gate"
	KindMessage  Kind = "message"
	KindTimer    Kind = "timer"
	KindCallback Kind = "callback"
	KindChildRun Kind = "child_run"
	KindSignal   Kind = "signal"
)

func (k Kind) Valid() bool {
	switch k {
	case KindGate, KindMessage, KindTimer, KindCallback, KindChildRun, KindSignal:
		return true
	default:
		return false
	}
}

// Status is the durable resolution state of a wait.
type Status string

const (
	StatusOpen     Status = "open"
	StatusResumed  Status = "resumed"
	StatusTimedOut Status = "timed_out"
	StatusCanceled Status = "canceled"
)

func (s Status) Valid() bool {
	switch s {
	case StatusOpen, StatusResumed, StatusTimedOut, StatusCanceled:
		return true
	default:
		return false
	}
}

// WakeSource identifies the adapter through which a wait can be resolved.
type WakeSource string

const (
	WakeGate     WakeSource = "gate"
	WakeMessage  WakeSource = "message"
	WakeTimer    WakeSource = "timer"
	WakeCallback WakeSource = "callback"
	WakeChildRun WakeSource = "child_run"
	WakeSignal   WakeSource = "signal"
)

func (s WakeSource) Valid() bool {
	switch s {
	case WakeGate, WakeMessage, WakeTimer, WakeCallback, WakeChildRun, WakeSignal:
		return true
	default:
		return false
	}
}

// Visibility describes who may discover a wait without changing value-plane
// redaction or retention semantics.
type Visibility string

const (
	VisibilityPublic  Visibility = "public"
	VisibilityPrivate Visibility = "private"
	VisibilitySecret  Visibility = "secret"
)

func (v Visibility) Valid() bool {
	switch v {
	case VisibilityPublic, VisibilityPrivate, VisibilitySecret:
		return true
	default:
		return false
	}
}

// ResponderAuthority declares the host policy that may accept a response.
// Kind is intentionally open for extraction; Reference is opaque to core.
type ResponderAuthority struct {
	Kind       string            `json:"kind"`
	Reference  string            `json:"reference,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (a ResponderAuthority) Validate() error {
	if err := requiredText("responder authority kind", a.Kind); err != nil {
		return err
	}
	if err := optionalText("responder authority reference", a.Reference); err != nil {
		return err
	}
	return validateStringMap("responder authority attributes", a.Attributes)
}

// Responder records the already-authorized principal or adapter that resolved
// a wait. It is provenance, not a host authorization model.
type Responder struct {
	Kind       string            `json:"kind"`
	Reference  string            `json:"reference"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (r Responder) Validate() error {
	if err := requiredText("responder kind", r.Kind); err != nil {
		return err
	}
	if err := requiredText("responder reference", r.Reference); err != nil {
		return err
	}
	return validateStringMap("responder attributes", r.Attributes)
}

// Resolution is immutable terminal provenance. PayloadDigest binds a resumed
// record to the accepted typed payload without retaining the payload inline.
type Resolution struct {
	Source         WakeSource `json:"source"`
	Responder      Responder  `json:"responder"`
	PayloadDigest  string     `json:"payload_digest,omitempty"`
	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	ResolvedAt     time.Time  `json:"resolved_at"`
}

func (r Resolution) Validate() error {
	if !r.Source.Valid() {
		return fmt.Errorf("unsupported resolution source %q", r.Source)
	}
	if err := r.Responder.Validate(); err != nil {
		return err
	}
	if r.PayloadDigest != "" {
		if err := values.ValidateDigest(r.PayloadDigest); err != nil {
			return fmt.Errorf("resolution payload digest: %w", err)
		}
	}
	if err := optionalText("resolution idempotency key", r.IdempotencyKey); err != nil {
		return err
	}
	if r.ResolvedAt.IsZero() {
		return fmt.Errorf("resolution time is required")
	}
	return nil
}

// Record is the canonical semantic wait record. Runtime storage envelopes add
// the wait and invocation identities plus CAS generations and storage times.
type Record struct {
	Kind              Kind                `json:"kind"`
	Correlation       string              `json:"correlation"`
	Deadline          time.Time           `json:"deadline,omitempty"`
	Payload           *values.ValueSetRef `json:"payload,omitempty"`
	ResumeSchema      SchemaRef           `json:"resume_schema"`
	ResumeTokenDigest string              `json:"resume_token_digest,omitempty"`
	ResumeURL         string              `json:"resume_url,omitempty"`
	Visibility        Visibility          `json:"visibility"`
	Authority         ResponderAuthority  `json:"authority"`
	WakeSource        WakeSource          `json:"wake_source"`
	Status            Status              `json:"status"`
	ResumeValues      *values.ValueSetRef `json:"resume_values,omitempty"`
	Resolution        *Resolution         `json:"resolution,omitempty"`
}

// Validate rejects ambiguous records and token-bearing persisted URLs.
func (r Record) Validate() error {
	if !r.Kind.Valid() {
		return fmt.Errorf("unsupported wait kind %q", r.Kind)
	}
	if err := requiredText("wait correlation", r.Correlation); err != nil {
		return err
	}
	if r.Payload != nil {
		if err := r.Payload.Validate(); err != nil {
			return fmt.Errorf("wait payload: %w", err)
		}
	}
	if err := r.ResumeSchema.Validate(); err != nil {
		return fmt.Errorf("wait resume schema: %w", err)
	}
	if r.ResumeTokenDigest != "" {
		if err := values.ValidateDigest(r.ResumeTokenDigest); err != nil {
			return fmt.Errorf("wait resume token digest: %w", err)
		}
	}
	if err := validateResumeURL(r.ResumeURL); err != nil {
		return err
	}
	if !r.Visibility.Valid() {
		return fmt.Errorf("unsupported wait visibility %q", r.Visibility)
	}
	if err := r.Authority.Validate(); err != nil {
		return err
	}
	if !r.WakeSource.Valid() {
		return fmt.Errorf("unsupported wait wake source %q", r.WakeSource)
	}
	if !r.Status.Valid() {
		return fmt.Errorf("unsupported wait status %q", r.Status)
	}
	if r.ResumeValues != nil {
		if err := r.ResumeValues.Validate(); err != nil {
			return fmt.Errorf("wait resume values: %w", err)
		}
	}
	switch r.Status {
	case StatusOpen:
		if r.ResumeValues != nil || r.Resolution != nil {
			return fmt.Errorf("open wait must not contain a resolution outcome")
		}
	case StatusResumed:
		if r.Resolution == nil {
			return fmt.Errorf("resumed wait requires resolution provenance")
		}
		if r.ResumeValues == nil && r.Resolution.PayloadDigest != "" {
			return fmt.Errorf("resumed wait without values must not contain a payload digest")
		}
		if r.ResumeValues != nil && r.Resolution.PayloadDigest != r.ResumeValues.Digest {
			return fmt.Errorf("resumed wait payload digest must match resume values")
		}
		if r.Resolution.Source != r.WakeSource {
			return fmt.Errorf("resumed wait resolution source must match wake source")
		}
	case StatusTimedOut:
		if r.ResumeValues != nil || r.Resolution == nil {
			return fmt.Errorf("closed wait requires provenance and no resume values")
		}
		if r.Deadline.IsZero() || r.Resolution.Source != WakeTimer || r.Resolution.Responder.Kind != "system" || r.Resolution.PayloadDigest != "" {
			return fmt.Errorf("timed-out wait requires a deadline and timer/system provenance without payload")
		}
	case StatusCanceled:
		if r.ResumeValues != nil || r.Resolution == nil {
			return fmt.Errorf("closed wait requires provenance and no resume values")
		}
		if r.Resolution.Responder.Kind != "system" || r.Resolution.PayloadDigest != "" {
			return fmt.Errorf("canceled wait requires system provenance without payload")
		}
	}
	if r.Resolution != nil {
		if err := r.Resolution.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func validateResumeURL(raw string) error {
	if raw == "" {
		return nil
	}
	if err := optionalText("wait resume URL", raw); err != nil {
		return err
	}
	parsed, err := url.Parse(raw)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" {
		return fmt.Errorf("wait resume URL must be absolute")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return fmt.Errorf("wait resume URL must use http or https")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("wait resume URL must not contain userinfo, query, or fragment")
	}
	return nil
}

func requiredText(field, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must contain valid UTF-8", field)
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > 4096 {
		return fmt.Errorf("%s exceeds 4096 bytes", field)
	}
	return nil
}

func optionalText(field, value string) error {
	if value == "" {
		return nil
	}
	return requiredText(field, value)
}

func validateStringMap(field string, entries map[string]string) error {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := requiredText(field+" key", key); err != nil {
			return err
		}
		value := entries[key]
		if !utf8.ValidString(value) {
			return fmt.Errorf("%s[%q] must contain valid UTF-8", field, key)
		}
	}
	return nil
}
