package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/hadron/workflow/graph"
	"github.com/hollis-labs/hadron/workflow/stepkind"
	"github.com/hollis-labs/hadron/workflow/values"
)

const (
	// KindName is the heavy external-operation executor emitted inside the
	// generated child workflow. agent_launch itself is source/graph sugar.
	KindName = "agent_session"
	// KindVersion is the first immutable agent-session contract.
	KindVersion = "v1"

	OutputHandle = "handle"
	OutputStatus = "status"
	OutputResult = "result"

	CodeInvalidInvocation = "agent_invalid_invocation"
	CodeLaunchFailed      = "agent_launch_failed"
	CodeLaunchConflict    = "agent_launch_conflict"
	CodeInvalidResult     = "agent_invalid_result"
	CodeObserveFailed     = "agent_observe_failed"
	CodeHeartbeatFailed   = "agent_heartbeat_failed"
	CodeCancelFailed      = "agent_cancel_failed"
)

var (
	ErrInvalidOptions = errors.New("invalid agent adapter options")
	ErrInvalidConfig  = errors.New("invalid agent session configuration")
	ErrLaunchConflict = errors.New("agent launch idempotency conflict")
	ErrInvalidResult  = errors.New("invalid agent session result")
)

// LogicalIdentity excludes attempt number so ambiguous retries of the same
// expanded child node use one launch identity.
type LogicalIdentity struct {
	RunID     string `json:"run_id"`
	NodeID    string `json:"node_id"`
	Iteration string `json:"iteration,omitempty"`
}

// Validate checks the immutable logical invocation identity used across
// attempts. NodeID follows the canonical graph identifier vocabulary.
func (i LogicalIdentity) Validate() error {
	if !stableText(i.RunID, 4096) || i.Iteration != "" && !stableText(i.Iteration, 4096) {
		return fmt.Errorf("launch identity is invalid")
	}
	if err := graph.ValidateID(i.NodeID); err != nil {
		return fmt.Errorf("launch identity node: %w", err)
	}
	return nil
}

// LaunchRequest is the immutable extraction-safe launch envelope. Inputs stay
// typed; resolved credentials and provider-specific session/team data must not
// enter this contract. IdempotencyKey identifies the same logical launch
// across attempts and process recovery.
type LaunchRequest struct {
	Identity       LogicalIdentity `json:"identity"`
	Substrate      string          `json:"substrate"`
	LaunchID       string          `json:"launch_id"`
	LogicalAgentID string          `json:"logical_agent_id"`
	Prompt         string          `json:"prompt,omitempty"`
	Correlation    string          `json:"correlation"`
	Inputs         values.ValueSet `json:"inputs"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// Digest returns the canonical immutable request digest used to reject a
// changed replay. JSON numbers remain exact through Value's own encoding.
func (r LaunchRequest) Digest() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	encoded, err := canonicalJSON(r)
	if err != nil {
		return "", err
	}
	return values.SHA256Digest(encoded), nil
}

// Validate checks the extraction-safe launch boundary.
func (r LaunchRequest) Validate() error {
	if err := r.Identity.Validate(); err != nil {
		return err
	}
	for _, field := range []struct {
		name, value string
	}{
		{"substrate", r.Substrate}, {"launch_id", r.LaunchID}, {"logical_agent_id", r.LogicalAgentID},
		{"correlation", r.Correlation}, {"idempotency_key", r.IdempotencyKey},
	} {
		if !stableText(field.value, 4096) {
			return fmt.Errorf("%s is required as stable text", field.name)
		}
	}
	if !optionalText(r.Prompt, 1<<20) {
		return fmt.Errorf("prompt must be stable UTF-8")
	}
	if err := values.ValidatePersistableSet(r.Inputs); err != nil {
		return fmt.Errorf("launch inputs: %w", err)
	}
	return nil
}

// SessionRef is the minimum durable, non-secret reference required to
// observe, heartbeat, or cancel an agent session after restart.
type SessionRef struct {
	ID            string `json:"id"`
	Substrate     string `json:"substrate"`
	Correlation   string `json:"correlation"`
	RequestDigest string `json:"request_digest"`
}

// Validate rejects ambiguous or credential-bearing durable references.
func (r SessionRef) Validate() error {
	if !stableText(r.ID, 4096) || !stableText(r.Substrate, 4096) || !stableText(r.Correlation, 4096) {
		return fmt.Errorf("session reference identity is invalid")
	}
	if err := values.ValidateDigest(r.RequestDigest); err != nil {
		return fmt.Errorf("session request digest: %w", err)
	}
	return nil
}

// SessionHandle is the typed safe handle exposed to workflow consumers. URIs
// must not carry userinfo, query parameters, fragments, or credentials.
type SessionHandle struct {
	SessionID   string `json:"session_id"`
	SessionURI  string `json:"session_uri,omitempty"`
	Mailbox     string `json:"mailbox,omitempty"`
	Substrate   string `json:"substrate"`
	Correlation string `json:"correlation"`
}

// Validate checks durable handle identity without interpreting substrate
// semantics.
func (h SessionHandle) Validate() error {
	if !stableText(h.SessionID, 4096) || !stableText(h.Substrate, 4096) || !stableText(h.Correlation, 4096) {
		return fmt.Errorf("session handle identity is invalid")
	}
	for _, field := range []struct {
		name, raw string
	}{{"session_uri", h.SessionURI}, {"mailbox", h.Mailbox}} {
		if field.raw == "" {
			continue
		}
		parsed, err := url.Parse(field.raw)
		if err != nil || parsed == nil || !parsed.IsAbs() || parsed.Scheme == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("session handle %s must be an absolute credential-free URI", field.name)
		}
	}
	return nil
}

// LaunchOutcome distinguishes a new host launch from an exact replay.
type LaunchOutcome string

const (
	LaunchApplied  LaunchOutcome = "applied"
	LaunchReplayed LaunchOutcome = "replayed"
)

func (o LaunchOutcome) valid() bool { return o == LaunchApplied || o == LaunchReplayed }

// LaunchResult binds one exact request digest to its durable session ref and
// safe workflow handle.
type LaunchResult struct {
	Outcome LaunchOutcome `json:"outcome"`
	Ref     SessionRef    `json:"ref"`
	Handle  SessionHandle `json:"handle"`
}

// Validate requires exact launch identity and replay metadata.
func (r LaunchResult) Validate(request LaunchRequest) error {
	if !r.Outcome.valid() {
		return fmt.Errorf("unsupported launch outcome %q", r.Outcome)
	}
	if err := r.Ref.Validate(); err != nil {
		return err
	}
	if err := r.Handle.Validate(); err != nil {
		return err
	}
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	if r.Ref.RequestDigest != digest || r.Ref.Substrate != request.Substrate || r.Ref.Correlation != request.Correlation ||
		r.Handle.SessionID != r.Ref.ID || r.Handle.Substrate != request.Substrate || r.Handle.Correlation != request.Correlation {
		return fmt.Errorf("launch result does not match immutable request")
	}
	return nil
}

// SessionState is the host-observed terminal or pending session state.
type SessionState string

const (
	SessionPending   SessionState = "pending"
	SessionSucceeded SessionState = "succeeded"
	SessionFailed    SessionState = "failed"
	SessionCanceled  SessionState = "canceled"
)

func (s SessionState) valid() bool {
	switch s {
	case SessionPending, SessionSucceeded, SessionFailed, SessionCanceled:
		return true
	default:
		return false
	}
}

// SessionFailure contains only safe-to-persist failure metadata. Cause stays
// process-local and is never formatted by the adapter.
type SessionFailure struct {
	Code      string
	Message   string
	Retryable bool
	Cause     error
}

// SessionObservation is returned by the injected host. Result is an exact
// typed value; the adapter applies a private/run minimum classification before
// returning it to runtime persistence.
type SessionObservation struct {
	State    SessionState
	Handle   SessionHandle
	Result   *values.Value
	Progress map[string]string
	Failure  *SessionFailure
}

// SessionHost is the complete app-owned boundary for durable agent sessions.
// LaunchSession must be concurrency-safe and exact-replay safe by
// IdempotencyKey, returning ErrLaunchConflict for a changed request. The other
// methods must locate work solely from SessionRef so runtime recovery after a
// process restart never depends on process-local adapter state. Hosts may use
// Prompt and Inputs only at the immediate launch boundary; durable host state
// stores the request digest and non-secret references, never raw prompt,
// resolved credentials, or typed input payloads.
type SessionHost interface {
	LaunchSession(context.Context, LaunchRequest) (LaunchResult, error)
	ObserveSession(context.Context, SessionRef) (SessionObservation, error)
	HeartbeatSession(context.Context, SessionRef) error
	CancelSession(context.Context, SessionRef) error
}

// SessionHostFuncs adapts functions to SessionHost for hosts and tests.
type SessionHostFuncs struct {
	Launch    func(context.Context, LaunchRequest) (LaunchResult, error)
	Observe   func(context.Context, SessionRef) (SessionObservation, error)
	Heartbeat func(context.Context, SessionRef) error
	Cancel    func(context.Context, SessionRef) error
}

func (f SessionHostFuncs) LaunchSession(ctx context.Context, request LaunchRequest) (LaunchResult, error) {
	if f.Launch == nil {
		return LaunchResult{}, ErrInvalidOptions
	}
	return f.Launch(ctx, request)
}

func (f SessionHostFuncs) ObserveSession(ctx context.Context, ref SessionRef) (SessionObservation, error) {
	if f.Observe == nil {
		return SessionObservation{}, ErrInvalidOptions
	}
	return f.Observe(ctx, ref)
}

func (f SessionHostFuncs) HeartbeatSession(ctx context.Context, ref SessionRef) error {
	if f.Heartbeat == nil {
		return ErrInvalidOptions
	}
	return f.Heartbeat(ctx, ref)
}

func (f SessionHostFuncs) CancelSession(ctx context.Context, ref SessionRef) error {
	if f.Cancel == nil {
		return ErrInvalidOptions
	}
	return f.Cancel(ctx, ref)
}

// Options supplies the host boundary for agent_session@v1.
type Options struct{ Host SessionHost }

func canonicalJSON(input any) ([]byte, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return json.Marshal(normalized)
}

func stableText(value string, maximum int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) || len(value) > maximum {
		return false
	}
	for _, current := range value {
		if unicode.IsControl(current) {
			return false
		}
	}
	return true
}

func optionalText(value string, maximum int) bool {
	if value == "" {
		return true
	}
	return stableText(value, maximum)
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

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func validateSafeStringMap(input map[string]string) error {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !stableText(key, 128) || !optionalText(input[key], 4096) {
			return fmt.Errorf("progress contains invalid stable text")
		}
	}
	return nil
}

func classifyHostFailure(code, message string, cause error) error {
	classification := stepkind.ClassifyError(cause)
	if classification == stepkind.RetryUnspecified {
		classification = stepkind.Retryable
	}
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: classification, Cause: cause}
}
