package tetherhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	agentadapter "github.com/hollis-labs/go-workflow/adapters/agent"
	"github.com/hollis-labs/go-workflow/stepkind"
	"github.com/hollis-labs/go-workflow/values"
	"github.com/hollis-labs/hadron/internal/settings"
)

// Failure codes the host reports. The go-workflow agent kind carries them
// into the step's durable failure.
const (
	CodeUnknownLogicalAgent = "agent_unknown_logical_agent"
	CodeUnknownSubstrate    = "agent_unknown_substrate"
	CodeInputsUnsupported   = "agent_inputs_unsupported"
	CodeNoResult            = "agent_no_result"
	CodeSessionCanceled     = "agent_session_canceled"
	CodeSessionLost         = "agent_session_lost"
	CodeSessionFailed       = "agent_session_failed"
	CodeSessionMissing      = "agent_session_missing"
	CodeHostUnreachable     = "agent_host_unreachable"
)

// InputsUnsupportedMessage is the stable refusal for typed agent inputs.
const InputsUnsupportedMessage = "agent_session typed inputs are not supported by the Tether session host yet"

// SessionURIPrefix prefixes the session id in a handle's session_uri.
const SessionURIPrefix = "tether://local/sessions/"

// lostExitCode is the exit code muxd's restart sweep records for a session
// whose process it lost.
const lostExitCode = -1

// Options configures a Host.
type Options struct {
	// Client reaches muxd. Required.
	Client Client
	// Substrates are the configured agent substrates; only entries of kind
	// tether_session are served. A launch naming any other substrate fails
	// permanently.
	Substrates map[string]settings.AgentSubstrateSettings
	// Secret is the per-Hadron result secret (see LoadOrCreateSecret).
	Secret []byte
	// Now defaults to time.Now.
	Now func() time.Time
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// LaunchAttempts bounds how many times one LaunchSession call sends the
	// keyed create/launch while muxd is unreachable. Default 3.
	LaunchAttempts int
	// LaunchBackoff is the pause between those attempts. Default 500ms.
	LaunchBackoff time.Duration
}

const (
	defaultLaunchAttempts = 3
	defaultLaunchBackoff  = 500 * time.Millisecond
)

type substrate struct {
	launch             string
	launches           map[string]string
	resultOptional     bool
	stopOnResult       bool
	unreachableTimeout time.Duration
}

// Host implements agentadapter.SessionHost over a Tether Client.
//
// Correctness never depends on process-local state: observe, heartbeat and
// cancel locate the session and recompute its result nonce from the durable
// SessionRef alone. The only in-memory state is the unreachable timer (when
// muxd was first seen unreachable for a ref), which restarts from zero if
// hadrond restarts, and a de-duplication set for invalid-reply log lines.
type Host struct {
	client     Client
	substrates map[string]substrate
	secret     []byte
	now        func() time.Time
	logger     *slog.Logger
	attempts   int
	backoff    time.Duration

	mu          sync.Mutex
	unreachable map[string]time.Time
	logged      map[string]struct{}
}

var _ agentadapter.SessionHost = (*Host)(nil)

// New validates options and constructs a Host.
func New(options Options) (*Host, error) {
	if options.Client == nil {
		return nil, errors.New("tether session host requires a client")
	}
	if len(options.Secret) < secretSize {
		return nil, errors.New("tether session host requires a 32-byte result secret")
	}
	substrates := make(map[string]substrate)
	for name, configured := range options.Substrates {
		if configured.Kind != settings.AgentSubstrateKindTetherSession {
			continue
		}
		if configured.Tether == nil {
			return nil, fmt.Errorf("agent substrate %q has no tether settings", name)
		}
		timeout, err := configured.Tether.EffectiveUnreachableTimeout()
		if err != nil {
			return nil, fmt.Errorf("agent substrate %q unreachable_timeout: %w", name, err)
		}
		launches := make(map[string]string, len(configured.Tether.Launches))
		for agent, launch := range configured.Tether.Launches {
			launches[agent] = launch
		}
		substrates[name] = substrate{
			launch: configured.Tether.Launch, launches: launches,
			resultOptional: configured.Tether.ResultOptional, stopOnResult: configured.Tether.EffectiveStopOnResult(),
			unreachableTimeout: timeout,
		}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	attempts := options.LaunchAttempts
	if attempts <= 0 {
		attempts = defaultLaunchAttempts
	}
	backoff := options.LaunchBackoff
	if backoff <= 0 {
		backoff = defaultLaunchBackoff
	}
	return &Host{
		client: options.Client, substrates: substrates, secret: append([]byte(nil), options.Secret...),
		now: now, logger: logger, attempts: attempts, backoff: backoff,
		unreachable: make(map[string]time.Time), logged: make(map[string]struct{}),
	}, nil
}

// Enabled reports whether any tether_session substrate is configured.
func (h *Host) Enabled() bool { return h != nil && len(h.substrates) > 0 }

// ServesSubstrate reports whether name is a configured tether_session substrate.
func (h *Host) ServesSubstrate(name string) bool {
	if h == nil {
		return false
	}
	_, ok := h.substrates[name]
	return ok
}

// LaunchSession creates (or replays) the keyed Tether session for request and
// launches it. Every byte sent to Tether is a pure function of request and the
// result secret, so a runtime retry of an ambiguous launch replays instead of
// conflicting.
func (h *Host) LaunchSession(ctx context.Context, request agentadapter.LaunchRequest) (agentadapter.LaunchResult, error) {
	config, ok := h.substrates[request.Substrate]
	if !ok {
		return agentadapter.LaunchResult{}, permanent(CodeUnknownSubstrate, "agent substrate "+strconv.Quote(request.Substrate)+" is not a configured tether_session substrate")
	}
	// Validation refuses typed inputs before a run exists; this is the
	// fail-closed backstop for plans that reach launch another way.
	if len(request.Inputs) != 0 {
		return agentadapter.LaunchResult{}, permanent(CodeInputsUnsupported, InputsUnsupportedMessage)
	}
	launch, ok := config.launches[request.LogicalAgentID]
	if !ok {
		launch = config.launch
	}
	if launch == "" {
		return agentadapter.LaunchResult{}, permanent(CodeUnknownLogicalAgent,
			"agent substrate "+strconv.Quote(request.Substrate)+" has no Tether launch for logical agent "+strconv.Quote(request.LogicalAgentID))
	}
	digest, err := request.Digest()
	if err != nil {
		return agentadapter.LaunchResult{}, &stepkind.ExecutionError{
			Code: agentadapter.CodeInvalidInvocation, Message: "agent launch request is invalid", Classification: stepkind.RetryPermanent, Cause: err,
		}
	}
	create := CreateRequest{
		Launch:         launch,
		PromptAppend:   PromptAppend(request.Prompt, request.Correlation, ResultNonce(h.secret, digest, request.Correlation)),
		IdempotencyKey: TetherKey(request),
	}
	created, launched, err := h.createAndLaunch(ctx, create)
	if err != nil {
		return agentadapter.LaunchResult{}, err
	}
	outcome := agentadapter.LaunchApplied
	if created.Replayed || launched.Replayed {
		outcome = agentadapter.LaunchReplayed
	}
	ref := agentadapter.SessionRef{ID: created.ID, Substrate: request.Substrate, Correlation: request.Correlation, RequestDigest: digest}
	return agentadapter.LaunchResult{Outcome: outcome, Ref: ref, Handle: handleFor(ref)}, nil
}

// createAndLaunch sends the keyed create and the launch, replaying both while
// muxd is unreachable. The replay is safe because create is keyed and its
// request is deterministic, and it matters because hadrond does not retry a
// failed step attempt: a create whose response was lost would otherwise fail
// the step and leave the created session running unobserved.
func (h *Host) createAndLaunch(ctx context.Context, create CreateRequest) (Session, Session, error) {
	var lastErr error
	for attempt := 1; attempt <= h.attempts; attempt++ {
		if attempt > 1 {
			timer := time.NewTimer(h.backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return Session{}, Session{}, ctx.Err()
			case <-timer.C:
			}
		}
		created, err := h.client.CreateSession(ctx, create)
		if err != nil {
			lastErr = launchError("create", err)
			if errors.Is(err, ErrUnavailable) {
				continue
			}
			return Session{}, Session{}, lastErr
		}
		launched, err := h.client.LaunchSession(ctx, created.ID)
		if err != nil {
			lastErr = launchError("launch", err)
			if errors.Is(err, ErrUnavailable) {
				continue
			}
			return Session{}, Session{}, lastErr
		}
		return created, launched, nil
	}
	return Session{}, Session{}, lastErr
}

func launchError(stage string, err error) error {
	switch {
	case errors.Is(err, ErrIdempotencyConflict):
		return fmt.Errorf("tether session %s: %w: %w", stage, agentadapter.ErrLaunchConflict, err)
	case errors.Is(err, ErrUnavailable):
		return retryable(CodeHostUnreachable, "tether daemon is unreachable", err)
	default:
		return fmt.Errorf("tether session %s: %w", stage, err)
	}
}

// ObserveSession reads the correlation thread first, so a result reply wins
// over any later session state (including a restart sweep or a stop). Only
// when no valid reply exists does the session lifecycle decide.
func (h *Host) ObserveSession(ctx context.Context, ref agentadapter.SessionRef) (agentadapter.SessionObservation, error) {
	config, ok := h.substrates[ref.Substrate]
	if !ok {
		return agentadapter.SessionObservation{}, permanent(CodeUnknownSubstrate, "agent substrate "+strconv.Quote(ref.Substrate)+" is not a configured tether_session substrate")
	}
	replies, err := h.client.ThreadReplies(ctx, ref.Correlation)
	if err != nil && !errors.Is(err, ErrNotFound) {
		if errors.Is(err, ErrUnavailable) {
			return h.unreachableObservation(ref, config, err), nil
		}
		return agentadapter.SessionObservation{}, err
	}
	h.reachable(ref.ID)
	if result, found := h.firstValidResult(ref, replies); found {
		value, valueErr := resultValue(ref, result)
		if valueErr != nil {
			// A nonce-bearing reply whose result cannot become a workflow value
			// is still the agent's one answer; fail rather than wait forever.
			observation := failed(ref, agentadapter.CodeInvalidResult, "agent result reply is not a valid JSON value", false)
			observation.Failure.Cause = valueErr
			return observation, nil //nolint:nilerr // The invalid result is reported as the step's terminal failure.
		}
		if config.stopOnResult {
			h.stop(ctx, ref)
		}
		return agentadapter.SessionObservation{State: agentadapter.SessionSucceeded, Handle: handleFor(ref), Result: &value}, nil
	}

	state, err := h.client.GetSession(ctx, ref.ID)
	switch {
	case errors.Is(err, ErrNotFound):
		return failed(ref, CodeSessionMissing, "tether session no longer exists", false), nil
	case errors.Is(err, ErrUnavailable):
		return h.unreachableObservation(ref, config, err), nil
	case err != nil:
		return agentadapter.SessionObservation{}, err
	}
	switch state.State {
	case StateCreated, StateReady, StateLaunching, StateRunning:
		progress := map[string]string{"state": state.State}
		health, healthErr := h.client.SessionHealth(ctx, ref.ID)
		switch {
		case healthErr == nil:
			progress["alive"] = strconv.FormatBool(health.Alive)
		case errors.Is(healthErr, ErrUnavailable):
			return h.unreachableObservation(ref, config, healthErr), nil
		default:
			progress["alive"] = "unknown"
		}
		return agentadapter.SessionObservation{State: agentadapter.SessionPending, Handle: handleFor(ref), Progress: progress}, nil
	case StateCompleted:
		if config.resultOptional {
			null, nullErr := resultValue(ref, json.RawMessage("null"))
			if nullErr != nil {
				return agentadapter.SessionObservation{}, nullErr
			}
			return agentadapter.SessionObservation{State: agentadapter.SessionSucceeded, Handle: handleFor(ref), Result: &null}, nil
		}
		return failed(ref, CodeNoResult, "agent session completed without a result reply", false), nil
	case StateKilled:
		// Killed covers Hadron's own cancel and a human stopping the session
		// in Tether; both close the step canceled, not failed.
		observation := failed(ref, CodeSessionCanceled, "tether session was stopped", false)
		observation.State = agentadapter.SessionCanceled
		return observation, nil
	case StateFailed:
		if state.ExitCode != nil && *state.ExitCode == lostExitCode {
			return failed(ref, CodeSessionLost, "tether session was lost when its daemon restarted", false), nil
		}
		message := "tether session failed"
		if state.ExitCode != nil {
			message += " with exit code " + strconv.Itoa(*state.ExitCode)
		}
		return failed(ref, CodeSessionFailed, message, false), nil
	default:
		return agentadapter.SessionObservation{}, fmt.Errorf("tether session has unknown state %q", state.State)
	}
}

// HeartbeatSession probes session health.
//
// go-workflow's external-operation coordinator calls Heartbeat before
// Observe and skips Observe when Heartbeat errors. A heartbeat error for an
// exited or missing session would therefore keep the step pending forever,
// so only reachability is reported here: an exited, not-alive or missing
// session heartbeats nil and Observe decides the terminal outcome. While muxd
// is unreachable the heartbeat fails retryably until the unreachable timeout
// elapses, then returns nil so Observe can close the step
// agent_host_unreachable.
func (h *Host) HeartbeatSession(ctx context.Context, ref agentadapter.SessionRef) error {
	config, ok := h.substrates[ref.Substrate]
	if !ok {
		return permanent(CodeUnknownSubstrate, "agent substrate "+strconv.Quote(ref.Substrate)+" is not a configured tether_session substrate")
	}
	health, err := h.client.SessionHealth(ctx, ref.ID)
	switch {
	case err == nil:
		h.reachable(ref.ID)
		if !health.Alive {
			h.logger.Debug("tether session is not alive; observation decides its outcome", "session_id", ref.ID)
		}
		return nil
	case errors.Is(err, ErrNotFound):
		h.reachable(ref.ID)
		return nil
	case errors.Is(err, ErrUnavailable):
		if _, expired := h.markUnreachable(ref.ID, config.unreachableTimeout); expired {
			return nil
		}
		return retryable(CodeHostUnreachable, "tether daemon is unreachable", err)
	default:
		return err
	}
}

// CancelSession stops the session. Stopping an exited or missing session
// succeeds.
func (h *Host) CancelSession(ctx context.Context, ref agentadapter.SessionRef) error {
	err := h.client.StopSession(ctx, ref.ID)
	switch {
	case err == nil, errors.Is(err, ErrNotFound):
		h.reachable(ref.ID)
		return nil
	case errors.Is(err, ErrUnavailable):
		return retryable(CodeHostUnreachable, "tether daemon is unreachable", err)
	default:
		return err
	}
}

// firstValidResult returns the "result" JSON of the first valid reply.
//
// Tether's reply "from" is asserted by the sending caller, not authenticated
// (same-host trust, ADR 0045; the gap is tracked with CW-20260918-0037). It is
// logged but never used to accept or reject a reply: the HMAC nonce, which
// only the launched session was told, is the sole proof of origin. Invalid or
// forged replies are ignored and logged once each.
func (h *Host) firstValidResult(ref agentadapter.SessionRef, replies []Reply) (json.RawMessage, bool) {
	nonce := ResultNonce(h.secret, ref.RequestDigest, ref.Correlation)
	for _, reply := range replies {
		result, err := parseResultReply(reply, nonce)
		if err == nil {
			return result, true
		}
		h.logInvalidReply(ref, reply, err)
	}
	return nil, false
}

func (h *Host) logInvalidReply(ref agentadapter.SessionRef, reply Reply, reason error) {
	key := ref.ID + "\x00" + reply.MessageID
	h.mu.Lock()
	if _, seen := h.logged[key]; seen {
		h.mu.Unlock()
		return
	}
	if len(h.logged) >= 4096 {
		h.logged = make(map[string]struct{})
	}
	h.logged[key] = struct{}{}
	h.mu.Unlock()
	h.logger.Warn("ignoring invalid agent result reply",
		"session_id", ref.ID, "thread_id", ref.Correlation, "message_id", reply.MessageID,
		"kind", reply.Kind, "to", reply.To, "from_unverified", reply.From, "reason", reason.Error())
}

func (h *Host) stop(ctx context.Context, ref agentadapter.SessionRef) {
	if err := h.client.StopSession(ctx, ref.ID); err != nil && !errors.Is(err, ErrNotFound) {
		h.logger.Warn("could not stop tether session after its result", "session_id", ref.ID, "error", err)
	}
}

// unreachableObservation keeps the step pending while muxd is unreachable,
// then fails it retryably once the substrate's unreachable timeout elapses.
// Returning pending (not an error) keeps the operation pending with visible
// progress; the coordinator would also keep it pending on an error.
func (h *Host) unreachableObservation(ref agentadapter.SessionRef, config substrate, cause error) agentadapter.SessionObservation {
	since, expired := h.markUnreachable(ref.ID, config.unreachableTimeout)
	if expired {
		observation := failed(ref, CodeHostUnreachable, "tether daemon unreachable for longer than "+config.unreachableTimeout.String(), true)
		observation.Failure.Cause = cause
		return observation
	}
	return agentadapter.SessionObservation{
		State: agentadapter.SessionPending, Handle: handleFor(ref),
		Progress: map[string]string{"state": "unreachable", "unreachable_since": since.UTC().Format(time.RFC3339)},
	}
}

// markUnreachable records the first time ref was seen unreachable and reports
// whether timeout has elapsed since.
func (h *Host) markUnreachable(id string, timeout time.Duration) (time.Time, bool) {
	now := h.now()
	h.mu.Lock()
	defer h.mu.Unlock()
	since, ok := h.unreachable[id]
	if !ok {
		since = now
		h.unreachable[id] = since
	}
	return since, now.Sub(since) >= timeout
}

func (h *Host) reachable(id string) {
	h.mu.Lock()
	delete(h.unreachable, id)
	h.mu.Unlock()
}

func handleFor(ref agentadapter.SessionRef) agentadapter.SessionHandle {
	return agentadapter.SessionHandle{
		SessionID: ref.ID, SessionURI: SessionURIPrefix + ref.ID, Mailbox: ResultMailbox,
		Substrate: ref.Substrate, Correlation: ref.Correlation,
	}
}

func failed(ref agentadapter.SessionRef, code, message string, isRetryable bool) agentadapter.SessionObservation {
	return agentadapter.SessionObservation{
		State: agentadapter.SessionFailed, Handle: handleFor(ref),
		Failure: &agentadapter.SessionFailure{Code: code, Message: message, Retryable: isRetryable},
	}
}

func resultValue(ref agentadapter.SessionRef, raw json.RawMessage) (values.Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return values.Value{}, err
	}
	return values.NewInline(decoded, values.Metadata{
		Producer:  values.Producer{Kind: agentadapter.KindName, Reference: ref.ID, Output: agentadapter.OutputResult},
		MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
	})
}

func permanent(code, message string) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.RetryPermanent}
}

func retryable(code, message string, cause error) error {
	return &stepkind.ExecutionError{Code: code, Message: message, Classification: stepkind.Retryable, Cause: cause}
}
