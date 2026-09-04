package agentsubstrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	agentadapter "github.com/hollis-labs/go-workflow/adapters/agent"
	"github.com/hollis-labs/go-workflow/values"

	"github.com/hollis-labs/hadron/internal/execution"

	agentsessions "github.com/hollis-labs/agentkit/agentsessions"
)

// LegacyWorkflowBridgeOptions binds the workflow adapter to the legacy Hadron
// launcher without exposing blueprint or project paths in workflow core.
type LegacyWorkflowBridgeOptions struct {
	BlueprintPath  string
	StepDir        string
	ReplySubstrate string
}

// LegacyWorkflowBridge maps Hadron's agentkit-owned launcher to the
// extraction-safe agent SessionHost for compatibility tests and migrations.
// It is not a production SessionHost: its replay journal is process-local and stores only the
// request digest plus non-secret handle/ref metadata; raw prompt and typed
// inputs are never journaled. The current Launcher registry and this replay
// journal are process-local; this bridge therefore fails closed after daemon
// restart and cannot fence a legacy ambiguous launch. Hosts requiring restart
// recovery must inject a genuinely durable SessionHost rather than use this
// compatibility bridge.
type LegacyWorkflowBridge struct {
	backend workflowBackend
	options LegacyWorkflowBridgeOptions

	mu      sync.Mutex
	replays map[string]bridgeReplay
	flights map[string]*bridgeFlight
}

type bridgeReplay struct {
	digest string
	result agentadapter.LaunchResult
}

type bridgeFlight struct{ done chan struct{} }

type bridgeSession struct {
	state agentsessions.State
	meta  map[string]string
	alive bool
}

type workflowBackend interface {
	launch(context.Context, execution.AgentLaunchRequest) (execution.AgentLaunchResult, error)
	session(string) (bridgeSession, bool)
	stop(context.Context, string) error
}

type launcherWorkflowBackend struct{ launcher *Launcher }

func (b launcherWorkflowBackend) launch(ctx context.Context, request execution.AgentLaunchRequest) (execution.AgentLaunchResult, error) {
	return b.launcher.LaunchAgent(ctx, request)
}

func (b launcherWorkflowBackend) session(id string) (bridgeSession, bool) {
	info, ok := b.launcher.sessions.Get(id)
	if !ok {
		return bridgeSession{}, false
	}
	alive := false
	if health, found := b.launcher.sessions.Health(id); found {
		alive = health.Health.Alive
	}
	return bridgeSession{state: info.State, meta: cloneBridgeMap(info.Meta), alive: alive}, true
}

func (b launcherWorkflowBackend) stop(ctx context.Context, id string) error {
	return b.launcher.sessions.Stop(ctx, id)
}

// NewLegacyWorkflowBridge constructs the explicitly non-production legacy
// binding. New workflow hosts must bind a durable agent.SessionHost instead.
func NewLegacyWorkflowBridge(launcher *Launcher, options LegacyWorkflowBridgeOptions) (*LegacyWorkflowBridge, error) {
	if launcher == nil || launcher.sessions == nil {
		return nil, fmt.Errorf("agent workflow bridge requires a launcher")
	}
	return newLegacyWorkflowBridge(launcherWorkflowBackend{launcher: launcher}, options)
}

func newLegacyWorkflowBridge(backend workflowBackend, options LegacyWorkflowBridgeOptions) (*LegacyWorkflowBridge, error) {
	if nilBridgeInterface(backend) {
		return nil, fmt.Errorf("agent workflow bridge requires a backend")
	}
	return &LegacyWorkflowBridge{
		backend: backend, options: options, replays: make(map[string]bridgeReplay), flights: make(map[string]*bridgeFlight),
	}, nil
}

// LaunchSession launches or exactly replays one logical workflow agent
// session. The bridge serializes the legacy launch boundary because that API
// has no atomic keyed start operation of its own.
func (b *LegacyWorkflowBridge) LaunchSession(ctx context.Context, request agentadapter.LaunchRequest) (agentadapter.LaunchResult, error) {
	if ctx == nil {
		return agentadapter.LaunchResult{}, errors.New("agent workflow bridge requires context")
	}
	if err := ctx.Err(); err != nil {
		return agentadapter.LaunchResult{}, err
	}
	if b == nil || nilBridgeInterface(b.backend) {
		return agentadapter.LaunchResult{}, errors.New("agent workflow bridge is not initialized")
	}
	if err := request.Validate(); err != nil {
		return agentadapter.LaunchResult{}, errors.New("agent workflow launch request is invalid")
	}
	if len(request.Inputs) != 0 {
		return agentadapter.LaunchResult{}, errors.New("agent workflow bridge does not support typed launch inputs")
	}
	digest, digestErr := request.Digest()
	if digestErr != nil {
		return agentadapter.LaunchResult{}, errors.New("agent workflow launch request is invalid")
	}

	for {
		b.mu.Lock()
		if replay, ok := b.replays[request.IdempotencyKey]; ok {
			b.mu.Unlock()
			if replay.digest != digest {
				return agentadapter.LaunchResult{}, agentadapter.ErrLaunchConflict
			}
			result := cloneLaunchResult(replay.result)
			result.Outcome = agentadapter.LaunchReplayed
			return result, nil
		}
		if flight, ok := b.flights[request.IdempotencyKey]; ok {
			b.mu.Unlock()
			select {
			case <-ctx.Done():
				return agentadapter.LaunchResult{}, ctx.Err()
			case <-flight.done:
				continue
			}
		}
		flight := &bridgeFlight{done: make(chan struct{})}
		b.flights[request.IdempotencyKey] = flight
		b.mu.Unlock()
		return b.launchSession(ctx, request, digest, flight)
	}
}

func (b *LegacyWorkflowBridge) launchSession(ctx context.Context, request agentadapter.LaunchRequest, digest string, flight *bridgeFlight) (agentadapter.LaunchResult, error) {
	defer b.finishFlight(request.IdempotencyKey, flight)
	if contextErr := ctx.Err(); contextErr != nil {
		return agentadapter.LaunchResult{}, contextErr
	}
	legacy, launchErr := b.backend.launch(ctx, execution.AgentLaunchRequest{
		Substrate: request.Substrate, LaunchID: request.LaunchID, LogicalAgentID: request.LogicalAgentID,
		PromptAppend: request.Prompt, BlueprintPath: b.options.BlueprintPath, StepDir: b.options.StepDir,
		Metadata: map[string]any{"correlation_id": request.Correlation, "reply_substrate": b.options.ReplySubstrate},
	})
	if launchErr != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return agentadapter.LaunchResult{}, contextErr
		}
		return agentadapter.LaunchResult{}, &bridgeError{message: "agent substrate launch failed", cause: launchErr}
	}
	result, resultErr := bridgeLaunchResult(request, digest, legacy)
	if resultErr != nil {
		return agentadapter.LaunchResult{}, resultErr
	}
	b.mu.Lock()
	b.replays[request.IdempotencyKey] = bridgeReplay{digest: digest, result: cloneLaunchResult(result)}
	b.mu.Unlock()
	if contextErr := ctx.Err(); contextErr != nil {
		return agentadapter.LaunchResult{}, contextErr
	}
	return result, nil
}

func (b *LegacyWorkflowBridge) finishFlight(key string, flight *bridgeFlight) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if current, ok := b.flights[key]; ok && current == flight {
		delete(b.flights, key)
		close(flight.done)
	}
}

// ObserveSession maps agentkit lifecycle state into the workflow adapter's
// closed state vocabulary. A missing restarted local session fails closed;
// durable remote substrates may provide a different SessionHost.
func (b *LegacyWorkflowBridge) ObserveSession(ctx context.Context, ref agentadapter.SessionRef) (agentadapter.SessionObservation, error) {
	if err := b.validateOperation(ctx, ref); err != nil {
		return agentadapter.SessionObservation{}, err
	}
	session, ok := b.backend.session(ref.ID)
	if !ok {
		return agentadapter.SessionObservation{}, &bridgeError{message: "agent session is unavailable after recovery"}
	}
	if err := ctx.Err(); err != nil {
		return agentadapter.SessionObservation{}, err
	}
	handle, err := bridgeHandle(ref, session.meta)
	if err != nil {
		return agentadapter.SessionObservation{}, err
	}
	switch session.state {
	case agentsessions.StateLaunching, agentsessions.StateRunning:
		return agentadapter.SessionObservation{
			State: agentadapter.SessionPending, Handle: handle,
			Progress: map[string]string{"state": string(session.state), "alive": fmt.Sprint(session.alive)},
		}, nil
	case agentsessions.StateDone:
		result, err := values.NewInline(nil, values.Metadata{
			Producer:  values.Producer{Kind: "agent_substrate", Reference: ref.ID, Output: agentadapter.OutputResult},
			MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun,
		})
		if err != nil {
			return agentadapter.SessionObservation{}, &bridgeError{message: "agent session result could not be represented", cause: err}
		}
		return agentadapter.SessionObservation{State: agentadapter.SessionSucceeded, Handle: handle, Result: &result, Progress: map[string]string{"state": string(session.state)}}, nil
	case agentsessions.StateFailed:
		return agentadapter.SessionObservation{State: agentadapter.SessionFailed, Failure: &agentadapter.SessionFailure{
			Code: "agent_session_failed", Message: "agent session failed", Retryable: false,
		}, Progress: map[string]string{"state": string(session.state)}}, nil
	default:
		return agentadapter.SessionObservation{}, &bridgeError{message: "agent session state is unsupported"}
	}
}

// HeartbeatSession verifies that the launcher still recognizes the durable
// session. Terminal state remains observable and is not turned into a
// transport failure merely because the process is no longer alive.
func (b *LegacyWorkflowBridge) HeartbeatSession(ctx context.Context, ref agentadapter.SessionRef) error {
	if err := b.validateOperation(ctx, ref); err != nil {
		return err
	}
	if _, ok := b.backend.session(ref.ID); !ok {
		return &bridgeError{message: "agent session heartbeat is unavailable"}
	}
	return ctx.Err()
}

// CancelSession requests idempotent agentkit session termination. Already
// terminal sessions need no additional mutation.
func (b *LegacyWorkflowBridge) CancelSession(ctx context.Context, ref agentadapter.SessionRef) error {
	if err := b.validateOperation(ctx, ref); err != nil {
		return err
	}
	session, ok := b.backend.session(ref.ID)
	if !ok {
		return &bridgeError{message: "agent session cancellation target is unavailable"}
	}
	if session.state == agentsessions.StateDone || session.state == agentsessions.StateFailed {
		return nil
	}
	if err := b.backend.stop(ctx, ref.ID); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return &bridgeError{message: "agent session cancellation failed", cause: err}
	}
	return ctx.Err()
}

func (b *LegacyWorkflowBridge) validateOperation(ctx context.Context, ref agentadapter.SessionRef) error {
	if ctx == nil {
		return errors.New("agent workflow bridge requires context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || nilBridgeInterface(b.backend) {
		return errors.New("agent workflow bridge is not initialized")
	}
	if err := ref.Validate(); err != nil {
		return errors.New("agent workflow session reference is invalid")
	}
	return nil
}

func bridgeLaunchResult(request agentadapter.LaunchRequest, digest string, legacy execution.AgentLaunchResult) (agentadapter.LaunchResult, error) {
	sessionURI, _ := legacy.Handles["session_urn"].(string)
	handle := agentadapter.SessionHandle{
		SessionID: legacy.SessionID, SessionURI: sessionURI, Mailbox: legacy.Mailbox,
		Substrate: request.Substrate, Correlation: request.Correlation,
	}
	result := agentadapter.LaunchResult{
		Outcome: agentadapter.LaunchApplied,
		Ref: agentadapter.SessionRef{
			ID: legacy.SessionID, Substrate: request.Substrate, Correlation: request.Correlation, RequestDigest: digest,
		},
		Handle: handle,
	}
	if err := result.Validate(request); err != nil {
		return agentadapter.LaunchResult{}, &bridgeError{message: "agent substrate returned an invalid safe handle", cause: err}
	}
	return result, nil
}

func bridgeHandle(ref agentadapter.SessionRef, metadata map[string]string) (agentadapter.SessionHandle, error) {
	handle := agentadapter.SessionHandle{
		SessionID: ref.ID, SessionURI: metadata["session_uri"], Mailbox: metadata["mailbox"],
		Substrate: ref.Substrate, Correlation: ref.Correlation,
	}
	if err := handle.Validate(); err != nil {
		return agentadapter.SessionHandle{}, &bridgeError{message: "agent substrate stored an invalid safe handle", cause: err}
	}
	return handle, nil
}

func cloneLaunchResult(input agentadapter.LaunchResult) agentadapter.LaunchResult { return input }

func cloneBridgeMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func nilBridgeInterface(value any) bool {
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

type bridgeError struct {
	message string
	cause   error
}

func (e *bridgeError) Error() string { return "agent workflow bridge: " + e.message }
func (e *bridgeError) Unwrap() error { return e.cause }

var _ agentadapter.SessionHost = (*LegacyWorkflowBridge)(nil)
