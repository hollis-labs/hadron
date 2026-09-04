package agentsubstrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	agentadapter "github.com/hollis-labs/go-workflow/adapters/agent"
	"github.com/hollis-labs/go-workflow/values"

	"github.com/hollis-labs/hadron/internal/execution"

	agentsessions "github.com/hollis-labs/agentkit/agentsessions"
)

type fakeWorkflowBackend struct {
	launches   []execution.AgentLaunchRequest
	result     execution.AgentLaunchResult
	sessions   map[string]bridgeSession
	launchHook func(context.Context) error
	stopHook   func(context.Context) error
}

func (f *fakeWorkflowBackend) launch(ctx context.Context, request execution.AgentLaunchRequest) (execution.AgentLaunchResult, error) {
	if f.launchHook != nil {
		if err := f.launchHook(ctx); err != nil {
			return execution.AgentLaunchResult{}, err
		}
	}
	f.launches = append(f.launches, request)
	return f.result, nil
}

func (f *fakeWorkflowBackend) session(id string) (bridgeSession, bool) {
	value, ok := f.sessions[id]
	return value, ok
}

func (f *fakeWorkflowBackend) stop(ctx context.Context, _ string) error {
	if f.stopHook != nil {
		return f.stopHook(ctx)
	}
	return nil
}

func TestLegacyWorkflowBridgeMapsSafeLaunchAndExactProcessReplay(t *testing.T) {
	backend := &fakeWorkflowBackend{
		result:   execution.AgentLaunchResult{SessionID: "session-1", Mailbox: "mailbox://agent/one", Handles: map[string]any{"session_urn": "agent://session/one"}},
		sessions: map[string]bridgeSession{},
	}
	bridge, err := newLegacyWorkflowBridge(backend, LegacyWorkflowBridgeOptions{BlueprintPath: "workflow-generated", StepDir: "agent", ReplySubstrate: "workflow-message"})
	if err != nil {
		t.Fatal(err)
	}
	request := bridgeRequest(t)
	first, err := bridge.LaunchSession(t.Context(), request)
	if err != nil || first.Outcome != agentadapter.LaunchApplied {
		t.Fatalf("LaunchSession = %#v, %v", first, err)
	}
	second, err := bridge.LaunchSession(t.Context(), request)
	if err != nil || second.Outcome != agentadapter.LaunchReplayed || len(backend.launches) != 1 {
		t.Fatalf("replay = %#v, %v; launches=%d", second, err, len(backend.launches))
	}
	legacy := backend.launches[0]
	if legacy.PromptAppend != "prompt" || legacy.Metadata["correlation_id"] != "agent:parent:launch" || legacy.Metadata["reply_substrate"] != "workflow-message" {
		t.Fatalf("legacy request = %#v", legacy)
	}
	changed := request
	changed.Prompt = "changed"
	if _, err := bridge.LaunchSession(t.Context(), changed); !errors.Is(err, agentadapter.ErrLaunchConflict) {
		t.Fatalf("changed replay = %v", err)
	}

	// The compatibility bridge has no typed legacy input port and fails closed
	// rather than silently discarding workflow data.
	request.Inputs = values.ValueSet{"payload": bridgeInline(t, "typed input")}
	if _, err := bridge.LaunchSession(t.Context(), request); err == nil || !strings.Contains(err.Error(), "does not support typed launch inputs") {
		t.Fatalf("typed input launch = %v", err)
	}
}

func TestLegacyWorkflowBridgeLifecycleAndSafeFailures(t *testing.T) {
	backend := &fakeWorkflowBackend{
		result:   execution.AgentLaunchResult{SessionID: "session-1", Mailbox: "mailbox://agent/one", Handles: map[string]any{"session_urn": "agent://session/one"}},
		sessions: map[string]bridgeSession{},
	}
	bridge, _ := newLegacyWorkflowBridge(backend, LegacyWorkflowBridgeOptions{})
	launched, err := bridge.LaunchSession(t.Context(), bridgeRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	backend.sessions[launched.Ref.ID] = bridgeSession{state: agentsessions.StateRunning, alive: true, meta: map[string]string{
		"session_uri": "agent://session/one", "mailbox": "mailbox://agent/one",
	}}
	pending, err := bridge.ObserveSession(t.Context(), launched.Ref)
	if err != nil || pending.State != agentadapter.SessionPending || pending.Progress["alive"] != "true" {
		t.Fatalf("Observe(pending) = %#v, %v", pending, err)
	}
	backend.sessions[launched.Ref.ID] = bridgeSession{state: agentsessions.StateDone, meta: backend.sessions[launched.Ref.ID].meta}
	done, err := bridge.ObserveSession(t.Context(), launched.Ref)
	if err != nil || done.State != agentadapter.SessionSucceeded || done.Result == nil || done.Result.Redaction != values.RedactionPrivate {
		t.Fatalf("Observe(done) = %#v, %v", done, err)
	}
	if heartbeatErr := bridge.HeartbeatSession(t.Context(), launched.Ref); heartbeatErr != nil {
		t.Fatal(heartbeatErr)
	}
	if cancelErr := bridge.CancelSession(t.Context(), launched.Ref); cancelErr != nil {
		t.Fatal(cancelErr)
	}

	raw := errors.New("provider token raw-secret")
	backend.sessions[launched.Ref.ID] = bridgeSession{state: agentsessions.StateRunning}
	backend.stopHook = func(context.Context) error { return raw }
	err = bridge.CancelSession(t.Context(), launched.Ref)
	if !errors.Is(err, raw) || strings.Contains(err.Error(), raw.Error()) {
		t.Fatalf("safe cancel error = %T %v", err, err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	backend.stopHook = func(context.Context) error { cancel(); return nil }
	if err := bridge.CancelSession(ctx, launched.Ref); !errors.Is(err, context.Canceled) {
		t.Fatalf("late cancellation = %v", err)
	}

	var typedNil *fakeWorkflowBackend
	if _, err := newLegacyWorkflowBridge(typedNil, LegacyWorkflowBridgeOptions{}); err == nil {
		t.Fatal("typed-nil backend accepted")
	}
}

func TestLegacyWorkflowBridgeFailsClosedWhenProcessLocalSessionIsGone(t *testing.T) {
	backend := &fakeWorkflowBackend{
		result:   execution.AgentLaunchResult{SessionID: "session-1", Mailbox: "mailbox://agent/one", Handles: map[string]any{"session_urn": "agent://session/one"}},
		sessions: map[string]bridgeSession{},
	}
	bridge, _ := newLegacyWorkflowBridge(backend, LegacyWorkflowBridgeOptions{})
	launched, err := bridge.LaunchSession(t.Context(), bridgeRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	// The compatibility launcher cannot reattach after its process-local
	// manager is lost; it returns a safe operational error instead of claiming
	// the generic SessionHost restart guarantee.
	if _, err := bridge.ObserveSession(t.Context(), launched.Ref); err == nil || !strings.Contains(err.Error(), "unavailable after recovery") {
		t.Fatalf("Observe(missing process-local session) = %v", err)
	}
}

func TestLegacyWorkflowBridgeConcurrentReplayWaitObservesCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	backend := &fakeWorkflowBackend{
		result:   execution.AgentLaunchResult{SessionID: "session-1", Mailbox: "mailbox://agent/one", Handles: map[string]any{"session_urn": "agent://session/one"}},
		sessions: map[string]bridgeSession{},
		launchHook: func(context.Context) error {
			close(started)
			<-release
			return nil
		},
	}
	bridge, _ := newLegacyWorkflowBridge(backend, LegacyWorkflowBridgeOptions{})
	request := bridgeRequest(t)
	firstDone := make(chan error, 1)
	go func() {
		_, err := bridge.LaunchSession(context.Background(), request)
		firstDone <- err
	}()
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := bridge.LaunchSession(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled replay waiter = %v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if len(backend.launches) != 1 {
		t.Fatalf("legacy launches = %d", len(backend.launches))
	}
}

func TestLegacyWorkflowBridgeLateCancellationJournalsSuccessfulLegacyLaunch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	backend := &fakeWorkflowBackend{
		result:   execution.AgentLaunchResult{SessionID: "session-1", Mailbox: "mailbox://agent/one", Handles: map[string]any{"session_urn": "agent://session/one"}},
		sessions: map[string]bridgeSession{},
		launchHook: func(context.Context) error {
			cancel()
			return nil
		},
	}
	bridge, _ := newLegacyWorkflowBridge(backend, LegacyWorkflowBridgeOptions{})
	request := bridgeRequest(t)
	if _, err := bridge.LaunchSession(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("late-canceled launch = %v", err)
	}
	replayed, err := bridge.LaunchSession(t.Context(), request)
	if err != nil || replayed.Outcome != agentadapter.LaunchReplayed || len(backend.launches) != 1 {
		t.Fatalf("retry = %#v, %v; launches=%d", replayed, err, len(backend.launches))
	}
}

func bridgeRequest(t *testing.T) agentadapter.LaunchRequest {
	t.Helper()
	return agentadapter.LaunchRequest{
		Identity:  agentadapter.LogicalIdentity{RunID: "child-run", NodeID: "session"},
		Substrate: "local", LaunchID: "main", LogicalAgentID: "worker", Prompt: "prompt",
		Correlation: "agent:parent:launch", Inputs: values.ValueSet{}, IdempotencyKey: "launch-key",
	}
}

func bridgeInline(t *testing.T, payload any) values.Value {
	t.Helper()
	value, err := values.NewInline(payload, values.Metadata{Producer: values.Producer{Kind: "test", Reference: "test/input"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
