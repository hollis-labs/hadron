//go:build tether_integration

package tetherint

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	tether "github.com/hollis-labs/go-tether-client"
	agentadapter "github.com/hollis-labs/go-workflow/adapters/agent"
	"github.com/hollis-labs/go-workflow/values"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/hadron/internal/tetherhost"
)

const runTimeout = 90 * time.Second

// assertVerdict checks the run output is exactly the fake agent's reply result.
func assertVerdict(t *testing.T, run inspection, start agentStart) {
	t.Helper()
	verdict, ok := run.output(t, "verdict").(map[string]any)
	if !ok {
		t.Fatalf("verdict = %#v", run.output(t, "verdict"))
	}
	pid, _ := verdict["agent_pid"].(json.Number)
	if len(verdict) != 2 || verdict["ok"] != true || pid.String() != strconv.Itoa(start.PID) {
		t.Fatalf("verdict = %#v, want {ok:true agent_pid:%d}", verdict, start.PID)
	}
}

// 1. The agent replies: the run succeeds with the reply's result, and the
// session is stopped once the result is accepted (stop_on_result).
func TestTetherAgentLaunchSucceedsWithReply(t *testing.T) {
	e := newIsoEnv(t)
	e.setAgentMode("reply")
	e.startMuxd()
	h := e.startHadrond()
	h.runWorkflow("int-success")
	start := e.waitAgentStarted(1)
	if start.Thread == "" {
		t.Fatal("fake agent found no thread_id in its prompt")
	}
	run := h.waitTerminal("int-success", runTimeout)
	if run.Run.Status != "succeeded" {
		t.Fatalf("run = %s\n%s", run.Run.Status, h.describe("int-success"))
	}
	assertVerdict(t, run, start)
	session := e.onlySession()
	if len(e.agentStarts()) != 1 {
		t.Fatalf("fake agent starts = %d", len(e.agentStarts()))
	}
	// stop_on_result: Hadron stops the session after accepting the result;
	// the fake agent would otherwise run forever.
	e.waitStopped(session.ID, start.PID)
}

// 2. The agent exits without replying: the step fails agent_no_result.
func TestTetherAgentExitWithoutReplyFailsNoResult(t *testing.T) {
	e := newIsoEnv(t)
	e.setAgentMode("exit")
	e.startMuxd()
	h := e.startHadrond()
	h.runWorkflow("int-no-result")
	e.waitAgentStarted(1)
	run := h.waitTerminal("int-no-result", runTimeout)
	status, failure := h.sessionStep("int-no-result")
	if run.Run.Status != "failed" || status != "failed" || failure != tetherhost.CodeNoResult {
		t.Fatalf("run = %s, session step = %s %s\n%s", run.Run.Status, status, failure, h.describe("int-no-result"))
	}
	session := e.onlySession()
	if session.State != "completed" || session.ExitCode == nil || *session.ExitCode != 0 {
		t.Fatalf("tether session = %s exit %v", session.State, session.ExitCode)
	}
}

// 3. hadrond restarts mid-session: runtime B observes the session runtime A
// launched; exactly one Tether session and one agent start exist.
func TestTetherAgentSurvivesHadronRestart(t *testing.T) {
	e := newIsoEnv(t)
	e.setAgentMode("trigger")
	e.startMuxd()
	first := e.startHadrond()
	first.runWorkflow("int-hadron-restart")
	start := e.waitAgentStarted(1)
	first.waitSessionPending("int-hadron-restart")
	first.stop()

	e.triggerReply()
	e.waitAgentEvent("replied")
	second := e.startHadrond()
	run := second.waitTerminal("int-hadron-restart", runTimeout)
	if run.Run.Status != "succeeded" {
		t.Fatalf("run after restart = %s\n%s", run.Run.Status, second.describe("int-hadron-restart"))
	}
	assertVerdict(t, run, start)
	e.onlySession()
	if starts := e.agentStarts(); len(starts) != 1 {
		t.Fatalf("restart relaunched the agent: starts=%#v", starts)
	}
}

// 4. muxd restarts while the session runs: muxd's restart sweep fails the
// session (exit -1) and the step fails agent_session_lost.
func TestTetherAgentMuxdRestartLosesSession(t *testing.T) {
	e := newIsoEnv(t)
	e.setAgentMode("block")
	m := e.startMuxd()
	h := e.startHadrond()
	h.runWorkflow("int-muxd-restart")
	e.waitAgentStarted(1)
	h.waitSessionPending("int-muxd-restart")
	session := e.onlySession()

	m.restart()
	swept := e.session(session.ID)
	if swept.State != "failed" || swept.ExitCode == nil || *swept.ExitCode != -1 {
		t.Fatalf("session after muxd restart = %s exit %v, want failed -1", swept.State, swept.ExitCode)
	}
	run := h.waitTerminal("int-muxd-restart", runTimeout)
	status, failure := h.sessionStep("int-muxd-restart")
	if run.Run.Status != "failed" || status != "failed" || failure != tetherhost.CodeSessionLost {
		t.Fatalf("run = %s, session step = %s %s\n%s", run.Run.Status, status, failure, h.describe("int-muxd-restart"))
	}
}

// 5. The agent replies, then muxd restarts before Hadron observes: the reply
// is durable and wins over the restart sweep's failed session.
func TestTetherAgentReplySurvivesMuxdRestart(t *testing.T) {
	e := newIsoEnv(t)
	e.setAgentMode("trigger")
	m := e.startMuxd()
	first := e.startHadrond()
	first.runWorkflow("int-reply-restart")
	start := e.waitAgentStarted(1)
	first.waitSessionPending("int-reply-restart")
	// Hadron is down while the agent replies and muxd restarts, so nothing
	// observes the reply before the sweep.
	first.stop()
	e.triggerReply()
	e.waitAgentEvent("replied")
	session := e.onlySession()
	m.restart()
	if swept := e.session(session.ID); swept.State != "failed" {
		t.Fatalf("session after muxd restart = %s, want failed", swept.State)
	}

	second := e.startHadrond()
	run := second.waitTerminal("int-reply-restart", runTimeout)
	if run.Run.Status != "succeeded" {
		t.Fatalf("run = %s\n%s", run.Run.Status, second.describe("int-reply-restart"))
	}
	assertVerdict(t, run, start)
}

// 6. Retried launches converge on one Tether session.
func TestTetherLaunchRetryGivesOneSession(t *testing.T) {
	t.Run("host replays a keyed launch", func(t *testing.T) {
		e := newIsoEnv(t)
		e.setAgentMode("reply")
		e.startMuxd()
		host, request := newDirectHost(t, e)
		first, err := host.LaunchSession(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		second, err := host.LaunchSession(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if first.Outcome != agentadapter.LaunchApplied || second.Outcome != agentadapter.LaunchReplayed || second.Ref.ID != first.Ref.ID {
			t.Fatalf("launches = %#v then %#v", first, second)
		}
		if session := e.onlySession(); session.ID != first.Ref.ID {
			t.Fatalf("tether session %s, host ref %s", session.ID, first.Ref.ID)
		}
		start := e.waitAgentStarted(1)
		assertHostObservesReply(t, host, first.Ref, start)
		if len(e.agentStarts()) != 1 {
			t.Fatalf("agent starts = %d", len(e.agentStarts()))
		}
	})

	t.Run("create response lost before the host saw it", func(t *testing.T) {
		e := newIsoEnv(t)
		e.setAgentMode("reply")
		e.startMuxd()
		host, request := newDirectHost(t, e)
		// Send the exact keyed create the host sends, as if its response had
		// been lost: the session exists in muxd but Hadron never learned its id.
		digest, err := request.Digest()
		if err != nil {
			t.Fatal(err)
		}
		created, err := e.tetherClient().CreateSessionWithInput(t.Context(), tether.LaunchRequest{
			Launch:         launchID,
			PromptAppend:   tetherhost.PromptAppend(request.Prompt, request.Correlation, tetherhost.ResultNonce(hostSecret(t), digest, request.Correlation)),
			IdempotencyKey: tetherhost.TetherKey(request),
		})
		if err != nil || created.Replayed {
			t.Fatalf("raw create = %#v, %v", created, err)
		}
		launched, err := host.LaunchSession(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if launched.Outcome != agentadapter.LaunchReplayed || launched.Ref.ID != created.ID {
			t.Fatalf("launch after lost create = %#v, want replay of %s", launched, created.ID)
		}
		e.onlySession()
		start := e.waitAgentStarted(1)
		assertHostObservesReply(t, host, launched.Ref, start)
	})

	t.Run("hadrond killed right after launch", func(t *testing.T) {
		// A SIGKILL as soon as the agent process starts lands after the Tether
		// launch and, depending on timing, before or after the step records it;
		// either way runtime B must converge on the one session.
		e := newIsoEnv(t)
		e.setAgentMode("trigger")
		e.startMuxd()
		first := e.startHadrond()
		first.runWorkflow("int-crash-after-launch")
		start := e.waitAgentStarted(1)
		first.d.kill()
		second := e.startHadrond()
		e.triggerReply()
		run := second.waitTerminal("int-crash-after-launch", runTimeout)
		if run.Run.Status != "succeeded" {
			t.Fatalf("run = %s\n%s", run.Run.Status, second.describe("int-crash-after-launch"))
		}
		assertVerdict(t, run, start)
		e.onlySession()
		if starts := e.agentStarts(); len(starts) != 1 {
			t.Fatalf("agent starts = %#v", starts)
		}
	})
}

var directHostSecret []byte

func hostSecret(t *testing.T) []byte {
	t.Helper()
	if directHostSecret == nil {
		directHostSecret = make([]byte, 32)
		if _, err := rand.Read(directHostSecret); err != nil {
			t.Fatal(err)
		}
	}
	return directHostSecret
}

// newDirectHost builds Hadron's production session host (tetherhost.Host
// over the go-tether-client adapter) against the test muxd.
func newDirectHost(t *testing.T, e *isoEnv) (*tetherhost.Host, agentadapter.LaunchRequest) {
	t.Helper()
	e.guardPath(e.socket)
	client, err := tetherhost.NewTetherClient(e.socket)
	if err != nil {
		t.Fatal(err)
	}
	host, err := tetherhost.New(tetherhost.Options{
		Client: client, Secret: hostSecret(t),
		Substrates: map[string]settings.AgentSubstrateSettings{
			substrateName: {Kind: settings.AgentSubstrateKindTetherSession, Tether: &settings.TetherSubstrateSettings{Endpoint: e.socket, Launch: launchID}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := agentadapter.LaunchRequest{
		Identity:  agentadapter.LogicalIdentity{RunID: "direct-run", NodeID: "session"},
		Substrate: substrateName, LaunchID: "direct", LogicalAgentID: "reviewer",
		Prompt: "Review the change.", Correlation: "agent:direct-run:session", IdempotencyKey: "direct-launch-key",
		Inputs: values.ValueSet{},
	}
	return host, request
}

func assertHostObservesReply(t *testing.T, host *tetherhost.Host, ref agentadapter.SessionRef, start agentStart) {
	t.Helper()
	var observed agentadapter.SessionObservation
	eventually(t, 60*time.Second, "host observes the reply", func() bool {
		var err error
		observed, err = host.ObserveSession(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		return observed.State != agentadapter.SessionPending
	})
	if observed.State != agentadapter.SessionSucceeded || observed.Result == nil {
		t.Fatalf("observation = %#v", observed)
	}
	result, ok := observed.Result.Inline.(map[string]any)
	pid, _ := result["agent_pid"].(json.Number)
	if !ok || result["ok"] != true || pid.String() != strconv.Itoa(start.PID) {
		t.Fatalf("result = %#v", observed.Result.Inline)
	}
}

// 7. Canceling the run stops the Tether session.
func TestTetherAgentRunCancelStopsSession(t *testing.T) {
	knownGap(t, "Tether records a stopped session as completed with the kill's exit code, never killed; "+
		"the host observes the canceled step as agent_no_result and hadrond's cancellation recovery fails on every pass (workflow host not ready)")
	e := newIsoEnv(t)
	e.setAgentMode("block")
	e.startMuxd()
	h := e.startHadrond()
	h.runWorkflow("int-cancel")
	start := e.waitAgentStarted(1)
	h.waitSessionPending("int-cancel")
	session := e.onlySession()
	h.cancel("int-cancel")
	run := h.waitTerminal("int-cancel", runTimeout)
	if run.Run.Status != "canceled" {
		t.Fatalf("run = %s\n%s", run.Run.Status, h.describe("int-cancel"))
	}
	if status, _ := h.sessionStep("int-cancel"); status != "canceled" {
		t.Fatalf("session step = %s\n%s", status, h.describe("int-cancel"))
	}
	// The session is stopped: its process is gone and muxd has it terminal
	// (as "completed"; see waitStopped for why never "killed").
	stopped := e.waitStopped(session.ID, start.PID)
	if health, err := e.tetherClient().SessionHealth(context.Background(), session.ID); err == nil && health.Alive {
		t.Fatalf("session still alive after cancel: %#v (state %s)", health, stopped.State)
	}
}

// 8. muxd is down when the step launches: the step pends without failing,
// then launches and completes once muxd is up.
func TestTetherAgentPendsUntilMuxdStarts(t *testing.T) {
	knownGap(t, "a launch while muxd is unreachable fails the step agent_launch_failed after the host's 3 launch attempts; "+
		"hadrond does not retry failed step attempts")
	e := newIsoEnv(t)
	e.setAgentMode("reply")
	h := e.startHadrond()
	h.runWorkflow("int-muxd-down")
	never(t, 8*time.Second, "run finished while muxd was down", func() bool {
		inspected, err := h.inspect("int-muxd-down")
		if err != nil || !terminal(inspected.Run.Status) {
			return false
		}
		t.Logf("while muxd was down:\n%s", h.describe("int-muxd-down"))
		return true
	})
	e.startMuxd()
	start := e.waitAgentStarted(1)
	run := h.waitTerminal("int-muxd-down", runTimeout)
	if run.Run.Status != "succeeded" {
		t.Fatalf("run = %s\n%s", run.Run.Status, h.describe("int-muxd-down"))
	}
	assertVerdict(t, run, start)
	e.onlySession()
}
