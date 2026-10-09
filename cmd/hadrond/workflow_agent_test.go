package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	agentadapter "github.com/hollis-labs/libs/workflow/adapters/agent"
	"github.com/hollis-labs/libs/workflow/graph"
	workflowruntime "github.com/hollis-labs/libs/workflow/runtime"
	"github.com/hollis-labs/libs/workflow/values"
	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/config"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/hadron/internal/tetherhost"
	"github.com/hollis-labs/hadron/internal/tetherhost/tethertest"
)

const tetherAgentReviewSource = `workflow: {id: agent-review, version: v1}
steps:
  - id: review
    agent_launch:
      substrate: tether
      logical_agent_id: reviewer
      prompt_append: Review the change.
      wait: {timeout: 1h}
outputs:
  verdict:
    type: object
    value: steps.review.outputs.payload.result
`

func tetherTestSettings(mutate func(*settings.TetherSubstrateSettings)) *settings.Settings {
	sett := testWorkflowSettings()
	tether := &settings.TetherSubstrateSettings{Launch: "claude-default", Launches: map[string]string{"reviewer": "claude-review"}}
	if mutate != nil {
		mutate(tether)
	}
	sett.AgentSubstrates["tether"] = settings.AgentSubstrateSettings{Kind: settings.AgentSubstrateKindTetherSession, Tether: tether}
	return sett
}

type tetherRuntimeFixture struct {
	runtime *productionWorkflowRuntime
	cfg     *config.Config
	store   *persistence.Store
	state   *persistence.WorkflowStateStore
	fake    *tethertest.Fake
	sett    *settings.Settings
}

func newTetherRuntimeFixture(t *testing.T, sett *settings.Settings, sources map[string]string) *tetherRuntimeFixture {
	t.Helper()
	fake := tethertest.New()
	runtime, cfg, store := newTestProductionWorkflowRuntimeWithOptions(t, sett, withTetherClient(fake))
	if runtime.agentSessions == nil {
		t.Fatal("agent_session@v1 stayed gated with a client and a tether_session substrate")
	}
	for name, source := range sources {
		if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), name+".workflow.yaml"), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	return &tetherRuntimeFixture{runtime: runtime, cfg: cfg, store: store, state: state, fake: fake, sett: sett}
}

func (f *tetherRuntimeFixture) start(t *testing.T, name string, runID workflowruntime.RunID) workflowruntime.RunID {
	t.Helper()
	return startTetherRun(t, f.runtime, name, runID)
}

func startTetherRun(t *testing.T, runtime *productionWorkflowRuntime, name string, runID workflowruntime.RunID) workflowruntime.RunID {
	t.Helper()
	ctx := productionLocalContext(t)
	definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: name, Locator: name + ".workflow.yaml", Version: "v1"}
	started, err := runtime.operations.RunWorkflow(ctx, appworkflow.RunWorkflowRequest{
		RunID: runID, IdempotencyKey: string(runID), Definition: definition, Confirmed: true,
		Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	})
	if err != nil || started.Run == nil {
		validated, validationErr := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: definition})
		t.Fatalf("RunWorkflow = %#v, %v; validation=%#v, %v", started, err, validated.Diagnostics, validationErr)
	}
	return started.Run.ID
}

// waitForSession returns the single Tether session once the step launched it
// and it is running.
func waitForSession(t *testing.T, fake *tethertest.Fake) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if ids := fake.SessionIDs(); len(ids) > 0 && fake.State(ids[0]) == tetherhost.StateRunning {
			return ids[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no running Tether session; creates=%d launches=%d", fake.Creates(), fake.Launches())
	return ""
}

var (
	contractThread = regexp.MustCompile(`(?m)^  thread_id: (.+)$`)
	contractNonce  = regexp.MustCompile(`"nonce": "([0-9a-f]{64})"`)
)

// replyAsAgent does what the launched agent does: read the result contract
// from its prompt and reply on the thread with the nonce.
func replyAsAgent(t *testing.T, fake *tethertest.Fake, sessionID, result string) {
	t.Helper()
	request, ok := fake.Request(sessionID)
	if !ok {
		t.Fatalf("session %s has no create request", sessionID)
	}
	thread := contractThread.FindStringSubmatch(request.PromptAppend)
	nonce := contractNonce.FindStringSubmatch(request.PromptAppend)
	if thread == nil || nonce == nil {
		t.Fatalf("prompt has no result contract:\n%s", request.PromptAppend)
	}
	fake.PostReply(thread[1], tetherhost.Reply{
		Kind: tetherhost.ReplyKindResponse, From: "agent://reviewer", To: tetherhost.ResultMailbox,
		Payload: json.RawMessage(`{"nonce":"` + nonce[1] + `","result":` + result + `}`),
	})
}

func waitForTerminalRun(t *testing.T, state *persistence.WorkflowStateStore, runID workflowruntime.RunID) workflowruntime.RunSnapshot {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for {
		run, err := state.LoadRun(t.Context(), runID)
		if err == nil && run.Status.Terminal() {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not finish: %#v, %v; %s", runID, run, err, describeTetherRun(t, state, runID))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func describeTetherRun(t *testing.T, state *persistence.WorkflowStateStore, runID workflowruntime.RunID) string {
	t.Helper()
	var report strings.Builder
	report.WriteString(describeProductionRun(t, state, runID))
	for _, nodeID := range []string{"review", "review-launch", "session"} {
		node, err := state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: runID, NodeID: nodeID})
		if err != nil {
			continue
		}
		report.WriteString("\n" + string(runID) + "/" + nodeID + " status=" + string(node.Status))
		if node.LatestAttempt > 0 {
			attempt, attemptErr := state.LoadAttempt(t.Context(), workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
			if attemptErr == nil && attempt.Failure != nil {
				report.WriteString(" failure=" + attempt.Failure.Code + ": " + attempt.Failure.Message)
			}
		}
	}
	children, _ := state.ListChildRuns(t.Context(), runID)
	for _, child := range children {
		report.WriteString(describeTetherRun(t, state, child.ChildRunID))
	}
	return report.String()
}

// sessionFailure returns the session step's latest failure code in the
// run's agent child.
func sessionFailure(t *testing.T, state *persistence.WorkflowStateStore, runID workflowruntime.RunID) (workflowruntime.NodeStatus, string) {
	t.Helper()
	children, err := state.ListChildRuns(t.Context(), runID)
	if err != nil || len(children) != 1 {
		t.Fatalf("child runs = %#v, %v", children, err)
	}
	node, err := state.LoadNodeInvocation(t.Context(), workflowruntime.NodeInvocationID{RunID: children[0].ChildRunID, NodeID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := state.LoadAttempt(t.Context(), workflowruntime.AttemptID{Invocation: node.ID, Number: node.LatestAttempt})
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Failure == nil {
		return node.Status, ""
	}
	return node.Status, attempt.Failure.Code
}

func runOutputs(t *testing.T, state *persistence.WorkflowStateStore, run workflowruntime.RunSnapshot) map[string]any {
	t.Helper()
	if run.Outputs == nil {
		t.Fatalf("run %s has no outputs", run.ID)
	}
	outputs, err := state.LoadValues(t.Context(), *run.Outputs)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]any, len(outputs))
	for name, value := range outputs {
		result[name] = value.Inline
	}
	return result
}

func TestTetherAgentLaunchDeliversReplyResult(t *testing.T) {
	f := newTetherRuntimeFixture(t, tetherTestSettings(nil), map[string]string{"agent-review": tetherAgentReviewSource})
	runID := f.start(t, "agent-review", "agent-review-one")
	session := waitForSession(t, f.fake)
	request, _ := f.fake.Request(session)
	if request.Launch != "claude-review" || !strings.HasPrefix(request.PromptAppend, "Review the change.\n\n----- BEGIN HADRON RESULT CONTRACT -----") ||
		!strings.HasPrefix(request.IdempotencyKey, "hadron/") {
		t.Fatalf("create request = %#v", request)
	}
	replyAsAgent(t, f.fake, session, `{"verdict":"approve"}`)
	run := waitForTerminalRun(t, f.state, runID)
	if run.Status != workflowruntime.RunSucceeded {
		t.Fatalf("run = %s; %s", run.Status, describeTetherRun(t, f.state, runID))
	}
	verdict, ok := runOutputs(t, f.state, run)["verdict"].(map[string]any)
	if !ok || verdict["verdict"] != "approve" {
		t.Fatalf("outputs = %#v", runOutputs(t, f.state, run))
	}
	if f.fake.Creates() != 1 || f.fake.Stops() != 1 || f.fake.State(session) != tetherhost.StateKilled {
		t.Fatalf("creates=%d stops=%d state=%s", f.fake.Creates(), f.fake.Stops(), f.fake.State(session))
	}
}

func TestTetherAgentCompletedWithoutReplyFailsNoResult(t *testing.T) {
	f := newTetherRuntimeFixture(t, tetherTestSettings(nil), map[string]string{"agent-review": tetherAgentReviewSource})
	runID := f.start(t, "agent-review", "agent-review-no-result")
	session := waitForSession(t, f.fake)
	f.fake.Complete(session)
	run := waitForTerminalRun(t, f.state, runID)
	if run.Status != workflowruntime.RunFailed {
		t.Fatalf("run = %s; %s", run.Status, describeTetherRun(t, f.state, runID))
	}
	if status, code := sessionFailure(t, f.state, runID); status != workflowruntime.NodeFailed || code != tetherhost.CodeNoResult {
		t.Fatalf("session step = %s %s", status, code)
	}
}

func TestTetherAgentSurvivesHadronRestartMidSession(t *testing.T) {
	sett := tetherTestSettings(nil)
	first := newTetherRuntimeFixture(t, sett, map[string]string{"agent-review": tetherAgentReviewSource})
	runID := first.start(t, "agent-review", "agent-review-restart")
	session := waitForSession(t, first.fake)
	// Let the external operation persist before stopping runtime A.
	waitForExternalOperation(t, first.state)
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := first.runtime.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}

	replyAsAgent(t, first.fake, session, `{"verdict":"after-restart"}`)
	second, err := newProductionWorkflowRuntime(first.store, first.cfg, sett, withTetherClient(first.fake))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Shutdown(context.Background()) })
	run := waitForTerminalRun(t, first.state, runID)
	if run.Status != workflowruntime.RunSucceeded {
		t.Fatalf("run after restart = %s; %s", run.Status, describeTetherRun(t, first.state, runID))
	}
	if verdict, _ := runOutputs(t, first.state, run)["verdict"].(map[string]any); verdict["verdict"] != "after-restart" {
		t.Fatalf("outputs = %#v", runOutputs(t, first.state, run))
	}
	if first.fake.Creates() != 1 || first.fake.Launches() != 1 {
		t.Fatalf("restart relaunched: creates=%d launches=%d", first.fake.Creates(), first.fake.Launches())
	}
}

func waitForExternalOperation(t *testing.T, state *persistence.WorkflowStateStore) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		operations, err := state.RecoverExternalOperations(t.Context(), workflowruntime.ExternalOperationQuery{Limit: 10})
		if err == nil && len(operations) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no pending external operation was persisted")
}

func TestTetherAgentRestartSweepLosesSessionUnlessReplied(t *testing.T) {
	t.Run("sweep without reply", func(t *testing.T) {
		f := newTetherRuntimeFixture(t, tetherTestSettings(nil), map[string]string{"agent-review": tetherAgentReviewSource})
		runID := f.start(t, "agent-review", "agent-review-swept")
		waitForSession(t, f.fake)
		f.fake.RestartSweep()
		run := waitForTerminalRun(t, f.state, runID)
		if status, code := sessionFailure(t, f.state, runID); run.Status != workflowruntime.RunFailed || status != workflowruntime.NodeFailed || code != tetherhost.CodeSessionLost {
			t.Fatalf("run = %s, session step = %s %s", run.Status, status, code)
		}
	})
	t.Run("reply then sweep", func(t *testing.T) {
		f := newTetherRuntimeFixture(t, tetherTestSettings(nil), map[string]string{"agent-review": tetherAgentReviewSource})
		runID := f.start(t, "agent-review", "agent-review-replied-swept")
		session := waitForSession(t, f.fake)
		replyAsAgent(t, f.fake, session, `{"verdict":"kept"}`)
		f.fake.RestartSweep()
		run := waitForTerminalRun(t, f.state, runID)
		if run.Status != workflowruntime.RunSucceeded {
			t.Fatalf("run = %s; %s", run.Status, describeTetherRun(t, f.state, runID))
		}
	})
}

func TestTetherAgentRunCancelStopsSession(t *testing.T) {
	f := newTetherRuntimeFixture(t, tetherTestSettings(nil), map[string]string{"agent-review": tetherAgentReviewSource})
	runID := f.start(t, "agent-review", "agent-review-cancel")
	session := waitForSession(t, f.fake)
	waitForExternalOperation(t, f.state)
	if _, err := f.runtime.operations.CancelWorkflowRun(productionLocalContext(t), appworkflow.CancelWorkflowRunRequest{
		RunID: runID, IdempotencyKey: "cancel-agent-review", Reason: "operator", Identity: appworkflow.IdentityRequest{SourceAuthority: "http"},
	}); err != nil {
		t.Fatal(err)
	}
	run := waitForTerminalRun(t, f.state, runID)
	if run.Status != workflowruntime.RunCanceled {
		t.Fatalf("run = %s; %s", run.Status, describeTetherRun(t, f.state, runID))
	}
	deadline := time.Now().Add(30 * time.Second)
	for f.fake.State(session) != tetherhost.StateKilled {
		if time.Now().After(deadline) {
			t.Fatalf("session state after cancel = %s; %s", f.fake.State(session), describeTetherRun(t, f.state, runID))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestTetherAgentHumanStopCancelsStep(t *testing.T) {
	f := newTetherRuntimeFixture(t, tetherTestSettings(nil), map[string]string{"agent-review": tetherAgentReviewSource})
	runID := f.start(t, "agent-review", "agent-review-human-stop")
	session := waitForSession(t, f.fake)
	f.fake.Kill(session)
	run := waitForTerminalRun(t, f.state, runID)
	if status, code := sessionFailure(t, f.state, runID); run.Status == workflowruntime.RunSucceeded || status != workflowruntime.NodeCanceled || code != tetherhost.CodeSessionCanceled {
		t.Fatalf("run = %s, session step = %s %s", run.Status, status, code)
	}
}

func TestTetherAgentUnavailableDaemonPendsThenCompletes(t *testing.T) {
	f := newTetherRuntimeFixture(t, tetherTestSettings(nil), map[string]string{"agent-review": tetherAgentReviewSource})
	runID := f.start(t, "agent-review", "agent-review-unavailable")
	session := waitForSession(t, f.fake)
	waitForExternalOperation(t, f.state)
	f.fake.SetUnavailable(true)
	// The agent still reaches muxd; only Hadron's path is down.
	replyAsAgent(t, f.fake, session, `{"verdict":"late"}`)
	// Two reconcile passes while muxd is unreachable leave the step pending.
	time.Sleep(2*workflowExternalReconcileInterval + 500*time.Millisecond)
	if run, err := f.state.LoadRun(t.Context(), runID); err != nil || run.Status.Terminal() {
		t.Fatalf("run closed while muxd was unreachable: %#v, %v", run, err)
	}
	f.fake.SetUnavailable(false)
	run := waitForTerminalRun(t, f.state, runID)
	if run.Status != workflowruntime.RunSucceeded {
		t.Fatalf("run = %s; %s", run.Status, describeTetherRun(t, f.state, runID))
	}
}

// hadrond does not retry failed step attempts (the production Host has no
// retry coordinator), so the session host replays the keyed create itself.
func TestTetherAgentLostCreateResponseReplaysOneSession(t *testing.T) {
	f := newTetherRuntimeFixture(t, tetherTestSettings(nil), map[string]string{"agent-review": tetherAgentReviewSource})
	f.fake.LoseCreateResponses(1)
	runID := f.start(t, "agent-review", "agent-review-lost-create")
	session := waitForSession(t, f.fake)
	replyAsAgent(t, f.fake, session, `{"verdict":"once"}`)
	run := waitForTerminalRun(t, f.state, runID)
	if run.Status != workflowruntime.RunSucceeded {
		t.Fatalf("run = %s; %s", run.Status, describeTetherRun(t, f.state, runID))
	}
	if f.fake.Creates() != 1 || len(f.fake.SessionIDs()) != 1 || f.fake.Launches() != 1 {
		t.Fatalf("lost create response duplicated the session: creates=%d sessions=%v", f.fake.Creates(), f.fake.SessionIDs())
	}
}

func TestTetherAgentTypedInputsRefusedAtValidation(t *testing.T) {
	sources := map[string]string{
		"agent-typed-sugar": `workflow: {id: agent-typed-sugar, version: v1}
inputs:
  - name: diff
    type: string
    required: true
steps:
  - id: review
    agent_launch:
      substrate: tether
      logical_agent_id: reviewer
      wait: false
    with:
      diff: inputs.diff
`,
		"agent-typed-direct": `workflow: {id: agent-typed-direct, version: v1}
inputs:
  - name: diff
    type: string
    required: true
steps:
  - id: session
    kind: agent_session
    kind_version: v1
    config:
      substrate: tether
      launch_id: review
      logical_agent_id: reviewer
    with:
      diff: inputs.diff
`,
	}
	f := newTetherRuntimeFixture(t, tetherTestSettings(nil), sources)
	ctx := productionLocalContext(t)
	for name := range sources {
		t.Run(name, func(t *testing.T) {
			definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: name, Locator: name + ".workflow.yaml", Version: "v1"}
			validated, err := f.runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: definition})
			if err != nil || validated.Plan != nil || !diagnosticsMention(validated.Diagnostics, tetherhost.InputsUnsupportedMessage) {
				t.Fatalf("validation = %#v, %v", validated.Diagnostics, err)
			}
		})
	}
	if f.fake.Creates() != 0 {
		t.Fatal("validation reached Tether")
	}
}

func TestProductionAgentSessionGateAndEnabledCompositions(t *testing.T) {
	boundary := func(runtime *productionWorkflowRuntime) []appworkflow.KindRef {
		var got []appworkflow.KindRef
		for _, spec := range runtime.host.Registry().List() {
			got = append(got, appworkflow.KindRef{Name: spec.Name, Version: spec.Version})
		}
		sort.Slice(got, func(i, j int) bool {
			if got[i].Name == got[j].Name {
				return got[i].Version < got[j].Version
			}
			return got[i].Name < got[j].Name
		})
		return got
	}
	agentSession := appworkflow.KindRef{Name: agentadapter.KindName, Version: agentadapter.KindVersion}
	directSource := `workflow: {id: agent-direct, version: v1}
steps:
  - id: session
    kind: agent_session
    kind_version: v1
    config:
      substrate: tether
      launch_id: review
      logical_agent_id: reviewer
`
	// The go-tether-client adapter never dials at construction, so a socket
	// that does not exist must not stop hadrond from starting.
	downDaemon := tetherTestSettings(func(tether *settings.TetherSubstrateSettings) {
		tether.Endpoint = filepath.Join(t.TempDir(), "absent-muxd.sock")
	})
	for name, tc := range map[string]struct {
		sett    *settings.Settings
		options []productionWorkflowOption
		enabled bool
	}{
		"no tether substrate":                      {testWorkflowSettings(), nil, false},
		"injected client, no tether substrate":     {testWorkflowSettings(), []productionWorkflowOption{withTetherClient(tethertest.New())}, false},
		"tether substrate, go-tether-client, down": {downDaemon, nil, true},
		"tether substrate, injected client":        {tetherTestSettings(nil), []productionWorkflowOption{withTetherClient(tethertest.New())}, true},
	} {
		t.Run(name, func(t *testing.T) {
			runtime, cfg, _ := newTestProductionWorkflowRuntimeWithOptions(t, tc.sett, tc.options...)
			for source, text := range map[string]string{"agent-review": tetherAgentReviewSource, "agent-direct": directSource} {
				if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), source+".workflow.yaml"), []byte(text), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := runtime.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
			got := boundary(runtime)
			if want := productionWorkflowKindBoundary(tc.enabled); !slices.Equal(got, want) {
				t.Fatalf("production kinds = %#v, want %#v", got, want)
			}
			if slices.Contains(got, agentSession) != tc.enabled || (runtime.agentSessions != nil) != tc.enabled {
				t.Fatalf("agent_session@v1 registered = %v, want %v", slices.Contains(got, agentSession), tc.enabled)
			}
			ctx := productionLocalContext(t)
			for _, source := range []string{"agent-review", "agent-direct"} {
				definition := graph.DefinitionRef{Kind: appworkflow.DefinitionKindFile, ID: source, Locator: source + ".workflow.yaml", Version: "v1"}
				validated, err := runtime.operations.ValidateWorkflow(ctx, appworkflow.ValidateWorkflowRequest{Definition: definition})
				if err != nil {
					t.Fatal(err)
				}
				gated := diagnosticsMention(validated.Diagnostics, agentSessionGateMessage)
				if tc.enabled && (gated || validated.Plan == nil) {
					t.Fatalf("%s refused while enabled: %#v", source, validated.Diagnostics)
				}
				if !tc.enabled && (!gated || validated.Plan != nil) {
					t.Fatalf("%s accepted while gated: %#v", source, validated.Diagnostics)
				}
			}
			if tc.enabled && tc.options == nil {
				// The real adapter reports the absent daemon as unreachable.
				if _, launchErr := runtime.agentSessions.LaunchSession(t.Context(), agentadapter.LaunchRequest{
					Identity: agentadapter.LogicalIdentity{RunID: "r", NodeID: "n"}, Substrate: "tether", LaunchID: "l",
					LogicalAgentID: "reviewer", Correlation: "c", IdempotencyKey: "k", Inputs: values.ValueSet{},
				}); !errors.Is(launchErr, tetherhost.ErrUnavailable) {
					t.Fatalf("launch against an absent muxd = %v", launchErr)
				}
			}
			_, secretErr := os.Stat(filepath.Join(cfg.DataDir, tetherhost.SecretFileName))
			if (secretErr == nil) != tc.enabled {
				t.Fatalf("result secret presence = %v, want %v", secretErr == nil, tc.enabled)
			}
		})
	}
}

type tetherTestClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *tetherTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

func (c *tetherTestClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.offset += d
	c.mu.Unlock()
}

func TestTetherAgentUnreachableTimeoutFailsStep(t *testing.T) {
	sett := tetherTestSettings(func(tether *settings.TetherSubstrateSettings) { tether.UnreachableTimeout = "1m" })
	fake := tethertest.New()
	clock := &tetherTestClock{}
	runtime, cfg, store := newTestProductionWorkflowRuntimeWithOptions(t, sett, withTetherClient(fake), withTetherClock(clock.Now))
	if err := os.WriteFile(filepath.Join(workflowSourceRoot(cfg), "agent-review.workflow.yaml"), []byte(tetherAgentReviewSource), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	runID := startTetherRun(t, runtime, "agent-review", "agent-review-unreachable-timeout")
	waitForSession(t, fake)
	waitForExternalOperation(t, state)
	fake.SetUnavailable(true)
	// Let one reconcile pass start the unreachable timer, then pass the timeout.
	time.Sleep(workflowExternalReconcileInterval + 500*time.Millisecond)
	if run, loadErr := state.LoadRun(t.Context(), runID); loadErr != nil || run.Status.Terminal() {
		t.Fatalf("run closed before the unreachable timeout: %#v, %v", run, loadErr)
	}
	clock.Advance(2 * time.Minute)
	run := waitForTerminalRun(t, state, runID)
	if status, code := sessionFailure(t, state, runID); run.Status != workflowruntime.RunFailed || status != workflowruntime.NodeFailed || code != tetherhost.CodeHostUnreachable {
		t.Fatalf("run = %s, session step = %s %s", run.Status, status, code)
	}
}

func TestTetherSessionSubstratesMustShareOneEndpoint(t *testing.T) {
	substrates := map[string]settings.AgentSubstrateSettings{
		"a": {Kind: settings.AgentSubstrateKindTetherSession, Tether: &settings.TetherSubstrateSettings{Launch: "l"}},
		"b": {Kind: settings.AgentSubstrateKindTetherSession, Tether: &settings.TetherSubstrateSettings{Launch: "l", Endpoint: settings.DefaultTetherEndpoint}},
	}
	if endpoint, err := sharedTetherEndpoint(substrates); err != nil || endpoint != settings.DefaultTetherEndpoint {
		t.Fatalf("shared endpoint = %q, %v", endpoint, err)
	}
	substrates["c"] = settings.AgentSubstrateSettings{Kind: settings.AgentSubstrateKindTetherSession, Tether: &settings.TetherSubstrateSettings{Launch: "l", Endpoint: "/elsewhere/muxd.sock"}}
	if _, err := sharedTetherEndpoint(substrates); err == nil || !strings.Contains(err.Error(), "different muxd endpoints") {
		t.Fatalf("mixed endpoints = %v", err)
	}
	sett := tetherTestSettings(nil)
	sett.AgentSubstrates["second"] = settings.AgentSubstrateSettings{Kind: settings.AgentSubstrateKindTetherSession, Tether: &settings.TetherSubstrateSettings{Launch: "l", Endpoint: "/elsewhere/muxd.sock"}}
	if _, err := newTetherSessionHost(t.TempDir(), sett.AgentSubstrates, productionWorkflowOptions{}); err == nil {
		t.Fatal("hadrond composed one client for two muxd endpoints")
	}
}
