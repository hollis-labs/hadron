package tetherhost_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	agentadapter "github.com/hollis-labs/libs/workflow/adapters/agent"
	"github.com/hollis-labs/libs/workflow/stepkind"
	"github.com/hollis-labs/libs/workflow/values"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/hadron/internal/tetherhost"
	"github.com/hollis-labs/hadron/internal/tetherhost/tethertest"
)

var testSecret = bytes.Repeat([]byte{7}, 32)

func launchRequest() agentadapter.LaunchRequest {
	return agentadapter.LaunchRequest{
		Identity:  agentadapter.LogicalIdentity{RunID: "run-1", NodeID: "session"},
		Substrate: "tether", LaunchID: "review", LogicalAgentID: "reviewer",
		Prompt: "Review the diff.", Correlation: "agent:parent-1:review", IdempotencyKey: "idem-1", Inputs: values.ValueSet{},
	}
}

func tetherSubstrates(mutate func(*settings.TetherSubstrateSettings)) map[string]settings.AgentSubstrateSettings {
	tether := &settings.TetherSubstrateSettings{Launch: "default-launch", Launches: map[string]string{"reviewer": "review-launch"}}
	if mutate != nil {
		mutate(tether)
	}
	return map[string]settings.AgentSubstrateSettings{
		"tether": {Kind: settings.AgentSubstrateKindTetherSession, Tether: tether},
		"other":  {Kind: "go_agent_runtime", Provider: "p", Runtime: "r"},
	}
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type fixture struct {
	host  *tetherhost.Host
	fake  *tethertest.Fake
	clock *clock
	logs  *syncBuffer
	rec   *recordingClient
}

func newFixture(t *testing.T, mutate func(*settings.TetherSubstrateSettings)) *fixture {
	t.Helper()
	fake := tethertest.New()
	rec := &recordingClient{Fake: fake}
	clk := &clock{now: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)}
	logs := &syncBuffer{}
	host, err := tetherhost.New(tetherhost.Options{
		Client: rec, Substrates: tetherSubstrates(mutate), Secret: testSecret, Now: clk.Now,
		Logger:         slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		LaunchAttempts: 2, LaunchBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{host: host, fake: fake, clock: clk, logs: logs, rec: rec}
}

// recordingClient captures every CreateRequest the host sends.
type recordingClient struct {
	*tethertest.Fake
	mu      sync.Mutex
	creates []tetherhost.CreateRequest
}

func (r *recordingClient) CreateSession(ctx context.Context, request tetherhost.CreateRequest) (tetherhost.Session, error) {
	r.mu.Lock()
	r.creates = append(r.creates, request)
	r.mu.Unlock()
	return r.Fake.CreateSession(ctx, request)
}

func (f *fixture) launch(t *testing.T, request agentadapter.LaunchRequest) agentadapter.LaunchResult {
	t.Helper()
	result, err := f.host.LaunchSession(t.Context(), request)
	if err != nil {
		t.Fatalf("LaunchSession = %v", err)
	}
	if err := result.Validate(request); err != nil {
		t.Fatalf("LaunchResult does not validate against its request: %v", err)
	}
	return result
}

func nonceFor(t *testing.T, request agentadapter.LaunchRequest) string {
	t.Helper()
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return tetherhost.ResultNonce(testSecret, digest, request.Correlation)
}

func resultReply(nonce string, result string) tetherhost.Reply {
	return tetherhost.Reply{
		Kind: tetherhost.ReplyKindResponse, From: "agent://reviewer", To: tetherhost.ResultMailbox,
		Payload: json.RawMessage(`{"nonce":"` + nonce + `","result":` + result + `}`),
	}
}

func observe(t *testing.T, host *tetherhost.Host, ref agentadapter.SessionRef) agentadapter.SessionObservation {
	t.Helper()
	observation, err := host.ObserveSession(t.Context(), ref)
	if err != nil {
		t.Fatalf("ObserveSession = %v", err)
	}
	return observation
}

func TestTetherKeyFormat(t *testing.T) {
	request := launchRequest()
	key := tetherhost.TetherKey(request)
	if !strings.HasPrefix(key, "hadron/run-1/session/-/") {
		t.Fatalf("key = %q", key)
	}
	k := key[strings.LastIndex(key, "/")+1:]
	if len(k) != 16 || strings.Trim(k, "0123456789abcdef") != "" {
		t.Fatalf("key suffix = %q", k)
	}
	request.Identity.Iteration = "3"
	if got := tetherhost.TetherKey(request); got != "hadron/run-1/session/3/"+k {
		t.Fatalf("iteration key = %q", got)
	}
	request.IdempotencyKey = "idem-2"
	if got := tetherhost.TetherKey(request); strings.HasSuffix(got, k) {
		t.Fatalf("different idempotency keys share a Tether key suffix: %q", got)
	}
}

func TestTetherKeyLengthCap(t *testing.T) {
	request := launchRequest()
	request.Identity.RunID = strings.Repeat("r", 4000)
	request.Identity.NodeID = "node-" + strings.Repeat("n", 100)
	request.Identity.Iteration = strings.Repeat("é", 300)
	key := tetherhost.TetherKey(request)
	if len(key) > 512 {
		t.Fatalf("key length = %d", len(key))
	}
	short := launchRequest()
	suffix := tetherhost.TetherKey(short)
	suffix = suffix[strings.LastIndex(suffix, "/"):]
	if !strings.HasSuffix(key, suffix) {
		t.Fatalf("capped key lost its idempotency suffix: %q", key)
	}
	if !strings.Contains(key, "/node-"+strings.Repeat("n", 100)+"/") {
		t.Fatalf("short node component was truncated needlessly: %q", key)
	}
	if !json.Valid([]byte(`"` + key + `"`)) {
		t.Fatalf("capped key split a rune: %q", key)
	}
	if again := tetherhost.TetherKey(request); again != key {
		t.Fatal("TetherKey is not deterministic")
	}
}

func TestPromptAppendIsDeterministicAndNonceDistinct(t *testing.T) {
	first := newFixture(t, nil)
	request := launchRequest()
	first.launch(t, request)
	first.launch(t, request)
	second := newFixture(t, nil)
	second.launch(t, request)
	if len(first.rec.creates) != 2 || first.rec.creates[0] != first.rec.creates[1] || first.rec.creates[0] != second.rec.creates[0] {
		t.Fatalf("create requests differ across identical launches: %#v / %#v", first.rec.creates, second.rec.creates)
	}
	create := first.rec.creates[0]
	nonce := nonceFor(t, request)
	want := "Review the diff.\n\n----- BEGIN HADRON RESULT CONTRACT -----\n" +
		"When your task is complete, send exactly one message with the mux_message_send tool:\n" +
		"  kind: response\n  to: msg://agent/hadron/results\n  thread_id: agent:parent-1:review\n" +
		`  payload: {"nonce": "` + nonce + `", "result": <your result as a JSON value>}` + "\n" +
		"Do not send the nonce anywhere else. Hadron treats the first valid reply on this thread as your step result.\n" +
		"----- END HADRON RESULT CONTRACT -----\n"
	if create.PromptAppend != want || create.Launch != "review-launch" || create.IdempotencyKey != tetherhost.TetherKey(request) {
		t.Fatalf("create request = %#v", create)
	}
	if len(nonce) != 64 || nonce != nonceFor(t, request) {
		t.Fatalf("nonce = %q", nonce)
	}
	other := request
	other.IdempotencyKey = "idem-2"
	if nonceFor(t, other) == nonce {
		t.Fatal("nonce does not vary with the request")
	}
	otherCorrelation := request
	otherCorrelation.Correlation = "agent:parent-2:review"
	if nonceFor(t, otherCorrelation) == nonce {
		t.Fatal("nonce does not vary with the correlation")
	}
	if tetherhost.ResultNonce(bytes.Repeat([]byte{8}, 32), "sha256:x", "c") == tetherhost.ResultNonce(testSecret, "sha256:x", "c") {
		t.Fatal("nonce does not depend on the secret")
	}
	if got := tetherhost.PromptAppend("", "c", "n"); !strings.HasPrefix(got, "----- BEGIN HADRON RESULT CONTRACT -----\n") {
		t.Fatalf("prompt-less append = %q", got)
	}
}

func TestLoadOrCreateSecretCreatesOnceAndReuses(t *testing.T) {
	dir := t.TempDir()
	first, err := tetherhost.LoadOrCreateSecret(dir)
	if err != nil || len(first) != 32 {
		t.Fatalf("first secret = %x, %v", first, err)
	}
	info, err := os.Stat(filepath.Join(dir, tetherhost.SecretFileName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("secret file = %v, %v", info, err)
	}
	second, err := tetherhost.LoadOrCreateSecret(dir)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("reloaded secret differs: %v", err)
	}

	concurrent := t.TempDir()
	var wg sync.WaitGroup
	results := make([][]byte, 8)
	for index := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[index], _ = tetherhost.LoadOrCreateSecret(concurrent)
		}()
	}
	wg.Wait()
	for _, secret := range results {
		if len(secret) != 32 || !bytes.Equal(secret, results[0]) {
			t.Fatal("concurrent first opens published different secrets")
		}
	}
	entries, _ := os.ReadDir(concurrent)
	if len(entries) != 1 {
		t.Fatalf("secret directory holds %d entries, want 1", len(entries))
	}

	if err := os.Chmod(filepath.Join(dir, tetherhost.SecretFileName), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := tetherhost.LoadOrCreateSecret(dir); err == nil {
		t.Fatal("group-readable secret was accepted")
	}
	if _, err := tetherhost.LoadOrCreateSecret(""); err == nil {
		t.Fatal("empty data dir was accepted")
	}
}

func TestLaunchAppliedThenReplayed(t *testing.T) {
	f := newFixture(t, nil)
	request := launchRequest()
	first := f.launch(t, request)
	if first.Outcome != agentadapter.LaunchApplied {
		t.Fatalf("first outcome = %s", first.Outcome)
	}
	digest, _ := request.Digest()
	wantRef := agentadapter.SessionRef{ID: first.Ref.ID, Substrate: "tether", Correlation: request.Correlation, RequestDigest: digest}
	wantHandle := agentadapter.SessionHandle{
		SessionID: first.Ref.ID, SessionURI: "tether://local/sessions/" + first.Ref.ID, Mailbox: "msg://agent/hadron/results",
		Substrate: "tether", Correlation: request.Correlation,
	}
	if first.Ref != wantRef || first.Handle != wantHandle {
		t.Fatalf("launch result = %#v", first)
	}
	second := f.launch(t, request)
	if second.Outcome != agentadapter.LaunchReplayed || second.Ref != first.Ref {
		t.Fatalf("replay = %#v", second)
	}
	if f.fake.Creates() != 1 || f.fake.Launches() != 1 || f.fake.State(first.Ref.ID) != tetherhost.StateRunning {
		t.Fatalf("creates=%d launches=%d state=%s", f.fake.Creates(), f.fake.Launches(), f.fake.State(first.Ref.ID))
	}

	// A create whose response was lost replays within the same call.
	lost := newFixture(t, nil)
	lost.fake.LoseCreateResponses(1)
	retried := lost.launch(t, request)
	if retried.Outcome != agentadapter.LaunchReplayed || lost.fake.Creates() != 1 || lost.fake.Launches() != 1 {
		t.Fatalf("launch after lost create = %#v creates=%d", retried, lost.fake.Creates())
	}
	// Losing every attempt's response fails retryably; the next runtime
	// attempt (same key) replays the one session.
	exhausted := newFixture(t, nil)
	exhausted.fake.LoseCreateResponses(2)
	if _, err := exhausted.host.LaunchSession(t.Context(), request); !errors.Is(err, tetherhost.ErrUnavailable) || stepkind.ClassifyError(err) != stepkind.Retryable {
		t.Fatalf("exhausted lost create = %v", err)
	}
	if again := exhausted.launch(t, request); again.Outcome != agentadapter.LaunchReplayed || exhausted.fake.Creates() != 1 {
		t.Fatalf("replay after exhausted attempts = %#v creates=%d", again, exhausted.fake.Creates())
	}
}

func TestLaunchConflictIsAgentLaunchConflict(t *testing.T) {
	f := newFixture(t, nil)
	request := launchRequest()
	f.launch(t, request)
	// Same logical launch (same Tether key), different prompt.
	changed := request
	changed.Prompt = "Something else."
	_, err := f.host.LaunchSession(t.Context(), changed)
	if !errors.Is(err, agentadapter.ErrLaunchConflict) || !errors.Is(err, tetherhost.ErrIdempotencyConflict) {
		t.Fatalf("conflict = %v", err)
	}
}

func TestLaunchPermanentRefusals(t *testing.T) {
	f := newFixture(t, func(tether *settings.TetherSubstrateSettings) { tether.Launch = "" })
	for name, tc := range map[string]struct {
		mutate func(*agentadapter.LaunchRequest)
		code   string
	}{
		"unknown logical agent": {func(r *agentadapter.LaunchRequest) { r.LogicalAgentID = "stranger" }, tetherhost.CodeUnknownLogicalAgent},
		"unknown substrate":     {func(r *agentadapter.LaunchRequest) { r.Substrate = "missing" }, tetherhost.CodeUnknownSubstrate},
		"non-tether substrate":  {func(r *agentadapter.LaunchRequest) { r.Substrate = "other" }, tetherhost.CodeUnknownSubstrate},
		"typed inputs": {func(r *agentadapter.LaunchRequest) {
			value, _ := values.NewInline("x", values.Metadata{Producer: values.Producer{Kind: "test", Reference: "t"}, MediaType: "application/json", Redaction: values.RedactionPrivate, Retention: values.RetentionRun})
			r.Inputs = values.ValueSet{"diff": value}
		}, tetherhost.CodeInputsUnsupported},
	} {
		t.Run(name, func(t *testing.T) {
			request := launchRequest()
			tc.mutate(&request)
			_, err := f.host.LaunchSession(t.Context(), request)
			var execution *stepkind.ExecutionError
			if !errors.As(err, &execution) || execution.Code != tc.code || stepkind.ClassifyError(err) != stepkind.RetryPermanent {
				t.Fatalf("LaunchSession = %v", err)
			}
		})
	}
	if f.fake.Creates() != 0 {
		t.Fatal("a refused launch reached Tether")
	}
	// The launches map resolves a mapped logical agent without a default.
	result := f.launch(t, launchRequest())
	if request, _ := f.fake.Request(result.Ref.ID); request.Launch != "review-launch" {
		t.Fatalf("mapped launch = %q", request.Launch)
	}
	// The default launch serves unmapped agents.
	withDefault := newFixture(t, nil)
	unmapped := launchRequest()
	unmapped.LogicalAgentID = "writer"
	result = withDefault.launch(t, unmapped)
	if request, _ := withDefault.fake.Request(result.Ref.ID); request.Launch != "default-launch" {
		t.Fatalf("default launch = %q", request.Launch)
	}
}

func TestLaunchUnavailableIsRetryable(t *testing.T) {
	f := newFixture(t, nil)
	f.fake.SetUnavailable(true)
	_, err := f.host.LaunchSession(t.Context(), launchRequest())
	var execution *stepkind.ExecutionError
	if !errors.As(err, &execution) || execution.Code != tetherhost.CodeHostUnreachable || stepkind.ClassifyError(err) != stepkind.Retryable {
		t.Fatalf("unavailable launch = %v", err)
	}
}

func TestObserveSessionStateMappings(t *testing.T) {
	for name, tc := range map[string]struct {
		setup    func(f *tethertest.Fake, id string)
		state    agentadapter.SessionState
		code     string
		progress map[string]string
	}{
		"running":           {func(*tethertest.Fake, string) {}, agentadapter.SessionPending, "", map[string]string{"state": "running", "alive": "true"}},
		"completed":         {func(f *tethertest.Fake, id string) { f.Complete(id) }, agentadapter.SessionFailed, tetherhost.CodeNoResult, nil},
		"killed":            {func(f *tethertest.Fake, id string) { f.Kill(id) }, agentadapter.SessionCanceled, tetherhost.CodeSessionCanceled, nil},
		"failed swept":      {func(f *tethertest.Fake, _ string) { f.RestartSweep() }, agentadapter.SessionFailed, tetherhost.CodeSessionLost, nil},
		"failed exit":       {func(f *tethertest.Fake, id string) { f.Fail(id, 2) }, agentadapter.SessionFailed, tetherhost.CodeSessionFailed, nil},
		"missing (unknown)": {nil, agentadapter.SessionFailed, tetherhost.CodeSessionMissing, nil},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil)
			launched := f.launch(t, launchRequest())
			ref := launched.Ref
			if tc.setup == nil {
				ref.ID = "sess-unknown"
			} else {
				tc.setup(f.fake, ref.ID)
			}
			observation := observe(t, f.host, ref)
			if observation.State != tc.state {
				t.Fatalf("state = %s, want %s (%#v)", observation.State, tc.state, observation)
			}
			if tc.code != "" && (observation.Failure == nil || observation.Failure.Code != tc.code || observation.Failure.Retryable) {
				t.Fatalf("failure = %#v", observation.Failure)
			}
			if tc.progress != nil && (observation.Progress["state"] != tc.progress["state"] || observation.Progress["alive"] != tc.progress["alive"]) {
				t.Fatalf("progress = %#v", observation.Progress)
			}
			if observation.Result != nil {
				t.Fatalf("non-success observation carried a result: %#v", observation)
			}
		})
	}
}

func TestObserveAcceptsFirstValidReply(t *testing.T) {
	f := newFixture(t, nil)
	request := launchRequest()
	launched := f.launch(t, request)
	nonce := nonceFor(t, request)
	f.fake.PostReply(request.Correlation, resultReply(nonce, `{"verdict":"approve","score":0.75}`))
	f.fake.PostReply(request.Correlation, resultReply(nonce, `"second"`))
	observation := observe(t, f.host, launched.Ref)
	if observation.State != agentadapter.SessionSucceeded || observation.Handle != launched.Handle || observation.Result == nil {
		t.Fatalf("observation = %#v", observation)
	}
	object, ok := observation.Result.Inline.(map[string]any)
	if !ok || object["verdict"] != "approve" || observation.Result.Type != values.TypeObject {
		t.Fatalf("result = %#v", observation.Result)
	}
	if err := observation.Result.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestObserveIgnoresForgedAndInvalidReplies(t *testing.T) {
	f := newFixture(t, nil)
	request := launchRequest()
	launched := f.launch(t, request)
	nonce := nonceFor(t, request)
	otherRequest := request
	otherRequest.IdempotencyKey = "someone-else"
	forged := []tetherhost.Reply{
		{MessageID: "wrong-nonce", Kind: "response", From: "agent://reviewer", To: tetherhost.ResultMailbox, Payload: json.RawMessage(`{"nonce":"deadbeef","result":1}`)},
		{MessageID: "other-sender", Kind: "response", From: "agent://intruder", To: tetherhost.ResultMailbox, Payload: json.RawMessage(`{"nonce":"` + nonceFor(t, otherRequest) + `","result":1}`)},
		{MessageID: "wrong-kind", Kind: "request", From: "agent://reviewer", To: tetherhost.ResultMailbox, Payload: json.RawMessage(`{"nonce":"` + nonce + `","result":1}`)},
		{MessageID: "wrong-to", Kind: "response", From: "agent://reviewer", To: "msg://agent/elsewhere", Payload: json.RawMessage(`{"nonce":"` + nonce + `","result":1}`)},
		{MessageID: "bad-json", Kind: "response", From: "agent://reviewer", To: tetherhost.ResultMailbox, Payload: json.RawMessage(`{"nonce":`)},
		{MessageID: "no-result", Kind: "response", From: "agent://reviewer", To: tetherhost.ResultMailbox, Payload: json.RawMessage(`{"nonce":"` + nonce + `"}`)},
		{MessageID: "not-object", Kind: "response", From: "agent://reviewer", To: tetherhost.ResultMailbox, Payload: json.RawMessage(`["` + nonce + `"]`)},
	}
	for _, reply := range forged {
		f.fake.PostReply(request.Correlation, reply)
	}
	for range 2 {
		observation := observe(t, f.host, launched.Ref)
		if observation.State != agentadapter.SessionPending {
			t.Fatalf("forged replies changed the step: %#v", observation)
		}
	}
	logs := f.logs.String()
	for _, reply := range forged {
		if strings.Count(logs, "message_id="+reply.MessageID) != 1 {
			t.Fatalf("reply %s not logged exactly once:\n%s", reply.MessageID, logs)
		}
	}
	if !strings.Contains(logs, "from_unverified=agent://intruder") {
		t.Fatalf("unverified sender not recorded:\n%s", logs)
	}
	// A valid reply after the forgeries is accepted.
	f.fake.PostReply(request.Correlation, resultReply(nonce, `"ok"`))
	if observation := observe(t, f.host, launched.Ref); observation.State != agentadapter.SessionSucceeded || observation.Result.Inline != "ok" {
		t.Fatalf("valid reply after forgeries = %#v", observation)
	}
}

func TestObserveReplyWinsOverLaterSessionFailure(t *testing.T) {
	f := newFixture(t, nil)
	request := launchRequest()
	launched := f.launch(t, request)
	f.fake.PostReply(request.Correlation, resultReply(nonceFor(t, request), `42`))
	f.fake.RestartSweep()
	observation := observe(t, f.host, launched.Ref)
	if observation.State != agentadapter.SessionSucceeded || observation.Result.Type != values.TypeNumber {
		t.Fatalf("reply then sweep = %#v", observation)
	}
}

func TestObserveResultOptional(t *testing.T) {
	f := newFixture(t, func(tether *settings.TetherSubstrateSettings) { tether.ResultOptional = true })
	launched := f.launch(t, launchRequest())
	f.fake.Complete(launched.Ref.ID)
	observation := observe(t, f.host, launched.Ref)
	if observation.State != agentadapter.SessionSucceeded || observation.Result == nil || observation.Result.Type != values.TypeNull || observation.Result.Inline != nil {
		t.Fatalf("result_optional completion = %#v", observation)
	}
	f.fake.Fail(launched.Ref.ID, 1)
	if observation := observe(t, f.host, launched.Ref); observation.State != agentadapter.SessionFailed {
		t.Fatalf("result_optional does not cover failures: %#v", observation)
	}
}

func TestObserveStopOnResult(t *testing.T) {
	disabled := false
	for name, tc := range map[string]struct {
		mutate    func(*settings.TetherSubstrateSettings)
		wantStops int
		wantState string
	}{
		"default stops once": {nil, 1, tetherhost.StateKilled},
		"disabled":           {func(tether *settings.TetherSubstrateSettings) { tether.StopOnResult = &disabled }, 0, tetherhost.StateRunning},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, tc.mutate)
			request := launchRequest()
			launched := f.launch(t, request)
			f.fake.PostReply(request.Correlation, resultReply(nonceFor(t, request), `true`))
			if observation := observe(t, f.host, launched.Ref); observation.State != agentadapter.SessionSucceeded {
				t.Fatalf("observation = %#v", observation)
			}
			if f.fake.Stops() != tc.wantStops || f.fake.State(launched.Ref.ID) != tc.wantState {
				t.Fatalf("stops = %d state = %s", f.fake.Stops(), f.fake.State(launched.Ref.ID))
			}
			// A stopped session still yields its result: the reply is read first.
			if observation := observe(t, f.host, launched.Ref); observation.State != agentadapter.SessionSucceeded {
				t.Fatalf("re-observation after stop = %#v", observation)
			}
		})
	}
}

func TestObserveUnreachablePendsThenFailsAfterTimeout(t *testing.T) {
	f := newFixture(t, func(tether *settings.TetherSubstrateSettings) { tether.UnreachableTimeout = "5m" })
	launched := f.launch(t, launchRequest())
	f.fake.SetUnavailable(true)
	first := observe(t, f.host, launched.Ref)
	if first.State != agentadapter.SessionPending || first.Progress["state"] != "unreachable" || first.Progress["unreachable_since"] == "" {
		t.Fatalf("first unreachable observation = %#v", first)
	}
	if err := f.host.HeartbeatSession(t.Context(), launched.Ref); stepkind.ClassifyError(err) != stepkind.Retryable || !errors.Is(err, tetherhost.ErrUnavailable) {
		t.Fatalf("unreachable heartbeat = %v", err)
	}
	f.clock.Advance(4 * time.Minute)
	if observation := observe(t, f.host, launched.Ref); observation.State != agentadapter.SessionPending {
		t.Fatalf("before timeout = %#v", observation)
	}
	// Reachability clears the timer.
	f.fake.SetUnavailable(false)
	if observation := observe(t, f.host, launched.Ref); observation.State != agentadapter.SessionPending || observation.Progress["state"] != "running" {
		t.Fatalf("reachable again = %#v", observation)
	}
	f.fake.SetUnavailable(true)
	f.clock.Advance(4 * time.Minute)
	if observation := observe(t, f.host, launched.Ref); observation.State != agentadapter.SessionPending {
		t.Fatalf("timer did not restart after reachability: %#v", observation)
	}
	f.clock.Advance(5 * time.Minute)
	if err := f.host.HeartbeatSession(t.Context(), launched.Ref); err != nil {
		t.Fatalf("expired heartbeat must let observation close the step: %v", err)
	}
	final := observe(t, f.host, launched.Ref)
	if final.State != agentadapter.SessionFailed || final.Failure.Code != tetherhost.CodeHostUnreachable || !final.Failure.Retryable {
		t.Fatalf("after timeout = %#v", final)
	}
}

func TestHeartbeatSession(t *testing.T) {
	f := newFixture(t, nil)
	launched := f.launch(t, launchRequest())
	if err := f.host.HeartbeatSession(t.Context(), launched.Ref); err != nil {
		t.Fatalf("alive heartbeat = %v", err)
	}
	// An exited or missing session must not block observation, which the
	// go-workflow coordinator skips when a heartbeat errors.
	f.fake.Complete(launched.Ref.ID)
	if err := f.host.HeartbeatSession(t.Context(), launched.Ref); err != nil {
		t.Fatalf("exited heartbeat = %v", err)
	}
	missing := launched.Ref
	missing.ID = "sess-unknown"
	if err := f.host.HeartbeatSession(t.Context(), missing); err != nil {
		t.Fatalf("missing heartbeat = %v", err)
	}
	unknown := launched.Ref
	unknown.Substrate = "other"
	if err := f.host.HeartbeatSession(t.Context(), unknown); stepkind.ClassifyError(err) != stepkind.RetryPermanent {
		t.Fatalf("non-tether substrate heartbeat = %v", err)
	}
}

func TestCancelSession(t *testing.T) {
	f := newFixture(t, nil)
	launched := f.launch(t, launchRequest())
	if err := f.host.CancelSession(t.Context(), launched.Ref); err != nil {
		t.Fatal(err)
	}
	if err := f.host.CancelSession(t.Context(), launched.Ref); err != nil {
		t.Fatalf("second cancel = %v", err)
	}
	if f.fake.State(launched.Ref.ID) != tetherhost.StateKilled {
		t.Fatalf("state after cancel = %s", f.fake.State(launched.Ref.ID))
	}
	if observation := observe(t, f.host, launched.Ref); observation.State != agentadapter.SessionCanceled || observation.Failure.Code != tetherhost.CodeSessionCanceled {
		t.Fatalf("observation after cancel = %#v", observation)
	}
	missing := launched.Ref
	missing.ID = "sess-unknown"
	if err := f.host.CancelSession(t.Context(), missing); err != nil {
		t.Fatalf("cancel of a missing session = %v", err)
	}
	f.fake.SetUnavailable(true)
	if err := f.host.CancelSession(t.Context(), launched.Ref); stepkind.ClassifyError(err) != stepkind.Retryable {
		t.Fatalf("unreachable cancel = %v", err)
	}
}

func TestHostDrivesGoWorkflowAgentKind(t *testing.T) {
	f := newFixture(t, nil)
	kind, err := agentadapter.New(agentadapter.Options{Host: f.host})
	if err != nil {
		t.Fatal(err)
	}
	invocation := stepkind.Invocation{
		Identity:       stepkind.InvocationIdentity{RunID: "child-run", NodeID: "session", Attempt: 1},
		Config:         map[string]any{"substrate": "tether", "launch_id": "review", "logical_agent_id": "reviewer", "prompt": "Go."},
		IdempotencyKey: "invocation-key", Inputs: values.ValueSet{},
	}
	executed, err := kind.Execute(t.Context(), stepkind.PreparedInvocation{Invocation: invocation})
	if err != nil || executed.Outcome != stepkind.StepExternal || executed.External == nil {
		t.Fatalf("Execute = %#v, %v", executed, err)
	}
	correlation := executed.External.Metadata["correlation"]
	digest := executed.External.Metadata["request_digest"]
	f.fake.PostReply(correlation, resultReply(tetherhost.ResultNonce(testSecret, digest, correlation), `{"ok":true}`))
	if heartbeatErr := kind.Heartbeat(t.Context(), *executed.External); heartbeatErr != nil {
		t.Fatal(heartbeatErr)
	}
	observed, err := kind.Observe(t.Context(), *executed.External)
	if err != nil || observed.State != stepkind.ObservationSucceeded || observed.Result == nil {
		t.Fatalf("Observe = %#v, %v", observed, err)
	}
	if got := observed.Result.Outputs[agentadapter.OutputResult].Inline.(map[string]any)["ok"]; got != true {
		t.Fatalf("result output = %#v", observed.Result.Outputs)
	}
}
