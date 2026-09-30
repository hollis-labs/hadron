package tetherhost_test

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/hollis-labs/hadron/internal/tetherhost"
)

// fakeMuxd is a minimal muxd HTTP surface for the adapter tests.
type fakeMuxd struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   []map[string]any
}

func (m *fakeMuxd) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Body != nil && r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&body)
		}
		m.mu.Lock()
		m.requests = append(m.requests, r)
		m.bodies = append(m.bodies, body)
		m.mu.Unlock()
		write := func(status int, value any) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			if err := json.NewEncoder(w).Encode(value); err != nil {
				t.Error(err)
			}
		}
		apiError := func(status int, code string) {
			write(status, map[string]any{"error": map[string]string{"code": code, "message": code}})
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/sessions":
			switch body["idempotency_key"] {
			case "fresh":
				write(http.StatusCreated, map[string]any{"id": "s-fresh", "replayed": false})
			case "replay":
				write(http.StatusOK, map[string]any{"id": "s-replay", "replayed": true})
			case "conflict":
				apiError(http.StatusConflict, "idempotency_conflict")
			default:
				apiError(http.StatusBadRequest, "invalid_request")
			}
		case r.Method == http.MethodPost && r.URL.Path == "/sessions/s-fresh/launch":
			write(http.StatusOK, map[string]any{"id": "s-fresh", "replayed": false})
		case r.Method == http.MethodPost && r.URL.Path == "/sessions/s-replay/launch":
			write(http.StatusOK, map[string]any{"id": "s-replay", "replayed": true})
		case r.Method == http.MethodGet && r.URL.Path == "/sessions/s-fresh":
			write(http.StatusOK, map[string]any{"id": "s-fresh", "state": "failed", "exit_code": -1})
		case r.Method == http.MethodGet && r.URL.Path == "/sessions/s-fresh/health":
			write(http.StatusOK, map[string]any{"session_id": "s-fresh", "alive": true})
		case r.Method == http.MethodPost && r.URL.Path == "/sessions/s-fresh/stop":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/sessions/s-exited/stop":
			apiError(http.StatusNotFound, "not_found")
		case r.Method == http.MethodPost && r.URL.Path == "/sessions/s-broken/stop":
			apiError(http.StatusInternalServerError, "internal_error")
		case r.Method == http.MethodGet && r.URL.Path == "/messages/thread/agent:run-1:review":
			write(http.StatusOK, map[string]any{"messages": []map[string]any{
				{"id": "m1", "kind": "request", "from": "msg://agent/hadron/results", "to": "msg://session/local/s-fresh", "payload": map[string]any{"x": 1}, "created_at": "2026-09-30T12:00:00Z"},
				{"id": "m2", "kind": "response", "from": "msg://agent/local/reviewer", "to": "msg://agent/hadron/results", "thread_id": "agent:run-1:review", "payload": map[string]any{"nonce": "n", "result": "ok"}, "created_at": "2026-09-30T12:01:00Z"},
			}})
		default:
			apiError(http.StatusNotFound, "not_found")
		}
	}
}

func (m *fakeMuxd) last() (*http.Request, map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests[len(m.requests)-1], m.bodies[len(m.bodies)-1]
}

func newAdapter(t *testing.T) (*tetherhost.TetherClient, *fakeMuxd) {
	t.Helper()
	muxd := &fakeMuxd{}
	server := httptest.NewServer(muxd.handler(t))
	t.Cleanup(server.Close)
	client, err := tetherhost.NewTetherClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return client, muxd
}

func TestTetherClientCreateAndLaunchReplayed(t *testing.T) {
	client, muxd := newAdapter(t)
	fresh, err := client.CreateSession(t.Context(), tetherhost.CreateRequest{Launch: "claude", PromptAppend: "contract", IdempotencyKey: "fresh"})
	if err != nil || fresh != (tetherhost.Session{ID: "s-fresh"}) {
		t.Fatalf("fresh create = %#v, %v", fresh, err)
	}
	_, body := muxd.last()
	if body["launch"] != "claude" || body["prompt_append"] != "contract" || body["idempotency_key"] != "fresh" {
		t.Fatalf("create body = %#v", body)
	}
	replay, err := client.CreateSession(t.Context(), tetherhost.CreateRequest{Launch: "claude", IdempotencyKey: "replay"})
	if err != nil || replay != (tetherhost.Session{ID: "s-replay", Replayed: true}) {
		t.Fatalf("replayed create = %#v, %v", replay, err)
	}
	if launched, err := client.LaunchSession(t.Context(), "s-fresh"); err != nil || launched.Replayed {
		t.Fatalf("fresh launch = %#v, %v", launched, err)
	}
	if launched, err := client.LaunchSession(t.Context(), "s-replay"); err != nil || !launched.Replayed {
		t.Fatalf("replayed launch = %#v, %v", launched, err)
	}
}

func TestTetherClientMapsErrors(t *testing.T) {
	client, _ := newAdapter(t)
	if _, err := client.CreateSession(t.Context(), tetherhost.CreateRequest{Launch: "l", IdempotencyKey: "conflict"}); !errors.Is(err, tetherhost.ErrIdempotencyConflict) {
		t.Fatalf("409 conflict = %v", err)
	}
	if _, err := client.GetSession(t.Context(), "s-unknown"); !errors.Is(err, tetherhost.ErrNotFound) {
		t.Fatalf("404 = %v", err)
	}
	if _, err := client.SessionHealth(t.Context(), "s-unknown"); !errors.Is(err, tetherhost.ErrNotFound) {
		t.Fatalf("404 health = %v", err)
	}
	if _, err := client.CreateSession(t.Context(), tetherhost.CreateRequest{Launch: "l", IdempotencyKey: "other"}); err == nil ||
		errors.Is(err, tetherhost.ErrNotFound) || errors.Is(err, tetherhost.ErrUnavailable) || errors.Is(err, tetherhost.ErrIdempotencyConflict) {
		t.Fatalf("400 must pass through unclassified: %v", err)
	}
	if err := client.StopSession(t.Context(), "s-exited"); err != nil {
		t.Fatalf("stopping a session that is not running = %v", err)
	}
	if err := client.StopSession(t.Context(), "s-broken"); err == nil {
		t.Fatal("500 on stop was swallowed")
	}
	if err := client.StopSession(t.Context(), "s-fresh"); err != nil {
		t.Fatalf("stop = %v", err)
	}
}

func TestTetherClientReadsSessionAndHealth(t *testing.T) {
	client, _ := newAdapter(t)
	state, err := client.GetSession(t.Context(), "s-fresh")
	if err != nil || state.State != "failed" || state.ExitCode == nil || *state.ExitCode != -1 {
		t.Fatalf("GetSession = %#v, %v", state, err)
	}
	if health, err := client.SessionHealth(t.Context(), "s-fresh"); err != nil || !health.Alive {
		t.Fatalf("SessionHealth = %#v, %v", health, err)
	}
}

func TestTetherClientUnreachableDaemon(t *testing.T) {
	missing, err := tetherhost.NewTetherClient(filepath.Join(t.TempDir(), "muxd.sock"))
	if err != nil {
		t.Fatalf("construction must not dial: %v", err)
	}
	if _, getErr := missing.GetSession(t.Context(), "s"); !errors.Is(getErr, tetherhost.ErrUnavailable) {
		t.Fatalf("missing socket = %v", getErr)
	}
	if _, threadErr := missing.ThreadReplies(t.Context(), "thread"); !errors.Is(threadErr, tetherhost.ErrUnavailable) {
		t.Fatalf("missing socket thread read = %v", threadErr)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	refused, err := tetherhost.NewTetherClient("tcp:" + address)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := refused.CreateSession(t.Context(), tetherhost.CreateRequest{Launch: "l", IdempotencyKey: "k"}); !errors.Is(err, tetherhost.ErrUnavailable) {
		t.Fatalf("connection refused = %v", err)
	}
	if err := refused.StopSession(t.Context(), "s"); !errors.Is(err, tetherhost.ErrUnavailable) {
		t.Fatalf("connection refused stop = %v", err)
	}
}

func TestTetherClientThreadRepliesAsResultMailbox(t *testing.T) {
	client, muxd := newAdapter(t)
	replies, err := client.ThreadReplies(t.Context(), "agent:run-1:review")
	if err != nil || len(replies) != 2 {
		t.Fatalf("ThreadReplies = %#v, %v", replies, err)
	}
	request, _ := muxd.last()
	if got := request.URL.Query().Get("as"); got != tetherhost.ResultMailbox {
		t.Fatalf("thread read asserted as=%q", got)
	}
	want := tetherhost.Reply{MessageID: "m2", Kind: "response", From: "msg://agent/local/reviewer", To: tetherhost.ResultMailbox}
	got := replies[1]
	payload := string(got.Payload)
	got.Payload = nil
	if !reflect.DeepEqual(got, want) || !strings.Contains(payload, `"nonce":"n"`) || !strings.Contains(payload, `"result":"ok"`) {
		t.Fatalf("reply = %#v payload=%s", replies[1], payload)
	}
	if replies[0].MessageID != "m1" || replies[0].Kind != "request" {
		t.Fatalf("thread order = %#v", replies)
	}
	if _, err := client.ThreadReplies(t.Context(), "unknown-thread"); !errors.Is(err, tetherhost.ErrNotFound) {
		t.Fatalf("unknown thread = %v", err)
	}
}

func TestTetherClientEndpointForms(t *testing.T) {
	for _, endpoint := range []string{"", "~/.tether/run/muxd.sock", "/tmp/muxd.sock", "unix:/tmp/muxd.sock", "tcp:127.0.0.1:1", "http://127.0.0.1:1"} {
		if _, err := tetherhost.NewTetherClient(endpoint); err != nil {
			t.Fatalf("NewTetherClient(%q) = %v", endpoint, err)
		}
	}
	if _, err := tetherhost.NewTetherClient("relative/muxd.sock"); err == nil {
		t.Fatal("relative socket path was accepted")
	}
}
