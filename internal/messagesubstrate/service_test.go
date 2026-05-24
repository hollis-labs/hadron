package messagesubstrate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hollis-labs/go-messaging"

	"github.com/hollis-labs/hadron/internal/execution"
	"github.com/hollis-labs/hadron/internal/settings"
)

func TestRemoteHTTPSubstrate_PollSendGetConsume(t *testing.T) {
	var sentToNotify bool
	message := messaging.Envelope{
		ID:        "msg-remote-1",
		Kind:      messaging.MsgKindNotice,
		From:      messaging.Address{Kind: messaging.KindService, Authority: "agent-mux", ID: "sender"},
		To:        messaging.Address{Kind: messaging.KindAgent, Authority: "agent-mux", ID: "worker-1"},
		ThreadID:  "corr-123",
		Payload:   json.RawMessage(`{"ok":true}`),
		Metadata:  map[string]string{"correlation_id": "corr-123"},
		CreatedAt: mustTime(t, "2026-05-24T00:00:00Z"),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/messages/notify":
			sentToNotify = true
			writeJSON(t, w, map[string]any{"message": message})
		case r.Method == http.MethodGet && r.URL.Path == "/messages/inbox":
			if got := r.URL.Query().Get("thread_id"); got != "corr-123" {
				t.Fatalf("thread_id = %q, want corr-123", got)
			}
			writeJSON(t, w, map[string]any{"messages": []messaging.Envelope{message}})
		case r.Method == http.MethodGet && r.URL.Path == "/messages/msg-remote-1":
			writeJSON(t, w, message)
		case r.Method == http.MethodPost && r.URL.Path == "/messages/msg-remote-1/consume":
			if got := r.URL.Query().Get("as"); got != message.To.URN() {
				t.Fatalf("consume as = %q, want %q", got, message.To.URN())
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	svc := New(nil, map[string]settings.MessageSubstrateSetting{
		"remote_mailbox": {
			Kind:       kindTetherHTTP,
			BaseURL:    server.URL,
			Authority:  "agent-mux",
			NotifyWake: true,
		},
	})

	sent, err := svc.Send(context.Background(), "remote_mailbox", message)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !sentToNotify {
		t.Fatal("expected notify endpoint to be used")
	}
	if sent.ID != message.ID {
		t.Fatalf("sent id = %q, want %q", sent.ID, message.ID)
	}

	got, err := svc.PollMessage(context.Background(), executionQuery("remote_mailbox", message.To.URN(), "corr-123"))
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if got == nil || !strings.Contains(got.Body, `"ok":true`) {
		t.Fatalf("unexpected polled message: %+v", got)
	}

	env, err := svc.Get(context.Background(), "remote_mailbox", message.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if env.ID != message.ID {
		t.Fatalf("get id = %q, want %q", env.ID, message.ID)
	}

	if err := svc.Consume(context.Background(), "remote_mailbox", message.ID); err != nil {
		t.Fatalf("consume: %v", err)
	}
}

func executionQuery(substrate, to, correlationID string) execution.MessageQuery {
	return execution.MessageQuery{
		Substrate:     substrate,
		To:            to,
		CorrelationID: correlationID,
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func mustTime(t *testing.T, v string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, v)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return ts
}
