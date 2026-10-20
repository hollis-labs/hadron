package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/hubrelay"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/hadron/internal/settings"
	"github.com/hollis-labs/libs/harness/core/adapters/runtimeevents/ingest"
	"github.com/hollis-labs/libs/mesh/eventbus"
	"github.com/hollis-labs/libs/mesh/eventbus/relay"
)

// fakeHub stands in for the hub's ingest route and accepts everything. Nothing
// here touches a real hub.
type fakeHub struct {
	mu   sync.Mutex
	ids  []string
	body []map[string]json.RawMessage
}

func (h *fakeHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var req struct {
		Events []json.RawMessage `json:"events"`
	}
	if json.Unmarshal(raw, &req) != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	type result struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Cursor string `json:"cursor"`
	}
	var results []result
	h.mu.Lock()
	for i, e := range req.Events {
		var m map[string]json.RawMessage
		_ = json.Unmarshal(e, &m)
		var id string
		_ = json.Unmarshal(m["id"], &id)
		h.ids = append(h.ids, id)
		h.body = append(h.body, m)
		results = append(results, result{ID: id, Status: "accepted", Cursor: "c" + string(rune('a'+i))})
	}
	h.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"results": results, "accepted": len(results), "duplicate": 0, "rejected": 0})
}

func (h *fakeHub) seen() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.ids...)
}

// A real run's outbox, shipped to a fake hub: every event once, in order, with
// nothing the hub owns, and a restart sends only what is new.
func TestHubRelayShipsTheOutboxOnceAndResumesAfterARestart(t *testing.T) {
	store := runTransformToSuccess(t, &settings.Settings{Events: settings.EventSettings{Outbox: true}})
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	all, err := state.EventOutboxAfter(t.Context(), 0, 100)
	if err != nil || len(all) == 0 {
		t.Fatalf("outbox = %d rows, %v", len(all), err)
	}

	hub := &fakeHub{}
	srv := httptest.NewServer(hub)
	defer srv.Close()
	spool, cursor := t.TempDir(), filepath.Join(t.TempDir(), "outbox.cursor")

	run := func() (stop func(), r *relay.Relay) {
		client, newErr := eventbus.New(eventbus.Config{URL: srv.URL, SpoolDir: spool, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		if newErr != nil {
			t.Fatal(newErr)
		}
		r, newErr = relay.New(relay.Config{Source: hubrelay.Source{Store: state}, Cursor: relay.FileCursor{Path: cursor}, Deliverer: client, Poll: 5 * time.Millisecond})
		if newErr != nil {
			t.Fatal(newErr)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { defer close(done); _ = r.Run(ctx) }()
		return func() { cancel(); <-done; _ = client.Close(context.Background()) }, r
	}

	stop, r := run()
	waitFor(t, func() bool { return len(hub.seen()) == len(all) && r.Stats().Cursor == int64(all[len(all)-1].Seq) })
	stop()
	stop, _ = run() // a restart: the cursor is in its file
	time.Sleep(100 * time.Millisecond)
	stop()

	var want []string
	for _, e := range all {
		want = append(want, e.EventID)
	}
	got := hub.seen()
	if len(got) != len(want) {
		t.Fatalf("hub saw %d events, want %d (every event once): %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d = %s, want %s", i, got[i], want[i])
		}
	}
	for _, b := range hub.body {
		for _, f := range []string{"actor", "recorded_at", "cursor"} {
			if _, has := b[f]; has {
				t.Fatalf("event %s carries %q, which is the hub's to assign", b["id"], f)
			}
		}
		if string(b["app"]) != `"hadron"` {
			t.Fatalf("app = %s, want hadron", b["app"])
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Item removes what the hub owns from a copy and leaves the stored envelope alone.
func TestHubItemRemovesHubOwnedFieldsFromACopy(t *testing.T) {
	stored := []byte(`{"id":"e1","app":"hadron","actor":{"urn":"msg://service/hadron"},"recorded_at":"2026-10-20T00:00:00Z","cursor":"x","kind":"run.state_changed"}`)
	entry := persistence.OutboxEntry{Seq: 3, EventID: "e1", Envelope: stored}
	item, err := hubrelay.Item(entry)
	if err != nil {
		t.Fatal(err)
	}
	var sent map[string]json.RawMessage
	if err := json.Unmarshal(item.Body, &sent); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"actor", "recorded_at", "cursor"} {
		if _, has := sent[f]; has {
			t.Fatalf("%q was sent", f)
		}
	}
	if item.ID != "e1" || item.Priority != eventbus.PriorityHigh || string(sent["kind"]) != `"run.state_changed"` {
		t.Fatalf("item = %+v %s", item, item.Body)
	}
	if string(entry.Envelope) != string(stored) {
		t.Fatal("the stored envelope was changed")
	}
}

// The relay is off unless events.hub_relay and events.outbox are both on.
func TestHubRelayIsOffByDefault(t *testing.T) {
	for name, sett := range map[string]*settings.Settings{
		"default":          {},
		"outbox only":      {Events: settings.EventSettings{Outbox: true}},
		"relay w/o outbox": {Events: settings.EventSettings{HubRelay: true}},
	} {
		t.Run(name, func(t *testing.T) {
			runtime, _, _ := newTestProductionWorkflowRuntimeWithSettings(t, sett)
			if runtime.hubRelay != nil {
				t.Fatal("the hub relay is composed although it is not fully enabled")
			}
		})
	}
	runtime, _, _ := newTestProductionWorkflowRuntimeWithSettings(t, &settings.Settings{Events: settings.EventSettings{Outbox: true, HubRelay: true}})
	if runtime.hubRelay == nil {
		t.Fatal("events.outbox and events.hub_relay should compose the relay")
	}
}

// With the relay switched on but no HOLLIS_EVENTS_URL there is no hub: nothing
// runs, and stopping is harmless.
func TestHubRelayDoesNothingWithoutAURL(t *testing.T) {
	t.Setenv(eventbus.EnvURL, "")
	runtime, _, store := newTestProductionWorkflowRuntimeWithSettings(t, &settings.Settings{Events: settings.EventSettings{Outbox: true, HubRelay: true}})
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	_ = runtime
	stop := hubRelayStarter(state, t.TempDir())(t.Context())
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stop blocked although no relay was started")
	}
}

// What the relay sends must pass the hub's own ingest checks (the same package the
// hub runs), or the hub would reject it and the relay would quarantine it. This
// covers the families a real run produces: the admission gate.evaluated and the
// run.state_changed events. Families a run of this workflow does not produce
// (cancellation, a gate wait, a confirmation) are not exercised here.
func TestHubRelayItemsPassTheHubsIngestChecks(t *testing.T) {
	store := runTransformToSuccess(t, &settings.Settings{Events: settings.EventSettings{Outbox: true}})
	state, err := persistence.NewWorkflowStateStore(store)
	if err != nil {
		t.Fatal(err)
	}
	all, err := state.EventOutboxAfter(t.Context(), 0, 100)
	if err != nil || len(all) == 0 {
		t.Fatalf("outbox = %d rows, %v", len(all), err)
	}
	kinds := map[string]bool{}
	for _, e := range all {
		item, itemErr := hubrelay.Item(e)
		if itemErr != nil {
			t.Fatal(itemErr)
		}
		if _, rej := ingest.CheckItem(hubrelay.App, item.Body, ingest.Options{}); rej != nil {
			t.Errorf("outbox seq %d rejected by the hub's ingest check: %+v\n%s", e.Seq, rej, item.Body)
		}
		var env struct {
			Kind string `json:"kind"`
		}
		_ = json.Unmarshal(e.Envelope, &env)
		kinds[env.Kind] = true
	}
	for _, want := range []string{"gate.evaluated", "run.state_changed"} {
		if !kinds[want] {
			t.Errorf("the run produced no %s event; the conformance check no longer covers it (got %v)", want, kinds)
		}
	}
}
