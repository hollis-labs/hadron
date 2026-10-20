// Package hubrelay ships the workflow event outbox (workflow_event_outbox) to the
// event hub with the relay in mesh/eventbus. The outbox is already the durable,
// transactional record of every exported event; this package only reads it in
// order and hands each envelope to the hub client.
package hubrelay

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/libs/mesh/eventbus"
	"github.com/hollis-labs/libs/mesh/eventbus/relay"
)

// Store is the part of the workflow state store the relay uses.
type Store interface {
	EventOutboxAfter(ctx context.Context, after uint64, limit int) ([]persistence.OutboxEntry, error)
}

// App is the name Hadron's events carry and its credential is issued for.
const App = "hadron"

// producerOwned are the envelope fields the hub assigns. A producer never sends
// them; the outbox envelope carries the actor Hadron attributed, which the hub
// replaces with the credential's own.
var producerOwned = []string{"actor", "recorded_at", "cursor"}

// Item turns a stored envelope into the hub's ingest body: the stored bytes with
// the hub-owned fields removed and the app filled in. The outbox row is not changed.
func Item(e persistence.OutboxEntry) (eventbus.Item, error) {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(e.Envelope, &env); err != nil {
		return eventbus.Item{}, fmt.Errorf("hubrelay: outbox seq %d: %w", e.Seq, err)
	}
	for _, k := range producerOwned {
		delete(env, k)
	}
	// The outbox envelope does not name its app; the hub checks it against the
	// credential's, so the relay says it.
	if len(env["app"]) == 0 {
		env["app"] = json.RawMessage(`"` + App + `"`)
	}
	body, err := json.Marshal(env)
	if err != nil {
		return eventbus.Item{}, fmt.Errorf("hubrelay: outbox seq %d: %w", e.Seq, err)
	}
	// Every exported event is a state change or a gate decision: protected.
	// Deliver bypasses the client's queue, so this only labels it.
	return eventbus.Item{ID: e.EventID, Priority: eventbus.PriorityHigh, Body: body}, nil
}

// Source reads the outbox for a relay. Hadron never trims the outbox, so there
// is no gap to report.
type Source struct{ Store Store }

var _ relay.Source = Source{}

// After implements relay.Source.
func (s Source) After(ctx context.Context, after int64, limit int) ([]relay.Entry, error) {
	if after < 0 {
		after = 0
	}
	rows, err := s.Store.EventOutboxAfter(ctx, uint64(after), limit)
	if err != nil {
		return nil, err
	}
	out := make([]relay.Entry, 0, len(rows))
	for _, r := range rows {
		if r.Seq > math.MaxInt64 {
			return nil, fmt.Errorf("hubrelay: outbox seq %d out of range", r.Seq)
		}
		item, err := Item(r)
		if err != nil {
			return nil, err
		}
		out = append(out, relay.Entry{Seq: int64(r.Seq), Item: item})
	}
	return out, nil
}
