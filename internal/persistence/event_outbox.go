package persistence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	workflowruntime "github.com/hollis-labs/libs/workflow/engine/runtime"
)

// OutboxMapper turns a workflow event into an event-stream envelope. It is
// called inside the transaction that appends the event, so it must be quick and
// must not touch the network or the database.
type OutboxMapper interface {
	// MapWorkflowEvent returns the envelope JSON for ev, or ok false when the
	// event is not exported. seq is the outbox sequence the envelope will carry
	// and generation the event log's generation; the mapper stamps both.
	MapWorkflowEvent(ctx context.Context, ev workflowruntime.Event, generation, seq uint64) (envelope []byte, eventID string, ok bool, err error)
}

// OutboxEntry is one stored envelope.
type OutboxEntry struct {
	Seq         uint64
	RunID       workflowruntime.RunID
	RunSequence uint64
	EventID     string
	Envelope    []byte
	CreatedAt   time.Time
}

// eventObserver is implemented by the SQL handle a write transaction gives its
// closure when the outbox is enabled: appendWorkflowEvent tells it about every
// event it appends, inside the same transaction.
type eventObserver interface {
	observeEvent(ctx context.Context, ev workflowruntime.Event) error
}

type observedSQL struct {
	workflowSQL
	outbox *eventOutbox
}

func (o observedSQL) observeEvent(ctx context.Context, ev workflowruntime.Event) error {
	return o.outbox.observe(ctx, o.workflowSQL, ev)
}

type eventOutbox struct {
	mapper     OutboxMapper
	generation uint64
	now        func() time.Time
	skipped    atomic.Uint64
}

// EnableEventOutbox starts writing envelopes for the events mapper selects, in
// the same transaction as each event. Without it nothing is written. It reads
// the event log's generation (migration 0032), which becomes the envelopes'.
func (s *WorkflowStateStore) EnableEventOutbox(ctx context.Context, mapper OutboxMapper) error {
	if mapper == nil {
		return errors.New("event outbox requires a mapper")
	}
	generation, err := s.StoreGeneration(ctx)
	if err != nil {
		return err
	}
	number, err := generationNumber(generation)
	if err != nil {
		return err
	}
	s.outbox = &eventOutbox{mapper: mapper, generation: number, now: time.Now}
	return nil
}

// generationNumber is the envelope generation derived from the store generation:
// its first 52 bits (13 hex digits), nonzero and below 2^53 so it is exact in every JSON reader. It is stable for a database and changes
// when a different one is created or restored over it.
func generationNumber(storeGeneration string) (uint64, error) {
	if len(storeGeneration) < 13 {
		return 0, fmt.Errorf("store generation %q is too short", storeGeneration)
	}
	n, err := strconv.ParseUint(storeGeneration[:13], 16, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("store generation %q is not hexadecimal", storeGeneration)
	}
	return n, nil
}

// Skip classes recorded when an envelope could not be written. A mapper may name
// its own by returning an error with a SkipClass() string method.
const (
	skipMapper = "mapper_error"
	skipPanic  = "mapper_panic"
	skipInsert = "insert_error"
)

type skipClasser interface{ SkipClass() string }

// observe writes the envelope for ev inside the caller's transaction, under a
// SAVEPOINT. Recording never rejects a valid mutation (platform rule, CW-0149): if
// the envelope cannot be produced or stored, only the envelope work is rolled back
// to the savepoint, a workflow_event_outbox_skipped row says which event and why,
// the failure is logged at ERROR and counted, and nil is returned so the workflow
// event commits. The sequence counter is not advanced for a skipped event. Only a
// database error in the outer transaction itself (the savepoint, the rollback, or
// the skipped row) fails the write.
func (o *eventOutbox) observe(ctx context.Context, query workflowSQL, ev workflowruntime.Event) error {
	if _, err := query.ExecContext(ctx, `SAVEPOINT workflow_event_outbox`); err != nil {
		return fmt.Errorf("outbox savepoint: %w", err)
	}
	class, cause := o.record(ctx, query, ev)
	if cause == nil {
		if _, err := query.ExecContext(ctx, `RELEASE workflow_event_outbox`); err != nil {
			return fmt.Errorf("outbox release: %w", err)
		}
		return nil
	}
	if _, err := query.ExecContext(ctx, `ROLLBACK TO workflow_event_outbox`); err != nil {
		return fmt.Errorf("outbox rollback: %w (after %s)", err, class)
	}
	if _, err := query.ExecContext(ctx, `RELEASE workflow_event_outbox`); err != nil {
		return fmt.Errorf("outbox release after rollback: %w", err)
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_event_outbox_skipped(run_id, run_sequence, error_class, created_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(run_id, run_sequence) DO UPDATE SET error_class = excluded.error_class, created_at = excluded.created_at, repaired_at = NULL`,
		string(ev.RunID), int64(ev.Sequence), class, workflowTime(o.now())); err != nil { // #nosec G115 -- a positive sequence
		return fmt.Errorf("record skipped outbox entry: %w", err)
	}
	o.skipped.Add(1)
	slog.Error("event outbox skipped an event", "run_id", string(ev.RunID), "run_sequence", ev.Sequence, "event_type", ev.Type, "class", class)
	return nil
}

// record maps ev and inserts its envelope. It returns the skip class and cause on
// failure. A panic in the mapper is a failure like any other.
func (o *eventOutbox) record(ctx context.Context, query workflowSQL, ev workflowruntime.Event) (class string, cause error) {
	defer func() {
		if r := recover(); r != nil {
			class, cause = skipPanic, fmt.Errorf("mapper panicked: %T", r)
		}
	}()
	var last int64
	if err := query.QueryRowContext(ctx, `SELECT last_seq FROM workflow_event_outbox_counter WHERE id = 1`).Scan(&last); err != nil {
		return skipInsert, fmt.Errorf("load outbox counter: %w", err)
	}
	seq := uint64(last) + 1 // #nosec G115 -- last_seq is never negative
	envelope, eventID, ok, err := o.mapper.MapWorkflowEvent(ctx, ev, o.generation, seq)
	if err != nil {
		class = skipMapper
		var c skipClasser
		if errors.As(err, &c) {
			class = c.SkipClass()
		}
		return class, err
	}
	if !ok {
		return "", nil
	}
	if _, err := query.ExecContext(ctx, `UPDATE workflow_event_outbox_counter SET last_seq = ? WHERE id = 1`, int64(seq)); err != nil { // #nosec G115 -- bounded by the table
		return skipInsert, fmt.Errorf("advance outbox counter: %w", err)
	}
	if _, err := query.ExecContext(ctx, `
INSERT INTO workflow_event_outbox(outbox_seq, run_id, run_sequence, event_id, envelope_json, created_at)
VALUES (?, ?, ?, ?, ?, ?)`, int64(seq), string(ev.RunID), int64(ev.Sequence), eventID, string(envelope), workflowTime(o.now())); err != nil { // #nosec G115 -- bounded by the table
		return skipInsert, fmt.Errorf("insert outbox entry: %w", err)
	}
	return "", nil
}

// EventOutboxSkipped is the number of events the outbox has skipped since this
// store enabled it (see also the workflow_event_outbox_skipped table, which is
// durable).
func (s *WorkflowStateStore) EventOutboxSkipped() uint64 {
	if s.outbox == nil {
		return 0
	}
	return s.outbox.skipped.Load()
}

// EventOutboxAfter returns up to limit outbox entries with a sequence greater
// than after, in order. It is how a relay reads the outbox.
func (s *WorkflowStateStore) EventOutboxAfter(ctx context.Context, after uint64, limit int) ([]OutboxEntry, error) {
	if err := checkWorkflowContext(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT outbox_seq, run_id, run_sequence, event_id, envelope_json, created_at
FROM workflow_event_outbox WHERE outbox_seq > ? ORDER BY outbox_seq LIMIT ?`, int64(after), limit) // #nosec G115 -- sequences are far below 2^63
	if err != nil {
		return nil, fmt.Errorf("read event outbox: %w", err)
	}
	defer closeRows(rows)
	var out []OutboxEntry
	for rows.Next() {
		var (
			e                            OutboxEntry
			seq, runSeq                  int64
			runID, envelope, createdText string
		)
		if scanErr := rows.Scan(&seq, &runID, &runSeq, &e.EventID, &envelope, &createdText); scanErr != nil {
			return nil, fmt.Errorf("scan event outbox: %w", scanErr)
		}
		e.Seq, e.RunID, e.RunSequence, e.Envelope = uint64(seq), workflowruntime.RunID(runID), uint64(runSeq), []byte(envelope) // #nosec G115 -- the table holds positive values
		if e.CreatedAt, err = parseWorkflowTime("outbox created_at", createdText); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
