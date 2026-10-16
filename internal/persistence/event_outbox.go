package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hollis-labs/hadron/internal/eventmap"
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

// AdmissionMapper is implemented by a mapper that also exports the host's admission
// policy decisions (gate hadron.admission).
type AdmissionMapper interface {
	MapAdmission(ctx context.Context, a eventmap.Admission, generation, seq uint64) (envelope []byte, eventID string, ok bool, err error)
}

// ConfirmationMapper is implemented by a mapper that also exports the satisfied
// confirmation of an admission decision.
type ConfirmationMapper interface {
	MapAdmissionConfirmation(ctx context.Context, c eventmap.AdmissionConfirmation, generation, seq uint64) (envelope []byte, eventID string, ok bool, err error)
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

// admissionObserver is implemented by the same handle for the host store's
// admission policy decision.
type admissionObserver interface {
	observeAdmission(ctx context.Context, a eventmap.Admission) error
}

// confirmationObserver is the same for the satisfied confirmation of an admission
// decision, written in the transaction that records the start.
type confirmationObserver interface {
	observeConfirmation(ctx context.Context, c eventmap.AdmissionConfirmation) error
}

type observedSQL struct {
	workflowSQL
	outbox *eventOutbox
}

func (o observedSQL) observeEvent(ctx context.Context, ev workflowruntime.Event) error {
	return o.outbox.observe(ctx, o.workflowSQL, ev)
}

func (o observedSQL) observeConfirmation(ctx context.Context, c eventmap.AdmissionConfirmation) error {
	return o.outbox.observeConfirmation(ctx, o.workflowSQL, c)
}

func (o observedSQL) observeAdmission(ctx context.Context, a eventmap.Admission) error {
	return o.outbox.observeAdmission(ctx, o.workflowSQL, a)
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

// outboxKey names the source of one envelope in the outbox and in the skipped
// table. A workflow event is (run id, per-run sequence), which starts at 1. An
// admission decision is (decision id, 0): sequence 0 is never a workflow event's, so
// the two cannot collide whatever the ids look like.
type outboxKey struct {
	runID    string
	sequence int64
	what     string // for the log line
}

// mapFunc produces the envelope for the next outbox sequence.
type mapFunc func(seq uint64) (envelope []byte, eventID string, ok bool, err error)

// admissionKey is the one place that decides where an admission decision sits in the
// outbox: the decision id in the run_id column and sequence 0, which no workflow
// event takes. A ruling on the key is a change here.
func admissionKey(a eventmap.Admission) outboxKey {
	return outboxKey{runID: a.DecisionID, sequence: 0, what: "admission"}
}

// confirmationKey is where the second event of a decision, its satisfied
// confirmation, sits: the same decision id with sequence 1. A decision has at most
// these two events, so one export per decision holds by the unique key.
func confirmationKey(c eventmap.AdmissionConfirmation) outboxKey {
	return outboxKey{runID: c.DecisionID, sequence: 1, what: "admission_confirmation"}
}

// admissionCause returns the id of the admission event that led to the run of ev, when
// ev is the run's first state change and an admission decision for that run id was
// exported. It is empty otherwise (no decision, or an earlier state change already
// carried the cause), and any lookup error just means no causation: it must never
// fail the write. The decision row carries the run id it was made for, and the
// admission envelope sits in the outbox under (decision id, 0).
func admissionCause(ctx context.Context, query workflowSQL, ev workflowruntime.Event) string {
	if ev.Type != workflowruntime.EventRunStatusChanged && ev.Type != workflowruntime.EventRunCancellationRequested {
		return ""
	}
	var earlier int
	if err := query.QueryRowContext(ctx, `
SELECT COUNT(1) FROM workflow_events
WHERE run_id = ? AND sequence < ? AND event_type IN (?, ?)`,
		string(ev.RunID), int64(ev.Sequence), workflowruntime.EventRunStatusChanged, workflowruntime.EventRunCancellationRequested).Scan(&earlier); err != nil || earlier != 0 { // #nosec G115 -- a positive sequence
		return ""
	}
	// The decision's last exported event: its satisfied confirmation (sequence 1) if
	// there is one, else the admission itself (sequence 0).
	var eventID string
	var sequence int
	if err := query.QueryRowContext(ctx, `
SELECT o.event_id, o.run_sequence FROM workflow_host_policy_decisions d
JOIN workflow_event_outbox o ON o.run_id = d.decision_id AND o.run_sequence IN (0, 1)
WHERE d.run_id = ? ORDER BY d.decided_at, d.decision_id, o.run_sequence DESC LIMIT 1`, string(ev.RunID)).Scan(&eventID, &sequence); err != nil {
		return ""
	}
	if sequence == 1 {
		// A confirm dry run is passed without a confirmation and links to no run.
		var dry sql.NullInt64
		if err := query.QueryRowContext(ctx, `SELECT json_extract(request_json, '$.dry_run') FROM workflow_host_starts WHERE run_id = ?`, string(ev.RunID)).Scan(&dry); err != nil || (dry.Valid && dry.Int64 != 0) {
			return ""
		}
	}
	return eventID
}

func eventKey(ev workflowruntime.Event) outboxKey {
	return outboxKey{runID: string(ev.RunID), sequence: int64(ev.Sequence), what: ev.Type} // #nosec G115 -- a positive sequence
}

// observe writes the envelope for ev inside the caller's transaction, under a
// SAVEPOINT. Recording never rejects a valid mutation (platform rule, CW-0149): if
// the envelope cannot be produced or stored, only the envelope work is rolled back
// to the savepoint, a workflow_event_outbox_skipped row says which event and why,
// the failure is logged at ERROR and counted, and nil is returned so the workflow
// event commits. The sequence counter is not advanced for a skipped event. Only a
// database error in the outer transaction itself (the savepoint, the rollback, or
// the skipped row) fails the write.
func (o *eventOutbox) observe(ctx context.Context, query workflowSQL, ev workflowruntime.Event) error {
	return o.guard(ctx, query, eventKey(ev), func(seq uint64) ([]byte, string, bool, error) {
		mapCtx := ctx
		if cause := admissionCause(ctx, query, ev); cause != "" {
			mapCtx = eventmap.WithCausation(ctx, cause)
		}
		return o.mapper.MapWorkflowEvent(mapCtx, ev, o.generation, seq)
	})
}

// observeAdmission is observe for an admission policy decision, written by the host
// store in its own transaction. A mapper that cannot map admissions skips them
// silently (the flag-off case has no outbox at all).
func (o *eventOutbox) observeAdmission(ctx context.Context, query workflowSQL, a eventmap.Admission) error {
	mapper, ok := o.mapper.(AdmissionMapper)
	if !ok {
		return nil
	}
	return o.guard(ctx, query, admissionKey(a), func(seq uint64) ([]byte, string, bool, error) {
		// One admission event per decision across both entry points (the evaluation
		// and the start): if it is already there, this is not a second one.
		var exists int
		if err := query.QueryRowContext(ctx, `SELECT COUNT(1) FROM workflow_event_outbox WHERE run_id = ? AND run_sequence = 0`, a.DecisionID).Scan(&exists); err != nil {
			return nil, "", false, err
		}
		if exists != 0 {
			return nil, "", false, nil
		}
		return mapper.MapAdmission(ctx, a, o.generation, seq)
	})
}

// observeConfirmation exports the satisfied confirmation of a confirm decision as a
// second gate.evaluated (pass) on the same admission subject, caused by the blocked
// event. A decision's confirmation is exported once: a second one writes nothing.
func (o *eventOutbox) observeConfirmation(ctx context.Context, query workflowSQL, c eventmap.AdmissionConfirmation) error {
	mapper, ok := o.mapper.(ConfirmationMapper)
	if !ok {
		return nil
	}
	return o.guard(ctx, query, confirmationKey(c), func(seq uint64) ([]byte, string, bool, error) {
		var exists int
		if err := query.QueryRowContext(ctx, `SELECT COUNT(1) FROM workflow_event_outbox WHERE run_id = ? AND run_sequence = 1`, c.DecisionID).Scan(&exists); err != nil {
			return nil, "", false, err
		}
		if exists != 0 {
			return nil, "", false, nil
		}
		// The blocked event this answers. If it was never exported (skipped, or the
		// outbox was enabled later), the pass simply has no cause.
		var blocked string
		err := query.QueryRowContext(ctx, `SELECT event_id FROM workflow_event_outbox WHERE run_id = ? AND run_sequence = 0`, c.DecisionID).Scan(&blocked)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, "", false, err
		}
		c.BlockedEventID = blocked
		return mapper.MapAdmissionConfirmation(ctx, c, o.generation, seq)
	})
}

func (o *eventOutbox) guard(ctx context.Context, query workflowSQL, key outboxKey, produce mapFunc) error {
	if _, err := query.ExecContext(ctx, `SAVEPOINT workflow_event_outbox`); err != nil {
		return fmt.Errorf("outbox savepoint: %w", err)
	}
	class, cause := o.record(ctx, query, key, produce)
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
		key.runID, key.sequence, class, workflowTime(o.now())); err != nil {
		return fmt.Errorf("record skipped outbox entry: %w", err)
	}
	o.skipped.Add(1)
	slog.Error("event outbox skipped an event", "run_id", key.runID, "run_sequence", key.sequence, "event_type", key.what, "class", class)
	return nil
}

// record maps and inserts one envelope. It returns the skip class and cause on
// failure. A panic in the mapper is a failure like any other.
func (o *eventOutbox) record(ctx context.Context, query workflowSQL, key outboxKey, produce mapFunc) (class string, cause error) {
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
	envelope, eventID, ok, err := produce(seq)
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
VALUES (?, ?, ?, ?, ?, ?)`, int64(seq), key.runID, key.sequence, eventID, string(envelope), workflowTime(o.now())); err != nil { // #nosec G115 -- bounded by the table
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
