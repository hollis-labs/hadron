-- Local outbox of event-stream envelopes derived from workflow_events, written in
-- the same transaction as the event (CW-20261016-0141 S2). Empty unless the
-- exporter is enabled. Additive; migrations here are forward-only, so to roll
-- back run DROP TABLE workflow_event_outbox; DROP TABLE workflow_event_outbox_counter;
-- DROP INDEX workflow_event_outbox_skipped_open;
-- DROP TABLE workflow_event_outbox_skipped;
-- (nothing else references them).
-- run_id and run_sequence name the source of an envelope. For a workflow event they
-- are the run and its per-run workflow_events sequence (which starts at 1). For an
-- admission policy decision (CW-0237) run_id holds the DECISION id and run_sequence is
-- 0, which no workflow event can take; the skipped table below uses the same key, so a
-- repair pass must re-derive a sequence-0 row from the policy decision
-- (workflow_host_policy_decisions), not from workflow_events.
CREATE TABLE workflow_event_outbox (
    outbox_seq INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL,
    run_sequence INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    envelope_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (run_id, run_sequence)
);
-- The producer-wide envelope sequence. A counter row, not MAX+1, so trimming the
-- tail of the outbox can never make a sequence number come back.
CREATE TABLE workflow_event_outbox_counter (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_seq INTEGER NOT NULL
);
INSERT INTO workflow_event_outbox_counter(id, last_seq) VALUES (1, 0);
-- An event whose envelope could not be produced or stored. Recording never rejects
-- a valid mutation: the workflow event commits and the failure is recorded here
-- (and logged and counted), so a repair or a backfill knows what to redo.
CREATE TABLE workflow_event_outbox_skipped (
    run_id TEXT NOT NULL,
    run_sequence INTEGER NOT NULL,
    error_class TEXT NOT NULL,
    created_at TEXT NOT NULL,
    -- NULL until a repair pass has re-exported the event; the open rows are the
    -- repair worklist.
    repaired_at TEXT,
    PRIMARY KEY (run_id, run_sequence)
);
CREATE INDEX workflow_event_outbox_skipped_open ON workflow_event_outbox_skipped(created_at) WHERE repaired_at IS NULL;
