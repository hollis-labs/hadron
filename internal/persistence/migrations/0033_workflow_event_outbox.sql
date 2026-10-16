-- Local outbox of event-stream envelopes derived from workflow_events, written in
-- the same transaction as the event (CW-20261016-0141 S2). Empty unless the
-- exporter is enabled. Additive; migrations here are forward-only, so to roll
-- back run DROP TABLE workflow_event_outbox; DROP TABLE workflow_event_outbox_counter;
-- (nothing else references them).
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
