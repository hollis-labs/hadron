-- Provenance-safe memoization and run-scoped pinned-output bindings.

ALTER TABLE workflow_node_invocations ADD COLUMN outcome_origin TEXT NOT NULL DEFAULT '';
ALTER TABLE workflow_node_invocations ADD COLUMN memo_key_digest TEXT NOT NULL DEFAULT '';

CREATE TABLE workflow_memo_entries (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    cache_key TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    source_node_id TEXT NOT NULL,
    source_iteration TEXT NOT NULL DEFAULT '',
    source_attempt INTEGER NOT NULL CHECK (source_attempt > 0),
    entry_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    UNIQUE (source_run_id, source_node_id, source_iteration, source_attempt),
    FOREIGN KEY (source_run_id, source_node_id, source_iteration, source_attempt)
        REFERENCES workflow_attempts(run_id, node_id, iteration, attempt_number)
);

CREATE INDEX idx_workflow_memo_lookup
    ON workflow_memo_entries(cache_key, created_at DESC, sequence DESC);

CREATE TRIGGER workflow_memo_entries_reject_update
BEFORE UPDATE ON workflow_memo_entries
BEGIN
    SELECT RAISE(ABORT, 'workflow memo entries are append-only');
END;

CREATE TRIGGER workflow_memo_entries_reject_delete
BEFORE DELETE ON workflow_memo_entries
BEGIN
    SELECT RAISE(ABORT, 'workflow memo entries are append-only');
END;

CREATE TABLE workflow_pin_bindings (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, iteration),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);

CREATE TRIGGER workflow_pin_bindings_reject_update
BEFORE UPDATE ON workflow_pin_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow pin bindings are immutable');
END;

CREATE TRIGGER workflow_pin_bindings_reject_delete
BEFORE DELETE ON workflow_pin_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow pin bindings are immutable');
END;

CREATE TABLE workflow_reuse_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TRIGGER workflow_reuse_idempotency_reject_update
BEFORE UPDATE ON workflow_reuse_idempotency
BEGIN
    SELECT RAISE(ABORT, 'workflow reuse outcomes are immutable');
END;

CREATE TRIGGER workflow_reuse_idempotency_reject_delete
BEFORE DELETE ON workflow_reuse_idempotency
BEGIN
    SELECT RAISE(ABORT, 'workflow reuse outcomes are immutable');
END;
