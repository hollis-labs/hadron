-- Restart-durable crash reconciliation and replay provenance. Replay-created
-- runs and invocations remain in the canonical graph-native state tables.

CREATE TABLE workflow_crash_recovery_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    FOREIGN KEY (run_id, node_id, iteration, attempt_number)
        REFERENCES workflow_attempts(run_id, node_id, iteration, attempt_number)
);

CREATE INDEX idx_workflow_crash_recovery_attempt
    ON workflow_crash_recovery_idempotency(run_id, node_id, iteration, attempt_number);

CREATE TABLE workflow_replay_provenance (
    run_id TEXT PRIMARY KEY,
    source_run_id TEXT NOT NULL,
    from_node_id TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    request_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
    FOREIGN KEY (source_run_id) REFERENCES workflow_runs(run_id)
);

CREATE INDEX idx_workflow_replay_source
    ON workflow_replay_provenance(source_run_id, created_at, run_id);

CREATE TRIGGER workflow_replay_provenance_reject_update
BEFORE UPDATE ON workflow_replay_provenance
BEGIN
    SELECT RAISE(ABORT, 'workflow replay provenance is immutable');
END;

CREATE TABLE workflow_node_input_bindings (
    idempotency_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    UNIQUE (run_id, node_id, iteration),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);

CREATE TRIGGER workflow_node_input_bindings_reject_update
BEFORE UPDATE ON workflow_node_input_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow node input binding is immutable');
END;

CREATE TRIGGER workflow_node_input_bindings_reject_delete
BEFORE DELETE ON workflow_node_input_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow node input binding is immutable');
END;

CREATE TRIGGER workflow_crash_recovery_reject_update
BEFORE UPDATE ON workflow_crash_recovery_idempotency
BEGIN
    SELECT RAISE(ABORT, 'workflow crash recovery fact is immutable');
END;

CREATE TRIGGER workflow_crash_recovery_reject_delete
BEFORE DELETE ON workflow_crash_recovery_idempotency
BEGIN
    SELECT RAISE(ABORT, 'workflow crash recovery fact is immutable');
END;

CREATE TRIGGER workflow_replay_provenance_reject_delete
BEFORE DELETE ON workflow_replay_provenance
BEGIN
    SELECT RAISE(ABORT, 'workflow replay provenance is immutable');
END;
