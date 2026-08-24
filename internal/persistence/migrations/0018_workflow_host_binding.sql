-- Hadron-owned graph workflow host journals. Immutable semantic requests are
-- separated from mutable recovery checkpoints; core runtime tables remain
-- application-neutral.

CREATE TABLE workflow_host_starts (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE workflow_host_start_progress (
    run_id TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    updated_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_host_starts(run_id)
);

CREATE INDEX idx_workflow_host_start_recovery
    ON workflow_host_start_progress(phase, updated_at, run_id);

CREATE TABLE workflow_host_policy_decisions (
    decision_id TEXT PRIMARY KEY,
    start_key TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL,
    decision_json TEXT NOT NULL,
    decided_at TEXT NOT NULL
);

CREATE INDEX idx_workflow_host_policy_run
    ON workflow_host_policy_decisions(run_id, decided_at, decision_id);

CREATE TABLE workflow_host_cancellations (
    idempotency_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    request_json TEXT NOT NULL,
    binding_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE INDEX idx_workflow_host_cancellation_recovery
    ON workflow_host_cancellations(recorded_at, idempotency_key);

CREATE TABLE workflow_call_resolutions (
    resolution_key TEXT PRIMARY KEY,
    parent_run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    record_json TEXT NOT NULL,
    event_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE INDEX idx_workflow_call_resolutions_parent
    ON workflow_call_resolutions(parent_run_id, node_id, iteration);

CREATE TABLE workflow_child_run_start_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    child_run_id TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TRIGGER workflow_host_starts_reject_update
BEFORE UPDATE ON workflow_host_starts BEGIN
    SELECT RAISE(ABORT, 'workflow host starts are immutable');
END;

CREATE TRIGGER workflow_host_starts_reject_delete
BEFORE DELETE ON workflow_host_starts BEGIN
    SELECT RAISE(ABORT, 'workflow host starts are durable history');
END;

CREATE TRIGGER workflow_host_policy_reject_update
BEFORE UPDATE ON workflow_host_policy_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow host policy decisions are append-only');
END;

CREATE TRIGGER workflow_host_policy_reject_delete
BEFORE DELETE ON workflow_host_policy_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow host policy decisions are durable history');
END;

CREATE TRIGGER workflow_host_cancellations_reject_update
BEFORE UPDATE ON workflow_host_cancellations BEGIN
    SELECT RAISE(ABORT, 'workflow host cancellations are immutable');
END;

CREATE TRIGGER workflow_host_cancellations_reject_delete
BEFORE DELETE ON workflow_host_cancellations BEGIN
    SELECT RAISE(ABORT, 'workflow host cancellations are durable history');
END;

CREATE TRIGGER workflow_call_resolutions_reject_update
BEFORE UPDATE ON workflow_call_resolutions BEGIN
    SELECT RAISE(ABORT, 'workflow call resolutions are immutable');
END;

CREATE TRIGGER workflow_call_resolutions_reject_delete
BEFORE DELETE ON workflow_call_resolutions BEGIN
    SELECT RAISE(ABORT, 'workflow call resolutions are durable history');
END;

CREATE TRIGGER workflow_child_run_starts_reject_update
BEFORE UPDATE ON workflow_child_run_start_idempotency BEGIN
    SELECT RAISE(ABORT, 'workflow child run starts are immutable');
END;

CREATE TRIGGER workflow_child_run_starts_reject_delete
BEFORE DELETE ON workflow_child_run_start_idempotency BEGIN
    SELECT RAISE(ABORT, 'workflow child run starts are durable history');
END;
