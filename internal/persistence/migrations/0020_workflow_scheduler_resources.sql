-- Cross-process scheduler resource definitions, lease-fenced holders, and
-- diagnostic waiters. Capacity ownership is committed atomically with the
-- workflow node claim that supplies claim_generation and lease expiry.

CREATE TABLE workflow_scheduler_resources (
    resource_key TEXT PRIMARY KEY,
    limit_value INTEGER NOT NULL CHECK (limit_value > 0),
    resource_json TEXT NOT NULL
);

CREATE TABLE workflow_scheduler_holders (
    resource_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    units INTEGER NOT NULL CHECK (units > 0),
    claim_generation INTEGER NOT NULL CHECK (claim_generation > 0),
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY (resource_key, run_id, node_id, iteration),
    FOREIGN KEY (resource_key) REFERENCES workflow_scheduler_resources(resource_key),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);

CREATE INDEX idx_workflow_scheduler_holders_capacity
    ON workflow_scheduler_holders(resource_key, expires_at, run_id, node_id, iteration);

CREATE INDEX idx_workflow_scheduler_holders_invocation
    ON workflow_scheduler_holders(run_id, node_id, iteration, expires_at);

CREATE TABLE workflow_scheduler_waiters (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL,
    enqueued_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, iteration),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);

CREATE INDEX idx_workflow_scheduler_waiters_order
    ON workflow_scheduler_waiters(enqueued_at, priority DESC, run_id, node_id, iteration);

CREATE TABLE workflow_scheduler_admission_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

-- One immutable winning fail-fast trigger per run. The referenced
-- workflow_terminal_intents row remains the sole admission fence and cleanup
-- owner; this row makes concurrent trigger convergence inspectable.
CREATE TABLE workflow_run_policy_decisions (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);

CREATE TRIGGER workflow_scheduler_resources_immutable
BEFORE UPDATE ON workflow_scheduler_resources BEGIN
    SELECT RAISE(ABORT, 'workflow scheduler resource definitions are immutable');
END;

CREATE TRIGGER workflow_scheduler_resources_reject_delete
BEFORE DELETE ON workflow_scheduler_resources BEGIN
    SELECT RAISE(ABORT, 'workflow scheduler resource definitions are durable configuration');
END;

CREATE TRIGGER workflow_run_policy_decisions_immutable
BEFORE UPDATE ON workflow_run_policy_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow run policy decisions are immutable');
END;

CREATE TRIGGER workflow_run_policy_decisions_reject_delete
BEFORE DELETE ON workflow_run_policy_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow run policy decisions are durable history');
END;
