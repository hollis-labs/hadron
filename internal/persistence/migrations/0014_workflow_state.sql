-- Graph-native workflow runtime state. These tables are intentionally separate
-- from the legacy blueprint and pipeline execution tables.

CREATE TABLE workflow_plan_refs (
    digest TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    plan_snapshot_json TEXT,
    source_map_json TEXT,
    source_snapshot_json TEXT
);

CREATE UNIQUE INDEX idx_workflow_plan_refs_identity
    ON workflow_plan_refs(plan_id, version, digest);

CREATE TABLE workflow_runs (
    run_id TEXT PRIMARY KEY,
    plan_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    inputs_ref_json TEXT,
    outputs_ref_json TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (plan_digest) REFERENCES workflow_plan_refs(digest)
);

CREATE INDEX idx_workflow_runs_recovery
    ON workflow_runs(status, created_at, run_id);

CREATE TABLE workflow_run_start_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TABLE workflow_node_invocations (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    blocked_json TEXT,
    inputs_ref_json TEXT,
    outputs_ref_json TEXT,
    wait_id TEXT,
    latest_attempt INTEGER NOT NULL DEFAULT 0 CHECK (latest_attempt >= 0),
    priority INTEGER NOT NULL DEFAULT 0,
    claim_generation INTEGER NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, iteration),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);

CREATE INDEX idx_workflow_nodes_recovery
    ON workflow_node_invocations(status, priority DESC, created_at, run_id, node_id, iteration);

CREATE TABLE workflow_node_leases (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    owner TEXT NOT NULL,
    token TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    expires_at TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, iteration),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
        ON DELETE CASCADE
);

CREATE INDEX idx_workflow_node_leases_expiry
    ON workflow_node_leases(expires_at, run_id, node_id, iteration);

CREATE TABLE workflow_claim_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TABLE workflow_attempts (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    status TEXT NOT NULL,
    executor_json TEXT NOT NULL,
    inputs_ref_json TEXT,
    outputs_ref_json TEXT,
    failure_json TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, iteration, attempt_number),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
        ON DELETE CASCADE
);

CREATE INDEX idx_workflow_attempts_invocation
    ON workflow_attempts(run_id, node_id, iteration, attempt_number);

CREATE TABLE workflow_waits (
    wait_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    resume_values_ref_json TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);

CREATE INDEX idx_workflow_waits_recovery
    ON workflow_waits(status, updated_at, wait_id);

CREATE TABLE workflow_wait_resume_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TABLE workflow_value_sets (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT NOT NULL,
    owner_json TEXT NOT NULL,
    values_json TEXT NOT NULL
);

CREATE INDEX idx_workflow_value_sets_digest
    ON workflow_value_sets(digest, sequence);

CREATE TABLE workflow_event_sequences (
    run_id TEXT PRIMARY KEY,
    last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0)
);

CREATE TABLE workflow_events (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    invocation_json TEXT,
    attempt_json TEXT,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    attributes_json TEXT,
    values_ref_json TEXT,
    redaction TEXT NOT NULL,
    retention TEXT NOT NULL,
    PRIMARY KEY (run_id, sequence)
);

CREATE INDEX idx_workflow_events_type
    ON workflow_events(event_type, occurred_at, run_id, sequence);

CREATE TRIGGER workflow_events_reject_update
BEFORE UPDATE ON workflow_events
BEGIN
    SELECT RAISE(ABORT, 'workflow events are append-only');
END;

CREATE TRIGGER workflow_events_reject_delete
BEFORE DELETE ON workflow_events
BEGIN
    SELECT RAISE(ABORT, 'workflow events are append-only');
END;

CREATE TABLE workflow_cache_entries (
    cache_key TEXT PRIMARY KEY,
    plan_digest TEXT NOT NULL,
    node_id TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    outputs_ref_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE INDEX idx_workflow_cache_expiry
    ON workflow_cache_entries(expires_at, cache_key);

CREATE TABLE workflow_pinned_values (
    pin_key TEXT PRIMARY KEY,
    value_ref_json TEXT NOT NULL,
    pinned_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE INDEX idx_workflow_pins_expiry
    ON workflow_pinned_values(expires_at, pin_key);

CREATE TABLE workflow_external_activations (
    idempotency_key TEXT PRIMARY KEY,
    activation_id TEXT NOT NULL,
    requested_run_id TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE INDEX idx_workflow_activations_run
    ON workflow_external_activations(requested_run_id, activation_id);

CREATE INDEX idx_workflow_activations_registration
    ON workflow_external_activations(activation_id, idempotency_key);
