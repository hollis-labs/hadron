CREATE INDEX idx_workflow_waits_named_signal
    ON workflow_waits(
        run_id,
        status,
        json_extract(record_json, '$.signal_name'),
        json_extract(record_json, '$.correlation'),
        wait_id
    );

CREATE TABLE workflow_run_updates (
    idempotency_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    correlation TEXT NOT NULL,
    wait_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'applied', 'closed')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    request_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (wait_id) REFERENCES workflow_waits(wait_id) ON DELETE RESTRICT
);

CREATE INDEX idx_workflow_run_updates_recovery
    ON workflow_run_updates(status, updated_at, idempotency_key);
CREATE INDEX idx_workflow_run_updates_run
    ON workflow_run_updates(run_id, created_at, idempotency_key);

CREATE TABLE workflow_non_durable_runs (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_digest TEXT NOT NULL,
    record_json TEXT NOT NULL,
    completed_at TEXT NOT NULL
);

CREATE INDEX idx_workflow_non_durable_runs_completed
    ON workflow_non_durable_runs(completed_at, run_id);

CREATE TABLE workflow_reactors (
    reactor_id TEXT PRIMARY KEY,
    registration_id TEXT NOT NULL,
    registration_generation INTEGER NOT NULL CHECK (registration_generation > 0),
    correlation TEXT NOT NULL,
    current_generation INTEGER NOT NULL CHECK (current_generation > 0),
    current_run_id TEXT NOT NULL UNIQUE,
    continue_after_events INTEGER NOT NULL CHECK (continue_after_events > 0 AND continue_after_events <= 1000000),
    event_count INTEGER NOT NULL CHECK (event_count >= 0),
    status TEXT NOT NULL CHECK (status IN ('starting', 'waiting', 'rolling', 'failed', 'closed')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (registration_id, registration_generation, correlation)
);

CREATE TABLE workflow_reactor_generations (
    reactor_id TEXT NOT NULL,
    reactor_generation INTEGER NOT NULL CHECK (reactor_generation > 0),
    run_id TEXT NOT NULL UNIQUE,
    plan_digest TEXT NOT NULL,
    provenance_digest TEXT NOT NULL,
    state_ref_json TEXT,
    event_count INTEGER NOT NULL CHECK (event_count >= 0),
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (reactor_id, reactor_generation),
    FOREIGN KEY (reactor_id) REFERENCES workflow_reactors(reactor_id) ON DELETE RESTRICT
);

CREATE TABLE workflow_reactor_deliveries (
    reactor_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    reactor_generation INTEGER NOT NULL CHECK (reactor_generation > 0),
    run_id TEXT NOT NULL,
	starts_generation INTEGER NOT NULL CHECK (starts_generation IN (0, 1)),
	claimed_wait_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'applying', 'applied', 'closed')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    request_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (reactor_id, idempotency_key),
    FOREIGN KEY (reactor_id, reactor_generation)
        REFERENCES workflow_reactor_generations(reactor_id, reactor_generation)
        ON DELETE RESTRICT
);

CREATE INDEX idx_workflow_reactor_deliveries_recovery
    ON workflow_reactor_deliveries(status, updated_at, reactor_id, idempotency_key);

CREATE TABLE workflow_reactor_continuations (
    idempotency_key TEXT PRIMARY KEY,
    reactor_id TEXT NOT NULL,
    from_generation INTEGER NOT NULL CHECK (from_generation > 0),
    to_generation INTEGER NOT NULL CHECK (to_generation > 0),
    from_run_id TEXT NOT NULL UNIQUE,
    to_run_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('pending', 'started', 'completed')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    request_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (reactor_id) REFERENCES workflow_reactors(reactor_id) ON DELETE RESTRICT
);

CREATE INDEX idx_workflow_reactor_continuations_recovery
    ON workflow_reactor_continuations(status, updated_at, idempotency_key);

CREATE TABLE workflow_failure_hooks (
    source_run_id TEXT PRIMARY KEY,
    handler_run_id TEXT NOT NULL UNIQUE,
    depth INTEGER NOT NULL CHECK (depth >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'starting', 'started', 'suppressed', 'failed')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    request_json TEXT NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_workflow_failure_hooks_recovery
    ON workflow_failure_hooks(status, updated_at, source_run_id);
