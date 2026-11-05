-- The Hadron schema, squashed to one migration (decision: clean break, 2026-11-04).
--
-- Source: the final state of the old hollis-labs/hadron migrations 0001-0033 applied in
-- order, restricted to the workflow engine host's durable state (the workflow_* family
-- and store_generation). Everything else the old schema held is either archive
-- (blueprint_*, pipeline_*, human_gates, legacy_schedule_*) and is NOT carried forward,
-- or belongs to a subsystem that is added when it is ported (schedules, triggers,
-- messages, workspaces, runs, run_events); see docs/status.md.
--
-- Until the first release this is the ONLY migration: a schema change edits this file,
-- because there are no deployed databases to migrate. After the first release changes
-- are new numbered files. Triggers carry semicolons, so every statement is bracketed.

-- +goose Up

-- +goose StatementBegin
CREATE TABLE workflow_plan_refs (
    digest TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    plan_snapshot_json TEXT,
    source_map_json TEXT,
    source_snapshot_json TEXT
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE UNIQUE INDEX idx_workflow_plan_refs_identity
    ON workflow_plan_refs(plan_id, version, digest);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_runs_recovery
    ON workflow_runs(status, created_at, run_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_run_start_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
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
    updated_at TEXT NOT NULL, outcome_origin TEXT NOT NULL DEFAULT '', memo_key_digest TEXT NOT NULL DEFAULT '', phase TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, node_id, iteration),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_nodes_recovery
    ON workflow_node_invocations(status, priority DESC, created_at, run_id, node_id, iteration);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_node_leases_expiry
    ON workflow_node_leases(expires_at, run_id, node_id, iteration);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_claim_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_attempts_invocation
    ON workflow_attempts(run_id, node_id, iteration, attempt_number);
-- +goose StatementEnd

-- +goose StatementBegin
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
    resolved_at TEXT, record_json TEXT, deadline TEXT,
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_waits_recovery
    ON workflow_waits(status, updated_at, wait_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_wait_resume_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_value_sets (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    digest TEXT NOT NULL,
    owner_json TEXT NOT NULL,
    values_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_value_sets_digest
    ON workflow_value_sets(digest, sequence);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_event_sequences (
    run_id TEXT PRIMARY KEY,
    last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0)
);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_events_type
    ON workflow_events(event_type, occurred_at, run_id, sequence);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_events_reject_update
BEFORE UPDATE ON workflow_events
BEGIN
    SELECT RAISE(ABORT, 'workflow events are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_events_reject_delete
BEFORE DELETE ON workflow_events
BEGIN
    SELECT RAISE(ABORT, 'workflow events are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_cache_entries (
    cache_key TEXT PRIMARY KEY,
    plan_digest TEXT NOT NULL,
    node_id TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    outputs_ref_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_cache_expiry
    ON workflow_cache_entries(expires_at, cache_key);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_pinned_values (
    pin_key TEXT PRIMARY KEY,
    value_ref_json TEXT NOT NULL,
    pinned_at TEXT NOT NULL,
    expires_at TEXT
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_pins_expiry
    ON workflow_pinned_values(expires_at, pin_key);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_external_activations (
    idempotency_key TEXT PRIMARY KEY,
    activation_id TEXT NOT NULL,
    requested_run_id TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_activations_run
    ON workflow_external_activations(requested_run_id, activation_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_activations_registration
    ON workflow_external_activations(activation_id, idempotency_key);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_waits_deadline
    ON workflow_waits(status, deadline, created_at, wait_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_waits_correlation
    ON workflow_waits(status, json_extract(record_json, '$.correlation'), wait_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_wait_suspend_idempotency (
    wait_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_wait_resume_results (
    wait_id TEXT PRIMARY KEY,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_wait_timeout_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_wait_attempt_bindings (
    wait_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    UNIQUE (run_id, node_id, iteration, attempt_number),
    FOREIGN KEY (wait_id) REFERENCES workflow_waits(wait_id),
    FOREIGN KEY (run_id, node_id, iteration, attempt_number)
        REFERENCES workflow_attempts(run_id, node_id, iteration, attempt_number)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_wait_attempt_lookup
    ON workflow_wait_attempt_bindings(run_id, node_id, iteration, attempt_number, wait_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_external_operations (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    ref_json TEXT NOT NULL,
    invocation_json TEXT NOT NULL,
    status TEXT NOT NULL,
    progress_json TEXT,
    outputs_ref_json TEXT,
    failure_json TEXT,
    cancel_requested_at TEXT,
    last_observed_at TEXT,
    last_heartbeat_at TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, iteration, attempt_number),
    FOREIGN KEY (run_id, node_id, iteration, attempt_number)
        REFERENCES workflow_attempts(run_id, node_id, iteration, attempt_number)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_external_operations_recovery
    ON workflow_external_operations(status, updated_at, run_id, node_id, iteration, attempt_number);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_external_operations_cancel
    ON workflow_external_operations(status, cancel_requested_at, updated_at);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_external_operations_immutable_binding
BEFORE UPDATE ON workflow_external_operations
WHEN OLD.run_id <> NEW.run_id
  OR OLD.node_id <> NEW.node_id
  OR OLD.iteration <> NEW.iteration
  OR OLD.attempt_number <> NEW.attempt_number
  OR OLD.ref_json <> NEW.ref_json
  OR OLD.invocation_json <> NEW.invocation_json
BEGIN
    SELECT RAISE(ABORT, 'workflow external operation binding is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_external_operations_reject_delete
BEFORE DELETE ON workflow_external_operations
BEGIN
    SELECT RAISE(ABORT, 'workflow external operations are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_retry_activations (
    activation_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    status TEXT NOT NULL,
    fire_at TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    snapshot_json TEXT NOT NULL,
    UNIQUE (run_id, node_id, iteration, attempt_number),
    FOREIGN KEY (run_id, node_id, iteration, attempt_number)
        REFERENCES workflow_attempts(run_id, node_id, iteration, attempt_number)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_retry_activations_recovery
    ON workflow_retry_activations(status, fire_at, activation_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_retry_activation_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_fanouts (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '' CHECK (iteration = ''),
    status TEXT NOT NULL,
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency >= 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    snapshot_json TEXT NOT NULL, fail_fast INTEGER NOT NULL DEFAULT 0 CHECK (fail_fast IN (0, 1)),
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_fanouts_status
    ON workflow_fanouts(status, run_id, node_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_fanout_items (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    item_index INTEGER NOT NULL CHECK (item_index >= 0),
    iteration TEXT NOT NULL,
    inputs_ref_json TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, item_index),
    UNIQUE (run_id, node_id, iteration),
    FOREIGN KEY (run_id, node_id)
        REFERENCES workflow_fanouts(run_id, node_id),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_fanout_items_invocation
    ON workflow_fanout_items(run_id, node_id, iteration, item_index);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_child_runs (
    parent_run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    child_run_id TEXT NOT NULL,
    policy TEXT NOT NULL,
    created_at TEXT NOT NULL,
    link_json TEXT NOT NULL,
    PRIMARY KEY (parent_run_id, node_id, iteration),
    UNIQUE (child_run_id),
    FOREIGN KEY (parent_run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration),
    FOREIGN KEY (child_run_id) REFERENCES workflow_runs(run_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_child_runs_parent
    ON workflow_child_runs(parent_run_id, child_run_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_cancellation_intents (
    intent_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    snapshot_json TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_cancellation_intents_recovery
    ON workflow_cancellation_intents(status, requested_at, intent_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_run_cancellation_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_retry_activations_immutable_attempt
BEFORE UPDATE ON workflow_retry_activations
WHEN OLD.run_id <> NEW.run_id
  OR OLD.node_id <> NEW.node_id
  OR OLD.iteration <> NEW.iteration
  OR OLD.attempt_number <> NEW.attempt_number
BEGIN
    SELECT RAISE(ABORT, 'workflow retry activation attempt is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_fanout_items_immutable
BEFORE UPDATE ON workflow_fanout_items
BEGIN
    SELECT RAISE(ABORT, 'workflow fan-out item bindings are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_fanout_items_reject_delete
BEFORE DELETE ON workflow_fanout_items
BEGIN
    SELECT RAISE(ABORT, 'workflow fan-out item bindings are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_child_runs_immutable
BEFORE UPDATE ON workflow_child_runs
BEGIN
    SELECT RAISE(ABORT, 'workflow child run links are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_child_runs_reject_delete
BEFORE DELETE ON workflow_child_runs
BEGIN
    SELECT RAISE(ABORT, 'workflow child run links are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_host_starts (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_host_start_progress (
    run_id TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    updated_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_host_starts(run_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_host_start_recovery
    ON workflow_host_start_progress(phase, updated_at, run_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_host_policy_decisions (
    decision_id TEXT PRIMARY KEY,
    start_key TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL,
    decision_json TEXT NOT NULL,
    decided_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_host_policy_run
    ON workflow_host_policy_decisions(run_id, decided_at, decision_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_host_cancellations (
    idempotency_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    request_json TEXT NOT NULL,
    binding_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_host_cancellation_recovery
    ON workflow_host_cancellations(recorded_at, idempotency_key);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_call_resolutions (
    resolution_key TEXT PRIMARY KEY,
    parent_run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    record_json TEXT NOT NULL,
    event_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_call_resolutions_parent
    ON workflow_call_resolutions(parent_run_id, node_id, iteration);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_child_run_start_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    child_run_id TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_host_starts_reject_update
BEFORE UPDATE ON workflow_host_starts BEGIN
    SELECT RAISE(ABORT, 'workflow host starts are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_host_starts_reject_delete
BEFORE DELETE ON workflow_host_starts BEGIN
    SELECT RAISE(ABORT, 'workflow host starts are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_host_policy_reject_update
BEFORE UPDATE ON workflow_host_policy_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow host policy decisions are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_host_policy_reject_delete
BEFORE DELETE ON workflow_host_policy_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow host policy decisions are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_host_cancellations_reject_update
BEFORE UPDATE ON workflow_host_cancellations BEGIN
    SELECT RAISE(ABORT, 'workflow host cancellations are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_host_cancellations_reject_delete
BEFORE DELETE ON workflow_host_cancellations BEGIN
    SELECT RAISE(ABORT, 'workflow host cancellations are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_call_resolutions_reject_update
BEFORE UPDATE ON workflow_call_resolutions BEGIN
    SELECT RAISE(ABORT, 'workflow call resolutions are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_call_resolutions_reject_delete
BEFORE DELETE ON workflow_call_resolutions BEGIN
    SELECT RAISE(ABORT, 'workflow call resolutions are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_child_run_starts_reject_update
BEFORE UPDATE ON workflow_child_run_start_idempotency BEGIN
    SELECT RAISE(ABORT, 'workflow child run starts are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_child_run_starts_reject_delete
BEFORE DELETE ON workflow_child_run_start_idempotency BEGIN
    SELECT RAISE(ABORT, 'workflow child run starts are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_control_decisions (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    outcome TEXT NOT NULL,
    source_generation INTEGER NOT NULL CHECK (source_generation > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at TEXT NOT NULL,
    error_values_sequence INTEGER,
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, iteration, kind),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration),
    FOREIGN KEY (error_values_sequence) REFERENCES workflow_value_sets(sequence)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_control_decisions_run
    ON workflow_control_decisions(run_id, node_id, iteration, kind);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_terminal_intents (
    run_id TEXT PRIMARY KEY,
    intended_status TEXT NOT NULL,
    status TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    error_values_sequence INTEGER,
    immutable_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
    FOREIGN KEY (error_values_sequence) REFERENCES workflow_value_sets(sequence)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_terminal_intents_recovery
    ON workflow_terminal_intents(status, updated_at, run_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_control_cancellation_trees (
    root_run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (root_run_id) REFERENCES workflow_runs(run_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_control_decisions_reject_update
BEFORE UPDATE ON workflow_control_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow control decisions are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_control_decisions_reject_delete
BEFORE DELETE ON workflow_control_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow control decisions are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_terminal_intents_immutable_fields
BEFORE UPDATE ON workflow_terminal_intents
WHEN OLD.run_id <> NEW.run_id
  OR OLD.intended_status <> NEW.intended_status
  OR OLD.idempotency_key <> NEW.idempotency_key
  OR OLD.created_at <> NEW.created_at
  OR COALESCE(OLD.error_values_sequence, -1) <> COALESCE(NEW.error_values_sequence, -1)
  OR OLD.immutable_json <> NEW.immutable_json
BEGIN
    SELECT RAISE(ABORT, 'workflow terminal intent identity is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_terminal_intents_valid_transition
BEFORE UPDATE ON workflow_terminal_intents
WHEN OLD.status <> 'pending'
  OR NEW.status <> 'completed'
  OR NEW.generation <> OLD.generation + 1
  OR NEW.completed_at IS NULL
  OR NEW.completed_at <> NEW.updated_at
BEGIN
    SELECT RAISE(ABORT, 'workflow terminal intent transition is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_terminal_intents_reject_delete
BEFORE DELETE ON workflow_terminal_intents BEGIN
    SELECT RAISE(ABORT, 'workflow terminal intents are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_control_cancellation_trees_reject_update
BEFORE UPDATE ON workflow_control_cancellation_trees BEGIN
    SELECT RAISE(ABORT, 'workflow control cancellation trees are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_control_cancellation_trees_reject_delete
BEFORE DELETE ON workflow_control_cancellation_trees BEGIN
    SELECT RAISE(ABORT, 'workflow control cancellation trees are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_scheduler_resources (
    resource_key TEXT PRIMARY KEY,
    limit_value INTEGER NOT NULL CHECK (limit_value > 0),
    resource_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_scheduler_holders_capacity
    ON workflow_scheduler_holders(resource_key, expires_at, run_id, node_id, iteration);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_scheduler_holders_invocation
    ON workflow_scheduler_holders(run_id, node_id, iteration, expires_at);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_scheduler_waiters_order
    ON workflow_scheduler_waiters(enqueued_at, priority DESC, run_id, node_id, iteration);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_scheduler_admission_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_run_policy_decisions (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_scheduler_resources_immutable
BEFORE UPDATE ON workflow_scheduler_resources BEGIN
    SELECT RAISE(ABORT, 'workflow scheduler resource definitions are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_scheduler_resources_reject_delete
BEFORE DELETE ON workflow_scheduler_resources BEGIN
    SELECT RAISE(ABORT, 'workflow scheduler resource definitions are durable configuration');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_run_policy_decisions_immutable
BEFORE UPDATE ON workflow_run_policy_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow run policy decisions are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_run_policy_decisions_reject_delete
BEFORE DELETE ON workflow_run_policy_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow run policy decisions are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_crash_recovery_attempt
    ON workflow_crash_recovery_idempotency(run_id, node_id, iteration, attempt_number);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_replay_source
    ON workflow_replay_provenance(source_run_id, created_at, run_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_replay_provenance_reject_update
BEFORE UPDATE ON workflow_replay_provenance
BEGIN
    SELECT RAISE(ABORT, 'workflow replay provenance is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_node_input_bindings_reject_update
BEFORE UPDATE ON workflow_node_input_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow node input binding is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_node_input_bindings_reject_delete
BEFORE DELETE ON workflow_node_input_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow node input binding is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_crash_recovery_reject_update
BEFORE UPDATE ON workflow_crash_recovery_idempotency
BEGIN
    SELECT RAISE(ABORT, 'workflow crash recovery fact is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_crash_recovery_reject_delete
BEFORE DELETE ON workflow_crash_recovery_idempotency
BEGIN
    SELECT RAISE(ABORT, 'workflow crash recovery fact is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_replay_provenance_reject_delete
BEFORE DELETE ON workflow_replay_provenance
BEGIN
    SELECT RAISE(ABORT, 'workflow replay provenance is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_memo_lookup
    ON workflow_memo_entries(cache_key, created_at DESC, sequence DESC);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_memo_entries_reject_update
BEFORE UPDATE ON workflow_memo_entries
BEGIN
    SELECT RAISE(ABORT, 'workflow memo entries are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_memo_entries_reject_delete
BEFORE DELETE ON workflow_memo_entries
BEGIN
    SELECT RAISE(ABORT, 'workflow memo entries are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_pin_bindings_reject_update
BEFORE UPDATE ON workflow_pin_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow pin bindings are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_pin_bindings_reject_delete
BEFORE DELETE ON workflow_pin_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow pin bindings are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_reuse_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_reuse_idempotency_reject_update
BEFORE UPDATE ON workflow_reuse_idempotency
BEGIN
    SELECT RAISE(ABORT, 'workflow reuse outcomes are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_reuse_idempotency_reject_delete
BEFORE DELETE ON workflow_reuse_idempotency
BEGIN
    SELECT RAISE(ABORT, 'workflow reuse outcomes are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_activation_registrations (
    registration_id TEXT PRIMARY KEY,
    version TEXT NOT NULL,
    source_kind TEXT NOT NULL,
	 scope_key TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    expires_at TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    registration_json BLOB NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_activation_registrations_due
    ON workflow_activation_registrations(enabled, expires_at, source_kind, registration_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_activation_registrations_scope
    ON workflow_activation_registrations(scope_key, registration_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_activation_schedules (
    registration_id TEXT PRIMARY KEY,
    cron_expr TEXT NOT NULL,
    last_run_at TEXT,
    next_run_at TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    generation INTEGER NOT NULL CHECK (generation > 0),
    retry_json BLOB NOT NULL,
    payload_json BLOB NOT NULL,
    FOREIGN KEY (registration_id) REFERENCES workflow_activation_registrations(registration_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_activation_schedules_due
    ON workflow_activation_schedules(enabled, next_run_at, registration_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_activation_fires (
    fire_id TEXT PRIMARY KEY,
    registration_id TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    fired_at TEXT,
    attempt INTEGER NOT NULL CHECK (attempt >= 0),
    status TEXT NOT NULL,
    next_attempt_at TEXT,
    claim_expires_at TEXT,
    last_error_code TEXT,
    retry_json BLOB NOT NULL,
    job_type TEXT NOT NULL,
    payload_json BLOB NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    FOREIGN KEY (registration_id) REFERENCES workflow_activation_registrations(registration_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_activation_fires_due
    ON workflow_activation_fires(status, next_attempt_at, fire_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_activation_fires_registration
    ON workflow_activation_fires(registration_id, scheduled_at, fire_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_activation_attempts (
    fire_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    claimed_at TEXT NOT NULL,
    claim_expires_at TEXT NOT NULL,
    PRIMARY KEY (fire_id, attempt),
    FOREIGN KEY (fire_id) REFERENCES workflow_activation_fires(fire_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_activation_attempt_results (
    fire_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    outcome TEXT NOT NULL,
    reason_code TEXT,
    completed_at TEXT NOT NULL,
    PRIMARY KEY (fire_id, attempt),
    FOREIGN KEY (fire_id, attempt) REFERENCES workflow_activation_attempts(fire_id, attempt)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_activation_dispatches (
    fire_id TEXT PRIMARY KEY,
    registration_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    status TEXT NOT NULL,
    logical_run_id TEXT NOT NULL,
    physical_run_id TEXT,
    host_start_key TEXT,
    scheduled_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    reason_code TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    dispatch_json BLOB NOT NULL,
    FOREIGN KEY (fire_id) REFERENCES workflow_activation_fires(fire_id),
    FOREIGN KEY (registration_id) REFERENCES workflow_activation_registrations(registration_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_activation_dispatches_overlap
    ON workflow_activation_dispatches(registration_id, status, logical_run_id, fire_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_activation_external_events (
    registration_id TEXT NOT NULL,
	 idempotency_key TEXT NOT NULL,
    event_digest TEXT NOT NULL,
    fire_id TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL,
    event_json BLOB NOT NULL,
	 PRIMARY KEY (registration_id, idempotency_key),
	 FOREIGN KEY (registration_id) REFERENCES workflow_activation_registrations(registration_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_activation_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    registration_id TEXT,
    fire_id TEXT,
    attempt INTEGER,
    kind TEXT NOT NULL,
    reason_code TEXT,
    occurred_at TEXT NOT NULL,
    event_json BLOB NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_activation_events_registration
    ON workflow_activation_events(registration_id, sequence);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_activation_events_fire
    ON workflow_activation_events(fire_id, sequence);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_callback_registrations (
    callback_id TEXT PRIMARY KEY,
    wait_id TEXT NOT NULL,
    correlation TEXT NOT NULL,
    credential_digest TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    registration_json BLOB NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_callback_registrations_wait
    ON workflow_callback_registrations(wait_id, callback_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_callback_deliveries (
    callback_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    received_at TEXT NOT NULL,
    completed_at TEXT,
    outcome TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    PRIMARY KEY (callback_id, idempotency_key),
    FOREIGN KEY (callback_id) REFERENCES workflow_callback_registrations(callback_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_activation_events_no_update
BEFORE UPDATE ON workflow_activation_events
BEGIN
    SELECT RAISE(ABORT, 'workflow activation events are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_activation_events_no_delete
BEFORE DELETE ON workflow_activation_events
BEGIN
    SELECT RAISE(ABORT, 'workflow activation events are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_activation_attempts_no_update
BEFORE UPDATE ON workflow_activation_attempts BEGIN
    SELECT RAISE(ABORT, 'workflow activation attempts are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_activation_attempts_no_delete
BEFORE DELETE ON workflow_activation_attempts BEGIN
    SELECT RAISE(ABORT, 'workflow activation attempts are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_activation_attempt_results_no_update
BEFORE UPDATE ON workflow_activation_attempt_results BEGIN
    SELECT RAISE(ABORT, 'workflow activation attempt results are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_activation_attempt_results_no_delete
BEFORE DELETE ON workflow_activation_attempt_results BEGIN
    SELECT RAISE(ABORT, 'workflow activation attempt results are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_activation_external_events_no_update
BEFORE UPDATE ON workflow_activation_external_events BEGIN
    SELECT RAISE(ABORT, 'workflow activation external events are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_activation_external_events_no_delete
BEFORE DELETE ON workflow_activation_external_events BEGIN
    SELECT RAISE(ABORT, 'workflow activation external events are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_services (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('launching', 'starting', 'ready', 'stopping', 'stopped', 'failed')),
    generation INTEGER NOT NULL CHECK (generation > 0),
    updated_at TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id, iteration),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
        ON DELETE RESTRICT
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_services_recovery
    ON workflow_services(status, updated_at, run_id, node_id, iteration);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_services_reject_delete
BEFORE DELETE ON workflow_services
BEGIN
    SELECT RAISE(ABORT, 'workflow service records are immutable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_waits_named_signal
    ON workflow_waits(
        run_id,
        status,
        json_extract(record_json, '$.signal_name'),
        json_extract(record_json, '$.correlation'),
        wait_id
    );
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_run_updates_recovery
    ON workflow_run_updates(status, updated_at, idempotency_key);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_run_updates_run
    ON workflow_run_updates(run_id, created_at, idempotency_key);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_non_durable_runs (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_digest TEXT NOT NULL,
    record_json TEXT NOT NULL,
    completed_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_non_durable_runs_completed
    ON workflow_non_durable_runs(completed_at, run_id);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_reactor_deliveries_recovery
    ON workflow_reactor_deliveries(status, updated_at, reactor_id, idempotency_key);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_reactor_continuations_recovery
    ON workflow_reactor_continuations(status, updated_at, idempotency_key);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_failure_hooks_recovery
    ON workflow_failure_hooks(status, updated_at, source_run_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_exposure_profiles (
    profile_id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL CHECK (generation > 0),
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_mcp_principals (
    principal_id TEXT PRIMARY KEY,
    credential_digest TEXT NOT NULL UNIQUE,
    profile_id TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (profile_id) REFERENCES workflow_exposure_profiles(profile_id) ON DELETE RESTRICT
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_mcp_principals_profile
    ON workflow_mcp_principals(profile_id, principal_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_a2a_tasks (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    definition_json TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    host_start_key TEXT NOT NULL UNIQUE,
    owner_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_a2a_tasks_no_update
BEFORE UPDATE ON workflow_a2a_tasks BEGIN
    SELECT RAISE(ABORT, 'workflow A2A task correlations are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_a2a_tasks_no_delete
BEFORE DELETE ON workflow_a2a_tasks BEGIN
    SELECT RAISE(ABORT, 'workflow A2A task correlations are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_plan_snapshots (
    snapshot_digest TEXT PRIMARY KEY,
    plan_digest TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    plan_snapshot_json TEXT NOT NULL,
    source_map_json TEXT NOT NULL,
    source_snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (plan_digest) REFERENCES workflow_plan_refs(digest)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_plan_snapshots_plan
    ON workflow_plan_snapshots(plan_digest, snapshot_digest);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_host_start_plan_snapshots (
    run_id TEXT PRIMARY KEY,
    snapshot_digest TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_host_starts(run_id),
    FOREIGN KEY (snapshot_digest) REFERENCES workflow_plan_snapshots(snapshot_digest),
    FOREIGN KEY (plan_digest) REFERENCES workflow_plan_refs(digest)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_host_start_plan_snapshot_ref
    ON workflow_host_start_plan_snapshots(snapshot_digest, run_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_plan_snapshots_reject_update
BEFORE UPDATE ON workflow_plan_snapshots BEGIN
    SELECT RAISE(ABORT, 'workflow plan snapshots are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_host_start_plan_snapshots_reject_update
BEFORE UPDATE ON workflow_host_start_plan_snapshots BEGIN
    SELECT RAISE(ABORT, 'workflow host start plan snapshot links are immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_host_start_plan_snapshots_reject_delete
BEFORE DELETE ON workflow_host_start_plan_snapshots BEGIN
    SELECT RAISE(ABORT, 'workflow host start plan snapshot links are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_node_invocations_phase_valid_insert
BEFORE INSERT ON workflow_node_invocations
WHEN NEW.phase NOT IN ('', 'compensation')
BEGIN
    SELECT RAISE(ABORT, 'workflow node invocation phase is invalid');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_node_invocations_phase_immutable
BEFORE UPDATE ON workflow_node_invocations
WHEN NEW.phase <> OLD.phase OR NEW.phase NOT IN ('', 'compensation')
BEGIN
    SELECT RAISE(ABORT, 'workflow node invocation phase is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_compensation_ledgers (
    run_id TEXT PRIMARY KEY,
    plan_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    outcome TEXT NOT NULL DEFAULT '',
    generation INTEGER NOT NULL CHECK (generation > 0),
    updated_at TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
    FOREIGN KEY (plan_digest) REFERENCES workflow_plan_refs(digest)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_compensation_recovery
    ON workflow_compensation_ledgers(status, updated_at, run_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_compensation_entries (
    run_id TEXT NOT NULL,
    entry_id TEXT NOT NULL,
    source_node_id TEXT NOT NULL,
    source_iteration TEXT NOT NULL DEFAULT '',
    source_attempt INTEGER NOT NULL CHECK (source_attempt > 0),
    handler_node_id TEXT NOT NULL,
    handler_iteration TEXT NOT NULL,
    status TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    updated_at TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY (run_id, entry_id),
    UNIQUE (run_id, handler_node_id, handler_iteration),
    FOREIGN KEY (run_id) REFERENCES workflow_compensation_ledgers(run_id),
    FOREIGN KEY (run_id, source_node_id, source_iteration, source_attempt)
        REFERENCES workflow_attempts(run_id, node_id, iteration, attempt_number)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_workflow_compensation_entries_status
    ON workflow_compensation_entries(run_id, status, entry_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_compensation_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_compensation_ledgers(run_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_compensation_ledgers_immutable_identity
BEFORE UPDATE ON workflow_compensation_ledgers
WHEN NEW.run_id <> OLD.run_id OR NEW.plan_digest <> OLD.plan_digest
  OR json_extract(NEW.snapshot_json, '$.created_at') IS NOT json_extract(OLD.snapshot_json, '$.created_at')
  OR (
    NOT (OLD.status = 'collecting' AND NEW.status IN ('frozen', 'terminal'))
    AND (
      json_extract(NEW.snapshot_json, '$.trigger') IS NOT json_extract(OLD.snapshot_json, '$.trigger')
      OR json_extract(NEW.snapshot_json, '$.original_status') IS NOT json_extract(OLD.snapshot_json, '$.original_status')
      OR json_extract(NEW.snapshot_json, '$.original_failure') IS NOT json_extract(OLD.snapshot_json, '$.original_failure')
    )
  )
BEGIN
    SELECT RAISE(ABORT, 'workflow compensation ledger identity is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_compensation_entries_immutable_evidence
BEFORE UPDATE ON workflow_compensation_entries
WHEN NEW.run_id <> OLD.run_id OR NEW.entry_id <> OLD.entry_id
  OR NEW.source_node_id <> OLD.source_node_id OR NEW.source_iteration <> OLD.source_iteration
  OR NEW.source_attempt <> OLD.source_attempt OR NEW.handler_node_id <> OLD.handler_node_id
  OR json_extract(NEW.snapshot_json, '$.plan_digest') IS NOT json_extract(OLD.snapshot_json, '$.plan_digest')
  OR json_extract(NEW.snapshot_json, '$.operation') IS NOT json_extract(OLD.snapshot_json, '$.operation')
  OR json_extract(NEW.snapshot_json, '$.evidence_digest') IS NOT json_extract(OLD.snapshot_json, '$.evidence_digest')
  OR json_extract(NEW.snapshot_json, '$.original_inputs') IS NOT json_extract(OLD.snapshot_json, '$.original_inputs')
  OR json_extract(NEW.snapshot_json, '$.original_outputs') IS NOT json_extract(OLD.snapshot_json, '$.original_outputs')
  OR json_extract(NEW.snapshot_json, '$.original_error') IS NOT json_extract(OLD.snapshot_json, '$.original_error')
  OR json_extract(NEW.snapshot_json, '$.receipt') IS NOT json_extract(OLD.snapshot_json, '$.receipt')
  OR json_extract(NEW.snapshot_json, '$.child_run_id') IS NOT json_extract(OLD.snapshot_json, '$.child_run_id')
  OR json_extract(NEW.snapshot_json, '$.created_at') IS NOT json_extract(OLD.snapshot_json, '$.created_at')
  OR (
    NOT (OLD.status = 'eligible' AND NEW.status = 'pending')
    AND json_extract(NEW.snapshot_json, '$.prerequisites') IS NOT json_extract(OLD.snapshot_json, '$.prerequisites')
  )
BEGIN
    SELECT RAISE(ABORT, 'workflow compensation entry evidence is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_compensation_ledgers_cycles_append_only
BEFORE UPDATE ON workflow_compensation_ledgers
WHEN OLD.status <> 'collecting' AND (
  coalesce(json_array_length(json_extract(NEW.snapshot_json, '$.cycles')), 0)
    < coalesce(json_array_length(json_extract(OLD.snapshot_json, '$.cycles')), 0)
  OR coalesce(json_array_length(json_extract(NEW.snapshot_json, '$.cycles')), 0)
    > coalesce(json_array_length(json_extract(OLD.snapshot_json, '$.cycles')), 0) + 1
  OR EXISTS (
    SELECT 1
    FROM json_each(json_extract(OLD.snapshot_json, '$.cycles')) AS old_cycle
    LEFT JOIN json_each(json_extract(NEW.snapshot_json, '$.cycles')) AS new_cycle
      ON new_cycle.key = old_cycle.key
    WHERE (
      coalesce(json_array_length(json_extract(NEW.snapshot_json, '$.cycles')), 0)
        > coalesce(json_array_length(json_extract(OLD.snapshot_json, '$.cycles')), 0)
      OR old_cycle.key < coalesce(json_array_length(json_extract(OLD.snapshot_json, '$.cycles')), 0) - 1
    ) AND new_cycle.value IS NOT old_cycle.value
  )
  OR (
    coalesce(json_array_length(json_extract(NEW.snapshot_json, '$.cycles')), 0)
      = coalesce(json_array_length(json_extract(OLD.snapshot_json, '$.cycles')), 0)
    AND (
      json_extract(NEW.snapshot_json, '$.cycles[#-1].number') IS NOT json_extract(OLD.snapshot_json, '$.cycles[#-1].number')
      OR json_extract(NEW.snapshot_json, '$.cycles[#-1].attestation') IS NOT json_extract(OLD.snapshot_json, '$.cycles[#-1].attestation')
      OR json_extract(NEW.snapshot_json, '$.cycles[#-1].started_at') IS NOT json_extract(OLD.snapshot_json, '$.cycles[#-1].started_at')
    )
  )
  OR (
    coalesce(json_array_length(json_extract(NEW.snapshot_json, '$.cycles')), 0)
      = coalesce(json_array_length(json_extract(OLD.snapshot_json, '$.cycles')), 0)
    AND (
      json_extract(OLD.snapshot_json, '$.cycles[#-1].outcome') IS NOT NULL
      AND json_extract(NEW.snapshot_json, '$.cycles[#-1].outcome') IS NOT json_extract(OLD.snapshot_json, '$.cycles[#-1].outcome')
      OR (json_extract(OLD.snapshot_json, '$.cycles[#-1].completed_at') IS NOT NULL
      AND json_extract(OLD.snapshot_json, '$.cycles[#-1].completed_at') <> '0001-01-01T00:00:00Z')
      AND json_extract(NEW.snapshot_json, '$.cycles[#-1].completed_at') IS NOT json_extract(OLD.snapshot_json, '$.cycles[#-1].completed_at')
      OR json_extract(OLD.snapshot_json, '$.cycles[#-1].cancel_reason') IS NOT NULL
      AND json_extract(NEW.snapshot_json, '$.cycles[#-1].cancel_reason') IS NOT json_extract(OLD.snapshot_json, '$.cycles[#-1].cancel_reason')
    )
  )
)
BEGIN
    SELECT RAISE(ABORT, 'workflow compensation cycles are append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_compensation_entries_history_append_only
BEFORE UPDATE ON workflow_compensation_entries
WHEN coalesce(json_array_length(json_extract(NEW.snapshot_json, '$.history')), 0)
       < coalesce(json_array_length(json_extract(OLD.snapshot_json, '$.history')), 0)
  OR coalesce(json_array_length(json_extract(NEW.snapshot_json, '$.history')), 0)
       > coalesce(json_array_length(json_extract(OLD.snapshot_json, '$.history')), 0) + 1
  OR EXISTS (
    SELECT 1
    FROM json_each(coalesce(json_extract(OLD.snapshot_json, '$.history'), json('[]'))) AS old_history
    LEFT JOIN json_each(coalesce(json_extract(NEW.snapshot_json, '$.history'), json('[]'))) AS new_history
      ON new_history.key = old_history.key
    WHERE new_history.value IS NOT old_history.value
  )
BEGIN
    SELECT RAISE(ABORT, 'workflow compensation entry history is append-only');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_compensation_ledgers_reject_delete
BEFORE DELETE ON workflow_compensation_ledgers BEGIN
    SELECT RAISE(ABORT, 'workflow compensation ledgers are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_compensation_entries_reject_delete
BEFORE DELETE ON workflow_compensation_entries BEGIN
    SELECT RAISE(ABORT, 'workflow compensation entries are durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_compensation_idempotency_reject_update
BEFORE UPDATE ON workflow_compensation_idempotency BEGIN
    SELECT RAISE(ABORT, 'workflow compensation idempotency is immutable');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER workflow_compensation_idempotency_reject_delete
BEFORE DELETE ON workflow_compensation_idempotency BEGIN
    SELECT RAISE(ABORT, 'workflow compensation idempotency is durable history');
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_mcp_credentials (
    credential_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES workflow_mcp_principals(principal_id) ON DELETE CASCADE,
    credential_digest TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    expires_at TEXT,
    overlap_until TEXT,
    last_used_at TEXT,
    revoked_at TEXT
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_mcp_credentials_principal ON workflow_mcp_credentials(principal_id, credential_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_credential_operations (
    principal_id TEXT NOT NULL,
    key_digest TEXT NOT NULL,
    operation TEXT NOT NULL,
    actor TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    operation_generation INTEGER NOT NULL CHECK(operation_generation > 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY(principal_id, key_digest)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_credential_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_id TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    operation TEXT NOT NULL,
    actor TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE store_generation (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    generation TEXT NOT NULL CHECK (length(generation) = 32)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_event_outbox (
    outbox_seq INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL,
    run_sequence INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    envelope_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (run_id, run_sequence)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workflow_event_outbox_counter (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_seq INTEGER NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
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
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX workflow_event_outbox_skipped_open ON workflow_event_outbox_skipped(created_at) WHERE repaired_at IS NULL;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_event_outbox_skipped;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_event_outbox_counter;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_event_outbox;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS store_generation;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_credential_audit;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_credential_operations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_mcp_credentials;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_compensation_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_compensation_entries;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_compensation_ledgers;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_host_start_plan_snapshots;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_plan_snapshots;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_a2a_tasks;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_mcp_principals;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_exposure_profiles;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_failure_hooks;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_reactor_continuations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_reactor_deliveries;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_reactor_generations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_reactors;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_non_durable_runs;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_run_updates;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_services;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_callback_deliveries;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_callback_registrations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_activation_events;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_activation_external_events;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_activation_dispatches;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_activation_attempt_results;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_activation_attempts;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_activation_fires;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_activation_schedules;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_activation_registrations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_reuse_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_pin_bindings;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_memo_entries;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_node_input_bindings;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_replay_provenance;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_crash_recovery_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_run_policy_decisions;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_scheduler_admission_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_scheduler_waiters;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_scheduler_holders;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_scheduler_resources;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_control_cancellation_trees;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_terminal_intents;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_control_decisions;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_child_run_start_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_call_resolutions;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_host_cancellations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_host_policy_decisions;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_host_start_progress;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_host_starts;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_run_cancellation_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_cancellation_intents;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_child_runs;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_fanout_items;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_fanouts;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_retry_activation_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_retry_activations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_external_operations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_wait_attempt_bindings;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_wait_timeout_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_wait_resume_results;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_wait_suspend_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_external_activations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_pinned_values;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_cache_entries;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_events;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_event_sequences;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_value_sets;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_wait_resume_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_waits;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_attempts;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_claim_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_node_leases;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_node_invocations;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_run_start_idempotency;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_runs;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS workflow_plan_refs;
-- +goose StatementEnd
