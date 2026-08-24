-- Restart-durable retry timers, run-cancellation work, and runtime fan-out.
-- These graph-native tables remain separate from legacy pipeline persistence.

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

CREATE INDEX idx_workflow_retry_activations_recovery
    ON workflow_retry_activations(status, fire_at, activation_id);

CREATE TABLE workflow_retry_activation_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TABLE workflow_fanouts (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    iteration TEXT NOT NULL DEFAULT '' CHECK (iteration = ''),
    status TEXT NOT NULL,
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency >= 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    snapshot_json TEXT NOT NULL,
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id, node_id, iteration)
        REFERENCES workflow_node_invocations(run_id, node_id, iteration)
);

CREATE INDEX idx_workflow_fanouts_status
    ON workflow_fanouts(status, run_id, node_id);

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

CREATE INDEX idx_workflow_fanout_items_invocation
    ON workflow_fanout_items(run_id, node_id, iteration, item_index);

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

CREATE INDEX idx_workflow_child_runs_parent
    ON workflow_child_runs(parent_run_id, child_run_id);

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

CREATE INDEX idx_workflow_cancellation_intents_recovery
    ON workflow_cancellation_intents(status, requested_at, intent_id);

CREATE TABLE workflow_run_cancellation_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TRIGGER workflow_retry_activations_immutable_attempt
BEFORE UPDATE ON workflow_retry_activations
WHEN OLD.run_id <> NEW.run_id
  OR OLD.node_id <> NEW.node_id
  OR OLD.iteration <> NEW.iteration
  OR OLD.attempt_number <> NEW.attempt_number
BEGIN
    SELECT RAISE(ABORT, 'workflow retry activation attempt is immutable');
END;

CREATE TRIGGER workflow_fanout_items_immutable
BEFORE UPDATE ON workflow_fanout_items
BEGIN
    SELECT RAISE(ABORT, 'workflow fan-out item bindings are immutable');
END;

CREATE TRIGGER workflow_fanout_items_reject_delete
BEFORE DELETE ON workflow_fanout_items
BEGIN
    SELECT RAISE(ABORT, 'workflow fan-out item bindings are durable history');
END;

CREATE TRIGGER workflow_child_runs_immutable
BEFORE UPDATE ON workflow_child_runs
BEGIN
    SELECT RAISE(ABORT, 'workflow child run links are immutable');
END;

CREATE TRIGGER workflow_child_runs_reject_delete
BEFORE DELETE ON workflow_child_runs
BEGIN
    SELECT RAISE(ABORT, 'workflow child run links are durable history');
END;
