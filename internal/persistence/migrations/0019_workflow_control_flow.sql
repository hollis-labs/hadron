-- Durable graph-native switch/catch decisions and run terminal intents.
-- Typed errors remain in workflow_value_sets and are referenced by sequence;
-- all mutations are coordinated by WorkflowStateStore transactions.

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

CREATE INDEX idx_workflow_control_decisions_run
    ON workflow_control_decisions(run_id, node_id, iteration, kind);

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

CREATE INDEX idx_workflow_terminal_intents_recovery
    ON workflow_terminal_intents(status, updated_at, run_id);

-- The immutable canonical request proves that one atomic cancellation covered
-- the exact locally owned ParentCloseCancel descendant closure. The legacy
-- cancellation idempotency row remains the root-request replay contract used
-- by WorkflowHostStore.
CREATE TABLE workflow_control_cancellation_trees (
    root_run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (root_run_id) REFERENCES workflow_runs(run_id)
);

CREATE TRIGGER workflow_control_decisions_reject_update
BEFORE UPDATE ON workflow_control_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow control decisions are immutable');
END;

CREATE TRIGGER workflow_control_decisions_reject_delete
BEFORE DELETE ON workflow_control_decisions BEGIN
    SELECT RAISE(ABORT, 'workflow control decisions are durable history');
END;

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

CREATE TRIGGER workflow_terminal_intents_reject_delete
BEFORE DELETE ON workflow_terminal_intents BEGIN
    SELECT RAISE(ABORT, 'workflow terminal intents are durable history');
END;

CREATE TRIGGER workflow_control_cancellation_trees_reject_update
BEFORE UPDATE ON workflow_control_cancellation_trees BEGIN
    SELECT RAISE(ABORT, 'workflow control cancellation trees are immutable');
END;

CREATE TRIGGER workflow_control_cancellation_trees_reject_delete
BEFORE DELETE ON workflow_control_cancellation_trees BEGIN
    SELECT RAISE(ABORT, 'workflow control cancellation trees are durable history');
END;
