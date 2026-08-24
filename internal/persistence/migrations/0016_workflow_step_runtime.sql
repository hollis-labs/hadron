-- Bind generic waits to the exact unfinished attempt they suspend. The unique
-- attempt key intentionally defines one generic wait per logical attempt;
-- multi-stage suspension requires a new attempt rather than timestamp-based
-- continuation inference.
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

CREATE INDEX idx_workflow_wait_attempt_lookup
    ON workflow_wait_attempt_bindings(run_id, node_id, iteration, attempt_number, wait_id);

-- Backfill only a current, payload-safe 0015 wait whose node timestamp and
-- lifecycle state prove it belongs to the node's latest unfinished attempt.
-- Legacy-unresumable rows remain readable but intentionally unbound.
INSERT INTO workflow_wait_attempt_bindings(wait_id, run_id, node_id, iteration, attempt_number)
SELECT w.wait_id, w.run_id, w.node_id, w.iteration, n.latest_attempt
FROM workflow_waits w
JOIN workflow_node_invocations n
  ON n.run_id = w.run_id AND n.node_id = w.node_id AND n.iteration = w.iteration
JOIN workflow_attempts a
  ON a.run_id = n.run_id AND a.node_id = n.node_id AND a.iteration = n.iteration
 AND a.attempt_number = n.latest_attempt
WHERE a.status = 'running'
  AND w.updated_at = n.updated_at
  AND ((w.status = 'open' AND n.status = 'waiting')
    OR (w.status = 'resumed' AND n.status = 'ready'))
  AND COALESCE(json_extract(w.record_json, '$.authority.kind'), '') <> 'legacy_unresumable'
  AND (
      SELECT COUNT(*) FROM workflow_waits candidate
      WHERE candidate.run_id = w.run_id AND candidate.node_id = w.node_id
        AND candidate.iteration = w.iteration
        AND candidate.updated_at = n.updated_at
        AND ((candidate.status = 'open' AND n.status = 'waiting')
          OR (candidate.status = 'resumed' AND n.status = 'ready'))
        AND COALESCE(json_extract(candidate.record_json, '$.authority.kind'), '') <> 'legacy_unresumable'
  ) = 1;

-- One immutable adapter-owned operation is bound to one exact attempt. Mutable
-- observation fields advance only through generation-CAS runtime operations.
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

CREATE INDEX idx_workflow_external_operations_recovery
    ON workflow_external_operations(status, updated_at, run_id, node_id, iteration, attempt_number);

CREATE INDEX idx_workflow_external_operations_cancel
    ON workflow_external_operations(status, cancel_requested_at, updated_at);

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

CREATE TRIGGER workflow_external_operations_reject_delete
BEFORE DELETE ON workflow_external_operations
BEGIN
    SELECT RAISE(ABORT, 'workflow external operations are durable history');
END;
