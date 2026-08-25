ALTER TABLE workflow_node_invocations ADD COLUMN phase TEXT NOT NULL DEFAULT '';

CREATE TRIGGER workflow_node_invocations_phase_valid_insert
BEFORE INSERT ON workflow_node_invocations
WHEN NEW.phase NOT IN ('', 'compensation')
BEGIN
    SELECT RAISE(ABORT, 'workflow node invocation phase is invalid');
END;

CREATE TRIGGER workflow_node_invocations_phase_immutable
BEFORE UPDATE ON workflow_node_invocations
WHEN NEW.phase <> OLD.phase OR NEW.phase NOT IN ('', 'compensation')
BEGIN
    SELECT RAISE(ABORT, 'workflow node invocation phase is immutable');
END;

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

CREATE INDEX idx_workflow_compensation_recovery
    ON workflow_compensation_ledgers(status, updated_at, run_id);

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

CREATE INDEX idx_workflow_compensation_entries_status
    ON workflow_compensation_entries(run_id, status, entry_id);

CREATE TABLE workflow_compensation_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_compensation_ledgers(run_id)
);

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

CREATE TRIGGER workflow_compensation_ledgers_reject_delete
BEFORE DELETE ON workflow_compensation_ledgers BEGIN
    SELECT RAISE(ABORT, 'workflow compensation ledgers are durable history');
END;

CREATE TRIGGER workflow_compensation_entries_reject_delete
BEFORE DELETE ON workflow_compensation_entries BEGIN
    SELECT RAISE(ABORT, 'workflow compensation entries are durable history');
END;

CREATE TRIGGER workflow_compensation_idempotency_reject_update
BEFORE UPDATE ON workflow_compensation_idempotency BEGIN
    SELECT RAISE(ABORT, 'workflow compensation idempotency is immutable');
END;

CREATE TRIGGER workflow_compensation_idempotency_reject_delete
BEFORE DELETE ON workflow_compensation_idempotency BEGIN
    SELECT RAISE(ABORT, 'workflow compensation idempotency is durable history');
END;
