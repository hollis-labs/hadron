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

CREATE TRIGGER workflow_a2a_tasks_no_update
BEFORE UPDATE ON workflow_a2a_tasks BEGIN
    SELECT RAISE(ABORT, 'workflow A2A task correlations are append-only');
END;

CREATE TRIGGER workflow_a2a_tasks_no_delete
BEFORE DELETE ON workflow_a2a_tasks BEGIN
    SELECT RAISE(ABORT, 'workflow A2A task correlations are append-only');
END;
