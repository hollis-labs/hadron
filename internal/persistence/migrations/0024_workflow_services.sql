ALTER TABLE workflow_fanouts
    ADD COLUMN fail_fast INTEGER NOT NULL DEFAULT 0 CHECK (fail_fast IN (0, 1));

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

CREATE INDEX idx_workflow_services_recovery
    ON workflow_services(status, updated_at, run_id, node_id, iteration);

CREATE TRIGGER workflow_services_reject_delete
BEFORE DELETE ON workflow_services
BEGIN
    SELECT RAISE(ABORT, 'workflow service records are immutable history');
END;
