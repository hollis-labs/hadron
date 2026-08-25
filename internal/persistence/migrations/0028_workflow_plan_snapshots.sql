-- Exact, locator-sensitive durable plan/source variants. ExecutionPlan.Digest
-- is intentionally relocation-stable, so it cannot identify this material.

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

CREATE INDEX idx_workflow_plan_snapshots_plan
    ON workflow_plan_snapshots(plan_digest, snapshot_digest);

CREATE TABLE workflow_host_start_plan_snapshots (
    run_id TEXT PRIMARY KEY,
    snapshot_digest TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES workflow_host_starts(run_id),
    FOREIGN KEY (snapshot_digest) REFERENCES workflow_plan_snapshots(snapshot_digest),
    FOREIGN KEY (plan_digest) REFERENCES workflow_plan_refs(digest)
);

CREATE INDEX idx_workflow_host_start_plan_snapshot_ref
    ON workflow_host_start_plan_snapshots(snapshot_digest, run_id);

CREATE TRIGGER workflow_plan_snapshots_reject_update
BEFORE UPDATE ON workflow_plan_snapshots BEGIN
    SELECT RAISE(ABORT, 'workflow plan snapshots are immutable');
END;

CREATE TRIGGER workflow_host_start_plan_snapshots_reject_update
BEFORE UPDATE ON workflow_host_start_plan_snapshots BEGIN
    SELECT RAISE(ABORT, 'workflow host start plan snapshot links are immutable');
END;

CREATE TRIGGER workflow_host_start_plan_snapshots_reject_delete
BEFORE DELETE ON workflow_host_start_plan_snapshots BEGIN
    SELECT RAISE(ABORT, 'workflow host start plan snapshot links are durable history');
END;
