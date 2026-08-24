ALTER TABLE workflow_waits ADD COLUMN record_json TEXT;
ALTER TABLE workflow_waits ADD COLUMN deadline TEXT;

-- Rows created by migration 0014 did not persist correlation, authority, or a
-- resume-token capability. They remain readable and timeout/cancel history is
-- preserved, but open rows are deliberately unresumable: the migration marks
-- their authority accordingly and assigns a token digest for which no raw
-- token was ever issued.
UPDATE workflow_waits
SET deadline = CASE WHEN status = 'timed_out' THEN resolved_at ELSE NULL END,
    record_json = json_object(
        'kind', CASE WHEN status = 'timed_out' THEN 'timer' ELSE 'signal' END,
        'correlation', wait_id,
        'deadline', CASE WHEN status = 'timed_out' THEN resolved_at ELSE NULL END,
        'resume_schema', json_object(
            'digest', 'sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
            'schema', json('{}')
        ),
        'resume_token_digest', 'sha256:e19e9ffb88b17165916bcb255875c04c023f664d2a1cf235b95b68199ebfa54e',
        'visibility', 'private',
        'authority', json_object('kind', 'legacy_unresumable', 'reference', 'migration-0015'),
        'wake_source', CASE WHEN status = 'timed_out' THEN 'timer' ELSE 'signal' END,
        'status', status,
        'resume_values', CASE WHEN resume_values_ref_json IS NULL THEN NULL ELSE json(resume_values_ref_json) END,
        'resolution', CASE
            WHEN status = 'resumed' THEN json_object(
                'source', 'signal',
                'responder', json_object('kind', 'legacy', 'reference', 'migration-0015'),
                'payload_digest', CASE WHEN resume_values_ref_json IS NULL THEN '' ELSE json_extract(resume_values_ref_json, '$.digest') END,
                'resolved_at', resolved_at
            )
            WHEN status = 'timed_out' THEN json_object(
                'source', 'timer',
                'responder', json_object('kind', 'system', 'reference', 'wait-timeout'),
                'resolved_at', resolved_at
            )
            WHEN status = 'canceled' THEN json_object(
                'source', 'signal',
                'responder', json_object('kind', 'system', 'reference', 'migration-0015'),
                'resolved_at', resolved_at
            )
            ELSE NULL
        END
    );

CREATE INDEX idx_workflow_waits_deadline
    ON workflow_waits(status, deadline, created_at, wait_id);

CREATE INDEX idx_workflow_waits_correlation
    ON workflow_waits(status, json_extract(record_json, '$.correlation'), wait_id);

-- Wait identity is the suspension idempotency identity. The table retains the
-- canonical request and complete atomic result for exact retries.
CREATE TABLE workflow_wait_suspend_idempotency (
    wait_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);

CREATE TABLE workflow_wait_resume_results (
    wait_id TEXT PRIMARY KEY,
    result_json TEXT NOT NULL
);

CREATE TABLE workflow_wait_timeout_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
