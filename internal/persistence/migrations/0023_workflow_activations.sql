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

CREATE INDEX workflow_activation_registrations_due
    ON workflow_activation_registrations(enabled, expires_at, source_kind, registration_id);
CREATE INDEX workflow_activation_registrations_scope
    ON workflow_activation_registrations(scope_key, registration_id);

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

CREATE INDEX workflow_activation_schedules_due
    ON workflow_activation_schedules(enabled, next_run_at, registration_id);

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

CREATE INDEX workflow_activation_fires_due
    ON workflow_activation_fires(status, next_attempt_at, fire_id);
CREATE INDEX workflow_activation_fires_registration
    ON workflow_activation_fires(registration_id, scheduled_at, fire_id);

CREATE TABLE workflow_activation_attempts (
    fire_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    claimed_at TEXT NOT NULL,
    claim_expires_at TEXT NOT NULL,
    PRIMARY KEY (fire_id, attempt),
    FOREIGN KEY (fire_id) REFERENCES workflow_activation_fires(fire_id)
);

CREATE TABLE workflow_activation_attempt_results (
    fire_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    outcome TEXT NOT NULL,
    reason_code TEXT,
    completed_at TEXT NOT NULL,
    PRIMARY KEY (fire_id, attempt),
    FOREIGN KEY (fire_id, attempt) REFERENCES workflow_activation_attempts(fire_id, attempt)
);

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

CREATE INDEX workflow_activation_dispatches_overlap
    ON workflow_activation_dispatches(registration_id, status, logical_run_id, fire_id);

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

CREATE INDEX workflow_activation_events_registration
    ON workflow_activation_events(registration_id, sequence);
CREATE INDEX workflow_activation_events_fire
    ON workflow_activation_events(fire_id, sequence);

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

CREATE INDEX workflow_callback_registrations_wait
    ON workflow_callback_registrations(wait_id, callback_id);

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

CREATE TRIGGER workflow_activation_events_no_update
BEFORE UPDATE ON workflow_activation_events
BEGIN
    SELECT RAISE(ABORT, 'workflow activation events are append-only');
END;

CREATE TRIGGER workflow_activation_events_no_delete
BEFORE DELETE ON workflow_activation_events
BEGIN
    SELECT RAISE(ABORT, 'workflow activation events are append-only');
END;

CREATE TRIGGER workflow_activation_attempts_no_update
BEFORE UPDATE ON workflow_activation_attempts BEGIN
    SELECT RAISE(ABORT, 'workflow activation attempts are append-only');
END;
CREATE TRIGGER workflow_activation_attempts_no_delete
BEFORE DELETE ON workflow_activation_attempts BEGIN
    SELECT RAISE(ABORT, 'workflow activation attempts are append-only');
END;
CREATE TRIGGER workflow_activation_attempt_results_no_update
BEFORE UPDATE ON workflow_activation_attempt_results BEGIN
    SELECT RAISE(ABORT, 'workflow activation attempt results are append-only');
END;
CREATE TRIGGER workflow_activation_attempt_results_no_delete
BEFORE DELETE ON workflow_activation_attempt_results BEGIN
    SELECT RAISE(ABORT, 'workflow activation attempt results are append-only');
END;
CREATE TRIGGER workflow_activation_external_events_no_update
BEFORE UPDATE ON workflow_activation_external_events BEGIN
    SELECT RAISE(ABORT, 'workflow activation external events are immutable');
END;
CREATE TRIGGER workflow_activation_external_events_no_delete
BEFORE DELETE ON workflow_activation_external_events BEGIN
    SELECT RAISE(ABORT, 'workflow activation external events are immutable');
END;

-- Quarantined legacy schedule fire history. New graph-native registrations
-- use workflow_activation_* exclusively; these tables only preserve the old
-- blueprint scheduler while it consumes the expanded go-scheduler contract.
CREATE TABLE legacy_schedule_fires (
    fire_id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    fired_at TEXT,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    next_attempt_at TEXT,
	claim_expires_at TEXT,
    retry_json BLOB NOT NULL,
    job_type TEXT NOT NULL,
    payload_json BLOB NOT NULL,
    last_error_code TEXT,
    FOREIGN KEY (schedule_id) REFERENCES schedules(id)
);
CREATE INDEX legacy_schedule_fires_due ON legacy_schedule_fires(status, next_attempt_at, fire_id);
CREATE TABLE legacy_schedule_fire_attempts (
    fire_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    claimed_at TEXT NOT NULL,
    completed_at TEXT,
    outcome TEXT,
    PRIMARY KEY (fire_id, attempt),
    FOREIGN KEY (fire_id) REFERENCES legacy_schedule_fires(fire_id)
);
