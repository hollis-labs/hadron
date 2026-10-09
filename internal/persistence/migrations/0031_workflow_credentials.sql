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
CREATE INDEX workflow_mcp_credentials_principal ON workflow_mcp_credentials(principal_id, credential_id);
INSERT INTO workflow_mcp_credentials(credential_id, principal_id, credential_digest, created_at)
SELECT 'cred_' || lower(hex(randomblob(16))), principal_id, credential_digest, created_at FROM workflow_mcp_principals;

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
