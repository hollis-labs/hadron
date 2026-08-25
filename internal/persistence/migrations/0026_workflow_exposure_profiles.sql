CREATE TABLE workflow_exposure_profiles (
    profile_id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL CHECK (generation > 0),
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE workflow_mcp_principals (
    principal_id TEXT PRIMARY KEY,
    credential_digest TEXT NOT NULL UNIQUE,
    profile_id TEXT,
    generation INTEGER NOT NULL CHECK (generation > 0),
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (profile_id) REFERENCES workflow_exposure_profiles(profile_id) ON DELETE RESTRICT
);

CREATE INDEX idx_workflow_mcp_principals_profile
    ON workflow_mcp_principals(profile_id, principal_id);
