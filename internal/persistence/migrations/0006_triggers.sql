CREATE TABLE IF NOT EXISTS triggers (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL DEFAULT 'webhook',
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    blueprint_path TEXT NOT NULL,
    workspace_id TEXT NOT NULL DEFAULT 'default',
    secret_hash TEXT,
    extract_inputs TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    one_shot INTEGER NOT NULL DEFAULT 0,
    ttl_expires_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    fired_count INTEGER NOT NULL DEFAULT 0,
    last_fired_at TEXT
);
