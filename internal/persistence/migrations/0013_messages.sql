CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    substrate TEXT NOT NULL,
    kind TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT '',
    from_urn TEXT NOT NULL,
    to_urn TEXT NOT NULL,
    thread_id TEXT NOT NULL DEFAULT '',
    in_reply_to TEXT NOT NULL DEFAULT '',
    correlation_id TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    consumed_at TEXT,
    canceled_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_messages_substrate_to_created
    ON messages(substrate, to_urn, created_at);

CREATE INDEX IF NOT EXISTS idx_messages_substrate_correlation
    ON messages(substrate, correlation_id, created_at);

CREATE INDEX IF NOT EXISTS idx_messages_thread
    ON messages(thread_id, created_at);
