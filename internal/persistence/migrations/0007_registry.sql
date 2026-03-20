CREATE TABLE IF NOT EXISTS blueprint_registry (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT,
    title TEXT,
    description TEXT,
    author TEXT,
    tags TEXT,
    version_hash TEXT NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    inputs_json TEXT,
    indexed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_registry_name ON blueprint_registry(name);
CREATE INDEX IF NOT EXISTS idx_registry_slug ON blueprint_registry(slug);
