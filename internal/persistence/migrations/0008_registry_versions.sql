CREATE TABLE IF NOT EXISTS blueprint_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    blueprint_name TEXT NOT NULL,
    version_hash TEXT NOT NULL,
    file_path TEXT NOT NULL,
    indexed_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(blueprint_name, version_hash)
);
