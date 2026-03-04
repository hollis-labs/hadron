CREATE TABLE IF NOT EXISTS workspaces (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

INSERT OR IGNORE INTO workspaces(id, name, created_at, updated_at)
VALUES ('default', 'Default', strftime('%Y-%m-%dT%H:%M:%SZ','now'), strftime('%Y-%m-%dT%H:%M:%SZ','now'));

ALTER TABLE runs ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE schedules ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE pipeline_runs ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE pipeline_stage_runs ADD COLUMN workspace_id TEXT NOT NULL DEFAULT 'default';

CREATE INDEX IF NOT EXISTS idx_runs_workspace_created ON runs(workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_schedules_workspace ON schedules(workspace_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_workspace_created ON pipeline_runs(workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_stage_runs_workspace ON pipeline_stage_runs(workspace_id, pipeline_run_id, stage_index);
