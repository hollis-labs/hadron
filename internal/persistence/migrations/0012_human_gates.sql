CREATE TABLE IF NOT EXISTS human_gates (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL DEFAULT 'default',
  run_id TEXT NOT NULL,
  step_name TEXT NOT NULL,
  prompt TEXT NOT NULL,
  options_json TEXT NOT NULL DEFAULT '[]',
  status TEXT NOT NULL,
  decision TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  expires_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_human_gates_run_id ON human_gates(run_id);
CREATE INDEX IF NOT EXISTS idx_human_gates_workspace_status ON human_gates(workspace_id, status);
