CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline_run_id TEXT NOT NULL,
  stage_index INTEGER NOT NULL,
  stage_name TEXT NOT NULL,
  run_id TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id),
  FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_runs_pipeline
  ON pipeline_stage_runs(pipeline_run_id, stage_index);
