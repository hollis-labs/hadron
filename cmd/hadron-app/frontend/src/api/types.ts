// ── Core types matching hadrond JSON responses ────────────────────────

export interface Run {
  id: string;
  status: string;
  blueprint_path: string;
  created_at: string;
  started_at?: string | null;
  ended_at?: string | null;
  error_message?: string | null;
  input_json?: string | null;
  workspace_id: string;
}

export interface RunEvent {
  id: number;
  run_id: string;
  event_type: string;
  message?: string | null;
  created_at: string;
  step_name?: string | null;
}

export interface Workspace {
  id: string;
  name: string;
  created_at: string;
  updated_at: string;
}

export interface Schedule {
  id: string;
  workspace_id: string;
  name: string;
  blueprint_path: string;
  cron_expr: string;
  enabled: boolean;
  created_at: string;
  updated_at: string;
  last_run_at?: string | null;
  next_run_at?: string | null;
}

export interface Health {
  status: string;
  version: string;
  service: string;
}

export interface ListResponse<T> {
  items: T[];
  next_cursor?: string | null;
}

export interface CreateScheduleRequest {
  name?: string;
  blueprint_path: string;
  cron_expr?: string;
  run_at?: string;
  enabled?: boolean;
  workspace_id?: string;
}

export interface EnqueueRunRequest {
  workspace_id?: string;
  blueprint_path: string;
  inputs?: Record<string, unknown>;
  dry_run?: boolean;
}

export interface BlueprintInput {
  name: string;
  label: string;
  description: string;
  type: 'string' | 'number' | 'boolean' | 'array';
  required: boolean;
  default: string;   // pre-stringified by Go
  enum: string[];
  // Validation (may be absent in JSON — optional)
  pattern?: string;
  min_length?: number;
  max_length?: number;
  min?: number;
  max?: number;
  items_type?: string;
}

export interface Pipeline {
  id: string;
  workspace_id: string;
  pipeline_path: string;
  status: string;
  error_message?: string | null;
  created_at: string;
  started_at?: string | null;
  ended_at?: string | null;
}

export interface PipelineStage {
  id: number;
  workspace_id: string;
  pipeline_run_id: string;
  stage_index: number;
  stage_name: string;
  run_id: string;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface EnqueuePipelineRequest {
  workspace_id?: string;
  pipeline_path: string;
  inputs?: Record<string, unknown>;
}

// ── Parsed blueprint types (matching Go blueprint.Blueprint) ──────────

export interface ParsedBlueprint {
  version: string;
  blueprint: {
    name: string; slug: string; title: string; description: string;
    author: string; license: string; tags: string[]; homepage: string;
  };
  project: {
    type: string; name: string; dir: string; path: string;
    php_version: string; node: boolean; vars: Record<string, unknown>;
  };
  env: Record<string, string>;
  inputs: BlueprintInput[];
  packages: {
    composer?: { require: string[]; require_dev: string[] } | null;
    npm?: { deps: string[]; dev: string[] } | null;
    pip?: { deps: string[]; dev: string[] } | null;
    brew?: { formulae: string[]; casks: string[]; taps: string[] } | null;
    go?: { tools: string[] } | null;
  };
  git: { init: boolean; create_github_repo: boolean; visibility: string; remote: string; branch: string };
  stubs: { enabled: boolean; search_paths: string[]; strict_match: boolean };
  imports: { path: string; alias: string; with: Record<string, unknown> }[];
  hooks: {
    before_run: { name: string; cmd: string; if: string }[];
    after_run: { name: string; cmd: string; if: string }[];
    on_error: { name: string; cmd: string; if: string }[];
  };
  steps: {
    section: string;
    tasks: {
      name: string; cmd: string; run: string; call: string;
      if: string; with: Record<string, unknown>; dir: string;
      env: Record<string, string>; retry: number;
      retry_delay_seconds: number; timeout_seconds: number;
      continue_on_error: boolean; enabled: boolean | null;
      on_success: { type: string; value: string }[];
      on_fail: { type: string; value: string }[];
    }[];
  }[];
}

export interface BlueprintMetaSummary {
  name: string;
  slug: string;
  title: string;
  description: string;
  tags: string[];
  version: string;
  input_count: number;
  step_count: number;
  section_count: number;
  has_imports: boolean;
}

// ── Telemetry types (matching Go TelemetryRunSummary / TelemetryLogEntry) ──

export interface TelemetryRunSummary {
  run_id: string;
  file_size: number;
  modified_at: string;
  event_count: number;
}

export interface TelemetryLogEntry {
  ts: string;
  level: string;
  event: string;
  run_id?: string;
  section?: string;
  step?: string;
  msg?: string;
}

// ── Wizard types (for create/edit flows) ──────────────────────────────

export interface WizardBlueprint {
  version: string;
  blueprint: {
    name: string; slug: string; title: string; description: string;
    author: string; license: string; tags: string[]; homepage: string;
  };
  project: {
    type: string; name: string; dir: string; path: string;
    php_version: string; node: boolean; vars: Record<string, string>;
  };
  env: Record<string, string>;
  inputs: WizardInput[];
  packages: {
    composer_require: string[]; composer_dev: string[];
    npm_deps: string[]; npm_dev: string[];
    pip_deps: string[]; pip_dev: string[];
    brew_formulae: string[]; brew_casks: string[];
    go_tools: string[];
  };
  steps: {
    section: string;
    tasks: WizardTask[];
  }[];
  // Advanced fields
  git: {
    init: boolean;
    create_github_repo: boolean;
    visibility: string;
    remote: string;
    branch: string;
  };
  stubs: {
    enabled: boolean;
    search_paths: string[];
    strict_match: boolean;
  };
  imports: {
    path: string;
    alias: string;
    with: Record<string, string>;
  }[];
  hooks: {
    before_run: { name: string; cmd: string; if_expr: string }[];
    after_run: { name: string; cmd: string; if_expr: string }[];
    on_error: { name: string; cmd: string; if_expr: string }[];
  };
}

export interface WizardInput {
  name: string; label: string; description: string;
  type: 'string' | 'number' | 'boolean' | 'array';
  required: boolean; default_value: string;
  enum_values: string;
  pattern: string; min_length: string; max_length: string;
  min: string; max: string; items_type: string;
}

export interface WizardTask {
  name: string; cmd: string; call: string; if_expr: string;
  dir: string; env: Record<string, string>;
  retry: string; retry_delay_seconds: string; timeout_seconds: string;
  continue_on_error: boolean; enabled: boolean;
  on_success: { type: string; value: string }[];
  on_fail: { type: string; value: string }[];
}

// ── Wails Go binding types ────────────────────────────────────────────

export interface FileEntry {
  name: string;
  path: string;
  isDir: boolean;
}

export interface ValidateResult {
  valid: boolean;
  error?: string;
}

// ── Settings types (matching Go settings.Settings) ────────────────────

export interface HadronSettings {
  execution: {
    allowedCommands: string[];
    deniedCommands: string[];
    allowedDirs: string[];
    deniedDirs: string[];
    maxConcurrentJobs: number;
    defaultTimeout: number;
    workers: number;
  };
  safety: {
    requireConfirmation: boolean;
    dryRunByDefault: boolean;
    blockSudo: boolean;
    sandboxMode: boolean;
  };
  telemetry: {
    enabled: boolean;
    retainDays: number;
  };
}
