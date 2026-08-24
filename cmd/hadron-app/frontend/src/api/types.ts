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

export interface MCPCallDiagnostic {
  sequence: number;
  step_name: string;
  server: string;
  tool: string;
  transport: string;
  status: string;
  retry_count: number;
  attempt_count: number;
  reused_client: boolean;
  health_probe: boolean;
  reconnected: boolean;
  truncated: boolean;
  result_json?: string | null;
  error_message?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
}

export interface OperationDiagnostic {
  sequence: number;
  kind: 'mcp_call' | 'http_call' | 'message_wait' | 'agent_launch' | 'human_gate' | string;
  step_name: string;
  status: string;
  started_at?: string | null;
  finished_at?: string | null;
  error_message?: string | null;
  truncated: boolean;
  result_json?: string | null;
  server?: string | null;
  tool?: string | null;
  transport?: string | null;
  retry_count: number;
  attempt_count: number;
  reused_client: boolean;
  health_probe: boolean;
  reconnected: boolean;
  method?: string | null;
  url?: string | null;
  status_code?: number | null;
  duration_ms?: number | null;
  substrate?: string | null;
  to?: string | null;
  correlation_id?: string | null;
  timeout_ms?: number | null;
  poll_count: number;
  message_id?: string | null;
  logical_agent_id?: string | null;
  launch_id?: string | null;
  gate_id?: string | null;
  decision?: string | null;
  prompt?: string | null;
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

// ── Graph-native workflow diagnostics (shared rundiagnostics JSON) ────

export interface WorkflowValueSetRef {
  id: string;
  digest: string;
}

export interface WorkflowNodeInvocationID {
  run_id: string;
  node_id: string;
  iteration?: number;
}

export interface WorkflowAttemptID {
  invocation: WorkflowNodeInvocationID;
  number: number;
}

export interface WorkflowSourceDiagnostic {
  format: string;
  locator: string;
  start_line?: number;
  start_column?: number;
  end_line?: number;
  end_column?: number;
  path?: string[];
}

export interface WorkflowPositionDiagnostic {
  x: number;
  y: number;
}

export interface WorkflowRetryDiagnostic {
  attempts: number;
  strategy?: string;
  initial_delay?: string;
  max_delay?: string;
}

export interface WorkflowPlanNodeDiagnostic {
  id: string;
  display_name?: string;
  kind: string;
  kind_version?: string;
  ready_when: string;
  needs?: string[];
  declared_effects?: string[];
  finally?: boolean;
  catch_targets?: string[];
  switch_targets?: string[];
  position?: WorkflowPositionDiagnostic;
  retry?: WorkflowRetryDiagnostic;
  source?: WorkflowSourceDiagnostic;
}

export interface WorkflowInvocationValueDiagnostic {
  invocation: WorkflowNodeInvocationID;
  values: WorkflowValueSetRef;
}

export interface WorkflowEdgeValueFlowDiagnostic {
  source_outputs?: WorkflowInvocationValueDiagnostic[];
  target_inputs?: WorkflowInvocationValueDiagnostic[];
  values_omitted?: boolean;
}

export interface WorkflowPlanEdgeDiagnostic {
  from: string;
  to: string;
  kind: 'control' | 'data' | string;
  source?: WorkflowSourceDiagnostic;
  value_flow?: WorkflowEdgeValueFlowDiagnostic;
}

export interface WorkflowPlanDiagnostic {
  id: string;
  version: string;
  digest: string;
  schema_version: string;
  graph_digest: string;
  definition: {
    authority?: string;
    kind?: string;
    id?: string;
    locator?: string;
    version?: string;
    digest?: string;
  };
  provenance: {
    authority?: string;
    origin?: string;
    locator?: string;
    revision?: string;
    digest?: string;
  };
  source_digests?: { format: string; digest: string }[];
  source?: WorkflowSourceDiagnostic;
  nodes: WorkflowPlanNodeDiagnostic[];
  edges?: WorkflowPlanEdgeDiagnostic[];
  activations?: { id: string; kind: string; source?: WorkflowSourceDiagnostic }[];
}

export interface WorkflowFailureDiagnostic {
  code: string;
  message: string;
  retryable?: boolean;
  details?: Record<string, string>;
  masked?: boolean;
}

export interface WorkflowAttemptDiagnostic {
  number: number;
  status: string;
  executor: {
    kind: string;
    version: string;
    target?: string;
    attributes?: Record<string, string>;
    masked?: boolean;
  };
  inputs?: WorkflowValueSetRef;
  outputs?: WorkflowValueSetRef;
  failure?: WorkflowFailureDiagnostic;
  started_at: string;
  finished_at?: string;
  generation: number;
}

export interface WorkflowWaitDiagnostic {
  id: string;
  kind: string;
  status: string;
  wake_source: string;
  visibility: string;
  wake_at?: string;
  deadline?: string;
  payload?: WorkflowValueSetRef;
  resume_values?: WorkflowValueSetRef;
  resolution?: {
    source: string;
    responder_kind: string;
    payload_digest?: string;
    resolved_at: string;
  };
  generation: number;
  created_at: string;
  updated_at: string;
  resolved_at?: string;
}

export interface WorkflowNodeDiagnostic {
  id: WorkflowNodeInvocationID;
  status: string;
  origin?: string;
  memo_key_digest?: string;
  inputs?: WorkflowValueSetRef;
  outputs?: WorkflowValueSetRef;
  latest_attempt?: number;
  priority?: number;
  claim_generation: number;
  generation: number;
  created_at: string;
  updated_at: string;
  source?: WorkflowSourceDiagnostic;
  definition: WorkflowPlanNodeDiagnostic;
  attempts?: WorkflowAttemptDiagnostic[];
  wait?: WorkflowWaitDiagnostic;
  explanation: {
    code: string;
    message: string;
    dependencies?: WorkflowNodeInvocationID[];
    details?: Record<string, string>;
    failure?: WorkflowFailureDiagnostic;
    masked?: boolean;
  };
  upstream?: { node_id: string; invocations?: { id: WorkflowNodeInvocationID; status: string }[] }[];
  downstream?: { node_id: string; declared_effects?: string[]; source?: WorkflowSourceDiagnostic }[];
  pin?: {
    outputs: WorkflowValueSetRef;
    source: WorkflowNodeInvocationID;
    source_plan_digest: string;
    source_origin: string;
    output_schema_digest: string;
    policy_code: string;
    policy_reason: string;
    bound_at: string;
  };
  resources: WorkflowNodeResourceDiagnostic;
  attempts_truncated?: boolean;
}

export interface WorkflowSchedulerResourceID {
  kind: string;
  name?: string;
  run_id?: string;
  node_id?: string;
}

export interface WorkflowSchedulerResourceRequirement {
  resource: WorkflowSchedulerResourceID;
  units: number;
  limit: number;
}

export interface WorkflowSchedulerResourceHolder {
  resource: WorkflowSchedulerResourceID;
  invocation: WorkflowNodeInvocationID;
  units: number;
  claim_generation: number;
  owner: string;
  acquired_at: string;
  expires_at: string;
}

export interface WorkflowSchedulerResourceWaiter {
  invocation: WorkflowNodeInvocationID;
  requirements: WorkflowSchedulerResourceRequirement[];
  blocked: WorkflowSchedulerResourceID[];
  priority?: number;
  enqueued_at: string;
  updated_at: string;
}

export interface WorkflowNodeResourceDiagnostic {
  holders?: WorkflowSchedulerResourceHolder[];
  waiter?: WorkflowSchedulerResourceWaiter;
}

export interface WorkflowRenderedValue {
  type: string;
  payload: unknown;
  producer: { kind: string; reference: string; output?: string };
  media_type: string;
  digest: string;
  redaction: 'public' | 'private' | 'secret' | string;
  retention: string;
  masked: boolean;
}

export interface WorkflowValueSetDiagnostic {
  ref: WorkflowValueSetRef;
  roles: string[];
  values: Record<string, WorkflowRenderedValue>;
}

export interface WorkflowControlDecisionDiagnostic {
  source: WorkflowNodeInvocationID;
  kind: string;
  outcome: string;
  rule_index?: number;
  targets?: WorkflowNodeInvocationID[];
  bind_as?: string;
  error?: WorkflowValueSetRef;
  generation: number;
  created_at: string;
}

export interface WorkflowRenderedEvent {
  sequence: number;
  run_id: string;
  invocation?: WorkflowNodeInvocationID;
  attempt?: WorkflowAttemptID;
  type: string;
  occurred_at: string;
  attributes?: Record<string, string>;
  values?: WorkflowValueSetRef;
  redaction: string;
  retention: string;
  masked: boolean;
}

export interface WorkflowStartPolicyDiagnostic {
  declared_effects: string[];
  required_capabilities?: string[];
  blast_radius: Record<string, number>;
  node_count: number;
  dry_run_available: boolean;
  confirmation_advised: boolean;
  decision: string;
  exposure_ref?: string;
  exposure_masked?: boolean;
}

export interface WorkflowGraphDiagnostic {
  schema_version: string;
  run: {
    id: string;
    plan: { id: string; version: string; digest: string; schema_version: string };
    status: string;
    inputs?: WorkflowValueSetRef;
    outputs?: WorkflowValueSetRef;
    generation: number;
    created_at: string;
    updated_at: string;
  };
  plan: WorkflowPlanDiagnostic;
  nodes: WorkflowNodeDiagnostic[];
  values?: WorkflowValueSetDiagnostic[];
  events?: WorkflowRenderedEvent[];
  control: {
    decisions?: WorkflowControlDecisionDiagnostic[];
    terminal_intent?: {
      intended_status: string;
      reason?: WorkflowFailureDiagnostic;
      status: string;
      finalizers?: { invocation: WorkflowNodeInvocationID; scope?: WorkflowNodeInvocationID[]; order: number }[];
    };
  };
  replay?: {
    source_run_id: string;
    from_node_id: string;
    plan_digest: string;
    created_at: string;
    policy?: { invocation: WorkflowNodeInvocationID; attempt?: WorkflowAttemptID; allow: boolean; code: string; reason: string }[];
  };
  resources?: {
    holders: WorkflowSchedulerResourceHolder[];
    waiters: WorkflowSchedulerResourceWaiter[];
  };
  start_activation?: { activation_id: string; fire_identity_digest: string; occurred_at: string };
  start_policy?: WorkflowStartPolicyDiagnostic;
  activation_attempts?: unknown[];
  capabilities: Record<string, boolean>;
  omissions?: string[];
  truncated: Record<string, boolean>;
}

export interface WorkflowResumeRequest {
  run_id: string;
  identity: { source_authority: string };
  wait_id: string;
  correlation: string;
  token: string;
  wake_source: string;
  payload: {
    type: string;
    inline: unknown;
    producer: { kind: string; reference: string; output: string };
    media_type: string;
    digest: string;
    redaction: string;
    retention: string;
  };
  idempotency_key: string;
}

export interface WorkflowResumeResult {
  outcome: string;
  wait?: { id: string; status: string };
  node?: { id: WorkflowNodeInvocationID; status: string };
  attempt?: { id: WorkflowAttemptID; status: string };
  values?: WorkflowValueSetRef;
}

export interface ValidateResult {
  valid: boolean;
  error?: string;
}

// ── Settings types (matching Go settings.Settings) ────────────────────

export interface HadronSettings {
  blueprint_dir: string;
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
