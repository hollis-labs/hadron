// Demo data for GUI testing without a running daemon
import type { Run, RunEvent, Schedule, Pipeline, PipelineStage, ListResponse, Health, Workspace, TelemetryRunSummary, TelemetryLogEntry } from '../api/types';

// ── Helpers ──

function hoursAgo(h: number): string {
  return new Date(Date.now() - h * 3600000).toISOString();
}

function minutesAgo(m: number): string {
  return new Date(Date.now() - m * 60000).toISOString();
}

function hoursFromNow(h: number): string {
  return new Date(Date.now() + h * 3600000).toISOString();
}

// ── Runs ──

export const DEMO_RUNS: Run[] = [
  {
    id: 'run-demo-001-a1b2c3d4',
    status: 'success',
    blueprint_path: '/blueprints/deploy-staging.yaml',
    created_at: hoursAgo(1),
    started_at: hoursAgo(1),
    ended_at: minutesAgo(55),
    workspace_id: 'default',
  },
  {
    id: 'run-demo-002-e5f6g7h8',
    status: 'failed',
    blueprint_path: '/blueprints/test-suite.yaml',
    created_at: hoursAgo(2),
    started_at: hoursAgo(2),
    ended_at: minutesAgo(115),
    error_message: 'Exit code 1: test_auth_flow failed — expected 200, got 401',
    workspace_id: 'default',
  },
  {
    id: 'run-demo-003-i9j0k1l2',
    status: 'running',
    blueprint_path: '/blueprints/build-frontend.yaml',
    created_at: minutesAgo(3),
    started_at: minutesAgo(3),
    workspace_id: 'default',
  },
  {
    id: 'run-demo-004-m3n4o5p6',
    status: 'success',
    blueprint_path: '/blueprints/db-backup.yaml',
    created_at: hoursAgo(3),
    started_at: hoursAgo(3),
    ended_at: minutesAgo(175),
    workspace_id: 'default',
  },
  {
    id: 'run-demo-005-q7r8s9t0',
    status: 'success',
    blueprint_path: '/blueprints/deploy-staging.yaml',
    created_at: hoursAgo(5),
    started_at: hoursAgo(5),
    ended_at: hoursAgo(5) + '',
    workspace_id: 'default',
  },
  {
    id: 'run-demo-006-u1v2w3x4',
    status: 'failed',
    blueprint_path: '/blueprints/lint-check.yaml',
    created_at: hoursAgo(6),
    started_at: hoursAgo(6),
    ended_at: minutesAgo(355),
    error_message: 'ESLint found 12 errors and 3 warnings',
    workspace_id: 'default',
  },
  {
    id: 'run-demo-007-y5z6a7b8',
    status: 'success',
    blueprint_path: '/blueprints/build-frontend.yaml',
    created_at: hoursAgo(8),
    started_at: hoursAgo(8),
    ended_at: hoursAgo(8),
    workspace_id: 'default',
  },
  {
    id: 'run-demo-008-c9d0e1f2',
    status: 'success',
    blueprint_path: '/blueprints/node-scaffold.yaml',
    created_at: hoursAgo(10),
    started_at: hoursAgo(10),
    ended_at: hoursAgo(10),
    workspace_id: 'default',
  },
  {
    id: 'run-demo-009-g3h4i5j6',
    status: 'success',
    blueprint_path: '/blueprints/deploy-staging.yaml',
    created_at: hoursAgo(12),
    started_at: hoursAgo(12),
    ended_at: hoursAgo(12),
    workspace_id: 'default',
  },
  {
    id: 'run-demo-010-k7l8m9n0',
    status: 'failed',
    blueprint_path: '/blueprints/test-suite.yaml',
    created_at: hoursAgo(14),
    started_at: hoursAgo(14),
    ended_at: hoursAgo(14),
    error_message: 'Timeout: test exceeded 60s limit',
    workspace_id: 'default',
  },
  {
    id: 'run-demo-011-o1p2q3r4',
    status: 'success',
    blueprint_path: '/blueprints/build-frontend.yaml',
    created_at: hoursAgo(16),
    started_at: hoursAgo(16),
    ended_at: hoursAgo(16),
    workspace_id: 'default',
  },
  {
    id: 'run-demo-012-s5t6u7v8',
    status: 'success',
    blueprint_path: '/blueprints/db-backup.yaml',
    created_at: hoursAgo(20),
    started_at: hoursAgo(20),
    ended_at: hoursAgo(20),
    workspace_id: 'default',
  },
];

// Fix durations to be more realistic
DEMO_RUNS[0].ended_at = new Date(new Date(DEMO_RUNS[0].started_at!).getTime() + 45000).toISOString(); // 45s
DEMO_RUNS[1].ended_at = new Date(new Date(DEMO_RUNS[1].started_at!).getTime() + 23000).toISOString(); // 23s
DEMO_RUNS[3].ended_at = new Date(new Date(DEMO_RUNS[3].started_at!).getTime() + 8200).toISOString();  // 8.2s
DEMO_RUNS[4].ended_at = new Date(new Date(DEMO_RUNS[4].started_at!).getTime() + 52000).toISOString(); // 52s
DEMO_RUNS[5].ended_at = new Date(new Date(DEMO_RUNS[5].started_at!).getTime() + 6100).toISOString();  // 6.1s
DEMO_RUNS[6].ended_at = new Date(new Date(DEMO_RUNS[6].started_at!).getTime() + 38000).toISOString(); // 38s
DEMO_RUNS[7].ended_at = new Date(new Date(DEMO_RUNS[7].started_at!).getTime() + 12400).toISOString(); // 12.4s
DEMO_RUNS[8].ended_at = new Date(new Date(DEMO_RUNS[8].started_at!).getTime() + 41000).toISOString(); // 41s
DEMO_RUNS[9].ended_at = new Date(new Date(DEMO_RUNS[9].started_at!).getTime() + 60000).toISOString(); // 60s
DEMO_RUNS[10].ended_at = new Date(new Date(DEMO_RUNS[10].started_at!).getTime() + 35000).toISOString(); // 35s
DEMO_RUNS[11].ended_at = new Date(new Date(DEMO_RUNS[11].started_at!).getTime() + 9500).toISOString(); // 9.5s

// ── Run Events (for detail page) ──

export function getDemoRunEvents(runId: string): RunEvent[] {
  const run = DEMO_RUNS.find(r => r.id === runId);
  if (!run) return [];
  const base = new Date(run.started_at!).getTime();
  const bp = run.blueprint_path.split('/').pop()?.replace('.yaml', '') ?? 'task';

  if (run.status === 'running') {
    return [
      { id: 1, run_id: runId, event_type: 'run_start', message: 'Blueprint execution started', created_at: new Date(base).toISOString() },
      { id: 2, run_id: runId, event_type: 'step_start', step_name: 'install-deps', message: 'Installing dependencies...', created_at: new Date(base + 500).toISOString() },
      { id: 3, run_id: runId, event_type: 'stdout', step_name: 'install-deps', message: 'npm ci --silent', created_at: new Date(base + 1000).toISOString() },
      { id: 4, run_id: runId, event_type: 'stdout', step_name: 'install-deps', message: 'added 847 packages in 12.4s', created_at: new Date(base + 12000).toISOString() },
      { id: 5, run_id: runId, event_type: 'step_end', step_name: 'install-deps', message: 'Done', created_at: new Date(base + 12500).toISOString() },
      { id: 6, run_id: runId, event_type: 'step_start', step_name: 'build', message: 'Building frontend...', created_at: new Date(base + 13000).toISOString() },
      { id: 7, run_id: runId, event_type: 'stdout', step_name: 'build', message: 'vite v5.4.21 building for production...', created_at: new Date(base + 14000).toISOString() },
      { id: 8, run_id: runId, event_type: 'stdout', step_name: 'build', message: 'transforming (1423 modules)...', created_at: new Date(base + 45000).toISOString() },
    ];
  }

  if (run.status === 'failed') {
    return [
      { id: 1, run_id: runId, event_type: 'run_start', message: 'Blueprint execution started', created_at: new Date(base).toISOString() },
      { id: 2, run_id: runId, event_type: 'step_start', step_name: 'setup', message: 'Setting up environment', created_at: new Date(base + 200).toISOString() },
      { id: 3, run_id: runId, event_type: 'step_end', step_name: 'setup', message: 'Done', created_at: new Date(base + 2000).toISOString() },
      { id: 4, run_id: runId, event_type: 'step_start', step_name: `run-${bp}`, message: `Executing ${bp}`, created_at: new Date(base + 2500).toISOString() },
      { id: 5, run_id: runId, event_type: 'stdout', step_name: `run-${bp}`, message: 'Running checks...', created_at: new Date(base + 3000).toISOString() },
      { id: 6, run_id: runId, event_type: 'stderr', step_name: `run-${bp}`, message: run.error_message ?? 'Unknown error', created_at: new Date(base + 18000).toISOString() },
      { id: 7, run_id: runId, event_type: 'error', step_name: `run-${bp}`, message: `Step failed: ${run.error_message ?? 'exit code 1'}`, created_at: new Date(base + 18500).toISOString() },
      { id: 8, run_id: runId, event_type: 'run_end', message: 'Blueprint execution failed', created_at: new Date(base + 19000).toISOString() },
    ];
  }

  // Success
  return [
    { id: 1, run_id: runId, event_type: 'run_start', message: 'Blueprint execution started', created_at: new Date(base).toISOString() },
    { id: 2, run_id: runId, event_type: 'step_start', step_name: 'setup', message: 'Setting up environment', created_at: new Date(base + 200).toISOString() },
    { id: 3, run_id: runId, event_type: 'stdout', step_name: 'setup', message: 'Environment ready', created_at: new Date(base + 1500).toISOString() },
    { id: 4, run_id: runId, event_type: 'step_end', step_name: 'setup', message: 'Done', created_at: new Date(base + 2000).toISOString() },
    { id: 5, run_id: runId, event_type: 'step_start', step_name: `run-${bp}`, message: `Executing ${bp}`, created_at: new Date(base + 2500).toISOString() },
    { id: 6, run_id: runId, event_type: 'stdout', step_name: `run-${bp}`, message: 'Step 1/3: Preparing...', created_at: new Date(base + 5000).toISOString() },
    { id: 7, run_id: runId, event_type: 'stdout', step_name: `run-${bp}`, message: 'Step 2/3: Processing...', created_at: new Date(base + 15000).toISOString() },
    { id: 8, run_id: runId, event_type: 'stdout', step_name: `run-${bp}`, message: 'Step 3/3: Finalizing...', created_at: new Date(base + 30000).toISOString() },
    { id: 9, run_id: runId, event_type: 'step_end', step_name: `run-${bp}`, message: 'Done', created_at: run.ended_at! },
    { id: 10, run_id: runId, event_type: 'cleanup', step_name: 'cleanup', message: 'Cleaning up temp files', created_at: run.ended_at! },
    { id: 11, run_id: runId, event_type: 'run_end', message: 'Blueprint execution completed successfully', created_at: run.ended_at! },
  ];
}

// ── Schedules ──

export const DEMO_SCHEDULES: Schedule[] = [
  {
    id: 'sch-demo-001',
    workspace_id: 'default',
    name: 'nightly-backup',
    blueprint_path: '/blueprints/db-backup.yaml',
    cron_expr: '0 2 * * *',
    enabled: true,
    created_at: hoursAgo(168),
    updated_at: hoursAgo(2),
    next_run_at: hoursFromNow(6),
  },
  {
    id: 'sch-demo-002',
    workspace_id: 'default',
    name: 'deploy-staging',
    blueprint_path: '/blueprints/deploy-staging.yaml',
    cron_expr: '0 */4 * * *',
    enabled: true,
    created_at: hoursAgo(72),
    updated_at: hoursAgo(4),
    next_run_at: hoursFromNow(2),
  },
  {
    id: 'sch-demo-003',
    workspace_id: 'default',
    name: 'lint-and-test',
    blueprint_path: '/blueprints/test-suite.yaml',
    cron_expr: '*/30 * * * *',
    enabled: true,
    created_at: hoursAgo(48),
    updated_at: hoursAgo(1),
    next_run_at: hoursFromNow(0.5),
  },
  {
    id: 'sch-demo-004',
    workspace_id: 'default',
    name: 'weekly-report',
    blueprint_path: '/blueprints/weekly-report.yaml',
    cron_expr: '0 9 * * 1',
    enabled: false,
    created_at: hoursAgo(336),
    updated_at: hoursAgo(100),
    next_run_at: null,
  },
  {
    id: 'sch-demo-005',
    workspace_id: 'default',
    name: 'one-time-migration',
    blueprint_path: '/blueprints/data-migration.yaml',
    cron_expr: '',
    enabled: true,
    created_at: hoursAgo(1),
    updated_at: hoursAgo(1),
    next_run_at: hoursFromNow(24),
  },
];

// ── Pipelines ──

export const DEMO_PIPELINES: Pipeline[] = [
  {
    id: 'pl-demo-001-a1b2',
    workspace_id: 'default',
    pipeline_path: '/pipelines/full-deploy.yaml',
    status: 'success',
    created_at: hoursAgo(2),
    started_at: hoursAgo(2),
    ended_at: minutesAgo(90),
  },
  {
    id: 'pl-demo-002-c3d4',
    workspace_id: 'default',
    pipeline_path: '/pipelines/ci-pipeline.yaml',
    status: 'failed',
    error_message: 'Stage "run-tests" failed',
    created_at: hoursAgo(5),
    started_at: hoursAgo(5),
    ended_at: hoursAgo(5),
  },
  {
    id: 'pl-demo-003-e5f6',
    workspace_id: 'default',
    pipeline_path: '/pipelines/full-deploy.yaml',
    status: 'running',
    created_at: minutesAgo(5),
    started_at: minutesAgo(5),
  },
  {
    id: 'pl-demo-004-g7h8',
    workspace_id: 'default',
    pipeline_path: '/pipelines/nightly-build.yaml',
    status: 'success',
    created_at: hoursAgo(24),
    started_at: hoursAgo(24),
    ended_at: hoursAgo(24),
  },
];

// Fix pipeline durations
DEMO_PIPELINES[0].ended_at = new Date(new Date(DEMO_PIPELINES[0].started_at!).getTime() + 180000).toISOString(); // 3m
DEMO_PIPELINES[1].ended_at = new Date(new Date(DEMO_PIPELINES[1].started_at!).getTime() + 95000).toISOString();  // 1m35s
DEMO_PIPELINES[3].ended_at = new Date(new Date(DEMO_PIPELINES[3].started_at!).getTime() + 240000).toISOString(); // 4m

export function getDemoPipelineStages(pipelineId: string): PipelineStage[] {
  if (pipelineId === 'pl-demo-001-a1b2') {
    return [
      { id: 1, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 0, stage_name: 'build-backend', run_id: 'run-demo-004-m3n4o5p6', status: 'success', created_at: hoursAgo(2), updated_at: hoursAgo(2) },
      { id: 2, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 1, stage_name: 'build-frontend', run_id: 'run-demo-007-y5z6a7b8', status: 'success', created_at: hoursAgo(2), updated_at: hoursAgo(2) },
      { id: 3, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 2, stage_name: 'deploy', run_id: 'run-demo-001-a1b2c3d4', status: 'success', created_at: hoursAgo(2), updated_at: hoursAgo(2) },
    ];
  }
  if (pipelineId === 'pl-demo-002-c3d4') {
    return [
      { id: 4, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 0, stage_name: 'lint', run_id: 'run-demo-006-u1v2w3x4', status: 'success', created_at: hoursAgo(5), updated_at: hoursAgo(5) },
      { id: 5, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 1, stage_name: 'run-tests', run_id: 'run-demo-002-e5f6g7h8', status: 'failed', created_at: hoursAgo(5), updated_at: hoursAgo(5) },
    ];
  }
  if (pipelineId === 'pl-demo-003-e5f6') {
    return [
      { id: 6, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 0, stage_name: 'install', run_id: 'run-demo-008-c9d0e1f2', status: 'success', created_at: minutesAgo(5), updated_at: minutesAgo(4) },
      { id: 7, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 1, stage_name: 'build', run_id: 'run-demo-003-i9j0k1l2', status: 'running', created_at: minutesAgo(3), updated_at: minutesAgo(1) },
      { id: 8, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 2, stage_name: 'deploy', run_id: '', status: 'queued', created_at: minutesAgo(5), updated_at: minutesAgo(5) },
    ];
  }
  return [
    { id: 9, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 0, stage_name: 'build', run_id: 'run-demo-011-o1p2q3r4', status: 'success', created_at: hoursAgo(24), updated_at: hoursAgo(24) },
    { id: 10, workspace_id: 'default', pipeline_run_id: pipelineId, stage_index: 1, stage_name: 'test', run_id: 'run-demo-009-g3h4i5j6', status: 'success', created_at: hoursAgo(24), updated_at: hoursAgo(24) },
  ];
}

// ── Telemetry ──

export const DEMO_TELEMETRY_RUNS: TelemetryRunSummary[] = [
  { run_id: 'run-demo-001-a1b2c3d4', file_size: 4820, modified_at: hoursAgo(1), event_count: 11 },
  { run_id: 'run-demo-002-e5f6g7h8', file_size: 3210, modified_at: hoursAgo(2), event_count: 8 },
  { run_id: 'run-demo-003-i9j0k1l2', file_size: 2100, modified_at: minutesAgo(3), event_count: 8 },
  { run_id: 'run-demo-004-m3n4o5p6', file_size: 5400, modified_at: hoursAgo(3), event_count: 11 },
  { run_id: 'run-demo-007-y5z6a7b8', file_size: 3800, modified_at: hoursAgo(8), event_count: 11 },
];

export function getDemoTelemetryEntries(runId: string): TelemetryLogEntry[] {
  const run = DEMO_RUNS.find(r => r.id === runId);
  if (!run?.started_at) return [];
  const base = new Date(run.started_at).getTime();
  return [
    { ts: new Date(base).toISOString(), level: 'info', event: 'run_start', run_id: runId, msg: 'Blueprint execution started' },
    { ts: new Date(base + 200).toISOString(), level: 'info', event: 'step_start', run_id: runId, section: 'main', step: 'setup', msg: 'Setting up environment' },
    { ts: new Date(base + 500).toISOString(), level: 'debug', event: 'env_resolve', run_id: runId, section: 'main', step: 'setup', msg: 'Resolved 3 environment variables' },
    { ts: new Date(base + 2000).toISOString(), level: 'info', event: 'step_end', run_id: runId, section: 'main', step: 'setup', msg: 'Complete (1.8s)' },
    { ts: new Date(base + 5000).toISOString(), level: 'info', event: 'step_start', run_id: runId, section: 'main', step: 'execute', msg: 'Executing main task' },
    { ts: new Date(base + 8000).toISOString(), level: 'debug', event: 'cmd_output', run_id: runId, section: 'main', step: 'execute', msg: 'Process output captured (245 bytes)' },
    ...(run.status === 'failed' ? [
      { ts: new Date(base + 15000).toISOString(), level: 'warn' as const, event: 'retry', run_id: runId, section: 'main', step: 'execute', msg: 'Retrying (attempt 2/3)...' },
      { ts: new Date(base + 18000).toISOString(), level: 'error' as const, event: 'step_fail', run_id: runId, section: 'main', step: 'execute', msg: run.error_message ?? 'Step failed' },
    ] : [
      { ts: new Date(base + 25000).toISOString(), level: 'info' as const, event: 'step_end', run_id: runId, section: 'main', step: 'execute', msg: 'Complete (20s)' },
    ]),
    { ts: new Date(base + 30000).toISOString(), level: 'info', event: 'run_end', run_id: runId, msg: run.status === 'failed' ? 'Run failed' : 'Run completed successfully' },
  ];
}

// ── Health ──

export const DEMO_HEALTH: Health = {
  status: 'ok',
  version: '0.4.0-demo',
  service: 'hadrond',
};

// ── Workspaces ──

export const DEMO_WORKSPACES: Workspace[] = [
  { id: 'default', name: 'Default', created_at: hoursAgo(720), updated_at: hoursAgo(1) },
  { id: 'staging', name: 'Staging', created_at: hoursAgo(168), updated_at: hoursAgo(24) },
  { id: 'dev', name: 'Development', created_at: hoursAgo(48), updated_at: hoursAgo(12) },
];
