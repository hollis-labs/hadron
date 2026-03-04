import type { Run, RunEvent, ListResponse, Health, EnqueueRunRequest, FileEntry, ValidateResult, Schedule, CreateScheduleRequest, BlueprintInput, ParsedBlueprint, BlueprintMetaSummary, Pipeline, PipelineStage, EnqueuePipelineRequest, Workspace, HadronSettings, TelemetryRunSummary, TelemetryLogEntry } from './types';
import { isDemoMode } from '../demo/demoMode';
import { DEMO_RUNS, getDemoRunEvents, DEMO_SCHEDULES, DEMO_PIPELINES, getDemoPipelineStages, DEMO_TELEMETRY_RUNS, getDemoTelemetryEntries, DEMO_HEALTH, DEMO_WORKSPACES } from '../demo/data';

// ── Base URL management ───────────────────────────────────────────────

let _base = 'http://127.0.0.1:8095';

export function setBaseURL(url: string) {
  _base = url;
}

export function getBaseURL(): string {
  return _base;
}

// ── Generic fetch helper ──────────────────────────────────────────────

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(`${_base}${path}`, {
    headers: { 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  });
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({ error: resp.statusText }));
    throw new Error(err.error ?? `HTTP ${resp.status}`);
  }
  return resp.json() as Promise<T>;
}

// ── Health ────────────────────────────────────────────────────────────

export async function getHealth(): Promise<Health> {
  if (isDemoMode()) return DEMO_HEALTH;
  return apiFetch<Health>('/v1/health');
}

// ── Runs ──────────────────────────────────────────────────────────────

export async function listRuns(params?: {
  workspace_id?: string;
  limit?: number;
  cursor?: string;
}): Promise<ListResponse<Run>> {
  if (isDemoMode()) {
    const items = params?.limit ? DEMO_RUNS.slice(0, params.limit) : DEMO_RUNS;
    return { items };
  }
  const q = new URLSearchParams();
  if (params?.workspace_id) q.set('workspace_id', params.workspace_id);
  if (params?.limit) q.set('limit', String(params.limit));
  if (params?.cursor) q.set('cursor', params.cursor);
  const qs = q.toString() ? `?${q.toString()}` : '';
  return apiFetch<ListResponse<Run>>(`/v1/runs${qs}`);
}

export async function getRun(id: string): Promise<Run> {
  if (isDemoMode()) {
    const run = DEMO_RUNS.find(r => r.id === id);
    if (run) return run;
    throw new Error('Run not found');
  }
  return apiFetch<Run>(`/v1/runs/${id}`);
}

export async function enqueueRun(req: EnqueueRunRequest): Promise<Run> {
  return apiFetch<Run>('/v1/runs', {
    method: 'POST',
    body: JSON.stringify(req),
  });
}

export async function cancelRun(id: string): Promise<void> {
  await apiFetch(`/v1/runs/${id}`, { method: 'DELETE' });
}

// ── Run events ────────────────────────────────────────────────────────

export async function listRunEvents(
  runId: string,
  params?: { limit?: number; cursor?: string }
): Promise<ListResponse<RunEvent>> {
  if (isDemoMode()) return { items: getDemoRunEvents(runId) };
  const q = new URLSearchParams();
  if (params?.limit) q.set('limit', String(params.limit));
  if (params?.cursor) q.set('cursor', params.cursor);
  const qs = q.toString() ? `?${q.toString()}` : '';
  return apiFetch<ListResponse<RunEvent>>(`/v1/runs/${runId}/events${qs}`);
}

// ── Schedules ─────────────────────────────────────────────────────────

export async function listSchedules(params?: {
  workspace_id?: string;
}): Promise<ListResponse<Schedule>> {
  if (isDemoMode()) return { items: DEMO_SCHEDULES };
  const q = new URLSearchParams();
  if (params?.workspace_id) q.set('workspace_id', params.workspace_id);
  const qs = q.toString() ? `?${q.toString()}` : '';
  return apiFetch<ListResponse<Schedule>>(`/v1/schedules${qs}`);
}

export async function createSchedule(req: CreateScheduleRequest): Promise<Schedule> {
  return apiFetch<Schedule>('/v1/schedules', {
    method: 'POST',
    body: JSON.stringify(req),
  });
}

export async function patchSchedule(id: string, updates: { name?: string; cron_expr?: string; blueprint_path?: string; enabled?: boolean }): Promise<Schedule> {
  return apiFetch<Schedule>(`/v1/schedules/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(updates),
  });
}

export async function deleteSchedule(id: string): Promise<void> {
  await apiFetch(`/v1/schedules/${id}`, { method: 'DELETE' });
}

// ── Blueprint validation ──────────────────────────────────────────────
// Note: validation by path goes through the Go binding (reads file server-side).

// ── Wails Go bindings ─────────────────────────────────────────────────

/* eslint-disable @typescript-eslint/no-explicit-any */
const go: any = (window as any).go?.main?.App;

export async function getDaemonAddr(): Promise<string> {
  if (go?.GetDaemonAddr) return go.GetDaemonAddr();
  return '';
}

export async function getDaemonStatus(): Promise<string> {
  if (go?.GetDaemonStatus) return go.GetDaemonStatus();
  return 'stopped';
}

export async function openDirectoryDialog(): Promise<string> {
  if (go?.OpenDirectoryDialog) return go.OpenDirectoryDialog();
  return '';
}

export async function selectDirectoryDialog(): Promise<string> {
  if (go?.SelectDirectoryDialog) return go.SelectDirectoryDialog();
  return '';
}

export async function listFilesInDir(dir: string): Promise<FileEntry[]> {
  if (go?.ListFilesInDir) return go.ListFilesInDir(dir);
  return [];
}

export async function validateBlueprintFile(path: string): Promise<ValidateResult> {
  if (go?.ValidateBlueprintFile) return go.ValidateBlueprintFile(path);
  return { valid: false, error: 'Wails binding not available' };
}

export async function selectBlueprintFile(): Promise<string> {
  if (go?.SelectBlueprintFile) return go.SelectBlueprintFile();
  return '';
}

// ── Preferences (Go binding wrappers) ─────────────────────────────────

export async function getPreference(key: string): Promise<string> {
  if (go?.GetPreference) return go.GetPreference(key);
  return '';
}

export async function setPreference(key: string, value: string): Promise<void> {
  if (go?.SetPreference) return go.SetPreference(key, value);
}

// ── Blueprint inputs (Go binding) ─────────────────────────────────────

export async function parseBlueprintInputs(path: string): Promise<BlueprintInput[]> {
  if (go?.ParseBlueprintInputs) return go.ParseBlueprintInputs(path);
  return [];
}

// ── Blueprint file operations (Go bindings) ───────────────────────────

export async function readBlueprintFile(path: string): Promise<string> {
  if (go?.ReadBlueprintFile) return go.ReadBlueprintFile(path);
  return '';
}

export async function parseBlueprintFull(path: string): Promise<ParsedBlueprint> {
  if (go?.ParseBlueprintFull) {
    const json = await go.ParseBlueprintFull(path);
    return JSON.parse(json);
  }
  throw new Error('Wails binding not available');
}

export async function saveBlueprintFile(path: string, content: string): Promise<void> {
  if (go?.SaveBlueprintFile) return go.SaveBlueprintFile(path, content);
  throw new Error('Wails binding not available');
}

export async function createBlueprintFile(dir: string, filename: string, content: string): Promise<string> {
  if (go?.CreateBlueprintFile) return go.CreateBlueprintFile(dir, filename, content);
  throw new Error('Wails binding not available');
}

export async function deleteBlueprintFile(path: string): Promise<void> {
  if (go?.DeleteBlueprintFile) return go.DeleteBlueprintFile(path);
  throw new Error('Wails binding not available');
}

export async function createDirectory(parentDir: string, name: string): Promise<string> {
  if (go?.CreateDirectory) return go.CreateDirectory(parentDir, name);
  throw new Error('Wails binding not available');
}

export async function moveBlueprintFile(srcPath: string, destDir: string): Promise<string> {
  if (go?.MoveBlueprintFile) return go.MoveBlueprintFile(srcPath, destDir);
  throw new Error('Wails binding not available');
}

export async function copyBlueprintFile(srcPath: string, destDir: string): Promise<string> {
  if (go?.CopyBlueprintFile) return go.CopyBlueprintFile(srcPath, destDir);
  throw new Error('Wails binding not available');
}

export async function archiveBlueprintFile(srcPath: string): Promise<void> {
  if (go?.ArchiveBlueprintFile) return go.ArchiveBlueprintFile(srcPath);
  throw new Error('Wails binding not available');
}

export async function getBlueprintMetadata(path: string): Promise<BlueprintMetaSummary> {
  if (go?.GetBlueprintMetadata) {
    const json = await go.GetBlueprintMetadata(path);
    return JSON.parse(json);
  }
  throw new Error('Wails binding not available');
}

// ── Blueprint directory (Go bindings) ─────────────────────────────────

export async function getBlueprintDir(): Promise<string> {
  if (go?.GetBlueprintDir) return go.GetBlueprintDir();
  return '';
}

export async function setBlueprintDir(dir: string): Promise<void> {
  if (go?.SetBlueprintDir) return go.SetBlueprintDir(dir);
}

// ── Settings (Go bindings) ────────────────────────────────────────────

export async function getSettings(): Promise<HadronSettings> {
  if (go?.GetSettings) {
    const json = await go.GetSettings();
    return JSON.parse(json);
  }
  throw new Error('Wails binding not available');
}

export async function saveSettings(s: HadronSettings): Promise<void> {
  if (go?.SaveSettings) return go.SaveSettings(JSON.stringify(s));
  throw new Error('Wails binding not available');
}

// ── Telemetry (Go bindings) ───────────────────────────────────────────

export async function listTelemetryRuns(): Promise<TelemetryRunSummary[]> {
  if (isDemoMode()) return DEMO_TELEMETRY_RUNS;
  if (go?.ListTelemetryRuns) {
    const json = await go.ListTelemetryRuns();
    if (!json) return [];
    const parsed = JSON.parse(json);
    return Array.isArray(parsed) ? parsed : [];
  }
  return [];
}

export async function readTelemetryLog(runID: string): Promise<TelemetryLogEntry[]> {
  if (isDemoMode()) return getDemoTelemetryEntries(runID);
  if (go?.ReadTelemetryLog) {
    const json = await go.ReadTelemetryLog(runID);
    if (!json) return [];
    const parsed = JSON.parse(json);
    return Array.isArray(parsed) ? parsed : [];
  }
  return [];
}

export async function deleteTelemetryLog(runID: string): Promise<void> {
  if (go?.DeleteTelemetryLog) return go.DeleteTelemetryLog(runID);
  throw new Error('Wails binding not available');
}

// ── Pipelines (REST) ──────────────────────────────────────────────────

export async function listPipelines(params?: {
  workspace_id?: string;
  limit?: number;
}): Promise<ListResponse<Pipeline>> {
  if (isDemoMode()) return { items: DEMO_PIPELINES };
  const q = new URLSearchParams();
  if (params?.workspace_id) q.set('workspace_id', params.workspace_id);
  if (params?.limit) q.set('limit', String(params.limit));
  const qs = q.toString() ? `?${q.toString()}` : '';
  return apiFetch<ListResponse<Pipeline>>(`/v1/pipelines${qs}`);
}

export async function enqueuePipeline(req: EnqueuePipelineRequest): Promise<Pipeline> {
  return apiFetch<Pipeline>('/v1/pipelines', {
    method: 'POST',
    body: JSON.stringify(req),
  });
}

export async function getPipelineStages(pipelineId: string): Promise<{ items: PipelineStage[] }> {
  if (isDemoMode()) return { items: getDemoPipelineStages(pipelineId) };
  return apiFetch<{ items: PipelineStage[] }>(`/v1/pipelines/${pipelineId}/stages`);
}

// ── Workspaces (REST) ─────────────────────────────────────────────────

export async function listWorkspaces(): Promise<ListResponse<Workspace>> {
  if (isDemoMode()) return { items: DEMO_WORKSPACES };
  return apiFetch<ListResponse<Workspace>>('/v1/workspaces');
}

export async function createWorkspace(id: string, name?: string): Promise<Workspace> {
  return apiFetch<Workspace>('/v1/workspaces', {
    method: 'POST',
    body: JSON.stringify({ id, name: name ?? id }),
  });
}
