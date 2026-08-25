import type { Run, RunEvent, ListResponse, Health, EnqueueRunRequest, FileEntry, ValidateResult, Schedule, CreateScheduleRequest, BlueprintInput, ParsedBlueprint, BlueprintMetaSummary, Pipeline, PipelineStage, EnqueuePipelineRequest, Workspace, HadronSettings, TelemetryRunSummary, TelemetryLogEntry, MCPCallDiagnostic, OperationDiagnostic } from './types';
import { isDemoMode } from '../demo/demoMode';
import { DEMO_RUNS, getDemoRunEvents, getDemoRunMCPCalls, getDemoRunOperations, DEMO_SCHEDULES, DEMO_PIPELINES, getDemoPipelineStages, DEMO_TELEMETRY_RUNS, getDemoTelemetryEntries, DEMO_HEALTH, DEMO_WORKSPACES } from '../demo/data';
import { apiFetch, getAPIBaseURL, setAPIBaseURL } from './http';

// ── Base URL management ───────────────────────────────────────────────

// Production uses the daemon origin that served the SPA. Tests and explicitly
// separate development hosts can override this value.
export function setBaseURL(url: string) {
  setAPIBaseURL(url);
}

export function getBaseURL(): string {
  return getAPIBaseURL();
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

export async function listRunMCPCalls(runId: string): Promise<ListResponse<MCPCallDiagnostic> & { count: number }> {
  if (isDemoMode()) {
    const items = getDemoRunMCPCalls(runId);
    return { items, count: items.length };
  }
  return apiFetch<ListResponse<MCPCallDiagnostic> & { count: number }>(`/v1/runs/${runId}/mcp-calls`);
}

export async function listRunOperations(
  runId: string,
  params?: { kind?: string; limit?: number; cursor?: string }
): Promise<ListResponse<OperationDiagnostic> & { count: number; total_count?: number; next_cursor?: string | null }> {
  if (isDemoMode()) {
    const items = getDemoRunOperations(runId).filter(item => !params?.kind || item.kind === params.kind);
    return { items, count: items.length, total_count: items.length, next_cursor: null };
  }
  const q = new URLSearchParams();
  if (params?.kind) q.set('kind', params.kind);
  if (params?.limit) q.set('limit', String(params.limit));
  if (params?.cursor) q.set('cursor', params.cursor);
  const qs = q.toString() ? `?${q.toString()}` : '';
  return apiFetch<ListResponse<OperationDiagnostic> & { count: number; total_count?: number; next_cursor?: string | null }>(`/v1/runs/${runId}/operations${qs}`);
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

// ── Browser capability fences ─────────────────────────────────────────
// These legacy file/settings screens remain outside the graph workflow path.
// Their former desktop-only operations are intentionally unavailable until a
// separately owned daemon contract exists; no private HTTP semantic is added.

function unavailable(capability: string): never {
  throw new Error(`${capability} is unavailable in the daemon-served UI`);
}

export async function getDaemonAddr(): Promise<string> {
  return globalThis.location?.host ?? '';
}

export async function getDaemonStatus(): Promise<string> {
  try { await getHealth(); return 'running'; } catch { return 'unavailable'; }
}

export async function openDirectoryDialog(): Promise<string> { return ''; }
export async function selectDirectoryDialog(): Promise<string> { return ''; }
export async function selectBlueprintFile(): Promise<string> { return ''; }
export async function listFilesInDir(_dir: string): Promise<FileEntry[]> { return []; }
export async function listBlueprintFilesInDir(_dir: string): Promise<FileEntry[]> { return []; }
export async function listPipelineFilesInDir(_dir: string): Promise<FileEntry[]> { return []; }

export async function validateBlueprintFile(_path: string): Promise<ValidateResult> {
  return { valid: false, error: 'Legacy blueprint file validation is unavailable in the daemon-served UI' };
}

export async function getPreference(key: string): Promise<string> {
  return globalThis.localStorage?.getItem(`hadron:${key}`) ?? '';
}

export async function setPreference(key: string, value: string): Promise<void> {
  globalThis.localStorage?.setItem(`hadron:${key}`, value);
}

export async function parseBlueprintInputs(_path: string): Promise<BlueprintInput[]> { return unavailable('Legacy blueprint parsing'); }
export async function readBlueprintFile(_path: string): Promise<string> { return unavailable('Legacy blueprint file reads'); }
export async function parseBlueprintFull(_path: string): Promise<ParsedBlueprint> { return unavailable('Legacy blueprint parsing'); }
export async function saveBlueprintFile(_path: string, _content: string): Promise<void> { return unavailable('Legacy blueprint file writes'); }
export async function createBlueprintFile(_dir: string, _filename: string, _content: string): Promise<string> { return unavailable('Legacy blueprint file creation'); }
export async function deleteBlueprintFile(_path: string): Promise<void> { return unavailable('Legacy blueprint file deletion'); }
export async function createDirectory(_parentDir: string, _name: string): Promise<string> { return unavailable('Legacy source directory mutation'); }
export async function moveBlueprintFile(_srcPath: string, _destDir: string): Promise<string> { return unavailable('Legacy blueprint file moves'); }
export async function copyBlueprintFile(_srcPath: string, _destDir: string): Promise<string> { return unavailable('Legacy blueprint file copies'); }
export async function archiveBlueprintFile(_srcPath: string): Promise<void> { return unavailable('Legacy blueprint file archival'); }
export async function getBlueprintMetadata(_path: string): Promise<BlueprintMetaSummary> { return unavailable('Legacy blueprint metadata'); }
export async function getBlueprintDir(): Promise<string> { return ''; }
export async function setBlueprintDir(_dir: string): Promise<void> { return unavailable('Legacy blueprint settings'); }
export async function getSettings(): Promise<HadronSettings> { return unavailable('Legacy desktop settings'); }
export async function saveSettings(_settings: HadronSettings): Promise<void> { return unavailable('Legacy desktop settings'); }

export async function listTelemetryRuns(): Promise<TelemetryRunSummary[]> {
  return isDemoMode() ? DEMO_TELEMETRY_RUNS : [];
}

export async function readTelemetryLog(runID: string): Promise<TelemetryLogEntry[]> {
  return isDemoMode() ? getDemoTelemetryEntries(runID) : [];
}

export async function deleteTelemetryLog(_runID: string): Promise<void> { return unavailable('Legacy telemetry deletion'); }

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
