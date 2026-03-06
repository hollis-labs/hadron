import { useState, useCallback } from 'react';
import { toast } from 'sonner';
import { FolderOpen, Play, ChevronLeft, Folder, FileCode, X, Layers, Plus, Pencil, Trash2, ArrowUp, ArrowDown } from 'lucide-react';
import { usePoll } from '../hooks/usePoll';
import { openDirectoryDialog, listFilesInDir, enqueuePipeline, listPipelines, getPipelineStages, readBlueprintFile, saveBlueprintFile, createBlueprintFile, deleteBlueprintFile, selectBlueprintFile } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import type { FileEntry, Pipeline, PipelineStage } from '../api/types';

interface PipelinesPageProps {
  daemonStatus: string;
  workspaceId: string;
  onOpenRun: (runId: string) => void;
  onOpenPipeline?: (path: string) => void;
}

function formatDuration(start?: string | null, end?: string | null): string {
  if (!start || !end) return '—';
  const ms = new Date(end).getTime() - new Date(start).getTime();
  if (ms < 0) return '—';
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatTime(ts?: string | null): string {
  if (!ts) return '—';
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function shortPath(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts.slice(-2).join('/');
}

// Pipeline editor types
interface PipelineStageForm {
  name: string;
  blueprint_path: string;
  inputs: Record<string, string>;
  condition: string; // if: field
}

interface PipelineForm {
  name: string;
  stop_on_fail: boolean;
  stages: PipelineStageForm[];
  inputs: Record<string, string>; // top-level pipeline inputs
}

const EMPTY_STAGE: PipelineStageForm = { name: '', blueprint_path: '', inputs: {}, condition: '' };
const EMPTY_FORM: PipelineForm = { name: '', stop_on_fail: true, stages: [{ ...EMPTY_STAGE }], inputs: {} };

// Simple YAML serializer for pipeline spec
function serializePipeline(form: PipelineForm): string {
  let yaml = `meta:\n  name: "${form.name}"\n\n`;
  yaml += `stop_on_fail: ${form.stop_on_fail}\n\n`;
  yaml += `stages:\n`;
  for (const stage of form.stages) {
    yaml += `  - name: "${stage.name}"\n`;
    yaml += `    blueprint_path: "${stage.blueprint_path}"\n`;
    if (stage.condition.trim()) {
      yaml += `    if: "${stage.condition}"\n`;
    }
    const inputKeys = Object.keys(stage.inputs);
    if (inputKeys.length > 0) {
      yaml += `    inputs:\n`;
      for (const key of inputKeys) {
        yaml += `      ${key}: "${stage.inputs[key]}"\n`;
      }
    }
    yaml += `\n`;
  }
  // Top-level pipeline inputs
  const topInputKeys = Object.keys(form.inputs);
  if (topInputKeys.length > 0) {
    yaml += `inputs:\n`;
    for (const key of topInputKeys) {
      yaml += `  ${key}: "${form.inputs[key]}"\n`;
    }
  }
  return yaml.trimEnd() + '\n';
}

// Helper: strip quotes from a YAML value
function unquote(v: string): string {
  return v.trim().replace(/^["']/, '').replace(/["']$/, '');
}

// Indentation-aware YAML parser for pipeline spec
function parsePipelineYaml(content: string): PipelineForm | null {
  try {
    const form: PipelineForm = { name: '', stop_on_fail: true, stages: [], inputs: {} };
    const lines = content.split('\n');

    // State machine: track which top-level block we're in
    type Section = 'none' | 'meta' | 'stages' | 'inputs';
    let section: Section = 'none';
    let currentStage: PipelineStageForm | null = null;
    let inStageInputs = false;

    for (const line of lines) {
      // Skip blank lines and comments
      if (line.trim() === '' || line.trim().startsWith('#')) continue;

      // Measure indentation (number of leading spaces)
      const indent = line.search(/\S/);

      // Top-level keys (indent 0)
      if (indent === 0) {
        const trimmed = line.trim();
        if (trimmed === 'meta:') {
          // Flush any pending stage
          if (currentStage) { form.stages.push(currentStage); currentStage = null; }
          inStageInputs = false;
          section = 'meta';
          continue;
        }
        if (trimmed.startsWith('stop_on_fail:')) {
          if (currentStage) { form.stages.push(currentStage); currentStage = null; }
          inStageInputs = false;
          section = 'none';
          form.stop_on_fail = trimmed.includes('true');
          continue;
        }
        if (trimmed === 'stages:') {
          if (currentStage) { form.stages.push(currentStage); currentStage = null; }
          inStageInputs = false;
          section = 'stages';
          continue;
        }
        if (trimmed === 'inputs:') {
          if (currentStage) { form.stages.push(currentStage); currentStage = null; }
          inStageInputs = false;
          section = 'inputs';
          continue;
        }
        // Unknown top-level key — skip
        section = 'none';
        continue;
      }

      // meta: block (indent 2)
      if (section === 'meta' && indent >= 2) {
        const trimmed = line.trim();
        if (trimmed.startsWith('name:')) {
          form.name = unquote(trimmed.slice(5));
        }
        continue;
      }

      // stages: block
      if (section === 'stages') {
        const trimmed = line.trim();

        // New stage item: "  - name: ..."
        if (trimmed.startsWith('- ')) {
          if (currentStage) form.stages.push(currentStage);
          inStageInputs = false;
          currentStage = { name: '', blueprint_path: '', inputs: {}, condition: '' };
          // The "- " prefix may contain name: on same line
          const after = trimmed.slice(2).trim();
          if (after.startsWith('name:')) {
            currentStage.name = unquote(after.slice(5));
          }
          continue;
        }

        // Stage properties (indent 4+)
        if (currentStage && indent >= 4) {
          if (inStageInputs && indent >= 6) {
            // key: value inside stage inputs
            const colonIdx = trimmed.indexOf(':');
            if (colonIdx > 0) {
              const key = trimmed.slice(0, colonIdx).trim();
              const val = unquote(trimmed.slice(colonIdx + 1));
              currentStage.inputs[key] = val;
            }
            continue;
          }
          // Not inside inputs (or back to indent 4)
          inStageInputs = false;
          if (trimmed.startsWith('blueprint_path:')) {
            currentStage.blueprint_path = unquote(trimmed.slice(15));
          } else if (trimmed.startsWith('name:')) {
            currentStage.name = unquote(trimmed.slice(5));
          } else if (trimmed.startsWith('if:')) {
            currentStage.condition = unquote(trimmed.slice(3));
          } else if (trimmed === 'inputs:') {
            inStageInputs = true;
          }
          continue;
        }
        continue;
      }

      // Top-level inputs: block (indent 2)
      if (section === 'inputs' && indent >= 2) {
        const trimmed = line.trim();
        const colonIdx = trimmed.indexOf(':');
        if (colonIdx > 0) {
          const key = trimmed.slice(0, colonIdx).trim();
          const val = unquote(trimmed.slice(colonIdx + 1));
          form.inputs[key] = val;
        }
        continue;
      }
    }

    // Flush last stage
    if (currentStage) form.stages.push(currentStage);
    if (form.stages.length === 0) form.stages.push({ ...EMPTY_STAGE });

    return form;
  } catch {
    return null;
  }
}

export function PipelinesPage({ daemonStatus, workspaceId, onOpenRun, onOpenPipeline }: PipelinesPageProps) {
  const [rootDir, setRootDir] = useState<string>('');
  const [currentDir, setCurrentDir] = useState<string>('');
  const [entries, setEntries] = useState<FileEntry[]>([]);
  const [dirError, setDirError] = useState<string | null>(null);
  const [runningPaths, setRunningPaths] = useState<Set<string>>(new Set());

  // Stage detail modal
  const [stageModal, setStageModal] = useState<{ pipelineId: string; stages: PipelineStage[] } | null>(null);
  const [stagesLoading, setStagesLoading] = useState(false);

  // Pipeline editor modal
  const [editorMode, setEditorMode] = useState<'create' | 'edit' | null>(null);
  const [editorForm, setEditorForm] = useState<PipelineForm>(EMPTY_FORM);
  const [editorPath, setEditorPath] = useState<string | null>(null); // only for edit
  const [editorSaving, setEditorSaving] = useState(false);
  const [editorError, setEditorError] = useState<string | null>(null);

  // Delete confirmation
  const [deleteTarget, setDeleteTarget] = useState<FileEntry | null>(null);

  const fetcher = useCallback(
    () => listPipelines({ workspace_id: workspaceId, limit: 20 }),
    [workspaceId]
  );
  const { data: pipelineData } = usePoll(fetcher, 5000, daemonStatus === 'running');
  const pipelines: Pipeline[] = pipelineData?.items ?? [];

  const loadDir = async (dir: string) => {
    setDirError(null);
    try {
      const items = await listFilesInDir(dir);
      setEntries(items ?? []);
    } catch (err) {
      setDirError(String(err));
      setEntries([]);
    }
  };

  const handleOpenFolder = async () => {
    const dir = await openDirectoryDialog();
    if (dir) {
      setRootDir(dir);
      setCurrentDir(dir);
      loadDir(dir);
    }
  };

  const handleDrillDown = (entry: FileEntry) => {
    if (entry.isDir) {
      setCurrentDir(entry.path);
      loadDir(entry.path);
    }
  };

  const handleBack = () => {
    if (!currentDir || currentDir === rootDir) return;
    const parts = currentDir.split(/[/\\]/);
    parts.pop();
    const parent = parts.join('/') || '/';
    setCurrentDir(parent);
    loadDir(parent);
  };

  const handleRun = async (entry: FileEntry) => {
    if (daemonStatus !== 'running') return;
    setRunningPaths(prev => new Set(prev).add(entry.path));
    try {
      await enqueuePipeline({ pipeline_path: entry.path, workspace_id: workspaceId });
      toast.success('Pipeline started');
    } catch (err) {
      toast.error(`Failed to run pipeline: ${err}`);
    } finally {
      setRunningPaths(prev => {
        const next = new Set(prev);
        next.delete(entry.path);
        return next;
      });
    }
  };

  const handleShowStages = async (pipeline: Pipeline) => {
    setStagesLoading(true);
    try {
      const res = await getPipelineStages(pipeline.id);
      setStageModal({ pipelineId: pipeline.id, stages: res.items ?? [] });
    } catch (err) {
      toast.error(`Failed to fetch stages: ${err}`);
    } finally {
      setStagesLoading(false);
    }
  };

  // ── Pipeline CRUD handlers ──

  const handleNewPipeline = () => {
    if (!currentDir) { toast.error('Open a folder first'); return; }
    setEditorMode('create');
    setEditorForm({ ...EMPTY_FORM, stages: [{ ...EMPTY_STAGE }] });
    setEditorPath(null);
    setEditorError(null);
  };

  const handleEditPipeline = async (entry: FileEntry) => {
    try {
      const content = await readBlueprintFile(entry.path);
      const form = parsePipelineYaml(content);
      if (!form) {
        toast.error('Failed to parse pipeline file');
        return;
      }
      setEditorMode('edit');
      setEditorForm(form);
      setEditorPath(entry.path);
      setEditorError(null);
    } catch (err) {
      toast.error(`Failed to read file: ${err}`);
    }
  };

  const handleEditorSave = async () => {
    if (!editorForm.name.trim()) { setEditorError('Pipeline name is required'); return; }
    const validStages = editorForm.stages.filter(s => s.name.trim() && s.blueprint_path.trim());
    if (validStages.length === 0) { setEditorError('At least one stage with name and blueprint path is required'); return; }
    setEditorError(null);
    setEditorSaving(true);
    try {
      const yaml = serializePipeline({ ...editorForm, stages: validStages });
      if (editorMode === 'create') {
        const slug = editorForm.name.trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/-+$/, '');
        const filename = `${slug}.yaml`;
        await createBlueprintFile(currentDir, filename, yaml);
        toast.success(`Pipeline "${filename}" created`);
      } else if (editorPath) {
        await saveBlueprintFile(editorPath, yaml);
        toast.success('Pipeline updated');
      }
      setEditorMode(null);
      loadDir(currentDir);
    } catch (err) {
      toast.error(`Save failed: ${err}`);
      setEditorError(String(err));
    } finally {
      setEditorSaving(false);
    }
  };

  const handleDeletePipeline = async () => {
    if (!deleteTarget) return;
    try {
      await deleteBlueprintFile(deleteTarget.path);
      toast.success('Pipeline file deleted');
      setDeleteTarget(null);
      loadDir(currentDir);
    } catch (err) {
      toast.error(`Delete failed: ${err}`);
    }
  };

  // Stage management helpers
  const addStage = () => {
    setEditorForm(f => ({ ...f, stages: [...f.stages, { ...EMPTY_STAGE }] }));
  };

  const removeStage = (index: number) => {
    setEditorForm(f => ({ ...f, stages: f.stages.filter((_, i) => i !== index) }));
  };

  const updateStage = (index: number, field: keyof PipelineStageForm, value: string) => {
    setEditorForm(f => ({
      ...f,
      stages: f.stages.map((s, i) => i === index ? { ...s, [field]: value } : s),
    }));
  };

  const moveStage = (index: number, direction: -1 | 1) => {
    const newIndex = index + direction;
    if (newIndex < 0 || newIndex >= editorForm.stages.length) return;
    setEditorForm(f => {
      const stages = [...f.stages];
      [stages[index], stages[newIndex]] = [stages[newIndex], stages[index]];
      return { ...f, stages };
    });
  };

  const browseStagePath = async (index: number) => {
    const path = await selectBlueprintFile();
    if (path) updateStage(index, 'blueprint_path', path);
  };

  // Stage input management
  const addStageInput = (stageIndex: number) => {
    setEditorForm(f => ({
      ...f,
      stages: f.stages.map((s, i) => {
        if (i !== stageIndex) return s;
        // Find a unique placeholder key
        let k = '';
        let n = 0;
        while (k in s.inputs) { n++; k = `key${n}`; }
        return { ...s, inputs: { ...s.inputs, [k]: '' } };
      }),
    }));
  };

  const updateStageInput = (stageIndex: number, oldKey: string, newKey: string, value: string) => {
    setEditorForm(f => ({
      ...f,
      stages: f.stages.map((s, i) => {
        if (i !== stageIndex) return s;
        const entries = Object.entries(s.inputs).map(([k, v]) =>
          k === oldKey ? [newKey, value] : [k, v]
        );
        return { ...s, inputs: Object.fromEntries(entries) };
      }),
    }));
  };

  const removeStageInput = (stageIndex: number, key: string) => {
    setEditorForm(f => ({
      ...f,
      stages: f.stages.map((s, i) => {
        if (i !== stageIndex) return s;
        const { [key]: _, ...rest } = s.inputs;
        return { ...s, inputs: rest };
      }),
    }));
  };

  // Top-level pipeline input management
  const addPipelineInput = () => {
    setEditorForm(f => {
      let k = '';
      let n = 0;
      while (k in f.inputs) { n++; k = `key${n}`; }
      return { ...f, inputs: { ...f.inputs, [k]: '' } };
    });
  };

  const updatePipelineInput = (oldKey: string, newKey: string, value: string) => {
    setEditorForm(f => {
      const entries = Object.entries(f.inputs).map(([k, v]) =>
        k === oldKey ? [newKey, value] : [k, v]
      );
      return { ...f, inputs: Object.fromEntries(entries) };
    });
  };

  const removePipelineInput = (key: string) => {
    setEditorForm(f => {
      const { [key]: _, ...rest } = f.inputs;
      return { ...f, inputs: rest };
    });
  };

  const canGoBack = currentDir && currentDir !== rootDir;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem', height: '100%' }}>
      {/* Toolbar */}
      <div className="page-header" style={{ gap: '0.5rem' }}>
        <span className="page-title">Pipelines</span>
        {canGoBack && (
          <button className="hud-button-ghost" onClick={handleBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
            <ChevronLeft size={13} /> Up
          </button>
        )}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: '0.35rem' }}>
          {currentDir && (
            <button className="hud-button" onClick={handleNewPipeline} style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
              <Plus size={14} /> New Pipeline
            </button>
          )}
          <button className="hud-button" onClick={handleOpenFolder} style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <FolderOpen size={14} /> Open Folder
          </button>
        </div>
      </div>

      {/* Current path */}
      {currentDir && (
        <div style={{ fontSize: '0.75rem', color: 'rgb(var(--muted))', fontFamily: 'monospace', wordBreak: 'break-all' }}>
          {currentDir}
        </div>
      )}

      {/* Dir error */}
      {dirError && (
        <div style={{ color: 'rgb(var(--danger))', fontSize: '0.8rem', padding: '0.5rem 0.75rem', background: 'rgba(var(--danger) / 0.1)', borderRadius: '4px', border: '1px solid rgba(var(--danger) / 0.3)' }}>
          {dirError}
        </div>
      )}

      {/* File list */}
      <div className="file-list">
        {!currentDir ? (
          <EmptyState message="No folder selected" sub="Click 'Open Folder' to browse pipeline YAML files" />
        ) : entries.length === 0 ? (
          <EmptyState message="Empty folder" sub="No YAML pipeline files found. Click 'New Pipeline' to create one." />
        ) : (
          entries.map(entry => {
            const isRunning = runningPaths.has(entry.path);
            return (
              <div
                key={entry.path}
                className="file-row"
                onClick={() => entry.isDir ? handleDrillDown(entry) : onOpenPipeline?.(entry.path)}
                style={{ cursor: 'pointer' }}
              >
                <span style={{ color: entry.isDir ? 'rgb(var(--warn))' : 'rgb(var(--accent))', flexShrink: 0 }}>
                  {entry.isDir ? <Folder size={15} /> : <FileCode size={15} />}
                </span>
                <span className="file-row-name" style={{ color: entry.isDir ? 'rgb(var(--text))' : undefined }}>
                  {entry.name}
                  {entry.isDir && <span style={{ color: 'rgb(var(--muted))', fontSize: '0.75rem', marginLeft: '0.3rem' }}>/</span>}
                </span>
                {!entry.isDir && (
                  <div className="file-row-actions" onClick={e => e.stopPropagation()} style={{ display: 'flex', gap: '0.25rem' }}>
                    <button
                      className="hud-button-ghost"
                      onClick={() => handleEditPipeline(entry)}
                      title="Edit pipeline"
                      style={{ padding: '0.15rem 0.3rem' }}
                    >
                      <Pencil size={12} />
                    </button>
                    <button
                      className="hud-button-ghost"
                      onClick={() => setDeleteTarget(entry)}
                      title="Delete pipeline"
                      style={{ padding: '0.15rem 0.3rem', color: 'rgb(var(--danger))' }}
                    >
                      <Trash2 size={12} />
                    </button>
                    <button
                      className="hud-button"
                      onClick={() => handleRun(entry)}
                      disabled={daemonStatus !== 'running' || isRunning}
                      style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: '0.7rem' }}
                    >
                      <Play size={12} /> {isRunning ? 'Running...' : 'Run'}
                    </button>
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>

      {/* Recent pipeline runs */}
      <div style={{ marginTop: '0.5rem' }}>
        <div style={{ fontSize: '0.68rem', letterSpacing: '0.1em', textTransform: 'uppercase', color: 'rgb(var(--muted))', marginBottom: '0.4rem' }}>
          Recent Pipeline Runs
        </div>
        <div className="hud-panel" style={{ overflow: 'hidden' }}>
          {pipelines.length === 0 ? (
            <EmptyState message="No pipeline runs yet" sub="Run a pipeline file above to see results here" />
          ) : (
            <table className="hud-table">
              <thead>
                <tr>
                  <th>Pipeline</th>
                  <th>Status</th>
                  <th>Started</th>
                  <th>Duration</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {pipelines.map(p => (
                  <tr key={p.id}>
                    <td style={{ fontFamily: 'monospace', fontSize: '0.8rem', color: 'rgb(var(--muted))' }}>
                      {shortPath(p.pipeline_path)}
                    </td>
                    <td>
                      <span style={{
                        fontSize: '0.72rem',
                        color: p.status === 'success' ? 'rgb(var(--ok))'
                          : p.status === 'failed' ? 'rgb(var(--danger))'
                          : p.status === 'running' ? 'rgb(var(--accent))'
                          : 'rgb(var(--muted))',
                        letterSpacing: '0.05em',
                      }}>
                        {p.status}
                      </span>
                    </td>
                    <td style={{ fontSize: '0.78rem', color: 'rgb(var(--muted))' }}>
                      {formatTime(p.started_at)}
                    </td>
                    <td style={{ fontSize: '0.78rem', color: 'rgb(var(--muted))' }}>
                      {formatDuration(p.started_at, p.ended_at)}
                    </td>
                    <td>
                      <button
                        className="hud-button-ghost"
                        onClick={() => handleShowStages(p)}
                        disabled={stagesLoading}
                        style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: '0.7rem' }}
                      >
                        <Layers size={12} /> Stages
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* Stage detail modal */}
      {stageModal && (
        <div className="hud-modal-overlay" onClick={() => setStageModal(null)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '600px', width: '100%' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }}>
              <div>
                <div style={{ fontSize: '0.68rem', letterSpacing: '0.1em', textTransform: 'uppercase', color: 'rgb(var(--muted))' }}>Pipeline Stages</div>
                <div style={{ fontSize: '0.75rem', fontFamily: 'monospace', color: 'rgb(var(--muted))', marginTop: '0.15rem' }}>{stageModal.pipelineId}</div>
              </div>
              <button className="hud-button-ghost" onClick={() => setStageModal(null)} style={{ padding: '0.25rem' }}>
                <X size={15} />
              </button>
            </div>

            {stageModal.stages.length === 0 ? (
              <EmptyState message="No stages" sub="This pipeline run has no recorded stages yet" />
            ) : (
              <table className="hud-table">
                <thead>
                  <tr>
                    <th>#</th>
                    <th>Stage</th>
                    <th>Status</th>
                    <th>Run ID</th>
                  </tr>
                </thead>
                <tbody>
                  {stageModal.stages.map(stage => (
                    <tr key={stage.id}>
                      <td style={{ color: 'rgb(var(--muted))', fontSize: '0.8rem' }}>{stage.stage_index + 1}</td>
                      <td style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{stage.stage_name}</td>
                      <td>
                        <span style={{
                          fontSize: '0.72rem',
                          color: stage.status === 'success' ? 'rgb(var(--ok))'
                            : stage.status === 'failed' ? 'rgb(var(--danger))'
                            : stage.status === 'running' ? 'rgb(var(--accent))'
                            : stage.status === 'skipped' ? 'rgb(var(--warn))'
                            : 'rgb(var(--muted))',
                        }}>
                          {stage.status}
                        </span>
                      </td>
                      <td>
                        {stage.run_id ? (
                          <button
                            className="hud-button-ghost"
                            onClick={() => { setStageModal(null); onOpenRun(stage.run_id); }}
                            style={{ fontFamily: 'monospace', fontSize: '0.72rem', padding: '0.1rem 0.3rem' }}
                          >
                            {stage.run_id.slice(-8)}
                          </button>
                        ) : (
                          <span style={{ color: 'rgb(var(--muted))', fontSize: '0.78rem' }}>—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button className="hud-button-ghost" onClick={() => setStageModal(null)}>Close</button>
            </div>
          </div>
        </div>
      )}

      {/* Pipeline editor modal (create / edit) */}
      {editorMode && (
        <div className="hud-modal-overlay" onClick={() => setEditorMode(null)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '600px', width: '100%' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1.25rem' }}>
              <Layers size={16} style={{ color: 'rgb(var(--accent))' }} />
              <span style={{ fontWeight: 600, fontSize: '0.9rem', letterSpacing: '0.05em' }}>
                {editorMode === 'create' ? 'New Pipeline' : 'Edit Pipeline'}
              </span>
            </div>

            {/* Pipeline name */}
            <div style={{ marginBottom: '0.75rem' }}>
              <label className="hud-label">Pipeline Name</label>
              <input
                className="hud-input"
                placeholder="e.g. deploy-staging"
                value={editorForm.name}
                onChange={e => setEditorForm(f => ({ ...f, name: e.target.value }))}
                style={{ width: '100%', boxSizing: 'border-box' }}
                autoFocus
              />
            </div>

            {/* Stop on fail */}
            <div style={{ marginBottom: '0.75rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <input
                type="checkbox"
                id="pl-stop-on-fail"
                checked={editorForm.stop_on_fail}
                onChange={e => setEditorForm(f => ({ ...f, stop_on_fail: e.target.checked }))}
                style={{ accentColor: 'rgb(var(--ok))' }}
              />
              <label htmlFor="pl-stop-on-fail" style={{ fontSize: '0.82rem', color: 'rgb(var(--text))', cursor: 'pointer' }}>
                Stop on first failure
              </label>
            </div>

            {/* Stages */}
            <div style={{ marginBottom: '0.75rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.4rem' }}>
                <label className="hud-label" style={{ margin: 0 }}>Stages</label>
                <button
                  type="button"
                  className="hud-button-ghost"
                  onClick={addStage}
                  style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', fontSize: '0.7rem', padding: '0.15rem 0.4rem' }}
                >
                  <Plus size={12} /> Add Stage
                </button>
              </div>

              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {editorForm.stages.map((stage, i) => (
                  <div
                    key={i}
                    style={{
                      padding: '0.5rem',
                      background: 'rgb(var(--panel2))',
                      borderRadius: '4px',
                      border: '1px solid rgb(var(--border))',
                    }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginBottom: '0.35rem' }}>
                      <span style={{ fontSize: '0.68rem', color: 'rgb(var(--muted))', fontWeight: 600, width: '1.5rem', textAlign: 'center' }}>
                        {i + 1}
                      </span>
                      <input
                        className="hud-input"
                        placeholder="Stage name"
                        value={stage.name}
                        onChange={e => updateStage(i, 'name', e.target.value)}
                        style={{ flex: 1, fontSize: '0.8rem', padding: '0.25rem 0.4rem' }}
                      />
                      <button
                        type="button"
                        className="hud-button-ghost"
                        onClick={() => moveStage(i, -1)}
                        disabled={i === 0}
                        style={{ padding: '0.1rem' }}
                        title="Move up"
                      >
                        <ArrowUp size={12} />
                      </button>
                      <button
                        type="button"
                        className="hud-button-ghost"
                        onClick={() => moveStage(i, 1)}
                        disabled={i === editorForm.stages.length - 1}
                        style={{ padding: '0.1rem' }}
                        title="Move down"
                      >
                        <ArrowDown size={12} />
                      </button>
                      {editorForm.stages.length > 1 && (
                        <button
                          type="button"
                          className="hud-button-ghost"
                          onClick={() => removeStage(i)}
                          style={{ padding: '0.1rem', color: 'rgb(var(--danger))' }}
                          title="Remove stage"
                        >
                          <X size={12} />
                        </button>
                      )}
                    </div>
                    <div style={{ display: 'flex', gap: '0.3rem', marginLeft: '1.9rem' }}>
                      <input
                        className="hud-input"
                        placeholder="Blueprint path"
                        value={stage.blueprint_path}
                        onChange={e => updateStage(i, 'blueprint_path', e.target.value)}
                        style={{ flex: 1, fontSize: '0.78rem', padding: '0.25rem 0.4rem', fontFamily: 'monospace' }}
                      />
                      <button
                        type="button"
                        className="hud-button-ghost"
                        onClick={() => browseStagePath(i)}
                        style={{ fontSize: '0.7rem', padding: '0.2rem 0.4rem', whiteSpace: 'nowrap' }}
                      >
                        <FolderOpen size={11} />
                      </button>
                    </div>
                    {/* Condition (if:) */}
                    <div style={{ display: 'flex', gap: '0.3rem', marginLeft: '1.9rem', marginTop: '0.25rem' }}>
                      <input
                        className="hud-input"
                        placeholder="if: condition (optional)"
                        value={stage.condition}
                        onChange={e => setEditorForm(f => ({
                          ...f,
                          stages: f.stages.map((s, si) => si === i ? { ...s, condition: e.target.value } : s),
                        }))}
                        style={{ flex: 1, fontSize: '0.72rem', padding: '0.2rem 0.35rem', fontFamily: 'monospace', color: stage.condition ? 'rgb(var(--warn))' : undefined }}
                      />
                    </div>
                    {/* Stage inputs */}
                    <div style={{ marginLeft: '1.9rem', marginTop: '0.3rem' }}>
                      {Object.keys(stage.inputs).length > 0 && (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.2rem', marginBottom: '0.2rem' }}>
                          {Object.entries(stage.inputs).map(([key, val], ki) => (
                            <div key={ki} style={{ display: 'flex', gap: '0.25rem', alignItems: 'center' }}>
                              <input
                                className="hud-input"
                                placeholder="key"
                                value={key}
                                onChange={e => updateStageInput(i, key, e.target.value, val)}
                                style={{ width: '35%', fontSize: '0.72rem', padding: '0.2rem 0.35rem', fontFamily: 'monospace' }}
                              />
                              <span style={{ color: 'rgb(var(--muted))', fontSize: '0.7rem' }}>=</span>
                              <input
                                className="hud-input"
                                placeholder="value"
                                value={val}
                                onChange={e => updateStageInput(i, key, key, e.target.value)}
                                style={{ flex: 1, fontSize: '0.72rem', padding: '0.2rem 0.35rem', fontFamily: 'monospace' }}
                              />
                              <button
                                type="button"
                                className="hud-button-ghost"
                                onClick={() => removeStageInput(i, key)}
                                style={{ padding: '0.1rem', color: 'rgb(var(--danger))' }}
                              >
                                <X size={10} />
                              </button>
                            </div>
                          ))}
                        </div>
                      )}
                      <button
                        type="button"
                        className="hud-button-ghost"
                        onClick={() => addStageInput(i)}
                        style={{ fontSize: '0.65rem', padding: '0.1rem 0.3rem', color: 'rgb(var(--muted))' }}
                      >
                        <Plus size={10} /> Input
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Top-level pipeline inputs */}
            <div style={{ marginBottom: '0.75rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.4rem' }}>
                <label className="hud-label" style={{ margin: 0 }}>Pipeline Inputs</label>
                <button
                  type="button"
                  className="hud-button-ghost"
                  onClick={addPipelineInput}
                  style={{ display: 'flex', alignItems: 'center', gap: '0.25rem', fontSize: '0.7rem', padding: '0.15rem 0.4rem' }}
                >
                  <Plus size={12} /> Add Input
                </button>
              </div>
              {Object.keys(editorForm.inputs).length > 0 ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                  {Object.entries(editorForm.inputs).map(([key, val], ki) => (
                    <div key={ki} style={{ display: 'flex', gap: '0.25rem', alignItems: 'center' }}>
                      <input
                        className="hud-input"
                        placeholder="key"
                        value={key}
                        onChange={e => updatePipelineInput(key, e.target.value, val)}
                        style={{ width: '35%', fontSize: '0.78rem', padding: '0.25rem 0.4rem', fontFamily: 'monospace' }}
                      />
                      <span style={{ color: 'rgb(var(--muted))', fontSize: '0.75rem' }}>=</span>
                      <input
                        className="hud-input"
                        placeholder="value / default"
                        value={val}
                        onChange={e => updatePipelineInput(key, key, e.target.value)}
                        style={{ flex: 1, fontSize: '0.78rem', padding: '0.25rem 0.4rem', fontFamily: 'monospace' }}
                      />
                      <button
                        type="button"
                        className="hud-button-ghost"
                        onClick={() => removePipelineInput(key)}
                        style={{ padding: '0.15rem', color: 'rgb(var(--danger))' }}
                      >
                        <X size={12} />
                      </button>
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ fontSize: '0.72rem', color: 'rgb(var(--muted))', fontStyle: 'italic' }}>
                  No pipeline-level inputs defined
                </div>
              )}
            </div>

            {/* Error */}
            {editorError && (
              <div style={{
                color: 'rgb(var(--danger))',
                fontSize: '0.78rem',
                marginBottom: '0.75rem',
                padding: '0.4rem 0.6rem',
                background: 'rgba(var(--danger) / 0.1)',
                borderRadius: '4px',
                border: '1px solid rgba(var(--danger) / 0.3)',
              }}>
                {editorError}
              </div>
            )}

            {/* Actions */}
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.5rem' }}>
              <button className="hud-button-ghost" onClick={() => setEditorMode(null)}>
                Cancel
              </button>
              <button
                className="hud-button"
                onClick={handleEditorSave}
                disabled={editorSaving}
                style={{ borderColor: 'rgba(var(--ok) / 0.5)', color: 'rgb(var(--ok))' }}
              >
                {editorSaving ? 'Saving...' : editorMode === 'create' ? 'Create' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete confirmation modal */}
      {deleteTarget && (
        <div className="hud-modal-overlay" onClick={() => setDeleteTarget(null)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '400px' }}>
            <div style={{ padding: '1.25rem' }}>
              <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Delete Pipeline</div>
              <div style={{ fontSize: '0.8rem', color: 'rgb(var(--muted))', marginBottom: '1rem' }}>
                Delete <span style={{ fontFamily: 'monospace', color: 'rgb(var(--accent))' }}>{deleteTarget.name}</span>? This cannot be undone.
              </div>
              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                <button className="hud-button-ghost" onClick={() => setDeleteTarget(null)}>Cancel</button>
                <button
                  className="hud-button"
                  style={{ borderColor: 'rgb(var(--danger))', color: 'rgb(var(--danger))' }}
                  onClick={handleDeletePipeline}
                >
                  Delete
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
