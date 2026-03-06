import { useState, useEffect } from 'react';
import { toast } from 'sonner';
import { ChevronLeft, Play, Copy, X, MoreHorizontal, Trash2, Pencil, Layers, ChevronDown, ChevronRight } from 'lucide-react';
import { readBlueprintFile, enqueuePipeline, deleteBlueprintFile } from '../api/client';
import { Spinner } from '../components/ui/Spinner';

interface PipelineDetailPageProps {
  path: string;
  onBack: () => void;
  onOpenRun: (runId: string) => void;
  onEditPipeline?: (path: string) => void;
  daemonStatus: string;
  workspaceId: string;
}

// Minimal pipeline types for the detail view
interface PipelineStageDetail {
  name: string;
  blueprint_path: string;
  inputs: Record<string, string>;
  condition: string;
}

interface PipelineDetail {
  name: string;
  stop_on_fail: boolean;
  stages: PipelineStageDetail[];
  inputs: Record<string, string>;
}

function basename(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts[parts.length - 1] || p;
}

function unquote(v: string): string {
  return v.trim().replace(/^["']/, '').replace(/["']$/, '');
}

function parsePipelineYaml(content: string): PipelineDetail | null {
  try {
    const detail: PipelineDetail = { name: '', stop_on_fail: true, stages: [], inputs: {} };
    const lines = content.split('\n');

    type Section = 'none' | 'meta' | 'stages' | 'inputs';
    let section: Section = 'none';
    let currentStage: PipelineStageDetail | null = null;
    let inStageInputs = false;

    for (const line of lines) {
      if (line.trim() === '' || line.trim().startsWith('#')) continue;
      const indent = line.search(/\S/);

      if (indent === 0) {
        const trimmed = line.trim();
        if (trimmed === 'meta:') {
          if (currentStage) { detail.stages.push(currentStage); currentStage = null; }
          inStageInputs = false; section = 'meta'; continue;
        }
        if (trimmed.startsWith('stop_on_fail:')) {
          if (currentStage) { detail.stages.push(currentStage); currentStage = null; }
          inStageInputs = false; section = 'none';
          detail.stop_on_fail = trimmed.includes('true'); continue;
        }
        if (trimmed === 'stages:') {
          if (currentStage) { detail.stages.push(currentStage); currentStage = null; }
          inStageInputs = false; section = 'stages'; continue;
        }
        if (trimmed === 'inputs:') {
          if (currentStage) { detail.stages.push(currentStage); currentStage = null; }
          inStageInputs = false; section = 'inputs'; continue;
        }
        section = 'none'; continue;
      }

      if (section === 'meta' && indent >= 2) {
        const trimmed = line.trim();
        if (trimmed.startsWith('name:')) detail.name = unquote(trimmed.slice(5));
        continue;
      }

      if (section === 'stages') {
        const trimmed = line.trim();
        if (trimmed.startsWith('- ')) {
          if (currentStage) detail.stages.push(currentStage);
          inStageInputs = false;
          currentStage = { name: '', blueprint_path: '', inputs: {}, condition: '' };
          const after = trimmed.slice(2).trim();
          if (after.startsWith('name:')) currentStage.name = unquote(after.slice(5));
          continue;
        }
        if (currentStage && indent >= 4) {
          if (inStageInputs && indent >= 6) {
            const colonIdx = trimmed.indexOf(':');
            if (colonIdx > 0) {
              currentStage.inputs[trimmed.slice(0, colonIdx).trim()] = unquote(trimmed.slice(colonIdx + 1));
            }
            continue;
          }
          inStageInputs = false;
          if (trimmed.startsWith('blueprint_path:')) currentStage.blueprint_path = unquote(trimmed.slice(15));
          else if (trimmed.startsWith('name:')) currentStage.name = unquote(trimmed.slice(5));
          else if (trimmed.startsWith('if:')) currentStage.condition = unquote(trimmed.slice(3));
          else if (trimmed === 'inputs:') inStageInputs = true;
          continue;
        }
        continue;
      }

      if (section === 'inputs' && indent >= 2) {
        const trimmed = line.trim();
        const colonIdx = trimmed.indexOf(':');
        if (colonIdx > 0) {
          detail.inputs[trimmed.slice(0, colonIdx).trim()] = unquote(trimmed.slice(colonIdx + 1));
        }
        continue;
      }
    }

    if (currentStage) detail.stages.push(currentStage);
    return detail;
  } catch {
    return null;
  }
}

export function PipelineDetailPage({ path, onBack, onOpenRun, onEditPipeline, daemonStatus, workspaceId }: PipelineDetailPageProps) {
  const [pipeline, setPipeline] = useState<PipelineDetail | null>(null);
  const [rawYaml, setRawYaml] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showYamlModal, setShowYamlModal] = useState(false);
  const [showActionMenu, setShowActionMenu] = useState(false);
  const [deleteConfirm, setDeleteConfirm] = useState(false);
  const [expandedStages, setExpandedStages] = useState<Set<number>>(new Set());
  const [running, setRunning] = useState(false);

  useEffect(() => {
    setLoading(true);
    setError(null);
    readBlueprintFile(path)
      .then(content => {
        setRawYaml(content);
        const parsed = parsePipelineYaml(content);
        if (!parsed) throw new Error('Failed to parse pipeline YAML');
        setPipeline(parsed);
        setExpandedStages(new Set(parsed.stages.map((_, i) => i)));
      })
      .catch(err => {
        setError(err.message || 'Failed to load pipeline');
        toast.error('Failed to load pipeline');
      })
      .finally(() => setLoading(false));
  }, [path]);

  const handleRun = async () => {
    if (daemonStatus !== 'running') return;
    setRunning(true);
    try {
      await enqueuePipeline({ pipeline_path: path, workspace_id: workspaceId });
      toast.success('Pipeline started');
    } catch (err) {
      toast.error(`Failed to run pipeline: ${err}`);
    } finally {
      setRunning(false);
    }
  };

  const handleDelete = async () => {
    try {
      await deleteBlueprintFile(path);
      toast.success('Pipeline deleted');
      onBack();
    } catch (err) {
      toast.error(`Delete failed: ${err}`);
    }
  };

  const handleCopyYaml = () => {
    navigator.clipboard.writeText(rawYaml).then(() => toast.success('Copied to clipboard'));
  };

  const toggleStage = (idx: number) => {
    setExpandedStages(prev => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx); else next.add(idx);
      return next;
    });
  };

  if (loading) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
        <div className="page-header">
          <button className="hud-button-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
            <ChevronLeft size={13} /> Back
          </button>
          <span className="page-title">Loading...</span>
          <Spinner size={14} />
        </div>
      </div>
    );
  }

  if (error || !pipeline) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
        <div className="page-header">
          <button className="hud-button-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
            <ChevronLeft size={13} /> Back
          </button>
          <span className="page-title">Error</span>
        </div>
        <div style={{ color: 'rgb(var(--danger))', fontSize: '0.8rem', padding: '0.75rem', background: 'rgba(var(--danger) / 0.1)', borderRadius: '4px', border: '1px solid rgba(var(--danger) / 0.3)' }}>
          {error || 'Failed to load pipeline'}
        </div>
      </div>
    );
  }

  const title = pipeline.name || basename(path);
  const topInputKeys = Object.keys(pipeline.inputs);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
      {/* Header */}
      <div className="page-header" style={{ gap: '0.5rem' }}>
        <button className="hud-button-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <ChevronLeft size={13} /> Back
        </button>
        <Layers size={15} style={{ color: 'rgb(var(--accent))', flexShrink: 0 }} />
        <span className="page-title" style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {title}
        </span>
        <button
          className="hud-button"
          onClick={handleRun}
          disabled={daemonStatus !== 'running' || running}
          style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', borderColor: 'rgba(var(--ok) / 0.5)', color: 'rgb(var(--ok))' }}
        >
          <Play size={12} /> {running ? 'Running...' : 'Run'}
        </button>
        <div style={{ position: 'relative' }}>
          <button className="hud-button-ghost" onClick={() => setShowActionMenu(!showActionMenu)} style={{ padding: '0.3rem 0.5rem' }}>
            <MoreHorizontal size={15} />
          </button>
          {showActionMenu && (
            <div
              style={{
                position: 'absolute', right: 0, top: '100%', marginTop: '0.25rem',
                background: 'rgb(var(--panel))', border: '1px solid rgb(var(--border))',
                borderRadius: '4px', minWidth: '160px', zIndex: 50, boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
              }}
              onClick={() => setShowActionMenu(false)}
            >
              <button
                onClick={() => setShowYamlModal(true)}
                style={{
                  display: 'flex', alignItems: 'center', gap: '0.5rem', width: '100%',
                  padding: '0.5rem 0.75rem', background: 'none', border: 'none',
                  color: 'rgb(var(--text))', cursor: 'pointer', fontFamily: 'var(--font-mono)',
                  fontSize: '0.8rem', textAlign: 'left',
                }}
              >
                <Copy size={13} /> View YAML
              </button>
              {onEditPipeline && (
                <button
                  onClick={() => onEditPipeline(path)}
                  style={{
                    display: 'flex', alignItems: 'center', gap: '0.5rem', width: '100%',
                    padding: '0.5rem 0.75rem', background: 'none', border: 'none',
                    color: 'rgb(var(--text))', cursor: 'pointer', fontFamily: 'var(--font-mono)',
                    fontSize: '0.8rem', textAlign: 'left',
                  }}
                >
                  <Pencil size={13} /> Edit
                </button>
              )}
              <button
                onClick={() => setDeleteConfirm(true)}
                style={{
                  display: 'flex', alignItems: 'center', gap: '0.5rem', width: '100%',
                  padding: '0.5rem 0.75rem', background: 'none', border: 'none',
                  color: 'rgb(var(--danger))', cursor: 'pointer', fontFamily: 'var(--font-mono)',
                  fontSize: '0.8rem', textAlign: 'left',
                }}
              >
                <Trash2 size={13} /> Delete
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Main content — split layout like BlueprintDetailPage */}
      <div className="bp-detail-split">
        {/* Left — Metadata */}
        <div className="bp-detail-meta">
          <div className="bp-meta-section">
            <div className="bp-meta-section-title">Pipeline</div>
            <div className="bp-meta-field">
              <span className="bp-meta-field-label">Name</span>
              <span style={{ fontFamily: 'monospace' }}>{pipeline.name || '—'}</span>
            </div>
            <div className="bp-meta-field">
              <span className="bp-meta-field-label">Stages</span>
              <span>{pipeline.stages.length}</span>
            </div>
            <div className="bp-meta-field">
              <span className="bp-meta-field-label">Stop on Fail</span>
              <span className={`bp-badge ${pipeline.stop_on_fail ? 'bp-badge-warn' : 'bp-badge-info'}`}>
                {pipeline.stop_on_fail ? 'yes' : 'no'}
              </span>
            </div>
            <div className="bp-meta-field" style={{ flexDirection: 'column' }}>
              <span className="bp-meta-field-label">Path</span>
              <span style={{ fontSize: '0.72rem', fontFamily: 'monospace', color: 'rgb(var(--muted))', wordBreak: 'break-all' }}>{path}</span>
            </div>
          </div>

          {/* Pipeline-level inputs */}
          <div className="bp-meta-section">
            <div className="bp-meta-section-title">Pipeline Inputs ({topInputKeys.length})</div>
            {topInputKeys.length === 0 ? (
              <div style={{ fontSize: '0.78rem', color: 'rgb(var(--muted))' }}>No pipeline-level inputs</div>
            ) : (
              topInputKeys.map((key, i) => (
                <div key={i} style={{ padding: '0.3rem 0', borderBottom: i < topInputKeys.length - 1 ? '1px solid rgba(var(--border), 0.5)' : 'none' }}>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.4rem' }}>
                    <span style={{ color: 'rgb(var(--accent))', fontFamily: 'monospace', fontSize: '0.8rem' }}>{key}</span>
                    {pipeline.inputs[key] && (
                      <span style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))' }}>
                        = <span style={{ fontFamily: 'monospace' }}>{pipeline.inputs[key]}</span>
                      </span>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Right — Stage Timeline */}
        <div className="bp-detail-steps">
          {pipeline.stages.map((stage, si) => {
            const isExpanded = expandedStages.has(si);
            const inputKeys = Object.keys(stage.inputs);
            return (
              <div key={si} style={{ marginBottom: '0.5rem' }}>
                {/* Stage header */}
                <div className="bp-section-header" onClick={() => toggleStage(si)}>
                  {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                  <span style={{ fontFamily: 'monospace', color: 'rgb(var(--accent))', fontSize: '0.82rem' }}>
                    {stage.name || `Stage ${si + 1}`}
                  </span>
                  <span style={{ fontSize: '0.68rem', color: 'rgb(var(--muted))', marginLeft: '0.5rem' }}>
                    #{si + 1}
                  </span>
                  {stage.condition && (
                    <span className="bp-badge bp-badge-warn" style={{ marginLeft: '0.3rem' }}>
                      conditional
                    </span>
                  )}
                  {inputKeys.length > 0 && (
                    <span className="bp-badge bp-badge-info" style={{ marginLeft: '0.3rem' }}>
                      {inputKeys.length} input{inputKeys.length !== 1 ? 's' : ''}
                    </span>
                  )}
                </div>

                {/* Stage detail */}
                {isExpanded && (
                  <div className="bp-task-expanded">
                    <div className="hud-label">Blueprint</div>
                    <pre style={{ fontSize: '0.78rem' }}>{stage.blueprint_path}</pre>

                    {stage.condition && (
                      <div style={{ fontSize: '0.75rem', marginTop: '0.25rem' }}>
                        <span className="hud-label">Condition: </span>
                        <span style={{ fontFamily: 'monospace', color: 'rgb(var(--warn))' }}>{stage.condition}</span>
                      </div>
                    )}

                    {inputKeys.length > 0 && (
                      <>
                        <div className="hud-label" style={{ marginTop: '0.5rem' }}>Inputs</div>
                        <pre style={{ fontSize: '0.78rem' }}>
                          {inputKeys.map(k => `${k}: ${stage.inputs[k]}`).join('\n')}
                        </pre>
                      </>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Footer */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '0.5rem 0', borderTop: '1px solid rgb(var(--border))',
        fontSize: '0.75rem', color: 'rgb(var(--muted))', flexShrink: 0,
      }}>
        <span>{pipeline.stages.length} stages · {topInputKeys.length} inputs · stop_on_fail: {pipeline.stop_on_fail ? 'yes' : 'no'}</span>
        <button className="hud-button-ghost" onClick={() => setShowYamlModal(true)} style={{ fontSize: '0.72rem' }}>
          View YAML
        </button>
      </div>

      {/* YAML Modal */}
      {showYamlModal && (
        <div className="hud-modal-overlay" onClick={() => setShowYamlModal(false)}>
          <div
            className="hud-modal"
            onClick={e => e.stopPropagation()}
            style={{ maxWidth: '900px', width: '90vw', maxHeight: '85vh', display: 'flex', flexDirection: 'column' }}
          >
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
              <span style={{ fontSize: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.1em', color: 'rgb(var(--muted))' }}>
                Pipeline YAML
              </span>
              <div style={{ display: 'flex', gap: '0.4rem' }}>
                <button className="hud-button-ghost" onClick={handleCopyYaml} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: '0.72rem' }}>
                  <Copy size={12} /> Copy
                </button>
                <button className="hud-button-ghost" onClick={() => setShowYamlModal(false)} style={{ padding: '0.25rem' }}>
                  <X size={15} />
                </button>
              </div>
            </div>
            <pre style={{
              flex: 1, overflow: 'auto', padding: '0.75rem',
              background: 'rgb(var(--bg))', borderRadius: '4px',
              border: '1px solid rgb(var(--border))',
              fontSize: '0.78rem', lineHeight: '1.6', whiteSpace: 'pre-wrap',
              wordBreak: 'break-all',
            }}>
              {rawYaml}
            </pre>
          </div>
        </div>
      )}

      {/* Delete confirmation */}
      {deleteConfirm && (
        <div className="hud-modal-overlay" onClick={() => setDeleteConfirm(false)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()}>
            <h3 style={{ fontSize: '0.9rem', marginBottom: '0.5rem' }}>Delete Pipeline</h3>
            <p style={{ fontSize: '0.8rem', color: 'rgb(var(--muted))', marginBottom: '0.5rem' }}>
              Are you sure you want to delete this pipeline? This cannot be undone.
            </p>
            <p style={{ fontFamily: 'monospace', fontSize: '0.75rem', color: 'rgb(var(--muted))', wordBreak: 'break-all' }}>
              {path}
            </p>
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button className="hud-button-ghost" onClick={() => setDeleteConfirm(false)}>Cancel</button>
              <button
                className="hud-button"
                style={{ borderColor: 'rgb(var(--danger))', color: 'rgb(var(--danger))' }}
                onClick={handleDelete}
              >
                Delete
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
