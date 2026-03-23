import { useState, useEffect } from 'react';
import { toast } from 'sonner';
import { ChevronLeft, Play, ChevronDown, ChevronRight, Copy, X, MoreHorizontal, Trash2, Pencil } from 'lucide-react';
import { parseBlueprintFull, readBlueprintFile, enqueueRun, parseBlueprintInputs, deleteBlueprintFile, getSettings } from '../api/client';
import { Spinner } from '../components/ui/Spinner';
import { RunInputsModal } from '../components/ui/RunInputsModal';
import type { ParsedBlueprint, BlueprintInput, FileEntry } from '../api/types';

interface BlueprintDetailPageProps {
  path: string;
  onBack: () => void;
  onOpenRun: (runId: string) => void;
  onEditBlueprint?: (path: string) => void;
  daemonStatus: string;
  workspaceId: string;
}

function basename(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts[parts.length - 1] || p;
}

export function BlueprintDetailPage({ path, onBack, onOpenRun, onEditBlueprint, daemonStatus, workspaceId }: BlueprintDetailPageProps) {
  const [blueprint, setBlueprint] = useState<ParsedBlueprint | null>(null);
  const [rawYaml, setRawYaml] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showYamlModal, setShowYamlModal] = useState(false);
  const [showRunModal, setShowRunModal] = useState(false);
  const [runInputs, setRunInputs] = useState<BlueprintInput[]>([]);
  const [showActionMenu, setShowActionMenu] = useState(false);
  const [expandedSections, setExpandedSections] = useState<Set<number>>(new Set());
  const [expandedTasks, setExpandedTasks] = useState<Set<string>>(new Set());
  const [deleteConfirm, setDeleteConfirm] = useState(false);
  const [showConfirmModal, setShowConfirmModal] = useState(false);

  useEffect(() => {
    setLoading(true);
    setError(null);
    Promise.all([
      parseBlueprintFull(path),
      readBlueprintFile(path),
    ]).then(([bp, yaml]) => {
      setBlueprint(bp);
      setRawYaml(yaml);
      setExpandedSections(new Set(bp.steps.map((_, i) => i)));
    }).catch(err => {
      setError(err.message || 'Failed to load blueprint');
      toast.error('Failed to load blueprint');
    }).finally(() => setLoading(false));
  }, [path]);

  const handleRun = async () => {
    try {
      const inputs = await parseBlueprintInputs(path);
      if (inputs && inputs.length > 0) {
        setRunInputs(inputs);
        setShowRunModal(true);
      } else {
        try {
          const settings = await getSettings();
          if (settings.safety.requireConfirmation) {
            setShowConfirmModal(true);
            return;
          }
        } catch { /* proceed without confirmation */ }
        const run = await enqueueRun({ blueprint_path: path, workspace_id: workspaceId });
        toast.success('Run enqueued');
        onOpenRun(run.id);
      }
    } catch (err: unknown) {
      toast.error(`Failed to start run: ${err}`);
    }
  };

  const handleConfirmRun = async () => {
    setShowConfirmModal(false);
    try {
      const run = await enqueueRun({ blueprint_path: path, workspace_id: workspaceId });
      toast.success('Run enqueued');
      onOpenRun(run.id);
    } catch (err: unknown) {
      toast.error(`Failed to start run: ${err}`);
    }
  };

  const handleRunConfirm = async (values: Record<string, unknown>, dryRun: boolean) => {
    setShowRunModal(false);
    try {
      const run = await enqueueRun({ blueprint_path: path, workspace_id: workspaceId, inputs: values, dry_run: dryRun || undefined });
      toast.success(dryRun ? 'Dry run enqueued' : 'Run enqueued');
      onOpenRun(run.id);
    } catch (err: unknown) {
      toast.error(`Failed to start run: ${err}`);
    }
  };

  const handleDelete = async () => {
    try {
      await deleteBlueprintFile(path);
      toast.success('Blueprint deleted');
      onBack();
    } catch (err: unknown) {
      toast.error(`Delete failed: ${err}`);
    }
  };

  const handleCopyYaml = () => {
    navigator.clipboard.writeText(rawYaml).then(() => {
      toast.success('Copied to clipboard');
    });
  };

  const toggleSection = (idx: number) => {
    setExpandedSections(prev => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx); else next.add(idx);
      return next;
    });
  };

  const toggleTask = (key: string) => {
    setExpandedTasks(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  };

  if (loading) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
        <div className="page-header">
          <button className="btn btn-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
            <ChevronLeft size={13} /> Back
          </button>
          <span className="page-title">Loading...</span>
          <Spinner size={14} />
        </div>
      </div>
    );
  }

  if (error || !blueprint) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
        <div className="page-header">
          <button className="btn btn-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
            <ChevronLeft size={13} /> Back
          </button>
          <span className="page-title">Error</span>
        </div>
        <div style={{ color: 'var(--status-failed)', fontSize: 'var(--text-md)', padding: '0.75rem', background: 'var(--status-failed-bg)', borderRadius: '4px', border: '1px solid rgba(239, 68, 68, 0.3)' }}>
          {error || 'Failed to load blueprint'}
        </div>
      </div>
    );
  }

  const bp = blueprint;
  const title = bp.blueprint.title || bp.blueprint.name || basename(path);
  const totalTasks = bp.steps.reduce((sum, s) => sum + s.tasks.length, 0);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
      {/* Header */}
      <div className="page-header" style={{ gap: '0.5rem' }}>
        <button className="btn btn-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <ChevronLeft size={13} /> Back
        </button>
        <span className="page-title" style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {title}
        </span>
        <button
          className="btn btn-primary"
          onClick={handleRun}
          disabled={daemonStatus !== 'running'}
          style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', borderColor: 'rgba(59, 130, 246, 0.5)', color: 'var(--status-success)' }}
        >
          <Play size={12} /> Run
        </button>
        <div style={{ position: 'relative' }}>
          <button className="btn btn-ghost" onClick={() => setShowActionMenu(!showActionMenu)} style={{ padding: '0.3rem 0.5rem' }}>
            <MoreHorizontal size={15} />
          </button>
          {showActionMenu && (
            <div
              style={{
                position: 'absolute', right: 0, top: '100%', marginTop: '0.25rem',
                background: 'var(--bg-surface)', border: '1px solid var(--border-default)',
                borderRadius: '4px', minWidth: '160px', zIndex: 50, boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
              }}
              onClick={() => setShowActionMenu(false)}
            >
              <button
                onClick={() => setShowYamlModal(true)}
                style={{
                  display: 'flex', alignItems: 'center', gap: '0.5rem', width: '100%',
                  padding: '0.5rem 0.75rem', background: 'none', border: 'none',
                  color: 'var(--text-primary)', cursor: 'pointer', fontFamily: 'var(--font-mono)',
                  fontSize: 'var(--text-md)', textAlign: 'left',
                }}
              >
                <Copy size={13} /> View YAML
              </button>
              {onEditBlueprint && (
                <button
                  onClick={() => onEditBlueprint(path)}
                  style={{
                    display: 'flex', alignItems: 'center', gap: '0.5rem', width: '100%',
                    padding: '0.5rem 0.75rem', background: 'none', border: 'none',
                    color: 'var(--text-primary)', cursor: 'pointer', fontFamily: 'var(--font-mono)',
                    fontSize: 'var(--text-md)', textAlign: 'left',
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
                  color: 'var(--status-failed)', cursor: 'pointer', fontFamily: 'var(--font-mono)',
                  fontSize: 'var(--text-md)', textAlign: 'left',
                }}
              >
                <Trash2 size={13} /> Delete
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Main content */}
      <div className="bp-detail-split">
        {/* Left — Metadata */}
        <div className="bp-detail-meta">
          {/* Blueprint identity */}
          <div className="bp-meta-section">
            <div className="bp-meta-section-title">Metadata</div>
            {bp.blueprint.name && (
              <div className="bp-meta-field"><span className="bp-meta-field-label">Name</span><span>{bp.blueprint.name}</span></div>
            )}
            {bp.blueprint.slug && (
              <div className="bp-meta-field"><span className="bp-meta-field-label">Slug</span><span style={{ fontFamily: 'monospace' }}>{bp.blueprint.slug}</span></div>
            )}
            {bp.version && (
              <div className="bp-meta-field"><span className="bp-meta-field-label">Version</span><span className="bp-badge bp-badge-info">v{bp.version}</span></div>
            )}
            {bp.blueprint.description && (
              <div className="bp-meta-field" style={{ flexDirection: 'column' }}>
                <span className="bp-meta-field-label">Description</span>
                <span style={{ color: 'var(--text-primary)', fontSize: 'var(--text-sm)', lineHeight: '1.4' }}>{bp.blueprint.description}</span>
              </div>
            )}
            {bp.blueprint.author && (
              <div className="bp-meta-field"><span className="bp-meta-field-label">Author</span><span>{bp.blueprint.author}</span></div>
            )}
            {bp.blueprint.license && (
              <div className="bp-meta-field"><span className="bp-meta-field-label">License</span><span>{bp.blueprint.license}</span></div>
            )}
            {bp.blueprint.homepage && (
              <div className="bp-meta-field"><span className="bp-meta-field-label">Homepage</span><span style={{ wordBreak: 'break-all', fontSize: 'var(--text-sm)' }}>{bp.blueprint.homepage}</span></div>
            )}
            {bp.blueprint.tags && bp.blueprint.tags.length > 0 && (
              <div style={{ marginTop: '0.35rem' }}>
                {bp.blueprint.tags.map((tag, i) => (
                  <span key={i} className="bp-tag">{tag}</span>
                ))}
              </div>
            )}
          </div>

          {/* Inputs */}
          <div className="bp-meta-section">
            <div className="bp-meta-section-title">Inputs ({bp.inputs?.length ?? 0})</div>
            {(!bp.inputs || bp.inputs.length === 0) ? (
              <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>No inputs defined</div>
            ) : (
              bp.inputs.map((inp, i) => (
                <div key={i} style={{ padding: '0.35rem 0', borderBottom: i < bp.inputs.length - 1 ? '1px solid rgba(var(--border), 0.5)' : 'none' }}>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.4rem' }}>
                    <span style={{ color: 'var(--accent)', fontFamily: 'monospace', fontSize: 'var(--text-md)' }}>{inp.name}</span>
                    <span className="bp-badge bp-badge-info">{inp.type}</span>
                    {inp.required && <span style={{ color: 'var(--status-failed)', fontSize: 'var(--text-xs)' }}>*</span>}
                  </div>
                  {inp.description && (
                    <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginTop: '0.15rem' }}>{inp.description}</div>
                  )}
                  {inp.default != null && inp.default !== '' && (
                    <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: '0.1rem' }}>Default: <span style={{ fontFamily: 'monospace' }}>{String(inp.default)}</span></div>
                  )}
                  {inp.enum && inp.enum.length > 0 && (
                    <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: '0.1rem' }}>Enum: {inp.enum.join(', ')}</div>
                  )}
                </div>
              ))
            )}
          </div>

          {/* Imports */}
          {bp.imports && bp.imports.length > 0 && (
            <div className="bp-meta-section">
              <div className="bp-meta-section-title">Imports ({bp.imports.length})</div>
              {bp.imports.map((imp, i) => (
                <div key={i} style={{
                  padding: '0.45rem 0.6rem', marginBottom: '0.35rem',
                  background: 'var(--bg-raised)', borderRadius: '4px',
                  border: '1px solid var(--border-default)',
                }}>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.5rem' }}>
                    <span style={{ color: 'var(--accent)', fontFamily: 'monospace', fontSize: 'var(--text-md)', fontWeight: 700 }}>
                      {imp.alias || '(unnamed)'}
                    </span>
                    <span className="bp-badge bp-badge-info">import</span>
                  </div>
                  <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', fontFamily: 'monospace', marginTop: '0.15rem', wordBreak: 'break-all' }}>
                    {imp.path}
                  </div>
                  {imp.with && Object.keys(imp.with).length > 0 && (
                    <div style={{ marginTop: '0.3rem', paddingTop: '0.25rem', borderTop: '1px solid rgba(var(--border), 0.5)' }}>
                      <span style={{ fontSize: 'var(--text-xs)', textTransform: 'uppercase', letterSpacing: '0.1em', color: 'var(--text-tertiary)' }}>with</span>
                      <div style={{ marginTop: '0.15rem' }}>
                        {Object.entries(imp.with).map(([k, v]) => (
                          <div key={k} style={{ fontSize: 'var(--text-sm)', fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                            <span style={{ color: 'var(--text-tertiary)' }}>{k}:</span> {String(v)}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* Hooks */}
          {bp.hooks && (bp.hooks.before_run?.length > 0 || bp.hooks.after_run?.length > 0 || bp.hooks.on_error?.length > 0) && (
            <div className="bp-meta-section">
              <div className="bp-meta-section-title">Hooks</div>
              {bp.hooks.before_run?.length > 0 && (
                <div className="bp-meta-field"><span className="bp-meta-field-label">before_run</span><span>{bp.hooks.before_run.length}</span></div>
              )}
              {bp.hooks.after_run?.length > 0 && (
                <div className="bp-meta-field"><span className="bp-meta-field-label">after_run</span><span>{bp.hooks.after_run.length}</span></div>
              )}
              {bp.hooks.on_error?.length > 0 && (
                <div className="bp-meta-field"><span className="bp-meta-field-label">on_error</span><span>{bp.hooks.on_error.length}</span></div>
              )}
            </div>
          )}
        </div>

        {/* Right — Step Timeline */}
        <div className="bp-detail-steps">
          {bp.steps.map((section, si) => (
            <div key={si} style={{ marginBottom: '0.75rem' }}>
              {/* Section header */}
              <div className="bp-section-header" onClick={() => toggleSection(si)}>
                {expandedSections.has(si) ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                <span style={{ color: 'var(--status-success)' }}>
                  {section.section || `Section ${si + 1}`}
                </span>
                <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginLeft: 'auto' }}>
                  {section.tasks.length} task{section.tasks.length !== 1 ? 's' : ''}
                </span>
              </div>

              {/* Tasks */}
              {expandedSections.has(si) && section.tasks.map((task, ti) => {
                const taskKey = `${si}-${ti}`;
                const isExpanded = expandedTasks.has(taskKey);
                const hasCondition = !!task.if;
                const isDisabled = task.enabled === false;
                const isCall = !!task.call;
                const cmdPreview = (task.cmd || task.run || (task.call ? `call: ${task.call}` : '')).split('\n')[0].slice(0, 80);

                return (
                  <div key={ti}>
                    <div className="bp-task-row" onClick={() => toggleTask(taskKey)}>
                      <div className={`bp-task-dot ${isDisabled ? 'disabled' : hasCondition ? 'conditional' : 'enabled'}`} />
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{ display: 'flex', alignItems: 'baseline', gap: '0.4rem', flexWrap: 'wrap' }}>
                          <span style={{ color: 'var(--accent)', fontFamily: 'monospace', fontSize: 'var(--text-md)' }}>
                            {task.name || `task-${ti + 1}`}
                          </span>
                          {isDisabled && <span className="bp-badge bp-badge-muted">disabled</span>}
                          {hasCondition && <span className="bp-badge bp-badge-warn">conditional</span>}
                          {isCall && <span className="bp-badge bp-badge-info">call</span>}
                          {task.retry > 0 && <span className="bp-badge bp-badge-info">retry:{task.retry}</span>}
                          {task.timeout_seconds > 0 && <span className="bp-badge bp-badge-info">{task.timeout_seconds}s</span>}
                          {task.continue_on_error && <span className="bp-badge bp-badge-warn">continue</span>}
                          {task.env && Object.keys(task.env).length > 0 && <span className="bp-badge bp-badge-info">env:{Object.keys(task.env).length}</span>}
                          {task.on_success && task.on_success.length > 0 && <span className="bp-badge bp-badge-ok">on_success</span>}
                          {task.on_fail && task.on_fail.length > 0 && <span className="bp-badge bp-badge-danger">on_fail</span>}
                        </div>
                        {cmdPreview && (
                          <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', fontFamily: 'monospace', marginTop: '0.15rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {cmdPreview}
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Expanded task detail */}
                    {isExpanded && (
                      <div className="bp-task-expanded">
                        {(task.cmd || task.run) && (
                          <>
                            <div className="hud-label">Command</div>
                            <pre>{task.cmd || task.run}</pre>
                          </>
                        )}
                        {task.call && (
                          <>
                            <div className="hud-label">Call</div>
                            <pre>{task.call}</pre>
                            {bp.imports && bp.imports.some(imp => imp.alias === task.call) && (
                              <div style={{ fontSize: 'var(--text-sm)', color: 'var(--status-success)', marginTop: '0.15rem' }}>
                                Resolves to import: <span style={{ fontFamily: 'monospace' }}>{bp.imports.find(imp => imp.alias === task.call)?.path}</span>
                              </div>
                            )}
                            {task.with && Object.keys(task.with).length > 0 && (
                              <>
                                <div className="hud-label" style={{ marginTop: '0.5rem' }}>With</div>
                                <pre>{Object.entries(task.with).map(([k, v]) => `${k}: ${v}`).join('\n')}</pre>
                              </>
                            )}
                          </>
                        )}
                        {task.dir && (
                          <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginTop: '0.25rem' }}>
                            <span className="hud-label">Dir: </span>{task.dir}
                          </div>
                        )}
                        {task.if && (
                          <div style={{ fontSize: 'var(--text-sm)', marginTop: '0.25rem' }}>
                            <span className="hud-label">Condition: </span>
                            <span style={{ fontFamily: 'monospace', color: 'var(--status-running)' }}>{task.if}</span>
                          </div>
                        )}
                        {task.env && Object.keys(task.env).length > 0 && (
                          <>
                            <div className="hud-label" style={{ marginTop: '0.5rem' }}>Environment</div>
                            <pre>{Object.entries(task.env).map(([k, v]) => `${k}=${v}`).join('\n')}</pre>
                          </>
                        )}
                        {task.on_success && task.on_success.length > 0 && (
                          <div style={{ fontSize: 'var(--text-sm)', marginTop: '0.25rem' }}>
                            <span className="hud-label">On Success: </span>
                            {task.on_success.map((h, hi) => (
                              <span key={hi} style={{ fontFamily: 'monospace', color: 'var(--status-success)' }}>
                                {h.type}: {h.value}{hi < task.on_success.length - 1 ? ', ' : ''}
                              </span>
                            ))}
                          </div>
                        )}
                        {task.on_fail && task.on_fail.length > 0 && (
                          <div style={{ fontSize: 'var(--text-sm)', marginTop: '0.25rem' }}>
                            <span className="hud-label">On Fail: </span>
                            {task.on_fail.map((h, hi) => (
                              <span key={hi} style={{ fontFamily: 'monospace', color: 'var(--status-failed)' }}>
                                {h.type}: {h.value}{hi < task.on_fail.length - 1 ? ', ' : ''}
                              </span>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          ))}
        </div>
      </div>

      {/* Footer */}
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '0.5rem 0', borderTop: '1px solid var(--border-default)',
        fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', flexShrink: 0,
      }}>
        <span>{bp.steps.length} sections · {totalTasks} tasks · {bp.inputs?.length ?? 0} inputs</span>
        <button className="btn btn-ghost" onClick={() => setShowYamlModal(true)} style={{ fontSize: 'var(--text-sm)' }}>
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
              <span style={{ fontSize: 'var(--text-sm)', textTransform: 'uppercase', letterSpacing: '0.1em', color: 'var(--text-tertiary)' }}>
                Blueprint YAML
              </span>
              <div style={{ display: 'flex', gap: '0.4rem' }}>
                <button className="btn btn-ghost" onClick={handleCopyYaml} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: 'var(--text-sm)' }}>
                  <Copy size={12} /> Copy
                </button>
                <button className="btn btn-ghost" onClick={() => setShowYamlModal(false)} style={{ padding: '0.25rem' }}>
                  <X size={15} />
                </button>
              </div>
            </div>
            <pre style={{
              flex: 1, overflow: 'auto', padding: '0.75rem',
              background: 'var(--bg-base)', borderRadius: '4px',
              border: '1px solid var(--border-default)',
              fontSize: 'var(--text-sm)', lineHeight: '1.6', whiteSpace: 'pre-wrap',
              wordBreak: 'break-all',
            }}>
              {rawYaml}
            </pre>
          </div>
        </div>
      )}

      {/* Run inputs modal */}
      {showRunModal && (
        <RunInputsModal
          entry={{ name: basename(path), path, isDir: false }}
          inputs={runInputs}
          onConfirm={handleRunConfirm}
          onCancel={() => setShowRunModal(false)}
        />
      )}

      {/* Run confirmation (requireConfirmation setting) */}
      {showConfirmModal && (
        <div className="hud-modal-overlay" onClick={() => setShowConfirmModal(false)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '400px' }}>
            <div style={{ padding: '1.25rem' }}>
              <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Run blueprint?</div>
              <div style={{ fontSize: 'var(--text-md)', color: 'var(--text-tertiary)', marginBottom: '1rem' }}>
                Execute <span style={{ fontFamily: 'monospace', color: 'var(--accent)' }}>
                  {blueprint?.blueprint?.name || path.split('/').pop()}
                </span> with no inputs?
              </div>
              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                <button className="btn btn-ghost" onClick={() => setShowConfirmModal(false)}>Cancel</button>
                <button className="btn btn-primary" style={{ borderColor: 'rgba(59, 130, 246, 0.5)', color: 'var(--status-success)' }}
                  onClick={handleConfirmRun}>
                  Run
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Delete confirmation */}
      {deleteConfirm && (
        <div className="hud-modal-overlay" onClick={() => setDeleteConfirm(false)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()}>
            <h3 style={{ fontSize: 'var(--text-base)', marginBottom: '0.5rem' }}>Delete Blueprint</h3>
            <p style={{ fontSize: 'var(--text-md)', color: 'var(--text-tertiary)', marginBottom: '0.5rem' }}>
              Are you sure you want to delete this blueprint? This cannot be undone.
            </p>
            <p style={{ fontFamily: 'monospace', fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', wordBreak: 'break-all' }}>
              {path}
            </p>
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button className="btn btn-ghost" onClick={() => setDeleteConfirm(false)}>Cancel</button>
              <button
                className="btn btn-primary"
                style={{ borderColor: 'var(--status-failed)', color: 'var(--status-failed)' }}
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
