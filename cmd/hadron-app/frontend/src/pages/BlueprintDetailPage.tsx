import { useState, useEffect } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import { Badge } from '@/components/ui/badge';
import { ChevronLeft, Play, ChevronDown, ChevronRight, Copy, MoreHorizontal, Trash2, Pencil } from 'lucide-react';
import { parseBlueprintFull, readBlueprintFile, enqueueRun, parseBlueprintInputs, deleteBlueprintFile, getSettings } from '../api/client';
import { Spinner } from '../components/ui/Spinner';
import { RunInputsModal } from '../components/ui/RunInputsModal';
import type { ParsedBlueprint, BlueprintInput } from '../api/types';
import { useDaemon } from '../contexts/DaemonContext';
import { useNavigation } from '../contexts/NavigationContext';
import { cn } from '@/lib/utils';

function basename(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts[parts.length - 1] || p;
}

export function BlueprintDetailPage() {
  const daemon = useDaemon();
  const nav = useNavigation();
  const path = nav.selectedBlueprintPath!;
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
        const run = await enqueueRun({ blueprint_path: path, workspace_id: daemon.workspaceId });
        toast.success('Run enqueued');
        nav.openRun(run.id);
      }
    } catch (err: unknown) {
      toast.error(`Failed to start run: ${err}`);
    }
  };

  const handleConfirmRun = async () => {
    setShowConfirmModal(false);
    try {
      const run = await enqueueRun({ blueprint_path: path, workspace_id: daemon.workspaceId });
      toast.success('Run enqueued');
      nav.openRun(run.id);
    } catch (err: unknown) {
      toast.error(`Failed to start run: ${err}`);
    }
  };

  const handleRunConfirm = async (values: Record<string, unknown>, dryRun: boolean) => {
    setShowRunModal(false);
    try {
      const run = await enqueueRun({ blueprint_path: path, workspace_id: daemon.workspaceId, inputs: values, dry_run: dryRun || undefined });
      toast.success(dryRun ? 'Dry run enqueued' : 'Run enqueued');
      nav.openRun(run.id);
    } catch (err: unknown) {
      toast.error(`Failed to start run: ${err}`);
    }
  };

  const handleDelete = async () => {
    try {
      await deleteBlueprintFile(path);
      toast.success('Blueprint deleted');
      nav.goBack();
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
      <div className="flex flex-col h-full gap-3">
        <div className="flex items-center justify-between mb-6">
          <Button variant="ghost" onClick={nav.goBack}>
            <ChevronLeft size={13} /> Back
          </Button>
          <span className="text-xl font-semibold text-foreground tracking-tight">Loading...</span>
          <Spinner size={14} />
        </div>
      </div>
    );
  }

  if (error || !blueprint) {
    return (
      <div className="flex flex-col h-full gap-3">
        <div className="flex items-center justify-between mb-6">
          <Button variant="ghost" onClick={nav.goBack}>
            <ChevronLeft size={13} /> Back
          </Button>
          <span className="text-xl font-semibold text-foreground tracking-tight">Error</span>
        </div>
        <div className="text-red-400 text-sm p-3 bg-red-500/10 rounded border border-red-500/30">
          {error || 'Failed to load blueprint'}
        </div>
      </div>
    );
  }

  const bp = blueprint;
  const title = bp.blueprint.title || bp.blueprint.name || basename(path);
  const totalTasks = bp.steps.reduce((sum, s) => sum + s.tasks.length, 0);

  return (
    <div className="flex flex-col h-full gap-3">
      {/* Header */}
      <div className="flex items-center justify-between mb-6 gap-2">
        <Button variant="ghost" onClick={nav.goBack}>
          <ChevronLeft size={13} /> Back
        </Button>
        <span className="text-xl font-semibold text-foreground tracking-tight flex-1 truncate">
          {title}
        </span>
        <Button
          onClick={handleRun}
          disabled={daemon.status !== 'running'}
          className="border-blue-500/50 text-blue-400"
        >
          <Play size={12} /> Run
        </Button>
        <div className="relative">
          <Button variant="ghost" onClick={() => setShowActionMenu(!showActionMenu)} size="xs">
            <MoreHorizontal size={15} />
          </Button>
          {showActionMenu && (
            <div
              className="absolute right-0 top-full mt-1 bg-card border border-border rounded shadow-lg min-w-[160px] z-50"
              onClick={() => setShowActionMenu(false)}
            >
              <button
                onClick={() => setShowYamlModal(true)}
                className="flex items-center gap-2 w-full px-3 py-2 text-sm font-mono text-foreground hover:bg-muted/50 transition-colors bg-transparent border-none cursor-pointer text-left"
              >
                <Copy size={13} /> View YAML
              </button>
              <button
                onClick={() => nav.openWizard(path)}
                className="flex items-center gap-2 w-full px-3 py-2 text-sm font-mono text-foreground hover:bg-muted/50 transition-colors bg-transparent border-none cursor-pointer text-left"
              >
                <Pencil size={13} /> Edit
              </button>
              <button
                onClick={() => setDeleteConfirm(true)}
                className="flex items-center gap-2 w-full px-3 py-2 text-sm font-mono text-red-400 hover:bg-muted/50 transition-colors bg-transparent border-none cursor-pointer text-left"
              >
                <Trash2 size={13} /> Delete
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Main content */}
      <div className="flex flex-1 gap-4 overflow-hidden">
        {/* Left — Metadata */}
        <div className="w-72 shrink-0 overflow-y-auto flex flex-col gap-3">
          {/* Blueprint identity */}
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Metadata</div>
            {bp.blueprint.name && (
              <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">Name</span><span>{bp.blueprint.name}</span></div>
            )}
            {bp.blueprint.slug && (
              <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">Slug</span><span className="font-mono">{bp.blueprint.slug}</span></div>
            )}
            {bp.version && (
              <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">Version</span><Badge variant="secondary">v{bp.version}</Badge></div>
            )}
            {bp.blueprint.description && (
              <div className="flex flex-col py-1 text-sm">
                <span className="text-muted-foreground">Description</span>
                <span className="text-foreground text-sm leading-relaxed">{bp.blueprint.description}</span>
              </div>
            )}
            {bp.blueprint.author && (
              <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">Author</span><span>{bp.blueprint.author}</span></div>
            )}
            {bp.blueprint.license && (
              <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">License</span><span>{bp.blueprint.license}</span></div>
            )}
            {bp.blueprint.homepage && (
              <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">Homepage</span><span className="break-all text-sm">{bp.blueprint.homepage}</span></div>
            )}
            {bp.blueprint.tags && bp.blueprint.tags.length > 0 && (
              <div className="mt-1.5">
                {bp.blueprint.tags.map((tag, i) => (
                  <span key={i} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-muted text-muted-foreground mr-1 mb-1">{tag}</span>
                ))}
              </div>
            )}
          </div>

          {/* Inputs */}
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Inputs ({bp.inputs?.length ?? 0})</div>
            {(!bp.inputs || bp.inputs.length === 0) ? (
              <div className="text-sm text-muted-foreground">No inputs defined</div>
            ) : (
              bp.inputs.map((inp, i) => (
                <div key={i} className={cn('py-1.5', i < bp.inputs.length - 1 && 'border-b border-border/50')}>
                  <div className="flex items-baseline gap-1.5">
                    <span className="text-blue-400 font-mono text-sm">{inp.name}</span>
                    <Badge variant="secondary">{inp.type}</Badge>
                    {inp.required && <span className="text-red-400 text-xs">*</span>}
                  </div>
                  {inp.description && (
                    <div className="text-sm text-muted-foreground mt-0.5">{inp.description}</div>
                  )}
                  {inp.default != null && inp.default !== '' && (
                    <div className="text-xs text-muted-foreground mt-0.5">Default: <span className="font-mono">{String(inp.default)}</span></div>
                  )}
                  {inp.enum && inp.enum.length > 0 && (
                    <div className="text-xs text-muted-foreground mt-0.5">Enum: {inp.enum.join(', ')}</div>
                  )}
                </div>
              ))
            )}
          </div>

          {/* Imports */}
          {bp.imports && bp.imports.length > 0 && (
            <div className="rounded-lg border border-border bg-card p-4">
              <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Imports ({bp.imports.length})</div>
              {bp.imports.map((imp, i) => (
                <div key={i} className="py-2 px-2.5 mb-1.5 bg-muted rounded border border-border">
                  <div className="flex items-baseline gap-2">
                    <span className="text-blue-400 font-mono text-sm font-bold">
                      {imp.alias || '(unnamed)'}
                    </span>
                    <Badge variant="secondary">import</Badge>
                  </div>
                  <div className="text-sm text-muted-foreground font-mono mt-0.5 break-all">
                    {imp.path}
                  </div>
                  {imp.with && Object.keys(imp.with).length > 0 && (
                    <div className="mt-1 pt-1 border-t border-border/50">
                      <span className="text-xs uppercase tracking-wider text-muted-foreground">with</span>
                      <div className="mt-0.5">
                        {Object.entries(imp.with).map(([k, v]) => (
                          <div key={k} className="text-sm font-mono text-foreground">
                            <span className="text-muted-foreground">{k}:</span> {String(v)}
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
            <div className="rounded-lg border border-border bg-card p-4">
              <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Hooks</div>
              {bp.hooks.before_run?.length > 0 && (
                <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">before_run</span><span>{bp.hooks.before_run.length}</span></div>
              )}
              {bp.hooks.after_run?.length > 0 && (
                <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">after_run</span><span>{bp.hooks.after_run.length}</span></div>
              )}
              {bp.hooks.on_error?.length > 0 && (
                <div className="flex items-center justify-between py-1 text-sm"><span className="text-muted-foreground">on_error</span><span>{bp.hooks.on_error.length}</span></div>
              )}
            </div>
          )}
        </div>

        {/* Right — Step Timeline */}
        <div className="flex-1 overflow-y-auto">
          {bp.steps.map((section, si) => (
            <div key={si} className="mb-3">
              {/* Section header */}
              <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-muted/50 cursor-pointer select-none text-sm font-medium" onClick={() => toggleSection(si)}>
                {expandedSections.has(si) ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                <span className="text-blue-400">
                  {section.section || `Section ${si + 1}`}
                </span>
                <span className="text-xs text-muted-foreground ml-auto">
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
                    <div className="flex items-start gap-2 px-3 py-2 cursor-pointer hover:bg-muted/30 rounded" onClick={() => toggleTask(taskKey)}>
                      <div className={cn('size-2 rounded-full mt-1.5 shrink-0', isDisabled ? 'bg-muted-foreground' : hasCondition ? 'bg-amber-400' : 'bg-blue-400')} />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-baseline gap-1.5 flex-wrap">
                          <span className="text-blue-400 font-mono text-sm">
                            {task.name || `task-${ti + 1}`}
                          </span>
                          {isDisabled && <Badge variant="queued">disabled</Badge>}
                          {hasCondition && <Badge variant="running">conditional</Badge>}
                          {isCall && <Badge variant="secondary">call</Badge>}
                          {task.retry > 0 && <Badge variant="secondary">retry:{task.retry}</Badge>}
                          {task.timeout_seconds > 0 && <Badge variant="secondary">{task.timeout_seconds}s</Badge>}
                          {task.continue_on_error && <Badge variant="running">continue</Badge>}
                          {task.env && Object.keys(task.env).length > 0 && <Badge variant="secondary">env:{Object.keys(task.env).length}</Badge>}
                          {task.on_success && task.on_success.length > 0 && <Badge variant="success">on_success</Badge>}
                          {task.on_fail && task.on_fail.length > 0 && <Badge variant="failed">on_fail</Badge>}
                        </div>
                        {cmdPreview && (
                          <div className="text-sm text-muted-foreground font-mono mt-0.5 truncate">
                            {cmdPreview}
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Expanded task detail */}
                    {isExpanded && (
                      <div className="ml-6 px-3 py-2 mb-1 rounded bg-muted/30 border border-border">
                        {(task.cmd || task.run) && (
                          <>
                            <div className="text-sm font-medium text-muted-foreground">Command</div>
                            <pre>{task.cmd || task.run}</pre>
                          </>
                        )}
                        {task.call && (
                          <>
                            <div className="text-sm font-medium text-muted-foreground">Call</div>
                            <pre>{task.call}</pre>
                            {bp.imports && bp.imports.some(imp => imp.alias === task.call) && (
                              <div className="text-sm text-blue-400 mt-0.5">
                                Resolves to import: <span className="font-mono">{bp.imports.find(imp => imp.alias === task.call)?.path}</span>
                              </div>
                            )}
                            {task.with && Object.keys(task.with).length > 0 && (
                              <>
                                <div className="text-sm font-medium text-muted-foreground mt-2">With</div>
                                <pre>{Object.entries(task.with).map(([k, v]) => `${k}: ${v}`).join('\n')}</pre>
                              </>
                            )}
                          </>
                        )}
                        {task.dir && (
                          <div className="text-sm text-muted-foreground mt-1">
                            <span className="text-sm font-medium text-muted-foreground">Dir: </span>{task.dir}
                          </div>
                        )}
                        {task.if && (
                          <div className="text-sm mt-1">
                            <span className="text-sm font-medium text-muted-foreground">Condition: </span>
                            <span className="font-mono text-amber-400">{task.if}</span>
                          </div>
                        )}
                        {task.env && Object.keys(task.env).length > 0 && (
                          <>
                            <div className="text-sm font-medium text-muted-foreground mt-2">Environment</div>
                            <pre>{Object.entries(task.env).map(([k, v]) => `${k}=${v}`).join('\n')}</pre>
                          </>
                        )}
                        {task.on_success && task.on_success.length > 0 && (
                          <div className="text-sm mt-1">
                            <span className="text-sm font-medium text-muted-foreground">On Success: </span>
                            {task.on_success.map((h, hi) => (
                              <span key={hi} className="font-mono text-blue-400">
                                {h.type}: {h.value}{hi < task.on_success.length - 1 ? ', ' : ''}
                              </span>
                            ))}
                          </div>
                        )}
                        {task.on_fail && task.on_fail.length > 0 && (
                          <div className="text-sm mt-1">
                            <span className="text-sm font-medium text-muted-foreground">On Fail: </span>
                            {task.on_fail.map((h, hi) => (
                              <span key={hi} className="font-mono text-red-400">
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
      <div className="flex items-center justify-between py-2 border-t border-border text-sm text-muted-foreground shrink-0">
        <span>{bp.steps.length} sections · {totalTasks} tasks · {bp.inputs?.length ?? 0} inputs</span>
        <Button variant="ghost" onClick={() => setShowYamlModal(true)} size="sm">
          View YAML
        </Button>
      </div>

      {/* YAML Modal */}
      <Dialog open={showYamlModal} onOpenChange={(open) => { if (!open) setShowYamlModal(false); }}>
        <DialogContent className="sm:max-w-3xl max-h-[85vh] flex flex-col">
          <DialogHeader className="flex flex-row items-center justify-between">
            <DialogTitle>Blueprint YAML</DialogTitle>
            <Button variant="ghost" onClick={handleCopyYaml} size="sm">
              <Copy size={12} /> Copy
            </Button>
          </DialogHeader>
          <pre className="flex-1 overflow-auto p-3 bg-background rounded border border-border text-sm leading-relaxed whitespace-pre-wrap break-all">
            {rawYaml}
          </pre>
        </DialogContent>
      </Dialog>

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
      <AlertDialog open={showConfirmModal} onOpenChange={(open) => { if (!open) setShowConfirmModal(false); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Run blueprint?</AlertDialogTitle>
            <AlertDialogDescription>
              Execute <span className="font-mono text-blue-400">
                {blueprint?.blueprint?.name || path.split('/').pop()}
              </span> with no inputs?
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirmRun}>Run</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Delete confirmation */}
      <AlertDialog open={deleteConfirm} onOpenChange={(open) => { if (!open) setDeleteConfirm(false); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Blueprint</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete this blueprint? This cannot be undone.
              <br />
              <span className="font-mono text-sm break-all">
                {path}
              </span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={handleDelete}>Delete</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
