import { useState, useEffect } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ChevronLeft, Play, Copy, MoreHorizontal, Trash2, Pencil, Layers, ChevronDown, ChevronRight, Workflow } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import { readBlueprintFile, enqueuePipeline, deleteBlueprintFile } from '../api/client';
import { Spinner } from '../components/ui/Spinner';
import { basename } from '../utils/path';
import { parsePipelineYaml } from '../utils/yaml';
import { useDaemon } from '../contexts/DaemonContext';
import { useNavigation } from '../contexts/NavigationContext';
import { cn } from '@/lib/utils';

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

function parsePipelineToDetail(content: string): PipelineDetail | null {
  const raw = parsePipelineYaml(content);
  if (!raw) return null;
  return {
    name: raw.name,
    stop_on_fail: raw.stop_on_fail,
    stages: raw.stages.map(s => ({
      name: s.name,
      blueprint_path: s.blueprint_path,
      inputs: s.inputs,
      condition: s.condition,
    })),
    inputs: raw.inputs,
  };
}

export function PipelineDetailPage() {
  const daemon = useDaemon();
  const nav = useNavigation();
  const path = nav.selectedPipelinePath!;
  const daemonStatus = daemon.status;
  const workspaceId = daemon.workspaceId;
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
        const parsed = parsePipelineToDetail(content);
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
      nav.goBack();
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

  if (error || !pipeline) {
    return (
      <div className="flex flex-col h-full gap-3">
        <div className="flex items-center justify-between mb-6">
          <Button variant="ghost" onClick={nav.goBack}>
            <ChevronLeft size={13} /> Back
          </Button>
          <span className="text-xl font-semibold text-foreground tracking-tight">Error</span>
        </div>
        <div className="text-red-400 text-sm p-3 bg-red-500/10 rounded border border-red-500/30">
          {error || 'Failed to load pipeline'}
        </div>
      </div>
    );
  }

  const title = pipeline.name || basename(path);
  const topInputKeys = Object.keys(pipeline.inputs);

  return (
    <div className="flex flex-col h-full gap-3">
      {/* Header */}
      <div className="flex items-center justify-between mb-6 gap-2">
        <Button variant="ghost" onClick={nav.goBack}>
          <ChevronLeft size={13} /> Back
        </Button>
        <Layers size={15} className="text-primary shrink-0" />
        <span className="text-xl font-semibold text-foreground tracking-tight flex-1 overflow-hidden text-ellipsis whitespace-nowrap">
          {title}
        </span>
        <Button
          onClick={() => nav.openFlowBuilder(path)}
        >
          <Workflow size={12} /> Flow
        </Button>
        <Button
          onClick={handleRun}
          disabled={daemonStatus !== 'running' || running}
          className="border-blue-500/50 text-blue-400"
        >
          <Play size={12} /> {running ? 'Running...' : 'Run'}
        </Button>
        <div className="relative">
          <Button variant="ghost" size="xs" onClick={() => setShowActionMenu(!showActionMenu)}>
            <MoreHorizontal size={15} />
          </Button>
          {showActionMenu && (
            <div
              className="absolute right-0 top-full mt-1 bg-card border border-border rounded min-w-[160px] z-50 shadow-lg"
              onClick={() => setShowActionMenu(false)}
            >
              <button
                onClick={() => setShowYamlModal(true)}
                className="flex items-center gap-2 w-full px-3 py-2 bg-transparent border-none text-foreground cursor-pointer font-mono text-sm text-left hover:bg-muted"
              >
                <Copy size={13} /> View YAML
              </button>
              <button
                onClick={() => nav.navigate('pipelines')}
                className="flex items-center gap-2 w-full px-3 py-2 bg-transparent border-none text-foreground cursor-pointer font-mono text-sm text-left hover:bg-muted"
              >
                <Pencil size={13} /> Edit
              </button>
              <button
                onClick={() => setDeleteConfirm(true)}
                className="flex items-center gap-2 w-full px-3 py-2 bg-transparent border-none text-red-400 cursor-pointer font-mono text-sm text-left hover:bg-muted"
              >
                <Trash2 size={13} /> Delete
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Main content — split layout like BlueprintDetailPage */}
      <div className="flex gap-6 flex-1 overflow-hidden">
        {/* Left — Metadata */}
        <div className="w-80 min-w-[280px] overflow-y-auto pr-2">
          <div className="mb-4">
            <div className="text-xs uppercase tracking-widest text-muted-foreground mb-1.5 border-b border-border pb-1">Pipeline</div>
            <div className="flex gap-2 text-sm py-0.5">
              <span className="text-muted-foreground min-w-[80px] shrink-0">Name</span>
              <span className="font-mono">{pipeline.name || '—'}</span>
            </div>
            <div className="flex gap-2 text-sm py-0.5">
              <span className="text-muted-foreground min-w-[80px] shrink-0">Stages</span>
              <span>{pipeline.stages.length}</span>
            </div>
            <div className="flex gap-2 text-sm py-0.5">
              <span className="text-muted-foreground min-w-[80px] shrink-0">Stop on Fail</span>
              <Badge variant={pipeline.stop_on_fail ? "running" : "secondary"}>
                {pipeline.stop_on_fail ? 'yes' : 'no'}
              </Badge>
            </div>
            <div className="flex flex-col gap-2 text-sm py-0.5">
              <span className="text-muted-foreground min-w-[80px] shrink-0">Path</span>
              <span className="text-sm font-mono text-muted-foreground break-all">{path}</span>
            </div>
          </div>

          {/* Pipeline-level inputs */}
          <div className="mb-4">
            <div className="text-xs uppercase tracking-widest text-muted-foreground mb-1.5 border-b border-border pb-1">Pipeline Inputs ({topInputKeys.length})</div>
            {topInputKeys.length === 0 ? (
              <div className="text-sm text-muted-foreground">No pipeline-level inputs</div>
            ) : (
              topInputKeys.map((key, i) => (
                <div key={i} className={cn("py-1", i < topInputKeys.length - 1 && "border-b border-border/50")}>
                  <div className="flex items-baseline gap-1.5">
                    <span className="text-primary font-mono text-sm">{key}</span>
                    {pipeline.inputs[key] && (
                      <span className="text-xs text-muted-foreground">
                        = <span className="font-mono">{pipeline.inputs[key]}</span>
                      </span>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Right — Stage Timeline */}
        <div className="flex-1 overflow-y-auto">
          {pipeline.stages.map((stage, si) => {
            const isExpanded = expandedStages.has(si);
            const inputKeys = Object.keys(stage.inputs);
            return (
              <div key={si} className="mb-2">
                {/* Stage header */}
                <div
                  className="flex items-center gap-2 py-2 cursor-pointer select-none text-sm hover:text-blue-400"
                  onClick={() => toggleStage(si)}
                >
                  {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                  <span className="font-mono text-primary text-sm">
                    {stage.name || `Stage ${si + 1}`}
                  </span>
                  <span className="text-xs text-muted-foreground ml-2">
                    #{si + 1}
                  </span>
                  {stage.condition && (
                    <Badge variant="running" className="ml-1">
                      conditional
                    </Badge>
                  )}
                  {inputKeys.length > 0 && (
                    <Badge variant="secondary" className="ml-1">
                      {inputKeys.length} input{inputKeys.length !== 1 ? 's' : ''}
                    </Badge>
                  )}
                </div>

                {/* Stage detail */}
                {isExpanded && (
                  <div className="p-3 my-1 ml-4 bg-card rounded border border-border">
                    <span className="text-sm font-medium text-muted-foreground">Blueprint</span>
                    <pre className="my-2 p-2 bg-background rounded text-xs overflow-x-auto whitespace-pre-wrap break-all">{stage.blueprint_path}</pre>

                    {stage.condition && (
                      <div className="text-sm mt-1">
                        <span className="text-sm font-medium text-muted-foreground">Condition: </span>
                        <span className="font-mono text-amber-400">{stage.condition}</span>
                      </div>
                    )}

                    {inputKeys.length > 0 && (
                      <>
                        <span className="text-sm font-medium text-muted-foreground block mt-2">Inputs</span>
                        <pre className="my-2 p-2 bg-background rounded text-xs overflow-x-auto whitespace-pre-wrap break-all">
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
      <div className="flex items-center justify-between py-2 border-t border-border text-sm text-muted-foreground shrink-0">
        <span>{pipeline.stages.length} stages · {topInputKeys.length} inputs · stop_on_fail: {pipeline.stop_on_fail ? 'yes' : 'no'}</span>
        <Button variant="ghost" size="sm" onClick={() => setShowYamlModal(true)}>
          View YAML
        </Button>
      </div>

      {/* YAML Modal */}
      <Dialog open={showYamlModal} onOpenChange={setShowYamlModal}>
        <DialogContent className="sm:max-w-3xl max-h-[85vh] flex flex-col">
          <DialogHeader>
            <DialogTitle className="flex items-center justify-between">
              <span>Pipeline YAML</span>
              <Button variant="ghost" size="sm" onClick={handleCopyYaml}>
                <Copy size={12} /> Copy
              </Button>
            </DialogTitle>
          </DialogHeader>
          <pre className="flex-1 overflow-auto p-3 bg-background rounded border border-border text-sm leading-relaxed whitespace-pre-wrap break-all">
            {rawYaml}
          </pre>
        </DialogContent>
      </Dialog>

      {/* Delete confirmation */}
      <AlertDialog open={deleteConfirm} onOpenChange={setDeleteConfirm}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Pipeline</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete this pipeline? This cannot be undone.
              <span className="block font-mono text-sm text-muted-foreground break-all mt-2">
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
