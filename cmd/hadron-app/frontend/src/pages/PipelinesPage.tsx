import { useState, useCallback } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { FolderOpen, Play, ChevronLeft, Folder, FileCode, Layers, Plus, Pencil, Trash2, Workflow, MoreHorizontal } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { usePoll } from '../hooks/usePoll';
import { openDirectoryDialog, listPipelineFilesInDir, enqueuePipeline, listPipelines, getPipelineStages, readBlueprintFile, deleteBlueprintFile } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import { formatDuration, formatTime } from '../utils/format';
import { shortPath } from '../utils/path';
import { cn } from '@/lib/utils';
import type { FileEntry, Pipeline, PipelineStage } from '../api/types';
import { PipelineEditorModal } from '@/components/pipelines/PipelineEditorModal';
import { EMPTY_FORM, EMPTY_STAGE, parsePipelineToForm } from '@/utils/pipelineYaml';
import type { PipelineForm } from '@/utils/pipelineYaml';

import { useDaemon } from '../contexts/DaemonContext';
import { useNavigation } from '../contexts/NavigationContext';

export function PipelinesPage() {
  const daemon = useDaemon();
  const nav = useNavigation();
  const daemonStatus = daemon.status;
  const workspaceId = daemon.workspaceId;
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
  const [editorPath, setEditorPath] = useState<string | null>(null);

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
      const items = await listPipelineFilesInDir(dir);
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
  };

  const handleEditPipeline = async (entry: FileEntry) => {
    try {
      const content = await readBlueprintFile(entry.path);
      const form = parsePipelineToForm(content);
      if (!form) {
        toast.error('Failed to parse pipeline file');
        return;
      }
      setEditorMode('edit');
      setEditorForm(form);
      setEditorPath(entry.path);
    } catch (err) {
      toast.error(`Failed to read file: ${err}`);
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

  const canGoBack = currentDir && currentDir !== rootDir;

  return (
    <div className="flex flex-col gap-2 h-full">
      {/* Toolbar */}
      <div className="flex items-center justify-between gap-2">
        <span className="text-xl font-semibold text-foreground tracking-tight">Pipelines</span>
        {canGoBack && (
          <Button variant="ghost" onClick={handleBack}>
            <ChevronLeft size={13} /> Up
          </Button>
        )}
        <div className="ml-auto flex gap-1.5">
          {currentDir && (
            <Button onClick={handleNewPipeline} className="bg-blue-500 text-white hover:bg-blue-600">
              <Plus size={14} /> New Pipeline
            </Button>
          )}
          <Button onClick={handleOpenFolder} className="bg-yellow-500 text-yellow-950 hover:bg-yellow-600">
            <FolderOpen size={14} /> Open Folder
          </Button>
        </div>
      </div>

      {/* Current path */}
      {currentDir && (
        <div className="text-sm text-muted-foreground font-mono break-all">
          {currentDir}
        </div>
      )}

      {/* Dir error */}
      {dirError && (
        <div className="text-red-400 text-sm px-3 py-2 bg-red-500/10 rounded border border-red-500/30">
          {dirError}
        </div>
      )}

      {/* File list */}
      <div className="flex flex-col gap-px">
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
                className={cn(
                  'flex items-center gap-2 px-3 py-1 rounded cursor-pointer transition-colors',
                  'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
                  'border border-transparent',
                )}
                onClick={() => entry.isDir ? handleDrillDown(entry) : nav.openPipeline(entry.path)}
              >
                <span className={cn('shrink-0', entry.isDir ? 'text-yellow-500' : 'text-blue-400')}>
                  {entry.isDir ? <Folder size={15} /> : <FileCode size={15} />}
                </span>
                <span className="flex-1 min-w-0 -mt-[3px]">
                  {entry.name}
                  {entry.isDir && <span className="text-muted-foreground text-sm ml-1">/</span>}
                </span>
                {!entry.isDir && (
                  <div className="flex items-center gap-1 shrink-0" onClick={e => e.stopPropagation()}>
                    <Button
                      size="xs"
                      onClick={() => handleRun(entry)}
                      disabled={daemonStatus !== 'running' || isRunning}
                    >
                      <Play size={12} /> {isRunning ? 'Running…' : 'Run'}
                    </Button>
                    <DropdownMenu>
                      <DropdownMenuTrigger className="inline-flex items-center justify-center h-7 px-2 rounded-md text-xs font-medium bg-muted text-muted-foreground hover:bg-muted/80 hover:text-foreground transition-colors">
                        <MoreHorizontal size={14} />
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        <DropdownMenuItem onClick={() => nav.openFlowBuilder(entry.path)}>
                          <Workflow size={12} /> Flow Builder
                        </DropdownMenuItem>
                        <DropdownMenuItem onClick={() => handleEditPipeline(entry)}>
                          <Pencil size={12} /> Edit
                        </DropdownMenuItem>
                        <DropdownMenuItem onClick={() => setDeleteTarget(entry)} className="text-red-400 focus:text-red-400">
                          <Trash2 size={12} /> Delete
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>

      {/* Recent pipeline runs */}
      <div className="mt-1">
        <div className="text-xs tracking-wider uppercase text-muted-foreground mb-1.5">
          Recent Pipeline Runs
        </div>
        <div className="flex flex-col gap-px">
          {pipelines.length === 0 ? (
            <EmptyState message="No pipeline runs yet" sub="Run a pipeline file above to see results here" />
          ) : (
            pipelines.map(p => (
              <div
                key={p.id}
                className={cn(
                  'flex items-center gap-3 px-3 py-1.5 rounded cursor-pointer transition-colors',
                  'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
                  'border border-transparent',
                )}
                onClick={() => handleShowStages(p)}
              >
                <div className="flex-1 min-w-0 font-mono text-sm truncate">
                  {shortPath(p.pipeline_path)}
                </div>
                <span className={cn('text-sm shrink-0',
                  p.status === 'success' && 'text-blue-400',
                  p.status === 'failed' && 'text-red-400',
                  p.status === 'running' && 'text-amber-400',
                  !['success', 'failed', 'running'].includes(p.status) && 'text-muted-foreground',
                )}>
                  {p.status}
                </span>
                <div className="shrink-0 text-sm text-muted-foreground whitespace-nowrap w-28 text-right">
                  {formatTime(p.started_at)}
                </div>
                <div className="shrink-0 text-sm font-mono text-muted-foreground whitespace-nowrap w-16 text-right">
                  {formatDuration(p.started_at, p.ended_at)}
                </div>
                <Button
                  variant="ghost"
                  size="xs"
                  onClick={(e) => { e.stopPropagation(); handleShowStages(p); }}
                  disabled={stagesLoading}
                >
                  <Layers size={12} /> Stages
                </Button>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Stage detail modal */}
      <Dialog open={!!stageModal} onOpenChange={(open) => { if (!open) setStageModal(null); }}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Pipeline Stages</DialogTitle>
            {stageModal && (
              <div className="text-sm font-mono text-muted-foreground mt-0.5">{stageModal.pipelineId}</div>
            )}
          </DialogHeader>

          {stageModal && stageModal.stages.length === 0 ? (
            <EmptyState message="No stages" sub="This pipeline run has no recorded stages yet" />
          ) : stageModal && (
            <table className="w-full border-collapse">
              <thead>
                <tr>
                  <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50">#</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50">Stage</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50">Status</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50">Run ID</th>
                </tr>
              </thead>
              <tbody>
                {stageModal.stages.map(stage => (
                  <tr key={stage.id}>
                    <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border">{stage.stage_index + 1}</td>
                    <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border font-mono">{stage.stage_name}</td>
                    <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border">
                      <span className={cn('text-sm tracking-wide',
                        stage.status === 'success' && 'text-blue-400',
                        stage.status === 'failed' && 'text-red-400',
                        stage.status === 'running' && 'text-blue-400',
                        stage.status === 'skipped' && 'text-amber-400',
                        !['success', 'failed', 'running', 'skipped'].includes(stage.status) && 'text-muted-foreground',
                      )}>
                        {stage.status}
                      </span>
                    </td>
                    <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border">
                      {stage.run_id ? (
                        <Button
                          variant="ghost"
                          size="sm"
                          className="font-mono px-1 py-0.5"
                          onClick={() => { setStageModal(null); nav.openRun(stage.run_id); }}
                        >
                          {stage.run_id.slice(-8)}
                        </Button>
                      ) : (
                        <span className="text-muted-foreground text-sm">-</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          <DialogFooter showCloseButton />
        </DialogContent>
      </Dialog>

      {/* Pipeline editor modal (create / edit) */}
      <PipelineEditorModal
        mode={editorMode}
        initialForm={editorForm}
        editorPath={editorPath}
        currentDir={currentDir}
        onClose={() => setEditorMode(null)}
        onSaved={() => loadDir(currentDir)}
      />

      {/* Delete confirmation modal */}
      <AlertDialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Pipeline</AlertDialogTitle>
            <AlertDialogDescription>
              Delete <span className="font-mono text-blue-400">{deleteTarget?.name}</span>? This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={handleDeletePipeline}>Delete</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
