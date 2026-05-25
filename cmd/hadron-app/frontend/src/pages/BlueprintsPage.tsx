import { useState, useEffect, useCallback, useRef } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import { openDirectoryDialog, listBlueprintFilesInDir, validateBlueprintFile, enqueueRun, parseBlueprintInputs, deleteBlueprintFile, getBlueprintMetadata, getSettings, createDirectory, selectDirectoryDialog, moveBlueprintFile, copyBlueprintFile, archiveBlueprintFile, getBlueprintDir, setBlueprintDir } from '../api/client';
import { ValidateResult, FileEntry, BlueprintInput, BlueprintMetaSummary } from '../api/types';
import { Input } from '@/components/ui/input';
import { FolderOpen, FolderPlus, Play, CheckCircle, ChevronLeft, Folder, FileCode, Plus, Trash2, RefreshCw, MoveRight, Copy, Archive, MoreHorizontal } from 'lucide-react';
import { Checkbox } from '@/components/ui/checkbox';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { EmptyState } from '../components/ui/EmptyState';
import { RunInputsModal } from '../components/ui/RunInputsModal';
import { useDaemon } from '../contexts/DaemonContext';
import { useNavigation } from '../contexts/NavigationContext';
import { cn } from '@/lib/utils';

export function BlueprintsPage() {
  const daemon = useDaemon();
  const nav = useNavigation();
  const [rootDir, setRootDir] = useState<string>('');
  const [currentDir, setCurrentDir] = useState<string>('');
  const [entries, setEntries] = useState<FileEntry[]>([]);
  const [validateStates, setValidateStates] = useState<Record<string, ValidateResult>>({});
  const [runningPaths, setRunningPaths] = useState<Set<string>>(new Set());
  const [error, setError] = useState<string | null>(null);
  const [inputModalEntry, setInputModalEntry] = useState<FileEntry | null>(null);
  const [inputModalInputs, setInputModalInputs] = useState<BlueprintInput[]>([]);
  const [search, setSearch] = useState('');
  const [sortBy, setSortBy] = useState<string>(() => 'name-asc');
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null);
  const [metaCache, setMetaCache] = useState<Record<string, BlueprintMetaSummary>>({});
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [batchDeleteConfirm, setBatchDeleteConfirm] = useState(false);
  const [confirmEntry, setConfirmEntry] = useState<FileEntry | null>(null);
  const [focusIndex, setFocusIndex] = useState(-1);
  const focusRef = useRef<HTMLDivElement>(null);

  const loadDir = useCallback(async (dir: string) => {
    setError(null);
    try {
      const items = await listBlueprintFilesInDir(dir);
      setEntries(items ?? []);
    } catch (err) {
      setError(String(err));
      setEntries([]);
    }
  }, []);

  // Restore blueprint folder from settings.json (authoritative source)
  useEffect(() => {
    getBlueprintDir().then(dir => {
      if (dir) {
        setRootDir(dir);
        setCurrentDir(dir);
      }
    });
  }, []);

  useEffect(() => {
    if (currentDir) loadDir(currentDir);
  }, [currentDir, loadDir]);

  // Listen for global refresh shortcut (R key)
  useEffect(() => {
    const handler = () => { if (currentDir) loadDir(currentDir); };
    window.addEventListener('hadron:refresh', handler);
    return () => window.removeEventListener('hadron:refresh', handler);
  }, [currentDir, loadDir]);

  // Lazy-load metadata for all .yaml files after entries change
  useEffect(() => {
    const yamlFiles = entries.filter(e => !e.isDir && /\.ya?ml$/i.test(e.name));
    if (yamlFiles.length === 0) return;
    let cancelled = false;
    yamlFiles.forEach(entry => {
      if (metaCache[entry.path]) return; // already cached
      getBlueprintMetadata(entry.path)
        .then(meta => {
          if (!cancelled) {
            setMetaCache(prev => ({ ...prev, [entry.path]: meta }));
          }
        })
        .catch(() => {}); // silently skip unparseable files
    });
    return () => { cancelled = true; };
  }, [entries]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleOpenFolder = async () => {
    const dir = await openDirectoryDialog();
    if (dir) {
      setRootDir(dir);
      setCurrentDir(dir);
      setValidateStates({});
      setMetaCache({});
      setSelected(new Set());
      setBlueprintDir(dir); // persist to settings.json (authoritative source)
    }
  };

  const handleNewFolder = async () => {
    if (!currentDir) return;
    const name = window.prompt('Folder name:');
    if (!name) return;
    try {
      await createDirectory(currentDir, name);
      toast.success(`Folder "${name}" created`);
      loadDir(currentDir);
    } catch (err) {
      toast.error(`Failed to create folder: ${err}`);
    }
  };

  const handleDrillDown = (entry: FileEntry) => {
    if (entry.isDir) {
      setCurrentDir(entry.path);
      setValidateStates({});
      setMetaCache({});
      setSelected(new Set());
    }
  };

  const handleBack = () => {
    if (!currentDir || currentDir === rootDir) return;
    const parts = currentDir.split(/[/\\]/);
    parts.pop();
    const parent = parts.join('/') || '/';
    setCurrentDir(parent);
    setValidateStates({});
    setMetaCache({});
    setSelected(new Set());
  };

  const handleValidate = async (entry: FileEntry) => {
    const result = await validateBlueprintFile(entry.path);
    setValidateStates(prev => ({ ...prev, [entry.path]: result }));
    if (result.valid) {
      toast.success('Blueprint valid');
    } else {
      toast.error(`Validation failed: ${result.error ?? 'unknown error'}`);
    }
  };

  const doEnqueue = async (entry: FileEntry, inputs: Record<string, unknown>, dryRun = false) => {
    setRunningPaths(prev => new Set(prev).add(entry.path));
    try {
      const run = await enqueueRun({ blueprint_path: entry.path, workspace_id: daemon.workspaceId, inputs, dry_run: dryRun || undefined });
      toast.success(dryRun ? 'Dry run enqueued' : 'Run enqueued');
      nav.openRun(run.id);
    } catch (err) {
      toast.error(`Failed to start run: ${err}`);
    } finally {
      setRunningPaths(prev => {
        const next = new Set(prev);
        next.delete(entry.path);
        return next;
      });
    }
  };

  const handleRun = async (entry: FileEntry) => {
    if (daemon.status !== 'running') return;
    const inputs = await parseBlueprintInputs(entry.path);
    if (inputs && inputs.length > 0) {
      setInputModalInputs(inputs);
      setInputModalEntry(entry);
    } else {
      try {
        const settings = await getSettings();
        if (settings.safety.requireConfirmation) {
          setConfirmEntry(entry);
          return;
        }
      } catch { /* proceed */ }
      doEnqueue(entry, {});
    }
  };

  // ── Selection helpers ──
  const toggleSelect = (path: string) => {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path); else next.add(path);
      return next;
    });
  };

  const selectAllFiles = () => {
    const filePaths = entries.filter(e => !e.isDir).map(e => e.path);
    setSelected(prev => prev.size === filePaths.length ? new Set() : new Set(filePaths));
  };

  const handleBatchValidate = async () => {
    const paths = Array.from(selected);
    let valid = 0;
    let invalid = 0;
    for (const path of paths) {
      const result = await validateBlueprintFile(path);
      setValidateStates(prev => ({ ...prev, [path]: result }));
      if (result.valid) valid++; else invalid++;
    }
    toast.success(`Validated ${paths.length}: ${valid} valid, ${invalid} invalid`);
  };

  const handleBatchRun = async () => {
    const paths = Array.from(selected);
    let ok = 0;
    for (const path of paths) {
      try {
        await enqueueRun({ blueprint_path: path, workspace_id: daemon.workspaceId });
        ok++;
      } catch { /* skip failures */ }
    }
    toast.success(`Enqueued ${ok} of ${paths.length} runs`);
    setSelected(new Set());
    if (ok > 0) {
      nav.navigate('runs');
    }
  };

  const handleBatchDelete = async () => {
    const paths = Array.from(selected);
    let ok = 0;
    for (const path of paths) {
      try {
        await deleteBlueprintFile(path);
        ok++;
      } catch { /* skip */ }
    }
    toast.success(`Deleted ${ok} of ${paths.length} blueprints`);
    setBatchDeleteConfirm(false);
    setSelected(new Set());
    if (currentDir) loadDir(currentDir);
  };

  const handleBatchMove = async () => {
    const destDir = await selectDirectoryDialog();
    if (!destDir) return;
    const paths = Array.from(selected);
    let ok = 0;
    for (const path of paths) {
      try { await moveBlueprintFile(path, destDir); ok++; } catch { /* skip */ }
    }
    toast.success(`Moved ${ok} of ${paths.length} blueprints`);
    setSelected(new Set());
    if (currentDir) loadDir(currentDir);
  };

  const handleBatchCopy = async () => {
    const destDir = await selectDirectoryDialog();
    if (!destDir) return;
    const paths = Array.from(selected);
    let ok = 0;
    for (const path of paths) {
      try { await copyBlueprintFile(path, destDir); ok++; } catch { /* skip */ }
    }
    toast.success(`Copied ${ok} of ${paths.length} blueprints`);
    setSelected(new Set());
  };

  const handleBatchArchive = async () => {
    const paths = Array.from(selected);
    let ok = 0;
    for (const path of paths) {
      try { await archiveBlueprintFile(path); ok++; } catch { /* skip */ }
    }
    toast.success(`Archived ${ok} of ${paths.length} blueprints`);
    setSelected(new Set());
    if (currentDir) loadDir(currentDir);
  };

  const handleDelete = async (path: string) => {
    try {
      await deleteBlueprintFile(path);
      toast.success('Blueprint deleted');
      setDeleteConfirm(null);
      if (currentDir) loadDir(currentDir);
    } catch (err: unknown) {
      toast.error(`Delete failed: ${err}`);
    }
  };

  // Filter and sort entries
  const filteredEntries = entries
    .filter(e => {
      if (!search) return true;
      if (e.isDir) return true; // always show dirs
      return e.name.toLowerCase().includes(search.toLowerCase());
    })
    .sort((a, b) => {
      // Directories always first
      if (a.isDir && !b.isDir) return -1;
      if (!a.isDir && b.isDir) return 1;
      if (sortBy === 'name-desc') return b.name.localeCompare(a.name);
      if (sortBy === 'steps-asc' || sortBy === 'steps-desc') {
        const aSteps = metaCache[a.path]?.step_count ?? 0;
        const bSteps = metaCache[b.path]?.step_count ?? 0;
        return sortBy === 'steps-asc' ? aSteps - bSteps : bSteps - aSteps;
      }
      return a.name.localeCompare(b.name);
    });

  // Arrow key navigation
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      const count = filteredEntries.length;
      if (count === 0) return;
      if (e.key === 'ArrowDown') { e.preventDefault(); setFocusIndex(prev => Math.min(prev + 1, count - 1)); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); setFocusIndex(prev => Math.max(prev - 1, 0)); }
      else if (e.key === 'Enter' && focusIndex >= 0 && focusIndex < count) {
        e.preventDefault();
        const entry = filteredEntries[focusIndex];
        if (entry.isDir) setCurrentDir(entry.path);
        else nav.openBlueprint(entry.path);
      } else if (e.key === ' ' && focusIndex >= 0 && focusIndex < count) {
        e.preventDefault();
        const entry = filteredEntries[focusIndex];
        if (!entry.isDir) toggleSelect(entry.path);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [focusIndex, filteredEntries, nav]);

  useEffect(() => { setFocusIndex(-1); }, [currentDir, search]);
  useEffect(() => { focusRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' }); }, [focusIndex]);

  const canGoBack = currentDir && currentDir !== rootDir;

  return (
    <div className="flex flex-col gap-2 h-full">
      {/* Toolbar */}
      <div className="flex items-center justify-between gap-2">
        <span className="text-xl font-semibold text-foreground tracking-tight">Blueprints</span>
        {currentDir && (
          <Button
            variant="ghost"
            onClick={() => loadDir(currentDir)}
            title="Refresh (R)"
            className="p-1"
          >
            <RefreshCw size={14} />
          </Button>
        )}
        {canGoBack && (
          <Button variant="ghost" onClick={handleBack}>
            <ChevronLeft size={13} /> Up
          </Button>
        )}
        <Button onClick={() => nav.openWizard()} className="ml-auto bg-blue-500 text-white hover:bg-blue-600">
          <Plus size={14} /> New Blueprint
        </Button>
        {currentDir && (
          <Button onClick={handleNewFolder} className="bg-yellow-500 text-yellow-950 hover:bg-yellow-600">
            <FolderPlus size={14} /> New Folder
          </Button>
        )}
        <Button onClick={handleOpenFolder} className="bg-yellow-500 text-yellow-950 hover:bg-yellow-600">
          <FolderOpen size={14} /> Open Folder
        </Button>
      </div>

      {/* Current path */}
      {currentDir && (
        <div className="text-sm text-muted-foreground font-mono break-all">
          {currentDir}
        </div>
      )}

      {/* Search + Sort */}
      {currentDir && (
        <div className="flex gap-2 items-center">
          <Input
            type="text"
            placeholder="Filter blueprints..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') {
                if (search) { setSearch(''); e.stopPropagation(); }
                else { (e.target as HTMLInputElement).blur(); }
              }
            }}
            className="flex-1 h-10 border-border/60 text-sm placeholder:text-muted-foreground/50 focus-visible:border-blue-500 focus-visible:ring-0 dark:focus-visible:bg-blue-500/10 focus-visible:shadow-[inset_0_0_12px_rgba(59,130,246,0.08),0_0_8px_rgba(59,130,246,0.06)] focus-visible:text-blue-100 transition-all"
          />
          {search && (
            <Button variant="ghost" size="xs" onClick={() => setSearch('')}>
              Clear
            </Button>
          )}
          <div className="flex gap-1">
            {['name-asc', 'name-desc', 'steps-asc', 'steps-desc'].map(key => {
              const labels: Record<string, string> = {
                'name-asc': 'A\u2013Z',
                'name-desc': 'Z\u2013A',
                'steps-asc': 'Simple',
                'steps-desc': 'Complex',
              };
              return (
                <button
                  key={key}
                  onClick={() => setSortBy(key)}
                  className={cn(
                    'h-8 px-3 rounded-md text-xs font-medium transition-colors',
                    'border border-border/60 bg-transparent',
                    'hover:bg-muted/60 hover:text-foreground',
                    sortBy === key ? 'text-blue-400 border-blue-500/40 bg-blue-500/[0.06]' : 'text-muted-foreground',
                  )}
                >
                  {labels[key]}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="text-red-400 text-sm px-3 py-2 bg-red-500/10 rounded border border-red-500/30">
          {error}
        </div>
      )}

      {/* Batch actions bar */}
      {selected.size > 0 && (
        <div className="flex items-center gap-2 px-3 py-1.5 bg-blue-500/[0.08] border border-blue-500/20 rounded">
          <span className="text-sm text-blue-400">
            {selected.size} selected
          </span>
          <Button variant="ghost" size="xs" onClick={selectAllFiles}>
            {selected.size === entries.filter(e => !e.isDir).length ? 'Deselect All' : 'Select All'}
          </Button>
          <Button variant="ghost" size="xs" onClick={handleBatchValidate}>
            Validate All
          </Button>
          <Button
            variant="ghost"
            size="xs"
            onClick={handleBatchRun}
            disabled={daemon.status !== 'running'}
          >
            Run All
          </Button>
          <Button variant="ghost" size="xs" onClick={handleBatchMove}>
            <MoveRight size={11} /> Move
          </Button>
          <Button variant="ghost" size="xs" onClick={handleBatchCopy}>
            <Copy size={11} /> Copy
          </Button>
          <Button variant="ghost" size="xs" onClick={handleBatchArchive}>
            <Archive size={11} /> Archive
          </Button>
          <Button
            variant="ghost"
            size="xs"
            onClick={() => setBatchDeleteConfirm(true)}
            className="text-red-400"
          >
            Delete Selected
          </Button>
          <Button variant="ghost" size="xs" onClick={() => setSelected(new Set())} className="ml-auto">
            Clear
          </Button>
        </div>
      )}

      {/* File list */}
      <div className="flex flex-col gap-px flex-1 overflow-y-auto">
        {!currentDir ? (
          <EmptyState message="No folder selected" sub="Click 'Open Blueprint' to select a .yaml file and browse its folder" />
        ) : filteredEntries.length === 0 ? (
          <EmptyState message={search ? 'No matches' : 'Empty folder'} sub={search ? `No blueprints matching "${search}"` : 'No YAML blueprints found'} />
        ) : (
          filteredEntries.map((entry, i) => {
            const validateResult = validateStates[entry.path];
            const isRunning = runningPaths.has(entry.path);

            return (
              <div
                key={entry.path}
                className={cn(
                  'flex items-start gap-2 py-1 rounded transition-colors',
                  'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
                  'border border-transparent',
                  entry.isDir ? 'cursor-pointer pl-3 pr-3' : 'cursor-default pl-3 pr-3',
                  i === focusIndex && 'bg-blue-500/[0.06] border-blue-500/30',
                )}
                ref={i === focusIndex ? focusRef : undefined}
                onClick={() => entry.isDir ? handleDrillDown(entry) : undefined}
              >
                {/* Checkbox — fixed-width column, negative margin to pull left */}
                {!entry.isDir ? (
                  <div className="-ml-2 w-5 shrink-0 flex items-start pt-px" onClick={e => e.stopPropagation()}>
                    <Checkbox
                      checked={selected.has(entry.path)}
                      onCheckedChange={() => toggleSelect(entry.path)}
                    />
                  </div>
                ) : (
                  <div className="-ml-2 w-5 shrink-0" />
                )}

                {/* Icon — same line as first text row */}
                <span className={cn('shrink-0 mt-px', entry.isDir ? 'text-yellow-500' : 'text-blue-400')}>
                  {entry.isDir ? <Folder size={15} /> : <FileCode size={15} />}
                </span>

                {/* Name + metadata — takes ~85% leaving room for actions */}
                <div
                  className={cn(
                    'min-w-0 flex-1 max-w-[85%] -mt-[3px]',
                    entry.isDir ? 'text-foreground' : 'cursor-pointer',
                  )}
                  onClick={() => { if (!entry.isDir) nav.openBlueprint(entry.path); }}
                >
                  <div className="flex items-center gap-2">
                    <span className="leading-tight">
                      {entry.name}
                      {entry.isDir && <span className="text-muted-foreground text-sm ml-1">/</span>}
                    </span>
                    {/* Tags inline with name */}
                    {!entry.isDir && metaCache[entry.path]?.tags && metaCache[entry.path].tags.length > 0 && (
                      <span className="flex items-center gap-1 shrink-0">
                        {metaCache[entry.path].tags.slice(0, 4).map(t => (
                          <span key={t} className="px-1.5 py-0.5 rounded text-xs bg-muted text-muted-foreground">{t}</span>
                        ))}
                        {metaCache[entry.path].tags.length > 4 && <span className="px-1.5 py-0.5 rounded text-xs bg-muted text-muted-foreground">+{metaCache[entry.path].tags.length - 4}</span>}
                      </span>
                    )}
                  </div>
                  {!entry.isDir && metaCache[entry.path]?.description && (
                    <div className="text-xs text-muted-foreground mt-0.5 line-clamp-2">
                      {metaCache[entry.path].description}
                    </div>
                  )}
                </div>

                {/* Validate result badge */}
                {!entry.isDir && validateResult && (
                  <span className={cn('text-xs tracking-wide shrink-0', validateResult.valid ? 'text-blue-400' : 'text-red-400')}>
                    {validateResult.valid ? '✓ valid' : `✗ ${validateResult.error ?? 'invalid'}`}
                  </span>
                )}

                {/* Actions — pushed right */}
                {!entry.isDir && (
                  <div className="flex items-center gap-1 shrink-0 ml-auto" onClick={e => e.stopPropagation()}>
                    <Button
                      size="xs"
                      onClick={() => handleRun(entry)}
                      disabled={daemon.status !== 'running' || isRunning}
                    >
                      <Play size={12} /> {isRunning ? 'Running…' : 'Run'}
                    </Button>
                    <DropdownMenu>
                      <DropdownMenuTrigger className="inline-flex items-center justify-center h-7 px-2 rounded-md text-xs font-medium bg-muted text-muted-foreground hover:bg-muted/80 hover:text-foreground transition-colors">
                        <MoreHorizontal size={14} />
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end">
                        <DropdownMenuItem onClick={() => handleValidate(entry)}>
                          <CheckCircle size={12} /> Validate
                        </DropdownMenuItem>
                        <DropdownMenuItem onClick={() => setDeleteConfirm(entry.path)} className="text-red-400 focus:text-red-400">
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

      {/* Inputs modal */}
      {inputModalEntry && (
        <RunInputsModal
          entry={inputModalEntry}
          inputs={inputModalInputs}
          onConfirm={(values, dryRun) => {
            const entry = inputModalEntry;
            setInputModalEntry(null);
            doEnqueue(entry, values, dryRun);
          }}
          onCancel={() => setInputModalEntry(null)}
        />
      )}

      {/* Batch delete confirmation */}
      <AlertDialog open={batchDeleteConfirm} onOpenChange={(open) => { if (!open) setBatchDeleteConfirm(false); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete {selected.size} Blueprints</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete {selected.size} selected blueprint{selected.size !== 1 ? 's' : ''}? This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={handleBatchDelete}>Delete {selected.size}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Run confirmation (requireConfirmation setting) */}
      <AlertDialog open={!!confirmEntry} onOpenChange={(open) => { if (!open) setConfirmEntry(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Run blueprint?</AlertDialogTitle>
            <AlertDialogDescription>
              Execute <span className="font-mono text-blue-400">
                {confirmEntry?.name}
              </span> with no inputs?
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={() => { const e = confirmEntry; setConfirmEntry(null); if (e) doEnqueue(e, {}); }}>Run</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Delete confirmation modal */}
      <AlertDialog open={!!deleteConfirm} onOpenChange={(open) => { if (!open) setDeleteConfirm(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Blueprint</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete this blueprint? This cannot be undone.
              <br />
              <span className="font-mono text-sm break-all">
                {deleteConfirm}
              </span>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={() => { if (deleteConfirm) handleDelete(deleteConfirm); }}>Delete</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
