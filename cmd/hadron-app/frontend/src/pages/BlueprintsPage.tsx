import { useState, useEffect, useCallback, useRef } from 'react';
import { toast } from 'sonner';
import { openDirectoryDialog, listFilesInDir, validateBlueprintFile, enqueueRun, parseBlueprintInputs, deleteBlueprintFile, getBlueprintMetadata, getSettings, createDirectory, selectDirectoryDialog, moveBlueprintFile, copyBlueprintFile, archiveBlueprintFile, getBlueprintDir, setBlueprintDir } from '../api/client';
import { ValidateResult, FileEntry, BlueprintInput, BlueprintMetaSummary } from '../api/types';
import { FolderOpen, FolderPlus, Play, CheckCircle, ChevronLeft, Folder, FileCode, Plus, Trash2, RefreshCw, MoveRight, Copy, Archive } from 'lucide-react';
import { EmptyState } from '../components/ui/EmptyState';
import { RunInputsModal } from '../components/ui/RunInputsModal';

interface BlueprintsPageProps {
  daemonStatus: string;
  workspaceId: string;
  onRunCreated: (runId: string) => void;
  onOpenBlueprint?: (path: string) => void;
  onNewBlueprint?: () => void;
  onBatchRunComplete?: () => void;
}

export function BlueprintsPage({ daemonStatus, workspaceId, onRunCreated, onOpenBlueprint, onNewBlueprint, onBatchRunComplete }: BlueprintsPageProps) {
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
      const items = await listFilesInDir(dir);
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
      const run = await enqueueRun({ blueprint_path: entry.path, workspace_id: workspaceId, inputs, dry_run: dryRun || undefined });
      toast.success(dryRun ? 'Dry run enqueued' : 'Run enqueued');
      onRunCreated(run.id);
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
    if (daemonStatus !== 'running') return;
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
        await enqueueRun({ blueprint_path: path, workspace_id: workspaceId });
        ok++;
      } catch { /* skip failures */ }
    }
    toast.success(`Enqueued ${ok} of ${paths.length} runs`);
    setSelected(new Set());
    if (ok > 0 && onBatchRunComplete) {
      onBatchRunComplete();
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
        else if (onOpenBlueprint) onOpenBlueprint(entry.path);
      } else if (e.key === ' ' && focusIndex >= 0 && focusIndex < count) {
        e.preventDefault();
        const entry = filteredEntries[focusIndex];
        if (!entry.isDir) toggleSelect(entry.path);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [focusIndex, filteredEntries, onOpenBlueprint]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => { setFocusIndex(-1); }, [currentDir, search]);
  useEffect(() => { focusRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' }); }, [focusIndex]);

  const canGoBack = currentDir && currentDir !== rootDir;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem', height: '100%' }}>
      {/* Toolbar */}
      <div className="page-header" style={{ gap: '0.5rem' }}>
        <span className="page-title">Blueprints</span>
        {currentDir && (
          <button
            className="btn btn-ghost"
            onClick={() => loadDir(currentDir)}
            title="Refresh (R)"
            style={{ display: 'flex', alignItems: 'center', padding: '0.25rem' }}
          >
            <RefreshCw size={14} />
          </button>
        )}
        {canGoBack && (
          <button className="btn btn-ghost" onClick={handleBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
            <ChevronLeft size={13} /> Up
          </button>
        )}
        {onNewBlueprint && (
          <button className="btn btn-primary" onClick={onNewBlueprint} style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: '0.4rem', borderColor: 'rgba(59, 130, 246, 0.5)', color: 'var(--status-success)' }}>
            <Plus size={14} /> New Blueprint
          </button>
        )}
        {currentDir && (
          <button className="btn btn-ghost" onClick={handleNewFolder} style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <FolderPlus size={14} /> New Folder
          </button>
        )}
        <button className="btn btn-primary" onClick={handleOpenFolder} style={{ marginLeft: (!onNewBlueprint && !currentDir) ? 'auto' : undefined, display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
          <FolderOpen size={14} /> Open Folder
        </button>
      </div>

      {/* Current path */}
      {currentDir && (
        <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', fontFamily: 'monospace', wordBreak: 'break-all' }}>
          {currentDir}
        </div>
      )}

      {/* Search + Sort */}
      {currentDir && (
        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
          <input
            className="hud-input"
            type="text"
            placeholder="Filter blueprints..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ flex: 1 }}
          />
          {search && (
            <button className="btn btn-ghost" onClick={() => setSearch('')} style={{ padding: '0.3rem 0.5rem', fontSize: 'var(--text-xs)' }}>
              Clear
            </button>
          )}
          <select
            className="hud-input"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            style={{ width: 'auto' }}
          >
            <option value="name-asc">Name A-Z</option>
            <option value="name-desc">Name Z-A</option>
          </select>
          {entries.some(e => !e.isDir) && (
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', cursor: 'pointer', fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)' }}>
              <input
                type="checkbox"
                checked={selected.size > 0 && selected.size === entries.filter(e => !e.isDir).length}
                onChange={selectAllFiles}
                style={{ accentColor: 'var(--status-success)' }}
              />
              All
            </label>
          )}
        </div>
      )}

      {/* Error */}
      {error && (
        <div style={{ color: 'var(--status-failed)', fontSize: 'var(--text-md)', padding: '0.5rem 0.75rem', background: 'var(--status-failed-bg)', borderRadius: '4px', border: '1px solid rgba(239, 68, 68, 0.3)' }}>
          {error}
        </div>
      )}

      {/* Batch actions bar */}
      {selected.size > 0 && (
        <div style={{
          display: 'flex', alignItems: 'center', gap: '0.5rem',
          padding: '0.4rem 0.75rem', background: 'rgba(59, 130, 246, 0.08)',
          border: '1px solid rgba(var(--ok) / 0.2)', borderRadius: '4px',
        }}>
          <span style={{ fontSize: 'var(--text-sm)', color: 'var(--status-success)' }}>
            {selected.size} selected
          </span>
          <button className="btn btn-ghost" onClick={handleBatchValidate} style={{ fontSize: 'var(--text-xs)' }}>
            Validate All
          </button>
          <button
            className="btn btn-ghost"
            onClick={handleBatchRun}
            disabled={daemonStatus !== 'running'}
            style={{ fontSize: 'var(--text-xs)' }}
          >
            Run All
          </button>
          <button className="btn btn-ghost" onClick={handleBatchMove} style={{ fontSize: 'var(--text-xs)', display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <MoveRight size={11} /> Move
          </button>
          <button className="btn btn-ghost" onClick={handleBatchCopy} style={{ fontSize: 'var(--text-xs)', display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <Copy size={11} /> Copy
          </button>
          <button className="btn btn-ghost" onClick={handleBatchArchive} style={{ fontSize: 'var(--text-xs)', display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
            <Archive size={11} /> Archive
          </button>
          <button
            className="btn btn-ghost"
            onClick={() => setBatchDeleteConfirm(true)}
            style={{ fontSize: 'var(--text-xs)', color: 'var(--status-failed)' }}
          >
            Delete Selected
          </button>
          <button className="btn btn-ghost" onClick={() => setSelected(new Set())} style={{ fontSize: 'var(--text-xs)', marginLeft: 'auto' }}>
            Clear
          </button>
        </div>
      )}

      {/* File list */}
      <div className="file-list" style={{ flex: 1, overflowY: 'auto' }}>
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
                className="file-row"
                ref={i === focusIndex ? focusRef : undefined}
                onClick={() => entry.isDir ? handleDrillDown(entry) : undefined}
                style={{
                  cursor: entry.isDir ? 'pointer' : 'default',
                  ...(i === focusIndex ? { background: 'var(--bg-hover)', outline: '1px solid rgba(var(--accent), 0.3)' } : {}),
                }}
              >
                {/* Checkbox (files only) */}
                {!entry.isDir && (
                  <input
                    type="checkbox"
                    checked={selected.has(entry.path)}
                    onChange={() => toggleSelect(entry.path)}
                    onClick={e => e.stopPropagation()}
                    style={{ accentColor: 'var(--status-success)', flexShrink: 0, cursor: 'pointer' }}
                  />
                )}

                {/* Icon */}
                <span style={{ color: entry.isDir ? 'var(--status-running)' : 'var(--status-success)', flexShrink: 0 }}>
                  {entry.isDir ? <Folder size={15} /> : <FileCode size={15} />}
                </span>

                {/* Name + metadata */}
                <div
                  className="file-row-name"
                  style={{ color: entry.isDir ? 'var(--text-primary)' : undefined, cursor: !entry.isDir ? 'pointer' : undefined }}
                  onClick={() => { if (!entry.isDir && onOpenBlueprint) onOpenBlueprint(entry.path); }}
                >
                  <span>
                    {entry.name}
                    {entry.isDir && <span style={{ color: 'var(--text-tertiary)', fontSize: 'var(--text-sm)', marginLeft: '0.3rem' }}>/</span>}
                  </span>
                  {!entry.isDir && metaCache[entry.path] && (() => {
                    const meta = metaCache[entry.path];
                    return (
                      <div className="file-row-meta">
                        {meta.description && (
                          <span className="file-row-meta-desc">
                            {meta.description.length > 80 ? meta.description.slice(0, 80) + '…' : meta.description}
                          </span>
                        )}
                        {meta.tags && meta.tags.length > 0 && (
                          <span className="file-row-meta-tags">
                            {meta.tags.slice(0, 4).map(t => (
                              <span key={t} className="file-row-meta-tag">{t}</span>
                            ))}
                            {meta.tags.length > 4 && <span className="file-row-meta-tag">+{meta.tags.length - 4}</span>}
                          </span>
                        )}
                        <span className="file-row-meta-counts">
                          {meta.input_count > 0 && <span>{meta.input_count} input{meta.input_count !== 1 ? 's' : ''}</span>}
                          {meta.step_count > 0 && <span>{meta.step_count} step{meta.step_count !== 1 ? 's' : ''}</span>}
                          {meta.has_imports && <span>imports</span>}
                        </span>
                      </div>
                    );
                  })()}
                </div>

                {/* Validate result badge */}
                {!entry.isDir && validateResult && (
                  <span style={{
                    fontSize: 'var(--text-xs)',
                    color: validateResult.valid ? 'var(--status-success)' : 'var(--status-failed)',
                    letterSpacing: '0.06em',
                    flexShrink: 0,
                  }}>
                    {validateResult.valid ? '✓ valid' : `✗ ${validateResult.error ?? 'invalid'}`}
                  </span>
                )}

                {/* Actions */}
                {!entry.isDir && (
                  <div className="file-row-actions" onClick={e => e.stopPropagation()}>
                    <button
                      className="btn btn-ghost"
                      onClick={() => handleValidate(entry)}
                      style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: 'var(--text-xs)' }}
                    >
                      <CheckCircle size={12} /> Validate
                    </button>
                    <button
                      className="btn btn-primary"
                      onClick={() => handleRun(entry)}
                      disabled={daemonStatus !== 'running' || isRunning}
                      style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', fontSize: 'var(--text-xs)' }}
                    >
                      <Play size={12} /> {isRunning ? 'Running…' : 'Run'}
                    </button>
                    <button
                      className="btn btn-ghost"
                      onClick={() => setDeleteConfirm(entry.path)}
                      style={{ padding: '0.3rem', display: 'flex', alignItems: 'center' }}
                    >
                      <Trash2 size={12} style={{ color: 'var(--status-failed)' }} />
                    </button>
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
      {batchDeleteConfirm && (
        <div className="hud-modal-overlay" onClick={() => setBatchDeleteConfirm(false)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()}>
            <h3 style={{ fontSize: 'var(--text-base)', marginBottom: '0.5rem' }}>Delete {selected.size} Blueprints</h3>
            <p style={{ fontSize: 'var(--text-md)', color: 'var(--text-tertiary)', marginBottom: '0.5rem' }}>
              Are you sure you want to delete {selected.size} selected blueprint{selected.size !== 1 ? 's' : ''}? This cannot be undone.
            </p>
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button className="btn btn-ghost" onClick={() => setBatchDeleteConfirm(false)}>Cancel</button>
              <button
                className="btn btn-primary"
                style={{ borderColor: 'var(--status-failed)', color: 'var(--status-failed)' }}
                onClick={handleBatchDelete}
              >
                Delete {selected.size}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Run confirmation (requireConfirmation setting) */}
      {confirmEntry && (
        <div className="hud-modal-overlay" onClick={() => setConfirmEntry(null)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '400px' }}>
            <div style={{ padding: '1.25rem' }}>
              <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Run blueprint?</div>
              <div style={{ fontSize: 'var(--text-md)', color: 'var(--text-tertiary)', marginBottom: '1rem' }}>
                Execute <span style={{ fontFamily: 'monospace', color: 'var(--accent)' }}>
                  {confirmEntry.name}
                </span> with no inputs?
              </div>
              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                <button className="btn btn-ghost" onClick={() => setConfirmEntry(null)}>Cancel</button>
                <button className="btn btn-primary" style={{ borderColor: 'rgba(59, 130, 246, 0.5)', color: 'var(--status-success)' }}
                  onClick={() => { const e = confirmEntry; setConfirmEntry(null); doEnqueue(e, {}); }}>
                  Run
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Delete confirmation modal */}
      {deleteConfirm && (
        <div className="hud-modal-overlay" onClick={() => setDeleteConfirm(null)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()}>
            <h3 style={{ fontSize: 'var(--text-base)', marginBottom: '0.5rem' }}>Delete Blueprint</h3>
            <p style={{ fontSize: 'var(--text-md)', color: 'var(--text-tertiary)', marginBottom: '0.5rem' }}>
              Are you sure you want to delete this blueprint? This cannot be undone.
            </p>
            <p style={{ fontFamily: 'monospace', fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', wordBreak: 'break-all' }}>
              {deleteConfirm}
            </p>
            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
              <button className="btn btn-ghost" onClick={() => setDeleteConfirm(null)}>Cancel</button>
              <button
                className="btn btn-primary"
                style={{ borderColor: 'var(--status-failed)', color: 'var(--status-failed)' }}
                onClick={() => handleDelete(deleteConfirm)}
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
