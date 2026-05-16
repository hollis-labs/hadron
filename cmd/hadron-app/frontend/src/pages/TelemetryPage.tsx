import { useState, useEffect, useCallback, Component, type ReactNode } from 'react';
import { toast } from 'sonner';
import { RefreshCw, MoreHorizontal, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { cn } from '@/lib/utils';
import { listTelemetryRuns, readTelemetryLog, deleteTelemetryLog } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import type { TelemetryRunSummary, TelemetryLogEntry } from '../api/types';
import { useAsyncResource } from '@/hooks/useAsyncResource';

// Error boundary to prevent page from crashing the whole app
class TelemetryErrorBoundary extends Component<{ children: ReactNode; onRetry: () => void }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) { return { error }; }
  render() {
    if (this.state.error) {
      return (
        <div className="p-8 text-center">
          <div className="text-red-400 mb-2 font-semibold">
            Activity Log encountered an error
          </div>
          <div className="font-mono text-sm text-muted-foreground mb-4">
            {this.state.error.message}
          </div>
          <Button onClick={() => { this.setState({ error: null }); this.props.onRetry(); }}>
            Retry
          </Button>
        </div>
      );
    }
    return this.props.children;
  }
}

import { useNavigation } from '../contexts/NavigationContext';

const LEVEL_COLORS: Record<string, string> = {
  info: 'text-primary',
  warn: 'text-amber-400',
  error: 'text-red-400',
  debug: 'text-muted-foreground',
};

const LEVEL_FILTERS = ['all', 'info', 'warn', 'error', 'debug'] as const;
type LevelFilter = typeof LEVEL_FILTERS[number];

import { formatFileSize, formatDate, formatTimestamp } from '../utils/format';

export function TelemetryPage() {
  const [retryKey, setRetryKey] = useState(0);
  return (
    <TelemetryErrorBoundary onRetry={() => setRetryKey(k => k + 1)} key={retryKey}>
      <TelemetryPageInner />
    </TelemetryErrorBoundary>
  );
}

function TelemetryPageInner() {
  const nav = useNavigation();
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [levelFilter, setLevelFilter] = useState<LevelFilter>('all');
  const [search, setSearch] = useState('');
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null);

  const fetchRuns = useCallback(() => listTelemetryRuns(), []);
  const {
    data: loadedRuns,
    loading,
    error: runsError,
    refresh: refreshRuns,
  } = useAsyncResource<TelemetryRunSummary[]>(fetchRuns);
  const runs = loadedRuns ?? [];

  const fetchEntries = useCallback(async () => {
    if (!selectedRunId) return [];
    return readTelemetryLog(selectedRunId);
  }, [selectedRunId]);
  const {
    data: loadedEntries,
    loading: entriesLoading,
    error: entriesError,
    refresh: refreshEntries,
    setData: setEntries,
  } = useAsyncResource<TelemetryLogEntry[]>(fetchEntries, { enabled: selectedRunId !== null });
  const entries = loadedEntries ?? [];

  // Surface load failures — including the initial fetch — instead of leaving
  // the user with a silently empty list. The hook's `error` is a fresh object
  // per failure, so each failed load (initial or refresh) toasts once.
  useEffect(() => {
    if (runsError) toast.error('Failed to load telemetry runs');
  }, [runsError]);
  useEffect(() => {
    if (entriesError) toast.error('Failed to load log');
  }, [entriesError]);

  const reloadRuns = useCallback(() => refreshRuns(), [refreshRuns]);

  const reloadEntries = useCallback(async () => {
    if (!selectedRunId) return null;
    return refreshEntries();
  }, [refreshEntries, selectedRunId]);

  // Listen for global refresh shortcut
  useEffect(() => {
    const handler = () => {
      void reloadRuns();
      if (selectedRunId) void reloadEntries();
    };
    window.addEventListener('hadron:refresh', handler);
    return () => window.removeEventListener('hadron:refresh', handler);
  }, [reloadRuns, reloadEntries, selectedRunId]);

  const handleSelectRun = (runId: string) => {
    setSelectedRunId(runId);
    setLevelFilter('all');
    setSearch('');
  };

  const handleBack = () => {
    setSelectedRunId(null);
    setEntries([]);
  };

  const handleDelete = async (runId: string) => {
    try {
      await deleteTelemetryLog(runId);
      toast.success('Log deleted');
      setDeleteConfirm(null);
      if (selectedRunId === runId) handleBack();
      void reloadRuns();
    } catch (err) {
      toast.error(`Delete failed: ${err}`);
    }
  };

  const filteredEntries = entries.filter(e => {
    if (!e) return false;
    if (levelFilter !== 'all' && (e.level ?? '') !== levelFilter) return false;
    if (search) {
      const q = search.toLowerCase();
      if (
        !(e.event ?? '').toLowerCase().includes(q) &&
        !(e.msg ?? '').toLowerCase().includes(q) &&
        !(e.section ?? '').toLowerCase().includes(q) &&
        !(e.step ?? '').toLowerCase().includes(q)
      ) return false;
    }
    return true;
  });

  // Level counts for badges
  const levelCounts = entries.reduce((acc, e) => {
    if (!e) return acc;
    const lvl = e.level ?? 'unknown';
    acc[lvl] = (acc[lvl] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  // ── Detail view (log entries for selected run) ──
  if (selectedRunId) {
    return (
      <div className="flex flex-col gap-2 h-full">
        <div className="flex items-center justify-between">
          <div className="text-xl font-semibold text-foreground tracking-tight">Run {selectedRunId.slice(-8)}</div>
          <div className="flex gap-2">
            <Button variant="ghost" onClick={handleBack}>Back</Button>
            <Button variant="outline" onClick={() => nav.openRun(selectedRunId)}>View Run</Button>
          </div>
        </div>

        {/* Search + Level filters */}
        <div className="flex items-center gap-2">
          <Input
            type="text"
            placeholder="Search events, messages, steps..."
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
            <Button variant="ghost" size="xs" onClick={() => setSearch('')}>Clear</Button>
          )}
          <div className="flex gap-1">
            {LEVEL_FILTERS.map(lf => {
              const count = lf === 'all' ? entries.length : (levelCounts[lf] || 0);
              const isActive = levelFilter === lf;
              const chipColor = lf !== 'all' && isActive ? ({
                info: 'text-blue-400 border-blue-500/40 bg-blue-500/[0.06]',
                warn: 'text-amber-400 border-amber-500/40 bg-amber-500/[0.06]',
                error: 'text-red-400 border-red-500/40 bg-red-500/[0.06]',
                debug: 'text-muted-foreground border-border/60 bg-muted/30',
              }[lf]) : undefined;
              return (
                <button
                  key={lf}
                  onClick={() => setLevelFilter(lf)}
                  className={cn(
                    'h-8 px-3 rounded-md text-xs font-medium transition-colors',
                    'border border-border/60 bg-transparent',
                    'hover:bg-muted/60 hover:text-foreground',
                    isActive
                      ? chipColor || 'text-blue-400 border-blue-500/40 bg-blue-500/[0.06]'
                      : 'text-muted-foreground',
                  )}
                >
                  {lf === 'all' ? 'All' : lf.charAt(0).toUpperCase() + lf.slice(1)}
                  {count > 0 && <span className="opacity-60 ml-1">({count})</span>}
                </button>
              );
            })}
          </div>
        </div>

        {/* Log entries */}
        <div className="flex flex-col gap-px flex-1 overflow-y-auto">
          {entriesLoading ? (
            <EmptyState message="Loading log entries..." />
          ) : filteredEntries.length === 0 ? (
            <EmptyState
              message={entries.length === 0 ? 'No log entries' : 'No matching entries'}
              sub={entries.length > 0 ? `${entries.length} entries total` : undefined}
            />
          ) : (
            filteredEntries.map((entry, i) => (
              <div
                key={i}
                className={cn(
                  'flex items-start gap-3 px-3 py-1 rounded transition-colors',
                  'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
                  'border border-transparent',
                )}
              >
                <span className="shrink-0 font-mono text-xs text-muted-foreground mt-0.5 w-28 whitespace-nowrap mr-2">{formatTimestamp(entry.ts)}</span>
                <span className={cn('shrink-0 text-xs font-semibold uppercase tracking-wider w-10', LEVEL_COLORS[entry.level] ?? 'text-foreground')}>
                  {entry.level}
                </span>
                <span className="shrink-0 font-mono text-xs text-primary w-24 truncate">{entry.event}</span>
                <span className="shrink-0 text-xs text-muted-foreground w-20 truncate">{entry.section || '—'}</span>
                <span className="shrink-0 text-xs text-muted-foreground w-20 truncate">{entry.step || '—'}</span>
                <span className="flex-1 text-xs break-words min-w-0">{entry.msg || '—'}</span>
              </div>
            ))
          )}
        </div>

        {entries.length > 0 && (
          <p className="text-xs text-muted-foreground">
            Showing {filteredEntries.length} of {entries.length} entries
          </p>
        )}
      </div>
    );
  }

  // ── List view (telemetry run files) ──
  return (
    <div className="flex flex-col gap-2 h-full">
      <div className="flex items-center justify-between">
        <div>
          <div className="text-xl font-semibold text-foreground tracking-tight">Telemetry</div>
          {loading && <div className="text-sm text-muted-foreground mt-0.5">Loading…</div>}
        </div>
        <Button variant="ghost" onClick={() => void reloadRuns()} title="Refresh (R)">
          <RefreshCw size={14} />
        </Button>
      </div>

      <div className="flex flex-col gap-px flex-1 overflow-y-auto">
        {runs.length === 0 ? (
          <EmptyState
            message="No telemetry logs"
            sub="Run a blueprint to generate telemetry data"
          />
        ) : (
          runs.map(run => (
            <div
              key={run.run_id}
              onClick={() => handleSelectRun(run.run_id)}
              className={cn(
                'flex items-center gap-3 px-3 py-1.5 rounded cursor-pointer transition-colors',
                'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
                'border border-transparent',
              )}
            >
              <div className="flex-1 min-w-0 font-mono text-sm">{run.run_id.slice(-12)}</div>
              <div className="shrink-0 text-sm text-primary w-12 text-right">{run.event_count}</div>
              <div className="shrink-0 text-sm text-muted-foreground w-16 text-right">{formatFileSize(run.file_size)}</div>
              <div className="shrink-0 text-sm text-muted-foreground w-36 text-right">{formatDate(run.modified_at)}</div>
              <DropdownMenu>
                <DropdownMenuTrigger className="inline-flex items-center justify-center h-7 px-2 rounded-md text-xs font-medium bg-muted text-muted-foreground hover:bg-muted/80 hover:text-foreground transition-colors shrink-0" onClick={e => e.stopPropagation()}>
                  <MoreHorizontal size={14} />
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onClick={(e) => { e.stopPropagation(); setDeleteConfirm(run.run_id); }} className="text-red-400 focus:text-red-400">
                    <Trash2 size={12} /> Delete
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          ))
        )}
      </div>

      {runs.length > 0 && (
        <p className="text-xs text-muted-foreground">
          {runs.length} log file{runs.length !== 1 ? 's' : ''} · {formatFileSize(runs.reduce((s, r) => s + r.file_size, 0))} total
        </p>
      )}

      <AlertDialog open={!!deleteConfirm} onOpenChange={(open) => { if (!open) setDeleteConfirm(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete telemetry log?</AlertDialogTitle>
            <AlertDialogDescription>
              {deleteConfirm ? `This will permanently delete the log file for run ${deleteConfirm.slice(-12)}.` : ''}
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
