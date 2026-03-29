import { useState, useEffect, useCallback, Component, type ReactNode } from 'react';
import { toast } from 'sonner';
import { RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import { listTelemetryRuns, readTelemetryLog, deleteTelemetryLog } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import type { TelemetryRunSummary, TelemetryLogEntry } from '../api/types';

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

const LEVEL_BORDER_COLORS: Record<string, string> = {
  info: 'border-primary text-primary',
  warn: 'border-amber-400 text-amber-400',
  error: 'border-red-400 text-red-400',
  debug: 'border-muted-foreground text-muted-foreground',
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
  const [runs, setRuns] = useState<TelemetryRunSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [entries, setEntries] = useState<TelemetryLogEntry[]>([]);
  const [entriesLoading, setEntriesLoading] = useState(false);
  const [levelFilter, setLevelFilter] = useState<LevelFilter>('all');
  const [search, setSearch] = useState('');
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null);

  const fetchRuns = useCallback(async () => {
    setLoading(true);
    try {
      const data = await listTelemetryRuns();
      setRuns(data);
    } catch (err) {
      toast.error(`Failed to load telemetry runs: ${err}`);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchRuns(); }, [fetchRuns]);

  // Listen for global refresh shortcut
  useEffect(() => {
    const handler = () => {
      fetchRuns();
      if (selectedRunId) loadEntries(selectedRunId);
    };
    window.addEventListener('hadron:refresh', handler);
    return () => window.removeEventListener('hadron:refresh', handler);
  }, [fetchRuns, selectedRunId]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadEntries = async (runId: string) => {
    setEntriesLoading(true);
    try {
      const data = await readTelemetryLog(runId);
      setEntries(data);
    } catch (err) {
      toast.error(`Failed to load log: ${err}`);
      setEntries([]);
    } finally {
      setEntriesLoading(false);
    }
  };

  const handleSelectRun = (runId: string) => {
    setSelectedRunId(runId);
    setLevelFilter('all');
    setSearch('');
    loadEntries(runId);
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
      fetchRuns();
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
      <div>
        <div className="flex items-center justify-between mb-6">
          <div>
            <div className="text-xl font-semibold text-foreground tracking-tight">Run {selectedRunId.slice(-8)}</div>
          </div>
          <div className="flex gap-2">
            <Button variant="ghost" onClick={handleBack}>Back</Button>
            <Button variant="outline" onClick={() => nav.openRun(selectedRunId)}>View Run</Button>
          </div>
        </div>

        {/* Level filters */}
        <div className="flex gap-3 mb-4 flex-wrap items-center">
          <div className="flex gap-1">
            {LEVEL_FILTERS.map(lf => {
              const count = lf === 'all' ? entries.length : (levelCounts[lf] || 0);
              const isActive = levelFilter === lf;
              return (
                <Button
                  key={lf}
                  variant={isActive ? "outline" : "ghost"}
                  size="xs"
                  onClick={() => setLevelFilter(lf)}
                  className={cn(isActive && lf !== 'all' && LEVEL_BORDER_COLORS[lf])}
                >
                  {lf === 'all' ? 'All' : lf.charAt(0).toUpperCase() + lf.slice(1)}
                  {count > 0 && <span className="opacity-60">({count})</span>}
                </Button>
              );
            })}
          </div>
          <Input
            type="text"
            placeholder="Search events, messages, steps..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="flex-1 min-w-[180px]"
          />
          {search && (
            <Button variant="ghost" size="xs" onClick={() => setSearch('')}>Clear</Button>
          )}
        </div>

        {/* Log entries table */}
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          {entriesLoading ? (
            <EmptyState message="Loading log entries..." />
          ) : filteredEntries.length === 0 ? (
            <EmptyState
              message={entries.length === 0 ? 'No log entries' : 'No matching entries'}
              sub={entries.length > 0 ? `${entries.length} entries total` : undefined}
            />
          ) : (
            <table className="w-full border-collapse">
              <thead>
                <tr>
                  <th className="col-shrink">Time</th>
                  <th className="col-shrink">Level</th>
                  <th className="col-shrink">Event</th>
                  <th className="col-shrink">Section</th>
                  <th className="col-shrink">Step</th>
                  <th className="col-primary">Message</th>
                </tr>
              </thead>
              <tbody>
                {filteredEntries.map((entry, i) => (
                  <tr key={i} className="cursor-default">
                    <td className="font-mono text-muted-foreground">{formatTimestamp(entry.ts)}</td>
                    <td>
                      <span className={cn(
                        'text-xs font-semibold uppercase tracking-wider',
                        LEVEL_COLORS[entry.level] ?? 'text-foreground'
                      )}>
                        {entry.level}
                      </span>
                    </td>
                    <td className="font-mono text-primary">{entry.event}</td>
                    <td className="text-muted-foreground">{entry.section || '—'}</td>
                    <td className="text-muted-foreground">{entry.step || '—'}</td>
                    <td className="break-words">{entry.msg || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {entries.length > 0 && (
          <div className="text-xs text-muted-foreground mt-2">
            Showing {filteredEntries.length} of {entries.length} entries
          </div>
        )}
      </div>
    );
  }

  // ── List view (telemetry run files) ──
  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <div className="text-xl font-semibold text-foreground tracking-tight">Telemetry</div>
          {loading && <div className="text-sm text-muted-foreground mt-0.5">Loading…</div>}
        </div>
        <Button variant="ghost" onClick={fetchRuns} title="Refresh (R)">
          <RefreshCw size={14} />
        </Button>
      </div>

      <div className="rounded-lg border border-border bg-card overflow-hidden">
        {runs.length === 0 ? (
          <EmptyState
            message="No telemetry logs"
            sub="Run a blueprint to generate telemetry data"
          />
        ) : (
          <table className="w-full border-collapse">
            <thead>
              <tr>
                <th className="col-primary">Run ID</th>
                <th className="col-shrink col-right">Events</th>
                <th className="col-shrink col-right">Size</th>
                <th className="col-shrink col-right">Modified</th>
                <th className="col-shrink"></th>
              </tr>
            </thead>
            <tbody>
              {runs.map(run => (
                <tr key={run.run_id} onClick={() => handleSelectRun(run.run_id)}>
                  <td className="font-mono col-primary">{run.run_id.slice(-12)}</td>
                  <td className="col-shrink col-right"><span className="text-primary">{run.event_count}</span></td>
                  <td className="col-shrink col-right text-muted-foreground">{formatFileSize(run.file_size)}</td>
                  <td className="text-muted-foreground">{formatDate(run.modified_at)}</td>
                  <td>
                    <Button
                      variant="ghost"
                      size="xs"
                      onClick={(e) => { e.stopPropagation(); setDeleteConfirm(run.run_id); }}
                      className="text-red-400"
                    >
                      Del
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {runs.length > 0 && (
        <div className="text-xs text-muted-foreground mt-2">
          {runs.length} log file{runs.length !== 1 ? 's' : ''} · {formatFileSize(runs.reduce((s, r) => s + r.file_size, 0))} total
        </div>
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
