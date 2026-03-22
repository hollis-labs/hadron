import { useState, useEffect, useCallback, Component, type ReactNode } from 'react';
import { toast } from 'sonner';
import { RefreshCw } from 'lucide-react';
import { listTelemetryRuns, readTelemetryLog, deleteTelemetryLog } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import { ConfirmDialog } from '../components/ui/ConfirmDialog';
import type { TelemetryRunSummary, TelemetryLogEntry } from '../api/types';

// Error boundary to prevent page from crashing the whole app
class TelemetryErrorBoundary extends Component<{ children: ReactNode; onRetry: () => void }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) { return { error }; }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 'var(--space-8)', textAlign: 'center' }}>
          <div style={{ color: 'var(--status-failed)', marginBottom: 'var(--space-2)', fontWeight: 600 }}>
            Activity Log encountered an error
          </div>
          <div className="mono" style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginBottom: 'var(--space-4)' }}>
            {this.state.error.message}
          </div>
          <button className="btn btn-primary" onClick={() => { this.setState({ error: null }); this.props.onRetry(); }}>
            Retry
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

interface TelemetryPageProps {
  onOpenRun: (runId: string) => void;
}

const LEVEL_COLORS: Record<string, string> = {
  info: 'var(--accent)',
  warn: 'var(--status-running)',
  error: 'var(--status-failed)',
  debug: 'var(--text-tertiary)',
};

const LEVEL_FILTERS = ['all', 'info', 'warn', 'error', 'debug'] as const;
type LevelFilter = typeof LEVEL_FILTERS[number];

import { formatFileSize, formatDate, formatTimestamp } from '../utils/format';

export function TelemetryPage(props: TelemetryPageProps) {
  const [retryKey, setRetryKey] = useState(0);
  return (
    <TelemetryErrorBoundary onRetry={() => setRetryKey(k => k + 1)} key={retryKey}>
      <TelemetryPageInner {...props} />
    </TelemetryErrorBoundary>
  );
}

function TelemetryPageInner({ onOpenRun }: TelemetryPageProps) {
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
        <div className="page-header">
          <div>
            <div className="page-title">Run {selectedRunId.slice(-8)}</div>
          </div>
          <div style={{ display: 'flex', gap: 'var(--space-2)' }}>
            <button className="btn btn-ghost" onClick={handleBack}>Back</button>
            <button className="btn" onClick={() => onOpenRun(selectedRunId)}>View Run</button>
          </div>
        </div>

        {/* Level filters */}
        <div style={{ display: 'flex', gap: 'var(--space-3)', marginBottom: 'var(--space-4)', flexWrap: 'wrap', alignItems: 'center' }}>
          <div style={{ display: 'flex', gap: 'var(--space-1)' }}>
            {LEVEL_FILTERS.map(lf => {
              const count = lf === 'all' ? entries.length : (levelCounts[lf] || 0);
              const isActive = levelFilter === lf;
              const color = isActive && lf !== 'all' ? LEVEL_COLORS[lf] : undefined;
              return (
                <button
                  key={lf}
                  className={`btn ${isActive ? '' : 'btn-ghost'}`}
                  onClick={() => setLevelFilter(lf)}
                  style={{ padding: '4px 12px', fontSize: 'var(--text-xs)', ...(color ? { borderColor: color, color } : {}) }}
                >
                  {lf === 'all' ? 'All' : lf.charAt(0).toUpperCase() + lf.slice(1)}
                  {count > 0 && <span style={{ opacity: 0.6 }}>({count})</span>}
                </button>
              );
            })}
          </div>
          <input
            className="hud-input"
            type="text"
            placeholder="Search events, messages, steps..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ flex: 1, minWidth: 180 }}
          />
          {search && (
            <button className="btn btn-ghost" onClick={() => setSearch('')} style={{ fontSize: 'var(--text-xs)' }}>Clear</button>
          )}
        </div>

        {/* Log entries table */}
        <div className="section">
          {entriesLoading ? (
            <EmptyState message="Loading log entries..." />
          ) : filteredEntries.length === 0 ? (
            <EmptyState
              message={entries.length === 0 ? 'No log entries' : 'No matching entries'}
              sub={entries.length > 0 ? `${entries.length} entries total` : undefined}
            />
          ) : (
            <table className="table">
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
                  <tr key={i} style={{ cursor: 'default' }}>
                    <td className="mono" style={{ color: 'var(--text-tertiary)' }}>{formatTimestamp(entry.ts)}</td>
                    <td>
                      <span style={{ fontSize: 'var(--text-xs)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em', color: LEVEL_COLORS[entry.level] ?? 'var(--text-primary)' }}>
                        {entry.level}
                      </span>
                    </td>
                    <td className="mono" style={{ color: 'var(--accent)' }}>{entry.event}</td>
                    <td style={{ color: 'var(--text-tertiary)' }}>{entry.section || '—'}</td>
                    <td style={{ color: 'var(--text-tertiary)' }}>{entry.step || '—'}</td>
                    <td style={{ wordBreak: 'break-word' }}>{entry.msg || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {entries.length > 0 && (
          <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: 'var(--space-2)' }}>
            Showing {filteredEntries.length} of {entries.length} entries
          </div>
        )}
      </div>
    );
  }

  // ── List view (telemetry run files) ──
  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Telemetry</div>
          {loading && <div className="page-subtitle">Loading…</div>}
        </div>
        <button className="btn btn-ghost" onClick={fetchRuns} title="Refresh (R)">
          <RefreshCw size={14} />
        </button>
      </div>

      <div className="section">
        {runs.length === 0 ? (
          <EmptyState
            message="No telemetry logs"
            sub="Run a blueprint to generate telemetry data"
          />
        ) : (
          <table className="table">
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
                  <td className="mono col-primary">{run.run_id.slice(-12)}</td>
                  <td className="col-shrink col-right"><span style={{ color: 'var(--accent)' }}>{run.event_count}</span></td>
                  <td className="col-shrink col-right" style={{ color: 'var(--text-tertiary)' }}>{formatFileSize(run.file_size)}</td>
                  <td style={{ color: 'var(--text-tertiary)' }}>{formatDate(run.modified_at)}</td>
                  <td>
                    <button
                      className="btn btn-ghost"
                      onClick={(e) => { e.stopPropagation(); setDeleteConfirm(run.run_id); }}
                      style={{ padding: '2px 8px', fontSize: 'var(--text-xs)', color: 'var(--status-failed)' }}
                    >
                      Del
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {runs.length > 0 && (
        <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: 'var(--space-2)' }}>
          {runs.length} log file{runs.length !== 1 ? 's' : ''} · {formatFileSize(runs.reduce((s, r) => s + r.file_size, 0))} total
        </div>
      )}

      {deleteConfirm && (
        <ConfirmDialog
          title="Delete telemetry log?"
          message={`This will permanently delete the log file for run ${deleteConfirm.slice(-12)}.`}
          confirmLabel="Delete"
          danger
          onConfirm={() => handleDelete(deleteConfirm)}
          onCancel={() => setDeleteConfirm(null)}
        />
      )}
    </div>
  );
}
