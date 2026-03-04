import { useState, useEffect, useCallback, Component, type ReactNode } from 'react';
import { toast } from 'sonner';
import { RefreshCw } from 'lucide-react';
import { listTelemetryRuns, readTelemetryLog, deleteTelemetryLog } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import type { TelemetryRunSummary, TelemetryLogEntry } from '../api/types';

// Error boundary to prevent page from crashing the whole app
class TelemetryErrorBoundary extends Component<{ children: ReactNode; onRetry: () => void }, { error: Error | null }> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) { return { error }; }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: '2rem', textAlign: 'center' }}>
          <div style={{ color: 'rgb(var(--danger))', marginBottom: '0.5rem', fontWeight: 600 }}>
            Activity Log encountered an error
          </div>
          <div style={{ fontSize: '0.78rem', color: 'rgb(var(--muted))', marginBottom: '1rem', fontFamily: 'monospace' }}>
            {this.state.error.message}
          </div>
          <button className="hud-button" onClick={() => { this.setState({ error: null }); this.props.onRetry(); }}>
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
  info: 'rgb(var(--accent))',
  warn: 'rgb(var(--warn))',
  error: 'rgb(var(--danger))',
  debug: 'rgb(var(--muted))',
};

const LEVEL_FILTERS = ['all', 'info', 'warn', 'error', 'debug'] as const;
type LevelFilter = typeof LEVEL_FILTERS[number];

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(ts: string): string {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    if (isNaN(d.getTime())) return '—';
    return d.toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  } catch { return '—'; }
}

function formatTimestamp(ts: string): string {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    if (isNaN(d.getTime())) return '—';
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3 } as Intl.DateTimeFormatOptions);
  } catch { return '—'; }
}

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
          <button className="hud-button-ghost" onClick={handleBack} style={{ padding: '0.25rem 0.5rem', fontSize: '0.75rem' }}>
            ← Back
          </button>
          <span className="page-title" style={{ marginLeft: '0.5rem' }}>
            Run {selectedRunId.slice(-8)}
          </span>
          <button
            className="hud-button-ghost"
            onClick={() => onOpenRun(selectedRunId)}
            style={{ marginLeft: 'auto', padding: '0.25rem 0.5rem', fontSize: '0.7rem' }}
          >
            View Run →
          </button>
        </div>

        {/* Level summary */}
        <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.75rem', flexWrap: 'wrap', alignItems: 'center' }}>
          {LEVEL_FILTERS.map(lf => {
            const count = lf === 'all' ? entries.length : (levelCounts[lf] || 0);
            return (
              <button
                key={lf}
                className={levelFilter === lf ? 'hud-button' : 'hud-button-ghost'}
                onClick={() => setLevelFilter(lf)}
                style={{
                  padding: '0.25rem 0.6rem',
                  fontSize: '0.7rem',
                  ...(levelFilter === lf && lf === 'error' ? { borderColor: 'rgb(var(--danger))', color: 'rgb(var(--danger))' } : {}),
                  ...(levelFilter === lf && lf === 'warn' ? { borderColor: 'rgb(var(--warn))', color: 'rgb(var(--warn))' } : {}),
                  ...(levelFilter === lf && lf === 'info' ? { borderColor: 'rgb(var(--accent))', color: 'rgb(var(--accent))' } : {}),
                }}
              >
                {lf === 'all' ? 'All' : lf.charAt(0).toUpperCase() + lf.slice(1)}
                {count > 0 && <span style={{ marginLeft: '0.3rem', opacity: 0.6 }}>({count})</span>}
              </button>
            );
          })}

          <input
            className="hud-input"
            type="text"
            placeholder="Search events, messages, steps..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ flex: 1, minWidth: '180px' }}
          />
          {search && (
            <button className="hud-button-ghost" onClick={() => setSearch('')} style={{ padding: '0.3rem 0.5rem', fontSize: '0.7rem' }}>
              Clear
            </button>
          )}
        </div>

        {/* Log entries table */}
        <div className="hud-panel" style={{ overflow: 'hidden' }}>
          {entriesLoading ? (
            <EmptyState message="Loading log entries..." />
          ) : filteredEntries.length === 0 ? (
            <EmptyState
              message={entries.length === 0 ? 'No log entries' : 'No matching entries'}
              sub={entries.length > 0 ? `${entries.length} entries total` : undefined}
            />
          ) : (
            <table className="hud-table">
              <thead>
                <tr>
                  <th style={{ width: '100px' }}>Time</th>
                  <th style={{ width: '55px' }}>Level</th>
                  <th style={{ width: '120px' }}>Event</th>
                  <th style={{ width: '100px' }}>Section</th>
                  <th style={{ width: '120px' }}>Step</th>
                  <th>Message</th>
                </tr>
              </thead>
              <tbody>
                {filteredEntries.map((entry, i) => (
                  <tr key={i} style={{ cursor: 'default' }}>
                    <td style={{ fontFamily: 'monospace', fontSize: '0.72rem', color: 'rgb(var(--muted))' }}>
                      {formatTimestamp(entry.ts)}
                    </td>
                    <td>
                      <span style={{
                        fontSize: '0.7rem',
                        fontWeight: 600,
                        textTransform: 'uppercase',
                        letterSpacing: '0.04em',
                        color: LEVEL_COLORS[entry.level] ?? 'rgb(var(--text))',
                      }}>
                        {entry.level}
                      </span>
                    </td>
                    <td style={{ fontFamily: 'monospace', fontSize: '0.75rem', color: 'rgb(var(--accent))' }}>
                      {entry.event}
                    </td>
                    <td style={{ fontSize: '0.75rem', color: 'rgb(var(--muted))' }}>
                      {entry.section || '—'}
                    </td>
                    <td style={{ fontSize: '0.75rem', color: 'rgb(var(--muted))' }}>
                      {entry.step || '—'}
                    </td>
                    <td style={{ fontSize: '0.75rem', wordBreak: 'break-word' }}>
                      {entry.msg || '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {entries.length > 0 && (
          <div style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))', marginTop: '0.5rem' }}>
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
        <span className="page-title">Telemetry</span>
        <button
          className="hud-button-ghost"
          onClick={fetchRuns}
          title="Refresh (R)"
          style={{ display: 'flex', alignItems: 'center', padding: '0.25rem' }}
        >
          <RefreshCw size={14} />
        </button>
        {loading && <span style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))' }}>Loading…</span>}
      </div>

      <div className="hud-panel" style={{ overflow: 'hidden' }}>
        {runs.length === 0 ? (
          <EmptyState
            message="No telemetry logs"
            sub="Run a blueprint to generate telemetry data"
          />
        ) : (
          <table className="hud-table">
            <thead>
              <tr>
                <th>Run ID</th>
                <th>Events</th>
                <th>Size</th>
                <th>Last Modified</th>
                <th style={{ width: '60px' }}></th>
              </tr>
            </thead>
            <tbody>
              {runs.map(run => (
                <tr key={run.run_id} onClick={() => handleSelectRun(run.run_id)}>
                  <td style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
                    {run.run_id.slice(-12)}
                  </td>
                  <td style={{ fontSize: '0.8rem' }}>
                    <span style={{ color: 'rgb(var(--accent))' }}>{run.event_count}</span>
                  </td>
                  <td style={{ fontSize: '0.8rem', color: 'rgb(var(--muted))' }}>
                    {formatFileSize(run.file_size)}
                  </td>
                  <td style={{ fontSize: '0.8rem', color: 'rgb(var(--muted))' }}>
                    {formatDate(run.modified_at)}
                  </td>
                  <td>
                    <button
                      className="hud-button-ghost"
                      onClick={(e) => { e.stopPropagation(); setDeleteConfirm(run.run_id); }}
                      style={{ padding: '0.15rem 0.4rem', fontSize: '0.65rem', color: 'rgb(var(--danger))' }}
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
        <div style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))', marginTop: '0.5rem' }}>
          {runs.length} log file{runs.length !== 1 ? 's' : ''} · {formatFileSize(runs.reduce((s, r) => s + r.file_size, 0))} total
        </div>
      )}

      {/* Delete confirmation modal */}
      {deleteConfirm && (
        <div className="hud-modal-overlay" onClick={() => setDeleteConfirm(null)}>
          <div className="hud-modal" onClick={(e) => e.stopPropagation()} style={{ maxWidth: '400px' }}>
            <div style={{ padding: '1.25rem' }}>
              <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Delete telemetry log?</div>
              <div style={{ fontSize: '0.8rem', color: 'rgb(var(--muted))', marginBottom: '1rem' }}>
                This will permanently delete the log file for run <span style={{ fontFamily: 'monospace', color: 'rgb(var(--accent))' }}>{deleteConfirm.slice(-12)}</span>.
              </div>
              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                <button className="hud-button-ghost" onClick={() => setDeleteConfirm(null)}>Cancel</button>
                <button
                  className="hud-button"
                  style={{ borderColor: 'rgb(var(--danger))', color: 'rgb(var(--danger))' }}
                  onClick={() => handleDelete(deleteConfirm)}
                >
                  Delete
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
