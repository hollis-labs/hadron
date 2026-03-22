import { useState, useCallback, useEffect, useRef } from 'react';
import { RefreshCw } from 'lucide-react';
import { usePoll } from '../hooks/usePoll';
import { listRuns } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { EmptyState } from '../components/ui/EmptyState';
import { formatRunDuration, formatTime } from '../utils/format';
import { shortPath } from '../utils/path';
import type { Run } from '../api/types';

interface RunsPageProps {
  daemonStatus: string;
  onOpenRun: (runId: string) => void;
}

const STATUS_FILTERS = ['all', 'running', 'success', 'failed', 'canceled'] as const;
type StatusFilter = typeof STATUS_FILTERS[number];

const FILTER_COLORS: Record<string, string> = {
  running: 'var(--status-running)',
  success: 'var(--status-success)',
  failed: 'var(--status-failed)',
};

export function RunsPage({ daemonStatus, onOpenRun }: RunsPageProps) {
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [search, setSearch] = useState('');
  const [focusIndex, setFocusIndex] = useState(-1);
  const focusRef = useRef<HTMLTableRowElement>(null);

  const fetcher = useCallback(() => listRuns({ limit: 100 }), []);
  const { data, loading, refresh } = usePoll(fetcher, 3000, daemonStatus === 'running');

  useEffect(() => {
    const handler = () => refresh();
    window.addEventListener('hadron:refresh', handler);
    return () => window.removeEventListener('hadron:refresh', handler);
  }, [refresh]);

  const runs = data?.items ?? [];

  const filteredRuns = runs.filter(run => {
    if (statusFilter !== 'all' && run.status !== statusFilter) return false;
    if (search) {
      const q = search.toLowerCase();
      if (!run.blueprint_path.toLowerCase().includes(q) && !run.id.toLowerCase().includes(q)) return false;
    }
    return true;
  });

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      const count = filteredRuns.length;
      if (count === 0) return;
      if (e.key === 'ArrowDown') { e.preventDefault(); setFocusIndex(prev => Math.min(prev + 1, count - 1)); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); setFocusIndex(prev => Math.max(prev - 1, 0)); }
      else if (e.key === 'Enter' && focusIndex >= 0 && focusIndex < count) { e.preventDefault(); onOpenRun(filteredRuns[focusIndex].id); }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [focusIndex, filteredRuns, onOpenRun]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => { setFocusIndex(-1); }, [statusFilter, search]);
  useEffect(() => { focusRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' }); }, [focusIndex]);

  return (
    <div>
      <div className="page-header">
        <div>
          <div className="page-title">Runs</div>
          {loading && <div className="page-subtitle">Refreshing…</div>}
        </div>
        <button className="btn btn-ghost" onClick={refresh} title="Refresh (R)">
          <RefreshCw size={14} />
        </button>
      </div>

      {/* Filters */}
      <div style={{ display: 'flex', gap: 'var(--space-3)', alignItems: 'center', marginBottom: 'var(--space-4)', flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', gap: 'var(--space-1)' }}>
          {STATUS_FILTERS.map(sf => {
            const isActive = statusFilter === sf;
            const color = isActive && sf !== 'all' ? FILTER_COLORS[sf] : undefined;
            return (
              <button
                key={sf}
                className={`btn ${isActive ? '' : 'btn-ghost'}`}
                onClick={() => setStatusFilter(sf)}
                style={{
                  padding: '4px 12px',
                  fontSize: 'var(--text-xs)',
                  ...(color ? { borderColor: color, color } : {}),
                }}
              >
                {sf === 'all' ? 'All' : sf.charAt(0).toUpperCase() + sf.slice(1)}
              </button>
            );
          })}
        </div>

        <input
          className="hud-input"
          type="text"
          placeholder="Search by blueprint or run ID..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{ flex: 1, minWidth: 180 }}
        />
        {search && (
          <button className="btn btn-ghost" onClick={() => setSearch('')} style={{ fontSize: 'var(--text-xs)' }}>
            Clear
          </button>
        )}
      </div>

      <div className="section">
        {filteredRuns.length === 0 ? (
          <EmptyState
            message={runs.length === 0 ? 'No runs' : 'No matching runs'}
            sub={runs.length === 0 ? 'Run a blueprint to see history here' : `No runs matching current filters`}
          />
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th className="col-primary">Blueprint</th>
                <th className="col-shrink">Status</th>
                <th className="col-shrink col-right">Started</th>
                <th className="col-shrink col-right">Duration</th>
              </tr>
            </thead>
            <tbody>
              {filteredRuns.map((run, i) => (
                <tr
                  key={run.id}
                  onClick={() => onOpenRun(run.id)}
                  ref={i === focusIndex ? focusRef : undefined}
                  style={i === focusIndex ? { background: 'var(--bg-active)', outline: '1px solid var(--border-focus)' } : undefined}
                >
                  <td className="col-primary">
                    <div className="mono">{shortPath(run.blueprint_path)}</div>
                    <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: 1 }}>{run.id.slice(-8)}</div>
                  </td>
                  <td className="col-shrink">
                    <StatusBadge status={run.status} />
                  </td>
                  <td className="col-shrink col-right" style={{ color: 'var(--text-tertiary)' }}>
                    {run.started_at ? formatTime(run.started_at) : '—'}
                  </td>
                  <td className="mono col-shrink col-right">
                    {formatRunDuration(run)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {runs.length > 0 && (
        <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: 'var(--space-2)' }}>
          Showing {filteredRuns.length} of {runs.length} runs
        </div>
      )}
    </div>
  );
}
