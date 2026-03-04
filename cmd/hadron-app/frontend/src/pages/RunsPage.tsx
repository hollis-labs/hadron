import { useState, useCallback, useEffect, useRef } from 'react';
import { RefreshCw } from 'lucide-react';
import { usePoll } from '../hooks/usePoll';
import { listRuns } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { EmptyState } from '../components/ui/EmptyState';
import type { Run } from '../api/types';

interface RunsPageProps {
  daemonStatus: string;
  onOpenRun: (runId: string) => void;
}

const STATUS_FILTERS = ['all', 'running', 'success', 'failed', 'canceled'] as const;
type StatusFilter = typeof STATUS_FILTERS[number];

function formatDuration(run: Run): string {
  if (!run.started_at) return '—';
  const end = run.ended_at ? new Date(run.ended_at) : new Date();
  const start = new Date(run.started_at);
  const ms = end.getTime() - start.getTime();
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

function formatTime(ts: string): string {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function shortPath(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts.slice(-2).join('/');
}

export function RunsPage({ daemonStatus, onOpenRun }: RunsPageProps) {
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [search, setSearch] = useState('');

  const [focusIndex, setFocusIndex] = useState(-1);
  const focusRef = useRef<HTMLTableRowElement>(null);

  const fetcher = useCallback(() => listRuns({ limit: 100 }), []);
  const { data, loading, refresh } = usePoll(fetcher, 3000, daemonStatus === 'running');

  // Listen for global refresh shortcut (R key)
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

  // Arrow key navigation
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
        <span className="page-title">Run Log</span>
        <button
          className="hud-button-ghost"
          onClick={refresh}
          title="Refresh (R)"
          style={{ display: 'flex', alignItems: 'center', padding: '0.25rem' }}
        >
          <RefreshCw size={14} />
        </button>
        {loading && <span style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))' }}>Refreshing…</span>}
      </div>

      {/* Filters */}
      <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', marginBottom: '0.75rem', flexWrap: 'wrap' }}>
        {/* Status chips */}
        <div style={{ display: 'flex', gap: '0.25rem' }}>
          {STATUS_FILTERS.map(sf => (
            <button
              key={sf}
              className={statusFilter === sf ? 'hud-button' : 'hud-button-ghost'}
              onClick={() => setStatusFilter(sf)}
              style={{
                padding: '0.25rem 0.6rem',
                fontSize: '0.7rem',
                ...(statusFilter === sf && sf === 'running' ? { borderColor: 'rgb(var(--warn))', color: 'rgb(var(--warn))' } : {}),
                ...(statusFilter === sf && sf === 'success' ? { borderColor: 'rgb(var(--ok))', color: 'rgb(var(--ok))' } : {}),
                ...(statusFilter === sf && sf === 'failed' ? { borderColor: 'rgb(var(--danger))', color: 'rgb(var(--danger))' } : {}),
              }}
            >
              {sf === 'all' ? 'All' : sf.charAt(0).toUpperCase() + sf.slice(1)}
            </button>
          ))}
        </div>

        {/* Search */}
        <input
          className="hud-input"
          type="text"
          placeholder="Search by blueprint or run ID..."
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

      <div className="hud-panel" style={{ overflow: 'hidden' }}>
        {filteredRuns.length === 0 ? (
          <EmptyState
            message={runs.length === 0 ? 'No runs' : 'No matching runs'}
            sub={runs.length === 0 ? 'Run a blueprint to see history here' : `No runs matching "${statusFilter !== 'all' ? statusFilter : ''}${search ? (statusFilter !== 'all' ? ' + ' : '') + search : ''}"`}
          />
        ) : (
          <table className="hud-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Blueprint</th>
                <th>Status</th>
                <th>Started</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody>
              {filteredRuns.map((run, i) => (
                <tr
                  key={run.id}
                  onClick={() => onOpenRun(run.id)}
                  ref={i === focusIndex ? focusRef : undefined}
                  style={i === focusIndex ? { background: 'rgba(var(--text), 0.05)', outline: '1px solid rgba(var(--accent), 0.3)' } : undefined}
                >
                  <td style={{ color: 'rgb(var(--muted))', fontSize: '0.75rem', fontFamily: 'monospace' }}>
                    {run.id.slice(-8)}
                  </td>
                  <td style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
                    {shortPath(run.blueprint_path)}
                  </td>
                  <td>
                    <StatusBadge status={run.status} />
                  </td>
                  <td style={{ color: 'rgb(var(--muted))', fontSize: '0.8rem' }}>
                    {run.started_at ? formatTime(run.started_at) : '—'}
                  </td>
                  <td style={{ color: 'rgb(var(--muted))', fontSize: '0.8rem' }}>
                    {formatDuration(run)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Count label */}
      {runs.length > 0 && (
        <div style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))', marginTop: '0.5rem' }}>
          Showing {filteredRuns.length} of {runs.length} runs
        </div>
      )}
    </div>
  );
}
