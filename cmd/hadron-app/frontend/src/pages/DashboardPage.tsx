import { useCallback, useEffect, useMemo } from 'react';
import { RefreshCw } from 'lucide-react';
import { usePoll } from '../hooks/usePoll';
import { listRuns } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { EmptyState } from '../components/ui/EmptyState';
import type { Run } from '../api/types';

interface DashboardPageProps {
  daemonStatus: string;
  daemonAddr: string;
  onOpenRun: (runId: string) => void;
}

function formatDuration(run: Run): string {
  if (!run.started_at) return '—';
  const end = run.ended_at ? new Date(run.ended_at) : new Date();
  const start = new Date(run.started_at);
  const ms = end.getTime() - start.getTime();
  return formatMs(ms);
}

function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

function computeAvgDuration(runs: Run[]): number {
  const completed = runs.filter(r => r.started_at && r.ended_at);
  if (completed.length === 0) return 0;
  const totalMs = completed.reduce((sum, r) => {
    return sum + (new Date(r.ended_at!).getTime() - new Date(r.started_at!).getTime());
  }, 0);
  return Math.round(totalMs / completed.length);
}

interface BlueprintStats {
  path: string;
  name: string;
  total: number;
  succeeded: number;
  failed: number;
  successRate: number;
  avgMs: number;
}

function computePerBlueprint(runs: Run[]): BlueprintStats[] {
  const byPath = new Map<string, Run[]>();
  for (const run of runs) {
    const key = run.blueprint_path;
    if (!byPath.has(key)) byPath.set(key, []);
    byPath.get(key)!.push(run);
  }

  const stats: BlueprintStats[] = [];
  for (const [path, pathRuns] of byPath) {
    const succeeded = pathRuns.filter(r => r.status === 'success').length;
    const failed = pathRuns.filter(r => r.status === 'failed').length;
    stats.push({
      path,
      name: shortPath(path),
      total: pathRuns.length,
      succeeded,
      failed,
      successRate: pathRuns.length > 0 ? Math.round((succeeded / pathRuns.length) * 100) : 0,
      avgMs: computeAvgDuration(pathRuns),
    });
  }

  return stats.sort((a, b) => b.total - a.total).slice(0, 10);
}

function formatTime(ts: string): string {
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function shortPath(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts.slice(-2).join('/');
}

export function DashboardPage({ daemonStatus, daemonAddr, onOpenRun }: DashboardPageProps) {
  const fetcher = useCallback(() => listRuns({ limit: 100 }), []);
  const { data, refresh } = usePoll(fetcher, 3000, daemonStatus === 'running');

  // Listen for global refresh shortcut (R key)
  useEffect(() => {
    const handler = () => refresh();
    window.addEventListener('hadron:refresh', handler);
    return () => window.removeEventListener('hadron:refresh', handler);
  }, [refresh]);

  const runs = data?.items ?? [];
  const total = runs.length;
  const running = runs.filter(r => r.status === 'running').length;
  const failed = runs.filter(r => r.status === 'failed').length;
  const succeeded = runs.filter(r => r.status === 'success').length;
  const successRate = total > 0 ? Math.round((succeeded / total) * 100) : 0;
  const avgDuration = computeAvgDuration(runs);
  const perBlueprint = computePerBlueprint(runs);
  const recent = runs.slice(0, 10);

  // Activity timeline: runs per hour for last 24h
  const timeline = useMemo(() => {
    const now = new Date();
    const buckets: { hour: number; label: string; success: number; failed: number; other: number }[] = [];
    for (let i = 23; i >= 0; i--) {
      const t = new Date(now.getTime() - i * 3600000);
      buckets.push({
        hour: t.getHours(),
        label: t.toLocaleTimeString([], { hour: '2-digit', hour12: false }),
        success: 0,
        failed: 0,
        other: 0,
      });
    }
    const cutoff = now.getTime() - 24 * 3600000;
    for (const run of runs) {
      if (!run.started_at) continue;
      const ts = new Date(run.started_at).getTime();
      if (ts < cutoff) continue;
      const hoursAgo = Math.floor((now.getTime() - ts) / 3600000);
      const idx = 23 - Math.min(hoursAgo, 23);
      if (run.status === 'success') buckets[idx].success++;
      else if (run.status === 'failed') buckets[idx].failed++;
      else buckets[idx].other++;
    }
    return buckets;
  }, [runs]);

  return (
    <div>
      <div className="page-header">
        <span className="page-title">Dashboard</span>
        <button
          className="hud-button-ghost"
          onClick={refresh}
          title="Refresh (R)"
          style={{ display: 'flex', alignItems: 'center', padding: '0.25rem' }}
        >
          <RefreshCw size={14} />
        </button>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">Total Runs</div>
          <div className="stat-value">{total}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Running</div>
          <div className="stat-value" style={{ color: running > 0 ? 'rgb(var(--warn))' : undefined }}>
            {running}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Failed</div>
          <div className="stat-value" style={{ color: failed > 0 ? 'rgb(var(--danger))' : undefined }}>
            {failed}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Succeeded</div>
          <div className="stat-value" style={{ color: succeeded > 0 ? 'rgb(var(--ok))' : undefined }}>
            {succeeded}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Success Rate</div>
          <div className="stat-value" style={{ color: total === 0 ? undefined : successRate >= 80 ? 'rgb(var(--ok))' : successRate >= 50 ? 'rgb(var(--warn))' : 'rgb(var(--danger))' }}>
            {total > 0 ? `${successRate}%` : '—'}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Avg Duration</div>
          <div className="stat-value" style={{ fontSize: '1.2rem' }}>
            {avgDuration > 0 ? formatMs(avgDuration) : '—'}
          </div>
        </div>
      </div>

      {/* Daemon info */}
      {daemonStatus !== 'running' && (
        <div className="hud-panel" style={{ padding: '0.75rem 1rem', marginBottom: '1rem', color: 'rgb(var(--warn))' }}>
          {daemonStatus === 'error' ? 'Daemon error — check that hadrond is installed.' : 'Daemon starting…'}
        </div>
      )}

      {/* Recent runs */}
      <div style={{ marginBottom: '0.5rem' }}>
        <span className="hud-label">Recent Runs</span>
      </div>
      <div className="hud-panel" style={{ overflow: 'hidden' }}>
        {recent.length === 0 ? (
          <EmptyState
            message="No runs yet"
            sub={daemonStatus === 'running' ? `daemon running at ${daemonAddr}` : undefined}
          />
        ) : (
          <table className="hud-table">
            <thead>
              <tr>
                <th>Blueprint</th>
                <th>Status</th>
                <th>Started</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody>
              {recent.map(run => (
                <tr key={run.id} onClick={() => onOpenRun(run.id)}>
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

      {/* Activity timeline */}
      {runs.length > 0 && (() => {
        const maxCount = Math.max(...timeline.map(b => b.success + b.failed + b.other), 1);
        return (
          <>
            <div style={{ marginBottom: '0.5rem', marginTop: '1.25rem' }}>
              <span className="hud-label">Activity — Last 24 Hours</span>
            </div>
            <div className="hud-panel" style={{ padding: '0.75rem 1rem' }}>
              <div style={{ display: 'flex', alignItems: 'flex-end', gap: '2px', height: '80px' }}>
                {timeline.map((bucket, i) => {
                  const total = bucket.success + bucket.failed + bucket.other;
                  const barH = total > 0 ? Math.max((total / maxCount) * 80, 4) : 0;
                  return (
                    <div
                      key={i}
                      style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'flex-end', height: '100%' }}
                      title={`${bucket.label}:00 — ${total} run${total !== 1 ? 's' : ''} (${bucket.success} ok, ${bucket.failed} fail)`}
                    >
                      {total > 0 && (
                        <div style={{ width: '100%', height: `${barH}px`, display: 'flex', flexDirection: 'column', borderRadius: '2px', overflow: 'hidden' }}>
                          {bucket.failed > 0 && (
                            <div style={{ flex: bucket.failed, background: 'rgb(var(--danger))' }} />
                          )}
                          {bucket.other > 0 && (
                            <div style={{ flex: bucket.other, background: 'rgb(var(--warn))' }} />
                          )}
                          {bucket.success > 0 && (
                            <div style={{ flex: bucket.success, background: 'rgb(var(--ok))' }} />
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
              {/* Hour labels — show every 4th */}
              <div style={{ display: 'flex', gap: '2px', marginTop: '0.3rem' }}>
                {timeline.map((bucket, i) => (
                  <div key={i} style={{ flex: 1, textAlign: 'center', fontSize: '0.58rem', color: 'rgb(var(--muted))' }}>
                    {i % 4 === 0 ? bucket.label : ''}
                  </div>
                ))}
              </div>
            </div>
          </>
        );
      })()}

      {/* Per-blueprint stats */}
      {perBlueprint.length > 0 && (
        <>
          <div style={{ marginBottom: '0.5rem', marginTop: '1.25rem' }}>
            <span className="hud-label">Blueprint Stats</span>
          </div>
          <div className="hud-panel" style={{ overflow: 'hidden' }}>
            <table className="hud-table">
              <thead>
                <tr>
                  <th>Blueprint</th>
                  <th>Runs</th>
                  <th>Success</th>
                  <th>Avg Duration</th>
                </tr>
              </thead>
              <tbody>
                {perBlueprint.map(bp => (
                  <tr key={bp.path} style={{ cursor: 'default' }}>
                    <td style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
                      {bp.name}
                    </td>
                    <td style={{ fontSize: '0.8rem' }}>{bp.total}</td>
                    <td>
                      <span style={{
                        fontSize: '0.8rem',
                        color: bp.successRate >= 80 ? 'rgb(var(--ok))' : bp.successRate >= 50 ? 'rgb(var(--warn))' : 'rgb(var(--danger))',
                      }}>
                        {bp.successRate}%
                      </span>
                    </td>
                    <td style={{ color: 'rgb(var(--muted))', fontSize: '0.8rem' }}>
                      {bp.avgMs > 0 ? formatMs(bp.avgMs) : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
