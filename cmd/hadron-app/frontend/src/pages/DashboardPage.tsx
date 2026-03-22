import { useCallback, useEffect, useMemo } from 'react';
import { RefreshCw } from 'lucide-react';
import { usePoll } from '../hooks/usePoll';
import { listRuns } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { EmptyState } from '../components/ui/EmptyState';
import { formatRunDuration, formatMs, formatTime, computeAvgDuration } from '../utils/format';
import { shortPath } from '../utils/path';
import type { Run } from '../api/types';

interface DashboardPageProps {
  daemonStatus: string;
  daemonAddr: string;
  onOpenRun: (runId: string) => void;
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
        <div>
          <div className="page-title">Overview</div>
          <div className="page-subtitle">Last 24 hours</div>
        </div>
        <button className="btn" onClick={refresh} title="Refresh (R)">
          <RefreshCw size={14} /> Refresh
        </button>
      </div>

      {/* Stat cards */}
      <div className="stat-grid">
        <div className="stat-card">
          <span className="stat-label">Total Runs</span>
          <span className="stat-value">{total.toLocaleString()}</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Running</span>
          <span className={`stat-value${running > 0 ? ' running' : ''}`}>{running}</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Succeeded</span>
          <span className={`stat-value${succeeded > 0 ? ' success' : ''}`}>{succeeded.toLocaleString()}</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Failed</span>
          <span className={`stat-value${failed > 0 ? ' failed' : ''}`}>{failed}</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Success Rate</span>
          <span className={`stat-value${total > 0 ? (successRate >= 80 ? ' success' : ' failed') : ''}`}>
            {total > 0 ? `${successRate}%` : '—'}
          </span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Avg Duration</span>
          <span className="stat-value" style={{ fontSize: 'var(--text-xl)' }}>
            {avgDuration > 0 ? formatMs(avgDuration) : '—'}
          </span>
        </div>
      </div>

      {/* Daemon warning */}
      {daemonStatus !== 'running' && (
        <div className="section" style={{ padding: 'var(--space-4) var(--space-5)', marginBottom: 'var(--space-4)', color: 'var(--status-running)' }}>
          {daemonStatus === 'error' ? 'Daemon error — check that hadrond is installed.' : 'Daemon starting…'}
        </div>
      )}

      {/* Two-column: Recent Runs + Activity */}
      <div className="section-grid">
        {/* Recent Runs */}
        <div className="section">
          <div className="section-header">
            <span className="section-title">Recent Runs</span>
            <span className="section-action">View all</span>
          </div>
          {recent.length === 0 ? (
            <EmptyState
              message="No runs yet"
              sub={daemonStatus === 'running' ? `daemon running at ${daemonAddr}` : undefined}
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
                {recent.map(run => (
                  <tr key={run.id} onClick={() => onOpenRun(run.id)}>
                    <td className="mono col-primary">{shortPath(run.blueprint_path)}</td>
                    <td className="col-shrink"><StatusBadge status={run.status} /></td>
                    <td className="col-shrink col-right" style={{ color: 'var(--text-tertiary)' }}>{run.started_at ? formatTime(run.started_at) : '—'}</td>
                    <td className="mono col-shrink col-right">{formatRunDuration(run)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Activity Timeline */}
        <div className="section">
          <div className="section-header">
            <span className="section-title">Activity</span>
            <span style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>24h</span>
          </div>
          {runs.length > 0 && (() => {
            const maxCount = Math.max(...timeline.map(b => b.success + b.failed + b.other), 1);
            return (
              <>
                <div className="timeline-chart">
                  {timeline.map((bucket, i) => {
                    const bTotal = bucket.success + bucket.failed + bucket.other;
                    const pct = bTotal > 0 ? Math.max((bTotal / maxCount) * 100, 5) : 0;
                    const cls = bTotal === 0 ? '' : bucket.failed > 0 && bucket.success > 0 ? 'mixed' : bucket.failed > 0 ? 'failed' : 'success';
                    return (
                      <div
                        key={i}
                        className={`timeline-bar ${cls}`}
                        style={{ height: `${pct}%` }}
                        title={`${bucket.label}:00 — ${bTotal} run${bTotal !== 1 ? 's' : ''}`}
                      />
                    );
                  })}
                </div>
                <div className="timeline-labels">
                  {timeline.map((bucket, i) => (
                    <span key={i}>{i % 4 === 0 ? bucket.label : ''}</span>
                  ))}
                </div>
              </>
            );
          })()}

          {/* Per-blueprint stats below the chart */}
          {perBlueprint.length > 0 && (
            <>
              <div className="section-header" style={{ borderTop: '1px solid var(--border-subtle)' }}>
                <span className="section-title">Top Blueprints</span>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th className="col-primary">Blueprint</th>
                    <th className="col-shrink col-right">Runs</th>
                    <th className="col-shrink col-right">Success</th>
                    <th className="col-shrink col-right">Avg</th>
                  </tr>
                </thead>
                <tbody>
                  {perBlueprint.map(bp => (
                    <tr key={bp.path} style={{ cursor: 'default' }}>
                      <td className="mono col-primary">{bp.name}</td>
                      <td className="mono col-shrink col-right">{bp.total}</td>
                      <td className="col-shrink col-right">
                        <span style={{
                          color: bp.successRate >= 80 ? 'var(--status-success)' : bp.successRate >= 50 ? 'var(--status-running)' : 'var(--status-failed)',
                        }}>
                          {bp.successRate}%
                        </span>
                      </td>
                      <td className="mono col-shrink col-right">{bp.avgMs > 0 ? formatMs(bp.avgMs) : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
