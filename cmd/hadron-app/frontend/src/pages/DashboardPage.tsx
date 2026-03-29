import { useCallback, useEffect, useMemo } from 'react';
import { RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { useDaemon } from '../contexts/DaemonContext';
import { useNavigation } from '../contexts/NavigationContext';
import { usePoll } from '../hooks/usePoll';
import { listRuns } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { EmptyState } from '../components/ui/EmptyState';
import { formatRunDuration, formatMs, formatTime, computeAvgDuration } from '../utils/format';
import { shortPath } from '../utils/path';
import type { Run } from '../api/types';

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

export function DashboardPage() {
  const daemon = useDaemon();
  const nav = useNavigation();
  const fetcher = useCallback(() => listRuns({ limit: 100 }), []);
  const { data, refresh } = usePoll(fetcher, 3000, daemon.status === 'running');

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
      <div className="flex items-center justify-between mb-6">
        <div>
          <div className="text-xl font-semibold text-foreground tracking-tight">Overview</div>
          <div className="text-sm text-muted-foreground">Last 24 hours</div>
        </div>
        <Button variant="outline" onClick={refresh} title="Refresh (R)">
          <RefreshCw size={14} /> Refresh
        </Button>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-6 gap-3 mb-6">
        <div className="flex flex-col gap-1 rounded-lg border border-border bg-card p-4">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Total Runs</span>
          <span className="text-2xl font-bold font-mono text-foreground tracking-tight leading-tight">{total.toLocaleString()}</span>
        </div>
        <div className="flex flex-col gap-1 rounded-lg border border-border bg-card p-4">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Running</span>
          <span className={cn('text-2xl font-bold font-mono text-foreground tracking-tight leading-tight', running > 0 && 'text-amber-400')}>{running}</span>
        </div>
        <div className="flex flex-col gap-1 rounded-lg border border-border bg-card p-4">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Succeeded</span>
          <span className={cn('text-2xl font-bold font-mono text-foreground tracking-tight leading-tight', succeeded > 0 && 'text-blue-400')}>{succeeded.toLocaleString()}</span>
        </div>
        <div className="flex flex-col gap-1 rounded-lg border border-border bg-card p-4">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Failed</span>
          <span className={cn('text-2xl font-bold font-mono text-foreground tracking-tight leading-tight', failed > 0 && 'text-red-400')}>{failed}</span>
        </div>
        <div className="flex flex-col gap-1 rounded-lg border border-border bg-card p-4">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Success Rate</span>
          <span className={cn(
            'text-2xl font-bold font-mono text-foreground tracking-tight leading-tight',
            total > 0 && (successRate >= 80 ? 'text-blue-400' : 'text-red-400'),
          )}>
            {total > 0 ? `${successRate}%` : '\u2014'}
          </span>
        </div>
        <div className="flex flex-col gap-1 rounded-lg border border-border bg-card p-4">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Avg Duration</span>
          <span className="text-xl font-bold font-mono text-foreground tracking-tight leading-tight">
            {avgDuration > 0 ? formatMs(avgDuration) : '\u2014'}
          </span>
        </div>
      </div>

      {/* Daemon warning */}
      {daemon.status !== 'running' && (
        <div className="rounded-lg border border-border bg-card overflow-hidden px-5 py-4 mb-4 text-amber-400">
          {daemon.status === 'error' ? 'Daemon error \u2014 check that hadrond is installed.' : 'Daemon starting\u2026'}
        </div>
      )}

      {/* Two-column: Recent Runs + Activity */}
      <div className="grid grid-cols-2 gap-4 mb-6">
        {/* Recent Runs */}
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-4 border-b border-border">
            <span className="text-base font-semibold text-foreground">Recent Runs</span>
            <span className="text-sm text-muted-foreground cursor-pointer flex items-center gap-1 hover:text-primary transition-colors">View all</span>
          </div>
          {recent.length === 0 ? (
            <EmptyState
              message="No runs yet"
              sub={daemon.status === 'running' ? `daemon running at ${daemon.address}` : undefined}
            />
          ) : (
            <table className="w-full border-collapse">
              <thead>
                <tr>
                  <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 w-full">Blueprint</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap">Status</th>
                  <th className="text-right px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap">Started</th>
                  <th className="text-right px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap">Duration</th>
                </tr>
              </thead>
              <tbody>
                {recent.map(run => (
                  <tr key={run.id} className="cursor-pointer transition-colors hover:bg-muted/50" onClick={() => nav.openRun(run.id)}>
                    <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border font-mono w-full">{shortPath(run.blueprint_path)}</td>
                    <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border whitespace-nowrap"><StatusBadge status={run.status} /></td>
                    <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border whitespace-nowrap text-right">{run.started_at ? formatTime(run.started_at) : '\u2014'}</td>
                    <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border font-mono whitespace-nowrap text-right">{formatRunDuration(run)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Activity Timeline */}
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-4 border-b border-border">
            <span className="text-base font-semibold text-foreground">Activity</span>
            <span className="text-sm text-muted-foreground">24h</span>
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
                        title={`${bucket.label}:00 \u2014 ${bTotal} run${bTotal !== 1 ? 's' : ''}`}
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
              <div className="flex items-center justify-between px-5 py-4 border-b border-border border-t">
                <span className="text-base font-semibold text-foreground">Top Blueprints</span>
              </div>
              <table className="w-full border-collapse">
                <thead>
                  <tr>
                    <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 w-full">Blueprint</th>
                    <th className="text-right px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap">Runs</th>
                    <th className="text-right px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap">Success</th>
                    <th className="text-right px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap">Avg</th>
                  </tr>
                </thead>
                <tbody>
                  {perBlueprint.map(bp => (
                    <tr key={bp.path} className="cursor-default">
                      <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border font-mono w-full">{bp.name}</td>
                      <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border font-mono whitespace-nowrap text-right">{bp.total}</td>
                      <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border whitespace-nowrap text-right">
                        <span className={cn(
                          bp.successRate >= 80 ? 'text-blue-400' : bp.successRate >= 50 ? 'text-amber-400' : 'text-red-400',
                        )}>
                          {bp.successRate}%
                        </span>
                      </td>
                      <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border font-mono whitespace-nowrap text-right">{bp.avgMs > 0 ? formatMs(bp.avgMs) : '\u2014'}</td>
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
