import { useCallback, useEffect } from 'react';
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

  return (
    <div className="flex flex-col gap-4 h-full">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <div className="text-xl font-semibold text-foreground tracking-tight">Overview</div>
          <div className="text-sm text-muted-foreground">Last 24 hours</div>
        </div>
        <Button variant="ghost" onClick={refresh} title="Refresh (R)">
          <RefreshCw size={14} />
        </Button>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-6 gap-3">
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
        <div className="rounded-lg border border-border bg-card overflow-hidden px-5 py-4 text-amber-400">
          {daemon.status === 'error' ? 'Daemon error \u2014 check that hadrond is installed.' : 'Daemon starting\u2026'}
        </div>
      )}

      {/* Two-column: Recent Runs + Top Blueprints */}
      <div className="grid grid-cols-2 gap-6 flex-1 min-h-0">
        {/* Recent Runs */}
        <div className="flex flex-col gap-1 min-h-0">
          <div className="flex items-center justify-between">
            <span className="text-xs tracking-wider uppercase text-muted-foreground">Recent Runs</span>
            <button onClick={() => nav.navigate('runs')} className="text-xs text-muted-foreground hover:text-primary transition-colors">View all</button>
          </div>
          <div className="flex flex-col gap-px flex-1 overflow-y-auto">
            {recent.length === 0 ? (
              <EmptyState
                message="No runs yet"
                sub={daemon.status === 'running' ? `daemon running at ${daemon.address}` : undefined}
              />
            ) : (
              recent.map(run => (
                <div
                  key={run.id}
                  onClick={() => nav.openRun(run.id)}
                  className={cn(
                    'flex items-center gap-3 px-3 py-1.5 rounded cursor-pointer transition-colors',
                    'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
                    'border border-transparent',
                  )}
                >
                  <div className="flex-1 min-w-0 font-mono text-sm truncate">{shortPath(run.blueprint_path)}</div>
                  <div className="shrink-0"><StatusBadge status={run.status} /></div>
                  <div className="shrink-0 text-sm text-muted-foreground whitespace-nowrap w-28 text-right">
                    {run.started_at ? formatTime(run.started_at) : '\u2014'}
                  </div>
                  <div className="shrink-0 text-sm font-mono text-muted-foreground whitespace-nowrap w-12 text-right">
                    {formatRunDuration(run)}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Top Blueprints */}
        <div className="flex flex-col gap-1 min-h-0">
          <span className="text-xs tracking-wider uppercase text-muted-foreground">Top Blueprints</span>
          <div className="flex flex-col gap-px flex-1 overflow-y-auto">
            {perBlueprint.length === 0 ? (
              <EmptyState message="No data" sub="Run some blueprints to see stats" />
            ) : (
              perBlueprint.map(bp => (
                <div
                  key={bp.path}
                  className={cn(
                    'flex items-center gap-3 px-3 py-1.5 rounded transition-colors',
                    'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
                    'border border-transparent',
                  )}
                >
                  <div className="flex-1 min-w-0 font-mono text-sm truncate">{bp.name}</div>
                  <div className="shrink-0 text-sm font-mono text-muted-foreground w-10 text-right">{bp.total}</div>
                  <div className="shrink-0 text-sm w-14 text-right">
                    <span className={cn(
                      bp.successRate >= 80 ? 'text-blue-400' : bp.successRate >= 50 ? 'text-amber-400' : 'text-red-400',
                    )}>
                      {bp.successRate}%
                    </span>
                  </div>
                  <div className="shrink-0 text-sm font-mono text-muted-foreground w-16 text-right">
                    {bp.avgMs > 0 ? formatMs(bp.avgMs) : '\u2014'}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
