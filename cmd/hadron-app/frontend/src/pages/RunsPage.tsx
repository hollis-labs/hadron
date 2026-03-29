import { useState, useCallback, useEffect, useRef } from 'react';
import { RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { cn } from '@/lib/utils';
import { useDaemon } from '../contexts/DaemonContext';
import { useNavigation } from '../contexts/NavigationContext';
import { usePoll } from '../hooks/usePoll';
import { listRuns } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { EmptyState } from '../components/ui/EmptyState';
import { formatRunDuration, formatTime } from '../utils/format';
import { shortPath } from '../utils/path';

const STATUS_FILTERS = ['all', 'running', 'success', 'failed', 'canceled'] as const;
type StatusFilter = typeof STATUS_FILTERS[number];

const FILTER_STYLES: Record<string, string> = {
  running: 'border-amber-500 text-amber-400',
  success: 'border-blue-500 text-blue-400',
  failed: 'border-red-500 text-red-400',
};

export function RunsPage() {
  const daemon = useDaemon();
  const nav = useNavigation();
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [search, setSearch] = useState('');
  const [focusIndex, setFocusIndex] = useState(-1);
  const focusRef = useRef<HTMLTableRowElement>(null);

  const fetcher = useCallback(() => listRuns({ limit: 100 }), []);
  const { data, loading, refresh } = usePoll(fetcher, 3000, daemon.status === 'running');

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
      else if (e.key === 'Enter' && focusIndex >= 0 && focusIndex < count) { e.preventDefault(); nav.openRun(filteredRuns[focusIndex].id); }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [focusIndex, filteredRuns, nav]);

  useEffect(() => { setFocusIndex(-1); }, [statusFilter, search]);
  useEffect(() => { focusRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' }); }, [focusIndex]);

  return (
    <div>
      {/* Page header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-semibold text-foreground tracking-tight">Runs</h1>
          {loading && <p className="text-sm text-muted-foreground">Refreshing…</p>}
        </div>
        <Button variant="ghost" onClick={refresh} title="Refresh (R)">
          <RefreshCw size={14} />
        </Button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3 mb-4">
        <div className="flex gap-1">
          {STATUS_FILTERS.map(sf => {
            const isActive = statusFilter === sf;
            return (
              <Button
                key={sf}
                variant={isActive ? "outline" : "ghost"}
                size="xs"
                onClick={() => setStatusFilter(sf)}
                className={cn(isActive && sf !== 'all' && FILTER_STYLES[sf])}
              >
                {sf === 'all' ? 'All' : sf.charAt(0).toUpperCase() + sf.slice(1)}
              </Button>
            );
          })}
        </div>

        <Input
          type="text"
          placeholder="Search by blueprint or run ID..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="flex-1 min-w-[180px]"
        />
        {search && (
          <Button variant="ghost" size="xs" onClick={() => setSearch('')}>
            Clear
          </Button>
        )}
      </div>

      {/* Runs table */}
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        {filteredRuns.length === 0 ? (
          <EmptyState
            message={runs.length === 0 ? 'No runs' : 'No matching runs'}
            sub={runs.length === 0 ? 'Run a blueprint to see history here' : 'No runs matching current filters'}
          />
        ) : (
          <table className="w-full border-collapse">
            <thead>
              <tr>
                <th className="w-full text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50">Blueprint</th>
                <th className="whitespace-nowrap text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50">Status</th>
                <th className="whitespace-nowrap text-right px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50">Started</th>
                <th className="whitespace-nowrap text-right px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50">Duration</th>
              </tr>
            </thead>
            <tbody>
              {filteredRuns.map((run, i) => (
                <tr
                  key={run.id}
                  onClick={() => nav.openRun(run.id)}
                  ref={i === focusIndex ? focusRef : undefined}
                  className={cn(
                    "cursor-pointer transition-colors hover:bg-muted/50 border-t border-border",
                    i === focusIndex && "bg-accent/10 outline outline-1 outline-ring"
                  )}
                >
                  <td className="w-full px-5 py-3 text-sm text-muted-foreground">
                    <div className="font-mono text-sm">{shortPath(run.blueprint_path)}</div>
                    <div className="text-xs text-muted-foreground mt-px">{run.id.slice(-8)}</div>
                  </td>
                  <td className="whitespace-nowrap px-5 py-3">
                    <StatusBadge status={run.status} />
                  </td>
                  <td className="whitespace-nowrap text-right px-5 py-3 text-sm text-muted-foreground">
                    {run.started_at ? formatTime(run.started_at) : '—'}
                  </td>
                  <td className="whitespace-nowrap text-right px-5 py-3 text-sm font-mono">
                    {formatRunDuration(run)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {runs.length > 0 && (
        <p className="text-xs text-muted-foreground mt-2">
          Showing {filteredRuns.length} of {runs.length} runs
        </p>
      )}
    </div>
  );
}
