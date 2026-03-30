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

const CHIP_COLORS: Record<string, string> = {
  running: 'text-amber-400 border-amber-500/40 bg-amber-500/[0.06]',
  success: 'text-blue-400 border-blue-500/40 bg-blue-500/[0.06]',
  failed: 'text-red-400 border-red-500/40 bg-red-500/[0.06]',
  canceled: 'text-muted-foreground border-border/60 bg-muted/30',
};

export function RunsPage() {
  const daemon = useDaemon();
  const nav = useNavigation();
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [search, setSearch] = useState('');
  const [focusIndex, setFocusIndex] = useState(-1);
  const focusRef = useRef<HTMLDivElement>(null);

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
    <div className="flex flex-col gap-2 h-full">
      {/* Page header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold text-foreground tracking-tight">Runs</h1>
          {loading && <p className="text-sm text-muted-foreground">Refreshing…</p>}
        </div>
        <Button variant="ghost" onClick={refresh} title="Refresh (R)">
          <RefreshCw size={14} />
        </Button>
      </div>

      {/* Search + Filter chips */}
      <div className="flex items-center gap-2">
        <Input
          type="text"
          placeholder="Search by blueprint or run ID..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Escape') {
              if (search) { setSearch(''); e.stopPropagation(); }
              else { (e.target as HTMLInputElement).blur(); }
            }
          }}
          className="flex-1 h-10 border-border/60 text-sm placeholder:text-muted-foreground/50 focus-visible:border-blue-500 focus-visible:ring-0 dark:focus-visible:bg-blue-500/10 focus-visible:shadow-[inset_0_0_12px_rgba(59,130,246,0.08),0_0_8px_rgba(59,130,246,0.06)] focus-visible:text-blue-100 transition-all"
        />
        {search && (
          <Button variant="ghost" size="xs" onClick={() => setSearch('')}>
            Clear
          </Button>
        )}
        <div className="flex gap-1">
          {STATUS_FILTERS.map(sf => {
            const isActive = statusFilter === sf;
            return (
              <button
                key={sf}
                onClick={() => setStatusFilter(sf)}
                className={cn(
                  'h-8 px-3 rounded-md text-xs font-medium transition-colors',
                  'border border-border/60 bg-transparent',
                  'hover:bg-muted/60 hover:text-foreground',
                  isActive
                    ? (sf !== 'all' && CHIP_COLORS[sf]) || 'text-blue-400 border-blue-500/40 bg-blue-500/[0.06]'
                    : 'text-muted-foreground',
                )}
              >
                {sf === 'all' ? 'All' : sf.charAt(0).toUpperCase() + sf.slice(1)}
              </button>
            );
          })}
        </div>
      </div>

      {/* Run list */}
      <div className="flex flex-col gap-px flex-1 overflow-y-auto">
        {filteredRuns.length === 0 ? (
          <EmptyState
            message={runs.length === 0 ? 'No runs' : 'No matching runs'}
            sub={runs.length === 0 ? 'Run a blueprint to see history here' : 'No runs matching current filters'}
          />
        ) : (
          filteredRuns.map((run, i) => (
            <div
              key={run.id}
              ref={i === focusIndex ? focusRef : undefined}
              onClick={() => nav.openRun(run.id)}
              className={cn(
                'flex items-center gap-3 px-3 py-1.5 rounded cursor-pointer transition-colors',
                'hover:bg-blue-500/[0.06] hover:border hover:border-blue-500/30',
                'border border-transparent',
                i === focusIndex && 'bg-blue-500/[0.06] border-blue-500/30',
              )}
            >
              {/* Blueprint + run ID */}
              <div className="flex-1 min-w-0">
                <div className="font-mono text-sm truncate">{shortPath(run.blueprint_path)}</div>
                <div className="text-xs text-muted-foreground mt-px">{run.id.slice(-8)}</div>
              </div>

              {/* Status */}
              <div className="shrink-0">
                <StatusBadge status={run.status} />
              </div>

              {/* Started */}
              <div className="shrink-0 text-sm text-muted-foreground whitespace-nowrap w-28 text-right">
                {run.started_at ? formatTime(run.started_at) : '—'}
              </div>

              {/* Duration */}
              <div className="shrink-0 text-sm font-mono text-muted-foreground whitespace-nowrap w-16 text-right">
                {formatRunDuration(run)}
              </div>
            </div>
          ))
        )}
      </div>

      {runs.length > 0 && (
        <p className="text-xs text-muted-foreground">
          Showing {filteredRuns.length} of {runs.length} runs
        </p>
      )}
    </div>
  );
}
