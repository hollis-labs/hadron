import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { toast } from 'sonner';
import { useNavigation } from '../contexts/NavigationContext';
import { usePoll } from '../hooks/usePoll';
import { useRunEvents } from '../hooks/useRunEvents';
import { getRun, cancelRun } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { Spinner } from '../components/ui/Spinner';
import { ChevronDown, Copy, Square, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { formatDuration } from '../utils/format';
import { shortPath } from '../utils/path';
import type { RunEvent } from '../api/types';

const TERMINAL = new Set(['success', 'failed', 'canceled', 'cancelled']);

// ── Task grouping ────────────────────────────────────────────────────

interface TaskGroup {
  stepName: string;
  status: 'pending' | 'running' | 'success' | 'failed' | 'skipped';
  events: RunEvent[];
  startedAt?: string;
  endedAt?: string;
}

// Status priority: higher number = more terminal. Only allow forward transitions.
const STATUS_RANK: Record<string, number> = {
  pending: 0,
  running: 1,
  success: 2,
  skipped: 2,
  failed: 2,
};

function groupEventsByStep(events: RunEvent[]): TaskGroup[] {
  // Events may arrive in DESC order from the API — sort ascending by id for correct status progression.
  const sorted = [...events].sort((a, b) => a.id - b.id);

  const groups = new Map<string, TaskGroup>();
  const order: string[] = [];

  for (const event of sorted) {
    const step = event.step_name || '__global__';
    if (!groups.has(step)) {
      groups.set(step, { stepName: step, status: 'pending', events: [] });
      order.push(step);
    }
    const group = groups.get(step)!;
    group.events.push(event);

    const t = event.event_type;
    let nextStatus: TaskGroup['status'] | null = null;

    if (t === 'step_start' || t === 'task_start') {
      nextStatus = 'running';
      if (!group.startedAt) group.startedAt = event.created_at;
    } else if (t === 'step_success' || t === 'step_call_success' || t === 'step_end' || t === 'task_end' || t === 'success' || t.includes('complete')) {
      nextStatus = 'success';
      group.endedAt = event.created_at;
    } else if (t === 'step_skipped' || t === 'step_skipped_condition') {
      nextStatus = 'skipped';
      group.endedAt = event.created_at;
    } else if (t === 'step_skipped_error') {
      // continue_on_error was set — override prior step_error, mark as success (non-fatal)
      nextStatus = 'success';
      group.endedAt = event.created_at;
    } else if (t === 'step_error' || t === 'step_call_error' || t === 'error' || t === 'failed') {
      nextStatus = 'failed';
      group.endedAt = event.created_at;
    }

    // Only allow forward status transitions (pending → running → terminal)
    if (nextStatus && (STATUS_RANK[nextStatus] ?? 0) >= (STATUS_RANK[group.status] ?? 0)) {
      group.status = nextStatus;
    }
  }

  return order.map(step => groups.get(step)!);
}

const TASK_ICON: Record<string, { char: string; cls: string }> = {
  success: { char: '✓', cls: 'success' },
  failed: { char: '✗', cls: 'failed' },
  running: { char: '↻', cls: 'running' },
  skipped: { char: '—', cls: 'queued' },
};

function eventTypeColorClass(eventType: string): string {
  if (eventType.includes('error') || eventType.includes('fail')) return 'text-red-400';
  if (eventType.includes('start') || eventType.includes('complete') || eventType.includes('success')) return 'text-blue-400';
  if (eventType.includes('log')) return 'text-muted-foreground';
  return 'text-accent';
}

function eventMessageColorClass(eventType: string): string {
  if (eventType === 'stderr' || eventType.includes('error')) return 'text-red-400';
  if (eventType.includes('success') || eventType.includes('complete')) return 'text-blue-400';
  return 'text-muted-foreground';
}

// ── Main component ───────────────────────────────────────────────────

export function RunDetailPage() {
  const nav = useNavigation();
  const runId = nav.selectedRunId!;
  const fetcher = useCallback(() => getRun(runId), [runId]);
  const { data: run, error: runError } = usePoll(fetcher, 2000, true);
  const isTerminal = run ? TERMINAL.has(run.status) : false;
  const events = useRunEvents(runId, !isTerminal);
  const logRef = useRef<HTMLDivElement>(null);
  const [viewMode, setViewMode] = useState<'grouped' | 'raw'>('grouped');
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

  const taskGroups = useMemo(() => groupEventsByStep(events), [events]);

  useEffect(() => {
    setExpandedGroups(prev => {
      const next = new Set(prev);
      let changed = false;
      for (const g of taskGroups) {
        if (g.status === 'running' && !next.has(g.stepName)) {
          next.add(g.stepName);
          changed = true;
        } else if ((g.status === 'success' || g.status === 'failed' || g.status === 'skipped') && next.has(g.stepName)) {
          next.delete(g.stepName);
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [taskGroups]);

  useEffect(() => {
    if (!isTerminal && logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [events, isTerminal]);

  const handleCancel = async () => {
    try {
      await cancelRun(runId);
      toast.success('Run canceled');
    } catch { /* ignore */ }
  };

  const toggleGroup = (stepName: string) => {
    setExpandedGroups(prev => {
      const next = new Set(prev);
      if (next.has(stepName)) next.delete(stepName); else next.add(stepName);
      return next;
    });
  };

  const realGroups = taskGroups.filter(g => g.stepName !== '__global__');
  const completedCount = realGroups.filter(g => g.status === 'success' || g.status === 'failed' || g.status === 'skipped').length;
  const progress = realGroups.length > 0 ? (completedCount / realGroups.length) * 100 : 0;

  return (
    <div className="flex flex-col h-full gap-4">
      {/* Run header */}
      {run && (
        <div className="flex items-start justify-between gap-4">
          <div className="flex flex-col gap-2">
            <div className="flex items-center gap-3">
              <h1 className="font-mono text-xl font-semibold tracking-tight">
                {shortPath(run.blueprint_path)}
              </h1>
              <StatusBadge status={run.status} />
              {!isTerminal && <Spinner size={14} />}
            </div>
            <div className="flex items-center gap-4 text-sm text-muted-foreground">
              <span>Duration: <strong className="text-foreground">{formatDuration(run.started_at, run.ended_at)}</strong></span>
              <span className="font-mono text-xs bg-muted px-1.5 py-0.5 rounded border border-border">
                {runId.slice(-8)}
              </span>
            </div>
          </div>
          <div className="flex gap-2 shrink-0">
            {!isTerminal && (
              <Button variant="destructive" onClick={handleCancel}>
                <Square size={12} /> Cancel
              </Button>
            )}
            <Button variant="outline" onClick={() => { /* rerun placeholder */ }}>
              <RefreshCw size={12} /> Rerun
            </Button>
          </div>
        </div>
      )}

      {/* Progress bar */}
      {realGroups.length > 0 && (
        <div>
          <div className="flex justify-between mb-1">
            <span className="text-sm text-foreground font-medium">Task Progress</span>
            <span className="font-mono text-sm text-muted-foreground">{completedCount} / {realGroups.length} tasks</span>
          </div>
          <div className="w-full h-1 rounded-full bg-muted overflow-hidden">
            <div
              className={cn(
                "h-full rounded-full transition-all duration-500",
                isTerminal && run?.status === 'failed' ? 'bg-red-400' : isTerminal ? 'bg-blue-400' : 'bg-amber-400'
              )}
              style={{ width: `${progress}%` }}
            />
          </div>
        </div>
      )}

      {/* Error banner */}
      {run?.error_message && (
        <div className="rounded-lg border border-red-500/30 bg-card overflow-hidden px-4 py-3 text-red-400">
          {run.error_message}
        </div>
      )}
      {runError && (
        <div className="rounded-lg border border-red-500/30 bg-card overflow-hidden px-4 py-3 text-red-400">
          Error fetching run: {runError.message}
        </div>
      )}

      {/* View toggle */}
      <div className="flex items-center justify-between">
        <span className="text-sm font-semibold text-foreground">Tasks</span>
        <div className="flex bg-muted border border-border rounded-md p-0.5 gap-0.5">
          <button
            onClick={() => setViewMode('grouped')}
            className={cn(
              "px-3 py-1 rounded text-sm font-medium border-none cursor-pointer transition-colors",
              viewMode === 'grouped'
                ? "bg-background text-foreground"
                : "bg-transparent text-muted-foreground hover:text-foreground"
            )}
          >Grouped</button>
          <button
            onClick={() => setViewMode('raw')}
            className={cn(
              "px-3 py-1 rounded text-sm font-medium border-none cursor-pointer transition-colors",
              viewMode === 'raw'
                ? "bg-background text-foreground"
                : "bg-transparent text-muted-foreground hover:text-foreground"
            )}
          >Raw</button>
        </div>
      </div>

      {/* Task list */}
      <div ref={logRef} className="flex-1 min-h-0 overflow-y-auto flex flex-col gap-2">
        {events.length === 0 ? (
          <div className="text-muted-foreground p-8 text-center">
            {!run ? 'Loading...' : isTerminal ? 'No events recorded.' : 'Waiting for events...'}
          </div>
        ) : viewMode === 'raw' ? (
          <div className="rounded-lg border border-border bg-card overflow-hidden p-4">
            {events.map(ev => (
              <div key={ev.id} className="flex gap-3 font-mono text-sm leading-relaxed py-px">
                <span className={cn("shrink-0 text-xs min-w-[90px]", eventTypeColorClass(ev.event_type))}>
                  [{ev.event_type}]
                </span>
                <span className={cn(
                  "whitespace-pre-wrap break-all",
                  ev.event_type === 'stderr' || ev.event_type.includes('error') ? 'text-red-400' : 'text-muted-foreground'
                )}>
                  {ev.step_name && <span className="text-accent">{ev.step_name} </span>}
                  {ev.message ?? ''}
                </span>
              </div>
            ))}
          </div>
        ) : (
          taskGroups.map(group => {
            const icon = TASK_ICON[group.status] ?? { char: '◻', cls: 'queued' };
            const isExpanded = expandedGroups.has(group.stepName);
            const displayName = group.stepName === '__global__' ? 'Global' : group.stepName;
            const duration = group.startedAt ? formatDuration(group.startedAt, group.endedAt) : '';
            const isRunning = group.status === 'running';

            return (
              <div
                key={group.stepName}
                className="rounded-lg border border-border bg-card overflow-hidden"
                style={isRunning ? { borderColor: 'rgba(245, 158, 11, 0.2)' } : undefined}
              >
                <div
                  onClick={() => toggleGroup(group.stepName)}
                  className="flex items-center px-4 py-3 gap-3 cursor-pointer transition-colors hover:bg-muted/50"
                >
                  <span className={cn("flex size-5 items-center justify-center rounded-full text-xs font-bold", {
                    "bg-blue-500/20 text-blue-400": icon.cls === "success",
                    "bg-amber-500/20 text-amber-400": icon.cls === "running",
                    "bg-red-500/20 text-red-400": icon.cls === "failed",
                    "bg-zinc-500/20 text-zinc-400": icon.cls === "queued",
                    "bg-purple-500/20 text-purple-400": icon.cls === "canceled",
                  })}>
                    {isRunning ? <Spinner size={12} /> : icon.char}
                  </span>
                  <span className="flex-1 text-sm font-medium text-foreground">{displayName}</span>
                  <span className={cn(
                    "font-mono text-sm",
                    isRunning ? "text-amber-400" : "text-muted-foreground"
                  )}>
                    {isRunning ? '(running)' : duration}
                  </span>
                  <Button
                    variant="ghost"
                    size="xs"
                    onClick={(e) => {
                      e.stopPropagation();
                      const text = group.events.map(ev => ev.message ?? '').filter(Boolean).join('\n');
                      navigator.clipboard.writeText(text);
                      toast.success('Output copied');
                    }}
                    title="Copy output"
                  >
                    <Copy size={13} />
                  </Button>
                  <span className={cn(
                    "text-muted-foreground transition-transform duration-200",
                    isExpanded && "rotate-180"
                  )}>
                    <ChevronDown size={14} />
                  </span>
                </div>

                {isExpanded && (
                  <div className="border-t border-border bg-background p-4 max-h-[300px] overflow-y-auto">
                    {group.events.map(ev => (
                      <div key={ev.id} className="flex gap-3 font-mono text-sm leading-relaxed py-px">
                        <span className="text-muted-foreground shrink-0 text-xs min-w-[72px] select-none">
                          {ev.created_at ? new Date(ev.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : ''}
                        </span>
                        <span className={cn(
                          "whitespace-pre-wrap break-all",
                          eventMessageColorClass(ev.event_type)
                        )}>
                          {ev.message ?? ''}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
