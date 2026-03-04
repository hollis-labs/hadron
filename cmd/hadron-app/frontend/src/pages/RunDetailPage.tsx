import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { toast } from 'sonner';
import { usePoll } from '../hooks/usePoll';
import { useRunEvents } from '../hooks/useRunEvents';
import { getRun, cancelRun } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { Spinner } from '../components/ui/Spinner';
import { ChevronLeft, ChevronDown, ChevronRight } from 'lucide-react';
import type { RunEvent } from '../api/types';

interface RunDetailPageProps {
  runId: string;
  onBack: () => void;
}

const TERMINAL = new Set(['success', 'failed', 'canceled', 'cancelled']);

function formatDuration(startedAt?: string | null, endedAt?: string | null): string {
  if (!startedAt) return '—';
  const end = endedAt ? new Date(endedAt) : new Date();
  const ms = end.getTime() - new Date(startedAt).getTime();
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

function shortPath(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts.slice(-2).join('/');
}

// ── Task grouping ────────────────────────────────────────────────────

interface TaskGroup {
  stepName: string;
  status: 'pending' | 'running' | 'success' | 'failed' | 'skipped';
  events: RunEvent[];
  startedAt?: string;
  endedAt?: string;
}

function groupEventsByStep(events: RunEvent[]): TaskGroup[] {
  const groups = new Map<string, TaskGroup>();
  const order: string[] = [];

  for (const event of events) {
    const step = event.step_name || '__global__';
    if (!groups.has(step)) {
      groups.set(step, { stepName: step, status: 'pending', events: [] });
      order.push(step);
    }
    const group = groups.get(step)!;
    group.events.push(event);

    const t = event.event_type;
    if (t === 'step_start' || t === 'task_start') {
      group.status = 'running';
      if (!group.startedAt) group.startedAt = event.created_at;
    } else if (t === 'step_end' || t === 'task_end' || t === 'success' || t.includes('complete')) {
      group.status = 'success';
      group.endedAt = event.created_at;
    } else if (t === 'error' || t === 'failed' || t.includes('fail')) {
      group.status = 'failed';
      group.endedAt = event.created_at;
    }
  }

  return order.map(step => groups.get(step)!);
}

function statusIcon(status: string): { char: string; color: string } {
  switch (status) {
    case 'success': return { char: '\u2713', color: 'rgb(var(--ok))' };
    case 'failed': return { char: '\u2717', color: 'rgb(var(--danger))' };
    case 'running': return { char: '\u21BB', color: 'rgb(var(--warn))' };
    default: return { char: '\u25FB', color: 'rgb(var(--muted))' };
  }
}

// ── Raw event row (for fallback view) ────────────────────────────────

function eventTypeColor(eventType: string): string {
  if (eventType.includes('error') || eventType.includes('fail')) return 'rgb(var(--danger))';
  if (eventType.includes('start') || eventType.includes('queued')) return 'rgb(var(--ok))';
  if (eventType.includes('complete') || eventType.includes('success')) return 'rgb(var(--ok))';
  if (eventType.includes('log')) return 'rgb(var(--muted))';
  return 'rgb(var(--accent))';
}

function EventRow({ ev }: { ev: RunEvent }) {
  return (
    <div className="event-row">
      <span className="event-type" style={{ color: eventTypeColor(ev.event_type) }}>
        [{ev.event_type}]
      </span>
      <span className="event-msg">
        {ev.step_name && (
          <span style={{ color: 'rgb(var(--accent))' }}>{ev.step_name} </span>
        )}
        {ev.message ?? ''}
      </span>
    </div>
  );
}

// ── Main component ───────────────────────────────────────────────────

export function RunDetailPage({ runId, onBack }: RunDetailPageProps) {
  const fetcher = useCallback(() => getRun(runId), [runId]);
  const { data: run, error: runError } = usePoll(fetcher, 2000, true);
  const isTerminal = run ? TERMINAL.has(run.status) : false;
  const events = useRunEvents(runId, !isTerminal);
  const logRef = useRef<HTMLDivElement>(null);
  const [viewMode, setViewMode] = useState<'grouped' | 'raw'>('grouped');
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

  const taskGroups = useMemo(() => groupEventsByStep(events), [events]);

  // Auto-expand running tasks
  useEffect(() => {
    const running = taskGroups.filter(g => g.status === 'running').map(g => g.stepName);
    if (running.length > 0) {
      setExpandedGroups(prev => {
        const next = new Set(prev);
        running.forEach(s => next.add(s));
        return next;
      });
    }
  }, [taskGroups]);

  // Auto-scroll
  useEffect(() => {
    if (!isTerminal && logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [events, isTerminal]);

  const handleCancel = async () => {
    try {
      await cancelRun(runId);
      toast.success('Run canceled');
    } catch {
      // Ignore
    }
  };

  const toggleGroup = (stepName: string) => {
    setExpandedGroups(prev => {
      const next = new Set(prev);
      if (next.has(stepName)) next.delete(stepName); else next.add(stepName);
      return next;
    });
  };

  // Progress calculation
  const realGroups = taskGroups.filter(g => g.stepName !== '__global__');
  const completedCount = realGroups.filter(g => g.status === 'success' || g.status === 'failed').length;
  const progress = realGroups.length > 0 ? (completedCount / realGroups.length) * 100 : 0;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: '0.75rem' }}>
      {/* Header */}
      <div className="page-header" style={{ gap: '0.5rem' }}>
        <button className="hud-button-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <ChevronLeft size={13} /> Back
        </button>
        <span className="page-title">Run Detail</span>
        {run && !isTerminal && <Spinner size={14} />}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: '0.25rem' }}>
          <button
            className={viewMode === 'grouped' ? 'hud-button' : 'hud-button-ghost'}
            onClick={() => setViewMode('grouped')}
            style={{ fontSize: '0.68rem', padding: '0.2rem 0.5rem' }}
          >
            Grouped
          </button>
          <button
            className={viewMode === 'raw' ? 'hud-button' : 'hud-button-ghost'}
            onClick={() => setViewMode('raw')}
            style={{ fontSize: '0.68rem', padding: '0.2rem 0.5rem' }}
          >
            Raw
          </button>
        </div>
      </div>

      {/* Run meta */}
      {run && (
        <div className="hud-panel" style={{ padding: '0.75rem 1rem' }}>
          <div className="run-meta">
            <div className="run-meta-item">
              <strong>{shortPath(run.blueprint_path)}</strong>
            </div>
            <div className="run-meta-item">
              Status: <StatusBadge status={run.status} />
            </div>
            <div className="run-meta-item">
              Duration: <strong>{formatDuration(run.started_at, run.ended_at)}</strong>
            </div>
            <div className="run-meta-item" style={{ color: 'rgb(var(--muted))', fontSize: '0.75rem', fontFamily: 'monospace' }}>
              {runId}
            </div>
            {!isTerminal && (
              <button
                className="hud-button"
                onClick={handleCancel}
                style={{ marginLeft: 'auto', color: 'rgb(var(--danger))', borderColor: 'rgba(var(--danger) / 0.4)' }}
              >
                Cancel
              </button>
            )}
          </div>
          {run.error_message && (
            <div style={{ marginTop: '0.5rem', color: 'rgb(var(--danger))', fontSize: '0.8rem' }}>
              Error: {run.error_message}
            </div>
          )}
        </div>
      )}

      {/* Progress bar */}
      {realGroups.length > 0 && (
        <div className="run-progress-bar">
          <div className="run-progress-fill" style={{ width: `${progress}%` }} />
        </div>
      )}

      {/* Fetch error */}
      {runError && (
        <div style={{ color: 'rgb(var(--danger))', fontSize: '0.8rem', padding: '0.5rem 0.75rem', background: 'rgba(var(--danger) / 0.1)', borderRadius: '4px', border: '1px solid rgba(var(--danger) / 0.3)' }}>
          Error fetching run: {runError.message}
        </div>
      )}

      {/* Event display */}
      <div
        ref={logRef}
        className="event-log"
        style={{ flex: 1, minHeight: 0 }}
      >
        {events.length === 0 ? (
          <span style={{ color: 'rgb(var(--muted))' }}>
            {!run
              ? 'Loading\u2026'
              : isTerminal
                ? 'No events recorded.'
                : 'Waiting for events\u2026'}
          </span>
        ) : viewMode === 'raw' ? (
          events.map(ev => <EventRow key={ev.id} ev={ev} />)
        ) : (
          taskGroups.map(group => {
            const icon = statusIcon(group.status);
            const isExpanded = expandedGroups.has(group.stepName);
            const displayName = group.stepName === '__global__' ? 'Global' : group.stepName;
            const duration = group.startedAt
              ? formatDuration(group.startedAt, group.endedAt)
              : '';

            return (
              <div key={group.stepName} className="run-task-group">
                <div className="run-task-header" onClick={() => toggleGroup(group.stepName)}>
                  {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                  <span style={{ color: icon.color, fontSize: '1rem', lineHeight: 1, width: '16px', textAlign: 'center' }}>
                    {group.status === 'running' ? <span className="pulse-running">{icon.char}</span> : icon.char}
                  </span>
                  <span style={{ flex: 1, fontSize: '0.82rem', fontFamily: 'monospace' }}>{displayName}</span>
                  <span style={{ fontSize: '0.72rem', color: 'rgb(var(--muted))' }}>
                    {group.status === 'running' ? '(running)' : duration}
                  </span>
                  <button
                    className="hud-button-ghost"
                    onClick={(e) => {
                      e.stopPropagation();
                      const text = group.events.map(ev => ev.message ?? '').filter(Boolean).join('\n');
                      navigator.clipboard.writeText(text);
                      toast.success('Output copied');
                    }}
                    style={{ fontSize: '0.65rem', padding: '0.15rem 0.4rem', opacity: 0.6 }}
                    title="Copy output to clipboard"
                  >
                    Copy
                  </button>
                </div>

                {isExpanded && (
                  <div className="run-task-logs">
                    {group.events.map(ev => (
                      <div key={ev.id} style={{ padding: '0.1rem 0' }}>
                        <span style={{ color: eventTypeColor(ev.event_type), fontSize: '0.68rem', marginRight: '0.4rem' }}>
                          [{ev.event_type}]
                        </span>
                        <span style={{
                          fontSize: '0.78rem',
                          ...(ev.event_type === 'stderr' || ev.event_type.includes('error') || ev.event_type.includes('fail')
                            ? { color: 'rgb(var(--danger))' }
                            : ev.event_type === 'stdout'
                              ? { fontFamily: 'monospace' }
                              : {}),
                        }}>{ev.message ?? ''}</span>
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
