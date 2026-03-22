import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { toast } from 'sonner';
import { usePoll } from '../hooks/usePoll';
import { useRunEvents } from '../hooks/useRunEvents';
import { getRun, cancelRun } from '../api/client';
import { StatusBadge } from '../components/ui/StatusBadge';
import { Spinner } from '../components/ui/Spinner';
import { ChevronDown, ChevronRight, Copy, Square, RefreshCw } from 'lucide-react';
import { formatDuration } from '../utils/format';
import { shortPath } from '../utils/path';
import type { RunEvent } from '../api/types';

interface RunDetailPageProps {
  runId: string;
  onBack: () => void;
}

const TERMINAL = new Set(['success', 'failed', 'canceled', 'cancelled']);

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

const TASK_ICON: Record<string, { char: string; cls: string }> = {
  success: { char: '✓', cls: 'success' },
  failed: { char: '✗', cls: 'failed' },
  running: { char: '↻', cls: 'running' },
};

function eventTypeColor(eventType: string): string {
  if (eventType.includes('error') || eventType.includes('fail')) return 'var(--status-failed)';
  if (eventType.includes('start') || eventType.includes('complete') || eventType.includes('success')) return 'var(--status-success)';
  if (eventType.includes('log')) return 'var(--text-tertiary)';
  return 'var(--accent)';
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
  const completedCount = realGroups.filter(g => g.status === 'success' || g.status === 'failed').length;
  const progress = realGroups.length > 0 ? (completedCount / realGroups.length) * 100 : 0;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', gap: 'var(--space-4)' }}>
      {/* Run header */}
      {run && (
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 'var(--space-4)' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)' }}>
              <h1 className="mono" style={{ fontSize: 'var(--text-xl)', fontWeight: 600, letterSpacing: '-0.01em' }}>
                {shortPath(run.blueprint_path)}
              </h1>
              <StatusBadge status={run.status} />
              {!isTerminal && <Spinner size={14} />}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-4)', fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>
              <span>Duration: <strong style={{ color: 'var(--text-secondary)' }}>{formatDuration(run.started_at, run.ended_at)}</strong></span>
              <span className="mono" style={{ fontSize: 'var(--text-xs)', background: 'var(--bg-raised)', padding: '2px 6px', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border-subtle)' }}>
                {runId.slice(-8)}
              </span>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 'var(--space-2)', flexShrink: 0 }}>
            {!isTerminal && (
              <button className="btn btn-danger" onClick={handleCancel}>
                <Square size={12} /> Cancel
              </button>
            )}
            <button className="btn" onClick={() => { /* rerun placeholder */ }}>
              <RefreshCw size={12} /> Rerun
            </button>
          </div>
        </div>
      )}

      {/* Progress bar */}
      {realGroups.length > 0 && (
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 'var(--space-1)' }}>
            <span style={{ fontSize: 'var(--text-sm)', color: 'var(--text-secondary)', fontWeight: 500 }}>Task Progress</span>
            <span className="mono" style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>{completedCount} / {realGroups.length} tasks</span>
          </div>
          <div style={{ height: 4, background: 'var(--bg-overlay)', borderRadius: 2, overflow: 'hidden' }}>
            <div style={{
              height: '100%', borderRadius: 2, width: `${progress}%`,
              background: isTerminal && run?.status === 'failed' ? 'var(--status-failed)' : isTerminal ? 'var(--status-success)' : 'var(--status-running)',
              transition: 'width 0.5s ease',
            }} />
          </div>
        </div>
      )}

      {/* Error banner */}
      {run?.error_message && (
        <div className="section" style={{ padding: 'var(--space-3) var(--space-4)', color: 'var(--status-failed)', borderColor: 'rgba(239, 68, 68, 0.3)' }}>
          {run.error_message}
        </div>
      )}
      {runError && (
        <div className="section" style={{ padding: 'var(--space-3) var(--space-4)', color: 'var(--status-failed)', borderColor: 'rgba(239, 68, 68, 0.3)' }}>
          Error fetching run: {runError.message}
        </div>
      )}

      {/* View toggle */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span style={{ fontSize: 'var(--text-md)', fontWeight: 600 }}>Tasks</span>
        <div style={{ display: 'flex', background: 'var(--bg-raised)', border: '1px solid var(--border-default)', borderRadius: 'var(--radius-md)', padding: 2, gap: 2 }}>
          <button
            onClick={() => setViewMode('grouped')}
            style={{
              padding: '4px 12px', borderRadius: 'var(--radius-sm)', fontSize: 'var(--text-sm)',
              fontWeight: 500, fontFamily: 'var(--font-ui)', border: 'none', cursor: 'pointer',
              background: viewMode === 'grouped' ? 'var(--bg-active)' : 'transparent',
              color: viewMode === 'grouped' ? 'var(--text-primary)' : 'var(--text-tertiary)',
            }}
          >Grouped</button>
          <button
            onClick={() => setViewMode('raw')}
            style={{
              padding: '4px 12px', borderRadius: 'var(--radius-sm)', fontSize: 'var(--text-sm)',
              fontWeight: 500, fontFamily: 'var(--font-ui)', border: 'none', cursor: 'pointer',
              background: viewMode === 'raw' ? 'var(--bg-active)' : 'transparent',
              color: viewMode === 'raw' ? 'var(--text-primary)' : 'var(--text-tertiary)',
            }}
          >Raw</button>
        </div>
      </div>

      {/* Task list */}
      <div ref={logRef} style={{ flex: 1, minHeight: 0, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 'var(--space-2)' }}>
        {events.length === 0 ? (
          <div style={{ color: 'var(--text-tertiary)', padding: 'var(--space-8)', textAlign: 'center' }}>
            {!run ? 'Loading…' : isTerminal ? 'No events recorded.' : 'Waiting for events…'}
          </div>
        ) : viewMode === 'raw' ? (
          <div className="section" style={{ padding: 'var(--space-4)' }}>
            {events.map(ev => (
              <div key={ev.id} style={{ display: 'flex', gap: 'var(--space-3)', fontFamily: 'var(--font-mono)', fontSize: 'var(--text-sm)', lineHeight: 1.6, padding: '1px 0' }}>
                <span style={{ color: eventTypeColor(ev.event_type), flexShrink: 0, fontSize: 'var(--text-xs)', minWidth: 90 }}>
                  [{ev.event_type}]
                </span>
                <span style={{
                  color: ev.event_type === 'stderr' || ev.event_type.includes('error') ? 'var(--status-failed)' : 'var(--text-secondary)',
                  whiteSpace: 'pre-wrap', wordBreak: 'break-all',
                }}>
                  {ev.step_name && <span style={{ color: 'var(--accent)' }}>{ev.step_name} </span>}
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
              <div key={group.stepName} className="section" style={isRunning ? { borderColor: 'rgba(245, 158, 11, 0.2)' } : undefined}>
                <div
                  onClick={() => toggleGroup(group.stepName)}
                  style={{
                    display: 'flex', alignItems: 'center', padding: 'var(--space-3) var(--space-4)',
                    gap: 'var(--space-3)', cursor: 'pointer', transition: 'background 0.1s ease',
                  }}
                >
                  <div className={`badge badge-${icon.cls}`} style={{ width: 20, height: 20, borderRadius: '50%', padding: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, fontWeight: 700 }}>
                    {isRunning ? <Spinner size={12} /> : icon.char}
                  </div>
                  <span style={{ flex: 1, fontSize: 'var(--text-md)', fontWeight: 500 }}>{displayName}</span>
                  <span className="mono" style={{ fontSize: 'var(--text-sm)', color: isRunning ? 'var(--status-running)' : 'var(--text-tertiary)' }}>
                    {isRunning ? '(running)' : duration}
                  </span>
                  <button
                    className="btn btn-ghost"
                    onClick={(e) => {
                      e.stopPropagation();
                      const text = group.events.map(ev => ev.message ?? '').filter(Boolean).join('\n');
                      navigator.clipboard.writeText(text);
                      toast.success('Output copied');
                    }}
                    style={{ padding: '2px 6px' }}
                    title="Copy output"
                  >
                    <Copy size={13} />
                  </button>
                  <span style={{ color: 'var(--text-tertiary)', transition: 'transform 0.2s ease', transform: isExpanded ? 'rotate(180deg)' : 'none' }}>
                    <ChevronDown size={14} />
                  </span>
                </div>

                {isExpanded && (
                  <div style={{ borderTop: '1px solid var(--border-subtle)', background: 'var(--bg-base)', padding: 'var(--space-4)', maxHeight: 300, overflowY: 'auto' }}>
                    {group.events.map(ev => (
                      <div key={ev.id} style={{ display: 'flex', gap: 'var(--space-3)', fontFamily: 'var(--font-mono)', fontSize: 'var(--text-sm)', lineHeight: 1.6, padding: '1px 0' }}>
                        <span style={{ color: 'var(--text-tertiary)', flexShrink: 0, fontSize: 'var(--text-xs)', minWidth: 72, userSelect: 'none' }}>
                          {ev.created_at ? new Date(ev.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : ''}
                        </span>
                        <span style={{
                          whiteSpace: 'pre-wrap', wordBreak: 'break-all',
                          color: ev.event_type === 'stderr' || ev.event_type.includes('error') ? 'var(--status-failed)'
                            : ev.event_type.includes('success') || ev.event_type.includes('complete') ? 'var(--status-success)'
                            : 'var(--text-secondary)',
                        }}>
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
