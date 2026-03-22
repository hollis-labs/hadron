import type { Run } from '../api/types';

/** Format milliseconds into a human-readable duration string. */
export function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

/** Format a duration from start/end timestamp strings. */
export function formatDuration(startedAt?: string | null, endedAt?: string | null): string {
  if (!startedAt) return '—';
  const end = endedAt ? new Date(endedAt) : new Date();
  const ms = end.getTime() - new Date(startedAt).getTime();
  if (ms < 0) return '—';
  return formatMs(ms);
}

/** Format duration directly from a Run object. */
export function formatRunDuration(run: Run): string {
  return formatDuration(run.started_at, run.ended_at);
}

/** Format a timestamp as HH:MM:SS. */
export function formatTime(ts?: string | null): string {
  if (!ts) return '—';
  return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

/** Format a timestamp with millisecond precision. */
export function formatTimestamp(ts: string): string {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    if (isNaN(d.getTime())) return '—';
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3 } as Intl.DateTimeFormatOptions);
  } catch { return '—'; }
}

/** Format a timestamp as "Mon DD, HH:MM". */
export function formatDate(ts: string): string {
  if (!ts) return '—';
  try {
    const d = new Date(ts);
    if (isNaN(d.getTime())) return '—';
    return d.toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  } catch { return '—'; }
}

/** Format a next-run timestamp for schedule display. */
export function formatNextRun(ts: string | null | undefined): string {
  if (!ts) return '—';
  return new Date(ts).toLocaleString([], {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

/** Format bytes into a human-readable file size. */
export function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/** Compute average duration in ms across completed runs. */
export function computeAvgDuration(runs: Run[]): number {
  const completed = runs.filter(r => r.started_at && r.ended_at);
  if (completed.length === 0) return 0;
  const totalMs = completed.reduce((sum, r) => {
    return sum + (new Date(r.ended_at!).getTime() - new Date(r.started_at!).getTime());
  }, 0);
  return Math.round(totalMs / completed.length);
}
