import { useState, useEffect, useRef, useCallback } from 'react';
import { listRunEvents } from '../api/client';
import type { RunEvent } from '../api/types';

export function useRunEvents(runId: string, enabled: boolean): RunEvent[] {
  const [events, setEvents] = useState<RunEvent[]>([]);
  const cursorRef = useRef<string | undefined>(undefined);
  const runIdRef = useRef(runId);

  // Reset state when switching to a different run
  useEffect(() => {
    if (runIdRef.current !== runId) {
      runIdRef.current = runId;
      setEvents([]);
      cursorRef.current = undefined;
    }
  }, [runId]);

  const fetchEvents = useCallback(async () => {
    if (!runId) return;
    try {
      const res = await listRunEvents(runId, {
        limit: 200,
        cursor: cursorRef.current,
      });
      if (res.items && res.items.length > 0) {
        if (res.next_cursor) {
          cursorRef.current = res.next_cursor;
        }
        setEvents(prev => {
          const ids = new Set(prev.map(e => e.id));
          const newItems = res.items.filter(e => !ids.has(e.id));
          return newItems.length > 0 ? [...prev, ...newItems] : prev;
        });
      }
    } catch {
      // swallow transient network errors
    }
  }, [runId]);

  useEffect(() => {
    if (!runId) return;

    if (!enabled) {
      // Run just became terminal. The execution goroutine may have written events
      // a few ms after the status was set. Wait briefly then do a final fetch.
      const t = setTimeout(fetchEvents, 500);
      return () => clearTimeout(t);
    }

    // Run is active — poll continuously.
    fetchEvents();
    const timer = setInterval(fetchEvents, 1500);
    return () => clearInterval(timer);
  }, [enabled, runId, fetchEvents]);

  return events;
}
