import { useState, useEffect, useCallback, useRef } from 'react';

interface PollResult<T> {
  data: T | null;
  loading: boolean;
  error: Error | null;
  refresh: () => void;
}

export function usePoll<T>(
  fetcher: () => Promise<T>,
  interval: number,
  enabled = true
): PollResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const fetcherRef = useRef(fetcher);
  const counterRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Keep the fetcher ref current without resetting the interval
  useEffect(() => {
    fetcherRef.current = fetcher;
  }, [fetcher]);

  const runFetch = useCallback(async () => {
    const id = ++counterRef.current;
    setLoading(true);
    try {
      const result = await fetcherRef.current();
      if (id === counterRef.current) {
        setData(result);
        setError(null);
      }
    } catch (err) {
      if (id === counterRef.current) {
        setError(err instanceof Error ? err : new Error(String(err)));
      }
    } finally {
      if (id === counterRef.current) {
        setLoading(false);
      }
    }
  }, []);

  const refresh = useCallback(() => {
    runFetch();
  }, [runFetch]);

  useEffect(() => {
    if (!enabled) return;

    const startTimer = () => {
      timerRef.current = setInterval(runFetch, interval);
    };

    const stopTimer = () => {
      if (timerRef.current !== null) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
    };

    const onVisibility = () => {
      if (document.hidden) {
        stopTimer();
      } else {
        runFetch();
        startTimer();
      }
    };

    // Initial fetch and timer
    runFetch();
    startTimer();

    document.addEventListener('visibilitychange', onVisibility);

    return () => {
      stopTimer();
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [enabled, interval, runFetch]);

  return { data, loading, error, refresh };
}
