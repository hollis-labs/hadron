import { useCallback, useEffect, useRef, useState } from 'react';

interface AsyncResourceState<T> {
  data: T | null;
  loading: boolean;
  error: Error | null;
}

interface AsyncResourceOptions {
  enabled?: boolean;
  immediate?: boolean;
}

export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

export function useAsyncResource<T>(
  fetcher: () => Promise<T>,
  options: AsyncResourceOptions = {}
) {
  const { enabled = true, immediate = true } = options;
  const [state, setState] = useState<AsyncResourceState<T>>({
    data: null,
    loading: immediate && enabled,
    error: null,
  });
  const requestRef = useRef(0);

  const refresh = useCallback(async () => {
    if (!enabled) return null;
    const requestID = ++requestRef.current;
    setState(prev => ({ ...prev, loading: true, error: null }));
    try {
      const data = await fetcher();
      if (requestID === requestRef.current) {
        setState({ data, loading: false, error: null });
      }
      return data;
    } catch (err) {
      const error = toError(err);
      if (requestID === requestRef.current) {
        setState(prev => ({ ...prev, loading: false, error }));
      }
      return null;
    }
  }, [enabled, fetcher]);

  useEffect(() => {
    if (!immediate || !enabled) return;
    void refresh();
  }, [enabled, immediate, refresh]);

  return { ...state, refresh, setData: (data: T | null) => setState(prev => ({ ...prev, data })) };
}
