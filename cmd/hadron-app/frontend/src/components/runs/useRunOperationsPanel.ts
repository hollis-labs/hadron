import { useCallback, useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
import { listRunOperations } from '../../api/client';
import type { OperationDiagnostic } from '../../api/types';
import {
  filterVisibleOperations,
  restoreOperationsPanelState,
  summarizeOperationStatuses,
  type SavedOperationsPanelState,
} from './runOperationsPanel.helpers';

const OPERATIONS_PAGE_SIZE = 25;

const savedOperationsPanelStateByRun = new Map<string, SavedOperationsPanelState>();

export function useRunOperationsPanel({ runId, isTerminal }: { runId: string; isTerminal: boolean }) {
  const [operationKind, setOperationKind] = useState('');
  const [operationSearch, setOperationSearch] = useState('');
  const [operations, setOperations] = useState<OperationDiagnostic[]>([]);
  const [operationsLoaded, setOperationsLoaded] = useState(false);
  const [operationsTotalCount, setOperationsTotalCount] = useState(0);
  const [operationsLoadingMore, setOperationsLoadingMore] = useState(false);
  const [activeOperationSequence, setActiveOperationSequence] = useState<number | null>(null);
  const [expandedOperations, setExpandedOperations] = useState<Set<number>>(new Set());
  const [expandedOperationPayloads, setExpandedOperationPayloads] = useState<Set<number>>(new Set());
  const [scrollPositions, setScrollPositions] = useState<Record<string, number>>({});

  const operationsCursor = operations.length > 0 ? String(operations[operations.length - 1].sequence) : undefined;
  const hasMoreOperations = operations.length < operationsTotalCount;
  const normalizedOperationSearch = operationSearch.trim().toLowerCase();
  const visibleOperations = useMemo(
    () => filterVisibleOperations(operations, normalizedOperationSearch),
    [operations, normalizedOperationSearch],
  );
  const visibleStatusSummary = useMemo(() => summarizeOperationStatuses(visibleOperations), [visibleOperations]);
  const loadedStatusSummary = useMemo(() => summarizeOperationStatuses(operations), [operations]);
  const isInitialLoading = !operationsLoaded && operations.length === 0;
  const remainingOperations = Math.max(operationsTotalCount - operations.length, 0);

  useEffect(() => {
    const saved = restoreOperationsPanelState(savedOperationsPanelStateByRun.get(runId));
    setOperationKind(saved.operationKind);
    setOperationSearch(saved.operationSearch);
    setActiveOperationSequence(saved.activeOperationSequence);
    setExpandedOperations(new Set(saved.expandedOperations));
    setExpandedOperationPayloads(new Set(saved.expandedOperationPayloads));
    setScrollPositions(saved.scrollPositions);
    setOperations([]);
    setOperationsLoaded(false);
    setOperationsTotalCount(0);
    setOperationsLoadingMore(false);
  }, [runId]);

  useEffect(() => {
    setOperations([]);
    setOperationsLoaded(false);
    setOperationsTotalCount(0);
    setOperationsLoadingMore(false);
  }, [operationKind]);

  useEffect(() => {
    savedOperationsPanelStateByRun.set(runId, {
      operationKind,
      operationSearch,
      activeOperationSequence,
      expandedOperations: Array.from(expandedOperations),
      expandedOperationPayloads: Array.from(expandedOperationPayloads),
      scrollPositions,
    });
  }, [runId, operationKind, operationSearch, activeOperationSequence, expandedOperations, expandedOperationPayloads, scrollPositions]);

  useEffect(() => {
    let cancelled = false;

    const fetchOperations = async () => {
      try {
        const res = await listRunOperations(runId, {
          kind: operationKind || undefined,
          limit: OPERATIONS_PAGE_SIZE,
        });
        if (!cancelled) {
          setOperations(res.items);
          setOperationsTotalCount(res.total_count ?? res.count);
          setOperationsLoaded(true);
          setOperationsLoadingMore(false);
        }
      } catch {
        if (!cancelled) {
          setOperationsLoaded(true);
          setOperationsLoadingMore(false);
        }
      }
    };

    void fetchOperations();
    return () => {
      cancelled = true;
    };
  }, [runId, operationKind]);

  useEffect(() => {
    if (isTerminal || !operationsCursor) return;
    let cancelled = false;

    const appendOperations = async () => {
      try {
        const res = await listRunOperations(runId, {
          kind: operationKind || undefined,
          limit: OPERATIONS_PAGE_SIZE,
          cursor: operationsCursor,
        });
        if (cancelled || res.items.length === 0) return;
        setOperations((prev) => {
          const seen = new Set(prev.map((item) => item.sequence));
          const appended = res.items.filter((item) => !seen.has(item.sequence));
          return appended.length > 0 ? [...prev, ...appended] : prev;
        });
        setOperationsTotalCount(res.total_count ?? res.count);
      } catch {
        // swallow transient polling errors
      }
    };

    const timer = setInterval(() => {
      void appendOperations();
    }, 2000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [runId, isTerminal, operationKind, operationsCursor]);

  useEffect(() => {
    if (visibleOperations.length === 0) {
      setActiveOperationSequence(null);
      return;
    }
    if (activeOperationSequence == null) return;
    const stillVisible = visibleOperations.some((op) => op.sequence === activeOperationSequence);
    if (!stillVisible) {
      setActiveOperationSequence(visibleOperations[0].sequence);
    }
  }, [visibleOperations, activeOperationSequence]);

  const toggleOperation = useCallback((sequence: number) => {
    setExpandedOperations((prev) => {
      const next = new Set(prev);
      if (next.has(sequence)) next.delete(sequence);
      else next.add(sequence);
      return next;
    });
  }, []);

  const toggleOperationPayload = useCallback((sequence: number) => {
    setExpandedOperationPayloads((prev) => {
      const next = new Set(prev);
      if (next.has(sequence)) next.delete(sequence);
      else next.add(sequence);
      return next;
    });
  }, []);

  const copyOperationPayload = useCallback((op: OperationDiagnostic) => {
    const text = op.result_json || op.error_message || '';
    if (!text) return;
    void navigator.clipboard.writeText(text).then(
      () => {
        toast.success('Operation payload copied');
      },
      () => {
        toast.error('Unable to copy operation payload');
      },
    );
  }, []);

  const handleLoadMoreOperations = useCallback(async () => {
    if (!operationsCursor || operationsLoadingMore || !hasMoreOperations) return;
    setOperationsLoadingMore(true);
    try {
      const res = await listRunOperations(runId, {
        kind: operationKind || undefined,
        limit: OPERATIONS_PAGE_SIZE,
        cursor: operationsCursor,
      });
      setOperations((prev) => {
        const seen = new Set(prev.map((item) => item.sequence));
        const appended = res.items.filter((item) => !seen.has(item.sequence));
        return appended.length > 0 ? [...prev, ...appended] : prev;
      });
      setOperationsTotalCount(res.total_count ?? res.count);
    } finally {
      setOperationsLoadingMore(false);
    }
  }, [runId, operationKind, operationsCursor, operationsLoadingMore, hasMoreOperations]);

  const setScrollPosition = useCallback((key: string, top: number) => {
    setScrollPositions((prev) => {
      if (prev[key] === top) return prev;
      return { ...prev, [key]: top };
    });
  }, []);

  const scrollPositionFor = useCallback((key: string) => scrollPositions[key] ?? 0, [scrollPositions]);

  return {
    operationKind,
    setOperationKind,
    operationSearch,
    setOperationSearch,
    operations,
    operationsLoaded,
    operationsTotalCount,
    operationsLoadingMore,
    isInitialLoading,
    remainingOperations,
    activeOperationSequence,
    setActiveOperationSequence,
    expandedOperations,
    expandedOperationPayloads,
    operationsCursor,
    hasMoreOperations,
    normalizedOperationSearch,
    visibleOperations,
    visibleStatusSummary,
    loadedStatusSummary,
    toggleOperation,
    toggleOperationPayload,
    copyOperationPayload,
    handleLoadMoreOperations,
    setScrollPosition,
    scrollPositionFor,
  };
}
