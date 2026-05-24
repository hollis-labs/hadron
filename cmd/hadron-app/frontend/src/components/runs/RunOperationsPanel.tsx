import { useEffect, useRef } from 'react'
import { Search, Wrench, X } from 'lucide-react'
import { Spinner } from '../ui/Spinner'
import { Badge } from '../ui/badge'
import { Input } from '../ui/input'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { operationFilterKey, renderHighlightedText } from './runOperationsDisplay.helpers'
import { RunOperationRow } from './RunOperationRow'
import { useRunOperationsKeyboard } from './useRunOperationsKeyboard'
import { useRunOperationsPanel } from './useRunOperationsPanel'

export function RunOperationsPanel({ runId, isTerminal }: { runId: string; isTerminal: boolean }) {
  const operationsLogRef = useRef<HTMLDivElement>(null)
  const operationsSearchRef = useRef<HTMLInputElement | null>(null)
  const operationRowRefs = useRef<Record<number, HTMLButtonElement | null>>({})
  const {
    operationKind,
    setOperationKind,
    operationSearch,
    setOperationSearch,
    operations,
    operationsTotalCount,
    operationsLoadingMore,
    isInitialLoading,
    remainingOperations,
    activeOperationSequence,
    setActiveOperationSequence,
    expandedOperations,
    expandedOperationPayloads,
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
  } = useRunOperationsPanel({ runId, isTerminal })

  useEffect(() => {
    const current = operationsLogRef.current
    if (!current) return
    current.scrollTop = scrollPositionFor(operationFilterKey(operationKind))
  }, [operationKind, operations.length, scrollPositionFor])

  useRunOperationsKeyboard({
    searchRef: operationsSearchRef,
    rowRefs: operationRowRefs,
    visibleSequences: visibleOperations.map((op) => op.sequence),
    activeSequence: activeOperationSequence,
    searchValue: operationSearch,
    setSearchValue: setOperationSearch,
    setActiveSequence: setActiveOperationSequence,
    toggleSequence: toggleOperation,
  })

  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between gap-4 border-b border-border px-4 py-3">
        <div className="flex items-center gap-2">
          <Wrench size={15} className="text-muted-foreground" />
          <span className="text-sm font-semibold text-foreground">Operations</span>
          <Badge variant="outline">
            {visibleOperations.length}
            {visibleOperations.length !== operations.length ? ` / ${operations.length}` : ''}
            {' / '}
            {operationsTotalCount || operations.length}
          </Badge>
        </div>
        {!isTerminal && (
          <Badge variant="outline" className="gap-1">
            <Spinner size={12} />
            Live
          </Badge>
        )}
      </div>

      <div className="border-b border-border px-4 py-3">
        <div className="flex flex-wrap items-center gap-1">
          {[
            { value: '', label: 'All' },
            { value: 'mcp_call', label: 'MCP' },
            { value: 'http_call', label: 'HTTP' },
            { value: 'message_wait', label: 'Wait' },
            { value: 'agent_launch', label: 'Launch' },
            { value: 'human_gate', label: 'Gate' },
          ].map((option) => (
            <button
              key={option.value || 'all'}
              type="button"
              aria-pressed={operationKind === option.value}
              onClick={() => setOperationKind(option.value)}
              className={cn(
                'px-2.5 py-1 rounded text-xs font-medium transition-colors',
                operationKind === option.value
                  ? 'bg-background text-foreground border border-border'
                  : 'text-muted-foreground hover:text-foreground',
              )}
            >
              {option.label}
            </button>
          ))}
        </div>

        <div className="mt-3 flex flex-wrap items-center gap-2">
          <div className="relative min-w-[240px] flex-1 max-w-sm">
            <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground" />
            <Input
              ref={operationsSearchRef}
              value={operationSearch}
              onChange={(e) => setOperationSearch(e.currentTarget.value)}
              placeholder="Search step, tool, URL, message, launch"
              className="pl-8 pr-8 text-sm"
              aria-label="Search operations"
            />
            {operationSearch && (
              <button
                type="button"
                onClick={() => setOperationSearch('')}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                title="Clear search"
                aria-label="Clear operation search"
              >
                <X size={14} />
              </button>
            )}
          </div>

          <div className="flex flex-wrap items-center gap-1">
            {visibleStatusSummary.running > 0 && <Badge variant="running">{visibleStatusSummary.running} running</Badge>}
            {visibleStatusSummary.failed > 0 && <Badge variant="failed">{visibleStatusSummary.failed} failed</Badge>}
            {visibleStatusSummary.success > 0 && <Badge variant="success">{visibleStatusSummary.success} success</Badge>}
            {visibleStatusSummary.queued > 0 && <Badge variant="queued">{visibleStatusSummary.queued} queued</Badge>}
            {visibleOperations.length !== operations.length && <Badge variant="outline">{visibleOperations.length} matching</Badge>}
            {visibleOperations.length === operations.length && operations.length > 0 && loadedStatusSummary.running > 0 && (
              <Badge variant="outline">{loadedStatusSummary.running} active</Badge>
            )}
          </div>
        </div>
      </div>

      {isInitialLoading ? (
        <div className="flex items-center gap-2 px-4 py-5 text-sm text-muted-foreground">
          <Spinner size={14} />
          Loading operation diagnostics...
        </div>
      ) : operations.length === 0 ? (
        <div className="px-4 py-5 text-sm text-muted-foreground">No operation diagnostics recorded for this run.</div>
      ) : visibleOperations.length === 0 ? (
        <div className="flex items-center justify-between gap-3 px-4 py-5 text-sm text-muted-foreground">
          <span>No loaded operations match this search.</span>
          {operationSearch && (
            <Button variant="outline" size="sm" onClick={() => setOperationSearch('')}>
              Clear Search
            </Button>
          )}
        </div>
      ) : (
        <div
          ref={operationsLogRef}
          role="list"
          className="max-h-[360px] overflow-y-auto divide-y divide-border"
          onScroll={(e) => {
            setScrollPosition(operationFilterKey(operationKind), e.currentTarget.scrollTop)
          }}
        >
          {visibleOperations.map((op) => (
            <RunOperationRow
              key={`${op.sequence}-${op.kind}-${op.started_at ?? 'na'}`}
              ref={(el) => {
                operationRowRefs.current[op.sequence] = el
              }}
              operation={op}
              isExpanded={expandedOperations.has(op.sequence)}
              isPayloadExpanded={expandedOperationPayloads.has(op.sequence)}
              isActive={activeOperationSequence === op.sequence}
              normalizedSearch={normalizedOperationSearch}
              renderHighlightedText={renderHighlightedText}
              onToggle={toggleOperation}
              onTogglePayload={toggleOperationPayload}
              onCopyPayload={copyOperationPayload}
              onFocusRow={setActiveOperationSequence}
            />
          ))}
        </div>
      )}

      {hasMoreOperations && (
        <div className="flex items-center justify-between gap-3 border-t border-border px-4 py-3">
          <div className="text-xs text-muted-foreground">
            {operations.length} loaded
            {operationsTotalCount > 0 ? ` of ${operationsTotalCount}` : ''}
            {remainingOperations > 0 ? `, ${remainingOperations} remaining` : ''}
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={handleLoadMoreOperations}
            disabled={operationsLoadingMore}
          >
            {operationsLoadingMore ? <Spinner size={12} /> : null}
            Load More
          </Button>
        </div>
      )}
    </div>
  )
}
