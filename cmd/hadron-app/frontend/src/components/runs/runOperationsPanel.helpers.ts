import type { OperationDiagnostic } from '@/api/types'

export interface SavedOperationsPanelState {
  operationKind: string
  operationSearch: string
  activeOperationSequence: number | null
  expandedOperations: number[]
  expandedOperationPayloads: number[]
  scrollPositions: Record<string, number>
}

export interface NormalizedOperationsPanelState extends SavedOperationsPanelState {}

export type KeyboardAction =
  | { type: 'none' }
  | { type: 'focus_search' }
  | { type: 'clear_search' }
  | { type: 'blur_search' }
  | { type: 'focus_sequence'; sequence: number }
  | { type: 'toggle_sequence'; sequence: number }

export interface OperationStatusSummary {
  success: number
  running: number
  failed: number
  queued: number
}

export function operationSearchText(op: OperationDiagnostic): string {
  return [
    op.step_name,
    op.kind,
    op.tool,
    op.server,
    op.transport,
    op.method,
    op.url,
    op.to,
    op.correlation_id,
    op.message_id,
    op.substrate,
    op.launch_id,
    op.logical_agent_id,
    op.gate_id,
    op.decision,
    op.prompt,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase()
}

export function filterVisibleOperations(items: OperationDiagnostic[], search: string): OperationDiagnostic[] {
  const normalized = search.trim().toLowerCase()
  if (!normalized) return items
  return items.filter((op) => operationSearchText(op).includes(normalized))
}

export function restoreOperationsPanelState(saved?: SavedOperationsPanelState): NormalizedOperationsPanelState {
  return {
    operationKind: saved?.operationKind ?? '',
    operationSearch: saved?.operationSearch ?? '',
    activeOperationSequence: saved?.activeOperationSequence ?? null,
    expandedOperations: saved?.expandedOperations ?? [],
    expandedOperationPayloads: saved?.expandedOperationPayloads ?? [],
    scrollPositions: saved?.scrollPositions ?? {},
  }
}

export function summarizeOperationStatuses(items: OperationDiagnostic[]): OperationStatusSummary {
  return items.reduce<OperationStatusSummary>(
    (summary, op) => {
      if (op.status === 'success') summary.success += 1
      else if (op.status === 'running') summary.running += 1
      else if (op.status === 'failed') summary.failed += 1
      else summary.queued += 1
      return summary
    },
    { success: 0, running: 0, failed: 0, queued: 0 },
  )
}

export function resolveKeyboardAction(input: {
  key: string
  editable: boolean
  searchFocused: boolean
  searchValue: string
  visibleSequences: number[]
  activeSequence: number | null
  targetIsButton: boolean
  metaKey?: boolean
  ctrlKey?: boolean
  altKey?: boolean
}): KeyboardAction {
  const {
    key,
    editable,
    searchFocused,
    searchValue,
    visibleSequences,
    activeSequence,
    targetIsButton,
    metaKey = false,
    ctrlKey = false,
    altKey = false,
  } = input

  if (key === '/' && !editable && !metaKey && !ctrlKey && !altKey) {
    return { type: 'focus_search' }
  }

  if (key === 'Escape') {
    if (searchFocused && searchValue) return { type: 'clear_search' }
    if (searchFocused) return { type: 'blur_search' }
  }

  if (editable || visibleSequences.length === 0) return { type: 'none' }

  const currentIndex = activeSequence == null ? -1 : visibleSequences.indexOf(activeSequence)

  if (key === 'ArrowDown') {
    const nextIndex = currentIndex < 0 ? 0 : Math.min(currentIndex + 1, visibleSequences.length - 1)
    return { type: 'focus_sequence', sequence: visibleSequences[nextIndex] }
  }

  if (key === 'ArrowUp') {
    const nextIndex = currentIndex < 0 ? visibleSequences.length - 1 : Math.max(currentIndex - 1, 0)
    return { type: 'focus_sequence', sequence: visibleSequences[nextIndex] }
  }

  if (key === 'Enter' && currentIndex >= 0 && !targetIsButton) {
    return { type: 'toggle_sequence', sequence: visibleSequences[currentIndex] }
  }

  return { type: 'none' }
}
