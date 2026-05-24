import assert from 'node:assert/strict'
import test from 'node:test'

import type { OperationDiagnostic } from '@/api/types'
import {
  filterVisibleOperations,
  resolveKeyboardAction,
  restoreOperationsPanelState,
  summarizeOperationStatuses,
} from './runOperationsPanel.helpers'

function op(overrides: Partial<OperationDiagnostic> = {}): OperationDiagnostic {
  return {
    sequence: 1,
    kind: 'mcp_call',
    step_name: 'fetch-status',
    status: 'success',
    truncated: false,
    retry_count: 0,
    attempt_count: 0,
    reused_client: false,
    health_probe: false,
    reconnected: false,
    poll_count: 0,
    tool: 'repo_status',
    server: 'tether',
    transport: 'streamable_http',
    ...overrides,
  }
}

test('filterVisibleOperations matches step, tool, url, message, and launch fields', () => {
  const items = [
    op({ sequence: 1, step_name: 'fetch-health', kind: 'http_call', method: 'GET', url: 'http://127.0.0.1:8095/v1/health' }),
    op({ sequence: 2, kind: 'message_wait', message_id: 'msg-42', correlation_id: 'corr-abc' }),
    op({ sequence: 3, kind: 'agent_launch', launch_id: 'torque-monitor-correlator' }),
  ]

  assert.deepEqual(filterVisibleOperations(items, 'health').map(item => item.sequence), [1])
  assert.deepEqual(filterVisibleOperations(items, 'msg-42').map(item => item.sequence), [2])
  assert.deepEqual(filterVisibleOperations(items, 'correlator').map(item => item.sequence), [3])
})

test('restoreOperationsPanelState normalizes missing fields', () => {
  assert.deepEqual(restoreOperationsPanelState(undefined), {
    operationKind: '',
    operationSearch: '',
    activeOperationSequence: null,
    expandedOperations: [],
    expandedOperationPayloads: [],
    scrollPositions: {},
  })
})

test('summarizeOperationStatuses groups statuses into UI buckets', () => {
  const summary = summarizeOperationStatuses([
    op({ status: 'success' }),
    op({ status: 'running' }),
    op({ status: 'failed' }),
    op({ status: 'queued' }),
    op({ status: 'skipped' }),
  ])

  assert.deepEqual(summary, {
    success: 1,
    running: 1,
    failed: 1,
    queued: 2,
  })
})

test('resolveKeyboardAction focuses search on slash', () => {
  const action = resolveKeyboardAction({
    key: '/',
    editable: false,
    searchFocused: false,
    searchValue: '',
    visibleSequences: [1, 2],
    activeSequence: null,
    targetIsButton: false,
  })
  assert.deepEqual(action, { type: 'focus_search' })
})

test('resolveKeyboardAction clears or blurs search on escape', () => {
  assert.deepEqual(resolveKeyboardAction({
    key: 'Escape',
    editable: true,
    searchFocused: true,
    searchValue: 'status',
    visibleSequences: [1],
    activeSequence: 1,
    targetIsButton: false,
  }), { type: 'clear_search' })

  assert.deepEqual(resolveKeyboardAction({
    key: 'Escape',
    editable: true,
    searchFocused: true,
    searchValue: '',
    visibleSequences: [1],
    activeSequence: 1,
    targetIsButton: false,
  }), { type: 'blur_search' })
})

test('resolveKeyboardAction navigates visible sequences with arrows', () => {
  assert.deepEqual(resolveKeyboardAction({
    key: 'ArrowDown',
    editable: false,
    searchFocused: false,
    searchValue: '',
    visibleSequences: [10, 20, 30],
    activeSequence: 20,
    targetIsButton: false,
  }), { type: 'focus_sequence', sequence: 30 })

  assert.deepEqual(resolveKeyboardAction({
    key: 'ArrowUp',
    editable: false,
    searchFocused: false,
    searchValue: '',
    visibleSequences: [10, 20, 30],
    activeSequence: null,
    targetIsButton: false,
  }), { type: 'focus_sequence', sequence: 30 })
})

test('resolveKeyboardAction toggles active row on enter but not from button targets', () => {
  assert.deepEqual(resolveKeyboardAction({
    key: 'Enter',
    editable: false,
    searchFocused: false,
    searchValue: '',
    visibleSequences: [10, 20],
    activeSequence: 20,
    targetIsButton: false,
  }), { type: 'toggle_sequence', sequence: 20 })

  assert.deepEqual(resolveKeyboardAction({
    key: 'Enter',
    editable: false,
    searchFocused: false,
    searchValue: '',
    visibleSequences: [10, 20],
    activeSequence: 20,
    targetIsButton: true,
  }), { type: 'none' })
})
