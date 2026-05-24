import assert from 'node:assert/strict'
import test from 'node:test'

import type { OperationDiagnostic } from '@/api/types'
import {
  diagnosticStatusVariant,
  formatDiagnosticTime,
  operationKindLabel,
  operationPayload,
  operationPrimaryLabel,
  truncatePreview,
} from './runOperationRow.helpers'

function op(overrides: Partial<OperationDiagnostic> = {}): OperationDiagnostic {
  return {
    sequence: 1,
    kind: 'mcp_call',
    step_name: 'step',
    status: 'success',
    retry_count: 0,
    attempt_count: 0,
    reused_client: false,
    health_probe: false,
    reconnected: false,
    truncated: false,
    poll_count: 0,
    tool: 'repo_status',
    server: 'tether',
    ...overrides,
  }
}

test('operationPrimaryLabel formats known operation kinds', () => {
  assert.equal(operationPrimaryLabel(op()), 'repo_status')
  assert.equal(operationPrimaryLabel(op({ kind: 'http_call', method: 'GET', url: 'http://127.0.0.1:8095/v1/health' })), 'GET http://127.0.0.1:8095/v1/health')
  assert.equal(operationPrimaryLabel(op({ kind: 'message_wait', to: 'mailbox://agent/replies' })), 'mailbox://agent/replies')
  assert.equal(operationPrimaryLabel(op({ kind: 'agent_launch', launch_id: 'torque-monitor-correlator' })), 'torque-monitor-correlator')
})

test('operationKindLabel returns concise labels', () => {
  assert.equal(operationKindLabel('mcp_call'), 'MCP')
  assert.equal(operationKindLabel('http_call'), 'HTTP')
  assert.equal(operationKindLabel('message_wait'), 'Wait')
  assert.equal(operationKindLabel('agent_launch'), 'Launch')
})

test('truncatePreview preserves short values and truncates long values', () => {
  assert.deepEqual(truncatePreview('short', 10), { text: 'short', truncated: false })
  assert.deepEqual(truncatePreview('abcdefghijk', 5), { text: 'abcde...', truncated: true })
})

test('operationPayload prefers result_json over error_message', () => {
  assert.equal(operationPayload(op({ result_json: '{"ok":true}', error_message: 'bad' })), '{"ok":true}')
  assert.equal(operationPayload(op({ result_json: '', error_message: 'bad' })), 'bad')
  assert.equal(operationPayload(op({ result_json: '', error_message: '' })), '')
})

test('diagnosticStatusVariant normalizes status buckets', () => {
  assert.equal(diagnosticStatusVariant('success'), 'success')
  assert.equal(diagnosticStatusVariant('running'), 'running')
  assert.equal(diagnosticStatusVariant('failed'), 'failed')
  assert.equal(diagnosticStatusVariant('timeout'), 'queued')
})

test('formatDiagnosticTime returns empty for missing values', () => {
  assert.equal(formatDiagnosticTime(''), '')
  assert.ok(formatDiagnosticTime('2026-05-24T12:34:56Z').length > 0)
})
