import type { OperationDiagnostic } from '@/api/types'

export const OPERATION_PREVIEW_CHARS = 280

export function diagnosticStatusVariant(status: string): 'success' | 'running' | 'failed' | 'queued' {
  if (status === 'success') return 'success'
  if (status === 'running') return 'running'
  if (status === 'failed') return 'failed'
  return 'queued'
}

export function operationKindLabel(kind: string): string {
  switch (kind) {
    case 'mcp_call':
      return 'MCP'
    case 'http_call':
      return 'HTTP'
    case 'message_wait':
      return 'Wait'
    case 'agent_launch':
      return 'Launch'
    case 'human_gate':
      return 'Gate'
    default:
      return kind
  }
}

export function operationPrimaryLabel(op: OperationDiagnostic): string {
  switch (op.kind) {
    case 'mcp_call':
      return op.tool || 'MCP call'
    case 'http_call':
      return [op.method, op.url].filter(Boolean).join(' ')
    case 'message_wait':
      return op.to || op.correlation_id || 'message wait'
    case 'agent_launch':
      return op.launch_id || op.logical_agent_id || 'agent launch'
    case 'human_gate':
      return op.prompt || op.gate_id || 'human gate'
    default:
      return op.kind
  }
}

export function formatDiagnosticTime(value?: string | null): string {
  if (!value) return ''
  return new Date(value).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

export function truncatePreview(value: string, maxChars: number = OPERATION_PREVIEW_CHARS): { text: string; truncated: boolean } {
  if (value.length <= maxChars) return { text: value, truncated: false }
  return { text: `${value.slice(0, maxChars)}...`, truncated: true }
}

export function operationPayload(op: OperationDiagnostic): string {
  return op.result_json || op.error_message || ''
}
