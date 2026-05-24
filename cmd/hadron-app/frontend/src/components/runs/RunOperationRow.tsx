import { forwardRef } from 'react'
import { Badge } from '../ui/badge'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { ChevronDown, Clock3, Copy, Globe, RotateCcw, Send, Server, ShieldCheck, TriangleAlert, Wrench, UserRoundCheck } from 'lucide-react'
import type { OperationDiagnostic } from '../../api/types'
import {
  diagnosticStatusVariant,
  formatDiagnosticTime,
  operationKindLabel,
  operationPayload,
  operationPrimaryLabel,
  truncatePreview,
} from './runOperationRow.helpers'

function operationKindIcon(kind: string) {
  switch (kind) {
    case 'mcp_call':
      return <Server size={14} className="text-muted-foreground" />
    case 'http_call':
      return <Globe size={14} className="text-muted-foreground" />
    case 'message_wait':
      return <Clock3 size={14} className="text-muted-foreground" />
    case 'agent_launch':
      return <Send size={14} className="text-muted-foreground" />
    case 'human_gate':
      return <UserRoundCheck size={14} className="text-muted-foreground" />
    default:
      return <Wrench size={14} className="text-muted-foreground" />
  }
}

export const RunOperationRow = forwardRef<HTMLButtonElement, {
  operation: OperationDiagnostic
  isExpanded: boolean
  isPayloadExpanded: boolean
  isActive: boolean
  normalizedSearch: string
  renderHighlightedText: (value: string, query: string) => React.ReactNode
  onToggle: (sequence: number) => void
  onTogglePayload: (sequence: number) => void
  onCopyPayload: (op: OperationDiagnostic) => void
  onFocusRow: (sequence: number) => void
}>(({ operation: op, isExpanded, isPayloadExpanded, isActive, normalizedSearch, renderHighlightedText, onToggle, onTogglePayload, onCopyPayload, onFocusRow }, ref) => {
  const payload = operationPayload(op)
  const payloadPreview = truncatePreview(payload)

  const renderMetaValue = (label: string, value: string | number) => (
    <span>
      {label}: <span className="text-foreground">{typeof value === 'string' ? renderHighlightedText(value, normalizedSearch) : value}</span>
    </span>
  )

  return (
    <div role="listitem" className="px-4 py-3">
      <div className="flex items-start gap-2">
        <button
          ref={ref}
          onClick={() => onToggle(op.sequence)}
          onFocus={() => onFocusRow(op.sequence)}
          aria-expanded={isExpanded}
          aria-pressed={isActive}
          aria-label={`${operationKindLabel(op.kind)} operation ${operationPrimaryLabel(op)}`}
          className={cn(
            "flex-1 min-w-0 text-left rounded-md outline-none focus-visible:ring-2 focus-visible:ring-ring/50",
            isActive && "ring-1 ring-border"
          )}
        >
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant={diagnosticStatusVariant(op.status)}>{op.status}</Badge>
            <Badge variant="outline" className="gap-1">
              {operationKindIcon(op.kind)}
              {operationKindLabel(op.kind)}
            </Badge>
            <span className="font-mono text-sm text-foreground break-all">{renderHighlightedText(operationPrimaryLabel(op), normalizedSearch)}</span>
            {op.server && <span className="text-sm text-muted-foreground">{renderHighlightedText(op.server, normalizedSearch)}</span>}
            {op.transport && <Badge variant="outline">{op.transport}</Badge>}
            {op.truncated && <Badge variant="outline">truncated</Badge>}
            {op.retry_count > 0 && (
              <Badge variant="outline" className="gap-1">
                <RotateCcw size={11} />
                {op.retry_count} retry
              </Badge>
            )}
            {op.reconnected && (
              <Badge variant="outline" className="gap-1">
                <ShieldCheck size={11} />
                reconnected
              </Badge>
            )}
            {op.health_probe && (
              <Badge variant="outline" className="gap-1">
                <Wrench size={11} />
                probed
              </Badge>
            )}
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
            {renderMetaValue('Step', op.step_name)}
            {op.attempt_count > 1 && renderMetaValue('Attempts', op.attempt_count)}
            {op.status_code != null && renderMetaValue('Status', op.status_code)}
            {op.duration_ms != null && renderMetaValue('Duration', `${op.duration_ms}ms`)}
            {op.timeout_ms != null && renderMetaValue('Timeout', `${op.timeout_ms}ms`)}
            {op.poll_count > 0 && renderMetaValue('Polls', op.poll_count)}
            {op.message_id && renderMetaValue('Message', op.message_id)}
            {op.correlation_id && renderMetaValue('Correlation', op.correlation_id)}
            {op.substrate && renderMetaValue('Substrate', op.substrate)}
            {op.launch_id && renderMetaValue('Launch', op.launch_id)}
            {op.logical_agent_id && renderMetaValue('Agent', op.logical_agent_id)}
            {op.gate_id && renderMetaValue('Gate', op.gate_id)}
            {op.decision && renderMetaValue('Decision', op.decision)}
            {op.started_at && <span>Started: <span className="text-foreground">{formatDiagnosticTime(op.started_at)}</span></span>}
            {op.finished_at && <span>Finished: <span className="text-foreground">{formatDiagnosticTime(op.finished_at)}</span></span>}
          </div>
        </button>
        <div className="flex items-center gap-1 shrink-0">
          {payload && (
            <Button
              variant="ghost"
              size="xs"
              onClick={() => onCopyPayload(op)}
              title="Copy payload"
            >
              <Copy size={13} />
            </Button>
          )}
          <Button
            variant="ghost"
            size="xs"
            onClick={() => onToggle(op.sequence)}
            title={isExpanded ? 'Collapse operation' : 'Expand operation'}
          >
            <ChevronDown size={14} className={cn("transition-transform", isExpanded && "rotate-180")} />
          </Button>
        </div>
      </div>
      {isExpanded && (
        <div className="mt-3 border-t border-border pt-3">
          {op.error_message && (
            <div className="flex items-start gap-2 rounded-md border border-red-500/20 bg-red-500/5 px-3 py-2 text-sm text-red-400">
              <TriangleAlert size={14} className="mt-0.5 shrink-0" />
              <span>{renderHighlightedText(op.error_message, normalizedSearch)}</span>
            </div>
          )}
          {op.result_json && (
            <div className="mt-2">
              <div className="mb-2 flex items-center justify-between gap-2">
                <span className="text-xs font-medium text-muted-foreground">Result</span>
                <div className="flex items-center gap-1">
                  {payloadPreview.truncated && (
                    <Button
                      variant="ghost"
                      size="xs"
                      onClick={() => onTogglePayload(op.sequence)}
                    >
                      {isPayloadExpanded ? 'Collapse payload' : 'Expand payload'}
                    </Button>
                  )}
                  <Button
                    variant="ghost"
                    size="xs"
                    onClick={() => onCopyPayload(op)}
                  >
                    <Copy size={13} />
                    Copy
                  </Button>
                </div>
              </div>
              <pre className="max-h-80 overflow-auto rounded-md border border-border bg-background px-3 py-2 font-mono text-xs text-muted-foreground whitespace-pre-wrap break-all">
                {renderHighlightedText(isPayloadExpanded ? op.result_json : payloadPreview.text, normalizedSearch)}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
})

RunOperationRow.displayName = 'RunOperationRow'
