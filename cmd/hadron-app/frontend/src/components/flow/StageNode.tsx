import { memo } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { Zap } from 'lucide-react';

export type StageStatus = 'idle' | 'running' | 'success' | 'failed' | 'skipped';

export interface StageNodeData extends Record<string, unknown> {
  label: string;
  blueprintPath: string;
  status: StageStatus;
  condition?: string;
  isStart?: boolean;
  inputs?: Record<string, string>;
  outputs?: string[];
}

const STATUS_COLORS: Record<StageStatus, string> = {
  idle: 'rgb(var(--muted))',
  running: 'rgb(var(--warn))',
  success: 'rgb(var(--ok))',
  failed: 'rgb(var(--danger))',
  skipped: 'rgb(var(--muted))',
};

function StageNodeComponent({ data, selected }: NodeProps & { data: StageNodeData }) {
  const nodeData = data as StageNodeData;
  const isStart = nodeData.isStart ?? false;
  const statusColor = STATUS_COLORS[nodeData.status] ?? STATUS_COLORS.idle;

  return (
    <div
      className="stage-node"
      style={{
        border: selected
          ? '1px solid rgb(var(--ok))'
          : '1px solid rgb(var(--border))',
        boxShadow: selected ? '0 0 0 1px rgb(var(--ok))' : undefined,
      }}
    >
      {/* Input handle — hidden for start nodes */}
      {!isStart && (
        <Handle
          type="target"
          position={Position.Top}
          className="stage-handle"
        />
      )}

      {/* Header */}
      <div className="stage-node-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', flex: 1, minWidth: 0 }}>
          {isStart && (
            <Zap size={12} style={{ color: 'rgb(var(--ok))', flexShrink: 0 }} />
          )}
          <div
            className="stage-status-dot"
            style={{
              background: statusColor,
              boxShadow: nodeData.status === 'running' ? `0 0 6px ${statusColor}` : undefined,
            }}
          />
          <span className="stage-node-name">{nodeData.label}</span>
        </div>
        {nodeData.condition && (
          <span className="bp-badge bp-badge-warn" style={{ fontSize: '8px', padding: '0 4px', lineHeight: '14px' }}>
            if
          </span>
        )}
      </div>

      {/* Blueprint path subtitle */}
      <div className="stage-node-path">
        {nodeData.blueprintPath}
      </div>

      {/* Output handle */}
      <Handle
        type="source"
        position={Position.Bottom}
        className="stage-handle"
      />
    </div>
  );
}

export const StageNode = memo(StageNodeComponent);
