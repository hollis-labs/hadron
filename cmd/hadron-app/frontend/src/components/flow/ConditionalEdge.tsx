import { memo } from 'react';
import {
  BaseEdge,
  EdgeLabelRenderer,
  getSmoothStepPath,
  type EdgeProps,
} from '@xyflow/react';

export type EdgeStatus = 'idle' | 'running' | 'success' | 'failed' | 'skipped';

export interface StageEdgeData extends Record<string, unknown> {
  condition?: string;
  status?: EdgeStatus;
}

const STATUS_STROKE: Record<EdgeStatus, string> = {
  idle: 'rgb(var(--border))',
  running: 'rgb(var(--warn))',
  success: 'rgb(var(--ok))',
  failed: 'rgb(var(--danger))',
  skipped: 'rgb(var(--muted))',
};

function ConditionalEdgeComponent({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
  markerEnd,
}: EdgeProps) {
  const edgeData = (data ?? {}) as StageEdgeData;
  const condition = edgeData.condition;
  const status: EdgeStatus = edgeData.status ?? 'idle';
  const isConditional = !!condition;
  const isRunning = status === 'running';

  const strokeColor = selected ? 'rgb(var(--ok))' : STATUS_STROKE[status];

  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    borderRadius: 8,
  });

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        markerEnd={markerEnd}
        style={{
          stroke: strokeColor,
          strokeWidth: selected ? 2 : 1.5,
          strokeDasharray: isConditional ? '6 4' : undefined,
          animation: isRunning ? 'flow-dash 0.5s linear infinite' : undefined,
        }}
      />
      {isConditional && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              pointerEvents: 'all',
              fontSize: '0.6rem',
              fontFamily: 'var(--font-mono)',
              color: 'rgb(var(--warn))',
              background: 'rgb(var(--panel))',
              border: '1px solid rgb(var(--border))',
              borderRadius: '3px',
              padding: '1px 5px',
              whiteSpace: 'nowrap',
              maxWidth: '120px',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
            }}
            title={condition}
          >
            if: {condition}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

export const ConditionalEdge = memo(ConditionalEdgeComponent);
