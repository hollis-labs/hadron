import { memo } from 'react';
import { BaseEdge, EdgeLabelRenderer, getSmoothStepPath, type EdgeProps } from '@xyflow/react';
import { LockKeyhole } from 'lucide-react';

import { sourceLabel, type WorkflowRunEdgeData } from './workflowGraph.helpers';

const ROUTE_COLOR: Record<string, string> = {
  data: '#3b82f6',
  control: '#71717a',
  catch: '#ef4444',
  switch: '#f59e0b',
  finally: '#a855f7',
};

function WorkflowRunEdgeComponent({
  id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, markerEnd, data, selected,
}: EdgeProps) {
  const edge = (data ?? {}) as WorkflowRunEdgeData;
  const [path, labelX, labelY] = getSmoothStepPath({
    sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, borderRadius: 12,
  });
  const failed = edge.state === 'failed';
  const stroke = failed ? '#ef4444' : ROUTE_COLOR[edge.kind] ?? ROUTE_COLOR.control;
  const dash = edge.kind === 'catch' ? '8 5' : edge.kind === 'finally' ? '2 5' : edge.kind === 'switch' ? '10 4 2 4' : undefined;
  const valueLabel = edge.valuesOmitted
    ? 'values omitted'
    : edge.valueNames.length > 0
      ? `${edge.valueNames.length} typed`
      : edge.kind;

  return (
    <>
      <BaseEdge
        id={id}
        path={path}
        markerEnd={markerEnd}
        className={`workflow-run-edge workflow-run-edge--${edge.state}`}
        style={{
          stroke,
          strokeWidth: selected || edge.state === 'selected' ? 2.5 : edge.state === 'traversed' ? 2 : 1.5,
          strokeDasharray: dash,
          opacity: edge.state === 'idle' || edge.state === 'skipped' ? 0.48 : 1,
        }}
      />
      <EdgeLabelRenderer>
        <div
          className={`workflow-edge-label workflow-edge-label--${edge.kind}`}
          style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }}
          title={`${edge.valueNames.join(', ') || edge.kind} · ${sourceLabel(edge.sourceRef)}`}
        >
          <span>{valueLabel}{edge.sourceRef?.start_line ? ` · L${edge.sourceRef.start_line}` : ''}</span>
          {edge.maskedValueCount > 0 && <LockKeyhole size={10} aria-label={`${edge.maskedValueCount} masked values`} />}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

export const WorkflowRunEdge = memo(WorkflowRunEdgeComponent);
