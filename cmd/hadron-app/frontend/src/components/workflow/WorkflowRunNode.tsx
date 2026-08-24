import { memo } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { CircleDot, GitPullRequest, LockKeyhole, RotateCcw } from 'lucide-react';

import { cn } from '../../lib/utils';
import { sourceLabel, type WorkflowRunNodeData } from './workflowGraph.helpers';

const STATUS_CLASS: Record<string, string> = {
  succeeded: 'workflow-node--success',
  success: 'workflow-node--success',
  running: 'workflow-node--running',
  claimed: 'workflow-node--running',
  waiting: 'workflow-node--waiting',
  failed: 'workflow-node--failed',
  crashed: 'workflow-node--failed',
  timed_out: 'workflow-node--failed',
  skipped: 'workflow-node--skipped',
  canceled: 'workflow-node--canceled',
  cancelled: 'workflow-node--canceled',
};

function WorkflowRunNodeComponent({ data, selected }: NodeProps & { data: WorkflowRunNodeData }) {
  const node = data as WorkflowRunNodeData;
  const definition = node.definition;
  const latestAttempt = Math.max(0, ...node.invocations.map(invocation => invocation.latest_attempt ?? 0));
  const maxAttempts = definition.retry?.attempts ?? (latestAttempt > 0 ? 1 : 0);
  const waiting = node.invocations.some(invocation => invocation.wait?.status === 'open');
  const pinned = node.invocations.some(invocation => invocation.pin !== undefined || invocation.origin === 'pinned');
  const replayed = node.invocations.some(invocation => invocation.origin === 'replayed' || invocation.origin === 'memoized');
  const source = sourceLabel(definition.source);

  return (
    <article
      className={cn('workflow-run-node', STATUS_CLASS[node.status] ?? 'workflow-node--pending', selected && 'is-selected')}
      aria-label={`${definition.display_name || definition.id}: ${node.status}`}
    >
      <Handle type="target" position={Position.Left} className="workflow-node-handle workflow-node-handle--control" />
      <div className="workflow-node-state-rail" aria-hidden="true" />
      <header className="workflow-node-header">
        <span className="workflow-node-kind">{definition.kind}</span>
        <span className="workflow-node-status"><CircleDot size={11} /> {node.status.replace(/_/g, ' ')}</span>
      </header>
      <div className="workflow-node-title">{definition.display_name || definition.id}</div>
      <div className="workflow-node-id">{definition.id}</div>
      <div className="workflow-node-facts">
        {maxAttempts > 0 && <span><RotateCcw size={11} /> {latestAttempt}/{maxAttempts}</span>}
        {waiting && <span className="is-waiting"><LockKeyhole size={11} /> wait open</span>}
        {pinned && <span><GitPullRequest size={11} /> pinned</span>}
        {replayed && <span><RotateCcw size={11} /> reused</span>}
      </div>
      <footer className="workflow-node-source" title={source}>
        <span>{definition.source?.format ?? 'source'}</span>
        <code>{definition.source?.start_line ? `L${definition.source.start_line}` : 'unmapped'}</code>
        {!node.authoredPosition && <span className="workflow-node-layout">auto</span>}
      </footer>
      <Handle type="source" position={Position.Right} className="workflow-node-handle workflow-node-handle--data" />
    </article>
  );
}

export const WorkflowRunNode = memo(WorkflowRunNodeComponent);
