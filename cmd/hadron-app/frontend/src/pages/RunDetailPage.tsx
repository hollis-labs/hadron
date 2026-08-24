import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Background,
  BackgroundVariant,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { AlertTriangle, ChevronLeft, GitBranch, LockKeyhole, RefreshCw, Square, Workflow } from 'lucide-react';
import { toast } from 'sonner';

import { cancelWorkflowRun, inspectWorkflowRun } from '../api/client';
import type { WorkflowNodeDiagnostic } from '../api/types';
import { WorkflowEventTrail } from '../components/workflow/WorkflowEventTrail';
import { WorkflowResumeDialog } from '../components/workflow/WorkflowResumeDialog';
import { WorkflowRunEdge } from '../components/workflow/WorkflowRunEdge';
import { WorkflowRunInspector, WorkflowValueLedger } from '../components/workflow/WorkflowRunInspector';
import { WorkflowRunNode } from '../components/workflow/WorkflowRunNode';
import {
  buildWorkflowGraph,
  isTerminalWorkflowRun,
  type WorkflowRunEdgeData,
  type WorkflowRunNodeData,
} from '../components/workflow/workflowGraph.helpers';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '../components/ui/alert-dialog';
import { Button } from '../components/ui/button';
import { Spinner } from '../components/ui/Spinner';
import { StatusBadge } from '../components/ui/StatusBadge';
import { useNavigation } from '../contexts/NavigationContext';
import { usePoll } from '../hooks/usePoll';
import { formatDuration } from '../utils/format';
import './workflowRun.css';

const nodeTypes = { workflowRunNode: WorkflowRunNode };
const edgeTypes = { workflowRunEdge: WorkflowRunEdge };
const FAILED_NODE_STATES = new Set(['failed', 'crashed', 'timed_out']);

function markerColor(kind: WorkflowRunEdgeData['kind']): string {
  if (kind === 'data') return '#3b82f6';
  if (kind === 'catch') return '#ef4444';
  if (kind === 'switch') return '#f59e0b';
  if (kind === 'finally') return '#a855f7';
  return '#71717a';
}

function selectInitialNode(nodes: Node<WorkflowRunNodeData>[]): string | null {
  const priority = ['failed', 'crashed', 'timed_out', 'waiting', 'running', 'claimed', 'blocked', 'ready', 'pending'];
  for (const status of priority) {
    const found = nodes.find(node => node.data.status === status);
    if (found) return found.id;
  }
  return nodes[0]?.id ?? null;
}

function plural(count: number, singular: string): string {
  return `${count} ${singular}${count === 1 ? '' : 's'}`;
}

export function RunDetailPage() {
  const navigation = useNavigation();
  const runId = navigation.selectedRunId!;
  const fetcher = useCallback(() => inspectWorkflowRun(runId), [runId]);
  const [polling, setPolling] = useState(true);
  const { data: result, error, loading, refresh } = usePoll(fetcher, 2_000, polling);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [resumeNode, setResumeNode] = useState<WorkflowNodeDiagnostic>();
  const [cancelOpen, setCancelOpen] = useState(false);
  const [canceling, setCanceling] = useState(false);

  useEffect(() => {
    setPolling(result ? !isTerminalWorkflowRun(result.run.status) : true);
  }, [result]);

  useEffect(() => {
    const listener = () => refresh();
    window.addEventListener('hadron:refresh', listener);
    return () => window.removeEventListener('hadron:refresh', listener);
  }, [refresh]);

  const graph = useMemo(() => result ? buildWorkflowGraph(result) : { nodes: [], edges: [] }, [result]);
  const flowNodes = graph.nodes as Node<WorkflowRunNodeData>[];
  const flowEdges = graph.edges.map(edge => ({
    ...edge,
    markerEnd: { type: MarkerType.ArrowClosed, color: markerColor(edge.data.kind), width: 16, height: 16 },
  })) as Edge<WorkflowRunEdgeData>[];

  useEffect(() => {
    if (flowNodes.length === 0) {
      setSelectedNodeId(null);
      return;
    }
    if (!selectedNodeId || !flowNodes.some(node => node.id === selectedNodeId)) {
      setSelectedNodeId(selectInitialNode(flowNodes));
    }
  }, [flowNodes, selectedNodeId]);

  const cancelRun = async () => {
    setCanceling(true);
    try {
      await cancelWorkflowRun(runId, crypto.randomUUID());
      toast.success('Cancellation requested');
      setCancelOpen(false);
      refresh();
    } catch {
      toast.error('Cancellation was not accepted. Refresh the run and try again.');
    } finally {
      setCanceling(false);
    }
  };

  if (!result && loading) {
    return (
      <section className="workflow-page-state" aria-live="polite">
        <Spinner size={18} />
        <strong>Loading graph-native diagnostics</strong>
        <span>Reading the durable plan, node state, and safe operational facts.</span>
      </section>
    );
  }

  if (!result && error) {
    return (
      <section className="workflow-page-state workflow-page-state--error" role="alert">
        <AlertTriangle size={24} />
        <strong>Graph diagnostics are unavailable</strong>
        <span>The graph-native workflow inspection route did not return a diagnostic. Legacy run details are intentionally not substituted.</span>
        <div>
          <Button onClick={refresh}><RefreshCw size={13} /> Retry inspection</Button>
          <Button variant="outline" onClick={navigation.goBack}><ChevronLeft size={13} /> Back to runs</Button>
        </div>
      </section>
    );
  }

  if (!result) return null;

  const terminal = isTerminalWorkflowRun(result.run.status);
  const failedNodes = result.nodes.filter(node => FAILED_NODE_STATES.has(node.status)).length;
  const openWaits = result.nodes.filter(node => node.wait?.status === 'open').length;
  const pinCount = result.nodes.filter(node => node.pin !== undefined).length;
  const holders = result.resources?.holders.length ?? 0;
  const waiters = result.resources?.waiters.length ?? 0;
  const decisions = result.control.decisions?.length ?? 0;
  const boundaryFacts = [
    ...Object.entries(result.truncated).filter(([, active]) => active).map(([key]) => `${key} truncated`),
    ...(result.omissions ?? []),
  ];
  const authoredPositions = result.plan.nodes.filter(node => node.position !== undefined).length;

  return (
    <div className="workflow-run-page">
      <header className="workflow-command-bar">
        <Button variant="ghost" size="icon-sm" aria-label="Back to runs" onClick={navigation.goBack}><ChevronLeft size={15} /></Button>
        <div className="workflow-command-identity">
          <Workflow size={16} />
          <div>
            <strong>{result.plan.definition.id || result.plan.id}</strong>
            <code>{result.run.id}</code>
          </div>
        </div>
        <StatusBadge status={result.run.status} />
        {!terminal && <span className="workflow-live-indicator"><i /> durable live state</span>}
        <div className="workflow-command-spacer" />
        <span className="workflow-run-duration">{formatDuration(result.run.created_at, terminal ? result.run.updated_at : undefined)}</span>
        <Button variant="outline" size="sm" onClick={refresh} disabled={loading}><RefreshCw size={12} /> Refresh</Button>
        {!terminal && <Button variant="destructive" size="sm" onClick={() => setCancelOpen(true)}><Square size={11} /> Cancel</Button>}
      </header>

      {error && (
        <div className="workflow-inline-warning" role="status">
          <AlertTriangle size={13} /> The latest refresh failed. Showing the last bounded diagnostic.
          <Button variant="ghost" size="xs" onClick={refresh}>Retry</Button>
        </div>
      )}

      {boundaryFacts.length > 0 && (
        <div className="workflow-boundary-notice" role="status">
          <AlertTriangle size={13} />
          <span>Bounded view:</span>
          {boundaryFacts.map(fact => <code key={fact}>{fact.replace(/_/g, ' ')}</code>)}
          <strong>Missing facts are not reconstructed.</strong>
        </div>
      )}

      <section className="workflow-fact-bar" aria-label="Run operational summary">
        <div><span>Durable status</span><strong>{result.run.status.replace(/_/g, ' ')}</strong><code>gen {result.run.generation}</code></div>
        <div><span>Execution</span><strong>{plural(openWaits, 'open wait')}</strong><code>{plural(failedNodes, 'failed node')}</code></div>
        <div><span>Concurrency</span><strong>{plural(holders, 'holder')}</strong><code>{plural(waiters, 'queued')}</code></div>
        <div><span>Reuse</span><strong>{plural(pinCount, 'pin')}</strong><code>{result.replay ? 'replay bound' : 'no replay'}</code></div>
        <div><span>Control</span><strong>{plural(decisions, 'decision')}</strong><code>{result.control.terminal_intent?.status || 'intent open'}</code></div>
      </section>

      <main className="workflow-graph-layout">
        <section className="workflow-graph-panel" aria-label="Workflow execution graph">
          <header className="workflow-graph-heading">
            <div>
              <span>Execution graph</span>
              <strong>{result.plan.nodes.length} nodes · {result.plan.edges?.length ?? 0} edges</strong>
            </div>
            <div className="workflow-graph-legend" aria-label="Edge legend">
              <span className="is-data">typed data</span>
              <span className="is-control">control</span>
              <span className="is-catch">catch</span>
              <span className="is-finally">finally</span>
            </div>
          </header>
          {flowNodes.length === 0 ? (
            <div className="workflow-empty-graph">
              <GitBranch size={22} />
              <strong>No graph nodes were returned</strong>
              <span>Increase the diagnostic node bound or verify that the referenced plan is available.</span>
              <Button variant="outline" size="sm" onClick={refresh}>Retry inspection</Button>
            </div>
          ) : (
            <div className="workflow-graph-canvas">
              <ReactFlow
                nodes={flowNodes}
                edges={flowEdges}
                nodeTypes={nodeTypes}
                edgeTypes={edgeTypes}
                onNodeClick={(_, node) => setSelectedNodeId(node.id)}
                onSelectionChange={({ nodes }) => setSelectedNodeId(nodes[0]?.id ?? null)}
                nodesDraggable={false}
                nodesConnectable={false}
                edgesFocusable
                fitView
                fitViewOptions={{ padding: 0.24, maxZoom: 1.1 }}
                minZoom={0.25}
                maxZoom={1.6}
                proOptions={{ hideAttribution: true }}
              >
                <Background variant={BackgroundVariant.Dots} gap={20} size={1} color="#27272a" />
                <MiniMap
                  nodeColor={node => FAILED_NODE_STATES.has(String(node.data?.status)) ? '#ef4444' : node.data?.status === 'waiting' ? '#f59e0b' : '#3f3f46'}
                  maskColor="rgba(9, 9, 11, 0.76)"
                  pannable
                  zoomable
                />
                <Controls showInteractive={false} />
              </ReactFlow>
            </div>
          )}
          <footer className="workflow-graph-footer">
            <span>{authoredPositions}/{result.plan.nodes.length} authored positions</span>
            <span><LockKeyhole size={11} /> edge values use shared redacted references</span>
          </footer>
        </section>

        <WorkflowRunInspector result={result} selectedNodeId={selectedNodeId} onResume={setResumeNode} />
      </main>

      <section className="workflow-detail-grid">
        <WorkflowValueLedger result={result} />
        <WorkflowEventTrail result={result} />
      </section>

      <WorkflowResumeDialog
        open={resumeNode !== undefined}
        runId={runId}
        node={resumeNode}
        onOpenChange={open => { if (!open) setResumeNode(undefined); }}
        onResumed={() => {
          toast.success('Wait resume accepted');
          refresh();
        }}
      />

      <AlertDialog open={cancelOpen} onOpenChange={setCancelOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Cancel this workflow run?</AlertDialogTitle>
            <AlertDialogDescription>
              Cancellation stops new ordinary work and allows declared finalizer processing. Effects that already completed are not rolled back.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={canceling}>Keep running</AlertDialogCancel>
            <AlertDialogAction variant="destructive" disabled={canceling} onClick={cancelRun}>{canceling ? 'Requesting…' : 'Request cancel'}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
