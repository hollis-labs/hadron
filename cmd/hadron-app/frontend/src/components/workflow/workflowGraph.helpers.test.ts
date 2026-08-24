import assert from 'node:assert/strict';
import test from 'node:test';

import type { WorkflowGraphDiagnostic } from '../../api/types';
import { getDemoWorkflowDiagnostic } from '../../demo/workflowData';
import { buildWorkflowGraph, isTerminalWorkflowRun, selectPrimaryInvocation, sortWorkflowInvocations } from './workflowGraph.helpers';

test('buildWorkflowGraph preserves authored positions and uses a deterministic layered fallback', () => {
  const result = getDemoWorkflowDiagnostic('run-demo-001-a1b2c3d4');
  const first = buildWorkflowGraph(result);
  const second = buildWorkflowGraph(structuredClone(result));
  assert.deepEqual(first, second);

  const receive = first.nodes.find(node => node.id === 'receive');
  const qualify = first.nodes.find(node => node.id === 'qualify');
  assert.deepEqual(receive?.position, { x: 20, y: 140 });
  assert.equal(receive?.data.authoredPosition, true);
  assert.deepEqual(qualify?.position, { x: 340, y: 50 });
  assert.equal(qualify?.data.authoredPosition, false);
});

test('buildWorkflowGraph distinguishes typed/control/finally flow and uses shared value associations', () => {
  const result = getDemoWorkflowDiagnostic('run-demo-001-a1b2c3d4');
  const graph = buildWorkflowGraph(result);
  const firstData = graph.edges.find(edge => edge.source === 'receive');
  const catchEdge = graph.edges.find(edge => edge.target === 'cleanup' && edge.data.kind === 'catch');
  const finalizer = graph.edges.find(edge => edge.target === 'cleanup' && edge.data.kind === 'finally');
  assert.equal(firstData?.data.kind, 'data');
  assert.deepEqual(firstData?.data.valueNames, ['request']);
  assert.equal(firstData?.data.state, 'traversed');
  assert.equal(catchEdge?.data.kind, 'catch');
  assert.equal(finalizer?.data.kind, 'finally');
});

test('graph fixtures cover active, failed, waiting, and completed durable states', () => {
  const cases = [
    ['run-demo-003-i9j0k1l2', 'qualify', 'running'],
    ['run-demo-002-e5f6g7h8', 'qualify', 'failed'],
    ['run-demo-waiting-r3s4t5u6', 'approval', 'waiting'],
    ['run-demo-001-a1b2c3d4', 'publish', 'succeeded'],
  ] as const;
  for (const [runId, nodeId, status] of cases) {
    const graph = buildWorkflowGraph(getDemoWorkflowDiagnostic(runId));
    assert.equal(graph.nodes.find(node => node.id === nodeId)?.data.status, status, runId);
  }
});

test('edge value omission and redaction remain explicit without copying payloads into graph state', () => {
  const failed = getDemoWorkflowDiagnostic('run-demo-002-e5f6g7h8');
  const edge = buildWorkflowGraph(failed).edges.find(item => item.source === 'qualify' && item.target === 'approval');
  assert.equal(edge?.data.valuesOmitted, true);

  const waiting = getDemoWorkflowDiagnostic('run-demo-waiting-r3s4t5u6');
  const waitingEdge = buildWorkflowGraph(waiting).edges.find(item => item.source === 'qualify' && item.target === 'approval');
  assert.equal(waitingEdge?.data.maskedValueCount, 2);
  const encoded = JSON.stringify(waitingEdge);
  assert.equal(encoded.includes('artifact://run/report.json'), false);
  assert.equal(encoded.includes('[REDACTED]'), false);
});

test('edge truncation remains a transport fact and is not reconstructed in the frontend', () => {
  const result = getDemoWorkflowDiagnostic('run-demo-001-a1b2c3d4');
  const truncated: WorkflowGraphDiagnostic = {
    ...result,
    plan: { ...result.plan, edges: result.plan.edges?.slice(0, 1) },
    truncated: { ...result.truncated, edges: true },
  };
  const graph = buildWorkflowGraph(truncated);
  assert.equal(graph.edges.length, 1);
  assert.equal(truncated.truncated.edges, true);
});

test('invocation selection prefers an open wait and otherwise the latest highest-priority iteration', () => {
  const result = getDemoWorkflowDiagnostic('run-demo-waiting-r3s4t5u6');
  const base = result.nodes.find(node => node.id.node_id === 'approval')!;
  const completed = { ...structuredClone(base), id: { ...base.id, iteration: 0 }, status: 'succeeded', wait: undefined, updated_at: '2026-08-24T19:00:00Z' };
  const open = { ...structuredClone(base), id: { ...base.id, iteration: 1 }, updated_at: '2026-08-24T18:00:00Z' };
  assert.equal(selectPrimaryInvocation([completed, open])?.id.iteration, 1);

  const failed = { ...completed, id: { ...base.id, iteration: 2 }, status: 'failed', updated_at: '2026-08-24T20:00:00Z' };
  assert.deepEqual(sortWorkflowInvocations([completed, failed]).map(node => node.id.iteration), [2, 0]);
});

test('terminal workflow states stop polling for crashes and timeouts as well as ordinary completion', () => {
  for (const status of ['succeeded', 'failed', 'crashed', 'timed_out', 'canceled']) {
    assert.equal(isTerminalWorkflowRun(status), true, status);
  }
  assert.equal(isTerminalWorkflowRun('running'), false);
});
