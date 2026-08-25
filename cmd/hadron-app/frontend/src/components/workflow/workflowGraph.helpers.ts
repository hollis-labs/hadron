import type {
  WorkflowControlDecisionDiagnostic,
  WorkflowGraphDiagnostic,
  WorkflowNodeDiagnostic,
  WorkflowPlanEdgeDiagnostic,
  WorkflowPlanNodeDiagnostic,
  WorkflowSourceDiagnostic,
  WorkflowValueSetDiagnostic,
  WorkflowValueSetRef,
} from '../../api/types';

export type WorkflowRouteKind = 'data' | 'control' | 'catch' | 'switch' | 'finally';
export type WorkflowFlowState = 'idle' | 'active' | 'traversed' | 'failed' | 'skipped' | 'selected';

export interface WorkflowRunNodeData extends Record<string, unknown> {
  definition: WorkflowPlanNodeDiagnostic;
  invocations: WorkflowNodeDiagnostic[];
  status: string;
  authoredPosition: boolean;
}

export interface WorkflowRunEdgeData extends Record<string, unknown> {
  kind: WorkflowRouteKind;
  state: WorkflowFlowState;
  valueNames: string[];
  maskedValueCount: number;
  valuesOmitted: boolean;
  sourceRef?: WorkflowSourceDiagnostic;
}

export interface WorkflowGraphNode {
  id: string;
  type: 'workflowRunNode';
  position: { x: number; y: number };
  data: WorkflowRunNodeData;
}

export interface WorkflowGraphEdge {
  id: string;
  type: 'workflowRunEdge';
  source: string;
  target: string;
  animated: boolean;
  data: WorkflowRunEdgeData;
}

const STATUS_PRIORITY: Record<string, number> = {
  failed: 100,
  crashed: 95,
  timed_out: 90,
  running: 80,
  claimed: 75,
  ready: 70,
  waiting: 65,
  blocked: 60,
  canceling: 55,
  pending: 50,
  canceled: 40,
  cancelled: 40,
  skipped: 30,
  succeeded: 20,
  success: 20,
};

const TERMINAL_NODE_STATUSES = new Set([
  'succeeded', 'success', 'failed', 'crashed', 'timed_out', 'canceled', 'cancelled', 'skipped',
]);

function refKey(ref: WorkflowValueSetRef): string {
  return `${ref.id}\0${ref.digest}`;
}

export function aggregateNodeStatus(invocations: WorkflowNodeDiagnostic[]): string {
  if (invocations.length === 0) return 'pending';
  return [...invocations]
    .sort((left, right) => {
      const priority = (STATUS_PRIORITY[right.status] ?? 0) - (STATUS_PRIORITY[left.status] ?? 0);
      if (priority !== 0) return priority;
      return invocationKey(left).localeCompare(invocationKey(right));
    })[0].status;
}

export function invocationKey(node: WorkflowNodeDiagnostic): string {
  const iteration = node.id.iteration === undefined ? '' : `:${node.id.iteration}`;
  return `${node.id.node_id}${iteration}`;
}

function invocationSelectionPriority(node: WorkflowNodeDiagnostic): number {
  if (node.wait?.status === 'open') return 1_000;
  return STATUS_PRIORITY[node.status] ?? 0;
}

export function sortWorkflowInvocations(invocations: WorkflowNodeDiagnostic[]): WorkflowNodeDiagnostic[] {
  return [...invocations].sort((left, right) => {
    const priority = invocationSelectionPriority(right) - invocationSelectionPriority(left);
    if (priority !== 0) return priority;
    const updated = Date.parse(right.updated_at) - Date.parse(left.updated_at);
    if (Number.isFinite(updated) && updated !== 0) return updated;
    const iteration = (right.id.iteration ?? '').localeCompare(left.id.iteration ?? '', undefined, { numeric: true });
    if (iteration !== 0) return iteration;
    return invocationKey(left).localeCompare(invocationKey(right));
  });
}

export function selectPrimaryInvocation(invocations: WorkflowNodeDiagnostic[]): WorkflowNodeDiagnostic | undefined {
  return sortWorkflowInvocations(invocations)[0];
}

function routeKind(edge: WorkflowPlanEdgeDiagnostic, definitions: Map<string, WorkflowPlanNodeDiagnostic>): WorkflowRouteKind {
  const source = definitions.get(edge.from);
  const target = definitions.get(edge.to);
  if (source?.catch_targets?.includes(edge.to)) return 'catch';
  if (source?.switch_targets?.includes(edge.to)) return 'switch';
  if (target?.finally) return 'finally';
  return edge.kind === 'data' ? 'data' : 'control';
}

function selectedDecision(
  edge: WorkflowPlanEdgeDiagnostic,
  decisions: WorkflowControlDecisionDiagnostic[],
): WorkflowControlDecisionDiagnostic | undefined {
  return decisions.find(decision =>
    decision.source.node_id === edge.from &&
    decision.targets?.some(target => target.node_id === edge.to),
  );
}

function edgeState(
  edge: WorkflowPlanEdgeDiagnostic,
  byNode: Map<string, WorkflowNodeDiagnostic[]>,
  selected: boolean,
): WorkflowFlowState {
  if (selected) return 'selected';
  const sourceStatus = aggregateNodeStatus(byNode.get(edge.from) ?? []);
  const targetStatus = aggregateNodeStatus(byNode.get(edge.to) ?? []);
  if (sourceStatus === 'failed' || sourceStatus === 'crashed' || sourceStatus === 'timed_out') return 'failed';
  if (targetStatus === 'skipped') return 'skipped';
  if (sourceStatus === 'running' || sourceStatus === 'claimed' || targetStatus === 'running') return 'active';
  if (TERMINAL_NODE_STATUSES.has(sourceStatus) && targetStatus !== 'pending') return 'traversed';
  return 'idle';
}

function edgeValueFacts(
  edge: WorkflowPlanEdgeDiagnostic,
  values: Map<string, WorkflowValueSetDiagnostic>,
): Pick<WorkflowRunEdgeData, 'valueNames' | 'maskedValueCount' | 'valuesOmitted'> {
  const names = new Set<string>();
  let maskedValueCount = 0;
  const refs = [
    ...(edge.value_flow?.source_outputs ?? []),
    ...(edge.value_flow?.target_inputs ?? []),
  ];
  const seen = new Set<string>();
  for (const association of refs) {
    const key = refKey(association.values);
    if (seen.has(key)) continue;
    seen.add(key);
    const set = values.get(key);
    if (!set) continue;
    for (const [name, value] of Object.entries(set.values)) {
      names.add(name);
      if (value.masked || value.redaction === 'secret') maskedValueCount += 1;
    }
  }
  return {
    valueNames: [...names].sort(),
    maskedValueCount,
    valuesOmitted: edge.value_flow?.values_omitted ?? false,
  };
}

function deterministicPositions(nodes: WorkflowPlanNodeDiagnostic[], edges: WorkflowPlanEdgeDiagnostic[]): Map<string, { x: number; y: number }> {
  const ids = nodes.map(node => node.id).sort();
  const visible = new Set(ids);
  const incoming = new Map(ids.map(id => [id, 0]));
  const outgoing = new Map(ids.map(id => [id, [] as string[]]));
  for (const edge of edges) {
    if (!visible.has(edge.from) || !visible.has(edge.to)) continue;
    incoming.set(edge.to, (incoming.get(edge.to) ?? 0) + 1);
    outgoing.get(edge.from)?.push(edge.to);
  }
  for (const targets of outgoing.values()) targets.sort();
  const depths = new Map(ids.map(id => [id, 0]));
  const queue = ids.filter(id => incoming.get(id) === 0);
  const visited = new Set<string>();
  while (queue.length > 0) {
    const current = queue.shift()!;
    visited.add(current);
    for (const target of outgoing.get(current) ?? []) {
      depths.set(target, Math.max(depths.get(target) ?? 0, (depths.get(current) ?? 0) + 1));
      incoming.set(target, (incoming.get(target) ?? 1) - 1);
      if (incoming.get(target) === 0) {
        queue.push(target);
        queue.sort();
      }
    }
  }
  // A validated plan is acyclic. This stable fallback keeps a corrupt or
  // partially-truncated response inspectable without inventing edge semantics.
  for (const id of ids) {
    if (!visited.has(id)) depths.set(id, 0);
  }
  const byDepth = new Map<number, string[]>();
  for (const id of ids) {
    const depth = depths.get(id) ?? 0;
    const layer = byDepth.get(depth) ?? [];
    layer.push(id);
    byDepth.set(depth, layer);
  }
  const positions = new Map<string, { x: number; y: number }>();
  for (const [depth, layer] of [...byDepth.entries()].sort(([left], [right]) => left - right)) {
    layer.sort();
    layer.forEach((id, index) => positions.set(id, { x: 40 + depth * 300, y: 50 + index * 190 }));
  }
  return positions;
}

function hasFinitePosition(node: WorkflowPlanNodeDiagnostic): boolean {
  return node.position !== undefined && Number.isFinite(node.position.x) && Number.isFinite(node.position.y);
}

export function buildWorkflowGraph(result: WorkflowGraphDiagnostic): { nodes: WorkflowGraphNode[]; edges: WorkflowGraphEdge[] } {
  const planEdges = result.plan.edges ?? [];
  const definitions = new Map(result.plan.nodes.map(node => [node.id, node]));
  const invocationsByNode = new Map<string, WorkflowNodeDiagnostic[]>();
  for (const invocation of result.nodes) {
    const list = invocationsByNode.get(invocation.id.node_id) ?? [];
    list.push(invocation);
    invocationsByNode.set(invocation.id.node_id, list);
  }
  for (const invocations of invocationsByNode.values()) {
    invocations.sort((left, right) => invocationKey(left).localeCompare(invocationKey(right)));
  }
  const fallback = deterministicPositions(result.plan.nodes, planEdges);
  const nodes = [...result.plan.nodes]
    .sort((left, right) => left.id.localeCompare(right.id))
    .map(definition => {
      const authoredPosition = hasFinitePosition(definition);
      return {
        id: definition.id,
        type: 'workflowRunNode' as const,
        position: authoredPosition ? { ...definition.position! } : fallback.get(definition.id) ?? { x: 40, y: 50 },
        data: {
          definition,
          invocations: invocationsByNode.get(definition.id) ?? [],
          status: aggregateNodeStatus(invocationsByNode.get(definition.id) ?? []),
          authoredPosition,
        },
      };
    });
  const values = new Map((result.values ?? []).map(set => [refKey(set.ref), set]));
  const decisions = result.control.decisions ?? [];
  const edges = planEdges.map((edge, index) => {
    const kind = routeKind(edge, definitions);
    const selected = selectedDecision(edge, decisions) !== undefined;
    const state = edgeState(edge, invocationsByNode, selected);
    return {
      id: `${edge.from}:${edge.to}:${edge.kind}:${index}`,
      type: 'workflowRunEdge' as const,
      source: edge.from,
      target: edge.to,
      animated: state === 'active',
      data: {
        kind,
        state,
        sourceRef: edge.source,
        ...edgeValueFacts(edge, values),
      },
    };
  });
  return { nodes, edges };
}

export function isTerminalWorkflowRun(status: string): boolean {
  return new Set(['succeeded', 'success', 'failed', 'crashed', 'timed_out', 'canceled', 'cancelled']).has(status);
}

export function findValueSet(result: WorkflowGraphDiagnostic, ref?: WorkflowValueSetRef): WorkflowValueSetDiagnostic | undefined {
  if (!ref) return undefined;
  return (result.values ?? []).find(set => refKey(set.ref) === refKey(ref));
}

export function sourceLabel(source?: WorkflowSourceDiagnostic): string {
  if (!source) return 'Source unavailable';
  const line = source.start_line ? `:${source.start_line}${source.start_column ? `:${source.start_column}` : ''}` : '';
  return `${source.locator}${line}`;
}
