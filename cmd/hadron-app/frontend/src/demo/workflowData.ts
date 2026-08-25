import type {
  WorkflowGraphDiagnostic,
  WorkflowNodeDiagnostic,
  WorkflowPlanNodeDiagnostic,
  WorkflowRenderedValue,
  WorkflowValueSetDiagnostic,
  WorkflowValueSetRef,
} from '../api/types';

const REDACTED = '[REDACTED]';
const BASE = Date.parse('2026-08-24T18:00:00Z');

function at(minutes: number): string {
  return new Date(BASE + minutes * 60_000).toISOString();
}

function digest(character: string): string {
  return `sha256:${character.repeat(64).slice(0, 64)}`;
}

function ref(id: string, character: string): WorkflowValueSetRef {
  return { id, digest: digest(character) };
}

const RECEIVE = ref('values-receive', '1');
const QUALIFY = ref('values-qualify', '2');
const APPROVAL = ref('values-approval', '3');
const PUBLISH = ref('values-publish', '4');

function renderedValue(
  type: string,
  payload: unknown,
  options: Partial<WorkflowRenderedValue> = {},
): WorkflowRenderedValue {
  return {
    type,
    payload,
    producer: { kind: 'workflow-node', reference: 'fixture' },
    media_type: 'application/json',
    digest: digest('a'),
    redaction: 'public',
    retention: 'run',
    masked: false,
    ...options,
  };
}

const VALUE_SETS: WorkflowValueSetDiagnostic[] = [
  {
    ref: RECEIVE,
    roles: ['node.receive.outputs'],
    values: {
      request: renderedValue('object', { release: '2026.08.24', channel: 'stable' }),
    },
  },
  {
    ref: QUALIFY,
    roles: ['node.qualify.outputs', 'node.approval.inputs'],
    values: {
      review_context: renderedValue('object', REDACTED, { redaction: 'private', masked: true }),
      report: renderedValue('artifact', {
        store: 'hadron-local',
        uri: 'artifact://run/report.json',
        digest: digest('b'),
        media_type: 'application/json',
        size_bytes: 4821,
        producer: { kind: 'workflow-node', reference: 'qualify', output: 'report' },
        redaction: 'public',
        retention: 'run',
      }, { media_type: 'application/json' }),
      credential: renderedValue('secret_ref', REDACTED, { redaction: 'secret', masked: true }),
    },
  },
  {
    ref: APPROVAL,
    roles: ['node.approval.outputs', 'node.publish.inputs', 'node.approval.wait.resume'],
    values: {
      decision: renderedValue('string', 'approved'),
      reviewer: renderedValue('string', REDACTED, { redaction: 'private', masked: true }),
    },
  },
  {
    ref: PUBLISH,
    roles: ['node.publish.outputs', 'run.outputs'],
    values: {
      release_url: renderedValue('string', 'https://releases.example.test/2026.08.24'),
    },
  },
];

const PLAN_NODES: WorkflowPlanNodeDiagnostic[] = [
  {
    id: 'receive', display_name: 'Receive release', kind: 'transform', kind_version: 'v1', ready_when: 'all_success',
    declared_effects: ['compute'], position: { x: 20, y: 140 },
    source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 18, path: ['steps', '0'] },
  },
  {
    id: 'qualify', display_name: 'Qualify candidate', kind: 'http', kind_version: 'v1', ready_when: 'all_success', needs: ['receive'],
    declared_effects: ['read'], catch_targets: ['cleanup'], retry: { attempts: 3, strategy: 'exponential', initial_delay: '2s', max_delay: '30s' },
    source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 27, path: ['steps', '1'] },
  },
  {
    id: 'approval', display_name: 'Release approval', kind: 'gate', kind_version: 'v1', ready_when: 'all_success', needs: ['qualify'],
    declared_effects: ['read'], position: { x: 590, y: 70 },
    source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 46, path: ['steps', '2'] },
  },
  {
    id: 'publish', display_name: 'Publish release', kind: 'cmd', kind_version: 'v1', ready_when: 'all_success', needs: ['approval'],
    declared_effects: ['mutate'], retry: { attempts: 2, strategy: 'fixed', initial_delay: '5s', max_delay: '5s' },
    source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 61, path: ['steps', '3'] },
  },
  {
    id: 'cleanup', display_name: 'Release cleanup', kind: 'cmd', kind_version: 'v1', ready_when: 'always', finally: true,
    declared_effects: ['mutate'], position: { x: 890, y: 310 },
    source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 78, path: ['steps', '4'] },
  },
];

type Scenario = 'active' | 'failed' | 'waiting' | 'completed';

function scenarioFor(runId: string): Scenario {
  if (runId.includes('waiting')) return 'waiting';
  if (runId.includes('002') || runId.includes('006') || runId.includes('010')) return 'failed';
  if (runId.includes('003')) return 'active';
  return 'completed';
}

function node(
  runId: string,
  definition: WorkflowPlanNodeDiagnostic,
  status: string,
  options: Partial<WorkflowNodeDiagnostic> = {},
): WorkflowNodeDiagnostic {
  const base: WorkflowNodeDiagnostic = {
    id: { run_id: runId, node_id: definition.id },
    status,
    origin: 'executed',
    latest_attempt: status === 'pending' ? 0 : 1,
    claim_generation: 0,
    generation: 1,
    created_at: at(0),
    updated_at: at(8),
    source: definition.source,
    definition,
    attempts: status === 'pending' ? [] : [{
      number: 1,
      status,
      executor: { kind: definition.kind, version: definition.kind_version ?? 'v1', target: 'local' },
      started_at: at(1),
      finished_at: status === 'running' || status === 'waiting' ? undefined : at(6),
      generation: 1,
    }],
    explanation: { code: `node_${status}`, message: status === 'pending' ? 'Waiting for upstream dependencies.' : `Node is ${status}.` },
    resources: {},
  };
  const merged = { ...base, ...options };
  if (!merged.explanation) merged.explanation = base.explanation;
  return merged;
}

function scenarioNodes(runId: string, scenario: Scenario): WorkflowNodeDiagnostic[] {
  const [receive, qualify, approval, publish, cleanup] = PLAN_NODES;
  const receiveNode = node(runId, receive, 'succeeded', { outputs: RECEIVE });
  const qualifyStatus = scenario === 'failed' ? 'failed' : scenario === 'active' ? 'running' : 'succeeded';
  const qualifyNode = node(runId, qualify, qualifyStatus, {
    inputs: RECEIVE,
    outputs: scenario === 'active' || scenario === 'failed' ? undefined : QUALIFY,
    latest_attempt: scenario === 'failed' ? 3 : 1,
    attempts: scenario === 'failed' ? [1, 2, 3].map(number => ({
      number,
      status: 'failed',
      executor: { kind: 'http', version: 'v1', target: 'https://release-check.example.test' },
      failure: { code: 'provider_unavailable', message: number === 3 ? 'Release qualification provider remained unavailable.' : 'Temporary provider failure.', retryable: number < 3 },
      started_at: at(number * 2),
      finished_at: at(number * 2 + 1),
      generation: number,
    })) : undefined,
    explanation: scenario === 'failed'
      ? { code: 'node_failed', message: 'Retry budget exhausted after three attempts.', failure: { code: 'provider_unavailable', message: 'Release qualification provider remained unavailable.' } }
      : undefined,
  });
  const approvalStatus = scenario === 'waiting' ? 'waiting' : scenario === 'completed' ? 'succeeded' : 'pending';
  const approvalNode = node(runId, approval, approvalStatus, {
    inputs: scenario === 'active' || scenario === 'failed' ? undefined : QUALIFY,
    outputs: scenario === 'completed' ? APPROVAL : undefined,
    wait: scenario === 'waiting' ? {
      id: 'wait-release-approval', kind: 'gate', status: 'open', wake_source: 'gate', visibility: 'private',
      payload: QUALIFY, generation: 1, created_at: at(7), updated_at: at(7), deadline: at(67),
    } : undefined,
    explanation: scenario === 'waiting'
      ? { code: 'wait_open', message: 'Waiting for an authorized release decision.' }
      : undefined,
  });
  const publishStatus = scenario === 'completed' ? 'succeeded' : scenario === 'failed' ? 'skipped' : 'pending';
  const publishNode = node(runId, publish, publishStatus, {
    inputs: scenario === 'completed' ? APPROVAL : undefined,
    outputs: scenario === 'completed' ? PUBLISH : undefined,
    explanation: scenario === 'failed'
      ? { code: 'upstream_terminal', message: 'Skipped because qualification failed.' }
      : undefined,
  });
  const cleanupStatus = scenario === 'completed' || scenario === 'failed' ? 'succeeded' : 'pending';
  const cleanupNode = node(runId, cleanup, cleanupStatus, {
    explanation: { code: cleanupStatus === 'succeeded' ? 'finalizer_completed' : 'finalizer_pending', message: cleanupStatus === 'succeeded' ? 'Finalizer completed.' : 'Finalizer will run when the graph closes.' },
  });
  return [receiveNode, qualifyNode, approvalNode, publishNode, cleanupNode];
}

export function getDemoWorkflowDiagnostic(runId: string): WorkflowGraphDiagnostic {
  const scenario = scenarioFor(runId);
  const nodes = scenarioNodes(runId, scenario);
  const status = scenario === 'completed' ? 'succeeded' : scenario === 'failed' ? 'failed' : 'running';
  const values = scenario === 'active' ? VALUE_SETS.slice(0, 1) : scenario === 'failed' ? VALUE_SETS.slice(0, 1) : scenario === 'waiting' ? VALUE_SETS.slice(0, 2) : VALUE_SETS;
  return {
    schema_version: '1',
    run: {
      id: runId,
      plan: { id: 'release-workflow', version: '2.4.0', digest: digest('c'), schema_version: '1' },
      status,
      inputs: RECEIVE,
      outputs: scenario === 'completed' ? PUBLISH : undefined,
      generation: 7,
      created_at: at(0),
      updated_at: at(12),
    },
    plan: {
      id: 'release-workflow', version: '2.4.0', digest: digest('c'), schema_version: '1', graph_digest: digest('d'),
      definition: { authority: 'registry', kind: 'workflow', id: 'release/publish', version: '2.4.0', digest: digest('e') },
      provenance: { authority: 'project', origin: 'registry', locator: '/workflows/release.workflow.yaml', revision: 'release-24', digest: digest('e') },
      source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 1 },
      nodes: PLAN_NODES,
      edges: [
        { from: 'receive', to: 'qualify', kind: 'data', source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 29 }, value_flow: { source_outputs: [{ invocation: nodes[0].id, values: RECEIVE }], target_inputs: [{ invocation: nodes[1].id, values: RECEIVE }] } },
        { from: 'qualify', to: 'cleanup', kind: 'control', source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 41 } },
        { from: 'qualify', to: 'approval', kind: 'data', source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 47 }, value_flow: scenario === 'active' || scenario === 'failed' ? { values_omitted: scenario === 'failed' } : { source_outputs: [{ invocation: nodes[1].id, values: QUALIFY }], target_inputs: [{ invocation: nodes[2].id, values: QUALIFY }] } },
        { from: 'approval', to: 'publish', kind: 'data', source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 62 }, value_flow: scenario === 'completed' ? { source_outputs: [{ invocation: nodes[2].id, values: APPROVAL }], target_inputs: [{ invocation: nodes[3].id, values: APPROVAL }] } : {} },
        { from: 'publish', to: 'cleanup', kind: 'control', source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 79 } },
      ],
      activations: [{ id: 'release-requested', kind: 'event', source: { format: 'workflow', locator: '/workflows/release.workflow.yaml', start_line: 8 } }],
    },
    nodes,
    values,
    events: [
      { sequence: 1, run_id: runId, invocation: nodes[0].id, type: 'node_status_changed', occurred_at: at(1), attributes: { to_status: 'succeeded' }, redaction: 'public', retention: 'run', masked: false },
      { sequence: 2, run_id: runId, invocation: nodes[1].id, type: 'node_attempt_finished', occurred_at: at(6), attributes: scenario === 'failed' ? { failure_code: 'provider_unavailable' } : { to_status: nodes[1].status }, redaction: scenario === 'failed' ? 'private' : 'public', retention: 'run', masked: scenario === 'failed' },
    ],
    control: {
      decisions: scenario === 'failed' ? [{ source: nodes[1].id, kind: 'catch', outcome: 'selected', targets: [nodes[4].id], generation: 1, created_at: at(10) }] : [],
      terminal_intent: scenario === 'failed' || scenario === 'completed' ? { intended_status: status, status: 'completed', finalizers: [{ invocation: nodes[4].id, scope: nodes.slice(0, 4).map(item => item.id), order: 0 }] } : undefined,
    },
    replay: scenario === 'completed' ? { source_run_id: 'run-release-previous', from_node_id: 'qualify', plan_digest: digest('c'), created_at: at(-30), policy: [{ invocation: nodes[0].id, allow: true, code: 'reused', reason: 'immutable output reused' }] } : undefined,
    resources: scenario === 'active' ? {
      holders: [{
        resource: { kind: 'worker', name: 'global' }, invocation: nodes[1].id, units: 1,
        claim_generation: 1, owner: 'local-worker', acquired_at: at(8), expires_at: at(13),
      }],
      waiters: [],
    } : scenario === 'waiting' ? {
      holders: [],
      waiters: [{
        invocation: nodes[2].id,
        requirements: [{ resource: { kind: 'concurrency_key', name: 'release-channel' }, units: 1, limit: 1 }],
        blocked: [{ kind: 'concurrency_key', name: 'release-channel' }],
        priority: 10, enqueued_at: at(6), updated_at: at(7),
      }],
    } : { holders: [], waiters: [] },
    start_activation: { activation_id: 'release-requested', fire_identity_digest: digest('f'), occurred_at: at(0) },
    start_policy: {
      declared_effects: ['read', 'compute', 'mutate'], required_capabilities: ['network', 'process'],
      blast_radius: { read: 2, compute: 1, mutate: 2 }, node_count: 5,
      dry_run_available: false, confirmation_advised: true, decision: 'allow', exposure_ref: 'release-operators',
    },
    capabilities: { control_decisions: true, replay_provenance: true, pin_bindings: true, concurrency_state: true, start_binding: true, activation_attempts: true },
    omissions: [],
    truncated: scenario === 'failed' ? { attempts: true, events: true, values: true } : {},
  };
}
