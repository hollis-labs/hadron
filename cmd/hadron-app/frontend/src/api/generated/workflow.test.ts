import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import {
  createGraphAuthoringEnvelope,
  createWorkflowSourceAuthoringEnvelope,
  decodeAuthoringEnvelope,
  HadronWorkflowClient,
  WORKFLOW_API_SCHEMA,
  WORKFLOW_AUTHORING_SCHEMA,
  WORKFLOW_GRAPH_SCHEMA,
  WORKFLOW_SOURCE_SCHEMA,
  type WorkflowTransport,
} from './workflow';

const graph = {
  id: 'generated-client',
  version: 'v1',
  digest: '',
  nodes: [],
};

test('generated authoring decoder negotiates exact schemas and rejects unknown fields', () => {
  const canonicalFixture = JSON.parse(readFileSync(new URL('./testdata/equivalent.graph.json', import.meta.url), 'utf8'));
  assert.deepEqual(createGraphAuthoringEnvelope(canonicalFixture).graph, canonicalFixture);
  const envelope = createGraphAuthoringEnvelope(graph);
  assert.equal(envelope.schema_id, WORKFLOW_AUTHORING_SCHEMA.id);
  assert.equal(envelope.material_schema_id, WORKFLOW_GRAPH_SCHEMA.id);
  assert.equal(WORKFLOW_API_SCHEMA.version, '1');
  assert.equal(createWorkflowSourceAuthoringEnvelope('workflow: {id: generated, version: v1}').material_schema_id, WORKFLOW_SOURCE_SCHEMA.id);

  assert.throws(() => decodeAuthoringEnvelope({ ...envelope, unexpected: true }), /unknown/);
  assert.throws(() => decodeAuthoringEnvelope({ ...envelope, graph: { ...graph, unexpected: true } }), /unknown/);
  assert.throws(() => decodeAuthoringEnvelope({ ...envelope, schema_version: '2' }), /unsupported/);
  assert.throws(() => decodeAuthoringEnvelope(envelope, { maximum_bytes: 1 }), /maximum_bytes/);
  assert.throws(() => decodeAuthoringEnvelope({
    ...envelope,
    graph: { ...graph, nodes: [{ id: 'one', kind: 'fixture' }] },
  }, { maximum_nodes: 0 }), /structural bounds/);
  assert.throws(() => decodeAuthoringEnvelope({
    ...envelope,
    graph: { ...graph, edges: [{ from: 'one', to: 'two', kind: 'control' }] },
  }, { maximum_edges: 0 }), /structural bounds/);
  assert.throws(() => decodeAuthoringEnvelope({ ...envelope, graph: { ...graph, metadata: { nested: true } } }, { maximum_depth: 2 }), /maximum_depth/);

  for (const field of ['maximum_bytes', 'maximum_depth', 'maximum_nodes', 'maximum_edges'] as const) {
    for (const invalid of [Number.NaN, Number.POSITIVE_INFINITY, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      assert.throws(() => decodeAuthoringEnvelope(envelope, { [field]: invalid }), /finite nonnegative safe integer/);
    }
  }
});

test('generated authoring decoder rejects lossy JavaScript values and returns a canonical clone', () => {
  const withConfig = (value: unknown) => ({
    ...createGraphAuthoringEnvelope(graph),
    graph: {
      ...graph,
      nodes: [{ id: 'work', kind: 'fixture', config: { nested: value } }],
    },
  });
  for (const value of [undefined, () => 'lost', Symbol('lost'), Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1, 1n, new Date(0)]) {
    assert.throws(() => decodeAuthoringEnvelope(withConfig(value)), /JSON-safe|plain JSON/);
  }
  const sparse = new Array<unknown>(2);
  sparse[1] = 'present';
  assert.throws(() => decodeAuthoringEnvelope(withConfig(sparse)), /sparse array hole/);

  const original = withConfig({ list: [1, 'two', null], enabled: true });
  const decoded = decodeAuthoringEnvelope(original);
  assert.notStrictEqual(decoded, original);
  assert.deepEqual(decoded, original);
  (original.graph.nodes[0].config.nested as { enabled: boolean }).enabled = false;
  assert.deepEqual(decoded.graph?.nodes[0].config?.nested, { list: [1, 'two', null], enabled: true });
});

test('generated client uses escaped opaque IDs and mutation idempotency headers', async () => {
  const calls: Array<{ path: string; init: RequestInit }> = [];
  const transport: WorkflowTransport = async <T>(path: string, init: RequestInit) => {
    calls.push({ path, init });
    return {} as T;
  };
  const client = new HadronWorkflowClient(transport);
  await client.cancelWorkflowRun({
    run_id: 'source/id',
    identity: { source_authority: 'http' },
    idempotency_key: 'cancel-key',
  });
  await client.rerunWorkflow({
    source_run_id: 'source/id',
    run_id: 'next',
    from_node_id: 'work',
    identity: { source_authority: 'http' },
    idempotency_key: 'rerun-key',
  });
  await client.fireWorkflowActivation({
    registration_id: 'activation/id',
    idempotency_key: 'fire-key',
    occurred_at: '2026-08-24T12:00:00Z',
  });

  assert.deepEqual(calls.map(call => call.path), [
    '/v1/workflows/runs/source%2Fid/cancel',
    '/v1/workflows/runs/source%2Fid/rerun',
    '/v1/workflows/activations/activation%2Fid/fire',
  ]);
  assert.deepEqual(calls.map(call => new Headers(call.init.headers).get('Idempotency-Key')), ['cancel-key', 'rerun-key', 'fire-key']);
});
