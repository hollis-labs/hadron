import assert from 'node:assert/strict';
import test from 'node:test';

import { setAPIBaseURL } from './http';
import {
  explainWorkflow,
  inspectWorkflowRun,
  cancelWorkflowRun,
  rerunWorkflowRun,
  resumeWorkflowWait,
  runWorkflow,
  validateWorkflow,
} from './workflows';

type CapturedRequest = { url: string; init?: RequestInit };

function captureFetch(response: unknown): CapturedRequest[] {
  const calls: CapturedRequest[] = [];
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    calls.push({ url: String(input), init });
    return new Response(JSON.stringify(response), { status: 200, headers: { 'Content-Type': 'application/json' } });
  }) as typeof fetch;
  return calls;
}

test('workflow commands use the shared same-origin HTTP surface', async () => {
  setAPIBaseURL('');
  const calls = captureFetch({ diagnostics: [], dry_run: false, run: { id: 'run-1' } });
  const definition = { kind: 'file' as const, id: 'deploy', locator: 'deploy.workflow.yaml' };

  await validateWorkflow(definition);
  await explainWorkflow({ run_id: 'explain-1', definition, idempotency_key: 'explain-1' });
  await runWorkflow({ run_id: 'run-1', definition, idempotency_key: 'run-1' });
  await inspectWorkflowRun('run-1');

  assert.deepEqual(calls.map(call => call.url), [
    '/v1/workflows/validate',
    '/v1/workflows/explain',
    '/v1/workflows/runs',
    '/v1/workflows/runs/run-1/inspect',
  ]);
  for (const call of calls) {
    const body = JSON.parse(String(call.init?.body));
    assert.equal(body.identity.source_authority, 'http');
    assert.equal(call.init?.method, 'POST');
  }
  assert.equal(new Headers(calls[1].init?.headers).get('Idempotency-Key'), 'explain-1');
  assert.equal(new Headers(calls[2].init?.headers).get('Idempotency-Key'), 'run-1');
});

test('workflow replay preserves an escaped opaque run ID', async () => {
  setAPIBaseURL('http://daemon.test/');
  const calls = captureFetch({ outcome: 'applied', run: { id: 'next' }, provenance: {} });
  await rerunWorkflowRun('source/id', 'deploy', 'next', 'replay-key');
  assert.equal(calls[0].url, 'http://daemon.test/v1/workflows/runs/source%2Fid/rerun');
  const body = JSON.parse(String(calls[0].init?.body));
  assert.deepEqual(body, {
    source_run_id: 'source/id',
    run_id: 'next',
    from_node_id: 'deploy',
    idempotency_key: 'replay-key',
    identity: { source_authority: 'http' },
  });
});

test('cancel, resume, and rerun send exact mutation idempotency headers', async () => {
  setAPIBaseURL('');
  const calls = captureFetch({ outcome: 'applied', run: { id: 'next' }, provenance: {} });
  await cancelWorkflowRun('source/cancel', 'cancel-key');
  const resumePayload = {
    type: 'object',
    inline: { approved: true },
    producer: { kind: 'operator', reference: 'browser', output: 'resume' },
    media_type: 'application/json',
    digest: 'sha256:resume',
    redaction: 'private',
    retention: 'run',
  };
  await resumeWorkflowWait({
    run_id: 'source/resume',
    wait_id: 'wait-1',
    correlation: 'correlation-1',
    token: 'opaque-token',
    wake_source: 'gate',
    payload: resumePayload,
    idempotency_key: 'resume-key',
  });
  await rerunWorkflowRun('source/replay', 'deploy', 'next', 'rerun-key');

  assert.deepEqual(calls.map(call => call.url), [
    '/v1/workflows/runs/source%2Fcancel/cancel',
    '/v1/workflows/runs/source%2Fresume/resume',
    '/v1/workflows/runs/source%2Freplay/rerun',
  ]);
  assert.deepEqual(calls.map(call => new Headers(call.init?.headers).get('Idempotency-Key')), [
    'cancel-key', 'resume-key', 'rerun-key',
  ]);
  for (const call of calls) {
    assert.equal(new Headers(call.init?.headers).get('Content-Type'), 'application/json');
    assert.equal(call.init?.method, 'POST');
  }
  assert.deepEqual(JSON.parse(String(calls[0].init?.body)), {
    run_id: 'source/cancel',
    identity: { source_authority: 'http' },
    idempotency_key: 'cancel-key',
    reason: 'operator requested cancellation from browser UI',
  });
  assert.deepEqual(JSON.parse(String(calls[1].init?.body)), {
    run_id: 'source/resume',
    wait_id: 'wait-1',
    correlation: 'correlation-1',
    token: 'opaque-token',
    wake_source: 'gate',
    payload: resumePayload,
    idempotency_key: 'resume-key',
    identity: { source_authority: 'http' },
  });
  assert.deepEqual(JSON.parse(String(calls[2].init?.body)), {
    source_run_id: 'source/replay',
    run_id: 'next',
    from_node_id: 'deploy',
    idempotency_key: 'rerun-key',
    identity: { source_authority: 'http' },
  });
});
