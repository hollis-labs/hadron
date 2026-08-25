import assert from 'node:assert/strict';
import test from 'node:test';

import { buildInlineResumeValue, canonicalJSONStringify, workflowWaitResumePolicy } from './workflowResume.helpers';

test('canonicalJSONStringify orders nested object keys without changing arrays', () => {
  assert.equal(canonicalJSONStringify({ z: 1, a: { y: 2, b: 3 }, rows: [{ z: 1, a: 2 }] }), '{"a":{"b":3,"y":2},"rows":[{"a":2,"z":1}],"z":1}');
});

test('canonicalJSONStringify rejects integers that cannot cross JSON losslessly', () => {
  assert.throws(() => canonicalJSONStringify({ value: Number.MAX_SAFE_INTEGER + 1 }), /lossless JSON/);
});

test('buildInlineResumeValue produces the strict private typed envelope', async () => {
  const result = await buildInlineResumeValue('{"decision":"approve"}', 'wait-one');
  assert.equal(result.type, 'object');
  assert.deepEqual(result.inline, { decision: 'approve' });
  assert.equal(result.producer.reference, 'wait-one');
  assert.equal(result.redaction, 'private');
  assert.match(result.digest, /^sha256:[a-f0-9]{64}$/);
});

test('wait resume policy distinguishes credentialless, callback, and system wake sources', () => {
  for (const source of ['gate', 'message', 'signal']) {
    assert.deepEqual(workflowWaitResumePolicy(source), {
      manual: true,
      tokenRequired: false,
      guidance: 'This authorized wait route does not use a one-time token.',
    });
  }
  assert.equal(workflowWaitResumePolicy('callback').manual, true);
  assert.equal(workflowWaitResumePolicy('callback').tokenRequired, true);
  assert.equal(workflowWaitResumePolicy('timer').manual, false);
  assert.equal(workflowWaitResumePolicy('child_run').manual, false);
  assert.equal(workflowWaitResumePolicy('future_source').manual, false);
});
