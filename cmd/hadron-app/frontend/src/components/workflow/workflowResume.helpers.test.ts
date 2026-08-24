import assert from 'node:assert/strict';
import test from 'node:test';

import { buildInlineResumeValue, canonicalJSONStringify } from './workflowResume.helpers';

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
