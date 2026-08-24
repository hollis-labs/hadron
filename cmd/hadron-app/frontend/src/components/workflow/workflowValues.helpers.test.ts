import assert from 'node:assert/strict';
import test from 'node:test';

import type { WorkflowRenderedValue } from '../../api/types';
import { REDACTED_VALUE, workflowValuePreview } from './workflowValues.helpers';

function value(overrides: Partial<WorkflowRenderedValue> = {}): WorkflowRenderedValue {
  return {
    type: 'string', payload: 'visible', producer: { kind: 'test', reference: 'value' }, media_type: 'application/json',
    digest: `sha256:${'a'.repeat(64)}`, redaction: 'public', retention: 'run', masked: false, ...overrides,
  };
}

test('workflowValuePreview always preserves secret and masked markers', () => {
  assert.deepEqual(workflowValuePreview(value({ payload: 'must-not-render', redaction: 'secret' })), {
    text: REDACTED_VALUE, masked: true, truncated: false,
  });
  assert.equal(workflowValuePreview(value({ payload: 'must-not-render', masked: true })).text, REDACTED_VALUE);
});

test('workflowValuePreview exposes artifact metadata without its URI', () => {
  const preview = workflowValuePreview(value({
    type: 'artifact',
    payload: { store: 'local', uri: 'artifact://private/path', digest: 'sha256:artifact', media_type: 'text/plain', size_bytes: 42 },
  }));
  assert.deepEqual(preview.artifact, { store: 'local', digest: 'sha256:artifact', mediaType: 'text/plain', sizeBytes: 42 });
  assert.equal(JSON.stringify(preview).includes('artifact://private/path'), false);
});

test('workflowValuePreview reports bounded inline truncation', () => {
  const preview = workflowValuePreview(value({ payload: 'x'.repeat(50) }), 12);
  assert.equal(preview.truncated, true);
  assert.equal(preview.text, `${'x'.repeat(12)}…`);
});
