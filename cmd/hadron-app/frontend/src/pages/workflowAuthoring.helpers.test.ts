import assert from 'node:assert/strict';
import test from 'node:test';

import { createWorkflowRunID, parseWorkflowInputs } from './workflowAuthoring.helpers';

test('typed workflow inputs require a JSON object', () => {
  assert.deepEqual(parseWorkflowInputs('{"count":2,"enabled":true}'), { count: 2, enabled: true });
  assert.throws(() => parseWorkflowInputs('["not","an","object"]'), /JSON object/);
  assert.throws(() => parseWorkflowInputs('null'), /JSON object/);
});

test('workflow run identifiers retain an operation prefix', () => {
  assert.equal(createWorkflowRunID('ui', '0000'), 'ui-0000');
});
