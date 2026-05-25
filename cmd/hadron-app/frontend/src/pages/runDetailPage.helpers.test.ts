import assert from 'node:assert/strict'
import test from 'node:test'

import type { RunEvent } from '../api/types'
import { collapseWaitPollEvents, isWaitingForReply, waitPollSummaryEventType } from './runDetailPage.helpers'

function event(overrides: Partial<RunEvent> = {}): RunEvent {
  return {
    id: 1,
    run_id: 'run-1',
    event_type: 'step_start',
    message: '',
    created_at: '2026-05-25T04:00:00Z',
    step_name: 'wait-for-reply',
    ...overrides,
  }
}

test('collapseWaitPollEvents condenses consecutive wait polls', () => {
  const events = [
    event({ id: 1, event_type: 'message_wait_start' }),
    event({ id: 2, event_type: 'message_wait_poll', message: 'no matching message' }),
    event({ id: 3, event_type: 'message_wait_poll', message: 'no matching message' }),
    event({ id: 4, event_type: 'message_wait_poll', message: 'no matching message' }),
    event({ id: 5, event_type: 'message_wait_reply', message: '{"message_id":"msg-1"}' }),
  ]

  assert.deepEqual(collapseWaitPollEvents(events).map(item => [item.id, item.event_type, item.message]), [
    [1, 'message_wait_start', ''],
    [2, waitPollSummaryEventType(), 'Waiting for reply... (3 checks)'],
    [5, 'message_wait_reply', '{"message_id":"msg-1"}'],
  ])
})

test('isWaitingForReply returns true only for active waits', () => {
  assert.equal(isWaitingForReply([
    event({ id: 1, event_type: 'message_wait_start' }),
    event({ id: 2, event_type: 'message_wait_poll', message: 'no matching message' }),
  ]), true)

  assert.equal(isWaitingForReply([
    event({ id: 1, event_type: 'message_wait_start' }),
    event({ id: 2, event_type: 'message_wait_timeout', message: 'timed out' }),
  ]), false)

  assert.equal(isWaitingForReply([
    event({ id: 1, event_type: 'step_start' }),
  ]), false)
})
