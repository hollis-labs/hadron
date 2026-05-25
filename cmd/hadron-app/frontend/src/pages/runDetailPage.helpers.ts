import type { RunEvent } from '../api/types'

const WAIT_POLL_SUMMARY_EVENT = 'message_wait_poll_summary'

export function collapseWaitPollEvents(events: RunEvent[]): RunEvent[] {
  const collapsed: RunEvent[] = []
  const sorted = [...events].sort((a, b) => a.id - b.id)

  for (let i = 0; i < sorted.length; i += 1) {
    const event = sorted[i]
    if (event.event_type !== 'message_wait_poll') {
      collapsed.push(event)
      continue
    }

    let count = 1
    let last = event
    while (i+1 < sorted.length) {
      const next = sorted[i + 1]
      if (next.event_type !== 'message_wait_poll' || next.step_name !== event.step_name) {
        break
      }
      count += 1
      last = next
      i += 1
    }

    collapsed.push({
      ...last,
      id: event.id,
      event_type: WAIT_POLL_SUMMARY_EVENT,
      message: count === 1 ? 'Waiting for reply...' : `Waiting for reply... (${count} checks)`,
    })
  }

  return collapsed
}

export function isWaitingForReply(events: RunEvent[]): boolean {
  if (events.length === 0) {
    return false
  }
  let sawWaitStart = false
  for (const event of events) {
    if (event.event_type === 'message_wait_start') {
      sawWaitStart = true
    }
    if (event.event_type === 'message_wait_reply' || event.event_type === 'message_wait_timeout' || event.event_type === 'step_error') {
      return false
    }
  }
  if (!sawWaitStart) {
    return false
  }
  const last = events[events.length - 1]
  return last.event_type === 'message_wait_poll' || last.event_type === WAIT_POLL_SUMMARY_EVENT || last.event_type === 'message_wait_start'
}

export function waitPollSummaryEventType(): string {
  return WAIT_POLL_SUMMARY_EVENT
}
