// Package eventmap maps Hadron's durable workflow events (workflow_events) to
// event-stream envelopes (event stream contract C1/C2, CW-20261016-0141 S2).
//
// Only three families are exported; everything else stays in the local, resumable
// event feed:
//
//   - run.status_changed -> run.state_changed
//   - node.attempt_finished -> step.executed
//   - wait.suspended of a gate wait -> gate.evaluated with result blocked
//
// A gate's pass or fail is not exported yet: the persisted wait.resumed event
// carries the responder and a payload digest but no explicit decision, so there is
// nothing to map without guessing. The admission policy evaluation
// (gate hadron.admission) is written by the host store outside workflow_events and
// is a separate slice.
//
// Every envelope is built field by field from the event's identifiers and
// statuses, never from values or free text, and passes through the eventstage
// chain at insert: Stamp, Sanitize (a strict per-kind allowlist local to Hadron,
// behind eventstage's PayloadSanitizer seam so the shared redaction adapter can
// replace it), Classify. Subject is msg://workflow_run/hadron/<run>, the source
// channel is workflow_events, and the per-run workflow_events sequence travels in
// the payload as run_sequence: the envelope sequence is the outbox's own counter.
package eventmap
