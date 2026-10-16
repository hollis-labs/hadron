// Package eventmap maps Hadron's durable workflow events (workflow_events) to
// event-stream envelopes (event stream contract C1/C2, CW-20261016-0141 S2).
//
// Only two families are exported; everything else stays in the local, resumable
// event feed:
//
//   - run.status_changed -> run.state_changed (entity workflow_run)
//   - run.cancellation_requested that moves the run to canceled -> run.state_changed
//     (the engine records a cancel as this event, not as a status change; the cancel
//     reason code stays local)
//   - wait.suspended of a gate wait -> gate.evaluated with result blocked (gate kind
//     approval for a human-decision wait, custom for any other gate wait)
//
// The admission policy decision is the third family. The host store persists it
// outside workflow_events, in its own transaction, so the same outbox write happens
// there (persistence.WorkflowHostStore.RecordPolicyEvaluation): gate.evaluated with
// gate hadron.admission (kind custom), allow -> pass, deny -> fail, confirm ->
// blocked. EVERY admission event, allow included, has the subject
// msg://workflow_admission/hadron/<decision id>: a subject must name something that
// exists, and a run may not (a dry run, a confirm). The run an allow goes on to create
// cites the admission event as its cause: its first run.state_changed carries
// corr.causation_id = the admission event's id (architect ruling
// hadron_admission_event_subject_2026_10_16).
//
// A confirm decision exports `blocked`. When a start then satisfies it (a caller
// confirmed, or a dry run, which proceeds without confirmation), the transaction that
// records the start exports a SECOND gate.evaluated on the same admission subject:
// result pass, blocking false, corr.causation_id = the blocked event, actor = the
// confirmer (verified "token" for the operator credential over HTTP, else "claimed";
// no actor for a dry run), at key (decision id, run_sequence 1). The run's first
// run.state_changed cites that pass, not the blocked event; a dry run's run cites
// nothing (CW-0250, architect ruling rev 2). Hadron records no rejected or expired
// confirmation, so there is no fail counterpart yet.
//
// The outbox key of an admission is (decision id, run_sequence 0): in
// workflow_event_outbox and in workflow_event_outbox_skipped the run_id column holds
// the DECISION id for these rows, and no workflow event can take sequence 0 (they
// start at 1). A repair pass must re-derive a sequence-0 skipped row from the policy
// decision (workflow_host_policy_decisions), not from workflow_events. The
// decision's reason and attributes are free text and never leave.
//
// node.* events are NOT exported: they stay in Hadron's own subscription, and
// step.executed@1 is for deterministic steps and reaction rules, not workflow nodes.
// Any global node kind would come through the contract lane.
//
// A human gate's pass or fail is not exported yet: it waits for an explicit option
// outcome in the workflow definition (CW-0236), and nothing is inferred from option
// ids or labels.
//
// Every envelope is built field by field from the event's identifiers and
// statuses, never from values or free text, and passes through the eventstage
// chain at insert: Stamp, Sanitize (a strict per-kind allowlist local to Hadron,
// behind eventstage's PayloadSanitizer seam so the shared redaction adapter can
// replace it), Classify. Subject is msg://workflow_run/hadron/<run>, the source
// channel is workflow_events, and the per-run workflow_events sequence travels in
// the payload as run_sequence: the envelope sequence is the outbox's own counter.
//
// The envelope generation is derived, not stored: the first 52 bits of the event
// log's store_generation (migration 0032). It is nonzero and below 2^53, so every JSON reader keeps it exact, is stable for a database and changes
// when a new database is created or a different one is restored over it, which is
// exactly when per-producer sequences stop being comparable.
//
// Recording never rejects a valid mutation (platform rule CW-0149). The outbox
// insert runs inside a SAVEPOINT in the transaction that appends the workflow
// event. If the mapper fails (error, panic, or a *SkipError from the chain) or the
// insert fails, the savepoint is rolled back, the workflow event still commits, the
// outbox counter is not advanced, and a row in workflow_event_outbox_skipped
// records the run, the run sequence and a reason class (no payload), with a log
// line and the EventOutboxSkipped counter. Only a database error in the outer
// transaction fails the write. The skipped rows are the repair worklist.
//
// chain.go is the whole eventstage wiring and is meant to be read and copied by the
// other producers.
package eventmap
