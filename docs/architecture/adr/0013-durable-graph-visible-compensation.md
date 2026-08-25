# ADR 0013: Durable Graph-Visible Compensation With A Per-Run Saga Ledger

**Status:** Accepted<br>
**Date:** 2026-08-25

## Context

Hadron's graph, typed values, effect and idempotency metadata, child calls,
CAS-backed runtime records, replay, cancellation intents, and `finally` nodes
are now stable. They do not, however, say which completed effects remain
eligible to be undone, how rollback survives a crash, or how a partial rollback
is reported. Treating compensation as ordinary `finally` work is insufficient:
finalizers run for cleanup regardless of whether an effect was applied, and
they do not retain a durable eligibility or attempt ledger. The contract must
remain application-neutral; connector-specific undo behavior stays in step
adapters or exact-digest child workflows.

## Options Considered

1. **Graph-visible handlers plus a durable per-run saga ledger.** An effectful
   node names a dormant ordinary graph node as its compensation handler. A
   handler may itself be a `call` node bound to an exact child definition. The
   runtime freezes eligibility and ordering in durable records, then schedules
   handlers through the normal executor and attempt machinery. This adds state,
   but keeps rollback inspectable, replayable, and provider-neutral.
2. **An optional `Compensate` method on each executor, unwound in completion
   order.** This is compact, but hides rollback outside the graph, couples the
   engine to provider behavior, makes parallel completion order affect
   semantics, and gives child workflows and operator tooling no stable plan to
   inspect.
3. **Express rollback only with existing `catch` and `finally` nodes.** This
   avoids a new runtime phase, but cannot durably distinguish eligible,
   compensated, failed, and indeterminate effects; it also cannot safely resume
   a partial rollback or prevent a handler from running when its effect never
   happened.
4. **Leave compensation to Cerberus or another product-specific saga
   subsystem.** This preserves a smaller core but violates the extraction-ready
   boundary and forces every host to invent incompatible persistence,
   cancellation, and replay semantics.

## Decision

Select option 1. A node opts in by referencing one graph-visible compensation
handler; call-based rollback is represented by referencing an ordinary dormant
`call` handler, not by adding a second execution mechanism. Handlers never
participate in the forward phase and cannot themselves register compensation.
Structural validation resolves handler references and prevents cycles. Binding
also requires truthful, operation-specific reversibility evidence from the
registered adapter or host policy, plus explicit effects and an intrinsic or
keyed idempotency declaration for the handler. Generic effect metadata alone
never advertises that an operation can be undone.

Compensation is triggered only by an explicit workflow policy (unsuccessful
terminal intent, cancellation, timeout, and/or an authorized manual rollback
of a successful run). After forward work is fenced and running attempts have
converged, the store atomically freezes a plan-digest-bound saga ledger. An
entry becomes eligible only when an attempt durably reports that its effect was
applied and records typed original input, output, error, and compensation
receipt references. The ledger records the original invocation and attempt,
handler, eligibility reason, dependencies, attempts, outputs, failures, and
CAS generation. Compensation uses the normal claims, retry, timeout, policy,
executor, and event contracts under a distinct compensation phase.

Ordering is reverse dependency order, never incidental wall-clock completion
order: if B depended on A, B's handler must reach a terminal outcome before A's
handler can run. Independent branches and fan-out items may compensate in
parallel subject to ordinary resource limits. The default failure policy is
best-effort: exhaust the declared retries, retain each failure, and continue
with other eligible branches. The original run status and failure history are
immutable. A separate compensation summary reaches `succeeded`, `partial`,
`failed`, or `canceled`, so operators can distinguish the forward outcome from
the rollback outcome.

Each child run owns its own ledger. When unwinding a `call`, the parent first
requests and waits for the child ledger to finish; an explicitly declared
parent handler then covers only parent-side effects. Compensation precedes
ordinary `finally` cleanup so teardown cannot remove resources needed for
rollback, and finalizers still run after partial or failed compensation. A
normal run-cancellation request may trigger compensation but does not cancel
it. Stopping rollback requires a separate authorized cancellation request,
which fences pending handlers, uses ordinary cancellation for active attempts,
and preserves already completed compensation.

Forward replay creates a new run and never reopens or copies a prior saga
ledger. Replay may not reuse outputs from an invocation whose effect was
successfully compensated, and replay across a partial, failed, canceled, or
indeterminate compensation entry requires an explicit policy attestation of
external state. Retrying rollback resumes the existing ledger with stable
idempotency keys; it does not erase or replace prior attempts.

## Consequences

The runtime gains a durable compensation phase, records, store/conformance
contracts, binding checks, expression context for the original invocation, and
operator inspection fields. Terminalization must sequence forward convergence,
compensation, and then `finally`, which is more complex than executor callbacks.
Adapters must truthfully attest reversibility and produce a durable receipt;
some mutating operations will therefore remain intentionally non-compensable.
Best-effort unwind can leave a partial external state, but that state is explicit
and resumable rather than hidden behind a generic failed run. Reverse dependency
ordering is deterministic and parallelizable, though workflows needing stricter
business order must encode it in the forward graph or handler dependencies.
The model adds no Cerberus, Torque, Nanite, provider, or transport semantics to
the engine core.
