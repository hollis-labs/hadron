# Owner Input Register

W07-T10 now requires owner approval. Its code dependencies are integrated, but
the task's explicit decision gate prohibits implementation until the owner
approves compensation registration, ordering, failure policy, persistence,
replay, cancellation, child-run, and `finally` semantics.

The proposed ADR is
[`adr-durable-graph-visible-compensation.md`](/Users/chrispian/dev/agent-os/workspaces/drafts/hadron/session-20260824-3fb3749b/adr-durable-graph-visible-compensation.md).
It recommends graph-visible handlers backed by a durable per-run saga ledger,
reverse-dependency unwind, best-effort continuation with a separate rollback
outcome, explicit reversibility evidence, child-owned ledgers, compensation
before finalizers, and replay fencing for already-compensated effects.

No W07-T10 implementation or durable ADR promotion has started.
