# Workflow Engine Orchestrator Handoff

**Status:** execution protocol
**Task graph:** [README](README.md)
**Coverage authority:** [coverage matrix](coverage-matrix.md)

This document defines how an orchestrator assigns, integrates, verifies, and
closes the workflow-engine tasks. The task files define implementation scope;
this document defines execution discipline.

## Authority Order

Use this order when documents differ:

1. current user scope and the planning assumptions in [README](README.md);
2. accepted ADRs 0006 through 0012;
3. the area architecture under
   [`docs/architecture/workflow-engine-future-state`](../../architecture/workflow-engine-future-state/README.md);
4. the [coverage matrix](coverage-matrix.md) and task specifications;
5. the two exploration sources as requirement evidence where they have not been
   superseded.

An implementation agent may refine private types and file placement in sympathy
with the repository. It may not reverse an ADR, remove a covered capability, add
another semantic runtime, or expand repository scope without escalation.

## Repository Scope

- Default write root:
  `/Users/chrispian/dev/hollis-labs/apps/hadron`.
- W00-T07 write root only:
  `/Users/chrispian/dev/hollis-labs/libs/go-scheduler`.
- Nanite may be inspected as evidence for W00-T07 but is read-only.
- Torque is represented by a local fake MCP server in W06-T09 and is read-only.
- Cerberus and every other application are read-only context. They own their own
  later adoption work.
- Creating a new shared workflow repository, moving packages to another module,
  and coordinating downstream migrations are outside this execution plan.

## Dispatch Rules

1. Dispatch only task IDs listed in the README task index.
2. A task is eligible only after every listed dependency is integrated and its
   verification is green.
3. Give one implementation task to one subagent at a time. Split a task only by
   adding child task IDs and dependencies to the plan first.
4. Use isolated worktrees for concurrent write agents. If the environment shares
   one checkout, serialize writes; read-only review agents may still run in
   parallel.
5. Do not assign two concurrent agents ownership of the same package, schema,
   migration sequence, generated artifact, task index, or public API surface.
6. The orchestrator, not implementation agents, updates task checkboxes,
   dependency state, integration records, and wave gates.
7. One task should produce one reviewable commit unless a repository migration
   requires an explicitly documented ordered series.

## Task Brief

Every dispatched brief must include:

- task ID and link to its full specification;
- repository and isolated worktree path;
- integrated dependency commits or versions;
- concrete deliverables and expected package/file ownership;
- acceptance criteria and exact verification commands;
- relevant ADR and architecture links;
- prohibited repositories or public-contract changes;
- required final report format.

Subagents must read the entire task specification and linked primary
architecture notes before editing. Source exploration documents are supporting
context, not authority over a recorded supersession.

## Ownership Lanes

Use these as default write-serialization lanes. The dispatched brief narrows the
exact files after dependency integration:

| Lane | Primary ownership | Serialize when |
| --- | --- | --- |
| Foundation | `workflow` package layout, conformance, diagnostics, import guards | Public package names or conformance entry points overlap |
| Shared scheduler | `/Users/chrispian/dev/hollis-labs/libs/go-scheduler` | Always one writer; W00-T07 is the only task in this plan |
| Graph/compiler | `workflow/graph`, `workflow/compile`, generated schema | IR structs, schema, digests, source maps, or validation overlap |
| Values/state | `workflow/values`, state interfaces, artifacts, persistence migrations | Value envelopes, store methods, migrations, or generated DTOs overlap |
| Runtime/waits | `workflow/runtime`, `workflow/wait`, runtime conformance | State transitions, claims, readiness, waits, or replay overlap |
| Executors | step-kind registry and one adapter package per task | Registry/runtime contract changes overlap; independent adapters may use worktrees |
| Hadron host | `internal/appworkflow`, registry, schedules/triggers, diagnostics | Service DTOs, host construction, activation rows, or registry schema overlap |
| Public surfaces | CLI, HTTP, MCP, A2A, frontend | Shared appworkflow DTOs or generated clients overlap |
| Later capabilities | The package named by the W07 task | Entry dependencies or public schema changes overlap |

The README and planning files are orchestrator-owned integration artifacts; an
implementation agent proposes tracker changes in its report rather than editing
task status concurrently.

## Subagent Report

The final report for each task must contain:

- resulting commit SHA or an explicit statement that no commit was made;
- files and public contracts changed;
- acceptance criteria satisfied;
- verification commands and outcomes;
- generated artifacts or migrations added;
- decisions made within delegated authority;
- remaining risks, skipped checks, or blockers;
- any proposed change to the coverage matrix or dependency graph.

The orchestrator independently reviews the diff and reruns risk-proportionate
verification before integrating and checking off the task.

## Integration Rules

- Integrate dependencies before dependents; do not rely on unmerged worktree
  state.
- Regenerate and verify schemas or clients in the same task that changes their
  source-of-truth types.
- Database migrations are append-only and receive integration-order review.
- Public API changes receive import-guard, schema, conformance, and external
  package tests before dependents start.
- A task that discovers an architecture conflict stops at a report or ADR draft;
  it does not choose a new public direction implicitly.
- Pre-existing unrelated failures must be recorded with evidence. They do not
  waive task-specific verification.

## Wave Gates

- **Wave 00:** package boundaries, import guards, diagnostics, conformance
  skeleton, registry skeleton, and `go-scheduler` timed activation contracts are
  verified.
- **Wave 01:** source, graph IR, schema, source maps, validation, fixtures, and
  activation templates compile deterministically.
- **Wave 02:** typed values, expressions, inferred edges, binding, persistence
  contracts, redaction, and secret handling pass conformance.
- **Wave 03:** durable scheduling, states, waits, recovery/replay, concurrency,
  control flow, memoization, and pinned values pass race and restart tests.
- **Wave 04:** initial executors and verification use only registry/runtime
  contracts and return typed outputs.
- **Wave 05:** Hadron host, registry, contract tests, source activations,
  provenance, scheduler binding, and diagnostics operate through one service.
- **Wave 06:** all public doors use that service, legacy execution is
  quarantined, and W06-T09 passes end to end. This is the initial engine release
  gate.
- **Wave 07:** deferred capabilities are independently promotable after their
  entry criteria. W07-T10 has an explicit owner-approved ADR gate.

## Completion Rule

The plan is complete when every required Wave 00-06 task is checked, W06-T09
passes, all wave-gate verification is green, generated artifacts are current,
and every coverage-matrix row is `implemented`, `superseded`, or assigned to an
explicit Wave 07 task. Downstream application adoption is not part of this
completion condition.
