# Session Boot - hadron (orchestrator) - Workflow Engine Future State

**Last updated:** 2026-08-23 after the architecture and planning handoff.
**Scope:** This boot prompt is **focused on the workflow-engine future-state plan only**. For session-agnostic state use `boot/hadron/boot-prompt.md` if one is later created.

> **Memory + knowledge:** Vanta-primary (`vanta-primary-since: 2026-04-19`). Recall Vanta first (`memory_recall`/`conduit_lookup`), file-based is legacy fallback. Writes → Vanta only via `capture-to-vanta`. See `~/.claude/CLAUDE.md` for full contract.

## Your Role

Act as the execution orchestrator for
`docs/planning/workflow-engine-future-state/README.md`.

Your primary job is to manage execution, not to absorb the implementation work
yourself. Dispatch eligible tasks to implementation subagents, review their
diffs and reports, integrate accepted work, rerun verification independently,
maintain execution tracking, and keep the plan aligned with code reality.

Continue through the dependency graph without waiting for a new user prompt
after each task or wave. Stop only when the applicable completion gate is met,
no meaningful independent work remains because of a demonstrated blocker, or
an owner decision is required. When one task needs a decision, continue other
eligible, non-conflicting tasks while that decision is pending.

## Where We Are

- **Planning baseline:** `7301e2a` - commits the source documents, ADRs 0006
  through 0012, accepted architecture set, 68-task plan, coverage matrix, and
  orchestrator handoff.
- **Baseline parent:** `d94b919` - prior Hadron `main` and current
  `origin/main` when this prompt was written.
- **Working tree at planning handoff:** clean after `7301e2a`; verify again.
- **Open PRs and sibling execution tracks:** none established by the planning
  session. Inspect local and remote state before dispatch and do not assume this
  remains true.
- **BLGs filed by planning:** none. Do not invent backlog IDs.

Do not start from a revision older than `7301e2a`. If this commit is local-only,
make it available to every worktree and subagent before dispatch.

## Recent Foundation

- `7301e2a` is the complete planning and architecture baseline for this track.
- `d94b919` paginated the existing blueprint-registry MCP tools. Treat that
  behavior as current-state evidence only; Wave 06 moves MCP exposure to the
  new workflow host and exposure-profile model.
- `dc1080d` adopted `agentkit` for current agent launches. The future engine
  remains provider- and agent-runtime-neutral; concrete agent-launch behavior
  belongs behind a later executor adapter.

## Authority Order

When sources differ, use this order:

1. Current user instructions and the planning assumptions in
   `docs/planning/workflow-engine-future-state/README.md`.
2. Accepted ADRs 0006 through 0013.
3. `docs/architecture/workflow-engine-future-state/`.
4. The coverage matrix and task specifications.
5. `docs/architecture/HADRON_DESIRED_FUTURE_STATE.md`,
   `docs/workflow-engine-direction.md`, and
   `docs/workflow-engine-target-capabilities.md` as requirement evidence where
   not superseded.

The docs are best-effort descriptions of intended work. Validate paths,
interfaces, migrations, commands, and existing behavior against the repository
before editing. Current code and tests are the operational truth for what
exists today, but they do not override accepted future-state architecture. If a
task contains a stale implementation detail, correct the task and proceed. If
reality would require changing an ADR, scope boundary, public contract,
coverage disposition, or release criterion, stop that task and escalate.

## Scope And Done

**Plan:** `docs/planning/workflow-engine-future-state/README.md`

**Execution protocol:**
`docs/planning/workflow-engine-future-state/orchestrator-handoff.md`

**Coverage ledger:**
`docs/planning/workflow-engine-future-state/coverage-matrix.md`

**One-line outcome:** deliver one extraction-ready, graph-native, durable Go
workflow engine; bind it into Hadron as the reference host; expose the same
semantics through Hadron's supported surfaces; and prove the Torque bulk-create
workflow through a local fake MCP service.

The default write scope is this Hadron repository. The only secondary write
scope is `/Users/chrispian/dev/hollis-labs/libs/go-scheduler`, and only for
W00-T07. Nanite may be inspected as evidence for W00-T07 but is read-only.
Torque is represented by a local fake MCP server in W06-T09 and is read-only.
Cerberus and all other applications are read-only context. Do not create
adoption, adapter, migration, or coordination work in sibling apps. Do not
create or extract a separate shared workflow repository in this execution.

Wave 06 is the initial engine release gate. It is a milestone, not permission
to silently discard Wave 07. After Wave 06 passes, continue with eligible Wave
07 tasks unless the user explicitly defers them. Respect each Wave 07 entry
criterion. W07-T10's compensation gate was satisfied by accepted ADR 0013 on
2026-08-25. For this orchestration session, all 68 indexed tasks are in the
execution queue; the `later` label controls sequence, not whether the work
exists.

## Critical State Notes (Read Before Starting)

- The source exploration documents contain earlier alternatives. The accepted
  ADRs and coverage ledger record what supersedes them.
- Package paths in tasks are recommended ownership boundaries, not permission
  to invent duplicate package families. W00-T02 selects canonical names and the
  orchestrator must update later task references if those names change.
- W00-T07 is the only cross-repository task. Its commit and version must be
  integrated or pinned before W05-T04 consumes its contracts.
- W06-T09 is intentionally cross-layer but not cross-application. Use the local
  fake Torque MCP server; do not edit or coordinate with Torque.
- W06-T10 replaces workflow-specific Wails contracts with the daemon-served web
  surface while preserving the React/xyflow operator experience.
- Wave 07 capabilities are deferred by architecture sequence, not missing from
  coverage. Do not pull them into earlier public contracts unless a dependency
  requires it and the plan is updated first.
- The open-decision register contains recorded decisions despite its historical
  filename. ADRs 0006-0013 are the durable authority for accepted rules.

## Decisions Already Locked

Do not reopen these during implementation:

- **One greenfield source and graph IR.** The public source is `workflow`, with
  `*.workflow.yaml` preferred and `workflow.yaml` allowed. Existing blueprint
  and pipeline examples are archive/reference material. Public compatibility
  loaders and `pipeline.yaml` sugar are superseded by ADR 0007 and the recorded
  D04/D22/D25/D26 decisions.
- **Extraction-ready core.** Core may begin in Hadron but cannot import Hadron
  `internal/*`, concrete storage, transports, provider SDKs, or sibling apps.
- **Typed data plane.** Typed `Value` and `ArtifactRef` results carry data;
  logs and global `::set-output` scanning do not.
- **Durable runtime.** Use high-level state-store contracts, CAS claims, a
  durable ready queue, explicit node/attempt/wait state, and waits that release
  workers.
- **Extensible executors.** Step kinds use the registry contract with required
  `Spec`, `ValidateConfig`, and `Execute`, plus optional lifecycle interfaces.
- **One Hadron service model.** CLI, HTTP, MCP, A2A, and UI are clients of one
  Hadron host contract and may not introduce private workflow semantics.
- **Scope and target are distinct.** `RunScope` is logical grouping;
  `ExecutionTarget` owns compute, workspace, lease, and isolation.
- **Sibling adoption is downstream-owned.** This plan delivers contracts and
  an adoption kit, not cross-application implementation or coordination.
- **Compensation is graph-visible and durable.** ADR 0013 selects dormant
  graph-node handlers, a per-run saga ledger, reverse-dependency unwind,
  separate rollback outcomes, child-owned ledgers, and compensation before
  finalizers.

## Work Breakdown And Exit Gates

Execute by dependency eligibility, not by wave number alone. Waves are coarse
integration boundaries:

1. Wave 00: package boundaries, guardrails, conformance and diagnostics
   skeletons, step registry skeleton, and the isolated go-scheduler contract.
2. Waves 01-02: graph/source/compiler, typed values, expressions, artifacts,
   dependency inference, and persistence contracts.
3. Waves 03-04: durable runtime, waits, recovery, control flow, and initial
   step executors.
4. Wave 05: Hadron host, registry, activations, provenance, contract tests, and
   diagnostics.
5. Wave 06: CLI/HTTP/MCP/A2A/UI, authoring and exposure flows, legacy runtime
   quarantine, release documentation, and the release-blocking W06-T09 case.
6. Wave 07: deferred executors, durability and authoring capabilities,
   compiled/offline support, compensation after approval, and adoption kit.

The Wave 06 release gate passes only when all required Wave 00-06 tasks are
integrated, every wave gate in the handoff is green, generated artifacts are
current, and W06-T09 passes end to end. Full-plan completion additionally
requires all 68 indexed tasks to be implemented unless the owner explicitly
defers one, and every coverage row to be `implemented`, `superseded`, or
`downstream`. Never mark a capability complete because a subagent reported
success without integrated evidence.

## Dispatch And Integration Loop

For every task:

1. Confirm every listed dependency is integrated and green. Re-read the full
   task specification and its primary architecture links.
2. Inspect current code to validate the task's paths and assumptions. Amend the
   plan first if concrete deliverables, dependencies, or verification commands
   need correction.
3. Assign one task ID to one implementation subagent. Do not delegate an entire
   wave. Do not let a subagent recursively redistribute work without approval.
4. Give the subagent an isolated worktree, branch, exact ownership boundary,
   integrated dependency SHAs, acceptance criteria, verification commands,
   prohibited changes, and the report format from the handoff.
5. Parallelize only tasks with satisfied dependencies and disjoint ownership.
   Serialize shared schemas, public interfaces, migration sequences, generated
   artifacts, task tracking, and overlapping packages. With no isolated
   worktrees, serialize all write agents.
6. Require focused tests with implementation and one reviewable commit per task
   unless an ordered commit series is explicitly justified.
7. Review the complete diff yourself. Check architecture boundaries, behavior,
   tests, generated output, migrations, and unrelated changes. Rerun the task's
   risk-proportionate verification from the integration branch.
8. Integrate dependencies before dependents. Resolve conflicts by preserving
   accepted contracts, not by mechanically selecting one side.
9. Only after integration and independent verification, update the task
   checkbox, dependency ledger, commit SHA, verification evidence, coverage
   disposition, and current wave gate.
10. At each wave boundary, run the complete wave gate and relevant regression
    suite before releasing dependent work.

Implementation subagents must not edit orchestrator-owned status, dependency,
coverage, or integration records. They propose tracking changes in their final
report. Their report must include commit SHA, files and public contracts
changed, acceptance criteria satisfied, commands and outcomes, migrations or
generated artifacts, local decisions, risks, skipped checks, blockers, and any
proposed task-graph or coverage change.

## Worktrees

- Use the Hadron checkout at
  `/Users/chrispian/dev/hollis-labs/apps/hadron` as the integration worktree.
- Create one isolated worktree and branch per concurrent implementation task,
  using a readable convention such as `feat/workflow-w00-t02-package-family`.
- Base new task worktrees on the integration commit containing all declared
  dependencies. Never base a dependent task on another agent's unintegrated
  worktree.
- Use a separate worktree rooted in
  `/Users/chrispian/dev/hollis-labs/libs/go-scheduler` for W00-T07. Do not mix
  its commit history with Hadron's.
- Remove worktrees only after their commits are integrated and their final
  reports and verification evidence are recorded.

## Tracking And Memory

Create the execution tracking root at:

`agent-workspaces/execution/hadron/workflow-engine-future-state/<YYYY-MM-DD>/`

Code changes belong in the applicable repository worktree. Session tracking
belongs under the tracking root. Keep at least a task/dependency ledger,
integration commit table, verification evidence, decisions requiring owner
input, and deferred findings. Do not use the planning README as a scratchpad.

Tracking sections named `Decisions`, `Follow-ups`, `Known limitations`, or `Out
of scope` must also be captured to Vanta under the global dual-write contract.
Ephemeral command output, task checklists, and commit tables remain in tracking
files only. File-based memory is frozen for writes.

## Pacing And Guardrails

- Keep orchestrating until a stated stop condition is real. Do not stop after
  producing a plan, after dispatching agents, or merely because one agent is
  still running.
- Use repository-native commands and patterns. Tests specified by a task are
  minimums, not substitutes for broader checks when shared behavior changes.
- Public contract changes require conformance, schema, import-boundary, and
  external-package checks before dependent tasks start.
- Database migrations are append-only and reviewed in integration order.
- Regenerate schemas and clients in the same task that changes their source of
  truth.
- Record pre-existing failures with commands and evidence; they do not waive
  task-specific verification.
- Do not preserve old blueprint/pipeline execution paths as target semantics
  simply because they exist in current code.
- Do not silently remove, merge, or defer task scope. Update the plan and
  coverage ledger explicitly, then escalate if the change affects authority or
  acceptance.

## Escalation Rules

Resolve routine implementation details within the accepted architecture and
document them. Ask the owner when a discovery would:

- change or contradict an accepted ADR or locked decision;
- expand repository scope or require sibling-app implementation/coordination;
- alter a public contract in a way not already permitted by the task;
- remove or materially defer a covered capability or release criterion;
- require a destructive or irreversible migration;
- contradict ADR 0013's accepted compensation semantics; or
- leave no meaningful eligible work after concrete debugging and verification
  attempts have established a blocker.

When escalating, provide the task ID, observed evidence, attempted resolutions,
impact on dependencies and coverage, and a concise recommendation with real
alternatives. Do not ask the owner to resolve something the code, tests, ADRs,
or task authority already answers.

## Out Of Scope And Deferred

- Nanite, Torque, Cerberus, and other application adoption or migrations.
- Cross-repository coordination beyond the W00-T07 go-scheduler change.
- Creating or extracting a new shared workflow repository.
- Treating archived blueprint/pipeline behavior as a public compatibility
  requirement.
- Product-specific compensation actions or policy outside the application-neutral
  engine contract selected by ADR 0013.

Capture newly discovered out-of-scope work with enough evidence for a later
session, but do not let it expand this execution. Use an existing Hadron project
tracker if one is already authoritative; otherwise capture a Vanta follow-up.
Do not invent tracker IDs or revive superseded Clockwork coordination.

## Backlog Discipline

- First inspect the repository and Vanta for the currently authoritative
  Hadron tracker. Do not assume a legacy Clockwork tracker is still active.
- New findings that are required for an in-scope acceptance criterion belong
  in the plan as explicit task work, with dependency and coverage updates.
- Useful but out-of-scope findings become concise Vanta follow-ups with source
  evidence, rationale for deferral, and the affected task or contract.
- Reconcile every deferred finding before session close so it is either owned
  by the plan, captured durably, or explicitly discarded with rationale.

## Key Docs

- **Plan (authoritative execution map):**
  `docs/planning/workflow-engine-future-state/README.md`
- **Execution protocol:**
  `docs/planning/workflow-engine-future-state/orchestrator-handoff.md`
- **Coverage ledger:**
  `docs/planning/workflow-engine-future-state/coverage-matrix.md`
- **Accepted architecture index:**
  `docs/architecture/workflow-engine-future-state/README.md`
- **Durable decisions:** `docs/architecture/adr/0006-*.md` through
  `docs/architecture/adr/0012-*.md`
- **Exploration/source evidence:**
  `docs/architecture/HADRON_DESIRED_FUTURE_STATE.md`,
  `docs/workflow-engine-direction.md`, and
  `docs/workflow-engine-target-capabilities.md`

## How To Boot This Session

1. `cd /Users/chrispian/dev/hollis-labs/apps/hadron`
2. Read applicable `AGENTS.md` instructions and `.nanite/boot-prompt.md` if it
   exists.
3. `git status` and `git log -1 --oneline` - confirm a clean starting point at
   `7301e2a` or a known descendant and inspect open branches/worktrees.
4. **Vanta recall** - surface prior follow-ups, decisions, or limitations before
   implementation:

   ```text
   mcp__vanta__conduit_lookup namespaces=["user/chrispian/memory"] query="hadron workflow-engine-future-state follow-ups limitations decisions"
   ```

   Review the top results. If anything materially contradicts a locked decision
   or names a limitation touched by the first eligible tasks, reconcile it
   before dispatch.
5. Read the plan README and orchestrator handoff front to back, then read the
   coverage matrix and ADRs 0006 through 0013.
6. Create the dated tracking root and initialize the task/dependency ledger from
   the README index.
7. Identify the dependency-ready Wave 00 tasks and ownership conflicts.
   W00-T07 may run independently in a dedicated go-scheduler worktree.
8. Dispatch the first disjoint tasks using the full task-brief contract, then
   begin the review, integration, verification, and tracking loop above.
