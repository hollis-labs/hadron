# Workflow Engine Future-State Task Plan

**Status:** orchestrator execution handoff
**Architecture source:** [workflow-engine-future-state](../../architecture/workflow-engine-future-state/README.md)
**Coverage ledger:** [source-to-task coverage](coverage-matrix.md)
**Execution protocol:** [orchestrator handoff](orchestrator-handoff.md)
**Tracking scope:** concrete implementation tasks grouped by wave and area

This folder turns the desired workflow-engine architecture into task groups.
The waves below are coarse implementation boundaries, not a sprint schedule.
Each task has exact dependencies, concrete work, acceptance criteria, and
verification guidance so an orchestrator can assign it to an implementation
subagent once its dependencies are complete.

When the two exploration sources conflict with accepted architecture or ADRs,
the accepted decision supersedes the earlier source statement. The principal
example is source-format compatibility: the exploration drafts proposed
blueprint/pipeline compatibility loaders, while ADR 0007 and the accepted
greenfield/archive decision select one new `workflow` source format. The
[coverage ledger](coverage-matrix.md) records these dispositions explicitly so
superseded requirements are not mistaken for omissions.

## Planning Assumptions

- The public workflow source format is greenfield and named `workflow`.
- Current blueprint and pipeline examples are archive/reference material.
- No public legacy parser is required for the initial implementation.
- The reusable engine may start in this repo, but its core packages must remain
  extraction-ready and must not import `github.com/hollis-labs/hadron/internal/...`.
- Implementation scope is this Hadron repository plus
  `/Users/chrispian/dev/hollis-labs/libs/go-scheduler` for W00-T07 only.
- Nanite, Torque, Cerberus, and other applications receive no code, migration,
  adapter, or coordination tasks in this plan. They own adoption after the
  Hadron engine contract is complete.
- The first implementation path includes `cmd`, `transform`, `call`, `sleep`,
  `wait_for`, `human_gate`, `message_wait`, `mcp`, and `http`.
- `llm`, `script`, `agent_launch`, generated API clients, and compiled/offline
  support are later waves unless product priority changes.

## Wave Map

| Wave | Folder | Purpose | Exit Condition |
| --- | --- | --- | --- |
| 00 | [foundation](wave-00-foundation/tasks.md) | Establish repo boundaries, archive examples, install conformance scaffolding, and define the step-kind contract skeleton. | New workflow packages and guardrails exist; old examples are not treated as source contracts. |
| 01 | [graph-source](wave-01-graph-source/tasks.md) | Define graph IR, source model, compiler, source maps, and validation. | `*.workflow.yaml` and `workflow.yaml` compile into a persisted `ExecutionPlan` shape with diagnostics. |
| 02 | [values-state](wave-02-values-state/tasks.md) | Build typed values, artifacts, expressions, plan binding, and state-store contracts. | Runtime can bind inputs, evaluate expressions, persist values, and enforce redaction/retention metadata. |
| 03 | [scheduler-waits](wave-03-scheduler-waits/tasks.md) | Implement durable ready-queue runtime, node state, retries, waits, resumes, and recovery. | Nodes run from persisted state, waits release workers, and recovery can resume incomplete runs. |
| 04 | [step-kinds](wave-04-step-kinds/tasks.md) | Implement the first built-in step kinds through a registry contract. | Initial step kinds validate configs, execute through core contracts, and return typed outputs. |
| 05 | [hadron-host](wave-05-hadron-host/tasks.md) | Bind the core into Hadron app/service, persistence, registry, schedules, triggers, and diagnostics. | Hadron starts and operates graph-native workflows through its daemon-backed surfaces. |
| 06 | [surfaces-cleanup](wave-06-surfaces-cleanup/tasks.md) | Expose CLI, HTTP, MCP, A2A, UI flows, then quarantine/remove legacy execution paths. | Users and agents interact with one workflow semantic model across surfaces. |
| 07 | [later-capabilities](wave-07-later-capabilities/tasks.md) | Implement deferred step kinds, advanced durability/control-flow, offline builds, and public adoption artifacts after the first durable graph runtime exists. | Every deferred source capability has an implementation task or an explicit superseded/out-of-scope disposition. |

## Task Index

| ID | Task | Wave | Depends On | Track |
| --- | --- | --- | --- | --- |
| W00-T01 | Archive legacy blueprint and pipeline examples | 00 | none | [x] |
| W00-T02 | Scaffold extraction-ready workflow package family | 00 | none | [x] |
| W00-T03 | Add dependency and import guardrails | 00 | W00-T02 | [x] |
| W00-T04 | Create conformance harness skeleton | 00 | W00-T02 | [x] |
| W00-T05 | Establish diagnostics and error-code conventions | 00 | W00-T02, W01-T01 | [x] |
| W00-T06 | Define step-kind contract skeleton | 00 | W00-T02, W00-T05, W01-T01 | [x] |
| W00-T07 | Standardize timed activation contracts in `go-scheduler` | 00 | none | [x] |
| W01-T01 | Define graph IR Go types | 01 | W00-T02 | [x] |
| W01-T02 | Generate graph IR JSON Schema | 01 | W01-T01 | [x] |
| W01-T03 | Implement workflow source loader | 01 | W01-T01, W00-T05 | [x] |
| W01-T04 | Compile source to execution plan with source maps | 01 | W01-T01, W01-T02, W01-T03 | [x] |
| W01-T05 | Implement validation passes | 01 | W01-T04, W00-T06 | [x] |
| W01-T06 | Add greenfield acceptance fixtures | 01 | W01-T04 | [x] |
| W01-T07 | Compile workflow activation declarations | 01 | W01-T03, W01-T04 | [x] |
| W02-T01 | Implement typed `Value` and `ArtifactRef` model | 02 | W01-T01 | [x] |
| W02-T02 | Add expression and interpolation engine | 02 | W02-T01, W00-T05 | [x] |
| W02-T03 | Bind workflow inputs and outputs | 02 | W02-T01, W02-T02, W02-T04 | [x] |
| W02-T04 | Define runtime state-store interface | 02 | W01-T04, W02-T01 | [x] |
| W02-T05 | Implement Hadron SQLite state adapter schema | 02 | W02-T04 | [x] |
| W02-T06 | Enforce redaction, retention, and secret-ref metadata | 02 | W02-T01, W02-T04 | [x] |
| W02-T07 | Infer data dependencies and enforce value visibility | 02 | W01-T04, W01-T05, W02-T02 | [x] |
| W02-T08 | Implement artifact-store contract and Hadron adapter | 02 | W02-T01, W02-T04, W02-T06 | [x] |
| W03-T01 | Implement node lifecycle state machine | 03 | W02-T04 | [x] |
| W03-T02 | Implement durable ready queue and claims | 03 | W03-T01 | [x] |
| W03-T03 | Implement readiness rules, skip, failure, and timeout propagation | 03 | W02-T02, W03-T01 | [x] |
| W03-T04 | Implement retry, backoff, cancellation, and fan-out | 03 | W02-T02, W02-T07, W03-T01, W03-T02, W03-T05 | [x] |
| W03-T05 | Implement generic `WaitRecord` suspend/resume | 03 | W03-T01, W02-T05 | [x] |
| W03-T06 | Implement crash recovery and replay | 03 | W03-T02, W03-T05, W03-T08 | [x] |
| W03-T07 | Enforce scheduler concurrency resources and run policies | 03 | W03-T02, W03-T04 | [x] |
| W03-T08 | Implement catch, finally, switch, and error-as-data semantics | 03 | W02-T07, W03-T03, W03-T04 | [x] |
| W03-T09 | Implement memoization and pinned-value execution | 03 | W02-T04, W03-T06, W03-T08 | [x] |
| W04-T01 | Harden step-kind runtime integration | 04 | W00-T06, W03-T01 | [x] |
| W04-T02 | Implement `transform` executor | 04 | W02-T02, W04-T01 | [x] |
| W04-T03 | Implement `cmd` executor | 04 | W02-T01, W04-T01 | [x] |
| W04-T04 | Implement `http` executor | 04 | W02-T01, W04-T01 | [x] |
| W04-T05 | Implement `mcp` executor | 04 | W02-T01, W04-T01 | [x] |
| W04-T06 | Implement `call` executor | 04 | W01-T04, W03-T02, W04-T01 | [x] |
| W04-T07 | Implement wait-backed executor set | 04 | W03-T05, W04-T01 | [x] |
| W04-T08 | Implement node verification modifier | 04 | W02-T02, W03-T08, W04-T01 | [x] |
| W05-T01 | Create Hadron workflow host binding | 05 | W02-T05, W02-T08, W03-T02, W04-T01, W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07 | [x] |
| W05-T02 | Introduce `RunScope` and `ExecutionTarget` | 05 | W05-T01 | [x] |
| W05-T03 | Bind registry and definition resolution | 05 | W01-T04, W05-T01 | [x] |
| W05-T04 | Bind schedules and triggers to activation registrations | 05 | W00-T07, W03-T05, W05-T03 | [x] |
| W05-T05 | Persist plan/source snapshots and provenance | 05 | W01-T04, W05-T03 | [ ] |
| W05-T06 | Update run inspection and diagnostics | 05 | W03-T01, W03-T07, W05-T01 | [ ] |
| W05-T07 | Add workflow contract-test and registration service | 05 | W03-T06, W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07, W05-T01, W05-T03 | [x] |
| W05-T08 | Materialize source-declared activations | 05 | W01-T07, W05-T03, W05-T04 | [ ] |
| W06-T01 | Add workflow CLI commands | 06 | W05-T03, W05-T06 | [ ] |
| W06-T02 | Add HTTP workflow API surface | 06 | W05-T01, W05-T06 | [ ] |
| W06-T03 | Update MCP workflow exposure profiles | 06 | W05-T03, W05-T07, W06-T02 | [ ] |
| W06-T04 | Bind A2A task/run correlation | 06 | W05-T01, W06-T02 | [ ] |
| W06-T05 | Update desktop graph/run views | 06 | W01-T04, W05-T06 | [ ] |
| W06-T06 | Quarantine old blueprint/pipeline runtime paths | 06 | W06-T01, W06-T02, W06-T03, W06-T04, W06-T05 | [ ] |
| W06-T07 | Final docs, examples, and release checks | 06 | W06-T06, W06-T08, W06-T09, W06-T10 | [ ] |
| W06-T08 | Add workflow authoring, registry, and tool-building surfaces | 06 | W05-T07, W06-T01, W06-T03, W06-T04 | [ ] |
| W06-T09 | Prove the Torque bulk-create end-to-end acceptance case | 06 | W03-T04, W03-T07, W03-T08, W04-T02, W04-T05, W05-T01, W05-T03, W05-T07, W06-T03 | [ ] |
| W06-T10 | Move the workflow UI off legacy Wails contracts | 06 | W06-T02, W06-T05 | [ ] |
| W07-T01 | Add provider-agnostic `llm` step kind | 07 | W04-T08, W05-T01 | [x] |
| W07-T02 | Add goja-backed `script` step kind | 07 | W04-T01, W02-T02 | [x] |
| W07-T03 | Reintroduce `agent_launch` as workflow sugar | 07 | W03-T05, W04-T06 | [x] |
| W07-T04 | Add generated API client step families | 07 | W04-T01, W06-T02 | [ ] |
| W07-T05 | Add compiled/offline workflow build path | 07 | W01-T02, W04-T02, W04-T03, W04-T04, W04-T05, W04-T06, W04-T07 | [x] |
| W07-T06 | Finalize public engine boundary and downstream adoption kit | 07 | W00-T04, W06-T07 | [ ] |
| W07-T07 | Add `emit` and `checkpoint` step kinds | 07 | W04-T01, W04-T07, W05-T01 | [x] |
| W07-T08 | Add reactor, signal, and durability controls | 07 | W00-T07, W03-T06, W04-T07, W05-T04 | [ ] |
| W07-T09 | Add advanced graph authoring and service-node semantics | 07 | W02-T07, W03-T08, W04-T07 | [ ] |
| W07-T10 | Add compensation and rollback contracts | 07 | W03-T08, W04-T06 | [ ] |
| W07-T11 | Add SDK and agent-authored IR front ends | 07 | W01-T02, W05-T03, W06-T01, W06-T02 | [ ] |

## Verification Strategy

- Unit tests should land with every task that adds core behavior.
- Conformance tests should become the shared acceptance boundary for compiler,
  state store, scheduler, waits, and step-kind behavior.
- A task is not complete until its listed verification passes and its dependency
  outputs have been integrated.
- W06-T09 is the release-blocking semantic acceptance test: the Torque-style
  workflow must run end to end and be callable as a pinned MCP tool.
- Integration tests should prove Hadron app/service wiring only after the core
  contracts are covered directly.
- Legacy examples should be used as reference fixtures only after they are
  archived and intentionally rewritten into graph-native workflows.

## Handoff Notes

- Follow [orchestrator-handoff.md](orchestrator-handoff.md) for dispatch,
  isolation, integration, task-report, and escalation rules.
- Dependencies in the task index are authoritative task IDs. Wave numbers group
  work but do not override the dependency graph.
- The orchestrator owns package-name and public-contract decisions that remain
  within accepted architecture; a subagent must escalate choices that would
  change an ADR or the coverage ledger.
- Parallel write agents require isolated worktrees and disjoint task ownership.
  Without isolation, serialize implementation tasks.
- Wave 07 is part of the complete future-state plan but does not block the Wave
  06 engine release gate unless product priority promotes one of its tasks.
