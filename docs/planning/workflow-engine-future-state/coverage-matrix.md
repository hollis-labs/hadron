# Workflow Engine Source-To-Task Coverage

**Status:** requirement disposition ledger
**Sources:**
[`HADRON_DESIRED_FUTURE_STATE.md`](../../architecture/HADRON_DESIRED_FUTURE_STATE.md)
and
[`workflow-engine-target-capabilities.md`](../../workflow-engine-target-capabilities.md)
**Execution plan:** [README](README.md)

This ledger is the completeness check between the two exploration sources and
the executable task plan. `Implemented` means integrated evidence satisfies the
capability. `Superseded` means a later accepted architecture decision
intentionally replaced the source statement. `Downstream` means the engine
contract is delivered here but another application owns its later adoption.
`Eligible` is temporary and prevents full-plan completion until the remaining
approved task is implemented.

## Supersession Rule

The two sources are evidence and intent, not the last word where accepted ADRs
or the detailed architecture made a later choice. In particular:

- Blueprint and pipeline compatibility loaders, continued public
  `pipeline.yaml` sugar, and preservation of legacy examples are superseded by
  ADR 0007 plus D04/D22/D25/D26: one greenfield `workflow` source, archived
  examples, and no public legacy parser commitment. Covered by W00-T01,
  W01-T03, and W06-T06.
- Cross-application implementation and coordinated adoption are superseded for
  this execution plan by the current scope: Hadron builds and documents the
  public engine contract; each application adopts it later. Covered by W07-T06
  as an adoption kit only.
- The accepted readiness vocabulary is the D10 subset (`all_success`,
  `all_done`, `one_failed`, `all_failed`, `none_failed`, `always`) rather than
  every Airflow rule surveyed in the exploration document. Covered by W03-T03.

## Core And Source

| Source requirement | Disposition | Owning tasks |
| --- | --- | --- |
| Embeddable Go engine with no daemon/app/storage/provider dependency | Implemented | W00-T02, W00-T03, W07-T06 |
| Hadron remains the reference host and operator product | Implemented | W05-T01 through W05-T08, W06-T01 through W06-T09 |
| One semantic graph across embedded, CLI, HTTP, MCP, A2A, UI, and offline doors | Implemented | W01-T01, W05-T01, W06-T01 through W06-T06, W07-T05 |
| Blueprint/pipeline compatibility loaders and public sugar | Superseded | W00-T01, W01-T03, W06-T06 |
| Stable IDs, version, digest, provenance, schemas, source refs/maps | Implemented | W01-T01 through W01-T05, W05-T05 |
| `call.mode` supports `inline` and `run`, child outputs, target and parent-close policy | Implemented | W01-T01, W04-T06 |
| Source `on:` declarations lowered to operational activations | Implemented | W01-T07, W05-T04, W05-T08 |
| Go structs as IR authority plus deterministic JSON Schema | Implemented | W01-T01, W01-T02 |
| SDK, UI, and agent-emitted inputs remain views over the IR | Implemented | W07-T11 |

## Values And Expressions

| Source requirement | Disposition | Owning tasks |
| --- | --- | --- |
| Typed inline JSON values and referenced large/binary/sensitive artifacts | Implemented | W02-T01, W02-T03, W02-T05, W02-T08 |
| Declared node/workflow output schemas and typed child/tool results | Implemented | W01-T01, W02-T03, W04-T01 through W04-T07 |
| `expr-lang/expr` plus interpolation only in strings | Implemented | W02-T02 |
| Fail-hard unresolved references and scoped visibility | Implemented | W01-T05, W02-T02, W02-T07 |
| Inferred data edges plus explicit ordering-only `needs` | Implemented | W02-T07 |
| Error and fan-out failure data available to expressions | Implemented | W03-T04, W03-T08 |
| Explicit `cmd` capture (`json`, `lines`, `kv`) and scoped legacy shim | Implemented | W04-T03, W06-T06 |
| Redaction, retention, opaque secret refs, and stream masking | Implemented | W02-T01, W02-T06, W04-T03 through W04-T05 |
| Memoization by safe content key and pinned development data | Implemented | W03-T09, W06-T01 |

## Runtime And Control Flow

| Source requirement | Disposition | Owning tasks |
| --- | --- | --- |
| Durable node/attempt/value/wait/event journal and CAS claims | Implemented | W02-T04, W02-T05, W03-T01, W03-T02 |
| Distinct failed, crashed, canceled, timed-out, waiting, skipped, and blocked states | Implemented | W02-T04, W03-T01 |
| Ready queue with no topological level barrier | Implemented | W03-T02, W03-T03 |
| Bounded pool, per-effect/capability limits, cross-run keys, fairness hooks | Implemented | W03-T02, W03-T07 |
| Fail-fast/run-to-completion and tolerated fan-out failure | Implemented | W03-T04, W03-T07 |
| `if`, named readiness, switch, catch, continue-on-error, finally, join | Implemented | W03-T03, W03-T08, W07-T09 |
| Runtime `for_each`, bounded fan-out, per-item attempts/output/error | Implemented | W03-T04 |
| Effect-aware retry, backoff, idempotency, cancellation, and timeout taxonomy | Implemented | W01-T05, W03-T04, W04-T01 |
| Crash recovery and replay from a selected node using journaled outputs | Implemented | W03-T06 |
| Compensation/rollback as an application-neutral design input | In progress | W07-T10 |

## Waits And Timed Activation

| Source requirement | Disposition | Owning tasks |
| --- | --- | --- |
| Generic `WaitRecord`, worker release, correlated/idempotent resume | Implemented | W03-T05 |
| Gate, message, callback, signal, timer, event, and child-run wakes use one path | Implemented | W03-T05, W04-T07, W07-T08 |
| Shared gate/checkpoint vocabulary with product-owned authority/presentation | Implemented | W04-T07, W07-T07 |
| Environment-bound and optional/non-blocking manual gates | Implemented | W04-T07, W07-T09 |
| Stable scheduler fire identity, attempts, retry/exhaustion, observations, configurable clock/cadence/batch, CAS tests | Implemented | W00-T07 |
| Hadron activation registrations, overlap/missed-fire/catchup/reuse policy | Implemented | W05-T04, W05-T08 |
| Long-lived reactors, signals/queries/updates, continue-as-new, durability modes | Implemented | W07-T08 |

## Step Kinds And Effects

| Source requirement | Disposition | Owning tasks |
| --- | --- | --- |
| Versioned registry with config/input/output schema, effects, capabilities, idempotency, retry, cancel, observe, suspend, embedded metadata | Implemented | W00-T06, W04-T01 |
| Initial `transform`, `cmd`, `http`, `mcp`, `call`, `sleep`, `wait_for`, `human_gate`, `message_wait` | Implemented | W04-T02 through W04-T07 |
| `verify` modifier and literal tool-activity evidence | Implemented | W04-T08 |
| Provider-agnostic typed `llm`, restricted tools, audit records, schema repair/fail | Implemented | W07-T01 |
| Sandboxed goja `script`, capability surface, schema inference | Implemented | W07-T02 |
| Heavy `agent_launch` plus correlated wait sugar | Implemented | W07-T03, W07-T08 |
| Generated OpenAPI/AsyncAPI/gRPC/GraphQL operation families | Implemented | W07-T04 |
| `emit` and `checkpoint` | Implemented | W07-T07 |
| Effect-driven retry/recovery/MCP/confirmation/dry-run/blast-radius policy | Implemented | W01-T05, W03-T04, W03-T07, W05-T01, W06-T01, W06-T03 |

## Hadron Product Surfaces

| Source requirement | Disposition | Owning tasks |
| --- | --- | --- |
| Registry search/version/digest/provenance/package/pin/publish | Implemented | W05-T03, W05-T05, W05-T07, W06-T08 |
| Definition-level contract tests with mocked executors | Implemented | W05-T07 |
| Agent-builds-tools flow from discovery through tested pinned exposure | Implemented | W05-T07, W06-T08 |
| CLI and HTTP validate/explain/run/inspect/cancel/resume/rerun/signals | Implemented | W06-T01, W06-T02, W07-T08 |
| MCP meta, pinned, discoverable/lazy, hidden tiers; namespaces/budgets/catalog/list-changed | Implemented | W06-T03, W06-T06 |
| A2A durable task correlation and schema/effect/provenance-derived cards | Implemented | W06-T04 |
| UI graph, edge values, waits, artifacts, source maps, exposure, blast radius, replay | Implemented | W06-T02, W06-T05, W06-T06, W06-T10 |
| Legacy Wails workflow UI replaced while preserving the flow canvas | Implemented | W06-T10 |
| Compiled CLI and stdio MCP-server artifacts | Implemented | W07-T05 |
| Global run-failure workflow | Implemented | W07-T08 |
| Torque bulk-create runs end to end as a pinned MCP tool | Implemented | W06-T09 |

## Advanced Authoring

| Source requirement | Disposition | Owning tasks |
| --- | --- | --- |
| Static matrices lower to fan-out | Implemented | W07-T09 |
| Daemon/service steps with readiness and teardown | Implemented | W07-T09 |
| Dynamic graphs use validated generated child definitions, not live mutation | Implemented | W07-T09 |
| Fluent/Go/TypeScript/agent front ends remain views over IR | Implemented | W07-T11 |
| No mutable single state document, implicit per-item execution, jq language, level barriers, live plan patching, or global log output scan | Implemented | W01-T01, W02-T01, W02-T02, W03-T02, W03-T04, W04-T03, W06-T06 |

## Consumer Boundary

| Source requirement | Disposition | Owning tasks |
| --- | --- | --- |
| Engine public contracts remain usable without Hadron app/service | Implemented | W00-T02 through W00-T06, W07-T06 |
| Nanite/Torque/Cerberus product semantics stay outside core | Implemented | W00-T03, W04-T07, W07-T06, W07-T07, W07-T10 |
| Nanite LLM/tool/flex/loop/team/reflex executors and presentation adapters | Downstream | Public extension contracts: W00-T06, W04-T01, W07-T06; Nanite owns implementation |
| Torque task/checkpoint lifecycle, policy, escalation, and task-state adapter | Downstream | Public wait/checkpoint contracts: W03-T05, W04-T07, W07-T07; Torque owns implementation |
| Cerberus execution targets and connector action adapters | Downstream | Public target/call/compensation contracts: W04-T06, W05-T02, W07-T10; Cerberus owns implementation |
| Nanite, Torque, Cerberus, and other app adapters/migrations/conformance adoption | Downstream | Explicitly outside this execution plan; each application owns adoption after engine completion |
| Extraction to a separate shared workflow repository | Downstream | W07-T06 documents criteria only; no repository creation or move in this plan |

## Legacy Follow-Ups

| Source follow-up | Disposition | Owning tasks |
| --- | --- | --- |
| `::set-output` injection risk and safety documentation | Implemented | W04-T03, W06-T06, W06-T07 |
| Timestamp MCP session replaced by real principal/profile | Implemented | W06-T02, W06-T03 |
| Worker-holding gate/message polling | Implemented | W03-T05, W04-T07, W06-T06 |
| Pipeline 60-second wait and `on_success: step` stub | Superseded | W03-T04, W03-T08, W06-T06 |
| Evaluate legacy blueprint broker against tool-building flow | Implemented | W06-T08 |
