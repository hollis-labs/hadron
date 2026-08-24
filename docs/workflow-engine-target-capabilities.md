# Hadron Workflow Engine — Target Capabilities

**Status:** Exploration record and target direction (pre-design)
**Date:** 2026-08-23
**Companion to:** [workflow-engine-direction.md](workflow-engine-direction.md)
**Scope:** Findings from the live code, gap analysis against the intended
product, a survey of prior art, the target shape of the engine, the decisions
made in this session, and the open questions for the design session. This is
not an implementation plan and does not define phases or estimates.

The direction document says what Hadron must *not* own. This document says
what Hadron *can become* inside those boundaries. Where the two appear to
conflict, the direction document's ownership rules win; nothing here requires
Hadron to own business definitions, secrets, or another application's state.

---

## 1. Why this document exists

Hadron started as an answer to one problem: agents rewrite the same code to
solve the same problem over and over (scaffold a project, deploy, add tasks to
a tracker). The intended shape was never "simple automations." It was a glue
language: piece together commands, APIs, data sources, and agents into what is
functionally a new, purpose-built tool — and make that tool reusable,
discoverable, and auditable.

Two recent events made the gap between that intent and the current engine
concrete:

1. **Torque has no bulk task create.** It exposes `torque_task_bulk_update`,
   `_bulk_transition`, `_bulk_delete`, `_bulk_tag`, and
   `torque_task_subtodo_bulk_add`, but not `torque_task_bulk_create`. An agent
   should be able to write a reusable Hadron blueprint that provides one. It
   cannot — see §4.
2. **Nanite shipped Agent Workflows** (2026-08-13 onward): a deterministic DAG
   engine for agent steps. Its code cites Hadron's pipeline runner as its
   source and reimplements it at finer granularity. The two engines have now
   diverged in opposite directions, each holding roughly half of what the
   other lacks — see §3.

The longer arc: Hollis Labs applications are experiments to find the right
shapes, which are then extracted into shared Go libraries; those libraries are
the building blocks for purpose-built apps and, ultimately, a software
factory. The workflow engine described here is one of those extraction
targets. This document records the shape so the design session can start
from evidence rather than memory.

---

## 2. Findings — Hadron as built

All references are to HEAD `d94b919` on 2026-08-23. Line numbers are
approximate to within a few lines.

### 2.1 Two languages stacked

Hadron is not one workflow language. It is two, layered:

| Layer | Unit | Execution model | Concurrency | Data flow |
|---|---|---|---|---|
| Blueprint | sections → steps | strictly sequential | none | none inside a blueprint |
| Pipeline | stages (each a blueprint run) | DAG via `depends_on`, Kahn levels | level barrier | `{{ stages.X.key }}` strings |

The engine that matters — the DAG, the toposort, the template data plane —
lives in the *pipeline* layer and operates on whole blueprint runs. The
blueprint layer, where every actual capability call happens, has no graph
and no values.

### 2.2 Blueprint execution model

- **Definition.** `internal/blueprint/blueprint.go:20-33` — `Blueprint{Version,
  Spec, Project, Env, Inputs, Packages, Git, Stubs, Tools, Imports, Hooks,
  Steps []Section}`. `Section{Section string, Steps []Step}` (`:254-259`, YAML
  key still `tasks`). `Step` (`:269-292`) carries exactly one executable kind:
  `cmd`/`run`, `call`, `http_call`, `mcp_call`, `message_wait`,
  `agent_launch`, or `human_gate` (enforced at `:646-657`), plus `if`, `with`,
  `dir`, `env`, retry/backoff/timeout, `continue_on_error`, `enabled`,
  `on_success`/`on_fail` hooks.
- **Inputs but no outputs.** `Input` (`:56-72`) is a reasonably typed schema
  (string/number/boolean/array, enum, pattern, min/max). There is **no
  `outputs:` block at any level.** A blueprint has a typed input contract and
  an untyped "it ran" result.
- **Render once, up front.** `RenderForExecution` (`:953-1058`) deep-copies the
  blueprint and renders every string field through Go `text/template` against
  `BuildTemplateContext` (`:875-949`): `inputs`, `env`, `project`, `packages`,
  `git`, `stubs`, `workspace`, `blueprint`. This happens **before any step
  executes**, so no step result can ever be referenced by a later step.
- **Execution loop.** `internal/execution/manager.go:142-195`:

  ```go
  for _, section := range bp.Steps {
      for stepIdx, step := range section.Steps {
          // enabled check, if check, dispatch by kind, on_fail / on_success hooks
      }
  }
  ```

  Sequential by construction. Each step kind dispatches to its executor
  (`:161-175`).
- **Conditions.** `if:` is rendered at parse time against static inputs, then
  evaluated by `evaluateCondition` (`manager.go:559-575`), a truthy-string
  check ("1"/"true"/"yes"/number≠0; unknown strings are *true*). It cannot
  observe what a step returned.
- **Child calls return nothing.** `executeCallStep` (`manager.go:441-460`)
  merges `imports[].with` and `step.with` into child inputs and recurses into
  `executeFile`. The child's events are emitted under the parent run ID; no
  value comes back.
- **Command execution.** `execCmd` (`manager.go:311-353`) runs `bash -lc <cmd>`
  under a PTY, streams each line as a `log` event. stdout is never captured
  into a variable. stderr is not separable (PTY).
- **Output escape hatch: log lines.** `http_call.go`, `mcp_call.go`,
  `agent_launch.go`, `human_gate.go`, and `message_wait.go` each emit their
  results as `log` events of the form `::set-output key=value` (the GitHub
  Actions convention). Nothing inside the blueprint reads them; the pipeline
  runner scrapes them afterwards (§2.3). The data plane and the log plane are
  the same plane.
- **Stub.** `on_success: [{type: step, value: X}]` ("jump to step") emits a
  message and does nothing (`manager.go:395-397`).
- **Scaffolding residue.** `Project.PHPVersion`, `Project.Node`, `Packages`
  (composer/npm/pip/brew/go), `Git`, `Stubs`, `Tools.Install` are baked into
  the universal blueprint struct (`blueprint.go:46-227`) and the template
  context. The direction document already flags this.

### 2.3 Pipeline execution model

- **Definition.** `internal/pipeline/pipeline.go:15-47` — `Spec{Meta,
  StopOnFail, Defaults, Stages, Inputs}`; `Stage{Name, BlueprintPath, Inputs,
  If, DependsOn, Position{X,Y}, Outputs map[string]string, WaitTimeoutSeconds,
  Async}`.
- **Validation.** `Validate` (`:70-160`) checks names, paths, output key
  pattern, unknown/self dependencies, and runs a 3-colour DFS for cycles.
- **Scheduling.** `TopoSort` (`toposort.go`) is Kahn's algorithm producing
  `[][]Stage` levels (v1 fallback: no `depends_on` → one stage per level, in
  order). `Runner.execute` (`runner.go`) iterates levels; within a level every
  stage runs in its own goroutine under one `sync.WaitGroup`; the level is
  fully joined before the next begins. **A slow stage in level N delays every
  stage in level N+1, including those whose own dependencies finished long
  ago.**
- **Data plane.** `resolveStageInputs`/`resolveTemplate` substitute
  `{{ stages.<name>.<key> }}`, `{{ .stages.<name>.outputs.<key> }}`,
  `{{ .stages.<name>.status }}`, `{{ inputs.<key> }}` by string scanning.
  Unresolvable references are **silently dropped**. `buildStageOutputsSnapshot`
  converts every value with `fmt.Sprintf("%v")` — all values are strings by
  the time a downstream stage sees them.
- **Output capture.** `captureStageOutputs` calls `ListRunEvents(runID, 1000)`
  and scans every message for `::set-output`. stdout = last 64 KB of `log`
  lines. Stage-level `outputs:` map entries are either the literals `stdout`,
  `stderr` (always empty), `exit_code`, or passthrough strings. Because the
  scan covers every `log` line, anything a `cmd` prints — including content
  it read from an untrusted source — can set a stage output, and stage
  `if:` conditions consume those outputs. This is the same injection
  exposure that led GitHub to deprecate `::set-output` in 2022 (§5.1).
- **Completion detection.** `waitForRunTerminal` polls `GetRun` every 75 ms.
  The default stage wait timeout is **60 s** (`pipeline.go:StageWaitTimeout`),
  which is short for anything that launches an agent.
- **Conditionals.** Stage `if:` is a real Go template with `eq`/`ne`/`and`/`or`
  over `.stages.<name>.status|outputs` and `.inputs`
  (`evaluateIfTemplate`), then `evaluateCondition` with `==`/`!=` support.
  This is the most capable condition evaluation in Hadron and it is only
  available at stage granularity.
- **Run identity.** Each stage is enqueued as an ordinary `execution.Request`
  with run ID `plr-<pipelineRunID>-<idx>` into the **same** manager worker
  pool. A pipeline with more parallel stages than workers queues. Stage runs
  are real `runs` rows with their own events — this is the only semantic
  difference between a pipeline stage and a blueprint `call`.

### 2.4 Waits and workers

- `NewManager` (`internal/execution/lifecycle.go:15-34`) starts a fixed pool
  of `workers` goroutines consuming a `chan Request` with capacity 128.
- `human_gate` (`human_gate.go`) creates a `human_gates` row and then **loops,
  polling `GetHumanGate` every `poll_interval_seconds` (default 1 s) until
  decided or timed out.** `message_wait` (`message_wait.go`) does the same
  against `MessageSource.PollMessage`. Both hold a worker for the entire
  wait. A gate with a 30-minute timeout occupies a worker for 30 minutes.
- There is no suspend/resume. `worker()` (`manager.go:25-69`) has no recovery
  path: a daemon restart loses in-flight runs.
- Cancellation via `context` works (`Manager.Cancel`, `lifecycle.go:86-99`).

### 2.5 Agent launch

- `executeAgentLaunchStep` (`agent_launch.go`) calls
  `AgentLauncher.LaunchAgent` and **returns immediately** with `session_id`,
  `mailbox`, and handles. It is fire-and-forget; a result is obtained by
  pairing it with a `message_wait` on the mailbox with a shared
  `correlation_id` (`examples/agentic-launch-and-wait.yaml`).
- `agentsubstrate.Launcher.LaunchAgent` (`launcher.go:89-178`) resolves a
  provider/runtime binding through agentkit (`runtimebind.Resolve`), builds a
  CLI adapter, plants a boot directory, starts an `agentsessions` session,
  and fires a detached kickoff turn (`Boot @./boot.md`, `:158-162`). Replies
  flow through a reply outbox watched for 15 minutes, or a fallback reply
  assembled from the kickoff turn's output.
- **Every LLM use is a full CLI agent session.** There is no lightweight
  "prompt → model → structured result" step. go-providers and go-llm-types
  are already in the dependency graph via agentkit, so the plumbing exists;
  the step kind does not.
- Only substrate kind: `go_agent_runtime` (legacy name for the agentkit path).

### 2.6 Platform surfaces (what is already solid)

The platform around the engine is more mature than the engine:

- **Daemon + persistence.** SQLite with migrations 0001–0013: `runs`,
  `run_events` (append-only), `pipeline_runs`, `pipeline_stage_runs`
  (+`outputs_json`), `schedules`, `triggers` (webhook HMAC + fs/fsnotify with
  debounce, `extract_inputs` from body/header/query, `created_by`),
  `blueprint_registry` (+versions, content hashes), `human_gates`, `messages`
  (substrate/to/thread/correlation), `workspaces`.
- **Observability.** OTel spans on every run and step kind (`feotel.StartSpan`
  throughout `internal/execution`), structured JSONL telemetry, append-only
  run events, `hadron_run_operations`/`rundiagnostics` summaries.
- **Doors.** CLI (`run`, `validate`, `lint`, `fmt`, `pack`/`unpack` `.hbp`,
  `registry index|search|show|versions`, `schedule`, `trigger`, `gate submit`,
  `pipeline run`, `test-gen`, `agent-card`), HTTP API, MCP server with ~45
  `hadron_*` tools (blueprint discover/search/get/validate/lint/schema/broker,
  run enqueue/get/events/operations/cancel, pipeline enqueue/graph/stages,
  schedule/trigger/workspace CRUD, human_gate get/submit, message
  send/consume/inbox/list/thread, registry, skills), A2A with agent cards
  where each blueprint becomes a skill (`internal/agentcard/agentcard.go:78`),
  and a Wails desktop app with a flow canvas (stages carry `position`).
- **Executor interfaces.** `MCPCaller`, `MessageSource`, `AgentLauncher`
  (`internal/execution/types.go`) are clean seams; the MCP client
  (`internal/mcpadapter/internal_caller.go`) handles transport reuse, health
  probes, reconnects, retries, and reports that metadata into events.
- **Safety.** Command allow/deny substring checks and path checks via
  `SettingsValidator`; per-tool MCP annotations (`readOnlyHint`,
  `destructiveHint`, `idempotentHint`) derived from a behaviour table
  (`internal/mcpadapter/server_surface.go:119-121`); bearer token and
  `scopes` on the MCP adapter (`adapter.go:154`).
- **Registry.** Directory indexing with content hashes and version history,
  name/slug resolution for `imports` and pipeline `blueprint_path`, pack
  bundles with dependency collection.

### 2.7 Gap table

| # | Gap | Evidence | Blocks |
|---|---|---|---|
| 1 | No iteration (`for_each`/map) | no such field in `Step` | Torque bulk create, any batch job, n8n item model |
| 2 | No typed data plane inside a blueprint | render-once (`blueprint.go:953`), `::set-output` logs | glue language, conditionals on results, composition |
| 3 | No step-level concurrency | `manager.go:142-195` | parallel API calls, fan-out inside a tool |
| 4 | `call` returns nothing; no `outputs:` | `manager.go:441-460`, `Blueprint` struct | blueprints as functions/tools, A2A/MCP output schemas |
| 5 | No expression language | Go template + truthy strings | `if` on data, filters, arithmetic, routing |
| 6 | No lightweight `llm` step | only `agent_launch` | classify/extract/judge/route nodes |
| 7 | Waits hold workers; no suspend/resume/recovery | `human_gate.go`, `message_wait.go`, `worker()` | long gates, many concurrent runs, daemon restarts |
| 8 | Level-barrier scheduling | `toposort.go`, `runner.go` | throughput on uneven DAGs |
| 9 | Control flow limited to `if` + `continue_on_error` + hooks | `Step` fields | switch/route, try/catch, compensation |
| 10 | Polling everywhere (75 ms stage, 1 s gate/message, 50 ms) | `runner.go`, `human_gate.go` | latency, CPU, scale |
| 11 | Outputs scraped from log lines | `captureStageOutputs` | correctness (size caps, ordering), retention policy |
| 12 | Scaffolding fields in the universal struct | `blueprint.go:46-227` | language narrowness (direction doc) |
| 13 | Pipeline stage wait default 60 s | `pipeline.go` | agent stages time out silently |
| 14 | Auto-exposing every blueprint as an MCP tool floods agent context | history (§7.1) | blueprints-as-tools |

---

## 3. Findings — Nanite Agent Workflows

Mapped from `apps/nanite` HEAD `094c8d59` on 2026-08-23. Nanite has two
unrelated "workflow" packages; the legacy `internal/workflow` (generic YAML
pipeline runner) is not discussed. The Agent Workflows pillar is
`internal/agentworkflow` (types/interfaces) plus `internal/service/workflow_*.go`
(engine), first commit `64bf9382` on 2026-08-13.

### 3.1 Lineage

`internal/agentworkflow/dag.go:12-13` and
`internal/service/workflow_engine.go:30-35` cite `apps/hadron/internal/pipeline`
`TopoSort` and `Runner` as the direct source, "applied at per-step
granularity." The scheduler is the same Kahn-levels-plus-barrier loop with
per-level goroutine fan-out (`workflow_engine.go:270-403`). Data flow is the
same string templating (`{{steps.X.output}}`, `{{input.k}}`), with one
deliberate divergence: unresolvable references **fail hard**
(`:781-785`) where Hadron drops them silently. Hadron is cited as prior art
only; there are zero imports (`internal/background/types.go:20`: "D1 — Hadron
is NOT the dispatch primitive"). The Hadron context gate in `contextbroker`
was retired on 2026-08-23 (AD-07).

### 3.2 What Nanite added

- **Step kinds** (`types.go:7-47`): `llm`, `tool`, `gate`, `flex`, `loop`.
  `verify` is a *modifier* on llm/tool steps, not a kind (`:207-241`).
- **`llm` step** (`interfaces.go:31-44`, `workflow_step_executor.go:205-297`):
  `Provider`, `Model`, `SystemPrompt`, `Messages`, `Tools []string`,
  `MaxToolIterations` (default 10), `SessionID`/`AgentID`,
  `EnableContextAssembly`. Tools are restricted to **exactly** the listed set
  — an unknown name is a hard error (`:349-372`), and a backstop refuses a
  `tool_use` outside the set even if the provider emits one (`:271-281`).
  Returns `LLMStepResult{Text, ToolCalls []ToolCallRecord, Usage,
  StopReason}`. `ToolCallRecord` exists so verification can check literal
  tool activity rather than the model's self-report; the doc comment names
  the motivating failure: "an agent fabricating a Torque fetch instead of
  calling it."
- **`verify`**: engine checks (`no_error`, `tool_called`, `output_contains`,
  `tests_pass`, `lint_pass`) or an agent reviewer with an injection-hardened
  prompt that demands `PASS`/`FAIL` and fails closed on anything unparseable
  (`:399-459`).
- **Durable waits**: one pattern reused three times — a step marks
  `waiting_on_{gate,flex,loop}`, the engine returns, and a caller re-drives
  `Resume` (`:156-230`), which reloads step rows and replays the levels. Every
  transition is persisted immediately with `context.WithoutCancel` so
  bookkeeping survives cancellation. At-least-once: a step found `running` on
  resume is re-run.
- **Scoped visibility**: a step's template scope is exactly its declared
  `DependsOn` (`:703-711`).
- **Loop**: delegated to a separate goal-driven `internal/loop` engine (one
  `WorkflowRun` per iteration; decisions `CONTINUE | RETRY | REPLAN |
  REARCHITECT | WAIT | ESCALATE | COMPLETE | FAIL`, deterministic-first with an
  LLM fallback only on a sustained no-progress streak). A loop is explicitly
  "not a DAG" because the plan may change shape between iterations.
- **Teams**: `CompileTeam` (`team_compiler.go:104-178`) compiles phases into a
  strictly linear flex/gate chain; "TeamRun IS a WorkflowRun."
- **External engines**: LangGraph/CrewAI/ADK/AutoGen/LangChain run as
  subprocesses that call back through MCP `workflow_execute_{llm,tool}_step`
  and ignore `Steps`.

### 3.3 What Nanite lacks (that Hadron has)

No conditionals of any kind, no retry/backoff, no `continue_on_error`, no
cmd/http/mcp-as-step (tools go through the harness), no triggers (scheduler is
a port of Hadron's adapter split), no registry or pack, no event log for this
engine, and the definition is not persisted (Resume requires the caller to
re-supply it — the team launcher keeps compiled definitions registered while
waiting). Concurrency within a level is unbounded.

### 3.4 Side by side

| Dimension | Hadron (blueprint / pipeline) | Nanite Agent Workflows | Target (§6) |
|---|---|---|---|
| Unit of graph | pipeline stage (= blueprint run) | step | step/node |
| Scheduling | Kahn levels + barrier | Kahn levels + barrier | ready queue, per-node readiness |
| Concurrency | level, bounded by worker pool | level, unbounded | bounded pool + per-key semaphores |
| Step kinds | cmd, call, http, mcp, message_wait, agent_launch, human_gate | llm, tool, gate, flex, loop | union + script, transform, sleep, wait_for |
| Verify | — | modifier (engine/agent) | modifier (adopt) |
| Conditionals | blueprint: truthy string; pipeline: Go template | none | expression language |
| Iteration | none | loop engine (separate) | `for_each` + loop-as-node |
| Retry / errors | retry+backoff, continue_on_error, on_fail hooks | none (skip cone) | policy object, effect-aware, on_error |
| Data flow | stringified `{{stages.X.y}}` (pipeline only) | stringified `{{steps.X.output}}` | typed values on edges |
| Outputs | `::set-output` log lines; no `outputs:` | single `Output string` | declared `outputs:` schema at step and definition level |
| Waits | hold worker, poll | waiting_* + Resume | durable suspend; wake via trigger/event |
| Durability | run_events; no recovery | run/step rows; at-least-once | run_steps + values; replay from node |
| Triggers | webhook, fs, cron | cron (ported) | `on:` sugar → registrations |
| Registry / packaging | yes | in-memory | yes, digest-addressed |
| Exposure | MCP meta-tools, A2A skills | MCP self-tools | profiles: pinned/discoverable/hidden |

---

## 4. Acceptance case: Torque bulk create

Torque either already has a bulk create or will (it was requested alongside
the other bulk/batch changes). The blueprint below is therefore a
**validation case for the engine, not a capability Hadron needs to supply**
— it is kept because its shape (array input → N typed calls with bounded
concurrency and retries → typed outputs) is exactly the shape the engine
must handle, and because it is the case that originally exposed the gap.

**Today.** Inputs can declare `tasks: array`, but no step can iterate. The
available workarounds each defeat the purpose: a `cmd` step running a bash
loop over `curl` (loses `mcp_call`, typed results, per-item events, and
retries), an `agent_launch` to "do it" (a full agent session for a
deterministic job), or hand-unrolling N `mcp_call` steps (not reusable).

**Target.** Illustrative syntax — the design session owns the final form.
Expression-typed fields (`if`, `for_each`, `transform`, `outputs`) take bare
expressions; string fields interpolate with `{{ }}`.

```yaml
blueprint:
  name: torque-task-bulk-create
  namespace: torque
  description: Create many Torque tasks in one call and return their IDs.
  effects: [mutate]

inputs:
  - name: project_id
    type: string
    required: true
  - name: tasks
    type: array
    items_type: object
    required: true

steps:
  - name: create
    for_each: inputs.tasks
    concurrency: 4
    mcp_call:
      server: torque
      tool: torque_task_create
      arguments:
        project_id: "{{ inputs.project_id }}"
        title: "{{ item.title }}"
        description: "{{ item.description }}"
    retry:
      attempts: 3
      backoff: exponential
      idempotency_key: "{{ inputs.project_id }}:{{ item.title }}"

  - name: summarize
    needs: [create]
    transform:
      created: map(steps.create.items, .outputs.result_json.id)
      failed:  filter(steps.create.items, .status == "failed")

outputs:
  created: steps.summarize.outputs.created
  failed:  steps.summarize.outputs.failed
  count:   len(steps.summarize.outputs.created)
```

Once registered, this blueprint is a callable with an input schema and an
output schema. Under the exposure model in §7 it appears to agents in the
`torque` namespace as a direct tool named `torque_task_bulk_create`, with
MCP annotations derived from `effects`. This is the acceptance test for the
engine core: when this file runs end to end and is callable as a tool, the
core exists.

---

## 5. Prior art: is the blueprint/pipeline distinction standard?

The question from the direction document (Q1) is whether blueprint and
pipeline should remain separate definition kinds. Survey:

| System | Definition unit(s) | Why more than one level | Sequence vs graph | Fan-out | Outputs |
|---|---|---|---|---|---|
| GitHub Actions | workflow → job → step; reusable workflow (job-level call), composite action (step-level call) | **job = runner/VM isolation** | steps sequential; jobs DAG via `needs` | `strategy.matrix` | job `outputs:`; step outputs via `$GITHUB_OUTPUT` (the `::set-output` lineage) |
| GitLab CI | pipeline → stage → job → script | **job = runner** | stages sequential or `needs` DAG | `parallel`/`matrix` | artifacts, dotenv reports |
| Tekton | Pipeline → Task → Step | **Task = pod; steps = containers sharing a workspace** | tasks DAG via `runAfter`; steps sequential | `matrix` | typed `results`, `$(tasks.x.results.y)` |
| Argo Workflows | **one `Workflow`**; template types `steps`, `dag`, `container`, `script`, `suspend`, `resource` | none — template type, not definition kind | both, chosen per template | `withItems`/`withParam`/`withSequence` + `parallelism` | `outputs.parameters/artifacts`; expr-lang in `{{= }}` |
| Airflow | DAG → tasks (+TaskGroup) | none | graph only; sequence = edges | dynamic task mapping | XCom |
| AWS Step Functions | state machine; states Task/Parallel/Map/Choice/Wait/Pass/Succeed/Fail | none; nesting via Parallel/Map branches or invoking another machine | graph of states | `Map` with `MaxConcurrency` | JSON state, JSONPath/JSONata |
| CNCF Serverless Workflow | one workflow; states operation/event/switch/parallel/foreach/inject/sleep/callback | none | graph | `foreach` with batch size | jq/JS expressions |
| Temporal | workflow (deterministic, replayed) + activity (side effects, retried); child workflows | **determinism/durability axis**, not isolation | code | code | typed returns |
| n8n | workflow = nodes + connections; sub-workflow via Execute Workflow node | none | graph | items model; Split In Batches / Loop Over Items | JSON items on edges |
| Nanite Agent Workflows | WorkflowDefinition steps DAG; Loop is a peer entity; Team compiles to a workflow | none (loop separated because the plan can change shape) | DAG levels | loop engine | `Output string` |
| Hadron today | blueprint (sequential) + pipeline (DAG of blueprints) | **run identity only** (stage = own `runs` row) | blueprint sequential; pipeline DAG | none | `::set-output` |

**Conclusion.** A two-level definition model exists in exactly two
situations: when the outer unit is an **isolation boundary** (a runner, VM,
or pod that inner steps share — GHA, GitLab, Tekton), or when the split marks
a **determinism boundary** (Temporal's replayable workflow vs. effectful
activity). Systems without those concerns (Argo, Airflow, Step Functions,
Serverless Workflow, n8n) use one definition kind with node/template types.

Hadron's stage-vs-call difference is neither. It is whether the child gets its
own run record and events — an observability property. The isolation concern
will exist later (a child running on a Cerberus execution target), and it is
also a property of the call, not a different kind of file. The determinism
concern is already handled structurally: Hadron's orchestration is data (the
graph), so it is deterministic by construction; only nodes have effects. That
is Temporal's workflow/activity split for free — the scheduler is the
"workflow," nodes are the "activities" — without requiring a second
definition kind.

Argo is the closest precedent for the target: one definition, template types
for sequential groups vs. DAGs, `withItems` for fan-out, typed outputs,
`suspend` for gates, and expr-lang for expressions.

### 5.1 Patterns from those systems worth carrying into the design

Grouped by the part of §6 they affect. Each names its precedent so the
design session can look at the original.

**Readiness and failure semantics**

- **Enumerated trigger rules.** Airflow's `trigger_rule` vocabulary
  (`all_success` default, `all_failed`, `all_done`, `one_success`,
  `one_failed`, `none_failed`, `none_failed_min_one_success`, `always`) is
  the most complete statement of "when does a node become ready given
  upstream statuses." Argo's enhanced `depends: "A && (B.Succeeded ||
  C.Failed)"` and GHA's `if: always() | failure()` are the same idea. Hadron
  needs this precisely because of the notify/cleanup case: the default must
  be "skip when an upstream failed" (Nanite's skip cone), and a node that
  wants to run *on* failure must opt out of that default explicitly. Adopt a
  `ready_when` (or status-qualified `needs`) with Airflow's names; `if:` is
  evaluated only after readiness.
- **`finally` nodes.** Tekton's `finally:` tasks run after the graph
  regardless of outcome with access to every node's status; Airflow's
  setup/teardown pairs scope the same idea to a group. Hadron's
  `hooks.after_run` is a command, not a node; make it nodes so they get
  retries, outputs, and events.
- **Failed vs. crashed.** Prefect distinguishes `Failed` (the task returned
  failure) from `Crashed` (infrastructure/executor error). Nanite already
  behaves this way (`Err != nil` aborts the run; `IsError` skips the cone)
  without naming it. Name both states; recovery policy differs.
- **Tolerated failure on fan-out.** Step Functions Distributed Map has
  `ToleratedFailurePercentage`/`ToleratedFailureCount`. For bulk operations
  this is the natural contract: `for_each.tolerate: {failures: 10%}` fails
  the batch only past the threshold and otherwise collects failures as data.
- **Inferred data edges.** Prefect and Dagster infer dependencies from value
  references; Nanite requires `depends_on` to be declared and errors
  otherwise. With typed `steps.X.outputs.y` references the compiler can
  infer the edge, so `needs:` is only written for ordering-only
  dependencies. Visibility scope becomes explicit ∪ inferred, which keeps
  Nanite's "you can only read what you depend on" rule while removing the
  boilerplate.

**Waits, wakes, and durability**

- **Resume URLs unify every wait.** n8n's Wait node exposes
  `$execution.resumeUrl`; Windmill's suspend step issues approval/resume
  URLs. Hadron's `triggers` table already has `one_shot` and
  `ttl_expires_at` (migration 0006). A suspended node can *be* a one-shot,
  TTL-bounded webhook trigger bound to that node: human-gate submit, message
  arrival, and external callbacks all become "a trigger fires into a
  suspended node." This collapses three wake mechanisms into one that
  already exists.
- **Waits live outside the worker pool.** Airflow moved sensors from `poke`
  (hold the slot) to `reschedule` and then to deferrable operators with a
  separate *triggerer* process that holds thousands of waits cheaply;
  Prefect distinguishes `pause` (keeps infrastructure) from `suspend`
  (releases it). This is the precedent for §6.7 and the vocabulary to use.
- **Step journaling for replay.** DBOS, Inngest, Restate, and Trigger.dev
  implement durable execution by journaling each step's result keyed by
  (run, step, attempt) and returning journaled results on replay. It is the
  cheapest correct implementation of §6.7 and of replay-from-node, and it
  is also how the Workflow tool that produced this document resumes runs.
- **Signals, queries, updates.** Temporal lets the outside world signal a
  running workflow with data, query its state read-only, and update it.
  `message_wait` and `human_gate` are signals in disguise; generalize to
  `wait_for: {signal: <name>}` with a typed payload, and treat run status
  endpoints as queries.
- **Parent-close policy.** Temporal's child workflows declare what happens
  when the parent closes (`terminate`, `abandon`, `request_cancel`). Hadron's
  `async` stages and `agent_launch` need this: `call.on_parent_cancel:
  cancel | abandon`.
- **Timeout taxonomy.** Temporal's schedule-to-start, start-to-close,
  schedule-to-close, and heartbeat timeouts are distinct for a reason. Once
  runs can be queued or suspended, "how long may this wait before it is
  stale" (schedule-to-start) is a different question from "how long may it
  execute." Heartbeats matter for `agent_launch` and long `cmd` steps.
- **Continue-as-new.** Long-lived reactors (`on: message`) accumulate
  unbounded history; Temporal bounds it by rolling into a fresh run. Apply
  the same to Hadron's `run_events` for long-lived runs.
- **Durability modes.** Step Functions Express (short, high-volume, no
  durable history) vs. Standard (durable, long). A blueprint with no waits
  and only `read`/`compute` effects can run without per-node persistence —
  which is also the compiled-binary mode. `durability: none | steps` falls
  out of effect classification.

**Activation and concurrency**

- **Cron overlap and missed-fire vocabulary.** Kubernetes/Argo CronWorkflow:
  `concurrencyPolicy: Allow | Forbid | Replace` and
  `startingDeadlineSeconds`; Airflow: `catchup`. This answers the direction
  document's overlap/missed-fire question with standard names. GHA
  concurrency groups with `cancel-in-progress` and GitLab `interruptible`
  are `Replace` at the trigger level.
- **Run ID reuse policy.** Temporal's workflow-ID reuse policy (reject,
  allow duplicate, terminate existing) is the activation-deduplication
  contract the direction document asks for; pair it with a run-level
  `idempotency_key`.
- **Named concurrency resources.** Argo `synchronization` (mutex/semaphore),
  GitLab `resource_group`, Airflow pools with priority weights — all the
  same thing as §6.6's concurrency keys; Airflow's priority weights are the
  part the sketch lacks.
- **Static matrices.** GHA `strategy.matrix` with `include`/`exclude`,
  `fail-fast`, `max-parallel` is compile-time fan-out over a product; keep
  it as sugar over `for_each`.
- **Daemon steps.** Argo `daemon: true` keeps a step running as a service
  for its siblings and tears it down at the end — "start the dev server,
  run the tests, stop it" as one blueprint.
- **Event bindings.** Argo `WorkflowEventBinding` and Airflow datasets
  (data-aware scheduling) are the precedent for `on:` blocks and for
  cross-blueprint event edges.

**Values, outputs, and security**

- **Log-line outputs are an injection vector.** GitHub deprecated
  `::set-output` and `::save-state` in October 2022 because anything a step
  prints can set outputs. Hadron's `captureStageOutputs` scans *every* log
  line of *every* event for `::set-output`, and stage `if:` conditions
  consume those outputs — so a `cmd` that prints attacker-influenced text
  can steer control flow. This is a concrete reason, beyond cleanliness, for
  §6.5's typed executor returns, and it belongs in the safety review.
- **Secret masking.** GHA masks known secret values in the log stream.
  Whatever the direction document's reference/redact model resolves, masking
  resolved values in `run_events` and PTY output is the minimum.
- **Two output kinds.** Argo parameters vs. artifacts, Tekton's hard size cap
  on `results`, Airflow XCom with pluggable backends for large values, n8n's
  binary data manager: every mature system splits small inline values from
  referenced artifacts. Make the split explicit in the value model rather
  than discovering it through size bugs.
- **Memoization by key.** Argo `memoize: {key, maxAge}`, GHA `hashFiles`
  cache keys, Prefect `cache_key_fn`. With effect classes, `read`/`compute`
  nodes are safely memoizable by content key; this is a large win for the
  scaffold and agent-dev-loop use cases.
- **Pinned data.** n8n lets you pin a node's output so downstream
  development runs against recorded data without executing it. This is the
  contract-test and replay story from a different angle:
  `hadron run --pin steps.fetch=<file>`.

**Composition and calling**

- **Typed API calls from specs.** Serverless Workflow defines functions by
  `openapi` operation, `asyncapi`, `grpc`, and `graphql`; MCP gives the same
  for tools. An `openapi_call: {spec, operation}` node derives argument and
  output schemas from the spec the way `mcp_call` does from tool
  annotations, and removes most hand-written `http_call` plumbing in a glue
  language.
- **Definition resolvers.** Tekton resolvers fetch a task by reference type
  (git repo + path + revision, OCI bundle, in-cluster, http, hub); Tekton
  Chains signs and verifies definitions. This is the direction document's
  `DefinitionRef` authority and trust classification with working
  precedents, including pinning by digest.
- **Dynamic graphs by generation, not mutation.** GitLab dynamic child
  pipelines have a job emit YAML that runs as a child pipeline. This is the
  sanctioned shape for "the plan changes": generate a child definition,
  validate it, `call` it. It is also the runtime form of the
  agent-builds-tools flywheel, and it is why Nanite's loop engine is a peer
  of the DAG rather than a mutation of it.
- **Environment-bound gates.** GHA environments with required reviewers bind
  a gate to a named environment whose approver list is policy, not
  definition content — the direction document's "Hadron enforces a supplied
  authority policy" with a concrete shape: `human_gate.environment:
  production`.
- **Non-blocking manual steps.** GitLab `when: manual` + `allow_failure:
  true` is a gate that does not block the graph: run the extra step if a
  human triggers it, otherwise proceed. A small variant of `human_gate`
  worth having.
- **Error as data on a second output.** n8n nodes have a success output and
  an error output; Step Functions `Catch` routes to a state. Rather than
  only `on_error: {node}`, expose `steps.X.error` and for_each failures as
  values so error handling composes like any other data flow.
- **Global failure handler.** n8n's error workflow runs on any failed
  execution in the workspace — `settings.on_run_failed: <blueprint>` for
  alerting without editing every definition.
- **Script schema inference.** Windmill derives a script's input schema from
  its function signature and then exposes the script as a webhook, form,
  and CLI automatically. The `script` node should infer its input/output
  schema the same way so scripts are first-class tools without a separate
  declaration.

### 5.2 Patterns consciously rejected

- **A single mutable state document** (Step Functions `ResultPath` /
  `OutputPath` JSONPath plumbing). Per-node outputs are easier to reason
  about under parallelism and match Argo, Nanite, and the DAG model.
- **Implicit per-item execution** (n8n's items model, where every node runs
  once per incoming item). Explicit `for_each` is a simpler mental model and
  agents reason about it more reliably.
- **jq as the expression language** (Serverless Workflow). Powerful but
  opaque; expr-lang reads like code and has the data built-ins.
- **Two composition mechanisms** (GHA reusable workflows vs. composite
  actions). They exist only because of the job/step split; one `call`
  suffices.
- **Live patching of in-flight definitions** (Temporal worker versioning).
  Runs bind to an immutable plan digest and finish on it.
- **Level barriers** (GitLab stages, Hadron and Nanite today). GitLab's own
  `needs` documentation sells it as letting jobs run "out of stage order";
  the ready queue is the settled answer.
- **Log-line output capture** (`::set-output`, GitLab dotenv reports). Every
  system that started here replaced it, and GitHub did so for security.

---

## 6. Target shape

### 6.1 Thesis

Every stated target — a glue language for purpose-built tools, n8n-style
workflows, agent DAGs, a fluent front-end, compiled standalone tools — is the
same artifact: **a typed dataflow graph with a durable, observable executor
and many front doors.** n8n is dataflow where items flow on edges. Agent
workflows are dataflow where some nodes are model turns. A fluent DSL is
dataflow with chaining syntax. Compiling to a tool is serializing the graph
plus a runtime. Hadron already owns the daemon, the doors, the audit trail,
and the registry. What it lacks is the graph-with-values core.

### 6.2 Layers

```text
Front-ends   YAML blueprint | YAML pipeline (sugar) | desktop canvas | fluent DSL (later) | Go/TS SDK | agent-emitted IR
                  │ parse
IR           Graph{ inputs, outputs, on[], nodes[ id, kind, config, needs, if, for_each, concurrency,
                    retry, timeout, effects, outputs_schema, verify ] }
                  │ compile: validate · resolve imports → digests · toposort · effect analysis
Plan         immutable, digest-addressed (direction doc: ExecutionPlan)
                  │ bind: inputs · execution target · policy/grants · adapters · caller
Run          ready-queue scheduler · durable node state · suspend/resume · typed values on edges
                  │ executor registry (StepKind → executor)
Kinds        cmd | http | mcp | llm | agent_launch | message_wait | human_gate | call | script | transform | sleep | wait_for
                  │ exposure
Doors        CLI | HTTP | MCP (profiled) | A2A skill | compiled binary | compiled MCP server
```

This is the direction document's Build/Release/Run (source → ExecutionPlan →
BoundRun) with the Run layer made concrete. Each layer compounds the one
below it: values enable expressions, expressions enable `for_each` and real
conditionals, node state enables durable waits and replay, declared outputs
enable tools and compilation.

### 6.3 One definition kind

Blueprint and pipeline compile to one IR. A `section` is an implicit
sequential chain (each step `needs` the previous unless it declares its own
`needs`), so existing blueprints keep their semantics. A pipeline `stage` is
a `call` node with `mode: run` (own run record, as today) and its
`depends_on`, `if`, `inputs`, `outputs`, `position`, and `async` map
one-to-one. The `pipeline.yaml` loader remains as authoring sugar; no `kind:`
field is needed, though one may be accepted as a lint/profile hint.

```yaml
- name: deploy
  call: deploy.yaml          # or a registry name, or name@digest
  mode: run                  # inline (default) | run (own run record, events, cancel handle)
  target: local              # later: a Cerberus execution target
  with: { build_version: steps.build.outputs.version }
  needs: [lint, test-unit]
```

### 6.4 Node kinds

| Kind | Today | Target notes |
|---|---|---|
| `cmd` | yes | capture stdout/stderr/exit_code as outputs; optional `parse: json|lines|kv`; keep PTY streaming as events |
| `http_call` | yes | outputs already structured (`status_code`, `body_json`); drop local-only restriction behind policy |
| `mcp_call` | yes | outputs = tool result; effects inferred from MCP annotations |
| `llm` | no | adopt Nanite's contract: provider/model/system/prompt/tools (restricted set)/max_iterations/`output_schema` → structured JSON; `verify` modifier |
| `agent_launch` | yes | keep as the heavy primitive; add `wait: true` sugar that composes launch + message_wait on the returned mailbox |
| `message_wait` | yes | becomes a durable suspend with wake from the message substrate |
| `human_gate` | yes | becomes a durable suspend with wake from `gate submit` |
| `call` | yes | returns child `outputs`; `mode: inline|run`; `target:` |
| `script` | no | sandboxed in-process JS via goja (Python via subprocess later) with a `hadron` object: `hadron.mcp(...)`, `hadron.http(...)`; the escape hatch n8n's Code node provides |
| `transform` | no | pure expression node producing outputs; no effects; the data-munging primitive |
| `sleep` | no | durable timer |
| `wait_for` | no | durable suspend until a trigger/event/run completes (`wait_for: {run: steps.deploy.outputs.run_id}` for async calls) |

Modifiers on any node: `needs`, `if`, `for_each` (+`concurrency`,
`item`/`index` scope, collected `items` output), `retry` (policy object),
`timeout`, `continue_on_error`, `on_error` (route to a node with the error in
scope), `effects`, `outputs` (schema), `verify`, `concurrency_key`.

### 6.5 Values and expressions

- Executors return `StepResult{Status, Outputs map[string]any, Error,
  Attempts, ...}`; `::set-output` emission is removed (a compatibility shim
  may parse it from `cmd` output if `parse: set-output` is requested).
- The expression context: `inputs`, `steps.<id>.{outputs, status, error,
  items}`, `item`/`index` inside `for_each`, `run`, `env` (subject to the
  direction document's secret-handling rules), `workspace`.
- Visibility is scoped to declared `needs` (adopted from Nanite); an
  unresolvable reference fails at compile time where statically detectable
  and at run time otherwise — never silently.
- **Expression language candidates.** `expr-lang/expr` (Go-native,
  sandboxed, non-Turing-complete, typed, rich built-ins — `filter`, `map`,
  `groupBy`, `sum`, pipes; used by Argo for `{{= }}`) or CEL (stricter
  typing, cost limits; Kubernetes' choice). Leaning expr for a glue language
  because data-munging built-ins matter more than admission-policy rigor.
  Decision belongs to the design session. `{{ }}` Go templates remain for
  string interpolation only.

### 6.6 Scheduler

Replace level barriers with a ready queue: compute in-degrees once; enqueue
all zero-in-degree nodes; when a node reaches a terminal state, decrement
each dependent's in-degree and enqueue at zero. Nodes run on a bounded pool
shared across runs, with per-key semaphores for external resource limits
(`concurrency_key: torque-api`, `max: 4`, configured in settings). A
`for_each` body is expanded into N node invocations at readiness time, not at
compile time, so the plan stays static while the run is dynamic.

### 6.7 Durable state, suspend, resume, replay

- A `run_steps` (node invocation) table: `run_id, node_id, iteration,
  attempt, status, inputs_ref, outputs_ref, error, started_at, ended_at,
  wake_on`. Small values inline; large values by reference with size caps
  (direction document retention rules).
- Waits (`human_gate`, `message_wait`, `sleep`, `wait_for`) persist a
  `waiting` state with a `wake_on` descriptor and release the worker. A wake
  is an activation: gate submit, message arrival, timer, trigger, or child
  run terminal state re-enqueues the run. Hadron already has every wake
  source as a daemon subsystem; this unifies them behind one "resume run"
  path instead of the polling loops in §2.4.
- Recovery: on daemon start, runs with non-terminal state are re-driven from
  `run_steps`; a node found `running` is re-run only if its effects permit it
  (`read`/`compute` freely; `mutate` with an idempotency key; `destructive`
  requires a decision). This is stronger than Nanite's at-least-once and
  falls directly out of effect classification.
- Replay: `hadron rerun <run> --from <node>` reuses persisted upstream
  outputs and re-executes from the named node. This is the agent development
  loop's most valuable feature and costs nothing extra once outputs are
  durable.

### 6.8 Control flow and errors

- `if` and `switch` (first matching arm routes to nodes) via the expression
  language.
- `on_error: {node: cleanup}` with `error` in scope; `continue_on_error`
  retained.
- `retry` as a policy object `{attempts, backoff, max_delay, on: [timeout,
  5xx, ...], idempotency_key}`; the compiler warns or refuses when a retry
  policy is attached to a `mutate`/`destructive` node without an idempotency
  key.
- Effects `read | compute | materialize | mutate | destructive` on nodes and
  rolled up to the definition (direction document). These drive MCP
  annotations, dry-run truthfulness, retry safety, recovery policy, and
  agent-facing confirmation policy.

### 6.9 Triggers as authoring sugar

```yaml
on:
  webhook: { path: /torque/bulk-create, extract: { tasks: body.tasks } }
  schedule: "0 6 * * *"
  message: { to: msg://agent/hadron/bulk-create }
```

Registering a blueprint materializes these as activation registrations
(direction document §Activation registration). The durable rows remain
Hadron-owned operational state; the `on:` block is the declarative source.

### 6.10 Composition

`call` returns the child's declared `outputs`, so blueprints compose like
functions; depth is bounded (`maxCallDepth` exists). Children are resolved
by path, registry name, or `name@digest`, and the resolved digest is recorded
on the run (direction document §DefinitionRef). Imports with `with:` defaults
remain as partial application.

---

## 7. Exposure model: selective MCP tools

### 7.1 The flooding problem

Auto-registering every indexed blueprint as an MCP tool was tried and
removed: every tool definition costs the calling agent context on every
turn, and a registry of a few hundred blueprints makes the agent worse at
everything else. The current state is the other extreme — agents see only
`hadron_*` meta-tools and must `hadron_blueprint_search` → read the input
schema → `hadron_run_enqueue` by path → poll `hadron_run_events`. That
indirection has no type checking at the tool boundary and no output
contract. Neither extreme is the target.

### 7.2 Three tiers

| Tier | What the agent sees | Who decides |
|---|---|---|
| **Pinned** | first-class MCP tools with input/output schemas, named `<namespace>_<slug>` | the caller's exposure profile (§7.3) |
| **Discoverable** | one meta-tool pair: `hadron_tools_search(query) → names + one-liners`, `hadron_tools_load(names[]) → schemas`, after which the loaded tools become session-scoped first-class tools | the profile's search scope |
| **Hidden** | nothing | other workspaces, effects the profile denies, unlisted namespaces |

The discoverable tier is the deferred-tool pattern (names visible, schemas on
demand) and it is what MCP supports natively: the server advertises
`tools.listChanged`, adds tools per session, and sends
`notifications/tools/list_changed`. The `mark3labs/mcp-go` dependency already
has per-session tool registration.

### 7.3 Profiles and namespaces

- A blueprint declares a `namespace` (or inherits one from the registry
  index root it was found under). Namespaces are the unit of "show me these
  by default."
- An application or agent registers an **exposure profile** with Hadron,
  bound to its principal. Existing hooks: the MCP adapter already takes a
  bearer `token` and `scopes` (`adapter.go:154`); triggers already record
  `created_by` (`tools.go:1262`) and support `trigger_list_mine`; workspaces
  already scope runs. The missing piece is that the adapter's `sessionID` is
  a timestamp (`adapter.go:172`) rather than a resolved principal.

  ```yaml
  profile: nanite-reviewer
  namespaces: [torque, git]          # pinned by default
  pin: [release/cut-release]          # explicit additions
  deny_effects: [destructive]         # never direct; requires a grant
  max_direct_tools: 24                # compile-time budget; refuse beyond
  search_scope: public                # public | namespaces | all
  lazy_load: true
  ```

- **An agent's own namespace.** Blueprints an agent authored for itself
  (the flywheel, §8) live in that agent's namespace and are pinned by
  default. The tools an agent built for itself are always on its belt.
- **Default profile = meta-tools only.** Today's behaviour remains the
  safe default for an unknown caller.
- Tool schemas are derived from `inputs`/`outputs` and kept compact
  (one-line descriptions, no prose). A catalog resource
  (`hadron://blueprints/<namespace>`) lets an agent browse cheaply without
  tool-schema cost. Tether's MCP gateway may own cross-application profile
  policy later (direction document §MCP); Hadron must still work without it.

### 7.4 Other doors

- **A2A** already maps blueprints to skills; declared outputs complete the
  contract. Task-to-run correlation should move to the persisted run model
  (direction document notes it is in memory).
- **Compiled binary.** `hadron build <bp> -o <name>` embeds the compiled plan
  in a daemon-less runtime (in-memory store, no SQLite): CLI flags from
  `inputs`, JSON on stdout from `outputs`. `--as mcp-server` emits a stdio MCP
  server exposing the blueprint as one tool. Node kinds that need daemon
  services (`human_gate`, `message_wait`, `agent_launch`) are either refused
  at build time or bound to a remote daemon. Go makes this nearly free and
  it is the most distinctive door Hadron can offer.
- **Desktop canvas.** Positions move from stages to nodes; once edges carry
  typed values, any historical run can show what flowed on each edge.

---

## 8. Further ideas surfaced in this session

- **Agent-builds-tools flywheel.** Missing capability → search registry →
  draft blueprint → `validate` + `test-gen` + contract tests
  (`tests: [{inputs, expect_outputs}]` with mocked executors) → register
  under the agent's namespace → pinned for that agent, discoverable for
  others, pinned by digest. Hadron is the only portfolio component that owns
  validation, registry, execution, and exposure together, so it is the only
  place this loop closes. `hadron_blueprint_broker` is a seed.
- **Effect-driven policy.** `read`-only tools may be callable by agents
  without confirmation; `mutate` requires a profile grant; `destructive`
  requires a human gate or explicit policy. `hadron explain` prints the plan
  and its blast radius before a run.
- **Concurrency keys across runs.** Rate limits to external systems are
  enforced once, in the daemon, rather than per blueprint.
- **Structured `cmd` outputs.** `parse: json` turns any CLI into a typed node
  without a wrapper.
- **`llm` as a typed function.** With `output_schema` enforced and `verify`
  available, classification, extraction, routing, and judging become ordinary
  nodes; the heavy `agent_launch` is reserved for open-ended work.
- **Reactors.** A blueprint with `on: message` and a durable `message_wait`
  is a long-lived event consumer — the bridge to Nanite's reflexes without
  either application owning the other.

---

## 9. Relationship to Nanite and extraction

Nanite's engine is a by-hand fork of Hadron's pipeline runner and will keep
drifting. The portfolio practice is explore-by-duplication, then extract the
settled shape into a shared Go library. Given D1 ("Hadron is NOT the dispatch
primitive"), the extraction is not "Nanite calls Hadron"; it is a library
both consume.

| Shared library (candidate) | Hadron keeps | Nanite keeps |
|---|---|---|
| graph IR + validation + toposort | daemon, persistence, run events | harness, sessions, context assembly |
| ready-queue scheduler + suspend/resume contract | cmd/http/mcp executors, triggers, registry, pack | llm/tool/flex executors over its tool broker |
| expression engine + templating rules | exposure profiles, A2A, compile | loop engine, teams, reflexes |
| `StepKind` registry interface (executor plugin shape) | | |
| node state model (`run_steps`) as an interface | SQLite impl | its store impl |

Nanite's `StepExecutor` and Hadron's `MCPCaller`/`MessageSource`/`AgentLauncher`
are already the same plugin shape; a shared `StepKind` registry unifies them.
Until the shape settles, both engines stay in their applications; the
extraction is queued behind the portfolio-wide library audit.

---

## 10. Decisions

These are directional decisions made in this session; each is a candidate
ADR once the design session confirms it.

1. **One definition kind.** Blueprint and pipeline compile to a single graph
   IR. A pipeline stage is a `call` node with `mode: run`; `pipeline.yaml`
   stays as sugar. *Rationale:* prior art (§5) shows multi-level models exist
   only at isolation or determinism boundaries; Hadron's only difference is
   run identity, which is a property of the call.
2. **Engine core first, inside-out.** Values → expressions → `for_each` and
   conditionals → ready-queue scheduler → durable node state and waits →
   `llm`/`script`/`transform` → doors. *Rationale:* each layer unlocks the
   next; the doors are mechanical once the core exists.
3. **No new language.** A fluent DSL, if ever built, is a view over the graph
   IR, not a programming language; the IR with a schema and validator is the
   contract agents program against. *Rationale:* syntax is not where the value
   is, and a standalone language competes with tools that already exist. The
   DSL interest may be pursued separately.
4. **Explore by duplication now, extract later.** Hadron and Nanite keep their
   engines until the shape settles; the shared library is a later extraction,
   sequenced with the portfolio library audit.
5. **Selective MCP exposure.** Default is meta-tools only. Direct tools come
   from a per-principal exposure profile over namespaces with an explicit
   budget; everything else is discoverable via search + lazy per-session
   loading; denied effects are hidden. *Rationale:* auto-exposure flooded
   agent context (§7.1).
6. **Adopt from Nanite:** the `llm` step contract with capability-restricted
   tools and `ToolCallRecord`, `verify` as a modifier, fail-hard on
   unresolvable references, visibility scoped to declared dependencies, and
   the suspend/resume pattern.
7. **Acceptance test.** The Torque bulk-create blueprint (§4) running end to
   end and callable as a pinned MCP tool is the definition of "the engine
   core exists." It is a validation case, not a capability Torque lacks;
   if Torque's own bulk create exists by then, the blueprint still runs
   against `torque_task_create` as the test.
9. **Adopt from prior art (§5.1), pending design-session confirmation:**
   Airflow-style readiness rules with skip-on-upstream-failure as the
   default; `finally` nodes; failed vs. crashed states; tolerated-failure
   thresholds on `for_each`; inferred data edges with explicit `needs` for
   ordering only; suspended nodes implemented as one-shot TTL triggers with
   resume URLs; step journaling as the replay mechanism; the
   Allow/Forbid/Replace + starting-deadline vocabulary for schedules; an
   explicit small-value/artifact split; memoization by key for
   `read`/`compute` nodes; secret masking in the event stream.
8. **Boundaries unchanged.** Nothing here supersedes the direction
   document's ownership rules; effect classification and DefinitionRef from
   that document become node- and run-level facts in this design.

Leaning, to confirm in the design session: expr-lang for expressions with
`{{ }}` retained for string interpolation; goja for the `script` node.

---

## 11. Open questions for the design session

1. Exact IR schema: node identity, how `section` and `stage` lower, naming of
   the definition kind (branding is a separate session).
2. Value model: inline size cap, reference type for large outputs, retention
   and redaction per value class (direction document §Observability).
3. Scheduler: pool sizing, per-key semaphores, priority and fairness across
   runs, `for_each` expansion limits.
4. Suspend/resume: wake routing from each source (gate, message, timer,
   trigger, child run) to a run; exactly-once vs. at-least-once per effect
   class; idempotency key semantics.
5. `llm` node: bind providers through go-providers directly or through
   agentkit; where the restricted tool set comes from (Hadron's configured
   MCP servers, registry blueprints, both); `output_schema` enforcement and
   repair policy.
6. `script` node: goja capability surface of the `hadron` object, time and
   memory limits, whether Python is in scope.
7. Expression language: expr vs. CEL; the function library; whether `env`
   and `readFile` survive given the direction document's secret rules.
8. Principal model: token → principal → profile; where profiles live (Hadron
   settings, workspace, Tether); how an agent claims a namespace.
9. Compiled binary: which node kinds are compilable offline; how `mcp_call`
   binds servers and `llm` binds providers in a standalone build.
10. Migration: compatibility loaders for existing blueprints and pipelines;
    what the `::set-output` shim supports; removal of scaffolding fields.
11. Canvas: node-level positions and edge value display.
12. Extraction boundary: what the shared library's public API is, and which
    of Hadron's and Nanite's tests become its conformance suite.

---

## 12. Follow-up candidates

- Portfolio-wide audit of applications for extractable libraries (already
  planned); the engine core is one entry.
- Check whether Torque's bulk create has landed (it was requested with the
  other bulk/batch changes). Either way the §4 blueprint is written as a
  validation case against `torque_task_create`.
- Security review item: the `::set-output` log-line scan in
  `captureStageOutputs` is an output/control-flow injection vector (§2.3,
  §5.1); the typed-executor-returns work removes it, but it should be
  tracked in `docs/safety.md` until then.
- Small fixes independent of the redesign: pipeline stage wait default of
  60 s; the `on_success: step` stub; MCP adapter `sessionID` as a real
  principal; polling intervals.
- Evaluate `hadron_blueprint_broker` against the flywheel idea.
- Decide whether Nanite adopts retry/`continue_on_error`/conditionals now or
  waits for the shared library.

## 13. Known limitations of this exploration

- Findings are from reading HEAD, not from running or benchmarking it.
- `internal/mcpadapter/tools.go` (~3.6k lines) and the desktop frontend were
  sampled, not read fully.
- Nanite was mapped by a sub-agent against its HEAD; `internal/loop` was read
  at the engine/decision layer only.
- Prior-art details are from general knowledge of those systems, not from
  re-reading their specifications in this session.

---

## Appendix A — File reference index

Hadron (`apps/hadron`):

- `internal/blueprint/blueprint.go` — model `:20-33`, `Step` `:269-292`,
  one-kind rule `:646-657`, `BuildTemplateContext` `:875`,
  `RenderForExecution` `:953`, template funcs `:1178-1236`
- `internal/execution/lifecycle.go` — `NewManager` `:15-34`, `Cancel` `:86`
- `internal/execution/manager.go` — `worker` `:25-69`, loop `:142-195`,
  `runStep` `:206`, `execCmd` `:311-353`, hooks `:381-419`, `call` `:441-460`,
  `evaluateCondition` `:559-575`
- `internal/execution/{http_call,mcp_call,agent_launch,human_gate,message_wait}.go`
- `internal/execution/types.go` — `RunStore`, `MCPCaller`, `MessageSource`,
  `AgentLauncher`, `Request`, `Manager`
- `internal/pipeline/pipeline.go` — `Spec`/`Stage` `:15-47`, `Validate`,
  `StageWaitTimeout`
- `internal/pipeline/toposort.go` — Kahn levels
- `internal/pipeline/runner.go` — `execute`, `executeStage`,
  `captureStageOutputs`, `resolveTemplate`, `evaluateIfTemplate`,
  `waitForRunTerminal`
- `internal/agentsubstrate/launcher.go` — `LaunchAgent` `:89-178`
- `internal/persistence/migrations/0001..0013`
- `internal/mcpadapter/adapter.go` — `New` `:154`, `sessionID` `:172`;
  `server_surface.go:119-121` annotations; `tools.go:1262,1322` `created_by`
- `internal/agentcard/agentcard.go:78` — `SkillFromBlueprint`
- `examples/agentic-launch-and-wait.yaml`, `examples/pipeline-v2-dag/pipeline.yaml`

Nanite (`apps/nanite`):

- `internal/agentworkflow/{types,interfaces,dag,validate,registry,definition_yaml}.go`
- `internal/service/workflow_engine.go` — scheduler `:270-403`, templating
  `:773-888`, `Resume` `:156-230`
- `internal/service/workflow_step_executor.go` — turn loop `:205-297`, tool
  restriction `:349-372`, verify `:399-459`
- `internal/service/workflow_engine_{loop,flex}.go`, `team_compiler.go:104-178`
- `internal/loop/{engine,decide}.go`
- `internal/store/workflow_runs.go`
- `internal/scheduler/runner_adapter.go:43-49`
- `internal/background/types.go:20` (D1)

## Appendix B — Existing example rewritten in the target form

`examples/agentic-launch-and-wait.yaml` today pairs `agent_launch` with a
`message_wait` and a hand-maintained correlation ID. In the target form:

```yaml
blueprint:
  name: agentic-launch-and-wait
  namespace: examples

steps:
  - name: review
    agent_launch:
      substrate: local_runtime
      logical_agent_id: reviewer-1
      prompt_append: Read the injected instructions and reply on your mailbox when done.
      injection:
        native_files:
          - rel_path: context/reply-contract.md
            source: "{{ inputs.contract }}"
    wait: { timeout: 30m }        # sugar: message_wait on steps.review.outputs.mailbox with the run's correlation id

  - name: triage
    needs: [review]
    llm:
      model: default
      prompt: |
        Classify this review as one of: approve, request_changes, escalate.
        Review: {{ steps.review.outputs.reply.body }}
      output_schema: { type: object, properties: { verdict: { enum: [approve, request_changes, escalate] } } }
    verify: { mode: engine, check: no_error }

  - name: escalate
    needs: [triage]
    if: steps.triage.outputs.verdict == "escalate"
    human_gate:
      prompt: "Reviewer escalated. Proceed?"
      options: [{ id: yes, label: Yes }, { id: no, label: No }]
      timeout: 24h

outputs:
  verdict: steps.triage.outputs.verdict
  decision: steps.escalate.outputs.decision
```
