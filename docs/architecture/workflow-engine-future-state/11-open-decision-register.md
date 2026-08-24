# 11 - Open Decision Register

**Status:** placeholders for architecture iteration
**Scope:** decisions needed before implementation planning

These are not accepted ADRs. They are the open choices surfaced while expanding
the desired future state into architecture docs.

## D01 - Public Engine Package Boundary

**Question:** What module/repository contains the reusable engine core?

**Options:**

- in this repository first, then extract;
- new shared repo, for example `hollis-labs/go-workflow`;
- package family under a Hadron-named module with app packages kept separate.

**Decision:** start inside Hadron if that is the practical first step, but keep
the engine boundary extraction-ready for a shared module such as
`github.com/hollis-labs/go-workflow`. The core must not import Hadron
`internal/*`.

**Impacts:** [01](01-package-boundaries.md),
[09](09-consumer-boundaries-extraction.md).

## D02 - Core Dependency Allowlist

**Question:** Which non-standard dependencies may engine core import?

**Options:**

- standard library plus schema/expression libraries only;
- allow selected Hollis Labs primitive libraries;
- allow broader adapter libraries in core.

**Decision:** core imports stay narrow: stdlib plus selected schema,
expression, and test dependencies. Concrete provider, MCP, daemon, app, and
storage dependencies stay out of core.

**Impacts:** [01](01-package-boundaries.md),
[06](06-step-kinds-executors.md).

## D03 - Exact Graph IR Schema

**Question:** What is the canonical graph IR schema and persisted plan format?

**Options:**

- Go structs with generated JSON Schema;
- hand-written JSON Schema as primary contract;
- protobuf or another IDL.

**Decision:** Go structs are the source of truth, with generated JSON Schema
for validation, UI, agents, and serialized plans.

**Impacts:** [02](02-graph-ir-source-formats.md).

## D04 - Public Source Kinds

**Question:** Do blueprint and pipeline remain separate public authoring kinds?

**Options:**

- keep both as source formats indefinitely;
- converge authoring syntax around one `workflow` format;
- keep compatibility loaders but make new authoring one format only.

**Decision:** treat the workflow language as greenfield. Current blueprint and
pipeline files have no external consumers and should be archived/reference
material, not compatibility constraints. The target has one graph-native public
source format over the IR.

**Impacts:** [02](02-graph-ir-source-formats.md),
[10](10-migration-safety-compatibility.md).

## D05 - Source Map Persistence

**Question:** Are source maps persisted on `ExecutionPlan`, run records, or only
produced during validation?

**Options:**

- persist full source map with plan;
- persist compact source references on node invocations;
- keep source maps in compile artifacts only.

**Decision:** persist source maps on `ExecutionPlan`; node invocations store
compact references into the plan-level source map.

**Impacts:** [02](02-graph-ir-source-formats.md),
[07](07-activation-run-binding.md).

## D06 - Expression Language

**Question:** Which expression language powers `if`, `for_each`, transforms,
and output bindings?

**Options:**

- `expr-lang/expr`;
- CEL;
- constrained custom evaluator.

**Decision:** use `expr-lang/expr`, with `{{ }}` retained only for string
interpolation.

**Impacts:** [03](03-values-expressions-artifacts.md),
[04](04-execution-state-scheduler.md).

## D07 - Value And Artifact Envelope

**Question:** What is the exact envelope for inline values, artifact references,
redaction, retention, digests, and producer metadata?

**Options:**

- one `Value` envelope for inline and artifact values;
- separate `Value` and `Artifact` tables/interfaces;
- defer artifact store until typed inline values exist.

**Decision:** use one `Value` envelope for inline values and artifact
references. Small JSON-compatible values are inline; large, binary, sensitive,
or long-lived values use `ArtifactRef`. Every value carries producer metadata,
media type, retention class, redaction class, and digest where applicable.

**Impacts:** [03](03-values-expressions-artifacts.md),
[04](04-execution-state-scheduler.md),
[10](10-migration-safety-compatibility.md).

## D08 - State Store Interface

**Question:** What state-store contract does the runtime require?

**Options:**

- high-level run/node/value methods;
- event-sourced journal with projections;
- SQL-shaped interfaces close to current Hadron persistence.

**Decision:** use high-level runtime interfaces, not SQL-shaped APIs and not a
pure event-sourced core. The contract exposes runs, node invocations, attempts,
waits, values, events, and compare-and-swap claims. Hadron SQLite is one
adapter.

**Impacts:** [04](04-execution-state-scheduler.md),
[07](07-activation-run-binding.md).

## D09 - Scheduler Fairness And Priority

**Question:** How are ready nodes prioritized across runs?

**Options:**

- FIFO;
- weighted per-run fairness;
- priority queues with starvation protection;
- host-provided scheduler policy.

**Decision:** scheduler fairness is host-configurable with FIFO as the default.
The first contract supports priority and per-run fairness hooks without making
a complex scheduler mandatory on day one.

**Impacts:** [04](04-execution-state-scheduler.md).

## D10 - Readiness Vocabulary

**Question:** How does a node express readiness when upstream nodes fail,
skip, or timeout?

**Options:**

- Airflow-style `all_success`, `one_failed`, `all_done`, etc.;
- status-qualified `needs`;
- boolean expression over dependency states.

**Decision:** use Airflow-style named readiness rules. `all_success` is the
default; other rules include `all_done`, `one_failed`, `all_failed`,
`none_failed`, and `always`. `if` is evaluated only as a data predicate after
readiness is satisfied.

**Impacts:** [04](04-execution-state-scheduler.md).

## D11 - Fan-Out Failure Policy

**Question:** How does `for_each` tolerate partial failures?

**Options:**

- fail on first item failure;
- collect all failures as data, then decide downstream;
- tolerated count/percentage thresholds.

**Decision:** fan-out defaults to fail on unhandled item failure. Workflows may
explicitly tolerate failures by count or percentage. Fan-out always collects
per-item status, output, and error data for downstream handling.

**Impacts:** [04](04-execution-state-scheduler.md).

## D12 - Wait Implementation

**Question:** Are waits implemented through a dedicated wait table, one-shot
triggers, or both?

**Options:**

- generic `waits` table plus activation scheduler;
- one-shot TTL trigger rows as wait records;
- wait table with trigger rows as app-service materializations.

**Decision:** core owns a generic wait contract and `WaitRecord`. Hadron may
materialize callback wakes as one-shot TTL triggers, but one-shot triggers are
an app-service implementation detail, not the core wait model.

**Impacts:** [05](05-waits-gates-callbacks.md),
[07](07-activation-run-binding.md).

## D13 - Resume Idempotency

**Question:** What idempotency guarantee applies to gate/message/callback
resumes?

**Options:**

- exactly once by wait ID and idempotency key;
- at least once with idempotent state transitions;
- host-specific.

**Decision:** resume transitions are idempotent by `wait_id`, with an optional
caller-provided idempotency key. Duplicate resumes return the existing accepted
result or a structured already-resumed response.

**Impacts:** [05](05-waits-gates-callbacks.md).

## D14 - Gate And Checkpoint Boundary

**Question:** What package owns gate/checkpoint semantics above generic waits?

**Options:**

- engine core includes `gate`;
- shared gate/checkpoint package layered over core waits;
- each product implements gates independently over wait contracts.

**Decision:** generic wait semantics live in core. A shared gate/checkpoint
package defines prompt, options, decision, and escalation vocabulary. Product
policy and presentation stay product-owned.

**Impacts:** [05](05-waits-gates-callbacks.md),
[09](09-consumer-boundaries-extraction.md).

## D15 - Step Executor Lifecycle

**Question:** Which lifecycle methods must every step kind implement?

**Options:**

- `Validate` and `Execute` only;
- `Validate`, `Prepare`, `Execute`, `Observe`, `Cancel`, `Finalize`;
- minimal core lifecycle plus optional interfaces.

**Decision:** required lifecycle is minimal: `Spec`, `ValidateConfig`, and
`Execute`. Optional interfaces cover `Prepare`, `Observe`, `Cancel`, and
`Finalize`, advertised in step-kind metadata.

**Impacts:** [06](06-step-kinds-executors.md).

## D16 - LLM Binding

**Question:** What backs the `llm` executor?

**Options:**

- direct `go-providers`;
- `agentkit`;
- Nanite harness adapter;
- multiple bindings behind the same `llm` contract.

**Decision:** one provider-agnostic `llm` contract supports multiple executor
bindings, including direct `go-providers`, `agentkit`, and Nanite harness
adapters. Engine core imports none of those concrete dependencies.

**Impacts:** [06](06-step-kinds-executors.md),
[09](09-consumer-boundaries-extraction.md).

## D17 - Script Runtime

**Question:** Which runtime powers `script` nodes and what are the sandbox
limits?

**Options:**

- goja JavaScript only at first;
- Python subprocess;
- no script node until sandbox policy is settled.

**Decision:** use goja JavaScript first for local deterministic data
manipulation. Python is a later explicit subprocess/sandbox decision.

**Impacts:** [06](06-step-kinds-executors.md),
[10](10-migration-safety-compatibility.md).

## D18 - Activation Policies

**Question:** What vocabulary covers overlap, missed fires, catchup, and run ID
reuse?

**Options:**

- `Allow | Forbid | Replace` plus starting deadline and catchup;
- Temporal-like workflow ID reuse policies;
- custom Hadron-specific names.

**Decision:** use standard vocabulary: `Allow`, `Forbid`, `Replace`,
`starting_deadline`, `catchup`, and explicit run ID reuse policy.

**Impacts:** [07](07-activation-run-binding.md).

## D19 - Run Scope Naming

**Question:** What is the target name for Hadron's logical operational scope?

**Options:**

- use `RunScope`;
- use `ProjectScope`;
- use another product term.

**Decision:** use `RunScope` as the target architecture term. This is a clean
break; no legacy `workspace_id` support is required in the target design.
Compute workspace binding belongs to `ExecutionTarget`, never to `RunScope`.

**Impacts:** [07](07-activation-run-binding.md),
[10](10-migration-safety-compatibility.md).

## D20 - Principal And Exposure Profiles

**Question:** How does Hadron map token/session/caller to principal and
workflow exposure profile?

**Options:**

- Hadron-local principal/profile records;
- Tether-owned identity and profile policy;
- hybrid: Hadron local default, Tether optional authority.

**Decision:** principal and exposure profile records are Hadron-local by
default. A Tether policy adapter may become an optional authority later. MCP
token/session resolves to a principal, and the principal resolves to an
exposure profile.

**Impacts:** [08](08-hadron-app-service-surfaces.md).

## D21 - Compiled/Offline Subset

**Question:** Which node kinds can compile into daemon-less binaries or MCP
servers?

**Options:**

- only pure/read/compute nodes;
- allow MCP/LLM with external config bindings;
- allow waits through remote daemon binding;
- reject any daemon-service dependency.

**Decision:** support a conservative compiled/offline subset first:
pure/read/compute/materialize nodes that do not require daemon wait services.
MCP and LLM nodes are allowed only with explicit external config bindings.
Gates, messages, and callback waits require a remote daemon binding or are
rejected at build time.

**Impacts:** [08](08-hadron-app-service-surfaces.md),
[06](06-step-kinds-executors.md).

## D22 - Compatibility Profile

**Question:** What legacy behavior remains accepted and what warnings/errors
does the compiler emit?

**Options:**

- broad legacy mode with warnings;
- strict compatibility mode by default;
- target-only mode with one-time conversion tools.

**Decision:** target mode is greenfield. Current blueprint and pipeline
examples should be archived and selectively rewritten. A legacy parser/rewrite
aid is optional, not a public compatibility commitment.

**Impacts:** [10](10-migration-safety-compatibility.md).

## D23 - Secret And Redaction Contract

**Question:** What is the cross-subsystem secret reference and redaction model?

**Options:**

- `secret://authority/path#field` references with adapter resolution;
- host-specific reference schemes;
- adopt an existing external secret URI standard if one fits.

**Decision:** use opaque secret references resolved at adapter boundaries, for
example `secret://authority/path#field`. Hadron records references and
provenance, not secret material. Values and events carry redaction and
retention metadata.

**Impacts:** [03](03-values-expressions-artifacts.md),
[10](10-migration-safety-compatibility.md).

## D24 - Accepted ADR Set

**Question:** Which placeholders become ADRs before planning starts?

**Options:**

- one umbrella ADR for library-backed architecture;
- separate ADRs for package boundary, graph IR, values, scheduler, waits, and
  exposure;
- keep as architecture docs until implementation decisions are made.

**Decision:** write separate ADRs for accepted rules that constrain
implementation: package boundary, graph-native source/IR, typed values,
durable scheduler/waits, step executor registry, Hadron app-service exposure,
and greenfield/archive stance. Keep low-level schema details in architecture
docs until they harden.

Created ADRs:

- [ADR 0006: Reusable workflow engine boundary](../adr/0006-reusable-workflow-engine-boundary.md)
- [ADR 0007: Graph-native workflow source and IR](../adr/0007-graph-native-workflow-ir.md)
- [ADR 0008: Typed values and artifacts are the workflow data plane](../adr/0008-typed-values-and-artifacts.md)
- [ADR 0009: Durable ready-queue runtime and waits](../adr/0009-durable-ready-queue-runtime.md)
- [ADR 0010: Step executor registry](../adr/0010-step-executor-registry.md)
- [ADR 0011: Hadron host surfaces and exposure profiles](../adr/0011-hadron-host-surfaces-and-exposure.md)
- [ADR 0012: RunScope and ExecutionTarget are separate concepts](../adr/0012-run-scope-and-execution-target.md)

**Impacts:** all docs in this folder and [`../adr`](../adr).

## D25 - Workflow Source Name

**Question:** What is the canonical name and file convention for the new
graph-native source format?

**Decision:** name the source format `workflow`. Prefer `*.workflow.yaml` for
named workflow files and `workflow.yaml` for directory-default entrypoints.

**Impacts:** [02](02-graph-ir-source-formats.md),
[10](10-migration-safety-compatibility.md).

## D26 - Legacy Example Archive Policy

**Question:** What happens to current blueprint and pipeline examples?

**Decision:** archive current examples under
`examples/archive/legacy-blueprints-pipelines/`. Do not build a legacy parser
unless later implementation evidence proves it useful.

**Impacts:** [10](10-migration-safety-compatibility.md).

## D27 - Call Mode Names

**Question:** What names distinguish inline child execution from child run
execution?

**Decision:** use `call.mode: inline | run`. `inline` returns child outputs
inside the same run. `run` creates child run identity.

**Impacts:** [02](02-graph-ir-source-formats.md).

## D28 - Reproducibility Guarantee

**Question:** What must Hadron store so runs remain explainable and rerunnable
after source files change or disappear?

**Decision:** execution stores plan digest, source digests, and enough source
snapshot/cache material to rerun even if original source files change or
disappear.

**Impacts:** [07](07-activation-run-binding.md).

## D29 - Wait Timeout Semantics

**Question:** What does a wait timeout do?

**Decision:** timeout marks the node `timed_out`. Default timeout behavior
fails dependents through readiness rules unless a `catch` or error route
handles the timeout.

**Impacts:** [05](05-waits-gates-callbacks.md),
[04](04-execution-state-scheduler.md).

## D30 - Initial Built-In Step Kinds

**Question:** Which step kinds belong in the first planner slice?

**Decision:** initial built-in step kinds are `cmd`, `transform`, `call`,
`sleep`, `wait_for`, `human_gate`, `message_wait`, `mcp`, and `http`. `llm`,
`script`, `agent_launch`, and compiled/offline support are follow-on unless the
planner selects an AI-first milestone.

**Impacts:** [06](06-step-kinds-executors.md),
[08](08-hadron-app-service-surfaces.md).

## D31 - Initial Retention And Redaction Vocabulary

**Question:** What are the starting value/event classification vocabularies?

**Decision:** initial redaction classes are `public | private | secret`.
Initial retention classes are `none | run | project | external`.

**Impacts:** [03](03-values-expressions-artifacts.md),
[10](10-migration-safety-compatibility.md).
