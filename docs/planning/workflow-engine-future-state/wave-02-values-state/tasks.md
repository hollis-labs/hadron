# Wave 02 - Values, Expressions, And State Tasks

**Purpose:** create the data plane and durable runtime contracts used by the
scheduler and executors.
**Primary architecture refs:** [values and artifacts](../../../architecture/workflow-engine-future-state/03-values-expressions-artifacts.md), [execution state and scheduler](../../../architecture/workflow-engine-future-state/04-execution-state-scheduler.md), [ADR 0008](../../../architecture/adr/0008-typed-values-and-artifacts.md), [ADR 0009](../../../architecture/adr/0009-durable-ready-queue-runtime.md).

## W02-T01 - Implement Typed `Value` And `ArtifactRef` Model

**Objective:** replace string/log-based workflow data flow with typed values
and artifact references.

**Concrete work:**

- Add `workflow/values` package with `Value`, `ArtifactRef`, producer metadata,
  media type, digest, redaction class, and retention class.
- Support JSON-compatible scalar, array, and object values inline.
- Represent large, binary, sensitive, or long-lived data as `ArtifactRef`.
- Define value-set references used by run, node invocation, wait, and event
  records.
- Add JSON round-trip tests and digest tests.

**Acceptance criteria:**

- Values can carry either inline data or an artifact reference with explicit
  classification metadata.
- Logs are not modeled as the data plane.
- Redaction classes start as `public | private | secret`; retention classes
  start as `none | run | project | external`.

**Verification:**

- `go test ./workflow/values/...`

## W02-T02 - Add Expression And Interpolation Engine

**Objective:** evaluate `if`, `for_each`, transforms, bindings, and output
expressions through `expr-lang/expr`, with `{{ }}` used only for string
interpolation.

**Concrete work:**

- Add expression parsing and evaluation wrappers that accept typed value
  contexts.
- Define standard context roots such as `inputs`, `steps`, `item`, `run`, and
  host-provided `run_scope` and `execution_target` metadata. Treat `env` as a
  policy-gated reference surface rather than an ambient map.
- Add interpolation support for strings containing `{{ expression }}`.
- Make unresolved names, type mismatches, and hidden dependencies return
  diagnostics.
- Cache compiled expressions where safe.

**Acceptance criteria:**

- Data predicates and bindings evaluate from typed values, not rendered source
  templates.
- `{{ }}` is rejected where a raw expression is required.
- Expression errors include source-map references.

**Verification:**

- `go test ./workflow/values/... ./workflow/compile/...`
- Fixture tests for `if`, `for_each`, output binding, and interpolation.

## W02-T03 - Bind Workflow Inputs And Outputs

**Objective:** create the runtime boundary that turns a compiled plan plus
caller input into a bound run with typed inputs and declared outputs.

**Concrete work:**

- Define `BoundRun`, input binding, output binding, and value coercion rules.
- Validate required inputs, default values, schemas, and unknown caller input.
- Bind top-level workflow outputs from expressions evaluated after graph
  completion.
- Store input and output value-set references through the state-store contract.
- Add tests for missing input, invalid type, defaulting, and output expression
  failures.

**Acceptance criteria:**

- A run cannot start with invalid or incomplete required inputs.
- Top-level outputs are first-class typed values and can power `call` nodes,
  MCP tools, CLI results, HTTP responses, and A2A task results.
- Bound run data includes plan digest and provenance.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/values/...`

## W02-T04 - Define Runtime State-Store Interface

**Objective:** define the high-level persistence contract the scheduler uses
without exposing SQL details to the core.

**Concrete work:**

- Define interfaces for runs, node invocations, attempts, waits, values,
  events, plan refs, leases, compare-and-swap claims, cache entries, pinned
  values, and recovery queries.
- Model node statuses including pending, ready, running, waiting, succeeded,
  failed, skipped, canceled, timed_out, crashed, and blocked. Keep a structured
  blocked reason so readiness diagnostics do not overload `pending`.
- Define append-only event records with redaction/retention metadata.
- Include idempotency keys for start, claim, wait resume, and external
  activations.
- Add fake/in-memory store for conformance tests.

**Acceptance criteria:**

- Runtime code depends on store interfaces, not Hadron SQLite packages.
- Store operations are expressed in workflow terms, not table-shaped CRUD.
- The interface supports crash recovery and idempotent resume.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/conformance/...`

## W02-T05 - Implement Hadron SQLite State Adapter Schema

**Objective:** bind the state-store contract to Hadron's existing SQLite-backed
persistence without moving SQLite into core.

**Current code refs:** `../../../../internal/persistence`.

**Concrete work:**

- Add migrations for graph-native runs, node invocations, attempts, waits,
  values, plan snapshots, plan source maps, leases, and events.
- Implement a Hadron-owned adapter that satisfies the workflow state-store
  interface.
- Preserve existing run/event inspection where useful, but do not force the new
  runtime into old blueprint/pipeline table shapes.
- Add transaction boundaries for node claim, attempt start, wait creation,
  resume, and completion.

**Acceptance criteria:**

- Hadron SQLite can persist and recover an incomplete graph-native run.
- CAS/lease behavior prevents two workers from owning the same node attempt.
- Existing persistence tests keep passing.

**Verification:**

- `go test ./internal/persistence/... ./workflow/conformance/...`

## W02-T06 - Enforce Redaction, Retention, And Secret-Ref Metadata

**Objective:** make data classification a runtime invariant before outputs
reach logs, events, UI, HTTP, MCP, or A2A.

**Concrete work:**

- Validate opaque secret refs such as `secret://authority/path#field`.
- Store secret refs and provenance, never secret material, in workflow state.
- Add event and value rendering helpers that mask `secret` values and respect
  `private` display policy.
- Mask known resolved secret material in command streams, prompts, messages,
  HTTP/MCP observations, and other event payloads before persistence.
- Add retention hooks for run-scoped, project-scoped, external, and no-retain
  values.
- Add tests for event rendering, value serialization, and adapter-boundary
  secret resolution.

**Acceptance criteria:**

- Secret values cannot be persisted through the core `Value` model.
- Redacted event rendering is shared by CLI, HTTP, MCP, A2A, and UI callers.
- Retention metadata is present when values and artifacts are written.

**Verification:**

- `go test ./workflow/values/... ./workflow/runtime/... ./internal/...`

## W02-T07 - Infer Data Dependencies And Enforce Value Visibility

**Objective:** derive graph data edges from expressions while preserving
fail-hard, dependency-scoped access to upstream values.

**Concrete work:**

- Walk node bindings, `if`, `for_each`, switch arms, transforms, output
  expressions, verification rules, memoization keys, and activation input maps
  for `steps.<id>` references.
- Infer data dependencies from those references and union them with explicit
  `needs`, which remain the syntax for ordering-only dependencies.
- Re-run cycle detection and readiness validation after inferred edges are
  present.
- Reject references outside explicit or inferred visibility and report the
  producer, consumer, expression location, and remediation.
- Represent fan-out item references and optional-branch references that cannot
  be fully resolved until runtime without silently dropping them.
- Add a compiler/binder API that the runtime can use to build a scoped value
  context for each invocation.

**Acceptance criteria:**

- Referencing `steps.fetch.outputs.value` creates a data edge without requiring
  a redundant `needs: [fetch]` declaration.
- Ordering-only dependencies remain expressible without a value reference.
- Hidden, misspelled, or cyclic references fail with source-mapped diagnostics.
- Runtime contexts expose only inputs, runtime-scoped values, and upstream nodes
  in the explicit-plus-inferred dependency set.

**Verification:**

- `go test ./workflow/compile/... ./workflow/values/...`
- Conformance fixtures for inferred edges, hidden references, optional branches,
  fan-out item scopes, and cycles introduced by expressions.

## W02-T08 - Implement Artifact-Store Contract And Hadron Adapter

**Objective:** make large, binary, sensitive, and long-lived values usable by
reference through a real storage contract rather than leaving `ArtifactRef` as
metadata without a producer or resolver.

**Concrete work:**

- Define an application-neutral artifact interface for streaming put/open/stat,
  digest verification, authorized resolution, retention, and deletion/expiry.
- Keep artifact payloads outside core runtime state; store only `ArtifactRef`,
  classification, producer, digest, media type, size, authority, and provenance.
- Implement a Hadron-owned adapter under `internal/artifacts` or the selected
  equivalent for run/project-scoped artifacts and passthrough references to
  approved external authorities.
- Enforce inline size caps and promote oversized command, HTTP, MCP, script, and
  other executor results to artifact references without buffering unbounded data.
- Resolve secret/sensitive artifacts only at authorized adapter boundaries and
  prevent their material from entering events, diagnostics, or source snapshots.
- Implement retention cleanup for `none`, `run`, `project`, and `external`
  classes with observable, idempotent cleanup outcomes.

**Acceptance criteria:**

- Producers can stream an artifact and return a verified reference; consumers
  can stream it only after authority and policy checks.
- Large/binary values never depend on SQLite row-size accidents or global log
  capture.
- Digest mismatch, missing authority, expired retention, and unauthorized access
  fail with structured diagnostics.
- Run/project cleanup cannot delete externally owned content and is safe to
  retry.

**Verification:**

- `go test ./workflow/values/... ./internal/artifacts/... ./internal/persistence/...`
- Streaming size-cap, digest mismatch, redaction, external passthrough,
  authorization, expiry, and idempotent cleanup integration tests.
