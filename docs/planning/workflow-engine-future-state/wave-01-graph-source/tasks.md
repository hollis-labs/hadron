# Wave 01 - Graph Source And Compiler Tasks

**Purpose:** create the canonical graph IR and greenfield `workflow` source
compiler.
**Primary architecture refs:** [graph IR and source formats](../../../architecture/workflow-engine-future-state/02-graph-ir-source-formats.md), [activation and run binding](../../../architecture/workflow-engine-future-state/07-activation-run-binding.md), [ADR 0007](../../../architecture/adr/0007-graph-native-workflow-ir.md).

## W01-T01 - Define Graph IR Go Types

**Objective:** create the in-memory graph model shared by compiler, runtime,
UI, transports, and conformance tests.

**Concrete work:**

- Define core graph types under `workflow/graph`.
- Include stable workflow ID/version/digest, inputs, outputs, nodes, edges,
  readiness, `if`, `for_each`, config, bindings, effects, retry, timeout,
  catch/finally, switch, call spec, source activation declarations, source refs,
  and metadata.
- Include concurrency resources, run completion policy, verification,
  memoization, durability, service-node, and compensation extension envelopes so
  later tasks do not require alternate graph semantics.
- Model `CallSpec.Mode` as `inline | run`.
- Include placeholders for policy and execution-target requirements without
  importing Hadron policy packages.
- Add validation helpers for IDs and enum values.

**Acceptance criteria:**

- The IR can represent current sequential blueprint behavior as explicit
  dependencies and current pipeline stages as `call` nodes.
- The IR does not expose Hadron-specific run, workspace, registry, MCP server,
  or SQLite types.
- Basic type tests cover enum validation and ID normalization rules.

**Verification:**

- `go test ./workflow/graph/...`

## W01-T02 - Generate Graph IR JSON Schema

**Objective:** make Go structs the source of truth while producing schema for
YAML validation, UI, agents, and serialized plans.

**Concrete work:**

- Add a schema generation command or test-backed script for `workflow/graph`.
- Commit generated schema under a stable path such as
  `workflow/graph/schema/workflow.schema.json`.
- Add schema metadata for source authoring and serialized `ExecutionPlan`
  boundaries.
- Document regeneration commands.

**Acceptance criteria:**

- Schema generation is deterministic.
- CI or tests fail when generated schema is stale.
- Generated schema includes node kind extension points without baking
  adapter-specific config into core.

**Verification:**

- Schema regeneration command.
- `go test ./workflow/graph/...`

## W01-T03 - Implement Workflow Source Loader

**Objective:** parse `*.workflow.yaml` and directory `workflow.yaml` into a
source AST that preserves location and authoring shape.

**Concrete work:**

- Add `workflow/compile` source loading for files and in-memory bytes.
- Support preferred source names: `*.workflow.yaml` and `workflow.yaml`.
- Preserve YAML path, file, line, and column information where the parser makes
  it available.
- Reject blueprint/pipeline source as target input with a diagnostic that
  points to the archive/rewrite policy.
- Add source loader tests for valid files, malformed YAML, unsupported file
  names, and legacy source rejection.

**Acceptance criteria:**

- Source loading does not execute expressions or resolve definitions.
- Diagnostics are structured through W00-T05 conventions.
- Loader behavior is independent of Hadron registry or daemon state.

**Verification:**

- `go test ./workflow/compile/...`

## W01-T04 - Compile Source To ExecutionPlan With Source Maps

**Objective:** lower source AST into a durable `ExecutionPlan` that runtime can
bind and execute.

**Concrete work:**

- Define `ExecutionPlan`, `DefinitionRef`, provenance, source digest, plan
  digest, and source-map structures.
- Lower workflow inputs, outputs, steps, dependencies, `if`, `for_each`, retry,
  timeout, call, effects, and metadata into graph IR.
- Persist source maps on the plan, with node invocations later referencing
  compact plan-level entries.
- Add digesting that changes when semantic graph or relevant source changes.
- Add tests that assert source maps point to the expected YAML locations.

**Acceptance criteria:**

- A valid source file compiles to stable graph IDs and deterministic plan
  digest.
- Runtime-facing plan data is independent of the original file path except for
  provenance/source-map fields.
- Source maps are sufficient for validation and runtime diagnostics.

**Verification:**

- `go test ./workflow/compile/...`
- Snapshot tests for plan JSON and source maps.

## W01-T05 - Implement Validation Passes

**Objective:** reject invalid graphs before runtime binding.

**Concrete work:**

- Validate duplicate node IDs, cycles, unknown dependencies, unknown step
  kinds, invalid `call.mode`, unsupported readiness rules, and invalid
  `for_each` shape.
- Keep structural reference parsing independent from expression dependency
  inference; W02-T07 owns typed reference binding and visibility validation.
- Validate node config against registered step-kind schemas.
- Validate effect/retry combinations through policy hooks.
- Detect call cycles up to the configured maximum depth.

**Acceptance criteria:**

- Invalid workflows return structured diagnostics, not partial runtime starts.
- Validation can run without Hadron daemon services.
- Unknown step kinds can be reported even when adapters are not registered.

**Verification:**

- `go test ./workflow/compile/... ./workflow/stepkind/...`
- Conformance fixtures for graph validation failures.

## W01-T06 - Add Greenfield Acceptance Fixtures

**Objective:** create the first graph-native examples that represent the target
contract and replace legacy examples as active reference material.

**Concrete work:**

- Add a Torque-style bulk-create workflow fixture with array input, fan-out,
  bounded concurrency, MCP call, idempotency key, transform summary, and
  declared outputs.
- Add a small wait/gate fixture and a small HTTP/cmd/transform fixture.
- Add expected compile plans and diagnostics snapshots.
- Link active fixtures from docs and tests.

**Acceptance criteria:**

- Active examples are all graph-native `workflow` files.
- The fixtures cover data binding, control dependencies, typed outputs, and
  source-map diagnostics.
- Legacy examples remain available only under the archive path.

**Verification:**

- `go test ./workflow/compile/... ./workflow/conformance/...`

## W01-T07 - Compile Workflow Activation Declarations

**Objective:** compile source-level `on:` declarations into plan-owned
activation templates without turning schedules or triggers into alternate
workflow definitions.

**Concrete work:**

- Extend the source AST and graph model with `on.webhook`, `on.schedule`,
  `on.message`, file/event activation, and one-shot/TTL declaration shapes.
- Preserve input extraction/mapping expressions, overlap policy,
  `starting_deadline`, catchup, run-ID reuse, source authority, and provenance.
- Lower declarations into immutable activation templates on `ExecutionPlan`;
  do not create Hadron scheduler or trigger rows in the compiler.
- Validate duplicate/conflicting activation names, malformed cron/path/message
  declarations, and unsupported source-owned authority claims.
- Add source maps and deterministic digest coverage for every activation
  declaration.

**Acceptance criteria:**

- A workflow containing webhook, schedule, and message declarations compiles to
  one execution plan with source-mapped activation templates.
- Operational IDs, enabled state, fire history, and callback tokens are absent
  from the immutable plan.
- Invalid activation declarations fail compilation with structured diagnostics.
- W05-T08 can materialize registrations without reparsing workflow YAML.

**Verification:**

- `go test ./workflow/graph/... ./workflow/compile/...`
- Snapshot fixtures for webhook, schedule, message, file/event, and one-shot
  activation templates.
