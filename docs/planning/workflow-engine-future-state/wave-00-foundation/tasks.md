# Wave 00 - Foundation Tasks

**Purpose:** create the safe implementation lane before semantic work begins.
**Primary architecture refs:** [package boundaries](../../../architecture/workflow-engine-future-state/01-package-boundaries.md), [migration safety](../../../architecture/workflow-engine-future-state/10-migration-safety-compatibility.md), [step kinds and executors](../../../architecture/workflow-engine-future-state/06-step-kinds-executors.md), [ADR 0006](../../../architecture/adr/0006-reusable-workflow-engine-boundary.md), [ADR 0010](../../../architecture/adr/0010-step-executor-registry.md).

## W00-T01 - Archive Legacy Blueprint And Pipeline Examples

**Objective:** move current blueprint/pipeline examples out of the active
example namespace so they cannot become accidental compatibility tests.

**Current code refs:** `../../../../examples`, `../../../../internal/blueprint`,
`../../../../internal/pipeline`.

**Concrete work:**

- Create `examples/archive/legacy-blueprints-pipelines/`.
- Move existing blueprint and pipeline examples into that archive path.
- Add a short archive README explaining that these examples are reference input
  for selective rewrites, not public source-format commitments.
- Update tests, docs, or fixture paths that directly reference old example
  locations.
- Create a new `examples/workflow/` folder for graph-native examples introduced
  by later tasks.

**Acceptance criteria:**

- No active docs or tests present legacy examples as the preferred authoring
  format.
- Archived examples still preserve their original contents and relative context.
- `go test ./...` does not fail due to moved fixtures.

**Verification:**

- `rg "examples/" docs internal examples`
- Review matches so active references use `examples/workflow/` and legacy
  references use `examples/archive/legacy-blueprints-pipelines/`.
- `go test ./...`

## W00-T02 - Scaffold Extraction-Ready Workflow Package Family

**Objective:** create package boundaries that let the engine start in this repo
while remaining extractable to a shared module.

**Concrete work:**

- Add top-level package directories matching the architecture boundary, for
  example `workflow/graph`, `workflow/compile`, `workflow/values`,
  `workflow/runtime`, `workflow/wait`, `workflow/stepkind`, and
  `workflow/conformance`.
- Select and record the canonical package names in this task using the accepted
  ownership boundary; update later task paths if the selected names differ from
  the examples.
- Add minimal package docs that describe ownership, allowed imports, and
  stability expectations.
- Keep Hadron-specific bindings under `internal/appworkflow` or an equivalent
  Hadron-owned package, not under `workflow/`.
- Add a placeholder integration package only where it clarifies the host
  boundary.

**Acceptance criteria:**

- Engine-core packages can compile without importing Hadron `internal/*`.
- Package comments make it clear what belongs in core versus Hadron host code.
- The package family matches the future extraction shape documented in ADR
  0006.

**Verification:**

- `go test ./workflow/...`
- `go list -deps ./workflow/...`

## W00-T03 - Add Dependency And Import Guardrails

**Objective:** prevent future tasks from leaking Hadron app, transport, storage,
or provider dependencies into the core engine.

**Concrete work:**

- Add a test or script that fails when `./workflow/...` imports
  `github.com/hollis-labs/hadron/internal/...`.
- Add checks for concrete server, Wails, MCP server, SQLite persistence, model
  provider, and sibling-app packages in core imports.
- Document the allowlist for standard library, schema, expression, and test
  dependencies.
- Wire the guard into the standard verification path used by CI or `make test`
  if this repo has one.

**Acceptance criteria:**

- A deliberate forbidden import in a core package causes a deterministic test
  failure.
- Allowed adapter imports remain possible outside core.
- The guard can run locally without extra services.

**Verification:**

- `go test ./workflow/...`
- `go test ./...`

## W00-T04 - Create Conformance Harness Skeleton

**Objective:** establish black-box tests that Hadron, future extracted modules,
and sibling apps can reuse to prove workflow semantics.

**Concrete work:**

- Add `workflow/conformance` package with test-suite entry points for compiler,
  state store, scheduler, waits, and step-kind registry behavior.
- Define small host/store interfaces used only by the conformance suite.
- Add fixture folders for graph validation, source maps, values, scheduler,
  waits, and executor metadata.
- Include one deliberately tiny passing fixture and one failing fixture per
  major contract.

**Acceptance criteria:**

- A host adapter can call the conformance suite without importing Hadron app
  packages.
- The suite has stable fixture naming and failure messages.
- The initial suite is small but proves the harness pattern.

**Verification:**

- `go test ./workflow/conformance/...`

## W00-T05 - Establish Diagnostics And Error-Code Conventions

**Objective:** make compiler and runtime failures source-mapped, searchable, and
stable enough for CLI, HTTP, MCP, UI, and agents.

**Concrete work:**

- Define diagnostic severity, code, message, source reference, related
  references, and remediation fields.
- Reserve code prefixes for source validation, values, policy, effects, waits,
  persistence, and host integration.
- Add examples matching the architecture notes, such as unresolved references,
  unsafe effect/retry combinations, and archived output-shim warnings.
- Define JSON representation for diagnostics returned by transports.

**Acceptance criteria:**

- Compiler and validation tasks can return structured diagnostics without
  string parsing.
- Diagnostics include enough source map data to point back to YAML paths and
  line ranges when available.
- Transport layers can pass diagnostics through without redefining them.

**Verification:**

- Unit tests for diagnostic JSON round trips.
- Fixture tests for at least one source-mapped validation error.

## W00-T06 - Define Step-Kind Contract Skeleton

**Objective:** provide the compiler and runtime with a stable executor contract
before individual step kinds are implemented.

**Concrete work:**

- Add `workflow/stepkind` package with `StepKindSpec`, registry interfaces,
  config-schema metadata, input/output schema metadata, and effect metadata.
- Define required `Spec`, `ValidateConfig`, and `Execute` method shapes.
- Define optional lifecycle interfaces for `Prepare`, `Observe`, `Cancel`, and
  `Finalize`.
- Add no-op/fake step kinds for compiler and conformance tests.
- Keep concrete `cmd`, `http`, `mcp`, provider, and Hadron dependencies out of
  this package.

**Acceptance criteria:**

- Compiler validation can ask a registry whether a step kind exists and whether
  node config is valid.
- Runtime code can call a step kind through interfaces without knowing adapter
  implementation packages.
- The skeleton compiles independently from Hadron app/service code.

**Verification:**

- `go test ./workflow/stepkind/...`

## W00-T07 - Standardize Timed Activation Contracts In `go-scheduler`

**Objective:** evolve the shared scheduler library into the application-neutral
timed activation substrate required by workflow retries, waits, callbacks, and
scheduled starts.

**Repository scope:** `/Users/chrispian/dev/hollis-labs/libs/go-scheduler`
only. Nanite may be inspected as implementation evidence but receives no edits.

**Current code refs:** `scheduler.go`, `engine.go`, `engine_test.go`, `README.md`
in the `go-scheduler` repository.

**Concrete work:**

- Replace per-tick run identity with stable fire identity derived from schedule
  identity and scheduled fire time; preserve scheduled-at separately from
  observed fired-at time.
- Add application-neutral retry, backoff, maximum-attempt, exhaustion, and
  per-fire attempt/status contracts.
- Add observer hooks for claim, fire, retry, skip, success, exhaustion, disable,
  and engine error events without importing Hadron or Nanite event types.
- Make clock, tick cadence, and due-batch limit configurable while retaining
  backward-compatible defaults where practical.
- Preserve compare-and-swap claim semantics and add contract tests proving that
  concurrent engines cannot dispatch the same fire attempt twice.
- Keep application job types, producers, workflow definitions, loop/reflex
  behavior, event names, persistence schemas, and policy outside the library.
- Update library documentation and changelog with the new public contracts and
  any pre-1.0 migration notes.

**Acceptance criteria:**

- A scheduled fire has stable identity across retry attempts and process ticks.
- Retry exhaustion is observable and cannot silently reset on the next tick.
- Tests can inject a clock, cadence, and batch limit deterministically.
- Store and observer contracts remain application-neutral and have concurrent
  CAS conformance coverage.
- No Hadron, Nanite, Torque, or Cerberus package is imported by `go-scheduler`.

**Verification:**

- From `/Users/chrispian/dev/hollis-labs/libs/go-scheduler`: `go test ./...`
- `go test -race ./...`
- `go list -deps ./...` and review for application imports.
