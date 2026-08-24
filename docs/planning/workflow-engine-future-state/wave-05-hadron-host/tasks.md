# Wave 05 - Hadron Host Integration Tasks

**Purpose:** wire the reusable core into Hadron's daemon, persistence, registry,
activation, and diagnostics without recreating workflow semantics in app code.
**Primary architecture refs:** [package boundaries](../../../architecture/workflow-engine-future-state/01-package-boundaries.md), [activation and run binding](../../../architecture/workflow-engine-future-state/07-activation-run-binding.md), [Hadron surfaces](../../../architecture/workflow-engine-future-state/08-hadron-app-service-surfaces.md), [ADR 0011](../../../architecture/adr/0011-hadron-host-surfaces-and-exposure.md), [ADR 0012](../../../architecture/adr/0012-run-scope-and-execution-target.md).

## W05-T01 - Create Hadron Workflow Host Binding

**Objective:** expose a Hadron-owned host object that binds definitions,
state, executors, timers, policy, artifacts, telemetry, and identity to the
workflow core.

**Current code refs:** `../../../../internal/execution`,
`../../../../internal/persistence`, `../../../../internal/telemetry`.

**Concrete work:**

- Add `internal/appworkflow` or equivalent host-binding package.
- Integrate host start, health/readiness, graceful shutdown, and restart recovery
  so daemon lifecycle cannot strand claims, waits, or activation callbacks.
- Register initial step-kind adapters selected in Wave 04.
- Bind Hadron SQLite state adapter, event sink, artifact store, clock, and
  policy evaluator.
- Bind caller principal, source authority/trust, grants, `RunScope`,
  `ExecutionTarget`, and effective policy into every started run, and record
  policy decisions as append-only operational facts.
- Bind the workflow activation interface to the standardized `go-scheduler`
  contract from W00-T07 plus Hadron trigger persistence.
- Provide start, inspect, cancel, resume, and explain service methods.
- Roll up node effects and required capabilities into plan/run policy facts;
  expose confirmation and blast-radius explanations, and advertise dry-run only
  when every participating adapter can truthfully avoid side effects.
- Ensure Hadron host code imports workflow core, not the reverse.

**Acceptance criteria:**

- Hadron can start a graph-native workflow through one app-service entry point.
- Runtime behavior is delegated to workflow core contracts.
- Host construction is testable without starting every transport surface.

**Verification:**

- `go test ./internal/appworkflow/... ./workflow/...`

## W05-T02 - Introduce `RunScope` And `ExecutionTarget`

**Objective:** cleanly separate logical operational scope from compute/workspace
execution binding.

**Current code refs:** `../../../../internal/persistence/workspaces.go`,
`../../../../internal/execution/types.go`,
`../../../../internal/api/types.go`.

**Concrete work:**

- Add Hadron-local `RunScope` model for project/account/session/team/user
  logical scoping as selected by product needs.
- Add `ExecutionTarget` model for compute workspace, cwd, environment,
  capabilities, and sandbox policy.
- Replace new graph-native APIs that would otherwise accept `workspace_id` as a
  target concept.
- Migrate internal service calls for new workflows to use `RunScope` and
  `ExecutionTarget`.
- Add validation that a scope alone cannot grant compute access.

**Acceptance criteria:**

- New workflow runs store both scope and execution target when execution needs
  target binding.
- `workspace_id` is not part of the target graph-native public API.
- Policy decisions can inspect both logical scope and execution target.

**Verification:**

- `go test ./internal/appworkflow/... ./internal/persistence/...`

## W05-T03 - Bind Registry And Definition Resolution

**Objective:** make `DefinitionRef` resolution a Hadron host responsibility
that feeds the compiler and plan cache.

**Current code refs:** `../../../../internal/registry`,
`../../../../internal/pack`, `../../../../internal/specparse`.

**Concrete work:**

- Implement definition resolver for file paths, registry entries, pinned
  versions, and package references.
- Resolve to source bytes, source authority, digest, provenance, and trust
  class.
- Support directory-default `workflow.yaml` and named `*.workflow.yaml`.
- Cache compiled plans by resolved digest and compiler options.
- Return structured diagnostics for unresolved or unauthorized refs.

**Acceptance criteria:**

- CLI, HTTP, MCP, A2A, schedules, triggers, UI, and child calls use the same
  definition resolver.
- Plan compilation is reproducible from resolved source data.
- Registry/package concepts stay in Hadron host code.

**Verification:**

- `go test ./internal/appworkflow/... ./internal/registry/... ./workflow/compile/...`

## W05-T04 - Bind Schedules And Triggers To Activation Registrations

**Objective:** make schedules, triggers, webhooks, file watchers, timers, and
callbacks activate bound workflow definitions instead of ad hoc execution
requests.

**Current code refs:** `../../../../internal/scheduler`,
`../../../../internal/trigger`, `../../../../internal/persistence/schedules.go`,
`../../../../internal/persistence/triggers.go`.

**Concrete work:**

- Add activation registration model with definition ref, input bindings,
  principal, exposure profile, `RunScope`, `ExecutionTarget`, overlap policy,
  starting deadline, catchup, and run ID reuse policy.
- Map current scheduler jobs and trigger events into activation registrations.
- Map stable scheduler fire identity, attempt, retry/exhaustion, and observer
  events into Hadron activation records and diagnostics.
- Implement idempotency for external activation events.
- Materialize one-shot callback wakes as Hadron app-service records that resume
  core waits.
- Add tests for missed schedule, duplicate trigger, overlap forbid, and replace.

**Acceptance criteria:**

- All activation paths bind to the same run-start service.
- Activation policy names match `Allow | Forbid | Replace`,
  `starting_deadline`, and `catchup`.
- Callback waits are implemented as Hadron details over generic wait records.

**Verification:**

- `go test ./internal/scheduler/... ./internal/trigger/... ./internal/appworkflow/...`

## W05-T05 - Persist Plan/Source Snapshots And Provenance

**Objective:** keep runs explainable and rerunnable after workflow source files
change or disappear.

**Current code refs:** `../../../../internal/persistence`,
`../../../../internal/registry`.

**Concrete work:**

- Persist plan digest, source digest, source authority, source snapshot/cache
  material, compile options, and source map.
- Link run records to the exact execution plan used at start time.
- Add retention and cleanup rules for snapshots.
- Add explain APIs that show source/provenance without exposing secret values.
- Add tests that modify/delete source after run start and still inspect/rerun
  from stored material where policy allows.

**Acceptance criteria:**

- Run inspection can show the exact plan and source reference used.
- Reproducibility does not depend on mutable working files.
- Source snapshots honor redaction and retention rules.

**Verification:**

- `go test ./internal/appworkflow/... ./internal/persistence/...`

## W05-T06 - Update Run Inspection And Diagnostics

**Objective:** make operator diagnostics speak graph-native workflow concepts:
runs, node invocations, attempts, waits, values, plans, and source maps.

**Current code refs:** `../../../../internal/rundiagnostics`,
`../../../../internal/persistence/run_events.go`,
`../../../../internal/api/responses.go`.

**Concrete work:**

- Add diagnostics queries for graph, node state, attempts, waits, values,
  upstream readiness, downstream effects, and source refs.
- Add diagnostics for concurrency resources, blocked/crashed states, catch and
  finally decisions, replay provenance, memoized/pinned values, and activation
  fire attempts.
- Render redacted values and events through shared helpers.
- Keep historical run diagnostics readable while introducing graph-native
  result shapes.
- Add explain output for why nodes are pending, skipped, failed, timed out, or
  waiting.

**Acceptance criteria:**

- A failed graph-native run can be debugged from persisted state without logs
  as the source of truth.
- Diagnostics include source file/path/line where available.
- Transport surfaces can reuse the same diagnostics DTOs.

**Verification:**

- `go test ./internal/rundiagnostics/... ./internal/appworkflow/...`

## W05-T07 - Add Workflow Contract-Test And Registration Service

**Objective:** provide the Hadron-owned service path that validates reusable
workflows, runs definition-level contract tests with controlled executors, and
registers immutable versions for discovery and exposure.

**Current code refs:** `../../../../internal/registry`,
`../../../../internal/pack`, `../../../../internal/specparse`,
`../../../../internal/mcpadapter`.

**Concrete work:**

- Define graph-native workflow contract tests with typed inputs, expected
  outputs/errors, expected effects/tool calls, and deterministic executor mocks.
- Add graph-native contract-test generation that derives an editable test
  scaffold from workflow inputs, outputs, effects, and registered step schemas.
- Add service methods to validate, execute contract tests, register, version,
  digest, package, pin, unpin, publish, and inspect workflow definitions.
- Require successful validation and configured contract-test policy before a
  version can become publishable or directly exposed.
- Preserve source authority, namespace, provenance, trust class, plan digest,
  test result, publisher principal, and registration timestamp.
- Keep the registry as a resolution/discovery/provenance index rather than
  silently making it authoritative for project-owned source.
- Add explicit namespace ownership and authorization checks used by exposure
  profiles and agent-authored workflows.

**Acceptance criteria:**

- A workflow can be validated, tested with mocked adapters, registered under an
  authorized namespace, pinned by digest, packaged, and rediscovered.
- Test failures, undeclared effects, unauthorized namespaces, or mutable refs
  prevent publish/direct exposure according to policy.
- Contract tests exercise the same compiled plan, bindings, expressions, and
  typed outputs used by real runs.
- Registry operations do not introduce alternate workflow execution semantics.

**Verification:**

- `go test ./internal/appworkflow/... ./internal/registry/... ./internal/pack/...`
- Integration fixture covering validate, mocked contract test, register, version,
  digest pin, package, search, and resolve.

## W05-T08 - Materialize Source-Declared Activations

**Objective:** turn plan-owned activation templates into Hadron operational
registrations when a workflow is registered, updated, disabled, or removed.

**Concrete work:**

- Materialize compiled webhook, schedule, message, file/event, and one-shot/TTL
  templates through the W05-T04 activation service.
- Record whether project source or an operator is authoritative for each
  registration and retain the source plan/template digest.
- Reconcile registration updates idempotently without losing fire history or
  creating duplicate active callbacks.
- Validate principal, exposure profile, `RunScope`, `ExecutionTarget`, input
  mapping, overlap, missed-fire, catchup, and run-ID reuse policy at
  materialization time.
- Disable or retire derived rows when the authoritative workflow version is
  unregistered while preserving operational history according to retention.

**Acceptance criteria:**

- Registering a workflow with `on:` declarations creates the corresponding
  operational registrations without transport-specific parsing.
- Re-registering the same plan is idempotent; changing a declaration produces a
  traceable reconciliation.
- Ad hoc operator registrations and source-derived registrations have explicit,
  distinguishable authority.
- Every resulting activation starts a run through the common bound-run service.

**Verification:**

- `go test ./internal/appworkflow/... ./internal/scheduler/... ./internal/trigger/...`
- Integration fixtures for create, idempotent update, changed plan, disable,
  deletion, and duplicate external event delivery.
