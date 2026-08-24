# Wave 06 - Surfaces And Cleanup Tasks

**Purpose:** expose the new workflow model through Hadron's public surfaces and
retire old semantic paths from active use.
**Primary architecture refs:** [Hadron surfaces](../../../architecture/workflow-engine-future-state/08-hadron-app-service-surfaces.md), [migration safety](../../../architecture/workflow-engine-future-state/10-migration-safety-compatibility.md), [ADR 0011](../../../architecture/adr/0011-hadron-host-surfaces-and-exposure.md).

## W06-T01 - Add Workflow CLI Commands

**Objective:** let users validate, explain, run, inspect, cancel, and resume
graph-native workflows from the CLI.

**Concrete work:**

- Add commands such as `hadron workflow validate`, `hadron workflow explain`,
  `hadron workflow run`, `hadron workflow inspect`, `hadron workflow cancel`,
  `hadron workflow resume`, and `hadron workflow rerun --from <node>`.
- Return structured diagnostics and redacted values.
- Add effect/capability explanation and policy-checked dry-run output that fails
  closed when an executor cannot provide a truthful non-effecting preview.
- Support file refs, registry refs, input files, inline JSON input, run scope,
  execution target flags, and policy-checked `--pin <node>=<value-ref>` inputs.
- Keep command behavior routed through Hadron appworkflow services.

**Acceptance criteria:**

- CLI does not call compiler/runtime internals in a way that bypasses host
  policy.
- Validation works without starting a run.
- Run output is typed and can be emitted as JSON.

**Verification:**

- CLI unit tests and golden output tests.
- `go test ./...`

## W06-T02 - Add HTTP Workflow API Surface

**Objective:** expose graph-native workflow operations over Hadron's HTTP API
without introducing HTTP-specific workflow semantics.

**Current code refs:** `../../../../internal/api`.

**Concrete work:**

- Add endpoints for validate, run, inspect, cancel, resume, list waits, and
  fetch redacted values/events.
- Use shared appworkflow DTOs for diagnostics, run handles, node state, waits,
  and outputs.
- Resolve caller principal, exposure profile, scope, and execution target.
- Add idempotency-key support for run start and resume endpoints.
- Add tests for auth/profile denial, duplicate resume, and redacted responses.

**Acceptance criteria:**

- HTTP is a transport over the same appworkflow service used by CLI and MCP.
- Responses carry typed values and structured diagnostics.
- Unauthorized hidden workflows do not appear or run for denied principals.

**Verification:**

- `go test ./internal/api/... ./internal/appworkflow/...`

## W06-T03 - Update MCP Workflow Exposure Profiles

**Objective:** expose selected workflows as MCP tools through Hadron-local
principal and exposure-profile policy.

**Current code refs:** `../../../../internal/mcpadapter`.

**Concrete work:**

- Map MCP token/session to principal, then principal to exposure profile.
- Add Hadron-local persistent principal/exposure-profile records and shared
  appworkflow CRUD/resolution services; keep optional external identity/policy
  authorities behind adapters.
- Support meta-tools for workflow search, inspect, validate, run by reference,
  subscribe/stream and inspect run state, cancel, resume, submit gate/message,
  and send typed signals.
- Support pinned workflows as first-class MCP tools with generated schemas from
  workflow inputs/outputs.
- Support discoverable workflows through compact search results and lazy,
  per-session schema loading; advertise and emit `tools.listChanged` (or the
  protocol-equivalent notification) when tools are mounted or removed.
- Implement namespaces, explicit pins, denied effects, direct-tool budget,
  `search_scope`, lazy-load policy, default meta-tools-only profiles, and
  agent-owned namespace defaults.
- Expose a compact namespace catalog resource without loading every tool schema
  into the session context.
- Enforce exposure, redaction, and effect policy at the appworkflow service
  boundary.
- Add tests for hidden workflows, pinned tool schema, denied effects, and
  idempotent resume, search/load, session isolation, tool-budget refusal,
  namespace catalogs, and list-changed notifications.

**Acceptance criteria:**

- MCP exposure does not redefine workflow semantics.
- Pinned tool schemas are generated from the same graph/source schema and
  workflow IO model.
- Hidden/denied workflows are not listed for that principal/profile.
- Discoverable schemas are mounted only for the requesting authorized session
  and are removed when the session/profile changes.

**Verification:**

- `go test ./internal/mcpadapter/... ./internal/appworkflow/...`

## W06-T04 - Bind A2A Task/Run Correlation

**Objective:** make A2A task execution a durable workflow-run correlation
instead of a separate execution model.

**Current code refs:** `../../../../internal/a2a`,
`../../../../internal/agentcard`.

**Concrete work:**

- Map A2A task start to workflow run start with principal, scope, target, and
  idempotency.
- Map run status, waits, cancellation, events, and outputs back to A2A task
  state.
- Derive A2A agent-card skills from registry entries, input/output schemas,
  effects, definition digest, and provenance rather than legacy blueprint
  structs.
- Preserve task/run correlation through restart.
- Redact values and events according to exposure profile.
- Add tests for task creation, duplicate start, wait resume, cancel, and final
  output mapping.

**Acceptance criteria:**

- A2A is a transport over workflow runs, not a separate orchestration path.
- Task state remains recoverable from workflow state.
- A2A callers receive typed final outputs where allowed.

**Verification:**

- `go test ./internal/a2a/... ./internal/appworkflow/...`

## W06-T05 - Update Desktop Graph And Run Views

**Objective:** make the UI read graph IR, source maps, node state, waits, and
typed values from the app-service surface.

**Concrete work:**

- Replace section/pipeline-stage assumptions in graph views with graph IR
  nodes and edges.
- Use node metadata such as `position` when present.
- Show node status, attempts, waits, retry state, source refs, and redacted
  outputs.
- Show typed value flow on edges for historical runs, artifact metadata,
  concurrency-resource state, replay origin, and catch/finally decisions.
- Show registry/pin/exposure-profile state and effect/capability blast-radius
  explanations using app-service data.
- Add resume/cancel affordances through appworkflow APIs.
- Add UI tests or Playwright coverage for active, failed, waiting, and
  completed runs if the frontend harness exists.

**Acceptance criteria:**

- The desktop surface can visualize and inspect a graph-native workflow run.
- UI state is derived from app APIs, not direct knowledge of old blueprint or
  pipeline shapes.
- Redacted values remain redacted in rendered views.

**Verification:**

- Frontend test command used by the repo.
- `go test ./internal/api/...`

## W06-T06 - Quarantine Old Blueprint/Pipeline Runtime Paths

**Objective:** remove old execution engines from active public paths once the
new workflow host is available.

**Current code refs:** `../../../../internal/blueprint`,
`../../../../internal/execution`, `../../../../internal/pipeline`.

**Concrete work:**

- Route new CLI, HTTP, MCP, A2A, schedule, trigger, and UI workflow operations
  through graph-native appworkflow services.
- Remove or explicitly quarantine legacy blueprint execution and pipeline DAG
  execution paths from active commands.
- Delete active `::set-output` log scraping from graph-native output paths.
- Keep any retained legacy code marked as archive/rewrite helper only, with no
  public compatibility promise.
- Update tests that asserted old behavior to target the new workflow model or
  move them to archive/reference tests.

**Acceptance criteria:**

- There is one active workflow semantic runtime for new operations.
- Legacy examples and code cannot accidentally define the new contract.
- `::set-output` cannot set graph-native workflow data unless an explicit
  compatibility-origin capture is enabled.

**Verification:**

- `rg "::set-output|captureStageOutputs|internal/pipeline" internal workflow`
- `go test ./...`

## W06-T07 - Final Docs, Examples, And Release Checks

**Objective:** finish the implementation handoff with user-facing docs,
developer docs, and regression checks aligned to the new engine.

**Concrete work:**

- Update architecture docs only where implementation changed field names or
  invalidated placeholders.
- Add user docs for source format, examples, CLI commands, MCP exposure, waits,
  values, redaction, and troubleshooting.
- Add developer docs for package boundaries, adapters, conformance, and host
  binding.
- Update `docs/safety.md` to record removal of global `::set-output` scanning,
  secret-stream masking, effect enforcement, and any compatibility shim that
  remains.
- Ensure generated schema, examples, and conformance fixtures are linked from
  docs.
- Run full regression suite and capture command outputs in the implementation
  handoff.

**Acceptance criteria:**

- A new developer can find the source format, package boundary, and test
  strategy without reading old blueprint/pipeline docs first.
- Examples are graph-native and runnable.
- Release checks cover compiler, runtime, store, waits, adapters, and surfaces.

**Verification:**

- `go test ./...`
- Frontend/desktop test command used by the repo.
- Schema regeneration check.

## W06-T08 - Add Workflow Authoring, Registry, And Tool-Building Surfaces

**Objective:** expose the complete reusable-workflow lifecycle so users and
agents can turn a missing capability into a validated, registered, pinned tool
through Hadron's existing doors.

**Concrete work:**

- Add CLI, HTTP, MCP, and A2A operations for draft validation, contract-test
  generation/execution, registration, version inspection, namespace search,
  package, publish, pin/unpin, and exposure inspection as appropriate to each
  transport.
- Implement the agent flow: search existing workflows, submit/draft graph-native
  source, validate and test, register under an authorized namespace, pin by
  digest, and expose through MCP/A2A.
- Adapt `hadron_blueprint_broker` behavior worth preserving to the workflow
  service; retire blueprint-specific assumptions instead of creating a second
  authoring path.
- Return compact schemas and structured diagnostics suitable for agents without
  placing every registered workflow in context.
- Enforce source authority, namespace ownership, effect policy, publication
  policy, and direct-tool budgets at the shared appworkflow service boundary.

**Acceptance criteria:**

- A caller can discover that no suitable workflow exists, register a tested new
  workflow, pin its immutable digest, and invoke it through a generated tool
  without bypassing policy.
- The same registered version and test evidence appear consistently through CLI,
  HTTP, MCP, A2A, registry inspection, and UI.
- Failed validation/tests or unauthorized publication cannot create a directly
  exposed tool.
- No surface owns a private compiler, registry, or execution path.

**Verification:**

- `go test ./internal/appworkflow/... ./internal/api/... ./internal/mcpadapter/... ./internal/a2a/...`
- End-to-end fixture for search, draft, validate, contract test, register, pin,
  lazy load, invoke, inspect output, and unpin.

## W06-T09 - Prove The Torque Bulk-Create End-To-End Acceptance Case

**Objective:** prove the source document's definition of a completed engine core
through one cross-layer acceptance test owned entirely by Hadron.

**Scope:** use a deterministic local fake MCP server implementing
`torque_task_create`. Do not modify or coordinate with the Torque repository.

**Concrete work:**

- Use the graph-native Torque bulk-create workflow fixture with typed array
  input, inferred/explicit dependencies, bounded `for_each` concurrency, MCP
  calls, retry/idempotency policy, transform summary, and declared outputs.
- Register and contract-test the workflow under the `torque` namespace, pin its
  digest in an exposure profile, and resolve its generated MCP tool schema.
- Invoke the pinned tool through Hadron's MCP server and drive the real compiler,
  binder, durable scheduler, MCP executor, value store, transform executor,
  registry, host policy, and output path.
- Assert per-item attempts and outputs, concurrency bound, partial-failure data,
  final typed outputs, effect-derived MCP annotations, persisted provenance, and
  redacted inspection results.
- Run the case through daemon restart during an in-flight attempt or wait and
  prove recovery does not duplicate an idempotent create.

**Acceptance criteria:**

- The workflow runs end to end and is callable as a pinned first-class MCP tool
  named from its namespace and slug.
- The generated input/output schemas match the workflow contract.
- Four-or-configured concurrent item calls never exceed the declared limit.
- Retry and recovery preserve one logical result per input item.
- No output depends on `::set-output` or global log scanning.
- This test is required for the Wave 06 release gate and cannot be replaced by
  separate compiler, runtime, or MCP unit tests.

**Verification:**

- Run the dedicated end-to-end test command added by this task.
- `go test ./workflow/... ./internal/appworkflow/... ./internal/mcpadapter/...`
- `go test -race` for the packages participating in fan-out and MCP dispatch.

## W06-T10 - Move The Workflow UI Off Legacy Wails Contracts

**Objective:** preserve the React/xyflow operator experience while replacing
Wails-specific workflow contracts with a daemon-served, transport-neutral web UI
architecture.

**Current code refs:** `../../../../cmd/hadron-app`,
`../../../../cmd/hadron-app/frontend/src/api`,
`../../../../cmd/hadron-app/frontend/src/pages/FlowBuilderPage.tsx`, and
`../../../../cmd/hadron-app/frontend/wailsjs`.

**Concrete work:**

- Make the workflow frontend consume the HTTP/event APIs from W06-T02 for all
  graph, run, wait, registry, exposure, and authoring operations.
- Replace Wails-generated workflow DTOs, direct Go bindings, and Wails runtime
  events with generated/shared API types and transport-neutral subscriptions.
- Serve the built SPA through the Hadron daemon or the repository's selected
  embedded web-UI host so it works in a normal browser without Wails.
- Preserve and adapt the `@xyflow/react` canvas, node positions, editing and run
  inspection behavior from W06-T05.
- Remove the Wails workflow backend and generated binding dependency after
  parity tests pass; any retained desktop shell must be a thin optional client
  of the same daemon/web surface.
- Update build, install, and architecture documentation to identify the new UI
  source of truth and remove stale Wails workflow guidance.

**Acceptance criteria:**

- The complete workflow UI runs in a normal browser against `hadrond` with no
  Wails global, generated Wails model, or desktop-only workflow API.
- A retained desktop wrapper adds no workflow semantics and can be removed
  without changing the frontend application.
- The graph canvas and active/failed/waiting/completed run views retain parity
  with the W06-T05 acceptance cases.
- Headless Hadron builds do not require Wails to expose the operator UI.

**Verification:**

- Frontend unit/build commands from `cmd/hadron-app/frontend/package.json`.
- Playwright coverage in a browser against a local Hadron daemon for graph edit,
  run inspection, wait resume, registry/exposure inspection, and replay.
- `rg "wailsjs|window.runtime|github.com/wailsapp/wails" cmd/hadron-app` and
  review any retained matches as optional shell-only code.
- `go test ./...`
