# Wave 07 - Later Capability Tasks

**Purpose:** park important capabilities that should not block the first durable
graph runtime unless priority changes.
**Primary architecture refs:** [step kinds and executors](../../../architecture/workflow-engine-future-state/06-step-kinds-executors.md), [Hadron surfaces](../../../architecture/workflow-engine-future-state/08-hadron-app-service-surfaces.md), [consumer boundaries](../../../architecture/workflow-engine-future-state/09-consumer-boundaries-extraction.md).

## W07-T01 - Add Provider-Agnostic `llm` Step Kind

**Objective:** support model calls behind a stable workflow contract without
importing concrete providers into core.

**Entry criteria:** step-kind registry, typed values, redaction, policy, and
Hadron host binding are stable.

**Concrete work:**

- Define provider-agnostic `llm` config for host/profile provider selection,
  system/input messages, optional context assembly, exact tool allowlist,
  maximum tool iterations, output schema, budget, timeout, and streaming.
- Return typed output, raw text, literal `ToolCallRecord` entries, usage, stop
  reason, audit metadata, and provider/model binding provenance.
- Implement Hadron-owned adapter bindings for selected provider paths such as
  direct `go-providers` or `agentkit`, and document the public executor contract
  that a downstream Nanite harness adapter could implement without adding that
  adapter here.
- Enforce the node tool allowlist before dispatch and again when processing model
  tool-use output; unknown or out-of-scope tools fail closed.
- Enforce `output_schema` with an explicit repair-or-fail policy and integrate
  W04-T08 verification without creating an LLM-specific orchestration path.
- Distinguish provider/infrastructure errors, tool errors, schema failures, and
  model/verification decision failures in persisted state.
- Add prompt/input/output redaction and retention rules.
- Add policy checks for model/provider/tool access.

**Acceptance criteria:**

- Core runtime imports no concrete model provider dependencies.
- LLM outputs are typed and redacted consistently with other values.
- Provider-specific behavior stays behind adapters.
- Tool-call audit records reflect literal activity and cannot be supplied only by
  model self-report.
- Output schema, budget, timeout, restricted tools, and verification are covered
  by conformance tests.

**Verification:**

- `go test ./workflow/adapters/llm/... ./workflow/runtime/...`
- Adapter contract tests using a deterministic fake provider for tool allowlist,
  schema repair/fail, usage, stop reason, timeout, redaction, and verification.

## W07-T02 - Add Goja-Backed `script` Step Kind

**Objective:** support local deterministic data manipulation beyond expression
bindings after sandbox limits are explicit.

**Entry criteria:** expression engine and policy hooks are stable.

**Concrete work:**

- Implement goja JavaScript executor with resource limits.
- Restrict host access to approved value inputs and explicit returned outputs.
- Define deterministic input/output schema declaration or inference for exported
  functions so scripts can become typed workflow/tool boundaries.
- Expose any `hadron` helper object only through capability-checked modules such
  as approved HTTP or MCP calls; filesystem, network, module loading, clocks,
  randomness, and secrets are denied unless explicitly bound by policy.
- Add deterministic timeout, cancellation, and error mapping.
- Document why Python subprocess support remains a separate decision.

**Acceptance criteria:**

- Script nodes cannot access filesystem, network, or secrets unless explicit
  adapter policy allows it.
- Outputs are typed values and errors are source-mapped.
- Time, memory, module, and cancellation limits have deterministic tests.

**Verification:**

- `go test ./workflow/adapters/script/... ./workflow/runtime/...`
- Sandbox escape, timeout, memory, module, schema, cancellation, and capability
  tests using deterministic fixtures.

## W07-T03 - Reintroduce `agent_launch` As Workflow Sugar

**Objective:** preserve useful agent-launch workflows through ordinary graph
semantics instead of making agent launch a core primitive.

**Entry criteria:** `call`, waits, message waits, and any selected Hadron-owned
LLM/agent-substrate adapters are available.

**Concrete work:**

- Model agent launch as a composed child workflow or adapter-backed step kind.
- Use wait records for agent completion, callback, or message correlation.
- Implement `wait: true` sugar as launch plus correlated message/callback wait
  using returned handles and the parent run correlation ID.
- Apply heartbeat, timeout, cancellation, and parent-close policy through the
  ordinary runtime contracts.
- Map agent results to typed outputs.
- Keep Nanite-specific agent/session/team semantics outside core.

**Acceptance criteria:**

- Agent launch does not reintroduce a separate execution engine.
- Cancellation, timeout, wait, and output behavior match graph runtime rules.
- Fire-and-forget and wait-for-result modes both return typed handles with
  explicit lifecycle policy.

**Verification:**

- `go test ./workflow/adapters/agent/... ./workflow/wait/...`
- Integration fixtures for fire-and-forget, wait sugar, correlated reply,
  heartbeat timeout, parent cancellation, abandon, and restart recovery.

## W07-T04 - Add Generated API Client Step Families

**Objective:** support OpenAPI, AsyncAPI, gRPC, or GraphQL calls as generated
adapters or workflow source sugar after the base `http` and `mcp` paths prove
the model.

**Entry criteria:** HTTP executor, MCP executor, schema generation, and policy
effects are stable.

**Concrete work:**

- Decide whether generated calls are first-class step kinds, generated
  workflow definitions, or config sugar over `http`/`mcp`.
- Generate schemas, typed inputs, typed outputs, auth refs, and effect metadata
  from source specs.
- Add conformance fixtures for generated operation calls.

**Acceptance criteria:**

- Generated operation support does not require core runtime changes.
- Effect and credential policy remains visible before execution.

**Verification:**

- `go test ./workflow/adapters/... ./workflow/compile/...`
- Generation snapshots and fake-server contract tests for every selected spec
  family.

## W07-T05 - Add Compiled/Offline Workflow Build Path

**Objective:** allow selected workflows to build into daemon-less binaries or
MCP servers where no daemon service is needed.

**Entry criteria:** graph schema, step-kind metadata, and adapter dependency
contracts are stable.

**Concrete work:**

- Define build-time validation for daemon-less compatibility.
- Allow pure/read/compute/materialize nodes that do not require wait services.
- Allow MCP and LLM only with explicit external config bindings.
- Reject gates, messages, and callback waits unless a remote daemon binding is
  configured.
- Produce build artifacts with embedded plan digest and schema metadata.
- Generate CLI flags from workflow inputs and typed JSON on stdout from workflow
  outputs; support a stdio `--as mcp-server` artifact exposing one generated
  tool.
- Use an in-memory state implementation for the supported subset and require
  explicit external bindings for MCP/LLM or remote wait services.

**Acceptance criteria:**

- Unsupported daemon-dependent nodes fail at build time with diagnostics.
- Built artifacts execute the same graph semantics for the supported subset.
- Rebuilding the same plan and bindings is reproducible and carries the embedded
  plan digest and input/output schemas.

**Verification:**

- Build and execute a pure CLI workflow artifact and a stdio MCP-server artifact.
- Compare outputs and diagnostics with the daemon-hosted execution of the same
  plan.

## W07-T06 - Finalize Public Engine Boundary And Downstream Adoption Kit

**Objective:** finish a stable, extraction-ready public engine boundary in the
Hadron repository and publish everything downstream applications need to adopt
it later without coordinating or editing those applications in this plan.

**Entry criteria:** Hadron has shipped graph-native runtime paths and
conformance coverage is meaningful.

**Concrete work:**

- Freeze and document the public `workflow/*` package boundary, host/store/timer
  interfaces, step-kind registry, schema versioning, and compatibility policy.
- Publish a downstream adoption guide, minimal host example, fake adapters, and
  callable conformance-suite entry points from this repository.
- Add API and import-surface checks that prevent Hadron `internal/*` types or
  product-owned semantics from leaking into the public contract.
- Document semantic/versioning rules and the future extraction criteria for a
  dedicated shared module without creating or modifying another repository.
- Record Nanite, Torque, Cerberus, and other application adoption as explicitly
  downstream/out of scope; do not add their adapters or change their code.

**Acceptance criteria:**

- Shared core has no Hadron `internal/*` imports.
- An external Go host can compile the minimal example and invoke the conformance
  suites using only public Hadron module packages.
- Product-owned semantics stay in product packages.
- This task modifies only the Hadron repository.

**Verification:**

- `go test ./workflow/...`
- Compile the public-host example as an external-package test.
- `go list -deps ./workflow/...` and API-surface snapshot review.

## W07-T07 - Add `emit` And `checkpoint` Step Kinds

**Objective:** complete the standard step-kind vocabulary for explicit event
publication and domain checkpoint integration over generic effects and waits.

**Entry criteria:** step registry, typed values, wait contracts, policy, and
Hadron host bindings are stable.

**Concrete work:**

- Implement `emit` as an adapter-backed event/message publication with declared
  destination capability, effects, idempotency, typed envelope output, and
  redacted observations.
- Implement `checkpoint` through the shared gate/checkpoint package over
  `WaitRecord`, keeping task/work-item lifecycle and escalation policy outside
  core.
- Support checkpoint prompt/schema, responder, correlation, environment/policy
  subject, timeout, escalation metadata, and typed decision output.
- Add compiler schemas, executor metadata, policy hooks, and conformance fixtures
  for both kinds.

**Acceptance criteria:**

- Event publication is explicit workflow work with normal retry, effect, audit,
  and output semantics.
- Checkpoints suspend without holding workers and do not import Torque or another
  product's lifecycle types.
- Hosts can bind different event and checkpoint adapters through public
  interfaces.

**Verification:**

- `go test ./workflow/adapters/emit/... ./workflow/adapters/checkpoint/... ./workflow/wait/...`
- Fake-adapter tests for idempotent emit, checkpoint resume, timeout, escalation,
  cancellation, redaction, and denied authority.

## W07-T08 - Add Reactor, Signal, And Durability Controls

**Objective:** support long-lived event consumers and high-volume short runs
without unbounded histories or one mandatory persistence mode.

**Entry criteria:** activation registrations, generic waits, crash recovery, and
the standardized timed activation substrate are stable.

**Concrete work:**

- Generalize `wait_for` to named typed signals and expose read-only run queries
  plus tracked/idempotent update operations through the host service.
- Implement reactor workflows driven by `on.message` or event activations using
  ordinary run/wait semantics.
- Add `continue_as_new` policy that rolls long-lived history into a new run while
  preserving logical reactor identity, correlation, provenance, and selected
  state.
- Add `durability: none | steps` with compile/bind validation: non-durable mode
  is limited to compatible effects and node kinds and must preserve the same
  graph semantics.
- Add a host-level `on_run_failed` workflow hook with recursion protection,
  immutable definition binding, and explicit effect/policy handling.

**Acceptance criteria:**

- Signals, gate/message resumes, callbacks, timers, and child completion use the
  common idempotent resume/update path.
- A reactor can roll history without losing correlation or processing one event
  twice under the declared delivery policy.
- Non-durable mode refuses waits or unsafe effects and matches durable outputs
  for the supported subset.
- Global failure handling cannot recurse indefinitely or bypass policy.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/wait/... ./internal/appworkflow/...`
- Long-history reactor, continue-as-new, duplicate signal, non-durable parity,
  and recursive-failure-handler integration tests.

## W07-T09 - Add Advanced Graph Authoring And Service-Node Semantics

**Objective:** add higher-level authoring conveniences and service-style nodes
as lowering rules over the existing graph rather than alternate runtimes.

**Entry criteria:** dependency inference, switch/finally, fan-out, waits, and
child-call semantics are stable.

**Concrete work:**

- Add static matrix authoring sugar with include/exclude, fail-fast, and maximum
  parallelism, lowered to ordinary `for_each` invocations.
- Add explicit join/fan-in and sequential-group sugar that lower to graph
  dependencies and readiness rules.
- Add daemon/service nodes with readiness/heartbeat observation and guaranteed
  finally-based teardown for dependent sibling work.
- Add dynamic child-definition generation: a node may emit source/IR, but the
  host must validate, digest, authorize, and execute it through `call`; live
  mutation of the bound parent plan remains forbidden.
- Ensure optional/non-blocking manual gates lower to wait/readiness semantics and
  can proceed when not triggered according to an explicit policy.

**Acceptance criteria:**

- Matrix, join, sequential groups, and optional manual steps produce ordinary
  graph IR with no scheduler special case beyond their lowered semantics.
- Service nodes become ready before dependents and are torn down on success,
  failure, timeout, crash recovery, and cancellation.
- Generated child definitions cannot execute before normal validation, policy,
  digest, and provenance checks.

**Verification:**

- `go test ./workflow/compile/... ./workflow/runtime/...`
- Plan snapshots and runtime fixtures for matrix include/exclude, fan-in,
  service readiness/teardown, generated child validation, and optional gates.

## W07-T10 - Add Compensation And Rollback Contracts

**Objective:** resolve and implement application-neutral compensation semantics
for workflows whose materializing or mutating nodes need explicit rollback.

**Entry criteria:** catch/finally, effects, child calls, typed outputs, and
idempotency policy are stable.

**Decision gate:** resolved by
[ADR 0013](../../../architecture/adr/0013-durable-graph-visible-compensation.md),
approved on 2026-08-25. Implementation must preserve its compensation
registration, ordering, failure policy, persistence, replay, cancellation,
child-run, and `finally` semantics.

**Concrete work:**

- Define compensation handlers as graph-visible nodes or call references with
  typed access to the original result/error and explicit effects.
- Persist compensation eligibility, order, attempts, outputs, and failures as
  operational state.
- Define reverse dependency/order behavior, partial-compensation reporting,
  nested child-run behavior, and interaction with retries, replay, cancel, and
  finally.
- Add policy checks that prevent compensation from being advertised when an
  executor cannot truthfully undo the effect.
- Keep connector-specific rollback actions in adapters or child workflows.

**Acceptance criteria:**

- The accepted ADR and implementation define one explainable compensation model
  without importing Cerberus or another product.
- Compensation attempts are durable, idempotency-aware, inspectable, and cannot
  erase the original failure history.
- Partial or failed rollback has a distinct terminal/reporting outcome.

**Verification:**

- `go test ./workflow/runtime/... ./workflow/compile/...`
- Conformance fixtures for ordered rollback, nested calls, partial failure,
  cancellation, replay, and unsupported compensation claims.

## W07-T11 - Add SDK And Agent-Authored IR Front Ends

**Objective:** let Go, generated TypeScript clients, UI tooling, and agents
produce or invoke the same validated graph contract without creating a new
workflow language.

**Entry criteria:** graph JSON Schema, public engine API, definition resolution,
and CLI/HTTP contracts are stable.

**Concrete work:**

- Provide a Go builder/API that produces canonical graph/source structures and
  compiles through the normal validator.
- Generate a TypeScript schema/client surface for authoring and Hadron API calls
  from committed schemas rather than hand-maintained parallel types.
- Accept agent-emitted IR/source only through the normal parse, schema,
  validation, policy, digest, provenance, contract-test, and registration path.
- Treat any fluent/chaining API as a view over graph IR; do not add mutable
  in-flight plan execution or a separate programming language runtime.
- Add schema/version negotiation and compact diagnostics suited to generated
  clients and agents.

**Acceptance criteria:**

- Equivalent YAML, Go-builder, TypeScript-generated, UI, and agent-emitted inputs
  compile to semantically equivalent plan digests after canonicalization.
- Generated client artifacts fail CI when stale.
- No front end can bypass definition authority, validation, effects, or policy.

**Verification:**

- `go test ./workflow/... ./internal/api/...`
- TypeScript generation/build/test command selected by the repository.
- Cross-front-end golden tests comparing canonical plan JSON and diagnostics.
