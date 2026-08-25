# Embed the workflow engine in a Go host

Hadron's reusable engine boundary is the complete public
`github.com/hollis-labs/hadron/workflow/...` tree. It contains graph and source
contracts, compiler phases, typed values, runtime state machines, waits,
verification, step-kind SDKs and adapters, offline embedding, and conformance
fixtures. It has no dependency on `github.com/hollis-labs/hadron/internal/...`.

Hadron application services, registry publication, authentication, HTTP, MCP,
A2A, SQLite, workers, and the stock daemon capability profile are host
composition. They are deliberately not engine dependencies.

## Minimal path

The executable external-package example is
[`workflow/offline/adoption_external_test.go`](../workflow/offline/adoption_external_test.go).
It performs the complete portable sequence:

1. load one bounded graph-native source with `compile.LoadBytes`;
2. lower it with `compile.Compile`;
3. infer static expression dependencies with
   `compile.InferValueDependencies`;
4. register an exact `StepKind` implementation in `stepkind.MemoryRegistry`;
5. validate the inferred plan with `compile.ValidatePlan`;
6. build an immutable offline manifest and execute it through the ordinary
   runtime with `offline.Execute`; and
7. call the exhaustive `conformance.RunComplete` entry point from outside the
   conformance package.

`offline.Execute` is the smallest embedded host loop. It uses the canonical
binding, recovery, ready-queue, dispatch, wait, and output-finalization paths;
it is not a second interpreter. Use `offline.ExecuteWithStore` to supply a
host store.

## Host-owned bindings

A long-lived host normally supplies these seams:

| Concern | Public contract | Host responsibility |
|---|---|---|
| State | `runtime.StateStore` and narrower enabled-feature interfaces | Durable CAS, idempotency, append-only events, recovery queries, defensive ownership, and process coordination |
| Timers | `wait.ActivationScheduler` | Idempotent `Schedule`/`Cancel` by activation identity and restart recovery |
| Wait endpoints | `wait.Materializer` and `wait.ResponderAuthorizer` | Endpoint lifecycle and authenticated responder policy without persisting raw tokens |
| Kinds | `stepkind.Registry` and `stepkind.StepKind` | Freeze exact name/version/spec implementations before validation and execution |
| Policy | compile policy hooks and runtime authorization interfaces | Bind trusted host identity and evaluate immutable effects/capabilities; graph config cannot authorize itself |
| Artifacts/secrets | `values.ArtifactStore` and adapter-specific secret authorities | Keep artifact bytes and resolved credentials outside persisted value envelopes |
| Verification | `verification.Registry` | Freeze exact verifier contracts with the plan and runtime catalog |

`runtime/inmemory.Store` is the public concurrency-safe reference store used by
offline and contract qualification. It implements the runtime storage
semantics, but its durability is only the lifetime of one Go value. It does not
survive restart, reopen across processes, or satisfy a host's crash-recovery
promise. `runtime/runtimetest` is a deprecated source-compatible alias; new
code imports `runtime/inmemory`.

The engine does not prescribe a worker pool, database, HTTP server, principal,
registry, or scheduler implementation. A host must keep one exact plan and
kind catalog behind recovery, load nodes from that pinned plan rather than a
mutable source, and fence effectful execution with its own policy and durable
claims.

## Step-kind catalog

Every kind publishes an immutable `stepkind.StepKindSpec` before execution:
exact name/version, config/input/output schemas, effects, capabilities,
idempotency, retry safety, cancellation, observation, suspension, and optional
lifecycle hooks. `stepkind.Resolve` never selects a latest version when more
than one version exists. Register all implementations, verify the advertised
specs, then treat the registry as frozen for a plan's lifetime.

`workflow/stepkind/stepkindtest` provides public application-neutral fake kinds
for downstream tests. Concrete packages under `workflow/adapters` are optional
embeddable capabilities; importing an adapter does not enable it. The stock
Hadron daemon's six-kind profile is a product-host choice, not an engine limit.

## Schema and compatibility policy

The public formats are versioned independently and matched exactly:

- graph source uses the generated schema at
  [`workflow/graph/schema/workflow.schema.json`](../workflow/graph/schema/workflow.schema.json);
- compiled plans carry `compile.ExecutionPlanSchemaVersion` and immutable content
  digests;
- offline artifacts carry `offline.ManifestSchemaVersion`; and
- step kinds, definitions, verifiers, and adapters use exact declared
  versions and immutable schema/effect metadata.

Do not infer compatibility from a digest, choose a latest version, or accept an
unknown schema version. Additive Go APIs and optional schema fields may be
introduced compatibly. Removing or changing an exported contract, persisted
meaning, enum, required field, or exact-version behavior requires an explicit
versioned contract decision, updated conformance fixtures, and migration or
compatibility evidence appropriate to that boundary.

[`workflow/public-api.txt`](../workflow/public-api.txt) snapshots exported Go
declarations for every public `workflow/...` package, including adapters. The
import/API guard fails on unreviewed drift, Hadron-internal dependencies, or
unapproved core dependencies. The snapshot is change control while the engine
remains in the Hadron module; downstream consumers should pin a reviewed module
revision. It is not a claim that every concrete adapter is enabled by every
host.

Regenerate the graph schema and deliberately refresh a reviewed API change:

```sh
go generate ./workflow/graph
UPDATE_WORKFLOW_API=1 go test ./workflow/internal/importguard
git diff -- workflow/graph/schema workflow/public-api.txt
```

## Conformance

Use the embedded, bounded fixture store and isolated factories:

```go
conformance.RunRequired(t, conformance.EmbeddedFixtures(), requiredHost)
conformance.RunComplete(t, conformance.EmbeddedFixtures(), completeHost)
```

`RunRequired` covers compiler/source maps, state values, scheduler and control
flow, waits, and step-kind metadata. `RunComplete` additionally covers
verification and memoization and is the truthful exhaustive entry point.
`Host` and `RunAll` remain deprecated source-compatible names for the original
required set so existing adopters are not silently broken.

Fixture inputs are opaque to the harness. Each factory must create an isolated
runner that exercises the downstream implementation; merely matching the
fixture's expected outcome only tests harness wiring, not conformance.

## Extraction readiness

A shared module extraction is appropriate only after all of these stay true
through downstream adoption:

- the entire public tree has zero Hadron-internal or sibling-application
  dependencies;
- graph, plan, value, wait, runtime, and step-kind meanings remain stable under
  the API/schema guards and complete conformance suites;
- at least one non-Hadron host implements durable store/timer/policy/executor
  seams without importing Hadron product services;
- package names and dependency roots are stable enough for a module-path move;
  and
- the extraction can preserve exact version/digest and migration behavior
  without a second runtime or compatibility interpreter.

Nanite, Torque, Cerberus, and other applications are downstream consumers, not
owners of this task. Their adoption and any eventual repository split require
separate work and are intentionally out of scope here.
