# Workflow development guide

## Semantic ownership

Hadron has one graph-native semantic path:

```text
CLI / HTTP / UI / MCP / A2A / activations
                  ↓
        internal/appworkflow
                  ↓
 graph compile + policy + runtime + waits + values
                  ↓
       persistence / scheduler / adapters
```

Transport handlers decode bounded requests, bind trusted authentication, call
application services, and project safe DTOs. They must not import compiler,
runtime, or persistence internals or construct policy-authorizing facts.

Key boundaries:

- [`workflow/graph`](../workflow/graph): canonical graph IR and generated
  source schema.
- [`workflow/compile`](../workflow/compile): source lowering and immutable
  execution plans.
- [`workflow/stepkind`](../workflow/stepkind): executor contracts and frozen
  kind registry.
- [`workflow/runtime`](../workflow/runtime) and
  [`workflow/wait`](../workflow/wait): durable state transitions,
  scheduling, waits, replay, and recovery.
- [`workflow/values`](../workflow/values): typed values, artifacts, expression
  evaluation, visibility, and redaction.
- [`workflow/adapters`](../workflow/adapters): executor implementations and
  adapter conformance tests.
- [`internal/appworkflow`](../internal/appworkflow): authenticated application
  operations and lifecycle.
- [`internal/api`](../internal/api),
  [`internal/mcpadapter`](../internal/mcpadapter),
  [`internal/a2a`](../internal/a2a), and [`cmd/hadron`](../cmd/hadron): transport
  projections over those services.
- [`cmd/hadrond/workflow_runtime.go`](../cmd/hadrond/workflow_runtime.go):
  production composition, capability boundary, workers, activation scheduling,
  and graceful shutdown.

## Adding or embedding an adapter

An adapter declares an immutable `StepKindSpec`: name/version, config/input/
output schemas, effects, capabilities, idempotency, retry safety, cancellation,
observation, and suspension behavior. The host freezes the registry before
validation and execution. Runtime config cannot widen schemas or narrow
effects/capabilities.

Executor tests should prove:

- deterministic validation and immutable specs;
- exact typed input/output behavior and unsafe-number rejection;
- credentials arrive only as `SecretRef` and are resolved at the narrow
  adapter boundary;
- effects and required capabilities are visible before execution;
- idempotency/retry facts match actual transport behavior;
- errors and observations are bounded/redacted;
- cancellation, suspension, and replay follow the declared contract.

The stock daemon currently binds only the six kinds documented in
[Workflow authoring and operations](workflows.md). Adding an adapter package
does not make it a production capability.

## Host binding and conformance

Production composition must reuse the same catalog, resolver, host, policy,
diagnostics, lifecycle, and identity stores across surfaces. Background
activations bind their immutable durable registration identity before calling
the Host. Workers load the exact pinned recovery plan, use resource-aware
admission, renew lease-only state without invalidating semantic node CAS, and
drain before stores close.

Useful focused suites live beside their contract packages. The shared
[`workflow/conformance`](../workflow/conformance) harness and its
[`test fixtures`](../workflow/conformance/testdata/fixtures/README.md) exercise
portable kind contracts. Cross-surface tests
under `internal/appworkflow`, `internal/api`, `internal/mcpadapter`, and
`internal/a2a` prove identity binding, hidden/not-found equivalence, redaction,
idempotency, exact refs, and shared lifecycle evidence. `test/e2e` exercises
the built production daemon and active CLI.

## Generated contracts

Regenerate graph schema:

```sh
go generate ./workflow/graph
go test ./workflow/graph/...
```

When the workflow HTTP contract changes, use its repository generator rather
than editing the JSON Schema or TypeScript client by hand:

```sh
go generate ./internal/api
```

Committed generated artifacts must be byte-stable and frontend code must use
the generated client/types.

## Release checks

Run from a clean checkout:

```sh
make build
make e2e
go test -count=1 ./...
make test-ui
make typecheck
make lint
go generate ./workflow/graph
go test ./workflow/graph/...
go mod tidy
git diff --check
```

After generation and `go mod tidy`, the relevant committed artifacts and module
files must have no diff. Risk-sensitive changes should also repeat focused
suites and run `-race` for runtime, wait, persistence, and application-host
packages.

## Legacy boundary

Blueprint/pipeline parsers and execution code retained under legacy packages
are archive/rewrite-only and not mounted by production defaults. There is no
public compatibility execution path. The one graph-native output compatibility
shim is the explicit `workflow/adapters/cmd` capture mode for exactly one
selected stream using `parse: set-output` plus `compatibility: true`. It is not
a global scanner, emits a deprecation warning, and is unavailable in the stock
daemon because `cmd@v1` is not registered there.
