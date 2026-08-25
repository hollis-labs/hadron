# Hadron architecture

**Status:** Active implementation map

Hadron is a local-first durable workflow system. The production semantic
boundary is one graph-native application Host; every public surface projects
that host rather than maintaining a transport-local execution model.

## System map

```text
 hadron CLI       browser UI       MCP stdio        A2A / activation HTTP
     │                │                │                       │
     └──────────── bounded authenticated transports ───────────┘
                              │
                    internal/appworkflow
       identity · policy · workflow operations · lifecycle · exposure
                              │
          compile/plan · step-kind registry · runtime · waits · values
                              │
       SQLite persistence · resource scheduler · source activations
                              │
              exact frozen executor/adaptor contracts
```

## Production processes

### `hadrond serve`

The daemon opens durable state, applies migrations, builds one catalog,
resolver, policy, step-kind registry, workflow Host, lifecycle service, worker
pool, scheduler, trigger ingress, API server, and embedded UI. It starts the
activation scheduler before Host readiness and drains workers/timer callbacks
before closing stores. `/v1/health` reflects Host startup/recovery/readiness and
the injected build version.

The default no-token HTTP identity is restricted to loopback remote addresses,
loopback/localhost Host, and safe same-origin behavior. Durable bearer tokens
resolve to local principal/profile records. Transported principal/source fields
cannot override the authenticated binding.

### `hadrond mcp`

MCP stdio composes the same production graph services over the same durable
data model. It requires an explicit token, stores only its digest, and binds a
principal/exposure profile. The catalog and attestor files use cross-process
locking and refresh-visible reads so concurrent `serve` and `mcp` processes do
not overwrite or hide each other's registry state.

### `hadron`

The CLI is an HTTP client. Active roots are `build`, `daemon`, `version`,
`workflow`, and `workspace`. Workflow handlers use shared DTOs and never import
compiler/runtime/storage internals or authorize policy in Cobra.

### Browser UI and `hadron-app`

The React UI is generated-client-backed and served same-origin by `hadrond`.
Its active navigation is Registry, Workflow Graph, and Runs. The optional
`hadron-app` executable only starts/adopts the daemon and opens the same URL; it
does not own workflow semantics.

## Package ownership

- `workflow/graph`: graph IR and generated JSON Schema.
- `workflow/compile`: graph source lowering and immutable execution plan.
- `workflow/stepkind`: frozen executor contract registry.
- `workflow/runtime`: durable run/node transitions, replay, dispatch, and
  recovery.
- `workflow/wait`: durable waits, resume authority, timers, and signals.
- `workflow/values`: typed values, expression binding, artifacts, visibility,
  and redaction.
- `workflow/adapters`: independently embeddable step-kind implementations.
- `internal/appworkflow`: authenticated workflow operations, diagnostics,
  lifecycle, registration, exposure, and activation authority.
- `internal/persistence`: append-only SQLite migrations and workflow stores.
- `internal/api`, `internal/mcpadapter`, `internal/a2a`: bounded transport
  projections.
- `cmd/hadrond`: production composition and six-kind host capability profile.

## Run and recovery flow

1. A transport authenticates and binds identity, scope, and execution target.
2. Appworkflow resolves an exact file or registry definition, compiles and
   validates it against the frozen kind registry, and derives policy facts.
3. Policy admits, confirms, or denies the immutable start intent. Pins bind
   before readiness.
4. The Host durably binds the run and materializes nodes; resource-aware
   scheduling claims ready work using exact pinned recovery plans.
5. Executors emit typed results, durable waits, or safe failures. Lease renewal
   changes lease state without invalidating semantic node-generation CAS.
6. Waits, events, values, and final outputs persist to SQLite. Restart recovery
   reconstructs readiness and timers before reporting healthy.
7. Surfaces render only bounded redacted diagnostics and typed safe DTOs.

## Registry and activation flow

Authoring source is staged only long enough to validate and register an exact
immutable registry version with contract evidence. Operational activations
persist the exact registry execution ref, stable logical source owner, and plan
digest—not the ephemeral authoring locator.

The movable catalog `current` alias, exact registry-version pin, publication,
and exact profile exposure pin are distinct states. Current-alias mutation and
derived activation admission share a lock/fence, so schedules, direct external
starts, and reactor deliveries cannot start a stale version after an alias move.

## Safety boundary

Registered step-kind schemas/effects/capabilities are authoritative. The stock
daemon exposes exactly transform, script, sleep, wait_for, message_wait, and
human_gate at `v1`; other adapters remain embeddable but unavailable there.
Identity, idempotency, policy, qualification, exposure, diagnostics, and
redaction are enforced in appworkflow/Host services, never inferred by a
transport. See [Safety](../safety.md).

## Legacy status

Blueprint/pipeline packages and samples retained in the tree are
archive/rewrite-only behind an internal compatibility switch. Production
commands, default routes, MCP material, and UI navigation do not mount that
runtime, and no public compatibility promise remains. Historical ADRs retain
their original context; current graph-native authority is recorded in the
later workflow ADRs, especially ADR 0011.

Architecture decisions live under [`docs/architecture/adr`](adr/README.md).
