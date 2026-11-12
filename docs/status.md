# Status

Phases 0 and 1 of the port plan (Tesseract `plan_hadron_port_to_substrate_20261104`) are done, and the first composition root (core step kinds, framework config and identity, no agent sessions) runs under `hadron serve`, with the workflow API on `/api/workflows/...` and the workflow tools on MCP (HTTP `/mcp` and stdio `hadron mcp`).

## Ported

- **Schema** (`internal/persistence/migrations/sqlite/00001_init.sql`): the old app's workflow engine host state, squashed from its migrations 0001-0033 into one goose migration: the `workflow_*` family plus `store_generation`, with their indexes and the immutability triggers. Its DDL was checked object by object against the old final schema (204 objects, none missing, none different), and its two seed rows (`store_generation`, `workflow_event_outbox_counter`) are in the file; it is applied by Substrate's `database` module at start.
- Until the first release the schema lives in that one file and a change edits it; there are no deployed databases. After the first release changes are new numbered files.

- **Workflow state store** (`internal/persistence/workflow_*.go`, `event_outbox.go`): the whole `WorkflowStateStore` family from the old app (runs, node invocations, attempts, claims and leases, events, waits, fanout, retry, cancellation, control flow, memoization, scheduler resources, reactors, services, activations, compensation, credentials, exposure, A2A tasks, plan snapshots, host store) with its tests, 17k lines of code and 8k of tests. It is one tightly coupled implementation of the engine runtime's `StateStore`, so it came across as a unit rather than as separate run-lifecycle and wait slices. It opens through `NewStore(db)` on the database Substrate's module opened; tests use `Open(path)` (test-only, libs `core/store` plus the embedded migration).
- **`internal/appworkflow/hoststate`** (the host-side contracts the store speaks) and **`internal/eventmap`** (engine events to harness events), copied with their tests because the store depends on them. `internal/testdb` is the generic test-database helper.
- Tests dropped because they asserted the old migration history, which no longer exists: the `schema_migrations` version counts, the migration 0015 legacy-wait backfill and the migration 0031 credential-digest backfill. One test changed: foreign keys are now ON (the database module's standard) where the old store ran with them off; the store's own parent checks still reject orphans and the whole suite passes.

- **Engine host** (`internal/appworkflow`, 15k lines with tests): the composition root that binds the workflow engine's ports to identity, policy, the store, artifacts and the daemon lifecycle: host, lifecycle, definition sources and resolver, activations, exposure, A2A, compensation, recovery, reactors, operator, unattended, credentials, contracts. Its dependencies came with it: `internal/a2a`, `internal/artifacts`, `internal/unattended`, `internal/rundiagnostics`, and the live halves of `internal/registry` (workflow catalog and index) and `internal/pack` (workflow package and source). All tests pass (`go test ./...`).
- **Left behind in those packages on purpose** (blueprint-era, archive): `registry.go` (blueprint indexing), `pack.go` (the `.hbp` blueprint archive), the legacy run-event summaries in `rundiagnostics` (`SummarizeMCPCalls`, `SummarizeOperations`, which read the old `run_events` table), and the one appworkflow test that drove the old `/v1` HTTP API (`TestWorkflowHTTPRebindsAuthenticatedContextAtAppBoundary`). Its sibling fixtures stay.
- `internal/persistencetest.Open` opens a migrated database for other packages' tests.

## Composition root (`internal/runtime`, owner decisions 2026-11-05, Tesseract `hadron_composition_root_defaults`)

`runtime.New(store, cfg, options...)` builds the engine host, operator, exposure, lifecycle, A2A handler, workflow catalog, activations, node workers and external reconciler; `Start` and `Shutdown` run them. `serve` builds it over the database module's connection and starts it before the HTTP host.

- **Config**: a `hadron:` block in the Substrate config (`data_dir`, `workers`, `event_stream_limit`, `events.outbox`); unknown keys are errors. Replaces the old JSON settings file.
- **Identity**: the framework's verified `Principal` becomes the engine's identity binding (`BindingFromPrincipal`). Scopes `hadron.run`, `hadron.manage`, `hadron.confirm` (and `hadron.*`) become grants; agents never receive the confirm grant; the `off` dial yields the local operator. No local auth code.
- **Source activations**: the framework `scheduler` module over Hadron's activation tables (`RegisterScheduler`: `UseStoreFunc`, `UseRunner`, `UseOptionsFunc` with the claim lease and observer). The runtime starts and stops the engine with the recovered host.
- **Queue**: the `queue` module is not in the manifest; Hadron's claims and workers are its own execution semantics.
- **Agent sessions** (`tetherhost`, `agentsubstrate`, `llmprovider`, `messagesubstrate`) and **hub relay**: not built; `agent_session@v1` stays registered as a gated kind. Later, behind that path.
- **Exposure sessions** (durable token principals the old HTTP/MCP layers minted): not ported; they need redesign with the API and MCP phases, since the framework issues identity now.

## API and MCP (`internal/api`, `internal/mcptools`)

- **HTTP** (`internal/api`): ported from the old `/v1/workflows/...` handlers to `/api/workflows/...`: validate, explain, runs (start, inspect, cancel, resume, rerun, waits, values, events, SSE `events/stream`), activation fire, and the lifecycle routes (catalog search/inspect, author validate/scaffold/test/register, registry package/publish/pin/unpin/clear-current, exposure inspect/pin/unpin). Errors are flat `{code, message, details?}` with a fixed message per code (a hidden definition still looks like a missing one). `api.RegisterScopes` declares a scope per route (`hadron.run`, `hadron.manage`); an unlisted route is unmapped, which the guard refuses in enforce mode. Handlers hold no authentication code: `runtime.RequestAuthenticator` turns the verified principal into the host identity.
- **MCP** (`internal/mcptools`): static tools `hadron_workflow_*` registered on Substrate's MCP server with per-tool scopes; identity from the verified principal on both transports. `hadron mcp` builds the runtime in-process over the same database as the daemon (claims and leases fence two processes).
- **Merged on purpose** (fail forward): `hadron_workflow_run_subscribe` into `run_events` (`wait_ms`), and `run_resume`/`gate_submit`/`message_submit` into `run_resume` with `wake_source`. **Dropped**: `hadron_workflows_search`, `_load`, `_describe`, the dynamically generated per-workflow tools and the namespaces resource; they belonged to exposure sessions (a token's mounted tool profile), which the framework's identity replaces. Whether an agent should get per-workflow tools again is a design decision for the exposure redesign. `hadron_workflow_catalog_search` and `_inspect` cover discovery.
- **Dropped with the old server**: the CORS middleware and its test (the framework's host owns cross-origin policy), the operator sign-in routes, and the credentials routes.
- **A2A** (`internal/a2a`, `internal/agentcard`, `internal/api/a2a.go`): the agent card at `GET /.well-known/agent.json` and the task endpoints under `/a2a/tasks`, mounted through `httphost.Options.Mounts` (the protocol fixes those paths), scoped `hadron.run`. Decision to confirm: the card is **not** public (the old one was unauthenticated); it lists published workflows and their schemas, so it needs a token in enforce mode. Public discovery is a one-field change (`Mount.Open`). The base URL comes from the request host. The blueprint-era `SkillFromBlueprint` was not ported.
- **Not built**: CLI workflow commands (follow-up CW-20261112-0011 covers the thin-client shape), `reveal_private` is passed to the operator, which authorizes it (no exposure-profile display policy).
- **Not verified end to end**: enforce-mode access (a real token against each route and tool). The scope tables are unit-tested; the token flow belongs with the Cerberus/Tether auth setup.

## Not ported yet (rest of phase 2 and later)

Classified from the old import graph, to be confirmed as each is reached: **execution** (`internal/execution`, 1.6k lines: blueprint-era step executors and manager, importing `blueprint`; the engine's libs adapters (`call`, `agent`, `gate`, `llm`, `mcp`, `wait`) are the live path, so likely dropped); **scheduler and trigger** (legacy schedule fires and triggers; compare with the framework's `scheduler` module and the activation tables before porting); **tetherhost, hubrelay, messagesubstrate, agentsubstrate, llmprovider, launchartifacts** (integrations); **localauth** (dropped, replaced by the framework's auth); **settings, config** (replaced by Substrate config); **telemetry** (per-run JSONL, app code); **webui, hadron-app** (dropped).

## Schema still to add (with the subsystem that needs it)

These old tables are not in `00001` yet; each is added, redesigned if needed, when its consumer is ported: `schedules` and `triggers` (activation and unattended), `messages` (message waits, agent boot), `workspaces` (workspace commands), `runs` and `run_events` (execution manager). Every `workflow_*` table is already in. `settings` was a file, not a table.
Not carried forward (archive): `blueprint_registry`, `blueprint_versions`, `pipeline_runs`, `pipeline_stage_runs`, `human_gates`, `legacy_schedule_fires`, `legacy_schedule_fire_attempts`, `schema_migrations` (goose has its own version table).

## Next

1. (done) Persistence schema. The store code (Go) that reads and writes it moves with the engine host. Start empty, no converter (owner decision 2026-11-04); workflow definitions come across through the registry and pack format.
2. Engine host: `appworkflow`, `execution`, `registry`, `pack`, and Hadron's policy decision point.
3. Execution runtime (claims, workers, activations): app code; revisit queue and scheduler adoption with evidence.
4. API on `/api/...` with flat errors, events topics, scopes declared in `ui.json`-style route declarations.
5. MCP tools on both transports. 6. A2A and the agent card via `httphost.Options.Mounts`. 7. CLI workflow commands as a client of the daemon (`auth.HTTPClient`). 8. Frontend via gui-shell. 9. Parity and the owner's cutover decision.

## Decisions

- Name and location: this repo is `hadron-next` until cutover; module path is final. Private until cutover.
- Dropped: `hadron-app`, the archive code, sysop-ui. Start empty. Execution semantics stay app code.

## Cleanup (ported code that does not meet current standards)

Copied as is to get it running and tested; to be cleaned deliberately, not silently:

- Names carry a redundant `workflow` prefix inside package `persistence` (`WorkflowStateStore`, `workflowSQL`, `NewWorkflowStateStore`, about 600 identifiers); the package should read `persistence.StateStore`.
- Several files are 700 to 1500 lines (`workflow_activations.go`, `workflow_compensation.go`, `workflow_state_control_flow.go`); split by concern.
- `workflow_a2a.go` and `workflow_host_*.go` depend on `internal/appworkflow/hoststate`, so persistence imports a higher layer; the dependency should point the other way (contracts in a lower package).
- Timestamps are stored as RFC 3339 text through hand-written helpers; libs `core/store` has a time codec that should replace them.
- The hand-rolled event outbox (`event_outbox.go`) overlaps libs `core/store/outbox`; evaluate and pick one.
- `Store` is a thin wrapper kept so the ported signatures stay; the stores could take `*sql.DB` or core/store's `Querier` directly.

## Open

- `policy.AllowAll` is the framework PDP on purpose: routes are gated by declared scopes and the engine host decides what a run may do. Revisit when the API routes exist.
- `go.mod` requires an untagged Substrate commit; move to a tag when the owner tags the next release.
