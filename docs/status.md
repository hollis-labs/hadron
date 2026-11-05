# Status

Phases 0 and 1 of the port plan (Tesseract `plan_hadron_port_to_substrate_20261104`) are done: the skeleton runs and the schema is squashed and applied through Substrate.

## Ported

- **Schema** (`internal/persistence/migrations/sqlite/00001_init.sql`): the old app's workflow engine host state, squashed from its migrations 0001-0033 into one goose migration: the `workflow_*` family plus `store_generation`, with their indexes and the immutability triggers. Its DDL was checked object by object against the old final schema (204 objects, none missing, none different), and its two seed rows (`store_generation`, `workflow_event_outbox_counter`) are in the file; it is applied by Substrate's `database` module at start.
- Until the first release the schema lives in that one file and a change edits it; there are no deployed databases. After the first release changes are new numbered files.

- **Workflow state store** (`internal/persistence/workflow_*.go`, `event_outbox.go`): the whole `WorkflowStateStore` family from the old app (runs, node invocations, attempts, claims and leases, events, waits, fanout, retry, cancellation, control flow, memoization, scheduler resources, reactors, services, activations, compensation, credentials, exposure, A2A tasks, plan snapshots, host store) with its tests, 17k lines of code and 8k of tests. It is one tightly coupled implementation of the engine runtime's `StateStore`, so it came across as a unit rather than as separate run-lifecycle and wait slices. It opens through `NewStore(db)` on the database Substrate's module opened; tests use `Open(path)` (test-only, libs `core/store` plus the embedded migration).
- **`internal/appworkflow/hoststate`** (the host-side contracts the store speaks) and **`internal/eventmap`** (engine events to harness events), copied with their tests because the store depends on them. `internal/testdb` is the generic test-database helper.
- Tests dropped because they asserted the old migration history, which no longer exists: the `schema_migrations` version counts, the migration 0015 legacy-wait backfill and the migration 0031 credential-digest backfill. One test changed: foreign keys are now ON (the database module's standard) where the old store ran with them off; the store's own parent checks still reject orphans and the whole suite passes.

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

- `policy.AllowAll` is a placeholder PDP until the engine host is ported (see the TODO in `cmd/hadron/main.go`).
- `go.mod` requires an untagged Substrate commit; move to a tag when the owner tags the next release.
