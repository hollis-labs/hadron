# Status

Phases 0 and 1 of the port plan (Tesseract `plan_hadron_port_to_substrate_20261104`) are done: the skeleton runs and the schema is squashed and applied through Substrate.

## Ported

- **Schema** (`internal/persistence/migrations/sqlite/00001_init.sql`): the old app's workflow engine host state, squashed from its migrations 0001-0033 into one goose migration: the `workflow_*` family plus `store_generation`, with their indexes and the immutability triggers. It was checked object by object against the old final schema (204 objects, none missing, none different) and is applied by Substrate's `database` module at start.
- Until the first release the schema lives in that one file and a change edits it; there are no deployed databases. After the first release changes are new numbered files.

## Schema still to add (with the subsystem that needs it)

These old tables are not in `00001` yet; each is added, redesigned if needed, when its consumer is ported: `schedules` and `triggers` (activation and unattended), `messages` (message waits, agent boot), `workspaces` (workspace commands), `runs` and `run_events` (execution manager). Every `workflow_*` table is already in. `settings` was a file, not a table.
Not carried forward (archive): `blueprint_registry`, `blueprint_versions`, `pipeline_runs`, `pipeline_stage_runs`, `human_gates`, `legacy_schedule_fires`, `legacy_schedule_fire_attempts`, `schema_migrations` (goose has its own version table).
`store_generation` holds one row the old store seeded at open; seeding it moves with the store code.

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

None yet.

## Open

- `policy.AllowAll` is a placeholder PDP until the engine host is ported (see the TODO in `cmd/hadron/main.go`).
- `go.mod` requires an untagged Substrate commit; move to a tag when the owner tags the next release.
