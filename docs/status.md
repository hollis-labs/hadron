# Status

Phase 0 of the port plan (Tesseract `plan_hadron_port_to_substrate_20261104`) is done: the skeleton runs.

## Ported

Nothing yet. The shell comes from Substrate.

## Next

1. Persistence: one squashed goose schema (SQLite) for the new app; start empty, no converter (owner decision 2026-11-04). Workflow definitions come across through the registry and pack format, not the database.
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
