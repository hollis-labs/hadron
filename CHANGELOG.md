# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `hadron build`, workflow file and `ns/name@version` references, `explain`, and `-o text|json|yaml` output with `file:line:col` diagnostics on the workflow CLI. Every ref-taking command now takes one reference instead of NAME VERSION DIGEST.
- API schema v2 (`internal/api/schema`), HTTP trace propagation with an opt-in exporter (`HADRON_OTEL`), and the workflow examples with an acceptance test.

- Agent sessions: `agent_session@v1` and `agent_launch` on Tether sessions (`internal/tetherhost`, `hadron.agent_substrates` config), with the old behaviour tests (restart, cancel, unreachable daemon, replay). Substrate pin moves to `9c1da88` (BOM adds libs `mesh/core`).

- The rest of the workflow CLI (resume, signal, rerun, waits, values, authoring, registry, exposure) and a `POST /api/workflows/runs/{id}/signal` route (signal was MCP-only before).

- `hadron workflow run|validate|inspect|cancel|events|search`: operator commands that call the running daemon's `/api/workflows` routes with the framework's outbound token. Catalog search takes a required namespace (CLI flag and MCP argument).

- A2A: the agent card (`/.well-known/agent.json`) and task endpoints (`/a2a/tasks`...) on the Substrate host through `Mounts`, scoped `hadron.run`; `internal/a2a` and `internal/agentcard` ported with their tests (without the blueprint-era skill builder).

- Workflow API on `/api/workflows/...` (`internal/api`, ported from the old `/v1` handlers with their tests): runs, events and SSE stream, activations, lifecycle; flat `{code, message, details?}` errors; per-route scopes `hadron.run` and `hadron.manage`. Workflow tools on MCP (`internal/mcptools`): static `hadron_workflow_*` tools over HTTP `/mcp` and stdio `hadron mcp`, scoped per tool. `data_dir` is resolved to a real absolute path at config load (the artifact store refuses symlinked paths).

- First composition root (`internal/runtime`, `internal/activation`): the engine host, operator, exposure, lifecycle, A2A handler, catalog, activations, workers and external reconciler, built over Substrate's database, with a validated `hadron:` config block, Principal-to-grants identity, and source activations on the framework `scheduler` module. `hadron serve` builds, starts and shuts it down. The `queue` module left the manifest. Agent sessions, hub relay and exposure sessions are not built yet (see docs/status.md).

- Phase 2, the engine host: `internal/appworkflow` with `a2a`, `artifacts`, `unattended`, `rundiagnostics` and the live halves of `registry` (workflow catalog and index) and `pack` (workflow package and source), ported with their tests (all passing) and without the blueprint-era code. `internal/persistencetest` opens migrated databases for those tests.

- Phase 2 first slice: the whole workflow state store (`internal/persistence/workflow_*.go`, `event_outbox.go`) with its 8k lines of tests, plus `internal/appworkflow/hoststate`, `internal/eventmap` and `internal/testdb`, ported from the old app and passing against the squashed schema on Substrate's database module with foreign keys on. The Go `go.mod` follows the Substrate BOM (`pins check --app` runs in CI).

- Phase 1: the squashed schema (`internal/persistence`), one goose migration of the workflow engine host state (80 tables, 58 indexes, 66 triggers), verified against the old app's final schema and applied by Substrate's `database` module. Tests cover application, the append-only triggers surviving goose, and verify mode on reopen.

- Phase 0 skeleton on Substrate (`98dfec7`): manifest with the agentic profile plus cli, database (SQLite), queue, scheduler, events and policy; `serve`, `mcp` (stdio), and the framework's `version`, `config` and `health` commands. Binary-level tests cover version, health, serve (health, capabilities, me, graceful stop) and MCP initialize.
