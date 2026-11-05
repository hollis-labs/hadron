# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Phase 2, the engine host: `internal/appworkflow` with `a2a`, `artifacts`, `unattended`, `rundiagnostics` and the live halves of `registry` (workflow catalog and index) and `pack` (workflow package and source), ported with their tests (all passing) and without the blueprint-era code. `internal/persistencetest` opens migrated databases for those tests.

- Phase 2 first slice: the whole workflow state store (`internal/persistence/workflow_*.go`, `event_outbox.go`) with its 8k lines of tests, plus `internal/appworkflow/hoststate`, `internal/eventmap` and `internal/testdb`, ported from the old app and passing against the squashed schema on Substrate's database module with foreign keys on. The Go `go.mod` follows the Substrate BOM (`pins check --app` runs in CI).

- Phase 1: the squashed schema (`internal/persistence`), one goose migration of the workflow engine host state (80 tables, 58 indexes, 66 triggers), verified against the old app's final schema and applied by Substrate's `database` module. Tests cover application, the append-only triggers surviving goose, and verify mode on reopen.

- Phase 0 skeleton on Substrate (`98dfec7`): manifest with the agentic profile plus cli, database (SQLite), queue, scheduler, events and policy; `serve`, `mcp` (stdio), and the framework's `version`, `config` and `health` commands. Binary-level tests cover version, health, serve (health, capabilities, me, graceful stop) and MCP initialize.
