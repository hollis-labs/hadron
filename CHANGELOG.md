# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Phase 1: the squashed schema (`internal/persistence`), one goose migration of the workflow engine host state (80 tables, 58 indexes, 66 triggers), verified against the old app's final schema and applied by Substrate's `database` module. Tests cover application, the append-only triggers surviving goose, and verify mode on reopen.

- Phase 0 skeleton on Substrate (`98dfec7`): manifest with the agentic profile plus cli, database (SQLite), queue, scheduler, events and policy; `serve`, `mcp` (stdio), and the framework's `version`, `config` and `health` commands. Binary-level tests cover version, health, serve (health, capabilities, me, graceful stop) and MCP initialize.
