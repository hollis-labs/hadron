# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Phase 0 skeleton on Substrate (`98dfec7`): manifest with the agentic profile plus cli, database (SQLite), queue, scheduler, events and policy; `serve`, `mcp` (stdio), and the framework's `version`, `config` and `health` commands. Binary-level tests cover version, health, serve (health, capabilities, me, graceful stop) and MCP initialize.
