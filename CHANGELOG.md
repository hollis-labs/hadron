# Changelog

All notable changes to Hadron are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Pre-1.0: minor bumps for additive surface, patch bumps for fixes — breaking changes can land in any minor, and beta releases carry no compatibility promise.

History before `v0.4.0` (March–May 2026) is in the git log and is not backfilled; it covers the earlier blueprint/pipeline generation of the project, which is now archive material. This file is good-faith, not exhaustive.

## [Unreleased]

### Changed

- The MCP adapter dropped `mark3labs/mcp-go` in favor of the official-SDK
  `go-mcp` wrapper.
- API and MCP tool handlers were split into smaller files; obsolete
  persistence queue-entries table removed.
- README rewritten as a pre-release identity and stack-fit document.

### Fixed

- Lint findings (shadowed `err`, builtin shadowing), a flaky file-watch
  debounce test, and the agent-substrate kickoff reply lifecycle wait.

### Added

- Open-source project documents: `CHANGELOG.md`, `CONTRIBUTING.md`,
  `SECURITY.md`, `TRADEMARK.md`.

### Removed

- `CLAUDE.md` (agent guidance lives in `AGENTS.md`).

## [0.5.0-beta.2] - 2026-09-04

### Changed

- Releases are published through immutable draft releases in CI.
- Workflow conformance documentation qualified as exhaustive-for-scope.

## [0.5.0-beta.1] - 2026-09-04

### Added

- Graph-native workflow host on the pinned `go-workflow` engine: one
  authenticated application path (`internal/appworkflow`) shared by CLI, HTTP
  API, browser UI, MCP, A2A, schedules and external activations.
- Six frozen production step kinds: `transform@v1`, `script@v1`, `sleep@v1`,
  `wait_for@v1`, `message_wait@v1`, `human_gate@v1`.
- Durable graph-visible compensation, and a public engine boundary
  (`workflow/*`) that was later extracted as `go-workflow`.

### Changed

- Legacy blueprint and pipeline code is archived and no longer mounted by
  production defaults.

## [0.4.2-beta.1] - 2026-05-24

### Added

- Tagged release packaging, a hardened beta release surface, and the AI
  workflow runtime.

## [0.4.1] - 2026-05-15

### Fixed

- Telemetry Refresh button now goes through the async hook and load failures
  are surfaced instead of silently emptying the list.
- Wizard sections with no valid tasks are omitted from serialized YAML.
- Stale `EventsOffAll` declaration dropped from the Wails runtime typings.

## [0.4.0] - 2026-05-15

First tagged release: blueprint and pipeline execution, scheduling, MCP tools,
OpenTelemetry instrumentation, and the desktop/browser UI.

[Unreleased]: https://github.com/hollis-labs/hadron/compare/v0.5.0-beta.2...HEAD
[0.5.0-beta.2]: https://github.com/hollis-labs/hadron/compare/v0.5.0-beta.1...v0.5.0-beta.2
[0.5.0-beta.1]: https://github.com/hollis-labs/hadron/compare/v0.4.2-beta.1...v0.5.0-beta.1
[0.4.2-beta.1]: https://github.com/hollis-labs/hadron/compare/v0.4.1...v0.4.2-beta.1
[0.4.1]: https://github.com/hollis-labs/hadron/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/hollis-labs/hadron/releases/tag/v0.4.0
