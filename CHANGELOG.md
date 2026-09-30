# Changelog

All notable changes to Hadron are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Pre-1.0: minor bumps for additive surface, patch bumps for fixes — breaking changes can land in any minor, and beta releases carry no compatibility promise.

History before `v0.4.0` (March–May 2026) is in the git log and is not backfilled; it covers the earlier blueprint/pipeline generation of the project, which is now archive material. This file is good-faith, not exhaustive.

## [Unreleased]

### Security

- **Loopback is no longer trusted (CW-20260930-0234).** Any process on the
  machine, agents included, could previously call `127.0.0.1:8095`, act as the
  operator and confirm its own runs. `hadrond` now creates
  `~/.hadron/operator.token` (0600) on first start and requires it, or a
  browser session, for every workflow operation and workspace change.
  Credential-less loopback gets only `/v1/health` and the UI shell.
  - The `hadron` CLI sends the token automatically (`--token-file`, or
    `HADRON_TOKEN`; prefer the file, since an exported variable is inherited
    by every process started from that shell). New: `hadron ui` opens the
    browser UI through a single-use, 60-second sign-in link (HttpOnly,
    SameSite=Strict session cookie); `hadron auth rotate` replaces the token
    and ends every session. The desktop app signs the browser in the same way.
  - Only the operator can confirm. MCP and exposure tokens that send
    `confirmed: true` get `confirmation_not_permitted` (HTTP 403); an
    explicit grant path for them follows separately. The right is checked at
    confirmation time, not stored in identity bindings, so runs keep their
    owner.
  - Hadron removes `HADRON_TOKEN` from the environment of everything it
    launches.
  - **Limit:** an agent that can read `operator.token` can still act as the
    operator. Closing that needs agent sandboxing (CW-20260930-0237).
  - Transition: `hadrond serve --allow-unauthenticated-loopback` restores the
    old behavior, off by default, with a warning on every use; it will be
    removed. An older `hadron` CLI gets 401 against this daemon.

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

- Graph workflows: `call@v1` child runs (`mode: run`) in the production
  `hadrond` kind set. Call-started children pass the top-level start policy
  gate (plan validation, execution-target capabilities, persisted decision)
  before they are created; `mode: inline` is refused at validation. A
  `wait_for` child_run node on any parent node now wakes when the child ends.
- Graph workflows: `hadrond` reconciles suspended external operations in a
  background loop, and run cancellation cancels explicit-cancel external steps
  instead of reporting cancellation as unsupported.
- `agent_launch` is recognized but refused with a stable "agent_session@v1 is
  not enabled in this hadrond yet" message until a durable session host lands.
- Graph workflows: a Tether-backed `agent_session@v1` session host
  (`internal/tetherhost`). Each agent step is one keyed Tether session; the
  agent returns its result as a nonce-bearing reply that Hadron reads back on
  observe, so launch, observe, and cancel survive `hadrond` and muxd restarts.
  New `tether_session` agent substrate settings (`endpoint`, `launch`,
  `launches`, `result_optional`, `stop_on_result`, `unreachable_timeout`).
  Configuring one lifts the `agent_launch` gate; `hadrond` reaches muxd
  through go-tether-client v0.6.0 and still starts when muxd is down. Typed
  agent inputs are refused at validation.
- Dependencies: `github.com/hollis-labs/go-tether-client` v0.6.0 (new);
  `github.com/hollis-labs/go-messaging` v0.5.1 → v0.5.2 (required by it).
- Root node bindings now resolve `run.id`, which `agent_launch` expansion
  uses for its parent correlation.
- The local `operator:local` execution target now carries `workflow.call` and
  `agent.session.*`. The MCP principal deliberately keeps its previous
  capability set, so MCP tokens bootstrapped before this change keep working
  and no stored grant is widened silently; MCP-started runs cannot use `call`
  or agent nodes until that grant is extended explicitly.
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
