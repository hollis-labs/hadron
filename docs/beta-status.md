# Beta Status

Hadron by Hollis Labs is in active public beta development.

## What Is Ready

- daemon-backed blueprint execution
- validation and linting
- schedules and pipelines
- REST API and MCP adapter
- structured `http_call`, `mcp_call`, `message_wait`, `agent_launch`, and `human_gate` steps
- local-first SQLite-backed persistence and audit trail

## What Still Needs Hardening

- release and install ergonomics beyond the current beta paths
- race-condition cleanup in file-watch trigger handling
- continued docs and operator ergonomics cleanup
- broader release-readiness testing across the desktop app and MCP workflows

## Release Position

Hadron is not yet positioned as a stable 1.0 automation platform.

It is positioned as:

- open source
- MIT licensed
- usable for early adopters
- built in the open from this point forward

## Recommended Expectations

Use Hadron beta when you want:

- a local daemon for deterministic workflow execution
- inspectable automation with a persistent audit trail
- a platform to evaluate or build agent-aware automation patterns

Expect ongoing changes in:

- docs
- packaging
- workflow ergonomics
- some runtime surfaces that are newly landed

## Captured For Later

- a public blueprint marketplace/discovery surface for sharing reusable Hadron blueprints
