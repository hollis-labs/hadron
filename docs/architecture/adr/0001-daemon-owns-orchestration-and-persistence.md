# ADR 0001: Daemon Owns Orchestration and Persistence

**Status:** Accepted<br>
**Date:** 2026-05-12

## Context

Hadron has multiple operator surfaces: the `hadron` CLI, the Wails desktop app,
the MCP adapter, and direct HTTP clients. Each surface can initiate or inspect
blueprint runs, schedules, triggers, workspaces, and pipelines. If those clients
owned independent execution or persistence logic, run state would diverge and
beta behavior would depend on the entry point used.

The current daemon startup path opens the SQLite store, loads settings, starts
execution, scheduler, trigger, pipeline, telemetry, and API services, and then
serves the shared API.

## Decision

`hadrond` is the owner of orchestration and persistence. It is responsible for:

- opening and migrating the SQLite store
- persisting runs, run events, schedules, triggers, workspaces, and pipeline state
- owning the execution manager and background services
- enforcing runtime settings for execution
- exposing state and mutations through the HTTP API and MCP adapter

Clients may validate inputs, resolve local files, and present state, but they
must not create parallel durable state or bypass daemon-owned execution paths.

## Consequences

This keeps beta behavior consistent across CLI, desktop, MCP, and API usage. It
also means daemon availability is part of the local product contract: clients
must handle daemon startup, connection errors, and version/API compatibility
cleanly instead of silently falling back to alternate execution behavior.

Backend refactors should preserve this ownership model while splitting large
implementation files into smaller route, store, scheduler, trigger, execution,
and pipeline units.
