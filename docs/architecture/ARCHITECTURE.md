# Hadron Architecture

**Version:** 0.1  
**Date:** 2026-04-23  
**Status:** Active draft

Hadron is a local-first blueprint execution system with three primary surfaces:

- `hadrond`: the long-running daemon
- `hadron`: the CLI client
- `cmd/hadron-app`: the Wails desktop app backed by the daemon API

This document is the canonical high-level map for the current system shape.

## 1. System Overview

```
┌──────────────────────────────────────────────────────────────┐
│                         Hadron                               │
│                                                              │
│  ┌──────────────────────┐   ┌─────────────────────────────┐  │
│  │  hadron CLI          │   │  Wails Desktop App          │  │
│  │                      │   │  React + Vite frontend      │  │
│  │  run / validate      │   │  dashboards, runs, flows,   │  │
│  │  schedule / pipeline │   │  pipelines, settings        │  │
│  └──────────┬───────────┘   └──────────────┬──────────────┘  │
│             │                              │                 │
│             └──────────────HTTP API────────┘                 │
│                            /v1/*                             │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐   │
│  │                    hadrond daemon                     │   │
│  │                                                        │   │
│  │  API server      Execution manager    Scheduler       │   │
│  │  Trigger manager Pipeline runner      MCP adapter     │   │
│  └──────────────────────────┬─────────────────────────────┘   │
│                             │                                 │
│                 ┌───────────┴───────────┐                     │
│                 │     SQLite store       │                     │
│                 │ runs, schedules,       │                     │
│                 │ events, workspaces,    │                     │
│                 │ pipelines, triggers    │                     │
│                 └───────────────────────┘                     │
└──────────────────────────────────────────────────────────────┘
```

## 2. Runtime Components

### 2.1 `hadrond`

The daemon is the operational center of the system.

Primary responsibilities:

- expose the HTTP API
- persist runs, events, schedules, triggers, and workspaces
- enqueue and execute blueprint runs
- manage pipeline orchestration
- manage scheduled execution and file/webhook triggers
- expose an MCP adapter for agent-driven usage

Current startup flow:

1. Load config and ensure required directories exist.
2. Open the SQLite store and apply migrations.
3. Load runtime settings.
4. Start telemetry, execution manager, scheduler, and trigger services.
5. Build the HTTP API server with store-backed dependencies.
6. Optionally run as an MCP stdio adapter.

### 2.2 `hadron`

The CLI is a client of the daemon rather than a parallel execution engine.

Primary responsibilities:

- validate and run blueprints
- manage schedules, workspaces, pipelines, and registry interactions
- stream run status and events
- package and unpack blueprint artifacts

This split keeps orchestration and persistence in one place while allowing multiple clients to share the same daemon state.

### 2.3 Wails app

The desktop app provides a richer operator surface over the same daemon API.

Current frontend responsibilities:

- dashboard and run visibility
- blueprint browsing and details
- pipeline authoring and inspection
- flow-builder interactions
- scheduler, settings, telemetry, and help screens

The desktop app should remain a presentation layer over the daemon contract rather than introducing separate orchestration rules.

## 3. Backend Package Layout

High-value backend packages today:

- `internal/api`: HTTP handlers and request routing
- `internal/blueprint`: blueprint parsing, validation, and import handling
- `internal/execution`: run queue, worker pool, subprocess execution, event emission
- `internal/persistence`: SQLite access and migrations
- `internal/pipeline`: multi-stage pipeline orchestration
- `internal/scheduler`: cron-style schedule execution
- `internal/trigger`: webhook, file watch, and TTL-driven triggering
- `internal/mcpadapter`: MCP transport and tool exposure for agent workflows
- `internal/registry`: packaging, pinning, and registry resolution helpers
- `internal/settings`: runtime safety and execution settings

The current package boundaries are meaningful, but several large files should be split further so each package has a clearer internal structure.

## 4. Data Flow

### 4.1 Blueprint run flow

1. A client submits a run request.
2. The API persists the queued run.
3. The execution manager pulls the request from its queue.
4. The blueprint is parsed and executed step by step.
5. Run events and final status are persisted to SQLite.
6. CLI, desktop app, and external clients read status/events from the daemon.

### 4.2 Pipeline flow

1. A client starts a pipeline run.
2. The daemon resolves the pipeline definition and stage order.
3. Each stage submits blueprint runs through the shared execution path.
4. Stage status and outputs are persisted for inspection and downstream stages.

### 4.3 Trigger and schedule flow

1. Schedules and triggers are persisted in the store.
2. Background services watch for cron events, file events, webhook requests, or TTL expiry.
3. Matching triggers enqueue blueprint runs through the same execution manager.

## 5. Persistence Model

SQLite is the source of truth for local daemon state.

Stored concepts include:

- runs and run events
- schedules
- workspaces
- pipeline runs and stage runs
- triggers

The current store layer is functional and well-covered by tests, but it is still too centralized. The next structural improvement should split record types, migrations, and query families into separate files.

## 6. Frontend Architecture

The frontend is currently organized around route-like pages plus shared component families:

- `components/layout`
- `components/ui`
- `components/flow`
- `components/pipelines`
- `components/wizard`
- `pages/*`
- `contexts/*`
- `hooks/*`

This is already enough structure to support a stronger frontend quality baseline. The next modernization step is to add first-class linting, typecheck discipline, and a small test layer, then progressively separate page orchestration from reusable app state and data access patterns.

## 7. Trust and Safety Boundaries

Hadron’s core trust boundary is execution.

Important safety properties:

- blueprint execution must remain mediated by runtime settings and validation
- filesystem access and command execution should stay explicit and inspectable
- daemon-owned persistence should remain the single execution record
- external agent access through MCP should map to the same daemon capabilities rather than bypassing them

The repo already has safety documentation, but the next hardening step is to document which invariants are guaranteed by settings, which are guaranteed by code, and which still rely on convention.

## 8. Current Structural Priorities

The most important architecture follow-ups are:

1. Split `internal/api/server.go` into route registration, handlers, and request/response helpers.
2. Split `internal/persistence/store.go` into migrations plus query families.
3. Split `internal/execution/manager.go` into queue/worker coordination vs subprocess execution details.
4. Keep the ADR set current as major execution, pipeline, and desktop boundaries evolve.

## 9. Architecture Decision Records

Durable architecture decisions are recorded in
[`docs/architecture/adr`](adr/README.md). The initial ADR set covers daemon
ownership, client boundaries, blueprint execution dispatch, pipeline
orchestration, and Wails app layering.
