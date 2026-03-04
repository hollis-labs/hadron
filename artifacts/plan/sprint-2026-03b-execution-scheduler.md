---
type: sprint-plan
id: sprint-HAD-03b
title: "Sprint B — Execution Engine + Scheduler"
status: todo
created_at: 2026-02-28
---

# Sprint B: Execution Engine + Scheduler

## Goal

Implement the PTY-based execution engine, cron scheduler, and pipeline runner. By end of sprint, a blueprint can be loaded, rendered, and executed end-to-end in a test harness, with results persisted to SQLite.

## Entry Criteria

- Sprint A complete: spec, persistence packages build and test cleanly

## Exit Criteria

- `go test ./internal/execution/... ./internal/scheduler/... ./internal/pipeline/...` pass
- Integration test: load reference blueprint → normalize inputs → render → execute → events persisted to SQLite
- Scheduler engine starts, ticks, and can claim/dispatch a due schedule
- Pipeline runner executes a 2-stage pipeline, persisting stage run records

## Tasks

- TASK-HAD-20260228-006: `settings` package — safety validation (allowlist/denylist, path rules)
- TASK-HAD-20260228-007: `execution` package — PTY job manager, step runner, hooks
- TASK-HAD-20260228-008: `scheduler` package — cron engine with claim-and-update
- TASK-HAD-20260228-009: `pipeline` package — spec model, stage runner
- TASK-HAD-20260228-010: Execution integration test (blueprint → run → events)

## Notes

- Port PTY execution from `reference-only/nanite-wails-starter/backend/core/manager.go` — this is the source of truth for PTY, retry, timeout, condition eval, and per-task hooks
- Port `settings` from `reference-only/nanite-wails-starter/backend/settings/settings.go` — ValidateCommand, ValidatePath, DefaultSettings
- Port `scheduler` from `vnext-blueprint-runner/internal/scheduler/engine.go` as-is (rename module path)
- Port `pipeline` from `vnext-blueprint-runner/internal/pipeline/` as-is (rename module path)
- `execution.Manager` should use `persistence.Store` to persist run state and events — wails-starter only used in-memory, vnext had the pattern right
- Telemetry logger: port from `reference-only/nanite-wails-starter/backend/telemetry/telemetry.go`, wire into execution manager
