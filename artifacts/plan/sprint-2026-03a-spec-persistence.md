---
type: sprint-plan
id: sprint-HAD-03a
title: "Sprint A — Spec v0.4 + Persistence Foundation"
status: todo
created_at: 2026-02-28
---

# Sprint A: Spec v0.4 + Persistence Foundation

## Goal

Stand up a buildable Go module for Hadron with the v0.4 blueprint spec fully implemented and tested, and the SQLite persistence layer ported and wired in. No HTTP, no CLI yet — just the core data model and storage that everything else depends on.

## Entry Criteria

- ADR, spec, and architecture docs accepted (`artifacts/plan/00–02`)
- Reference source material identified (see architecture doc)

## Exit Criteria

- `go build ./...` passes with zero errors
- `go test ./internal/specparse/... ./internal/blueprint/... ./internal/persistence/...` all pass
- Blueprint v0.4 model fully validates the reference blueprint from `nanite-spec-v0.2/reference-blueprint.yaml`
- Blueprint v0.4 model validates a blueprint using new v0.4 features (hooks, typed inputs, import aliases)
- SQLite store opens, runs migrations, and can CRUD runs/schedules/workspaces/events

## Tasks

- TASK-HAD-20260228-001: Go module skeleton + project layout
- TASK-HAD-20260228-002: `specparse` package — YAML/JSONC parser
- TASK-HAD-20260228-003: `blueprint` package — v0.4 model, validation, template rendering
- TASK-HAD-20260228-004: `persistence` package — SQLite store with migrations
- TASK-HAD-20260228-005: `blueprint` package — v0.4 test suite + v0.2 compat validation

## Notes

- Port `specparse` from `vnext-blueprint-runner/internal/specparse/` (already clean and tested)
- Port `persistence` from `vnext-blueprint-runner/internal/persistence/` (rename module path `hollis-labs/cortex` → `hollis-labs/hadron`)
- For `blueprint` package: start from vnext's typed input/hook model, enrich with wails-starter's sections, packages, git, stubs, tools, per-task hooks, PTY template functions
- Do NOT port the GUI, API, or CLI yet
