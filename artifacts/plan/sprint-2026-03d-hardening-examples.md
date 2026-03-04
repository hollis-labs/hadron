---
type: sprint-plan
id: sprint-HAD-03d
title: "Sprint D — Hardening, Examples, and Docs"
status: todo
created_at: 2026-02-28
---

# Sprint D: Hardening, Examples, and Docs

## Goal

Production-ready Phase 1. Comprehensive test coverage, a reference example blueprint library, observability, and user-facing documentation. By end of sprint, Hadron is ready to move to `~/Projects-apps/hadron/` and be used by Volon and other suite apps.

## Entry Criteria

- Sprint C complete: daemon, CLI, and MCP all working end-to-end

## Exit Criteria

- `go test ./...` passes with meaningful coverage (target: 70%+ on core packages)
- At least 5 reference blueprints in `examples/` exercising v0.4 features
- `hadron validate` correctly catches all validation errors
- Blueprint auto-format/lint command works
- MCP `.mcp.json` example in repo, documented in `docs/mcp-setup.md`
- `docs/` has: spec reference, CLI reference, getting started guide
- Telemetry structured logging wired end-to-end (run start/end, step events, errors)
- Settings file (safety rules) documented and defaults hardened
- Ready to move to `~/Projects-apps/hadron/` and be treated as independent repo

## Tasks

- TASK-HAD-20260228-017: Example blueprints (v0.4 feature showcase)
- TASK-HAD-20260228-018: `blueprint` linter/formatter CLI command
- TASK-HAD-20260228-019: Telemetry — structured logging wired end-to-end
- TASK-HAD-20260228-020: End-to-end test suite (full stack: CLI → daemon → execute → events)
- TASK-HAD-20260228-021: User-facing docs (spec ref, CLI ref, getting started, MCP setup)

## Notes

### Examples to include
- `examples/hello-hadron.yaml` — minimal v0.4 blueprint (single section, 2 tasks)
- `examples/laravel-app.yaml` — Laravel scaffold (packages, git, stubs, sections) — port from v0.2 reference
- `examples/dev-cleanup.yaml` — daily cleanup routine (conditions, env, brew tools)
- `examples/pipeline-demo.yaml` — 2-stage pipeline (setup → test)
- `examples/parameterized.yaml` — blueprint with typed inputs, prompts, defaults, enum

### Hardening checklist
- [ ] All API endpoints return structured JSON errors
- [ ] PTY process cleanup on daemon shutdown (signal handling)
- [ ] SQLite WAL mode enabled for better concurrency
- [ ] Run events table: prune old events (configurable retention)
- [ ] Settings: `dryRunByDefault`, `sandboxMode`, `blockSudo` all tested
- [ ] Blueprint template `readFile` size guard enforced

### Docs
- `docs/getting-started.md` — install, first run, first schedule
- `docs/spec-v04.md` — human-readable spec reference (from `artifacts/plan/01-blueprint-spec-v04.md`)
- `docs/cli-reference.md` — every command and flag
- `docs/mcp-setup.md` — how to wire Hadron MCP into Claude Code / Volon
- `docs/safety.md` — settings.json safety configuration guide
