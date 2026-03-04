---
type: sprint-plan
id: sprint-HAD-03c
title: "Sprint C — REST API + CLI + MCP Adapter"
status: todo
created_at: 2026-02-28
---

# Sprint C: REST API + CLI + MCP Adapter

## Goal

Stand up the daemon (`hadrond`), CLI (`hadron`), and MCP adapter. Agents and humans can now use Hadron fully via CLI or MCP. This is the first externally usable version of Hadron.

## Entry Criteria

- Sprint B complete: execution, scheduler, pipeline build and test cleanly
- Integration test from Sprint B passes end-to-end

## Exit Criteria

- `go build ./cmd/hadrond ./cmd/hadron` passes
- `hadrond --addr 127.0.0.1:8095` starts and responds on `GET /v1/health`
- `hadron daemon status` reports daemon health
- `hadron run <blueprint>` executes a blueprint via the daemon, streams events
- `hadron validate <blueprint>` validates a blueprint file
- `hadron schedule create/list/enable/disable` work against the daemon
- MCP mode starts and all `hadron_*` tools respond correctly
- MCP tool `hadron_run_enqueue` successfully enqueues and executes a blueprint

## Tasks

- TASK-HAD-20260228-011: `config` package — runtime config, data dir defaults, flag parsing
- TASK-HAD-20260228-012: `api` package — HTTP REST server with all v1 endpoints
- TASK-HAD-20260228-013: `cmd/hadrond` — daemon entrypoint (HTTP mode + MCP mode)
- TASK-HAD-20260228-014: `cmd/hadron` — CLI (run, validate, blueprint, schedule, pipeline, workspace, daemon)
- TASK-HAD-20260228-015: `mcpadapter` package — MCP server with `hadron_*` tools
- TASK-HAD-20260228-016: API + MCP smoke tests

## Notes

### API
- Use `net/http` stdlib router or `chi` — keep dependencies lean
- Port route structure from `vnext-blueprint-runner/internal/api/server.go` (rename module)
- All list endpoints: cursor-based pagination (`cursor`, `limit` query params)
- All endpoints: workspace scoping via query param or path (`?workspace_id=`)
- Blueprint validation endpoint accepts raw YAML/JSON body

### CLI
- Use `cobra` or `flag` — cobra preferred for subcommand UX
- `hadron run` should support `--dry-run` (validate + render, do not execute)
- `hadron run` should support `--input key=value` flags for parameterized blueprints
- Stream run events to stdout during execution (poll `/v1/runs/:id/events`)

### MCP
- Port `vnext-blueprint-runner/internal/mcpadapter/` — rename all `cortex_*` tools to `hadron_*`
- Use `mark3labs/mcp-go` (already in vnext go.mod)
- Scope-based auth: bearer token in header, scopes validated per tool
- Add `hadron_blueprint_validate` tool (not in vnext, needed for agent use)
- Stdio transport for Claude Code MCP integration
- Document `.mcp.json` wiring in `docs/mcp-setup.md`
