# Agentic Workflows Boot / Handoff

**Date:** 2026-05-25  
**Branch:** `main`  
**Context:** beta-release hardening, AI-native blueprint workflows

## Start Here

Read these first:

- `docs/agentic-workflows.md`
- `docs/agent-runtime-roadmap.md`
- `docs/mcp-skills/message-workflows.md`
- `docs/spec-v04.md`

Then inspect current local status:

```sh
git status --short --branch
./bin/hadron daemon
cerberus resource status hadron-daemon-service
```

## Current Known Good Proof

The latest successful end-to-end dogfood run is:

- Pipeline: `pl-20260525-164402-0001`
- Run token: `dogfood-20260525164403`
- Final HTML:
  `/Users/chrispian/dev/chrispian/inbox/hadron-dogfood/runs/dogfood-20260525164403/newsletter.html`
- Final manifest:
  `/Users/chrispian/dev/chrispian/inbox/hadron-dogfood/runs/dogfood-20260525164403/artifacts/final-manifest.json`

Dogfood blueprints are currently local, not repo examples yet:

```text
/Users/chrispian/dev/chrispian/inbox/hadron-dogfood/blueprints/
```

The workflow proves:

- source fetch fan-out
- normalized source handoff
- AI synthesis artifact creation
- downstream AI HTML artifact creation from prior AI output
- explicit `hadron-reply` delivery through local mailbox
- final HTML/Markdown/manifest verification

## Fixes Landed In This Handoff

- `internal/agentsubstrate/launcher.go`
  - extended reply outbox watcher from 2 minutes to 15 minutes
  - fixes stranded replies from slow local AI agents
- `internal/pipeline/runner.go`
  - stage run IDs are now deterministic: `plr-<pipeline-run-id>-<stage-index>`
  - fixes `pipeline_stage_runs.run_id` pointing at non-existent runs
- `internal/pipeline/runner_test.go`
  - verifies recorded stage run IDs resolve to actual run records

## Verification Already Run

```sh
go test ./internal/agentsubstrate ./internal/execution ./internal/pipeline
make build
cerberus resource apply hadron-daemon-service
./bin/hadron pipeline run testdata/pipelines/simple-success.yaml
```

Post-apply sanity pipeline:

- `pl-20260525-165159-0001`
- status: `success`
- recorded stage runs:
  - `plr-pl-20260525-165159-0001-00`
  - `plr-pl-20260525-165159-0001-01`

## Next Follow-Ups

1. Fix `message_wait` log noise.
   Current behavior emits repeated `message_wait_poll: no matching message`
   events every poll interval. Correct for debugging, but poor GUI/operator UX.
   Prefer a compact progress event or aggregate diagnostic state.

2. Promote the newsletter dogfood flow into a repo example.
   The local blueprints are useful but should be cleaned into a portable sample
   before committing them as examples.

3. Add docs for AI-stage timeout guidance.
   Real local agent stages can take 2-5 minutes. Examples should either bound
   prompts tightly or set explicit stage wait timeouts.

4. Continue beta docs pass.
   Focus on setup, install/build, MCP setup, blueprint discovery, and practical
   sample use cases.

## Caveats

- The worktree had an unrelated deletion before this handoff:
  `cmd/hadron-app/frontend/dist/.gitkeep`.
  Do not restore or commit it unless explicitly asked.

- Legacy `.claude` system files are still tracked in this repo. The current
  handoff intentionally adds a tracked `docs/` boot prompt instead of editing
  those legacy files.
