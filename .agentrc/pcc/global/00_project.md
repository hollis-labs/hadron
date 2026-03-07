---
intent: pcc_global
project: hadron
updated_at: "2026-03-04"
---

# Project Identity

**Hadron** is a local-first, agent-first blueprint automation runner. It runs YAML blueprints -- ordered collections of shell tasks -- via a persistent local daemon. Blueprints are typed, parameterised, and composable.

Part of the Hollis Labs suite: Nanite, Volon, Cortex, Mentat, Hadron.

## Goals

- Run YAML blueprints as ordered shell task collections via persistent daemon
- Provide REST API and MCP adapter for AI agent integration (Claude Code, Mentat)
- Support parameterised inputs (string, number, boolean, array, enum)
- Enable scheduling via cron expressions
- Support multi-blueprint pipelines
- Provide validation, linting, and safety controls for blueprints

## Non-goals

- Not a general CI/CD platform
- Not a cloud service; local daemon only
- Not intended for untrusted blueprint execution without safety configuration

## Active configuration

- Module: `github.com/hollis-labs/hadron` (Go 1.25)
- Binaries: `hadrond` (daemon), `hadron` (CLI)
- Default daemon: REST API on local port
- MCP mode: JSON-RPC 2.0 over stdio with token-scoped access

## Evidence
- Last refreshed: 2026-03-04 (mentat PCC bootstrap)
- Sources: README.md, go.mod, Makefile
