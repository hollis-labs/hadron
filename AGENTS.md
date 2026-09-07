# Hadron

Hadron is a local-first daemon that validates, admits, executes and inspects
typed durable workflow graphs, and projects that one host over the CLI, HTTP
API, browser UI, MCP, A2A, schedules and external activations. It is not the
workflow engine: graph IR, compilation, runtime, waits and values live in the
pinned `github.com/hollis-labs/go-workflow` module. It is not a second runtime
per surface either — the CLI and UI are transports over the daemon.

## Start Here

- `README.md` covers the public surface, install path and documentation index.
- `docs/workflow-development.md` owns semantic ownership, the adapter contract,
  host binding, and the full release-check sequence.
- `internal/appworkflow/doc.go` is the single authenticated application path
  every transport calls.
- `cmd/hadrond/workflow_runtime.go` is production composition; it binds the
  frozen kind registry, workers, activation scheduling and shutdown.
- `internal/agentsubstrate/boot.go` renders boot context for agents Hadron
  launches, including what it reads from a target project.
- `docs/architecture/adr/` holds the durable decisions.

## Commands

```bash
go test ./internal/appworkflow ./internal/api ./internal/mcpadapter ./internal/a2a
make test
make lint
make e2e
```

The four-package run is the cross-surface suite: identity binding, redaction,
idempotency and hidden/not-found equivalence. `make e2e` builds the binaries and
exercises the production daemon. Add `make test-ui` and `make typecheck` when
`cmd/hadron-app/frontend` changes.

## Boundaries

Every surface goes through `internal/appworkflow`. Transport handlers decode
bounded requests, bind trusted authentication and project safe DTOs; they must
not import compiler, runtime or persistence internals, and must not construct
policy-authorizing facts.

The stock daemon binds exactly six frozen kinds — `transform@v1`, `script@v1`,
`sleep@v1`, `wait_for@v1`, `message_wait@v1`, `human_gate@v1` — at
`cmd/hadrond/workflow_runtime.go:155`. Adding an adapter package does not make
it a production capability. Runtime config cannot widen schemas or narrow
effects and capabilities.

Blueprint and pipeline code — `internal/blueprint`, `internal/pipeline`,
`internal/lint`, `schemas/*.json`, `docs/spec-v04.md`,
`examples/archive/legacy-blueprints-pipelines/` — is archive and rewrite
material. Production defaults do not mount it and it carries no public
compatibility promise. Do not extend it, and do not read it as evidence of
current behavior.

Run `go generate ./internal/api` rather than hand-editing the generated JSON
Schema or TypeScript client. A non-empty diff after generation or `go mod tidy`
fails the release checks.

`internal/agentsubstrate` reads a target project's `AGENTS.md` and
`.agent-ops/project.yaml` by walking up from the launched agent's project
directory. This repository no longer carries a `.agent-ops/project.yaml`; that
is a removed input here, not a dead reader.
