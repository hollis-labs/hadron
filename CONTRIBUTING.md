# Contributing

How a change gets from your clone into `main`. This is deliberately short: most
of what you need is already written somewhere closer to the thing it describes.

## Before your first change

`README.md` has the install and build steps, and `docs/workflow-development.md`
covers semantic ownership, the adapter contract and the full release-check
sequence. Run `lefthook install` once per clone — a tracked `lefthook.yml`
installs no hooks by itself, and a clone that skips it has no commit or push
checks and says nothing about it.

`AGENTS.md` is the fastest orientation to the layout and to the boundaries that
are not obvious from reading the code.

## The sequence

1. **Branch.** `<type>/<short-slug>`, where the type matches the change —
   `feat`, `fix`, `docs`, `chore`. Nothing enforces this; it is what the
   history does.
2. **Change one thing.** A branch carrying two unrelated changes costs the
   reviewer the ability to accept one and question the other.
3. **Run the checks** that match what you touched:
   ```
   make lint && make test          # backend
   make test-race                  # runtime, waits, persistence, triggers, host composition
   make test-ui && make typecheck  # only if cmd/hadron-app/frontend changed
   make e2e                        # builds binaries, exercises the production daemon
   ```
4. **Push and open a pull request.** A maintainer will review it.

Commit subjects follow the conventional-commit shape — a type, an optional
scope, a colon, then the summary.

## What a pull request should carry

The reviewer was not there when you made the decisions. State what the change
does, what it deliberately leaves alone, and the evidence that it works — the
commands you ran and what came back, not a claim that it passes. If a number
appears in the description, put the command that produced it beside it.

Add a line to `CHANGELOG.md` under `[Unreleased]`.

## The one that cannot be undone

**Do not widen what the stock daemon can do by accident.** The daemon binds
exactly six frozen step kinds, and runtime configuration cannot widen schemas
or narrow effects and capabilities. A change that adds a kind, a capability, or
a way for a transport to bypass `internal/appworkflow` alters the authorization
boundary, and workflow definitions and runs are durable — state admitted under
a wrong policy does not go away when you revert the code. Read the Boundaries
section of `AGENTS.md` and `docs/safety.md` first, and call the change out
explicitly in the pull request.

## Things that surprise people

- **Generated code.** Run `go generate ./internal/api` rather than editing the
  generated JSON Schema or TypeScript client; a non-empty diff after
  generation or `go mod tidy` fails the release checks.
- **Blueprint and pipeline code is archive.** `internal/blueprint`,
  `internal/pipeline`, `internal/lint` and `schemas/*.json` are not evidence of
  current behavior and are not extended.
- **The workflow engine is a separate module.** Graph IR, compilation,
  runtime, waits and values live in `go-workflow`, not here.
- **The four-package suite is not the gate.** `go test ./internal/appworkflow
  ./internal/api ./internal/mcpadapter ./internal/a2a` is a focused
  cross-surface property suite; `make test` is the backend gate.

## What this does not cover

- **Which change is worth making.** Open an issue to discuss larger features
  before building them.
- **Releases.** Maintainers cut those; the procedure is in `docs/release-ops.md`.
- **Engine changes.** Those belong in the `go-workflow` repository.
