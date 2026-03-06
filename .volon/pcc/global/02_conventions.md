---
intent: pcc_global
project: hadron
updated_at: "2026-03-04"
---

# Conventions

## Coding standards

- Go standard library style
- CLI built with cobra framework
- Internal packages under `internal/`; CLI entry points under `cmd/`
- Blueprint spec documented in `docs/spec-v04.md`
- Examples serve as living documentation in `examples/`

## Project structure

- `cmd/hadrond` -- daemon binary
- `cmd/hadron` -- CLI binary
- `cmd/hadron-app` -- Wails desktop app
- `internal/` -- all business logic
- `examples/` -- sample blueprints
- `docs/` -- documentation (getting-started, spec, CLI reference, MCP setup, safety)
- `test/e2e/` -- end-to-end tests
- `testdata/` -- test fixtures

## Naming conventions

- Blueprint files: descriptive YAML names (e.g., `hello-hadron.yaml`, `dev-cleanup.yaml`)
- No Volon task/backlog naming (project does not use volon.yaml)
- Docs: descriptive names in `docs/` (no numbered prefix)

## Test commands

```bash
make build    # build hadrond + hadron binaries
make test     # run unit tests (go test ./...)
make lint     # go vet
make e2e      # build + run end-to-end tests
make app      # build Wails desktop app
make app-dev  # run Wails desktop app in dev mode
```

## Blueprint conventions

- All blueprints are YAML with a typed parameter block
- Tasks are ordered, support conditions and `continue_on_error`
- Lifecycle hooks at blueprint and task level
- Safety settings configured via `docs/safety.md`

## Evidence
- Last refreshed: 2026-03-04 (mentat PCC bootstrap)
- Sources: Makefile, README.md, docs/, examples/
