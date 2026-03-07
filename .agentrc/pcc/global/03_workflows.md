---
intent: pcc_global
project: hadron
updated_at: "2026-03-04"
---

# Workflows

## Build

```bash
make build          # produces bin/hadrond and bin/hadron
export PATH="$PWD/bin:$PATH"
```

## Running the daemon

```bash
hadrond serve       # start persistent daemon
hadron daemon       # check daemon status
```

## Blueprint operations

```bash
# Run a blueprint
hadron run examples/hello-hadron.yaml

# Run with parameters
hadron run blueprints/my-blueprint.yaml \
  --set param1="value1" \
  --set param2="value2"

# Validate a blueprint
hadron validate examples/parameterized.yaml

# Lint a directory
hadron lint examples/
```

## Scheduling

```bash
# Create a scheduled run
hadron schedule create \
  --blueprint examples/hello-hadron.yaml \
  --cron "* * * * *" \
  --name hello-every-minute
```

## MCP mode (for Claude Code / AI agents)

```bash
hadrond mcp -token <secret> -token-scopes run.write,schedule.write,pipeline.write
```

Add to `.mcp.json` in target repo for Claude Code integration.

## Desktop app

```bash
make app       # build Wails desktop app
make app-dev   # dev mode with hot reload
```

## Testing

```bash
make test      # unit tests
make lint      # go vet
make e2e       # end-to-end tests (requires built binaries)
```

## Evidence
- Last refreshed: 2026-03-04 (mentat PCC bootstrap)
- Sources: README.md, Makefile, docs/getting-started.md, docs/cli-reference.md
