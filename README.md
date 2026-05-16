# Hadron

Local-first, agent-first blueprint automation runner.

Hadron runs YAML blueprints — ordered collections of shell tasks — via a persistent
local daemon.  Blueprints are typed, parameterised, and composable.  The daemon
exposes both a REST API and an MCP adapter so AI agents (Claude Code, Mentat)
can trigger and inspect runs without leaving the conversation.

Part of the **Hollis Labs** suite: Nanite · Volon · Vanta Conduit · Mentat · Hadron.

---

## Quick Start

```sh
# Build
make build
export PATH="$PWD/bin:$PATH"

# Start daemon
hadrond serve

# Run your first blueprint
hadron run examples/hello-hadron.yaml

# Validate a blueprint
hadron validate examples/parameterized.yaml

# Lint a directory of blueprints
hadron lint examples/

# Schedule a blueprint (runs every minute)
hadron schedule create \
  --blueprint examples/hello-hadron.yaml \
  --cron "* * * * *" \
  --name hello-every-minute

# Check daemon status
hadron daemon
```

---

## MCP Mode (for Claude Code)

```sh
hadrond mcp -token <secret> -token-scopes run.write,schedule.write,pipeline.write
```

Add to `.mcp.json` and Claude can validate, run, and inspect blueprints directly.
See [docs/mcp-setup.md](docs/mcp-setup.md).

---

## Documentation

| Doc | Description |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | Installation and first run |
| [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) | Current system architecture |
| [docs/spec-v04.md](docs/spec-v04.md) | Full blueprint spec reference |
| [docs/cli-reference.md](docs/cli-reference.md) | All CLI commands and flags |
| [docs/mcp-setup.md](docs/mcp-setup.md) | Claude Code MCP integration |
| [docs/safety.md](docs/safety.md) | Safety settings and trust levels |
| [docs/audits/README.md](docs/audits/README.md) | Audit conventions for deep review passes |

---

## Examples

| File | What it demonstrates |
|---|---|
| `examples/hello-hadron.yaml` | Minimal blueprint |
| `examples/parameterized.yaml` | All input types (string, number, boolean, array, enum) |
| `examples/dev-cleanup.yaml` | Conditional tasks, `continue_on_error`, env vars |
| `examples/hooks-demo.yaml` | Blueprint and per-task lifecycle hooks |
| `examples/laravel-app.yaml` | Realistic multi-section project scaffold |
| `examples/pipeline-demo/` | Multi-blueprint pipeline |

---

## Development

```sh
make build    # build hadrond + hadron binaries
make test     # run unit tests
make lint     # go vet
make e2e      # build + run end-to-end tests (requires built binaries)
```
