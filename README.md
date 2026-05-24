# Hadron by Hollis Labs

Local-first, agent-first blueprint automation runner.

Hadron runs typed YAML blueprints through a persistent local daemon. Blueprints
are ordered, composable collections of shell tasks with validation, scheduling,
pipelines, MCP access, and an append-only audit trail.

Hadron is free and open source under the MIT license. The project is being
built in the open and is currently in active beta development.

---

## Beta Status

Hadron is usable for local automation, daemon-backed runs, scheduling,
pipelines, MCP-driven workflows, and the current first-class agentic steps.

What "beta" means today:

- APIs and workflow primitives are still being hardened
- install and packaging are source-first today
- the desktop app is substantially complete but still stabilizing
- some docs and ergonomics are still catching up with the live daemon

## Install

### Option 1: Build from source

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make build
export PATH="$PWD/bin:$PATH"
```

### Option 2: Install binaries into a prefix

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make install PREFIX="$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
```

### Option 3: Install with `go install`

```sh
go install github.com/hollis-labs/hadron/cmd/hadrond@latest
go install github.com/hollis-labs/hadron/cmd/hadron@latest
```

Tagged releases also publish macOS and Linux tarballs for `hadron` and
`hadrond`. See [docs/install.md](docs/install.md) for prerequisites, paths,
release artifacts, and setup details.

Planned tap install:

```sh
brew install hollis-labs/tap/hadron
```

That path depends on the Hadron repo and release assets being publicly
downloadable.

## Quick Start

```sh
# Start the daemon
hadrond serve

# Run your first blueprint
hadron run examples/hello-hadron.yaml

# Validate a blueprint
hadron validate examples/parameterized.yaml

# Lint a directory of blueprints
hadron lint examples/

# Schedule a blueprint
hadron schedule create \
  --blueprint examples/hello-hadron.yaml \
  --cron "* * * * *" \
  --name hello-every-minute

# Check daemon status
hadron daemon
```

---

## MCP Mode

```sh
hadrond mcp -token <secret> -token-scopes run.write,schedule.write,pipeline.write
```

Configure your MCP client to launch `hadrond mcp` over stdio. See
[docs/mcp-setup.md](docs/mcp-setup.md).

---

## Documentation

| Doc | Description |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | Installation and first run |
| [docs/install.md](docs/install.md) | Source install, binary placement, and first daemon setup |
| [docs/beta-status.md](docs/beta-status.md) | Current public beta posture and remaining hardening areas |
| [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) | Current system architecture |
| [docs/spec-v04.md](docs/spec-v04.md) | Full blueprint spec reference |
| [docs/agentic-workflows.md](docs/agentic-workflows.md) | Current status of structured agentic workflow steps |
| [docs/agent-runtime-roadmap.md](docs/agent-runtime-roadmap.md) | Roadmap for go-agent-runtime-backed launch and abstract messaging |
| [docs/cli-reference.md](docs/cli-reference.md) | All CLI commands and flags |
| [docs/mcp-setup.md](docs/mcp-setup.md) | MCP client setup |
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
| `examples/agentic-message-wait-local.yaml` | Runnable local mailbox wait with self-targeted MCP message send |
| `examples/agentic-launch-and-wait.yaml` | Local runtime launch followed by correlated mailbox wait |
| `examples/pipeline-demo/` | Multi-blueprint pipeline |

---

## Development

```sh
make build    # build hadrond + hadron binaries
make test     # run unit tests
make test-ui  # run frontend tests
make typecheck
make lint     # go vet + linters + vuln checks
make e2e      # build + run end-to-end tests (requires built binaries)
```
