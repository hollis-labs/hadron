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
- packaging and install UX are still being improved
- the daemon-served browser UI is stabilizing; the desktop binary is now only
  an optional launcher
- some docs and ergonomics are still catching up with the live daemon

## Install

### Option 1: Homebrew

```sh
brew install hollis-labs/tap/hadron
```

### Option 2: Download a release tarball

```sh
curl -L -o hadron.tar.gz \
  https://github.com/hollis-labs/hadron/releases/download/v0.4.2-beta.1/hadron_v0.4.2-beta.1_darwin_arm64.tar.gz
tar -xzf hadron.tar.gz
cd hadron_v0.4.2-beta.1_darwin_arm64
install -d "$HOME/.local/bin"
install -m 0755 hadron hadrond hadron-app "$HOME/.local/bin/"
export PATH="$HOME/.local/bin:$PATH"
```

### Option 3: Build from source

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make build
export PATH="$PWD/bin:$PATH"
```

### Option 4: Install with `go install`

```sh
go install github.com/hollis-labs/hadron/cmd/hadrond@latest
go install github.com/hollis-labs/hadron/cmd/hadron@latest
go install github.com/hollis-labs/hadron/cmd/hadron-app@latest
```

See [docs/install.md](docs/install.md) for prerequisites, paths, release
artifacts, and first-time setup details.

## Quick Start

The commands below exercise the currently implemented legacy runtime with
archived reference inputs. They are smoke tests, not graph-native authoring
guidance; target-format examples will live under `examples/workflow/`.

```sh
# Start the daemon
hadrond serve

# Open the browser operator UI
open http://127.0.0.1:8095/

# Check daemon status
hadron daemon

# Run an archived legacy blueprint
hadron run examples/archive/legacy-blueprints-pipelines/hello-hadron.yaml

# Validate an archived legacy blueprint
hadron validate examples/archive/legacy-blueprints-pipelines/parameterized.yaml

# Lint the archived legacy inputs
hadron lint examples/archive/legacy-blueprints-pipelines/

# Schedule a blueprint
hadron schedule create \
  --blueprint examples/archive/legacy-blueprints-pipelines/hello-hadron.yaml \
  --cron "* * * * *" \
  --name hello-every-minute

# Inspect a blueprint locally
hadron blueprint show examples/archive/legacy-blueprints-pipelines/parameterized.yaml
```

---

## MCP Mode

```sh
hadrond mcp -token <secret> -token-scopes run.write,schedule.write,pipeline.write
```

Configure your MCP client to launch `hadrond mcp` over stdio. See
[docs/mcp-setup.md](docs/mcp-setup.md). The MCP surface includes blueprint
broker/discovery tools, prompt templates, and resource docs in addition to the
core run/schedule/pipeline controls.

---

## Documentation

| Doc | Description |
|---|---|
| [docs/getting-started.md](docs/getting-started.md) | Installation and first run |
| [docs/install.md](docs/install.md) | Source install, binary placement, and first daemon setup |
| [docs/use-cases.md](docs/use-cases.md) | Sample user and agent workflows |
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

## Archived Legacy Examples

These files preserve beta-era blueprint and pipeline behavior for reference and
selective rewriting. They are not the preferred future authoring format.

| File | What it demonstrates |
|---|---|
| `examples/archive/legacy-blueprints-pipelines/hello-hadron.yaml` | Minimal blueprint |
| `examples/archive/legacy-blueprints-pipelines/parameterized.yaml` | All input types (string, number, boolean, array, enum) |
| `examples/archive/legacy-blueprints-pipelines/dev-cleanup.yaml` | Conditional tasks, `continue_on_error`, env vars |
| `examples/archive/legacy-blueprints-pipelines/hooks-demo.yaml` | Blueprint and per-task lifecycle hooks |
| `examples/archive/legacy-blueprints-pipelines/laravel-app.yaml` | Realistic multi-section project scaffold |
| `examples/archive/legacy-blueprints-pipelines/agentic-message-wait-local.yaml` | Runnable local mailbox wait with self-targeted MCP message send |
| `examples/archive/legacy-blueprints-pipelines/agentic-launch-and-wait.yaml` | Local runtime launch followed by correlated mailbox wait |
| `examples/archive/legacy-blueprints-pipelines/pipeline-demo/` | Multi-blueprint pipeline |

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
