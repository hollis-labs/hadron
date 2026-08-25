# Hadron by Hollis Labs

Hadron is a local-first, agent-first runtime for typed, durable workflow
graphs. One graph-native application host owns validation, policy admission,
execution, waits, values, diagnostics, registry state, and exposure across the
CLI, HTTP API, browser UI, MCP, A2A, schedules, and external activations.

Hadron is MIT licensed and in active beta development.

## Install

```sh
brew install hollis-labs/tap/hadron
```

Or build from source:

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make build
export PATH="$PWD/bin:$PATH"
```

See [Installation](docs/install.md) for releases, `go install`, storage paths,
and the optional browser launcher.

## Quick start

Stage the example in the daemon's bounded workflow root, then start the daemon
in one terminal:

```sh
install -d "$HOME/.hadron/workflows"
install -m 0600 examples/workflow/production/hello-transform.workflow.yaml \
  "$HOME/.hadron/workflows/hello-transform.workflow.yaml"
hadrond serve
```

Then validate and start a graph-native workflow:

```sh
hadron daemon

hadron workflow validate \
  "$HOME/.hadron/workflows/hello-transform.workflow.yaml"

hadron workflow run \
  "$HOME/.hadron/workflows/hello-transform.workflow.yaml" \
  --run-id hello-1 \
  --idempotency-key hello-1 \
  --input-json '{"message":"Hello, graph"}' \
  --json

hadron workflow inspect hello-1 --json
```

Open `http://127.0.0.1:8095/` for the same registry, graph, and run surfaces.
The CLI and UI are transports over the daemon; they do not contain a second
workflow runtime.

The stock production host intentionally exposes six frozen kinds:
`transform@v1`, `script@v1`, `sleep@v1`, `wait_for@v1`, `message_wait@v1`, and
`human_gate@v1`. Other adapters in this repository are embeddable contracts,
not capabilities advertised by the stock daemon.

## MCP

Start the production MCP adapter over stdio with a durable bearer credential:

```sh
hadrond mcp -token '<secret>'
```

The first start creates a digest-only local principal and a bounded default
exposure profile. The same token reopens that identity; the raw token is not
persisted. See [MCP setup](docs/mcp-setup.md) for client configuration, profile
pins, lazy mounts, and the graph-native tool families.

## Documentation

| Guide | Purpose |
|---|---|
| [Getting started](docs/getting-started.md) | First daemon, validation, run, and inspection |
| [Workflow authoring and operations](docs/workflows.md) | Source form, references, kinds, waits, values, registry, and troubleshooting |
| [CLI reference](docs/cli-reference.md) | Active root and `hadron workflow` command contracts |
| [MCP setup](docs/mcp-setup.md) | Token bootstrap, exposure profiles, discovery, and tools |
| [Safety](docs/safety.md) | Identity, effects, secrets, redaction, and compatibility boundaries |
| [Workflow development](docs/workflow-development.md) | Package ownership, host composition, adapters, conformance, and release checks |
| [Generated graph schema](workflow/graph/schema/README.md) | Authoritative JSON Schema and generation command |
| [Architecture](docs/architecture/ARCHITECTURE.md) | Current graph-native system boundary |
| [Beta status](docs/beta-status.md) | Implemented surface and current constraints |

The beta-era blueprint/pipeline specification and examples are historical
rewrite material. They remain under `docs/spec-v04.md` and
`examples/archive/legacy-blueprints-pipelines/`; active commands do not execute
them and they carry no public compatibility promise.

## Development

```sh
make build
make test
make test-ui
make typecheck
make lint
make e2e
```

`make e2e` builds the current binaries and exercises the production daemon,
graph validation, typed run/inspect flow, and rejection of retired CLI roots.
