# Hadron

Hadron is a local-first, agent-first daemon for typed, durable workflow
graphs. One graph-native application host owns validation, policy admission,
execution, waits, values, diagnostics, and registry state, and projects that
same host identically over the CLI, HTTP API, browser UI, MCP, A2A,
schedules, and external activations — there is no second runtime per
transport.

> **Pre-release.** Hadron ships real beta releases (Homebrew tap, GitHub
> releases) and is in active use, but the public contract is still settling —
> authoring ergonomics, packaging, and the adapter surface can change without
> notice. Built in the open: this README describes what's implemented today,
> not a pitch for what's planned.

## What it is today

- **One host, every surface.** CLI, HTTP API, browser UI, MCP, A2A,
  schedules, and external activations all call the same authenticated
  application path (`internal/appworkflow`) — none of them embed a second
  workflow runtime.
- **Six frozen production primitives.** `transform@v1`, `script@v1`,
  `sleep@v1`, `wait_for@v1`, `message_wait@v1`, and `human_gate@v1` — the last
  one a first-class human-approval step, not automation bolted onto a
  workaround.
- **Durable by default.** SQLite-backed execution with recovery,
  resource-aware scheduling, fan-out occupancy, lease renewal, and graceful
  worker/timer shutdown.
- **MCP and A2A as native transports.** Session-isolated MCP discovery, lazy
  mounts and direct tools, plus native A2A task/run correlation — agents
  reach Hadron the same way they'd reach any other tool, not through a
  bespoke integration.

## Where it sits in the stack

```
  agents / clients     Nanite sessions, hadron CLI, browser UI, any MCP/A2A client
        │  MCP · A2A · HTTP · CLI
   ┌──────────┐
   │  Hadron  │   durable graph host: validate → admit → run → wait → recover
   └──────────┘
        │  built on
   go-workflow         the engine module Hadron originated and now ships standalone
```

Hadron isn't the workflow engine itself — graph IR, compilation, runtime,
waits, and values live in the pinned `go-workflow` module. Hadron is the
daemon and transports around it. Nanite's own Agent Workflows consume
`go-workflow` directly, as a separate durable host; Hadron and Nanite are
siblings on that engine, not layered on each other.

## Examples

**Daily use.** Chrispian runs graph-native workflows through `hadrond` for
durable, resumable automation — steps that need to survive a restart, wait
on an external signal, or pause for an explicit `human_gate@v1` approval
before continuing.

**Composition.** Any MCP or A2A client — a Nanite session, another agent
runtime — discovers Hadron's workflow tools, starts a run, and gets notified
when a `human_gate` or `wait_for` step needs input. Hadron never assumes who
the caller is; it only sees authenticated tool calls.

**Embedding.** The repository's adapter catalog is broader than the six kinds
the stock daemon exposes — the rest are embeddable contracts for building a
bespoke workflow host on the same engine without shipping inside Hadron
itself.

## Roadmap

- **Standalone `go-workflow` module.** The public `workflow/*` boundary was
  already published so Nanite could qualify it as a real non-Hadron durable
  host; next is extracting the proven packages into their own versioned
  module without weakening schemas, conformance, or Hadron compatibility.
- **Contract stabilization.** Public contracts and authoring ergonomics are
  still evolving, packaging/install UX is still being refined, and
  browser/agent-client interoperability keeps getting hardened — see
  [Beta status](docs/beta-status.md) for the current line between
  implemented and in-progress.

## License

Hadron is MIT licensed and in active beta development.

## Install

```sh
brew install hollis-labs/tap/hadron
```

Or build from source:

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
npm --prefix cmd/hadron-app/frontend ci
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
| [Workflow diagnostics and recovery](docs/workflow-diagnostics.md) | Graph-native inspect/events, telemetry export wiring, recovery actions, and local measurement |
| [CLI reference](docs/cli-reference.md) | Active root and `hadron workflow` command contracts |
| [MCP setup](docs/mcp-setup.md) | Token bootstrap, exposure profiles, discovery, and tools |
| [Safety](docs/safety.md) | Identity, effects, secrets, redaction, and compatibility boundaries |
| [Workflow development](docs/workflow-development.md) | Package ownership, host composition, adapters, conformance, and release checks |
| [Embed the workflow engine](docs/workflow-engine-adoption.md) | Public Go boundary, minimal host, storage/timers, conformance, compatibility, and extraction criteria |
| [Shared workflow library](https://github.com/hollis-labs/go-workflow/tree/v0.1.0) | Engine contracts, generated graph schema, adapters, and conformance suite |
| [Workflow library migration](docs/workflow-library-migration.md) | Hadron's pinned dependency and extraction boundary |
| [Architecture](docs/architecture/ARCHITECTURE.md) | Current graph-native system boundary |
| [Beta status](docs/beta-status.md) | Implemented surface and current constraints |

The beta-era blueprint/pipeline specification and examples are historical
rewrite material. They remain under `docs/spec-v04.md` and
`examples/archive/legacy-blueprints-pipelines/`; active commands do not execute
them and they carry no public compatibility promise.

## Development

```bash
make build
make test
make test-ui
make typecheck
make lint
make e2e
```

`make e2e` builds the current binaries and exercises the production daemon,
graph validation, typed run/inspect flow, and rejection of retired CLI roots.
