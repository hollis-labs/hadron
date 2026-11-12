# hadron

Hadron runs typed, durable workflow graphs. You describe a workflow as a graph of steps with typed inputs and outputs; the daemon runs it, survives restarts, waits on gates and messages, fires from schedules, and lets people and agents operate it over a CLI, an HTTP API, MCP and A2A.

It is built on [Substrate](https://github.com/hollis-labs/substrate), which supplies the shell (configuration, logging, health, HTTP, authentication, MCP, database, scheduler). This repository is the new Hadron; it replaces the previous one at cutover.

```sh
go build -o hadron ./cmd/hadron
./hadron serve &                       # daemon on 127.0.0.1:8095
install -d hadron-data/workflows
install -m 0600 examples/workflow/production/hello-transform.workflow.yaml hadron-data/workflows/
./hadron workflow run hello-transform.workflow.yaml --input-json '{"message":"hello"}' --run-id first
./hadron workflow inspect first
```

## Documentation

| | |
|---|---|
| [Getting started](docs/getting-started.md) | Build, configure, run the first workflow |
| [Workflows](docs/workflows.md) | Writing, validating, registering and publishing; confirmation and the unattended allow-list |
| [CLI reference](docs/cli.md) | Every command, reference syntax, output formats |
| [Configuration](docs/configuration.md) | The manifest, the `hadron:` block, overriding config, environment |
| [Access](docs/access.md) | Authentication, scopes, exposure profiles, the policy mode |
| [MCP and A2A](docs/mcp.md) | Tools for agents, typed per-workflow tools, stdio setup, the agent card |
| [Operations](docs/operations.md) | Data directory, backups, health, tracing, agent sessions, going to production |
| [Examples](examples/workflow/README.md) | Runnable workflows |

Development status, decisions and the parity list against the previous app are in `docs/status.md` and `docs/parity.md`.
