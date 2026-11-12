# Getting started

## Build

Hadron is one binary. From a checkout:

```sh
go build -o hadron ./cmd/hadron
./hadron version
```

Release binaries are built in CI; until the first release, build from source (Go 1.26).

## Run the daemon

```sh
./hadron serve
```

It listens on `127.0.0.1:8095`, keeps its database in `hadron.db` and its data in `hadron-data/`, both relative to the directory you start it in. Authentication is off by default in development: every caller is the local operator. Check it:

```sh
curl localhost:8095/healthz        # {"status":"ok","checks":{"database":"ok"}}
./hadron health                    # the same checks, in-process
./hadron config                    # the effective configuration (secrets redacted)
```

Run it from a fixed directory, or set `hadron.data_dir` and `database.sqlite.path` (see [Configuration](configuration.md)), so restarts find the same state.

## Your first workflow

A file workflow lives in the daemon's workflow directory, `<data_dir>/workflows`:

```sh
install -d hadron-data/workflows
install -m 0600 examples/workflow/production/hello-transform.workflow.yaml hadron-data/workflows/

./hadron workflow validate hello-transform.workflow.yaml
./hadron workflow run hello-transform.workflow.yaml \
  --input-json '{"message":"Hello, workflow"}' --run-id first
./hadron workflow inspect first
```

`validate` prints `valid hello-transform@v1 (<plan digest>)` and `graph <graph digest>`. `run` prints `run first phase=running status=...`; the work happens in the daemon, so `inspect` shows the run's nodes and status, and `events first` shows what happened.

Output is text by default. Add `-o json` for the daemon's response as it sent it, or `-o yaml`.

## What to read next

- To write your own workflows, and to share them with agents: [Workflows](workflows.md).
- To let agents call Hadron over MCP: [MCP and A2A](mcp.md).
- To turn authentication on: [Access](access.md).
