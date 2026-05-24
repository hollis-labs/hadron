# Getting Started with Hadron

Hadron is a local-first blueprint runner with three main entry points:

- `hadrond` runs the daemon
- `hadron` is the CLI client
- `hadrond mcp` exposes Hadron to MCP-compatible agents

This page is the shortest path from install to a working local setup.

## 1. Install

Pick one install path from [install.md](install.md). For most users, the
recommended paths are:

```sh
brew install hollis-labs/tap/hadron
```

or:

```sh
curl -L -o hadron.tar.gz \
  https://github.com/hollis-labs/hadron/releases/download/v0.4.2-beta.1/hadron_v0.4.2-beta.1_darwin_arm64.tar.gz
tar -xzf hadron.tar.gz
cd hadron_v0.4.2-beta.1_darwin_arm64
install -d "$HOME/.local/bin"
install -m 0755 hadron hadrond "$HOME/.local/bin/"
export PATH="$HOME/.local/bin:$PATH"
```

If you are developing Hadron itself, use a source install instead.

## 2. Start The Daemon

The CLI talks to a running local daemon.

```sh
hadrond serve
```

Expected result:

```text
{"level":"info","msg":"hadron daemon starting","addr":"127.0.0.1:8095",...}
```

By default Hadron stores state under `~/.hadron/`:

- database: `~/.hadron/state/hadron.db`
- logs: `~/.hadron/logs/`
- settings: `~/.hadron/settings.json`

In a second shell, confirm the daemon is reachable:

```sh
hadron daemon
```

## 3. Run A Blueprint

Start with the smallest example:

```sh
hadron run examples/hello-hadron.yaml
```

Typical output:

```text
run <id> queued
[started]
[log] Hello from Hadron!
[task_success] task succeeded
run completed successfully
```

Validate a blueprint without running it:

```sh
hadron validate examples/hello-hadron.yaml
```

Inspect a parameterized blueprint:

```sh
hadron blueprint show examples/parameterized.yaml
hadron run examples/parameterized.yaml --input app_name=demo --input worker_count=4
```

## 4. Create A Schedule

Schedule the hello-world blueprint to run every minute:

```sh
hadron schedule create \
  --blueprint examples/hello-hadron.yaml \
  --cron "* * * * *" \
  --name hello-every-minute
```

List schedules:

```sh
hadron schedule list
```

Disable or delete it later:

```sh
hadron schedule disable <schedule-id>
hadron schedule delete <schedule-id>
```

## 5. Connect An Agent Over MCP

Hadron can run as a stdio MCP server for agent clients:

```sh
hadrond mcp \
  -token "my-secret-token" \
  -token-scopes "run.write,run.cancel,schedule.write,pipeline.write,trigger.write,human_gate.write,message.write"
```

For the full MCP flow and tool model, see [mcp-setup.md](mcp-setup.md).

## Common First Tasks

- run a local cleanup or build blueprint
- validate and lint a blueprint directory before committing
- schedule recurring housekeeping jobs
- let an MCP agent discover and run a blueprint with structured diagnostics

For more concrete scenarios, see [use-cases.md](use-cases.md).

## What Next

- [install.md](install.md) for install and setup details
- [cli-reference.md](cli-reference.md) for command-by-command usage
- [mcp-setup.md](mcp-setup.md) for agent setup
- [spec-v04.md](spec-v04.md) for the full blueprint spec
- [safety.md](safety.md) for trust and execution controls
