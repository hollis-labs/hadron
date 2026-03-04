# Getting Started with Hadron

## Installation

Build from source (requires Go 1.22+):

```sh
git clone <repo>
cd hadron
make build
# Binaries: bin/hadron  bin/hadrond
export PATH="$PWD/bin:$PATH"
```

## First Run

**1. Start the daemon**

```sh
hadrond serve
# {"level":"info","msg":"hadron daemon starting","addr":"127.0.0.1:8095","version":"0.4.0",...}
```

**2. Run a blueprint**

```sh
hadron run examples/hello-hadron.yaml
# run abc123 queued
# [started]
# [log] Hello from Hadron!
# [task_success] task succeeded
# run completed successfully
```

**3. Validate a blueprint**

```sh
hadron validate examples/hello-hadron.yaml
# valid
```

**4. Check daemon health**

```sh
hadron daemon
# status: ok  version: 0.4.0
```

## First Schedule

Schedule `hello-hadron` to run every minute:

```sh
hadron schedule create \
  --blueprint examples/hello-hadron.yaml \
  --cron "* * * * *" \
  --name hello-every-minute

hadron schedule list
# <id>  examples/hello-hadron.yaml  * * * * *  enabled=true
```

After a minute, list runs to see it fired:

```sh
hadron run examples/hello-hadron.yaml   # or wait for the scheduler
```

Disable a schedule:

```sh
hadron schedule disable <id>
```

## What's Next

- [Blueprint spec v0.4](spec-v04.md) — full YAML reference
- [CLI reference](cli-reference.md) — all commands and flags
- [MCP setup](mcp-setup.md) — use Hadron with Claude Code
- [Safety settings](safety.md) — control what blueprints can do
