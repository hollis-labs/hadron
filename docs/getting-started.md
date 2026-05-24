# Getting Started with Hadron

Hadron is currently in public beta. Start with a source install, then bring up
the daemon locally.

## Installation

Install instructions live in [install.md](install.md). The shortest path is:

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make install PREFIX="$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
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
- [MCP setup](mcp-setup.md) — use Hadron with any MCP client
- [Safety settings](safety.md) — control what blueprints can do
- [Beta status](beta-status.md) — current release posture
