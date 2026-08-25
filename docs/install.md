# Install Hadron

Hadron is distributed during beta as:

- Homebrew via `hollis-labs/tap`
- GitHub release tarballs
- source installs from this repo
- `go install` for Go-based environments

## Prerequisites

- macOS or Linux
- no separate database dependency; Hadron uses local SQLite
- if building from source: Go `1.26.6+`, Node.js `24+`, npm, and `make`

## Recommended Paths

### Homebrew

```sh
brew install hollis-labs/tap/hadron
```

This installs:

- `hadron`
- `hadrond`

Verify:

```sh
hadron version
hadrond version
```

### Release Tarballs

Tagged beta releases publish tarballs for:

- macOS `amd64`
- macOS `arm64`
- Linux `amd64`
- Linux `arm64`

Each archive contains:

- `hadron`
- `hadrond`
- `hadron-app` (optional browser launcher)
- `README.md`
- `LICENSE`

Example:

```sh
curl -L -o hadron.tar.gz \
  https://github.com/hollis-labs/hadron/releases/download/v0.4.2-beta.1/hadron_v0.4.2-beta.1_darwin_arm64.tar.gz
tar -xzf hadron.tar.gz
cd hadron_v0.4.2-beta.1_darwin_arm64
install -d "$HOME/.local/bin"
install -m 0755 hadron hadrond hadron-app "$HOME/.local/bin/"
export PATH="$HOME/.local/bin:$PATH"
```

Releases also include `checksums.txt`.

## Source Installs

### Build In Place

Use this if you want repo-local binaries:

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make build
export PATH="$PWD/bin:$PATH"
```

### Install Into A Prefix

Use this if you want shell-visible binaries from a source checkout:

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make install PREFIX="$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
```

Defaults:

- `PREFIX=/usr/local`
- `BINDIR=$(PREFIX)/bin`

Example custom target:

```sh
make install BINDIR="$HOME/bin"
```

### `go install`

```sh
go install github.com/hollis-labs/hadron/cmd/hadrond@latest
go install github.com/hollis-labs/hadron/cmd/hadron@latest
go install github.com/hollis-labs/hadron/cmd/hadron-app@latest
```

This installs into one of:

- `$GOBIN`
- `$GOPATH/bin`
- `$HOME/go/bin`

## First-Time Setup

Start the daemon:

```sh
hadrond serve
```

By default Hadron stores state under `~/.hadron/`:

- database: `~/.hadron/state/hadron.db`
- run logs: `~/.hadron/logs/runs/`
- settings: `~/.hadron/settings.json`

Validate the install:

```sh
install -d "$HOME/.hadron/workflows"
install -m 0600 examples/workflow/production/hello-transform.workflow.yaml \
  "$HOME/.hadron/workflows/hello-transform.workflow.yaml"
hadron daemon
hadron workflow validate "$HOME/.hadron/workflows/hello-transform.workflow.yaml"
hadron workflow run "$HOME/.hadron/workflows/hello-transform.workflow.yaml" \
  --run-id install-smoke-1 \
  --idempotency-key install-smoke-1 \
  --input-json '{"message":"installed"}' \
  --json
```

The copy command requires a source checkout. For packaged installs, place a
graph-native workflow under `~/.hadron/workflows` and pass that file path. The
retired blueprint/pipeline root commands are not installed as compatibility
surfaces.

## Daemon And MCP Modes

Hadron has two different runtime modes:

- `hadrond serve`
  - runs the local HTTP daemon used by the CLI and serves the operator UI at
    `http://127.0.0.1:8095/`
- `hadrond mcp`
  - runs a stdio MCP server for agent clients and requires `-token`

They can point at the same `~/.hadron` data directory and SQLite database.

## Operator UI And Optional Launcher

`hadrond serve` embeds the production React application, so a normal browser is
the primary UI and a headless installation needs no desktop framework. The
optional `hadron-app` binary only starts/adopts the daemon and opens that URL;
it has no workflow DTOs, file bindings, or execution semantics.

```sh
make app
bin/hadron-app
```

For frontend iteration, run `hadrond serve` in one terminal and the Vite proxy
in another:

```sh
make app-dev
```

## Notes

- Hadron is MIT licensed.
- The CLI and daemon are beta software; expect continued UX and docs iteration.
- Homebrew and release tarballs are the cleanest non-source install paths today.
- Source installs remain the best fit when you are editing Hadron itself.
