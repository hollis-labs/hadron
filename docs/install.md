# Install Hadron

Hadron is distributed during beta as:

- Homebrew via `hollis-labs/tap`
- GitHub release tarballs
- source installs from this repo
- `go install` for Go-based environments

## Prerequisites

- macOS or Linux
- no separate database dependency; Hadron uses local SQLite
- if building from source: Go `1.26.3+` and `make`
- if developing the desktop app: Node.js for frontend tasks

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
- `README.md`
- `LICENSE`

Example:

```sh
curl -L -o hadron.tar.gz \
  https://github.com/hollis-labs/hadron/releases/download/v0.4.2-beta.1/hadron_v0.4.2-beta.1_darwin_arm64.tar.gz
tar -xzf hadron.tar.gz
cd hadron_v0.4.2-beta.1_darwin_arm64
install -d "$HOME/.local/bin"
install -m 0755 hadron hadrond "$HOME/.local/bin/"
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
- logs: `~/.hadron/logs/`
- settings: `~/.hadron/settings.json`

Validate the install:

```sh
hadron daemon
hadron validate examples/hello-hadron.yaml
hadron run examples/hello-hadron.yaml
```

## Daemon And MCP Modes

Hadron has two different runtime modes:

- `hadrond serve`
  - runs the local HTTP daemon used by the CLI and desktop app
- `hadrond mcp`
  - runs a stdio MCP server for agent clients

They can point at the same `~/.hadron` data directory and SQLite database.

## Desktop App

The Wails desktop app is still part of the beta surface and is built
separately:

```sh
make app
```

For frontend iteration:

```sh
make app-dev
```

## Notes

- Hadron is MIT licensed.
- The CLI and daemon are beta software; expect continued UX and docs iteration.
- Homebrew and release tarballs are the cleanest non-source install paths today.
- Source installs remain the best fit when you are editing Hadron itself.
