# Install Hadron

Hadron is currently distributed source-first during the public beta period.

## Prerequisites

- Go 1.26.3 or newer
- `make`
- optional: `golangci-lint`, `staticcheck`, `errcheck`, and `govulncheck` for `make lint`
- optional: Node.js if you are working on the desktop app frontend

## Install Options

### Build in-place

Use this if you are developing Hadron or want repo-local binaries:

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make build
export PATH="$PWD/bin:$PATH"
```

This produces:

- `bin/hadrond`
- `bin/hadron`

### Install into a prefix

Use this if you want normal shell-visible binaries without relying on `GOPATH`:

```sh
git clone git@github.com:hollis-labs/hadron.git
cd hadron
make install PREFIX="$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
```

Defaults:

- `PREFIX=/usr/local`
- `BINDIR=$(PREFIX)/bin`

You can override either value:

```sh
make install BINDIR="$HOME/bin"
```

### Install with `go install`

```sh
go install github.com/hollis-labs/hadron/cmd/hadrond@latest
go install github.com/hollis-labs/hadron/cmd/hadron@latest
```

This installs into your Go bin directory, usually one of:

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

## Desktop App

The Wails desktop app is still part of the beta surface and is built separately:

```sh
make app
```

For frontend iteration:

```sh
make app-dev
```

## Notes

- Hadron is MIT licensed.
- Public binary release packaging may change during beta.
- The current recommended path is source install plus `make install` or `go install`.
