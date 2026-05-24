# Install Hadron

Hadron is currently distributed as source installs and GitHub release
artifacts during the public beta period.

## Prerequisites

- Go 1.26.3 or newer
- `make`
- optional: `golangci-lint`, `staticcheck`, `errcheck`, and `govulncheck` for `make lint`
- optional: Node.js if you are working on the desktop app frontend

## Install Options

### Download a release artifact

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
  https://github.com/hollis-labs/hadron/releases/download/v0.4.0/hadron_v0.4.0_darwin_arm64.tar.gz
tar -xzf hadron.tar.gz
cd hadron_v0.4.0_darwin_arm64
install -d "$HOME/.local/bin"
install -m 0755 hadron hadrond "$HOME/.local/bin/"
export PATH="$HOME/.local/bin:$PATH"
```

Release downloads also include `checksums.txt` for verification.

### Install from the Hollis Labs tap

Hadron is intended to be installable from:

```sh
brew install hollis-labs/tap/hadron
```

Current constraint:

- this only works once the Hadron repo and release assets are publicly downloadable

If the repo or its release artifacts are still private, Homebrew users will get
download failures from the tap formula.

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
- Public binary packaging may still change during beta.
- The current recommended paths are release tarballs, `make install`, or `go install`.
- Homebrew tap publication depends on public GitHub release accessibility.
