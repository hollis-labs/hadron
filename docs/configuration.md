# Configuration

Hadron's shell is the Substrate framework, so configuration is Substrate's: a manifest that selects modules and gives each its settings, plus a Hadron block.

## The manifest

`cmd/hadron/substrate.yaml` is compiled into the binary. This is what ships:

```yaml
app: hadron
variant: app-host
profiles: [agentic]
modules:
  add: [cli, database, scheduler, events, policy]      # on top of the profile
config:
  logging:   { level: info, format: text }
  httphost:  { addr: "127.0.0.1:8095" }
  database:  { sqlite: { path: hadron.db } }
  scheduler: { autostart: false }                        # the runtime starts it
  policy:    { mode: shadow }                            # release: enforce
```

`hadron config` prints the effective result.

## Overriding it

Point `SUBSTRATE_CONFIG` at a YAML file with the keys you want to change, using the same structure as the manifest's `config:` block. It is merged over the compiled config:

```yaml
# /etc/hadron/override.yaml
httphost:
  addr: "127.0.0.1:9000"
hadron:
  data_dir: /var/lib/hadron
  workers: 6
database:
  sqlite:
    path: /var/lib/hadron/hadron.db
```

```sh
SUBSTRATE_CONFIG=/etc/hadron/override.yaml hadron serve
```

Relative paths resolve against the directory you start `hadron` in, so use absolute paths for a service.

## The `hadron:` block

| Key | Default | |
|---|---|---|
| `data_dir` | `hadron-data` | Workflow files, the catalog, artifacts, the allow-list, keys. Made owner-only and resolved to a real absolute path at start (the artifact store refuses a path through a symlink) |
| `workers` | `3` | Node workers executing steps |
| `event_stream_limit` | the engine default | Concurrent event streams per principal |
| `events.outbox` | `false` | Write an event-stream envelope in the same transaction as each state change, for a later relay. Nothing is sent anywhere |
| `agent_substrates` | none | Where `agent_session` steps run; see [Operations](operations.md). With none, the `agent_session` kind refuses with a clear message |

An unknown key under `hadron:` is an error, not ignored.

## Other modules you will touch

| Module | Keys | See |
|---|---|---|
| `httphost` | `addr` | Non-loopback addresses need authentication on (off and permissive allow only loopback, a tailnet or a unix socket) |
| `auth` | `mode`, `api_keys`, `jwks_url`, `issuer`, `audience`, `token_file`, `mint_url`, ... | [Access](access.md) |
| `policy` | `mode` (`shadow` or `enforce`), `fail_mode` (`closed`) | [Access](access.md) |
| `database` | `sqlite.path` | SQLite only today |
| `logging` | `level`, `format` | `text` or `json` |
| `mcp` | `version`, `scope` (default `hadron.mcp`), `allowed_origins`, `stdio_token_file` | [MCP](mcp.md) |

## Environment

| Variable | |
|---|---|
| `SUBSTRATE_CONFIG` | Path to the override file |
| `HOLLIS_AUTH_HADRON`, `HOLLIS_AUTH` | The authentication dial: `off`, `permissive`, `enforce` (the first, then the second, when `auth.mode` is not set). Default `off` |
| `HADRON_OTEL` | Truthy turns on the OpenTelemetry exporter ([Operations](operations.md)) |
| `HADRON_ENV`, `HOLLIS_ENV` | Deployment tag recorded on telemetry (default `dev`) |

The old app's `HADRON_*` settings, `settings.json` and `~/.hadron` are not read.
