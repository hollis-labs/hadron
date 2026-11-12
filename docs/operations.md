# Operations

## What is on disk

Two places hold state, both relative to the directory you start `hadron` in unless you set absolute paths ([Configuration](configuration.md)):

| Path | What |
|---|---|
| `hadron.db` (`database.sqlite.path`) | The database: runs, nodes, waits, events, claims, schedules, exposure profiles. SQLite, with its `-wal` and `-shm` files. The schema is applied on start |
| `hadron-data/` (`hadron.data_dir`, mode 0700) | Everything below |
| `hadron-data/workflows/` | File workflows (`*.workflow.yaml`) |
| `hadron-data/workflow-catalog.json` | The registered-workflow catalog (with a `.lock` file) |
| `hadron-data/workflow-artifacts/` | Artifact content addressed from run values |
| `hadron-data/workflow-contract-attestor.key` | The key that signs contract-test reports (with a `.lock` file). Generated on first start |
| `hadron-data/workflow-unattended.json` | The [unattended allow-list](workflows.md#the-unattended-allow-list). You create it |
| `hadron-data/agent-result-secret.key` | Per-installation secret for agent results. Created when an agent substrate is configured |

The files with `.key` in the name are secrets: the daemon creates them 0600 and nothing should read them but the daemon.

## Backups

Stop the daemon, then copy `hadron.db*` and the whole `hadron-data/` directory **together**: the catalog and the attestor key belong to the database's contents, and a restored database without its attestor key may fail to verify contract reports made before. A stopped daemon is the only point at which the SQLite files and the catalog are guaranteed consistent with each other.

Restoring is copying them back before `hadron serve`. State does not move from the previous Hadron.

## Health and logs

- `GET /healthz` (no authentication) reports `{"status":"ok","checks":{"database":"ok"}}` and is the readiness probe. `hadron health` runs the same checks in-process.
- `GET /api/capabilities` lists the modules started. `GET /api/me` shows who the daemon thinks you are.
- Logs go to stderr as text lines (`logging.format: json` for structured output; `logging.level` to change verbosity). Policy decisions are logged with the principal, action, resource and whether they would block.

## Tracing

Every HTTP request, including `/mcp`, continues the caller's W3C `traceparent` and runs in a server span, so Hadron's work appears in the caller's trace whether or not export is on. Set `HADRON_OTEL=1` to export spans and metrics (the exporter is the libs `core/util/otel` one; see its README for its environment). `HADRON_ENV` or `HOLLIS_ENV` tags the deployment. Stdio MCP does not carry trace context yet.

## Workers and schedules

`hadron.workers` node workers execute steps, claiming work from the database with leases, so a crashed worker's work is picked up again and two daemon processes on one database do not run the same step twice. On start the daemon recovers in-flight runs before it serves. Schedules and event activations fire through the framework scheduler, which claims each due fire with a lease.

## Stopping and restarting

`hadron serve` and `hadron mcp` stop on SIGINT or SIGTERM (what launchd, systemd and Cerberus send). A stop **drains**: the HTTP server closes, no new work is claimed, and a step already running gets up to 30 seconds to finish before it is cancelled. The next process picks up the run where it left off.

A step that is cut off mid-attempt (SIGKILL, a crash, a power loss, or a step longer than the drain) is **not retried**: the daemon cannot know whether its effects happened. Its claim lapses after 90 seconds, then the step and its run are marked `crashed`. Until then the run shows `running`. Steps that have not started are unaffected, and a run waiting on a timer, a gate or a message survives any restart. Give a service manager at least 30 seconds before it escalates to SIGKILL (launchd's `ExitTimeOut`, systemd's `TimeoutStopSec`).

## Agent sessions

`agent_session` steps (and the `agent_launch` shorthand) run agents as Tether sessions. Configure a substrate:

```yaml
hadron:
  agent_substrates:
    work:
      kind: tether_session
      tether:
        endpoint: ~/.tether/run/tetherd.sock   # the Tether daemon (default)
        launch: claude-work                    # the Tether launch to start; or launches: {logical_agent: launch_id}
        result_optional: false                 # true: a session that ends without replying succeeds with a null result
        stop_on_result: true                   # stop the session once its result is accepted
        unreachable_timeout: 10m               # how long the daemon may be unreachable before the step fails
```

Without a substrate the kind refuses with a clear message. All `tether_session` substrates must name the same endpoint. Steps may not bind typed inputs to a Tether session (the agent reads its context itself; put the task in `prompt_append`); validation says so. Hadron starts while the Tether daemon is down, and agent steps wait for it up to `unreachable_timeout`.

## Going to production

Development defaults are open on purpose. For a deployment:

- [ ] `auth.mode: enforce` with a verifier (`jwks_url`, or `api_keys`), and a token for each caller. The daemon refuses a non-loopback listener otherwise.
- [ ] `policy.mode: enforce`, after a run in `shadow` with no unexpected would-block lines.
- [ ] `httphost.addr` set deliberately; a reverse proxy or the tailnet in front of it.
- [ ] Absolute `database.sqlite.path` and `hadron.data_dir` on a volume you back up, owned by the service user, not group- or world-writable.
- [ ] Tokens: administrators with `hadron.manage` and `hadron.confirm`; agents with `hadron.run`, `hadron.mcp` and one `hadron.profile.<id>`; no agent with `hadron.manage`.
- [ ] A profile for every agent, with `denied_effects` where the agent should never mutate.
- [ ] `HADRON_OTEL` on, if you collect traces.
- [ ] A restore tested from a backup.

## Troubleshooting

| You see | Because |
|---|---|
| `daemon unreachable at ...` | The CLI cannot reach `httphost.addr` (or `--server`). Start `hadron serve` |
| `unauthenticated (401)` / `policy_denied (403)` | No valid token / the token lacks the route's scope. `GET /api/me` shows what the daemon saw |
| `not_found` on a workflow you know exists | The caller's profile does not cover it (a denied definition is reported as missing). Check `hadron workflow exposure inspect` and the token's `hadron.profile.<id>` |
| `confirmation_required` | The workflow mutates, destroys or calls another; rerun with `--confirm` as a person, or add an allow-list entry for unattended starts |
| `artifact stat failed: invalid` | `hadron.data_dir` goes through a symlink; Hadron resolves it, so this means the resolved path is unusable (permissions) |
| `HADR-HOST-021` on validate | The definition is not authorized for this caller |
| Tools missing from an agent's list | The workflow is not pinned in its profile, the token lacks `hadron.run`, or the workflow is unpublished and unpinned everywhere |
| Allow-list entries ignored | The file is group- or world-writable, owned by another user, or has an unknown field; the daemon logs why |
