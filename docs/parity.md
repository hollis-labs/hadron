# Parity: old Hadron against hadron-next

Compared against `hollis-labs/hadron` at `69c9d71` (the version in the scratch clone), surface by surface. Written 2026-11-11 for the owner's sign-off; the cutover decision is the owner's.

Legend: **Done** = present and tested. **Changed** = present in a deliberately different shape. **Dropped** = deliberately not carried (fail forward, owner directives of 2026-11-02/04). **Gap** = a live feature of the old app that the new one lacks. **Decide** = needs an owner decision before it can be classed.

## 1. Owner decisions (2026-11-11)

| # | Item | Decision |
|---|---|---|
| D1 | `hadron build` | Port. **Done** (`internal/offlinebuild`, `hadron build`). It compiles with the Go toolchain inside the Hadron module source tree, as before, so it works from a source checkout, not from a downloaded release binary |
| D2 | Workspaces | Drop |
| D3 | Hub relay | Parked for post-MVP review: CW-20261112-0028 |
| D4 | Per-workflow MCP tools / exposure sessions | Decided 2026-11-11 and built: profiles enforced through the policy engine, profile id as a token claim, typed tools. See `docs/status.md` "Exposure and access" |
| D5 | Agent card gated, not public | Accepted; documented here and in `docs/status.md` |
| D6 | In-process agent runtimes | Stay dropped for MVP; post-MVP review: CW-20261112-0029 |
| D7 | Consumers of the old names | Inventory done: section 10 |
| | `workflow unattended allow\|revoke\|list` | Follow-up: CW-20261112-0026 |
| | Thin stdio MCP client | Follow-up: CW-20261112-0011 |

Post-MVP items belong to the normal Hadron dev cycle, not this workstream. Frontend waits for the gui track.

## 2. Binaries and commands

| Old | New | Class |
|---|---|---|
| `hadrond` (daemon; flags `--addr --db --logs --data`) | `hadron serve`; settings in the Substrate config (`httphost.addr`, `database.sqlite.path`, `hadron.data_dir`); one binary | Changed |
| `hadrond mcp` (stdio, `--token`, `--token-file`, `HADRON_MCP_TOKEN`) | `hadron mcp --token-file`; token is a framework service token | Changed. Becoming a thin client: CW-20261112-0011 |
| `hadron version` | framework `version` | Done |
| `hadron daemon` (status) | framework `health` | Done |
| `hadron workflow validate / explain / run <file\|registry-ref>` | same, with `--input`, `--input-json`, `--run-id`, `--idempotency-key`, `--confirm`, `--dry-run`, `--pin`, identity flags | Done (file refs, `ns/name@version`, `ns/name@sha256:...`, exact `ns/name@version#sha256:...`) |
| `hadron workflow inspect / cancel / resume / rerun` | same names | Done (JSON output) |
| `hadron workflow catalog search / inspect`, `author *`, `registry *`, `exposure *` | `search`, `inspect-version`, `author *`, `registry *`, `exposure *` | Done. Different ref syntax (see above) |
| `hadron workflow unattended allow / revoke / list` (edits the allow-list file directly, daemon-less) | none; the daemon still reads `unattended.json` | **Gap**, follow-up CW-20261112-0026 (until then entries are added by hand) |
| CLI output: human diagnostics (`file:line:col`), `--json` | `-o text` (default: diagnostics `severity CODE file:line:col: message`, summary lines), `-o json`, `-o yaml`; commands without a text form print indented JSON | Done |
| `hadron build <workflow>` | `hadron build` | Done (D1) |
| `hadron auth ui / rotate`, `hadron credential issue / revoke / list / audit` | none; framework auth (Tether is the issuer; `HOLLIS_AUTH_HADRON` dial) | Dropped. Needs a doc: how an operator gets a token for the CLI (`auth.token_file` / `mint_url`) |
| `hadron workspace *` | none | Dropped, D2 |

## 3. HTTP

| Old | New | Class |
|---|---|---|
| `/v1/health` | `/healthz` | Done |
| `/v1/workflows/{validate,explain,runs,runs/{id}/{inspect,cancel,resume,rerun,waits,values,events},activations/{id}/fire,lifecycle/...}` | same under `/api/workflows/...`, flat errors, scopes | Done |
| `/v1/workflows/runs/{id}/events/stream` (SSE) | same under `/api/...` | Done |
| (no HTTP route) signal | `/api/workflows/runs/{id}/signal` | Done, new |
| `/.well-known/agent.json`, `/a2a/tasks...` | same paths via `Mounts` | Done (card gated, D5) |
| `/v1/auth/credentials/`, `/v1/auth/`, `/auth/` (operator sign-in, exposure credentials) | none | Dropped |
| `/v1/workspaces`, `/v1/messages*`, `/v1/runs`, `/v1/schedules`, `/v1/pipelines`, `/v1/triggers`, `/v1/human-gates`, `/v1/blueprints/validate` | none | Dropped (blueprint/pipeline era, already behind `EnableLegacyRuntime`), D2 for workspaces |
| Web UI served by the daemon | none | Frontend on hold until the gui track lands |
| CORS middleware | framework host | Changed |
| Published JSON schema of the API (generated, drift test) | v2: `internal/api/schema/workflow-api.schema.json` and `workflow.ts`, `/api/workflows` paths, per-operation scope, flat `ApiError`; `go generate ./internal/api` | Done |
| OTel trace propagation middleware, exporter init (`HADRON_OTEL`) | HTTP trace propagation and exporter in `cmd/hadron/telemetry.go`; the stream meter is in the runtime | Done for HTTP (and `/mcp`). Stdio MCP `_meta` propagation not wired. Moves into Substrate's OTel module when it exists |

## 4. MCP

| Old | New | Class |
|---|---|---|
| Workflow run tools: validate, run, run_inspect, run_cancel, run_events, run_subscribe, run_resume, gate_submit, message_submit, signal | `hadron_workflow_{validate,run,run_inspect,run_cancel,run_events,run_resume,signal}`; subscribe and the two submits merged | Changed |
| Lifecycle tools: catalog search/inspect, author validate/scaffold/test/register, registry package/publish/pin/unpin/clear_current, exposure inspect/pin/unpin | same names | Done (`catalog_search` needs a namespace) |
| `hadron_workflows_search / _load`, `hadron_workflow_describe`, dynamic per-workflow tools, namespaces resource | typed per-workflow tools (one per exposed workflow, filtered per caller); search/load/describe/namespaces not carried (discovery is `tools/list`, the agent card and `catalog_search`) | Changed |
| Blueprint, pipeline, run, schedule, trigger, human-gate, message, workspace, registry tools; skills tool; prompts/resources | none | Dropped (blueprint era). The skills tool and the `docs/mcp-skills` content were not reviewed: **check before closing** |
| Stdio and HTTP | both | Done |
| Principal tokens via `hadron credential` | framework auth, per-tool scopes | Changed |

## 5. Engine host and runtime

| Old | New | Class |
|---|---|---|
| Engine host (`appworkflow`), state store, hoststate, rundiagnostics, A2A, artifacts, unattended, workflow catalog, pack/registry (workflow halves) | ported with tests | Done |
| Composition root (`cmd/hadrond/workflow_*.go`), workers, activation bridge, external reconciler, contract attestor | `internal/runtime` | Done |
| Settings file (`settings.json`) | `hadron:` block, strict | Changed |
| Local auth + HTTP authenticator + exposure sessions | Principal to grants; no local auth | Changed; exposure sessions are D4 |
| Source activations (own scheduler/trigger) | framework `scheduler` module over the activation tables | Changed |
| Agent sessions on Tether | `tetherhost`, `hadron.agent_substrates` | Done. Not run against a live muxd |
| In-process agent runtimes, LLM provider, message substrates, launch artifacts | none | Dropped, D6 |
| Event outbox, event-stream limit | `hadron.events.outbox`, `event_stream_limit` | Done |
| Hub relay | none | D3 |
| Execution manager, blueprints, pipelines, `lint`, `testgen`, `specparse`, `telemetry` (per-run JSONL), legacy `scheduler` and `trigger` | none | Dropped. `telemetry` and `launchartifacts` were consumers of the execution manager only |
| `workflowdependency` (guard on the extracted-library import boundary) | none | Dropped as a package. Layering is now enforced by Substrate's module rules; no replacement check |

## 6. Data

Schema: one goose migration of the 80 `workflow_*` tables plus `store_generation`, verified against the old final schema. Not carried: `blueprint_registry`, `blueprint_versions`, `pipeline_runs`, `pipeline_stage_runs`, `human_gates`, `legacy_schedule_fires*`, and the unported `schedules`, `triggers`, `messages`, `workspaces`, `runs`, `run_events` (all legacy; confirm with D2). Existing data: **none moves** (owner decision: start empty). Anyone with in-flight runs or registered workflows on the old instance loses them at cutover.

## 7. Tests, examples, docs, release

| Old | New | Class |
|---|---|---|
| ~all workflow unit tests | ported with their code | Done |
| `test/e2e` (health, validate, run with typed JSON, production examples validate-and-run, workspace create, legacy roots refused, CLI refused without operator token) | binary-level tests in `cmd/hadron`: health, serve, MCP, CLI against a live daemon, output formats, file refs, build, shipped examples validate and run | Done except **enforce mode with a real token**, still untested |
| `examples/workflow/` (hello-transform, normalize-script, release-approval-gate; http-cmd-transform and torque-task-bulk-create) | the first three, with a rewritten README, validated and run in the acceptance test (`release-approval-gate` needed a static `environment` to validate on the current engine). The other two needed kinds the host does not register and tests that are gone | Done. `demos/` and `examples/archive`: dropped |
| Operator docs (`getting-started`, `install`, `cli-reference`, `mcp-setup`, `safety`, `workflow-development`, `workflows`, `workflow-diagnostics`, `credential-rotation`, `release-ops`, `agentic-workflows`) | `README.md`, `docs/{getting-started,workflows,cli,configuration,access,mcp,operations}.md`, written against the new code | Done. Not carried: `install` (no release binary yet), `release-ops` (the release process is the cutover work), `credential-rotation` (framework auth), `workflow-diagnostics` (folded into `workflows.md` and `operations.md`); the engine-direction and planning docs stay in the old repo as history |
| `Makefile`, lefthook, release scripts, `hadron.cerberus.yaml` (deploys `bin/hadrond`) | GitHub Actions CI only | **Gap** at cutover: build-1 CI (Cerberus handoff written), the Cerberus deploy file for `hadron serve`, install path, release workflow |
| Repo names | `hadron-next` private | At cutover: old to `hadron-legacy`, new to `hadron`, public (scan-public clean, allowlist reviewed) |

## 8. Known weak spots in what is done

- Agent sessions are tested against a fake Tether client, not a live muxd.
- Dead code from the old exposure sessions remains in `appworkflow` (`ResolveSession`, durable MCP principal methods and their persistence tests); cleanup list.
- The enforce-mode access path is now tested end to end (`TestEnforcedCallersSeeAndRunOnlyTheirProfile`). Agent sessions are still only tested against a fake Tether client.
- The cleanup list in `docs/status.md` (identifier names, file sizes, persistence importing a higher layer, timestamp helpers, outbox overlap with libs) is open.
- `go.mod` requires an untagged Substrate commit; the next Substrate tag is the owner's call.

## 9. What is left to cutover

1. Frontend, after the gui track lands.
2. CI on build-1, the Cerberus deploy file for `hadron serve`, and the consumer updates in section 10.
3. Live muxd check for agent sessions. (Enforce-mode access with real API keys is now covered by a binary test.)
4. Cutover: rename repos, make the new one public.

## 10. Consumers of the old surface (inventory, 2026-11-11)

Found outside the two Hadron repos by a read-only search of `~/dev`, `~/.claude`, `~/.codex`, `~/.tether` and `~/.cerberus`. The default address `127.0.0.1:8095` is unchanged.

Runtime callers, most likely to break silently first:

1. **Tether MCP proxy entry** `~/.tether/catalog/mcp-servers/hadron.yaml` spawns `/Users/<user>/go/bin/hadrond mcp`. Every `hadron_*` tool seen by Tether, Nanite, Torque and mux comes through it. It must become `hadron mcp --token-file <file>`; no token is passed today.
2. **Cerberus**: `~/.cerberus/registry.yaml` registers the old repo's `hadron.cerberus.yaml` (service on 8095, plus an old GUI entry on 34116) and a launchd job runs its binary. Re-point to the new repo's manifest running `hadron serve`; drop the GUI entry.
3. **Nanite embedded skills** (`hadron-run.md`, `blueprint-builder.md`, `qhealth.md`, `qstatus.md`): call blueprint-era tools (`hadron_blueprint_validate`, `hadron_run_enqueue`, `hadron_run_get`, `hadron_blueprints_list`) that the new app does not have, and probe `/v1/health`. Rewrite against `hadron_workflow_*` and `/healthz`.
4. **Tether tool broker rules** (`libs/go-toolbroker/broker/default-rules.yaml`, 43 hits for `hadron_bp_*` and friends) go dead silently, since the new app has no `bp_*` tools.
5. **`stack-explorer`** shells out to `hadron run <blueprint.yaml> --input ...` (`scripts/batch-scan.sh`, `internal/jobs/service.go`). The new CLI is `hadron workflow run`.
6. **Torque and Tachyon workflow console UIs** expect `hadron_workflow_gate_submit`, `run_subscribe` and the run/gate/signal tools (specs and client stubs, not live calls yet): now `run_resume` with `wake_source`, and `run_events` with `wait_ms`.
7. **`~/.hadron/`** (settings, `state/hadron.db`, blueprints, workflows, artifacts, attestor key) is ignored by the new app and not migrated.

Config, deploy and docs: `apps/current-cerberus-config.yaml` (`./bin/hadrond serve`, `hadron-gui`), Tether launch and boot-profile specs and `~/.config/tachyon/projects.json` and `~/.codex/config.toml` (project paths, if the repo path changes), the website module page (`hadrond serve`, `hadrond mcp -token`, download URL), `torque/docs/architecture/mcp-registry.md`, `tether/docs/{agent-reach,secrets}.md` (`HADRON_MCP_TOKEN`), `chrispian/scopes/hollis-labs/hadron.md`, and `stack-explorer/repos.example.yaml`. Test fixtures in Tether, Nanite and `go-toolselect` use old names as data only. Static server lists that name the id `hadron` (`torque/internal/config/mux_servers.go`, `mcp-wire`) are safe if the server id stays `hadron`.

Not covered: other machines' launchd and MCP entries, plugin caches and Tesseract-stored skills (Tesseract recall found none), CI release downloads in other monorepos.
