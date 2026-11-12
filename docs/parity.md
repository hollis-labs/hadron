# Parity: old Hadron against hadron-next

Compared against `hollis-labs/hadron` at `69c9d71` (the version in the scratch clone), surface by surface. Written 2026-11-11 for the owner's sign-off; the cutover decision is the owner's.

Legend: **Done** = present and tested. **Changed** = present in a deliberately different shape. **Dropped** = deliberately not carried (fail forward, owner directives of 2026-11-02/04). **Gap** = a live feature of the old app that the new one lacks. **Decide** = needs an owner decision before it can be classed.

## 1. What needs the owner

| # | Item | Class | Recommendation |
|---|---|---|---|
| D1 | `hadron build` (offline build of a workflow into a daemon-less executable; `internal/offlinebuild`, 866 lines on libs `workflow/engine/offline`) | Gap / Decide | Port (it is a live feature with a libs engine behind it) unless nobody uses it. Ask who does. |
| D2 | Workspaces (`hadron workspace list/create`, `/v1/workspaces`, `workspaces` table) | Decide | Drop. They belong to blueprint runs; the workflow engine's run scope is a different thing. |
| D3 | Hub relay (`hubrelay`, `events_hub.go`: outbox to the event hub) | Decide | Separate decision: is the event hub still the integration, or is framework SSE + MCP enough? |
| D4 | Per-workflow MCP tools and exposure sessions | Decide | Already on your list: exposure redesign discussion. |
| D5 | Agent card public or token-gated | Decide | Gated now (old was open). One-field change. |
| D6 | In-process agent runtimes (`agentsubstrate`, `llmprovider`, `messagesubstrate`, kind `go_agent_runtime`) | Decide | Dropped; confirm. Only Tether sessions exist. |
| D7 | Consumers of the old names (`hadrond`, `/v1/...`, `hadron_workflows_*` MCP tools, `~/.hadron` paths) in other repos, agents and configs | Gap | Inventory before cutover; each needs an update or an alias decision (aliases are against the no-shim rule, so updates). |

## 2. Binaries and commands

| Old | New | Class |
|---|---|---|
| `hadrond` (daemon; flags `--addr --db --logs --data`) | `hadron serve`; settings in the Substrate config (`httphost.addr`, `database.sqlite.path`, `hadron.data_dir`); one binary | Changed |
| `hadrond mcp` (stdio, `--token`, `--token-file`, `HADRON_MCP_TOKEN`) | `hadron mcp --token-file`; token is a framework service token | Changed. Becoming a thin client: CW-20261112-0011 |
| `hadron version` | framework `version` | Done |
| `hadron daemon` (status) | framework `health` | Done |
| `hadron workflow validate / run <file\|registry-ref>` | `workflow validate / run NAME VERSION DIGEST` (registry refs only) | **Gap**: no file references (`./x.workflow.yaml` as a `file` definition relative to the workflow root); no compact ref `ns/name@version#sha256:digest` |
| `hadron workflow explain` | HTTP route exists; no CLI command | **Gap** (small) |
| `hadron workflow inspect / cancel / resume / rerun` | same names | Done (JSON output) |
| `hadron workflow catalog search / inspect`, `author *`, `registry *`, `exposure *` | `search`, `inspect-version`, `author *`, `registry *`, `exposure *` | Done. Different ref syntax (see above) |
| `hadron workflow unattended allow / revoke / list` (edits the allow-list file directly, daemon-less) | none; the daemon still reads `unattended.json` | **Gap** (small, important: without it nobody can add an entry except by hand) |
| CLI output: human diagnostics (`file:line:col code: message`), tables | raw response JSON; failures print `code (status): message` | **Gap** (UX): port the diagnostic formatting for validate/run |
| `hadron build <workflow>` | none | **Gap**, D1 |
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
| Published JSON schema of the API (`internal/api/schema/workflow-api.schema.json`, generated, with a drift test) | none | **Gap**: regenerate for `/api/...` and flat errors, or drop the publication on purpose |
| OTel trace propagation middleware, `hadron.workflow.event_stream.rejected` meter | the stream meter is in the runtime; HTTP propagation is not wired | **Gap** pending Substrate's OTel module (not built) |

## 4. MCP

| Old | New | Class |
|---|---|---|
| Workflow run tools: validate, run, run_inspect, run_cancel, run_events, run_subscribe, run_resume, gate_submit, message_submit, signal | `hadron_workflow_{validate,run,run_inspect,run_cancel,run_events,run_resume,signal}`; subscribe and the two submits merged | Changed |
| Lifecycle tools: catalog search/inspect, author validate/scaffold/test/register, registry package/publish/pin/unpin/clear_current, exposure inspect/pin/unpin | same names | Done (`catalog_search` needs a namespace) |
| `hadron_workflows_search / _load`, `hadron_workflow_describe`, dynamic per-workflow tools, namespaces resource | none | Dropped with exposure sessions, D4 |
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
| `test/e2e` (build tag; health, validate, run with typed JSON, production examples validate-and-run, workspace create, legacy roots refused, CLI refused without operator token) | binary-level tests in `cmd/hadron` (health, serve, MCP, CLI against a live daemon) | **Gap**: production-examples run, and enforce mode with a real token (both untested) |
| `examples/workflow/` (hello-transform, normalize-script, http-cmd-transform, release-approval-gate, torque-task-bulk-create) | none | **Gap**: bring across as examples and acceptance inputs. `demos/` (8 blueprint-era) and `examples/archive`: drop |
| Operator docs (`getting-started`, `install`, `cli-reference`, `mcp-setup`, `safety`, `workflow-development`, `workflows`, `workflow-diagnostics`, `credential-rotation`, `release-ops`, `agentic-workflows`) | README, `docs/status.md`, this file | **Gap**: rewrite against the new surface, not copy; the engine-direction and planning docs stay in the old repo as history |
| `Makefile`, lefthook, release scripts, `hadron.cerberus.yaml` (deploys `bin/hadrond`) | GitHub Actions CI only | **Gap** at cutover: build-1 CI (Cerberus handoff written), the Cerberus deploy file for `hadron serve`, install path, release workflow |
| Repo names | `hadron-next` private | At cutover: old to `hadron-legacy`, new to `hadron`, public (scan-public clean, allowlist reviewed) |

## 8. Known weak spots in what is done

- Enforce-mode access is unit-tested at the scope tables only. No test sends a real token to a route or tool.
- Agent sessions are tested against a fake Tether client, not a live muxd.
- Exposure sessions are gone, yet the exposure profile routes and tools remain and still pin definitions into profiles that nothing mounts as tools. Until D4 they are inert surface.
- The cleanup list in `docs/status.md` (identifier names, file sizes, persistence importing a higher layer, timestamp helpers, outbox overlap with libs) is open.
- `go.mod` requires an untagged Substrate commit; the next Substrate tag is the owner's call.

## 9. Suggested order to close

1. Owner decisions D1 to D7 (D1, D2, D3, D5, D6 are one-line answers).
2. Small CLI gaps: file refs, `explain`, `unattended`, diagnostic formatting. Half a day.
3. Examples and e2e acceptance (production examples; enforce-mode token path).
4. `build` if D1 says port.
5. Exposure redesign (D4) and the OTel hookup when Substrate's module exists.
6. Operator docs, CI on build-1, Cerberus deploy file, consumer inventory (D7).
7. Frontend after the gui track lands.
8. Cutover.
