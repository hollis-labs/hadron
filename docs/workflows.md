# Workflow authoring and operations

## Source and schema

A graph-native YAML document contains `workflow`, optional `on`, typed
`inputs`, typed `outputs`, `steps`, and optional `finally`. Structural objects
are closed: unsupported fields produce diagnostics instead of being ignored.
Bindings use one explicit mode—`literal`, `expression`, or `interpolation`.
`needs` is the only dependency declaration lowered directly from source; the
production resolver subsequently infers deterministic data edges and value
visibility from static step-output references. See
[go-workflow's source contract](https://github.com/hollis-labs/go-workflow/blob/v0.1.0/compile/SOURCE.md) for the complete
lowering contract.

The authoritative generated JSON Schema is
[published by go-workflow](https://github.com/hollis-labs/go-workflow/blob/v0.1.0/graph/schema/workflow.schema.json).
Hadron consumes it through `github.com/hollis-labs/go-workflow/graph/schema`;
schema generation and compatibility checks run in that module.

Do not edit the schema or copy graph DTOs into a client by hand. The HTTP
schema and TypeScript client consume the same generated graph schema.

## Production host kinds

The stock `hadrond` capability profile registers exactly:

| Kind | Effect | Behavior |
|---|---|---|
| `transform@v1` | `compute` | Evaluate deterministic expressions into declared typed outputs |
| `script@v1` | `compute` | Run bounded deterministic JavaScript in the goja sandbox |
| `sleep@v1` | `read` | Suspend on a durable timer and resume through runtime provenance |
| `wait_for@v1` | `read` | Suspend for a typed authorized callback/continuation |
| `message_wait@v1` | `read` | Suspend for a correlated typed message |
| `human_gate@v1` | `read` | Suspend for an authorized human decision |
| `call@v1` | conservative (`read` through `destructive`) | Start a child workflow as a separately identified run (`mode: run`) |

The executor registry freezes name/version/schema/effects/capabilities before a
plan is admitted. A source cannot narrow those facts. Other repository adapters
are public embeddable contracts, but are unavailable in the stock production
host and therefore fail validation instead of being advertised.

### Child workflows (`call@v1`)

A `call` node resolves its child definition through the same resolver as a
top-level start and completes immediately with the child handle (`run-id`,
`status`, `events-ref`, `cancellation`, `outputs-ref`). Collect the child's
outputs with a separate `wait_for` node:

```yaml
steps:
  - id: launch
    kind: call
    kind_version: v1
    call:
      definition: {kind: file, id: child, locator: child.workflow.yaml, version: v1}
      mode: run
    with:
      message: inputs.message
    idempotency: {mode: keyed, scope: workflow}
    outputs:
      run-id: {type: string}
      status: {type: string}
      events-ref: {type: string}
      cancellation: {type: object}
      outputs-ref: {schema: {type: [object, "null"]}}
  - id: collect
    kind_version: v1
    needs: [launch]
    wait_for:
      child_run: {input: child, fail_on_unsuccessful: true}
      timeout: 1h
      payload_schema: {type: object}
    with:
      child: steps.launch.outputs["run-id"]
```

- Only `mode: run` is supported. `mode: inline` is refused when the
  definition is validated ("inline calls are not supported by hadrond"), so an
  inline call never starts a run.
- Every call-started child passes the same gate as a top-level start before it
  is created: the child plan is validated against the production kinds, policy
  facts are computed under the root run's identity (including execution-target
  capabilities), and the decision is recorded in the policy journal under
  `child-run:<child run id>`. A child the root caller could not start directly
  fails the call step. A child whose policy asks for confirmation runs only
  when the root start was itself confirmed.
- Plans containing a call node always require confirmation to start (the
  child is unresolved at start) and report dry run as unavailable.
- The child terminal status wakes the collecting `wait_for`; only that child
  run may resume it.

### `agent_launch` is not enabled yet

The compiler recognizes `agent_launch` (it lowers to `call@v1` plus a bundled
child running `agent_session@v1`), but this `hadrond` refuses it at
validation with:

> agent_session@v1 is not enabled in this hadrond yet: agent launch needs a
> durable session host (CW-20260930-0227)

A directly authored `agent_session` node is refused with the same message.
The local execution target already grants `agent.session.launch`,
`agent.session.observe`, and `agent.session.cancel` so enabling the session
host only lifts this gate.

### External operations

Kinds that hand work to an external system suspend it as a durable external
operation. `hadrond` reconciles pending operations every 2 seconds (batches of
100) and stops the loop with the other workflow workers on shutdown. Explicit
run cancellation cancels a suspended external step through its adapter.

Runnable examples are under [`examples/workflow/production`](../examples/workflow/production/).
The HTTP/cmd/MCP files one directory above are explicitly broader compiler or
fake-adapter conformance fixtures.

## Validate, explain, and run

```sh
hadron workflow validate <file|registry-selector> --json
hadron workflow explain <file|registry-selector> --input-json '{}' --json
hadron workflow run <file|registry-selector> \
  --run-id <id> --idempotency-key <key> --input-json '{}' --json
```

Validation cannot start a run. Explain is policy checked and returns the real
immutable plan/effect/capability/blast-radius facts. Run identity and the
idempotency key bind immutable start intent; a replay converges, while changed
definition/input/scope/target intent conflicts. Pins are immutable value refs
included in that digest and bound before any node becomes ready.

File refs retain the clean supplied path as their locator. These operational
commands accept `namespace/name@version` or
`namespace/name@sha256:<digest>`, but not the combined lifecycle grammar.
Catalog inspection and registry/exposure mutations require
`namespace/name@version#sha256:<digest>` so version and digest are bound
together; current aliases are not accepted for exact mutations.

## Confirmation and unattended starts

The production start policy asks for confirmation when a workflow's effects
advise it: any `mutate` or `destructive` effect, or an unresolved `call` node.
An interactive caller confirms with `--confirm` (CLI), `confirmed: true`
(HTTP, MCP, A2A) or the UI checkbox; the start record keeps who confirmed.
Retrying with the same idempotency key after `confirmation_required` is safe:
the stored decision is reused, and confirming is not part of the request
digest.

A start with nobody to confirm it (a schedule, timer, trigger, reactor or run
failure handler) is refused unless the operator allow-list covers it:

```sh
hadron workflow validate <file|registry-selector>   # prints the plan id and a "graph sha256:..." line
hadron workflow unattended allow --plan <plan-id> --digest <graph-digest> \
  --reason "nightly report" [--activation <registration-id>] \
  [--principal <principal>] [--expires 720h]
hadron workflow unattended list [--json]
hadron workflow unattended revoke <entry-id>
```

- An entry pins one plan id and one exact **graph digest** (the `graph` line
  of `workflow validate`, or `graph_digest` in its JSON). It is the same
  whether the workflow starts at top level or as a child. Editing the workflow
  changes it, so the edited workflow needs a new entry.
- **Child runs need their own entries.** A `call` step's child run is
  evaluated against the list on its own plan id and graph digest; the root's
  entry never covers it. Children resolve at call time and are not pinned by
  the root's digest, so otherwise an edited child could run unattended under
  the root's entry. Under an allow-listed root, a child that needs
  confirmation and has no entry of its own is refused
  (`child_run_policy_denied`). A root a human confirmed still covers its
  children. A child inherits its root's activation, so an entry scoped with
  `--activation` matches a child only when that activation started the run.
- `--activation` and `--principal` narrow the entry; `--expires` ends it.
  Expired entries stay in the file and show as `expired` in `list`, but never
  match.
- The allow-list can only waive a confirmation. It never overrides a denial.
- A matching start is allowed, and its persisted policy decision carries
  `allowlist_entry`, `allowlist_reason`, `allowlist_added_by`,
  `allowlist_added_at` and `allowlist_file_mtime`. The start record's
  `confirmation.allowlist_entry` names the entry too.
- A dry run has no effects, so it never asks for confirmation. It still needs
  every step to support dry-run and still honors a denial.

**Where the list lives and who can change it.** The list is
`~/.hadron/workflow-unattended.json` (under `--data-dir`). The CLI edits that
file directly; there is no HTTP, MCP or A2A route that reads or writes it, so
nothing on the loopback surface can change it. The daemon re-reads the file
when it changes and logs every entry added, changed, removed or expired, with
`added_by`, `added_at`, the reason and the file's mtime. It refuses the whole
file (allowing nothing, and logging why) if the file is a symlink, group- or
world-writable, owned by another uid, or malformed.

**The boundary is filesystem ownership, and the realistic threat is same-uid
agents, not other users.** Every agent session Hadron, Tether or Torque
launches runs as the operator's uid. Such an agent can write this file and
allow-list its own workflow, including an entry for a digest it just edited;
the digest pin does not stop that, and `added_by` is only a label. The
mitigation is sandboxing agent launches so they cannot write Hadron's data dir
(or the Tether and Torque state dirs), tracked as Torque task
CW-20260930-0237. Until that sandbox exists, treat the
allow-list as protection against mistakes and accidental unattended runs, not
against an agent acting on your account.

## Durable waits and signals

Wait-capable executors persist a `WaitRecord` and yield the worker. Runtime
recovery reconstructs timers and readiness after restart. Resume proves the
wait belongs to the run, binds the authenticated responder, and checks the
expected correlation, token when present, typed payload schema, wake source,
and responder authority. Duplicate delivery converges by wait identity.

Typed signals use the same application boundary and require their idempotency
key. Gate/message MCP aliases map to the same wait contract; they do not create
transport-private lifecycle.

## Values, artifacts, and redaction

Inputs, node outputs, wait payloads, and final outputs are typed `Value`
records with visibility and provenance. Inline values are bounded; larger data
uses immutable artifact references. Rendering is an explicit projection:

- public values can be displayed;
- private values require authorized private-display access;
- secret references and secret material remain masked in every display mode;
- diagnostics, events, HTTP/MCP/A2A results, and transport errors use bounded
  safe DTOs rather than raw runtime records.

Inspect with `hadron workflow inspect <run-id> --json`. Add
`--reveal-private` only when the caller is authorized and needs it; this does
not reveal secrets.

## Registry, qualification, and exposure

The lifecycle is `search → draft → validate → scaffold → contract test →
register → package → exact registry pin → publish → exposure-profile pin`.
Contract tests are deterministic evidence tied to the exact plan. Package
responses contain safe metadata, not raw source, credentials, or mock payloads.

Three states are independent:

1. `current` is the movable catalog alias.
2. A registry-version pin qualifies one exact immutable version and is a
   publication prerequisite.
3. An exposure-profile pin makes one exact published definition directly
   visible as a tool, subject to profile generation CAS, effects, collision,
   and budget preflight.

Removing an exposure pin removes the direct tool on session reconciliation;
it does not move `current` or unqualify the registry version.

## Source activations

`on:` declarations compile into immutable plan activations. Registering a
workflow as current materializes source-owned registrations through the shared
activation service. Moving/clearing current retires the prior projection, and
fire-time admission is fenced against the movable catalog alias so a stale
durable row cannot start the wrong version.

The public external fire route accepts only payload-ingress activation kinds,
requires the authorized exact registration handle returned by lifecycle
detail, validates occurrence/receipt time, and executes as the registration's
durable principal. Schedules and timers fire internally; they are not generic
external trigger handles.

## Troubleshooting

For a fuller graph-native operator walkthrough, see
[Workflow diagnostics and recovery](workflow-diagnostics.md).

- **`unknown command` for `run`/`validate`:** use `hadron workflow run` or
  `hadron workflow validate`; legacy root commands were retired.
- **unknown step kind:** compare the plan with the six production kinds above.
  A conformance example using HTTP/cmd/MCP is not daemon-runnable.
- **policy confirmation required:** inspect `workflow explain`, then repeat with
  `--confirm` only after reviewing the exact effects and target. For a
  schedule or trigger, see
  [Confirmation and unattended starts](#confirmation-and-unattended-starts).
- **hidden record looks missing:** intentional. Hidden and nonexistent records
  use the same safe not-found response.
- **run is waiting:** inspect the safe wait descriptor and resume using its
  exact wait/correlation/source contract and an authorized responder.
- **private output is masked:** request authorized private display; secret
  material cannot be unmasked.
- **daemon reports unavailable/recovering:** wait for host recovery. Health is
  backed by the shared workflow host rather than a hard-coded OK response.
