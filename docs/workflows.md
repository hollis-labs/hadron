# Workflow authoring and operations

## Source and schema

A graph-native YAML document contains `workflow`, optional `on`, typed
`inputs`, typed `outputs`, `steps`, and optional `finally`. Structural objects
are closed: unsupported fields produce diagnostics instead of being ignored.
Bindings use one explicit mode—`literal`, `expression`, or `interpolation`—and
`needs` is the only dependency source. See
[`workflow/compile/SOURCE.md`](../workflow/compile/SOURCE.md) for the complete
lowering contract.

The authoritative generated JSON Schema is
[`workflow/graph/schema/workflow.schema.json`](../workflow/graph/schema/workflow.schema.json).
It is generated from Go graph types with:

```sh
go generate ./workflow/graph
go test ./workflow/graph/...
```

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

The executor registry freezes name/version/schema/effects/capabilities before a
plan is admitted. A source cannot narrow those facts. Other repository adapters
are public embeddable contracts, but are unavailable in the stock production
host and therefore fail validation instead of being advertised.

Runnable examples are under [`examples/workflow/production`](../examples/workflow/production/).
The HTTP/cmd/MCP files one directory above are explicitly broader compiler or
fake-adapter conformance fixtures.

## Validate, explain, and run

```sh
hadron workflow validate <file|exact-registry-ref> --json
hadron workflow explain <file|exact-registry-ref> --input-json '{}' --json
hadron workflow run <file|exact-registry-ref> \
  --run-id <id> --idempotency-key <key> --input-json '{}' --json
```

Validation cannot start a run. Explain is policy checked and returns the real
immutable plan/effect/capability/blast-radius facts. Run identity and the
idempotency key bind immutable start intent; a replay converges, while changed
definition/input/scope/target intent conflicts. Pins are immutable value refs
included in that digest and bound before any node becomes ready.

File refs retain the clean supplied path as their locator. Registry refs use
the exact form `namespace/name@version#sha256:<digest>`; current aliases are not
accepted for exact mutations.

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

- **`unknown command` for `run`/`validate`:** use `hadron workflow run` or
  `hadron workflow validate`; legacy root commands were retired.
- **unknown step kind:** compare the plan with the six production kinds above.
  A conformance example using HTTP/cmd/MCP is not daemon-runnable.
- **policy confirmation required:** inspect `workflow explain`, then repeat with
  `--confirm` only after reviewing the exact effects and target.
- **hidden record looks missing:** intentional. Hidden and nonexistent records
  use the same safe not-found response.
- **run is waiting:** inspect the safe wait descriptor and resume using its
  exact wait/correlation/source contract and an authorized responder.
- **private output is masked:** request authorized private display; secret
  material cannot be unmasked.
- **daemon reports unavailable/recovering:** wait for host recovery. Health is
  backed by the shared workflow host rather than a hard-coded OK response.
