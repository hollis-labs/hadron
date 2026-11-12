# Workflows

A workflow is a graph of steps. Each step has a kind, typed inputs and outputs, and declared effects; the compiler checks the graph before anything runs, and the daemon runs it durably: state is in the database, so a restart resumes where it left off.

## Writing one

The two shipped examples are the best reference ([examples](../examples/workflow/README.md)). The shape:

```yaml
workflow:
  id: hello-transform
  version: v1
inputs:
  - name: message
    type: string
    required: true
steps:
  - id: echo
    kind_version: v1
    transform:                  # the step kind and its config
      result: inputs.message
    with:                       # inputs bound to this step
      message: inputs.message
    outputs:
      result: {type: string}
    effects: [compute]          # read, compute, materialize, mutate or destructive
outputs:
  result:
    type: string
    value: steps.echo.outputs.result
```

Steps run when the steps they `needs` are done, and read each other through `steps.<id>.outputs.<name>`.

The daemon registers these step kinds:

| Kind | For |
|---|---|
| `transform` | Compute outputs from expressions |
| `script` | Deterministic JavaScript in a bounded sandbox |
| `sleep`, `wait_for`, `message_wait` | Wait for time, a condition or a message |
| `human_gate` | Stop for a person's decision |
| `call` | Start another workflow as a child |
| `agent_session` and the `agent_launch` shorthand | Run an agent on a Tether session; only when `hadron.agent_substrates` names one ([Operations](operations.md)) |

A kind that is not on this list does not compile.

## Validate, then run

```sh
hadron workflow validate hello-transform.workflow.yaml     # compile and check; changes nothing
hadron workflow explain  hello-transform.workflow.yaml     # policy-check effects and blast radius; admits no work
hadron workflow run      hello-transform.workflow.yaml --input-json '{"message":"hi"}'
hadron workflow run      ... --dry-run                      # a side-effect-free preview, where the workflow supports it
```

Diagnostics print as `severity CODE file:line:col: message`, and the command exits non-zero when there are any. Re-running with the same `--run-id` and `--idempotency-key` is safe: you get the same run back. A different request under the same key is a conflict.

A run is operated by its id:

```sh
hadron workflow inspect <run>            # nodes, attempts, waits, values
hadron workflow events <run> [--after <cursor>] [--wait-ms 15000]
hadron workflow waits <run>              # what it is waiting for
hadron workflow resume <run> --wait-id W --correlation C --payload-json '<typed value>'
hadron workflow signal <run> --name N --correlation C --payload-json '<typed value>'
hadron workflow cancel <run> --reason "..."
hadron workflow rerun <run> --from-node <node>
```

Resume and signal payloads are typed values (type, inline data and provenance), not bare JSON; `waits` shows what a wait expects.

## Where a workflow lives

| Form | Reference | Who can use it |
|---|---|---|
| A file in `<data_dir>/workflows` | `name.workflow.yaml` | Administrators and the local operator |
| A registered version | `namespace/name@version` or `namespace/name@sha256:<digest>` | Anyone the [access rules](access.md) allow |

A file is quick for development. A **registered** version is immutable and addressed by digest; it is what you publish, pin into an exposure profile, schedule, or show to agents.

## Registering and publishing

Registration takes an authoring envelope (a draft) and a contract suite: deterministic test cases that must pass before the version exists.

```sh
hadron workflow author validate  --draft draft.json
hadron workflow author scaffold  --draft draft.json            # generates an editable contract-test scaffold
hadron workflow author test      --draft draft.json --suite suite.json
hadron workflow author register  --draft draft.json --suite suite.json [--make-current]
```

The draft and suite formats are described by the API schema, `internal/api/schema/workflow-api.schema.json` (and `workflow.ts` for TypeScript clients). Then, with the exact reference `namespace/name@version#sha256:<digest>`:

```sh
hadron workflow inspect-version <exact-ref>
hadron workflow registry pin <exact-ref>          # a pin is required before publishing
hadron workflow registry publish <exact-ref>      # published workflows are shown to agents as tools and in the agent card
hadron workflow registry unpin|clear-current <exact-ref>
hadron workflow registry package <exact-ref> --suite suite.json
hadron workflow search --namespace <ns> [query]
```

The catalog is searched one namespace at a time.

## Confirmation

A workflow with mutating or destructive effects, and every workflow that uses `call`, needs explicit confirmation to start: without it the start is refused with `confirmation_required`. A person confirms with `--confirm`. An **agent can never confirm**: its `confirmed` flag is refused (`confirmation_not_permitted`), whatever scopes it holds.

Work that starts with nobody to ask (a schedule, an event, a failure handler) cannot confirm either, so it needs an **unattended allow-list** entry.

### The unattended allow-list

`<data_dir>/workflow-unattended.json` lists exact workflows that may start without confirmation:

```json
{
  "version": 1,
  "entries": [
    {
      "id": "nightly-report",
      "plan_id": "nightly-report",
      "digest": "sha256:<graph digest>",
      "scope": {"activation_id": "", "principal": ""},
      "expires_at": "2027-01-01T00:00:00Z",
      "reason": "why this is safe unattended",
      "added_by": "you",
      "added_at": "2026-11-12T00:00:00Z"
    }
  ]
}
```

- `digest` is the compiled **graph digest** (`hadron workflow validate` prints it as `graph`). Editing the workflow changes it, so an edited workflow needs a new entry. A workflow started as a child of another needs its own entry.
- `scope` is optional and narrows the entry to one activation or one principal; `expires_at` is optional.
- The list can only waive a confirmation; it never overrides a denial. Each start it allows records which entry allowed it.
- The daemon re-reads the file when it changes. It **ignores the whole file** if it is group- or world-writable or owned by another user, and refuses unknown fields so a typo cannot widen an entry. Keep it mode 0600.
- There is no command to edit it yet (tracked as CW-20261112-0026); edit the file. Any process running as your user can write it, including agents you launch, so protect the data directory accordingly.

## Schedules and events

A registered workflow can declare activations (a `schedule` with a cron expression, or an `event`). The daemon's scheduler fires them; a fire is a start like any other, so it follows the confirmation and allow-list rules above. An activation fires for the **current** version, and moving the current alias moves the schedule.

An external system fires an event activation with `POST /api/workflows/activations/<registration-id>/fire` (an `idempotency_key` and the event's time are required).

## Building an executable

```sh
hadron build my.workflow.yaml -o ./my-tool                  # a CLI that runs the workflow with no daemon
hadron build my.workflow.yaml -o ./my-tool --as mcp-server --tool-name my_tool
```

Only workflows made of supported, self-contained kinds build; the command reports what blocks it with located diagnostics. `build` compiles with the Go toolchain inside the Hadron source tree, so it needs a source checkout, not a release binary.
