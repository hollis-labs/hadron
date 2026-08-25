# CLI reference

The active root command set is deliberately small:

```text
hadron build       build an offline workflow plan bundle
hadron daemon      report daemon and workflow-host health
hadron version     print build information
hadron workflow    operate graph-native workflows
hadron workspace   manage workspace metadata
```

The retired root commands `run`, `validate`, `lint`, `schedule`, `pipeline`,
and `blueprint` are unavailable. Use `hadron workflow ...` for execution and
source-declared `on:` activations for scheduling or external ingress.

All daemon commands accept `--addr` (default `http://127.0.0.1:8095`). The
daemon authenticates the caller; `--principal` is only an identity hint and
cannot override authenticated context.

## Definition references

A command accepting `<file|registry-ref>` takes either a supplied file path or
an exact registry reference. Lifecycle commands always require the exact form:

```text
namespace/name@version#sha256:<hex-digest>
```

The parser keeps the full namespace/name, version, and digest. It does not
derive semantic identity from a filename or accept an ambiguous current alias
where an exact mutation is required.

## Validate and explain

```sh
hadron workflow validate <file|registry-ref> [--json]
hadron workflow explain <file|registry-ref> \
  [--input <file|-> | --input-json <object>] [--confirm] [--json]
```

`validate` never starts a run. `explain` performs the same application-level
resolution and authorization and reports effects, capabilities, target, and
blast-radius facts. Explain/dry-run are truthful: unsupported non-effecting
preview fails closed, and any durable audit/binding record is reported rather
than described as “no record.” Neither operation admits runnable nodes.

## Run and inspect

```sh
hadron workflow run <file|registry-ref> \
  --run-id <id> --idempotency-key <key> \
  [--input <file|-> | --input-json <object>] \
  [--pin 'node={"id":"...","digest":"..."}'] \
  [--dry-run] [--confirm] [--json]

hadron workflow inspect <run-id> [--reveal-private] [--json]
```

Inputs are bounded typed JSON. Duplicate keys, trailing JSON, malformed values,
and oversized files are rejected. Pins are authorized immutable value
references and are bound before a run becomes runnable. Permanent pin rejection
leaves a terminal, inspectable run with no admitted work.

The start result is asynchronous. `inspect` returns safe run/node/wait/value
diagnostics. `--reveal-private` requires display authority; secret values stay
masked.

## Cancel, resume, and rerun

```sh
hadron workflow cancel <run-id> \
  --idempotency-key <key> [--reason <text>] [--json]

hadron workflow resume <run-id> \
  --wait <wait-id> --correlation <value> --source <source> \
  --token-file <path> \
  [--payload <file|-> | --payload-json <typed-value>] \
  [--idempotency-key <key>] [--json]

hadron workflow rerun <source-run-id> --from <node-id> \
  --run-id <new-id> --idempotency-key <key> [--json]
```

Cancel and rerun use the authenticated run-operation authority. Resume binds
the current responder, proves the wait belongs to the run, and delegates token,
schema, correlation, and responder authorization to the wait coordinator. A
run ID or wait ID is never a bearer capability.

## Scope and execution target

Validate, explain, run, inspect, cancel, resume, rerun, and lifecycle commands
share these selectors:

```text
--scope-kind project|account|session|team|user
--scope-id <id>
--target-id <id>
--target-kind <kind>                 repeatable
--target-capability <capability>     repeatable
--target-sandbox <mode>              repeatable
--target-label key=value             repeatable
```

They are policy inputs, not authorization assertions. The application host
binds them to the authenticated identity and fails closed on a mismatch.

## Catalog and lifecycle

```text
hadron workflow catalog search [query] [--namespace <name>] [--limit 1..100]
hadron workflow catalog inspect <exact-ref>

hadron workflow author validate <authoring-envelope.json> --id --version --namespace
hadron workflow author scaffold <authoring-envelope.json> --id --version --namespace
hadron workflow author test <authoring-envelope.json> --id --version --namespace --suite <json>
hadron workflow author register <authoring-envelope.json> --id --version --namespace --suite <json> [--make-current]

hadron workflow registry package <exact-ref> --suite <json>
hadron workflow registry pin-version <exact-ref>
hadron workflow registry unpin-version <exact-ref>
hadron workflow registry publish <exact-ref>
hadron workflow registry clear-current <exact-ref>

hadron workflow exposure inspect <profile-id>
hadron workflow exposure pin-definition <profile-id> <exact-ref> --expected-generation <n>
hadron workflow exposure unpin-definition <profile-id> <exact-ref> --expected-generation <n>
```

These expose three independent states honestly:

- `current` is a movable catalog alias changed by qualified register or exact
  clear-current.
- registry-version pin is exact qualification/publication state owned by the
  registration service.
- exposure-profile pin is exact tool visibility, mutated by profile-generation
  CAS after policy, effect, collision, and direct-tool-budget preflight.

Failed validation/tests, unauthorized namespaces, stale generations/digests,
and failed preflight do not partially mutate state.

## Output and errors

Use `--json` for stable typed DTOs. Nonzero operations may still write a safe
structured result before the concise stderr error—for example unsupported
dry-run or rejected pins. Transport errors and internal causes are redacted;
diagnostics contain bounded codes, paths, and safe messages.

Run `hadron <command> --help` for the executable's exact flags.
