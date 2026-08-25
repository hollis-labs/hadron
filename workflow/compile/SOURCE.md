# Graph-Native Workflow Source

The compiler accepts one `workflow` document from the source loader. The root
contains `workflow`, optional `on`, `inputs`, `outputs`, `steps`, and optional `finally`.
Structural fields are closed: unsupported fields produce `HADR-SOURCE-012`
instead of being discarded.

`workflow.id` or `workflow.name` supplies identity. Authoring names normalize
through `graph.NormalizeID`; a step's original `name` remains its display name.
The default workflow version is `1.0.0`. Workflow provenance may declare
authority, origin, revision, parents, and metadata; the compiler owns the
current source locator and digest.

## Source activation declarations

Optional `on` declarations compile into immutable `Graph.Activations`; they do
not create scheduler or trigger registrations. The accepted source keys and
their adapter-opaque canonical config are:

```yaml
on:
  webhook: {path: /hooks/tasks}
  schedule: {cron: "0 6 * * *"}
  message: {to: msg://agent/hadron/tasks}
  file: {path: inbox/tasks.json, events: [create, write, remove, rename]}
  event: {type: project.task.created, source: project://torque}
  one_shot: {path: /callbacks/setup, ttl: 15m}
```

`schedule` also accepts a five-field cron string shorthand. A kind accepts a
sequence of declarations when each item has a unique `name`; one mapping uses
the kind as its default name. Names normalize into stable activation IDs.
Webhook and one-shot paths are static root-relative routes. Cron validation is
syntax-only: runtime scheduling semantics remain a host responsibility.

All declarations are source-owned. `authority` may be omitted or set exactly
to `project`; workflow source cannot claim host or operator authority. The
compiler records immutable project/source provenance on every declaration.
`extract` is a binding mapping. Portable policy fields are `overlap` using
`Allow | Forbid | Replace`, positive `starting_deadline`, `catchup`,
`deduplication_key`, and `run_id_reuse` using
`reject | allow_duplicate | terminate_existing`.

`one_shot.ttl` is positive relative intent for later host materialization. It
is not an absolute expiry. Registration IDs, enabled state, expiry timestamps,
fire history, callback credentials, host ownership, and all other mutable
operational fields are rejected rather than serialized into the plan.

Inputs use a sequence. A schema may be an explicit JSON Schema object or the
`type`/`items_type` shorthand. Workflow and node outputs may use named mappings
or named sequences. Workflow outputs require a value; node outputs may be
executor-produced. Missing output schemas become open `{}` schemas.

Steps accept either:

- explicit `kind` plus optional opaque `config`; or
- one recognized executor field such as `cmd`, `mcp_call`, `http_call`,
  `transform`, `sleep`, `wait_for`, `human_gate`, or `message_wait`.

`agent_launch` is a recognized graph-native shorthand but requires the
explicit extraction-safe expander supplied by `workflow/adapters/agent` (for
example through `agent.Compile`). Core never imports adapters or performs an
implicit global registration. Pure expanders run before graph/plan digests are
computed. Generated child workflows are committed to the serialized plan's
`bundled_definitions`; `compile.NewBundledDefinitionResolver` resolves only an
exact immutable authority/kind/id/locator/version/digest tuple after restart.

`mcp_call` and `http_call` normalize to the registered kinds `mcp` and `http`.
Mapping-shaped executor values become opaque config unchanged. Scalar `cmd`
and `sleep` values become `command` and `duration` config fields. Multiple
executor fields, or executor shorthand mixed with explicit `kind`/`config`,
are errors. Registered-kind existence and config-schema validation belong to a
later validation pass.

Bindings use exactly one explicit mode:

```yaml
literal: <json-value>
expression: <raw-expression>
interpolation: <string-with-{{ markers }}>
```

In binding positions, a string containing interpolation markers is shorthand
for `interpolation`; another string is shorthand for `expression`; non-string
YAML is shorthand for `literal`. The compiler preserves expression text and
does not evaluate or resolve it.

`needs` is the only dependency source lowered in this task. Each need produces
both a `graph.Need` and a normalized explicit edge; expressions never infer
hidden edges. Scalar `for_each` is an items expression, and integer
`concurrency` sets its bounded fan-out. `retry.idempotency_key` lowers to keyed
idempotency. The full IR-shaped forms remain available for backoff,
idempotency, timeouts, catch, finally, switch, and call.

Every graph declaration and nested expression/binding carries its exact
`graph.SourceRef`. The plan and graph also retain full maps keyed by normalized
input, output, and node identities. Edge map keys use
`compile.EdgeSourceKey(from, to, kind)`.

Compilation does not evaluate expressions, resolve child definitions, infer
dependencies, query registries, validate kinds, or run topology/policy checks.

## Examples and conformance fixtures

The stock daemon's runnable examples live in
[`examples/workflow/production`](../../examples/workflow/production/). They use
only the production host's frozen capability set.

The three source files in the parent `examples/workflow` directory are compiler
and adapter conformance fixtures. Acceptance tests compile them with test kind
registries; `http_call`, `cmd`, and `mcp_call` in those fixtures are not claims
that the stock daemon enables those kinds. See the examples README for the
exact classification.
