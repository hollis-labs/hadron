# Graph-Native Workflow Source

The compiler accepts one `workflow` document from the source loader. The root
contains `workflow`, `inputs`, `outputs`, `steps`, and optional `finally`.
Structural fields are closed: unsupported fields produce `HADR-SOURCE-012`
instead of being discarded.

`workflow.id` or `workflow.name` supplies identity. Authoring names normalize
through `graph.NormalizeID`; a step's original `name` remains its display name.
The default workflow version is `1.0.0`. Workflow provenance may declare
authority, origin, revision, parents, and metadata; the compiler owns the
current source locator and digest.

Inputs use a sequence. A schema may be an explicit JSON Schema object or the
`type`/`items_type` shorthand. Workflow and node outputs may use named mappings
or named sequences. Workflow outputs require a value; node outputs may be
executor-produced. Missing output schemas become open `{}` schemas.

Steps accept either:

- explicit `kind` plus optional opaque `config`; or
- one recognized executor field such as `cmd`, `mcp_call`, `http_call`,
  `transform`, `sleep`, `wait_for`, `human_gate`, or `message_wait`.

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
