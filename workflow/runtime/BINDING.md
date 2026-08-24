# Workflow Run Binding

Run binding follows the immutable boundary:

```text
compile.ExecutionPlan -> runtime.BoundRun -> runtime.RunSnapshot
```

`BindRun` validates the entire caller input object before persistence. On a
diagnostic-free result it performs exactly one `StateStore.SaveValues` call and
returns a `BoundRun` carrying the persisted input reference, plan ID/version,
plan digest, plan schema version, creation time, and copied plan provenance.
The saved value set is release material, not evidence that a run started.
`StartBoundRun` is the only helper in this package that creates the pending run
and delegates exact idempotency replay/conflict decisions to `StateStore`.

`SaveValues` is not idempotent. Repeating `BindRun` may leave another immutable,
unreferenced input set. A start retry must reuse the same `BoundRun` and
`InputsRef`; it must not bind again.

Binding routes writes through `SaveValuesWithRetention`. A nil retention hook
still enforces persistability. A configured hook receives stable run/project/
external groups before and after the immutable write; `none` is rejected before
storage. A post-write hook failure exposes the unreferenced `ValueSetRef`
through `RetentionHookError`.

## Input rules

- Caller keys must exactly match normalized declared input names. Unknown keys
  fail rather than being discarded.
- A supplied caller value wins over a default. A missing declaration uses only
  a literal default. Expression and interpolation defaults fail at binding
  because no runtime expression scope exists yet.
- A required input with neither caller value nor literal default fails. An
  absent optional input is omitted rather than converted to null.
- Every declared schema is compiled even when its optional value is absent, so
  invalid contracts cannot enter a `BoundRun` merely because one caller omitted
  the field.
- Coercion means lossless native-JSON normalization only. Go integer families,
  finite floats, `json.Number`, arrays, and string-keyed maps normalize into the
  values envelope without losing numeric text. Strings are never parsed as
  numbers, booleans, null, arrays, or objects. Binary data must use an artifact
  boundary. Integer-valued floats beyond their exact consecutive-integer range
  are rejected; JSON callers should decode with `json.Decoder.UseNumber` when
  numeric precision matters.
- Callers may supply an already-valid `values.Value`, `values.ArtifactRef`, or
  typed secret reference Value for artifact-bearing and pre-classified inputs.
  The complete envelope is copied and preserved, then schema-validated.
  Secret-classified inline material is rejected; exact secret envelope
  passthrough is preserved and computed use fails closed. Descriptive
  `graph.Metadata` never changes the envelope.
- Each normalized value is checked against its complete inline JSON Schema.
  Local JSON Pointer and `$defs` references work; network, file, and all other
  external resource loading is denied.

New caller and default values use `application/json`, `private`, and `run` as
safe classification defaults. Their producer kinds are `workflow_input` and
`workflow_default`. `graph.Metadata` remains descriptive and is not interpreted
as classification policy; W02-T06 owns declaration and policy semantics.

## Output rules

`FinalizeRunOutputs` requires the exact plan, bound run, and persisted run
identity, plus a canonical terminal observation for every declared graph node.
The runtime progression owner decides whether terminal failures were handled;
the binder does not invent catch or success policy.

Workflow output expressions can see only nodes declared by the plan. The
binder evaluates and schema-validates every declared output before persistence.
Exact passthrough expressions such as `inputs.payload` and
`steps.render.outputs.report` preserve the complete existing `Value` envelope,
including inline/artifact mode, producer, digest, media type, redaction, and
retention. Computed, literal, and interpolated values use the
`workflow_output` producer with `application/json`, `private`, and `run`
defaults. Secret-classified Values and ArtifactRefs can only use exact
passthrough; they cannot be unwrapped or derived by expressions.

Only a complete output set is saved, and only its reference is published by the
atomic transition to `succeeded`. Evaluation, visibility, type, or schema
diagnostics save nothing. A storage failure publishes nothing. A later
transition failure can leave the complete saved set unreferenced, but cannot
publish partial outputs. Finalizing an already succeeded run loads and compares
the existing complete value set before any write: identical content is a
no-mutation replay and different content is `ErrOutputConflict`.

Binding diagnostics reserve `HADR-VALUE-010` through `HADR-VALUE-018`.
