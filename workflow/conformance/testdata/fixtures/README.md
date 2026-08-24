# Conformance Fixture Skeleton

Each stable fixture set starts with `pass.json` and `fail.json`. Most sets
retain the initial `accepted` harness flag until their semantic contracts land.
The `graph-validation` set now also contains real value-dependency inference
cases for inferred edges, hidden references, optional branches, fan-out item
scope, and expression-introduced cycles. Its payloads carry Graph values,
registered-kind inputs, and deterministic inference expectations. The
`executor-metadata` set contains real `stepkind.StepKindSpec` pass/fail cases.

Later implementation tasks replace or extend each payload with semantic cases
while retaining the directory names, fixture envelope, and deterministic suite
failure format.
