# Conformance Fixture Skeleton

Each stable fixture set starts with `pass.json` and `fail.json`. Most sets
retain the initial `accepted` harness flag until their semantic contracts land.
The `graph-validation` set now also contains real value-dependency inference
cases for inferred edges, hidden references, optional branches, fan-out item
scope, and expression-introduced cycles. Its payloads carry Graph values,
registered-kind inputs, and deterministic inference expectations. The
`scheduler` set contains semantic fixtures for all six readiness rules plus an
unsupported-rule rejection. The `waits` set contains semantic records for all
six generic wake sources plus an unsupported-source rejection. The
`executor-metadata` set covers valid and
missing metadata, duplicate registration, unknown exact resolution, adapter
config rejection, optional lifecycle/interface agreement, and immutable
registered-spec snapshots.

Later implementation tasks replace or extend each payload with semantic cases
while retaining the directory names, fixture envelope, and deterministic suite
failure format.
