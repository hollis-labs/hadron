# Conformance Fixture Skeleton

Each stable fixture set contains `pass.json` and `fail.json`. Most sets retain
the initial `accepted` harness flag until their semantic contracts land. The
`executor-metadata` set contains real `stepkind.StepKindSpec` pass/fail cases.

Later implementation tasks replace or extend each payload with semantic cases
while retaining the directory names, fixture envelope, and deterministic suite
failure format.
