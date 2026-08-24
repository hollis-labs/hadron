# Conformance Fixture Skeleton

Each stable fixture set contains `pass.json` and `fail.json`. The current
`accepted` input flag exists only to prove harness discovery and expected
pass/fail handling; it is not a workflow source, graph, value, state-store,
scheduler, wait, or step-kind contract.

Later implementation tasks replace or extend each payload with semantic cases
while retaining the directory names, fixture envelope, and deterministic suite
failure format.
