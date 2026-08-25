# Workflow use cases

These use cases describe the active graph-native host. Runnable stock-daemon
workflows must use its six registered kinds; broader adapters require an
explicit embedding and host capability profile.

## Typed local transformation

Use `transform@v1` for deterministic expression projection and `script@v1` for
bounded JavaScript with explicit input/output schemas. These are useful for
normalization, typed decision records, and preparing data between durable
waits. Start with the runnable files under
[`examples/workflow/production`](../examples/workflow/production/).

## Durable time and callback coordination

Use `sleep@v1` when progress should survive daemon restart without holding a
worker. Use `wait_for@v1` for a typed external continuation. Both persist the
wait before suspension, release executor capacity, and resume through the same
coordinator and replay rules.

Schedules and external inputs belong in source `on:` declarations. Publishing
a current workflow materializes registrations bound to its exact registry
definition. External callers fire only payload-ingress registrations returned
by authorized lifecycle inspection; timer and schedule IDs are not generic
HTTP triggers.

## Human approval and correlated messages

`human_gate@v1` exposes a typed decision schema and responder authority.
`message_wait@v1` waits for a correlated typed message. MCP gate/message aliases
and the CLI `workflow resume` command use the same durable wait contract; they
do not create private transport state.

The compiler conformance fixture
[`release-approval-gate.workflow.yaml`](../examples/workflow/release-approval-gate.workflow.yaml)
shows a human gate feeding a typed transform.

## Agent workflow discovery and invocation

An MCP agent can search its exposure scope, inspect an exact schema/effect
contract, lazy-load the selected definition, start its generated asynchronous
tool, and inspect redacted typed outputs. Exact profile pins become direct
tools; discoverable namespaces do not eagerly consume the direct-tool budget.

The authoring flywheel is available through the shared lifecycle service:
catalog search, bounded graph-native draft, validation, scaffold, deterministic
contract tests, authorized registration, package/qualification/publication,
and exact exposure pinning. Failed tests or policy/CAS/budget failures mutate
nothing.

## A2A task projection

Published exact workflow records become bounded A2A agent-card skills derived
from canonical schemas, effects, digest, and provenance. Task submit maps to a
durable workflow run; task status, waits, cancellation, resume, redacted events
and values, and allowed typed outputs remain projections of shared workflow
operations. Task IDs are not capabilities and correlation survives restart.

## What is not an active use case

Legacy blueprint files, pipeline DAGs, global shell-output scraping, and the
retired root commands are archive/rewrite material only. The Torque fake-MCP
workflow and HTTP/cmd workflow under `examples/workflow` are integration and
compiler fixtures, not stock-daemon recipes.
