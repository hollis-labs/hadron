# ADR 0006: Reusable Workflow Engine Boundary

**Status:** Accepted<br>
**Date:** 2026-08-24

## Context

Hadron's current workflow behavior lives under app-internal packages such as
`internal/blueprint`, `internal/execution`, and `internal/pipeline`. That shape
works for the Hadron daemon, but it prevents Nanite, Torque, Cerberus, and other
Go applications from embedding the workflow engine without depending on Hadron
app/service internals.

The desired future state distinguishes Hadron app/service from the reusable
workflow engine core. Hadron remains the reference host and operator surface,
but the workflow semantics must be consumable as a library.

## Decision

Design the workflow engine as an extraction-ready reusable package boundary.
The first implementation may live inside Hadron while semantics settle, but the
core must be structured so it can move to a shared module such as
`github.com/hollis-labs/go-workflow`.

Engine core may depend on the Go standard library and selected schema,
expression, and test dependencies. It must not import:

- Hadron `internal/*`;
- Hadron daemon, HTTP, MCP, A2A, CLI, Wails, registry, or settings packages;
- concrete SQLite storage;
- Nanite, Torque, Tether, or Cerberus app packages;
- concrete provider, MCP, LLM, or agent SDKs.

Adapters and host bindings own those concrete dependencies.

## Consequences

Hadron app/service becomes one host of the engine, not the definition of engine
semantics. Nanite and Torque can embed the core directly while keeping their
product-owned policy, state, and presentation.

The codebase needs clear import boundaries, conformance tests, and adapter
interfaces. Implementation work should treat any core dependency on Hadron app
packages as an architecture violation.
