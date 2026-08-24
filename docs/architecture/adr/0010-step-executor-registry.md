# ADR 0010: Step Executor Registry

**Status:** Accepted<br>
**Date:** 2026-08-24

## Context

Current execution dispatch is hard-coded by checking which step kind field is
present. Hadron already has useful narrow interfaces for MCP calls, message
polling, and agent launching, while Nanite has a separate `StepExecutor` shape
for LLM/tool workflow steps.

The target engine needs extensible step kinds without importing concrete app or
provider dependencies into core.

## Decision

Use a step-kind registry. The required executor lifecycle is minimal:
`Spec`, `ValidateConfig`, and `Execute`. Optional interfaces cover `Prepare`,
`Observe`, `Cancel`, and `Finalize`, and optional support is advertised in
step-kind metadata.

Each step kind declares schema, effects, idempotency, retry safety,
cancellation behavior, observation behavior, output schema, required
capabilities, and whether it can suspend or run embedded.

`llm` is one provider-agnostic contract with multiple possible executor
bindings, including direct `go-providers`, `agentkit`, and Nanite harness
adapters. Engine core imports none of those concrete dependencies.

`script` starts with goja JavaScript for local deterministic data manipulation.
Python is a later explicit subprocess/sandbox decision.

## Consequences

Step semantics become inspectable and enforceable before execution. Effects and
capabilities can drive retries, recovery, MCP annotations, confirmation policy,
and blast-radius explanations.

Hadron, Nanite, Torque, and future hosts can register product-owned executors
without forking engine semantics.
