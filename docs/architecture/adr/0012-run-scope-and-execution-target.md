# ADR 0012: RunScope and ExecutionTarget Are Separate Concepts

**Status:** Accepted<br>
**Date:** 2026-08-24

## Context

Current Hadron records use `workspace_id` as a logical grouping for runs and
related state. The term collides with compute workspaces such as
Cerberus/Coder-provided environments.

The target architecture needs a clean distinction between an operational scope
for Hadron records and the compute target where execution occurs.

## Decision

Use `RunScope` as the target architecture term for Hadron's logical
operational namespace. This is a clean break; no legacy `workspace_id` support
is required in the target design.

Use `ExecutionTarget` for compute binding. The local machine, a
Cerberus/Coder workspace, and future remote runners are execution targets.

`RunScope` never implies filesystem, compute environment, lease, readiness, or
isolation. `ExecutionTarget` owns those concepts.

## Consequences

APIs, plans, bound runs, and target architecture docs should use `RunScope` for
logical operational grouping and `ExecutionTarget` for compute/workspace
binding.

This avoids carrying ambiguous `workspace_id` terminology into the new
architecture.
