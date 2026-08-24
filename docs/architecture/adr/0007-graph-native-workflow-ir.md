# ADR 0007: Graph-Native Workflow Source and IR

**Status:** Accepted<br>
**Date:** 2026-08-24

## Context

Hadron currently has two source models: blueprints as sequential task sections
and pipelines as DAGs of blueprint runs. There are no external consumers of
these formats today; the existing files are primarily generated examples and
reference material.

Keeping both source models as compatibility constraints would preserve the
current split between sequential blueprint semantics and pipeline DAG
semantics. The target architecture needs one semantic model that all front
doors and embedding hosts drive.

## Decision

Hadron will define one graph-native workflow source format over one graph IR.
Blueprint and pipeline are not long-term public source kinds.

The graph IR is the semantic contract for validation, execution, UI rendering,
transport exposure, and tests. Go structs are the source of truth for the IR,
with generated JSON Schema for validation, UI, agents, and serialized plans.

Source maps are persisted on `ExecutionPlan`. Node invocations store compact
references into the plan-level source map.

Expressions use `expr-lang/expr`. `{{ }}` remains string interpolation only.

## Consequences

Existing blueprint and pipeline examples should be archived and selectively
rewritten into the new workflow source format. A legacy parser or rewrite aid
may be built if useful, but it is not a public compatibility commitment.

Implementation planning can start from a clean workflow language rather than
preserving the current blueprint/pipeline distinction.
