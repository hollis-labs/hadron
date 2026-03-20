---
type: spec
id: SPEC-HAD-002
title: "Hadron Pipeline Spec v2"
status: draft
date: 2026-03-20
---

# Hadron Pipeline Spec v2

Extends the v1 pipeline format with DAG execution, stage outputs, conditional edges,
and visual position metadata. v1 linear pipelines remain fully supported.

## New Fields

### `depends_on`

Declares explicit upstream dependencies for a stage. Replaces implicit array ordering.

```yaml
stages:
  - name: build
    blueprint_path: build.yaml

  - name: lint
    blueprint_path: lint.yaml
    depends_on:
      - build

  - name: deploy
    blueprint_path: deploy.yaml
    depends_on:
      - lint
      - test-unit     # fan-in: both must complete before deploy runs
```

Type: `[]string` — each entry is the `name` of another stage in the same pipeline.
Default: empty (no dependencies — stage is a root node).

### `position`

Layout metadata for visual canvas rendering. Has no effect on execution.

```yaml
stages:
  - name: build
    blueprint_path: build.yaml
    position:
      x: 300
      y: 50
```

Type: `{x: int, y: int}` — pixel coordinates on a canvas.
Default: omitted (UI may auto-layout).

### `outputs`

Named key-value pairs emitted by a stage, available to downstream stages via template context.

```yaml
stages:
  - name: build
    blueprint_path: build.yaml
    outputs:
      version: "1.4.0-{{ .run.id }}"
      artifact_path: "/dist/app.tar.gz"
```

Type: `map[string]string` — keys must match `^[a-zA-Z][a-zA-Z0-9_]*$`.
Values are template strings evaluated after the stage completes.

### `inputs`

Key-value pairs passed into a stage's blueprint as template context. Values may reference
upstream outputs using template syntax.

```yaml
stages:
  - name: deploy
    blueprint_path: deploy.yaml
    depends_on:
      - build
    inputs:
      build_version: "{{ .stages.build.outputs.version }}"
```

Type: `map[string]string` — keys are available in the blueprint as `{{ .inputs.<key> }}`.

### `if` (stage-level)

Conditional execution of a stage. The stage is skipped (status: `skipped`) when the
condition evaluates to false.

```yaml
stages:
  - name: notify
    blueprint_path: notify.yaml
    depends_on:
      - deploy
    if: '{{ eq .stages.deploy.status "failed" }}'
```

Type: `string` — Go template expression. Evaluated after all `depends_on` stages complete.
Truthy: renders to `"true"` or a non-empty string. Falsy: `"false"` or empty string.

## DAG Execution Model

### Topological Sort

Stages are sorted into parallel levels using Kahn's algorithm:

1. Build an adjacency graph from `depends_on` edges.
2. Identify root nodes (stages with no dependencies).
3. Assign each stage to the earliest level where all dependencies are satisfied.
4. Execute all stages at the same level in parallel.

For the example pipeline:

```
Level 0: build
Level 1: lint, test-unit, test-integration   (fan-out from build)
Level 2: deploy                               (fan-in from lint + test-unit)
Level 3: notify                               (conditional on deploy failure)
```

### Parallel Levels

All stages within a level execute concurrently. The scheduler waits for every stage
in a level to complete (or be skipped) before advancing to the next level.

### Conditional Routing

When a stage has an `if` field:

1. All `depends_on` stages must have completed (any terminal status: `passed`, `failed`, `skipped`).
2. The `if` template is evaluated against the current pipeline context.
3. If false, the stage is marked `skipped` and downstream stages see `status: "skipped"`.
4. If true, the stage executes normally.

### `stop_on_fail` Behavior

When `stop_on_fail: true`:

- A stage failure prevents all downstream stages from running (they are marked `skipped`).
- Stages at the same parallel level that are already running continue to completion.
- Stages with an `if` condition that explicitly checks for failure are still evaluated
  (they opted in to handling failures).

## Template Context Extensions

v2 adds stage-scoped context accessible from `if` fields, `inputs`, and `outputs`:

```
{{ .stages.<name>.status }}           # "passed" | "failed" | "skipped" | "running" | "pending"
{{ .stages.<name>.outputs.<key> }}    # output value from a completed stage
{{ .run.id }}                         # current pipeline run identifier
```

Stage status values:

| Status | Meaning |
|---|---|
| `pending` | Not yet scheduled |
| `running` | Currently executing |
| `passed` | Completed successfully |
| `failed` | Completed with error |
| `skipped` | Skipped due to condition or upstream failure |

## Backward Compatibility

v1 linear pipelines (no `depends_on` fields) work unchanged:

- When no stage declares `depends_on`, the scheduler falls back to **array order**:
  each stage implicitly depends on the previous stage.
- `meta.version` is optional. If omitted or set to `1`, array-order semantics apply.
  Set to `2` to enable DAG semantics even when some stages omit `depends_on`.
- Fields new to v2 (`position`, `outputs`, `inputs`, `if`) are ignored by v1 parsers.

```yaml
# This v1 pipeline still works — stages run in order: setup → test
meta:
  name: simple-pipeline

stages:
  - name: setup
    blueprint_path: setup.yaml

  - name: test
    blueprint_path: test.yaml
```

## Validation Rules

### Cycle Detection

The DAG must be acyclic. Before execution, the scheduler runs cycle detection
(DFS-based or via Kahn's algorithm — if topological sort does not consume all nodes,
a cycle exists). Cycles produce a validation error listing the involved stages.

### Unknown References

Every entry in `depends_on` must reference a stage `name` that exists in the same
pipeline. Unknown references are a validation error.

### Self-References

A stage cannot depend on itself. `depends_on: [self-name]` is a validation error.

### Output Key Format

Output keys must match `^[a-zA-Z][a-zA-Z0-9_]*$`. Invalid keys are a validation error.

### Template Reference Validation

Template expressions in `inputs`, `outputs`, and `if` that reference
`{{ .stages.<name>.outputs.<key> }}` are validated against declared outputs.
References to stages that do not declare the referenced output key produce a warning
(not a hard error, since outputs may be set dynamically at runtime).

## Reference Example

See `../../examples/pipeline-v2-dag/pipeline.yaml` for a complete working example
demonstrating fan-out, fan-in, conditional edges, outputs, and position metadata.
