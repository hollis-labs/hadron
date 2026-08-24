# 10 - Migration, Compatibility, And Safety

## Architectural Change

Treat the workflow language as greenfield while preserving useful existing
blueprint and pipeline examples as archived reference material.

There are no current external consumers to preserve. Legacy behavior should not
become the engine contract.

## Archive And Rewrite Model

Existing blueprint and pipeline examples should be archived or moved under a
reference/examples area before target-language work begins. Any example still
worth keeping should be rewritten intentionally into the graph-native workflow
source format.

A legacy loader may still be useful as an internal migration aid, but it is not
a required public compatibility layer.

If a loader is built, it should be explicit:

```go
type SourceLoader interface {
	CanLoad(source Source) bool
	Load(ctx context.Context, source Source, opts LoadOptions) (graph.Graph, Diagnostics, error)
}

type LoadOptions struct {
	Profile              string // archive-reference | strict-rewrite-aid
	Strict               bool
	SourceMap            bool
}
```

Potential profiles:

```text
archive-reference  parse old examples for diagnostics/reference only
strict-rewrite-aid fail on ambiguous or unsafe behavior
target             new graph semantics only
```

A planner can later decide whether a loader is worth building at all. The
architecture only requires that target semantics stay clean.

## Blueprint Compatibility

Current blueprint behavior that may be useful as rewrite input:

- sections and `tasks` arrays;
- `run` aliasing to `cmd`;
- exactly one executable kind per task;
- imports with `with` defaults;
- `retry`, `retry_delay_seconds`, `retry_backoff`, `timeout_seconds`;
- `continue_on_error`;
- `enabled`;
- `on_success` and `on_fail` as migration inputs.

Target rewrite:

- section order becomes explicit `needs`;
- `run` becomes `cmd`;
- `call` becomes a `call` node with `mode: inline`;
- static template fields become expression or interpolation bindings;
- scaffolding-specific fields become archived-source metadata unless promoted
  by decision.

## Pipeline Compatibility

Current pipeline behavior that may be useful as rewrite input:

- `stages`;
- `depends_on`;
- `if`;
- stage `inputs`;
- stage `outputs`;
- `position`;
- `async`;
- `stop_on_fail`;
- default stage wait timeout, with diagnostics if unsafe.

Target rewrite:

- stage becomes `kind: call`;
- `blueprint_path` becomes `DefinitionRef.locator`;
- `depends_on` becomes `needs`;
- stage `outputs` become child-output bindings or explicit rewrite diagnostics;
- `position` becomes node metadata;
- `async` becomes call parent-close/wait policy.

## `::set-output` Risk

Current pipeline output capture scans run event log messages:

- [`internal/pipeline/runner.go`](../../../internal/pipeline/runner.go)
  `captureStageOutputs` lists run events and scans messages for
  `::set-output`.
- `message_wait`, `human_gate`, `agent_launch`, `mcp_call`, and `http_call`
  emit compatibility output markers.

Target behavior:

- native executors return typed outputs;
- logs are not scanned globally;
- `::set-output` parsing is opt-in per node and stream;
- parsed values are marked as compatibility-origin values;
- workflows using the shim receive diagnostics;
- new source profiles disallow it.

This is both a correctness change and a safety change. Data/control-flow
outputs must not be set by arbitrary text in logs.

## Scaffolding Fields

Current `Blueprint` includes project scaffolding and framework fields:

- `project.php_version`;
- `project.node`;
- `packages.composer`;
- `packages.npm`;
- `packages.pip`;
- `git`;
- `stubs`;
- `tools.install`.

These fields were useful for early workflows but are not universal workflow
semantics. Target options:

- keep only in legacy loader metadata;
- express through ordinary `cmd` nodes;
- move to registered step kinds where a durable semantic contract is valuable;
- move to child workflows owned by the relevant publisher.

## Secrets And Redaction

Hadron is not a credential authority. Target contract:

```text
reference -> resolve -> inject -> redact -> forget
```

Architecture requirements:

- workflow definitions and settings carry secret references, not secret values;
- secret resolution happens at the narrowest adapter boundary;
- logs, events, outputs, messages, prompts, HTTP bodies, MCP results, and gate
  payloads have data classification and retention rules;
- known secret values are masked in streams where they may appear;
- app records store secret reference provenance, not raw material;
- webhook verification keys need a real custody decision, not misleading
  `secret_hash` naming.

## Safety Invariants

Execution is Hadron's primary trust boundary.

Target invariants:

- every run has caller, source authority, provenance, trust class, grants,
  execution target, and effective policy;
- every node declares capabilities, effects, retry safety, cancellation
  behavior, and output shape;
- policy decisions are recorded as operational facts;
- denied capabilities fail closed with explainable diagnostics;
- command execution validates resolved paths and capabilities structurally;
- sandbox/confirmation/dry-run claims correspond to real enforcement;
- destructive operations require explicit policy and user/agent authority;
- run events are append-only operational facts, not unclassified business data.

## Migration Diagnostics

Any archive/rewrite tooling should emit diagnostics such as:

```text
warning HADR-LEGACY-001: blueprint field project.php_version is legacy metadata
warning HADR-OUTPUT-002: stage build captures ::set-output from log stream
error HADR-REF-001: node deploy references steps.build.outputs.version without dependency
error HADR-EFFECT-001: destructive node delete has retry policy without idempotency proof
```

Diagnostics should include source-map pointers and suggested target syntax.

## Planner Boundary

This document does not sequence migration. It defines the greenfield target and
the reference value of old examples so the planner can later choose what to
archive, rewrite, or delete.

## Decisions

- Target mode is greenfield.
- Current blueprint and pipeline examples should be archived and selectively
  rewritten.
- A legacy parser/rewrite aid is optional and is not a public compatibility
  commitment.
- Archive current examples under
  `examples/archive/legacy-blueprints-pipelines/`.
- Do not build a legacy parser unless later implementation evidence proves it
  useful.
- Secrets are opaque references resolved at adapter boundaries, for example
  `secret://authority/path#field`.
- Hadron records secret references and provenance, not secret material.
- Values and events carry redaction and retention metadata.

## Decision Needed

- Which legacy fields remain useful as reference concepts.
- Whether `on_success: step` is removed, implemented, or lowered to graph
  control flow.
- Exact `::set-output` shim rules.
- Credential authority boundary details.
- Redaction and retention classes.
- Safety policy vocabulary for capabilities, effects, confirmations, and
  dry-run.
