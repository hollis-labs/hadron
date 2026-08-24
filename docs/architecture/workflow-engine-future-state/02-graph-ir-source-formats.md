# 02 - Graph IR And Source Formats

## Architectural Change

Hadron defines one graph-native workflow source format over a typed graph IR.
A workflow node, child call, UI canvas node, future SDK call, and agent-emitted
workflow all compile to the same semantic graph.

Current blueprint and pipeline files are historical/reference material. There
are no external consumers to preserve, so the target architecture does not need
to retain two public source kinds for compatibility.

Current behavior splits the semantics:

- blueprints are sequential sections of executable steps;
- pipelines are DAGs of blueprint runs with string-only output passing.

Target behavior makes run identity, isolation, and observability properties of
graph nodes instead of separate engines.

## Current Shape

Blueprint source:

- [`internal/blueprint/blueprint.go`](../../../internal/blueprint/blueprint.go)
  defines `Blueprint`, `Section`, and `Step`.
- A `Step` can have exactly one executable kind: `cmd`, `run`, `call`,
  `http_call`, `mcp_call`, `message_wait`, `agent_launch`, or `human_gate`.
- `RenderForExecution` renders string templates before any step executes, so
  step outputs cannot feed later blueprint steps.
- `executeFile` in
  [`internal/execution/manager.go`](../../../internal/execution/manager.go)
  loops over sections and steps sequentially.

Pipeline source:

- [`internal/pipeline/pipeline.go`](../../../internal/pipeline/pipeline.go)
  defines `Spec` and `Stage`.
- [`internal/pipeline/toposort.go`](../../../internal/pipeline/toposort.go)
  groups stages into execution levels.
- [`internal/pipeline/runner.go`](../../../internal/pipeline/runner.go)
  runs each level with goroutines, then waits for the whole level before
  moving on.

The stage-vs-call distinction is mostly observability: a pipeline stage gets
its own run record; a blueprint `call` recurses into the parent run and returns
no value.

## Target IR

The graph IR is the contract the compiler, runtime, UI, transports, and tests
share.

```go
package graph

type Graph struct {
	ID          string
	Version     string
	Digest      string
	Provenance  Provenance
	Inputs      []InputSpec
	Outputs     []OutputSpec
	Nodes       []Node
	Edges       []Edge
	Policy      GraphPolicy
	SourceMap   SourceMap
	Annotations map[string]any
}

type Node struct {
	ID             string
	DisplayName    string
	Kind           string
	KindVersion    string
	Needs          []Need
	ReadyWhen      ReadyRule
	If             *Expression
	ForEach        *ForEachSpec
	Config         map[string]any
	InputBindings  map[string]Binding
	Outputs        []OutputSpec
	Effects        EffectSet
	Retry          *RetryPolicy
	Timeout        *TimeoutPolicy
	Catch          []CatchRule
	Finally        bool
	ConcurrencyKey string
	Call           *CallSpec
	Source         SourceRef
	Metadata       map[string]any
}

type CallSpec struct {
	Definition DefinitionRef
	Mode       CallMode // inline | run
	Target     string
	OnParentCancel ParentClosePolicy // cancel | abandon | request_cancel
}
```

The exact fields are placeholders. The required concepts are not:

- stable workflow and node identity;
- input and output schemas;
- explicit data and control dependencies;
- node kind and node-local schema;
- source map back to blueprint section/step or pipeline stage;
- effect, retry, timeout, cancellation, and idempotency metadata;
- typed data bindings rather than log scraping;
- call mode for inline versus child-run behavior.

## Historical Source Mapping

The mappings below are migration/reference notes, not compatibility
requirements. Existing examples can be archived and selectively rewritten into
the target source format when they remain useful.

### Blueprint sections

An existing blueprint section can be rewritten by making implicit order
explicit:

```yaml
steps:
  - section: main
    tasks:
      - name: fetch
        http_call: { method: GET, url: "https://example.test/data" }
      - name: summarize
        cmd: "jq '.items | length' data.json"
```

Target shape:

```text
node fetch
node summarize needs [fetch]
```

The target source should prefer explicit graph dependencies. Sequential groups
can exist as authoring sugar later, but they are not the semantic baseline.

### Pipeline stages

An existing pipeline stage can be rewritten as a `call` node:

```yaml
- name: deploy
  blueprint_path: deploy.yaml
  depends_on: [lint, test-unit]
  inputs:
    build_version: "{{ .stages.build.outputs.version }}"
  position:
    x: 200
    y: 400
```

Target graph node:

```yaml
- id: deploy
  kind: call
  needs: [lint, test-unit]
  call:
    definition: { locator: deploy.yaml }
    mode: run
  with:
    build_version: steps.build.outputs.version
  metadata:
    position: { x: 200, y: 400 }
```

The runtime only sees a `call` node whose `mode` is `run`, meaning it gets
child run identity, events, status, and cancellation handles.

### Blueprint calls

An existing blueprint `call` maps to the same node kind:

```yaml
- name: prepare
  call: prepare.yaml
  with:
    project_id: "{{ inputs.project_id }}"
```

Target:

```yaml
- id: prepare
  kind: call
  call:
    definition: { locator: prepare.yaml }
    mode: inline
  with:
    project_id: inputs.project_id
```

The child returns declared outputs in either mode. The difference is only run
identity and host-level lifecycle behavior.

## Source Map

Every compiled node needs a source map entry for diagnostics, migration, UI,
and agent explainability.

```go
type SourceRef struct {
	Format      string // workflow | archived-blueprint | archived-pipeline | sdk | ui
	Locator     string
	StartLine   int
	EndLine     int
	Section     string
	StepName    string
	StageName   string
	SourcePath  []string // e.g. ["steps", "0", "tasks", "1"]
}
```

Source maps allow a runtime error to say:

```text
node deploy input build_version references missing output build.version
source: examples/archive/legacy-blueprints-pipelines/pipeline-v2-dag/pipeline.yaml:62
```

instead of exposing only a compiled node ID.

## Target Authoring Example

The Torque bulk-create acceptance case becomes a graph-native workflow:

```yaml
workflow:
  name: torque-task-bulk-create
  namespace: torque

inputs:
  - name: project_id
    type: string
    required: true
  - name: tasks
    type: array
    items_type: object
    required: true

steps:
  - name: create
    for_each: inputs.tasks
    concurrency: 4
    mcp_call:
      server: torque
      tool: torque_task_create
      arguments:
        project_id: "{{ inputs.project_id }}"
        title: "{{ item.title }}"
        description: "{{ item.description }}"
    retry:
      attempts: 3
      backoff: exponential
      idempotency_key: "{{ inputs.project_id }}:{{ item.title }}"

  - name: summarize
    needs: [create]
    transform:
      created: map(steps.create.items, .outputs.result_json.id)
      failed: filter(steps.create.items, .status == "failed")

outputs:
  created: steps.summarize.outputs.created
  failed: steps.summarize.outputs.failed
  count: len(steps.summarize.outputs.created)
```

This example validates the IR requirements: array input, dynamic fan-out,
bounded concurrency, typed MCP results, retries with idempotency, transform
node, declared outputs, and direct MCP exposure as a pinned tool.

## Validation Requirements

The compiler must reject:

- duplicate node IDs;
- cycles;
- unknown dependencies;
- output references outside the visible dependency scope;
- source features that cannot lower faithfully;
- unknown step kinds;
- node config that does not match the registered kind schema;
- effect/retry combinations that violate policy;
- `call` cycles beyond configured depth;
- unresolved definition references unless explicitly deferred by policy.

## Decisions

- The reusable engine boundary is designed for extraction to a shared module,
  but can start inside Hadron while semantics settle.
- Engine core imports stay narrow: stdlib plus selected schema, expression, and
  test dependencies.
- Go structs are the IR source of truth, with generated JSON Schema for
  validation, UI, agents, and serialized plans.
- Hadron treats the workflow language as greenfield. `blueprint` and `pipeline`
  are not long-term public source kinds.
- Source maps are persisted on `ExecutionPlan`; node invocations store compact
  references into that plan-level source map.
- `expr-lang/expr` is the expression language unless implementation evidence
  forces reconsideration. `{{ }}` remains string interpolation only.
- The graph-native source format is named `workflow`.
- Preferred workflow file names are `*.workflow.yaml` for named files and
  `workflow.yaml` for directory-default entrypoints.
- `call.mode` uses `inline | run`. `inline` returns child outputs inside the
  same run; `run` creates child run identity.

## Decision Needed

- Exact IR field names and JSON/YAML representation.
- Source-map granularity within the persisted `ExecutionPlan`.
