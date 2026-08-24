# 03 - Values, Expressions, And Artifacts

## Architectural Change

Replace string templates and log-scraped outputs with typed executor returns,
typed value bindings, expressions, and artifact references.

Logs remain operational event streams. They are not the workflow data plane.

## Current Shape

Hadron has typed blueprint inputs but no typed blueprint outputs:

- `Input` lives in
  [`internal/blueprint/blueprint.go`](../../../internal/blueprint/blueprint.go).
- `Blueprint` has no top-level `outputs`.
- `RenderForExecution` renders templates before execution, so step results
  cannot be referenced by later blueprint steps.
- `cmd` output is streamed to `run_events` through a PTY in
  [`internal/execution/manager.go`](../../../internal/execution/manager.go).
- `http_call`, `mcp_call`, `agent_launch`, `message_wait`, and `human_gate`
  emit compatibility `::set-output` log lines.
- [`internal/pipeline/runner.go`](../../../internal/pipeline/runner.go) scans
  run event log messages for `::set-output`, stores string outputs, and uses
  string replacement for downstream stage inputs.

This mixes logs, control flow, and data flow. It also makes arbitrary command
output capable of setting pipeline outputs if it prints the marker syntax.

## Target Value Model

Executors return structured results. The runtime validates and stores them.

```go
package runtime

type StepResult struct {
	Status      StepStatus
	Outputs     map[string]Value
	Error       *StepError
	Attempts    int
	Artifacts   []ArtifactRef
	Usage       map[string]any
	Diagnostics []Diagnostic
}

type Value struct {
	Type        string // string | number | boolean | object | array | null | artifact
	Inline      any
	ArtifactRef *ArtifactRef
	Redaction   RedactionClass
	Retention   RetentionClass
	MediaType   string
	Digest      string
}

type ArtifactRef struct {
	Store      string
	URI        string
	Digest     string
	MediaType  string
	SizeBytes  int64
	Producer   NodeInvocationID
	Retention  RetentionClass
	Redaction  RedactionClass
}
```

Small JSON-compatible values can flow inline. Large, binary, sensitive, or
long-lived content flows by reference. The artifact owner may be a project
workspace, object store, external provider, message system, or Hadron-managed
temporary artifact store.

## Output Contracts

Each node kind declares its possible output schema. Each node can narrow or
name outputs for its own contract.

```yaml
- name: fetch-ticket
  mcp_call:
    server: torque
    tool: torque_task_get
    arguments:
      id: "{{ inputs.task_id }}"
  outputs:
    task:
      type: object
      required: true
    title:
      type: string
      value: outputs.result_json.title
```

Top-level workflow outputs make a workflow callable as a function or tool:

```yaml
outputs:
  task_id:
    type: string
    value: steps.create.outputs.result_json.id
  created:
    type: boolean
    value: steps.create.status == "succeeded"
```

The runtime persists the output envelope, not just the final string value. That
envelope carries retention, redaction, producer, digest, and schema metadata.

## Expressions

Expressions operate on typed values. String interpolation is only an authoring
convenience for string fields.

Expression context:

```text
inputs.<name>
steps.<node_id>.outputs.<name>
steps.<node_id>.status
steps.<node_id>.error
steps.<node_id>.items
item
index
run
run_scope
execution_target
env            # only if policy allows the reference
```

Examples:

```yaml
if: steps.fetch.outputs.status_code == 200

for_each: inputs.tasks

transform:
  ids: map(steps.create.items, .outputs.result_json.id)
  failed: filter(steps.create.items, .status == "failed")
  needs_review: len(steps.create.items) != len(transform.ids)
```

Unresolved references fail. If the compiler can detect the error statically,
it should fail during compile. If the reference depends on runtime fan-out or
optional branches, it should fail the node with structured diagnostics.

## Visibility And Edges

Visibility is the union of:

- explicitly declared `needs`;
- inferred data dependencies from expressions;
- runtime-provided scoped values such as `inputs`, `run`, `item`, and `index`.

A node cannot read `steps.secret.outputs.token` merely because that step ran
earlier. It must depend on the producer or receive the value through a declared
binding.

This preserves Nanite's fail-hard scoped visibility while allowing the compiler
to infer data edges from expressions.

## `cmd` Output Handling

Command stdout and stderr should become structured streams and optional typed
outputs.

```yaml
- name: query
  cmd: "gh api repos/hollis-labs/hadron/issues/1"
  capture:
    stdout:
      as: output
      name: issue
      parse: json
    stderr:
      as: event_stream
```

Target executor result:

```go
StepResult{
	Status: Succeeded,
	Outputs: map[string]Value{
		"exit_code": IntValue(0),
		"issue": ObjectValue(parsedJSON),
	},
}
```

PTY streaming can stay for operator UX, but it cannot be the only way to
obtain data.

## Compatibility Shim

`::set-output` can exist only as an explicit compatibility parser:

```yaml
- name: legacy
  cmd: "./old-script.sh"
  capture:
    stdout:
      parse: set-output
      compatibility: true
```

Rules for the shim:

- disabled by default in new source profiles;
- only scans the selected stream for the selected node;
- never scans all run events;
- marks values with `origin: compatibility_set_output`;
- emits warnings during validation;
- subject to the same schema, redaction, and retention rules as native outputs.

## Artifacts And Redaction

Values need data classification at production time, not only when displayed.

```yaml
outputs:
  report:
    type: artifact
    media_type: application/pdf
    retention: project
    redaction: private
    value: steps.render.outputs.pdf
  summary:
    type: string
    retention: run
    redaction: public
    value: steps.summarize.outputs.text
```

The event stream should carry facts such as "artifact produced" and "value
redacted" without accidentally storing sensitive payloads.

## Decisions

- Use one `Value` envelope for both inline values and artifact references.
- Store small JSON-compatible values inline.
- Store large, binary, sensitive, or long-lived content through `ArtifactRef`.
- Every value carries producer metadata, media type, retention class, redaction
  class, and digest where applicable.
- Use `expr-lang/expr` for expressions. Keep `{{ }}` for string interpolation
  only.
- Secrets are opaque references resolved at adapter boundaries, for example
  `secret://authority/path#field`.
- Hadron records secret references and provenance, not secret material.
- Values and events carry redaction and retention metadata.
- Initial redaction classes are `public | private | secret`.
- Initial retention classes are `none | run | project | external`.

## Decision Needed

- Inline value size cap.
- Artifact store interface details.
- Whether `env` exists in expression scope and under what policy.
- Exact `cmd` capture syntax and whether stderr is separated by default.
- Compatibility `::set-output` behavior and removal horizon.
