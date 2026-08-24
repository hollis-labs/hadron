# 06 - Step Kinds And Executors

## Architectural Change

The engine core schedules and accounts for nodes. Registered step-kind
executors perform real work.

Hadron already has narrow interfaces for MCP, messages, and agent launch. The
future-state architecture generalizes that shape into a step-kind registry with
schemas, effect metadata, output contracts, suspension support, and conformance
tests.

## Current Shape

Useful seams already exist in
[`internal/execution/types.go`](../../../internal/execution/types.go):

- `MCPCaller`
- `MessageSource`
- `AgentLauncher`
- `SettingsValidator`

However, dispatch is still hard-coded in
[`internal/execution/manager.go`](../../../internal/execution/manager.go):

```go
if step.HTTPCall != nil {
	stepErr = r.executeHTTPCallStep(...)
} else if step.MCPCall != nil {
	stepErr = r.executeMCPCallStep(...)
} else if step.MessageWait != nil {
	stepErr = r.executeMessageWaitStep(...)
} else if step.AgentLaunch != nil {
	stepErr = r.executeAgentLaunchStep(...)
} else if step.HumanGate != nil {
	stepErr = r.executeHumanGateStep(...)
} else if strings.TrimSpace(step.Call) != "" {
	stepErr = r.executeCallStep(...)
} else {
	stepErr = r.runStep(...)
}
```

The target replaces kind-specific branching in the core with registry lookup
and metadata-driven validation.

## Step-Kind Contract

```go
type StepKindSpec struct {
	Name                 string
	Version              string
	ConfigSchema          SchemaRef
	InputSchema           SchemaRef
	OutputSchema          SchemaRef
	RequiredCapabilities  []Capability
	Effects               EffectSet
	Idempotency            IdempotencySpec
	RetrySafety           RetrySafety
	Cancellation           CancellationSpec
	Observation            ObservationSpec
	CanSuspend             bool
	EmbeddedModeSupported  bool
}

type StepKind interface {
	Spec() StepKindSpec
	ValidateConfig(ctx context.Context, cfg map[string]any) error
	Prepare(ctx context.Context, inv Invocation) (PreparedInvocation, error)
	Execute(ctx context.Context, inv PreparedInvocation) (StepResult, error)
	Cancel(ctx context.Context, ref ExternalOperationRef) error
	Observe(ctx context.Context, ref ExternalOperationRef) (Observation, error)
}

type Registry interface {
	Register(kind StepKind) error
	Lookup(name string, version string) (StepKind, bool)
	List() []StepKindSpec
}
```

The exact lifecycle can be smaller, but every kind must provide enough metadata
for validation, policy, retry, recovery, exposure, and operator explanation.

## Standard Step Kinds

| Kind | Role | Target notes |
|---|---|---|
| `cmd` | run local command | capture stdout/stderr/exit code as typed outputs; stream logs separately |
| `http` | call HTTP endpoint | typed status, headers, body, body_json; auth via secret references |
| `mcp` | call external MCP tool | output is tool result; effects inferred or overridden from annotations |
| `llm` | typed model turn | provider/model binding outside IR; restricted tool surface; schema output |
| `agent_launch` | launch full agent/session | heavy open-ended work; optional wait sugar |
| `message_wait` | wait for correlated message | domain adapter over generic wait |
| `human_gate` | wait for decision | domain adapter over generic wait |
| `call` | call child workflow | inline or child run; returns declared child outputs |
| `script` | sandboxed imperative transform | goja first candidate; Python later by subprocess decision |
| `transform` | pure expression transform | no effects; memoizable |
| `sleep` | durable timer | implemented through wait/timer activation |
| `wait_for` | generic external wait | signal, callback, run completion, event, or condition |
| `emit` | publish event/message | explicit effect and output envelope |
| `checkpoint` | domain checkpoint | package-level concept over waits and policy |

The core should not hard-code business-specific step semantics. Product-owned
capabilities should usually appear as MCP tools, HTTP/OpenAPI operations,
registered adapters, or child workflow calls.

## Effects

Every executable kind and node declares an effect classification:

```text
read | compute | materialize | mutate | destructive
```

Effects drive:

- retry safety;
- recovery behavior for claimed/running nodes after crash;
- MCP annotations and agent confirmation policy;
- dry-run truthfulness;
- memoization eligibility;
- blast-radius explanation;
- required grants.

Example:

```yaml
- name: delete-branch
  effects: [destructive]
  mcp_call:
    server: github
    tool: delete_branch
    arguments:
      repo: "{{ inputs.repo }}"
      branch: "{{ inputs.branch }}"
  confirmation:
    required: true
```

## LLM Node

`llm` is a typed workflow function, not a provider dependency in the IR.

```yaml
- name: classify
  llm:
    profile: small-structured
    system: "Classify the request."
    messages:
      - role: user
        content: "{{ inputs.text }}"
    tools: []
    output_schema:
      type: object
      required: [kind, confidence]
      properties:
        kind: { type: string, enum: [bug, feature, question] }
        confidence: { type: number }
    budget:
      max_tokens: 500
  verify:
    - type: output_schema
```

Executor binding options remain open:

- direct `go-providers` adapter;
- `agentkit` adapter;
- Nanite harness adapter;
- multiple adapters behind one `llm` step contract.

Required behavior:

- model/provider selection by host binding or profile;
- tool allowlist scoped to the node;
- tool-call records persisted for audit and verify;
- provider errors distinct from model-decision failures;
- output schema enforcement with explicit repair/fail policy.

## Agent Launch

`agent_launch` remains for open-ended or session-oriented work.

Target source can add wait sugar:

```yaml
- name: review
  agent_launch:
    substrate: local_runtime
    logical_agent_id: reviewer-1
    prompt_append: "Review the candidate patch."
    wait:
      timeout: 30m
```

The sugar composes an agent launch with a message wait on returned handles. It
does not make agent sessions the only AI primitive.

## Script And Transform

`transform` is pure and expression based:

```yaml
- name: summarize
  transform:
    created: map(steps.create.items, .outputs.id)
    count: len(transform.created)
```

`script` is an escape hatch for local data manipulation:

```yaml
- name: normalize
  script:
    runtime: goja
    code: |
      export default function(input) {
        return { title: input.title.trim(), slug: input.title.toLowerCase() }
      }
```

The script runtime must have explicit time, memory, module, network, file, and
capability boundaries.

## Decisions

- Required executor lifecycle is minimal: `Spec`, `ValidateConfig`, and
  `Execute`.
- Optional lifecycle interfaces cover `Prepare`, `Observe`, `Cancel`, and
  `Finalize`.
- Optional lifecycle support is advertised in step-kind metadata.
- `llm` is one provider-agnostic contract with multiple possible executor
  bindings.
- Possible `llm` executor bindings include direct `go-providers`, `agentkit`,
  and Nanite harness adapters.
- Engine core imports none of the concrete LLM/provider/harness dependencies.
- `script` starts with goja JavaScript for local deterministic data
  manipulation.
- Python is a later explicit subprocess/sandbox decision.
- Initial built-in step kinds are `cmd`, `transform`, `call`, `sleep`,
  `wait_for`, `human_gate`, `message_wait`, `mcp`, and `http`.
- `llm`, `script`, `agent_launch`, and compiled/offline support are follow-on
  unless the planner selects an AI-first milestone.

## Decision Needed

- Exact registry and executor method signatures.
- LLM output-schema repair policy.
- goja sandbox limits.
- How MCP annotations map to effect/idempotency metadata.
- Whether OpenAPI/AsyncAPI/gRPC/GraphQL calls are first-class step kinds or
  adapter-generated `http`/`mcp` nodes.
