# MCP setup

Hadron exposes the same graph-native application services over MCP stdio. The
adapter owns session mounts and protocol projection; it does not compile,
authorize, or execute workflows privately.

## Start the server

```sh
hadrond mcp \
  -data "$HOME/.hadron" \
  -db "$HOME/.hadron/state/hadron.db" \
  -logs "$HOME/.hadron/logs/runs" \
  -token '<secret>'
```

`-token` is required. On first use Hadron persists only its digest, creates a
local principal, and creates a bounded meta-only/default-agent-namespace
exposure profile. Restarting with the same token reuses that principal and
preserves profile changes such as exact pins. Surrounding whitespace and
control-bearing tokens are rejected rather than silently normalized.

Example client configuration:

```json
{
  "mcpServers": {
    "hadron": {
      "command": "/absolute/path/to/hadrond",
      "args": [
        "mcp",
        "-data", "/absolute/path/to/.hadron",
        "-db", "/absolute/path/to/.hadron/state/hadron.db",
        "-logs", "/absolute/path/to/.hadron/logs/runs",
        "-token", "replace-with-a-secret"
      ]
    }
  }
}
```

There is no `-token-scopes` flag. Durable principal/profile records and the
application authorization boundary determine access.

## Discovery and mounts

Sessions begin with bounded meta tools. Exposure is profile-driven:

- explicit exact pins become direct workflow tools;
- authorized namespace/catalog records remain discoverable, not eagerly
  mounted;
- `hadron_workflows_search` searches the session exposure view;
- `hadron_workflows_load` lazily mounts an exact discovered definition;
- generation changes reauthorize and reconcile mounts before every operation;
- collision or direct-tool budget failure is atomic and advertises no partial
  tool set.

Tool names are deterministic bounded ASCII. Each descriptor is derived from one
immutable definition and the canonical graph input/output schemas and effects.
Invoking a generated tool starts an asynchronous durable run and returns a run
handle. Follow it with inspect/events/subscribe; optional terminal `outputs`
conform to the nested workflow output schema.

## Graph operation tools

The graph-native families are:

- discovery: `hadron_workflows_search`, `hadron_workflows_load`,
  `hadron_workflow_describe`;
- execution: `hadron_workflow_validate`, `hadron_workflow_run`,
  `hadron_workflow_run_inspect`, `hadron_workflow_run_cancel`,
  `hadron_workflow_run_events`, `hadron_workflow_run_subscribe`;
- waits: `hadron_workflow_run_resume`, `hadron_workflow_gate_submit`,
  `hadron_workflow_message_submit`, `hadron_workflow_signal`;
- lifecycle discovery: `hadron_workflow_catalog_search`,
  `hadron_workflow_catalog_inspect`;
- authoring: `hadron_workflow_author_validate`, `_scaffold`, `_test`, and
  `_register`;
- registry: `hadron_workflow_registry_package`, `_pin_version`,
  `_unpin_version`, `_publish`, and `_clear_current`;
- exposure: `hadron_workflow_exposure_inspect`, `_pin`, and `_unpin`.

Use `hadron_skills` for the active start-here, workflow-lifecycle, and typed
run-inspection guidance. Workflow-only mode filters legacy blueprint, pipeline,
schedule, and message-broker instructions from tools, prompts, resources, and
skill responses.

## Security and data projection

The token binds a session to one durable principal/profile. Session IDs and run
IDs are not capabilities. Hidden workflows produce the same safe not-found
shape as nonexistent workflows. Private display is additive restriction and is
authorized before rendering. Raw workflow source, credentials, contract mock
payloads, secret values, lease/token state, and unbounded event bodies are not
projected through MCP.

Direct run tools are conservatively non-idempotent. Resume operations are
wait-ID idempotent, signal requires its key, and session-local load is replay
safe. Exact version and digest are preserved from descriptor through invocation.

## Troubleshooting

- **Server exits at startup:** supply a nonempty valid `-token` and writable,
  private data/database/log paths.
- **Only meta tools appear:** inspect the profile, search its authorized scope,
  then lazy-load a record or add an exact exposure pin through authorized
  lifecycle tooling.
- **A tool disappears:** the profile/catalog generation changed or current
  policy no longer permits it; mounts reconcile before use and fail closed.
- **A workflow cannot run:** confirm the record is exact/published as required,
  the exposure profile admits its effects, and all step kinds are supported by
  the production host.
