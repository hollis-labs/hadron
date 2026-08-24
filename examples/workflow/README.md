# Graph-Native Workflow Examples

These are the active examples for the graph-native `workflow` source contract:

- [`torque-task-bulk-create.workflow.yaml`](torque-task-bulk-create.workflow.yaml)
  demonstrates typed inputs and outputs, bounded fan-out, per-item MCP calls,
  retry idempotency, and a dependency-visible transform summary.
- [`release-approval-gate.workflow.yaml`](release-approval-gate.workflow.yaml)
  demonstrates a wait-like human gate, typed decision output, and downstream
  control plus data dependency.
- [`http-cmd-transform.workflow.yaml`](http-cmd-transform.workflow.yaml)
  demonstrates typed HTTP, command, and transform composition with explicit
  control dependencies and inferred data visibility.

Repository tests load these files through the production source loader,
compiler, value-dependency inference, and graph validator. Test-only no-op step
kinds stand in for executor implementations that later waves own. The examples
therefore document and verify the plan contract; they do not yet claim
end-to-end runtime or adapter execution.

Files under
[`../archive/legacy-blueprints-pipelines/`](../archive/legacy-blueprints-pipelines/)
remain historical rewrite references only and are not active workflow source.
