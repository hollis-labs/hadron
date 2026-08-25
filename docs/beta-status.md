# Beta status

Hadron's active product contract is graph-native. One durable application host
backs CLI, HTTP, browser UI, MCP, A2A, schedules, and external activations.

Implemented today:

- graph YAML compilation, generated schema, diagnostics, exact immutable plans,
  typed inputs/outputs, values, artifacts, and visibility-aware rendering;
- policy-checked validate/explain/run, truthful dry-run, inspect, cancel,
  authorized wait resume, downstream rerun, events, and subscription;
- SQLite durability, recovery, resource-aware scheduling, fan-out occupancy,
  lease renewal, and graceful worker/timer shutdown;
- registry qualification/publication/current state, deterministic contract-test
  evidence, lifecycle authoring, exact exposure-profile pins, and source
  activation materialization;
- session-isolated MCP discovery/lazy mounts/direct tools, A2A task/run
  correlation, generated workflow HTTP/TypeScript clients, and graph-native UI;
- production host support for `transform@v1`, `script@v1`, `sleep@v1`,
  `wait_for@v1`, `message_wait@v1`, and `human_gate@v1`.

Beta constraints:

- public contracts and authoring ergonomics can still evolve;
- the stock daemon capability profile is intentionally narrower than the
  repository's embeddable adapter catalog;
- packaging and installation UX are still being refined;
- browser and agent-client interoperability continues to receive hardening;
- examples outside `examples/workflow/production` may be compiler or adapter
  conformance fixtures rather than stock-daemon runnable workflows.

The beta-era blueprint and pipeline runtime is retired from production roots,
routes, MCP instructions, and UI navigation. Retained implementation and sample
files are archive/rewrite-only and have no public compatibility promise.

Before filing an issue, compare behavior with [Workflow authoring and
operations](workflows.md), [Safety](safety.md), and the executable command help.
