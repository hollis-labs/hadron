# Hadron MCP Start Here

Hadron exposes graph-native workflows over MCP. The recommended MCP flow is:

1. Call `hadron_skills` with no arguments when you need orientation.
2. Call `hadron_workflows_search` or `hadron_workflow_catalog_search` to find
   visible workflow definitions.
3. Call `hadron_workflows_load` when you need exact mounted workflow tools.
4. Call the mounted workflow tool or `hadron_workflow_run` to start a run.
5. Call `hadron_workflow_run_inspect` first for diagnostics, then
   `hadron_workflow_run_events` when you need the event trail.

Prefer discovery and description tools before guessing names, versions, digests,
or input shapes.
