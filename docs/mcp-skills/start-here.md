# Hadron MCP Start Here

Hadron is an agent-first blueprint runner. The recommended MCP flow is:

1. Call `hadron_skills` with no arguments when you need orientation.
2. Call `hadron_blueprint_broker` or `hadron_blueprint_discover` to find a likely blueprint.
3. Call `hadron_blueprint_schema` before enqueueing so you know the exact input shape.
4. Call `hadron_run_enqueue` to start the workflow.
5. Call `hadron_run_operations` for structured diagnostics and `hadron_run_events` for the raw append-only event log.

Prefer discovery and schema tools before guessing blueprint paths or inputs.
