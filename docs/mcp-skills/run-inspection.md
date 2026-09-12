# Run Inspection

Use `hadron_workflow_run_inspect` for the current graph-native run summary,
nodes, waits, values, resources, activation attempts, and redacted events.

Use `hadron_workflow_run_events` when you need the raw redacted graph-native
event trail.

Prefer `hadron_workflow_run_inspect` before reading event text when you need to
understand a failed or waiting step.

For the operator CLI/HTTP sequence, see `docs/workflow-diagnostics.md`.
