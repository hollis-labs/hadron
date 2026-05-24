# Blueprint Discovery

Use `hadron_blueprint_broker` when you want ranked blueprint recommendations with reasons and next steps.

Use `hadron_blueprint_discover` when you have a task and want likely-fit blueprints.

Use `hadron_blueprint_search` when you need deterministic keyword matching.

Use `hadron_blueprint_schema` after choosing a blueprint so you can construct valid inputs for `hadron_run_enqueue`.

Avoid relying on registry-only tools for first-pass agent discovery. Registry indexing remains useful operationally, but the discovery tools work directly from the configured blueprint directory.
