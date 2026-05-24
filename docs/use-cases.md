# Sample Use Cases

This page shows the kinds of workflows Hadron is already good at during the
public beta period.

## 1. Local Developer Automation

Use Hadron when you want repeatable local workflows with validation and an
audit trail instead of ad hoc shell scripts.

Examples:

- clean build artifacts across a repo
- run multi-step release prep
- scaffold or patch project files
- standardize recurring setup tasks for a team

Good starting examples:

- `examples/hello-hadron.yaml`
- `examples/dev-cleanup.yaml`
- `examples/hooks-demo.yaml`

## 2. Scheduled Housekeeping

Use Hadron when a task should run locally on a recurring schedule and you want
it represented as a blueprint instead of hidden in cron shell fragments.

Examples:

- daily repo maintenance
- periodic report generation
- cleanup tasks for local environments

Relevant commands:

```sh
hadron schedule create --blueprint <path> --cron "<expr>" --name <name>
hadron schedule list
```

## 3. Multi-Stage Pipelines

Use Hadron pipelines when a workflow is better expressed as several blueprints
with clear stage boundaries.

Examples:

- build -> verify -> package
- collect -> analyze -> report
- stage a deploy, then run post-checks

Starting point:

- `examples/pipeline-demo/`

## 4. Agent-Driven Workflow Execution

Use Hadron as an MCP-connected execution substrate when an agent should:

- discover a workflow
- inspect its input contract
- enqueue a run
- inspect structured diagnostics after execution

Recommended tool flow:

1. `hadron_skills`
2. `hadron_blueprint_broker`
3. `hadron_blueprint_schema`
4. `hadron_run_enqueue`
5. `hadron_run_operations`

Relevant docs:

- [mcp-setup.md](mcp-setup.md)
- [docs/mcp-skills/start-here.md](mcp-skills/start-here.md)

## 5. Agent-To-Agent Coordination

Hadron’s structured agentic steps let a blueprint coordinate local launched
agents, message passing, and human checkpoints.

Examples:

- launch an agent to perform a subtask, then wait for a reply
- send or wait on a local message thread
- pause for a human approval step before continuing

Starting examples:

- `examples/agentic-message-wait-local.yaml`
- `examples/agentic-launch-and-wait.yaml`

Reference:

- [agentic-workflows.md](agentic-workflows.md)

## 6. Beta-Era What Not To Assume

Hadron is already useful, but it is still beta. Do not assume yet that it is:

- a fully managed remote workflow platform
- a polished public blueprint marketplace
- a stable 1.0 API surface

Those are future directions, not current guarantees.
