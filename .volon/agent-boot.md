---
type: agent-boot
version: 1
updated_at: 2026-02-21
---

# Volon Agent Boot

## What is Volon

Volon is a multi-session orchestration system for repository automation. It coordinates human-directed workflows, task execution, and knowledge artifact generation through a single-writer Orchestrator that delegates bounded read-only work to sub-agents.

## Ground truth (always read these first)

- `volon.yaml` — system configuration (roles, storage, workflows, observability)
- `.volon/bootstrap.md` — current iteration state and next actions
- `.volon/pcc/` — project context cache (agent-only, ground truth for repo info)
- `.volon/tasks/` — task backlog and execution state
- `.volon/logs/` — run logs and decision logs
- `docs/03_workflow-contracts.md` — execution contract for workflows
- `docs/08_orchestrator.md` — Orchestrator role responsibilities

## Core rules

- **Single writer**: Only the Orchestrator may modify tasks, logs, PCC, or bootstrap. Sub-agents are read-only.
- **Ground truth in files, not chat**: Always re-ground from repo artifacts, never rely on conversation context.
- **Minimal diffs**: Small, verifiable steps. Each task should produce incremental changes.
- **Bootstrap boundaries**: Finalize each iteration with `/bootstrap-update` to enable clean restarts.
- **Bounded delegation**: Sub-agents execute single scoped objectives; no spawning other agents.
- **Deterministic pause/resume**: Use `/pause-task` and `/resume-task` with state on disk, not memory.

## How to start

1. Read `.volon/bootstrap.md` (if present) for current state and next action.
2. Identify your role: check `.volon/boot/` for your role addendum (orchestrator.md, worker.md, reviewer.md, architect.md).
3. If this session should use a different agent persona, run `/agent use <name>` before starting work. This prints the relevant boot prompt + guardrails; follow them immediately. To launch a fresh CLI session already in that profile, run `FORGE_AGENT_PROFILE=<name> scripts/volon-cli.sh --repo <path>` or pass `--agent <name>` to the script.
4. For Orchestrator: ground from `.volon/tasks/` by running `volon task list --status todo --priority A` (the Volon CLI is the canonical interface for create/start/done/list/reindex). Never hand-edit task frontmatter unless you are repairing a parse error.
5. For Workers/Reviewers: execute your single objective, return results in requested format, do not modify files.

At the beginning of any brand-new clone or export, run the **Initial Boot Checklist** from `docs/05_bootstrap.md` (Volon CLI bootstrap, PCC refresh, directory sanity). During normal work, follow the paired **Cleanup Cycle** in the same doc to keep PCC, task indexes, and bootstrap current.

## Quick boot

Copy/paste-friendly commands so you can start a clean profile in one step:

```
# CLI (Orchestrator example — set FORGE_AGENT_PROFILE or pass --agent)
FORGE_AGENT_PROFILE=orchestrator scripts/volon-cli.sh --repo /path/to/volon-dev --agent orchestrator --prompt-text "Resume sprint-2026-02 plan"

# CLI with external prompt file
scripts/volon-cli.sh --repo /path/to/volon-dev --agent architect --prompt-file /path/to/context.md

# Harness slash command
/invoke scripts/volon-cli.sh --repo /path/to/volon-dev --agent orchestrator --prompt-text "fresh boot" 
```

`--prompt-text` and `--prompt-file` are optional pass-through flags; they forward additional context to the Claude CLI without editing repo files.

## Your role

You are operating in Volon mode. Load the addendum for your role:
- **Orchestrator**: `.volon/boot/orchestrator.md` — you drive loops, write state, finalize iterations
- **Worker**: `.volon/boot/worker.md` — you execute scoped tasks, return results, read-only
- **Reviewer**: `.volon/boot/reviewer.md` — you scan and summarize, read-only

## Boot confirmation

On session start, the Orchestrator emits a structured boot confirmation block. See `.volon/boot/orchestrator.md` § "Boot confirmation output" for the exact format and required transition signals. Include the agent profile you are following (from `/agent use ...` or the `FORGE_AGENT_PROFILE` env var) so collaborators know which constraints apply.

## Reference map

| Topic | Doc |
|---|---|
| System config | `docs/01_config.md` |
| Project context cache | `docs/02_pcc.md` |
| PCC layers (L0/L2) | `docs/pcc_layers.md` |
| Workflow contracts | `docs/03_workflow-contracts.md` |
| Task model | `docs/04_task-model.md` |
| Iteration bootstrap | `docs/05_bootstrap.md` |
| Loop runner | `docs/06_loop-runner.md` |
| Sub-agents | `docs/07_subagents.md` |
| Orchestrator mode | `docs/08_orchestrator.md` |
| User commands | `docs/09_commands.md` |
| Pause/resume | `docs/10_pause_resume.md` |
| Git hooks | `docs/11_git-hooks.md` |
| Model config | `docs/12_model-config.md` |
| Inception workflow | `docs/13_inception-workflow.md` |
| Queue runner (concept) | `docs/queue_task_runner.md` |
