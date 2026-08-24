# Workflow Engine Future-State Execution

**Session:** `session-20260823-21fe5742`  
**Integration worktree:** `/Users/chrispian/dev/hollis-labs/apps/hadron`  
**Hadron baseline:** `68c435f` (contains planning baseline `7301e2a`)  
**go-scheduler baseline:** `962e415` (`v0.1.0`)

This directory contains orchestration state for the 68-task workflow-engine
future-state execution. Implementation changes live in isolated repository
worktrees. The authoritative task definitions remain under
`docs/planning/workflow-engine-future-state/`.

Files:

- `task-ledger.md`: dependency and execution state.
- `integration-commits.md`: accepted implementation commits and integration order.
- `verification-evidence.md`: independently rerun commands and outcomes.
- `owner-input.md`: decisions that require owner authority.
- `deferred-findings.md`: findings deliberately deferred from an in-scope task.
