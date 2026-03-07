# Hadron — Agent Boot

This is a Volon-managed project.

## Boot sequence

1. Read `.agentrc/agent-boot.md` — core rules and reference map
2. Read `.agentrc/bootstrap.md` — current iteration state
3. Review SessionStart output for assigned tasks
4. Claim highest-priority task: `volon task start <TASK-ID>`
5. Execute following description, pointers, and acceptance criteria
6. On completion: `volon task done <TASK-ID>`
7. Update bootstrap if iteration changed

## Rules

- **Claim before coding**: Always `volon task start <id>` before writing application code
- **One task at a time**: Complete or pause current task before starting another
- **Update status**: Use `volon task done|blocked|pause` on transitions
- **Follow conventions**: See `.agentrc/pcc/global/02_conventions.md`
- **Single writer**: You are the single writer. Sub-agents are read-only.

## Key files

| File | Purpose |
|------|---------|
| `agentrc.yaml` | Project configuration |
| `.agentrc/bootstrap.md` | Current iteration state |
| `.agentrc/agent-boot.md` | Core rules + reference map |
| `.agentrc/pcc/global/` | Project context cache |
| `.agentrc/tasks/` | Task files |
| `Makefile` | Build and test targets |
| `internal/persistence/store.go` | Core persistence layer |
| `docs/spec-v04.md` | Current specification |
