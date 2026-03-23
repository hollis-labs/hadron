# Hadron

## agentrc
- If `.agentrc/boot-prompt.md` exists, read it first for session context.
- If the user says "Boot <agent>", look up the agent in `.agentrc/config.yaml` under `agents:`. Load each role file from `~/.agentrc/roles/` (using the `file:` path from `~/.agentrc/config.yaml` role definitions), load the listed skills, and read the project context file from `.agentrc/` if specified.
- If the user says "Boot <role>" and no agent matches, fall back to loading that single role from `~/.agentrc/roles/` by type directory (domain/, stack/, meta/).
- After context compaction, re-read the active role and project context files.
- Do not guess when uncertain. Stop and ask.
- Prefer focused, minimal output. No trailing summaries.
- Sub-agent output stays in the sub-agent. Main context gets one-line confirmations.

## Key files

| File | Purpose |
|------|---------|
| `Makefile` | Build and test targets |
| `internal/persistence/store.go` | Core persistence layer |
| `docs/spec-v04.md` | Current specification |
