# Hadron Demos

Demo blueprints for live walkthroughs. Organized by complexity.

## Quick Runs (execute live, ~5-15s each)

| # | File | Shows |
|---|------|-------|
| 01 | `01-hello-world.yaml` | Minimal blueprint — "it works" |
| 02 | `02-system-recon.yaml` | Parameterized inputs, conditionals, env vars |
| 03 | `03-cross-system-mcp.yaml` | Pipeline calling Hadron + Cortex + Engine MCP tools |
| 04 | `04-git-audit.yaml` | Multi-repo git health check (practical output) |
| 05 | `05-hooks-lifecycle.yaml` | Before/after/error hooks + per-task hooks |

## Visual Flow Builder (load in canvas, walk through)

| # | Dir | Shows |
|---|-----|-------|
| 06 | `06-ci-pipeline/` | DAG: build → (lint ∥ test-unit ∥ test-integration) → deploy → notify |
| 07 | `07-retry-cascade/` | Retry logic, fallback stages, conditional error recovery |

## Concept Demo (walk through blueprint + emulated output)

| # | Dir | Shows |
|---|-----|-------|
| 08 | `08-security-swarm/` | Multi-agent security response: triage → parallel analysis → remediation |

## Running

```bash
# Single blueprint
hadron run demos/01-hello-world.yaml

# With inputs
hadron run demos/02-system-recon.yaml -i target_host=localhost -i check_docker=true

# Pipeline
hadron pipeline run demos/06-ci-pipeline/pipeline.yaml

# Dry run (no execution, just plan)
hadron run --dry-run demos/08-security-swarm/pipeline.yaml
```
