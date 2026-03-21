# Hadron Competitive Landscape — March 2026

## Executive Summary

Hadron occupies a unique position: **YAML-declarative + DAG execution + local-first + MCP-native + scaffold-capable**. No competitor combines all five. The closest are n8n (visual-first, not YAML), Windmill (code-first, weak AI), Kestra (YAML + DAG, needs server), and Task/Just (YAML + local, no DAG).

**Strategic position: "The automation runtime AI agents and developers share."**

---

## Scorecard

| Dimension | Hadron | n8n | Windmill | Temporal | Kestra | Task | Ansible | Rivet |
|---|---|---|---|---|---|---|---|---|
| YAML/declarative spec | ★★★ | ★ | ★★ | ☆ | ★★★ | ★★★ | ★★★ | ☆ |
| DAG execution | ★★★ | ★★ | ★★★ | ★★★ | ★★★ | ☆ | ☆ | ★★★ |
| Local-first | ★★★ | ★★ | ★★ | ★★ | ★ | ★★★ | ★★★ | ★★★ |
| AI/MCP native | ★★★ | ★ | ★ | ☆ | ★ | ☆ | ★ | ★★★ |
| Visual builder | ★★ | ★★★ | ★★ | ☆ | ★★★ | ☆ | ☆ | ★★★ |
| Template/scaffold | ★★★ | ☆ | ☆ | ☆ | ☆ | ☆ | ★★ | ☆ |
| Community/ecosystem | ★ | ★★★ | ★★ | ★★★ | ★★ | ★★ | ★★★ | ★ |
| Production hardened | ★★ | ★★ | ★★ | ★★★ | ★★ | ★★★ | ★★★ | ★ |

---

## Category 1: CLI Task Runners & Automation

| Tool | Stars | Spec | DAG | Local | AI | Visual | Gap vs Hadron |
|---|---|---|---|---|---|---|---|
| Task (taskfile.dev) | ~13k | YAML | Partial | Yes | None | No | No DAG, no templates, no AI |
| Just | ~23k | Justfile | No | Yes | None | No | No DAG, no parallelism, purely imperative |
| GNU Make | Ubiquitous | Makefile | Yes | Yes | None | No | Arcane syntax, no YAML, no AI |
| Nx | ~24k | JSON/TS | Yes | Hybrid | Plugin | Yes | Monorepo-only, JS ecosystem |
| Dagger | ~11k | Go/Python/TS | Yes | Yes | None | No | Container-dependent, no YAML |
| Earthly | ~11k | Earthfile | Yes | Yes | None | No | Container-dependent, build-only |
| Mise | ~12k | TOML | Partial | Yes | None | No | Task features immature |

**Takeaway**: Task/Just are the closest in philosophy but lack DAGs, AI, and templates. Hadron's differentiation is clear.

---

## Category 2: Workflow / Pipeline Engines

| Tool | Stars | Spec | DAG | Local | AI | Visual | Gap vs Hadron |
|---|---|---|---|---|---|---|---|
| Kestra | ~14k | YAML | Yes | Hybrid | Plugin | Yes | Needs server, no MCP, not local-first |
| Prefect | ~18k | Python | Yes | Hybrid | Plugin | Yes | Python-only, cloud-pushing |
| Temporal | ~12k | Code SDKs | Yes | Hybrid | None | Monitor only | No visual builder, no YAML, no AI |
| Airflow | ~38k | Python | Yes | Needs infra | Plugin | Yes | Heavy infra, Python-only, slow local |
| Argo Workflows | ~15k | K8s YAML | Yes | No | None | Yes | Kubernetes required |
| Windmill | ~12k | Code + visual | Yes | Hybrid | Basic | Yes | Needs server, smaller community |
| Inngest | ~5k | TS/Python | Yes | Hybrid | Yes | Basic | TS-centric, cloud-oriented |
| Hatchet | ~4k | Code SDKs | Yes | Hybrid | Yes | Dashboard | Young, needs server |

**Takeaway**: Kestra is closest (YAML + DAG + visual). Key gaps: no MCP, no local-first, no scaffold. Windmill is worth watching.

---

## Category 3: Blueprint / Scaffold Tools

| Tool | Stars | Spec | Gap vs Hadron |
|---|---|---|---|
| Cookiecutter | ~23k | Jinja2 + JSON | Generate only, no execution |
| Yeoman | ~9.5k | JavaScript | Aging, heavy, generate only |
| Hygen | ~5.5k | EJS + YAML | Generate only, less maintained |
| Plop | ~10k | JS + Handlebars | Micro-generator only |
| Backstage Templates | ~29k | YAML + Nunjucks | Needs Backstage server, enterprise-heavy |
| Copier | ~2k | Jinja2 + YAML | Generate only, smaller community |

**Takeaway**: No scaffold tool also executes pipelines. Hadron uniquely bridges generation and orchestration.

---

## Category 4: AI Workflow Builders

| Tool | Stars | Spec | DAG | Local | Visual | Gap vs Hadron |
|---|---|---|---|---|---|---|
| n8n | ~55k | Visual + JSON | Yes | Hybrid | Polished | Visual-first (not YAML), needs server |
| Rivet | ~3k | Visual (JSON) | Yes | Yes | Polished | AI-only, narrow scope |
| Flowise | ~35k | Visual (JSON) | Partial | Hybrid | Good | LangChain-dependent, not general |
| LangFlow | DataStax | Visual | Yes | Hybrid | Good | LangChain-dependent, Python-only |
| ComfyUI | Massive | Visual (JSON) | Yes | Yes | Polished | Image generation only |

**Takeaway**: AI workflow builders are all visual-first and AI-only. Hadron bridges general automation + AI.

---

## Category 5: IT Ops / Runbook Automation

| Tool | Stars | Spec | Gap vs Hadron |
|---|---|---|---|
| Ansible | ~63k | YAML | Infra-only, no DAG pipelines, no AI |
| Rundeck | PagerDuty | YAML | Dated UI, limited workflow complexity |
| StackStorm | ~6k | YAML | Event-driven but dated, no AI |
| SaltStack | Broadcom | YAML/Python | Community uncertain, no AI |

**Takeaway**: Ansible owns "YAML-as-automation" mindshare. Hadron must differentiate on modern DX, DAGs, and AI-native.

---

## Standards Landscape

| Standard | Body | Adoption | Hadron Action |
|---|---|---|---|
| GitHub Actions `needs` | GitHub | Massive | Already aligned (depends_on) |
| Serverless Workflow (CNCF) | CNCF | Moderate | Align terminology |
| Argo retryStrategy | CNCF | Wide | Adopt retry patterns |
| MCP | Anthropic/AAIF | Wide (97M+ monthly) | Already integrated |
| A2A Agent Cards | Google/AAIF | Growing | Planned integration |
| AAIF (umbrella) | Linux Foundation | Forming | Track closely |

---

## Market Gaps Hadron Can Fill

1. **"Makefile for the AI era"** — Task/Just for humans. Hadron for humans AND agents.
2. **"Local Airflow"** — DAGs without deploying infrastructure. Single binary.
3. **Self-hosted AI agent orchestration** — Cloud AI platforms leave privacy-conscious teams stranded.
4. **YAML + Visual bridge** — Define in YAML (devs), visualize for debugging (prosumers).
5. **Blueprint-to-execution pipeline** — Generate from template, then run setup, all in one tool.

---

## Things to Avoid

- Don't compete with Temporal on durability ($250M+ funding, their entire story)
- Don't build an integration catalog (unwinnable arms race)
- Don't go visual-first (n8n owns that)
- Don't overload YAML (Ansible taught us YAML at scale gets painful)
- Don't chase enterprise iPaaS (ServiceNow/MuleSoft are different universes)

---

## Pricing Reference (if commercialized)

| Segment | Price Point | Model |
|---|---|---|
| Individual developer | Free | Open source |
| Small team (5-20) | $50-200/month | Per-seat or usage |
| Mid-market (50-500) | $500-2K/month | Per-seat + usage |
| Enterprise | $20K-100K+/year | Platform license |

Open-core model dominates this segment. Free self-hosted drives adoption; cloud/enterprise features drive revenue.
