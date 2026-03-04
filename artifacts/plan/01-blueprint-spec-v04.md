---
type: spec
id: SPEC-HAD-001
title: "Hadron Blueprint Spec v0.4"
status: accepted
date: 2026-02-28
---

# Hadron Blueprint Spec v0.4

Merged from nanite-wails-starter v0.2 (richness) and vnext v0.3 (improvements).
This is the ground truth for the Go `blueprint` package implementation.

## Format

Supported: YAML, JSON, JSONC (with `//` and `/* */` comments, trailing commas).
Detection: by file extension first (`.yaml`/`.yml` → YAML, `.json`/`.jsonc` → JSONC), then content heuristics.

## Top-Level Fields

```yaml
version: "0.4"            # required
blueprint:                # required — meta block
  name: string            # required (or slug)
  slug: string            # URL-safe identifier
  title: string           # human display name
  description: string
  author: string
  license: string
  tags: [string]
  homepage: string

inputs: [Input]           # optional — parameterized inputs
project: Project          # optional — project context
env: {string: string}     # optional — environment variables
packages: Packages        # optional — package manager declarations
git: Git                  # optional — git config
stubs: Stubs              # optional — file stub config
tools: Tools              # optional — tool installation config
imports: [Import]         # optional — import other blueprints
hooks: Hooks              # optional — blueprint-level lifecycle hooks
steps: [Section]          # required — execution steps grouped in sections
```

## Input

```yaml
inputs:
  - name: app_name          # required, pattern: ^[a-zA-Z][a-zA-Z0-9_\-]*$
    label: "Application Name"
    description: string
    type: string|number|boolean|array   # required
    required: bool           # default false
    default: any
    enum: [any]
    prompt: string           # UI prompt text
    short_flag: string       # CLI short flag (e.g. "n" → -n)
    # type: string extras
    pattern: string          # regex
    min_length: int
    max_length: int
    # type: number extras
    min: float
    max: float
    # type: array extras
    items_type: string|number|boolean
```

## Project

```yaml
project:
  type: string             # e.g. app, lib, tool
  name: string
  dir: string              # working directory
  path: string             # base path for creation
  php_version: string      # e.g. "8.3"
  node: bool
  vars:                    # arbitrary key-value accessible in templates
    key: value
```

## Packages

```yaml
packages:
  composer:
    require: [string]
    require_dev: [string]
  npm:
    deps: [string]
    dev: [string]
  pip:
    deps: [string]
    dev: [string]
  brew:
    formulae: [string]
    casks: [string]
    taps: [string]
  go:
    tools: [string]
```

Shorthand: `composer: [string]` → normalized to `composer.require`.

## Git

```yaml
git:
  init: bool
  create_github_repo: bool
  visibility: public|private
  remote: string
  branch: string
```

## Stubs

```yaml
stubs:
  enabled: bool
  search_paths: [string]
  strict_match: bool
```

## Tools

```yaml
tools:
  install:
    homebrew: [string]
    apt: [string]
    custom: [string]
```

## Import

```yaml
imports:
  - path: string            # required — relative path to blueprint file
    alias: string           # optional — reference in step.call
    with:                   # optional — input overrides passed to imported blueprint
      key: value
```

Aliases must be unique within a blueprint. Duplicate aliases are a validation error.

## Hooks (Blueprint-level)

```yaml
hooks:
  before_run:
    - name: string
      cmd: string
      if: string            # template condition
  after_run:
    - name: string
      cmd: string
      if: string
  on_error:
    - name: string
      cmd: string
      if: string
```

## Steps (Sections)

```yaml
steps:
  - section: "Bootstrap"    # required — section label
    tasks:                  # required — at least one task
      - name: string        # required
        cmd: string         # shell command (required unless call is set)
        run: string         # alias for cmd (compat)
        call: string        # invoke imported blueprint by alias (mutually exclusive with cmd)
        if: string          # condition template (truthy string or shell exit 0)
        condition: string   # alias for if (v0.2 compat)
        dir: string
        env:
          KEY: value
        retry: int          # default 0
        retry_delay_seconds: int
        timeout_seconds: int
        continue_on_error: bool
        enabled: bool       # default true — false skips the task entirely
        with:               # key-value params passed when using call
          key: value
        on_success:         # per-task hooks on success
          - type: cmd|error|step|blueprint|call
            value: string
        on_fail:            # per-task hooks on failure
          - type: cmd|error|step|blueprint|call
            value: string
```

### Hook Types (per-task on_success/on_fail)

| Type | Behavior |
|---|---|
| `cmd` | Run a shell command |
| `error` | Emit an error message and halt (unless continue_on_error) |
| `step` | Jump to a named step within this blueprint |
| `blueprint` | Load and execute an external blueprint file (relative path) |
| `call` | Invoke an imported blueprint by alias |

## Template Engine

Go `text/template` with the following context and functions.

### Context

```
{{ .inputs.name }}
{{ .env.KEY }}
{{ .project.name }}, .project.type, .project.dir, .project.path, .project.php_version, .project.node, .project.vars.KEY
{{ .packages.composer }}, .packages.composerDev, .packages.npm, .packages.npmDev
{{ .git.init }}, .git.create_github_repo, ...
{{ .stubs.enabled }}, .stubs.search_paths, ...
{{ .blueprint.name }}, .blueprint.slug, .blueprint.title, .blueprint.version, .blueprint.path
{{ .workspace.id }}, .workspace.blueprint_dir, .workspace.root
```

### Functions

| Function | Signature |
|---|---|
| `upper` | `upper s string → string` |
| `lower` | `lower s string → string` |
| `trim` | `trim s string → string` |
| `replace` | `replace old new s string → string` |
| `split` | `split sep s string → []string` |
| `join` | `join sep []string → string` |
| `basename` | `basename path string → string` |
| `dirname` | `dirname path string → string` |
| `ext` | `ext path string → string` |
| `env` | `env key [default] → string` |
| `readFile` | `readFile path → string` (max 1MB) |
| `default` | `default def val → any` |
| `ternary` | `ternary bool ifTrue ifFalse → any` |
| `json` | `json val → string` (JSON-encode a value) |

## Validation Rules

1. `version` defaults to `"0.4"` if absent
2. `blueprint.name` or `blueprint.slug` required
3. `steps` must have at least one section with at least one task
4. Input names: pattern `^[a-zA-Z][a-zA-Z0-9_\-]*$`, unique within blueprint
5. Input types: `string`, `number`, `boolean`, `array`
6. Import aliases must be unique
7. Each task must have `cmd`/`run` OR `call` (not both, not neither)
8. `retry`, `retry_delay_seconds`, `timeout_seconds` must be >= 0
9. `if`/`condition` template renders to `"true"/"false"` string OR a shell command (exit 0 = true)

## Compatibility

- v0.2 blueprints with `condition:` field → accepted (alias for `if:`)
- v0.2 `retryDelay:` → accepted (alias for `retry_delay_seconds:`)
- v0.2 `continueOnError:` → accepted (alias for `continue_on_error:`)
- v0.2 `composer: [string]` shorthand → normalized

## Reference Example

See `../../reference-only/nanite-spec-v0.2/reference-blueprint.yaml` for a v0.2 example.
v0.4 example blueprints will be added during Sprint D (`hadron/examples/`).
