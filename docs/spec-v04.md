# Hadron Blueprint Spec v0.4

A blueprint is a YAML file that describes a sequence of tasks organized into sections.

## Minimal Example

```yaml
version: "0.4"

blueprint:
  name: hello
  title: Hello Hadron
  description: A minimal blueprint.
  author: Your Name
  tags: [example]

steps:
  - section: Greet
    tasks:
      - name: say-hello
        cmd: echo "Hello from Hadron!"
```

---

## Top-Level Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `version` | string | yes | Must be `"0.4"` |
| `blueprint` | object | yes | Blueprint metadata |
| `inputs` | list | no | Typed input definitions |
| `steps` | list | yes | Ordered list of sections |
| `imports` | list | no | Blueprint aliases for `call:` tasks |
| `hooks` | object | no | Blueprint-level lifecycle hooks |

---

## `blueprint` Block

```yaml
blueprint:
  name: my-app          # machine-readable identifier (required)
  title: My App         # human-readable title
  description: Sets up my app.
  author: Hollis Labs
  tags: [setup, app]
```

---

## `inputs` Block

Inputs are declared in the `inputs` list and referenced via `{{ .inputs.<name> }}` in task commands.

```yaml
inputs:
  - name: app_name
    label: Application Name
    description: Name of the application.
    type: string
    required: true
    pattern: "^[a-zA-Z][a-zA-Z0-9-]*$"
    min_length: 2
    max_length: 50
    prompt: "Application name?"

  - name: worker_count
    type: number
    default: 3
    min: 1
    max: 32

  - name: enable_debug
    type: boolean
    default: false

  - name: environment
    type: string
    default: development
    enum: [development, staging, production]

  - name: tags
    type: array
    items_type: string
```

### Input Types

| Type | Description | Extra fields |
|---|---|---|
| `string` | Text value | `pattern`, `min_length`, `max_length`, `enum` |
| `number` | Integer or float | `min`, `max` |
| `boolean` | `true` / `false` | — |
| `array` | List of values | `items_type` |

---

## `steps` Block

Steps is an ordered list of sections. Each section has a name and a list of tasks.

```yaml
steps:
  - section: Build
    tasks:
      - name: compile
        cmd: go build ./...

      - name: test
        cmd: go test ./...
        timeout_seconds: 120
        retry: 2
        retry_delay_seconds: 5
        continue_on_error: false
```

### Task Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Unique task identifier within section |
| `cmd` | string | — | Shell command to run (alias: `run`) |
| `call` | string | — | Import alias or path to sub-blueprint |
| `if` | string | — | Condition expression; skip task if falsy |
| `enabled` | bool | true | Set false to permanently skip task |
| `dir` | string | — | Working directory for the command |
| `env` | map | — | Additional environment variables |
| `timeout_seconds` | int | settings default | Max run time before timeout |
| `retry` | int | 0 | Number of retry attempts on failure |
| `retry_delay_seconds` | int | 0 | Delay between retries |
| `continue_on_error` | bool | false | Continue to next task on failure |
| `on_success` | list | — | Action hooks run on task success |
| `on_fail` | list | — | Action hooks run on task failure |

---

## Template Functions

Templates use Go `text/template` syntax. The context object has:

| Expression | Description |
|---|---|
| `{{ .inputs.<name> }}` | Input value |
| `{{ .project.path }}` | Absolute path of the blueprint file |
| `{{ .project.dir }}` | Directory of the blueprint file |
| `{{ .workspace.id }}` | Current workspace ID |

---

## `if:` Conditions

The `if` field is evaluated as a shell-like condition after template rendering.
Standard truthy values: `1`, `t`, `true`, `yes`, `y`, `on`.
Standard falsy values: `0`, `f`, `false`, `no`, `n`, `off`.

```yaml
- name: warn-prod
  if: "test '{{ .inputs.environment }}' = 'production'"
  cmd: echo "WARNING: targeting production"
```

---

## `imports` Block

Import aliases allow a task to `call:` a sub-blueprint:

```yaml
imports:
  - alias: setup
    path: ./setup.yaml
    with:
      app_name: "{{ .inputs.app_name }}"

steps:
  - section: Setup
    tasks:
      - name: run-setup
        call: setup
```

---

## `hooks` Block

Blueprint-level hooks run before/after the entire blueprint or on error.

```yaml
hooks:
  before_run:
    - name: pre-check
      cmd: echo "starting"
  after_run:
    - name: notify
      cmd: echo "finished"
  on_error:
    - name: alert
      cmd: echo "something went wrong"
```

### Per-Task Hooks (`on_success` / `on_fail`)

```yaml
tasks:
  - name: deploy
    cmd: ./deploy.sh
    on_success:
      - type: cmd
        value: echo "deploy succeeded"
    on_fail:
      - type: error
        value: "deployment failed — check logs"
      - type: cmd
        value: ./rollback.sh
```

Hook types: `cmd`, `error`, `step`, `blueprint`, `call`.

---

## Validation Rules

- `version` must be `"0.4"` (or omitted — defaults to `"0.4"`)
- `blueprint.name` is required
- Each task must have at least one of: `cmd`, `run`, `call`
- Input `pattern` must be a valid Go regex
- `enum` values must be non-empty strings
- `min_length` ≤ `max_length` (when both specified)
- `min` ≤ `max` (when both specified)

---

## Pipeline Spec

A pipeline chains multiple blueprints into ordered stages.

```yaml
meta:
  name: my-pipeline

stop_on_fail: true

stages:
  - name: setup
    blueprint_path: setup.yaml
  - name: test
    blueprint_path: test.yaml
```

| Field | Type | Default | Description |
|---|---|---|---|
| `meta.name` | string | required | Pipeline identifier |
| `stop_on_fail` | bool | false | Stop on first stage failure |
| `stages` | list | required | Ordered stage list |
| `stages[].name` | string | required | Stage name |
| `stages[].blueprint_path` | string | required | Path to blueprint file |
