import{G as e,U as t,V as n,n as r,q as i,t as a}from"./button-C_7cUJsy.js";import{t as o}from"./folder-plus-ocD-qMO2.js";import{B as s,c,d as l,f as u,it as d,k as f,o as p,s as m,u as h}from"./index-BboCKVAR.js";var g=i(e(),1),_=n(),v=[{key:`general`,label:`General`},{key:`blueprints`,label:`Blueprints`},{key:`pipelines`,label:`Pipelines`},{key:`examples`,label:`Examples`}],y=[{kind:`blueprint`,title:`Hello World`,description:`Minimal blueprint that echoes a greeting. Great starting point.`,filename:`hello-world.yaml`,yaml:`version: "0.4"
blueprint:
  name: hello-world
  slug: hello-world
  title: Hello World
  description: A minimal blueprint that prints a greeting

inputs:
  - name: name
    label: Your Name
    type: string
    required: true
    default: "World"

steps:
  - section: greeting
    tasks:
      - name: say-hello
        cmd: echo "Hello, {{ .inputs.name }}!"
`},{kind:`blueprint`,title:`Git Repo Setup`,description:`Initialize a git repository with a README and first commit.`,filename:`git-repo-setup.yaml`,yaml:`version: "0.4"
blueprint:
  name: git-repo-setup
  slug: git-repo-setup
  title: Git Repository Setup
  description: Initialize a new git repo with README and initial commit

inputs:
  - name: project_name
    label: Project Name
    type: string
    required: true
  - name: description
    label: Description
    type: string
    default: "A new project"

steps:
  - section: init
    tasks:
      - name: create-dir
        cmd: mkdir -p "{{ .inputs.project_name }}"
      - name: init-git
        cmd: git init
        dir: "{{ .inputs.project_name }}"
      - name: create-readme
        cmd: |
          cat > README.md << 'HEREDOC'
          # {{ .inputs.project_name }}

          {{ .inputs.description }}
          HEREDOC
        dir: "{{ .inputs.project_name }}"
      - name: initial-commit
        cmd: git add -A && git commit -m "Initial commit"
        dir: "{{ .inputs.project_name }}"
`},{kind:`blueprint`,title:`Node.js Project Scaffold`,description:`Create a new Node.js project with package.json and basic structure.`,filename:`node-scaffold.yaml`,yaml:`version: "0.4"
blueprint:
  name: node-scaffold
  slug: node-scaffold
  title: Node.js Project Scaffold
  description: Scaffold a Node.js project with package.json, src/, and test/

inputs:
  - name: project_name
    label: Project Name
    type: string
    required: true
  - name: use_typescript
    label: Use TypeScript?
    type: boolean
    default: "true"

steps:
  - section: scaffold
    tasks:
      - name: create-dirs
        cmd: mkdir -p src test
        dir: "{{ .inputs.project_name }}"
      - name: init-npm
        cmd: npm init -y
        dir: "{{ .inputs.project_name }}"
      - name: install-typescript
        cmd: npm install --save-dev typescript @types/node ts-node
        dir: "{{ .inputs.project_name }}"
        if: "{{ .inputs.use_typescript }}"
      - name: create-index
        cmd: |
          echo 'console.log("Hello from {{ .inputs.project_name }}!");' > src/index.ts
        dir: "{{ .inputs.project_name }}"
`},{kind:`blueprint`,title:`Database Backup`,description:`Backup a SQLite database with timestamped filename and optional compression.`,filename:`db-backup.yaml`,yaml:`version: "0.4"
blueprint:
  name: db-backup
  slug: db-backup
  title: Database Backup
  description: Backup a SQLite database with optional gzip compression

inputs:
  - name: db_path
    label: Database Path
    type: string
    required: true
    default: "./data.db"
  - name: backup_dir
    label: Backup Directory
    type: string
    default: "./backups"
  - name: compress
    label: Compress with gzip?
    type: boolean
    default: "true"

steps:
  - section: backup
    tasks:
      - name: create-backup-dir
        cmd: mkdir -p "{{ .inputs.backup_dir }}"
      - name: copy-database
        cmd: |
          TIMESTAMP=$(date +%Y%m%d_%H%M%S)
          cp "{{ .inputs.db_path }}" "{{ .inputs.backup_dir }}/backup_\${TIMESTAMP}.db"
      - name: compress-backup
        cmd: |
          LATEST=$(ls -t "{{ .inputs.backup_dir }}"/backup_*.db | head -1)
          gzip "$LATEST"
        if: "{{ .inputs.compress }}"
      - name: report
        cmd: |
          echo "Backup complete:"
          ls -lh "{{ .inputs.backup_dir }}"
`},{kind:`blueprint`,title:`Deploy Script`,description:`Multi-step deployment with build, test, and deploy stages.`,filename:`deploy.yaml`,yaml:`version: "0.4"
blueprint:
  name: deploy
  slug: deploy
  title: Deploy Script
  description: Build, test, and deploy with rollback on failure

inputs:
  - name: environment
    label: Environment
    type: string
    required: true
    enum: ["staging", "production"]
  - name: skip_tests
    label: Skip Tests?
    type: boolean
    default: "false"

hooks:
  on_error:
    - name: notify-failure
      cmd: echo "Deployment to {{ .inputs.environment }} FAILED"

steps:
  - section: build
    tasks:
      - name: install-deps
        cmd: npm ci
      - name: build
        cmd: npm run build

  - section: test
    tasks:
      - name: run-tests
        cmd: npm test
        if: '{{ not .inputs.skip_tests }}'

  - section: deploy
    tasks:
      - name: deploy
        cmd: echo "Deploying to {{ .inputs.environment }}..."
        timeout_seconds: 120
      - name: health-check
        cmd: echo "Health check passed"
        retry: 3
        retry_delay_seconds: 5
`},{kind:`pipeline`,title:`CI Pipeline`,description:`Lint, test, and build — a standard continuous integration flow.`,filename:`ci-pipeline.yaml`,yaml:`meta:
  name: ci-pipeline
stop_on_fail: true
stages:
  - name: lint
    blueprint_path: ./lint-check.yaml
  - name: test
    blueprint_path: ./test-suite.yaml
  - name: build
    blueprint_path: ./build-frontend.yaml
`},{kind:`pipeline`,title:`Full Stack Deploy`,description:`Build backend, build frontend, run tests, then deploy — all sequential.`,filename:`full-deploy.yaml`,yaml:`meta:
  name: full-stack-deploy
stop_on_fail: true
stages:
  - name: build-backend
    blueprint_path: ./build-backend.yaml
  - name: build-frontend
    blueprint_path: ./build-frontend.yaml
  - name: run-tests
    blueprint_path: ./test-suite.yaml
  - name: deploy
    blueprint_path: ./deploy.yaml
`},{kind:`pipeline`,title:`Nightly Build & Backup`,description:`Runs a full build then backs up the database. Continue even if build fails.`,filename:`nightly-build.yaml`,yaml:`meta:
  name: nightly-build-and-backup
stop_on_fail: false
stages:
  - name: full-build
    blueprint_path: ./build-frontend.yaml
  - name: run-tests
    blueprint_path: ./test-suite.yaml
  - name: backup-db
    blueprint_path: ./db-backup.yaml
`},{kind:`pipeline`,title:`Multi-Env Deploy`,description:`Deploy to staging first, then production. Stops if staging fails.`,filename:`multi-env-deploy.yaml`,yaml:`meta:
  name: multi-env-deploy
stop_on_fail: true
stages:
  - name: deploy-staging
    blueprint_path: ./deploy.yaml
    inputs:
      environment: staging
  - name: deploy-production
    blueprint_path: ./deploy.yaml
    inputs:
      environment: production
`}];async function b(e){let n=await s(`defaultBlueprintDir`);if(n||=await s(`lastBlueprintDir`),!(!n&&(n=await d(),!n)))try{await f(n,e.filename,e.yaml),t.success(`Added ${e.filename} to ${n.split(`/`).slice(-2).join(`/`)}`)}catch(e){t.error(`Failed to add file: ${e}`)}}function x(){let[e,n]=(0,g.useState)(`general`),[i,s]=(0,g.useState)(null),[d,f]=(0,g.useState)(`all`),x=y.filter(e=>d===`all`||e.kind===d);return(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`div`,{className:`flex items-center justify-between mb-6`,children:(0,_.jsx)(`span`,{className:`text-xl font-semibold text-foreground tracking-tight`,children:`Help`})}),(0,_.jsx)(`div`,{className:`flex gap-1 mb-4`,children:v.map(t=>(0,_.jsx)(a,{variant:e===t.key?`outline`:`ghost`,size:`sm`,onClick:()=>n(t.key),children:t.label},t.key))}),e===`general`&&(0,_.jsxs)(`div`,{className:`flex flex-col gap-5 max-w-4xl`,children:[(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`About`}),(0,_.jsxs)(`div`,{className:`text-sm mb-1`,children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`HADRON`}),(0,_.jsx)(`span`,{className:`text-muted-foreground ml-2`,children:`by Hollis Labs`})]}),(0,_.jsx)(`div`,{className:`text-sm text-muted-foreground leading-normal`,children:`A local-first, agent-first blueprint automation runner. Create, inspect, and run YAML blueprints that orchestrate multi-step workflows with inputs, conditions, retries, and more.`})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Quick Start`}),(0,_.jsxs)(`ol`,{className:`text-sm leading-relaxed pl-5 text-foreground`,children:[(0,_.jsxs)(`li`,{children:[`Go to `,(0,_.jsx)(`strong`,{children:`Blueprints`}),` and click `,(0,_.jsx)(`strong`,{children:`Open Folder`}),` to select a directory containing .yaml blueprints`]}),(0,_.jsx)(`li`,{children:`Click a blueprint name to view its details, inputs, and step timeline`}),(0,_.jsxs)(`li`,{children:[`Click `,(0,_.jsx)(`strong`,{children:`Run`}),` to execute a blueprint (fill in inputs if required)`]}),(0,_.jsxs)(`li`,{children:[`Check the `,(0,_.jsx)(`strong`,{children:`Run Log`}),` to monitor execution and view results`]}),(0,_.jsxs)(`li`,{children:[`Use `,(0,_.jsx)(`strong`,{children:`New Blueprint`}),` to create a blueprint from scratch using the wizard`]}),(0,_.jsxs)(`li`,{children:[`Set up recurring runs in `,(0,_.jsx)(`strong`,{children:`Schedules`}),` with cron expressions`]}),(0,_.jsxs)(`li`,{children:[`Chain blueprints together with `,(0,_.jsx)(`strong`,{children:`Pipelines`}),` for multi-step workflows`]})]})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Keyboard Shortcuts`}),(0,_.jsx)(`table`,{className:`w-full border-collapse`,children:(0,_.jsx)(`tbody`,{children:[[`Esc`,`Close modal / go back from detail pages`],[`R`,`Refresh current page data`],[`N`,`New blueprint (on Blueprints page)`],[`?`,`Open this Help page`],[`↑ / ↓`,`Navigate rows in lists (Blueprints, Run Log)`],[`Enter`,`Open selected item in list`],[`Space`,`Toggle selection (Blueprints page)`]].map(([e,t])=>(0,_.jsxs)(`tr`,{children:[(0,_.jsx)(`td`,{className:`py-1 w-[120px]`,children:(0,_.jsx)(`kbd`,{className:`bg-muted border border-border rounded px-2 py-0.5 text-sm font-mono`,children:e})}),(0,_.jsx)(`td`,{className:`py-1 text-sm text-muted-foreground`,children:t})]},e))})})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Pages Overview`}),(0,_.jsxs)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:[(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`Operations`}),` — Default landing view for run stats, activity, and blueprint health`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`Blueprints`}),` — Browse, create, edit, run, and manage blueprint YAML files`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`Pipelines`}),` — Chain blueprints into multi-stage workflows`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`Run Log`}),` — History of all blueprint runs with status, duration, and details`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`Schedules`}),` — Cron-based recurring runs and one-time scheduled executions`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`Telemetry`}),` — JSONL activity logs per run for debugging and auditing`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`Settings`}),` — Execution limits, safety controls, telemetry retention`]})]})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Resources`}),(0,_.jsx)(`div`,{className:`flex flex-col gap-1.5`,children:[[`Data directory`,`~/.hadron/`],[`Settings`,`~/.hadron/settings.json`],[`Run logs`,`~/.hadron/logs/runs/`],[`Database`,`~/.hadron/state/hadron.db`],[`Archive`,`~/.hadron/archive/`],[`Preferences`,`~/.hadron/preferences.json`]].map(([e,t])=>(0,_.jsxs)(`div`,{className:`text-sm text-muted-foreground`,children:[e,`: `,(0,_.jsx)(`span`,{className:`font-mono text-blue-400`,children:t})]},e))})]})]}),e===`blueprints`&&(0,_.jsxs)(`div`,{className:`flex flex-col gap-5 max-w-4xl`,children:[(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`What are Blueprints?`}),(0,_.jsx)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:`Blueprints are YAML files that define multi-step automation workflows. Each blueprint can declare inputs, environment variables, conditional steps, retries, and hooks. Hadron executes them locally using Go's text/template engine for dynamic values.`})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Blueprint Schema (v0.4)`}),(0,_.jsxs)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:[(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`version:`}),` Schema version (currently "0.4")`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`blueprint:`}),` name, slug, title, description, author, license, tags, homepage`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`project:`}),` type, name, dir, path, php_version, node, vars`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`env:`}),` key-value environment variables`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`inputs:`}),` name, label, type (string|number|boolean|array), required, default, enum, pattern`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`packages:`}),` npm, composer, pip, brew, go`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`steps:`}),` sections with tasks (name, cmd, call, if, retry, timeout, dir, env)`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`hooks:`}),` before_run, after_run, on_error`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`imports:`}),` path, alias, with`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`stubs:`}),` enabled, search_paths, strict_match`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`git:`}),` init, create_github_repo, visibility, remote, branch`]})]})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Template Variables`}),(0,_.jsxs)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:[(0,_.jsx)(`div`,{className:`mb-2 text-foreground`,children:`Blueprints use Go text/template syntax. Available variables:`}),(0,_.jsx)(`table`,{className:`w-full border-collapse`,children:(0,_.jsx)(`tbody`,{children:[[`{{ .inputs.name }}`,`User-provided input value by name`],[`{{ .env.KEY }}`,`Environment variable value`],[`{{ .project.name }}`,`Project name from blueprint config`],[`{{ .project.root }}`,`Resolved project root directory`],[`{{ .project.dir }}`,`Project directory (template-rendered)`],[`{{ .blueprint.name }}`,`Blueprint name from metadata`],[`{{ .blueprint.slug }}`,`Blueprint slug from metadata`],[`{{ .workspace.id }}`,`Current workspace identifier`]].map(([e,t])=>(0,_.jsxs)(`tr`,{children:[(0,_.jsx)(`td`,{className:`py-1 font-mono text-sm text-blue-400 whitespace-nowrap w-[220px]`,children:e}),(0,_.jsx)(`td`,{className:`py-1 text-sm`,children:t})]},e))})})]})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Template Functions`}),(0,_.jsx)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:(0,_.jsx)(`div`,{className:`grid grid-cols-2 gap-x-6 gap-y-1`,children:[[`upper`,`Convert to uppercase`],[`lower`,`Convert to lowercase`],[`trim`,`Remove whitespace`],[`replace`,`Replace substring`],[`split`,`Split by separator`],[`join`,`Join with separator`],[`basename`,`Filename from path`],[`dirname`,`Directory from path`],[`ext`,`File extension`],[`env`,`Read env variable`],[`readFile`,`Read file contents`],[`default`,`Fallback if empty`],[`ternary`,`Conditional value`],[`json`,`Marshal to JSON`]].map(([e,t])=>(0,_.jsxs)(`div`,{className:`flex gap-2 items-baseline`,children:[(0,_.jsx)(`span`,{className:`font-mono text-sm text-blue-400`,children:e}),(0,_.jsx)(`span`,{className:`text-sm`,children:t})]},e))})})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Task Options`}),(0,_.jsxs)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:[(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`cmd`}),` — Shell command to execute`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`call`}),` — Call another blueprint by path`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`if`}),` — Conditional expression (template syntax)`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`dir`}),` — Working directory for the command`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`env`}),` — Per-task environment variables`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`retry`}),` — Number of retry attempts on failure`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`retry_delay_seconds`}),` — Delay between retries`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`timeout_seconds`}),` — Maximum execution time`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`continue_on_error`}),` — Don't halt on failure`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`on_success / on_fail`}),` — Hooks triggered by outcome`]})]})]})]}),e===`pipelines`&&(0,_.jsxs)(`div`,{className:`flex flex-col gap-5 max-w-4xl`,children:[(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`What are Pipelines?`}),(0,_.jsx)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:`Pipelines chain multiple blueprints together into a sequential workflow. Each stage runs a blueprint, and (by default) the pipeline stops on the first failure. Use pipelines to orchestrate multi-step deployments, build chains, or any workflow that requires ordered execution.`})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Pipeline Schema`}),(0,_.jsxs)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:[(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`meta.name`}),` — Pipeline display name`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`stop_on_fail`}),` — Stop pipeline if a stage fails (default: true)`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`stages[]`}),` — Ordered list of stages to execute:`]}),(0,_.jsxs)(`div`,{className:`pl-4`,children:[(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`name`}),` — Stage identifier (required)`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`blueprint_path`}),` — Path to blueprint YAML (required)`]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`inputs`}),` — Key-value inputs passed to the blueprint`]})]}),(0,_.jsxs)(`div`,{children:[(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`inputs`}),` — Global inputs inherited by all stages`]})]})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Example Pipeline`}),(0,_.jsx)(`pre`,{className:`text-sm leading-normal text-foreground bg-muted p-3 rounded border border-border overflow-auto font-mono whitespace-pre m-0`,children:`meta:
  name: full-stack-deploy
stop_on_fail: true
stages:
  - name: build-backend
    blueprint_path: ./build-backend.yaml
  - name: build-frontend
    blueprint_path: ./build-frontend.yaml
  - name: run-tests
    blueprint_path: ./test-suite.yaml
  - name: deploy
    blueprint_path: ./deploy.yaml`})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`How to Use`}),(0,_.jsxs)(`ol`,{className:`text-sm leading-relaxed pl-5 text-foreground`,children:[(0,_.jsxs)(`li`,{children:[`Navigate to the `,(0,_.jsx)(`strong`,{children:`Pipelines`}),` page from the sidebar`]}),(0,_.jsxs)(`li`,{children:[`Click `,(0,_.jsx)(`strong`,{children:`Open Folder`}),` to select a directory containing pipeline YAML files`]}),(0,_.jsxs)(`li`,{children:[`Click `,(0,_.jsx)(`strong`,{children:`New Pipeline`}),` to create a pipeline with the visual editor`]}),(0,_.jsxs)(`li`,{children:[`Use the `,(0,_.jsx)(`strong`,{children:`Edit`}),` and `,(0,_.jsx)(`strong`,{children:`Delete`}),` buttons to manage existing pipelines`]}),(0,_.jsxs)(`li`,{children:[`Click `,(0,_.jsx)(`strong`,{children:`Run`}),` next to a pipeline file to start execution`]}),(0,_.jsxs)(`li`,{children:[`Monitor progress in the `,(0,_.jsx)(`strong`,{children:`Recent Pipeline Runs`}),` section below`]}),(0,_.jsxs)(`li`,{children:[`Click `,(0,_.jsx)(`strong`,{children:`Stages`}),` on a run to see individual stage status and jump to run details`]})]})]}),(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card p-4`,children:[(0,_.jsx)(`div`,{className:`text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2`,children:`Execution Behavior`}),(0,_.jsxs)(`div`,{className:`text-sm text-muted-foreground leading-relaxed`,children:[(0,_.jsxs)(`div`,{children:[`Stages execute `,(0,_.jsx)(`strong`,{className:`text-foreground`,children:`sequentially`}),` — each stage waits for the previous one to complete.`]}),(0,_.jsxs)(`div`,{className:`mt-1`,children:[`With `,(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`stop_on_fail: true`}),` (default), the pipeline halts when any stage fails.`]}),(0,_.jsxs)(`div`,{className:`mt-1`,children:[`With `,(0,_.jsx)(`strong`,{className:`text-blue-400`,children:`stop_on_fail: false`}),`, all stages run regardless of failures.`]}),(0,_.jsxs)(`div`,{className:`mt-1`,children:[`Each stage creates a separate `,(0,_.jsx)(`strong`,{className:`text-foreground`,children:`blueprint run`}),` visible in the Run Log.`]}),(0,_.jsxs)(`div`,{className:`mt-1`,children:[`Default stage timeout: `,(0,_.jsx)(`strong`,{className:`text-foreground`,children:`60 seconds`}),`. Blueprint-level timeouts override this.`]})]})]})]}),e===`examples`&&(0,_.jsxs)(`div`,{className:`flex flex-col gap-4 max-w-4xl`,children:[(0,_.jsxs)(`div`,{className:`text-sm text-muted-foreground leading-normal`,children:[`Sample blueprints and pipelines for common use cases. `,(0,_.jsx)(`strong`,{className:`text-foreground`,children:`View`}),` to see the full YAML, `,(0,_.jsx)(`strong`,{className:`text-foreground`,children:`Copy`}),` to clipboard, or `,(0,_.jsx)(`strong`,{className:`text-foreground`,children:`Add`}),` to save directly to your blueprints folder.`]}),(0,_.jsx)(`div`,{className:`flex gap-1`,children:[`all`,`blueprint`,`pipeline`].map(e=>(0,_.jsx)(a,{variant:d===e?`outline`:`ghost`,size:`sm`,onClick:()=>f(e),children:e===`all`?`All`:e===`blueprint`?`Blueprints`:`Pipelines`},e))}),x.map(e=>(0,_.jsxs)(`div`,{className:`rounded-lg border border-border bg-card px-4 py-3`,children:[(0,_.jsxs)(`div`,{className:`flex items-center gap-2 mb-1`,children:[(0,_.jsx)(`span`,{className:r(`text-xs font-bold uppercase tracking-wide px-1.5 py-0.5 rounded`,(e.kind,`bg-blue-400/15 text-blue-400`)),children:e.kind}),(0,_.jsx)(`div`,{className:`font-semibold text-sm text-foreground flex-1`,children:e.title}),(0,_.jsxs)(`div`,{className:`flex gap-1`,children:[(0,_.jsx)(a,{variant:`ghost`,size:`xs`,onClick:()=>s(e),children:`View`}),(0,_.jsx)(a,{variant:`ghost`,size:`xs`,onClick:()=>{navigator.clipboard.writeText(e.yaml),t.success(`Copied to clipboard`)},children:`Copy`}),(0,_.jsxs)(a,{size:`xs`,onClick:()=>b(e),children:[(0,_.jsx)(o,{size:11}),` Add`]})]})]}),(0,_.jsx)(`div`,{className:`text-sm text-muted-foreground`,children:e.description}),(0,_.jsx)(`div`,{className:`text-xs font-mono text-blue-400 mt-0.5`,children:e.filename})]},e.filename))]}),(0,_.jsx)(p,{open:!!i,onOpenChange:e=>{e||s(null)},children:(0,_.jsx)(c,{className:`sm:max-w-[650px] w-full`,children:i&&(0,_.jsxs)(_.Fragment,{children:[(0,_.jsx)(l,{children:(0,_.jsxs)(`div`,{className:`flex items-center justify-between`,children:[(0,_.jsxs)(`div`,{children:[(0,_.jsxs)(`div`,{className:`flex items-center gap-2`,children:[(0,_.jsx)(`span`,{className:r(`text-xs font-bold uppercase tracking-wide px-1.5 py-0.5 rounded`,(i.kind,`bg-blue-400/15 text-blue-400`)),children:i.kind}),(0,_.jsx)(u,{className:`font-semibold text-base`,children:i.title})]}),(0,_.jsx)(`div`,{className:`text-sm font-mono text-blue-400`,children:i.filename})]}),(0,_.jsxs)(`div`,{className:`flex gap-1`,children:[(0,_.jsx)(a,{variant:`ghost`,size:`sm`,onClick:()=>{navigator.clipboard.writeText(i.yaml),t.success(`Copied to clipboard`)},children:`Copy`}),(0,_.jsxs)(a,{size:`sm`,onClick:()=>b(i),children:[(0,_.jsx)(o,{size:11}),` Add to Blueprints`]})]})]})}),(0,_.jsx)(`pre`,{className:`text-sm leading-normal text-foreground bg-muted p-3 rounded border border-border overflow-auto font-mono whitespace-pre m-0 max-h-[60vh]`,children:i.yaml}),(0,_.jsx)(h,{children:(0,_.jsx)(m,{render:(0,_.jsx)(a,{variant:`ghost`}),children:`Close`})})]})})})]})}export{x as HelpPage};