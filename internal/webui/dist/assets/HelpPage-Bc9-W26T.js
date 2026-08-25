import{r as o,j as e,B as n,c as m,T as a,a8 as g,a9 as j,aa as f,ab as v,ac as y,aM as N,ae as p,Y as k,af as w}from"./index-CRdy992n.js";import{F as u}from"./folder-plus-B6ySVh7S.js";const _=[{key:"general",label:"General"},{key:"blueprints",label:"Blueprints"},{key:"pipelines",label:"Pipelines"},{key:"examples",label:"Examples"}],S=[{kind:"blueprint",title:"Hello World",description:"Minimal blueprint that echoes a greeting. Great starting point.",filename:"hello-world.yaml",yaml:`version: "0.4"
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
`},{kind:"blueprint",title:"Git Repo Setup",description:"Initialize a git repository with a README and first commit.",filename:"git-repo-setup.yaml",yaml:`version: "0.4"
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
`},{kind:"blueprint",title:"Node.js Project Scaffold",description:"Create a new Node.js project with package.json and basic structure.",filename:"node-scaffold.yaml",yaml:`version: "0.4"
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
`},{kind:"blueprint",title:"Database Backup",description:"Backup a SQLite database with timestamped filename and optional compression.",filename:"db-backup.yaml",yaml:`version: "0.4"
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
`},{kind:"blueprint",title:"Deploy Script",description:"Multi-step deployment with build, test, and deploy stages.",filename:"deploy.yaml",yaml:`version: "0.4"
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
`},{kind:"pipeline",title:"CI Pipeline",description:"Lint, test, and build — a standard continuous integration flow.",filename:"ci-pipeline.yaml",yaml:`meta:
  name: ci-pipeline
stop_on_fail: true
stages:
  - name: lint
    blueprint_path: ./lint-check.yaml
  - name: test
    blueprint_path: ./test-suite.yaml
  - name: build
    blueprint_path: ./build-frontend.yaml
`},{kind:"pipeline",title:"Full Stack Deploy",description:"Build backend, build frontend, run tests, then deploy — all sequential.",filename:"full-deploy.yaml",yaml:`meta:
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
`},{kind:"pipeline",title:"Nightly Build & Backup",description:"Runs a full build then backs up the database. Continue even if build fails.",filename:"nightly-build.yaml",yaml:`meta:
  name: nightly-build-and-backup
stop_on_fail: false
stages:
  - name: full-build
    blueprint_path: ./build-frontend.yaml
  - name: run-tests
    blueprint_path: ./test-suite.yaml
  - name: backup-db
    blueprint_path: ./db-backup.yaml
`},{kind:"pipeline",title:"Multi-Env Deploy",description:"Deploy to staging first, then production. Stops if staging fails.",filename:"multi-env-deploy.yaml",yaml:`meta:
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
`}];async function x(i){let r=await p("defaultBlueprintDir");if(r||(r=await p("lastBlueprintDir")),!(!r&&(r=await k(),!r)))try{await w(r,i.filename,i.yaml),a.success(`Added ${i.filename} to ${r.split("/").slice(-2).join("/")}`)}catch(s){a.error(`Failed to add file: ${s}`)}}function B(){const[i,r]=o.useState("general"),[s,c]=o.useState(null),[d,h]=o.useState("all"),b=S.filter(t=>d==="all"||t.kind===d);return e.jsxs("div",{children:[e.jsx("div",{className:"flex items-center justify-between mb-6",children:e.jsx("span",{className:"text-xl font-semibold text-foreground tracking-tight",children:"Help"})}),e.jsx("div",{className:"flex gap-1 mb-4",children:_.map(t=>e.jsx(n,{variant:i===t.key?"outline":"ghost",size:"sm",onClick:()=>r(t.key),children:t.label},t.key))}),i==="general"&&e.jsxs("div",{className:"flex flex-col gap-5 max-w-4xl",children:[e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"About"}),e.jsxs("div",{className:"text-sm mb-1",children:[e.jsx("strong",{className:"text-blue-400",children:"HADRON"}),e.jsx("span",{className:"text-muted-foreground ml-2",children:"by Hollis Labs"})]}),e.jsx("div",{className:"text-sm text-muted-foreground leading-normal",children:"A local-first, agent-first blueprint automation runner. Create, inspect, and run YAML blueprints that orchestrate multi-step workflows with inputs, conditions, retries, and more."})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Quick Start"}),e.jsxs("ol",{className:"text-sm leading-relaxed pl-5 text-foreground",children:[e.jsxs("li",{children:["Go to ",e.jsx("strong",{children:"Blueprints"})," and click ",e.jsx("strong",{children:"Open Folder"})," to select a directory containing .yaml blueprints"]}),e.jsx("li",{children:"Click a blueprint name to view its details, inputs, and step timeline"}),e.jsxs("li",{children:["Click ",e.jsx("strong",{children:"Run"})," to execute a blueprint (fill in inputs if required)"]}),e.jsxs("li",{children:["Check the ",e.jsx("strong",{children:"Run Log"})," to monitor execution and view results"]}),e.jsxs("li",{children:["Use ",e.jsx("strong",{children:"New Blueprint"})," to create a blueprint from scratch using the wizard"]}),e.jsxs("li",{children:["Set up recurring runs in ",e.jsx("strong",{children:"Schedules"})," with cron expressions"]}),e.jsxs("li",{children:["Chain blueprints together with ",e.jsx("strong",{children:"Pipelines"})," for multi-step workflows"]})]})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Keyboard Shortcuts"}),e.jsx("table",{className:"w-full border-collapse",children:e.jsx("tbody",{children:[["Esc","Close modal / go back from detail pages"],["R","Refresh current page data"],["N","New blueprint (on Blueprints page)"],["?","Open this Help page"],["↑ / ↓","Navigate rows in lists (Blueprints, Run Log)"],["Enter","Open selected item in list"],["Space","Toggle selection (Blueprints page)"]].map(([t,l])=>e.jsxs("tr",{children:[e.jsx("td",{className:"py-1 w-[120px]",children:e.jsx("kbd",{className:"bg-muted border border-border rounded px-2 py-0.5 text-sm font-mono",children:t})}),e.jsx("td",{className:"py-1 text-sm text-muted-foreground",children:l})]},t))})})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Pages Overview"}),e.jsxs("div",{className:"text-sm text-muted-foreground leading-relaxed",children:[e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"Operations"})," — Default landing view for run stats, activity, and blueprint health"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"Blueprints"})," — Browse, create, edit, run, and manage blueprint YAML files"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"Pipelines"})," — Chain blueprints into multi-stage workflows"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"Run Log"})," — History of all blueprint runs with status, duration, and details"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"Schedules"})," — Cron-based recurring runs and one-time scheduled executions"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"Telemetry"})," — JSONL activity logs per run for debugging and auditing"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"Settings"})," — Execution limits, safety controls, telemetry retention"]})]})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Resources"}),e.jsx("div",{className:"flex flex-col gap-1.5",children:[["Data directory","~/.hadron/"],["Settings","~/.hadron/settings.json"],["Run logs","~/.hadron/logs/runs/"],["Database","~/.hadron/state/hadron.db"],["Archive","~/.hadron/archive/"],["Preferences","~/.hadron/preferences.json"]].map(([t,l])=>e.jsxs("div",{className:"text-sm text-muted-foreground",children:[t,": ",e.jsx("span",{className:"font-mono text-blue-400",children:l})]},t))})]})]}),i==="blueprints"&&e.jsxs("div",{className:"flex flex-col gap-5 max-w-4xl",children:[e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"What are Blueprints?"}),e.jsx("div",{className:"text-sm text-muted-foreground leading-relaxed",children:"Blueprints are YAML files that define multi-step automation workflows. Each blueprint can declare inputs, environment variables, conditional steps, retries, and hooks. Hadron executes them locally using Go's text/template engine for dynamic values."})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Blueprint Schema (v0.4)"}),e.jsxs("div",{className:"text-sm text-muted-foreground leading-relaxed",children:[e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"version:"}),' Schema version (currently "0.4")']}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"blueprint:"})," name, slug, title, description, author, license, tags, homepage"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"project:"})," type, name, dir, path, php_version, node, vars"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"env:"})," key-value environment variables"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"inputs:"})," name, label, type (string|number|boolean|array), required, default, enum, pattern"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"packages:"})," npm, composer, pip, brew, go"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"steps:"})," sections with tasks (name, cmd, call, if, retry, timeout, dir, env)"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"hooks:"})," before_run, after_run, on_error"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"imports:"})," path, alias, with"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"stubs:"})," enabled, search_paths, strict_match"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"git:"})," init, create_github_repo, visibility, remote, branch"]})]})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Template Variables"}),e.jsxs("div",{className:"text-sm text-muted-foreground leading-relaxed",children:[e.jsx("div",{className:"mb-2 text-foreground",children:"Blueprints use Go text/template syntax. Available variables:"}),e.jsx("table",{className:"w-full border-collapse",children:e.jsx("tbody",{children:[["{{ .inputs.name }}","User-provided input value by name"],["{{ .env.KEY }}","Environment variable value"],["{{ .project.name }}","Project name from blueprint config"],["{{ .project.root }}","Resolved project root directory"],["{{ .project.dir }}","Project directory (template-rendered)"],["{{ .blueprint.name }}","Blueprint name from metadata"],["{{ .blueprint.slug }}","Blueprint slug from metadata"],["{{ .workspace.id }}","Current workspace identifier"]].map(([t,l])=>e.jsxs("tr",{children:[e.jsx("td",{className:"py-1 font-mono text-sm text-blue-400 whitespace-nowrap w-[220px]",children:t}),e.jsx("td",{className:"py-1 text-sm",children:l})]},t))})})]})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Template Functions"}),e.jsx("div",{className:"text-sm text-muted-foreground leading-relaxed",children:e.jsx("div",{className:"grid grid-cols-2 gap-x-6 gap-y-1",children:[["upper","Convert to uppercase"],["lower","Convert to lowercase"],["trim","Remove whitespace"],["replace","Replace substring"],["split","Split by separator"],["join","Join with separator"],["basename","Filename from path"],["dirname","Directory from path"],["ext","File extension"],["env","Read env variable"],["readFile","Read file contents"],["default","Fallback if empty"],["ternary","Conditional value"],["json","Marshal to JSON"]].map(([t,l])=>e.jsxs("div",{className:"flex gap-2 items-baseline",children:[e.jsx("span",{className:"font-mono text-sm text-blue-400",children:t}),e.jsx("span",{className:"text-sm",children:l})]},t))})})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Task Options"}),e.jsxs("div",{className:"text-sm text-muted-foreground leading-relaxed",children:[e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"cmd"})," — Shell command to execute"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"call"})," — Call another blueprint by path"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"if"})," — Conditional expression (template syntax)"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"dir"})," — Working directory for the command"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"env"})," — Per-task environment variables"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"retry"})," — Number of retry attempts on failure"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"retry_delay_seconds"})," — Delay between retries"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"timeout_seconds"})," — Maximum execution time"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"continue_on_error"})," — Don't halt on failure"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"on_success / on_fail"})," — Hooks triggered by outcome"]})]})]})]}),i==="pipelines"&&e.jsxs("div",{className:"flex flex-col gap-5 max-w-4xl",children:[e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"What are Pipelines?"}),e.jsx("div",{className:"text-sm text-muted-foreground leading-relaxed",children:"Pipelines chain multiple blueprints together into a sequential workflow. Each stage runs a blueprint, and (by default) the pipeline stops on the first failure. Use pipelines to orchestrate multi-step deployments, build chains, or any workflow that requires ordered execution."})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Pipeline Schema"}),e.jsxs("div",{className:"text-sm text-muted-foreground leading-relaxed",children:[e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"meta.name"})," — Pipeline display name"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"stop_on_fail"})," — Stop pipeline if a stage fails (default: true)"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"stages[]"})," — Ordered list of stages to execute:"]}),e.jsxs("div",{className:"pl-4",children:[e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"name"})," — Stage identifier (required)"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"blueprint_path"})," — Path to blueprint YAML (required)"]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"inputs"})," — Key-value inputs passed to the blueprint"]})]}),e.jsxs("div",{children:[e.jsx("strong",{className:"text-blue-400",children:"inputs"})," — Global inputs inherited by all stages"]})]})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Example Pipeline"}),e.jsx("pre",{className:"text-sm leading-normal text-foreground bg-muted p-3 rounded border border-border overflow-auto font-mono whitespace-pre m-0",children:`meta:
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
    blueprint_path: ./deploy.yaml`})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"How to Use"}),e.jsxs("ol",{className:"text-sm leading-relaxed pl-5 text-foreground",children:[e.jsxs("li",{children:["Navigate to the ",e.jsx("strong",{children:"Pipelines"})," page from the sidebar"]}),e.jsxs("li",{children:["Click ",e.jsx("strong",{children:"Open Folder"})," to select a directory containing pipeline YAML files"]}),e.jsxs("li",{children:["Click ",e.jsx("strong",{children:"New Pipeline"})," to create a pipeline with the visual editor"]}),e.jsxs("li",{children:["Use the ",e.jsx("strong",{children:"Edit"})," and ",e.jsx("strong",{children:"Delete"})," buttons to manage existing pipelines"]}),e.jsxs("li",{children:["Click ",e.jsx("strong",{children:"Run"})," next to a pipeline file to start execution"]}),e.jsxs("li",{children:["Monitor progress in the ",e.jsx("strong",{children:"Recent Pipeline Runs"})," section below"]}),e.jsxs("li",{children:["Click ",e.jsx("strong",{children:"Stages"})," on a run to see individual stage status and jump to run details"]})]})]}),e.jsxs("div",{className:"rounded-lg border border-border bg-card p-4",children:[e.jsx("div",{className:"text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2",children:"Execution Behavior"}),e.jsxs("div",{className:"text-sm text-muted-foreground leading-relaxed",children:[e.jsxs("div",{children:["Stages execute ",e.jsx("strong",{className:"text-foreground",children:"sequentially"})," — each stage waits for the previous one to complete."]}),e.jsxs("div",{className:"mt-1",children:["With ",e.jsx("strong",{className:"text-blue-400",children:"stop_on_fail: true"})," (default), the pipeline halts when any stage fails."]}),e.jsxs("div",{className:"mt-1",children:["With ",e.jsx("strong",{className:"text-blue-400",children:"stop_on_fail: false"}),", all stages run regardless of failures."]}),e.jsxs("div",{className:"mt-1",children:["Each stage creates a separate ",e.jsx("strong",{className:"text-foreground",children:"blueprint run"})," visible in the Run Log."]}),e.jsxs("div",{className:"mt-1",children:["Default stage timeout: ",e.jsx("strong",{className:"text-foreground",children:"60 seconds"}),". Blueprint-level timeouts override this."]})]})]})]}),i==="examples"&&e.jsxs("div",{className:"flex flex-col gap-4 max-w-4xl",children:[e.jsxs("div",{className:"text-sm text-muted-foreground leading-normal",children:["Sample blueprints and pipelines for common use cases. ",e.jsx("strong",{className:"text-foreground",children:"View"})," to see the full YAML, ",e.jsx("strong",{className:"text-foreground",children:"Copy"})," to clipboard, or ",e.jsx("strong",{className:"text-foreground",children:"Add"})," to save directly to your blueprints folder."]}),e.jsx("div",{className:"flex gap-1",children:["all","blueprint","pipeline"].map(t=>e.jsx(n,{variant:d===t?"outline":"ghost",size:"sm",onClick:()=>h(t),children:t==="all"?"All":t==="blueprint"?"Blueprints":"Pipelines"},t))}),b.map(t=>e.jsxs("div",{className:"rounded-lg border border-border bg-card px-4 py-3",children:[e.jsxs("div",{className:"flex items-center gap-2 mb-1",children:[e.jsx("span",{className:m("text-xs font-bold uppercase tracking-wide px-1.5 py-0.5 rounded",(t.kind==="pipeline","bg-blue-400/15 text-blue-400")),children:t.kind}),e.jsx("div",{className:"font-semibold text-sm text-foreground flex-1",children:t.title}),e.jsxs("div",{className:"flex gap-1",children:[e.jsx(n,{variant:"ghost",size:"xs",onClick:()=>c(t),children:"View"}),e.jsx(n,{variant:"ghost",size:"xs",onClick:()=>{navigator.clipboard.writeText(t.yaml),a.success("Copied to clipboard")},children:"Copy"}),e.jsxs(n,{size:"xs",onClick:()=>x(t),children:[e.jsx(u,{size:11})," Add"]})]})]}),e.jsx("div",{className:"text-sm text-muted-foreground",children:t.description}),e.jsx("div",{className:"text-xs font-mono text-blue-400 mt-0.5",children:t.filename})]},t.filename))]}),e.jsx(g,{open:!!s,onOpenChange:t=>{t||c(null)},children:e.jsx(j,{className:"sm:max-w-[650px] w-full",children:s&&e.jsxs(e.Fragment,{children:[e.jsx(f,{children:e.jsxs("div",{className:"flex items-center justify-between",children:[e.jsxs("div",{children:[e.jsxs("div",{className:"flex items-center gap-2",children:[e.jsx("span",{className:m("text-xs font-bold uppercase tracking-wide px-1.5 py-0.5 rounded",(s.kind==="pipeline","bg-blue-400/15 text-blue-400")),children:s.kind}),e.jsx(v,{className:"font-semibold text-base",children:s.title})]}),e.jsx("div",{className:"text-sm font-mono text-blue-400",children:s.filename})]}),e.jsxs("div",{className:"flex gap-1",children:[e.jsx(n,{variant:"ghost",size:"sm",onClick:()=>{navigator.clipboard.writeText(s.yaml),a.success("Copied to clipboard")},children:"Copy"}),e.jsxs(n,{size:"sm",onClick:()=>x(s),children:[e.jsx(u,{size:11})," Add to Blueprints"]})]})]})}),e.jsx("pre",{className:"text-sm leading-normal text-foreground bg-muted p-3 rounded border border-border overflow-auto font-mono whitespace-pre m-0 max-h-[60vh]",children:s.yaml}),e.jsx(y,{children:e.jsx(N,{render:e.jsx(n,{variant:"ghost"}),children:"Close"})})]})})})]})}export{B as HelpPage};
