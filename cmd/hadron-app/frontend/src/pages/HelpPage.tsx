import { useState } from 'react';
import { toast } from 'sonner';
import { FolderPlus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogClose } from '@/components/ui/dialog';
import { cn } from '@/lib/utils';
import { createBlueprintFile, getPreference, selectDirectoryDialog } from '../api/client';

type HelpTab = 'general' | 'blueprints' | 'pipelines' | 'examples';

const TABS: { key: HelpTab; label: string }[] = [
  { key: 'general', label: 'General' },
  { key: 'blueprints', label: 'Blueprints' },
  { key: 'pipelines', label: 'Pipelines' },
  { key: 'examples', label: 'Examples' },
];

// ── Sample data ──

interface SampleFile {
  title: string;
  description: string;
  filename: string;
  yaml: string;
  kind: 'blueprint' | 'pipeline';
}

const SAMPLES: SampleFile[] = [
  // ── Blueprints ──
  {
    kind: 'blueprint',
    title: 'Hello World',
    description: 'Minimal blueprint that echoes a greeting. Great starting point.',
    filename: 'hello-world.yaml',
    yaml: `version: "0.4"
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
`,
  },
  {
    kind: 'blueprint',
    title: 'Git Repo Setup',
    description: 'Initialize a git repository with a README and first commit.',
    filename: 'git-repo-setup.yaml',
    yaml: `version: "0.4"
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
`,
  },
  {
    kind: 'blueprint',
    title: 'Node.js Project Scaffold',
    description: 'Create a new Node.js project with package.json and basic structure.',
    filename: 'node-scaffold.yaml',
    yaml: `version: "0.4"
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
`,
  },
  {
    kind: 'blueprint',
    title: 'Database Backup',
    description: 'Backup a SQLite database with timestamped filename and optional compression.',
    filename: 'db-backup.yaml',
    yaml: `version: "0.4"
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
          cp "{{ .inputs.db_path }}" "{{ .inputs.backup_dir }}/backup_${'${'}TIMESTAMP}.db"
      - name: compress-backup
        cmd: |
          LATEST=$(ls -t "{{ .inputs.backup_dir }}"/backup_*.db | head -1)
          gzip "$LATEST"
        if: "{{ .inputs.compress }}"
      - name: report
        cmd: |
          echo "Backup complete:"
          ls -lh "{{ .inputs.backup_dir }}"
`,
  },
  {
    kind: 'blueprint',
    title: 'Deploy Script',
    description: 'Multi-step deployment with build, test, and deploy stages.',
    filename: 'deploy.yaml',
    yaml: `version: "0.4"
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
`,
  },
  // ── Pipelines ──
  {
    kind: 'pipeline',
    title: 'CI Pipeline',
    description: 'Lint, test, and build — a standard continuous integration flow.',
    filename: 'ci-pipeline.yaml',
    yaml: `meta:
  name: ci-pipeline
stop_on_fail: true
stages:
  - name: lint
    blueprint_path: ./lint-check.yaml
  - name: test
    blueprint_path: ./test-suite.yaml
  - name: build
    blueprint_path: ./build-frontend.yaml
`,
  },
  {
    kind: 'pipeline',
    title: 'Full Stack Deploy',
    description: 'Build backend, build frontend, run tests, then deploy — all sequential.',
    filename: 'full-deploy.yaml',
    yaml: `meta:
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
`,
  },
  {
    kind: 'pipeline',
    title: 'Nightly Build & Backup',
    description: 'Runs a full build then backs up the database. Continue even if build fails.',
    filename: 'nightly-build.yaml',
    yaml: `meta:
  name: nightly-build-and-backup
stop_on_fail: false
stages:
  - name: full-build
    blueprint_path: ./build-frontend.yaml
  - name: run-tests
    blueprint_path: ./test-suite.yaml
  - name: backup-db
    blueprint_path: ./db-backup.yaml
`,
  },
  {
    kind: 'pipeline',
    title: 'Multi-Env Deploy',
    description: 'Deploy to staging first, then production. Stops if staging fails.',
    filename: 'multi-env-deploy.yaml',
    yaml: `meta:
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
`,
  },
];

async function addFileToBlueprints(sample: SampleFile) {
  // Try default blueprint dir, then last used, then prompt
  let dir = await getPreference('defaultBlueprintDir');
  if (!dir) dir = await getPreference('lastBlueprintDir');
  if (!dir) {
    dir = await selectDirectoryDialog();
    if (!dir) return; // user cancelled
  }
  try {
    await createBlueprintFile(dir, sample.filename, sample.yaml);
    toast.success(`Added ${sample.filename} to ${dir.split('/').slice(-2).join('/')}`);
  } catch (err) {
    toast.error(`Failed to add file: ${err}`);
  }
}

export function HelpPage() {
  const [tab, setTab] = useState<HelpTab>('general');
  const [viewingSample, setViewingSample] = useState<SampleFile | null>(null);
  const [exampleFilter, setExampleFilter] = useState<'all' | 'blueprint' | 'pipeline'>('all');

  const filteredSamples = SAMPLES.filter(s =>
    exampleFilter === 'all' || s.kind === exampleFilter
  );

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <span className="text-xl font-semibold text-foreground tracking-tight">Help</span>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 mb-4">
        {TABS.map(t => (
          <Button
            key={t.key}
            variant={tab === t.key ? 'outline' : 'ghost'}
            size="sm"
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </Button>
        ))}
      </div>

      {/* ══════════════ Tab: General ══════════════ */}
      {tab === 'general' && (
        <div className="flex flex-col gap-5 max-w-4xl">
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">About</div>
            <div className="text-sm mb-1">
              <strong className="text-blue-400">HADRON</strong>
              <span className="text-muted-foreground ml-2">by Hollis Labs</span>
            </div>
            <div className="text-sm text-muted-foreground leading-normal">
              A local-first, agent-first blueprint automation runner. Create, inspect, and run YAML blueprints
              that orchestrate multi-step workflows with inputs, conditions, retries, and more.
            </div>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Quick Start</div>
            <ol className="text-sm leading-relaxed pl-5 text-foreground">
              <li>Go to <strong>Blueprints</strong> and click <strong>Open Folder</strong> to select a directory containing .yaml blueprints</li>
              <li>Click a blueprint name to view its details, inputs, and step timeline</li>
              <li>Click <strong>Run</strong> to execute a blueprint (fill in inputs if required)</li>
              <li>Check the <strong>Run Log</strong> to monitor execution and view results</li>
              <li>Use <strong>New Blueprint</strong> to create a blueprint from scratch using the wizard</li>
              <li>Set up recurring runs in <strong>Schedules</strong> with cron expressions</li>
              <li>Chain blueprints together with <strong>Pipelines</strong> for multi-step workflows</li>
            </ol>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Keyboard Shortcuts</div>
            <table className="w-full border-collapse">
              <tbody>
                {[
                  ['Esc', 'Close modal / go back from detail pages'],
                  ['R', 'Refresh current page data'],
                  ['N', 'New blueprint (on Blueprints page)'],
                  ['?', 'Open this Help page'],
                  ['\u2191 / \u2193', 'Navigate rows in lists (Blueprints, Run Log)'],
                  ['Enter', 'Open selected item in list'],
                  ['Space', 'Toggle selection (Blueprints page)'],
                ].map(([key, desc]) => (
                  <tr key={key}>
                    <td className="py-1 w-[120px]">
                      <kbd className="bg-muted border border-border rounded px-2 py-0.5 text-sm font-mono">{key}</kbd>
                    </td>
                    <td className="py-1 text-sm text-muted-foreground">{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Pages Overview</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              <div><strong className="text-blue-400">Dashboard</strong> — Run stats, activity timeline, blueprint success rates</div>
              <div><strong className="text-blue-400">Blueprints</strong> — Browse, create, edit, run, and manage blueprint YAML files</div>
              <div><strong className="text-blue-400">Pipelines</strong> — Chain blueprints into multi-stage workflows</div>
              <div><strong className="text-blue-400">Run Log</strong> — History of all blueprint runs with status, duration, and details</div>
              <div><strong className="text-blue-400">Schedules</strong> — Cron-based recurring runs and one-time scheduled executions</div>
              <div><strong className="text-blue-400">Telemetry</strong> — JSONL activity logs per run for debugging and auditing</div>
              <div><strong className="text-blue-400">Settings</strong> — Execution limits, safety controls, telemetry retention</div>
            </div>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Resources</div>
            <div className="flex flex-col gap-1.5">
              {[
                ['Data directory', '~/.hadron/'],
                ['Settings', '~/.hadron/settings.json'],
                ['Run logs', '~/.hadron/logs/runs/'],
                ['Database', '~/.hadron/state/hadron.db'],
                ['Archive', '~/.hadron/archive/'],
                ['Preferences', '~/.hadron/preferences.json'],
              ].map(([label, path]) => (
                <div key={label} className="text-sm text-muted-foreground">
                  {label}: <span className="font-mono text-blue-400">{path}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* ══════════════ Tab: Blueprints ══════════════ */}
      {tab === 'blueprints' && (
        <div className="flex flex-col gap-5 max-w-4xl">
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">What are Blueprints?</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              Blueprints are YAML files that define multi-step automation workflows. Each blueprint can declare
              inputs, environment variables, conditional steps, retries, and hooks. Hadron executes them locally
              using Go&apos;s text/template engine for dynamic values.
            </div>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Blueprint Schema (v0.4)</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              <div><strong className="text-blue-400">version:</strong> Schema version (currently "0.4")</div>
              <div><strong className="text-blue-400">blueprint:</strong> name, slug, title, description, author, license, tags, homepage</div>
              <div><strong className="text-blue-400">project:</strong> type, name, dir, path, php_version, node, vars</div>
              <div><strong className="text-blue-400">env:</strong> key-value environment variables</div>
              <div><strong className="text-blue-400">inputs:</strong> name, label, type (string|number|boolean|array), required, default, enum, pattern</div>
              <div><strong className="text-blue-400">packages:</strong> npm, composer, pip, brew, go</div>
              <div><strong className="text-blue-400">steps:</strong> sections with tasks (name, cmd, call, if, retry, timeout, dir, env)</div>
              <div><strong className="text-blue-400">hooks:</strong> before_run, after_run, on_error</div>
              <div><strong className="text-blue-400">imports:</strong> path, alias, with</div>
              <div><strong className="text-blue-400">stubs:</strong> enabled, search_paths, strict_match</div>
              <div><strong className="text-blue-400">git:</strong> init, create_github_repo, visibility, remote, branch</div>
            </div>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Template Variables</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              <div className="mb-2 text-foreground">
                Blueprints use Go text/template syntax. Available variables:
              </div>
              <table className="w-full border-collapse">
                <tbody>
                  {[
                    ['{{ .inputs.name }}', 'User-provided input value by name'],
                    ['{{ .env.KEY }}', 'Environment variable value'],
                    ['{{ .project.name }}', 'Project name from blueprint config'],
                    ['{{ .project.root }}', 'Resolved project root directory'],
                    ['{{ .project.dir }}', 'Project directory (template-rendered)'],
                    ['{{ .blueprint.name }}', 'Blueprint name from metadata'],
                    ['{{ .blueprint.slug }}', 'Blueprint slug from metadata'],
                    ['{{ .workspace.id }}', 'Current workspace identifier'],
                  ].map(([variable, desc]) => (
                    <tr key={variable}>
                      <td className="py-1 font-mono text-sm text-blue-400 whitespace-nowrap w-[220px]">{variable}</td>
                      <td className="py-1 text-sm">{desc}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Template Functions</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              <div className="grid grid-cols-2 gap-x-6 gap-y-1">
                {[
                  ['upper', 'Convert to uppercase'],
                  ['lower', 'Convert to lowercase'],
                  ['trim', 'Remove whitespace'],
                  ['replace', 'Replace substring'],
                  ['split', 'Split by separator'],
                  ['join', 'Join with separator'],
                  ['basename', 'Filename from path'],
                  ['dirname', 'Directory from path'],
                  ['ext', 'File extension'],
                  ['env', 'Read env variable'],
                  ['readFile', 'Read file contents'],
                  ['default', 'Fallback if empty'],
                  ['ternary', 'Conditional value'],
                  ['json', 'Marshal to JSON'],
                ].map(([fn, desc]) => (
                  <div key={fn} className="flex gap-2 items-baseline">
                    <span className="font-mono text-sm text-blue-400">{fn}</span>
                    <span className="text-sm">{desc}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Task Options</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              <div><strong className="text-blue-400">cmd</strong> — Shell command to execute</div>
              <div><strong className="text-blue-400">call</strong> — Call another blueprint by path</div>
              <div><strong className="text-blue-400">if</strong> — Conditional expression (template syntax)</div>
              <div><strong className="text-blue-400">dir</strong> — Working directory for the command</div>
              <div><strong className="text-blue-400">env</strong> — Per-task environment variables</div>
              <div><strong className="text-blue-400">retry</strong> — Number of retry attempts on failure</div>
              <div><strong className="text-blue-400">retry_delay_seconds</strong> — Delay between retries</div>
              <div><strong className="text-blue-400">timeout_seconds</strong> — Maximum execution time</div>
              <div><strong className="text-blue-400">continue_on_error</strong> — Don&apos;t halt on failure</div>
              <div><strong className="text-blue-400">on_success / on_fail</strong> — Hooks triggered by outcome</div>
            </div>
          </div>
        </div>
      )}

      {/* ══════════════ Tab: Pipelines ══════════════ */}
      {tab === 'pipelines' && (
        <div className="flex flex-col gap-5 max-w-4xl">
          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">What are Pipelines?</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              Pipelines chain multiple blueprints together into a sequential workflow. Each stage runs a blueprint,
              and (by default) the pipeline stops on the first failure. Use pipelines to orchestrate multi-step
              deployments, build chains, or any workflow that requires ordered execution.
            </div>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Pipeline Schema</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              <div><strong className="text-blue-400">meta.name</strong> — Pipeline display name</div>
              <div><strong className="text-blue-400">stop_on_fail</strong> — Stop pipeline if a stage fails (default: true)</div>
              <div><strong className="text-blue-400">stages[]</strong> — Ordered list of stages to execute:</div>
              <div className="pl-4">
                <div><strong className="text-blue-400">name</strong> — Stage identifier (required)</div>
                <div><strong className="text-blue-400">blueprint_path</strong> — Path to blueprint YAML (required)</div>
                <div><strong className="text-blue-400">inputs</strong> — Key-value inputs passed to the blueprint</div>
              </div>
              <div><strong className="text-blue-400">inputs</strong> — Global inputs inherited by all stages</div>
            </div>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Example Pipeline</div>
            <pre className="text-sm leading-normal text-foreground bg-muted p-3 rounded border border-border overflow-auto font-mono whitespace-pre m-0">
{`meta:
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
    blueprint_path: ./deploy.yaml`}
            </pre>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">How to Use</div>
            <ol className="text-sm leading-relaxed pl-5 text-foreground">
              <li>Navigate to the <strong>Pipelines</strong> page from the sidebar</li>
              <li>Click <strong>Open Folder</strong> to select a directory containing pipeline YAML files</li>
              <li>Click <strong>New Pipeline</strong> to create a pipeline with the visual editor</li>
              <li>Use the <strong>Edit</strong> and <strong>Delete</strong> buttons to manage existing pipelines</li>
              <li>Click <strong>Run</strong> next to a pipeline file to start execution</li>
              <li>Monitor progress in the <strong>Recent Pipeline Runs</strong> section below</li>
              <li>Click <strong>Stages</strong> on a run to see individual stage status and jump to run details</li>
            </ol>
          </div>

          <div className="rounded-lg border border-border bg-card p-4">
            <div className="text-xs uppercase tracking-wider text-muted-foreground font-medium mb-2">Execution Behavior</div>
            <div className="text-sm text-muted-foreground leading-relaxed">
              <div>Stages execute <strong className="text-foreground">sequentially</strong> — each stage waits for the previous one to complete.</div>
              <div className="mt-1">With <strong className="text-blue-400">stop_on_fail: true</strong> (default), the pipeline halts when any stage fails.</div>
              <div className="mt-1">With <strong className="text-blue-400">stop_on_fail: false</strong>, all stages run regardless of failures.</div>
              <div className="mt-1">Each stage creates a separate <strong className="text-foreground">blueprint run</strong> visible in the Run Log.</div>
              <div className="mt-1">Default stage timeout: <strong className="text-foreground">60 seconds</strong>. Blueprint-level timeouts override this.</div>
            </div>
          </div>
        </div>
      )}

      {/* ══════════════ Tab: Examples ══════════════ */}
      {tab === 'examples' && (
        <div className="flex flex-col gap-4 max-w-4xl">
          <div className="text-sm text-muted-foreground leading-normal">
            Sample blueprints and pipelines for common use cases. <strong className="text-foreground">View</strong> to
            see the full YAML, <strong className="text-foreground">Copy</strong> to clipboard,
            or <strong className="text-foreground">Add</strong> to save directly to your blueprints folder.
          </div>

          {/* Filter */}
          <div className="flex gap-1">
            {(['all', 'blueprint', 'pipeline'] as const).map(f => (
              <Button
                key={f}
                variant={exampleFilter === f ? 'outline' : 'ghost'}
                size="sm"
                onClick={() => setExampleFilter(f)}
              >
                {f === 'all' ? 'All' : f === 'blueprint' ? 'Blueprints' : 'Pipelines'}
              </Button>
            ))}
          </div>

          {filteredSamples.map(sample => (
            <div key={sample.filename} className="rounded-lg border border-border bg-card px-4 py-3">
              <div className="flex items-center gap-2 mb-1">
                <span className={cn(
                  'text-xs font-bold uppercase tracking-wide px-1.5 py-0.5 rounded',
                  sample.kind === 'pipeline' ? 'bg-blue-400/15 text-blue-400' : 'bg-blue-400/15 text-blue-400'
                )}>
                  {sample.kind}
                </span>
                <div className="font-semibold text-sm text-foreground flex-1">
                  {sample.title}
                </div>
                <div className="flex gap-1">
                  <Button
                    variant="ghost"
                    size="xs"
                    onClick={() => setViewingSample(sample)}
                  >
                    View
                  </Button>
                  <Button
                    variant="ghost"
                    size="xs"
                    onClick={() => {
                      navigator.clipboard.writeText(sample.yaml);
                      toast.success('Copied to clipboard');
                    }}
                  >
                    Copy
                  </Button>
                  <Button
                    size="xs"
                    onClick={() => addFileToBlueprints(sample)}
                  >
                    <FolderPlus size={11} /> Add
                  </Button>
                </div>
              </div>
              <div className="text-sm text-muted-foreground">
                {sample.description}
              </div>
              <div className="text-xs font-mono text-blue-400 mt-0.5">
                {sample.filename}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Sample viewer modal */}
      <Dialog open={!!viewingSample} onOpenChange={(open) => { if (!open) setViewingSample(null); }}>
        <DialogContent className="sm:max-w-[650px] w-full">
          {viewingSample && (
            <>
              <DialogHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className={cn(
                        'text-xs font-bold uppercase tracking-wide px-1.5 py-0.5 rounded',
                        viewingSample.kind === 'pipeline' ? 'bg-blue-400/15 text-blue-400' : 'bg-blue-400/15 text-blue-400'
                      )}>
                        {viewingSample.kind}
                      </span>
                      <DialogTitle className="font-semibold text-base">{viewingSample.title}</DialogTitle>
                    </div>
                    <div className="text-sm font-mono text-blue-400">{viewingSample.filename}</div>
                  </div>
                  <div className="flex gap-1">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        navigator.clipboard.writeText(viewingSample.yaml);
                        toast.success('Copied to clipboard');
                      }}
                    >
                      Copy
                    </Button>
                    <Button
                      size="sm"
                      onClick={() => addFileToBlueprints(viewingSample)}
                    >
                      <FolderPlus size={11} /> Add to Blueprints
                    </Button>
                  </div>
                </div>
              </DialogHeader>
              <pre className="text-sm leading-normal text-foreground bg-muted p-3 rounded border border-border overflow-auto font-mono whitespace-pre m-0 max-h-[60vh]">
                {viewingSample.yaml}
              </pre>
              <DialogFooter>
                <DialogClose render={<Button variant="ghost" />}>Close</DialogClose>
              </DialogFooter>
            </>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
