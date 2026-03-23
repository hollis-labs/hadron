import { useState } from 'react';
import { toast } from 'sonner';
import { FolderPlus } from 'lucide-react';
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
          cp "{{ .inputs.db_path }}" "{{ .inputs.backup_dir }}/backup_\${TIMESTAMP}.db"
      - name: compress-backup
        cmd: |
          LATEST=$(ls -t "{{ .inputs.backup_dir }}"/backup_*.db | head -1)
          gzip "\$LATEST"
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
      <div className="page-header">
        <span className="page-title">Help</span>
      </div>

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: '0.25rem', marginBottom: '1rem' }}>
        {TABS.map(t => (
          <button
            key={t.key}
            className={tab === t.key ? 'hud-button' : 'hud-button-ghost'}
            onClick={() => setTab(t.key)}
            style={{ fontSize: 'var(--text-sm)', padding: '0.3rem 0.75rem' }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* ══════════════ Tab: General ══════════════ */}
      {tab === 'general' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.25rem', maxWidth: '960px' }}>
          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>About</div>
            <div style={{ fontSize: 'var(--text-md)', marginBottom: '0.3rem' }}>
              <strong style={{ color: 'var(--status-success)' }}>HADRON</strong>
              <span style={{ color: 'var(--text-tertiary)', marginLeft: '0.5rem' }}>by Hollis Labs</span>
            </div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.5' }}>
              A local-first, agent-first blueprint automation runner. Create, inspect, and run YAML blueprints
              that orchestrate multi-step workflows with inputs, conditions, retries, and more.
            </div>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Quick Start</div>
            <ol style={{ fontSize: 'var(--text-md)', lineHeight: '1.7', paddingLeft: '1.2rem', color: 'var(--text-primary)' }}>
              <li>Go to <strong>Blueprints</strong> and click <strong>Open Folder</strong> to select a directory containing .yaml blueprints</li>
              <li>Click a blueprint name to view its details, inputs, and step timeline</li>
              <li>Click <strong>Run</strong> to execute a blueprint (fill in inputs if required)</li>
              <li>Check the <strong>Run Log</strong> to monitor execution and view results</li>
              <li>Use <strong>New Blueprint</strong> to create a blueprint from scratch using the wizard</li>
              <li>Set up recurring runs in <strong>Schedules</strong> with cron expressions</li>
              <li>Chain blueprints together with <strong>Pipelines</strong> for multi-step workflows</li>
            </ol>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Keyboard Shortcuts</div>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
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
                    <td style={{ padding: '0.3rem 0', width: '120px' }}>
                      <kbd style={{
                        background: 'var(--bg-raised)', border: '1px solid var(--border-default)',
                        borderRadius: '3px', padding: '2px 8px', fontSize: 'var(--text-sm)',
                        fontFamily: 'var(--font-mono)',
                      }}>{key}</kbd>
                    </td>
                    <td style={{ padding: '0.3rem 0', fontSize: 'var(--text-md)', color: 'var(--text-tertiary)' }}>{desc}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Pages Overview</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              <div><strong style={{ color: 'var(--accent)' }}>Dashboard</strong> — Run stats, activity timeline, blueprint success rates</div>
              <div><strong style={{ color: 'var(--accent)' }}>Blueprints</strong> — Browse, create, edit, run, and manage blueprint YAML files</div>
              <div><strong style={{ color: 'var(--accent)' }}>Pipelines</strong> — Chain blueprints into multi-stage workflows</div>
              <div><strong style={{ color: 'var(--accent)' }}>Run Log</strong> — History of all blueprint runs with status, duration, and details</div>
              <div><strong style={{ color: 'var(--accent)' }}>Schedules</strong> — Cron-based recurring runs and one-time scheduled executions</div>
              <div><strong style={{ color: 'var(--accent)' }}>Telemetry</strong> — JSONL activity logs per run for debugging and auditing</div>
              <div><strong style={{ color: 'var(--accent)' }}>Settings</strong> — Execution limits, safety controls, telemetry retention</div>
            </div>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Resources</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.35rem' }}>
              {[
                ['Data directory', '~/.hadron/'],
                ['Settings', '~/.hadron/settings.json'],
                ['Run logs', '~/.hadron/logs/runs/'],
                ['Database', '~/.hadron/state/hadron.db'],
                ['Archive', '~/.hadron/archive/'],
                ['Preferences', '~/.hadron/preferences.json'],
              ].map(([label, path]) => (
                <div key={label} style={{ fontSize: 'var(--text-md)', color: 'var(--text-tertiary)' }}>
                  {label}: <span style={{ fontFamily: 'monospace', color: 'var(--accent)' }}>{path}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* ══════════════ Tab: Blueprints ══════════════ */}
      {tab === 'blueprints' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.25rem', maxWidth: '960px' }}>
          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>What are Blueprints?</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              Blueprints are YAML files that define multi-step automation workflows. Each blueprint can declare
              inputs, environment variables, conditional steps, retries, and hooks. Hadron executes them locally
              using Go&apos;s text/template engine for dynamic values.
            </div>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Blueprint Schema (v0.4)</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              <div><strong style={{ color: 'var(--accent)' }}>version:</strong> Schema version (currently "0.4")</div>
              <div><strong style={{ color: 'var(--accent)' }}>blueprint:</strong> name, slug, title, description, author, license, tags, homepage</div>
              <div><strong style={{ color: 'var(--accent)' }}>project:</strong> type, name, dir, path, php_version, node, vars</div>
              <div><strong style={{ color: 'var(--accent)' }}>env:</strong> key-value environment variables</div>
              <div><strong style={{ color: 'var(--accent)' }}>inputs:</strong> name, label, type (string|number|boolean|array), required, default, enum, pattern</div>
              <div><strong style={{ color: 'var(--accent)' }}>packages:</strong> npm, composer, pip, brew, go</div>
              <div><strong style={{ color: 'var(--accent)' }}>steps:</strong> sections with tasks (name, cmd, call, if, retry, timeout, dir, env)</div>
              <div><strong style={{ color: 'var(--accent)' }}>hooks:</strong> before_run, after_run, on_error</div>
              <div><strong style={{ color: 'var(--accent)' }}>imports:</strong> path, alias, with</div>
              <div><strong style={{ color: 'var(--accent)' }}>stubs:</strong> enabled, search_paths, strict_match</div>
              <div><strong style={{ color: 'var(--accent)' }}>git:</strong> init, create_github_repo, visibility, remote, branch</div>
            </div>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Template Variables</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              <div style={{ marginBottom: '0.5rem', color: 'var(--text-primary)' }}>
                Blueprints use Go text/template syntax. Available variables:
              </div>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
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
                      <td style={{
                        padding: '0.25rem 0', fontFamily: 'monospace', fontSize: 'var(--text-sm)',
                        color: 'var(--accent)', whiteSpace: 'nowrap', width: '220px',
                      }}>{variable}</td>
                      <td style={{ padding: '0.25rem 0', fontSize: 'var(--text-sm)' }}>{desc}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Template Functions</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.3rem 1.5rem' }}>
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
                  <div key={fn} style={{ display: 'flex', gap: '0.5rem', alignItems: 'baseline' }}>
                    <span style={{ fontFamily: 'monospace', fontSize: 'var(--text-sm)', color: 'var(--accent)' }}>{fn}</span>
                    <span style={{ fontSize: 'var(--text-sm)' }}>{desc}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Task Options</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              <div><strong style={{ color: 'var(--accent)' }}>cmd</strong> — Shell command to execute</div>
              <div><strong style={{ color: 'var(--accent)' }}>call</strong> — Call another blueprint by path</div>
              <div><strong style={{ color: 'var(--accent)' }}>if</strong> — Conditional expression (template syntax)</div>
              <div><strong style={{ color: 'var(--accent)' }}>dir</strong> — Working directory for the command</div>
              <div><strong style={{ color: 'var(--accent)' }}>env</strong> — Per-task environment variables</div>
              <div><strong style={{ color: 'var(--accent)' }}>retry</strong> — Number of retry attempts on failure</div>
              <div><strong style={{ color: 'var(--accent)' }}>retry_delay_seconds</strong> — Delay between retries</div>
              <div><strong style={{ color: 'var(--accent)' }}>timeout_seconds</strong> — Maximum execution time</div>
              <div><strong style={{ color: 'var(--accent)' }}>continue_on_error</strong> — Don&apos;t halt on failure</div>
              <div><strong style={{ color: 'var(--accent)' }}>on_success / on_fail</strong> — Hooks triggered by outcome</div>
            </div>
          </div>
        </div>
      )}

      {/* ══════════════ Tab: Pipelines ══════════════ */}
      {tab === 'pipelines' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.25rem', maxWidth: '960px' }}>
          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>What are Pipelines?</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              Pipelines chain multiple blueprints together into a sequential workflow. Each stage runs a blueprint,
              and (by default) the pipeline stops on the first failure. Use pipelines to orchestrate multi-step
              deployments, build chains, or any workflow that requires ordered execution.
            </div>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Pipeline Schema</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              <div><strong style={{ color: 'var(--accent)' }}>meta.name</strong> — Pipeline display name</div>
              <div><strong style={{ color: 'var(--accent)' }}>stop_on_fail</strong> — Stop pipeline if a stage fails (default: true)</div>
              <div><strong style={{ color: 'var(--accent)' }}>stages[]</strong> — Ordered list of stages to execute:</div>
              <div style={{ paddingLeft: '1rem' }}>
                <div><strong style={{ color: 'var(--accent)' }}>name</strong> — Stage identifier (required)</div>
                <div><strong style={{ color: 'var(--accent)' }}>blueprint_path</strong> — Path to blueprint YAML (required)</div>
                <div><strong style={{ color: 'var(--accent)' }}>inputs</strong> — Key-value inputs passed to the blueprint</div>
              </div>
              <div><strong style={{ color: 'var(--accent)' }}>inputs</strong> — Global inputs inherited by all stages</div>
            </div>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Example Pipeline</div>
            <pre style={{
              fontSize: 'var(--text-sm)', lineHeight: '1.5', color: 'var(--text-primary)',
              background: 'var(--bg-raised)', padding: '0.75rem', borderRadius: '4px',
              border: '1px solid var(--border-default)', overflow: 'auto',
              fontFamily: 'monospace', whiteSpace: 'pre', margin: 0,
            }}>
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

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>How to Use</div>
            <ol style={{ fontSize: 'var(--text-md)', lineHeight: '1.7', paddingLeft: '1.2rem', color: 'var(--text-primary)' }}>
              <li>Navigate to the <strong>Pipelines</strong> page from the sidebar</li>
              <li>Click <strong>Open Folder</strong> to select a directory containing pipeline YAML files</li>
              <li>Click <strong>New Pipeline</strong> to create a pipeline with the visual editor</li>
              <li>Use the <strong>Edit</strong> and <strong>Delete</strong> buttons to manage existing pipelines</li>
              <li>Click <strong>Run</strong> next to a pipeline file to start execution</li>
              <li>Monitor progress in the <strong>Recent Pipeline Runs</strong> section below</li>
              <li>Click <strong>Stages</strong> on a run to see individual stage status and jump to run details</li>
            </ol>
          </div>

          <div className="section" style={{ padding: '1rem' }}>
            <div className="bp-meta-section-title" style={{ marginBottom: '0.5rem' }}>Execution Behavior</div>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.6' }}>
              <div>Stages execute <strong style={{ color: 'var(--text-primary)' }}>sequentially</strong> — each stage waits for the previous one to complete.</div>
              <div style={{ marginTop: '0.3rem' }}>With <strong style={{ color: 'var(--accent)' }}>stop_on_fail: true</strong> (default), the pipeline halts when any stage fails.</div>
              <div style={{ marginTop: '0.3rem' }}>With <strong style={{ color: 'var(--accent)' }}>stop_on_fail: false</strong>, all stages run regardless of failures.</div>
              <div style={{ marginTop: '0.3rem' }}>Each stage creates a separate <strong style={{ color: 'var(--text-primary)' }}>blueprint run</strong> visible in the Run Log.</div>
              <div style={{ marginTop: '0.3rem' }}>Default stage timeout: <strong style={{ color: 'var(--text-primary)' }}>60 seconds</strong>. Blueprint-level timeouts override this.</div>
            </div>
          </div>
        </div>
      )}

      {/* ══════════════ Tab: Examples ══════════════ */}
      {tab === 'examples' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem', maxWidth: '960px' }}>
          <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', lineHeight: '1.5' }}>
            Sample blueprints and pipelines for common use cases. <strong style={{ color: 'var(--text-primary)' }}>View</strong> to
            see the full YAML, <strong style={{ color: 'var(--text-primary)' }}>Copy</strong> to clipboard,
            or <strong style={{ color: 'var(--text-primary)' }}>Add</strong> to save directly to your blueprints folder.
          </div>

          {/* Filter */}
          <div style={{ display: 'flex', gap: '0.25rem' }}>
            {(['all', 'blueprint', 'pipeline'] as const).map(f => (
              <button
                key={f}
                className={exampleFilter === f ? 'hud-button' : 'hud-button-ghost'}
                onClick={() => setExampleFilter(f)}
                style={{ fontSize: 'var(--text-sm)', padding: '0.2rem 0.6rem' }}
              >
                {f === 'all' ? 'All' : f === 'blueprint' ? 'Blueprints' : 'Pipelines'}
              </button>
            ))}
          </div>

          {filteredSamples.map(sample => (
            <div key={sample.filename} className="section" style={{ padding: '0.75rem 1rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.25rem' }}>
                <span style={{
                  fontSize: 'var(--text-xs)', fontWeight: 700, letterSpacing: '0.08em',
                  textTransform: 'uppercase', padding: '0.1rem 0.35rem', borderRadius: '3px',
                  background: sample.kind === 'pipeline' ? 'rgba(var(--accent) / 0.15)' : 'rgba(var(--ok) / 0.15)',
                  color: sample.kind === 'pipeline' ? 'var(--accent)' : 'var(--status-success)',
                }}>
                  {sample.kind}
                </span>
                <div style={{ fontWeight: 600, fontSize: 'var(--text-md)', color: 'var(--text-primary)', flex: 1 }}>
                  {sample.title}
                </div>
                <div style={{ display: 'flex', gap: '0.25rem' }}>
                  <button
                    className="btn btn-ghost"
                    onClick={() => setViewingSample(sample)}
                    style={{ fontSize: 'var(--text-xs)', padding: '0.2rem 0.5rem' }}
                  >
                    View
                  </button>
                  <button
                    className="btn btn-ghost"
                    onClick={() => {
                      navigator.clipboard.writeText(sample.yaml);
                      toast.success('Copied to clipboard');
                    }}
                    style={{ fontSize: 'var(--text-xs)', padding: '0.2rem 0.5rem' }}
                  >
                    Copy
                  </button>
                  <button
                    className="btn btn-primary"
                    onClick={() => addFileToBlueprints(sample)}
                    style={{ fontSize: 'var(--text-xs)', padding: '0.2rem 0.5rem', display: 'flex', alignItems: 'center', gap: '0.25rem' }}
                  >
                    <FolderPlus size={11} /> Add
                  </button>
                </div>
              </div>
              <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>
                {sample.description}
              </div>
              <div style={{ fontSize: 'var(--text-xs)', fontFamily: 'monospace', color: 'var(--accent)', marginTop: '0.2rem' }}>
                {sample.filename}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Sample viewer modal */}
      {viewingSample && (
        <div className="hud-modal-overlay" onClick={() => setViewingSample(null)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '650px', width: '100%' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }}>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  <span style={{
                    fontSize: 'var(--text-xs)', fontWeight: 700, letterSpacing: '0.08em',
                    textTransform: 'uppercase', padding: '0.1rem 0.35rem', borderRadius: '3px',
                    background: viewingSample.kind === 'pipeline' ? 'rgba(var(--accent) / 0.15)' : 'rgba(var(--ok) / 0.15)',
                    color: viewingSample.kind === 'pipeline' ? 'var(--accent)' : 'var(--status-success)',
                  }}>
                    {viewingSample.kind}
                  </span>
                  <span style={{ fontWeight: 600, fontSize: 'var(--text-base)' }}>{viewingSample.title}</span>
                </div>
                <div style={{ fontSize: 'var(--text-sm)', fontFamily: 'monospace', color: 'var(--accent)' }}>{viewingSample.filename}</div>
              </div>
              <div style={{ display: 'flex', gap: '0.3rem' }}>
                <button
                  className="btn btn-ghost"
                  onClick={() => {
                    navigator.clipboard.writeText(viewingSample.yaml);
                    toast.success('Copied to clipboard');
                  }}
                  style={{ fontSize: 'var(--text-sm)', padding: '0.25rem 0.6rem' }}
                >
                  Copy
                </button>
                <button
                  className="btn btn-primary"
                  onClick={() => addFileToBlueprints(viewingSample)}
                  style={{ fontSize: 'var(--text-sm)', padding: '0.25rem 0.6rem', display: 'flex', alignItems: 'center', gap: '0.25rem' }}
                >
                  <FolderPlus size={11} /> Add to Blueprints
                </button>
              </div>
            </div>
            <pre style={{
              fontSize: 'var(--text-sm)', lineHeight: '1.5', color: 'var(--text-primary)',
              background: 'var(--bg-raised)', padding: '0.75rem', borderRadius: '4px',
              border: '1px solid var(--border-default)', overflow: 'auto',
              fontFamily: 'monospace', whiteSpace: 'pre', margin: 0, maxHeight: '60vh',
            }}>
              {viewingSample.yaml}
            </pre>
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '0.75rem' }}>
              <button className="btn btn-ghost" onClick={() => setViewingSample(null)}>Close</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
