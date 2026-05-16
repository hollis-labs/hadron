import assert from 'node:assert/strict'
import test from 'node:test'

import type { ParsedBlueprint, WizardBlueprint } from '@/api/types'
import { convertParsedToWizard, convertWizardToYaml, yamlValue } from './blueprintYaml'

function baseWizard(overrides: Partial<WizardBlueprint> = {}): WizardBlueprint {
  return {
    version: '0.4',
    blueprint: {
      name: 'deploy app',
      slug: 'deploy-app',
      title: 'Deploy App',
      description: 'Release: beta',
      author: 'Hadron',
      license: '',
      homepage: '',
      tags: ['beta', 'release'],
    },
    project: {
      type: 'node',
      name: 'hadron',
      dir: '',
      path: '',
      php_version: '',
      node: true,
      vars: { region: 'us-central1' },
    },
    env: { NODE_ENV: 'production' },
    inputs: [
      {
        name: 'environment',
        label: 'Environment',
        description: 'Target environment',
        type: 'string',
        required: true,
        default_value: 'beta',
        enum_values: 'beta, production',
        pattern: '',
        min_length: '',
        max_length: '',
        min: '',
        max: '',
        items_type: '',
      },
    ],
    packages: {
      composer_require: [],
      composer_dev: [],
      npm_deps: ['tsx'],
      npm_dev: ['typescript'],
      pip_deps: [],
      pip_dev: [],
      brew_formulae: [],
      brew_casks: [],
      go_tools: [],
    },
    steps: [
      {
        section: 'deploy',
        tasks: [
          {
            name: 'ship',
            cmd: 'npm run build',
            call: '',
            if_expr: 'inputs.environment == "beta"',
            dir: '',
            env: { CI: 'true' },
            retry: '2',
            retry_delay_seconds: '5',
            timeout_seconds: '60',
            continue_on_error: false,
            enabled: true,
            on_success: [{ type: 'cmd', value: 'echo shipped' }],
            on_fail: [],
          },
        ],
      },
    ],
    git: {
      init: true,
      create_github_repo: false,
      visibility: 'private',
      remote: '',
      branch: 'main',
    },
    stubs: {
      enabled: true,
      search_paths: ['stubs'],
      strict_match: true,
    },
    imports: [
      {
        path: './shared.yaml',
        alias: 'shared',
        with: { flavor: 'beta' },
      },
    ],
    hooks: {
      before_run: [{ name: 'prepare', cmd: 'echo prepare', if_expr: '' }],
      after_run: [],
      on_error: [],
    },
    ...overrides,
  }
}

test('yamlValue quotes scalars that would be ambiguous YAML', () => {
  assert.equal(yamlValue('Release: beta'), '"Release: beta"')
  assert.equal(yamlValue('plain-value'), 'plain-value')
  assert.equal(yamlValue(''), '""')
})

test('convertWizardToYaml serializes blueprint metadata, inputs, packages, and steps', () => {
  const yaml = convertWizardToYaml(baseWizard())

  assert.match(yaml, /^version: "0.4"/)
  assert.ok(yaml.includes('blueprint:\n  name: deploy app\n  slug: deploy-app\n  title: Deploy App\n  description: "Release: beta"'))
  assert.ok(yaml.includes('inputs:\n  - name: environment\n    label: Environment\n    description: Target environment\n    type: string\n    required: true\n    default: beta\n    enum: [beta, production]'))
  assert.ok(yaml.includes('packages:\n  npm:\n    deps: [tsx]\n    dev: [typescript]'))
  assert.ok(yaml.includes('steps:\n  - section: deploy\n    tasks:\n      - name: ship\n        cmd: npm run build\n        if: "inputs.environment == \\"beta\\""'))
  assert.ok(yaml.includes('on_success:\n          - type: cmd\n            value: echo shipped'))
})

test('convertWizardToYaml omits incomplete imports and sections with no valid tasks', () => {
  const yaml = convertWizardToYaml(baseWizard({
    imports: [{ path: '', alias: 'ignored', with: { key: 'value' } }],
    steps: [
      {
        section: '',
        tasks: [
          {
            name: '',
            cmd: '',
            call: '',
            if_expr: '',
            dir: '',
            env: {},
            retry: '0',
            retry_delay_seconds: '0',
            timeout_seconds: '0',
            continue_on_error: false,
            enabled: true,
            on_success: [],
            on_fail: [],
          },
        ],
      },
    ],
  }))

  assert.doesNotMatch(yaml, /imports:/)
  // The lone section's only task is incomplete, so the section is omitted
  // entirely — emitting `section:` with an empty `tasks:` would produce
  // YAML the backend rejects ("section must have at least one step").
  assert.doesNotMatch(yaml, /section:/)
  assert.ok(yaml.endsWith('steps:\n'))
})

test('convertParsedToWizard normalizes optional parsed blueprint fields for forms', () => {
  const parsed = {
    version: '',
    blueprint: {
      name: 'Build',
      slug: 'build',
      title: '',
      description: '',
      author: '',
      license: '',
      tags: undefined,
      homepage: '',
    },
    project: {
      type: 'go',
      name: 'hadron',
      dir: '',
      path: '',
      php_version: '',
      node: false,
      vars: { workers: 3 },
    },
    env: { MODE: 'beta' },
    inputs: [
      {
        name: 'count',
        label: '',
        description: '',
        type: 'number',
        required: true,
        default: 2,
        enum: ['1', '2'],
        min: 1,
        max: 5,
      },
    ],
    packages: {
      npm: { deps: ['vite'], dev: [] },
    },
    git: { init: false, create_github_repo: false, visibility: '', remote: '', branch: '' },
    stubs: { enabled: false, search_paths: [], strict_match: false },
    imports: [{ path: './common.yaml', alias: '', with: { retries: 2 } }],
    hooks: {
      before_run: [{ name: 'prepare', cmd: 'echo prepare', if: 'true' }],
      after_run: [],
      on_error: [],
    },
    steps: [
      {
        section: 'build',
        tasks: [
          {
            name: 'compile',
            cmd: '',
            run: 'go build ./...',
            call: '',
            if: '',
            with: {},
            dir: '',
            env: { CGO_ENABLED: '0' },
            retry: 1,
            retry_delay_seconds: 2,
            timeout_seconds: 30,
            continue_on_error: false,
            enabled: null,
            on_success: [],
            on_fail: [{ type: 'cmd', value: 'echo failed' }],
          },
        ],
      },
    ],
  } as unknown as ParsedBlueprint

  const wizard = convertParsedToWizard(parsed)

  assert.equal(wizard.version, '0.4')
  assert.deepEqual(wizard.blueprint.tags, [])
  assert.equal(wizard.project.vars.workers, '3')
  assert.equal(wizard.inputs[0].default_value, '2')
  assert.equal(wizard.inputs[0].enum_values, '1, 2')
  assert.equal(wizard.inputs[0].min, '1')
  assert.equal(wizard.packages.npm_deps[0], 'vite')
  assert.equal(wizard.imports[0].with.retries, '2')
  assert.equal(wizard.hooks.before_run[0].if_expr, 'true')
  assert.equal(wizard.steps[0].tasks[0].cmd, 'go build ./...')
  assert.equal(wizard.steps[0].tasks[0].enabled, true)
})
