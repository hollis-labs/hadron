import type { WizardBlueprint, WizardInput, ParsedBlueprint } from '@/api/types';

export function indent(s: string, n: number): string {
  const pad = '  '.repeat(n);
  return s.split('\n').map(l => pad + l).join('\n');
}

export function yamlValue(v: string): string {
  if (!v) return '""';
  if (/[:{}[\],&#*?|<>=!%@`]/.test(v) || v.includes('\n') || v.startsWith("'") || v.startsWith('"')) {
    return JSON.stringify(v);
  }
  return v;
}

export function convertWizardToYaml(data: WizardBlueprint): string {
  const lines: string[] = [];
  lines.push(`version: "${data.version}"`);
  lines.push('');

  // blueprint
  const bp = data.blueprint;
  if (bp.name || bp.slug) {
    lines.push('blueprint:');
    if (bp.name) lines.push(`  name: ${yamlValue(bp.name)}`);
    if (bp.slug) lines.push(`  slug: ${yamlValue(bp.slug)}`);
    if (bp.title) lines.push(`  title: ${yamlValue(bp.title)}`);
    if (bp.description) lines.push(`  description: ${yamlValue(bp.description)}`);
    if (bp.author) lines.push(`  author: ${yamlValue(bp.author)}`);
    if (bp.license) lines.push(`  license: ${yamlValue(bp.license)}`);
    if (bp.homepage) lines.push(`  homepage: ${yamlValue(bp.homepage)}`);
    if (bp.tags.length > 0) lines.push(`  tags: [${bp.tags.map(t => yamlValue(t)).join(', ')}]`);
    lines.push('');
  }

  // project
  const proj = data.project;
  if (proj.type || proj.name || proj.dir) {
    lines.push('project:');
    if (proj.type) lines.push(`  type: ${yamlValue(proj.type)}`);
    if (proj.name) lines.push(`  name: ${yamlValue(proj.name)}`);
    if (proj.dir) lines.push(`  dir: ${yamlValue(proj.dir)}`);
    if (proj.path) lines.push(`  path: ${yamlValue(proj.path)}`);
    if (proj.php_version) lines.push(`  php_version: ${yamlValue(proj.php_version)}`);
    if (proj.node) lines.push('  node: true');
    if (Object.keys(proj.vars).length > 0) {
      lines.push('  vars:');
      for (const [k, v] of Object.entries(proj.vars)) {
        if (k) lines.push(`    ${k}: ${yamlValue(v)}`);
      }
    }
    lines.push('');
  }

  // env
  if (Object.keys(data.env).length > 0) {
    lines.push('env:');
    for (const [k, v] of Object.entries(data.env)) {
      if (k) lines.push(`  ${k}: ${yamlValue(v)}`);
    }
    lines.push('');
  }

  // inputs
  if (data.inputs.length > 0) {
    lines.push('inputs:');
    for (const inp of data.inputs) {
      if (!inp.name) continue;
      lines.push(`  - name: ${yamlValue(inp.name)}`);
      if (inp.label) lines.push(`    label: ${yamlValue(inp.label)}`);
      if (inp.description) lines.push(`    description: ${yamlValue(inp.description)}`);
      lines.push(`    type: ${inp.type}`);
      if (inp.required) lines.push('    required: true');
      if (inp.default_value) lines.push(`    default: ${yamlValue(inp.default_value)}`);
      if (inp.enum_values) {
        const vals = inp.enum_values.split(',').map(s => s.trim()).filter(Boolean);
        if (vals.length > 0) lines.push(`    enum: [${vals.map(v => yamlValue(v)).join(', ')}]`);
      }
      if (inp.pattern) lines.push(`    pattern: ${yamlValue(inp.pattern)}`);
      if (inp.min_length) lines.push(`    min_length: ${inp.min_length}`);
      if (inp.max_length) lines.push(`    max_length: ${inp.max_length}`);
      if (inp.min) lines.push(`    min: ${inp.min}`);
      if (inp.max) lines.push(`    max: ${inp.max}`);
      if (inp.items_type) lines.push(`    items_type: ${inp.items_type}`);
    }
    lines.push('');
  }

  // packages
  const pkg = data.packages;
  const hasPackages = pkg.npm_deps.length > 0 || pkg.npm_dev.length > 0 ||
    pkg.composer_require.length > 0 || pkg.composer_dev.length > 0 ||
    pkg.pip_deps.length > 0 || pkg.pip_dev.length > 0 ||
    pkg.brew_formulae.length > 0 || pkg.brew_casks.length > 0 ||
    pkg.go_tools.length > 0;
  if (hasPackages) {
    lines.push('packages:');
    if (pkg.npm_deps.length > 0 || pkg.npm_dev.length > 0) {
      lines.push('  npm:');
      if (pkg.npm_deps.length > 0) lines.push(`    deps: [${pkg.npm_deps.map(v => yamlValue(v)).join(', ')}]`);
      if (pkg.npm_dev.length > 0) lines.push(`    dev: [${pkg.npm_dev.map(v => yamlValue(v)).join(', ')}]`);
    }
    if (pkg.composer_require.length > 0 || pkg.composer_dev.length > 0) {
      lines.push('  composer:');
      if (pkg.composer_require.length > 0) lines.push(`    require: [${pkg.composer_require.map(v => yamlValue(v)).join(', ')}]`);
      if (pkg.composer_dev.length > 0) lines.push(`    require_dev: [${pkg.composer_dev.map(v => yamlValue(v)).join(', ')}]`);
    }
    if (pkg.pip_deps.length > 0 || pkg.pip_dev.length > 0) {
      lines.push('  pip:');
      if (pkg.pip_deps.length > 0) lines.push(`    deps: [${pkg.pip_deps.map(v => yamlValue(v)).join(', ')}]`);
      if (pkg.pip_dev.length > 0) lines.push(`    dev: [${pkg.pip_dev.map(v => yamlValue(v)).join(', ')}]`);
    }
    if (pkg.brew_formulae.length > 0 || pkg.brew_casks.length > 0) {
      lines.push('  brew:');
      if (pkg.brew_formulae.length > 0) lines.push(`    formulae: [${pkg.brew_formulae.map(v => yamlValue(v)).join(', ')}]`);
      if (pkg.brew_casks.length > 0) lines.push(`    casks: [${pkg.brew_casks.map(v => yamlValue(v)).join(', ')}]`);
    }
    if (pkg.go_tools.length > 0) {
      lines.push('  go:');
      lines.push(`    tools: [${pkg.go_tools.map(v => yamlValue(v)).join(', ')}]`);
    }
    lines.push('');
  }

  // git
  if (data.git?.init || data.git?.create_github_repo || data.git?.remote) {
    lines.push('git:');
    if (data.git.init) lines.push('  init: true');
    if (data.git.create_github_repo) lines.push('  create_github_repo: true');
    if (data.git.visibility) lines.push(`  visibility: ${yamlValue(data.git.visibility)}`);
    if (data.git.remote) lines.push(`  remote: ${yamlValue(data.git.remote)}`);
    if (data.git.branch) lines.push(`  branch: ${yamlValue(data.git.branch)}`);
    lines.push('');
  }

  // stubs
  if (data.stubs?.enabled) {
    lines.push('stubs:');
    lines.push('  enabled: true');
    if (data.stubs.strict_match) lines.push('  strict_match: true');
    if (data.stubs.search_paths?.length > 0) {
      lines.push('  search_paths:');
      for (const sp of data.stubs.search_paths) {
        if (sp) lines.push(`    - ${yamlValue(sp)}`);
      }
    }
    lines.push('');
  }

  // imports
  if (data.imports?.length > 0) {
    const validImports = data.imports.filter(imp => imp.path);
    if (validImports.length > 0) {
      lines.push('imports:');
      for (const imp of validImports) {
        lines.push(`  - path: ${yamlValue(imp.path)}`);
        if (imp.alias) lines.push(`    alias: ${yamlValue(imp.alias)}`);
        const withEntries = Object.entries(imp.with || {}).filter(([k]) => k);
        if (withEntries.length > 0) {
          lines.push('    with:');
          for (const [k, v] of withEntries) {
            lines.push(`      ${k}: ${yamlValue(v)}`);
          }
        }
      }
      lines.push('');
    }
  }

  // hooks
  const hasHooks = (data.hooks?.before_run?.length > 0) ||
    (data.hooks?.after_run?.length > 0) || (data.hooks?.on_error?.length > 0);
  if (hasHooks) {
    lines.push('hooks:');
    for (const [bucket, hooks] of [
      ['before_run', data.hooks.before_run],
      ['after_run', data.hooks.after_run],
      ['on_error', data.hooks.on_error],
    ] as const) {
      const validHooks = (hooks || []).filter(h => h.name || h.cmd);
      if (validHooks.length > 0) {
        lines.push(`  ${bucket}:`);
        for (const h of validHooks) {
          lines.push(`    - name: ${yamlValue(h.name)}`);
          lines.push(`      cmd: ${yamlValue(h.cmd)}`);
          if (h.if_expr) lines.push(`      if: ${yamlValue(h.if_expr)}`);
        }
      }
    }
    lines.push('');
  }

  // steps
  lines.push('steps:');
  for (const sec of data.steps) {
    lines.push(`  - section: ${yamlValue(sec.section || 'default')}`);
    lines.push('    tasks:');
    for (const task of sec.tasks) {
      if (!task.name && !task.cmd && !task.call) continue;
      lines.push(`      - name: ${yamlValue(task.name)}`);
      if (task.cmd) {
        if (task.cmd.includes('\n')) {
          lines.push('        cmd: |');
          lines.push(indent(task.cmd, 5));
        } else {
          lines.push(`        cmd: ${yamlValue(task.cmd)}`);
        }
      }
      if (task.call) lines.push(`        call: ${yamlValue(task.call)}`);
      if (task.dir) lines.push(`        dir: ${yamlValue(task.dir)}`);
      if (task.if_expr) lines.push(`        if: ${yamlValue(task.if_expr)}`);
      if (!task.enabled) lines.push('        enabled: false');
      const retry = parseInt(task.retry);
      if (retry > 0) lines.push(`        retry: ${retry}`);
      const retryDelay = parseInt(task.retry_delay_seconds);
      if (retryDelay > 0) lines.push(`        retry_delay_seconds: ${retryDelay}`);
      const timeout = parseInt(task.timeout_seconds);
      if (timeout > 0) lines.push(`        timeout_seconds: ${timeout}`);
      if (task.continue_on_error) lines.push('        continue_on_error: true');
      if (Object.keys(task.env).length > 0) {
        lines.push('        env:');
        for (const [k, v] of Object.entries(task.env)) {
          if (k) lines.push(`          ${k}: ${yamlValue(v)}`);
        }
      }
      if (task.on_success?.length > 0) {
        const validHooks = task.on_success.filter(h => h.value);
        if (validHooks.length > 0) {
          lines.push('        on_success:');
          for (const h of validHooks) {
            lines.push(`          - type: ${h.type}`);
            lines.push(`            value: ${yamlValue(h.value)}`);
          }
        }
      }
      if (task.on_fail?.length > 0) {
        const validHooks = task.on_fail.filter(h => h.value);
        if (validHooks.length > 0) {
          lines.push('        on_fail:');
          for (const h of validHooks) {
            lines.push(`          - type: ${h.type}`);
            lines.push(`            value: ${yamlValue(h.value)}`);
          }
        }
      }
    }
  }

  return lines.join('\n') + '\n';
}

export function convertParsedToWizard(bp: ParsedBlueprint): WizardBlueprint {
  return {
    version: bp.version || '0.4',
    blueprint: { ...bp.blueprint, tags: bp.blueprint.tags || [] },
    project: {
      type: bp.project?.type || '', name: bp.project?.name || '',
      dir: bp.project?.dir || '', path: bp.project?.path || '',
      php_version: bp.project?.php_version || '', node: bp.project?.node || false,
      vars: Object.fromEntries(Object.entries(bp.project?.vars || {}).map(([k, v]) => [k, String(v)])),
    },
    env: bp.env || {},
    inputs: (bp.inputs || []).map(inp => ({
      name: inp.name, label: inp.label || '', description: inp.description || '',
      type: (inp.type as WizardInput['type']) || 'string',
      required: inp.required, default_value: inp.default != null ? String(inp.default) : '',
      enum_values: inp.enum ? inp.enum.join(', ') : '',
      pattern: inp.pattern || '',
      min_length: inp.min_length != null ? String(inp.min_length) : '',
      max_length: inp.max_length != null ? String(inp.max_length) : '',
      min: inp.min != null ? String(inp.min) : '',
      max: inp.max != null ? String(inp.max) : '',
      items_type: inp.items_type || '',
    })),
    packages: {
      composer_require: bp.packages?.composer?.require || [],
      composer_dev: bp.packages?.composer?.require_dev || [],
      npm_deps: bp.packages?.npm?.deps || [],
      npm_dev: bp.packages?.npm?.dev || [],
      pip_deps: bp.packages?.pip?.deps || [],
      pip_dev: bp.packages?.pip?.dev || [],
      brew_formulae: bp.packages?.brew?.formulae || [],
      brew_casks: bp.packages?.brew?.casks || [],
      go_tools: bp.packages?.go?.tools || [],
    },
    steps: (bp.steps || []).map(sec => ({
      section: sec.section,
      tasks: sec.tasks.map(t => ({
        name: t.name, cmd: t.cmd || t.run || '', call: t.call || '',
        if_expr: t.if || '', dir: t.dir || '', env: t.env || {},
        retry: String(t.retry || 0), retry_delay_seconds: String(t.retry_delay_seconds || 0),
        timeout_seconds: String(t.timeout_seconds || 0),
        continue_on_error: t.continue_on_error, enabled: t.enabled !== false,
        on_success: (t.on_success || []).map(h => ({ type: h.type || 'cmd', value: h.value || '' })),
        on_fail: (t.on_fail || []).map(h => ({ type: h.type || 'cmd', value: h.value || '' })),
      })),
    })),
    git: {
      init: bp.git?.init || false,
      create_github_repo: bp.git?.create_github_repo || false,
      visibility: bp.git?.visibility || '',
      remote: bp.git?.remote || '',
      branch: bp.git?.branch || '',
    },
    stubs: {
      enabled: bp.stubs?.enabled || false,
      search_paths: bp.stubs?.search_paths || [],
      strict_match: bp.stubs?.strict_match || false,
    },
    imports: (bp.imports || []).map(imp => ({
      path: imp.path || '',
      alias: imp.alias || '',
      with: Object.fromEntries(
        Object.entries(imp.with || {}).map(([k, v]) => [k, String(v)])
      ),
    })),
    hooks: {
      before_run: (bp.hooks?.before_run || []).map(h => ({
        name: h.name || '', cmd: h.cmd || '', if_expr: h.if || '',
      })),
      after_run: (bp.hooks?.after_run || []).map(h => ({
        name: h.name || '', cmd: h.cmd || '', if_expr: h.if || '',
      })),
      on_error: (bp.hooks?.on_error || []).map(h => ({
        name: h.name || '', cmd: h.cmd || '', if_expr: h.if || '',
      })),
    },
  };
}
