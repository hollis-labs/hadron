import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';
import { ChevronLeft, Plus, Trash2 } from 'lucide-react';
import { parseBlueprintFull, saveBlueprintFile, createBlueprintFile, getPreference } from '../api/client';
import type { WizardBlueprint, WizardInput, WizardTask, ParsedBlueprint } from '../api/types';

interface BlueprintWizardPageProps {
  editPath: string | null;
  onBack: () => void;
}

const WIZARD_STEPS = [
  { key: 'metadata', title: '1. Metadata', desc: 'Name, tags' },
  { key: 'project', title: '2. Project', desc: 'Type & config' },
  { key: 'env', title: '3. Env', desc: 'Env variables' },
  { key: 'packages', title: '4. Packages', desc: 'Dependencies' },
  { key: 'inputs', title: '5. Inputs', desc: 'Parameters' },
  { key: 'steps', title: '6. Steps', desc: 'Tasks' },
  { key: 'advanced', title: '7. Advanced', desc: 'Git, stubs, imports, hooks' },
  { key: 'review', title: '8. Review', desc: 'Preview & save' },
];

const AUTOSAVE_KEY = 'hadron_wizard_draft';

function newWizardInput(): WizardInput {
  return { name: '', label: '', description: '', type: 'string', required: false, default_value: '', enum_values: '', pattern: '', min_length: '', max_length: '', min: '', max: '', items_type: '' };
}

function newWizardTask(): WizardTask {
  return { name: '', cmd: '', call: '', if_expr: '', dir: '', env: {}, retry: '0', retry_delay_seconds: '0', timeout_seconds: '0', continue_on_error: false, enabled: true, on_success: [], on_fail: [] };
}

function defaultWizard(): WizardBlueprint {
  return {
    version: '0.4',
    blueprint: { name: '', slug: '', title: '', description: '', author: '', license: '', tags: [], homepage: '' },
    project: { type: '', name: '', dir: '', path: '', php_version: '', node: false, vars: {} },
    env: {},
    inputs: [],
    packages: { composer_require: [], composer_dev: [], npm_deps: [], npm_dev: [], pip_deps: [], pip_dev: [], brew_formulae: [], brew_casks: [], go_tools: [] },
    steps: [{ section: 'default', tasks: [newWizardTask()] }],
    git: { init: false, create_github_repo: false, visibility: '', remote: '', branch: '' },
    stubs: { enabled: false, search_paths: [], strict_match: false },
    imports: [],
    hooks: { before_run: [], after_run: [], on_error: [] },
  };
}

// ── YAML export ──────────────────────────────────────────────────────

function indent(s: string, n: number): string {
  const pad = '  '.repeat(n);
  return s.split('\n').map(l => pad + l).join('\n');
}

function yamlValue(v: string): string {
  if (!v) return '""';
  if (/[:{}\[\],&#*?|<>=!%@`]/.test(v) || v.includes('\n') || v.startsWith("'") || v.startsWith('"')) {
    return JSON.stringify(v);
  }
  return v;
}

function convertWizardToYaml(data: WizardBlueprint): string {
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

// ── Convert parsed blueprint to wizard format ────────────────────────

function convertParsedToWizard(bp: ParsedBlueprint): WizardBlueprint {
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

// ── Key-value list editor ────────────────────────────────────────────

function KVEditor({ data, onChange, keyPlaceholder, valuePlaceholder }: {
  data: Record<string, string>;
  onChange: (data: Record<string, string>) => void;
  keyPlaceholder?: string;
  valuePlaceholder?: string;
}) {
  const entries = Object.entries(data);
  const updateKey = (oldKey: string, newKey: string, value: string) => {
    const next = { ...data };
    delete next[oldKey];
    next[newKey] = value;
    onChange(next);
  };
  const updateValue = (key: string, value: string) => {
    onChange({ ...data, [key]: value });
  };
  const remove = (key: string) => {
    const next = { ...data };
    delete next[key];
    onChange(next);
  };
  const add = () => {
    onChange({ ...data, '': '' });
  };

  return (
    <div>
      {entries.map(([key, value], i) => (
        <div key={i} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: '0.5rem', marginBottom: '0.4rem', alignItems: 'center' }}>
          <input className="hud-input" value={key} placeholder={keyPlaceholder || 'key'} onChange={e => updateKey(key, e.target.value, value)} />
          <input className="hud-input" value={value} placeholder={valuePlaceholder || 'value'} onChange={e => updateValue(key, e.target.value)} />
          <button className="btn btn-ghost" onClick={() => remove(key)} style={{ padding: '0.3rem' }}><Trash2 size={13} style={{ color: 'var(--status-failed)' }} /></button>
        </div>
      ))}
      <button className="wizard-add-btn" onClick={add}><Plus size={14} /> Add</button>
    </div>
  );
}

// ── Package list editor ──────────────────────────────────────────────

function PackageList({ items, onAdd, onRemove, placeholder }: {
  items: string[]; onAdd: (v: string) => void; onRemove: (i: number) => void; placeholder: string;
}) {
  const [val, setVal] = useState('');
  return (
    <div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.3rem', marginBottom: '0.4rem' }}>
        {items.map((pkg, i) => (
          <span key={i} className="bp-tag">
            {pkg}
            <button onClick={() => onRemove(i)} style={{ marginLeft: '4px', cursor: 'pointer', background: 'none', border: 'none', color: 'inherit', fontSize: '12px' }}>&times;</button>
          </span>
        ))}
      </div>
      <input className="hud-input" value={val} placeholder={placeholder} style={{ width: '100%' }}
        onKeyDown={e => { if (e.key === 'Enter' && val.trim()) { onAdd(val.trim()); setVal(''); e.preventDefault(); } }}
        onChange={e => setVal(e.target.value)} />
    </div>
  );
}

// ── Main wizard ──────────────────────────────────────────────────────

export function BlueprintWizardPage({ editPath, onBack }: BlueprintWizardPageProps) {
  const [data, setData] = useState<WizardBlueprint>(defaultWizard);
  const [currentStep, setCurrentStep] = useState(0);
  const [saving, setSaving] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [newTag, setNewTag] = useState('');

  // Load data on mount
  useEffect(() => {
    if (editPath) {
      parseBlueprintFull(editPath).then(bp => {
        setData(convertParsedToWizard(bp));
        setLoaded(true);
      }).catch(err => {
        toast.error(`Failed to load blueprint: ${err}`);
        setLoaded(true);
      });
    } else {
      const saved = localStorage.getItem(AUTOSAVE_KEY);
      if (saved) {
        try { setData(JSON.parse(saved)); } catch { /* use defaults */ }
      }
      setLoaded(true);
    }
  }, [editPath]);

  // Auto-save draft (new blueprints only)
  useEffect(() => {
    if (editPath || !loaded) return;
    const timer = setTimeout(() => {
      localStorage.setItem(AUTOSAVE_KEY, JSON.stringify(data));
    }, 1000);
    return () => clearTimeout(timer);
  }, [data, editPath, loaded]);

  const updateBlueprint = useCallback((field: string, value: unknown) => {
    setData(prev => ({ ...prev, blueprint: { ...prev.blueprint, [field]: value } }));
  }, []);

  const updateProject = useCallback((field: string, value: unknown) => {
    setData(prev => ({ ...prev, project: { ...prev.project, [field]: value } }));
  }, []);

  // ── Save handler ─────────────────────────────────────────────────

  const handleSave = async () => {
    setSaving(true);
    try {
      const yaml = convertWizardToYaml(data);
      if (editPath) {
        await saveBlueprintFile(editPath, yaml);
        toast.success('Blueprint saved');
      } else {
        const filename = data.blueprint.slug || data.blueprint.name.toLowerCase().replace(/\s+/g, '-');
        if (!filename) {
          toast.error('Blueprint name or slug is required');
          setCurrentStep(0);
          setSaving(false);
          return;
        }
        const dir = await getPreference('lastBlueprintDir');
        if (!dir) {
          toast.error('Open a blueprint directory first');
          setSaving(false);
          return;
        }
        await createBlueprintFile(dir, filename, yaml);
        toast.success('Blueprint created: ' + filename);
        localStorage.removeItem(AUTOSAVE_KEY);
      }
      onBack();
    } catch (err: unknown) {
      toast.error(`Save failed: ${err}`);
    } finally {
      setSaving(false);
    }
  };

  // ── Step renderers ───────────────────────────────────────────────

  const renderMetadata = () => {
    return (
      <div>
        <h3 style={{ fontSize: 'var(--text-md)', color: 'var(--status-success)', marginBottom: '1rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Metadata</h3>
        <div className="wizard-grid-2">
          <div className="wizard-field">
            <label>Name *</label>
            <input className="hud-input" value={data.blueprint.name} onChange={e => updateBlueprint('name', e.target.value)}
              onBlur={() => { if (!data.blueprint.slug && data.blueprint.name) updateBlueprint('slug', data.blueprint.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')); }}
              placeholder="my-blueprint" />
          </div>
          <div className="wizard-field">
            <label>Slug</label>
            <input className="hud-input" value={data.blueprint.slug} onChange={e => updateBlueprint('slug', e.target.value)} placeholder="my-blueprint" />
          </div>
        </div>
        <div className="wizard-field">
          <label>Title</label>
          <input className="hud-input" value={data.blueprint.title} onChange={e => updateBlueprint('title', e.target.value)} placeholder="Human-readable title" style={{ width: '100%' }} />
        </div>
        <div className="wizard-field">
          <label>Description</label>
          <textarea className="hud-input" rows={3} value={data.blueprint.description} onChange={e => updateBlueprint('description', e.target.value)} placeholder="What this blueprint does" style={{ width: '100%', resize: 'vertical' }} />
        </div>
        <div className="wizard-grid-2">
          <div className="wizard-field">
            <label>Author</label>
            <input className="hud-input" value={data.blueprint.author} onChange={e => updateBlueprint('author', e.target.value)} />
          </div>
          <div className="wizard-field">
            <label>License</label>
            <input className="hud-input" value={data.blueprint.license} onChange={e => updateBlueprint('license', e.target.value)} placeholder="MIT" />
          </div>
        </div>
        <div className="wizard-field">
          <label>Homepage</label>
          <input className="hud-input" value={data.blueprint.homepage} onChange={e => updateBlueprint('homepage', e.target.value)} placeholder="https://..." style={{ width: '100%' }} />
        </div>
        <div className="wizard-field">
          <label>Tags</label>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem', alignItems: 'center' }}>
            {data.blueprint.tags.map((tag, i) => (
              <span key={i} className="bp-tag">
                {tag}
                <button onClick={() => updateBlueprint('tags', data.blueprint.tags.filter((_, j) => j !== i))}
                  style={{ marginLeft: '4px', cursor: 'pointer', background: 'none', border: 'none', color: 'inherit' }}>&times;</button>
              </span>
            ))}
            <input className="hud-input" placeholder="Add tag..." value={newTag}
              onChange={e => setNewTag(e.target.value)}
              onKeyDown={e => {
                if (e.key === 'Enter' && newTag.trim()) {
                  e.preventDefault();
                  updateBlueprint('tags', [...data.blueprint.tags, newTag.trim()]);
                  setNewTag('');
                }
              }}
              style={{ width: '120px', fontSize: '12px' }} />
          </div>
        </div>
      </div>
    );
  };

  const renderProject = () => (
    <div>
      <h3 style={{ fontSize: 'var(--text-md)', color: 'var(--status-success)', marginBottom: '1rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Project Configuration</h3>
      <div className="wizard-grid-2">
        <div className="wizard-field">
          <label>Project Type</label>
          <input className="hud-input" value={data.project.type} onChange={e => updateProject('type', e.target.value)} placeholder="webapp, api, cli..." list="project-types" />
          <datalist id="project-types"><option value="webapp" /><option value="api" /><option value="cli" /><option value="library" /><option value="script" /></datalist>
        </div>
        <div className="wizard-field">
          <label>Project Name</label>
          <input className="hud-input" value={data.project.name} onChange={e => updateProject('name', e.target.value)} />
        </div>
      </div>
      <div className="wizard-grid-2">
        <div className="wizard-field">
          <label>Directory</label>
          <input className="hud-input" value={data.project.dir} onChange={e => updateProject('dir', e.target.value)} />
        </div>
        <div className="wizard-field">
          <label>Path</label>
          <input className="hud-input" value={data.project.path} onChange={e => updateProject('path', e.target.value)} />
        </div>
      </div>
      <div className="wizard-grid-2">
        <div className="wizard-field">
          <label>PHP Version</label>
          <input className="hud-input" value={data.project.php_version} onChange={e => updateProject('php_version', e.target.value)} placeholder="8.2" />
        </div>
        <div className="wizard-field" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginTop: '1.2rem' }}>
          <input type="checkbox" checked={data.project.node} onChange={e => updateProject('node', e.target.checked)} style={{ accentColor: 'var(--status-success)' }} />
          <span style={{ fontSize: 'var(--text-md)' }}>Node.js project</span>
        </div>
      </div>
      <div className="wizard-field" style={{ marginTop: '0.5rem' }}>
        <label>Custom Variables</label>
        <KVEditor data={data.project.vars} onChange={vars => updateProject('vars', vars)} keyPlaceholder="variable" valuePlaceholder="value" />
      </div>
    </div>
  );

  const renderEnv = () => (
    <div>
      <h3 style={{ fontSize: 'var(--text-md)', color: 'var(--status-success)', marginBottom: '0.5rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Environment Variables</h3>
      <p style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginBottom: '1rem' }}>
        {'Available to all tasks via {{ .env.KEY }}.'}
      </p>
      <KVEditor data={data.env} onChange={env => setData(prev => ({ ...prev, env }))} keyPlaceholder="ENV_VAR" valuePlaceholder="value" />
    </div>
  );

  const renderPackages = () => {
    const updatePkg = (field: string, value: string[]) => {
      setData(prev => ({ ...prev, packages: { ...prev.packages, [field]: value } }));
    };
    const pkgSections = [
      { label: 'npm', deps: 'npm_deps', dev: 'npm_dev' },
      { label: 'composer', deps: 'composer_require', dev: 'composer_dev' },
      { label: 'pip', deps: 'pip_deps', dev: 'pip_dev' },
    ] as const;

    return (
      <div>
        <h3 style={{ fontSize: 'var(--text-md)', color: 'var(--status-success)', marginBottom: '0.5rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Packages</h3>
        <p style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginBottom: '1rem' }}>Declare dependencies. Press Enter to add.</p>
        {pkgSections.map(sec => (
          <div key={sec.label} style={{ marginBottom: '1rem' }}>
            <div className="hud-label" style={{ marginBottom: '0.3rem' }}>{sec.label}</div>
            <div className="wizard-grid-2">
              <div className="wizard-field">
                <label style={{ fontSize: 'var(--text-xs)' }}>Dependencies</label>
                <PackageList items={data.packages[sec.deps]} placeholder={`${sec.label} package`}
                  onAdd={v => updatePkg(sec.deps, [...data.packages[sec.deps], v])}
                  onRemove={i => updatePkg(sec.deps, data.packages[sec.deps].filter((_, j) => j !== i))} />
              </div>
              <div className="wizard-field">
                <label style={{ fontSize: 'var(--text-xs)' }}>Dev Dependencies</label>
                <PackageList items={data.packages[sec.dev]} placeholder={`${sec.label} dev package`}
                  onAdd={v => updatePkg(sec.dev, [...data.packages[sec.dev], v])}
                  onRemove={i => updatePkg(sec.dev, data.packages[sec.dev].filter((_, j) => j !== i))} />
              </div>
            </div>
          </div>
        ))}
        <div style={{ marginBottom: '1rem' }}>
          <div className="hud-label" style={{ marginBottom: '0.3rem' }}>brew</div>
          <div className="wizard-grid-2">
            <div className="wizard-field">
              <label style={{ fontSize: 'var(--text-xs)' }}>Formulae</label>
              <PackageList items={data.packages.brew_formulae} placeholder="formula"
                onAdd={v => updatePkg('brew_formulae', [...data.packages.brew_formulae, v])}
                onRemove={i => updatePkg('brew_formulae', data.packages.brew_formulae.filter((_, j) => j !== i))} />
            </div>
            <div className="wizard-field">
              <label style={{ fontSize: 'var(--text-xs)' }}>Casks</label>
              <PackageList items={data.packages.brew_casks} placeholder="cask"
                onAdd={v => updatePkg('brew_casks', [...data.packages.brew_casks, v])}
                onRemove={i => updatePkg('brew_casks', data.packages.brew_casks.filter((_, j) => j !== i))} />
            </div>
          </div>
        </div>
        <div>
          <div className="hud-label" style={{ marginBottom: '0.3rem' }}>go</div>
          <PackageList items={data.packages.go_tools} placeholder="go tool"
            onAdd={v => updatePkg('go_tools', [...data.packages.go_tools, v])}
            onRemove={i => updatePkg('go_tools', data.packages.go_tools.filter((_, j) => j !== i))} />
        </div>
      </div>
    );
  };

  const renderInputs = () => {
    const addInput = () => setData(prev => ({ ...prev, inputs: [...prev.inputs, newWizardInput()] }));
    const removeInput = (idx: number) => setData(prev => ({ ...prev, inputs: prev.inputs.filter((_, i) => i !== idx) }));
    const updateInput = (idx: number, field: string, value: unknown) => {
      setData(prev => ({ ...prev, inputs: prev.inputs.map((inp, i) => i === idx ? { ...inp, [field]: value } : inp) }));
    };

    return (
      <div>
        <h3 style={{ fontSize: 'var(--text-md)', color: 'var(--status-success)', marginBottom: '0.5rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Inputs</h3>
        <p style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginBottom: '1rem' }}>
          {'Parameters users provide when running. Available as {{ .inputs.name }}.'}
        </p>
        {data.inputs.map((inp, idx) => (
          <div key={idx} className="wizard-list-item">
            <div className="wizard-list-item-header">
              <span style={{ fontSize: 'var(--text-md)', color: 'var(--accent)' }}>Input {idx + 1}{inp.name ? `: ${inp.name}` : ''}</span>
              <button className="btn btn-ghost" onClick={() => removeInput(idx)} style={{ padding: '0.2rem' }}>
                <Trash2 size={13} style={{ color: 'var(--status-failed)' }} />
              </button>
            </div>
            <div className="wizard-grid-2">
              <div className="wizard-field"><label>Name *</label><input className="hud-input" value={inp.name} onChange={e => updateInput(idx, 'name', e.target.value)} placeholder="input_name" /></div>
              <div className="wizard-field"><label>Type *</label>
                <select className="hud-input" value={inp.type} onChange={e => updateInput(idx, 'type', e.target.value)}>
                  <option value="string">string</option><option value="number">number</option><option value="boolean">boolean</option><option value="array">array</option>
                </select>
              </div>
            </div>
            <div className="wizard-grid-2">
              <div className="wizard-field"><label>Label</label><input className="hud-input" value={inp.label} onChange={e => updateInput(idx, 'label', e.target.value)} placeholder="Human label" /></div>
              <div className="wizard-field"><label>Description</label><input className="hud-input" value={inp.description} onChange={e => updateInput(idx, 'description', e.target.value)} placeholder="Help text" /></div>
            </div>
            <div className="wizard-grid-2">
              <div className="wizard-field" style={{ flexDirection: 'row', alignItems: 'center', gap: '0.5rem' }}>
                <input type="checkbox" checked={inp.required} onChange={e => updateInput(idx, 'required', e.target.checked)} style={{ accentColor: 'var(--status-success)' }} />
                <span style={{ fontSize: 'var(--text-sm)' }}>Required</span>
              </div>
              <div className="wizard-field"><label>Default</label><input className="hud-input" value={inp.default_value} onChange={e => updateInput(idx, 'default_value', e.target.value)} /></div>
            </div>
            <div className="wizard-field"><label>Enum (comma-separated)</label><input className="hud-input" value={inp.enum_values} onChange={e => updateInput(idx, 'enum_values', e.target.value)} placeholder="opt1, opt2, opt3" style={{ width: '100%' }} /></div>
            <div className="wizard-grid-2">
              <div className="wizard-field"><label>Pattern (regex)</label><input className="hud-input" value={inp.pattern} onChange={e => updateInput(idx, 'pattern', e.target.value)} /></div>
              {inp.type === 'array' && <div className="wizard-field"><label>Items Type</label>
                <select className="hud-input" value={inp.items_type} onChange={e => updateInput(idx, 'items_type', e.target.value)}>
                  <option value="">any</option><option value="string">string</option><option value="number">number</option>
                </select>
              </div>}
            </div>
          </div>
        ))}
        <button className="wizard-add-btn" onClick={addInput}><Plus size={14} /> Add Input</button>
      </div>
    );
  };

  const renderSteps = () => {
    const addSection = () => setData(prev => ({ ...prev, steps: [...prev.steps, { section: '', tasks: [newWizardTask()] }] }));
    const removeSection = (si: number) => { if (data.steps.length <= 1) return; setData(prev => ({ ...prev, steps: prev.steps.filter((_, i) => i !== si) })); };
    const updateSectionName = (si: number, name: string) => setData(prev => ({ ...prev, steps: prev.steps.map((s, i) => i === si ? { ...s, section: name } : s) }));
    const addTask = (si: number) => setData(prev => ({ ...prev, steps: prev.steps.map((s, i) => i === si ? { ...s, tasks: [...s.tasks, newWizardTask()] } : s) }));
    const removeTask = (si: number, ti: number) => setData(prev => ({ ...prev, steps: prev.steps.map((s, i) => i === si ? { ...s, tasks: s.tasks.filter((_, j) => j !== ti) } : s) }));
    const updateTask = (si: number, ti: number, field: string, value: unknown) => {
      setData(prev => ({
        ...prev, steps: prev.steps.map((s, i) => i === si ? {
          ...s, tasks: s.tasks.map((t, j) => j === ti ? { ...t, [field]: value } : t)
        } : s)
      }));
    };

    type TaskHookType = 'on_success' | 'on_fail';
    const addTaskHook = (sectionIndex: number, taskIndex: number, hookType: TaskHookType) => {
      setData(prev => {
        const steps = [...prev.steps];
        const tasks = [...steps[sectionIndex].tasks];
        const task = { ...tasks[taskIndex] };
        task[hookType] = [...(task[hookType] || []), { type: 'cmd', value: '' }];
        tasks[taskIndex] = task;
        steps[sectionIndex] = { ...steps[sectionIndex], tasks };
        return { ...prev, steps };
      });
    };
    const updateTaskHook = (sectionIndex: number, taskIndex: number, hookType: TaskHookType, hookIndex: number, field: 'type' | 'value', value: string) => {
      setData(prev => {
        const steps = [...prev.steps];
        const tasks = [...steps[sectionIndex].tasks];
        const task = { ...tasks[taskIndex] };
        const hooks = [...(task[hookType] || [])];
        hooks[hookIndex] = { ...hooks[hookIndex], [field]: value };
        task[hookType] = hooks;
        tasks[taskIndex] = task;
        steps[sectionIndex] = { ...steps[sectionIndex], tasks };
        return { ...prev, steps };
      });
    };
    const removeTaskHook = (sectionIndex: number, taskIndex: number, hookType: TaskHookType, hookIndex: number) => {
      setData(prev => {
        const steps = [...prev.steps];
        const tasks = [...steps[sectionIndex].tasks];
        const task = { ...tasks[taskIndex] };
        task[hookType] = (task[hookType] || []).filter((_, i) => i !== hookIndex);
        tasks[taskIndex] = task;
        steps[sectionIndex] = { ...steps[sectionIndex], tasks };
        return { ...prev, steps };
      });
    };

    return (
      <div>
        <h3 style={{ fontSize: 'var(--text-md)', color: 'var(--status-success)', marginBottom: '0.5rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Steps & Tasks</h3>
        <p style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginBottom: '1rem' }}>Organize tasks into sections. Tasks execute in order.</p>
        {data.steps.map((section, si) => (
          <div key={si} className="wizard-list-item">
            <div className="wizard-list-item-header">
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <span className="hud-label">Section</span>
                <input className="hud-input" value={section.section} onChange={e => updateSectionName(si, e.target.value)} placeholder="section-name" style={{ width: '200px' }} />
              </div>
              <button className="btn btn-ghost" onClick={() => removeSection(si)} disabled={data.steps.length <= 1} style={{ padding: '0.2rem' }}>
                <Trash2 size={13} />
              </button>
            </div>

            {section.tasks.map((task, ti) => (
              <div key={ti} style={{ background: 'var(--bg-base)', border: '1px solid var(--border-default)', borderRadius: '4px', padding: '0.75rem', marginBottom: '0.5rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                  <span style={{ fontSize: 'var(--text-sm)', color: 'var(--accent)' }}>Task {ti + 1}{task.name ? `: ${task.name}` : ''}</span>
                  <button className="btn btn-ghost" onClick={() => removeTask(si, ti)} style={{ padding: '0.2rem' }}>
                    <Trash2 size={12} style={{ color: 'var(--status-failed)' }} />
                  </button>
                </div>
                <div className="wizard-field">
                  <label>Task Name</label>
                  <input className="hud-input" value={task.name} onChange={e => updateTask(si, ti, 'name', e.target.value)} placeholder="task-name" style={{ width: '100%', fontFamily: 'monospace' }} />
                </div>
                <div className="wizard-field">
                  <label>Command</label>
                  <textarea className="hud-input" rows={2} value={task.cmd} onChange={e => updateTask(si, ti, 'cmd', e.target.value)}
                    placeholder="echo 'hello world'" style={{ width: '100%', fontFamily: 'monospace', resize: 'vertical' }} />
                </div>
                <div className="wizard-grid-2">
                  <div className="wizard-field"><label>Working Dir</label><input className="hud-input" value={task.dir} onChange={e => updateTask(si, ti, 'dir', e.target.value)} /></div>
                  <div className="wizard-field" style={{ flexDirection: 'row', alignItems: 'center', gap: '0.5rem', marginTop: '1.2rem' }}>
                    <input type="checkbox" checked={task.enabled} onChange={e => updateTask(si, ti, 'enabled', e.target.checked)} style={{ accentColor: 'var(--status-success)' }} />
                    <span style={{ fontSize: 'var(--text-sm)' }}>Enabled</span>
                  </div>
                </div>
                <div className="wizard-field"><label>Condition (if)</label><input className="hud-input" value={task.if_expr} onChange={e => updateTask(si, ti, 'if_expr', e.target.value)} placeholder={'{{ eq .inputs.skip false }}'} style={{ width: '100%', fontFamily: 'monospace' }} /></div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '0.5rem' }}>
                  <div className="wizard-field"><label>Retry</label><input className="hud-input" type="number" min="0" value={task.retry} onChange={e => updateTask(si, ti, 'retry', e.target.value)} /></div>
                  <div className="wizard-field"><label>Retry Delay (s)</label><input className="hud-input" type="number" min="0" value={task.retry_delay_seconds} onChange={e => updateTask(si, ti, 'retry_delay_seconds', e.target.value)} /></div>
                  <div className="wizard-field"><label>Timeout (s)</label><input className="hud-input" type="number" min="0" value={task.timeout_seconds} onChange={e => updateTask(si, ti, 'timeout_seconds', e.target.value)} /></div>
                </div>
                <div className="wizard-field" style={{ flexDirection: 'row', alignItems: 'center', gap: '0.5rem' }}>
                  <input type="checkbox" checked={task.continue_on_error} onChange={e => updateTask(si, ti, 'continue_on_error', e.target.checked)} style={{ accentColor: 'var(--status-success)' }} />
                  <span style={{ fontSize: 'var(--text-sm)' }}>Continue on error</span>
                </div>
                {/* Per-task hooks */}
                <details style={{ marginTop: '0.5rem' }}>
                  <summary className="hud-label" style={{ cursor: 'pointer', fontSize: 'var(--text-sm)' }}>
                    Hooks
                    {((task.on_success?.length || 0) + (task.on_fail?.length || 0)) > 0 &&
                      <span style={{ color: 'var(--accent)', marginLeft: '0.3rem' }}>
                        ({(task.on_success?.length || 0) + (task.on_fail?.length || 0)})
                      </span>
                    }
                  </summary>
                  <div style={{ padding: '0.5rem 0', display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                    <div>
                      <div className="hud-label" style={{ fontSize: 'var(--text-xs)', marginBottom: '0.2rem' }}>On Success</div>
                      {(task.on_success || []).map((hook, hi) => (
                        <div key={hi} style={{ display: 'grid', gridTemplateColumns: 'auto 1fr auto', gap: '0.4rem', marginBottom: '0.3rem' }}>
                          <select className="hud-input" value={hook.type} onChange={e => updateTaskHook(si, ti, 'on_success', hi, 'type', e.target.value)} style={{ width: 'auto' }}>
                            <option value="cmd">cmd</option><option value="call">call</option>
                          </select>
                          <input className="hud-input" value={hook.value} onChange={e => updateTaskHook(si, ti, 'on_success', hi, 'value', e.target.value)} placeholder="command or alias" />
                          <button className="btn btn-ghost" onClick={() => removeTaskHook(si, ti, 'on_success', hi)}><Trash2 size={12} /></button>
                        </div>
                      ))}
                      <button className="wizard-add-btn" onClick={() => addTaskHook(si, ti, 'on_success')}><Plus size={12} /> Add</button>
                    </div>
                    <div>
                      <div className="hud-label" style={{ fontSize: 'var(--text-xs)', marginBottom: '0.2rem' }}>On Fail</div>
                      {(task.on_fail || []).map((hook, hi) => (
                        <div key={hi} style={{ display: 'grid', gridTemplateColumns: 'auto 1fr auto', gap: '0.4rem', marginBottom: '0.3rem' }}>
                          <select className="hud-input" value={hook.type} onChange={e => updateTaskHook(si, ti, 'on_fail', hi, 'type', e.target.value)} style={{ width: 'auto' }}>
                            <option value="cmd">cmd</option><option value="call">call</option>
                          </select>
                          <input className="hud-input" value={hook.value} onChange={e => updateTaskHook(si, ti, 'on_fail', hi, 'value', e.target.value)} placeholder="command or alias" />
                          <button className="btn btn-ghost" onClick={() => removeTaskHook(si, ti, 'on_fail', hi)}><Trash2 size={12} /></button>
                        </div>
                      ))}
                      <button className="wizard-add-btn" onClick={() => addTaskHook(si, ti, 'on_fail')}><Plus size={12} /> Add</button>
                    </div>
                  </div>
                </details>
              </div>
            ))}
            <button className="wizard-add-btn" onClick={() => addTask(si)}><Plus size={14} /> Add Task</button>
          </div>
        ))}
        <button className="wizard-add-btn" onClick={addSection} style={{ marginTop: '0.5rem' }}><Plus size={14} /> Add Section</button>
      </div>
    );
  };

  const renderAdvanced = () => {
    const updateGit = (field: string, value: unknown) => {
      setData(prev => ({ ...prev, git: { ...prev.git, [field]: value } }));
    };
    const updateStubs = (field: string, value: unknown) => {
      setData(prev => ({ ...prev, stubs: { ...prev.stubs, [field]: value } }));
    };
    const addStubSearchPath = () => {
      setData(prev => ({ ...prev, stubs: { ...prev.stubs, search_paths: [...prev.stubs.search_paths, ''] } }));
    };
    const updateStubSearchPath = (index: number, value: string) => {
      setData(prev => {
        const paths = [...prev.stubs.search_paths];
        paths[index] = value;
        return { ...prev, stubs: { ...prev.stubs, search_paths: paths } };
      });
    };
    const removeStubSearchPath = (index: number) => {
      setData(prev => ({ ...prev, stubs: { ...prev.stubs, search_paths: prev.stubs.search_paths.filter((_, i) => i !== index) } }));
    };
    const addImport = () => {
      setData(prev => ({ ...prev, imports: [...prev.imports, { path: '', alias: '', with: {} }] }));
    };
    const updateImport = (index: number, field: string, value: unknown) => {
      setData(prev => {
        const imports = [...prev.imports];
        imports[index] = { ...imports[index], [field]: value };
        return { ...prev, imports };
      });
    };
    const removeImport = (index: number) => {
      setData(prev => ({ ...prev, imports: prev.imports.filter((_, i) => i !== index) }));
    };
    type HookBucket = 'before_run' | 'after_run' | 'on_error';
    const addHook = (bucket: HookBucket) => {
      setData(prev => ({ ...prev, hooks: { ...prev.hooks, [bucket]: [...prev.hooks[bucket], { name: '', cmd: '', if_expr: '' }] } }));
    };
    const updateHook = (bucket: HookBucket, index: number, field: string, value: string) => {
      setData(prev => {
        const hooks = [...prev.hooks[bucket]];
        hooks[index] = { ...hooks[index], [field]: value };
        return { ...prev, hooks: { ...prev.hooks, [bucket]: hooks } };
      });
    };
    const removeHook = (bucket: HookBucket, index: number) => {
      setData(prev => ({ ...prev, hooks: { ...prev.hooks, [bucket]: prev.hooks[bucket].filter((_, i) => i !== index) } }));
    };

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        <h3 style={{ fontSize: 'var(--text-md)', color: 'var(--status-success)', marginBottom: '0', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Advanced</h3>

        {/* Git */}
        <details className="section" style={{ padding: '0.75rem 1rem' }}>
          <summary className="bp-meta-section-title" style={{ cursor: 'pointer', userSelect: 'none' }}>Git</summary>
          <div style={{ paddingTop: '0.75rem', display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
            <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
              <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', cursor: 'pointer' }}>
                <input type="checkbox" checked={data.git.init} onChange={e => updateGit('init', e.target.checked)} style={{ accentColor: 'var(--status-success)' }} />
                <span style={{ fontSize: 'var(--text-md)' }}>Initialize git repo</span>
              </label>
              <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', cursor: 'pointer' }}>
                <input type="checkbox" checked={data.git.create_github_repo} onChange={e => updateGit('create_github_repo', e.target.checked)} style={{ accentColor: 'var(--status-success)' }} />
                <span style={{ fontSize: 'var(--text-md)' }}>Create GitHub repo</span>
              </label>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '0.5rem' }}>
              <div className="wizard-field">
                <label>Visibility</label>
                <select className="hud-input" value={data.git.visibility} onChange={e => updateGit('visibility', e.target.value)}>
                  <option value="">—</option><option value="private">private</option><option value="public">public</option><option value="internal">internal</option>
                </select>
              </div>
              <div className="wizard-field">
                <label>Remote</label>
                <input className="hud-input" value={data.git.remote} onChange={e => updateGit('remote', e.target.value)} placeholder="origin URL" />
              </div>
              <div className="wizard-field">
                <label>Branch</label>
                <input className="hud-input" value={data.git.branch} onChange={e => updateGit('branch', e.target.value)} placeholder="main" />
              </div>
            </div>
          </div>
        </details>

        {/* Stubs */}
        <details className="section" style={{ padding: '0.75rem 1rem' }}>
          <summary className="bp-meta-section-title" style={{ cursor: 'pointer', userSelect: 'none' }}>Stubs</summary>
          <div style={{ paddingTop: '0.75rem', display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
            <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
              <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', cursor: 'pointer' }}>
                <input type="checkbox" checked={data.stubs.enabled} onChange={e => updateStubs('enabled', e.target.checked)} style={{ accentColor: 'var(--status-success)' }} />
                <span style={{ fontSize: 'var(--text-md)' }}>Enable stubs</span>
              </label>
              <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', cursor: 'pointer' }}>
                <input type="checkbox" checked={data.stubs.strict_match} onChange={e => updateStubs('strict_match', e.target.checked)} style={{ accentColor: 'var(--status-success)' }} />
                <span style={{ fontSize: 'var(--text-md)' }}>Strict match</span>
              </label>
            </div>
            <div>
              <div className="hud-label" style={{ marginBottom: '0.3rem' }}>Search Paths</div>
              {data.stubs.search_paths.map((sp, i) => (
                <div key={i} style={{ display: 'flex', gap: '0.4rem', marginBottom: '0.3rem' }}>
                  <input className="hud-input" value={sp} onChange={e => updateStubSearchPath(i, e.target.value)} placeholder="./stubs" style={{ flex: 1 }} />
                  <button className="btn btn-ghost" onClick={() => removeStubSearchPath(i)}><Trash2 size={12} /></button>
                </div>
              ))}
              <button className="wizard-add-btn" onClick={addStubSearchPath}><Plus size={12} /> Add search path</button>
            </div>
          </div>
        </details>

        {/* Imports */}
        <details className="section" style={{ padding: '0.75rem 1rem' }}>
          <summary className="bp-meta-section-title" style={{ cursor: 'pointer', userSelect: 'none' }}>
            Imports {data.imports.length > 0 && <span style={{ color: 'var(--text-tertiary)', fontWeight: 400 }}>({data.imports.length})</span>}
          </summary>
          <div style={{ paddingTop: '0.75rem', display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
            {data.imports.map((imp, i) => (
              <div key={i} className="wizard-task-card" style={{ background: 'var(--bg-base)', border: '1px solid var(--border-default)', borderRadius: '4px', padding: '0.75rem' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                  <span style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>Import {i + 1}</span>
                  <button className="btn btn-ghost" onClick={() => removeImport(i)}><Trash2 size={12} /></button>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem', marginBottom: '0.5rem' }}>
                  <div className="wizard-field"><label>Path</label><input className="hud-input" value={imp.path} onChange={e => updateImport(i, 'path', e.target.value)} placeholder="./base.yaml" /></div>
                  <div className="wizard-field"><label>Alias</label><input className="hud-input" value={imp.alias} onChange={e => updateImport(i, 'alias', e.target.value)} placeholder="base" /></div>
                </div>
                <div>
                  <div className="hud-label" style={{ marginBottom: '0.3rem' }}>With (overrides)</div>
                  <KVEditor data={imp.with} onChange={val => updateImport(i, 'with', val)} keyPlaceholder="key" valuePlaceholder="value" />
                </div>
              </div>
            ))}
            <button className="wizard-add-btn" onClick={addImport}><Plus size={12} /> Add import</button>
          </div>
        </details>

        {/* Hooks */}
        <details className="section" style={{ padding: '0.75rem 1rem' }}>
          <summary className="bp-meta-section-title" style={{ cursor: 'pointer', userSelect: 'none' }}>Hooks</summary>
          <div style={{ paddingTop: '0.75rem', display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
            {(['before_run', 'after_run', 'on_error'] as HookBucket[]).map(bucket => (
              <div key={bucket}>
                <div className="hud-label" style={{ marginBottom: '0.3rem', textTransform: 'capitalize' }}>{bucket.replace('_', ' ')}</div>
                {data.hooks[bucket].map((hook, hi) => (
                  <div key={hi} style={{ display: 'grid', gridTemplateColumns: '1fr 2fr 1fr auto', gap: '0.4rem', marginBottom: '0.3rem' }}>
                    <input className="hud-input" value={hook.name} onChange={e => updateHook(bucket, hi, 'name', e.target.value)} placeholder="name" />
                    <input className="hud-input" value={hook.cmd} onChange={e => updateHook(bucket, hi, 'cmd', e.target.value)} placeholder="command" />
                    <input className="hud-input" value={hook.if_expr} onChange={e => updateHook(bucket, hi, 'if_expr', e.target.value)} placeholder="if condition" />
                    <button className="btn btn-ghost" onClick={() => removeHook(bucket, hi)}><Trash2 size={12} /></button>
                  </div>
                ))}
                <button className="wizard-add-btn" onClick={() => addHook(bucket)}><Plus size={12} /> Add</button>
              </div>
            ))}
          </div>
        </details>
      </div>
    );
  };

  const renderReview = () => {
    const yaml = convertWizardToYaml(data);
    const warnings: string[] = [];
    if (!data.blueprint.name && !data.blueprint.slug) warnings.push('Name or slug is required');
    if (!data.blueprint.description) warnings.push('No description set');
    if (data.steps.length === 0) warnings.push('No sections defined');
    for (const sec of data.steps) {
      for (const t of sec.tasks) {
        if (!t.name) warnings.push(`Task in "${sec.section || 'unnamed'}" has no name`);
        if (!t.cmd && !t.call) warnings.push(`Task "${t.name || 'unnamed'}" has no command or call`);
      }
    }
    const totalTasks = data.steps.reduce((sum, s) => sum + s.tasks.length, 0);
    const requiredInputs = data.inputs.filter(i => i.required).length;

    return (
      <div>
        <h3 style={{ fontSize: 'var(--text-md)', color: 'var(--status-success)', marginBottom: '1rem', textTransform: 'uppercase', letterSpacing: '0.1em' }}>Review</h3>
        <div className="section" style={{ padding: '0.75rem 1rem', marginBottom: '1rem' }}>
          <div style={{ fontSize: 'var(--text-md)', marginBottom: '0.3rem' }}>
            <strong>{data.blueprint.name || data.blueprint.slug || '(unnamed)'}</strong> <span className="bp-badge bp-badge-info">v{data.version}</span>
          </div>
          {data.blueprint.description && <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', marginBottom: '0.3rem' }}>{data.blueprint.description}</div>}
          {data.blueprint.tags.length > 0 && <div style={{ marginBottom: '0.3rem' }}>{data.blueprint.tags.map((t, i) => <span key={i} className="bp-tag">{t}</span>)}</div>}
          <div style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>
            {data.inputs.length} inputs ({requiredInputs} required) · {data.steps.length} sections · {totalTasks} tasks
            {Object.keys(data.env).length > 0 && ` · ${Object.keys(data.env).length} env vars`}
          </div>
        </div>

        {warnings.length > 0 && (
          <div style={{ padding: '0.5rem 0.75rem', background: 'rgba(var(--warn), 0.1)', border: '1px solid rgba(var(--warn), 0.3)', borderRadius: '4px', marginBottom: '1rem' }}>
            <div style={{ fontSize: 'var(--text-sm)', color: 'var(--status-running)', textTransform: 'uppercase', marginBottom: '0.3rem' }}>Warnings</div>
            {warnings.map((w, i) => <div key={i} style={{ fontSize: 'var(--text-sm)', color: 'var(--status-running)' }}>- {w}</div>)}
          </div>
        )}

        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.3rem' }}>
          <span className="hud-label">YAML Preview</span>
          <button className="btn btn-ghost" onClick={() => { navigator.clipboard.writeText(yaml); toast.success('YAML copied'); }} style={{ fontSize: 'var(--text-xs)' }}>Copy</button>
        </div>
        <pre style={{ padding: '0.75rem', background: 'var(--bg-base)', border: '1px solid var(--border-default)', borderRadius: '4px', fontSize: 'var(--text-sm)', lineHeight: '1.5', overflow: 'auto', maxHeight: '50vh', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {yaml}
        </pre>
      </div>
    );
  };

  const stepContent = [renderMetadata, renderProject, renderEnv, renderPackages, renderInputs, renderSteps, renderAdvanced, renderReview];

  return (
    <div className="wizard-shell">
      {/* Header */}
      <div className="page-header" style={{ padding: '0 0 0.5rem 0', gap: '0.5rem' }}>
        <button className="btn btn-ghost" onClick={onBack} style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <ChevronLeft size={13} /> Back
        </button>
        <span className="page-title">{editPath ? 'Edit Blueprint' : 'New Blueprint'}</span>
      </div>

      {/* Body */}
      <div className="wizard-body">
        {/* Sidebar */}
        <div className="wizard-sidebar">
          {WIZARD_STEPS.map((step, i) => (
            <button key={step.key}
              className={`wizard-step-btn ${currentStep === i ? 'active' : ''}`}
              onClick={() => setCurrentStep(i)}
            >
              <div>{step.title}</div>
              <div className="wizard-step-desc">{step.desc}</div>
            </button>
          ))}
        </div>

        {/* Content */}
        <div className="wizard-content">
          {stepContent[currentStep]()}
        </div>
      </div>

      {/* Footer */}
      <div className="wizard-footer">
        <button className="btn btn-ghost" onClick={() => setCurrentStep(Math.max(0, currentStep - 1))} disabled={currentStep === 0}>
          Previous
        </button>
        <span style={{ fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)' }}>Step {currentStep + 1} of {WIZARD_STEPS.length}</span>
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button className="btn btn-ghost" onClick={() => setCurrentStep(Math.min(WIZARD_STEPS.length - 1, currentStep + 1))} disabled={currentStep === WIZARD_STEPS.length - 1}>
            Next
          </button>
          <button className="btn btn-primary" onClick={handleSave} disabled={saving} style={{ borderColor: 'rgba(var(--ok) / 0.5)', color: 'var(--status-success)' }}>
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  );
}
