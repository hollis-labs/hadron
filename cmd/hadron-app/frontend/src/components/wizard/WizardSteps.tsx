import { Plus, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { KVEditor } from '@/components/wizard/KVEditor';
import { PackageList } from '@/components/wizard/PackageList';
import { convertWizardToYaml } from '@/utils/blueprintYaml';
import { newWizardInput, newWizardTask } from '@/hooks/useWizardState';
import type { WizardBlueprint } from '@/api/types';

// ── Shared prop types ─────────────────────────────────────────────────

interface StepProps {
  data: WizardBlueprint;
  setData: React.Dispatch<React.SetStateAction<WizardBlueprint>>;
}

interface MetadataStepProps extends StepProps {
  newTag: string;
  setNewTag: (v: string) => void;
  updateBlueprint: (field: string, value: unknown) => void;
}

interface ProjectStepProps extends StepProps {
  updateProject: (field: string, value: unknown) => void;
}

interface ReviewStepProps extends StepProps {
  saving: boolean;
}

// ── 1. Metadata ───────────────────────────────────────────────────────

export function WizardMetadataStep({ data, newTag, setNewTag, updateBlueprint }: MetadataStepProps) {
  return (
    <div>
      <h3 className="text-sm text-blue-400 mb-4 uppercase tracking-wider">Metadata</h3>
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">Name *</label>
          <Input value={data.blueprint.name} onChange={e => updateBlueprint('name', e.target.value)}
            onBlur={() => { if (!data.blueprint.slug && data.blueprint.name) updateBlueprint('slug', data.blueprint.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')); }}
            placeholder="my-blueprint" />
        </div>
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">Slug</label>
          <Input value={data.blueprint.slug} onChange={e => updateBlueprint('slug', e.target.value)} placeholder="my-blueprint" />
        </div>
      </div>
      <div className="flex flex-col gap-1 mb-3">
        <label className="text-sm font-medium text-muted-foreground">Title</label>
        <Input value={data.blueprint.title} onChange={e => updateBlueprint('title', e.target.value)} placeholder="Human-readable title" className="w-full" />
      </div>
      <div className="flex flex-col gap-1 mb-3">
        <label className="text-sm font-medium text-muted-foreground">Description</label>
        <textarea className="flex min-h-[60px] w-full rounded-lg border border-input bg-transparent px-2.5 py-2 text-sm placeholder:text-muted-foreground focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:opacity-50 resize-y" rows={3} value={data.blueprint.description} onChange={e => updateBlueprint('description', e.target.value)} placeholder="What this blueprint does" />
      </div>
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">Author</label>
          <Input value={data.blueprint.author} onChange={e => updateBlueprint('author', e.target.value)} />
        </div>
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">License</label>
          <Input value={data.blueprint.license} onChange={e => updateBlueprint('license', e.target.value)} placeholder="MIT" />
        </div>
      </div>
      <div className="flex flex-col gap-1 mb-3">
        <label className="text-sm font-medium text-muted-foreground">Homepage</label>
        <Input value={data.blueprint.homepage} onChange={e => updateBlueprint('homepage', e.target.value)} placeholder="https://..." className="w-full" />
      </div>
      <div className="flex flex-col gap-1 mb-3">
        <label className="text-sm font-medium text-muted-foreground">Tags</label>
        <div className="flex flex-wrap gap-1.5 items-center">
          {data.blueprint.tags.map((tag, i) => (
            <span key={i} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-muted text-muted-foreground mr-1 mb-1">
              {tag}
              <button type="button" onClick={() => updateBlueprint('tags', data.blueprint.tags.filter((_, j) => j !== i))}
                className="ml-1 cursor-pointer bg-transparent border-none text-inherit text-xs">&times;</button>
            </span>
          ))}
          <Input placeholder="Add tag..." value={newTag}
            onChange={e => setNewTag(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter' && newTag.trim()) {
                e.preventDefault();
                updateBlueprint('tags', [...data.blueprint.tags, newTag.trim()]);
                setNewTag('');
              }
            }}
            className="w-[120px] text-xs" />
        </div>
      </div>
    </div>
  );
}

// ── 2. Project ────────────────────────────────────────────────────────

export function WizardProjectStep({ data, updateProject }: ProjectStepProps) {
  return (
    <div>
      <h3 className="text-sm text-blue-400 mb-4 uppercase tracking-wider">Project Configuration</h3>
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">Project Type</label>
          <Input value={data.project.type} onChange={e => updateProject('type', e.target.value)} placeholder="webapp, api, cli..." list="project-types" />
          <datalist id="project-types"><option value="webapp" /><option value="api" /><option value="cli" /><option value="library" /><option value="script" /></datalist>
        </div>
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">Project Name</label>
          <Input value={data.project.name} onChange={e => updateProject('name', e.target.value)} />
        </div>
      </div>
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">Directory</label>
          <Input value={data.project.dir} onChange={e => updateProject('dir', e.target.value)} />
        </div>
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">Path</label>
          <Input value={data.project.path} onChange={e => updateProject('path', e.target.value)} />
        </div>
      </div>
      <div className="grid grid-cols-2 gap-2">
        <div className="flex flex-col gap-1 mb-3">
          <label className="text-sm font-medium text-muted-foreground">PHP Version</label>
          <Input value={data.project.php_version} onChange={e => updateProject('php_version', e.target.value)} placeholder="8.2" />
        </div>
        <div className="flex flex-row items-center gap-2 mb-3 mt-5">
          <input type="checkbox" checked={data.project.node} onChange={e => updateProject('node', e.target.checked)} className="accent-blue-400" />
          <span className="text-sm">Node.js project</span>
        </div>
      </div>
      <div className="flex flex-col gap-1 mb-3 mt-2">
        <label className="text-sm font-medium text-muted-foreground">Custom Variables</label>
        <KVEditor data={data.project.vars} onChange={vars => updateProject('vars', vars)} keyPlaceholder="variable" valuePlaceholder="value" />
      </div>
    </div>
  );
}

// ── 3. Env ────────────────────────────────────────────────────────────

export function WizardEnvStep({ data, setData }: StepProps) {
  return (
    <div>
      <h3 className="text-sm text-blue-400 mb-2 uppercase tracking-wider">Environment Variables</h3>
      <p className="text-sm text-muted-foreground mb-4">
        {'Available to all tasks via {{ .env.KEY }}.'}
      </p>
      <KVEditor data={data.env} onChange={env => setData(prev => ({ ...prev, env }))} keyPlaceholder="ENV_VAR" valuePlaceholder="value" />
    </div>
  );
}

// ── 4. Packages ───────────────────────────────────────────────────────

export function WizardPackagesStep({ data, setData }: StepProps) {
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
      <h3 className="text-sm text-blue-400 mb-2 uppercase tracking-wider">Packages</h3>
      <p className="text-sm text-muted-foreground mb-4">Declare dependencies. Press Enter to add.</p>
      {pkgSections.map(sec => (
        <div key={sec.label} className="mb-4">
          <div className="text-sm font-medium text-muted-foreground mb-1">{sec.label}</div>
          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-col gap-1 mb-3">
              <label className="text-xs font-medium text-muted-foreground">Dependencies</label>
              <PackageList items={data.packages[sec.deps]} placeholder={`${sec.label} package`}
                onAdd={v => updatePkg(sec.deps, [...data.packages[sec.deps], v])}
                onRemove={i => updatePkg(sec.deps, data.packages[sec.deps].filter((_, j) => j !== i))} />
            </div>
            <div className="flex flex-col gap-1 mb-3">
              <label className="text-xs font-medium text-muted-foreground">Dev Dependencies</label>
              <PackageList items={data.packages[sec.dev]} placeholder={`${sec.label} dev package`}
                onAdd={v => updatePkg(sec.dev, [...data.packages[sec.dev], v])}
                onRemove={i => updatePkg(sec.dev, data.packages[sec.dev].filter((_, j) => j !== i))} />
            </div>
          </div>
        </div>
      ))}
      <div className="mb-4">
        <div className="text-sm font-medium text-muted-foreground mb-1">brew</div>
        <div className="grid grid-cols-2 gap-2">
          <div className="flex flex-col gap-1 mb-3">
            <label className="text-xs font-medium text-muted-foreground">Formulae</label>
            <PackageList items={data.packages.brew_formulae} placeholder="formula"
              onAdd={v => updatePkg('brew_formulae', [...data.packages.brew_formulae, v])}
              onRemove={i => updatePkg('brew_formulae', data.packages.brew_formulae.filter((_, j) => j !== i))} />
          </div>
          <div className="flex flex-col gap-1 mb-3">
            <label className="text-xs font-medium text-muted-foreground">Casks</label>
            <PackageList items={data.packages.brew_casks} placeholder="cask"
              onAdd={v => updatePkg('brew_casks', [...data.packages.brew_casks, v])}
              onRemove={i => updatePkg('brew_casks', data.packages.brew_casks.filter((_, j) => j !== i))} />
          </div>
        </div>
      </div>
      <div>
        <div className="text-sm font-medium text-muted-foreground mb-1">go</div>
        <PackageList items={data.packages.go_tools} placeholder="go tool"
          onAdd={v => updatePkg('go_tools', [...data.packages.go_tools, v])}
          onRemove={i => updatePkg('go_tools', data.packages.go_tools.filter((_, j) => j !== i))} />
      </div>
    </div>
  );
}

// ── 5. Inputs ─────────────────────────────────────────────────────────

export function WizardInputsStep({ data, setData }: StepProps) {
  const addInput = () => setData(prev => ({ ...prev, inputs: [...prev.inputs, newWizardInput()] }));
  const removeInput = (idx: number) => setData(prev => ({ ...prev, inputs: prev.inputs.filter((_, i) => i !== idx) }));
  const updateInput = (idx: number, field: string, value: unknown) => {
    setData(prev => ({ ...prev, inputs: prev.inputs.map((inp, i) => i === idx ? { ...inp, [field]: value } : inp) }));
  };

  return (
    <div>
      <h3 className="text-sm text-blue-400 mb-2 uppercase tracking-wider">Inputs</h3>
      <p className="text-sm text-muted-foreground mb-4">
        {'Parameters users provide when running. Available as {{ .inputs.name }}.'}
      </p>
      {data.inputs.map((inp, idx) => (
        <div key={idx} className="rounded-lg border border-border bg-card p-3 mb-3">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm text-blue-400">Input {idx + 1}{inp.name ? `: ${inp.name}` : ''}</span>
            <Button variant="ghost" size="icon-sm" onClick={() => removeInput(idx)}>
              <Trash2 size={13} className="text-red-400" />
            </Button>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Name *</label><Input value={inp.name} onChange={e => updateInput(idx, 'name', e.target.value)} placeholder="input_name" /></div>
            <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Type *</label>
              <select className="h-8 w-full rounded-lg border border-input bg-transparent px-2.5 py-1 text-sm focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50" value={inp.type} onChange={e => updateInput(idx, 'type', e.target.value)}>
                <option value="string">string</option><option value="number">number</option><option value="boolean">boolean</option><option value="array">array</option>
              </select>
            </div>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Label</label><Input value={inp.label} onChange={e => updateInput(idx, 'label', e.target.value)} placeholder="Human label" /></div>
            <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Description</label><Input value={inp.description} onChange={e => updateInput(idx, 'description', e.target.value)} placeholder="Help text" /></div>
          </div>
          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-row items-center gap-2 mb-3">
              <input type="checkbox" checked={inp.required} onChange={e => updateInput(idx, 'required', e.target.checked)} className="accent-blue-400" />
              <span className="text-sm">Required</span>
            </div>
            <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Default</label><Input value={inp.default_value} onChange={e => updateInput(idx, 'default_value', e.target.value)} /></div>
          </div>
          <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Enum (comma-separated)</label><Input value={inp.enum_values} onChange={e => updateInput(idx, 'enum_values', e.target.value)} placeholder="opt1, opt2, opt3" className="w-full" /></div>
          <div className="grid grid-cols-2 gap-2">
            <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Pattern (regex)</label><Input value={inp.pattern} onChange={e => updateInput(idx, 'pattern', e.target.value)} /></div>
            {inp.type === 'array' && <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Items Type</label>
              <select className="h-8 w-full rounded-lg border border-input bg-transparent px-2.5 py-1 text-sm focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50" value={inp.items_type} onChange={e => updateInput(idx, 'items_type', e.target.value)}>
                <option value="">any</option><option value="string">string</option><option value="number">number</option>
              </select>
            </div>}
          </div>
        </div>
      ))}
      <button className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none" onClick={addInput}><Plus size={14} /> Add Input</button>
    </div>
  );
}

// ── 6. Steps & Tasks ──────────────────────────────────────────────────

export function WizardStepsStep({ data, setData }: StepProps) {
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
      <h3 className="text-sm text-blue-400 mb-2 uppercase tracking-wider">Steps & Tasks</h3>
      <p className="text-sm text-muted-foreground mb-4">Organize tasks into sections. Tasks execute in order.</p>
      {data.steps.map((section, si) => (
        <div key={si} className="rounded-lg border border-border bg-card p-3 mb-3">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <span className="text-sm font-medium text-muted-foreground">Section</span>
              <Input value={section.section} onChange={e => updateSectionName(si, e.target.value)} placeholder="section-name" className="w-[200px]" />
            </div>
            <Button variant="ghost" size="icon-sm" onClick={() => removeSection(si)} disabled={data.steps.length <= 1}>
              <Trash2 size={13} />
            </Button>
          </div>

          {section.tasks.map((task, ti) => (
            <div key={ti} className="bg-background border border-border rounded p-3 mb-2">
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm text-blue-400">Task {ti + 1}{task.name ? `: ${task.name}` : ''}</span>
                <Button variant="ghost" size="icon-sm" onClick={() => removeTask(si, ti)}>
                  <Trash2 size={12} className="text-red-400" />
                </Button>
              </div>
              <div className="flex flex-col gap-1 mb-3">
                <label className="text-sm font-medium text-muted-foreground">Task Name</label>
                <Input value={task.name} onChange={e => updateTask(si, ti, 'name', e.target.value)} placeholder="task-name" className="w-full font-mono" />
              </div>
              <div className="flex flex-col gap-1 mb-3">
                <label className="text-sm font-medium text-muted-foreground">Command</label>
                <textarea className="flex min-h-[60px] w-full rounded-lg border border-input bg-transparent px-2.5 py-2 text-sm placeholder:text-muted-foreground focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:opacity-50 font-mono resize-y" rows={2} value={task.cmd} onChange={e => updateTask(si, ti, 'cmd', e.target.value)}
                  placeholder="echo 'hello world'" />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Working Dir</label><Input value={task.dir} onChange={e => updateTask(si, ti, 'dir', e.target.value)} /></div>
                <div className="flex flex-row items-center gap-2 mb-3 mt-5">
                  <input type="checkbox" checked={task.enabled} onChange={e => updateTask(si, ti, 'enabled', e.target.checked)} className="accent-blue-400" />
                  <span className="text-sm">Enabled</span>
                </div>
              </div>
              <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Condition (if)</label><Input value={task.if_expr} onChange={e => updateTask(si, ti, 'if_expr', e.target.value)} placeholder={'{{ eq .inputs.skip false }}'} className="w-full font-mono" /></div>
              <div className="grid grid-cols-3 gap-2">
                <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Retry</label><Input type="number" min="0" value={task.retry} onChange={e => updateTask(si, ti, 'retry', e.target.value)} /></div>
                <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Retry Delay (s)</label><Input type="number" min="0" value={task.retry_delay_seconds} onChange={e => updateTask(si, ti, 'retry_delay_seconds', e.target.value)} /></div>
                <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Timeout (s)</label><Input type="number" min="0" value={task.timeout_seconds} onChange={e => updateTask(si, ti, 'timeout_seconds', e.target.value)} /></div>
              </div>
              <div className="flex flex-row items-center gap-2 mb-3">
                <input type="checkbox" checked={task.continue_on_error} onChange={e => updateTask(si, ti, 'continue_on_error', e.target.checked)} className="accent-blue-400" />
                <span className="text-sm">Continue on error</span>
              </div>
              {/* Per-task hooks */}
              <details className="mt-2">
                <summary className="text-sm font-medium text-muted-foreground cursor-pointer select-none">
                  Hooks
                  {((task.on_success?.length || 0) + (task.on_fail?.length || 0)) > 0 &&
                    <span className="text-blue-400 ml-1">
                      ({(task.on_success?.length || 0) + (task.on_fail?.length || 0)})
                    </span>
                  }
                </summary>
                <div className="py-2 flex flex-col gap-2">
                  <div>
                    <div className="text-xs font-medium text-muted-foreground mb-0.5">On Success</div>
                    {(task.on_success || []).map((hook, hi) => (
                      <div key={hi} className="grid grid-cols-[auto_1fr_auto] gap-1.5 mb-1">
                        <select className="h-8 w-auto rounded-lg border border-input bg-transparent px-2.5 py-1 text-sm focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50" value={hook.type} onChange={e => updateTaskHook(si, ti, 'on_success', hi, 'type', e.target.value)}>
                          <option value="cmd">cmd</option><option value="call">call</option>
                        </select>
                        <Input value={hook.value} onChange={e => updateTaskHook(si, ti, 'on_success', hi, 'value', e.target.value)} placeholder="command or alias" />
                        <Button variant="ghost" size="icon-sm" onClick={() => removeTaskHook(si, ti, 'on_success', hi)}><Trash2 size={12} /></Button>
                      </div>
                    ))}
                    <button className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none" onClick={() => addTaskHook(si, ti, 'on_success')}><Plus size={12} /> Add</button>
                  </div>
                  <div>
                    <div className="text-xs font-medium text-muted-foreground mb-0.5">On Fail</div>
                    {(task.on_fail || []).map((hook, hi) => (
                      <div key={hi} className="grid grid-cols-[auto_1fr_auto] gap-1.5 mb-1">
                        <select className="h-8 w-auto rounded-lg border border-input bg-transparent px-2.5 py-1 text-sm focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50" value={hook.type} onChange={e => updateTaskHook(si, ti, 'on_fail', hi, 'type', e.target.value)}>
                          <option value="cmd">cmd</option><option value="call">call</option>
                        </select>
                        <Input value={hook.value} onChange={e => updateTaskHook(si, ti, 'on_fail', hi, 'value', e.target.value)} placeholder="command or alias" />
                        <Button variant="ghost" size="icon-sm" onClick={() => removeTaskHook(si, ti, 'on_fail', hi)}><Trash2 size={12} /></Button>
                      </div>
                    ))}
                    <button className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none" onClick={() => addTaskHook(si, ti, 'on_fail')}><Plus size={12} /> Add</button>
                  </div>
                </div>
              </details>
            </div>
          ))}
          <button className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none" onClick={() => addTask(si)}><Plus size={14} /> Add Task</button>
        </div>
      ))}
      <button className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none mt-2" onClick={addSection}><Plus size={14} /> Add Section</button>
    </div>
  );
}

// ── 7. Advanced ───────────────────────────────────────────────────────

export function WizardAdvancedStep({ data, setData }: StepProps) {
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
    <div className="flex flex-col gap-4">
      <h3 className="text-sm text-blue-400 mb-0 uppercase tracking-wider">Advanced</h3>

      {/* Git */}
      <details className="rounded-lg border border-border bg-card px-4 py-3">
        <summary className="text-xs uppercase tracking-wider text-muted-foreground font-medium cursor-pointer select-none">Git</summary>
        <div className="pt-3 flex flex-col gap-2.5">
          <div className="flex gap-4 flex-wrap">
            <label className="flex items-center gap-1.5 cursor-pointer">
              <input type="checkbox" checked={data.git.init} onChange={e => updateGit('init', e.target.checked)} className="accent-blue-400" />
              <span className="text-sm">Initialize git repo</span>
            </label>
            <label className="flex items-center gap-1.5 cursor-pointer">
              <input type="checkbox" checked={data.git.create_github_repo} onChange={e => updateGit('create_github_repo', e.target.checked)} className="accent-blue-400" />
              <span className="text-sm">Create GitHub repo</span>
            </label>
          </div>
          <div className="grid grid-cols-3 gap-2">
            <div className="flex flex-col gap-1 mb-3">
              <label className="text-sm font-medium text-muted-foreground">Visibility</label>
              <select className="h-8 w-full rounded-lg border border-input bg-transparent px-2.5 py-1 text-sm focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50" value={data.git.visibility} onChange={e => updateGit('visibility', e.target.value)}>
                <option value="">—</option><option value="private">private</option><option value="public">public</option><option value="internal">internal</option>
              </select>
            </div>
            <div className="flex flex-col gap-1 mb-3">
              <label className="text-sm font-medium text-muted-foreground">Remote</label>
              <Input value={data.git.remote} onChange={e => updateGit('remote', e.target.value)} placeholder="origin URL" />
            </div>
            <div className="flex flex-col gap-1 mb-3">
              <label className="text-sm font-medium text-muted-foreground">Branch</label>
              <Input value={data.git.branch} onChange={e => updateGit('branch', e.target.value)} placeholder="main" />
            </div>
          </div>
        </div>
      </details>

      {/* Stubs */}
      <details className="rounded-lg border border-border bg-card px-4 py-3">
        <summary className="text-xs uppercase tracking-wider text-muted-foreground font-medium cursor-pointer select-none">Stubs</summary>
        <div className="pt-3 flex flex-col gap-2.5">
          <div className="flex gap-4 flex-wrap">
            <label className="flex items-center gap-1.5 cursor-pointer">
              <input type="checkbox" checked={data.stubs.enabled} onChange={e => updateStubs('enabled', e.target.checked)} className="accent-blue-400" />
              <span className="text-sm">Enable stubs</span>
            </label>
            <label className="flex items-center gap-1.5 cursor-pointer">
              <input type="checkbox" checked={data.stubs.strict_match} onChange={e => updateStubs('strict_match', e.target.checked)} className="accent-blue-400" />
              <span className="text-sm">Strict match</span>
            </label>
          </div>
          <div>
            <div className="text-sm font-medium text-muted-foreground mb-1">Search Paths</div>
            {data.stubs.search_paths.map((sp, i) => (
              <div key={i} className="flex gap-1.5 mb-1">
                <Input value={sp} onChange={e => updateStubSearchPath(i, e.target.value)} placeholder="./stubs" className="flex-1" />
                <Button variant="ghost" size="icon-sm" onClick={() => removeStubSearchPath(i)}><Trash2 size={12} /></Button>
              </div>
            ))}
            <button className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none" onClick={addStubSearchPath}><Plus size={12} /> Add search path</button>
          </div>
        </div>
      </details>

      {/* Imports */}
      <details className="rounded-lg border border-border bg-card px-4 py-3">
        <summary className="text-xs uppercase tracking-wider text-muted-foreground font-medium cursor-pointer select-none">
          Imports {data.imports.length > 0 && <span className="text-muted-foreground font-normal">({data.imports.length})</span>}
        </summary>
        <div className="pt-3 flex flex-col gap-3">
          {data.imports.map((imp, i) => (
            <div key={i} className="bg-background border border-border rounded p-3">
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm text-muted-foreground">Import {i + 1}</span>
                <Button variant="ghost" size="icon-sm" onClick={() => removeImport(i)}><Trash2 size={12} /></Button>
              </div>
              <div className="grid grid-cols-2 gap-2 mb-2">
                <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Path</label><Input value={imp.path} onChange={e => updateImport(i, 'path', e.target.value)} placeholder="./base.yaml" /></div>
                <div className="flex flex-col gap-1 mb-3"><label className="text-sm font-medium text-muted-foreground">Alias</label><Input value={imp.alias} onChange={e => updateImport(i, 'alias', e.target.value)} placeholder="base" /></div>
              </div>
              <div>
                <div className="text-sm font-medium text-muted-foreground mb-1">With (overrides)</div>
                <KVEditor data={imp.with} onChange={val => updateImport(i, 'with', val)} keyPlaceholder="key" valuePlaceholder="value" />
              </div>
            </div>
          ))}
          <button className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none" onClick={addImport}><Plus size={12} /> Add import</button>
        </div>
      </details>

      {/* Hooks */}
      <details className="rounded-lg border border-border bg-card px-4 py-3">
        <summary className="text-xs uppercase tracking-wider text-muted-foreground font-medium cursor-pointer select-none">Hooks</summary>
        <div className="pt-3 flex flex-col gap-3">
          {(['before_run', 'after_run', 'on_error'] as HookBucket[]).map(bucket => (
            <div key={bucket}>
              <div className="text-sm font-medium text-muted-foreground mb-1 capitalize">{bucket.replace('_', ' ')}</div>
              {data.hooks[bucket].map((hook, hi) => (
                <div key={hi} className="grid grid-cols-[1fr_2fr_1fr_auto] gap-1.5 mb-1">
                  <Input value={hook.name} onChange={e => updateHook(bucket, hi, 'name', e.target.value)} placeholder="name" />
                  <Input value={hook.cmd} onChange={e => updateHook(bucket, hi, 'cmd', e.target.value)} placeholder="command" />
                  <Input value={hook.if_expr} onChange={e => updateHook(bucket, hi, 'if_expr', e.target.value)} placeholder="if condition" />
                  <Button variant="ghost" size="icon-sm" onClick={() => removeHook(bucket, hi)}><Trash2 size={12} /></Button>
                </div>
              ))}
              <button type="button" className="flex items-center gap-1 px-3 py-1.5 text-sm text-muted-foreground hover:text-foreground hover:bg-muted/50 rounded transition-colors cursor-pointer bg-transparent border-none" onClick={() => addHook(bucket)}><Plus size={12} /> Add</button>
            </div>
          ))}
        </div>
      </details>
    </div>
  );
}

// ── 8. Review ─────────────────────────────────────────────────────────

export function WizardReviewStep({ data }: ReviewStepProps) {
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
      <h3 className="text-sm text-blue-400 mb-4 uppercase tracking-wider">Review</h3>
      <div className="rounded-lg border border-border bg-card overflow-hidden px-4 py-3 mb-4">
        <div className="text-sm mb-1">
          <strong>{data.blueprint.name || data.blueprint.slug || '(unnamed)'}</strong> <Badge variant="secondary">v{data.version}</Badge>
        </div>
        {data.blueprint.description && <div className="text-sm text-muted-foreground mb-1">{data.blueprint.description}</div>}
        {data.blueprint.tags.length > 0 && <div className="mb-1">{data.blueprint.tags.map((t, i) => <span key={i} className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-muted text-muted-foreground mr-1 mb-1">{t}</span>)}</div>}
        <div className="text-sm text-muted-foreground">
          {data.inputs.length} inputs ({requiredInputs} required) · {data.steps.length} sections · {totalTasks} tasks
          {Object.keys(data.env).length > 0 && ` · ${Object.keys(data.env).length} env vars`}
        </div>
      </div>

      {warnings.length > 0 && (
        <div className="px-3 py-2 bg-amber-500/10 border border-amber-500/30 rounded mb-4">
          <div className="text-sm text-amber-400 uppercase mb-1">Warnings</div>
          {warnings.map((w, i) => <div key={i} className="text-sm text-amber-400">- {w}</div>)}
        </div>
      )}

      <div className="flex items-center justify-between mb-1">
        <span className="text-sm font-medium text-muted-foreground">YAML Preview</span>
        <Button variant="ghost" size="xs" onClick={() => { navigator.clipboard.writeText(yaml); toast.success('YAML copied'); }}>Copy</Button>
      </div>
      <pre className="p-3 bg-background border border-border rounded text-sm leading-normal overflow-auto max-h-[50vh] whitespace-pre-wrap break-all">
        {yaml}
      </pre>
    </div>
  );
}
