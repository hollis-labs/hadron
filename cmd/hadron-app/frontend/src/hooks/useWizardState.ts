import { useState, useEffect, useCallback } from 'react';
import { toast } from 'sonner';
import { parseBlueprintFull, saveBlueprintFile, createBlueprintFile, getPreference } from '@/api/client';
import type { WizardBlueprint, WizardInput, WizardTask } from '@/api/types';
import { convertWizardToYaml, convertParsedToWizard } from '@/utils/blueprintYaml';

const AUTOSAVE_KEY = 'hadron_wizard_draft';

export function newWizardInput(): WizardInput {
  return { name: '', label: '', description: '', type: 'string', required: false, default_value: '', enum_values: '', pattern: '', min_length: '', max_length: '', min: '', max: '', items_type: '' };
}

export function newWizardTask(): WizardTask {
  return { name: '', cmd: '', call: '', if_expr: '', dir: '', env: {}, retry: '0', retry_delay_seconds: '0', timeout_seconds: '0', continue_on_error: false, enabled: true, on_success: [], on_fail: [] };
}

export function defaultWizard(): WizardBlueprint {
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

interface UseWizardStateOptions {
  editPath: string | null | undefined;
  onBack: () => void;
}

export function useWizardState({ editPath, onBack }: UseWizardStateOptions) {
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

  return {
    data,
    setData,
    currentStep,
    setCurrentStep,
    saving,
    loaded,
    newTag,
    setNewTag,
    updateBlueprint,
    updateProject,
    handleSave,
  };
}
