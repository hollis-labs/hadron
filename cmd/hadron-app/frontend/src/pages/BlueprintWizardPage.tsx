import { ChevronLeft } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { useNavigation } from '@/contexts/NavigationContext';
import { useWizardState } from '@/hooks/useWizardState';
import {
  WizardMetadataStep,
  WizardProjectStep,
  WizardEnvStep,
  WizardPackagesStep,
  WizardInputsStep,
  WizardStepsStep,
  WizardAdvancedStep,
  WizardReviewStep,
} from '@/components/wizard/WizardSteps';

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

export function BlueprintWizardPage() {
  const nav = useNavigation();
  const editPath = nav.wizardEditPath;
  const onBack = nav.goBack;

  const {
    data, setData,
    currentStep, setCurrentStep,
    saving,
    newTag, setNewTag,
    updateBlueprint, updateProject,
    handleSave,
  } = useWizardState({ editPath, onBack });

  const stepContent = [
    <WizardMetadataStep key="metadata" data={data} setData={setData} newTag={newTag} setNewTag={setNewTag} updateBlueprint={updateBlueprint} />,
    <WizardProjectStep key="project" data={data} setData={setData} updateProject={updateProject} />,
    <WizardEnvStep key="env" data={data} setData={setData} />,
    <WizardPackagesStep key="packages" data={data} setData={setData} />,
    <WizardInputsStep key="inputs" data={data} setData={setData} />,
    <WizardStepsStep key="steps" data={data} setData={setData} />,
    <WizardAdvancedStep key="advanced" data={data} setData={setData} />,
    <WizardReviewStep key="review" data={data} setData={setData} saving={saving} />,
  ];

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between mb-6 pb-2 gap-2">
        <Button variant="ghost" onClick={onBack}>
          <ChevronLeft size={13} /> Back
        </Button>
        <span className="text-xl font-semibold text-foreground tracking-tight">{editPath ? 'Edit Blueprint' : 'New Blueprint'}</span>
      </div>

      {/* Body */}
      <div className="flex flex-1 overflow-hidden gap-4">
        {/* Sidebar */}
        <div className="w-48 shrink-0 flex flex-col gap-1 overflow-y-auto py-2">
          {WIZARD_STEPS.map((step, i) => (
            <button key={step.key}
              className={cn(
                'text-left px-3 py-2 rounded text-sm transition-colors hover:bg-muted/50',
                currentStep === i && 'bg-muted text-foreground font-medium'
              )}
              onClick={() => setCurrentStep(i)}
            >
              <div>{step.title}</div>
              <div className="text-xs text-muted-foreground mt-0.5">{step.desc}</div>
            </button>
          ))}
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto px-4 py-3">
          {stepContent[currentStep]}
        </div>
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between py-3 border-t border-border shrink-0">
        <Button variant="ghost" onClick={() => setCurrentStep(Math.max(0, currentStep - 1))} disabled={currentStep === 0}>
          Previous
        </Button>
        <span className="text-sm text-muted-foreground">Step {currentStep + 1} of {WIZARD_STEPS.length}</span>
        <div className="flex gap-2">
          <Button variant="ghost" onClick={() => setCurrentStep(Math.min(WIZARD_STEPS.length - 1, currentStep + 1))} disabled={currentStep === WIZARD_STEPS.length - 1}>
            Next
          </Button>
          <Button onClick={handleSave} disabled={saving} className="border-blue-500/50 text-blue-400">
            {saving ? 'Saving...' : 'Save'}
          </Button>
        </div>
      </div>
    </div>
  );
}
