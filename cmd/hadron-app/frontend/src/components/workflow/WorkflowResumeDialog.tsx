import { useEffect, useState } from 'react';
import { LockKeyhole } from 'lucide-react';

import { resumeWorkflowWait } from '../../api/client';
import type { WorkflowNodeDiagnostic } from '../../api/types';
import { Button } from '../ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../ui/dialog';
import { buildInlineResumeValue } from './workflowResume.helpers';

interface WorkflowResumeDialogProps {
  open: boolean;
  runId: string;
  node?: WorkflowNodeDiagnostic;
  onOpenChange: (open: boolean) => void;
  onResumed: () => void;
}

export function WorkflowResumeDialog({ open, runId, node, onOpenChange, onResumed }: WorkflowResumeDialogProps) {
  const wait = node?.wait;
  const [correlation, setCorrelation] = useState('');
  const [token, setToken] = useState('');
  const [payload, setPayload] = useState('{\n  "decision": "approve"\n}');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [idempotencyKey, setIdempotencyKey] = useState(() => crypto.randomUUID());

  useEffect(() => {
    if (!open) {
      setCorrelation('');
      setToken('');
      setError(null);
      setSubmitting(false);
      setIdempotencyKey(crypto.randomUUID());
    }
  }, [open]);

  const submit = async () => {
    if (!wait || correlation.trim() === '' || token.trim() === '') return;
    setSubmitting(true);
    setError(null);
    try {
      const typedPayload = await buildInlineResumeValue(payload, wait.id);
      await resumeWorkflowWait({
        run_id: runId,
        wait_id: wait.id,
        correlation: correlation.trim(),
        token,
        wake_source: wait.wake_source,
        payload: typedPayload,
        idempotency_key: idempotencyKey,
      });
      setToken('');
      onOpenChange(false);
      onResumed();
    } catch {
      setError('Resume was rejected. Verify the correlation, one-time token, and JSON payload, then try again.');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Resume {node?.definition.display_name || node?.id.node_id || 'workflow wait'}</DialogTitle>
          <DialogDescription>
            Submit a typed private value through the authorized <code>{wait?.wake_source ?? 'wait'}</code> route. The token is sent once and is never displayed in run diagnostics.
          </DialogDescription>
        </DialogHeader>
        <div className="workflow-resume-form">
          <label>
            <span>Correlation</span>
            <input value={correlation} onChange={event => setCorrelation(event.target.value)} autoComplete="off" spellCheck={false} placeholder="Exact wait correlation" />
          </label>
          <label>
            <span><LockKeyhole size={12} /> One-time token</span>
            <input type="password" value={token} onChange={event => setToken(event.target.value)} autoComplete="new-password" spellCheck={false} placeholder="Paste token" />
          </label>
          <label>
            <span>Resume payload · JSON</span>
            <textarea value={payload} onChange={event => setPayload(event.target.value)} rows={6} spellCheck={false} />
          </label>
          {error && <div className="workflow-action-error" role="alert">{error}</div>}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Close</Button>
          <Button onClick={submit} disabled={submitting || !wait || correlation.trim() === '' || token.trim() === ''}>
            {submitting ? 'Resuming…' : 'Resume wait'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
