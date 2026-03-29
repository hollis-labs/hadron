import { useState, useCallback } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { CalendarClock, Plus, Trash2, ToggleLeft, ToggleRight, FolderOpen, Pencil, RefreshCw } from 'lucide-react';
import { useDaemon } from '../contexts/DaemonContext';
import { usePoll } from '../hooks/usePoll';
import { listSchedules, createSchedule, patchSchedule, deleteSchedule, selectBlueprintFile } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import { CronBuilder } from '../components/ui/CronBuilder';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from '@/components/ui/dialog';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from '@/components/ui/alert-dialog';
import { formatNextRun } from '../utils/format';
import { shortPath } from '../utils/path';
import { cn } from '@/lib/utils';
import type { Schedule, CreateScheduleRequest } from '../api/types';

type ScheduleMode = 'cron' | 'once';

interface NewScheduleForm {
  name: string;
  blueprint_path: string;
  cron_expr: string;
  run_at_date: string;
  run_at_time: string;
  enabled: boolean;
}

const EMPTY_FORM: NewScheduleForm = {
  name: '',
  blueprint_path: '',
  cron_expr: '',
  run_at_date: '',
  run_at_time: '',
  enabled: true,
};

export function SchedulerPage() {
  const daemon = useDaemon();
  const fetcher = useCallback(() => listSchedules(), []);
  const { data, loading, refresh } = usePoll(fetcher, 5000, daemon.status === 'running');

  const [showModal, setShowModal] = useState(false);
  const [schedMode, setSchedMode] = useState<ScheduleMode>('cron');
  const [form, setForm] = useState<NewScheduleForm>(EMPTY_FORM);
  const [creating, setCreating] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);

  // Edit modal state
  const [editSchedule, setEditSchedule] = useState<Schedule | null>(null);
  const [editForm, setEditForm] = useState<NewScheduleForm>(EMPTY_FORM);
  const [editSaving, setEditSaving] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  const schedules: Schedule[] = data?.items ?? [];

  const handleToggle = async (schedule: Schedule) => {
    try {
      await patchSchedule(schedule.id, { enabled: !schedule.enabled });
      toast.success(`Schedule ${!schedule.enabled ? 'enabled' : 'disabled'}`);
      refresh();
    } catch (err) {
      toast.error(`Failed to update schedule: ${err}`);
    }
  };

  const [deleteTarget, setDeleteTarget] = useState<Schedule | null>(null);

  const handleDelete = async (schedule: Schedule) => {
    setDeleteTarget(schedule);
  };

  const confirmDelete = async () => {
    if (!deleteTarget) return;
    try {
      await deleteSchedule(deleteTarget.id);
      toast.success('Schedule deleted');
      setDeleteTarget(null);
      refresh();
    } catch (err) {
      toast.error(`Failed to delete schedule: ${err}`);
    }
  };

  const handleBrowse = async () => {
    const path = await selectBlueprintFile();
    if (path) {
      setForm(f => ({ ...f, blueprint_path: path }));
    }
  };

  const handleCreate = async () => {
    if (!form.blueprint_path.trim()) {
      setFormError('Blueprint path is required');
      return;
    }
    if (schedMode === 'cron' && !form.cron_expr.trim()) {
      setFormError('Cron expression is required');
      return;
    }
    if (schedMode === 'once' && (!form.run_at_date || !form.run_at_time)) {
      setFormError('Date and time are required for one-time schedule');
      return;
    }
    setFormError(null);
    setCreating(true);
    try {
      const req: CreateScheduleRequest = {
        blueprint_path: form.blueprint_path.trim(),
        enabled: form.enabled,
      };
      if (form.name.trim()) req.name = form.name.trim();
      if (schedMode === 'cron') {
        req.cron_expr = form.cron_expr.trim();
      } else {
        req.run_at = new Date(`${form.run_at_date}T${form.run_at_time}`).toISOString();
      }
      await createSchedule(req);
      toast.success('Schedule created');
      setShowModal(false);
      setForm(EMPTY_FORM);
      refresh();
    } catch (err) {
      toast.error(`Failed to create schedule: ${err}`);
      setFormError(String(err));
    } finally {
      setCreating(false);
    }
  };

  const handleModalClose = () => {
    setShowModal(false);
    setForm(EMPTY_FORM);
    setSchedMode('cron');
    setFormError(null);
  };

  const handleEdit = (schedule: Schedule) => {
    setEditSchedule(schedule);
    setEditForm({
      name: schedule.name || '',
      blueprint_path: schedule.blueprint_path,
      cron_expr: schedule.cron_expr,
      run_at_date: '',
      run_at_time: '',
      enabled: schedule.enabled,
    });
    setEditError(null);
  };

  const handleEditBrowse = async () => {
    const path = await selectBlueprintFile();
    if (path) setEditForm(f => ({ ...f, blueprint_path: path }));
  };

  const handleEditSave = async () => {
    if (!editSchedule) return;
    if (!editForm.cron_expr.trim()) { setEditError('Cron expression is required'); return; }
    if (!editForm.blueprint_path.trim()) { setEditError('Blueprint path is required'); return; }
    setEditError(null);
    setEditSaving(true);
    try {
      await patchSchedule(editSchedule.id, {
        name: editForm.name.trim(),
        cron_expr: editForm.cron_expr.trim(),
        blueprint_path: editForm.blueprint_path.trim(),
        enabled: editForm.enabled,
      });
      toast.success('Schedule updated');
      setEditSchedule(null);
      refresh();
    } catch (err) {
      toast.error(`Failed to update: ${err}`);
      setEditError(String(err));
    } finally {
      setEditSaving(false);
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <div className="text-xl font-semibold text-foreground tracking-tight">Schedules</div>
          {loading && <div className="text-sm text-muted-foreground">Refreshing…</div>}
        </div>
        <div className="flex gap-2">
          <Button variant="ghost" onClick={refresh} title="Refresh (R)">
            <RefreshCw size={14} />
          </Button>
          <Button onClick={() => setShowModal(true)}>
            <Plus size={14} /> New Schedule
          </Button>
        </div>
      </div>

      <div className="rounded-lg border border-border bg-card overflow-hidden">
        {schedules.length === 0 ? (
          <EmptyState message="No schedules" sub="Create a schedule to run blueprints on a cron expression" />
        ) : (
          <table className="w-full border-collapse">
            <thead>
              <tr>
                <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap"></th>
                <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 w-full">Schedule</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap">Cron</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap text-right">Next Run</th>
                <th className="text-left px-5 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider bg-muted/50 whitespace-nowrap"></th>
              </tr>
            </thead>
            <tbody>
              {schedules.map(schedule => (
                <tr key={schedule.id} className="cursor-default transition-colors hover:bg-muted/50">
                  <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border whitespace-nowrap">
                    <Button
                      variant="ghost"
                      size="xs"
                      onClick={() => handleToggle(schedule)}
                      className={cn(schedule.enabled ? 'text-primary' : 'text-muted-foreground')}
                      title={schedule.enabled ? 'Click to disable' : 'Click to enable'}
                    >
                      {schedule.enabled ? <><ToggleRight size={15} /> ON</> : <><ToggleLeft size={15} /> OFF</>}
                    </Button>
                  </td>
                  <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border w-full">
                    <div className="font-medium">
                      {schedule.name || <span className="text-muted-foreground">{schedule.id.slice(-8)}</span>}
                    </div>
                    <div className="font-mono text-xs text-muted-foreground mt-px">
                      {shortPath(schedule.blueprint_path)}
                    </div>
                  </td>
                  <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border font-mono whitespace-nowrap">
                    {schedule.cron_expr || <span className="text-primary italic">one-time</span>}
                  </td>
                  <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border whitespace-nowrap text-right">{formatNextRun(schedule.next_run_at)}</td>
                  <td className="px-5 py-3 text-sm text-muted-foreground border-t border-border whitespace-nowrap">
                    <div className="flex gap-1">
                      <Button variant="ghost" size="xs" onClick={() => handleEdit(schedule)} title="Edit">
                        <Pencil size={13} />
                      </Button>
                      <Button variant="ghost" size="xs" onClick={() => handleDelete(schedule)} className="text-red-400" title="Delete">
                        <Trash2 size={13} />
                      </Button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* New Schedule modal */}
      <Dialog open={showModal} onOpenChange={(open) => { if (!open) handleModalClose(); }}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>
              <div className="flex items-center gap-2">
                <CalendarClock size={16} className="text-primary" />
                <span>New Schedule</span>
              </div>
            </DialogTitle>
          </DialogHeader>

          <div className="flex flex-col gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">Name <span className="text-muted-foreground">(optional)</span></label>
              <Input placeholder="e.g. daily-cleanup" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} className="w-full" />
            </div>

            <div>
              <label className="block text-sm font-medium mb-1">Blueprint Path</label>
              <div className="flex gap-2">
                <Input placeholder="/path/to/blueprint.yaml" value={form.blueprint_path} onChange={e => setForm(f => ({ ...f, blueprint_path: e.target.value }))} className="flex-1" />
                <Button variant="ghost" onClick={handleBrowse}><FolderOpen size={13} /> Browse</Button>
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium mb-1">Type</label>
              <div className="flex gap-1">
                <Button variant={schedMode === 'cron' ? "outline" : "ghost"} onClick={() => setSchedMode('cron')} type="button" size="xs">Recurring (Cron)</Button>
                <Button variant={schedMode === 'once' ? "outline" : "ghost"} onClick={() => setSchedMode('once')} type="button" size="xs">One-Time</Button>
              </div>
            </div>

            {schedMode === 'cron' && (
              <div>
                <label className="block text-sm font-medium mb-1">Cron Expression</label>
                <CronBuilder value={form.cron_expr || '* * * * *'} onChange={cron => setForm(f => ({ ...f, cron_expr: cron }))} />
              </div>
            )}

            {schedMode === 'once' && (
              <div>
                <label className="block text-sm font-medium mb-1">Run At</label>
                <div className="flex gap-2">
                  <Input type="date" value={form.run_at_date} onChange={e => setForm(f => ({ ...f, run_at_date: e.target.value }))} className="flex-1" />
                  <Input type="time" value={form.run_at_time} onChange={e => setForm(f => ({ ...f, run_at_time: e.target.value }))} className="flex-1" />
                </div>
                <div className="text-xs text-muted-foreground mt-1">Runs once then disables itself.</div>
              </div>
            )}

            <label className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" checked={form.enabled} onChange={e => setForm(f => ({ ...f, enabled: e.target.checked }))} className="accent-primary" />
              <span className="text-sm">Enabled</span>
            </label>

            {formError && (
              <div className="text-red-400 text-sm px-3 py-2 bg-red-500/10 rounded border border-red-500/30">
                {formError}
              </div>
            )}
          </div>

          <DialogFooter>
            <Button variant="ghost" onClick={handleModalClose}>Cancel</Button>
            <Button onClick={handleCreate} disabled={creating}>{creating ? 'Creating…' : 'Create'}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Edit Schedule modal */}
      <Dialog open={!!editSchedule} onOpenChange={(open) => { if (!open) setEditSchedule(null); }}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>
              <div className="flex items-center gap-2">
                <Pencil size={16} className="text-primary" />
                <span>Edit Schedule</span>
              </div>
            </DialogTitle>
          </DialogHeader>

          <div className="flex flex-col gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">Name <span className="text-muted-foreground">(optional)</span></label>
              <Input placeholder="e.g. daily-cleanup" value={editForm.name} onChange={e => setEditForm(f => ({ ...f, name: e.target.value }))} className="w-full" />
            </div>

            <div>
              <label className="block text-sm font-medium mb-1">Blueprint Path</label>
              <div className="flex gap-2">
                <Input placeholder="/path/to/blueprint.yaml" value={editForm.blueprint_path} onChange={e => setEditForm(f => ({ ...f, blueprint_path: e.target.value }))} className="flex-1" />
                <Button variant="ghost" onClick={handleEditBrowse}><FolderOpen size={13} /> Browse</Button>
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium mb-1">Cron Expression</label>
              <CronBuilder value={editForm.cron_expr || '* * * * *'} onChange={cron => setEditForm(f => ({ ...f, cron_expr: cron }))} />
            </div>

            <label className="flex items-center gap-2 cursor-pointer">
              <input type="checkbox" checked={editForm.enabled} onChange={e => setEditForm(f => ({ ...f, enabled: e.target.checked }))} className="accent-primary" />
              <span className="text-sm">Enabled</span>
            </label>

            {editError && (
              <div className="text-red-400 text-sm px-3 py-2 bg-red-500/10 rounded border border-red-500/30">
                {editError}
              </div>
            )}
          </div>

          <DialogFooter>
            <Button variant="ghost" onClick={() => setEditSchedule(null)}>Cancel</Button>
            <Button onClick={handleEditSave} disabled={editSaving}>{editSaving ? 'Saving…' : 'Save'}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete confirmation */}
      <AlertDialog open={!!deleteTarget} onOpenChange={(open) => { if (!open) setDeleteTarget(null); }}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Schedule</AlertDialogTitle>
            <AlertDialogDescription>
              {deleteTarget ? `Delete schedule "${deleteTarget.name || deleteTarget.id.slice(-8)}"? This cannot be undone.` : ''}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction variant="destructive" onClick={confirmDelete}>Delete</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
