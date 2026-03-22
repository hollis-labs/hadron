import { useState, useCallback } from 'react';
import { toast } from 'sonner';
import { CalendarClock, Plus, Trash2, ToggleLeft, ToggleRight, FolderOpen, Pencil, RefreshCw } from 'lucide-react';
import { usePoll } from '../hooks/usePoll';
import { listSchedules, createSchedule, patchSchedule, deleteSchedule, selectBlueprintFile } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import { CronBuilder } from '../components/ui/CronBuilder';
import { Modal } from '../components/ui/Modal';
import { ConfirmDialog } from '../components/ui/ConfirmDialog';
import { formatNextRun } from '../utils/format';
import { shortPath } from '../utils/path';
import type { Schedule, CreateScheduleRequest } from '../api/types';

interface SchedulerPageProps {
  daemonStatus: string;
}

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

export function SchedulerPage({ daemonStatus }: SchedulerPageProps) {
  const fetcher = useCallback(() => listSchedules(), []);
  const { data, loading, refresh } = usePoll(fetcher, 5000, daemonStatus === 'running');

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
      <div className="page-header">
        <div>
          <div className="page-title">Schedules</div>
          {loading && <div className="page-subtitle">Refreshing…</div>}
        </div>
        <div style={{ display: 'flex', gap: 'var(--space-2)' }}>
          <button className="btn btn-ghost" onClick={refresh} title="Refresh (R)">
            <RefreshCw size={14} />
          </button>
          <button className="btn btn-primary" onClick={() => setShowModal(true)}>
            <Plus size={14} /> New Schedule
          </button>
        </div>
      </div>

      <div className="section">
        {schedules.length === 0 ? (
          <EmptyState message="No schedules" sub="Create a schedule to run blueprints on a cron expression" />
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th className="col-shrink"></th>
                <th className="col-primary">Schedule</th>
                <th className="col-shrink">Cron</th>
                <th className="col-shrink col-right">Next Run</th>
                <th className="col-shrink"></th>
              </tr>
            </thead>
            <tbody>
              {schedules.map(schedule => (
                <tr key={schedule.id} style={{ cursor: 'default' }}>
                  <td className="col-shrink">
                    <button
                      className="btn btn-ghost"
                      onClick={() => handleToggle(schedule)}
                      style={{ padding: '2px 8px', fontSize: 'var(--text-xs)', color: schedule.enabled ? 'var(--accent)' : 'var(--text-tertiary)' }}
                      title={schedule.enabled ? 'Click to disable' : 'Click to enable'}
                    >
                      {schedule.enabled ? <><ToggleRight size={15} /> ON</> : <><ToggleLeft size={15} /> OFF</>}
                    </button>
                  </td>
                  <td className="col-primary">
                    <div style={{ fontWeight: 500 }}>
                      {schedule.name || <span style={{ color: 'var(--text-tertiary)' }}>{schedule.id.slice(-8)}</span>}
                    </div>
                    <div className="mono" style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: 1 }}>
                      {shortPath(schedule.blueprint_path)}
                    </div>
                  </td>
                  <td className="mono col-shrink">
                    {schedule.cron_expr || <span style={{ color: 'var(--accent)', fontStyle: 'italic' }}>one-time</span>}
                  </td>
                  <td className="col-shrink col-right" style={{ color: 'var(--text-tertiary)' }}>{formatNextRun(schedule.next_run_at)}</td>
                  <td className="col-shrink" style={{ display: 'flex', gap: 4 }}>
                    <button className="btn btn-ghost" onClick={() => handleEdit(schedule)} style={{ padding: '2px 6px' }} title="Edit">
                      <Pencil size={13} />
                    </button>
                    <button className="btn btn-ghost" onClick={() => handleDelete(schedule)} style={{ padding: '2px 6px', color: 'var(--status-failed)' }} title="Delete">
                      <Trash2 size={13} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* New Schedule modal */}
      {showModal && (
        <Modal onClose={handleModalClose} maxWidth="520px">
          <div style={{ padding: 'var(--space-5)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', marginBottom: 'var(--space-5)' }}>
              <CalendarClock size={16} style={{ color: 'var(--accent)' }} />
              <span style={{ fontWeight: 600, fontSize: 'var(--text-base)' }}>New Schedule</span>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)' }}>
              <div>
                <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Name <span style={{ color: 'var(--text-tertiary)' }}>(optional)</span></label>
                <input className="hud-input" placeholder="e.g. daily-cleanup" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} style={{ width: '100%', boxSizing: 'border-box' }} />
              </div>

              <div>
                <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Blueprint Path</label>
                <div style={{ display: 'flex', gap: 'var(--space-2)' }}>
                  <input className="hud-input" placeholder="/path/to/blueprint.yaml" value={form.blueprint_path} onChange={e => setForm(f => ({ ...f, blueprint_path: e.target.value }))} style={{ flex: 1 }} />
                  <button className="btn btn-ghost" onClick={handleBrowse}><FolderOpen size={13} /> Browse</button>
                </div>
              </div>

              <div>
                <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Type</label>
                <div style={{ display: 'flex', gap: 'var(--space-1)' }}>
                  <button className={`btn ${schedMode === 'cron' ? '' : 'btn-ghost'}`} onClick={() => setSchedMode('cron')} type="button" style={{ fontSize: 'var(--text-xs)' }}>Recurring (Cron)</button>
                  <button className={`btn ${schedMode === 'once' ? '' : 'btn-ghost'}`} onClick={() => setSchedMode('once')} type="button" style={{ fontSize: 'var(--text-xs)' }}>One-Time</button>
                </div>
              </div>

              {schedMode === 'cron' && (
                <div>
                  <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Cron Expression</label>
                  <CronBuilder value={form.cron_expr || '* * * * *'} onChange={cron => setForm(f => ({ ...f, cron_expr: cron }))} />
                </div>
              )}

              {schedMode === 'once' && (
                <div>
                  <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Run At</label>
                  <div style={{ display: 'flex', gap: 'var(--space-2)' }}>
                    <input className="hud-input" type="date" value={form.run_at_date} onChange={e => setForm(f => ({ ...f, run_at_date: e.target.value }))} style={{ flex: 1 }} />
                    <input className="hud-input" type="time" value={form.run_at_time} onChange={e => setForm(f => ({ ...f, run_at_time: e.target.value }))} style={{ flex: 1 }} />
                  </div>
                  <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', marginTop: 'var(--space-1)' }}>Runs once then disables itself.</div>
                </div>
              )}

              <label style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', cursor: 'pointer' }}>
                <input type="checkbox" checked={form.enabled} onChange={e => setForm(f => ({ ...f, enabled: e.target.checked }))} style={{ accentColor: 'var(--accent)' }} />
                <span style={{ fontSize: 'var(--text-md)' }}>Enabled</span>
              </label>

              {formError && (
                <div style={{ color: 'var(--status-failed)', fontSize: 'var(--text-sm)', padding: 'var(--space-2) var(--space-3)', background: 'var(--status-failed-bg)', borderRadius: 'var(--radius-sm)', border: '1px solid rgba(239, 68, 68, 0.3)' }}>
                  {formError}
                </div>
              )}
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 'var(--space-2)', marginTop: 'var(--space-5)' }}>
              <button className="btn btn-ghost" onClick={handleModalClose}>Cancel</button>
              <button className="btn btn-primary" onClick={handleCreate} disabled={creating}>{creating ? 'Creating…' : 'Create'}</button>
            </div>
          </div>
        </Modal>
      )}

      {/* Edit Schedule modal */}
      {editSchedule && (
        <Modal onClose={() => setEditSchedule(null)} maxWidth="520px">
          <div style={{ padding: 'var(--space-5)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', marginBottom: 'var(--space-5)' }}>
              <Pencil size={16} style={{ color: 'var(--accent)' }} />
              <span style={{ fontWeight: 600, fontSize: 'var(--text-base)' }}>Edit Schedule</span>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)' }}>
              <div>
                <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Name <span style={{ color: 'var(--text-tertiary)' }}>(optional)</span></label>
                <input className="hud-input" placeholder="e.g. daily-cleanup" value={editForm.name} onChange={e => setEditForm(f => ({ ...f, name: e.target.value }))} style={{ width: '100%', boxSizing: 'border-box' }} />
              </div>

              <div>
                <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Blueprint Path</label>
                <div style={{ display: 'flex', gap: 'var(--space-2)' }}>
                  <input className="hud-input" placeholder="/path/to/blueprint.yaml" value={editForm.blueprint_path} onChange={e => setEditForm(f => ({ ...f, blueprint_path: e.target.value }))} style={{ flex: 1 }} />
                  <button className="btn btn-ghost" onClick={handleEditBrowse}><FolderOpen size={13} /> Browse</button>
                </div>
              </div>

              <div>
                <label style={{ display: 'block', fontSize: 'var(--text-sm)', fontWeight: 500, marginBottom: 'var(--space-1)' }}>Cron Expression</label>
                <CronBuilder value={editForm.cron_expr || '* * * * *'} onChange={cron => setEditForm(f => ({ ...f, cron_expr: cron }))} />
              </div>

              <label style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', cursor: 'pointer' }}>
                <input type="checkbox" checked={editForm.enabled} onChange={e => setEditForm(f => ({ ...f, enabled: e.target.checked }))} style={{ accentColor: 'var(--accent)' }} />
                <span style={{ fontSize: 'var(--text-md)' }}>Enabled</span>
              </label>

              {editError && (
                <div style={{ color: 'var(--status-failed)', fontSize: 'var(--text-sm)', padding: 'var(--space-2) var(--space-3)', background: 'var(--status-failed-bg)', borderRadius: 'var(--radius-sm)', border: '1px solid rgba(239, 68, 68, 0.3)' }}>
                  {editError}
                </div>
              )}
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 'var(--space-2)', marginTop: 'var(--space-5)' }}>
              <button className="btn btn-ghost" onClick={() => setEditSchedule(null)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleEditSave} disabled={editSaving}>{editSaving ? 'Saving…' : 'Save'}</button>
            </div>
          </div>
        </Modal>
      )}

      {/* Delete confirmation */}
      {deleteTarget && (
        <ConfirmDialog
          title="Delete Schedule"
          message={`Delete schedule "${deleteTarget.name || deleteTarget.id.slice(-8)}"? This cannot be undone.`}
          confirmLabel="Delete"
          danger
          onConfirm={confirmDelete}
          onCancel={() => setDeleteTarget(null)}
        />
      )}
    </div>
  );
}
