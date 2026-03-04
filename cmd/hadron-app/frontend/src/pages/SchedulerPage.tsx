import { useState, useCallback } from 'react';
import { toast } from 'sonner';
import { CalendarClock, Plus, Trash2, ToggleLeft, ToggleRight, FolderOpen, Pencil, RefreshCw } from 'lucide-react';
import { usePoll } from '../hooks/usePoll';
import { listSchedules, createSchedule, patchSchedule, deleteSchedule, selectBlueprintFile } from '../api/client';
import { EmptyState } from '../components/ui/EmptyState';
import { CronBuilder } from '../components/ui/CronBuilder';
import type { Schedule, CreateScheduleRequest } from '../api/types';

interface SchedulerPageProps {
  daemonStatus: string;
}

function formatNextRun(ts: string | null | undefined): string {
  if (!ts) return '—';
  return new Date(ts).toLocaleString([], {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

function shortPath(p: string): string {
  const parts = p.split(/[/\\]/);
  return parts.slice(-2).join('/');
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
      {/* Header */}
      <div className="page-header">
        <span className="page-title">Schedules</span>
        <button
          className="hud-button-ghost"
          onClick={refresh}
          title="Refresh (R)"
          style={{ display: 'flex', alignItems: 'center', padding: '0.25rem' }}
        >
          <RefreshCw size={14} />
        </button>
        {loading && <span style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))' }}>Refreshing…</span>}
        <button
          className="hud-button"
          onClick={() => setShowModal(true)}
          style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: '0.4rem' }}
        >
          <Plus size={14} /> New Schedule
        </button>
      </div>

      {/* Schedule table */}
      <div className="hud-panel" style={{ overflow: 'hidden' }}>
        {schedules.length === 0 ? (
          <EmptyState
            message="No schedules"
            sub="Create a schedule to run blueprints on a cron expression"
          />
        ) : (
          <table className="hud-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Blueprint</th>
                <th>Cron</th>
                <th>Enabled</th>
                <th>Next Run</th>
                <th style={{ width: '2rem' }}></th>
              </tr>
            </thead>
            <tbody>
              {schedules.map(schedule => (
                <tr key={schedule.id}>
                  <td style={{ fontFamily: 'monospace', fontSize: '0.82rem' }}>
                    {schedule.name || <span style={{ color: 'rgb(var(--muted))' }}>{schedule.id.slice(-8)}</span>}
                  </td>
                  <td style={{ fontFamily: 'monospace', fontSize: '0.8rem', color: 'rgb(var(--muted))' }}>
                    {shortPath(schedule.blueprint_path)}
                  </td>
                  <td style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
                    {schedule.cron_expr || <span style={{ color: 'rgb(var(--accent))', fontStyle: 'italic', fontFamily: 'inherit' }}>one-time</span>}
                  </td>
                  <td>
                    <button
                      className="hud-button-ghost"
                      onClick={() => handleToggle(schedule)}
                      style={{
                        display: 'flex', alignItems: 'center', gap: '0.3rem',
                        color: schedule.enabled ? 'rgb(var(--ok))' : 'rgb(var(--muted))',
                        fontSize: '0.75rem',
                        padding: '0.15rem 0.4rem',
                      }}
                      title={schedule.enabled ? 'Click to disable' : 'Click to enable'}
                    >
                      {schedule.enabled
                        ? <><ToggleRight size={15} /> ON</>
                        : <><ToggleLeft size={15} /> OFF</>
                      }
                    </button>
                  </td>
                  <td style={{ color: 'rgb(var(--muted))', fontSize: '0.8rem' }}>
                    {formatNextRun(schedule.next_run_at)}
                  </td>
                  <td style={{ display: 'flex', gap: '0.2rem' }}>
                    <button
                      className="hud-button-ghost"
                      onClick={() => handleEdit(schedule)}
                      style={{ padding: '0.15rem 0.35rem' }}
                      title="Edit schedule"
                    >
                      <Pencil size={13} />
                    </button>
                    <button
                      className="hud-button-ghost"
                      onClick={() => handleDelete(schedule)}
                      style={{ color: 'rgb(var(--danger))', padding: '0.15rem 0.35rem' }}
                      title="Delete schedule"
                    >
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
        <div className="hud-modal-overlay" onClick={handleModalClose}>
          <div className="hud-modal" onClick={e => e.stopPropagation()}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1.25rem' }}>
              <CalendarClock size={16} style={{ color: 'rgb(var(--accent))' }} />
              <span style={{ fontWeight: 600, fontSize: '0.9rem', letterSpacing: '0.05em' }}>New Schedule</span>
            </div>

            {/* Name */}
            <div style={{ marginBottom: '0.75rem' }}>
              <label className="hud-label">Name <span style={{ color: 'rgb(var(--muted))' }}>(optional)</span></label>
              <input
                className="hud-input"
                placeholder="e.g. daily-cleanup"
                value={form.name}
                onChange={e => setForm(f => ({ ...f, name: e.target.value }))}
                style={{ width: '100%', boxSizing: 'border-box' }}
              />
            </div>

            {/* Blueprint path */}
            <div style={{ marginBottom: '0.75rem' }}>
              <label className="hud-label">Blueprint Path</label>
              <div style={{ display: 'flex', gap: '0.4rem' }}>
                <input
                  className="hud-input"
                  placeholder="/path/to/blueprint.yaml"
                  value={form.blueprint_path}
                  onChange={e => setForm(f => ({ ...f, blueprint_path: e.target.value }))}
                  style={{ flex: 1 }}
                />
                <button
                  className="hud-button-ghost"
                  onClick={handleBrowse}
                  style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', whiteSpace: 'nowrap', fontSize: '0.78rem' }}
                >
                  <FolderOpen size={13} /> Browse
                </button>
              </div>
            </div>

            {/* Schedule type toggle */}
            <div style={{ marginBottom: '0.75rem' }}>
              <label className="hud-label">Type</label>
              <div style={{ display: 'flex', gap: '0.25rem' }}>
                <button
                  className={schedMode === 'cron' ? 'hud-button' : 'hud-button-ghost'}
                  onClick={() => setSchedMode('cron')}
                  style={{ fontSize: '0.72rem', padding: '0.2rem 0.6rem' }}
                  type="button"
                >
                  Recurring (Cron)
                </button>
                <button
                  className={schedMode === 'once' ? 'hud-button' : 'hud-button-ghost'}
                  onClick={() => setSchedMode('once')}
                  style={{ fontSize: '0.72rem', padding: '0.2rem 0.6rem' }}
                  type="button"
                >
                  One-Time
                </button>
              </div>
            </div>

            {/* Cron expression (recurring) */}
            {schedMode === 'cron' && (
              <div style={{ marginBottom: '0.75rem' }}>
                <label className="hud-label">Cron Expression</label>
                <CronBuilder
                  value={form.cron_expr || '* * * * *'}
                  onChange={cron => setForm(f => ({ ...f, cron_expr: cron }))}
                />
              </div>
            )}

            {/* Date/time picker (one-time) */}
            {schedMode === 'once' && (
              <div style={{ marginBottom: '0.75rem' }}>
                <label className="hud-label">Run At</label>
                <div style={{ display: 'flex', gap: '0.4rem' }}>
                  <input
                    className="hud-input"
                    type="date"
                    value={form.run_at_date}
                    onChange={e => setForm(f => ({ ...f, run_at_date: e.target.value }))}
                    style={{ flex: 1 }}
                  />
                  <input
                    className="hud-input"
                    type="time"
                    value={form.run_at_time}
                    onChange={e => setForm(f => ({ ...f, run_at_time: e.target.value }))}
                    style={{ flex: 1 }}
                  />
                </div>
                <div style={{ fontSize: '0.7rem', color: 'rgb(var(--muted))', marginTop: '0.3rem' }}>
                  Schedule will run once at the specified date and time, then disable itself.
                </div>
              </div>
            )}

            {/* Enabled checkbox */}
            <div style={{ marginBottom: '1rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <input
                type="checkbox"
                id="sched-enabled"
                checked={form.enabled}
                onChange={e => setForm(f => ({ ...f, enabled: e.target.checked }))}
                style={{ accentColor: 'rgb(var(--ok))' }}
              />
              <label htmlFor="sched-enabled" style={{ fontSize: '0.82rem', color: 'rgb(var(--text))', cursor: 'pointer' }}>
                Enabled
              </label>
            </div>

            {/* Error */}
            {formError && (
              <div style={{
                color: 'rgb(var(--danger))',
                fontSize: '0.78rem',
                marginBottom: '0.75rem',
                padding: '0.4rem 0.6rem',
                background: 'rgba(var(--danger) / 0.1)',
                borderRadius: '4px',
                border: '1px solid rgba(var(--danger) / 0.3)',
              }}>
                {formError}
              </div>
            )}

            {/* Actions */}
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.5rem' }}>
              <button className="hud-button-ghost" onClick={handleModalClose}>
                Cancel
              </button>
              <button
                className="hud-button"
                onClick={handleCreate}
                disabled={creating}
              >
                {creating ? 'Creating…' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Edit Schedule modal */}
      {editSchedule && (
        <div className="hud-modal-overlay" onClick={() => setEditSchedule(null)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1.25rem' }}>
              <Pencil size={16} style={{ color: 'rgb(var(--accent))' }} />
              <span style={{ fontWeight: 600, fontSize: '0.9rem', letterSpacing: '0.05em' }}>Edit Schedule</span>
            </div>

            {/* Name */}
            <div style={{ marginBottom: '0.75rem' }}>
              <label className="hud-label">Name <span style={{ color: 'rgb(var(--muted))' }}>(optional)</span></label>
              <input
                className="hud-input"
                placeholder="e.g. daily-cleanup"
                value={editForm.name}
                onChange={e => setEditForm(f => ({ ...f, name: e.target.value }))}
                style={{ width: '100%', boxSizing: 'border-box' }}
              />
            </div>

            {/* Blueprint path */}
            <div style={{ marginBottom: '0.75rem' }}>
              <label className="hud-label">Blueprint Path</label>
              <div style={{ display: 'flex', gap: '0.4rem' }}>
                <input
                  className="hud-input"
                  placeholder="/path/to/blueprint.yaml"
                  value={editForm.blueprint_path}
                  onChange={e => setEditForm(f => ({ ...f, blueprint_path: e.target.value }))}
                  style={{ flex: 1 }}
                />
                <button
                  className="hud-button-ghost"
                  onClick={handleEditBrowse}
                  style={{ display: 'flex', alignItems: 'center', gap: '0.3rem', whiteSpace: 'nowrap', fontSize: '0.78rem' }}
                >
                  <FolderOpen size={13} /> Browse
                </button>
              </div>
            </div>

            {/* Cron expression */}
            <div style={{ marginBottom: '0.75rem' }}>
              <label className="hud-label">Cron Expression</label>
              <CronBuilder
                value={editForm.cron_expr || '* * * * *'}
                onChange={cron => setEditForm(f => ({ ...f, cron_expr: cron }))}
              />
            </div>

            {/* Enabled checkbox */}
            <div style={{ marginBottom: '1rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <input
                type="checkbox"
                id="edit-sched-enabled"
                checked={editForm.enabled}
                onChange={e => setEditForm(f => ({ ...f, enabled: e.target.checked }))}
                style={{ accentColor: 'rgb(var(--ok))' }}
              />
              <label htmlFor="edit-sched-enabled" style={{ fontSize: '0.82rem', color: 'rgb(var(--text))', cursor: 'pointer' }}>
                Enabled
              </label>
            </div>

            {/* Error */}
            {editError && (
              <div style={{
                color: 'rgb(var(--danger))',
                fontSize: '0.78rem',
                marginBottom: '0.75rem',
                padding: '0.4rem 0.6rem',
                background: 'rgba(var(--danger) / 0.1)',
                borderRadius: '4px',
                border: '1px solid rgba(var(--danger) / 0.3)',
              }}>
                {editError}
              </div>
            )}

            {/* Actions */}
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.5rem' }}>
              <button className="hud-button-ghost" onClick={() => setEditSchedule(null)}>
                Cancel
              </button>
              <button
                className="hud-button"
                onClick={handleEditSave}
                disabled={editSaving}
                style={{ borderColor: 'rgba(var(--ok) / 0.5)', color: 'rgb(var(--ok))' }}
              >
                {editSaving ? 'Saving…' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete confirmation modal */}
      {deleteTarget && (
        <div className="hud-modal-overlay" onClick={() => setDeleteTarget(null)}>
          <div className="hud-modal" onClick={e => e.stopPropagation()} style={{ maxWidth: '400px' }}>
            <div style={{ padding: '1.25rem' }}>
              <div style={{ marginBottom: '0.75rem', fontWeight: 600 }}>Delete Schedule</div>
              <div style={{ fontSize: '0.8rem', color: 'rgb(var(--muted))', marginBottom: '1rem' }}>
                Delete schedule <span style={{ fontFamily: 'monospace', color: 'rgb(var(--accent))' }}>
                  {deleteTarget.name || deleteTarget.id.slice(-8)}
                </span>? This cannot be undone.
              </div>
              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                <button className="hud-button-ghost" onClick={() => setDeleteTarget(null)}>Cancel</button>
                <button
                  className="hud-button"
                  style={{ borderColor: 'rgb(var(--danger))', color: 'rgb(var(--danger))' }}
                  onClick={confirmDelete}
                >
                  Delete
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
