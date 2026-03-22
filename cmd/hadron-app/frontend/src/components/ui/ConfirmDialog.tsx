import { Modal } from './Modal';

interface ConfirmDialogProps {
  title: string;
  message: string;
  confirmLabel?: string;
  danger?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}

export function ConfirmDialog({ title, message, confirmLabel = 'Confirm', danger, onConfirm, onCancel }: ConfirmDialogProps) {
  return (
    <Modal onClose={onCancel} maxWidth="400px">
      <div style={{ padding: 'var(--space-5)' }}>
        <div style={{ marginBottom: 'var(--space-3)', fontWeight: 600, fontSize: 'var(--text-base)' }}>{title}</div>
        <p style={{ fontSize: 'var(--text-md)', color: 'var(--text-tertiary)', marginBottom: 'var(--space-4)' }}>{message}</p>
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 'var(--space-2)' }}>
          <button className="btn btn-ghost" onClick={onCancel}>Cancel</button>
          <button
            className={danger ? 'btn btn-danger' : 'btn btn-primary'}
            onClick={onConfirm}
          >
            {confirmLabel}
          </button>
        </div>
      </div>
    </Modal>
  );
}
