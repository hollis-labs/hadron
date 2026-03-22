import type { ReactNode } from 'react';

interface ModalProps {
  onClose: () => void;
  maxWidth?: string;
  children: ReactNode;
}

export function Modal({ onClose, maxWidth, children }: ModalProps) {
  return (
    <div className="hud-modal-overlay" onClick={onClose}>
      <div
        className="hud-modal"
        onClick={e => e.stopPropagation()}
        style={maxWidth ? { maxWidth } : undefined}
      >
        {children}
      </div>
    </div>
  );
}
