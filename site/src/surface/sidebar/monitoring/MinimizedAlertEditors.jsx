import { shortAddr } from "../../format.js";

export function MinimizedAlertEditors({ sessions, onRestore, onClose, inline = false }) {
  return (
    <div className={`ps-monitor-minimized-stack${inline ? " ps-monitor-minimized-stack-inline" : ""}`}>
      {sessions.map((session) => {
        const address = session.machine?.address || session.contract?.address;
        const label = session.machine?.name || shortAddr(address);
        return (
          <div key={session.key} className="ps-monitor-minimized-item">
            <button
              type="button"
              className="ps-monitor-minimized-restore"
              onClick={() => onRestore(session.key)}
              title={`Restore ${label}`}
            >
              <span>Alert</span>
              <strong>{label}</strong>
            </button>
            <button
              type="button"
              className="ps-monitor-minimized-close"
              onClick={() => onClose(session.key)}
              aria-label={`Close minimized alert for ${label}`}
            >
              ×
            </button>
          </div>
        );
      })}
    </div>
  );
}
