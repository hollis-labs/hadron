import { LockKeyhole } from 'lucide-react';

import type { WorkflowGraphDiagnostic } from '../../api/types';

export function WorkflowEventTrail({ result }: { result: WorkflowGraphDiagnostic }) {
  const events = result.events ?? [];
  return (
    <section className="workflow-detail-panel">
      <header><div><span>Durable event trail</span><strong>{events.length} events</strong></div><p>Persisted operational facts, ordered by sequence.</p></header>
      <div className="workflow-event-scroll">
        {events.length === 0 ? (
          <div className="workflow-empty-state">No durable events were returned for this run.</div>
        ) : events.map(event => (
          <article className="workflow-event-row" key={event.sequence}>
            <time>{new Date(event.occurred_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</time>
            <code>#{event.sequence}</code>
            <div>
              <strong>{event.type}</strong>
              <span>{event.invocation?.node_id || 'run'}</span>
            </div>
            <p>
              {event.masked
                ? <><LockKeyhole size={11} /> [REDACTED]</>
                : Object.entries(event.attributes ?? {}).map(([key, value]) => `${key}=${value}`).join(' · ') || 'No attributes'}
            </p>
          </article>
        ))}
      </div>
    </section>
  );
}
