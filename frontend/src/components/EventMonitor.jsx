import { useEffect, useRef } from 'react';

function colorize(entry) {
  if (entry.includes('[OrderApi'))        return '#60a5fa';
  if (entry.includes('[InvoiceApi'))      return '#34d399';
  if (entry.includes('[NotificationApi')) return '#f59e0b';
  return '#94a3b8';
}

export default function EventMonitor({ events, connected, onClear }) {
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [events]);

  return (
    <section className="panel panel--monitor">
      <div className="monitor-header">
        <h2 className="panel-title">
          Live Event Monitor
          <span className={`connection-dot ${connected ? 'connection-dot--on' : 'connection-dot--off'}`} />
        </h2>
        <button className="clear-btn" onClick={onClear}>Clear</button>
      </div>
      <p className="panel-subtitle">
        Real-time events from OrderApi · InvoiceApi · NotificationApi via SignalR WebSockets
      </p>
      <div className="console">
        {events.length === 0 && (
          <p className="console-empty">Waiting for events... Place an order to see the fan-out in action.</p>
        )}
        {events.map((entry, i) => (
          <p key={i} className="console-line" style={{ color: colorize(entry) }}>
            {entry}
          </p>
        ))}
        <div ref={bottomRef} />
      </div>
    </section>
  );
}
