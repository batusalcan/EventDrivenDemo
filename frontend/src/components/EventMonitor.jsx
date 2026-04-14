import { useEffect, useRef } from 'react';

const SOURCE_COLORS = {
  '[OrderApi]':       '#60a5fa',
  '[InvoiceApi]':     '#34d399',
  '[NotificationApi]':'#f59e0b',
};

function colorize(entry) {
  for (const [key, color] of Object.entries(SOURCE_COLORS)) {
    if (entry.includes(key)) return color;
  }
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
