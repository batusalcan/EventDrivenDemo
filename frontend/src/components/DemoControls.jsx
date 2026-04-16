import { useEffect, useState } from 'react';
import { getInvoiceConsumerStatus, pauseInvoiceConsumer, resumeInvoiceConsumer } from '../services/api';

export default function DemoControls() {
  const [isPaused, setIsPaused] = useState(false);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    getInvoiceConsumerStatus()
      .then(data => setIsPaused(data.isPaused))
      .catch(() => {});
  }, []);

  async function handleToggle() {
    setLoading(true);
    try {
      if (isPaused) {
        await resumeInvoiceConsumer();
        setIsPaused(false);
      } else {
        await pauseInvoiceConsumer();
        setIsPaused(true);
      }
    } catch (err) {
      console.error('Failed to toggle consumer', err);
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="panel">
      <h2 className="panel-title">Demo Controls</h2>
      <p className="panel-subtitle">Simulate advanced scenarios without touching the terminal.</p>

      <div className="demo-scenarios">

        {/* Fault Tolerance */}
        <div className="scenario-card">
          <div className="scenario-info">
            <span className="scenario-title">Fault Tolerance Demo</span>
            <span className="scenario-desc">
              Pause the Invoice consumer to simulate downtime. Place orders — they queue in the broker.
              Resume to watch all missed messages process instantly.
            </span>
            <span className={`consumer-status ${isPaused ? 'consumer-status--paused' : 'consumer-status--running'}`}>
              Invoice Consumer: {isPaused ? '⏸ PAUSED — messages queuing in broker' : '▶ RUNNING'}
            </span>
          </div>
          <button
            className={`demo-btn ${isPaused ? 'demo-btn--resume' : 'demo-btn--pause'}`}
            onClick={handleToggle}
            disabled={loading}
          >
            {loading ? '...' : isPaused ? '▶ Resume Consumer' : '⏸ Pause Consumer'}
          </button>
        </div>

        {/* Horizontal Scaling */}
        <div className="scenario-card">
          <div className="scenario-info">
            <span className="scenario-title">Horizontal Scaling Demo</span>
            <span className="scenario-desc">
              Run two Invoice API instances in the same Kafka consumer group.
              The broker distributes messages between them via round-robin — each message processed exactly once.
            </span>
          </div>
          <code className="demo-cmd">docker-compose up --scale invoice-api=2</code>
        </div>

      </div>
    </section>
  );
}
