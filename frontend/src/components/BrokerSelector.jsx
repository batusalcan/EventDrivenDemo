import { switchBroker } from '../services/api';

const BROKERS = [
  {
    id: 'Kafka',
    label: 'Apache Kafka',
    description: 'Local Docker container',
    icon: '⚡',
    color: '#1a73e8',
  },
  {
    id: 'Aws',
    label: 'AWS SNS + SQS',
    description: 'Amazon Web Services',
    icon: '☁',
    color: '#ff9900',
  },
  {
    id: 'Gcp',
    label: 'GCP Pub/Sub',
    description: 'Google Cloud Platform',
    icon: '🌐',
    color: '#34a853',
  },
];

export default function BrokerSelector({ activeBroker, onSwitch }) {
  async function handleSwitch(brokerId) {
    if (brokerId === activeBroker) return;
    try {
      await switchBroker(brokerId);
      onSwitch(brokerId);
    } catch (err) {
      console.error('Failed to switch broker', err);
    }
  }

  return (
    <section className="panel">
      <h2 className="panel-title">Broker Selector</h2>
      <p className="panel-subtitle">Select the active message broker. The switch happens at runtime with zero downtime.</p>
      <div className="broker-grid">
        {BROKERS.map((broker) => {
          const isActive = activeBroker === broker.id;
          return (
            <button
              key={broker.id}
              className={`broker-card ${isActive ? 'broker-card--active' : ''}`}
              style={{ '--broker-color': broker.color }}
              onClick={() => handleSwitch(broker.id)}
            >
              <span className="broker-icon">{broker.icon}</span>
              <span className="broker-label">{broker.label}</span>
              <span className="broker-desc">{broker.description}</span>
              {isActive && <span className="broker-badge">ACTIVE</span>}
            </button>
          );
        })}
      </div>
    </section>
  );
}
