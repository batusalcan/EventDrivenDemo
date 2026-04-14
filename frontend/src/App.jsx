import { useEffect, useRef, useState } from 'react';
import BrokerSelector from './components/BrokerSelector';
import EventMonitor from './components/EventMonitor';
import OrderForm from './components/OrderForm';
import { getActiveBroker } from './services/api';
import { createConnections, startConnections, stopConnections } from './services/signalr';
import './App.css';

export default function App() {
  const [activeBroker, setActiveBroker] = useState('Kafka');
  const [events, setEvents] = useState([]);
  const [connected, setConnected] = useState(false);
  const connectionsRef = useRef([]);

  // Load active broker on mount
  useEffect(() => {
    getActiveBroker()
      .then((data) => setActiveBroker(data.activeBroker))
      .catch(() => {});
  }, []);

  // Start SignalR connections on mount
  useEffect(() => {
    const conns = createConnections((entry) => {
      setEvents((prev) => [...prev.slice(-499), entry]);
    });
    connectionsRef.current = conns;

    startConnections(conns).then(() => setConnected(true));

    return () => {
      stopConnections(conns);
    };
  }, []);

  return (
    <div className="app">
      <header className="app-header">
        <h1 className="app-title">Event-Driven Architecture — System Control Center</h1>
        <p className="app-subtitle">Strategy Pattern · Multi-Broker · Fan-Out · Real-Time Streaming</p>
      </header>

      <main className="app-main">
        <div className="top-row">
          <BrokerSelector activeBroker={activeBroker} onSwitch={setActiveBroker} />
          <OrderForm activeBroker={activeBroker} />
        </div>

        <EventMonitor
          events={events}
          connected={connected}
          onClear={() => setEvents([])}
        />
      </main>
    </div>
  );
}
