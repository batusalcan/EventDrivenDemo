import { HubConnectionBuilder, LogLevel } from '@microsoft/signalr';

const HUBS = {
  orderApi:       'http://localhost:5263/hubs/events',
  invoiceApi:     'http://localhost:5034/hubs/events',
  notificationApi:'http://localhost:5059/hubs/events',
};

function buildConnection(url) {
  return new HubConnectionBuilder()
    .withUrl(url)
    .withAutomaticReconnect()
    .configureLogging(LogLevel.Warning)
    .build();
}

export function createConnections(onEvent) {
  const connections = Object.entries(HUBS).map(([, url]) => buildConnection(url));

  connections.forEach((conn) => {
    conn.on('ReceiveEvent', (entry) => onEvent(entry));
    conn.on('Connected', (msg) => onEvent(msg));
  });

  return connections;
}

export async function startConnections(connections) {
  await Promise.allSettled(connections.map((c) => c.start()));
}

export async function stopConnections(connections) {
  await Promise.allSettled(connections.map((c) => c.stop()));
}
