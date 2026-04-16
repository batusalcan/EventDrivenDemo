const ORDER_API        = 'http://localhost:5263';
const INVOICE_API      = 'http://localhost:5034';
const NOTIFICATION_API = 'http://localhost:5059';

export async function getActiveBroker() {
  const res = await fetch(`${ORDER_API}/api/system/active-broker`);
  return res.json();
}

// Switches the active broker on ALL three services simultaneously.
// The publisher (OrderApi) swaps its strategy; the consumer routers in
// InvoiceApi and NotificationApi detect the change and restart their loops.
export async function switchBroker(brokerType) {
  const body = JSON.stringify({ brokerType });
  const opts = { method: 'POST', headers: { 'Content-Type': 'application/json' }, body };

  const [order] = await Promise.all([
    fetch(`${ORDER_API}/api/system/switch-broker`, opts).then(r => r.json()),
    fetch(`${INVOICE_API}/api/system/switch-broker`, opts).then(r => r.json()),
    fetch(`${NOTIFICATION_API}/api/system/switch-broker`, opts).then(r => r.json()),
  ]);

  // Return the OrderApi response as the canonical result (all three should agree).
  return order;
}

export async function getInvoiceConsumerStatus() {
  const res = await fetch(`${INVOICE_API}/api/system/consumer-status`);
  return res.json();
}

export async function pauseInvoiceConsumer() {
  const res = await fetch(`${INVOICE_API}/api/system/pause-consumer`, { method: 'POST' });
  return res.json();
}

export async function resumeInvoiceConsumer() {
  const res = await fetch(`${INVOICE_API}/api/system/resume-consumer`, { method: 'POST' });
  return res.json();
}

export async function placeOrder(order) {
  const res = await fetch(`${ORDER_API}/api/orders`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(order),
  });
  return res.json();
}
