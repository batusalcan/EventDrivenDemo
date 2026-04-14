const ORDER_API = 'http://localhost:5263';

export async function getActiveBroker() {
  const res = await fetch(`${ORDER_API}/api/system/active-broker`);
  return res.json();
}

export async function switchBroker(brokerType) {
  const res = await fetch(`${ORDER_API}/api/system/switch-broker`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ brokerType }),
  });
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
