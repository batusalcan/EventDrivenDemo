import { useState } from 'react';
import { placeOrder } from '../services/api';

const DEFAULT_ITEMS = 'Product A, Product B';

export default function OrderForm({ activeBroker }) {
  const [customerId, setCustomerId] = useState('customer-001');
  const [amount, setAmount] = useState('149.99');
  const [items, setItems] = useState(DEFAULT_ITEMS);
  const [isVip, setIsVip] = useState(false);
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setLoading(true);
    setStatus(null);
    try {
      const result = await placeOrder({
        customerId,
        amount: parseFloat(amount),
        items: items.split(',').map((s) => s.trim()).filter(Boolean),
        isVip,
      });
      setStatus({ ok: true, message: `Order placed! ID: ${result.orderId} | Tier: ${result.customerTier}` });
    } catch (err) {
      setStatus({ ok: false, message: 'Failed to place order. Is the Order API running?' });
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="panel">
      <h2 className="panel-title">Place Order</h2>
      <p className="panel-subtitle">
        Publishing via <strong>{activeBroker}</strong>
      </p>
      <form className="order-form" onSubmit={handleSubmit}>
        <div className="form-group">
          <label>Customer ID</label>
          <input
            type="text"
            value={customerId}
            onChange={(e) => setCustomerId(e.target.value)}
            required
          />
        </div>

        <div className="form-group">
          <label>Amount ($)</label>
          <input
            type="number"
            step="0.01"
            min="0"
            value={amount}
            onChange={(e) => setAmount(e.target.value)}
            required
          />
        </div>

        <div className="form-group">
          <label>Items (comma separated)</label>
          <input
            type="text"
            value={items}
            onChange={(e) => setItems(e.target.value)}
            required
          />
        </div>

        <div className="form-group form-group--inline">
          <input
            id="vip-checkbox"
            type="checkbox"
            checked={isVip}
            onChange={(e) => setIsVip(e.target.checked)}
          />
          <label htmlFor="vip-checkbox">
            VIP Customer <span className="vip-hint">(routes SMS + Email notification)</span>
          </label>
        </div>

        <button className="submit-btn" type="submit" disabled={loading}>
          {loading ? 'Placing Order...' : '🚀 Place Order'}
        </button>

        {status && (
          <p className={`form-status ${status.ok ? 'form-status--ok' : 'form-status--err'}`}>
            {status.message}
          </p>
        )}
      </form>
    </section>
  );
}
