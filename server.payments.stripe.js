const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { parseActor, hasRole } = require('./src/utils/auth');
const { markInvoicePaid } = require('./src/payments/fulfill');

function stripeClient(){
  const key = process.env.STRIPE_SECRET_KEY;
  if (!key) throw new Error('STRIPE_SECRET_KEY missing');
  // dynamic import
  const Stripe = require('stripe');
  return new Stripe(key, { apiVersion: '2024-06-20' });
}

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '5mb' }));

function ensureBuyer(req, res, next) {
  const actor = parseActor(req);
  if (!(hasRole(actor, 'buyer','admin'))) return res.status(403).json({ error: 'buyer role required' });
  res.locals.__actor = actor; next();
}

app.post('/api/payments/stripe/session', ensureBuyer, async (req, res) => {
  try {
    const invoice_id = (req.body?.invoice_id || '').toString();
    if (!invoice_id) return res.status(400).json({ error: 'invoice_id required' });
    const inv = await db.query(`SELECT i.*, o.buyer_id, o.tenant_id, o.currency FROM public.invoices i JOIN public.orders o ON o.id=i.order_id WHERE i.id=$1`, [invoice_id]);
    if (!inv.rows.length) return res.status(404).json({ error: 'invoice not found' });
    const row = inv.rows[0];
    const actor = res.locals.__actor || {};
    if (row.buyer_id !== (actor.id||'') && !hasRole(actor,'admin')) return res.status(403).json({ error: 'forbidden' });

    const items = await db.query(`SELECT brand, code, qty, unit_price_cents FROM public.order_items WHERE order_id=$1`, [row.order_id]);
    const stripe = stripeClient();
    const success_url = (req.body?.success_url || process.env.PAY_SUCCESS_URL || 'https://example.com/pay/success') + `?invoice=${invoice_id}`;
    const cancel_url = (req.body?.cancel_url || process.env.PAY_CANCEL_URL || 'https://example.com/pay/cancel') + `?invoice=${invoice_id}`;

    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      metadata: { invoice_id, order_id: row.order_id, tenant_id: row.tenant_id||'' },
      line_items: items.rows.map(it => ({
        quantity: it.qty,
        price_data: {
          currency: (row.currency || 'usd').toLowerCase(),
          unit_amount: Number(it.unit_price_cents || 0),
          product_data: { name: `${it.brand} ${it.code}` }
        }
      })),
      success_url, cancel_url
    });

    const pay = await db.query(`
      INSERT INTO public.payments (invoice_id, tenant_id, provider, provider_session_id, status, amount_cents, currency, raw)
      VALUES ($1,$2,'stripe',$3,'requires_action',$4,$5,$6) RETURNING *`,
      [invoice_id, row.tenant_id||null, session.id, row.amount_cents, row.currency, { session: session.id }]);
    res.json({ ok: true, session_id: session.id, url: session.url, payment: pay.rows[0] });
  } catch (e) {
    console.error(e); res.status(400).json({ error: String(e.message || e) });
  }
});

// Stripe requires raw body for signature verification
app.post('/api/payments/stripe/webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  try {
    const whSecret = process.env.STRIPE_WEBHOOK_SECRET;
    const stripe = stripeClient();
    let event = req.body;
    if (whSecret) {
      const sig = req.headers['stripe-signature'];
      event = stripe.webhooks.constructEvent(req.body, sig, whSecret);
    }
    const type = event.type;
    if (type === 'checkout.session.completed') {
      const session = event.data.object;
      const invoice_id = session.metadata?.invoice_id;
      if (invoice_id) {
        await markInvoicePaid(invoice_id);
        await db.query(`UPDATE public.payments SET provider_payment_intent_id=$2, status='captured', events=COALESCE(events,'[]'::jsonb)||$3::jsonb WHERE provider_session_id=$1`,
                       [session.id, session.payment_intent || null, { e: type, at: new Date().toISOString() }]);
      }
    } else if (type === 'payment_intent.succeeded') {
      // handled above
    }
    res.json({ received: true });
  } catch (e) {
    console.error('stripe webhook error', e);
    res.status(400).send(`Webhook Error: ${e.message || e}`);
  }
});

module.exports = app;
