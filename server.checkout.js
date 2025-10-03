const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./db');
const { parseActor, hasRole } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

async function pickCheapestPlan(items){
  // items: [{brand, code, qty}]
  const assignments = [];
  for (const it of items) {
    const brand = it.brand, code = it.code, qty = Number(it.qty||0);
    if (!brand || !code || qty<=0) continue;
    let remain = qty;
    const q = await db.query(`
      SELECT id, brand, code, price_cents, currency, quantity_available, lead_time_days
      FROM public.listings
      WHERE brand_norm=lower($1) AND code_norm=lower($2) AND quantity_available > 0
      ORDER BY price_cents ASC, lead_time_days NULLS FIRST, created_at DESC
    `, [brand, code]);
    for (const l of q.rows) {
      if (remain <= 0) break;
      const take = Math.min(remain, Number(l.quantity_available||0));
      if (take <= 0) continue;
      assignments.push({
        brand, code,
        qty: take,
        unit_price_cents: Number(l.price_cents||0),
        currency: l.currency || 'USD',
        listing_id: l.id,
        is_alternative: false,
        lead_time_days: l.lead_time_days==null? null : Number(l.lead_time_days)
      });
      remain -= take;
    }
    if (remain > 0) {
      // backorder placeholder (no listing)
      assignments.push({
        brand, code,
        qty: remain,
        unit_price_cents: 0,
        currency: 'USD',
        listing_id: null,
        is_alternative: false,
        lead_time_days: null,
        backorder: true
      });
    }
  }
  // summarize
  let subtotal = 0;
  for (const a of assignments) subtotal += Number(a.unit_price_cents||0) * Number(a.qty||0);
  const tax = Math.round(subtotal * 0.0); // tax stub (0%)
  const shipping = 0;
  const total = subtotal + tax + shipping;
  return { assignments, totals: { subtotal_cents: subtotal, tax_cents: tax, shipping_cents: shipping, total_cents: total } };
}

function ensureBuyer(req, res, next){
  const actor = parseActor(req);
  if (!(hasRole(actor, 'buyer','admin'))) return res.status(403).json({ error: 'buyer role required' });
  res.locals.__actor = actor;
  next();
}

app.post('/api/checkout/preview', ensureBuyer, async (req, res) => {
  try {
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.status(400).json({ error: 'items[] required' });
    const plan = await pickCheapestPlan(items);
    res.json({ ok: true, plan });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

app.post('/api/checkout/create', ensureBuyer, async (req, res) => {
  const client = await db.pool.connect();
  try {
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.status(400).json({ error: 'items[] required' });
    const buyer = res.locals.__actor || {};
    const tenant_id = buyer.tenantId || null;
    const buyer_id = buyer.id || null;
    const plan = req.body?.plan || await pickCheapestPlan(items);
    const totals = plan.totals;
    await client.query('BEGIN');
    // order
    const ord = await client.query(`
      INSERT INTO public.orders (order_no, tenant_id, buyer_id, currency, status, subtotal_cents, tax_cents, shipping_cents, total_cents, notes)
      VALUES ('O'||nextval('seq_order_no')::text, $1,$2,$3,'awaiting_payment',$4,$5,$6,$7,$8)
      RETURNING *;
    `, [tenant_id, buyer_id, 'USD', totals.subtotal_cents, totals.tax_cents, totals.shipping_cents, totals.total_cents, req.body?.notes || null]);
    const order = ord.rows[0];
    for (const a of plan.assignments) {
      await client.query(`
        INSERT INTO public.order_items
          (order_id, brand, code, brand_norm, code_norm, qty, unit_price_cents, currency, listing_id, is_alternative, lead_time_days)
        VALUES ($1,$2,$3,lower($2),lower($3),$4,$5,$6,$7,$8,$9)
      `, [order.id, a.brand, a.code, a.qty, a.unit_price_cents, a.currency || 'USD', a.listing_id, !!a.is_alternative, a.lead_time_days]);
    }
    const inv = await client.query(`
      INSERT INTO public.invoices (order_id, invoice_no, status, currency, amount_cents)
      VALUES ($1, 'I'||nextval('seq_invoice_no')::text, 'unpaid', $2, $3)
      RETURNING *;
    `, [order.id, order.currency || 'USD', order.total_cents]);
    const invoice = inv.rows[0];
    await client.query('COMMIT');
    res.json({ ok: true, order, invoice });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  } finally {
    client.release();
  }
});

module.exports = app;
