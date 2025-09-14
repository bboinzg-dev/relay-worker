const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { parseActor, hasRole } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '5mb' }));

function ensureBuyerOrAdmin(req, res, next){
  const actor = parseActor(req);
  if (!(hasRole(actor, 'buyer','admin'))) return res.status(403).json({ error: 'buyer role required' });
  res.locals.__actor = actor; next();
}

app.post('/api/payments/session', ensureBuyerOrAdmin, async (req, res) => {
  try {
    const invoice_id = (req.body?.invoice_id || '').toString();
    const provider = (req.body?.provider || 'fakepg').toString();
    if (!invoice_id) return res.status(400).json({ error: 'invoice_id required' });
    const inv = await db.query(`SELECT i.*, o.buyer_id, o.tenant_id FROM public.invoices i JOIN public.orders o ON o.id=i.order_id WHERE i.id=$1`, [invoice_id]);
    if (!inv.rows.length) return res.status(404).json({ error: 'invoice not found' });
    const row = inv.rows[0];
    const actor = res.locals.__actor || {};
    if (row.buyer_id !== (actor.id||'') && !hasRole(actor,'admin')) return res.status(403).json({ error: 'forbidden' });

    const { v4: uuidv4 } = require('uuid');
    const sessionId = uuidv4();
    const pay = await db.query(`
      INSERT INTO public.payments (invoice_id, tenant_id, provider, provider_session_id, status, amount_cents, currency, raw)
      VALUES ($1,$2,$3,$4,'requires_action',$5,$6,$7)
      RETURNING *;
    `, [invoice_id, row.tenant_id || null, provider, sessionId, row.amount_cents, row.currency, { action: 'redirect', fake_url: `/pay/fake?sid=${sessionId}` }]);
    const payment = pay.rows[0];
    res.json({ ok: true, provider, session_id: sessionId, redirect_url: `/pay/fake?sid=${sessionId}`, payment });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

app.post('/api/payments/fake/capture', async (req, res) => {
  try {
    const sid = (req.body?.session_id || '').toString();
    if (!sid) return res.status(400).json({ error: 'session_id required' });
    const pay = await db.query(`SELECT * FROM public.payments WHERE provider='fakepg' AND provider_session_id=$1 LIMIT 1`, [sid]);
    if (!pay.rows.length) return res.status(404).json({ error: 'payment not found' });
    const p = pay.rows[0];
    await db.query(`UPDATE public.payments SET status='captured', raw = COALESCE(raw,'{}'::jsonb) || '{"captured":true}'::jsonb WHERE id=$1`, [p.id]);
    const inv = await db.query(`UPDATE public.invoices SET status='paid', paid_at=now() WHERE id=$1 RETURNING *`, [p.invoice_id]);
    await db.query(`UPDATE public.orders SET status='paid' WHERE id=$1`, [inv.rows[0].order_id]);
    res.json({ ok: true });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

module.exports = app;
