const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { parseActor, hasRole } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

function ensureBuyerOrAdmin(req, res, next) {
  const actor = parseActor(req);
  if (!(hasRole(actor, 'buyer', 'admin'))) return res.status(403).json({ error: 'buyer role required' });
  res.locals.__actor = actor;
  next();
}

app.get('/api/orders', ensureBuyerOrAdmin, async (req, res) => {
  try {
    const actor = res.locals.__actor || {};
    const scopeAll = req.query.scope === 'all' && hasRole(actor, 'admin');
    const wh = [];
    const args = [];
    if (!scopeAll) {
      args.push(actor.id || '');
      wh.push(`o.buyer_id = $${args.length}`);
    }
    const sql = `
      SELECT o.*, 
             (SELECT COUNT(*) FROM public.order_items oi WHERE oi.order_id=o.id) AS items_count,
             (SELECT status FROM public.invoices i WHERE i.order_id=o.id ORDER BY created_at DESC LIMIT 1) AS last_invoice_status
      FROM public.orders o
      ${wh.length? 'WHERE '+wh.join(' AND ') : ''}
      ORDER BY o.created_at DESC
      LIMIT 200
    `;
    const r = await db.query(sql, args);
    res.json({ items: r.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.get('/api/orders/:id', ensureBuyerOrAdmin, async (req, res) => {
  try {
    const actor = res.locals.__actor || {};
    const id = req.params.id;
    const ord = await db.query(`SELECT * FROM public.orders WHERE id=$1`, [id]);
    if (!ord.rows.length) return res.status(404).json({ error: 'not found' });
    const o = ord.rows[0];
    if (o.buyer_id !== (actor.id||'') && !hasRole(actor, 'admin')) {
      return res.status(403).json({ error: 'forbidden' });
    }
    const items = await db.query(`SELECT * FROM public.order_items WHERE order_id=$1 ORDER BY created_at`, [id]);
    const invoices = await db.query(`SELECT * FROM public.invoices WHERE order_id=$1 ORDER BY created_at`, [id]);
    const payments = invoices.rows.length ? await db.query(`SELECT * FROM public.payments WHERE invoice_id=$1 ORDER BY created_at`, [invoices.rows[0].id]) : { rows: [] };
    res.json({ order: o, items: items.rows, invoices: invoices.rows, payments: payments.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

module.exports = app;
