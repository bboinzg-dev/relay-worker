const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { parseActor, hasRole } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

function ensureAdmin(req, res, next){
  const actor = parseActor(req);
  if (!hasRole(actor, 'admin')) return res.status(403).json({ error: 'admin required' });
  res.locals.__actor = actor; next();
}

app.get('/api/admin/dashboard', ensureAdmin, async (req, res) => {
  try {
    const tenant = (req.query.tenant || '').toString() || null;
    const whereTenant = tenant ? `= ${db.esc(tenant)}` : 'IS NOT DISTINCT FROM NULL';
    const q = async (sql, args=[]) => (await db.query(sql, args)).rows[0] || { c:0 };
    const counts = {
      parts: (await q(`SELECT COUNT(*)::int AS c FROM public.relay_specs`)).c,
      listings: (await q(`SELECT COUNT(*)::int AS c FROM public.listings WHERE tenant_id ${whereTenant}`)).c,
      listings_pending: (await q(`SELECT COUNT(*)::int AS c FROM public.listings WHERE status='pending' AND (tenant_id ${whereTenant})`)).c,
      bids: (await q(`SELECT COUNT(*)::int AS c FROM public.bids WHERE tenant_id ${whereTenant}`)).c,
      purchase_requests: (await q(`SELECT COUNT(*)::int AS c FROM public.purchase_requests WHERE tenant_id ${whereTenant}`)).c,
      orders: (await q(`SELECT COUNT(*)::int AS c FROM public.orders WHERE tenant_id ${whereTenant}`)).c,
      invoices_unpaid: (await q(`SELECT COUNT(*)::int AS c FROM public.invoices WHERE status='unpaid' AND (tenant_id ${whereTenant})`)).c,
      quality_open: (await q(`SELECT COUNT(*)::int AS c FROM public.quality_issues WHERE resolved_at IS NULL AND (tenant_id ${whereTenant})`)).c,
      image_index: (await q(`SELECT COUNT(*)::int AS c FROM public.image_index WHERE tenant_id ${whereTenant}`)).c
    };
    const recentAudits = (await db.query(`SELECT id, actor_id, action, table_name, row_pk, created_at FROM public.audit_logs ORDER BY created_at DESC LIMIT 20`)).rows;
    const pendingListings = (await db.query(`SELECT * FROM public.listings WHERE status='pending' AND (tenant_id ${whereTenant}) ORDER BY created_at DESC LIMIT 50`)).rows;
    res.json({ counts, recentAudits, pendingListings });
  } catch (e) { console.error(e); res.status(500).json({ error: String(e.message || e) }); }
});

module.exports = app;
