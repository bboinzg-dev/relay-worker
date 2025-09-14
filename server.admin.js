const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { parseActor, hasRole } = require('./src/utils/auth');
const { writeAudit } = require('./src/utils/audit');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

function ensureAdmin(req, res, next){
  const actor = parseActor(req);
  if (!hasRole(actor, 'admin')) return res.status(403).json({ error: 'admin required' });
  res.locals.__actor = actor;
  next();
}

app.get('/api/admin/dashboard', ensureAdmin, async (req, res) => {
  try {
    const q = async (sql, args=[]) => (await db.query(sql, args)).rows[0] || { c:0 };
    const counts = {
      parts: (await q(`SELECT COUNT(*)::int AS c FROM public.relay_specs`)).c,
      listings: (await q(`SELECT COUNT(*)::int AS c FROM public.listings`)).c,
      listings_pending: (await q(`SELECT COUNT(*)::int AS c FROM public.listings WHERE status='pending'`)).c,
      bids: (await q(`SELECT COUNT(*)::int AS c FROM public.bids`)).c,
      purchase_requests: (await q(`SELECT COUNT(*)::int AS c FROM public.purchase_requests`)).c,
      orders: (await q(`SELECT COUNT(*)::int AS c FROM public.orders`)).c,
      invoices_unpaid: (await q(`SELECT COUNT(*)::int AS c FROM public.invoices WHERE status='unpaid'`)).c,
      quality_open: (await q(`SELECT COUNT(*)::int AS c FROM public.quality_issues WHERE resolved_at IS NULL`)).c,
      image_index: (await q(`SELECT COUNT(*)::int AS c FROM public.image_index`)).c
    };
    const recentAudits = (await db.query(`SELECT id, actor_id, action, table_name, row_pk, created_at FROM public.audit_logs ORDER BY created_at DESC LIMIT 20`)).rows;
    const pendingListings = (await db.query(`SELECT * FROM public.listings WHERE status='pending' ORDER BY created_at DESC LIMIT 50`)).rows;
    res.json({ counts, recentAudits, pendingListings });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Moderation: listings
app.get('/api/admin/listings', ensureAdmin, async (req, res) => {
  try {
    const status = (req.query.status || 'all').toString();
    const wh = [];
    if (status !== 'all') wh.push(`status=${db.esc(status)}`);
    const sql = `SELECT * FROM public.listings ${wh.length?'WHERE '+wh.join(' AND '):''} ORDER BY created_at DESC LIMIT 500`;
    const r = await db.query(sql);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

app.post('/api/admin/listings/:id/approve', ensureAdmin, async (req, res) => {
  try {
    const id = req.params.id;
    const before = (await db.query(`SELECT * FROM public.listings WHERE id=$1`, [id])).rows[0] || null;
    const r = await db.query(`UPDATE public.listings SET status='approved', moderated_by=$2, moderated_at=now(), blocked_reason=NULL WHERE id=$1 RETURNING *`, [id, res.locals.__actor.id || null]);
    await writeAudit(db, req, { action: 'approve.listing', table: 'listings', row_pk: id, before, after: r.rows[0] });
    res.json({ ok: true, item: r.rows[0] });
  } catch (e) { res.status(400).json({ error: String(e.message || e) }); }
});

app.post('/api/admin/listings/:id/block', ensureAdmin, async (req, res) => {
  try {
    const id = req.params.id;
    const reason = (req.body?.reason || '').toString() || null;
    const before = (await db.query(`SELECT * FROM public.listings WHERE id=$1`, [id])).rows[0] || null;
    const r = await db.query(`UPDATE public.listings SET status='blocked', blocked_reason=$2, moderated_by=$3, moderated_at=now() WHERE id=$1 RETURNING *`, [id, reason, res.locals.__actor.id || null]);
    await writeAudit(db, req, { action: 'block.listing', table: 'listings', row_pk: id, before, after: r.rows[0] });
    res.json({ ok: true, item: r.rows[0] });
  } catch (e) { res.status(400).json({ error: String(e.message || e) }); }
});

// Moderation: bids
app.get('/api/admin/bids', ensureAdmin, async (req, res) => {
  try {
    const status = (req.query.status || 'all').toString();
    const wh = [];
    if (status !== 'all') wh.push(`status=${db.esc(status)}`);
    const sql = `SELECT * FROM public.bids ${wh.length?'WHERE '+wh.join(' AND '):''} ORDER BY created_at DESC LIMIT 500`;
    const r = await db.query(sql);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

app.post('/api/admin/bids/:id/block', ensureAdmin, async (req, res) => {
  try {
    const id = req.params.id;
    const reason = (req.body?.reason || '').toString() || null;
    const before = (await db.query(`SELECT * FROM public.bids WHERE id=$1`, [id])).rows[0] || null;
    const r = await db.query(`UPDATE public.bids SET status='blocked', blocked_reason=$2, moderated_by=$3, moderated_at=now() WHERE id=$1 RETURNING *`, [id, reason, res.locals.__actor.id || null]);
    await writeAudit(db, req, { action: 'block.bid', table: 'bids', row_pk: id, before, after: r.rows[0] });
    res.json({ ok: true, item: r.rows[0] });
  } catch (e) { res.status(400).json({ error: String(e.message || e) }); }
});

// Audit feed
app.get('/api/admin/audit', ensureAdmin, async (req, res) => {
  try {
    const table = (req.query.table || '').toString();
    const actor = (req.query.actor || '').toString();
    const q = (req.query.q || '').toString();
    const wh = [];
    const args = [];
    if (table) { args.push(table); wh.push(`table_name=$${args.length}`); }
    if (actor) { args.push(actor); wh.push(`actor_id=$${args.length}`); }
    if (q) { args.push('%'+q.toLowerCase()+'%'); wh.push(`(lower(action) LIKE $${args.length} OR lower(row_pk) LIKE $${args.length})`); }
    const sql = `SELECT * FROM public.audit_logs ${wh.length?'WHERE '+wh.join(' AND '):''} ORDER BY created_at DESC LIMIT 500`;
    const r = await db.query(sql, args);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

module.exports = app;
