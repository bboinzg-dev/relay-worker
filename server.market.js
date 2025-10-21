
// server.market.js
'use strict';

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');
const { getPool } = require('./db');
const { parseActor, hasRole } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

const pool = getPool();
const query = (text, params) => pool.query(text, params);

const mapListingRow = (row = {}) => ({
  id: row.id,
  seller_id: row.seller_id,
  brand: row.brand,
  code: row.code,
  quantity_available: row.qty_available,
  qty_available: row.qty_available,
  unit_price: (row.unit_price_cents ?? 0) / 100,
  unit_price_cents: row.unit_price_cents ?? 0,
  currency: row.currency,
  lead_time_days: row.lead_time_days ?? 0,
  status: row.status,
  note: row.note ?? null,
  location: row.location ?? null,
  condition: row.condition ?? null,
  packaging: row.packaging ?? null,
  moq: row.moq ?? null,
  mpq: row.mpq ?? null,
  created_at: row.created_at,
  updated_at: row.updated_at,
});

function pick(h, k) { return h[k] || h[k.toLowerCase()] || h[k.toUpperCase()] || undefined; }
function getTenant(req) {
  // tenant via header (can be empty for single-tenant)
  const t = pick(req.headers || {}, 'x-actor-tenant') || null;
  return t;
}

function requireAuth(req, res) {
  const hdr = req.headers?.authorization || '';
  const token = hdr.startsWith('Bearer ') ? hdr.slice(7) : null;
  if (!token) {
    res.status(401).json({ error: 'unauthorized' });
    return null;
  }
  const secret = process.env.JWT_SECRET;
  if (!secret) {
    console.error('[listings] missing JWT_SECRET');
    res.status(500).json({ error: 'auth_not_configured' });
    return null;
  }
  try {
    return jwt.verify(token, secret);
  } catch (err) {
    console.warn('[listings] token verify failed', err?.message || err);
    res.status(401).json({ error: 'unauthorized' });
    return null;
  }
}

function toOptionalInteger(value, { min } = {}) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const int = Math.round(n);
  if (Number.isFinite(min) && int < min) return min;
  return int;
}

function toCents(unitPrice) {
  if (unitPrice == null || unitPrice === '') return null;
  const num = Number(unitPrice);
  if (!Number.isFinite(num)) return null;
  return Math.max(0, Math.round(num * 100));
}

const LISTING_STATUS = new Set(['pending', 'active', 'soldout', 'archived', 'inactive']);

/* ---------------- Listings (정찰제/재고) ---------------- */

// GET /api/listings?brand=&code=&status=active
app.get('/api/listings', async (req, res) => {
  try {
    const brand = (req.query.brand || '').toString();
    const code  = (req.query.code  || '').toString();
    const status = (req.query.status || 'active').toString();
    const where = [];
    const args = [];
    if (brand) { args.push(brand); where.push(`brand_norm = lower($${args.length})`); }
    if (code)  { args.push(code);  where.push(`code_norm  = lower($${args.length})`); }
    if (status) { args.push(status); where.push(`status = $${args.length}`); }
    const sql = `SELECT id, seller_id, brand, code, qty_available, unit_price_cents, currency, lead_time_days, moq, mpq, location, condition, packaging, note, status, created_at
                 FROM public.listings
                 ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
                 ORDER BY created_at DESC
                 LIMIT 200`;
    const r = await query(sql, args);
    res.json({ ok: true, items: r.rows });
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) }); }
});

// POST /api/listings  (seller 전용)
app.post('/api/listings', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!requireAuth(req, res)) return;
    if (!hasRole(actor, 'seller', 'admin')) return res.status(403).json({ error: 'seller role required' });
    const t = getTenant(req);
    const b = req.body || {};
    if (!b.brand || !b.code) {
      return res.status(400).json({ error: 'brand, code required' });
    }
    const unitPriceCents =
      toCents(b.unit_price) ?? toOptionalInteger(b.unit_price_cents, { min: 0 }) ?? 0;
    const sql = `INSERT INTO public.listings
      (tenant_id, seller_id, brand, code, qty_available, moq, mpq, unit_price_cents, currency, lead_time_days, location, condition, packaging, note, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9,'USD'),$10,$11,$12,$13,$14,COALESCE($15,'pending'))
      RETURNING id, status, created_at`;
    const r = await query(sql, [
      t,
      b.seller_id != null ? String(b.seller_id) : actor.id || null,
      String(b.brand),
      String(b.code),
      toOptionalInteger(b.qty_available, { min: 0 }) ?? 0,
      toOptionalInteger(b.moq, { min: 0 }),
      toOptionalInteger(b.mpq, { min: 0 }),
      unitPriceCents,
      b.currency ? String(b.currency) : 'USD',
      toOptionalInteger(b.lead_time_days, { min: 0 }),
      b.location != null ? String(b.location) : null,
      b.condition != null ? String(b.condition) : null,
      b.packaging != null ? String(b.packaging) : null,
      b.note != null ? String(b.note) : null,
      LISTING_STATUS.has(String(b.status || '').toLowerCase()) ? String(b.status).toLowerCase() : 'pending',
    ]);
    res.status(201).json(r.rows[0]);
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) }); }
});

// GET /api/listings/:id – 단건 조회
app.get('/api/listings/:id', async (req, res) => {
  try {
    const id = (req.params.id || '').toString();
    if (!id) return res.status(400).json({ ok: false, error: 'id required' });
    const r = await query(
      `SELECT id, tenant_id, seller_id, brand, code, qty_available, moq, mpq, unit_price_cents, currency, lead_time_days, location, condition, packaging, note, status, created_at, updated_at
         FROM public.listings WHERE id = $1`,
      [id]
    );
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, item: mapListingRow(r.rows[0]) });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// PATCH /api/listings/:id – 수량/상태/가격 등 수정
app.patch('/api/listings/:id', async (req, res) => {
  try {
    const id = (req.params.id || '').toString();
    if (!id) return res.status(400).json({ ok: false, error: 'id required' });

    const body = req.body || {};
    const sets = [];
    const params = [];

    const has = (key) => Object.prototype.hasOwnProperty.call(body, key);
    const pickFirst = (...keys) => {
      for (const key of keys) {
        if (has(key)) return body[key];
      }
      return undefined;
    };

    if (has('moq')) {
      sets.push(`moq = $${params.length + 1}`);
      params.push(toOptionalInteger(body.moq, { min: 0 }));
    }
    if (has('mpq')) {
      sets.push(`mpq = $${params.length + 1}`);
      params.push(toOptionalInteger(body.mpq, { min: 0 }));
    }

    if (has('qty_available') || has('quantity_available')) {
      const quantity = pickFirst('quantity_available', 'qty_available');
      sets.push(`qty_available = $${params.length + 1}`);
      params.push(toOptionalInteger(quantity, { min: 0 }) ?? 0);
    }

    if (has('unit_price') || has('unit_price_cents')) {
      const cents = has('unit_price')
        ? (toCents(body.unit_price) ?? 0)
        : toOptionalInteger(body.unit_price_cents, { min: 0 }) ?? 0;
      sets.push(`unit_price_cents = $${params.length + 1}`);
      params.push(cents);
    }

    if (has('currency')) {
      sets.push(`currency = $${params.length + 1}`);
      params.push(body.currency != null ? String(body.currency) : null);
    }

    if (has('lead_time_days')) {
      sets.push(`lead_time_days = $${params.length + 1}`);
      params.push(toOptionalInteger(body.lead_time_days, { min: 0 }));
    }

    if (has('location')) {
      sets.push(`location = $${params.length + 1}`);
      params.push(body.location != null ? String(body.location) : null);
    }

    if (has('condition')) {
      sets.push(`condition = $${params.length + 1}`);
      params.push(body.condition != null ? String(body.condition) : null);
    }

    if (has('packaging')) {
      sets.push(`packaging = $${params.length + 1}`);
      params.push(body.packaging != null ? String(body.packaging) : null);
    }

    if (has('note')) {
      sets.push(`note = $${params.length + 1}`);
      params.push(body.note != null ? String(body.note) : null);
    }

    if (has('status')) {
      const status = String(body.status || '').toLowerCase();
      if (!LISTING_STATUS.has(status)) {
        return res.status(400).json({ ok: false, error: 'invalid_status' });
      }
      sets.push(`status = $${params.length + 1}`);
      params.push(status);
    }

    if (!sets.length) {
      return res.status(400).json({ ok: false, error: 'no_fields' });
    }

    sets.push('updated_at = now()');
    params.push(id);

    const sql = `UPDATE public.listings SET ${sets.join(', ')} WHERE id = $${params.length}
      RETURNING id, tenant_id, seller_id, brand, code, qty_available, moq, mpq, unit_price_cents, currency, lead_time_days, location, condition, packaging, note, status, created_at, updated_at`;
    const r = await query(sql, params);
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, item: mapListingRow(r.rows[0]) });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// POST /api/listings/:id/purchase  → 주문 생성(간이)
app.post('/api/listings/:id/purchase', async (req, res) => {
  const client = await pool.connect();
  try {
    const actor = parseActor(req);
    if (!actor?.id) return res.status(401).json({ error: 'auth required' });
    const id = (req.params.id || '').toString();
    const qty = Number(req.body?.qty || 0);
    if (!id || qty <= 0) return res.status(400).json({ error: 'id & qty required' });

    await client.query('BEGIN');
    const L = (await client.query(`SELECT * FROM public.listings WHERE id=$1 FOR UPDATE`, [id])).rows[0];
    if (!L) throw new Error('listing not found');
    if (L.status !== 'active') throw new Error('listing not active');
    if (Number(L.qty_available) < qty) throw new Error('insufficient qty');

    const subtotal = qty * Number(L.unit_price_cents);
    const ord = await client.query(`
      INSERT INTO public.orders (order_no, tenant_id, buyer_id, status, currency, subtotal_cents, tax_cents, shipping_cents, total_cents, notes)
      VALUES ('O'||nextval('seq_order_no')::text, $1,$2,'awaiting_payment',$3,$4,0,0,$4,$5)
      RETURNING *`, [L.tenant_id || null, actor.id || null, L.currency || 'USD', subtotal, req.body?.notes || null]);
    const O = ord.rows[0];
    await client.query(`
      INSERT INTO public.order_items (order_id, brand, code, qty, unit_price_cents, currency, listing_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7)`, [O.id, L.brand, L.code, qty, L.unit_price_cents, L.currency || 'USD', L.id]);
    const remain = Number(L.qty_available) - qty;
    await client.query(`UPDATE public.listings SET qty_available=$2, status=CASE WHEN $2=0 THEN 'soldout' ELSE status END, updated_at=now() WHERE id=$1`, [L.id, remain]);
    await client.query('COMMIT');
    res.json({ ok: true, order: O, remaining: remain });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) });
  } finally { client.release(); }
});

/* ---------------- Purchase Requests (경매/RFQ) ---------------- */

app.get('/api/purchase-requests', async (req, res) => {
  try {
    const brand = (req.query.brand || '').toString();
    const code  = (req.query.code  || '').toString();
    const where = [];
    const args = [];
    if (brand) { args.push(brand); where.push(`brand_norm = lower($${args.length})`); }
    if (code)  { args.push(code);  where.push(`code_norm  = lower($${args.length})`); }
    const sql = `SELECT * FROM public.purchase_requests
                 ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
                 ORDER BY created_at DESC LIMIT 200`;
    const r = await query(sql, args);
    res.json({ ok: true, items: r.rows });
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) }); }
});

app.post('/api/purchase-requests', async (req, res) => {
  try {
    const actor = parseActor(req);
    const t = getTenant(req);
    const b = req.body || {};
    const sql = `INSERT INTO public.purchase_requests
      (tenant_id, buyer_id, brand, code, qty_required, need_by_date, target_unit_price_cents, allow_substitutes, notes, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,COALESCE($8,true),$9,'open')
      RETURNING *`;
    const r = await query(sql, [
      t, actor.id || null, b.brand, b.code, Number(b.qty || b.qty_required || 0),
      b.need_by_date || null, b.target_unit_price_cents || null, b.allow_substitutes !== false,
      b.notes || b.note || null
    ]);
    res.json({ ok: true, item: r.rows[0] });
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) }); }
});

// Confirm a bid; reduce outstanding qty in PR
app.post('/api/purchase-requests/:id/confirm', async (req, res) => {
  const client = await pool.connect();
  try {
    const prId = (req.params.id || '').toString();
    const bidId = (req.body?.bid_id || '').toString();
    const confirmQty = Number(req.body?.confirm_qty || 0);
    if (!prId || !bidId || confirmQty <= 0) return res.status(400).json({ error: 'prId, bid_id, confirm_qty required' });

    await client.query('BEGIN');
    const PR = (await client.query(`SELECT * FROM public.purchase_requests WHERE id=$1 FOR UPDATE`, [prId])).rows[0];
    if (!PR) throw new Error('PR not found');
    const BID = (await client.query(`SELECT * FROM public.bids WHERE id=$1 AND purchase_request_id=$2 FOR UPDATE`, [bidId, prId])).rows[0];
    if (!BID) throw new Error('bid not found');
    const remaining = Number(PR.qty_required) - Number(PR.qty_confirmed);
    if (confirmQty > remaining) throw new Error('confirm exceeds remaining');

    await client.query(`UPDATE public.bids SET status='accepted', updated_at=now() WHERE id=$1`, [bidId]);
    const newConfirmed = Number(PR.qty_confirmed) + confirmQty;
    const newStatus = newConfirmed >= Number(PR.qty_required) ? 'fulfilled' : 'partial';
    await client.query(`UPDATE public.purchase_requests SET qty_confirmed=$2, status=$3 WHERE id=$1`, [prId, newConfirmed, newStatus]);
    await client.query('COMMIT');
    res.json({ ok: true, qty_confirmed: newConfirmed, status: newStatus });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) });
  } finally { client.release(); }
});

/* ---------------- Bids ---------------- */

app.get('/api/bids', async (req, res) => {
  try {
    const prId = (req.query.pr || req.query.purchase_request_id || '').toString();
    const args = []; let where = '';
    if (prId) { args.push(prId); where = 'WHERE purchase_request_id = $1'; }
    const r = await query(`SELECT * FROM public.bids ${where} ORDER BY created_at DESC LIMIT 200`, args);
    res.json({ ok: true, items: r.rows });
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) }); }
});

app.post('/api/bids', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!hasRole(actor, 'seller', 'admin')) return res.status(403).json({ error: 'seller role required' });

    const tenantId = getTenant(req);
    const body = req.body || {};

    const prId = body.pr_id || body.purchase_request_id || body.pr || null;
    const offerQty =
      body.offer_qty ?? body.offer_quantity ?? body.qty_offer ?? null;
    const unitPriceCents = Number.isFinite(Number(body.unit_price_cents))
      ? Number(body.unit_price_cents)
      : (Number.isFinite(Number(body.unit_price))
          ? Math.max(0, Math.round(Number(body.unit_price) * 100))
          : 0);
    const offerBrand = body.offer_brand ?? body.brand ?? null;
    const offerCode = body.offer_code ?? body.code ?? null;
    const isSubstitute = !!(
      body.offer_is_substitute ?? body.is_alternative ?? body.is_substitute
    );
    const note = body.note ?? body.notes ?? null;

    const sql = `INSERT INTO public.bids
      (tenant_id, purchase_request_id, seller_id, offer_brand, offer_code, offer_is_substitute,
       offer_qty, unit_price_cents, currency, lead_time_days, note, quote_valid_until, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9,'USD'),$10,$11,$12,'offered')
      RETURNING *`;

    const r = await query(sql, [
      tenantId,
      prId,
      actor.id || null,
      offerBrand,
      offerCode,
      isSubstitute,
      Number(offerQty || 0),
      unitPriceCents,
      body.currency || 'USD',
      body.lead_time_days || null,
      note,
      body.quote_valid_until || null,
    ]);
    res.json({ ok: true, item: r.rows[0] });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

app.get('/api/seller/items', async (req, res) => {
  try {
    const actor = parseActor(req);
    const sellerId = (req.query.seller_id || actor?.id || '').toString();
    const status = (req.query.status || '').toString();
    const requestedLimit = Number(req.query.limit || 50);
    const limit = Math.min(
      200,
      Number.isFinite(requestedLimit) && requestedLimit > 0 ? requestedLimit : 50
    );

    const args = [];
    const where = [];
    if (sellerId) {
      args.push(sellerId);
      where.push(`seller_id = $${args.length}`);
    }
    if (status) {
      args.push(status);
      where.push(`status = $${args.length}`);
    }
    args.push(limit);

    const sql = `SELECT * FROM public.seller_items_v
                 ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
                 ORDER BY created_at DESC
                 LIMIT $${args.length}`;

    const r = await query(sql, args);
    const items = r.rows.map((row) => ({
      id: row.id,
      item_type: row.item_type,
      brand: row.brand,
      code: row.code,
      quantity_available: row.quantity_available ?? 0,
      unit_price: (row.unit_price_cents ?? 0) / 100,
      currency: row.currency || 'USD',
      lead_time_days: row.lead_time_days,
      status: row.status,
      created_at: row.created_at,
      updated_at: row.updated_at,
    }));

    res.json({ ok: true, items });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

module.exports = app;
