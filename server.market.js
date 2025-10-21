
// server.market.js
'use strict';

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const XLSX = require('xlsx');
const { getPool } = require('./db');
const { parseActor, hasRole } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

const pool = getPool();
const query = (text, params) => pool.query(text, params);
const upload = multer({ limits: { fileSize: 25 * 1024 * 1024 } });

const EXIM_API = 'https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON';
const FX_TARGETS = new Set(['USD', 'JPY', 'CNY', 'CHF', 'KRW']);

function prevYYYYMM(d = new Date()) {
  const y = d.getFullYear();
  const m = d.getMonth();
  const pm = m === 0 ? 12 : m;
  const py = m === 0 ? y - 1 : y;
  return Number(`${py}${String(pm).padStart(2, '0')}`);
}

function firstDayYYYYMM(yyyymm) {
  const s = String(yyyymm).padStart(6, '0');
  const year = Number(s.slice(0, 4));
  const month = Number(s.slice(4, 6)) - 1;
  return new Date(Date.UTC(year, month, 1));
}

function nextMonth(date) {
  const d = new Date(date.getTime());
  d.setUTCMonth(d.getUTCMonth() + 1);
  return d;
}

function ymd(date) {
  return date.toISOString().slice(0, 10).replace(/-/g, '');
}

async function eximFetchDaily(yyyymmdd, apiKey) {
  const url = `${EXIM_API}?authkey=${encodeURIComponent(apiKey)}&searchdate=${yyyymmdd}&data=AP01`;
  const rsp = await fetch(url, { method: 'GET' });
  if (!rsp.ok) throw new Error(`exim_http_${rsp.status}`);
  const arr = await rsp.json();
  if (!Array.isArray(arr)) throw new Error('exim_bad_payload');
  if (arr.length === 1 && arr[0] && typeof arr[0].result !== 'undefined' && arr[0].result !== 1) {
    throw new Error(`exim_result_${arr[0].result}`);
  }
  const out = {};
  for (const r of arr) {
    let unit = String(r.cur_unit || r.CUR_UNIT || '').trim();
    let v = String(r.deal_bas_r || r.DEAL_BAS_R || '').replace(/,/g, '');
    if (!unit || !v) continue;
    let rate = Number(v);
    if (!Number.isFinite(rate)) continue;
    if (/^JPY/i.test(unit)) {
      if (/\(100\)/.test(unit)) rate = rate / 100;
      unit = 'JPY';
  }
  if (unit === 'CNH') unit = 'CNY';
  if (FX_TARGETS.has(unit)) out[unit] = rate;
  }
  return out;
}

async function ensureMonthlyFX(yyyymm, client, { force = false } = {}) {
  const ym = Number(yyyymm);
  if (!Number.isFinite(ym) || String(ym).length < 6) {
    throw new Error('invalid_yyyymm');
  }

  if (!force) {
    const has = await client.query(
      `SELECT 1 FROM public.fx_rates_monthly WHERE yyyymm=$1 AND currency IN ('USD','JPY','CNY','CHF') LIMIT 1`,
      [ym]
    );
    if (has.rowCount) return;
  }

  const lockKey = 10_000_000_000n + BigInt(ym);
  const lock = await client
    .query('SELECT pg_try_advisory_lock($1::bigint) AS ok', [lockKey])
    .then((r) => r.rows?.[0]?.ok);
  if (!lock) {
    if (!force) return;
    throw new Error('fx_refresh_in_progress');
  }

  try {
    if (!force) {
      const again = await client.query(
        `SELECT 1 FROM public.fx_rates_monthly WHERE yyyymm=$1 AND currency IN ('USD','JPY','CNY','CHF') LIMIT 1`,
        [ym]
      );
      if (again.rowCount) return;
    }

    const apiKey = process.env.KOREAEXIM_API_KEY;
    if (!apiKey) throw new Error('missing_KOREAEXIM_API_KEY');

    const start = firstDayYYYYMM(ym);
    const end = nextMonth(start);

    for (let d = new Date(start.getTime()); d < end; d.setUTCDate(d.getUTCDate() + 1)) {
      const ymdStr = ymd(d);
      const daily = await eximFetchDaily(ymdStr, apiKey).catch((err) => {
        console.warn('[fx] fetch skip', ymdStr, err?.message || err);
        return null;
      });
      if (!daily) continue;
      for (const cur of ['USD', 'JPY', 'CNY', 'CHF']) {
        const rate = daily[cur];
        if (!rate) continue;
        await client.query(
          `INSERT INTO public.fx_rates_daily (provider, currency, rate_date, rate, source)
           VALUES ('koreaexim', $1, $2::date, $3, 'exim_deal_bas')
           ON CONFLICT (currency, rate_date)
           DO UPDATE SET rate = EXCLUDED.rate, source = EXCLUDED.source`,
          [cur, ymdStr, rate]
        );
      }
    }

    for (const cur of ['USD', 'JPY', 'CNY', 'CHF']) {
      const r = await client.query(
        `SELECT AVG(rate) AS avg_rate
           FROM public.fx_rates_daily
          WHERE currency=$1 AND rate_date >= $2::date AND rate_date < $3::date`,
        [cur, ymd(start), ymd(end)]
      );
      const avg = Number(r.rows?.[0]?.avg_rate || 0);
      if (avg > 0) {
        await client.query(
          `INSERT INTO public.fx_rates_monthly (currency, yyyymm, rate, source)
           VALUES ($1, $2, $3, 'exim_deal_bas_avg')
           ON CONFLICT (currency, yyyymm)
           DO UPDATE SET rate = EXCLUDED.rate, source = EXCLUDED.source`,
          [cur, ym, avg]
        );
      }
    }
  } finally {
    await client.query('SELECT pg_advisory_unlock($1::bigint)', [lockKey]).catch(() => {});
  }
}

async function enrichKRW(client, currency, unitPriceCents) {
  const curr = String(currency || 'USD').toUpperCase();
  const cents = Number(unitPriceCents ?? 0);
  if (!Number.isFinite(cents) || cents <= 0) {
    return { krw_cents: null, rate: null, yyyymm: prevYYYYMM(), src: null };
  }
  if (curr === 'KRW') {
    const won = Math.round((cents / 100) / 10) * 10;
    return {
      krw_cents: won * 100,
      rate: 1,
      yyyymm: prevYYYYMM(),
      src: 'exim_deal_bas_avg',
    };
  }
  const ym = prevYYYYMM();
  const rr = await client.query(
    `SELECT rate, source FROM public.fx_rates_monthly WHERE currency=$1 AND yyyymm=$2 LIMIT 1`,
    [curr, ym]
  );
  const rate = Number(rr.rows?.[0]?.rate || 0);
  if (!rate) {
    return { krw_cents: null, rate: null, yyyymm: ym, src: null };
  }
  const won = Math.round(((cents / 100) * rate) / 10) * 10;
  return {
    krw_cents: won * 100,
    rate,
    yyyymm: ym,
    src: rr.rows[0].source || 'exim_deal_bas_avg',
  };
}

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

app.post('/api/fx/refresh', async (req, res) => {
  const ym = Number((req.query.yyyymm || req.body?.yyyymm || prevYYYYMM()).toString());
  if (!Number.isFinite(ym) || String(ym).length < 6) {
    return res.status(400).json({ ok: false, error: 'invalid_yyyymm' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await ensureMonthlyFX(ym, client, { force: true });
    await client.query('COMMIT');
    res.json({ ok: true, yyyymm: ym, provider: 'koreaexim', currencies: ['USD', 'JPY', 'CNY', 'CHF'] });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    const status = e.message === 'missing_KOREAEXIM_API_KEY' ? 400 : 500;
    res.status(status).json({ ok: false, error: String(e.message || e) });
  } finally {
    client.release();
  }
});

app.get('/api/fx/latest', async (req, res) => {
  try {
    const curr = String((req.query.currency || req.query.curr || '')).toUpperCase() || 'KRW';
    const ym = prevYYYYMM();
    const autofill =
      String(req.query.autofill ?? '1') !== '0' && String(process.env.FX_AUTO_FILL ?? '1') !== '0';

    if (curr === 'KRW') {
      return res.json({
        ok: true,
        currency: 'KRW',
        rate: 1,
        yyyymm: ym,
        source: 'fixed_krw',
      });
    }

    const selectLatest = async () =>
      (
        await query(
          `SELECT rate, source FROM public.fx_rates_monthly WHERE currency=$1 AND yyyymm=$2 LIMIT 1`,
          [curr, ym]
        )
      ).rows[0] || null;

    let row = await selectLatest();

    if ((!row || !row.rate) && autofill) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await ensureMonthlyFX(ym, client);
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK');
        console.warn('[fx] autofill failed:', err?.message || err);
      } finally {
        client.release();
      }
      row = await selectLatest();
    }

    res.json({
      ok: true,
      currency: curr,
      rate: row?.rate || null,
      yyyymm: ym,
      source: row?.source || (autofill ? 'exim_deal_bas_avg' : null),
    });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

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
  const client = await pool.connect();
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
    const currency = (b.currency ? String(b.currency) : 'USD').toUpperCase();
    const fx = await enrichKRW(client, currency, unitPriceCents);
    const sql = `INSERT INTO public.listings
      (tenant_id, seller_id, brand, code, qty_available, moq, mpq, unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src, currency, lead_time_days, location, condition, packaging, note, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,COALESCE($13,'USD'),$14,$15,$16,$17,$18,COALESCE($19,'pending'))
      RETURNING id, status, created_at`;
    const r = await client.query(sql, [
      t,
      b.seller_id != null ? String(b.seller_id) : actor.id || null,
      String(b.brand),
      String(b.code),
      toOptionalInteger(b.qty_available, { min: 0 }) ?? 0,
      toOptionalInteger(b.moq, { min: 0 }),
      toOptionalInteger(b.mpq, { min: 0 }),
      unitPriceCents,
      fx.krw_cents,
      fx.rate,
      fx.yyyymm,
      fx.src,
      currency,
      toOptionalInteger(b.lead_time_days, { min: 0 }),
      b.location != null ? String(b.location) : null,
      b.condition != null ? String(b.condition) : null,
      b.packaging != null ? String(b.packaging) : null,
      b.note != null ? String(b.note) : null,
      LISTING_STATUS.has(String(b.status || '').toLowerCase()) ? String(b.status).toLowerCase() : 'pending',
    ]);
    res.status(201).json(r.rows[0]);
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) }); }
  finally { client.release(); }
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
  const client = await pool.connect();
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
    const currency = (body.currency || 'USD').toUpperCase();
    const fx = await enrichKRW(client, currency, unitPriceCents);
    const offerBrand = body.offer_brand ?? body.brand ?? null;
    const offerCode = body.offer_code ?? body.code ?? null;
    const isSubstitute = !!(
      body.offer_is_substitute ?? body.is_alternative ?? body.is_substitute
    );
    const note = body.note ?? body.notes ?? null;

    const sql = `INSERT INTO public.bids
      (tenant_id, purchase_request_id, seller_id, offer_brand, offer_code, offer_is_substitute,
       offer_qty, unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src, currency, lead_time_days, note, quote_valid_until, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,COALESCE($13,'USD'),$14,$15,$16,'offered')
      RETURNING *`;

    const r = await client.query(sql, [
      tenantId,
      prId,
      actor.id || null,
      offerBrand,
      offerCode,
      isSubstitute,
      Number(offerQty || 0),
      unitPriceCents,
      fx.krw_cents,
      fx.rate,
      fx.yyyymm,
      fx.src,
      currency,
      body.lead_time_days || null,
      note,
      body.quote_valid_until || null,
    ]);
    res.json({ ok: true, item: r.rows[0] });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok:false, error:String(e.message || e) });
  } finally {
    client.release();
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

app.post('/api/import/seller-items', upload.single('file'), async (req, res) => {
  try {
    const buf = req.file?.buffer;
    const kind = String(req.body?.kind || 'stock');
    if (!buf) return res.status(400).json({ ok: false, error: 'file_required' });

    const wb = XLSX.read(buf, { type: 'buffer' });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });

    if (!rows.length) {
      return res.json({ ok: true, items: [] });
    }

    const header = (rows[0] || []).map((h) => String(h).trim().toLowerCase());
    const idx = (patterns) =>
      header.findIndex((h) => patterns.some((p) => new RegExp(p, 'i').test(h)));

    const map = {
      brand: idx(['brand', '제조사']),
      code: idx(['part', 'parts?\\s*no', '제품명', '부품번호', 'code']),
      price: idx(['unit', '단가', 'price']),
      currency: idx(['currency', '통화']),
      qty: idx(['qty', '수량', '가용']),
      moq: idx(['moq']),
      lead: idx(['lead', '리드', '납기', 'days']),
      status: idx(['status', '상태']),
      offer_qty: idx(['offer', '견적\\s*수량']),
      is_alt: idx(['substitute', '대체']),
      valid_until: idx(['valid', '유효']),
    };

    const data = [];
    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      const brand = map.brand >= 0 ? row[map.brand] : row[0];
      const code = map.code >= 0 ? row[map.code] : row[1];
      const rawPrice = map.price >= 0 ? row[map.price] : null;
      const price = Number(String(rawPrice || '').replace(/[^\d.]/g, '')) || 0;
      if (!brand || !code || !price) continue;

      const base = {
        brand,
        code,
        unit_price: Number(price.toFixed(2)),
        currency: (map.currency >= 0 ? row[map.currency] : 'KRW').toString().toUpperCase(),
        lead_time_days:
          map.lead >= 0
            ? Number(String(row[map.lead]).replace(/[^\d]/g, '')) || null
            : null,
        status:
          map.status >= 0 ? String(row[map.status] || '').toLowerCase() : undefined,
        moq:
          map.moq >= 0
            ? Number(String(row[map.moq]).replace(/[^\d]/g, '')) || null
            : null,
      };

      if (kind === 'quote') {
        data.push({
          ...base,
          offer_qty:
            map.offer_qty >= 0
              ? Number(String(row[map.offer_qty]).replace(/[^\d]/g, '')) || 0
              : 0,
          offer_is_substitute:
            map.is_alt >= 0
              ? /y|1|true|대체/i.test(String(row[map.is_alt] || ''))
              : false,
          quote_valid_until:
            map.valid_until >= 0 ? String(row[map.valid_until] || '') : null,
        });
      } else {
        data.push({
          ...base,
          qty_available:
            map.qty >= 0
              ? Number(String(row[map.qty]).replace(/[^\d]/g, '')) || 0
              : 0,
        });
      }
    }

    res.json({ ok: true, items: data });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

module.exports = app;
