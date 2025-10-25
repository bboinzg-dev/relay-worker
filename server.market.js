
// server.market.js
'use strict';

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const XLSX = require('xlsx');
const { Storage } = require('@google-cloud/storage');
const { getPool } = require('./db');
const { parseActor } = require('./src/utils/auth');
const { requireSeller } = require('./auth.middleware');
const { fetchFx, toKrwCentsRounded10 } = require('./src/lib/fx');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

const pool = getPool();
const query = (text, params) => pool.query(text, params);
const upload = multer({ limits: { fileSize: 25 * 1024 * 1024 } });
const datasheetUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 },
});

const storage = new Storage();
const PRODUCT_ASSET_BUCKET = (() => {
  const raw = process.env.GCS_PRODUCT_BUCKET
    || process.env.ASSET_BUCKET
    || process.env.GCS_BUCKET
    || '';
  if (!raw) return '';
  const cleaned = String(raw).replace(/^gs:\/\//, '').trim();
  const slash = cleaned.indexOf('/');
  return slash >= 0 ? cleaned.slice(0, slash) : cleaned;
})();
const PRODUCT_ASSET_PREFIX = process.env.GCS_PRODUCT_PREFIX || 'seller-products';

const EXIM_API = 'https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON';
const FX_SET = new Set(['USD', 'JPY', 'CNY', 'CHF', 'KRW']);

function toKST(d = new Date()) {
  return new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Seoul' }));
}

function ymdHyphenKST(d = new Date()) {
  const k = toKST(d);
  const yyyy = k.getFullYear();
  const mm = String(k.getMonth() + 1).padStart(2, '0');
  const dd = String(k.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function ymdNumKST(d = new Date()) {
  return Number(ymdHyphenKST(d).replace(/-/g, ''));
}

function ymdStr(d = new Date()) {
  return ymdHyphenKST(d).replace(/-/g, '');
}

function prevBizday(d = new Date()) {
  const k = toKST(d);
  const dow = k.getDay();
  const back = dow === 1 ? 3 : dow === 0 ? 2 : 1;
  const p = new Date(k);
  p.setDate(k.getDate() - back);
  return p;
}

async function eximFetchDaily(yyyymmdd, apiKey) {
  const url = `${EXIM_API}?authkey=${encodeURIComponent(apiKey)}&searchdate=${yyyymmdd}&data=AP01`;
  const rsp = await fetch(url);
  if (!rsp.ok) throw new Error(`exim_http_${rsp.status}`);
  const arr = await rsp.json();
  if (!Array.isArray(arr)) throw new Error('exim_bad_payload');
  if (arr.length === 1 && typeof arr[0]?.result !== 'undefined' && arr[0].result !== 1) {
    throw new Error(`exim_result_${arr[0].result}`);
  }
  const out = {};
  for (const r of arr) {
    let unit = String(r.cur_unit || r.CUR_UNIT || '').trim();
    let rate = Number(String(r.deal_bas_r || r.DEAL_BAS_R || '').replace(/,/g, ''));
    if (!unit || !Number.isFinite(rate)) continue;
    if (/^JPY/i.test(unit)) {
      if (/\(100\)/.test(unit)) rate /= 100;
      unit = 'JPY';
    }
    if (unit === 'CNH') unit = 'CNY';
    if (FX_SET.has(unit)) out[unit] = rate;
  }
  return out;
}

async function enrichKRWDaily(client, currency, unitPriceCents) {
  const curr = String(currency || 'USD').toUpperCase();
  const cents = Number(unitPriceCents ?? 0);
  const todayHyphen = ymdHyphenKST();
  if (!Number.isFinite(cents) || cents <= 0) {
    return { krw_cents: null, rate: null, yyyymm: Number(todayHyphen.slice(0, 7).replace('-', '')), src: null };
  }
  if (curr === 'KRW') {
    const won = Math.round((cents / 100) / 10) * 10;
    return {
      krw_cents: won * 100,
      rate: 1,
      yyyymm: Number(todayHyphen.slice(0, 7).replace('-', '')),
      src: 'fixed_krw',
    };
  }

  const r = await client.query(
    `select rate, rate_date::text, source from public.fx_rates_daily where currency=$1 order by rate_date desc limit 1`,
    [curr]
  );
  const row = r.rows?.[0] || null;
  const rate = Number(row?.rate || 0);
  const rateDateHyphen = String(row?.rate_date || todayHyphen);
  const yyyymm = Number(rateDateHyphen.slice(0, 7).replace('-', ''));
  if (!rate) {
    return { krw_cents: null, rate: null, yyyymm, src: row?.source || null };
  }
  const won = Math.round(((cents / 100) * rate) / 10) * 10;
  return {
    krw_cents: won * 100,
    rate,
    yyyymm,
    src: row?.source || null,
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
  unit_price_krw: row.unit_price_krw_cents != null ? row.unit_price_krw_cents / 100 : null,
  unit_price_krw_cents: row.unit_price_krw_cents ?? null,
  unit_price_fx_rate: row.unit_price_fx_rate ?? null,
  unit_price_fx_yyyymm: row.unit_price_fx_yyyymm ?? null,
  unit_price_fx_src: row.unit_price_fx_src ?? null,
  currency: row.currency,
  lead_time_days: row.lead_time_days ?? 0,
  status: row.status,
  note: row.note ?? null,
  location: row.location ?? null,
  condition: row.condition ?? null,
  packaging: row.packaging ?? null,
  incoming_schedule1: row.incoming_schedule1 ?? null,
  incoming_qty1: row.incoming_qty1 ?? null,
  incoming_schedule2: row.incoming_schedule2 ?? null,
  incoming_qty2: row.incoming_qty2 ?? null,
  no_parcel: row.no_parcel === true,
  noParcel: row.no_parcel === true,
  image_url: row.image_url ?? null,
  datasheet_url: row.datasheet_url ?? null,
  moq: row.moq ?? null,
  mpq: row.mpq ?? null,
  mpq_required_order: !!row.mpq_required_order,
  part_type: row.part_type ?? null,
  mfg_year: row.mfg_year ?? null,
  is_over_2yrs: row.is_over_2yrs == null ? null : !!row.is_over_2yrs,
  created_at: row.created_at,
  updated_at: row.updated_at,
});

const mapPurchaseRequestRow = (row = {}) => {
  const required = Number(row.required_qty ?? row.qty_required ?? row.quantity_total ?? 0) || 0;
  return {
    id: row.id,
    brand: row.brand,
    code: row.code,
    required_qty: required,
    quantity_total: Number(row.quantity_total ?? required) || 0,
    quantity_outstanding: Number(row.quantity_outstanding ?? required) || 0,
    note: row.note ?? row.notes ?? null,
    need_by_date: row.need_by_date ?? null,
    bid_deadline_at: row.bid_deadline_at ?? null,
    allow_substitutes: row.allow_substitutes == null ? true : !!row.allow_substitutes,
    status: row.status,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
};

// bids 행 → 응답 JSON 정규화
const mapBidRow = (row = {}) => ({
  id: row.id,
  purchase_request_id: row.purchase_request_id ?? null,
  seller_id: row.seller_id ?? null,
  unit_price_cents: row.unit_price_cents ?? 0,
  unit_price: (row.unit_price_cents ?? 0) / 100,
  unit_price_krw_cents: row.unit_price_krw_cents ?? null,
  unit_price_krw: row.unit_price_krw_cents != null ? row.unit_price_krw_cents / 100 : null,
  unit_price_fx_rate: row.unit_price_fx_rate ?? null,
  unit_price_fx_yyyymm: row.unit_price_fx_yyyymm ?? null,
  unit_price_fx_src: row.unit_price_fx_src ?? null,
  currency: row.currency ?? null,
  offer_qty: row.offer_qty ?? null,
  lead_time_days: row.lead_time_days ?? null,
  note: row.note ?? null,
  status: row.status ?? 'offered',
  offer_brand: row.offer_brand ?? null,
  offer_code: row.offer_code ?? null,
  offer_is_substitute: row.offer_is_substitute == null ? null : !!row.offer_is_substitute,
  quote_valid_until: row.quote_valid_until ?? null,
  no_parcel: row.no_parcel === true,
  image_url: row.image_url ?? null,
  datasheet_url: row.datasheet_url ?? null,
  packaging: row.packaging ?? null,
  part_type: row.part_type ?? null,
  mfg_year: row.mfg_year ?? null,
  is_over_2yrs: row.is_over_2yrs == null ? null : !!row.is_over_2yrs,
  has_stock: row.has_stock == null ? null : !!row.has_stock,
  manufactured_month: row.manufactured_month ?? null,
  delivery_date: row.delivery_date ?? null,
  created_at: row.created_at,
  updated_at: row.updated_at,
});

function pick(h, k) { return h[k] || h[k.toLowerCase()] || h[k.toUpperCase()] || undefined; }
function getTenant(req) {
  // tenant via header (can be empty for single-tenant)
  const t = pick(req.headers || {}, 'x-actor-tenant') || null;
  return t;
}

function pickOwn(obj, ...keys) {
  for (const key of keys) {
    if (obj && Object.prototype.hasOwnProperty.call(obj, key) && obj[key] != null) {
      return obj[key];
    }
  }
  return undefined;
}

function safeSlug(value) {
  if (value == null) return '';
  return String(value)
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-zA-Z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .toLowerCase();
}

function resolveProductObjectPath({ brand, code, family, sellerId, kind, extension }) {
  const safeExt = String(extension || '').toLowerCase().replace(/[^a-z0-9]/g, '') || 'bin';
  const parts = [safeSlug(brand), safeSlug(code), safeSlug(family), safeSlug(sellerId)];
  const suffix = kind === 'datasheet' ? 'datasheet' : 'photo';
  parts.push(suffix);
  const baseName = parts.filter(Boolean).join('_') || `asset_${suffix}`;
  const ymd = ymdHyphenKST();
  const prefix = PRODUCT_ASSET_PREFIX.replace(/\/+$/g, '').replace(/^\/+/, '') || 'seller-products';
  return `${prefix}/${ymd}/${baseName}.${safeExt}`;
}

function guessFileExtension(file = {}) {
  const original = typeof file.originalname === 'string' ? file.originalname : '';
  const match = /\.([a-zA-Z0-9]{1,16})$/.exec(original.trim());
  if (match) {
    return match[1].toLowerCase();
  }
  const mime = typeof file.mimetype === 'string' ? file.mimetype.toLowerCase() : '';
  if (mime === 'application/pdf') return 'pdf';
  if (mime.startsWith('image/')) return mime.split('/')[1]?.replace(/[^a-z0-9]/g, '') || 'img';
  if (mime === 'text/plain') return 'txt';
  return '';
}

function parseBooleanish(value) {
  if (typeof value === 'boolean') return value;
  if (value == null) return null;
  const v = String(value).trim().toLowerCase();
  if (['1', 'y', 'yes', 'true', 'on'].includes(v)) return true;
  if (['0', 'n', 'no', 'false', 'off'].includes(v)) return false;
  return null;
}

function toOptionalTrimmed(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s.length ? s : null;
}

function toOptionalInteger(value, { min = -Infinity, max = Infinity } = {}) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const int = Math.trunc(n);
  if (int < min || int > max) return null;
  return int;
}

function toOptionalDateString(value) {
  const s = toOptionalTrimmed(value);
  return s && /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
}

function toCents(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100);
}

function yn(v) {
  return /^y|1|true$/i.test(String(v || '').trim());
}

function U(s) {
  if (s == null) return null;
  const trimmed = String(s).trim();
  return trimmed ? trimmed : null;
}

// 'YYYY-MM' 또는 'YYYY-MM-DD' → 그 달 1일로 보정
function toMonthStartDateString(value) {
  const s = toOptionalTrimmed(value);
  if (!s) return null;
  const m = /^(\d{4})-(\d{2})(?:-(\d{2}))?$/.exec(s);
  if (!m) return toOptionalDateString(value);
  const yyyy = Number(m[1]); const mm = Number(m[2]);
  if (!Number.isInteger(yyyy) || mm < 1 || mm > 12) return null;
  return `${m[1]}-${m[2]}-01`;
}

function currentKSTYear() {
  return toKST().getFullYear();
}

function safeParseActor(req) {
  try {
    return typeof parseActor === 'function' ? parseActor(req) : req.user || null;
  } catch (e) {
    return req.user || null;
  }
}

function safeGetTenant(req) {
  try {
    if (typeof getTenant === 'function') return getTenant(req);
    return req.headers?.['x-tenant-id'] || req.body?.tenant_id || req.query?.tenant_id || null;
  } catch (e) {
    return null;
  }
}

const fxHelper =
  typeof enrichKRWDaily === 'function'
    ? async (client, currency, unitPriceCents) => enrichKRWDaily(client ?? pool, currency, unitPriceCents)
    : async () => ({ krw_cents: null, rate: null, yyyymm: null, src: null });

const LISTING_STATUS = new Set(['pending', 'active', 'soldout', 'archived', 'inactive']);
const BID_STATUS = new Set(['offered', 'accepted', 'rejected', 'withdrawn', 'active']);

/* ---------------- Listings (정찰제/재고) ---------------- */

app.post('/api/uploads/product', requireSeller, upload.single('file'), async (req, res) => {
  try {
    if (!PRODUCT_ASSET_BUCKET) {
      return res.status(500).json({ ok: false, error: 'GCS_BUCKET_NOT_CONFIGURED' });
    }
    const actor = safeParseActor(req) || {};
    const sellerId = actor?.id != null ? String(actor.id) : null;
    if (!sellerId) {
      return res.status(401).json({ ok: false, error: 'auth_required' });
    }
    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ ok: false, error: 'file_required' });
    }

    let meta = {};
    if (typeof req.body?.meta === 'string') {
      try {
        meta = JSON.parse(req.body.meta);
      } catch (_) {
        meta = {};
      }
    } else if (req.body && typeof req.body === 'object') {
      meta = req.body;
    }

    const brand = pickOwn(meta, 'brand', 'offer_brand') || pickOwn(req.body, 'brand');
    const code = pickOwn(meta, 'code', 'offer_code', 'part_number') || pickOwn(req.body, 'code');
    const family = pickOwn(meta, 'family', 'part_type', 'partType');
    const kindRaw = pickOwn(meta, 'kind') || req.body?.kind;
    const kind = String(kindRaw || 'photo').toLowerCase() === 'datasheet' ? 'datasheet' : 'photo';
    const extension = guessFileExtension(req.file);
    const objectPath = resolveProductObjectPath({
      brand,
      code,
      family,
      sellerId,
      kind,
      extension,
    });

    const bucket = storage.bucket(PRODUCT_ASSET_BUCKET);
    const file = bucket.file(objectPath);
    await file.save(req.file.buffer, {
      resumable: false,
      contentType: req.file.mimetype || 'application/octet-stream',
      metadata: { cacheControl: 'public,max-age=31536000' },
    });

    const gcsUri = `gs://${PRODUCT_ASSET_BUCKET}/${objectPath}`;
    const publicUrl = `https://storage.googleapis.com/${PRODUCT_ASSET_BUCKET}/${encodeURI(objectPath)}`;

    return res.json({ ok: true, gcsUri, publicUrl, filename: objectPath.split('/').pop(), objectPath });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ ok: false, error: 'upload_failed' });
  }
});

app.post('/api/fx/fetch-daily', async (req, res) => {
  try {
    const apiKey = process.env.KOREAEXIM_API_KEY;
    if (!apiKey) return res.status(400).json({ ok: false, error: 'missing_KOREAEXIM_API_KEY' });

    const nowK = toKST();
    const ymdToday = ymdNumKST(nowK);
    const target = String(req.query.date || ymdToday);
    const snapshot = await eximFetchDaily(target, apiKey).catch(() => null);

    const client = await pool.connect();
    let responsePayload = null;
    try {
      await client.query('BEGIN');
      if (snapshot && (snapshot.USD || snapshot.JPY || snapshot.CNY || snapshot.CHF)) {
        for (const cur of ['USD', 'JPY', 'CNY', 'CHF']) {
          if (!snapshot[cur]) continue;
          await client.query(
            `INSERT INTO public.fx_rates_daily (provider,currency,rate_date,rate,source,collected_at)
             VALUES ('koreaexim',$1,$2::date,$3,'exim_deal_bas_daily', now())
             ON CONFLICT (currency,rate_date)
             DO UPDATE SET rate=EXCLUDED.rate, source=EXCLUDED.source, collected_at=EXCLUDED.collected_at`,
            [cur, target, snapshot[cur]]
          );
        }
        responsePayload = { ok: true, date: target, source: 'exim_deal_bas_daily', fallback: false };
      } else {
        const prevDate = prevBizday(nowK);
        const prevYmd = ymdStr(prevDate);
        const fallbackRates = await eximFetchDaily(prevYmd, apiKey);
        for (const cur of ['USD', 'JPY', 'CNY', 'CHF']) {
          if (!fallbackRates[cur]) continue;
          await client.query(
            `INSERT INTO public.fx_rates_daily (provider,currency,rate_date,rate,source,collected_at)
             VALUES ('koreaexim',$1,$2::date,$3,'exim_deal_bas_daily(prev_bizday)', now())
             ON CONFLICT (currency,rate_date)
             DO UPDATE SET rate=EXCLUDED.rate, source=EXCLUDED.source, collected_at=EXCLUDED.collected_at`,
            [cur, prevYmd, fallbackRates[cur]]
          );
        }
        responsePayload = {
          ok: true,
          date: prevYmd,
          source: 'exim_deal_bas_daily(prev_bizday)',
          fallback: true,
        };
      }
      await client.query('COMMIT');
      res.json(responsePayload);
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.post('/api/fx/recheck-today', async (req, res) => {
  try {
    const apiKey = process.env.KOREAEXIM_API_KEY;
    if (!apiKey) return res.status(400).json({ ok: false, error: 'missing_KOREAEXIM_API_KEY' });

    const y = String(ymdNumKST());
    const snapshot = await eximFetchDaily(y, apiKey).catch(() => null);
    if (!snapshot) return res.json({ ok: false, error: 'no_update' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const cur of ['USD', 'JPY', 'CNY', 'CHF']) {
        if (!snapshot[cur]) continue;
        await client.query(
          `INSERT INTO public.fx_rates_daily (provider,currency,rate_date,rate,source,collected_at)
           VALUES ('koreaexim',$1,$2::date,$3,'exim_deal_bas_daily', now())
           ON CONFLICT (currency,rate_date)
           DO UPDATE SET rate=EXCLUDED.rate, source=EXCLUDED.source, collected_at=EXCLUDED.collected_at`,
          [cur, y, snapshot[cur]]
        );
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    res.json({ ok: true, date: y, rechecked: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

app.get('/api/fx/today', async (req, res) => {
  try {
    const currency = String((req.query.currency || req.query.curr || '')).toUpperCase() || 'KRW';
    if (!FX_SET.has(currency)) {
      return res.status(400).json({ ok: false, error: 'unsupported_currency' });
    }

    if (currency === 'KRW') {
      const now = new Date();
      const todayHyphen = ymdHyphenKST(now);
      return res.json({
        ok: true,
        currency: 'KRW',
        rate: 1,
        rate_date: todayHyphen,
        collected_at: now.toISOString(),
        source: 'fixed_krw',
        is_prev_bizday: false,
      });
    }

    const todayHyphen = ymdHyphenKST();
    const todayCompact = todayHyphen.replace(/-/g, '');
    const autofill = String(req.query.autofill || '1') !== '0';

    const selectLatest = async () =>
      (
        await query(
          `select rate, rate_date::text, source, collected_at from public.fx_rates_daily where currency=$1 and rate_date=$2::date limit 1`,
          [currency, todayCompact]
        )
      ).rows[0] || null;

    let row = await selectLatest();

    if ((!row || !row.rate) && autofill) {
      const base = process.env.WORKER_URL || '';
      const endpoint = `${base}/api/fx/fetch-daily`;
      await fetch(endpoint, { method: 'POST' }).catch(() => {});
      row = (
        await query(
          `select rate, rate_date::text, source, collected_at from public.fx_rates_daily where currency=$1 order by rate_date desc limit 1`,
          [currency]
        )
      ).rows[0] || null;
    }

    if (!row) {
      return res.json({
        ok: true,
        currency,
        rate: null,
        rate_date: null,
        collected_at: null,
        source: null,
        is_prev_bizday: null,
      });
    }

    const rateDateHyphen = String(row.rate_date || '');
    const isPrev = rateDateHyphen.replace(/-/g, '') !== todayCompact;

    res.json({
      ok: true,
      currency,
      rate: Number(row.rate),
      rate_date: rateDateHyphen,
      collected_at: row.collected_at,
      source: row.source,
      is_prev_bizday: isPrev,
    });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

// GET /api/listings?brand=&code=&status=active
app.get('/api/listings', async (req, res) => {
  try {
    const actor = parseActor(req);
    const brand = (req.query.brand || '').toString();
    const code  = (req.query.code  || '').toString();
    const status = (req.query.status || 'active').toString();
    const mine = String(req.query.mine || '').toLowerCase();
    const isMine = mine === '1' || mine === 'true';
    const sellerId = isMine
      ? (actor?.id != null ? String(actor.id) : '')
      : (req.query.seller_id != null ? String(req.query.seller_id) : null);
    if (isMine && !sellerId) {
      return res.json({ ok: true, items: [] });
    }

    const where = [];
    const args = [];
    if (sellerId) { args.push(sellerId); where.push(`seller_id = $${args.length}`); }
    if (brand) { args.push(brand); where.push(`brand_norm = lower($${args.length})`); }
    if (code)  { args.push(code);  where.push(`code_norm  = lower($${args.length})`); }
    if (status) { args.push(status); where.push(`status = $${args.length}`); }
    const sql = `SELECT id, seller_id, brand, code, qty_available, unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src, currency, lead_time_days, moq, mpq, mpq_required_order, location, condition, packaging, note,
                       incoming_schedule1, incoming_qty1, incoming_schedule2, incoming_qty2,
                       no_parcel, image_url, datasheet_url, status, part_type, mfg_year, is_over_2yrs, created_at, updated_at
                 FROM public.listings
                 ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
                 ORDER BY created_at DESC
                 LIMIT 200`;
    const r = await query(sql, args);
    const items = r.rows.map((row) => ({
      ...row,
      quantity_available: row.qty_available,
      unit_price: (row.unit_price_cents ?? 0) / 100,
      unit_price_krw: row.unit_price_krw_cents != null ? row.unit_price_krw_cents / 100 : null,
      noParcel: row.no_parcel === true,
      incoming_schedule1: row.incoming_schedule1 ?? null,
      incoming_qty1: row.incoming_qty1 ?? null,
      incoming_schedule2: row.incoming_schedule2 ?? null,
      incoming_qty2: row.incoming_qty2 ?? null,
    }));
    res.json({ ok: true, items });
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e.message || e) }); }
});

// POST /api/listings (seller 전용)
app.post('/api/listings', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) {
      return res.status(401).json({ ok: false, error: 'auth' });
    }

    const tenantId = actor?.tenantId ?? actor?.tenant_id ?? null;
    const b = req.body || {};
    const sql = `
      INSERT INTO public.listings
        (tenant_id, seller_id, brand, code,
         qty_available, moq, mpq, mpq_required_order,
         unit_price_cents, currency, lead_time_days,
         location, condition, packaging, note, status,
         no_parcel, image_url, incoming_schedule1, incoming_qty1,
         incoming_schedule2, incoming_qty2, datasheet_url,
         part_type, mfg_year, is_over_2yrs)
      VALUES
        ($1,$2,$3,$4,
         $5,$6,$7,$8,
         $9, upper($10), $11,
         $12,$13,$14,$15, COALESCE($16,'pending'),
         $17,$18,$19,$20,
         $21,$22,$23,
         $24,$25,$26)
      RETURNING id`;
    const vals = [
      tenantId,
      String(actor.id),
      U(b.brand),
      U(b.code),
      Number(b.qty_available || 0),
      U(b.moq),
      U(b.mpq),
      yn(b.mpq_required_order),
      toCents(b.unit_price),
      b.currency,
      U(b.lead_time_days),
      U(b.location),
      U(b.condition),
      U(b.packaging),
      U(b.note),
      U(b.status),
      yn(b.no_parcel),
      U(b.image_url),
      U(b.incoming_schedule1),
      U(b.incoming_qty1),
      U(b.incoming_schedule2),
      U(b.incoming_qty2),
      U(b.datasheet_url),
      U(b.part_type),
      U(b.mfg_year),
      yn(b.is_over_2yrs),
    ];
    const r = await query(sql, vals);
    return res.json({ ok: true, id: r.rows[0]?.id });
  } catch (e) {
    console.error(e);
    const msg = e?.message || String(e);
    if (msg && msg.startsWith('fx_rate_not_found')) {
      return res.status(400).json({ ok: false, error: 'fx_rate_not_found' });
    }
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// GET /api/listings/:id – 단건 조회
app.get('/api/listings/:id', async (req, res) => {
  try {
    const id = (req.params.id || '').toString();
    if (!id) return res.status(400).json({ ok: false, error: 'id required' });
    const actor = parseActor(req);
    const sellerId = actor?.id != null ? String(actor.id) : '';
    if (!sellerId) {
      return res.status(404).json({ ok: false, error: 'not_found' });
    }
    const r = await query(
      `SELECT id, tenant_id, seller_id, brand, code, qty_available, moq, mpq, mpq_required_order, unit_price_cents, currency, lead_time_days,
              location, condition, packaging, note,
              incoming_schedule1, incoming_qty1, incoming_schedule2, incoming_qty2,
              no_parcel, image_url, datasheet_url, status, part_type, mfg_year, is_over_2yrs, created_at, updated_at
         FROM public.listings WHERE id = $1 AND seller_id = $2`,
      [id, sellerId]
    );
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, item: mapListingRow(r.rows[0]) });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: 'db_error' });
  }
});

// PATCH /api/listings/:id – 수량/상태/가격 등 수정
app.patch('/api/listings/:id', requireSeller, async (req, res) => {
  let client;
  let inTransaction = false;
  try {
    const id = (req.params.id || '').toString();
    if (!id) return res.status(400).json({ ok: false, error: 'id required' });

    const actor = parseActor(req) || {};
    const sellerId = actor?.id != null ? String(actor.id) : null;
    if (!sellerId) {
      return res.status(401).json({ ok: false, error: 'auth_required' });
    }

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

    if (has('brand')) {
      const brandValue = body.brand != null ? String(body.brand).trim() : '';
      if (!brandValue) return res.status(400).json({ ok: false, error: 'brand_required' });
      const brandIdx = params.length + 1;
      sets.push(`brand = $${brandIdx}`);
      params.push(brandValue);
    }

    if (has('code')) {
      const codeValue = body.code != null ? String(body.code).trim() : '';
      if (!codeValue) return res.status(400).json({ ok: false, error: 'code_required' });
      const codeIdx = params.length + 1;
      sets.push(`code = $${codeIdx}`);
      params.push(codeValue);
    }

    if (has('moq')) {
      sets.push(`moq = $${params.length + 1}`);
      params.push(toOptionalInteger(body.moq, { min: 0 }));
    }
    if (has('mpq')) {
      sets.push(`mpq = $${params.length + 1}`);
      params.push(toOptionalInteger(body.mpq, { min: 0 }));
    }
    if (has('mpq_required_order')) {
      sets.push(`mpq_required_order = $${params.length + 1}`);
      params.push(!!body.mpq_required_order);
    }

    if (has('qty_available') || has('quantity_available')) {
      const quantity = pickFirst('quantity_available', 'qty_available');
      sets.push(`qty_available = $${params.length + 1}`);
      params.push(toOptionalInteger(quantity, { min: 0 }) ?? 0);
    }

    let shouldRecomputeFx = false;
    if (has('unit_price') || has('unit_price_cents')) {
      const cents = has('unit_price')
        ? (toCents(body.unit_price) ?? 0)
        : toOptionalInteger(body.unit_price_cents, { min: 0 }) ?? 0;
      sets.push(`unit_price_cents = $${params.length + 1}`);
      params.push(cents);
      shouldRecomputeFx = true;
    }

    if (has('currency')) {
      const currencyRaw = body.currency != null ? String(body.currency).trim() : null;
      const normalizedCurrency = currencyRaw ? currencyRaw.toUpperCase() : currencyRaw;
      sets.push(`currency = $${params.length + 1}`);
      params.push(normalizedCurrency);
      shouldRecomputeFx = true;
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

    const incomingSchedule1Raw = pickFirst('incoming_schedule1', 'incomingSchedule1', 'incomingDate1');
    if (incomingSchedule1Raw !== undefined) {
      sets.push(`incoming_schedule1 = $${params.length + 1}`);
      params.push(toOptionalDateString(incomingSchedule1Raw));
    }
    const incomingQty1Raw = pickFirst('incoming_qty1', 'incomingQty1');
    if (incomingQty1Raw !== undefined) {
      sets.push(`incoming_qty1 = $${params.length + 1}`);
      params.push(toOptionalInteger(incomingQty1Raw, { min: 0 }));
    }
    const incomingSchedule2Raw = pickFirst('incoming_schedule2', 'incomingSchedule2', 'incomingDate2');
    if (incomingSchedule2Raw !== undefined) {
      sets.push(`incoming_schedule2 = $${params.length + 1}`);
      params.push(toOptionalDateString(incomingSchedule2Raw));
    }
    const incomingQty2Raw = pickFirst('incoming_qty2', 'incomingQty2');
    if (incomingQty2Raw !== undefined) {
      sets.push(`incoming_qty2 = $${params.length + 1}`);
      params.push(toOptionalInteger(incomingQty2Raw, { min: 0 }));
    }

    const hasNoParcel = has('no_parcel') || Object.prototype.hasOwnProperty.call(body, 'noParcel');
    if (hasNoParcel) {
      const noParcelValue = has('no_parcel') ? body.no_parcel : body.noParcel;
      sets.push(`no_parcel = $${params.length + 1}`);
      params.push(parseBooleanish(noParcelValue) === true);
    }

    if (has('image_url')) {
      const img = body.image_url != null ? String(body.image_url) : null;
      sets.push(`image_url = $${params.length + 1}`);
      params.push(img);
    }

    if (has('datasheet_url')) {
      const datasheet = body.datasheet_url != null ? String(body.datasheet_url) : null;
      sets.push(`datasheet_url = $${params.length + 1}`);
      params.push(datasheet);
    }
    if (has('datasheetUrl')) {
      const datasheet = body.datasheetUrl != null ? String(body.datasheetUrl) : null;
      sets.push(`datasheet_url = $${params.length + 1}`);
      params.push(datasheet);
    }

    if (has('part_type')) {
      sets.push(`part_type = $${params.length + 1}`);
      params.push(toOptionalTrimmed(body.part_type));
    }

    if (has('lead_time_days')) {
      let leadDays = toOptionalInteger(body.lead_time_days, { min: 0 });
      if (leadDays == null) leadDays = 2;
      sets.push(`lead_time_days = $${params.length + 1}`);
      params.push(leadDays);
    }

    if (has('mfg_year') || has('is_over_2yrs')) {
      let hasMfgYear = has('mfg_year');
      let mfgYear = hasMfgYear ? toOptionalInteger(body.mfg_year) : undefined;

      let hasIsOver = has('is_over_2yrs');
      let isOver = hasIsOver ? parseBooleanish(body.is_over_2yrs) : undefined;
      if (isOver === undefined) isOver = null;

      const nowYear = currentKSTYear();
      if (hasIsOver && isOver === true && !hasMfgYear) {
        mfgYear = nowYear - 2;
        hasMfgYear = true;
      }
      if (hasMfgYear && mfgYear != null && !hasIsOver) {
        isOver = nowYear - mfgYear >= 2;
        hasIsOver = true;
      }

      if (hasMfgYear) {
        sets.push(`mfg_year = $${params.length + 1}`);
        params.push(mfgYear ?? null);
      }
      if (hasIsOver) {
        sets.push(`is_over_2yrs = $${params.length + 1}`);
        params.push(isOver === null ? null : !!isOver);
      }
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

    client = await pool.connect();
    await client.query('BEGIN');
    inTransaction = true;

    const idParamIdx = params.length + 1;
    const sellerParamIdx = params.length + 2;
    params.push(id);
    params.push(sellerId);

    const sql = `UPDATE public.listings SET ${sets.join(', ')} WHERE id = $${idParamIdx} AND seller_id = $${sellerParamIdx}
      RETURNING id, tenant_id, seller_id, unit_price_cents, currency`;
    const r = await client.query(sql, params);
    if (!r.rows.length) {
      await client.query('ROLLBACK');
      inTransaction = false;
      return res.status(404).json({ ok: false, error: 'not_found' });
    }

    if (shouldRecomputeFx) {
      const base = r.rows[0] || {};
      const fx = await enrichKRWDaily(client, base.currency, base.unit_price_cents);
      await client.query(
        `UPDATE public.listings
           SET unit_price_krw_cents = $1,
               unit_price_fx_rate = $2,
               unit_price_fx_yyyymm = $3,
               unit_price_fx_src = $4
         WHERE id = $5 AND seller_id = $6`,
        [fx.krw_cents, fx.rate, fx.yyyymm, fx.src, id, sellerId]
      );
    }

    await client.query('COMMIT');
    inTransaction = false;

    const out = await query(
      `SELECT id, seller_id, brand, code, qty_available, moq, mpq, mpq_required_order,
              unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src,
              currency, lead_time_days, location, condition, packaging, note,
              incoming_schedule1, incoming_qty1, incoming_schedule2, incoming_qty2,
              no_parcel, image_url, datasheet_url, status,
              part_type, mfg_year, is_over_2yrs, created_at, updated_at
         FROM public.listings
        WHERE id = $1 AND seller_id = $2`,
      [id, sellerId]
    );
    return res.json({ ok: true, item: mapListingRow(out.rows[0]) });
  } catch (e) {
    if (inTransaction) { try { await client.query('ROLLBACK'); } catch {} }
    if (e?.code === '23505' && e?.constraint === 'ux_listings_seller_brand_code') {
      return res.status(409).json({ ok: false, error: 'duplicate_listing' });
    }
    console.error(e);
    res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    if (client) client.release();
  }
});

// DELETE /api/listings/:id – 단건 삭제(셀러 전용)
app.delete('/api/listings/:id', requireSeller, async (req, res) => {
  const id = (req.params.id || '').toString();
  if (!id) return res.status(400).json({ ok: false, error: 'id required' });
  const actor = parseActor(req) || {};
  if (!actor?.id) {
    return res.status(401).json({ ok: false, error: 'auth_required' });
  }
  const actorId = String(actor.id);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(
      `SELECT seller_id FROM public.listings WHERE id=$1`,
      [id]
    );
    if (!rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'not_found' });
    }
    const owner = String(rows[0].seller_id || '');
    if (owner && actorId && owner !== actorId) {
      await client.query('ROLLBACK');
      return res.status(403).json({ ok: false, error: 'forbidden' });
    }
    await client.query(`DELETE FROM public.listings WHERE id=$1`, [id]);
    await client.query('COMMIT');
    return res.json({ ok: true, id });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error(e);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    client.release();
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
    const status = (req.query.status || 'open').toString();

    const where = [];
    const args = [];
    if (brand) { args.push(brand); where.push(`brand_norm = lower($${args.length})`); }
    if (code)  { args.push(code);  where.push(`code_norm  = lower($${args.length})`); }
    if (status) { args.push(status); where.push(`status = $${args.length}`); }

    const sql = `
      SELECT id, brand, code, qty_required, notes, need_by_date, bid_deadline_at,
             allow_substitutes, status, created_at, updated_at
        FROM public.purchase_requests
       ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
       ORDER BY created_at DESC
       LIMIT 200`;
    const r = await query(sql, args);
    res.json({ ok: true, items: r.rows.map(mapPurchaseRequestRow) });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok:false, error:String(e.message || e) });
  }
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
    const BID = (await client.query(`SELECT * FROM public.plan_bids WHERE id=$1 AND purchase_request_id=$2 FOR UPDATE`, [bidId, prId])).rows[0];
    if (!BID) throw new Error('bid not found');
    const remaining = Number(PR.qty_required) - Number(PR.qty_confirmed);
    if (confirmQty > remaining) throw new Error('confirm exceeds remaining');

    await client.query(`UPDATE public.plan_bids SET status='accepted', updated_at=now() WHERE id=$1`, [bidId]);
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

app.get('/api/bid/open-items', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) return res.status(401).json({ error: 'auth required' });
    const sql = `
      SELECT id AS purchase_request_id, brand, code, qty_required,
             need_by_date, bid_deadline_at, category
        FROM public.vw_open_pr_for_seller
       ORDER BY COALESCE(bid_deadline_at, now() + interval '100 years') ASC,
                COALESCE(need_by_date, now() + interval '100 years') ASC,
                brand ASC, code ASC
    `;
    const r = await query(sql);
    return res.json(r.rows);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: 'db_error' });
  }
});

app.post('/api/uploads/datasheet', datasheetUpload.single('file'), async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) return res.status(401).json({ ok: false, error: 'auth required' });
    const file = req.file;
    if (!file) return res.status(400).json({ ok: false, error: 'file required' });

    const inserted = await query(`
      INSERT INTO public.file_blobs (content_type, filename, byte_len, data)
      VALUES ($1,$2,$3,$4)
      RETURNING id
    `, [file.mimetype || 'application/octet-stream', file.originalname || 'file', file.buffer?.length || 0, file.buffer]);

    const blobId = inserted.rows[0]?.id;
    return res.json({ ok: true, blob_id: blobId, url: blobId ? `/api/files/${blobId}` : null });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/files/:id', async (req, res) => {
  try {
    const id = (req.params.id || '').toString();
    if (!id) return res.status(400).json({ error: 'id required' });
    const r = await query(`
      SELECT content_type, filename, data
        FROM public.file_blobs
       WHERE id = $1
       LIMIT 1
    `, [id]);
    if (!r.rows.length) return res.status(404).json({ error: 'not_found' });
    const row = r.rows[0];
    const contentType = row.content_type || 'application/octet-stream';
    const filename = row.filename || 'file';
    res.setHeader('Content-Type', contentType);
    res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
    return res.send(row.data);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: 'db_error' });
  }
});

app.get('/api/fx/latest/:currency', async (req, res) => {
  try {
    const currency = (req.params.currency || 'KRW').toString().toUpperCase();
    const fx = await fetchFx(pool, currency);
    return res.json({
      ok: true,
      currency,
      rate: Number(fx.rate || 0),
      yyyymm: fx.yyyymm || null,
      source: fx.source || null,
    });
  } catch (e) {
    console.error(e);
    return res.status(404).json({ ok: false, error: 'fx_rate_not_found' });
  }
});

app.post('/api/bid/submit', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) return res.status(401).json({ ok: false, error: 'auth required' });
    const body = req.body || {};
    const purchaseRequestId = (body.purchase_request_id || body.pr_id || '').toString().trim();
    const unitPriceRaw = body.unit_price ?? body.unitPrice;
    const unitPrice = Number(unitPriceRaw);
    if (!purchaseRequestId) return res.status(400).json({ ok: false, error: 'purchase_request_id required' });
    if (!Number.isFinite(unitPrice) || unitPrice <= 0) {
      return res.status(400).json({ ok: false, error: 'unit_price must be > 0' });
    }

    const currency = (body.currency || 'KRW').toString().toUpperCase();
    const fx = await fetchFx(pool, currency);
    const unitPriceCents = Math.max(0, Math.round(unitPrice * 100));
    const unitPriceKrwCents = toKrwCentsRounded10(unitPrice, currency, fx.rate);
    const offerQty = toOptionalInteger(body.offer_qty ?? body.offerQty, { min: 0 });
    const leadTimeDays = toOptionalInteger(body.lead_time_days ?? body.leadTimeDays, { min: 0 });
    const note = toOptionalTrimmed(body.note);
    const packaging = toOptionalTrimmed(body.packaging);
    const partType = toOptionalTrimmed(body.part_type ?? body.partType);
    const quoteValid = body.quote_valid_until || body.quoteValidUntil || null;
    const datasheetUrl = toOptionalTrimmed(body.datasheet_url ?? body.datasheetUrl);

    const sql = `
      INSERT INTO public.plan_bids
        (purchase_request_id, seller_id,
         unit_price_cents, currency,
         unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src,
         offer_qty, lead_time_days, note, packaging, part_type,
         quote_valid_until, datasheet_url, status)
      VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'offered')
      RETURNING id
    `;

    const result = await query(sql, [
      purchaseRequestId,
      String(actor.id),
      unitPriceCents,
      currency,
      unitPriceKrwCents,
      fx.rate || null,
      fx.yyyymm || null,
      fx.source || null,
      offerQty,
      leadTimeDays,
      note,
      packaging,
      partType,
      quoteValid || null,
      datasheetUrl || null,
    ]);

    return res.json({ ok: true, id: result.rows[0]?.id || null });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

/* ---------------- Bids ---------------- */

// GET /api/bids?mine=1&pr_id=...&status=active&limit=50
app.get('/api/bids', async (req, res) => {
  try {
    const actor = safeParseActor(req);
    const mine = String(req.query.mine || '').toLowerCase();
    const isMine = mine === '1' || mine === 'true';
    const sellerId = isMine
      ? (actor?.id != null ? String(actor.id) : '')
      : (req.query.seller_id != null ? String(req.query.seller_id) : null);
    if (isMine && !sellerId) return res.json({ ok: true, items: [] });

    const prId = (req.query.pr_id || req.query.purchase_request_id || '').toString().trim();
    const status = (req.query.status || '').toString().trim();
    const limit = Math.min(Math.max(parseInt(String(req.query.limit || '50'), 10) || 50, 1), 200);

    const where = [];
    const args = [];
    if (sellerId) { args.push(sellerId); where.push(`seller_id = $${args.length}`); }
    if (prId)     { args.push(prId);     where.push(`purchase_request_id = $${args.length}`); }
    if (status)   { args.push(status);   where.push(`status = $${args.length}`); }

    const sql = `
      SELECT id, purchase_request_id, seller_id,
             unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src,
             currency, offer_qty, lead_time_days, note, status,
             offer_brand, offer_code, offer_is_substitute, quote_valid_until, no_parcel, image_url, datasheet_url,
             packaging, part_type, mfg_year, is_over_2yrs, has_stock, manufactured_month, delivery_date,
             created_at, updated_at
        FROM public.plan_bids
        ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
        ORDER BY created_at DESC
        LIMIT ${limit}`;
    const r = await query(sql, args);
    return res.json({ ok: true, items: r.rows.map(mapBidRow) });
  } catch (e) {
    console.error(e);
    return res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

// GET /api/bids/:id  (seller 전용)
app.get('/api/bids/:id', requireSeller, async (req, res) => {
  try {
    const actor = safeParseActor(req) || {};
    const sellerId = actor?.id != null ? String(actor.id) : null;
    if (!sellerId) return res.status(401).json({ ok: false, error: 'auth_required' });
    const id = String(req.params.id);
    const sql = `
      SELECT id, purchase_request_id, seller_id,
             unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src,
             currency, offer_qty, lead_time_days, note, status,
             offer_brand, offer_code, offer_is_substitute, quote_valid_until, no_parcel, image_url, datasheet_url,
             packaging, part_type, mfg_year, is_over_2yrs, has_stock, manufactured_month, delivery_date,
             created_at, updated_at
        FROM public.plan_bids
       WHERE id = $1 AND seller_id = $2
       LIMIT 1`;
    const r = await query(sql, [id, sellerId]);
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'not_found' });
    return res.json({ ok: true, item: mapBidRow(r.rows[0]) });
  } catch (e) {
    console.error(e);
    return res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

// PATCH /api/bids/:id  (seller 전용, 부분 수정)
app.patch('/api/bids/:id', requireSeller, async (req, res) => {
  const actor = safeParseActor(req) || {};
  const sellerId = actor?.id != null ? String(actor.id) : null;
  if (!sellerId) return res.status(401).json({ ok: false, error: 'auth_required' });
  const id = String(req.params.id);
  const b = req.body || {};
  const client = await pool.connect();
  try {
    const cur = await client.query(
      `SELECT * FROM public.plan_bids WHERE id = $1 AND seller_id = $2 LIMIT 1`,
      [id, sellerId]
    );
    if (!cur.rows.length) return res.status(404).json({ ok: false, error: 'not_found' });
    const row = cur.rows[0];

    const changes = {};
    if (Object.prototype.hasOwnProperty.call(b, 'offer_qty') || Object.prototype.hasOwnProperty.call(b, 'offer_quantity')) {
      changes.offer_qty = toOptionalInteger(pickOwn(b, 'offer_qty', 'offer_quantity'), { min: 0 });
    }

    let needFx = false;
    let unitPriceCents = null;
    if (Object.prototype.hasOwnProperty.call(b, 'unit_price') || Object.prototype.hasOwnProperty.call(b, 'unit_price_cents')) {
      unitPriceCents = toCents(b.unit_price) ?? toOptionalInteger(b.unit_price_cents, { min: 0 });
      if (unitPriceCents != null) {
        changes.unit_price_cents = unitPriceCents;
        needFx = true;
      }
    }
    if (Object.prototype.hasOwnProperty.call(b, 'currency')) {
      const curCurrency = String(b.currency || '').toUpperCase();
      if (curCurrency) {
        changes.currency = curCurrency;
        needFx = true;
      }
    }
    if (needFx) {
      const currency = changes.currency || row.currency || 'USD';
      const cents = (changes.unit_price_cents != null ? changes.unit_price_cents : row.unit_price_cents) || 0;
      const fx = await fxHelper(client, currency, cents);
      changes.unit_price_krw_cents = fx.krw_cents;
      changes.unit_price_fx_rate = fx.rate;
      changes.unit_price_fx_yyyymm = fx.yyyymm;
      changes.unit_price_fx_src = fx.src;
    }

    if (Object.prototype.hasOwnProperty.call(b, 'lead_time_days')) {
      changes.lead_time_days = toOptionalInteger(b.lead_time_days, { min: 0 });
    }
    if (Object.prototype.hasOwnProperty.call(b, 'note')) {
      changes.note = toOptionalTrimmed(b.note);
    }
    if (Object.prototype.hasOwnProperty.call(b, 'status')) {
      const s = toOptionalTrimmed(b.status);
      if (s && BID_STATUS.has(s.toLowerCase())) changes.status = s.toLowerCase();
    }
    if (Object.prototype.hasOwnProperty.call(b, 'offer_brand') || Object.prototype.hasOwnProperty.call(b, 'brand')) {
      changes.offer_brand = toOptionalTrimmed(pickOwn(b, 'offer_brand', 'brand'));
    }
    if (Object.prototype.hasOwnProperty.call(b, 'offer_code') || Object.prototype.hasOwnProperty.call(b, 'code')) {
      changes.offer_code = toOptionalTrimmed(pickOwn(b, 'offer_code', 'code'));
    }
    if (Object.prototype.hasOwnProperty.call(b, 'is_alternative') || Object.prototype.hasOwnProperty.call(b, 'offer_is_substitute')) {
      const v = parseBooleanish(pickOwn(b, 'is_alternative', 'offer_is_substitute'));
      changes.offer_is_substitute = v == null ? null : !!v;
    }
    if (Object.prototype.hasOwnProperty.call(b, 'quote_valid_until') || Object.prototype.hasOwnProperty.call(b, 'quoteValidUntil')) {
      changes.quote_valid_until = toOptionalDateString(pickOwn(b, 'quote_valid_until', 'quoteValidUntil'));
    }
    if (Object.prototype.hasOwnProperty.call(b, 'no_parcel') || Object.prototype.hasOwnProperty.call(b, 'noParcel')) {
      const v = parseBooleanish(pickOwn(b, 'no_parcel', 'noParcel'));
      changes.no_parcel = v === true;
    }
    if (Object.prototype.hasOwnProperty.call(b, 'image_url') || Object.prototype.hasOwnProperty.call(b, 'imageUrl')) {
      changes.image_url = toOptionalTrimmed(pickOwn(b, 'image_url', 'imageUrl'));
    }
    if (Object.prototype.hasOwnProperty.call(b, 'datasheet_url') || Object.prototype.hasOwnProperty.call(b, 'datasheetUrl')) {
      changes.datasheet_url = toOptionalTrimmed(pickOwn(b, 'datasheet_url', 'datasheetUrl'));
    }
    if (Object.prototype.hasOwnProperty.call(b, 'packaging')) {
      changes.packaging = toOptionalTrimmed(b.packaging);
    }
    if (Object.prototype.hasOwnProperty.call(b, 'part_type') || Object.prototype.hasOwnProperty.call(b, 'partType')) {
      changes.part_type = toOptionalTrimmed(pickOwn(b, 'part_type', 'partType'));
    }
    if (Object.prototype.hasOwnProperty.call(b, 'mfg_year') || Object.prototype.hasOwnProperty.call(b, 'mfgYear')) {
      changes.mfg_year = toOptionalInteger(pickOwn(b, 'mfg_year', 'mfgYear'));
    }
    if (Object.prototype.hasOwnProperty.call(b, 'is_over_2yrs') || Object.prototype.hasOwnProperty.call(b, 'isOverTwoYears')) {
      const v = parseBooleanish(pickOwn(b, 'is_over_2yrs', 'isOverTwoYears'));
      changes.is_over_2yrs = v == null ? null : !!v;
    }
    if (Object.prototype.hasOwnProperty.call(b, 'has_stock') || Object.prototype.hasOwnProperty.call(b, 'hasStock')) {
      const v = parseBooleanish(pickOwn(b, 'has_stock', 'hasStock'));
      changes.has_stock = v == null ? null : !!v;
    }
    if (Object.prototype.hasOwnProperty.call(b, 'manufactured_month') || Object.prototype.hasOwnProperty.call(b, 'manufacturedMonth')) {
      changes.manufactured_month = toMonthStartDateString(pickOwn(b, 'manufactured_month', 'manufacturedMonth'));
    }
    if (Object.prototype.hasOwnProperty.call(b, 'delivery_date') || Object.prototype.hasOwnProperty.call(b, 'deliveryDate')) {
      changes.delivery_date = toOptionalDateString(pickOwn(b, 'delivery_date', 'deliveryDate'));
    }

    const keys = Object.keys(changes);
    if (!keys.length) return res.json({ ok: true, item: mapBidRow(row) });

    const sets = [];
    const args = [];
    let n = 0;
    for (const key of keys) {
      args.push(changes[key]);
      sets.push(`${key} = $${++n}`);
    }
    args.push(id);
    args.push(sellerId);
    const sql = `
      UPDATE public.plan_bids
         SET ${sets.join(', ')}, updated_at = now()
       WHERE id = $${++n} AND seller_id = $${++n}
       RETURNING
         id, purchase_request_id, seller_id,
         unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src,
        currency, offer_qty, lead_time_days, note, status,
        offer_brand, offer_code, offer_is_substitute, quote_valid_until, no_parcel, image_url, datasheet_url,
        packaging, part_type, mfg_year, is_over_2yrs, has_stock, manufactured_month, delivery_date,
        created_at, updated_at`;
    const r = await client.query(sql, args);
    return res.json({ ok: true, item: mapBidRow(r.rows[0]) });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: 'db_error' });
  } finally {
    client.release();
  }
});

// DELETE /api/bids/:id  (seller 전용)
// 기본: 상태를 withdrawn 으로 변경(취소). 쿼리 ?hard=1 이면 실제 삭제.
app.delete('/api/bids/:id', requireSeller, async (req, res) => {
  const actor = safeParseActor(req) || {};
  const sellerId = actor?.id != null ? String(actor.id) : null;
  if (!sellerId) return res.status(401).json({ ok: false, error: 'auth_required' });
  const id = String(req.params.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, error: 'id_required' });
  try {
    const r = await query(`DELETE FROM public.plan_bids WHERE id = $1 AND seller_id = $2 RETURNING id`, [id, sellerId]);
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'not_found' });
    return res.json({ ok: true, deleted_id: id });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/seller/docs-requests', async (_req, res) => {
  res.json({ ok: true, items: [] });
});

// POST /api/bids (seller 전용)
app.post('/api/bids', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) {
      return res.status(401).json({ ok: false, error: 'auth' });
    }

    const b = req.body || {};
    const sql = `
      INSERT INTO public.bids
        (seller_id,
         offer_qty, unit_price_cents, currency, lead_time_days, note,
         offer_brand, offer_code, offer_is_substitute, quote_valid_until,
         no_parcel, image_url, packaging, part_type, mfg_year, is_over_2yrs,
         has_stock, manufactured_month, delivery_date, datasheet_url, status)
      VALUES
        ($1,
         $2, $3, upper($4), $5, $6,
         $7, $8, $9, $10,
         $11, $12, $13, $14, $15, $16,
         $17, $18, $19, $20, 'offered')
      RETURNING id`;
    const vals = [
      String(actor.id),
      Number(b.offer_qty || 0),
      toCents(b.unit_price),
      b.currency,
      U(b.lead_time_days),
      U(b.note),
      U(b.brand),
      U(b.code),
      yn(b.offer_is_substitute),
      U(b.quote_valid_until),
      yn(b.no_parcel),
      U(b.image_url),
      U(b.packaging),
      U(b.part_type),
      U(b.mfg_year),
      yn(b.is_over_2yrs),
      yn(b.has_stock),
      U(b.manufactured_month),
      U(b.delivery_date),
      U(b.datasheet_url),
    ];
    const r = await query(sql, vals);
    return res.json({ ok: true, id: r.rows[0]?.id });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: 'db_error' });
  }
});

app.get('/api/seller/items', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) {
      return res.status(401).json({ ok: false, error: 'auth_required' });
    }

    const sellerId = String(req.query.seller_id || actor.id || '').trim();
    const status = String(req.query.status || '').trim().toLowerCase();
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

    const sql = `SELECT id, seller_id, brand, code, qty_available, unit_price_cents,
                        unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm,
                        unit_price_fx_src, currency, lead_time_days, status, note,
                        incoming_schedule1, incoming_qty1, incoming_schedule2, incoming_qty2,
                        location, condition, packaging, no_parcel, image_url, datasheet_url, moq, mpq, mpq_required_order,
                        part_type, mfg_year, is_over_2yrs, created_at, updated_at
                 FROM public.listings
                 ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
                 ORDER BY created_at DESC
                 LIMIT $${args.length}`;

    const r = await query(sql, args);
    const items = r.rows.map((row) => ({
      ...mapListingRow(row),
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
    const kind = String(req.body?.kind || 'stock').toLowerCase();
    const auto = String(req.body?.mode || 'auto').toLowerCase() === 'auto';
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
      mpq: idx(['mpq']),
      lead: idx(['lead', '리드', '납기', 'days']),
      status: idx(['status', '상태']),
      location: idx(['location', '위치']),
      condition: idx(['condition', '컨디션', '상태\s*\(품질\)?']),
      packaging: idx(['packaging', '포장']),
      note: idx(['note', '비고']),
      offer_qty: idx(['offer', '견적\\s*수량']),
      is_alt: idx(['substitute', '대체']),
      valid_until: idx(['valid', '유효']),
      pr_id: idx(['pr', 'pr\\s*id', 'purchase\\s*request', 'purchase_request_id']),
      no_parcel: idx(['no\\s*parcel', '택배불가', 'no\\s*delivery']),
      image_url: idx(['image_url', 'image url', 'image', '이미지']),
    };

    const items = [];
    for (let r = 1; r < rows.length; r++) {
      const row = rows[r] || [];
      const brand = map.brand >= 0 ? row[map.brand] : row[0];
      const code = map.code >= 0 ? row[map.code] : row[1];
      const rawPrice = map.price >= 0 ? row[map.price] : null;
      const price = Number(String(rawPrice || '').replace(/[^\d.]/g, '')) || 0;
      if (!brand || !code || !price) continue;

      const altYN = map.is_alt >= 0 ? String(row[map.is_alt] || '').trim() : '';
      const noParcelYN =
        map.no_parcel >= 0 ? String(row[map.no_parcel] || '').trim() : '';
      const prId = map.pr_id >= 0 ? String(row[map.pr_id] || '').trim() : '';
      const imageUrl = map.image_url >= 0 ? String(row[map.image_url] || '').trim() : '';

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
          map.status >= 0
            ? String(row[map.status] || '').toLowerCase().trim() || undefined
            : undefined,
        moq:
          map.moq >= 0
            ? Number(String(row[map.moq]).replace(/[^\d]/g, '')) || null
            : null,
        mpq:
          map.mpq >= 0
            ? Number(String(row[map.mpq]).replace(/[^\d]/g, '')) || null
            : null,
        location:
          map.location >= 0
            ? String(row[map.location] || '').trim() || null
            : null,
        condition:
          map.condition >= 0
            ? String(row[map.condition] || '').trim() || null
            : null,
        packaging:
          map.packaging >= 0
            ? String(row[map.packaging] || '').trim() || null
            : null,
        note:
          map.note >= 0 ? String(row[map.note] || '').trim() || null : null,
      };

      if (kind === 'quote') {
        items.push({
          ...base,
          offer_qty:
            map.offer_qty >= 0
              ? Number(String(row[map.offer_qty]).replace(/[^\d]/g, '')) || 0
              : 0,
          offer_is_substitute: /^y(es)?$/i.test(altYN),
          quote_valid_until:
            map.valid_until >= 0 ? String(row[map.valid_until] || '') : null,
          purchase_request_id: prId || null,
          no_parcel: /^y(es)?$/i.test(noParcelYN),
          image_url: imageUrl || null,
        });
      } else {
        items.push({
          ...base,
          qty_available:
            map.qty >= 0
              ? Number(String(row[map.qty]).replace(/[^\d]/g, '')) || 0
              : 0,
          no_parcel: /^y(es)?$/i.test(noParcelYN),
        });
      }
    }

    if (!auto || !items.length) {
      return res.json({ ok: true, items });
    }

    const actor = parseActor(req);
    if (!actor?.id) {
      return res.status(401).json({ ok: false, error: 'auth' });
    }
    const tenantId = actor?.tenantId ?? actor?.tenant_id ?? null;

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const it of items) {
        if (kind === 'stock') {
          await client.query(
            `INSERT INTO public.listings
             (tenant_id, seller_id, brand, code, qty_available, moq, mpq, mpq_required_order,
              unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src,
              currency, lead_time_days, location, condition, packaging, note, status,
              no_parcel, image_url, incoming_schedule1, incoming_qty1, incoming_schedule2, incoming_qty2, datasheet_url)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
                     $9,$10,$11,$12,$13,
                     upper($14),$15,$16,$17,$18,$19,COALESCE($20,'pending'),
                     $21,$22,$23,$24,$25,$26,$27)` ,
            [
              tenantId,
              String(actor.id),
              U(it.brand),
              U(it.code),
              Number(it.qty_available || 0),
              U(it.moq),
              U(it.mpq),
              yn(it.mpq_required_order),
              toCents(it.unit_price),
              it.krw_cents ?? null,
              it.fx_rate ?? null,
              it.fx_yyyymm ?? null,
              it.fx_src ?? null,
              it.currency,
              U(it.lead_time_days),
              U(it.location),
              U(it.condition),
              U(it.packaging),
              U(it.note),
              U(it.status),
              yn(it.no_parcel),
              U(it.image_url),
              U(it.incoming_schedule1),
              U(it.incoming_qty1),
              U(it.incoming_schedule2),
              U(it.incoming_qty2),
              U(it.datasheet_url),
            ]
          );
        } else {
          const prId = U(it.purchase_request_id);
          if (prId) {
            await client.query(
              `INSERT INTO public.plan_bids
               (purchase_request_id, seller_id, offer_qty,
                unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src,
                currency, lead_time_days, note,
                offer_brand, offer_code, offer_is_substitute, quote_valid_until, no_parcel, image_url, packaging, part_type, mfg_year,
                is_over_2yrs, has_stock, manufactured_month, delivery_date, datasheet_url, status)
               VALUES ($1,$2,$3,
                       $4,$5,$6,$7,$8,
                       upper($9),$10,$11,
                       $12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,$25,'offered')` ,
              [
                prId,
                String(actor.id),
                Number(it.offer_qty || 0),
                toCents(it.unit_price),
                it.krw_cents ?? null,
                it.fx_rate ?? null,
                it.fx_yyyymm ?? null,
                it.fx_src ?? null,
                it.currency,
                U(it.lead_time_days),
                U(it.note),
                U(it.brand),
                U(it.code),
                yn(it.offer_is_substitute),
                U(it.quote_valid_until),
                yn(it.no_parcel),
                U(it.image_url),
                U(it.packaging),
                U(it.part_type),
                U(it.mfg_year),
                yn(it.is_over_2yrs),
                yn(it.has_stock),
                U(it.manufactured_month),
                U(it.delivery_date),
                U(it.datasheet_url),
              ]
            );
          } else {
            await client.query(
              `INSERT INTO public.bids
               (seller_id, offer_qty,
                unit_price_cents, unit_price_krw_cents, unit_price_fx_rate, unit_price_fx_yyyymm, unit_price_fx_src,
                currency, lead_time_days, note,
                offer_brand, offer_code, offer_is_substitute, quote_valid_until, no_parcel, image_url, packaging, part_type, mfg_year,
                is_over_2yrs, has_stock, manufactured_month, delivery_date, datasheet_url, status)
               VALUES ($1,$2,
                       $3,$4,$5,$6,$7,
                       upper($8),$9,$10,
                       $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                       $21,$22,$23,$24,'offered')` ,
              [
                String(actor.id),
                Number(it.offer_qty || 0),
                toCents(it.unit_price),
                it.krw_cents ?? null,
                it.fx_rate ?? null,
                it.fx_yyyymm ?? null,
                it.fx_src ?? null,
                it.currency,
                U(it.lead_time_days),
                U(it.note),
                U(it.brand),
                U(it.code),
                yn(it.offer_is_substitute),
                U(it.quote_valid_until),
                yn(it.no_parcel),
                U(it.image_url),
                U(it.packaging),
                U(it.part_type),
                U(it.mfg_year),
                yn(it.is_over_2yrs),
                yn(it.has_stock),
                U(it.manufactured_month),
                U(it.delivery_date),
                U(it.datasheet_url),
              ]
            );
          }
        }
      }
      await client.query('COMMIT');
      return res.json({ ok: true, committed: items.length });
    } catch (e) {
      try { await client.query('ROLLBACK'); } catch (_) {}
      console.error(e);
      return res.status(500).json({ ok: false, error: 'commit_failed' });
    } finally {
      client.release();
    }
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

(async function warmFXOnBoot() {
  try {
    const apiKey = process.env.KOREAEXIM_API_KEY;
    if (!apiKey) return;
    const base = process.env.WORKER_URL || '';
    await fetch(`${base}/api/fx/fetch-daily`, { method: 'POST' }).catch(() => {});

    const nowK = toKST();
    const minutesUntil1115 = (11 * 60 + 15) - (nowK.getHours() * 60 + nowK.getMinutes());
    if (minutesUntil1115 > 0) {
      setTimeout(() => {
        fetch(`${base}/api/fx/recheck-today`, { method: 'POST' }).catch(() => {});
      }, minutesUntil1115 * 60 * 1000);
    }
  } catch {
    // ignore warmup errors
  }
})();

module.exports = app;
