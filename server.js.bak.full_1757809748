const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '20mb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : false,
});

function ok(res, data) { return res.status(200).json({ ok: true, data }); }
function bad(res, msg) { return res.status(400).json({ ok: false, error: msg }); }
function err(res, e)   { return res.status(500).json({ ok: false, error: String(e?.message || e) }); }

app.get('/_healthz', (req, res) => res.type('text').send('ok'));
app.get('/_env', (req, res) => res.json({ ok: true, hasDb: !!process.env.DATABASE_URL, pgsslmode: process.env.PGSSLMODE || '' }));

// 상세
app.get('/parts/detail', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    if (!brand || !code) return bad(res, 'brand and code are required');
    const sql = `SELECT * FROM public.relay_specs WHERE LOWER(brand)=LOWER($1) AND LOWER(code)=LOWER($2) LIMIT 1`;
    const { rows } = await pool.query(sql, [brand, code]);
    if (!rows.length) return res.status(404).json({ ok: false, error: 'not_found' });
    const row = rows[0];
    if (!row.cover && process.env.GCS_BUCKET) row.cover = `https://storage.googleapis.com/${process.env.GCS_BUCKET}/images/${row.brand}/${row.code}/cover.png`;
    if (!row.datasheet_url && process.env.GCS_BUCKET) row.datasheet_url = `https://storage.googleapis.com/${process.env.GCS_BUCKET}/datasheets/${row.code}.pdf`;
    return ok(res, row);
  } catch (e) { return err(res, e); }
});

// 검색
app.get('/parts/search', async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    const limit  = Math.max(1, Math.min(100, parseInt(String(req.query.limit || '20'), 10) || 20));
    const offset = Math.max(0, parseInt(String(req.query.offset || '0'), 10) || 0);
    if (!q) return bad(res, 'q is required');
    const like = `%${q}%`;
    const sql = `
      SELECT brand, code, series, family_slug, display_name, datasheet_url, cover,
             coil_voltage_vdc, contact_form, contact_rating_text,
             dim_l_mm, dim_w_mm, dim_h_mm
      FROM public.relay_specs
      WHERE brand ILIKE $1 OR code ILIKE $1 OR series ILIKE $1 OR family_slug ILIKE $1
         OR display_name ILIKE $1 OR contact_rating_text ILIKE $1
      ORDER BY
        CASE WHEN code ILIKE $1 THEN 0 WHEN brand ILIKE $1 THEN 1 WHEN display_name ILIKE $1 THEN 2 ELSE 3 END,
        brand, code
      LIMIT $2 OFFSET $3
    `;
    const { rows } = await pool.query(sql, [like, limit, offset]);
    const data = rows.map(r => {
      if (!r.cover && process.env.GCS_BUCKET) r.cover = `https://storage.googleapis.com/${process.env.GCS_BUCKET}/images/${r.brand}/${r.code}/cover.png`;
      if (!r.datasheet_url && process.env.GCS_BUCKET) r.datasheet_url = `https://storage.googleapis.com/${process.env.GCS_BUCKET}/datasheets/${r.code}.pdf`;
      return r;
    });
    return ok(res, { items: data, total: data.length });
  } catch (e) { return err(res, e); }
});

// 대체품 v1
app.get('/parts/alternatives', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    const limit = Math.max(1, Math.min(50, parseInt(String(req.query.limit || '10'), 10) || 10));
    if (!brand || !code) return bad(res, 'brand and code are required');

    const baseSql = `SELECT brand, code, family_slug, contact_form, coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm FROM public.relay_specs WHERE LOWER(brand)=LOWER($1) AND LOWER(code)=LOWER($2) LIMIT 1`;
    const { rows: baseRows } = await pool.query(baseSql, [brand, code]);
    if (!baseRows.length) return res.status(404).json({ ok: false, error: 'base_not_found' });
    const base = baseRows[0];

    const altSql = `
      SELECT brand, code, series, family_slug, display_name, datasheet_url, cover,
             contact_form, coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm,
             (CASE WHEN contact_form = $3 THEN 0 ELSE 1 END) * 5
             + ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($4,0)) * 0.2
             + ABS(COALESCE(dim_l_mm,0) - COALESCE($5,0)) * 0.02
             + ABS(COALESCE(dim_w_mm,0) - COALESCE($6,0)) * 0.02
             + ABS(COALESCE(dim_h_mm,0) - COALESCE($7,0)) * 0.02
             AS score
      FROM public.relay_specs
      WHERE NOT (LOWER(brand)=LOWER($1) AND LOWER(code)=LOWER($2))
        AND family_slug = $8
      ORDER BY score ASC, brand, code
      LIMIT $9
    `;
    const params = [brand, code, base.contact_form || null, base.coil_voltage_vdc || null, base.dim_l_mm || null, base.dim_w_mm || null, base.dim_h_mm || null, base.family_slug || null, limit];
    const { rows } = await pool.query(altSql, params);
    const items = rows.map(r => {
      if (!r.cover && process.env.GCS_BUCKET) r.cover = `https://storage.googleapis.com/${process.env.GCS_BUCKET}/images/${r.brand}/${r.code}/cover.png`;
      if (!r.datasheet_url && process.env.GCS_BUCKET) r.datasheet_url = `https://storage.googleapis.com/${process.env.GCS_BUCKET}/datasheets/${r.code}.pdf`;
      return r;
    });
    return ok(res, { base, items });
  } catch (e) { return err(res, e); }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`worker listening on :${PORT}`));

// register parts routes

// ---- Parts: Detail (inline, no external module) ----
const pg = require('pg');
const __pool = global.__pgPool || (global.__pgPool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
}));

async function __detailHandler(req, res) {
  const brand = String(req.query.brand || '').trim();
  const code  = String(req.query.code  || '').trim();
  if (!brand || !code) return res.status(400).json({ ok:false, error:'brand and code are required' });
  try {
    const sql = `
      SELECT brand, code, series, display_name, family_slug, contact_form,
             coil_voltage_vdc, contact_rating_text,
             dim_l_mm, dim_w_mm, dim_h_mm,
             datasheet_url, cover
      FROM public.relay_specs
      WHERE lower(brand)=lower($1) AND lower(code)=lower($2)
      LIMIT 1
    `;
    const { rows } = await __pool.query(sql, [brand, code]);
    if (!rows.length) return res.status(404).json({ ok:false, error:'not_found' });

    const row = rows[0];
    const B = process.env.GCS_BUCKET;
    const b = (row.brand||'').toLowerCase();
    const c = (row.code||'').toLowerCase();
    if (!row.cover && B)        row.cover        = `https://storage.googleapis.com/${B}/images/${b}/${c}/cover.png`;
    if (!row.datasheet_url && B)row.datasheet_url= `https://storage.googleapis.com/${B}/datasheets/${c}.pdf`;

    return res.json({ ok:true, data: row });
  } catch (e) {
    console.error('detail error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
}

app.get('/parts/detail', __detailHandler);
app.get('/api/parts/detail', __detailHandler);

// ---- Strict Upsert by Blueprint ----
// POST /ingest/strict-upsert  (alias: /api/ingest/strict-upsert)
// body: { family_slug, brand, code, fields: {k:v}, datasheet_url?, cover? }
const { Pool: __Pool2 } = require('pg');
const __pool2 = global.__pool2 || (global.__pool2 = new __Pool2({
  connectionString: process.env.DATABASE_URL,
  max: 5,
}));

function __normKey(k) {
  return String(k || '').trim().toLowerCase();
}
function __asNumber(v) {
  if (v === null || v === undefined) return undefined;
  if (typeof v === 'number') return isFinite(v) ? v : undefined;
  const s = String(v);
  // pick first number like 1, 1.2, 1,200.5
  const m = s.replace(/,/g, '').match(/-?\d+(\.\d+)?/);
  return m ? Number(m[0]) : undefined;
}
// very small unit helper (extend later as needed)
function __convertWithUnit(val, expect, rawKey) {
  const s = String(val || '');
  const n = __asNumber(s);
  if (n === undefined) return undefined;
  const sL = s.toLowerCase();

  switch ((expect || '').toLowerCase()) {
    case 'mm': {
      // cm->mm, in->mm
      if (/\bcm\b/.test(sL)) return n * 10;
      if (/\bin(ch)?\b|["”]/.test(sL)) return n * 25.4;
      return n;
    }
    case 'vdc':
    case 'v': {
      return n; // treat v/vdc same scale
    }
    case 'ohm': {
      if (/k(ohm|Ω)/i.test(s)) return n * 1e3;
      if (/m(ohm|Ω)/i.test(s)) return n * 1e6;
      return n;
    }
    case 'ma': {
      if (/\ba\b/.test(sL)) return n * 1000;
      return n;
    }
    case 'mΩ':
    case 'mohm': {
      if (/ohm|Ω/i.test(s)) return n * 1000; // ohm -> mΩ
      return n;
    }
    default:
      return n;
  }
}

async function __loadBlueprint(client, family) {
  const sql = `
    SELECT fields, aliases, units
    FROM public.component_spec_blueprint
    WHERE family_slug=$1 AND is_active=true
    ORDER BY version DESC
    LIMIT 1
  `;
  const { rows } = await client.query(sql, [family]);
  return rows[0] || null;
}

function __normalizeByBlueprint(rawFields, bp) {
  const fields = Array.isArray(bp?.fields) ? bp.fields : (bp?.fields ? bp.fields : []);
  const aliases = bp?.aliases || {};
  const units = bp?.units || {};

  // build alias map (normalized key -> std key)
  const aliasMap = {};
  for (const std of fields) {
    const name = std.name;
    aliasMap[__normKey(name)] = name;
  }
  for (const [k, v] of Object.entries(aliases)) {
    aliasMap[__normKey(k)] = v;
  }

  const required = new Set(fields.filter(f => f.required).map(f => f.name));
  const types = Object.fromEntries(fields.map(f => [f.name, f.type || 'text']));
  const expectUnit = Object.fromEntries(Object.entries(units).map(([k,v]) => [k, String(v)]));

  const allowed = {};
  const extras = {};
  for (const [k, v] of Object.entries(rawFields || {})) {
    const std = aliasMap[__normKey(k)];
    if (std && std in types) {
      if (types[std] === 'number') {
        const vv = __convertWithUnit(v, expectUnit[std], std);
        if (vv !== undefined && !Number.isNaN(vv)) allowed[std] = vv;
      } else {
        // text
        const s = v == null ? null : String(v).trim();
        if (s !== null && s !== '') allowed[std] = s;
      }
    } else {
      extras[k] = v;
    }
  }

  // track missing required (for logging)
  const missing = [];
  for (const r of required) {
    if (!(r in allowed)) missing.push(r);
  }

  return { allowed, extras, missing };
}

async function __upsertRelay(client, base, allowed, extras) {
  // UPDATE first (case-insensitive key), then INSERT if not found
  const keys = Object.keys(allowed);
  const sets = [];
  const vals = [];
  let idx = 1;

  // base mandatory
  vals.push(base.brand); // $1
  vals.push(base.code);  // $2

  for (const k of keys) {
    sets.push(`${k}= $${idx + 2}`);
    vals.push(allowed[k]);
    idx++;
  }
  // datasheet_url/cover/specs_extra also set if provided
  if (base.datasheet_url) { sets.push(`datasheet_url = $${idx + 2}`); vals.push(base.datasheet_url); idx++; }
  if (base.cover)         { sets.push(`cover         = $${idx + 2}`); vals.push(base.cover);         idx++; }
  sets.push(`specs_extra   = COALESCE(specs_extra,'{}'::jsonb) || $${idx + 2}::jsonb`);
  vals.push(JSON.stringify(extras));
  idx++;

  sets.push(`updated_at = now()`);

  const updateSql = `
    UPDATE public.relay_specs
    SET ${sets.join(', ')}
    WHERE lower(brand)=lower($1) AND lower(code)=lower($2)
    RETURNING id
  `;

  const u = await client.query(updateSql, vals);
  if (u.rowCount > 0) return { mode:'update', id: u.rows[0].id };

  // INSERT
  const insertCols = ['brand','code'];
  const insertVals = ['$1','$2'];
  const insValsArr = [base.brand, base.code];
  let p = 3;

  for (const k of keys) { insertCols.push(k); insertVals.push(`$${p}`); insValsArr.push(allowed[k]); p++; }
  if (base.datasheet_url) { insertCols.push('datasheet_url'); insertVals.push(`$${p}`); insValsArr.push(base.datasheet_url); p++; }
  if (base.cover)         { insertCols.push('cover');         insertVals.push(`$${p}`); insValsArr.push(base.cover);         p++; }
  insertCols.push('specs_extra'); insertVals.push(`$${p}::jsonb`); insValsArr.push(JSON.stringify(extras)); p++;
  insertCols.push('created_at');  insertVals.push(`now()`);
  insertCols.push('updated_at');  insertVals.push(`now()`);

  const insertSql = `
    INSERT INTO public.relay_specs (${insertCols.join(',')})
    VALUES (${insertVals.join(',')})
    RETURNING id
  `;
  const i = await client.query(insertSql, insValsArr);
  return { mode:'insert', id: i.rows[0].id };
}

async function __upsertComponentItem(client, base, allowed, extras) {
  // ensure row for component_items too (generic table)
  const up = await client.query(`
    UPDATE public.component_items
    SET display_name=COALESCE($4, display_name),
        series=$5,
        datasheet_url=COALESCE($6, datasheet_url),
        cover=COALESCE($7, cover),
        specs = COALESCE(specs,'{}'::jsonb) || $8::jsonb,
        updated_at=now()
    WHERE family_slug=$1 AND lower(brand)=lower($2) AND lower(code)=lower($3)
    RETURNING id
  `, [base.family_slug, base.brand, base.code, base.display_name || null, allowed.series || null, base.datasheet_url || null, base.cover || null, JSON.stringify({ ...allowed, ...extras })]);
  if (up.rowCount > 0) return { mode:'update', id: up.rows[0].id };

  const ins = await client.query(`
    INSERT INTO public.component_items (family_slug, brand, code, display_name, series, datasheet_url, cover, specs, created_at, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb, now(), now())
    RETURNING id
  `, [base.family_slug, base.brand, base.code, base.display_name || null, allowed.series || null, base.datasheet_url || null, base.cover || null, JSON.stringify({ ...allowed, ...extras })]);
  return { mode:'insert', id: ins.rows[0].id };
}

app.post('/ingest/strict-upsert', async (req, res) => {
  const b = req.body || {};
  const family_slug = String(b.family_slug || '').trim().toLowerCase();
  const brand = String(b.brand || '').trim();
  const code  = String(b.code  || '').trim();
  if (!family_slug || !brand || !code || typeof b.fields !== 'object') {
    return res.status(400).json({ ok:false, error:'family_slug, brand, code, fields are required' });
  }

  const client = await __pool2.connect();
  try {
    const bp = await __loadBlueprint(client, family_slug);
    if (!bp) return res.status(400).json({ ok:false, error:`blueprint_not_found:${family_slug}` });

    const { allowed, extras, missing } = __normalizeByBlueprint(b.fields, bp);
    const base = {
      family_slug, brand, code,
      display_name: b.display_name || null,
      datasheet_url: b.datasheet_url || null,
      cover: b.cover || null
    };

    // family별 테이블로 업서트 (현재는 릴레이만 전용 테이블), + component_items에도 반영
    let detail;
    if (family_slug === 'relay') {
      detail = await __upsertRelay(client, base, allowed, extras);
    }

    const generic = await __upsertComponentItem(client, base, allowed, extras);

    return res.json({ ok:true, family_slug, brand, code, detail, generic, missing_required: missing });
  } catch (e) {
    console.error('strict-upsert error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  } finally {
    client.release();
  }
});

app.post('/api/ingest/strict-upsert', (req,res) => app._router.handle(req, res)); // alias

// ---- debug: list routes ----
app.get('/_routes', (_req, res) => {
  const out = [];
  (app._router?.stack || []).forEach((l) => {
    if (l.route && l.route.path) {
      const methods = Object.keys(l.route.methods).filter(k => l.route.methods[k]).map(m => m.toUpperCase());
      out.push({ methods, path: l.route.path });
    }
  });
  res.json({ ok: true, routes: out });
});
