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
