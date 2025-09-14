// server.js — Cloud Run worker (Express + pg)
const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '20mb' }));

// DB
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : false
});

// health/env
app.get('/_healthz', (_req, res) => res.type('text').send('ok'));
app.get('/_env', (_req, res) => res.json({
  ok: true,
  hasDb: !!process.env.DATABASE_URL,
  pgsslmode: process.env.PGSSLMODE || '',
  gcsBucket: process.env.GCS_BUCKET || null,
  GIT_SHA: process.env.GIT_SHA || null
}));

// GCS fallback
function addGcsFallback(row) {
  if (!row) return row;
  const B = process.env.GCS_BUCKET;
  const b = (row.brand || '').toLowerCase();
  const c = (row.code  || '').toLowerCase();
  if (B) {
    if (!row.cover)         row.cover         = `https://storage.googleapis.com/${B}/images/${b}/${c}/cover.png`;
    if (!row.datasheet_url) row.datasheet_url = `https://storage.googleapis.com/${B}/datasheets/${c}.pdf`;
  }
  return row;
}

// /parts/detail
app.get('/parts/detail', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand and code are required' });
    const sql = `
      SELECT id, brand, code, series, family_slug, display_name,
             contact_form, coil_voltage_vdc, contact_rating_text,
             dim_l_mm, dim_w_mm, dim_h_mm, datasheet_url, cover
      FROM public.relay_specs
      WHERE lower(brand)=lower($1) AND lower(code)=lower($2)
      LIMIT 1`;
    const { rows } = await pool.query(sql, [brand, code]);
    if (!rows.length) return res.status(404).json({ ok:false, error:'not_found' });
    return res.json({ ok:true, item: addGcsFallback(rows[0]) });
  } catch (e) { return res.status(500).json({ ok:false, error:'internal' }); }
});

// /parts/search
app.get('/parts/search', async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    if (!q) return res.status(400).json({ ok:false, error:'q is required' });
    const limit  = Math.max(1, Math.min(100, parseInt(String(req.query.limit || '20'), 10) || 20));
    const offset = Math.max(0, parseInt(String(req.query.offset || '0'), 10) || 0);
    const like = `%${q}%`;
    const sql = `
      SELECT brand, code, series, family_slug, display_name, datasheet_url, cover,
             coil_voltage_vdc, contact_form, contact_rating_text,
             dim_l_mm, dim_w_mm, dim_h_mm
      FROM public.relay_specs
      WHERE brand ILIKE $1 OR code ILIKE $1 OR series ILIKE $1 OR family_slug ILIKE $1
         OR display_name ILIKE $1 OR contact_rating_text ILIKE $1
      ORDER BY brand, code
      LIMIT $2 OFFSET $3`;
    const { rows } = await pool.query(sql, [like, limit, offset]);
    return res.json({ ok:true, items: rows.map(addGcsFallback), total: rows.length });
  } catch (e) { return res.status(500).json({ ok:false, error:'internal' }); }
});

// /parts/alternatives (규칙 v1)
app.get('/parts/alternatives', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    const limit = Math.max(1, Math.min(50, parseInt(String(req.query.limit || '10'), 10) || 10));
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand and code are required' });

    const baseSql = `
      SELECT brand, code, family_slug, contact_form, coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm
      FROM public.relay_specs WHERE lower(brand)=lower($1) AND lower(code)=lower($2) LIMIT 1`;
    const { rows: baseRows } = await pool.query(baseSql, [brand, code]);
    if (!baseRows.length) return res.status(404).json({ ok:false, error:'base_not_found' });
    const b = baseRows[0];

    const altSql = `
      SELECT brand, code, series, family_slug, display_name, datasheet_url, cover,
             contact_form, coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm,
             (CASE WHEN contact_form = $3 THEN 0 ELSE 1 END) * 5
             + ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($4,0)) * 0.2
             + ABS(COALESCE(dim_l_mm,0) - COALESCE($5,0)) * 0.02
             + ABS(COALESCE(dim_w_mm,0) - COALESCE($6,0)) * 0.02
             + ABS(COALESCE(dim_h_mm,0) - COALESCE($7,0)) * 0.02 AS score
      FROM public.relay_specs
      WHERE NOT (lower(brand)=lower($1) AND lower(code)=lower($2))
        AND family_slug = $8
      ORDER BY score ASC, brand, code
      LIMIT $9`;
    const params = [
      brand, code,
      b.contact_form || null,
      b.coil_voltage_vdc || null,
      b.dim_l_mm || null,
      b.dim_w_mm || null,
      b.dim_h_mm || null,
      b.family_slug || null,
      limit
    ];
    const { rows } = await pool.query(altSql, params);
    return res.json({ ok:true, base: b, items: rows.map(addGcsFallback) });
  } catch (e) { return res.status(500).json({ ok:false, error:'internal' }); }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`worker listening on :${PORT}`));
