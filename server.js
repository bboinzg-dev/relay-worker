// server.js — Cloud Run worker (Express + pg)
const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '20mb' }));

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
  } catch (e) {
    console.error('detail error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
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
  } catch (e) {
    console.error('search error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`worker listening on :${PORT}`));
