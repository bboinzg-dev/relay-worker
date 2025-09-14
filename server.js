// server.js — minimal, known-good
const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '20mb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : false,
});

// health/env
app.get('/_healthz', (_req, res) => res.type('text').send('ok'));
app.get('/_env', (_req, res) => res.json({
  ok: true, hasDb: !!process.env.DATABASE_URL,
  pgsslmode: process.env.PGSSLMODE || '',
  gcsBucket: process.env.GCS_BUCKET || null
}));

// debug routes
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

// /parts/detail
app.get('/parts/detail', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand and code are required' });
    const sql = `SELECT * FROM public.relay_specs WHERE lower(brand)=lower($1) AND lower(code)=lower($2) LIMIT 1`;
    const { rows } = await pool.query(sql, [brand, code]);
    if (!rows.length) return res.status(404).json({ ok:false, error:'not_found' });

    const row = rows[0];
    const B = process.env.GCS_BUCKET;
    const b = (row.brand||'').toLowerCase(), c = (row.code||'').toLowerCase();
    if (!row.cover && B) row.cover = `https://storage.googleapis.com/${B}/images/${b}/${c}/cover.png`;
    if (!row.datasheet_url && B) row.datasheet_url = `https://storage.googleapis.com/${B}/datasheets/${c}.pdf`;

    return res.json({ ok:true, item: row });
  } catch (e) {
    console.error('detail error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

// /parts/search (간단 버전)
app.get('/parts/search', async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    if (!q) return res.status(400).json({ ok:false, error:'q is required' });
    const like = `%${q}%`;
    const limit  = Math.max(1, Math.min(100, parseInt(String(req.query.limit || '20'), 10) || 20));
    const offset = Math.max(0, parseInt(String(req.query.offset || '0'), 10) || 0);
    const sql = `
      SELECT brand, code, series, family_slug, display_name, datasheet_url, cover,
             coil_voltage_vdc, contact_form, contact_rating_text,
             dim_l_mm, dim_w_mm, dim_h_mm
      FROM public.relay_specs
      WHERE brand ILIKE $1 OR code ILIKE $1 OR series ILIKE $1 OR family_slug ILIKE $1
         OR display_name ILIKE $1 OR contact_rating_text ILIKE $1
      ORDER BY brand, code
      LIMIT $2 OFFSET $3
    `;
    const { rows } = await pool.query(sql, [like, limit, offset]);
    const B = process.env.GCS_BUCKET;
    const items = rows.map(r => {
      const b = (r.brand||'').toLowerCase(), c = (r.code||'').toLowerCase();
      if (!r.cover && B) r.cover = `https://storage.googleapis.com/${B}/images/${b}/${c}/cover.png`;
      if (!r.datasheet_url && B) r.datasheet_url = `https://storage.googleapis.com/${B}/datasheets/${c}.pdf`;
      return r;
    });
    return res.json({ ok:true, items, total: items.length });
  } catch (e) {
    console.error('search error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

// listen
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`worker listening on :${PORT}`));
