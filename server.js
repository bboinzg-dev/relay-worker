// server.js — Cloud Run worker (Express + pg)
const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '20mb' }));

// ---- DB ----
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : false
});

// ---- helpers ----
const ok  = (res, data) => res.status(200).json({ ok: true, data });
const bad = (res, msg)  => res.status(400).json({ ok: false, error: msg });
const err = (res, e)    => res.status(500).json({ ok: false, error: String(e?.message || e) });

// ---- health/env ----
app.get('/_healthz', (_req, res) => res.type('text').send('ok'));
app.get('/_env', (_req, res) => res.json({
  ok: true,
  hasDb: !!process.env.DATABASE_URL,
  pgsslmode: process.env.PGSSLMODE || '',
  gcsBucket: process.env.GCS_BUCKET || null
}));

// ---- Parts: Detail ----
// GET /parts/detail?brand=omron&code=g2r-1a
app.get('/parts/detail', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    if (!brand || !code) return bad(res, 'brand and code are required');

    const sql = `
      SELECT *
      FROM public.relay_specs
      WHERE LOWER(brand) = LOWER($1)
        AND LOWER(code)  = LOWER($2)
      LIMIT 1
    `;
    const { rows } = await pool.query(sql, [brand, code]);
    if (!rows.length) return res.status(404).json({ ok: false, error: 'not_found' });

    const row = rows[0];
    // (옵션) GCS_BUCKET 존재 시 기본 cover/datasheet_url 유추
    const B = process.env.GCS_BUCKET;
    if (!row.cover && B) row.cover = `https://storage.googleapis.com/${B}/images/${row.brand}/${row.code}/cover.png`;
    if (!row.datasheet_url && B) row.datasheet_url = `https://storage.googleapis.com/${B}/datasheets/${row.code}.pdf`;

    return ok(res, row);
  } catch (e) { return err(res, e); }
});

// ---- Parts: Search ----
// GET /parts/search?q=omron g2r&limit=20&offset=0
app.get('/parts/search', async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    if (!q) return bad(res, 'q is required');
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
      ORDER BY
        CASE
          WHEN code ILIKE $1 THEN 0
          WHEN brand ILIKE $1 THEN 1
          WHEN display_name ILIKE $1 THEN 2
          ELSE 3
        END,
        brand, code
      LIMIT $2 OFFSET $3
    `;
    const { rows } = await pool.query(sql, [like, limit, offset]);
    const B = process.env.GCS_BUCKET;
    const data = rows.map(r => {
      if (!r.cover && B) r.cover = `https://storage.googleapis.com/${B}/images/${r.brand}/${r.code}/cover.png`;
      if (!r.datasheet_url && B) r.datasheet_url = `https://storage.googleapis.com/${B}/datasheets/${r.code}.pdf`;
      return r;
    });
    return ok(res, { items: data, total: data.length });
  } catch (e) { return err(res, e); }
});

// ---- Parts: Alternatives (v1 규칙 기반) ----
// GET /parts/alternatives?brand=omron&code=g2r-1a&limit=10
app.get('/parts/alternatives', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    const limit = Math.max(1, Math.min(50, parseInt(String(req.query.limit || '10'), 10) || 10));
    if (!brand || !code) return bad(res, 'brand and code are required');

    const baseSql = `
      SELECT brand, code, family_slug, contact_form, coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm
      FROM public.relay_specs
      WHERE LOWER(brand)=LOWER($1) AND LOWER(code)=LOWER($2)
      LIMIT 1
    `;
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
             + ABS(COALESCE(dim_h_mm,0) - COALESCE($7,0)) * 0.02 AS score
      FROM public.relay_specs
      WHERE NOT (LOWER(brand)=LOWER($1) AND LOWER(code)=LOWER($2))
        AND family_slug = $8
      ORDER BY score ASC, brand, code
      LIMIT $9
    `;
    const params = [
      brand, code,
      base.contact_form || null,
      base.coil_voltage_vdc || null,
      base.dim_l_mm || null,
      base.dim_w_mm || null,
      base.dim_h_mm || null,
      base.family_slug || null,
      limit
    ];
    const { rows } = await pool.query(altSql, params);
    const B = process.env.GCS_BUCKET;
    const items = rows.map(r => {
      if (!r.cover && B) r.cover = `https://storage.googleapis.com/${B}/images/${r.brand}/${r.code}/cover.png`;
      if (!r.datasheet_url && B) r.datasheet_url = `https://storage.googleapis.com/${B}/datasheets/${r.code}.pdf`;
      return r;
    });
    return ok(res, { base, items });
  } catch (e) { return err(res, e); }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`worker listening on :${PORT}`));
