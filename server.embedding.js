const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const db = require('./src/utils/db');
const { updateRowEmbedding } = require('./src/pipeline/embedding');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));

// POST /parts/embedding/rebuild  { table?: 'relay_specs' }
app.post('/parts/embedding/rebuild', async (req, res) => {
  try {
    const table = (req.body?.table || 'relay_specs').replace(/[^a-zA-Z0-9_]/g, '');
    const r = await db.query(`SELECT brand_norm, code_norm, brand, code, series, display_name, family_slug, contact_form, contact_rating_text, mounting_type, package_type, coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm FROM public/${table} LIMIT 5000`);
    let ok = 0;
    for (const row of r.rows) {
      try { await updateRowEmbedding(table, row); ok++; } catch {}
    }
    res.json({ ok: true, table, updated: ok, total: r.rows.length });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// GET /parts/alternatives/v2?brand=&code=&table=relay_specs&k=10
app.get('/parts/alternatives/v2', async (req, res) => {
  try {
    const brand = (req.query.brand || '').toString();
    const code = (req.query.code || '').toString();
    const table = (req.query.table || 'relay_specs').toString().replace(/[^a-zA-Z0-9_]/g, '');
    const k = Math.min(Number(req.query.k || 10), 50);
    if (!brand || !code) return res.status(400).json({ error: 'brand & code required' });

    const baseQ = await db.query(`SELECT *, CASE WHEN embedding IS NULL THEN 1 ELSE 0 END AS no_embed FROM public/${table} WHERE brand_norm=lower($1) AND code_norm=lower($2) LIMIT 1`, [brand, code]);
    if (!baseQ.rows.length) return res.status(404).json({ error: 'base not found' });
    const base = baseQ.rows[0];

    if (base.no_embed) {
      // fallback to v1 (assumes relay_specs presence)
      const rows = await db.query(
        `SELECT *,
          (CASE WHEN family_slug IS NOT NULL AND family_slug = $1 THEN 0 ELSE 1 END) * 1.0 +
          COALESCE(ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($2::numeric,0)) / 100.0, 1.0) AS score
         FROM public.${table}
         WHERE NOT (brand_norm=lower($3) AND code_norm=lower($4))
         ORDER BY score ASC
         LIMIT $5`,
        [base.family_slug || null, base.coil_voltage_vdc || null, brand, code, k]
      );
      return res.json({ base, items: rows.rows, mode: 'rule-fallback' });
    }

    const rows = await db.query(
      `SELECT *, (embedding <=> $1::vector) AS dist
         FROM public.${table}
        WHERE NOT (brand_norm=lower($2) AND code_norm=lower($3))
        ORDER BY embedding <=> $1::vector
        LIMIT $4`,
      [base.embedding, brand, code, k]
    );
    res.json({ base, items: rows.rows, mode: 'embedding' });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app; // for mount/merge
