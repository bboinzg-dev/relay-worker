// server.js — Cloud Run worker (Express + pg)
const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '20mb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : false,
  max: 5,
});

// ---- health/env
app.get('/_healthz', (_req, res) => res.type('text').send('ok'));
app.get('/_env', (_req, res) => res.json({
  ok: true,
  hasDb: !!process.env.DATABASE_URL,
  pgsslmode: process.env.PGSSLMODE || '',
  gcsBucket: process.env.GCS_BUCKET || null,
  GIT_SHA: process.env.GIT_SHA || null
}));

// ---- debug routes
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

// ---- helpers
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

let __relayCols;
async function getRelayColumns() {
  if (__relayCols) return __relayCols;
  const { rows } = await pool.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='relay_specs'
  `);
  __relayCols = new Set(rows.map(r => r.column_name));
  return __relayCols;
}

// ---- /parts/search  (q, limit, offset)
app.get('/parts/search', async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    if (!q) return res.status(400).json({ ok:false, error:'q is required' });
    const limit  = Math.max(1, Math.min(100, parseInt(String(req.query.limit || '20'), 10) || 20));
    const offset = Math.max(0, parseInt(String(req.query.offset || '0'), 10) || 0);
    const like = `%${q}%`;

    const sql = `
      SELECT brand, code, series, family_slug, display_name,
             coil_voltage_vdc, contact_form, contact_rating_text,
             dim_l_mm, dim_w_mm, dim_h_mm, datasheet_url, cover
      FROM public.relay_specs
      WHERE brand ILIKE $1 OR code ILIKE $1 OR series ILIKE $1 OR family_slug ILIKE $1
         OR display_name ILIKE $1 OR contact_rating_text ILIKE $1
      ORDER BY brand, code
      LIMIT $2 OFFSET $3
    `;
    const { rows } = await pool.query(sql, [like, limit, offset]);
    return res.json({ ok:true, items: rows.map(addGcsFallback), total: rows.length });
  } catch (e) {
    console.error('search error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

// ---- /parts/detail  (brand, code)
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
      LIMIT 1
    `;
    const { rows } = await pool.query(sql, [brand, code]);
    if (!rows.length) return res.status(404).json({ ok:false, error:'not_found' });
    return res.json({ ok:true, item: addGcsFallback(rows[0]) });
  } catch (e) {
    console.error('detail error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

// ---- /parts/alternatives  (brand, code, limit)
app.get('/parts/alternatives', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    const limit = Math.max(1, Math.min(20, parseInt(String(req.query.limit || '10'), 10) || 10));
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand and code are required' });

    const sql = `
      WITH base AS (
        SELECT brand, code, family_slug,
               NULLIF(regexp_replace(coil_voltage_vdc,'[^0-9\\.\\-]+','','g'),'')::numeric AS coil_v,
               NULLIF(regexp_replace(dim_l_mm,'[^0-9\\.\\-]+','','g'),'')::numeric AS l,
               NULLIF(regexp_replace(dim_w_mm,'[^0-9\\.\\-]+','','g'),'')::numeric AS w,
               NULLIF(regexp_replace(dim_h_mm,'[^0-9\\.\\-]+','','g'),'')::numeric AS h,
               contact_form
        FROM public.relay_specs
        WHERE lower(brand)=lower($1) AND lower(code)=lower($2)
        LIMIT 1
      ),
      cands AS (
        SELECT r.*,
               2.0 * (CASE WHEN r.family_slug = b.family_slug THEN 1 ELSE 0 END)
             + 1.5 * (CASE WHEN r.contact_form = b.contact_form THEN 1 ELSE 0 END)
             + 1.0 * (1.0 - LEAST(ABS(COALESCE(NULLIF(regexp_replace(r.coil_voltage_vdc,'[^0-9\\.\\-]+','','g'),'')::numeric,0)
                                           - COALESCE(b.coil_v,0))/30.0, 1.0))
             + 0.5 * (1.0 - LEAST(ABS(COALESCE(NULLIF(regexp_replace(r.dim_l_mm,'[^0-9\\.\\-]+','','g'),'')::numeric,0)
                                           - COALESCE(b.l,0))/50.0, 1.0))
             + 0.5 * (1.0 - LEAST(ABS(COALESCE(NULLIF(regexp_replace(r.dim_w_mm,'[^0-9\\.\\-]+','','g'),'')::numeric,0)
                                           - COALESCE(b.w,0))/20.0, 1.0))
             + 0.5 * (1.0 - LEAST(ABS(COALESCE(NULLIF(regexp_replace(r.dim_h_mm,'[^0-9\\.\\-]+','','g'),'')::numeric,0)
                                           - COALESCE(b.h,0))/30.0, 1.0)) AS score
        FROM public.relay_specs r
        CROSS JOIN base b
        WHERE NOT (lower(r.brand)=lower(b.brand) AND lower(r.code)=lower(b.code))
      )
      SELECT brand, code, series, family_slug, contact_form,
             coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm,
             datasheet_url, cover, score
      FROM cands
      WHERE score IS NOT NULL
      ORDER BY score DESC, brand, code
      LIMIT $3
    `;
    const { rows } = await pool.query(sql, [brand, code, limit]);
    return res.json({ ok:true, items: rows.map(addGcsFallback), total: rows.length });
  } catch (e) {
    console.error('alternatives error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

// ---- /ingest/strict-upsert (family_slug, brand, code, fields{}, display_name?, datasheet_url?, cover?)
app.post('/ingest/strict-upsert', async (req, res) => {
  const body = req.body || {};
  const family = String(body.family_slug || '').trim().toLowerCase();
  const brand  = String(body.brand || '').trim();
  const code   = String(body.code  || '').trim();
  if (!family || !brand || !code) return res.status(400).json({ ok:false, error:'family_slug, brand, code required' });

  const blueprint = new Set([
    'display_name','series','family_slug','contact_form','coil_voltage_vdc',
    'dim_l_mm','dim_w_mm','dim_h_mm','contact_rating_text','datasheet_url','cover'
  ]);

  const fields = body.fields || {};
  const mapped = {};
  for (const [k, v] of Object.entries(fields)) {
    const key = String(k).toLowerCase();
    if (/coil/.test(key) && /v/.test(key)) mapped['coil_voltage_vdc'] = String(v);
    else if (/length|dim[_ ]*l\b|l\W?mm/i.test(key)) mapped['dim_l_mm'] = String(v);
    else if (/width|dim[_ ]*w\b|w\W?mm/i.test(key))  mapped['dim_w_mm'] = String(v);
    else if (/height|dim[_ ]*h\b|h\W?mm/i.test(key)) mapped['dim_h_mm'] = String(v);
    else if (/contact.*form/i.test(key)) mapped['contact_form'] = String(v);
    else if (/contact.*rating/i.test(key)) mapped['contact_rating_text'] = String(v);
  }
  if (body.display_name)  mapped.display_name  = String(body.display_name);
  if (body.datasheet_url) mapped.datasheet_url = String(body.datasheet_url);
  if (body.cover)         mapped.cover         = String(body.cover);
  mapped.family_slug = family;

  // 숫자 추정
  for (const nk of ['coil_voltage_vdc','dim_l_mm','dim_w_mm','dim_h_mm']) {
    if (mapped[nk] != null) {
      const m = String(mapped[nk]).replace(/,/g,'').match(/-?\d+(\.\d+)?/);
      if (m) mapped[nk] = Number(m[0]);
    }
  }

  const colSet = await getRelayColumns();
  const cols = [], vals = [];
  for (const k of Object.keys(mapped)) {
    if (blueprint.has(k) && colSet.has(k)) { cols.push(k); vals.push(mapped[k]); }
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const sel = await client.query(
      `SELECT id FROM public.relay_specs WHERE lower(brand)=lower($1) AND lower(code)=lower($2) LIMIT 1`,
      [brand, code]
    );

    if (sel.rowCount) {
      const sets = cols.map((c,i)=> `"${c}" = $${i+3}`);
      const sql = `
        UPDATE public.relay_specs
        SET ${sets.join(', ')}, updated_at = now()
        WHERE id = $1
        RETURNING *;
      `;
      const { rows } = await client.query(sql, [sel.rows[0].id, brand, ...vals]);
      await client.query('COMMIT');
      return res.json({ ok:true, mode:'update', item: addGcsFallback(rows[0]) });
    } else {
      const sql = `
        INSERT INTO public.relay_specs
          (brand, code, ${cols.map(c=>`"${c}"`).join(', ')}, created_at, updated_at)
        VALUES
          ($1, $2, ${cols.map((_,i)=>`$${i+3}`).join(', ')}, now(), now())
        RETURNING *;
      `;
      const { rows } = await client.query(sql, [brand, code, ...vals]);
      await client.query('COMMIT');
      return res.json({ ok:true, mode:'insert', item: addGcsFallback(rows[0]) });
    }
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('strict-upsert error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  } finally {
    client.release();
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`worker listening on :${PORT}`));
// __END__
