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

// --- boot log (Cloud Run logs에서 확인용)
console.log('[BOOT]', {
  hasDb: !!process.env.DATABASE_URL,
  pgsslmode: process.env.PGSSLMODE,
  gcsBucket: process.env.GCS_BUCKET,
});

// --- health / env
app.get('/_healthz', (_req, res) => res.type('text').send('ok'));
app.get('/_env', (_req, res) => {
  res.json({
    ok: true,
    hasDb: !!process.env.DATABASE_URL,
    pgsslmode: process.env.PGSSLMODE || '',
    gcsBucket: process.env.GCS_BUCKET || null,
    GIT_SHA: process.env.GIT_SHA || null
  });
});

// --- routes inspector
app.get('/__routes', (_req, res) => {
  try {
    const list = (app._router?.stack || [])
      .filter(l => l.route && l.route.path)
      .map(l => ({
        path: l.route.path,
        methods: Object.keys(l.route.methods).filter(k => l.route.methods[k]).sort()
      }));
    res.json({ ok: true, routes: list });
  } catch (e) {
    res.status(500).json({ ok:false, error:'inspector_failed' });
  }
});

// --- helper: GCS 경로 폴백
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

// --- /parts/search?q=...&limit=...
app.get('/parts/search', async (req, res) => {
  const q = String(req.query.q || '').trim();
  const limit = Math.max(1, Math.min(50, Number(req.query.limit) || 20));
  if (!q) return res.status(400).json({ ok:false, error:'q is required' });

  try {
    const sql = `
      SELECT brand, code, series, display_name, family_slug,
             contact_form, coil_voltage_vdc,
             dim_l_mm, dim_w_mm, dim_h_mm,
             datasheet_url, cover
      FROM public.relay_specs
      WHERE
        brand ILIKE '%' || $1 || '%'
        OR code ILIKE '%' || $1 || '%'
        OR series ILIKE '%' || $1 || '%'
        OR display_name ILIKE '%' || $1 || '%'
        OR contact_rating_text ILIKE '%' || $1 || '%'
      ORDER BY brand ASC, code ASC
      LIMIT $2
    `;
    const { rows } = await pool.query(sql, [q, limit]);
    return res.json({ ok:true, items: rows.map(addGcsFallback), total: rows.length });
  } catch (e) {
    console.error('search error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

// --- /parts/detail?brand=&code=
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

// --- /parts/alternatives?brand=&code=&limit=
app.get('/parts/alternatives', async (req, res) => {
  try {
    const brand = String(req.query.brand || '').trim();
    const code  = String(req.query.code  || '').trim();
    const limit = Math.max(1, Math.min(50, Number(req.query.limit) || 10));
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand and code are required' });

    const baseSql = `
      SELECT family_slug, contact_form, coil_voltage_vdc,
             dim_l_mm, dim_w_mm, dim_h_mm
      FROM public.relay_specs
      WHERE lower(brand)=lower($1) AND lower(code)=lower($2)
      LIMIT 1
    `;
    const baseRs = await pool.query(baseSql, [brand, code]);
    if (!baseRs.rows.length) return res.status(404).json({ ok:false, error:'base_not_found' });
    const b = baseRs.rows[0];

    const candSql = `
      SELECT brand, code, series, family_slug, display_name,
             contact_form, coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm,
             datasheet_url, cover
      FROM public.relay_specs
      WHERE NOT (lower(brand)=lower($1) AND lower(code)=lower($2))
    `;
    const { rows } = await pool.query(candSql, [brand, code]);

    function score(r) {
      let s = 0;
      if (r.family_slug && b.family_slug && r.family_slug === b.family_slug) s += 2.0;
      if (r.contact_form && b.contact_form && r.contact_form === b.contact_form) s += 1.5;
      if (Number.isFinite(r.coil_voltage_vdc) && Number.isFinite(b.coil_voltage_vdc)) {
        s += 1.0 - Math.min(Math.abs(r.coil_voltage_vdc - b.coil_voltage_vdc) / 30.0, 1.0);
      }
      if (Number.isFinite(r.dim_l_mm) && Number.isFinite(b.dim_l_mm)) {
        s += 0.5 - Math.min(Math.abs(r.dim_l_mm - b.dim_l_mm) / 50.0, 0.5);
      }
      if (Number.isFinite(r.dim_w_mm) && Number.isFinite(b.dim_w_mm)) {
        s += 0.5 - Math.min(Math.abs(r.dim_w_mm - b.dim_w_mm) / 20.0, 0.5);
      }
      if (Number.isFinite(r.dim_h_mm) && Number.isFinite(b.dim_h_mm)) {
        s += 0.5 - Math.min(Math.abs(r.dim_h_mm - b.dim_h_mm) / 30.0, 0.5);
      }
      return s;
    }

    const ranked = rows
      .map(addGcsFallback)
      .map(r => ({ ...r, _score: score(r) }))
      .sort((a, b2) => b2._score - a._score)
      .slice(0, limit)
      .map(({ _score, ...rest }) => rest);

    return res.json({ ok:true, items: ranked, total: ranked.length });
  } catch (e) {
    console.error('alternatives error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

// --- 엄격 업서트: /ingest/strict-upsert
// body: { family_slug, brand, code, display_name?, datasheet_url?, cover?, fields: { ... } }
app.post('/ingest/strict-upsert', async (req, res) => {
  try {
    const { family_slug, brand, code, display_name, datasheet_url, cover, fields } = req.body || {};
    if (!family_slug || !brand || !code || typeof fields !== 'object') {
      return res.status(400).json({ ok:false, error:'missing_required' });
    }
    const allowKeys = new Set([
      'contact_form', 'contact_rating_text',
      'coil_voltage_vdc', 'dim_l_mm', 'dim_w_mm', 'dim_h_mm'
    ]);

    // 값 파싱
    const out = {};
    for (const [k,v] of Object.entries(fields)) {
      const key = k.toLowerCase().replace(/\s+/g,'_');
      if (!allowKeys.has(key)) continue;
      if (/_mm$/.test(key)) {
        out[key] = toNumberInUnit(v, 'mm');
      } else if (key === 'coil_voltage_vdc') {
        out[key] = toNumberInUnit(v, 'v');
      } else {
        out[key] = typeof v === 'string' ? v.trim() : v;
      }
    }

    // 존재여부 확인 → update or insert
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const sel = await client.query(
        `SELECT id FROM public.relay_specs WHERE lower(brand)=lower($1) AND lower(code)=lower($2) LIMIT 1`,
        [brand, code]
      );

      if (sel.rows.length) {
        const id = sel.rows[0].id;
        const cols = ['family_slug','display_name','datasheet_url','cover', ...Object.keys(out)];
        const sets = cols.map((c,i) => `${c} = $${i+3}`);
        const vals = [brand, code,
          family_slug || null, display_name || null, datasheet_url || null, cover || null,
          ...Object.keys(out).map(k => out[k]),
        ];
        const sql = `
          UPDATE public.relay_specs
          SET ${sets.join(', ')}, updated_at = now()
          WHERE id = ${id}
        `;
        await client.query(sql, vals);
      } else {
        const cols = ['brand','code','family_slug','display_name','datasheet_url','cover', ...Object.keys(out)];
        const placeholders = cols.map((_,i) => `$${i+1}`).join(', ');
        const vals = [brand, code, family_slug || null, display_name || null, datasheet_url || null, cover || null,
          ...Object.keys(out).map(k => out[k])
        ];
        await client.query(
          `INSERT INTO public.relay_specs(${cols.join(',')}) VALUES (${placeholders})`
          , vals
        );
      }
      await client.query('COMMIT');
      return res.json({ ok:true });
    } catch (e) {
      await client.query('ROLLBACK');
      console.error('strict-upsert error', e);
      return res.status(500).json({ ok:false, error:'internal' });
    } finally {
      client.release();
    }
  } catch (e) {
    console.error('strict-upsert outer error', e);
    return res.status(500).json({ ok:false, error:'internal' });
  }
});

function toNumberInUnit(val, expect) {
  if (val === null || val === undefined) return null;
  const s = String(val).trim().toLowerCase();
  const n = Number((s.replace(/,/g,'').match(/-?\d+(\.\d+)?/)||[])[0]);
  if (!isFinite(n)) return null;
  if (expect === 'mm') {
    if (/\bcm\b/.test(s)) return n * 10;
    if (/\bin(ch)?\b|["”]/.test(s)) return n * 25.4;
    return n;
  }
  if (expect === 'v') return n;
  return n;
}

// --- DB 핑
app.get('/_dbping', async (_req, res) => {
  try {
    const r = await pool.query('select 1 as ok');
    res.json({ ok: true, result: r.rows[0].ok });
  } catch (e) {
    console.error('dbping', e);
    res.status(500).json({ ok: false, error: 'db' });
  }
});

// --- 마지막: JSON 404
app.all('*', (req, res) => {
  res.status(404).json({ ok:false, error:'not_found', path:req.path });
});

// --- listen
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`[LISTEN] :${PORT}`));
