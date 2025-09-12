// server.js — Worker (Cloud Run)
// CommonJS (package.json: "type": "commonjs")
const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '10mb' }));

// DB 연결 (사설 IP 환경: DB_SSL=false 권장)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
});

// ---- helpers ----
function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}
const norm = (s) => (s ?? '').toString().trim().toLowerCase();
const toNum = (x) => {
  if (x == null) return null;
  const m = String(x).match(/-?\d+(?:\.\d+)?/);
  return m ? parseFloat(m[0]) : null;
};
function scoreCandidate(base, c) {
  let score = 0;
  // 대체품 추천 v1 가중치(간단 규칙)
  if (base.series && c.series && norm(base.series) === norm(c.series)) score += 1.0;
  if (base.contact_form && c.contact_form && norm(base.contact_form) === norm(c.contact_form)) score += 0.6;
  if (norm(base.brand) === norm(c.brand)) score += 0.5;
  // 코일 전압 근접 (최대 1.2)
  const bv = toNum(base.coil_voltage_vdc), cv = toNum(c.coil_voltage_vdc);
  if (bv != null && cv != null) {
    const diff = Math.min(Math.abs(bv - cv) / 24.0, 1);
    score += (1 - diff) * 1.2;
  }
  // 외형 치수 근접 (최대 0.9)
  const dims = ['dim_l_mm', 'dim_w_mm', 'dim_h_mm'];
  let dimsScore = 0, dimsCnt = 0;
  for (const k of dims) {
    const a = toNum(base[k]), b = toNum(c[k]);
    if (a != null && b != null) {
      const d = Math.min(Math.abs(a - b) / 50.0, 1);
      dimsScore += (1 - d);
      dimsCnt++;
    }
  }
  if (dimsCnt) score += (dimsScore / dimsCnt) * 0.9;
  return score;
}

// ---- health ----
app.get('/_healthz', (_req, res) => res.status(200).send('ok'));
app.get('/api/health', async (_req, res) => {
  try { await pool.query('select 1'); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: String(e) }); }
});

// ---- ingest (최소 스켈레톤) ----
app.post('/api/worker/ingest', async (req, res) => {
  try {
    const { gcsPdfUri } = req.body || {};
    if (!gcsPdfUri) return res.status(400).json({ ok:false, error:'gcsPdfUri required' });

    // 임시: 최소 기록만 남김 (DocAI 연동은 다른 파일/단계에서)
    await pool.query(
      `INSERT INTO public.relay_specs (gcs_pdf_uri, status, created_at, updated_at)
       VALUES ($1, 'RECEIVED', now(), now())`,
      [gcsPdfUri]
    );

    return res.json({ ok:true, accepted:true });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok:false, error: e.message || String(e) });
  }
});

// ---- Read APIs: 상세 & 대체품 ----

// GET /api/parts/detail?brand=...&code=...
app.get('/api/parts/detail', async (req, res) => {
  try {
    setCORS(res);
    const brand = req.query.brand, code = req.query.code;
    if (!brand || !code) return res.status(400).json({ ok: false, error: 'brand, code are required' });

    const sql = `
      SELECT *
      FROM public.relay_specs
      WHERE lower(trim(brand)) = lower(trim($1))
        AND lower(trim(code))  = lower(trim($2))
      ORDER BY updated_at DESC NULLS LAST
      LIMIT 1
    `;
    const { rows } = await pool.query(sql, [brand, code]);
    if (!rows.length) return res.status(404).json({ ok: false, error: 'not_found' });

    const it = rows[0];
    const datasheet_url = it.pdf_uri || it.datasheet_url || it.gcs_pdf_uri || null;
    const image_url = it.image_url || it.cover || null;

    return res.json({ ok: true, item: it, datasheet_url, image_url });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

// GET /api/parts/alternatives?brand=...&code=...&limit=10
app.get('/api/parts/alternatives', async (req, res) => {
  try {
    setCORS(res);
    const brand = req.query.brand, code = req.query.code;
    const limit = Math.min(parseInt(req.query.limit || '10', 10), 50);
    if (!brand || !code) return res.status(400).json({ ok: false, error: 'brand, code are required' });

    // 1) 기준 부품
    const baseSql = `
      SELECT id, brand, series, code, contact_form, coil_voltage_vdc,
             dim_l_mm, dim_w_mm, dim_h_mm, pdf_uri, datasheet_url, gcs_pdf_uri, updated_at
      FROM public.relay_specs
      WHERE lower(trim(brand)) = lower(trim($1))
        AND lower(trim(code))  = lower(trim($2))
      ORDER BY updated_at DESC NULLS LAST
      LIMIT 1
    `;
    const base = await pool.query(baseSql, [brand, code]).then(r => r.rows[0]);
    if (!base) return res.status(404).json({ ok: false, error: 'not_found' });

    // 2) 후보군 (동일 series OR 동일 brand OR 동일 contact_form)
    const candSql = `
      SELECT id, brand, series, code, contact_form, coil_voltage_vdc,
             dim_l_mm, dim_w_mm, dim_h_mm, pdf_uri, datasheet_url, gcs_pdf_uri, updated_at
      FROM public.relay_specs
      WHERE NOT (lower(trim(brand)) = lower(trim($1)) AND lower(trim(code)) = lower(trim($2)))
        AND (
              (series IS NOT NULL AND $3 IS NOT NULL AND lower(series) = lower($3))
           OR (lower(trim(brand)) = lower(trim($1)))
           OR (contact_form IS NOT NULL AND $4 IS NOT NULL AND contact_form = $4)
        )
      ORDER BY updated_at DESC NULLS LAST
      LIMIT 300
    `;
    const candParams = [brand, code, base.series || null, base.contact_form || null];
    const { rows: candidates } = await pool.query(candSql, candParams);

    // 3) 점수화
    const scored = candidates.map(c => ({ ...c, _score: scoreCandidate(base, c) }));
    scored.sort((a, b) => (b._score - a._score));
    const items = scored.slice(0, limit).map(({ _score, ...c }) => ({
      ...c,
      score: Math.round(_score * 1000) / 1000,
      datasheet_url: c.pdf_uri || c.datasheet_url || c.gcs_pdf_uri || null,
      reason: [
        (base.series && c.series && norm(base.series) === norm(c.series)) ? 'series' : null,
        (base.contact_form && c.contact_form && norm(base.contact_form) === norm(c.contact_form)) ? 'contact_form' : null,
        (norm(base.brand) === norm(c.brand)) ? 'brand' : null,
        (toNum(base.coil_voltage_vdc) != null && toNum(c.coil_voltage_vdc) != null) ? 'coil_voltage_near' : null,
        (toNum(base.dim_l_mm) != null && toNum(c.dim_l_mm) != null) ? 'dims_near' : null,
      ].filter(Boolean)
    }));

    return res.json({ ok: true, base: { brand: base.brand, code: base.code }, items });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

// ---- listen ----
const port = process.env.PORT || 8080;
app.listen(port, () => console.log('worker up on', port));
