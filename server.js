// server.js — Cloud Run Worker (CommonJS)
const express = require('express');
const { Pool } = require('pg');
const { Storage } = require('@google-cloud/storage');
const { VertexAI } = require('@google-cloud/vertexai');

const app = express();
app.use(express.json({ limit: '16mb' }));

// ---------- DB ----------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false,
});

// ---------- GCS ----------
const storage = new Storage();
function parseGcsUri(uri) {
  if (!uri || !uri.startsWith('gs://')) return null;
  const noScheme = uri.slice(5);
  const slash = noScheme.indexOf('/');
  if (slash < 0) return null;
  return { bucket: noScheme.slice(0, slash), name: noScheme.slice(slash + 1) };
}
async function signUrl(gcsUri, minutes = 15) {
  const parsed = parseGcsUri(gcsUri);
  if (!parsed) throw new Error('invalid_gcs_uri');
  const [url] = await storage
    .bucket(parsed.bucket)
    .file(parsed.name)
    .getSignedUrl({
      version: 'v4',
      action: 'read',
      expires: Date.now() + minutes * 60 * 1000,
    });
  return url;
}

// ---------- Vertex (Embedding) ----------
const vertex = new VertexAI({
  project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT_ID,
  location: process.env.VERTEX_LOCATION || 'us-central1',
});
const EMBEDDING_MODEL = process.env.VERTEX_EMBEDDING_MODEL || 'text-embedding-004';
// text → [float]
async function embedText(text) {
  // Vertex Generative API의 embedContent 사용
  const model = vertex.getGenerativeModel({ model: EMBEDDING_MODEL });
  const resp = await model.embedContent({
    content: { parts: [{ text: String(text || '').slice(0, 6000) }] },
  });
  const arr = resp?.embedding?.values || [];
  if (!arr.length) throw new Error('embedding_failed');
  return arr;
}

// ---------- helpers ----------
function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
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
  if (base.series && c.series && norm(base.series) === norm(c.series)) score += 1.0;
  if (base.contact_form && c.contact_form && norm(base.contact_form) === norm(c.contact_form)) score += 0.6;
  if (norm(base.brand) === norm(c.brand)) score += 0.5;
  const bv = toNum(base.coil_voltage_vdc), cv = toNum(c.coil_voltage_vdc);
  if (bv != null && cv != null) {
    const diff = Math.min(Math.abs(bv - cv) / 24.0, 1);
    score += (1 - diff) * 1.2;
  }
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
function buildSpecText(row) {
  const parts = [
    row.brand, row.series, row.code, row.contact_form,
    row.contact_rating_text, row.coil_voltage_vdc,
    `L${row.dim_l_mm} W${row.dim_w_mm} H${row.dim_h_mm}`,
  ].filter(Boolean);
  return parts.join(' · ');
}

// ---------- health ----------
app.get('/_healthz', (_req, res) => res.status(200).send('ok'));
app.get('/api/health', async (_req, res) => {
  try { await pool.query('select 1'); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: String(e) }); }
});

// ---------- ingest (스켈레톤) ----------
app.post('/api/worker/ingest', async (req, res) => {
  try {
    const { gcsPdfUri } = req.body || {};
    if (!gcsPdfUri) return res.status(400).json({ ok:false, error:'gcsPdfUri required' });
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

// ---------- signed URL ----------
app.get('/api/files/signed-url', async (req, res) => {
  try {
    setCORS(res);
    const { gcsUri, minutes } = req.query;
    if (!gcsUri) return res.status(400).json({ ok:false, error:'gcsUri required' });
    const url = await signUrl(String(gcsUri), Math.min(parseInt(minutes || '15',10), 60));
    res.json({ ok:true, url, expires_in_minutes: Math.min(parseInt(minutes || '15',10), 60) });
  } catch(e) {
    console.error(e);
    res.status(500).json({ ok:false, error:String(e) });
  }
});

// ---------- detail ----------
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
    // 우선순위: 명시적 datasheet_url > pdf_uri > gcs_pdf_uri
    const raw = it.datasheet_url || it.pdf_uri || it.gcs_pdf_uri || null;
    let datasheet_url = raw;
    if (raw && String(raw).startsWith('gs://')) {
      try { datasheet_url = await signUrl(raw, 20); } catch { /* 그대로 둠 */ }
    }
    const image_url = it.image_url || it.cover || null;
    res.json({ ok: true, item: it, datasheet_url, image_url });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

// ---------- alternatives (v1 + v2) ----------
// strategy=embedding 이고 embedding 존재하면 pgvector로 근접 이웃 조회,
// 아니면 v1(규칙 점수화)로 대체.
app.get('/api/parts/alternatives', async (req, res) => {
  try {
    setCORS(res);
    const brand = req.query.brand, code = req.query.code;
    const limit = Math.min(parseInt(req.query.limit || '10', 10), 50);
    const strategy = String(req.query.strategy || 'auto'); // auto|embedding|rules
    if (!brand || !code) return res.status(400).json({ ok: false, error: 'brand, code are required' });

    // 기준 부품
    const baseSql = `
      SELECT id, brand, series, code, contact_form, coil_voltage_vdc,
             dim_l_mm, dim_w_mm, dim_h_mm, pdf_uri, datasheet_url, gcs_pdf_uri,
             embedding, updated_at
      FROM public.relay_specs
      WHERE lower(trim(brand)) = lower(trim($1))
        AND lower(trim(code))  = lower(trim($2))
      ORDER BY updated_at DESC NULLS LAST
      LIMIT 1
    `;
    const base = await pool.query(baseSql, [brand, code]).then(r => r.rows[0]);
    if (!base) return res.status(404).json({ ok: false, error: 'not_found' });

    const useEmbedding = (strategy === 'embedding') || (strategy === 'auto' && base.embedding);
    if (useEmbedding && base.embedding) {
      // v2: pgvector 근접 이웃 (cosine distance)
      const q = `
        SELECT id, brand, series, code, contact_form, coil_voltage_vdc,
               dim_l_mm, dim_w_mm, dim_h_mm, pdf_uri, datasheet_url, gcs_pdf_uri,
               1 - (embedding <=> $1::vector) AS score
        FROM public.relay_specs
        WHERE NOT (lower(trim(brand)) = lower(trim($2)) AND lower(trim(code)) = lower(trim($3)))
          AND embedding IS NOT NULL
        ORDER BY embedding <=> $1::vector
        LIMIT $4
      `;
      const { rows } = await pool.query(q, [base.embedding, brand, code, limit]);
      const items = rows.map(r => ({
        ...r,
        score: Math.round((Number(r.score) || 0) * 1000) / 1000,
        datasheet_url: r.datasheet_url || r.pdf_uri || r.gcs_pdf_uri || null,
        reason: ['embedding'],
      }));
      return res.json({ ok: true, base: { brand: base.brand, code: base.code }, items });
    }

    // v1: 규칙 기반
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
    const scored = candidates.map(c => ({ ...c, _score: scoreCandidate(base, c) }));
    scored.sort((a, b) => (b._score - a._score));
    const items = scored.slice(0, limit).map(({ _score, ...c }) => ({
      ...c,
      score: Math.round(_score * 1000) / 1000,
      datasheet_url: c.datasheet_url || c.pdf_uri || c.gcs_pdf_uri || null,
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

// ---------- embedding backfill ----------
app.post('/api/embedding/backfill', async (req, res) => {
  try {
    setCORS(res);
    const limit = Math.min(parseInt(req.query.limit || '200', 10), 1000);
    // (1) 아직 임베딩 없는 행 조회
    const { rows } = await pool.query(`
      SELECT id, brand, series, code, contact_form, contact_rating_text,
             coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm
      FROM public.relay_specs
      WHERE embedding IS NULL
      ORDER BY updated_at DESC NULLS LAST
      LIMIT $1
    `, [limit]);
    if (!rows.length) return res.json({ ok:true, updated: 0 });

    // (2) 임베딩 생성 → 업데이트
    let updated = 0;
    for (const r of rows) {
      const text = buildSpecText(r);
      const vec = await embedText(text);
      // vec(float[]) → Postgres vector 리터럴로 변환: '[x,y,...]'
      const literal = '[' + vec.map(v => (typeof v === 'number' ? v.toFixed(6) : '0')).join(',') + ']';
      await pool.query(`UPDATE public.relay_specs SET embedding = $1::vector WHERE id = $2`, [literal, r.id]);
      updated++;
    }
    res.json({ ok:true, updated });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok:false, error:String(e) });
  }
});

// ---------- search (v2: 임베딩 + 어휘 혼합) ----------
app.get('/api/parts/search', async (req, res) => {
  try {
    setCORS(res);
    const q = String(req.query.q || '').trim();
    const brandFilter = req.query.brand ? String(req.query.brand) : null;
    const limit = Math.min(parseInt(req.query.limit || '20', 10), 100);
    if (!q) return res.status(400).json({ ok:false, error:'q required' });

    let vectorLiteral = null;
    try {
      const vec = await embedText(q);
      vectorLiteral = '[' + vec.map(v => (typeof v === 'number' ? v.toFixed(6) : '0')).join(',') + ']';
    } catch {
      // 임베딩 실패 시 어휘만
    }

    // 임베딩 경로
    if (vectorLiteral) {
      const sql = `
        SELECT id, brand, series, code, contact_form, coil_voltage_vdc,
               dim_l_mm, dim_w_mm, dim_h_mm, datasheet_url, pdf_uri, gcs_pdf_uri,
               1 - (embedding <=> $1::vector) AS score
        FROM public.relay_specs
        WHERE embedding IS NOT NULL
          ${brandFilter ? 'AND lower(trim(brand)) = lower(trim($2))' : ''}
        ORDER BY embedding <=> $1::vector
        LIMIT ${limit}
      `;
      const params = brandFilter ? [vectorLiteral, brandFilter] : [vectorLiteral];
      const { rows } = await pool.query(sql, params);
      const items = rows.map(r => ({
        ...r,
        score: Math.round((Number(r.score)||0)*1000)/1000,
        datasheet_url: r.datasheet_url || r.pdf_uri || r.gcs_pdf_uri || null,
      }));
      return res.json({ ok:true, items, mode:'embedding' });
    }

    // 어휘 경로(폴백)
    const like = `%${q}%`;
    const sql = `
      SELECT id, brand, series, code, contact_form, coil_voltage_vdc,
             dim_l_mm, dim_w_mm, dim_h_mm, datasheet_url, pdf_uri, gcs_pdf_uri
      FROM public.relay_specs
      WHERE (brand ILIKE $1 OR code ILIKE $1 OR series ILIKE $1 OR contact_rating_text ILIKE $1)
        ${brandFilter ? 'AND lower(trim(brand)) = lower(trim($2))' : ''}
      ORDER BY updated_at DESC NULLS LAST
      LIMIT ${limit}
    `;
    const params = brandFilter ? [like, brandFilter] : [like];
    const { rows } = await pool.query(sql, params);
    const items = rows.map(r => ({
      ...r,
      score: null,
      datasheet_url: r.datasheet_url || r.pdf_uri || r.gcs_pdf_uri || null,
    }));
    return res.json({ ok:true, items, mode:'lexical' });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  }
});

// ---------- listen ----------
const port = process.env.PORT || 8080;
app.listen(port, () => console.log('worker up on', port));
