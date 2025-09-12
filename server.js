// server.js — Cloud Run Worker (CommonJS)
const express = require('express');
const { Pool } = require('pg');
const { Storage } = require('@google-cloud/storage');
const { VertexAI } = require('@google-cloud/vertexai');

const app = express();
app.use(express.json({ limit: '32mb' })); // 사진 base64 대비

/* ===========================
   ========== DB =============
   =========================== */
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false,
});

/* ===========================
   ========== GCS ============
   =========================== */
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

/* ===========================
   ====== Vertex (Embed) =====
   =========================== */
const vertex = new VertexAI({
  project: process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT_ID,
  location: process.env.VERTEX_LOCATION || 'us-central1',
});
const TEXT_EMBED_MODEL = process.env.VERTEX_EMBEDDING_MODEL || 'text-embedding-004';
const IMAGE_EMBED_MODEL = process.env.VERTEX_IMAGE_EMBEDDING_MODEL || 'multimodalembedding@001';

// text → [float]
async function embedText(text) {
  const model = vertex.getGenerativeModel({ model: TEXT_EMBED_MODEL });
  const resp = await model.embedContent({
    content: { parts: [{ text: String(text || '').slice(0, 6000) }] },
  });
  const arr = resp?.embedding?.values || [];
  if (!arr.length) throw new Error('embedding_failed');
  return arr;
}

// image(fileUri | base64) → [float]
async function embedImage({ gcsUri, base64, mimeType }) {
  const model = vertex.getGenerativeModel({ model: IMAGE_EMBED_MODEL });
  let parts;
  if (gcsUri) {
    parts = [{ fileData: { fileUri: gcsUri, mimeType: mimeType || 'image/png' } }];
  } else if (base64) {
    parts = [{ inlineData: { data: base64, mimeType: mimeType || 'image/png' } }];
  } else {
    throw new Error('image_required');
  }
  const resp = await model.embedContent({ content: { parts } });
  const arr = resp?.embedding?.values || [];
  if (!arr.length) throw new Error('image_embedding_failed');
  return arr;
}

// vec(float[]) → Postgres vector 리터럴
function vecLiteral(arr) {
  return '[' + (arr || []).map(v => (typeof v === 'number' ? v.toFixed(6) : '0')).join(',') + ']';
}

/* ===========================
   ======= helpers ===========
   =========================== */
function setCORS(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}
const norm = s => (s ?? '').toString().trim().toLowerCase();
const toNum = x => {
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

/* ===========================
   ========= health ==========
   =========================== */
app.get('/_healthz', (_req, res) => res.status(200).send('ok'));
app.get('/api/health', async (_req, res) => {
  try { await pool.query('select 1'); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: String(e) }); }
});

/* ===========================
   ========= ingest ==========
   =========================== */
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

/* ===========================
   ======== files: URL =======
   =========================== */
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

/* ===========================
   ======= parts detail ======
   =========================== */
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
    const raw = it.datasheet_url || it.pdf_uri || it.gcs_pdf_uri || null;
    let datasheet_url = raw;
    if (raw && String(raw).startsWith('gs://')) {
      try { datasheet_url = await signUrl(raw, 20); } catch {}
    }
    const image_url = it.image_url || it.cover || null;
    res.json({ ok: true, item: it, datasheet_url, image_url });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: 'internal_error' });
  }
});

/* ===========================
   ===== alternatives ========
   =========================== */
app.get('/api/parts/alternatives', async (req, res) => {
  try {
    setCORS(res);
    const brand = req.query.brand, code = req.query.code;
    const limit = Math.min(parseInt(req.query.limit || '10', 10), 50);
    const strategy = String(req.query.strategy || 'auto');
    if (!brand || !code) return res.status(400).json({ ok: false, error: 'brand, code are required' });

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

/* ===========================
   ===== embedding utils =====
   =========================== */
app.post('/api/embedding/backfill', async (req, res) => {
  try {
    setCORS(res);
    const limit = Math.min(parseInt(req.query.limit || '200', 10), 1000);
    const { rows } = await pool.query(`
      SELECT id, brand, series, code, contact_form, contact_rating_text,
             coil_voltage_vdc, dim_l_mm, dim_w_mm, dim_h_mm
      FROM public.relay_specs
      WHERE embedding IS NULL
      ORDER BY updated_at DESC NULLS LAST
      LIMIT $1
    `, [limit]);
    if (!rows.length) return res.json({ ok:true, updated: 0 });

    let updated = 0;
    for (const r of rows) {
      const text = buildSpecText(r);
      const vec = await embedText(text);
      await pool.query(`UPDATE public.relay_specs SET embedding = $1::vector WHERE id = $2`, [vecLiteral(vec), r.id]);
      updated++;
    }
    res.json({ ok:true, updated });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok:false, error:String(e) });
  }
});

/* ===========================
   ========== search =========
   =========================== */
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
      vectorLiteral = vecLiteral(vec);
    } catch {}

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

/* ===========================
   ======== 거래 API =========
   =========================== */
/** (생략 없이 그대로 유지) listings / purchase-requests / bids / confirm / bom.import **/
/* …… 기존 PRO-2에서 제공한 거래 API 블록을 그대로 여기에 유지하세요 …… */

/* ===========================
   ==== Vision: index/ident ===
   =========================== */

/** POST /api/vision/index
 * body: { brand:string, code:string, gcsImageUri?:string, imageBase64?:string, mimeType?:string, imageUrl?:string }
 * 결과: 이미지 임베딩 생성 → image_index insert
 */
app.post('/api/vision/index', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const { brand, code, gcsImageUri, imageBase64, mimeType, imageUrl } = req.body || {};
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand, code required' });
    if (!gcsImageUri && !imageBase64 && !imageUrl) return res.status(400).json({ ok:false, error:'one of gcsImageUri/imageBase64/imageUrl required' });

    // imageUrl이 있으면 fetch하여 base64로 변환
    let base64 = imageBase64 || null;
    if (!gcsImageUri && imageUrl && !base64) {
      const resp = await fetch(imageUrl);
      if (!resp.ok) throw new Error('fetch_image_failed');
      const buf = Buffer.from(await resp.arrayBuffer());
      base64 = buf.toString('base64');
    }

    const vec = await embedImage({ gcsUri: gcsImageUri, base64, mimeType: mimeType || 'image/png' });
    await client.query('BEGIN');
    const { rows } = await client.query(`
      INSERT INTO public.image_index (brand, code, gcs_image_uri, image_url, embedding)
      VALUES ($1,$2,$3,$4,$5::vector)
      RETURNING *
    `, [brand, code, gcsImageUri || null, imageUrl || null, vecLiteral(vec)]);
    await client.query('COMMIT');
    res.json({ ok:true, item: rows[0] });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:String(e) });
  } finally {
    client.release();
  }
});

/** POST /api/vision/identify
 * body: { gcsImageUri?:string, imageBase64?:string, mimeType?:string, topK?:number, brand?:string }
 * 결과: 이미지 임베딩 → image_index 근접 탐색 → relay_specs 조인하여 후보 반환
 */
app.post('/api/vision/identify', async (req, res) => {
  try {
    setCORS(res);
    const { gcsImageUri, imageBase64, mimeType, topK, brand } = req.body || {};
    if (!gcsImageUri && !imageBase64) return res.status(400).json({ ok:false, error:'gcsImageUri or imageBase64 required' });

    const vec = await embedImage({ gcsUri: gcsImageUri, base64: imageBase64, mimeType: mimeType || 'image/png' });
    const K = Math.min(parseInt(topK || '8', 10), 50);

    // image_index 근접 → relay_specs 조인
    const sql = `
      SELECT
        ii.brand, ii.code, ii.gcs_image_uri, ii.image_url,
        1 - (ii.embedding <=> $1::vector) AS score,
        rs.series, rs.contact_form, rs.coil_voltage_vdc,
        rs.dim_l_mm, rs.dim_w_mm, rs.dim_h_mm,
        COALESCE(rs.datasheet_url, rs.pdf_uri, rs.gcs_pdf_uri) AS datasheet_url
      FROM public.image_index ii
      LEFT JOIN public.relay_specs rs
        ON lower(trim(rs.brand)) = lower(trim(ii.brand))
       AND lower(trim(rs.code))  = lower(trim(ii.code))
      WHERE 1=1
        ${brand ? 'AND lower(trim(ii.brand)) = lower(trim($2))' : ''}
      ORDER BY ii.embedding <=> $1::vector
      LIMIT ${K}
    `;
    const params = brand ? [vecLiteral(vec), brand] : [vecLiteral(vec)];
    const { rows } = await pool.query(sql, params);
    const items = rows.map(r => ({
      brand: r.brand, code: r.code,
      score: Math.round((Number(r.score)||0)*1000)/1000,
      series: r.series, contact_form: r.contact_form,
      coil_voltage_vdc: r.coil_voltage_vdc,
      dim_l_mm: r.dim_l_mm, dim_w_mm: r.dim_w_mm, dim_h_mm: r.dim_h_mm,
      datasheet_url: r.datasheet_url,
      image_url: r.gcs_image_uri || r.image_url || null
    }));
    res.json({ ok:true, items, mode:'image-embedding' });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok:false, error:String(e) });
  }
});

/** (옵션) 기존 스펙의 cover/image_url에서 백필 */
app.post('/api/vision/backfill-from-specs', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);

    const { rows } = await client.query(`
      SELECT brand, code, image_url, cover
      FROM public.relay_specs
      WHERE (image_url IS NOT NULL OR cover IS NOT NULL)
      ORDER BY updated_at DESC NULLS LAST
      LIMIT $1
    `, [limit]);

    let inserted = 0;
    await client.query('BEGIN');
    for (const r of rows) {
      const brand = r.brand, code = r.code;
      const src = r.image_url || r.cover;
      if (!src) continue;

      // 이미 같은 brand/code로 인덱스가 있으면 skip (느슨한 중복 회피)
      const { rows: exists } = await client.query(`
        SELECT 1 FROM public.image_index
        WHERE lower(trim(brand)) = lower(trim($1))
          AND lower(trim(code))  = lower(trim($2))
        LIMIT 1
      `, [brand, code]);
      if (exists.length) continue;

      let gcs = null, base64 = null, mime = 'image/png', httpUrl = null;
      if (String(src).startsWith('gs://')) {
        gcs = src;
      } else if (/^https?:\/\//i.test(String(src))) {
        httpUrl = src;
        try {
          const resp = await fetch(httpUrl);
          if (resp.ok) {
            const buf = Buffer.from(await resp.arrayBuffer());
            base64 = buf.toString('base64');
            const ct = resp.headers.get('content-type');
            if (ct && /^image\//.test(ct)) mime = ct;
          }
        } catch { /* skip on error */ }
      } else {
        // skip: 알 수 없는 포맷
        continue;
      }

      try {
        const vec = await embedImage({ gcsUri: gcs, base64, mimeType: mime });
        await client.query(`
          INSERT INTO public.image_index (brand, code, gcs_image_uri, image_url, embedding)
          VALUES ($1,$2,$3,$4,$5::vector)
        `, [brand, code, gcs || null, httpUrl || null, vecLiteral(vec)]);
        inserted++;
      } catch (err) {
        // 개별 오류는 건너뜀
      }
    }
    await client.query('COMMIT');
    res.json({ ok:true, inserted });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:String(e) });
  } finally {
    client.release();
  }
});

/* ===========================
   ========= listen ==========
   =========================== */
const port = process.env.PORT || 8080;
app.listen(port, () => console.log('worker up on', port));
