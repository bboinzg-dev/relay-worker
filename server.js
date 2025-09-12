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
async function embedText(text) {
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
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
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

// ---------- alternatives (rules + embedding) ----------
app.get('/api/parts/alternatives', async (req, res) => {
  try {
    setCORS(res);
    const brand = req.query.brand, code = req.query.code;
    const limit = Math.min(parseInt(req.query.limit || '10', 10), 50);
    const strategy = String(req.query.strategy || 'auto'); // auto|embedding|rules
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

// ---------- embedding backfill ----------
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

// ---------- search (embedding + lexical) ----------
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
   ========== 거래 API ==========
   =========================== */

/** Listings (정찰제 재고) */
// GET /api/listings?brand=&code=&q=&status=active&limit=20
app.get('/api/listings', async (req, res) => {
  try {
    setCORS(res);
    const q = String(req.query.q || '').trim();
    const brand = req.query.brand ? String(req.query.brand) : null;
    const code = req.query.code ? String(req.query.code) : null;
    const status = req.query.status ? String(req.query.status) : 'active';
    const limit = Math.min(parseInt(req.query.limit || '20', 10), 100);

    const params = [];
    let where = 'WHERE 1=1';
    if (status) { params.push(status); where += ` AND status = $${params.length}`; }
    if (brand)  { params.push(brand);  where += ` AND lower(trim(brand)) = lower(trim($${params.length}))`; }
    if (code)   { params.push(code);   where += ` AND lower(trim(code))  = lower(trim($${params.length}))`; }
    if (q)      { params.push(`%${q}%`); where += ` AND (brand ILIKE $${params.length} OR code ILIKE $${params.length} OR series ILIKE $${params.length})`; }

    const sql = `
      SELECT *
      FROM public.listings
      ${where}
      ORDER BY updated_at DESC NULLS LAST
      LIMIT ${limit}
    `;
    const { rows } = await pool.query(sql, params);
    res.json({ ok:true, items: rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  }
});

// POST /api/listings
app.post('/api/listings', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const {
      seller_id, brand, code, series,
      quantity_available, unit_price, currency,
      lead_time_days, moq, pack_qty, note,
      datasheet_url, gcs_pdf_uri, status
    } = req.body || {};

    if (!seller_id || !brand || !code || quantity_available == null || unit_price == null) {
      return res.status(400).json({ ok:false, error:'seller_id, brand, code, quantity_available, unit_price required' });
    }

    await client.query('BEGIN');
    const { rows } = await client.query(`
      INSERT INTO public.listings
        (seller_id, brand, code, series, quantity_available, unit_price, currency,
         lead_time_days, moq, pack_qty, note, datasheet_url, gcs_pdf_uri, status)
      VALUES
        ($1,$2,$3,$4,$5,$6,COALESCE($7,'USD'),
         $8,$9,$10,$11,$12,$13,COALESCE($14,'active'))
      RETURNING *
    `, [seller_id, brand, code, series || null, quantity_available, unit_price, currency || null,
        lead_time_days || null, moq || null, pack_qty || null, note || null, datasheet_url || null, gcs_pdf_uri || null, status || null]);
    await client.query('COMMIT');
    res.json({ ok:true, item: rows[0] });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  } finally {
    client.release();
  }
});

// PATCH /api/listings/:id
app.patch('/api/listings/:id', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const id = parseInt(req.params.id, 10);
    if (!id) return res.status(400).json({ ok:false, error:'invalid id' });

    const allowed = ['series','quantity_available','unit_price','currency','lead_time_days','moq','pack_qty','note','datasheet_url','gcs_pdf_uri','status'];
    const fields = [];
    const params = [];
    let i = 1;
    for (const k of allowed) {
      if (k in (req.body || {})) {
        fields.push(`${k} = $${i++}`);
        params.push(req.body[k]);
      }
    }
    if (!fields.length) return res.status(400).json({ ok:false, error:'no fields' });
    params.push(id);

    await client.query('BEGIN');
    const { rows } = await client.query(`UPDATE public.listings SET ${fields.join(', ')}, updated_at = now() WHERE id = $${i} RETURNING *`, params);
    await client.query('COMMIT');
    res.json({ ok:true, item: rows[0] });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  } finally {
    client.release();
  }
});

// POST /api/listings/:id/purchase   { buyer_id, quantity }
app.post('/api/listings/:id/purchase', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const id = parseInt(req.params.id, 10);
    const { buyer_id, quantity } = req.body || {};
    if (!id || !buyer_id || !quantity || quantity <= 0) {
      return res.status(400).json({ ok:false, error:'id, buyer_id, quantity required' });
    }

    await client.query('BEGIN');
    const { rows: Ls } = await client.query('SELECT * FROM public.listings WHERE id = $1 FOR UPDATE', [id]);
    if (!Ls.length) { await client.query('ROLLBACK'); return res.status(404).json({ ok:false, error:'listing_not_found' });}
    const L = Ls[0];
    if (L.status !== 'active') { await client.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'listing_inactive' });}
    if (quantity > L.quantity_available) { await client.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'insufficient_stock' });}

    const { rows: Os } = await client.query(`
      INSERT INTO public.orders (listing_id, buyer_id, seller_id, quantity, unit_price, currency, lead_time_days, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'placed') RETURNING *
    `, [L.id, buyer_id, L.seller_id, quantity, L.unit_price, L.currency, L.lead_time_days || null]);

    await client.query('UPDATE public.listings SET quantity_available = quantity_available - $1, updated_at = now() WHERE id = $2', [quantity, id]);

    await client.query('COMMIT');
    res.json({ ok:true, order: Os[0], remaining: L.quantity_available - quantity });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  } finally {
    client.release();
  }
});

/** Purchase Requests (구매요청) */
// GET /api/purchase-requests?status=open&brand=&code=&limit=20
app.get('/api/purchase-requests', async (req, res) => {
  try {
    setCORS(res);
    const status = req.query.status ? String(req.query.status) : null;
    const brand = req.query.brand ? String(req.query.brand) : null;
    const code = req.query.code ? String(req.query.code) : null;
    const limit = Math.min(parseInt(req.query.limit || '20', 10), 100);

    const params = [];
    let where = 'WHERE 1=1';
    if (status) { params.push(status); where += ` AND status = $${params.length}`; }
    if (brand)  { params.push(brand);  where += ` AND lower(trim(brand)) = lower(trim($${params.length}))`; }
    if (code)   { params.push(code);   where += ` AND lower(trim(code))  = lower(trim($${params.length}))`; }

    const sql = `
      SELECT *, (quantity_total - quantity_confirmed) AS quantity_outstanding
      FROM public.purchase_requests
      ${where}
      ORDER BY created_at DESC
      LIMIT ${limit}
    `;
    const { rows } = await pool.query(sql, params);
    res.json({ ok:true, items: rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  }
});

// POST /api/purchase-requests
app.post('/api/purchase-requests', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const {
      buyer_id, brand, code, family,
      quantity_total, due_date, allow_alternatives, notes
    } = req.body || {};
    if (!buyer_id || !quantity_total) {
      return res.status(400).json({ ok:false, error:'buyer_id, quantity_total required' });
    }

    await client.query('BEGIN');
    const { rows } = await client.query(`
      INSERT INTO public.purchase_requests
        (buyer_id, brand, code, family, quantity_total, due_date, allow_alternatives, notes, status)
      VALUES
        ($1,$2,$3,$4,$5,$6,COALESCE($7,true),$8,'open')
      RETURNING *, (quantity_total - quantity_confirmed) AS quantity_outstanding
    `, [buyer_id, brand || null, code || null, family || null, quantity_total, due_date || null, allow_alternatives, notes || null]);
    await client.query('COMMIT');
    res.json({ ok:true, item: rows[0] });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  } finally {
    client.release();
  }
});

// GET /api/purchase-requests/:id (상세 + bids)
app.get('/api/purchase-requests/:id', async (req, res) => {
  try {
    setCORS(res);
    const id = parseInt(req.params.id, 10);
    if (!id) return res.status(400).json({ ok:false, error:'invalid id' });
    const { rows: PRs } = await pool.query(`
      SELECT *, (quantity_total - quantity_confirmed) AS quantity_outstanding
      FROM public.purchase_requests WHERE id = $1
    `, [id]);
    if (!PRs.length) return res.status(404).json({ ok:false, error:'not_found' });
    const { rows: BIDs } = await pool.query(`SELECT * FROM public.bids WHERE pr_id = $1 ORDER BY created_at DESC`, [id]);
    const { rows: FILLS } = await pool.query(`SELECT * FROM public.request_fills WHERE pr_id = $1 ORDER BY created_at DESC`, [id]);
    res.json({ ok:true, pr: PRs[0], bids: BIDs, fills: FILLS });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  }
});

/** Bids (입찰) */
// GET /api/bids?pr_id=...
app.get('/api/bids', async (req, res) => {
  try {
    setCORS(res);
    const pr_id = req.query.pr_id ? parseInt(String(req.query.pr_id), 10) : null;
    if (!pr_id) return res.status(400).json({ ok:false, error:'pr_id required' });
    const { rows } = await pool.query(`SELECT * FROM public.bids WHERE pr_id = $1 ORDER BY created_at DESC`, [pr_id]);
    res.json({ ok:true, items: rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  }
});

// POST /api/bids
app.post('/api/bids', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const {
      pr_id, seller_id, brand, code, is_alternative,
      offer_quantity, unit_price, currency, lead_time_days,
      listing_id, expires_at
    } = req.body || {};

    if (!pr_id || !seller_id || !offer_quantity || !unit_price) {
      return res.status(400).json({ ok:false, error:'pr_id, seller_id, offer_quantity, unit_price required' });
    }

    await client.query('BEGIN');
    const { rows: PRs } = await client.query('SELECT * FROM public.purchase_requests WHERE id = $1 FOR UPDATE', [pr_id]);
    if (!PRs.length) { await client.query('ROLLBACK'); return res.status(404).json({ ok:false, error:'pr_not_found' });}

    const { rows } = await client.query(`
      INSERT INTO public.bids
      (pr_id, seller_id, brand, code, is_alternative, offer_quantity, unit_price, currency, lead_time_days, listing_id, expires_at, status)
      VALUES ($1,$2,$3,$4,COALESCE($5,false),$6,$7,COALESCE($8,'USD'),$9,$10,$11,'proposed')
      RETURNING *
    `, [pr_id, seller_id, brand || null, code || null, is_alternative, offer_quantity, unit_price, currency || null, lead_time_days || null, listing_id || null, expires_at || null]);
    await client.query('COMMIT');
    res.json({ ok:true, item: rows[0] });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  } finally {
    client.release();
  }
});

/** Confirm(바이어가 특정 입찰 일부 또는 전체 채택) */
// POST /api/purchase-requests/:id/confirm  { bid_id, buyer_id, quantity }
app.post('/api/purchase-requests/:id/confirm', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const pr_id = parseInt(req.params.id, 10);
    const { bid_id, buyer_id, quantity } = req.body || {};
    if (!pr_id || !bid_id || !buyer_id || !quantity || quantity <= 0) {
      return res.status(400).json({ ok:false, error:'pr_id, bid_id, buyer_id, quantity required' });
    }

    await client.query('BEGIN');

    // PR 락 + 검증
    const { rows: PRs } = await client.query('SELECT * FROM public.purchase_requests WHERE id = $1 FOR UPDATE', [pr_id]);
    if (!PRs.length) { await client.query('ROLLBACK'); return res.status(404).json({ ok:false, error:'pr_not_found' });}
    const PR = PRs[0];
    const outstanding = Number(PR.quantity_total) - Number(PR.quantity_confirmed);
    if (quantity > outstanding) { await client.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'quantity_exceeds_outstanding' });}

    // BID 락 + 검증
    const { rows: BIDs } = await client.query('SELECT * FROM public.bids WHERE id = $1 AND pr_id = $2 FOR UPDATE', [bid_id, pr_id]);
    if (!BIDs.length) { await client.query('ROLLBACK'); return res.status(404).json({ ok:false, error:'bid_not_found' });}
    const BID = BIDs[0];
    if (BID.status !== 'proposed' && BID.status !== 'partial') {
      await client.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'bid_not_open' });
    }
    if (quantity > Number(BID.offer_quantity)) {
      await client.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'quantity_exceeds_offer' });
    }

    // 재고(리스트) 연결된 제안이면 재고 차감
    if (BID.listing_id) {
      const { rows: Ls } = await client.query('SELECT * FROM public.listings WHERE id = $1 FOR UPDATE', [BID.listing_id]);
      if (!Ls.length) { await client.query('ROLLBACK'); return res.status(404).json({ ok:false, error:'listing_not_found' });}
      const L = Ls[0];
      if (quantity > Number(L.quantity_available)) {
        await client.query('ROLLBACK'); return res.status(400).json({ ok:false, error:'insufficient_stock' });
      }
      await client.query('UPDATE public.listings SET quantity_available = quantity_available - $1, updated_at = now() WHERE id = $2', [quantity, BID.listing_id]);
    }

    // fill 생성
    const { rows: FILLs } = await client.query(`
      INSERT INTO public.request_fills
      (pr_id, bid_id, buyer_id, seller_id, listing_id, quantity, unit_price, currency, lead_time_days, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'committed')
      RETURNING *
    `, [pr_id, bid_id, buyer_id, BID.seller_id, BID.listing_id || null, quantity, BID.unit_price, BID.currency, BID.lead_time_days || null]);

    // PR 집계 갱신
    const newConfirmed = Number(PR.quantity_confirmed) + quantity;
    const newStatus = newConfirmed >= Number(PR.quantity_total) ? 'filled' : (newConfirmed > 0 ? 'partial' : PR.status);
    await client.query('UPDATE public.purchase_requests SET quantity_confirmed = $1, status = $2, updated_at = now() WHERE id = $3', [newConfirmed, newStatus, pr_id]);

    // BID 상태 갱신(부분/완료)
    const remainingInBid = Number(BID.offer_quantity) - quantity;
    const bidStatus = remainingInBid > 0 ? 'partial' : 'accepted';
    await client.query('UPDATE public.bids SET offer_quantity = $1, status = $2, updated_at = now() WHERE id = $3', [remainingInBid, bidStatus, bid_id]);

    await client.query('COMMIT');
    res.json({ ok:true, fill: FILLs[0], pr_status: newStatus, pr_confirmed: newConfirmed, bid_remaining: remainingInBid });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  } finally {
    client.release();
  }
});

/** BOM import (구매요청 일괄 생성)
 * POST /api/bom/import
 * body: { buyer_id: string, items: [{brand?, code?, family?, quantity_total, due_date?, allow_alternatives?, notes?}] }
 */
app.post('/api/bom/import', async (req, res) => {
  const client = await pool.connect();
  try {
    setCORS(res);
    const { buyer_id, items } = req.body || {};
    if (!buyer_id || !Array.isArray(items) || !items.length) {
      return res.status(400).json({ ok:false, error:'buyer_id and items[] required' });
    }
    await client.query('BEGIN');
    const created = [];
    for (const it of items) {
      if (!it.quantity_total) continue;
      const { rows } = await client.query(`
        INSERT INTO public.purchase_requests
        (buyer_id, brand, code, family, quantity_total, due_date, allow_alternatives, notes, status)
        VALUES ($1,$2,$3,$4,$5,$6,COALESCE($7,true),$8,'open')
        RETURNING id
      `, [buyer_id, it.brand || null, it.code || null, it.family || null, it.quantity_total, it.due_date || null, it.allow_alternatives, it.notes || null]);
      created.push(rows[0].id);
    }
    await client.query('COMMIT');
    res.json({ ok:true, created_pr_ids: created });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error(e);
    res.status(500).json({ ok:false, error:'internal_error' });
  } finally {
    client.release();
  }
});

// ---------- listen ----------
const port = process.env.PORT || 8080;
app.listen(port, () => console.log('worker up on', port));
