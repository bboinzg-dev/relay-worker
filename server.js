const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');
const db = require('./src/utils/db');
const { getSignedUrl, canonicalDatasheetPath, moveObject } = require('./src/utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('./src/utils/schema');
const { runAutoIngest } = require('./src/pipeline/ingestAuto');
const { parseActor } = (()=>{ try { return require('./src/utils/auth'); } catch { return { parseActor: ()=>({}) }; } })();
const { notify, findFamilyForBrandCode } = (()=>{ try { return require('./src/utils/notify'); } catch { return { notify: async()=>({}), findFamilyForBrandCode: async()=>null }; } })();

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true }));
const upload = multer({ storage: multer.memoryStorage() });

const PORT = process.env.PORT || 8080;
const GCS_BUCKET_URI = process.env.GCS_BUCKET || '';
const GCS_BUCKET = GCS_BUCKET_URI.startsWith('gs://') ? GCS_BUCKET_URI.replace(/^gs:\/\//,'').split('/')[0] : '';

// --- health/env ---
app.get('/_healthz', (req, res) => res.type('text/plain').send('ok'));
app.get('/_env', (req, res) => {
  res.json({
    node: process.version,
    gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null,
    has_db: !!process.env.DATABASE_URL,
  });
});

// --- catalog registry / blueprint ---
app.get('/catalog/registry', async (req, res) => {
  try {
    const r = await db.query(`SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`);
    res.json({ items: r.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'registry failed' });
  }
});

app.get('/catalog/blueprint/:family', async (req, res) => {
  const family = req.params.family;
  try {
    const r = await db.query(`
      SELECT b.family_slug, r.specs_table, b.fields_json, b.prompt_template
      FROM public.component_spec_blueprint b
      JOIN public.component_registry r USING (family_slug)
      WHERE b.family_slug = $1
      LIMIT 1
    `, [family]);
  if (!r.rows.length) return res.status(404).json({ error: 'blueprint not found' });
    res.json(r.rows[0]);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'blueprint failed' });
  }
});

// --- files: signed URL ---
app.get('/api/files/signed-url', async (req, res) => {
  try {
    const gcsUri = req.query.gcsUri;
    const minutes = Number(req.query.minutes || 15);
    const url = await getSignedUrl(gcsUri, minutes, 'read');
    res.json({ url });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// --- files: move to canonical datasheet path ---
app.post('/api/files/move', async (req, res) => {
  try {
    const { srcGcsUri, family_slug, brand, code, dstGcsUri } = req.body || {};
    if (!srcGcsUri) return res.status(400).json({ error: 'srcGcsUri required' });
    const dst = dstGcsUri || canonicalDatasheetPath(GCS_BUCKET, family_slug, brand, code);
    const moved = await moveObject(srcGcsUri, dst);
    res.json({ ok: true, dstGcsUri: moved });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// --- parts: simple search/detail/alternatives (v1) ---
app.get('/parts/search', async (req, res) => {
  const q = (req.query.q || '').toString().trim();
  const limit = Math.min(Number(req.query.limit || 20), 100);
  try {
    const text = q ? `%${q.toLowerCase()}%` : '%';
    const rows = await db.query(
      `SELECT * FROM public.relay_specs
       WHERE brand_norm LIKE $1 OR code_norm LIKE $1 OR lower(series) LIKE $1 OR lower(display_name) LIKE $1
       ORDER BY updated_at DESC
       LIMIT $2`,
      [text, limit]
    );
    res.json({ items: rows.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'search failed' });
  }
});

app.get('/parts/detail', async (req, res) => {
  const brand = (req.query.brand || '').toString();
  const code = (req.query.code || '').toString();
  if (!brand || !code) return res.status(400).json({ error: 'brand & code required' });
  try {
    const row = await db.query(
      `SELECT * FROM public.relay_specs WHERE brand_norm=lower($1) AND code_norm=lower($2) LIMIT 1`,
      [brand, code]
    );
    if (!row.rows.length) return res.status(404).json({ error: 'not found' });
    res.json(row.rows[0]);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'detail failed' });
  }
});

app.get('/parts/alternatives', async (req, res) => {
  const brand = (req.query.brand || '').toString();
  const code = (req.query.code || '').toString();
  const limit = Math.min(Number(req.query.limit || 10), 50);
  if (!brand || !code) return res.status(400).json({ error: 'brand & code required' });
  try {
    const base = await db.query(
      `SELECT * FROM public.relay_specs WHERE brand_norm=lower($1) AND code_norm=lower($2) LIMIT 1`,
      [brand, code]
    );
    if (!base.rows.length) return res.status(404).json({ error: 'base not found' });
    const b = base.rows[0];
    const rows = await db.query(
      `SELECT *,
        (CASE WHEN family_slug IS NOT NULL AND family_slug = $1 THEN 0 ELSE 1 END) * 1.0 +
        COALESCE(ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($2::numeric,0)) / 100.0, 1.0) AS score
      FROM public.relay_specs
      WHERE NOT (brand_norm=lower($3) AND code_norm=lower($4))
      ORDER BY score ASC
      LIMIT $5`,
      [b.family_slug || null, b.coil_voltage_vdc || null, brand, code, limit]
    );
    res.json({ base: b, items: rows.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'alternatives failed' });
  }
});

// --- ingest: manual / bulk / auto ---
app.post('/ingest', async (req, res) => {
  try {
    const {
      family_slug,
      specs_table, brand, code, series, display_name,
      datasheet_url, cover, source_gcs_uri, raw_json=null,
      fields = {}, values = {},
    } = req.body || {};

    const table = (specs_table || `${family_slug}_specs`).replace(/[^a-zA-Z0-9_]/g, '');
    if (!brand || !code) return res.status(400).json({ error: 'brand & code required' });
    await ensureSpecsTable(table, fields);
    const row = await upsertByBrandCode(table, {
      brand, code, series, display_name, family_slug, datasheet_url, cover, source_gcs_uri, raw_json, ...values
    });
    res.json({ ok: true, table, row });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'ingest failed', detail: String(e.message || e) });
  }
});

app.post('/ingest/bulk', async (req, res) => {
  try {
    const { items = [] } = req.body || {};
    if (!Array.isArray(items) || !items.length) return res.status(400).json({ error: 'items[] required' });
    const out = [];
    for (const it of items) {
      const table = (it.specs_table || `${it.family_slug}_specs`).replace(/[^a-zA-Z0-9_]/g, '');
      await ensureSpecsTable(table, it.fields || {});
      const row = await upsertByBrandCode(table, {
        brand: it.brand, code: it.code, series: it.series, display_name: it.display_name,
        family_slug: it.family_slug, datasheet_url: it.datasheet_url, cover: it.cover,
        source_gcs_uri: it.source_gcs_uri, raw_json: it.raw_json || null, ...(it.values || {}),
      });
      out.push({ table, row });
    }
    res.json({ ok: true, count: out.length, items: out });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'bulk ingest failed', detail: String(e.message || e) });
  }
});

app.post('/ingest/auto', async (req, res) => {
  try {
    const { gcsUri, family_slug=null, brand=null, code=null, series=null, display_name=null } = req.body || {};
    const result = await runAutoIngest({ gcsUri, family_slug, brand, code, series, display_name });
    res.json(result);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// --- Listings / Purchase Requests / Bids / Orders / BOM(import) ---
function stampOwnerTenant(req) {
  const actor = parseActor(req);
  return { owner_id: actor.id || null, tenant_id: actor.tenantId || null, actor };
}

app.get('/api/listings', async (req, res) => {
  const brand = (req.query.brand || '').toString().toLowerCase();
  const code = (req.query.code || '').toString().toLowerCase();
  const text = (req.query.q || '').toString().toLowerCase();
  const limit = Math.min(Number(req.query.limit || 50), 200);
  const params = [];
  let where = [];
  if (brand) { params.push(brand); where.push('brand_norm = $' + params.length); }
  if (code)  { params.push(code);  where.push('code_norm = $' + params.length); }
  if (text)  { params.push('%'+text+'%'); where.push('(brand_norm LIKE $'+params.length+' OR code_norm LIKE $'+params.length+')'); }
  const sql = `SELECT * FROM public.listings ${where.length ? 'WHERE '+where.join(' AND ') : ''} ORDER BY created_at DESC LIMIT ${limit}`;
  const rows = await db.query(sql, params);
  res.json({ items: rows.rows });
});

app.post('/api/listings', async (req, res) => {
  const { owner_id, tenant_id } = stampOwnerTenant(req);
  const { brand, code, price_cents, currency='USD', quantity_available, lead_time_days=null, seller_ref=null, note=null } = req.body || {};
  if (!brand || !code || price_cents == null || quantity_available == null) return res.status(400).json({ error: 'missing fields' });
  const id = uuidv4();
  const row = await db.query(`
    INSERT INTO public.listings (id, brand, code, brand_norm, code_norm, price_cents, currency, quantity_available, lead_time_days, seller_ref, note, owner_id, tenant_id)
    VALUES ($1,$2,$3, lower($2), lower($3), $4,$5,$6,$7,$8,$9,$10,$11) RETURNING *;
  `, [id, brand, code, price_cents, currency, quantity_available, lead_time_days, seller_ref, note, owner_id, tenant_id]);
  res.json(row.rows[0]);
});

app.post('/api/listings/:id/purchase', async (req, res) => {
  const { owner_id, tenant_id } = stampOwnerTenant(req);
  const id = req.params.id;
  const qty = Number(req.body.quantity || 0);
  const buyer_ref = req.body.buyer_ref || null;
  if (!qty || qty <= 0) return res.status(400).json({ error: 'quantity > 0 required' });
  try {
    await db.query('BEGIN');
    const listing = await db.query('SELECT * FROM public.listings WHERE id=$1 FOR UPDATE', [id]);
    if (!listing.rows.length) throw new Error('listing not found');
    const l = listing.rows[0];
    if (l.quantity_available < qty) throw new Error('insufficient quantity');
    await db.query('UPDATE public.listings SET quantity_available=quantity_available-$2 WHERE id=$1', [id, qty]);
    const orderId = uuidv4();
    await db.query(`INSERT INTO public.orders (id, listing_id, quantity, buyer_ref, owner_id, tenant_id) VALUES ($1,$2,$3,$4,$5,$6)`, [orderId, id, qty, buyer_ref, owner_id, tenant_id]);
    await db.query('COMMIT');
    res.json({ ok: true, order_id: orderId });
  } catch (e) {
    await db.query('ROLLBACK');
    res.status(400).json({ error: String(e.message || e) });
  }
});

app.get('/api/purchase-requests', async (req, res) => {
  const rows = await db.query('SELECT * FROM public.purchase_requests ORDER BY created_at DESC LIMIT 200');
  res.json({ items: rows.rows });
});

app.post('/api/purchase-requests', async (req, res) => {
  const { owner_id, tenant_id } = stampOwnerTenant(req);
  const { brand, code, required_qty, lead_time_days=null, target_price_cents=null, buyer_ref=null, note=null, due_date=null } = req.body || {};
  if (!brand || !code || !required_qty) return res.status(400).json({ error: 'missing fields' });
  const id = uuidv4();
  const row = await db.query(`
    INSERT INTO public.purchase_requests (id, brand, code, brand_norm, code_norm, required_qty, lead_time_days, target_price_cents, buyer_ref, note, due_date, owner_id, tenant_id)
    VALUES ($1,$2,$3, lower($2), lower($3), $4,$5,$6,$7,$8,$9,$10,$11) RETURNING *;
  `, [id, brand, code, required_qty, lead_time_days, target_price_cents, buyer_ref, note, due_date, owner_id, tenant_id]);

  // Notify sellers
  try {
    const family = await findFamilyForBrandCode(brand, code);
    await notify('purchase_request.created', {
      tenant_id, actor_id: owner_id, family_slug: family, brand, code,
      data: { purchase_request_id: id, required_qty, lead_time_days, target_price_cents, note, due_date }
    });
  } catch (e) { console.warn('notify PR failed:', e.message || e); }

  res.json(row.rows[0]);
});

app.get('/api/bids', async (req, res) => {
  const pr = (req.query.purchase_request_id || '').toString();
  const params = []; let where = '';
  if (pr) { params.push(pr); where = 'WHERE purchase_request_id=$1'; }
  const rows = await db.query(`SELECT * FROM public.bids ${where} ORDER BY created_at DESC LIMIT 500`, params);
  res.json({ items: rows.rows });
});

app.post('/api/bids', async (req, res) => {
  const { owner_id, tenant_id } = stampOwnerTenant(req);
  const { purchase_request_id, seller_ref=null, offer_qty, price_cents, currency='USD', lead_time_days=null, is_alternative=false, alt_brand=null, alt_code=null, note=null } = req.body || {};
  if (!purchase_request_id || !offer_qty || price_cents == null) return res.status(400).json({ error: 'missing fields' });
  const id = uuidv4();
  const row = await db.query(`
    INSERT INTO public.bids (id, purchase_request_id, seller_ref, offer_qty, price_cents, currency, lead_time_days, is_alternative, alt_brand, alt_code, note, owner_id, tenant_id)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING *;
  `, [id, purchase_request_id, seller_ref, offer_qty, price_cents, currency, lead_time_days, is_alternative, alt_brand, alt_code, note, owner_id, tenant_id]);
  res.json(row.rows[0]);
});

app.post('/api/purchase-requests/:id/confirm', async (req, res) => {
  const { owner_id, tenant_id } = stampOwnerTenant(req);
  const id = req.params.id;
  const { bid_ids = [] } = req.body || {};
  if (!Array.isArray(bid_ids) || !bid_ids.length) return res.status(400).json({ error: 'bid_ids required' });
  try {
    await db.query('BEGIN');
    const pr = await db.query('SELECT * FROM public.purchase_requests WHERE id=$1 FOR UPDATE', [id]);
    if (!pr.rows.length) throw new Error('purchase request not found');
    let remaining = pr.rows[0].required_qty - (pr.rows[0].confirmed_qty || 0);
    for (const bidId of bid_ids) {
      if (remaining <= 0) break;
      const b = await db.query('SELECT * FROM public.bids WHERE id=$1 FOR UPDATE', [bidId]);
      if (!b.rows.length) throw new Error(`bid not found: ${bidId}`);
      const bid = b.rows[0];
      const take = Math.min(remaining, bid.offer_qty);
      if (take <= 0) continue;
      await db.query(`INSERT INTO public.confirmations (id, purchase_request_id, bid_id, confirmed_qty, owner_id, tenant_id) VALUES ($1,$2,$3,$4,$5,$6)`,
        [uuidv4(), id, bidId, take, owner_id, tenant_id]);
      remaining -= take;
    }
    const totalConfirmed = pr.rows[0].required_qty - remaining;
    await db.query('UPDATE public.purchase_requests SET confirmed_qty = $2, status = CASE WHEN $2 >= required_qty THEN $3 ELSE $4 END WHERE id=$1',
      [id, totalConfirmed, 'confirmed', 'partial']);
    await db.query('COMMIT');
    res.json({ ok: true });
  } catch (e) {
    await db.query('ROLLBACK');
    res.status(400).json({ error: String(e.message || e) });
  }
});

app.post('/api/bom/import', upload.single('file'), async (req, res) => {
  const { owner_id, tenant_id } = stampOwnerTenant(req);
  try {
    const uploadId = uuidv4();
    const meta = {
      id: uploadId,
      filename: req.file?.originalname || null,
      size: req.file?.size || null,
      contentType: req.file?.mimetype || null,
      note: req.body?.note || null,
    };
    await db.query(`INSERT INTO public.bom_uploads (id, filename, size, content_type, note, owner_id, tenant_id) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      [meta.id, meta.filename, meta.size, meta.contentType, meta.note, owner_id, tenant_id]);
    let rows = [];
    if (req.file) {
      const text = req.file.buffer.toString('utf8');
      for (const line of text.split(/\r?\n/)) {
        const t = line.trim();
        if (!t || t.startsWith('#')) continue;
        const [brand, code, qtyStr, needBy] = t.split(',').map(s => (s||'').trim());
        if (!brand || !code) continue;
        const qty = Number(qtyStr || '0');
        rows.push({ brand, code, qty, need_by: needBy || null });
      }
    } else if (Array.isArray(req.body?.rows)) {
      rows = req.body.rows;
    }
    for (const r of rows) {
      await db.query(`INSERT INTO public.bom_lines (upload_id, brand, code, brand_norm, code_norm, quantity, need_by, owner_id, tenant_id) VALUES ($1,$2,$3, lower($2), lower($3), $4,$5,$6,$7)`,
        [uploadId, r.brand, r.code, Number(r.qty || r.quantity || 0), r.need_by || null, owner_id, tenant_id]);
    }
    res.json({ ok: true, upload_id: uploadId, count: rows.length });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// Optional mounts
try { const visionApp = require('./server.vision'); app.use(visionApp); } catch {}
try { const embApp = require('./server.embedding'); app.use(embApp); } catch {}
try { const tenApp = require('./server.tenancy'); app.use(tenApp); } catch {}
try { const notifyApp = require('./server.notify'); app.use(notifyApp); } catch {}
try { const bomApp = require('./server.bom'); app.use(bomApp); } catch {}

// 404
app.use((req, res) => res.status(404).json({ error: 'not found' }));

app.listen(PORT, () => {
  console.log(`worker listening on :${PORT}`);
});
