/* server.js */
'use strict';

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const { v4: uuidv4 } = require('uuid');

// --- utils / services ---
const db = require('./src/utils/db');
const { getSignedUrl, canonicalDatasheetPath, moveObject } = require('./src/utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('./src/utils/schema');
const { runAutoIngest } = require('./src/pipeline/ingestAuto');
const authzGlobal = require('./src/mw/authzGlobal');

// optional utils (존재 안해도 동작하도록)
const {
  requestLogger, patchDbLogging, logError
} = (() => {
  try { return require('./src/utils/logger'); }
  catch { return { requestLogger: () => (req, res, next) => next(), patchDbLogging: () => {}, logError: () => {} }; }
})();

const { parseActor } = (() => {
  try { return require('./src/utils/auth'); }
  catch { return { parseActor: () => ({}) }; }
})();

const { notify, findFamilyForBrandCode } = (() => {
  try { return require('./src/utils/notify'); }
  catch { return { notify: async () => ({}), findFamilyForBrandCode: async () => null }; }
})();

// --- app bootstrap ---
const app = express();
app.use(requestLogger());
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true }));
const upload = multer({ storage: multer.memoryStorage() });

// Global authorization / tenancy guard
app.use(authzGlobal);

// DB logging patch (optional)
try { const { patchDbLogging } = require('./src/utils/logger'); patchDbLogging(require('./src/utils/db')); } catch {}

// envs
const PORT = process.env.PORT || 8080;

const GCS_BUCKET_URI = process.env.GCS_BUCKET || '';
const GCS_BUCKET = GCS_BUCKET_URI.startsWith('gs://')
  ? GCS_BUCKET_URI.replace(/^gs:\/\//, '').split('/')[0]
  : '';

// ---------------- health & env ----------------
app.listen(PORT, '0.0.0.0', () => console.log(`worker listening on :${PORT}`));
app.get('/_healthz', (req, res) => res.type('text/plain').send('ok'));
app.get('/_env', (req, res) => {
  res.json({
    node: process.version,
    gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null,
    has_db: !!process.env.DATABASE_URL,
  });
});

// --------------- catalog registry / blueprint ---------------
app.get('/catalog/registry', async (req, res) => {
  try {
    const r = await db.query(`
      SELECT family_slug, specs_table
      FROM public.component_registry
      ORDER BY family_slug
    `);
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

// ---------------- files: signed URL / move ----------------
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

// ---------------- parts: search / detail / alternatives ----------------
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
      [text, limit],
    );
    res.json({ items: rows.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'search failed' });
  }
});

app.get('/parts/detail', async (req, res) => {
  const brand = (req.query.brand || '').toString();
  const code  = (req.query.code  || '').toString();
  if (!brand || !code) return res.status(400).json({ error: 'brand & code required' });
  try {
    const row = await db.query(
      `SELECT * FROM public.relay_specs
       WHERE brand_norm = lower($1) AND code_norm = lower($2)
       LIMIT 1`,
      [brand, code],
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
  const code  = (req.query.code  || '').toString();
  const limit = Math.min(Number(req.query.limit || 10), 50);
  if (!brand || !code) return res.status(400).json({ error: 'brand & code required' });
  try {
    const base = await db.query(
      `SELECT * FROM public.relay_specs
       WHERE brand_norm = lower($1) AND code_norm = lower($2)
       LIMIT 1`,
      [brand, code],
    );
    if (!base.rows.length) return res.status(404).json({ error: 'base not found' });

    const b = base.rows[0];
    const rows = await db.query(
      `SELECT *,
        (CASE WHEN family_slug IS NOT NULL AND family_slug = $1 THEN 0 ELSE 1 END) * 1.0 +
        COALESCE(ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($2::numeric,0)) / 100.0, 1.0) AS score
       FROM public.relay_specs
       WHERE NOT (brand_norm = lower($3) AND code_norm = lower($4))
       ORDER BY score ASC
       LIMIT $5`,
      [b.family_slug || null, b.coil_voltage_vdc || null, brand, code, limit],
    );
    res.json({ base: b, items: rows.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'alternatives failed' });
  }
});

// ---------------- ingest: manual / bulk / auto ----------------
app.post('/ingest', async (req, res) => {
  try {
    const {
      family_slug,
      specs_table, brand, code, series, display_name,
      datasheet_url, cover, source_gcs_uri, raw_json = null,
      fields = {}, values = {},
      tenant_id = null, owner_id = null, created_by = null, updated_by = null,
    } = req.body || {};

    const table = (specs_table || `${family_slug}_specs`).replace(/[^a-zA-Z0-9_]/g, '');
    if (!brand || !code) return res.status(400).json({ error: 'brand & code required' });

    await ensureSpecsTable(table, fields);
    const row = await upsertByBrandCode(table, {
      brand, code, series, display_name, family_slug, datasheet_url, cover, source_gcs_uri, raw_json,
      tenant_id, owner_id, created_by, updated_by, ...values,
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
    if (!Array.isArray(items) || !items.length)
      return res.status(400).json({ error: 'items[] required' });

    const out = [];
    for (const it of items) {
      const table = (it.specs_table || `${it.family_slug}_specs`).replace(/[^a-zA-Z0-9_]/g, '');
      await ensureSpecsTable(table, it.fields || {});
      const row = await upsertByBrandCode(table, {
        brand: it.brand, code: it.code, series: it.series, display_name: it.display_name,
        family_slug: it.family_slug, datasheet_url: it.datasheet_url, cover: it.cover,
        source_gcs_uri: it.source_gcs_uri, raw_json: it.raw_json || null,
        tenant_id: it.tenant_id || null, owner_id: it.owner_id || null,
        created_by: it.created_by || null, updated_by: it.updated_by || null,
        ...(it.values || {}),
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
    const { gcsUri, family_slug = null, brand = null, code = null, series = null, display_name = null } = req.body || {};
    const result = await runAutoIngest({ gcsUri, family_slug, brand, code, series, display_name });
    res.json(result);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

/* ===== DEBUG/BOOT MARKS (반드시 한 번만) ===== */
console.log('[BOOT] server.js loaded, will mount /auth and catalog stubs');

+/* ===== (1) 카탈로그/검색 404 방지 — 프론트가 계속 치는 엔드포인트 ===== */
+(() => {
+  const h = (_req, res) => res.json({ ok: true, nodes: [] }); // TODO: 실제 구현으로 교체
+  app.get('/catalog/tree', h);
+  app.get('/api/catalog/tree', h);      // 프록시 경로도 허용
+  app.get('/search/facets',  (_req, res) => res.json({ ok: true, facets: {} }));
+  app.get('/api/search/facets', (_req, res) => res.json({ ok: true, facets: {} }));
+})();

/* ===== (2) /auth 라우터 마운트 (성공/실패 로그 + 실패시 폴백) ===== */
try {
  const authRouter = require('./src/routes/manager');  // CJS 라우터 기대
  app.use(authRouter);                                 // /auth/*
  app.use('/api/worker', authRouter);                  // 구 프리픽스도 겸용 허용
  console.log('[BOOT] mounted /auth routes from ./src/routes/manager.js');
} catch (e) {
  console.error('[BOOT] FAILED to mount ./src/routes/manager.js:', e && e.code ? e.code : e);

  // --- 폴백: 최소 동작 보장 — manager가 없어도 200으로 응답해 404를 없앤다 ---
  app.get('/auth/health', (req,res)=>res.json({ ok:true, stub:true }));
  app.post('/auth/signup', express.json({limit:'5mb'}), (req,res)=>{
    res.json({ ok:true, token:'stub-token', user: { username: req.body?.username, email: req.body?.email }});
  });
  app.post('/auth/login', express.json({limit:'2mb'}), (req,res)=>{
    res.json({ ok:true, token:'stub-token', user: { id: req.body?.usernameOrEmail || 'user' }});
  });
  console.log('[BOOT] fallback /auth routes registered (no manager.js)');
}




// ---------------- Optional mounts (있으면 사용, 없으면 스킵) ----------------
try { const visionApp  = require('./server.vision');       app.use(visionApp); }  catch {}
try { const embApp     = require('./server.embedding');    app.use(embApp); }     catch {}
try { const tenApp     = require('./server.tenancy');      app.use(tenApp); }     catch {}
try { const notifyApp  = require('./server.notify');       app.use(notifyApp); }  catch {}
try { const bomApp     = require('./server.bom');          app.use(bomApp); }     catch {}
try { const optApp     = require('./server.optimize');     app.use(optApp); }     catch {}
try { const tasksApp   = require('./server.notifyTasks');  app.use(tasksApp); }   catch {}
try { const opsApp     = require('./server.ops');          app.use(opsApp); }     catch {}
try { const schemaApp  = require('./server.schema');       app.use(schemaApp); }  catch {}

// ---------------- 404 & error ----------------
app.use((req, res) => res.status(404).json({ error: 'not found' }));

app.use((err, req, res, next) => {
  try { require('./src/utils/logger').logError(err, { path: req.originalUrl }); } catch {}
  res.status(500).json({ error: 'internal error' });
});

