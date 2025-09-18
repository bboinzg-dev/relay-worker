/* server.js */
'use strict';

/* ========== ENV (MUST BE ABOVE ANY USAGE) ========== */
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';

const GCS_BUCKET_URI = process.env.GCS_BUCKET || '';
const GCS_BUCKET = GCS_BUCKET_URI.startsWith('gs://')
  ? GCS_BUCKET_URI.replace(/^gs:\/\//, '').split('/')[0]
  : (GCS_BUCKET_URI || '');

/* ========== Requires ========== */
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

const { normalizeFamilySlug } = require('./src/utils/family');
const db = require('./src/utils/db');
const {
  getSignedUrl,
  canonicalDatasheetPath,
  moveObject
} = require('./src/utils/gcs');
const {
  ensureSpecsTable,
  upsertByBrandCode
} = require('./src/utils/schema');
const { runAutoIngest } = require('./src/pipeline/ingestAuto');
const authzGlobal = require('./src/mw/authzGlobal');

/* optional utils (있으면 사용, 없으면 no-op) */
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

/* ========== Additional Config ========== */
const defaultBuckets = ['partsplan-docai-us', 'partsplan-ds'];
if (GCS_BUCKET && !defaultBuckets.includes(GCS_BUCKET)) defaultBuckets.unshift(GCS_BUCKET);

const ALLOWED_BUCKETS = new Set(
  (process.env.ALLOWED_BUCKETS || defaultBuckets.join(','))
    .split(',').map(s => s.trim()).filter(Boolean)
);

/* CORS 허용 목록(CSV 또는 /regex/) */
function parseCorsOrigins(envStr) {
  if (!envStr) return null; // null → cors() 기본 허용
  const items = envStr.split(',').map(s => s.trim()).filter(Boolean);
  return items.map(p => {
    if (p.startsWith('/') && p.endsWith('/')) {
      const body = p.slice(1, -1);
      return new RegExp(body);
    }
    return p;
  });
}
const CORS_ALLOW = parseCorsOrigins(process.env.CORS_ALLOW_ORIGINS);

/* ========== App bootstrap ========== */
const app = express();
app.disable('x-powered-by');
// JSON body parser MUST come before routes
app.use(bodyParser.json({ limit: '25mb' }));
app.use(requestLogger());

/* CORS */
if (CORS_ALLOW) {
  app.use(cors({ origin: CORS_ALLOW, credentials: true }));
} else {
  app.use(cors());
}

/* Body parsers */
app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true }));

/* Multer (한 번만 선언) */
const upload = multer({ storage: multer.memoryStorage() });

/* 보안 헤더 */
app.use((req, res, next) => {
  res.setHeader('x-frame-options', 'SAMEORIGIN');
  res.setHeader('x-content-type-options', 'nosniff');
  res.setHeader('referrer-policy', 'strict-origin-when-cross-origin');
  next();
});

/* ========== Helpers ========== */
function parseCookie(name, cookieHeader) {
  if (!cookieHeader) return null;
  const m = new RegExp(`(?:^|;\\s*)${name}=([^;]+)`).exec(cookieHeader);
  return m ? decodeURIComponent(m[1]) : null;
}
function verifyJwtCookie(cookieHeader) {
  const raw = parseCookie('pp_session', cookieHeader);
  if (!raw) return null;
  try { return jwt.verify(raw, JWT_SECRET); } catch { return null; }
}

/* Cloud Run 경계에서 ID 토큰 검증됨 → 있으면 통과, 없으면 pp_session 검사 */
function requireSession(req, res, next) {
  const auth = String(req.headers.authorization || '');
  if (/^Bearer\s+.+/i.test(auth)) return next();
  const claims = verifyJwtCookie(req.headers.cookie || '');
  if (claims) { req.user = claims; return next(); }
  return res.status(401).json({ ok: false, error: 'UNAUTHORIZED' });
}

/* "gs://bucket/path" 유효성 & 화이트리스트 */
function parseGcsUri(gcsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gcsUri || ''));
  if (!m) throw new Error('INVALID_GCS_URI');
  return { bucket: m[1], path: m[2] };
}
function assertAllowedUri(gcsUri) {
  const { bucket } = parseGcsUri(gcsUri);
  if (!ALLOWED_BUCKETS.has(bucket)) throw new Error('BUCKET_NOT_ALLOWED');
}

/* ========== Upload endpoint (worker가 GCS에 직접 저장) ========== */
/* 두 경로 모두 허용: /api/files/upload, /files/upload */
app.post(['/api/files/upload', '/files/upload'], upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok:false, error:'file required' });
    if (!GCS_BUCKET) return res.status(500).json({ ok:false, error:'GCS_BUCKET not set' });

    const buf = req.file.buffer;
    const sha = crypto.createHash('sha256').update(buf).digest('hex');
    const safe = (req.file.originalname || 'datasheet.pdf').replace(/\s+/g,'_');
    const object = `incoming/${sha}_${Date.now()}_${safe}`;

    // utils/gcs의 move/signed-url은 사용 중이므로 client를 여기선 직접 쓰지 않고,
    // 저장만 Storage SDK로 수행 (utils 내부와 충돌 없음)
    const { Storage } = require('@google-cloud/storage');
    const storage = new Storage(
      process.env.GCP_SERVICE_ACCOUNT
        ? { credentials: JSON.parse(process.env.GCP_SERVICE_ACCOUNT) }
        : {}
    );

    await storage.bucket(GCS_BUCKET).file(object).save(buf, {
      contentType: req.file.mimetype || 'application/pdf',
      resumable: false, public: false, validation: false,
    });

    return res.json({ ok:true, gcsUri: `gs://${GCS_BUCKET}/${object}` });
  } catch (e) {
    console.error('[upload]', e);
    return res.status(400).json({ ok:false, error:String(e.message || e) });
  }
});

/* ========== Global auth/tenancy guard (쓰기계열 보호) ========== */
app.use(authzGlobal);

/* DB logging patch (optional) */
try {
  const { patchDbLogging } = require('./src/utils/logger');
  patchDbLogging(require('./src/utils/db'));
} catch {}

/* ========== Health & env ========== */
app.get('/_healthz', (_req, res) => res.type('text/plain').send('ok'));
app.get('/api/health', async (_req, res) => {
  try { await db.query('SELECT 1'); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok: false, error: String(e?.message || e) }); }
});
app.get('/_env', (_req, res) => {
  res.json({
    node: process.version,
    gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null,
    has_db: !!process.env.DATABASE_URL,
  });
});

/* ========== Catalog (registry/blueprint) ========== */
app.get('/catalog/registry', async (_req, res) => {
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

/* ========== Files: signed-url / move (세션 + 화이트리스트) ========== */
app.get('/api/files/signed-url', requireSession, async (req, res) => {
  try {
    const gcsUri = req.query.gcsUri;
    const minutes = Number(req.query.minutes || 15);
    assertAllowedUri(gcsUri);
    const url = await getSignedUrl(gcsUri, minutes, 'read');
    res.json({ url });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

app.post('/api/files/move', requireSession, async (req, res) => {
  try {
    const { srcGcsUri, family_slug, brand, code, dstGcsUri } = req.body || {};
    if (!srcGcsUri) return res.status(400).json({ error: 'srcGcsUri required' });

    assertAllowedUri(srcGcsUri);

    const dst = dstGcsUri || canonicalDatasheetPath(GCS_BUCKET, family_slug, brand, code);
    assertAllowedUri(dst);

    const moved = await moveObject(srcGcsUri, dst);
    res.json({ ok: true, dstGcsUri: moved });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

/* ========== Parts: search / detail / alternatives ========== */
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

/* ========== Ingest: manual / bulk / auto ========== */
app.post('/ingest', requireSession, async (req, res) => {
  try {
    const {
      family_slug: _family_slug,
      specs_table, brand, code, series, display_name,
      datasheet_url, cover, source_gcs_uri, raw_json = null,
      fields = {}, values = {},
      tenant_id = null, owner_id = null, created_by = null, updated_by = null,
    } = req.body || {};
    const family_slug = normalizeFamilySlug(_family_slug);

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

app.post('/ingest/bulk', requireSession, async (req, res) => {
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

/* 자동 인제스트 (신규) */
app.post('/ingest/auto', requireSession, async (req, res) => {
  try {
    const { gcsUri, family_slug = null, brand = null, code = null, series = null, display_name = null } = req.body || {};
    const result = await runAutoIngest({ gcsUri, family_slug, brand, code, series, display_name });
    res.json(result);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

/* 호환용: 프론트가 /api/worker/ingest 로 호출하던 경로 */
// /api/worker/ingest : Cloud Tasks/직접 호출 공용 엔드포인트
app.post('/api/worker/ingest', requireSession, async (req, res) => {
  try {
    // ✅ 둘 다 허용
    const { gcsUri, gcsPdfUri, brand, code, series, display_name } = req.body || {};
    const uri = gcsUri || gcsPdfUri;
    if (!uri || !/^gs:\/\//i.test(uri)) {
      // 디버그: 현재 받은 바디를 로그로 남김
      console.warn('[ingest] 400 gcsUri required — body=', req.body);
      return res.status(400).json({ ok:false, error:'gcsUri required (gs://...)' });
    }

    const out = await runAutoIngest({
      gcsUri: uri, brand, code, series, display_name
    });
    return res.json(out);
  } catch (e) {
    console.error('[ingest] 500', e);
    return res.status(500).json({ ok:false, error:String(e?.message || e) });
  }
});


/* ===== DEBUG/BOOT MARKS ===== */
console.log('[BOOT] server.js loaded, will mount /auth and catalog stubs');

/* ===== (1) 카탈로그/검색 404 방지 (프론트 프록시 경로 포함) ===== */
(() => {
  const h = (_req, res) => res.json({ ok: true, nodes: [] }); // TODO: 실제 구현으로 교체
  app.get('/catalog/tree', h);
  app.get('/api/catalog/tree', h);
  app.get('/search/facets',  (_req, res) => res.json({ ok: true, facets: {} }));
  app.get('/api/search/facets', (_req, res) => res.json({ ok: true, facets: {} }));
})();

/* ===== (2) /auth 라우터 마운트 (없으면 폴백 스텁) ===== */
try {
  const m = require('./src/routes/manager'); // CJS/ESM 호환
  const authRouter = m.default || m;
  app.use(authRouter);                // /auth/*
  app.use('/api/worker', authRouter); // 구 프리픽스도 허용
  console.log('[BOOT] mounted /auth routes from ./src/routes/manager.js');
} catch (e) {
  console.error('[BOOT] FAILED to mount ./src/routes/manager.js:', e && e.code ? e.code : e);
  const sign = (p) => jwt.sign(p, JWT_SECRET, { expiresIn: '7d' });

  app.get('/auth/health', (_req,res)=>res.json({ ok:true, stub:true }));
  app.post('/auth/signup', express.json({limit:'5mb'}), (req,res)=>{
    const p = req.body || {};
    const token = sign({
      uid: String(p.username || p.email || 'user'),
      username: p.username || '',
      email: p.email || '',
    });
    res.json({ ok:true, token, user: { username: p.username || '', email: p.email || '' }});
  });
  app.post('/auth/login', express.json({limit:'2mb'}), (req,res)=>{
    const p = req.body || {};
    const login = p.login || p.username || p.email || 'user';
    const token = sign({ uid: String(login), username: String(login), email: p.email || '' });
    res.json({ ok:true, token, user: { id: login, username: String(login), email: p.email || '' }});
  });
  console.log('[BOOT] fallback /auth routes registered (no manager.js)');
}

/* ========== Optional mounts (있으면 사용) ========== */
try { const visionApp  = require('./server.vision');       app.use(visionApp); }  catch {}
try { const embApp     = require('./server.embedding');    app.use(embApp); }     catch {}
try { const tenApp     = require('./server.tenancy');      app.use(tenApp); }     catch {}
try { const notifyApp  = require('./server.notify');       app.use(notifyApp); }  catch {}
try { const bomApp     = require('./server.bom');          app.use(bomApp); }     catch {}
try { const optApp     = require('./server.optimize');     app.use(optApp); }     catch {}
try { const tasksApp   = require('./server.notifyTasks');  app.use(tasksApp); }   catch {}
try { const opsApp     = require('./server.ops');          app.use(opsApp); }     catch {}
try { const schemaApp  = require('./server.schema');       app.use(schemaApp); }  catch {}

/* ========== 404 & error handlers ========== */
app.use((req, res) => res.status(404).json({ error: 'not found' }));
app.use((err, req, res, next) => {
  try { require('./src/utils/logger').logError(err, { path: req.originalUrl }); } catch {}
  res.status(500).json({ error: 'internal error' });
});

/* ========== Listen ========== */
app.listen(PORT, '0.0.0.0', () => console.log(`worker listening on :${PORT}`));
