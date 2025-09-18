/* server.js */
'use strict';

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

const db = require('./src/utils/db');
const { getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject, storage } = require('./src/utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('./src/utils/schema');
const { runAutoIngest } = require('./src/pipeline/ingestAuto');

const app = express();
app.disable('x-powered-by');

// ✅ Cloud Tasks/JSON 본문 파싱은 라우트보다 반드시 먼저
app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true }));

/* ---------------- Env / Config ---------------- */
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';

const GCS_BUCKET_URI = process.env.GCS_BUCKET || '';
const GCS_BUCKET = GCS_BUCKET_URI.startsWith('gs://')
  ? GCS_BUCKET_URI.replace(/^gs:\/\//, '').split('/')[0]
  : (GCS_BUCKET_URI || '');

const DEFAULT_ALLOWED_BUCKETS = ['partsplan-docai-us', 'partsplan-ds', GCS_BUCKET].filter(Boolean);
const ALLOWED_BUCKETS = new Set(
  (process.env.ALLOWED_BUCKETS || DEFAULT_ALLOWED_BUCKETS.join(','))
    .split(',').map(s => s.trim()).filter(Boolean)
);

function parseCorsOrigins(envStr) {
  if (!envStr) return null;
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

/* ---------------- CORS / Security ---------------- */
if (CORS_ALLOW) {
  app.use(cors({ origin: CORS_ALLOW, credentials: true }));
} else {
  app.use(cors());
}

app.use((req, res, next) => {
  res.setHeader('x-frame-options', 'SAMEORIGIN');
  res.setHeader('x-content-type-options', 'nosniff');
  res.setHeader('referrer-policy', 'strict-origin-when-cross-origin');
  next();
});

/* ---------------- Multer (파일 업로드) ---------------- */
const upload = multer({ storage: multer.memoryStorage() });

/* ---------------- 세션/인증 도우미 ---------------- */
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
function requireSession(req, res, next) {
  const auth = String(req.headers.authorization || '');
  if (/^Bearer\s+.+/i.test(auth)) return next(); // Cloud Run IAP/Invoker
  const claims = verifyJwtCookie(req.headers.cookie || '');
  if (claims) { req.user = claims; return next(); }
  return res.status(401).json({ ok: false, error: 'UNAUTHORIZED' });
}

/* ---------------- Health & Env ---------------- */
app.get('/_healthz', (_req, res) => res.type('text/plain').send('ok'));
app.get('/api/health', async (_req, res) => {
  try { await db.query('SELECT 1'); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok:false, error:String(e?.message || e) }); }
});
app.get('/_env', (_req, res) => {
  res.json({
    node: process.version,
    gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null,
    has_db: !!process.env.DATABASE_URL,
  });
});

/* ---------------- 파일 업로드(워커가 GCS에 저장) ---------------- */
// /api/files/upload 와 /files/upload 둘 다 허용
app.post(['/api/files/upload', '/files/upload'], upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok:false, error:'file required' });
    if (!GCS_BUCKET) return res.status(500).json({ ok:false, error:'GCS_BUCKET not set' });

    const buf = req.file.buffer;
    const sha = crypto.createHash('sha256').update(buf).digest('hex');
    const safe = (req.file.originalname || 'datasheet.pdf').replace(/\s+/g,'_');
    const object = `incoming/${sha}_${Date.now()}_${safe}`;

    await storage.bucket(GCS_BUCKET).file(object).save(buf, {
      contentType: req.file.mimetype || 'application/pdf',
      resumable: false, public: false, validation: false,
    });

    return res.json({ ok:true, gcsUri:`gs://${GCS_BUCKET}/${object}` });
  } catch (e) {
    console.error('[upload]', e);
    return res.status(400).json({ ok:false, error:String(e?.message || e) });
  }
});

/* ---------------- Files: signed-url / move ---------------- */
function parseGcsUri(gcsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gcsUri || ''));
  if (!m) throw new Error('INVALID_GCS_URI');
  return { bucket: m[1], path: m[2] };
}
function assertAllowedUri(gcsUri) {
  const { bucket } = parseGcsUri(gcsUri);
  if (!ALLOWED_BUCKETS.has(bucket)) throw new Error('BUCKET_NOT_ALLOWED');
}

app.get('/api/files/signed-url', requireSession, async (req, res) => {
  try {
    const gcsUri = req.query.gcsUri;
    const minutes = Number(req.query.minutes || 15);
    assertAllowedUri(gcsUri);
    const url = await getSignedUrl(gcsUri, minutes, 'read');
    res.json({ url });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok:false, error:String(e?.message || e) });
  }
});

app.post('/api/files/move', requireSession, async (req, res) => {
  try {
    const { srcGcsUri, family_slug, brand, code, dstGcsUri } = req.body || {};
    if (!srcGcsUri) return res.status(400).json({ ok:false, error:'srcGcsUri required' });

    assertAllowedUri(srcGcsUri);
    const dst = dstGcsUri || canonicalDatasheetPath(GCS_BUCKET, family_slug, brand, code);
    assertAllowedUri(dst);

    const moved = await moveObject(srcGcsUri, dst);
    res.json({ ok:true, dstGcsUri: moved });
  } catch (e) {
    console.error(e);
    res.status(400).json({ ok:false, error:String(e?.message || e) });
  }
});

/* ---------------- Catalog / Blueprint ---------------- */
app.get('/catalog/registry', async (_req, res) => {
  try {
    const r = await db.query(`
      SELECT family_slug, specs_table
      FROM public.component_registry
      ORDER BY family_slug`);
    res.json({ items: r.rows });
  } catch (e) {
    console.error(e); res.status(500).json({ ok:false, error:'registry failed' });
  }
});

app.get('/catalog/blueprint/:family', async (req, res) => {
  try {
    const r = await db.query(`
      SELECT b.family_slug, r.specs_table, b.fields_json, b.prompt_template
      FROM public.component_spec_blueprint b
      JOIN public.component_registry r USING (family_slug)
      WHERE b.family_slug = $1
      LIMIT 1`, [req.params.family]);
    if (!r.rows.length) return res.status(404).json({ ok:false, error:'blueprint not found' });
    res.json(r.rows[0]);
  } catch (e) {
    console.error(e); res.status(500).json({ ok:false, error:'blueprint failed' });
  }
});

/* ---------------- Parts (예시: relay 검색/상세/대안) ---------------- */
app.get('/parts/search', async (req, res) => {
  const q = (req.query.q || '').toString().trim();
  const limit = Math.min(Number(req.query.limit || 20), 100);
  try {
    const text = q ? `%${q.toLowerCase()}%` : '%';
    const rows = await db.query(
      `SELECT * FROM public.relay_specs
       WHERE brand_norm LIKE $1 OR code_norm LIKE $1 OR lower(series) LIKE $1 OR lower(display_name) LIKE $1
       ORDER BY updated_at DESC
       LIMIT $2`, [text, limit]);
    res.json({ items: rows.rows });
  } catch (e) { console.error(e); res.status(500).json({ ok:false, error:'search failed' }); }
});

app.get('/parts/detail', async (req, res) => {
  const brand = (req.query.brand || '').toString();
  const code  = (req.query.code  || '').toString();
  if (!brand || !code) return res.status(400).json({ ok:false, error:'brand & code required' });
  try {
    const row = await db.query(
      `SELECT * FROM public.relay_specs
       WHERE brand_norm = lower($1) AND code_norm = lower($2)
       LIMIT 1`, [brand, code]);
    if (!row.rows.length) return res.status(404).json({ ok:false, error:'not found' });
    res.json(row.rows[0]);
  } catch (e) { console.error(e); res.status(500).json({ ok:false, error:'detail failed' }); }
});

app.get('/parts/alternatives', async (req, res) => {
  const brand = (req.query.brand || '').toString();
  const code  = (req.query.code  || '').toString();
  const limit = Math.min(Number(req.query.limit || 10), 50);
  if (!brand || !code) return res.status(400).json({ ok:false, error:'brand & code required' });
  try {
    const base = await db.query(
      `SELECT * FROM public.relay_specs
       WHERE brand_norm = lower($1) AND code_norm = lower($2)
       LIMIT 1`, [brand, code]);
    if (!base.rows.length) return res.status(404).json({ ok:false, error:'base not found' });

    const b = base.rows[0];
    const rows = await db.query(
      `SELECT *,
        (CASE WHEN family_slug IS NOT NULL AND family_slug = $1 THEN 0 ELSE 1 END) * 1.0 +
        COALESCE(ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($2::numeric,0)) / 100.0, 1.0) AS score
       FROM public.relay_specs
       WHERE NOT (brand_norm = lower($3) AND code_norm = lower($4))
       ORDER BY score ASC
       LIMIT $5`,
       [b.family_slug || null, b.coil_voltage_vdc || null, brand, code, limit]);
    res.json({ base: b, items: rows.rows });
  } catch (e) { console.error(e); res.status(500).json({ ok:false, error:'alternatives failed' }); }
});

/* ---------------- Ingest: manual / bulk / auto ---------------- */
app.post('/ingest', requireSession, async (req, res) => {
  try {
    const {
      family_slug, specs_table, brand, code, series, display_name,
      datasheet_uri, cover, source_gcs_uri, raw_json = null,
      fields = {}, values = {}
    } = req.body || {};

    const table = (specs_table || `${family_slug}_specs`).replace(/[^a-zA-Z0-9_]/g, '');
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand & code required' });

    await ensureSpecsTable(table, fields);
    const row = await upsertByBrandCode(table, {
      brand, code, series, display_name, family_slug, datasheet_uri, cover, source_gcs_uri, raw_json, ...values
    });

    res.json({ ok:true, table, row });
  } catch (e) { console.error(e); res.status(500).json({ ok:false, error:'ingest failed', detail:String(e?.message || e) }); }
});

app.post('/ingest/bulk', requireSession, async (req, res) => {
  try {
    const { items = [] } = req.body || {};
    if (!Array.isArray(items) || !items.length)
      return res.status(400).json({ ok:false, error:'items[] required' });

    const out = [];
    for (const it of items) {
      const table = (it.specs_table || `${it.family_slug}_specs`).replace(/[^a-zA-Z0-9_]/g, '');
      await ensureSpecsTable(table, it.fields || {});
      const row = await upsertByBrandCode(table, {
        brand: it.brand, code: it.code, series: it.series, display_name: it.display_name,
        family_slug: it.family_slug, datasheet_uri: it.datasheet_uri, cover: it.cover,
        source_gcs_uri: it.source_gcs_uri, raw_json: it.raw_json || null,
        ...(it.values || {}),
      });
      out.push({ table, row });
    }
    res.json({ ok:true, count: out.length, items: out });
  } catch (e) { console.error(e); res.status(500).json({ ok:false, error:'bulk ingest failed', detail:String(e?.message || e) }); }
});

app.post('/ingest/auto', requireSession, async (req, res) => {
  try {
    const { gcsUri, gcsPdfUri, family_slug = null, brand = null, code = null, series = null, display_name = null } = req.body || {};
    const uri = gcsUri || gcsPdfUri;
    if (!uri) return res.status(400).json({ ok:false, error:'gcsUri required' });
    const result = await runAutoIngest({ gcsUri: uri, family_slug, brand, code, series, display_name });
    res.json(result);
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e?.message || e) }); }
});

/* ---------------- Cloud Tasks / Direct: 공용 인제스트 엔드포인트 ---------------- */
// ✅ gcsUri || gcsPdfUri 모두 허용 + 풍부한 진단 로그
app.post('/api/worker/ingest', requireSession, async (req, res) => {
  const startedAt = Date.now();
  try {
    const taskName  = req.get('X-Cloud-Tasks-TaskName') || null;
    const retryCnt  = req.get('X-Cloud-Tasks-TaskRetryCount') || null;
    const traceId   = req.get('X-Cloud-Trace-Context') || null;

    const { gcsUri, gcsPdfUri, brand, code, series, display_name } = req.body || {};
    const uri = gcsUri || gcsPdfUri;

    if (!uri || !/^gs:\/\//i.test(uri)) {
      console.warn('[ingest 400] invalid body', { taskName, retryCnt, traceId, body: req.body });
      return res.status(400).json({ ok:false, error:'gcsUri required (gs://...)' });
    }

    const out = await runAutoIngest({ gcsUri: uri, brand, code, series, display_name });

    console.log('[ingest 200]', {
      taskName, retryCnt, traceId,
      ms: Date.now() - startedAt,
      family: out?.family_slug, table: out?.specs_table, brand: out?.brand, code: out?.code
    });
    return res.json(out);
  } catch (e) {
    console.error('[ingest 500]', {
      ms: Date.now() - startedAt,
      error: e?.message || String(e),
      stack: e?.stack
    });
    return res.status(500).json({ ok:false, error:String(e?.message || e) });
  }
});

/* ===== (수정) /auth 라우터 마운트: 베이스 경로로 장착 + 실패 시 스텁 ===== */
try {
  const mod = require('./src/routes/manager');   // CJS/ESM 호환
  const authRouter = mod.default || mod;
  // ✅ 베이스 경로로 마운트: 라우터 내부가 '/login' 으로만 정의돼 있어도 '/auth/login' 경로가 생김
  app.use('/auth', authRouter);
  app.use('/api/worker/auth', authRouter);       // 구 프리픽스 호환 필요하면 유지
  console.log('[BOOT] mounted /auth routes (base-mounted)');
} catch (e) {
  console.error('[BOOT] FAILED to mount ./src/routes/manager.js:', e?.code || e);

  // ✅ 스텁 라우터(매니저 로드 실패해도 /auth/* 동작)
  const sign = (p)=> jwt.sign(p, JWT_SECRET, { expiresIn: '7d' });
  const stub = express.Router();
  stub.get('/health', (_req, res)=> res.json({ ok:true, stub:true }));

  // POST /auth/signup
  stub.post('/signup', express.json({ limit: '2mb' }), (req,res)=>{
    const p = req.body || {};
    const token = sign({ uid: String(p.username || p.email || 'user'), username: p.username || '' });
    return res.json({ ok:true, token, user:{ username: p.username || '', email: p.email || '' } });
  });

  // POST /auth/login
  stub.post('/login', express.json({ limit: '2mb' }), (req,res)=>{
    const p = req.body || {};
    const id = String(p.idOrEmail || p.username || p.email || 'user');
    const token = sign({ uid: id, username: id });
    return res.json({ ok:true, token, user:{ username: id } });
  });

  app.use('/auth', stub);
  app.use('/api/worker/auth', stub);
}


/* ---------------- 404 / error ---------------- */
app.use((req, res) => res.status(404).json({ ok:false, error:'not found' }));
app.use((err, req, res, next) => {
  try { require('./src/utils/logger').logError(err, { path: req.originalUrl }); } catch {}
  res.status(500).json({ ok:false, error:'internal error' });
});

/* ---------------- Listen ---------------- */
app.listen(PORT, '0.0.0.0', () => console.log(`worker listening on :${PORT}`));
