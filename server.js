/* relay-worker/server.js */
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

// (있으면) 모듈형 라우터 마운트
try { app.use(require('./server.health'));   console.log('[BOOT] mounted /api/health'); } catch {}
try { app.use(require('./server.optimize')); console.log('[BOOT] mounted /api/optimize/*'); } catch {}
try { app.use(require('./server.checkout')); console.log('[BOOT] mounted /api/checkout/*'); } catch {}
try { app.use(require('./server.bom'));      console.log('[BOOT] mounted /api/bom/*'); } catch {}
try { app.use(require('./server.notify'));   console.log('[BOOT] mounted /api/notify/*'); } catch {}
try { app.use(require('./server.market'));   console.log('[BOOT] mounted market routes'); } catch {}
try { app.use(require('./server.vision'));   console.log('[BOOT] mounted /api/vision/*'); } catch (e) {
  console.error('[BOOT] failed to mount /api/vision/*', e?.message || e);
}

app.use('/api/parts', require('./src/routes/parts'));
app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true }));
app.disable('x-powered-by');

// ---------------- Env
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';
const GCS_BUCKET_URI = process.env.GCS_BUCKET || '';
const GCS_BUCKET = GCS_BUCKET_URI.startsWith('gs://')
  ? GCS_BUCKET_URI.replace(/^gs:\/\//, '').split('/')[0]
  : (GCS_BUCKET_URI || '');
const DEFAULT_ALLOWED_BUCKETS = ['partsplan-docai-us', 'partsplan-ds', GCS_BUCKET].filter(Boolean);
const ALLOWED_BUCKETS = new Set((process.env.ALLOWED_BUCKETS || DEFAULT_ALLOWED_BUCKETS.join(',')).split(',').map(s => s.trim()).filter(Boolean));

function parseCorsOrigins(envStr) {
  if (!envStr) return null;
  const items = envStr.split(',').map(s => s.trim()).filter(Boolean);
  return items.map(p => (p.startsWith('/') && p.endsWith('/')) ? new RegExp(p.slice(1,-1)) : p);
}
const CORS_ALLOW = parseCorsOrigins(process.env.CORS_ALLOW_ORIGINS);
if (CORS_ALLOW) app.use(cors({ origin: CORS_ALLOW, credentials: true })); else app.use(cors());

// 보안 헤더
app.use((req, res, next) => {
  res.setHeader('x-frame-options', 'SAMEORIGIN');
  res.setHeader('x-content-type-options', 'nosniff');
  res.setHeader('referrer-policy', 'strict-origin-when-cross-origin');
  next();
});

// ---------------- 업로드
const upload = multer({ storage: multer.memoryStorage() });

function parseCookie(name, cookieHeader) {
  if (!cookieHeader) return null;
  const m = new RegExp('(?:^|;\\s*)' + name + '=([^;]+)').exec(cookieHeader);
  return m ? decodeURIComponent(m[1]) : null;
}
function verifyJwtCookie(cookieHeader) {
  const raw = parseCookie('pp_session', cookieHeader);
  if (!raw) return null;
  try { return jwt.verify(raw, JWT_SECRET); } catch { return null; }
}
function requireSession(req, res, next) {
  const auth = String(req.headers.authorization || '');
  if (/^Bearer\s+.+/i.test(auth)) return next();
  const claims = verifyJwtCookie(req.headers.cookie || '');
  if (claims) { req.user = claims; return next(); }
  return res.status(401).json({ ok:false, error:'UNAUTHORIZED' });
}

// health / env
app.get('/_healthz', (_req, res) => res.type('text/plain').send('ok'));
app.get('/api/health', async (_req, res) => {
  try { await db.query('SELECT 1'); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok:false, error:String(e?.message || e) }); }
});
app.get('/_env', (_req, res) => res.json({ node: process.version, gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null, has_db: !!process.env.DATABASE_URL }));

// GCS 도우미
function parseGcsUriLocal(gcsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(String(gcsUri || ''));
  if (!m) throw new Error('INVALID_GCS_URI');
  return { bucket: m[1], path: m[2] };
}
function assertAllowedUri(gcsUri) {
  const { bucket } = parseGcsUriLocal(gcsUri);
  if (!ALLOWED_BUCKETS.has(bucket)) throw new Error('BUCKET_NOT_ALLOWED');
}

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
    res.json({ ok:true, gcsUri:`gs://${GCS_BUCKET}/${object}` });
  } catch (e) {
    console.error('[upload]', e);
    res.status(400).json({ ok:false, error:String(e?.message || e) });
  }
});

app.get('/api/files/signed-url', requireSession, async (req, res) => {
  try {
    const gcsUri = req.query.gcsUri;
    const minutes = Number(req.query.minutes || 15);
    assertAllowedUri(gcsUri);
    const url = await getSignedUrl(gcsUri, minutes, 'read');
    res.json({ url });
  } catch (e) { res.status(400).json({ ok:false, error:String(e?.message || e) }); }
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
  } catch (e) { res.status(400).json({ ok:false, error:String(e?.message || e) }); }
});

// registry / blueprint
app.get('/catalog/registry', async (_req, res) => {
  try {
    const r = await db.query(`select family_slug, specs_table from public.component_registry order by family_slug`);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ ok:false, error:'registry failed' }); }
});
app.get('/catalog/blueprint/:family', async (req, res) => {
  try {
    const r = await db.query(`
      select b.family_slug, r.specs_table, b.fields_json, b.prompt_template
        from public.component_spec_blueprint b
        join public.component_registry r using (family_slug)
       where b.family_slug = $1
       limit 1`, [req.params.family]);
    if (!r.rows.length) return res.status(404).json({ ok:false, error:'blueprint not found' });
    res.json(r.rows[0]);
  } catch (e) { res.status(500).json({ ok:false, error:'blueprint failed' }); }
});

// parts (호환: family 미지정 시 relay view)
app.get('/parts/detail', async (req, res) => {
  const brand  = (req.query.brand || '').toString();
  const code   = (req.query.code  || '').toString();
  const family = (req.query.family || '').toString().toLowerCase();
  if (!brand || !code) return res.status(400).json({ ok:false, error:'brand & code required' });

  try {
    if (family) {
      const r = await db.query(`select specs_table from public.component_registry where family_slug=$1 limit 1`, [family]);
      const table = r.rows[0]?.specs_table;
      if (!table) return res.status(400).json({ ok:false, error:'UNKNOWN_FAMILY' });
      const row = await db.query(`select * from public.${table} where brand_norm=lower($1) and code_norm=lower($2) limit 1`, [brand, code]);
      return row.rows[0] ? res.json({ ok:true, item: row.rows[0] }) : res.status(404).json({ ok:false, error:'NOT_FOUND' });
    }
    const row = await db.query(`select * from public.relay_specs where brand_norm=lower($1) and code_norm=lower($2) limit 1`, [brand, code]);
    return row.rows[0] ? res.json({ ok:true, item: row.rows[0] }) : res.status(404).json({ ok:false, error:'NOT_FOUND' });
  } catch (e) { res.status(500).json({ ok:false, error:'detail_failed' }); }
});

app.get('/parts/search', async (req, res) => {
  const q      = (req.query.q || '').toString().trim();
  const limit  = Math.min(Number(req.query.limit || 20), 100);
  const family = (req.query.family || '').toString().toLowerCase();
  try {
    const text = q ? `%${q.toLowerCase()}%` : '%';
    if (family) {
      const r = await db.query(`select specs_table from public.component_registry where family_slug=$1 limit 1`, [family]);
      const table = r.rows[0]?.specs_table;
      if (!table) return res.status(400).json({ ok:false, error:'UNKNOWN_FAMILY' });
      const rows = await db.query(`
        select id, family_slug, brand, code, display_name,
               width_mm, height_mm, length_mm, image_uri, datasheet_uri, updated_at
          from public.${table}
         where brand_norm like $1 or code_norm like $1
            or lower(coalesce(series,'')) like $1
            or lower(coalesce(display_name,'')) like $1
         order by updated_at desc
         limit $2`, [text, limit]);
      return res.json({ ok:true, items: rows.rows });
    }
    // 통합 뷰 우선, 없으면 릴레이 뷰
    try {
      const rows = await db.query(`
        select id, family_slug, brand, code, display_name,
               width_mm, height_mm, length_mm, image_uri, datasheet_uri, updated_at
          from public.component_specs
         where brand_norm like $1 or code_norm like $1 or lower(coalesce(display_name,'')) like $1
         order by updated_at desc
         limit $2`, [text, limit]);
      return res.json({ ok:true, items: rows.rows });
    } catch {
      const rows = await db.query(`
        select * from public.relay_specs
         where brand_norm like $1 or code_norm like $1 or lower(series) like $1 or lower(display_name) like $1
         order by updated_at desc
         limit $2`, [text, limit]);
      return res.json({ ok:true, items: rows.rows });
    }
  } catch (e) { res.status(500).json({ ok:false, error:'search_failed' }); }
});

// Ingest
app.post('/ingest', requireSession, async (req, res) => {
  try {
    const { family_slug, specs_table, brand, code, series, display_name,
            datasheet_uri, cover, source_gcs_uri, raw_json = null,
            fields = {}, values = {} } = req.body || {};
    const table = (specs_table || `${family_slug}_specs`).replace(/[^a-zA-Z0-9_]/g, '');
    if (!brand || !code) return res.status(400).json({ ok:false, error:'brand & code required' });

    await ensureSpecsTable(table, fields);
    const row = await upsertByBrandCode(table, {
      brand, code, series, display_name, family_slug, datasheet_uri, cover, source_gcs_uri, raw_json, ...values
    });
    res.json({ ok:true, table, row });
  } catch (e) { res.status(500).json({ ok:false, error:'ingest failed', detail:String(e?.message || e) }); }
});

app.post('/ingest/bulk', requireSession, async (req, res) => {
  try {
    const { items = [] } = req.body || {};
    if (!Array.isArray(items) || !items.length) return res.status(400).json({ ok:false, error:'items[] required' });
    const out = [];
    for (const it of items) {
      const table = (it.specs_table || `${it.family_slug}_specs`).replace(/[^a-zA-Z0-9_]/g, '');
      await ensureSpecsTable(table, it.fields || {});
      const row = await upsertByBrandCode(table, {
        brand: it.brand, code: it.code, series: it.series, display_name: it.display_name,
        family_slug: it.family_slug, datasheet_uri: it.datasheet_uri, cover: it.cover,
        source_gcs_uri: it.source_gcs_uri, raw_json: it.raw_json || null, ...(it.values || {}),
      });
      out.push({ table, row });
    }
    res.json({ ok:true, count: out.length, items: out });
  } catch (e) { res.status(500).json({ ok:false, error:'bulk ingest failed', detail:String(e?.message || e) }); }
});

// Cloud Tasks에서 호출되는 엔드포인트(기존 유지)
app.post('/api/worker/ingest', requireSession, async (req, res) => {
  const startedAt = Date.now();
  const taskName  = req.get('X-Cloud-Tasks-TaskName') || null;
  const retryCnt  = Number(req.get('X-Cloud-Tasks-TaskRetryCount') || 0);
  try {
    const { gcsUri, gcsPdfUri, brand, code, series, display_name } = req.body || {};
    const uri = gcsUri || gcsPdfUri;
    if (!uri || !/^gs:\/\//i.test(uri)) {
      await db.query(
        `insert into public.ingest_run_logs (task_name, retry_count, gcs_uri, status, error_message)
         values ($1,$2,$3,'FAILED',$4)`,
        [taskName, retryCnt, uri || '', 'gcsUri required (gs://...)']
      );
      return res.status(400).json({ ok:false, error:'gcsUri required (gs://...)' });
    }
    await db.query(`
      create table if not exists public.ingest_run_logs (
        id uuid default gen_random_uuid() primary key,
        task_name text, retry_count integer, gcs_uri text not null,
        status text check (status in ('PROCESSING','SUCCEEDED','FAILED')),
        pred_family_slug text, pred_brand text, pred_code text,
        final_table text, final_family text, final_brand text, final_code text,
        final_datasheet text, duration_ms integer, error_message text,
        started_at timestamptz default now(), finished_at timestamptz
      )`);

    await db.query(
      `insert into public.ingest_run_logs (task_name, retry_count, gcs_uri, status)
       values ($1,$2,$3,'PROCESSING')`, [taskName, retryCnt, uri]
    );

    const out = await runAutoIngest({ gcsUri: uri, brand, code, series, display_name });

    await db.query(
      `update public.ingest_run_logs
          set finished_at = now(), duration_ms = $2,
              status = 'SUCCEEDED',
              final_table = $3, final_family = $4, final_brand = $5, final_code = $6, final_datasheet = $7
        where task_name = $1 and status = 'PROCESSING'`,
      [taskName, Date.now() - startedAt, out?.specs_table || null, out?.family || out?.family_slug || null,
       out?.brand ?? null, (Array.isArray(out?.codes) ? out.codes[0] : out?.code) ?? null, out?.datasheet_uri || uri]
    );

    return res.json(out);

  } catch (e) {
    try {
      await db.query(
        `update public.ingest_run_logs
            set finished_at = now(), duration_ms = $2, status = 'FAILED', error_message = $3
          where task_name = $1 and status = 'PROCESSING'`,
        [ taskName, Date.now()-startedAt, String(e?.message || e) ]
      );
    } catch {}
    return res.status(500).json({ ok:false, error:String(e?.message || e) });
  }
});

// 간단 트리 (기존 의존성 유지)
const catalogTree = (_req, res) => res.json({ ok:true, nodes: [] });
app.get('/catalog/tree', catalogTree);
app.get('/api/catalog/tree', catalogTree);

// 404 / error
app.use((req, res) => res.status(404).json({ ok:false, error:'not found' }));
app.use((err, req, res, next) => {
  try { require('./src/utils/logger').logError(err, { path: req.originalUrl }); } catch {}
  res.status(500).json({ ok:false, error:'internal error' });
});

app.listen(PORT, '0.0.0.0', () => console.log(`worker listening on :${PORT}`));
