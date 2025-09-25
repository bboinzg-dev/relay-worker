/* server.js */
'use strict';

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

const { getPool } = require('./db');
const { getFamilies, getBlueprint } = require('./lib/blueprint');
const { classifyFamily, extractByBlueprint } = require('./lib/llm');
const { Storage } = require('@google-cloud/storage');
const { getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject, parseGcsUri } = require('./src/utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('./src/utils/schema');
const { runAutoIngest } = require('./src/pipeline/ingestAuto');

const storage = new Storage();
const pgPool = getPool();
const query = (text, params) => pgPool.query(text, params);


// ───────────────── Cloud Tasks (enqueue next-step) ─────────────────
 const { CloudTasksClient } = require('@google-cloud/tasks');
 const PROJECT_ID       = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
 const TASKS_LOCATION   = process.env.TASKS_LOCATION   || 'asia-northeast3';
 const QUEUE_NAME       = process.env.QUEUE_NAME       || 'ingest-queue';
 const WORKER_TASK_URL  = process.env.WORKER_TASK_URL  || 'https://<YOUR-RUN-URL>/api/worker/ingest/run';
 const TASKS_INVOKER_SA = process.env.TASKS_INVOKER_SA || '';

 // lazy init: gRPC 문제 대비 regional endpoint + REST fallback
 let _tasks = null;
 let _queuePath = null;
 function getTasks() {
   if (!_tasks) {
    // 글로벌 엔드포인트 + REST fallback(HTTP/1)
    _tasks = new CloudTasksClient({ fallback: true });
     _queuePath = _tasks.queuePath(PROJECT_ID, TASKS_LOCATION, QUEUE_NAME);
   }
   return { tasks: _tasks, queuePath: _queuePath };
 }

 async function enqueueIngestRun(payload) {
   const { tasks, queuePath } = getTasks();
     if (!TASKS_INVOKER_SA) throw new Error('TASKS_INVOKER_SA not set');
   const audience = process.env.WORKER_AUDIENCE || new URL(WORKER_TASK_URL).origin;

     // ① 디스패치 데드라인 = 인제스트 예산(기본 120초) + 15초 여유
  const seconds = Math.ceil((Number(process.env.INGEST_BUDGET_MS || 120000) + 15000) / 1000);

  const task = {
    httpRequest: {
      httpMethod: 'POST',
      url: WORKER_TASK_URL,
      headers: { 'Content-Type': 'application/json' },
      body: Buffer.from(JSON.stringify(payload)).toString('base64'),
      ...(TASKS_INVOKER_SA
        ? { oidcToken: { serviceAccountEmail: TASKS_INVOKER_SA, audience } }
        : {}),
    },
    // ② Cloud Tasks에 타깃 응답 대기 한도를 명시(기본 10분 → 135초 내)
   dispatchDeadline: { seconds: Math.min(Math.ceil(seconds), 1800) },
  };

   // (선택) 10초로 RPC 타임아웃 단축 — 실패 시 바로 catch → DB만 FAILED 마킹
   await tasks.createTask({ parent: queuePath, task }, { timeout: 10000 });
 }


const app = express();

/* ---------------- Mount modular routers (keep existing) ---------------- */
try { app.use(require('./server.health'));   console.log('[BOOT] mounted /api/health'); } catch {}
try { app.use(require('./server.optimize')); console.log('[BOOT] mounted /api/optimize/*'); } catch {}
try { app.use(require('./server.checkout')); console.log('[BOOT] mounted /api/checkout/*'); } catch {}
try { app.use(require('./server.bom'));      console.log('[BOOT] mounted /api/bom/*'); } catch {}
try { app.use(require('./server.notify'));   console.log('[BOOT] mounted /api/notify/*'); } catch {}
try { app.use(require('./server.market'));   console.log('[BOOT] mounted /api/listings, /api/purchase-requests, /api/bids'); } catch {}
try { app.use(require('./src/routes/vision.upload')); console.log('[BOOT] mounted /api/vision/guess (upload)'); } catch {}
 // Seeding removed. DB is the single source of truth.
 // If you need to bootstrap locally, run a separate script (see scripts/seed-blueprints.js).


/* NOTE: The parts router already exists in your repo; keep it mounted. */
try { app.use('/api/parts', require('./src/routes/parts')); } catch {}

app.use(bodyParser.json({ limit: '25mb' }));
app.use(bodyParser.urlencoded({ extended: true }));
app.disable('x-powered-by');

/* ---------------- Env / Config ---------------- */
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';

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
// ---------------- Always-on Auth Router (최상단 고정) ----------------
// 프리뷰/개발용 간단 로그인: 어떤 값이든 username/email/id 중 하나만 있으면 JWT 발급
// 프런트는 이 응답의 token을 쿠키(pp_session)로 저장해서 사용합니다.
function issueToken(payload) {
  const id = String(
    payload.username || payload.email || payload.id || payload.idOrEmail || 'user'
  );
  return {
    id,
    token: jwt.sign({ uid: id, username: id }, JWT_SECRET, { expiresIn: '7d' }),
  };
}

function loginHandler(req, res) {
  try {
    const { id, token } = issueToken(req.body || {});
    return res.json({ ok: true, token, user: { username: id } });
  } catch (e) {
    return res.status(400).json({ ok: false, error: String(e?.message || e) });
  }
}

const authRouter = express.Router();
// 헬스(선택)
authRouter.get('/health', (_req, res) => res.json({ ok: true, stub: true }));
// 로그인/로그아웃
authRouter.post('/login', loginHandler);
authRouter.post('/logout', (_req, res) => res.json({ ok: true }));

// ✅ /auth/* 경로로 확정 마운트 (항상 가장 먼저 잡히게)
app.use('/auth', authRouter);

// (구버전 호환) /login 으로 들어오면 같은 핸들러 사용
app.post('/login', loginHandler);


/* ---------------- Upload ---------------- */
const upload = multer({ storage: multer.memoryStorage() });

/* Session helpers (Cloud Run Bearer 또는 pp_session 쿠키 허용) */
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
async function requireSession(req, res, next) {
  const auth = String(req.headers.authorization || '');
  if (/^Bearer\s+.+/i.test(auth)) return next(); // Cloud Run/IAP
  const claims = verifyJwtCookie(req.headers.cookie || '');
  if (claims) { req.user = claims; return next(); }
  return res.status(401).json({ ok: false, error: 'UNAUTHORIZED' });
}

/* ---------------- Health & Env ---------------- */
app.get('/_healthz', (_req, res) => res.type('text/plain').send('ok'));
app.get('/api/health', async (_req, res) => {
  try { await query('SELECT 1'); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ ok:false, error:String(e?.message || e) }); }
});
app.get('/_env', (_req, res) => {
  res.json({
    node: process.version,
    has_db: !!process.env.DATABASE_URL,
  });
});

/* ---------------- Files: upload / signed url / move ---------------- */
app.post(['/api/files/upload', '/files/upload'], upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok:false, error:'file required' });

    const buf = req.file.buffer;
    const sha = crypto.createHash('sha256').update(buf).digest('hex');
    const safe = (req.file.originalname || 'datasheet.pdf').replace(/\s+/g,'_');
    const object = `incoming/${sha}_${Date.now()}_${safe}`;

    const defaultBucket = process.env.GCS_BUCKET
      ? String(process.env.GCS_BUCKET).replace(/^gs:\/\//,'')
      : String(process.env.ASSET_BUCKET || '').replace(/^gs:\/\//,'');
    if (!defaultBucket) return res.status(500).json({ ok:false, error:'GCS_BUCKET not set' });

    await storage.bucket(defaultBucket).file(object).save(buf, {
      contentType: req.file.mimetype || 'application/pdf',
      resumable: false, public: false, validation: false,
    });

     const gcsUri = `gs://${defaultBucket}/${object}`;

 // ✅ 여기부터: 옵션 자동 인제스트 (multer가 form 필드를 req.body로 넣어줌)
    const ingestWanted =
      String(req.body?.ingest || req.query?.ingest || '').trim() === '1';

    if (!ingestWanted) {
      // 평소처럼 업로드 결과만 반환
      return res.json({ ok:true, gcsUri });
    }

    // 폼으로 넘어온 메타(있으면 사용, 없으면 추정)
    const family_slug = req.body?.family_slug || null;
    const brand       = req.body?.brand || null;
    const code        = req.body?.code  || null;
    const series      = req.body?.series || null;
    const displayName = req.body?.display_name || null;

    try {
      const out = await runAutoIngest({
        gcsUri, family_slug, brand, code, series, display_name: displayName,
      });
      // 인제스트까지 완료한 결과 반환
      return res.json({ ok:true, gcsUri, ingest: out });
    } catch (e) {
      // 인제스트 실패해도 업로드는 성공 → 본문에 결과만 첨부
      return res.json({ ok:true, gcsUri, ingest: { ok:false, error: String(e?.message || e) }});
    }
  } catch (e) {
    console.error('[upload]', e);
    return res.status(400).json({ ok:false, error:String(e?.message || e) });
  }
});


app.get('/api/files/signed-url', requireSession, async (req, res) => {
  try {
    const gcsUri = req.query.gcsUri;
    const minutes = Number(req.query.minutes || 15);
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

    const dst = dstGcsUri || canonicalDatasheetPath(
      (process.env.ASSET_BUCKET || process.env.GCS_BUCKET || '').replace(/^gs:\/\//,''),
      family_slug, brand, code
    );

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
    const r = await query(
      'SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug'
    );
    res.json({ items: r.rows });
  } catch (e) {
    console.error(e); res.status(500).json({ ok:false, error:'registry failed' });
  }
});

app.get('/catalog/blueprint/:family', async (req, res) => {
  try {
    const r = await query(`
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

/* ---------------- Parts (compat: relay + generic) ---------------- */
app.get('/parts/detail', async (req, res) => {
  const brand  = (req.query.brand || '').toString();
  const code   = (req.query.code  || '').toString();
  const family = (req.query.family || '').toString().toLowerCase();
  if (!brand || !code) return res.status(400).json({ ok:false, error:'brand & code required' });

  try {
    if (family) {
      const r = await query(`SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`, [family]);
      const table = r.rows[0]?.specs_table;
      if (!table) return res.status(400).json({ ok:false, error:'UNKNOWN_FAMILY' });
      const row = await query(`SELECT * FROM public.${table} WHERE brand_norm = lower($1) AND code_norm = lower($2) LIMIT 1`, [brand, code]);
      return row.rows[0]
        ? res.json({ ok:true, item: row.rows[0] })
        : res.status(404).json({ ok:false, error:'NOT_FOUND' });
    }

    // fallback: unified view if present, else legacy relay view
    try {
      const row = await query(
        `SELECT * FROM public.component_specs WHERE brand_norm = lower($1) AND code_norm = lower($2) LIMIT 1`,
        [brand, code]
      );
      if (row.rows[0]) return res.json({ ok:true, item: row.rows[0] });
    } catch {}
    const row = await query(
      `SELECT * FROM public.relay_specs WHERE brand_norm = lower($1) AND code_norm = lower($2) LIMIT 1`,
      [brand, code]
    );
    return row.rows[0]
      ? res.json({ ok:true, item: row.rows[0] })
      : res.status(404).json({ ok:false, error:'NOT_FOUND' });
  } catch (e) {
    console.error(e); res.status(500).json({ ok:false, error:'detail_failed' });
  }
});

app.get('/parts/search', async (req, res) => {
  const q      = (req.query.q || '').toString().trim();
  const limit  = Math.min(Number(req.query.limit || 20), 100);
  const family = (req.query.family || '').toString().toLowerCase();

  try {
    const text = q ? `%${q.toLowerCase()}%` : '%';

    if (family) {
      const r = await query(`SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`, [family]);
      const table = r.rows[0]?.specs_table;
      if (!table) return res.status(400).json({ ok:false, error:'UNKNOWN_FAMILY' });
      const rows = await query(
        `SELECT id, family_slug, brand, code, display_name,
                width_mm, height_mm, length_mm, image_uri, datasheet_uri, updated_at
           FROM public.${table}
          WHERE brand_norm LIKE $1 OR code_norm LIKE $1
             OR lower(coalesce(series,'')) LIKE $1
             OR lower(coalesce(display_name,'')) LIKE $1
          ORDER BY updated_at DESC
          LIMIT $2`,
        [text, limit]
      );
      return res.json({ ok:true, items: rows.rows });
    }

    // (fallback) unified view if present, else relay view
    try {
      const rows = await query(
        `SELECT id, family_slug, brand, code, display_name,
                width_mm, height_mm, length_mm, image_uri, datasheet_uri, updated_at
           FROM public.component_specs
          WHERE brand_norm LIKE $1 OR code_norm LIKE $1 OR lower(coalesce(display_name,'')) LIKE $1
          ORDER BY updated_at DESC
          LIMIT $2`,
        [text, limit]
      );
      return res.json({ ok:true, items: rows.rows });
    } catch {}
    const rows = await query(
      `SELECT * FROM public.relay_specs
        WHERE brand_norm LIKE $1 OR code_norm LIKE $1 OR lower(series) LIKE $1 OR lower(display_name) LIKE $1
        ORDER BY updated_at DESC
        LIMIT $2`,
      [text, limit]
    );
    return res.json({ ok:true, items: rows.rows });
  } catch (e) {
    console.error(e); res.status(500).json({ ok:false, error:'search_failed' });
  }
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
    const { gcsUri, gcsPdfUri, gcs_uri, gcs_pdf_uri, brand, code, series, display_name, family_slug } = req.body || {};
    const uri = gcsUri || gcsPdfUri || gcs_uri || gcs_pdf_uri;
    if (!uri) return res.status(400).json({ ok:false, error:'gcsUri required' });
    const result = await runAutoIngest({ gcsUri: uri, family_slug, brand, code, series, display_name });
    res.json(result);
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e?.message || e) }); }
});

app.post('/api/worker/ingest', requireSession, async (req, res) => {
  const startedAt = Date.now();
  const taskName  = req.get('X-Cloud-Tasks-TaskName') || null;
  const retryCnt  = Number(req.get('X-Cloud-Tasks-TaskRetryCount') || 0);
  const body = parseIngestBody(req);
  const series = req.body?.series || null;
  const displayName = req.body?.display_name || req.body?.displayName || null;
  try {
    const uri = body.gcs_uri;
    if (!uri || !/^gs:\/\//i.test(uri)) {
      await query(
        `INSERT INTO public.ingest_run_logs (gcs_uri, status, error, task_name, retry_count, uploader_id, content_type)
         VALUES ($1,'FAILED',$2,$3,$4,$5,$6)`,
        [uri || 'n/a', 'gcs_uri required (gs://...)', taskName, retryCnt, body.uploader_id, body.content_type]
      );
      return res.status(202).json({ ok: true, accepted: false });
    }

    const { rows:logRows } = await query(
      `INSERT INTO public.ingest_run_logs (task_name, retry_count, gcs_uri, status, uploader_id, content_type)
       VALUES ($1,$2,$3,'PROCESSING',$4,$5) RETURNING id`,
      [taskName, retryCnt, uri, body.uploader_id, body.content_type]
    );
    const runId = logRows[0]?.id;

    // ✅ 1) 즉시 ACK → Cloud Tasks는 이 시점에 "완료"로 처리(재시도 루프 종료)
    res.status(202).json({ ok: true, run_id: runId, accepted: true });

    // ▶ 2) 다음 Cloud Tasks 요청으로 실행을 넘김(체인) — 응답 보낸 뒤이므로 절대 다시 res.* 호출 금지
    enqueueIngestRun({ runId, gcsUri: uri, brand: body.brand, code: body.code, series, display_name: displayName, family_slug: body.family_slug })
      .catch(async (err) => {
        // enqueue 실패 시에도 응답은 이미 보냈으므로 DB만 FAILED로 마킹
        try {
          await query(
            `UPDATE public.ingest_run_logs
                SET finished_at = now(),
                    duration_ms = $2,
                    status = 'FAILED',
                    error = $3,
                    updated_at = now()
              WHERE id = $1`,
            [ runId, Date.now() - startedAt, `enqueue failed: ${String(err?.message || err)}` ]
          );
        } catch (_) {}
        console.error('[ingest enqueue failed]', err?.message || err);
      });

  } catch (e) {
 // 여기로 들어왔다면 아직 202를 보내기 전일 수도 있으므로, 응답 전/후 모두 안전하도록 처리
    // 1) DB 상태 갱신
    try {
      await query(
        `UPDATE public.ingest_run_logs
           SET finished_at = now(),
               duration_ms = $2,
               status = 'FAILED',
               error = $3,
               updated_at = now()
         WHERE task_name = $1
           AND status = 'PROCESSING'
         ORDER BY started_at DESC
         LIMIT 1`,
        [ taskName, Date.now()-startedAt, String(e?.message || e) ]
      );
    } catch (_) {}
    // 2) 아직 응답을 보내지 않았다면 500, 이미 보냈다면 로그만
    if (!res.headersSent) {
      console.error('[ingest 500]', { error: e?.message });
      return res.status(500).json({ ok:false, error:String(e?.message || e) });
    } else {
      console.error('[ingest post-ack error]', e?.message || e);
    }
  }
});

// ---- 유틸
const norm = s => (s || '').toString().trim().toLowerCase().replace(/\s+/g, '').replace(/[^a-z0-9._-]/g, '');

// 공통 바디 파서
function parseIngestBody(req) {
  const b = req.body || {};
  return {
    gcs_uri: b.gcs_uri || b.gcsUri || b.gcs_pdf_uri || b.gcsPdfUri || null,
    uploader_id: b.uploader_id || b.uploaderId || 'anonymous',
    content_type: b.content_type || b.mime || 'application/pdf',
    run_id: b.run_id || b.runId || null,
    family_slug: b.family_slug || null,
    brand: b.brand || null,
    code: b.code || null,
  };
}

// ---- ingest_run_logs 테이블 필요시 보장(앱 부팅시에 1회 호출)
async function ensureIngestRunLogs() {
  await query(`
    CREATE TABLE IF NOT EXISTS public.ingest_run_logs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      gcs_uri TEXT NOT NULL,
      family_slug TEXT,
      specs_table TEXT,
      status TEXT NOT NULL DEFAULT 'PENDING',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      started_at TIMESTAMPTZ,
      finished_at TIMESTAMPTZ,
      error TEXT,
      uploader_id TEXT,
      content_type TEXT,
      task_name TEXT,
      retry_count INTEGER,
      brand TEXT,
      brand_norm TEXT,
      code TEXT,
      code_norm TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      duration_ms INTEGER
    );
  `);
  await query(`CREATE INDEX IF NOT EXISTS idx_ingest_run_logs_created ON public.ingest_run_logs(created_at DESC);`);
}

// ---- GCS 다운로드
async function downloadBytes(gcsUri) {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(gcsUri);
  if (!m) throw new Error(`Bad gcs uri: ${gcsUri}`);
  const [bucket, name] = [m[1], m[2]];
  const [buf] = await storage.bucket(bucket).file(name).download();
  return buf;
}

// ---- 업서트 (specs_table 과 (brand_norm, code_norm) 기준 멱등)
async function upsertComponent({ family, specsTable, brand, code, datasheetUri, values }) {
const pool = pgPool;

  if (process.env.NO_SCHEMA_ENSURE !== '1') {
    await pool.query('SELECT public.ensure_specs_table($1)', [family]);
  }

  const brandNorm = norm(brand);
  const codeNorm = norm(code);

  const cols = Object.keys(values || {});
  const dbCols = cols.map(c => `"${c}"`);
  const dbVals = cols.map((_, i) => `$${i + 7}`);
  const insertCols = dbCols.length ? `, ${dbCols.join(',')}` : '';
  const insertVals = dbVals.length ? `, ${dbVals.join(',')}` : '';
  const updateCols = dbCols.length ? `${dbCols.map(c => `${c} = EXCLUDED.${c}`).join(', ')}, ` : '';

  const sql = `
    INSERT INTO public.${specsTable}
      (id, family_slug, brand, brand_norm, code, code_norm, datasheet_uri${insertCols})
    VALUES
      (gen_random_uuid(), $1, $2, $3, $4, $5, $6${insertVals})
    ON CONFLICT (brand_norm, code_norm)
    DO UPDATE SET
      brand = EXCLUDED.brand,
      datasheet_uri = EXCLUDED.datasheet_uri,
      ${updateCols}updated_at = now()
    RETURNING id;
  `;
  const params = [
    family, brand, brandNorm, code, codeNorm, datasheetUri,
    ...cols.map(k => values[k]),
  ];
  await pool.query(sql, params);
}

// ▶ 체인 실행 엔드포인트: 3-스텝 멱등 파이프라인
app.post('/api/worker/ingest/run', async (req, res) => {
  const parsedDeadline = parseInt(
    req.body?.deadline_ms || process.env.INGEST_DEADLINE_MS || '135000',
    10
  );
  const deadlineMs = Number.isFinite(parsedDeadline) ? parsedDeadline : 135000;
  const body = parseIngestBody(req);
  const runHint = body.run_id || req.body?.runId || 'n/a';
  console.log(`[ingest-run] killer armed at ${deadlineMs}ms for runId=${runHint}`);
  const timer = setTimeout(() => {
    console.warn(`[ingest-run] local deadline hit`);
    try { res.status(504).json({ error: 'deadline' }); } catch (e) {}
  }, deadlineMs);

  let rid = null;

  try {
    const gcsUri = body.gcs_uri;
    const runId = body.run_id || req.body?.runId || null;
    if (!gcsUri) {
      clearTimeout(timer);
      return res.status(400).json({ error: 'gcs_uri required' });
    }

    const { rows: runRows } = await query(`
      INSERT INTO public.ingest_run_logs (id, gcs_uri, status, started_at, updated_at)
      VALUES (COALESCE($1::uuid, gen_random_uuid()), $2, 'RUNNING', now(), now())
      ON CONFLICT (id) DO UPDATE SET status='RUNNING', started_at=COALESCE(ingest_run_logs.started_at, now()), updated_at=now()
      RETURNING id`, [runId, gcsUri]);
    rid = runRows[0].id;

    const pdfBytes = await downloadBytes(gcsUri);

    const families = await getFamilies();
    const allowed = families.map(f => f.family_slug);
    const fam = await classifyFamily({ pdfBytes, allowedFamilies: allowed });
    const family = fam.family_slug;

    const bp = await getBlueprint(family);
    const values = await extractByBlueprint({ pdfBytes, family, fields: bp });

    const match = families.find(f => f.family_slug === family);
    if (!match) throw new Error(`Unknown family ${family}`);
    const specsTable = match.specs_table;

    await upsertComponent({
      family,
      specsTable,
      brand: fam.brand || values.brand || 'unknown',
      code: fam.code || values.code || 'unknown',
      datasheetUri: gcsUri,
      values,
    });

    await query(`UPDATE public.ingest_run_logs
      SET status='SUCCEEDED', family_slug=$2, final_table=$3, brand=$4, brand_norm=$5, code=$6, code_norm=$7, finished_at=now(), updated_at=now(), error=NULL
      WHERE id=$1`,
      [rid, family, specsTable, fam.brand || null, norm(fam.brand), fam.code || null, norm(fam.code)]
    );

    clearTimeout(timer);
    return res.status(200).json({ ok: true, runId: rid, family });
  } catch (e) {
    console.error('[ingest-run] error', e);
    try {
      await query(`UPDATE public.ingest_run_logs SET status='FAILED', error=$2, updated_at=now(), finished_at=now() WHERE id=$1`,
        [rid || body.run_id || req.body?.runId || null, String(e?.message || e)]);
    } catch {}
    clearTimeout(timer);
    return res.status(500).json({ error: String(e?.message || e) });
  }
});


/* ---------------- 404 / error ---------------- */
app.use((req, res) => res.status(404).json({ ok:false, error:'not found' }));
app.use((err, req, res, next) => {
  try { require('./src/utils/logger').logError(err, { path: req.originalUrl }); } catch {}
  res.status(500).json({ ok:false, error:'internal error' });
});

/* ---------------- Boot-time setup ---------------- */
(async () => {
  try {
    await query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
    await query(`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`);
  } catch (e) {
    console.warn('[BOOT] ensure extensions failed:', e?.message || e);
  }
})();

ensureIngestRunLogs()
  .then(() => console.log('[BOOT] ensured ingest_run_logs'))
  .catch(e => console.warn('[BOOT] ensure ingest_run_logs failed:', e?.message || e));

/* ---------------- Listen ---------------- */
app.listen(PORT, '0.0.0.0', () => console.log(`worker listening on :${PORT}`));

module.exports = app;
