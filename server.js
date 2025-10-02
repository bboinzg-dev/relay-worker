/* server.js */
'use strict';

// ───────── 외부콜 차단 플래그 (배포 시 EXT_CALLS_OFF=1 이면 부팅 중 외부 HTTPS 호출 스킵)
const EXT_CALLS_OFF = process.env.EXT_CALLS_OFF === '1';

process.on('uncaughtException', (e) => {
  console.error('[FATAL][uncaughtException]', e?.message, e?.stack?.split('\n').slice(0, 4).join(' | '));
  process.exit(1);
});
process.on('unhandledRejection', (e) => {
  console.error('[FATAL][unhandledRejection]', e);
  process.exit(1);
});

// 필수 env 스모크 로그(민감값 제외)
(() => {
  const pick = (k) => (process.env[k] || '').toString();
  console.log('[BOOT env check]', {
    GCP_PROJECT_ID: !!pick('GCP_PROJECT_ID'),
    VERTEX_LOCATION: pick('VERTEX_LOCATION'),
    DOCAI_PROCESSOR_ID: !!pick('DOCAI_PROCESSOR_ID'),
    GCS_BUCKET: pick('GCS_BUCKET'),
    QUEUE_NAME: pick('QUEUE_NAME'),
    TASKS_LOCATION: pick('TASKS_LOCATION'),
    GEMINI_MODEL_CLASSIFY: pick('GEMINI_MODEL_CLASSIFY'),
    GEMINI_MODEL_EXTRACT: pick('GEMINI_MODEL_EXTRACT'),
  });
})();

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

// 1) DB 모듈: 로드 실패해도 서버는 떠야 함
let db;
try {
  db = require('./db');                    // 루트 경로
} catch (e1) {
  try { db = require('./src/utils/db'); }  // 예전 경로 호환
  catch (e2) {
    console.error('[BOOT] db load failed:', e2?.message || e1?.message);
    db = { query: async () => { throw new Error('DB_UNAVAILABLE'); } };
  }
}
const { getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject, storage, parseGcsUri } = require('./src/utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('./src/utils/schema');
// 3) ingestAuto: 부팅 시점에 절대 로드하지 말고, 요청 시점에만 로드
let __INGEST_MOD__ = null;
function getIngest() {
  if (__INGEST_MOD__) return __INGEST_MOD__;
  try {
    __INGEST_MOD__ = require('./src/pipeline/ingestAuto');
  } catch (e) {
    console.error('[INGEST] module load failed:', e?.message || e);
    __INGEST_MOD__ = {
      runAutoIngest: async () => { throw new Error('INGEST_MODULE_LOAD_FAILED'); },
      persistProcessedData: async () => { throw new Error('INGEST_MODULE_LOAD_FAILED'); },
    };
  }
  return __INGEST_MOD__;
}
const { enqueueIngest } = require('./src/utils/enqueue');



// ───────────────── Cloud Tasks (enqueue next-step) ─────────────────
// 2) Cloud Tasks: 런타임에 없으면 비활성화
let CloudTasksClient;
try { ({ CloudTasksClient } = require('@google-cloud/tasks')); }
catch (e) { console.warn('[BOOT] @google-cloud/tasks unavailable:', e?.message || e); }
const PROJECT_ID       = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const TASKS_LOCATION   = process.env.TASKS_LOCATION   || 'asia-northeast3';
const QUEUE_NAME       = process.env.QUEUE_NAME       || 'ingest-queue';
// step 라우트 폐지 → ingest 하나로 통일
const WORKER_TASK_URL = process.env.WORKER_TASK_URL || process.env.WORKER_STEP_URL || 'https://<YOUR-RUN-URL>/api/worker/ingest';
const TASKS_INVOKER_SA = process.env.TASKS_INVOKER_SA || '';

// ⚠️ 외부 API(예: Vertex/HTTP) 가능성이 있는 부팅 태스크는 가드 안에서만 실행
//   → 아래 부팅 IIFE 내부의  if (!EXT_CALLS_OFF)  블록으로 이동
//try { require('./src/tasks/embedFamilies').run().catch(console.error); } catch {}

// lazy init: gRPC 문제 대비 regional endpoint + REST fallback
let _tasks = null;
let _queuePath = null;
function getTasks() {
  if (!_tasks) {
    if (!CloudTasksClient) throw new Error('@google-cloud/tasks unavailable');
    // 글로벌 엔드포인트 + REST fallback(HTTP/1)
    _tasks = new CloudTasksClient({ fallback: true });
    _queuePath = _tasks.queuePath(PROJECT_ID, TASKS_LOCATION, QUEUE_NAME);
  }
  return { tasks: _tasks, queuePath: _queuePath };
}

async function enqueueIngestTask(payload = {}) {
  const { tasks, queuePath } = getTasks();
  if (!TASKS_INVOKER_SA) throw new Error('TASKS_INVOKER_SA not set');
  const audience = process.env.WORKER_AUDIENCE || new URL(WORKER_TASK_URL).origin;

  const bodyPayload = {
    fromTasks: true,
    payload,
  };
  const body = Buffer.from(JSON.stringify(bodyPayload)).toString('base64');

  const nowSeconds = Math.floor(Date.now() / 1000);
  const deadlineSeconds = Number(process.env.TASKS_DEADLINE_SEC || 150);
  const delaySeconds = Number(process.env.TASKS_DELAY_SEC || 5);
  const maxAttempts = Number(process.env.TASKS_MAX_ATTEMPTS || 12);
  const minBackoffSeconds = Number(process.env.TASKS_MIN_BACKOFF_SEC || 1);
  const maxBackoffSeconds = Number(process.env.TASKS_MAX_BACKOFF_SEC || 60);
  const maxDoublings = Number(process.env.TASKS_MAX_DOUBLINGS || 4);

  const task = {
    httpRequest: {
      httpMethod: 'POST',
      url: WORKER_TASK_URL,            // ★ ingest 하나로 통일
      headers: { 'Content-Type': 'application/json' },
      body,
      ...(TASKS_INVOKER_SA ? { oidcToken: { serviceAccountEmail: TASKS_INVOKER_SA, audience } } : {}),
    },
    // gRPC Duration 객체 (REST가 아님)
    dispatchDeadline: { seconds: deadlineSeconds, nanos: 0 },
    // 콜드스타트/일시 에러 완충
    scheduleTime: { seconds: nowSeconds + delaySeconds },
    retryConfig: {
      maxAttempts,
      minBackoff: { seconds: minBackoffSeconds },
      maxBackoff: { seconds: maxBackoffSeconds },
      maxDoublings,
    },
  };

  await tasks.createTask({ parent: queuePath, task }, { timeout: 10000 });
}


const app = express();

// ✅ 항상 찍히는 부팅 로그 + 간단 헬스/정보
console.log('[BOOT] server.js starting', {
  file: __filename,
  node: process.version,
  ROUTE_DEBUG: process.env.ROUTE_DEBUG || null,
  revision: process.env.K_REVISION || null,
});

// 가장 가벼운 핑
app.get('/_ping', (_req, res) => res.json({ ok: true, rev: process.env.K_REVISION || null }));

// 현재 실행 중인 엔트리/디렉토리 확인 (정말 server.js가 실행되는지 확인)
app.get('/_whoami', (_req, res) => {
  try {
    res.json({
      main: require.main && require.main.filename,
      dir: __dirname,
      ROUTE_DEBUG: process.env.ROUTE_DEBUG || null
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});


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
// --- AI routes mount (export 타입 자동 처리) ---
try {
  const ai = require('./server.ai');
  if (ai && typeof ai === 'object') {
    // server.ai.js 가 express.Router()를 export 하는 형태
    app.use('/api', ai);  // ==> 최종 경로: /api/ai/...
    console.log('[BOOT] mounted AI routes at /api/* (router export)');
  } else if (typeof ai === 'function') {
    // 만약 server.ai.js 가 (app)=>{ app.post('/api/ai/resolve', ...) } 로 export 한다면
    ai(app);
    console.log('[BOOT] mounted AI routes via function(export)');
  } else {
    console.warn('[BOOT] server.ai export type not supported:', typeof ai);
  }
} catch (e) {
  console.error('[BOOT] ai mount failed', e?.message || e);
  // 🔰 Fallback: server.ai.js 로딩 실패/누락 시에도 즉시 동작하도록 최소 라우트 제공
  const express = require('express');
  const fb = express.Router();
  fb.get('/ai/ping', (_req, res) => res.json({ ok: true, fallback: true }));
  fb.get('/ai/resolve', (req, res) => res.json({ ok: true, echo: String(req.query?.q || '') }));
  fb.post('/ai/resolve', (req, res) => res.json({ ok: true, echo: String((req.body && req.body.q) || '') }));
  app.use('/api', fb);
  console.warn('[BOOT] fallback AI routes mounted at /api/*');
}

/* ---------------- Mount modular routers (after global middleware) ---------------- */
try { app.use(require('./server.optimize')); console.log('[BOOT] mounted /api/optimize/*'); } catch {}
try { app.use(require('./server.checkout')); console.log('[BOOT] mounted /api/checkout/*'); } catch {}
try { app.use(require('./server.bom'));      console.log('[BOOT] mounted /api/bom/*'); } catch {}
try { app.use(require('./server.notify'));   console.log('[BOOT] mounted /api/notify/*'); } catch {}
try { app.use(require('./server.market'));   console.log('[BOOT] mounted /api/listings, /api/purchase-requests, /api/bids'); } catch {}
try { app.use(require('./src/routes/vision.upload')); console.log('[BOOT] mounted /api/vision/guess (upload)'); } catch {}
try { app.use(require('./server.ingest.status')); console.log('[BOOT] mounted /api/ingest/:id'); } catch {}

// 인라인 AI 라우터(간소 버전)는 제거 — server.ai.js 하나만 유지

/* NOTE: The parts router already exists in your repo; keep it mounted. */
try { app.use('/api/parts', require('./src/routes/parts')); } catch {}

// ✅ ALWAYS-ON 디버그 엔드포인트 (환경변수와 무관하게 항상 열림)
app.get('/_routes', (req, res) => {
  try {
    const list = [];
    app._router.stack.forEach((m) => {
      if (m.route && m.route.path) {
        const methods = Object.keys(m.route.methods).join(',').toUpperCase();
        list.push(`${methods.padEnd(6)} ${m.route.path}`);
      } else if (m.name === 'router' && m.handle?.stack) {
        m.handle.stack.forEach((h) => {
          const p = h.route?.path;
          const ms = h.route ? Object.keys(h.route.methods).join(',').toUpperCase() : '';
          if (p) list.push(`${ms.padEnd(6)} ${m.regexp?.toString() || ''}${p}`);
        });
      }
    });
    res.type('text/plain').send(list.join('\n') || 'no-routes');
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});
console.log('[BOOT] ALWAYS-ON route debug at /_routes');

// 🔍 컨테이너 내 파일 확인: server*.js 포함 여부 즉시 점검
const fs = require('fs');
app.get('/_ls', (_req, res) => {
  try {
    const here = fs.readdirSync(__dirname).sort();
    res.json({ dir: __dirname, files: here.filter(f => f.startsWith('server')) });
  } catch (e) {
    res.status(500).json({ ok:false, error: String(e?.message || e) });
  }
});
console.log('[BOOT] ALWAYS-ON file lister at /_ls');


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
      const overrides = {
        brand: brand || null,
        series: series || null,
      };
      const out = await getIngest().runAutoIngest({
        gcsUri,
        family_slug,
        brand,
        code,
        series,
        display_name: displayName,
        overrides,
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
    const r = await db.query(
      'SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug'
    );
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

/* ---------------- Parts (compat: relay + generic) ---------------- */
app.get('/parts/detail', async (req, res) => {
  const brand  = (req.query.brand || '').toString();
  const code   = (req.query.code  || '').toString();
  const family = (req.query.family || '').toString().toLowerCase();
  if (!brand || !code) return res.status(400).json({ ok:false, error:'brand & code required' });

  try {
    if (family) {
      const r = await db.query(
        `SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`,
        [family]
      );
      const table = r.rows[0]?.specs_table;
      if (!table) return res.status(400).json({ ok:false, error:'UNKNOWN_FAMILY' });
      if (!/^[a-zA-Z0-9_]+$/.test(table)) {
        console.error('[parts/detail] invalid table name', { table });
        return res.status(500).json({ ok:false, error:'INVALID_TABLE' });
      }
      const row = await db.query(
        `SELECT * FROM public.${table} WHERE brand_norm = lower($1) AND code_norm = lower($2) LIMIT 1`,
        [brand, code]
      );
      return row.rows[0]
        ? res.json({ ok:true, item: row.rows[0] })
        : res.status(404).json({ ok:false, error:'NOT_FOUND' });
    }

    // fallback: unified view if present, else legacy relay view
    try {
      const row = await db.query(
        `SELECT * FROM public.component_specs WHERE brand_norm = lower($1) AND code_norm = lower($2) LIMIT 1`,
        [brand, code]
      );
      if (row.rows[0]) return res.json({ ok:true, item: row.rows[0] });
    } catch {}
    const row = await db.query(
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
      const r = await db.query(`SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`, [family]);
      if (!r.rows[0]?.specs_table) return res.status(400).json({ ok:false, error:'UNKNOWN_FAMILY' });
      const rows = await db.query(
        `SELECT id, family_slug, brand, code, display_name,
                image_uri, datasheet_uri, updated_at
           FROM public.component_specs
          WHERE family_slug = $3
            AND (brand_norm LIKE $1 OR code_norm LIKE $1 OR lower(coalesce(display_name,'')) LIKE $1)
          ORDER BY updated_at DESC
          LIMIT $2`,
        [text, limit, family]
      );
      return res.json({ ok:true, items: rows.rows });
    }

    // (fallback) unified view if present, else relay view
    try {
      const rows = await db.query(
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
    const rows = await db.query(
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
      brand,
      code,
      pn: code,
      series,
      display_name: display_name || (brand ? `${brand} ${code}` : null),
      family_slug,
      datasheet_uri,
      cover,
      source_gcs_uri,
      raw_json,
      ...values,
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
        brand: it.brand,
        code: it.code,
        pn: it.pn || it.code,
        series: it.series,
        display_name: it.display_name || (it.brand && it.code ? `${it.brand} ${it.code}` : null),
        family_slug: it.family_slug,
        datasheet_uri: it.datasheet_uri,
        cover: it.cover,
        source_gcs_uri: it.source_gcs_uri,
        raw_json: it.raw_json || null,
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
    const overrides = {
      brand: brand || null,
      series: series || null,
    };
    const result = await getIngest().runAutoIngest({
      gcsUri: uri,
      family_slug,
      brand,
      code,
      series,
      display_name,
      overrides,
    });
    res.json(result);
  } catch (e) { console.error(e); res.status(400).json({ ok:false, error:String(e?.message || e) }); }
});

function pickFirstString(...values) {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function getTaskContext(req, phase) {
  const headerName = req.get('X-CloudTasks-TaskName') || req.get('X-Cloud-Tasks-TaskName') || null;
  const retryHeader = req.get('X-CloudTasks-TaskRetryCount') || req.get('X-Cloud-Tasks-TaskRetryCount');
  const parsedRetry = Number(retryHeader);
  return {
    taskName: headerName || `phase:${phase}`,
    retryCount: Number.isFinite(parsedRetry) ? parsedRetry : 0,
  };
}

async function markRunningState({ runId, gcsUri, taskName, retryCount }) {
  const safeRetryCount = Number.isFinite(retryCount) ? retryCount : 0;
  const update = await db.query(
    `UPDATE public.ingest_run_logs
        SET status = 'RUNNING',
            task_name = $2,
            retry_count = $3,
            gcs_uri = COALESCE($4, gcs_uri),
            run_id = COALESCE(run_id, $5)
            error_message = NULL,
            finished_at = NULL,
            duration_ms = NULL,
            final_table = NULL,
            final_family = NULL,
            final_brand = NULL,
            final_code = NULL,
            final_datasheet = NULL
      WHERE id = $1`,
    [runId, taskName || null, safeRetryCount, gcsUri || null, runId]
  );
  if (!update.rowCount) {
    await db.query(
      `INSERT INTO public.ingest_run_logs (id, run_id, task_name, retry_count, gcs_uri, status)
         VALUES ($1,$2,$3,$4,$5,'RUNNING')
         ON CONFLICT (id) DO NOTHING`,
      [runId, runId, taskName || null, safeRetryCount, gcsUri || null]
    );
  }
}

async function markRunningOrInsert(context) {
  return markRunningState(context);
}

async function markProcessing(context) {
  return markRunningState(context);
}

async function markPersisting(context) {
  return markRunningState(context);
}

async function markRunning(context) {
  return markRunningState(context);
}

async function markFailed({ runId, taskName, retryCount, error, durationMs }) {
  const errMsg = String(error || 'ingest_failed');
  const safeRetryCount = Number.isFinite(retryCount) ? retryCount : 0;
  const ms = Number.isFinite(durationMs) ? durationMs : 0;
  try {
    await db.query(
      `UPDATE public.ingest_run_logs
          SET finished_at = now(),
              duration_ms = $2,
              status = 'FAILED',
              task_name = $3,
              retry_count = $4,
              error_message = $5
        WHERE id = $1`,
      [runId, ms, taskName || null, safeRetryCount, errMsg]
    );
  } catch (err) {
    console.error('[ingest markFailed]', err?.message || err);
  }
}

async function markSucceeded({ runId, taskName, retryCount, result, gcsUri, durationMs, meta }) {
  const safeRetryCount = Number.isFinite(retryCount) ? retryCount : 0;
  const ms = Number.isFinite(durationMs) ? durationMs : 0;
  const family = result?.family || result?.family_slug || meta?.family_slug || meta?.family || null;
  const brand = result?.brand || meta?.brand || null;
  const code = (Array.isArray(result?.codes) ? result.codes[0] : result?.code) || meta?.code || null;
  const datasheet = result?.datasheet_uri || gcsUri || meta?.datasheet_uri || null;
  try {
    await db.query(
      `UPDATE public.ingest_run_logs
          SET finished_at = now(),
              duration_ms = $2,
              status = 'SUCCEEDED',
              task_name = $3,
              retry_count = $4,
              final_table = $5,
              final_family = $6,
              final_brand = $7,
              final_code  = $8,
              final_datasheet = $9,
              error_message = NULL
        WHERE id = $1`,
      [ runId, ms, taskName || null, safeRetryCount,
        result?.specs_table || null,
        family,
        brand,
        code,
        datasheet ]
    );
  } catch (err) {
    console.error('[ingest markSucceeded]', err?.message || err);
  }
}

app.post('/api/worker/ingest', requireSession, async (req, res) => {
  const rawBody = (req.body && typeof req.body === 'object') ? req.body : {};
  const payload = rawBody.fromTasks && rawBody.payload && typeof rawBody.payload === 'object'
    ? rawBody.payload
    : rawBody;

  const phaseInput = String(payload.phase || rawBody.phase || 'start').toLowerCase();
  const knownPhases = new Set(['start', 'process', 'persist']);
  const phase = knownPhases.has(phaseInput) ? phaseInput : 'start';
  const runId = pickFirstString(payload.runId, payload.run_id, rawBody.runId, rawBody.run_id) || generateRunId();
  const gcsUri = pickFirstString(
    payload.gcsUri,
    payload.gcs_uri,
    payload.gsUri,
    payload.gcsPdfUri,
    payload.gcs_pdf_uri,
    payload.uri,
    payload.url,
    rawBody.gcsUri,
    rawBody.gcs_uri,
    rawBody.gsUri,
    rawBody.gcsPdfUri,
    rawBody.uri,
    rawBody.url
  );

  if (!res.headersSent) {
    res.status(202).json({ ok: true, run_id: runId, runId, phase });
  }

  const { taskName, retryCount } = getTaskContext(req, phase);

  setImmediate(async () => {
    const startedAt = Date.now();
    const baseContext = { runId, gcsUri, taskName, retryCount };

    try {
      if (phase === 'start') {
        await markRunningOrInsert(baseContext);

        if (!gcsUri || !/^gs:\/\//i.test(gcsUri)) {
          throw new Error('gcsUri required');
        }

        const overrideBrand = payload?.overrides?.brand ?? payload?.brand ?? null;
        const overrideSeries = payload?.overrides?.series ?? payload?.series ?? null;
        const nextOverrides = {
          ...(payload?.overrides || {}),
          brand: overrideBrand,
          series: overrideSeries,
        };

        const nextPayload = {
          runId,
          run_id: runId,
          gcsUri,
          gcs_uri: gcsUri,
          family_slug: payload?.family_slug ?? null,
          brand: payload?.brand ?? null,
          code: payload?.code ?? null,
          series: payload?.series ?? null,
          display_name: payload?.display_name ?? null,
          uploader_id: payload?.uploader_id ?? null,
          overrides: nextOverrides,
          phase: 'process',
        };

        try {
          await enqueueIngest(nextPayload);
        } catch (err) {
          console.error(
            '[enqueue error]',
            err?.code || err?.response?.status || err?.message,
            err?.response?.data ? JSON.stringify(err.response.data) : (err?.details || '')
          );
          throw new Error(`enqueue failed: ${String(err?.message || err)}`);
        }

        return;
      }

      if (phase === 'process') {
        await markProcessing(baseContext);

        const overrideBrand = payload?.overrides?.brand ?? payload?.brand ?? null;
        const overrideSeries = payload?.overrides?.series ?? payload?.series ?? null;
        const nextOverrides = {
          ...(payload?.overrides || {}),
          brand: overrideBrand,
          series: overrideSeries,
        };

        const result = await getIngest().runAutoIngest({
          ...payload,
          runId,
          run_id: runId,
          gcsUri,
          gcs_uri: gcsUri,
          skipPersist: true,
          overrides: nextOverrides,
        });

        const processed = result?.processed;
        if (!processed || !Array.isArray(processed.records)) {
          throw new Error('process_no_records');
        }

        await markRunning(baseContext);

        const nextPayload = {
          runId,
          run_id: runId,
          gcsUri,
          gcs_uri: gcsUri,
          family_slug: payload?.family_slug ?? null,
          brand: payload?.brand ?? null,
          code: payload?.code ?? null,
          series: payload?.series ?? null,
          display_name: payload?.display_name ?? null,
          uploader_id: payload?.uploader_id ?? null,
          phase: 'persist',
          processed,
          overrides: nextOverrides,
        };

        try {
          await enqueueIngest(nextPayload);
        } catch (err) {
          console.error(
            '[enqueue error]',
            err?.code || err?.response?.status || err?.message,
            err?.response?.data ? JSON.stringify(err.response.data) : (err?.details || '')
          );
          throw new Error(`persist enqueue failed: ${String(err?.message || err)}`);
        }
        return;
      }

      if (phase === 'persist') {
        await markPersisting(baseContext);

        const overrideBrand = payload?.overrides?.brand ?? payload?.brand ?? null;
        const overrideSeries = payload?.overrides?.series ?? payload?.series ?? null;

        const out = await getIngest().persistProcessedData(payload?.processed || {}, {
          brand: overrideBrand,
          code: payload?.code ?? null,
          series: overrideSeries,
          display_name: payload?.display_name ?? null,
        });

        const failureReasons = new Set(Array.isArray(out?.reject_reasons) ? out.reject_reasons : []);
        const warningReasons = new Set(Array.isArray(out?.warnings) ? out.warnings : []);

        if (!out?.ok) {
          const reasonList = Array.from(new Set([...failureReasons, ...warningReasons]));
          const message = reasonList.length ? reasonList.join(',') : 'ingest_rejected';
          await markFailed({
            ...baseContext,
            error: message,
            durationMs: out?.ms ?? (Date.now() - startedAt),
          });
          return;
        }

        await markSucceeded({
          ...baseContext,
          result: out,
          gcsUri,
          durationMs: out?.ms ?? (Date.now() - startedAt),
          meta: payload,
        });
        return;
      }

      console.warn('[ingest] unknown phase', { phase, runId, taskName });
      await markFailed({ ...baseContext, error: 'unknown_phase', durationMs: Date.now() - startedAt });
    } catch (err) {
      console.error('[ingest async error]', err?.message || err);
      try {
        await markFailed({
          ...baseContext,
          error: err?.message || err,
          durationMs: Date.now() - startedAt,
        });
      } catch (innerErr) {
        console.error('[ingest async error][markFailed]', innerErr?.message || innerErr);
      }
    }
  });
});

async function seedManufacturerAliases() {
  try {
    const { rows } = await db.query(
      `SELECT lower(column_name) AS column
         FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'manufacturer_alias'`
    );
    const available = new Set(rows.map((r) => r.column));
    if (!available.has('brand') || !available.has('alias')) return;

    const seeds = [
      { brand: 'Panasonic', alias: 'Matsushita' },
      { brand: 'OMRON', alias: 'Omron Corporation' },
      { brand: 'TE Connectivity', alias: 'Tyco Electronics' },
      { brand: 'Finder', alias: 'Finder Relays' },
      { brand: 'Schneider Electric', alias: 'Square D' },
    ];

    for (const { brand, alias } of seeds) {
      const brandNorm = brand.toLowerCase();
      await db.query(
        `INSERT INTO public.manufacturer_alias (brand, alias, brand_norm)
         VALUES ($1,$2,$3)
         ON CONFLICT DO NOTHING`,
        [brand, alias, brandNorm]
      );
      await db.query(
        `INSERT INTO public.manufacturer_alias (brand, alias, brand_norm)
         VALUES ($1,'',$2)
         ON CONFLICT DO NOTHING`,
        [brand, brandNorm]
      );
    }
  } catch (err) {
    console.warn('[BOOT] seed manufacturer_alias skipped:', err?.message || err);
  }
}

async function seedExtractionRecipe() {
  try {
    const { rows } = await db.query(
      `SELECT lower(column_name) AS column
         FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'extraction_recipe'`
    );
    const available = new Set(rows.map((r) => r.column));
    if (!available.size) return;

    const payload = {};
    if (available.has('slug')) payload.slug = 'generic-universal';
    if (available.has('name')) payload.name = 'Generic Datasheet Extraction';
    if (available.has('description')) payload.description = 'Universal recipe for multi-brand datasheet parsing without series-specific assumptions.';
    if (available.has('family')) payload.family = null;
    if (available.has('is_active')) payload.is_active = true;
    if (available.has('rules_json')) {
      payload.rules_json = JSON.stringify({
        steps: [
          { action: 'collect_part_number_candidates', options: { allow_series_hint: false } },
          { action: 'filter_tokens', options: { blacklist: ['sample', 'example', 'typical'] } },
          { action: 'normalize_units', options: { prefer: 'si' } },
          { action: 'parse_numbers', options: { tolerant: true, preserve_range: true } },
        ],
      });
    }

    const entries = Object.entries(payload);
    if (!entries.length) return;

    const columns = entries.map(([col]) => `"${col}"`).join(',');
    const placeholders = entries.map((_, idx) => `$${idx + 1}`).join(',');
    const values = entries.map(([, value]) => value);

    let conflict = 'ON CONFLICT DO NOTHING';
    if (available.has('slug')) {
      const updateCols = entries
        .filter(([col]) => col !== 'slug')
        .map(([col]) => `"${col}" = EXCLUDED."${col}"`)
        .join(', ');
      conflict = updateCols ? `ON CONFLICT (slug) DO UPDATE SET ${updateCols}` : 'ON CONFLICT (slug) DO NOTHING';
    } else if (available.has('name')) {
      const updateCols = entries
        .filter(([col]) => col !== 'name')
        .map(([col]) => `"${col}" = EXCLUDED."${col}"`)
        .join(', ');
      conflict = updateCols ? `ON CONFLICT (name) DO UPDATE SET ${updateCols}` : 'ON CONFLICT (name) DO NOTHING';
    }

    await db.query(
      `INSERT INTO public.extraction_recipe (${columns})
       VALUES (${placeholders})
       ${conflict}`,
      values
    );
  } catch (err) {
    console.warn('[BOOT] seed extraction_recipe skipped:', err?.message || err);
  }
}

/* ---------------- Boot-time setup ---------------- */
(async () => {
  try {
    await db.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.ingest_run_logs (
        id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
        task_name text, retry_count integer, gcs_uri text not null,
        status text CHECK (status in ('RUNNING','SUCCEEDED','FAILED')),
        final_table text, final_family text, final_brand text, final_code text,
        final_datasheet text, duration_ms integer, error_message text,
        started_at timestamptz DEFAULT now(), finished_at timestamptz
      )
    `);
    await db.query(`UPDATE public.ingest_run_logs SET status='RUNNING' WHERE lower(status)='processing'`);
    await db.query(`ALTER TABLE public.ingest_run_logs DROP CONSTRAINT IF EXISTS ingest_run_logs_status_check`);
    await db.query(`ALTER TABLE public.ingest_run_logs ADD CONSTRAINT ingest_run_logs_status_check CHECK (status IN ('RUNNING','SUCCEEDED','FAILED'))`);
    await db.query(`ALTER TABLE public.ingest_run_logs ALTER COLUMN status SET DEFAULT 'RUNNING'`);

    await db.query(`ALTER TABLE IF EXISTS public.relay_power_specs ADD COLUMN IF NOT EXISTS coil_voltage_vdc text`);
    await db.query(`ALTER TABLE IF EXISTS public.relay_power_specs ADD COLUMN IF NOT EXISTS contact_form text`);
    await db.query(`ALTER TABLE IF EXISTS public.relay_power_specs ADD COLUMN IF NOT EXISTS suffix text`);
    await db.query(`ALTER TABLE IF EXISTS public.relay_signal_specs ADD COLUMN IF NOT EXISTS coil_voltage_vdc text`);
    await db.query(`ALTER TABLE IF EXISTS public.relay_signal_specs ADD COLUMN IF NOT EXISTS contact_arrangement text`);
    await seedManufacturerAliases();
    await seedExtractionRecipe();
    console.log('[BOOT] ensured ingest_run_logs');
    
    // ───────── 부팅 시 외부 HTTPS/Vertex 호출은 여기서만 (가드 적용)
    if (!EXT_CALLS_OFF) {
      try {
        // 예) 임베딩 워밍업, 외부 웹훅 통지, 지표 전송 등
        // 실제 존재하는 태스크만 남기고, 없으면 주석 그대로 두세요.
        const runEmbedWarmup = require('./src/tasks/embedFamilies')?.run;
        if (typeof runEmbedWarmup === 'function') {
          console.log('[BOOT] embed warmup start');
          await runEmbedWarmup().catch((e) => console.warn('[BOOT] embed warmup skipped:', e?.message || e));
        }
        // TODO: notifyWarmup / ensureIngestRunLogs 등 외부콜 루틴이 있다면 여기에서만 실행
      } catch (e) {
        console.warn('[BOOT] external init skipped:', e?.message || e);
      }
    } else {
      console.log('[BOOT] EXT_CALLS_OFF=1 → external warmups skipped');
    }
  } catch (e) {
    console.warn('[BOOT] ensure ingest_run_logs failed:', e?.message || e);
  }
})();

// ✅ 헬스 라우터는 404보다 "위"에, 맨 마지막에 1번만 마운트
try {
  const healthRouter = require('./server.health');
  console.log('[BOOT] health resolve =', require.resolve('./server.health'));
  app.use(healthRouter);
  console.log('[BOOT] mounted /_healthz, /_env, /api/health (simple=%s)', process.env.HEALTH_SIMPLE);
} catch (e) {
  console.error('[BOOT] health mount failed', e);
}

/* ---------------- 404 / error ---------------- */
app.use((req, res) => res.status(404).json({ ok:false, error:'not found' }));
app.use((err, req, res, next) => {
  try { require('./src/utils/logger').logError(err, { path: req.originalUrl }); } catch {}
  res.status(500).json({ ok:false, error:'internal error' });
});

// (C) 인라인 헬스 제거(충돌 원인)

/* ---------------- Listen ---------------- */
app.listen(PORT, '0.0.0.0', () => console.log(`worker listening on :${PORT}`));
