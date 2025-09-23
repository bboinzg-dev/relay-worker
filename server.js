/* server.js */
'use strict';

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const crypto = require('crypto');
const jwt = require('jsonwebtoken');

const db = require('./src/utils/db');
const { getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject, storage, parseGcsUri } = require('./src/utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('./src/utils/schema');
const { runAutoIngest } = require('./src/pipeline/ingestAuto');



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
    dispatchDeadline: { seconds },
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
  try { await db.query('SELECT 1'); res.json({ ok: true }); }
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
      const r = await db.query(`SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`, [family]);
      const table = r.rows[0]?.specs_table;
      if (!table) return res.status(400).json({ ok:false, error:'UNKNOWN_FAMILY' });
      const row = await db.query(`SELECT * FROM public.${table} WHERE brand_norm = lower($1) AND code_norm = lower($2) LIMIT 1`, [brand, code]);
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
      const table = r.rows[0]?.specs_table;
      if (!table) return res.status(400).json({ ok:false, error:'UNKNOWN_FAMILY' });
      const rows = await db.query(
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
  try {
    const { gcsUri, gcsPdfUri, brand, code, series, display_name, family_slug = null } = req.body || {};
    const uri = gcsUri || gcsPdfUri;
    if (!uri || !/^gs:\/\//i.test(uri)) {
      await db.query(
        `INSERT INTO public.ingest_run_logs (task_name, retry_count, gcs_uri, status, error_message)
         VALUES ($1,$2,$3,'FAILED',$4)`,
        [taskName, retryCnt, uri || '', 'gcsUri required (gs://...)']
      );
      return res.status(400).json({ ok:false, error:'gcsUri required (gs://...)' });
    }

    const { rows:logRows } = await db.query(
      `INSERT INTO public.ingest_run_logs (task_name, retry_count, gcs_uri, status)
       VALUES ($1,$2,$3,'PROCESSING') RETURNING id`,
      [taskName, retryCnt, uri]
    );
    const runId = logRows[0]?.id;

    // ✅ 1) 즉시 ACK → Cloud Tasks는 이 시점에 "완료"로 처리(재시도 루프 종료)
    res.status(202).json({ ok: true, run_id: runId });

    // ▶ 2) 다음 Cloud Tasks 요청으로 실행을 넘김(체인) — 응답 보낸 뒤이므로 절대 다시 res.* 호출 금지
    enqueueIngestRun({ runId, gcsUri: uri, brand, code, series, display_name, family_slug })
      .catch(async (err) => {
        // enqueue 실패 시에도 응답은 이미 보냈으므로 DB만 FAILED로 마킹
        try {
          await db.query(
            `UPDATE public.ingest_run_logs
                SET finished_at = now(),
                    duration_ms = $2,
                    status = 'FAILED',
                    error_message = $3
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
      await db.query(
        `UPDATE public.ingest_run_logs
           SET finished_at = now(),
               duration_ms = $2,
               status = 'FAILED',
               error_message = $3
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

// ▶ 체인 실행 엔드포인트: 여기서 runAutoIngest를 실제로 수행
app.post('/api/worker/ingest/run', async (req, res) => {
    const startedAt = Date.now();
  // 예산(기본 120초) + 15초 여유 안에 "반드시" 2xx로 ACK
  const deadlineMs = Number(process.env.INGEST_BUDGET_MS || 120000) + 15000;
  const killer = setTimeout(() => {
    if (!res.headersSent) {
      try { res.status(202).json({ ok: true, timeout: true }); } catch {}
    }
  }, deadlineMs);
  try {
    const { runId, gcsUri, brand, code, series, display_name, family_slug = null } = req.body || {};
    if (!runId || !gcsUri) return res.status(400).json({ ok:false, error:'runId & gcsUri required' });

    const label = `[ingest] ${runId}`;
    console.time(label);
    const out = await runAutoIngest({ gcsUri, brand, code, series, display_name, family_slug });
    console.timeEnd(label);

    await db.query(
      `UPDATE public.ingest_run_logs
          SET finished_at = now(),
              duration_ms = $2,
              status = 'SUCCEEDED',
              final_table = $3,
              final_family = $4,
              final_brand = $5,
              final_code  = $6,
              final_datasheet = $7
        WHERE id = $1`,
      [ runId, (out?.ms ?? (Date.now() - startedAt)),
        out?.specs_table || null,
        out?.family || out?.family_slug || null,
        out?.brand || null,
        (Array.isArray(out?.codes) ? out.codes[0] : out?.code) || null,
        out?.datasheet_uri || gcsUri ]
    );

    return res.json({ ok:true, run_id: runId });
  } catch (e) {
    await db.query(
      `UPDATE public.ingest_run_logs
          SET finished_at = now(), duration_ms = $2, status = 'FAILED', error_message = $3
        WHERE id = $1`,
      [ req.body?.runId || null, Date.now() - startedAt, String(e?.message || e) ]
    );
    console.error('[ingest-run failed]', e?.message || e);
    return res.status(500).json({ ok:false, error:String(e?.message||e) });
      } finally {
    clearTimeout(killer);
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
    await db.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.ingest_run_logs (
        id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
        task_name text, retry_count integer, gcs_uri text not null,
        status text CHECK (status in ('PROCESSING','SUCCEEDED','FAILED')),
        final_table text, final_family text, final_brand text, final_code text,
        final_datasheet text, duration_ms integer, error_message text,
        started_at timestamptz DEFAULT now(), finished_at timestamptz
      )
    `);
    console.log('[BOOT] ensured ingest_run_logs');
  } catch (e) {
    console.warn('[BOOT] ensure ingest_run_logs failed:', e?.message || e);
  }
})();

/* ---------------- Listen ---------------- */
app.listen(PORT, '0.0.0.0', () => console.log(`worker listening on :${PORT}`));
