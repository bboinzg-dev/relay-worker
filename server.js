'use strict';

/**
 * relay-worker/server.js  (CJS)
 * - 동적 import로 ./src/routes/manager.js 마운트 (ESM/CJS 모두 수용)
 * - Cloud Run에서 바로 동작하도록 헬스/환경/DB 체크/기본 라우트 포함
 * - /catalog/tree, /api/worker/ingest, /_tasks/notify 등 최소 엔드포인트 구현
 */

const express = require('express');
const pg = require('pg');
const crypto = require('node:crypto');

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '20mb' }));
// server.js 에서 마운트 (상단 app/use들 아래 아무 곳)
try { app.use(require('./server.health')); } catch {}

// ------------------------------------------------------------------
// DB Pool
// ------------------------------------------------------------------
const { Pool } = pg;
const useSsl =
  (String(process.env.DB_SSL || '').toLowerCase() === 'true') ||
  (String(process.env.PGSSLMODE || '').toLowerCase() === 'require');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: useSsl ? { rejectUnauthorized: false } : false,
});

// 간단한 요청 ID 및 로깅
app.use((req, res, next) => {
  const reqId =
    req.headers['x-request-id'] ||
    (crypto.randomUUID && crypto.randomUUID()) ||
    String(Date.now());
  res.locals.reqId = reqId;
  res.setHeader('x-request-id', reqId);
  next();
});

// ------------------------------------------------------------------
// Health / Env
// ------------------------------------------------------------------
app.get('/_healthz', (_req, res) => res.type('text/plain').send('ok'));

app.get('/_env', async (_req, res) => {
  try {
    const r = await pool.query('SELECT 1');
    res.json({
      node: process.version,
      has_db: r?.rowCount === 1,
      gcs_bucket: process.env.GCS_BUCKET || null,
    });
  } catch {
    res.json({ node: process.version, has_db: false, gcs_bucket: null });
  }
});

// DB 연결 실제 체크(+ 500 핸들링)
app.get('/api/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ------------------------------------------------------------------
// catalog tree (최소 구현: 테이블 없으면 빈 배열)
// homepage의 /api/catalog/tree → worker의 /catalog/tree 로 프록시됨
// ------------------------------------------------------------------
app.get('/catalog/tree', async (_req, res) => {
  try {
    const r = await pool.query(
      `SELECT family_slug FROM public.component_registry ORDER BY family_slug`
    );
    const items = r.rows.map((x) => x.family_slug);
    res.json({ items });
  } catch (e) {
    console.warn('[/catalog/tree] fallback:', e?.message || e);
    res.json({ items: [] });
  }
});

// ------------------------------------------------------------------
// ingest & notify (필요 시 Cloud Tasks에서 호출하는 엔드포인트)
// ------------------------------------------------------------------
app.post('/api/worker/ingest', async (req, res) => {
  try {
    const payload = req.body;
    // TODO: 실제 ingest 로직(큐 적재/DB 반영 등)
    res.json({ ok: true, received: Array.isArray(payload) ? payload.length : 1 });
  } catch (e) {
    console.error('[/api/worker/ingest] error:', e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.post('/_tasks/notify', async (req, res) => {
  try {
    // TODO: 작업 완료/실패 등의 알림 처리
    res.json({ ok: true });
  } catch (e) {
    console.error('[/_tasks/notify] error:', e);
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ------------------------------------------------------------------
// /auth 라우터 동적 마운트 (ESM/CJS 모두 수용). 실패 시 폴백 등록.
// ------------------------------------------------------------------
(async () => {
  try {
    const mod = await import('./src/routes/manager.js'); // ESM default 또는 CJS module.exports
    const authRouter = mod?.default || mod;

    if (authRouter && authRouter.use && authRouter.handle) {
      // Express Router
      app.use(authRouter);
      console.log('[worker] mounted /auth routes from ./src/routes/manager.js');
    } else if (typeof authRouter === 'function') {
      // 함수형 등록도 허용 (router(app))
      authRouter(app);
      console.log('[worker] mounted /auth routes via function signature');
    } else {
      console.warn('[worker] manager.js found but not a router/function');
    }
  } catch (e) {
    // 폴백(개발용): JWT 서명하여 /auth/* 제공 → manager.js 부팅 실패해도 로그인 스텁은 동작
    console.error('[worker] failed to mount ./src/routes/manager.js:', e?.message || e);

    const jwt = require('jsonwebtoken');
    const secret = process.env.JWT_SECRET || 'dev-secret';
    const sign = (p) => jwt.sign(p, secret, { expiresIn: '7d' });

    app.get('/auth/health', (_req, res) => res.json({ ok: true, stub: true }));

    app.post('/auth/signup', express.json({ limit: '5mb' }), (req, res) => {
      const p = req.body || {};
      const token = sign({
        uid: String(p.username || p.email || 'user'),
        username: p.username || '',
        email: p.email || '',
      });
      res.json({
        ok: true,
        token,
        user: { username: p.username || '', email: p.email || '' },
      });
    });

    app.post('/auth/login', express.json({ limit: '2mb' }), (req, res) => {
      const p = req.body || {};
      const login = p.login || p.username || p.email || 'user';
      const token = sign({
        uid: String(login),
        username: String(login),
        email: p.email || '',
      });
      res.json({
        ok: true,
        token,
        user: { id: login, username: String(login), email: p.email || '' },
      });
    });

    console.log('[worker] fallback /auth routes registered (no manager.js)');
  }
})();

// ------------------------------------------------------------------
// 에러 핸들러(최후)
// ------------------------------------------------------------------
app.use((err, _req, res, _next) => {
  console.error('[error]', err);
  res.status(500).json({ ok: false, error: 'INTERNAL' });
});
// server.js 에서 마운트 (상단 app/use들 아래 아무 곳)
try { app.use(require('./server.health')); } catch {}

// ------------------------------------------------------------------
// Export & Run
// ------------------------------------------------------------------
module.exports = app;

// Cloud Run에서 단독 실행 시 포트 리스닝
if (require.main === module) {
  const PORT = Number(process.env.PORT || 8080);
  app.listen(PORT, () => console.log(`[worker] listening on :${PORT}`));
}
