// server.health.js (CJS) — 단순/안전 헬스 엔드포인트 전용
'use strict';
const express = require('express');
const db = require('./src/utils/db'); // { query, ping, ... }

const router = express.Router();

// 옵션: 외부 콜 없이 DB 핑만 체크하도록 강제
const HEALTH_SIMPLE = process.env.HEALTH_SIMPLE === '1';

// (옵션) 버킷 노출은 최소화
const GCS_BUCKET_URI = process.env.GCS_BUCKET || '';
const GCS_BUCKET = GCS_BUCKET_URI.startsWith('gs://')
  ? GCS_BUCKET_URI.replace(/^gs:\/\//, '').split('/')[0]
  : (GCS_BUCKET_URI || '');

// ----- 내부 유틸 -----
async function dbPing(timeoutMs = 800) {
  if (HEALTH_SIMPLE) return true; // 완전 단순 모드
  // db.ping이 있으면 사용, 없으면 SELECT 1 + AbortController
  if (typeof db.ping === 'function') {
    return await db.ping(timeoutMs).catch(() => false);
  }
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const r = await db.query({ text: 'SELECT 1', signal: ctrl.signal });
      return r?.rowCount === 1;
    } finally {
      clearTimeout(t);
    }
  } catch {
    return false;
  }
}

function normalizeErr(e) {
  const msg = String(e?.message || e);
  if (/self-signed certificate|DEPTH_ZERO_SELF_SIGNED_CERT/i.test(msg)) return 'db_tls_self_signed';
  if (/password authentication failed/i.test(msg)) return 'db_auth_failed';
  if (/timeout|ETIMEDOUT|aborted/i.test(msg)) return 'db_timeout';
  return msg;
}

// ----- 라우트 -----

// LB/Cloud Run용 초경량 라이브니스
router.head('/_healthz', (_req, res) => res.status(200).end());
router.get('/_healthz', (_req, res) => res.type('text/plain').send('ok'));

// 상태 덤프(민감정보 제외)
router.get('/_env', async (_req, res) => {
  let has_db = false;
  try { has_db = await dbPing(500); } catch {}
  res.json({
    ok: true,
    node: process.version,
    uptime_s: Math.round(process.uptime()),
    project: process.env.PROJECT_ID || process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || null,
    region: process.env.TASKS_LOCATION || process.env.VERTEX_LOCATION || null,
    db: {
      has_db,
      pgsslmode: process.env.PGSSLMODE || null,
      tls_insecure: process.env.DB_TLS_INSECURE === '1',
    },
    gcs_bucket: GCS_BUCKET ? `gs://${GCS_BUCKET}` : null,
  });
});

// 실제 헬스: DB 핑만 수행(외부 HTTPS 호출 절대 금지)
router.get('/api/health', async (_req, res) => {
  try {
    const ok = await dbPing(600);
    return ok
      ? res.json({ ok: true })
      : res.status(500).json({ ok: false, error: 'db_ping_failed' });
  } catch (e) {
    return res.status(500).json({ ok: false, error: normalizeErr(e) });
  }
});

module.exports = router;
