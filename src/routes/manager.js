// relay-worker/src/routes/manager.js
const express = require('express');
const crypto = require('node:crypto');

const router = express.Router();

// 간단한 JWT 발급기 (HS256)
function issueToken(payload = {}) {
  const secret = process.env.JWT_SECRET || 'dev-secret';
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const body   = Buffer.from(JSON.stringify({ ...payload, iat: Math.floor(Date.now() / 1000) })).toString('base64url');
  const sig    = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
  return `${header}.${body}.${sig}`;
}

// Express의 body-parser 설정으로 JSON이면 req.body에 파싱되어 옵니다.
function getJsonBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body || '{}'); } catch { return {}; }
}

/**
 * POST /auth/signup
 * homepage → /api/signup → (proxy) → worker /auth/signup
 * 응답 JSON에 { token } 이 있으면 homepage가 세션쿠키로 변환합니다.
 */
router.post('/auth/signup', async (req, res) => {
  try {
    const b = getJsonBody(req);
    const userId = b.username || b.email || `u_${Date.now()}`;
    const user = {
      id: userId,
      email: b.email || '',
      roles: b.is_seller_requested ? ['seller'] : ['buyer'],
      profile: b.profile ?? null,
    };
    const token = issueToken({ sub: user.id, email: user.email, roles: user.roles });
    return res.json({ ok: true, user, token });
  } catch (e) {
    console.error('[auth/signup] error:', e);
    return res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

/**
 * POST /auth/login
 */
router.post('/auth/login', async (req, res) => {
  try {
    const b = getJsonBody(req);
    const userId = b.username || b.email || 'user';
    const user = {
      id: userId,
      email: b.email || '',
      roles: ['buyer'],
    };
    const token = issueToken({ sub: user.id, email: user.email, roles: user.roles });
    return res.json({ ok: true, user, token });
  } catch (e) {
    console.error('[auth/login] error:', e);
    return res.status(400).json({ ok: false, error: String(e.message || e) });
  }
});

module.exports = router;
module.exports.default = router; // import('./...').then(m => m.default) 형태 호환
