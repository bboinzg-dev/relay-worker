// relay-worker/src/routes/manager.js
const express = require('express');
const crypto  = require('node:crypto');

const router = express.Router();

// 간단한 HS256 토큰 (homepage가 token만 받아 세션쿠키로 바꿉니다)
function issueToken(payload = {}) {
  const secret = process.env.JWT_SECRET || 'dev-secret';
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const body   = Buffer.from(JSON.stringify({ ...payload, iat: Math.floor(Date.now() / 1000) })).toString('base64url');
  const sig    = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
  return `${header}.${body}.${sig}`;
}
function jsonBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  try { return JSON.parse(req.body || '{}'); } catch { return {}; }
}

/** POST /auth/signup */
router.post('/auth/signup', async (req, res) => {
  try {
    const b = jsonBody(req);
    const userId = b.username || b.email || `u_${Date.now()}`;
    const user   = {
      id: userId,
      email: b.email || '',
      roles: b.is_seller_requested ? ['seller'] : ['buyer'],
      profile: b.profile ?? null,
    };
    const token = issueToken({ sub: user.id, email: user.email, roles: user.roles });
    return res.json({ ok: true, user, token });
  } catch (e) {
    console.error('[auth/signup]', e);
    return res.status(400).json({ ok:false, error: String(e.message || e) });
  }
});

/** POST /auth/login */
router.post('/auth/login', async (req, res) => {
  try {
    const b = jsonBody(req);
    const userId = b.username || b.email || 'user';
    const user   = { id: userId, email: b.email || '', roles: ['buyer'] };
    const token  = issueToken({ sub: user.id, email: user.email, roles: user.roles });
    return res.json({ ok: true, user, token });
  } catch (e) {
    console.error('[auth/login]', e);
    return res.status(400).json({ ok:false, error: String(e.message || e) });
  }
});

module.exports = router;
module.exports.default = router; // 동적 import() 호환
