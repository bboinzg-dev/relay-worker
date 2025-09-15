// src/routes/manager.js
import express from 'express';
import crypto from 'node:crypto';

const router = express.Router();

// 공통: body가 application/json이 아닐 수도 있으니 대비
function readBody(req) {
  if (req.is('application/json')) return req.body || {};
  // text나 urlencoded인 경우
  try { return JSON.parse(req.body ?? '{}'); } catch { return {}; }
}

// 간단 토큰(JWT 대체). 홈에서 서명 검증은 하지 않고 쿠키 서명만 하므로 임시 토큰으로 충분.
function issueToken(payload = {}) {
  const secret = process.env.JWT_SECRET || 'dev-secret';
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const body   = Buffer.from(JSON.stringify({ ...payload, iat: Math.floor(Date.now()/1000) })).toString('base64url');
  const sig    = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
  return `${header}.${body}.${sig}`;
}

// POST /auth/signup
router.post('/auth/signup', async (req, res) => {
  try {
    const b = readBody(req);
    const user = {
      id: b.username || b.email || `u_${Date.now()}`,
      email: b.email || '',
      roles: (b.is_seller_requested ? ['seller'] : ['buyer']),
      profile: b.profile ?? null,
    };
    const token = issueToken({ sub: user.id, roles: user.roles, email: user.email });
    // 프런트는 JSON을 보고 set-cookie를 붙이므로 token만 돌려주면 됨
    return res.json({ ok: true, user, token });
  } catch (e) {
    console.error(e);
    return res.status(400).json({ ok:false, error: String(e.message || e) });
  }
});

// POST /auth/login  (아주 단순 버전)
router.post('/auth/login', async (req, res) => {
  try {
    const b = readBody(req);
    const user = {
      id: b.username || b.email || 'user',
      email: b.email || '',
      roles: ['buyer'],
    };
    const token = issueToken({ sub: user.id, roles: user.roles, email: user.email });
    return res.json({ ok: true, user, token });
  } catch (e) {
    console.error(e);
    return res.status(400).json({ ok:false, error: String(e.message || e) });
  }
});

export default router;         // ESM default
module.exports = router;       // CJS 호환
