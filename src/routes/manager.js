// relay-worker/src/routes/manager.js
import express from 'express';
import { Pool } from 'pg';
import crypto from 'node:crypto';

const router = express.Router();

// ---- DB Pool (Cloud SQL/Neon/Supabase 모두 호환)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // PGSSLMODE=disable 이 아니면 완화 SSL 사용(운영 일반값)
  ssl: (process.env.PGSSLMODE || '').toLowerCase() === 'disable' ? false : { rejectUnauthorized: false },
});

// ---- 토큰 발급 (HS256)
function issueToken(payload = {}) {
  const secret = process.env.JWT_SECRET || 'dev-secret';
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const body   = Buffer.from(JSON.stringify({ ...payload, iat: Math.floor(Date.now()/1000) })).toString('base64url');
  const sig    = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
  return `${header}.${body}.${sig}`;
}

// JSON 안전 파서 (multer/text 등 케이스 커버)
function getJsonBody(req) {
  if (req.is?.('application/json') && req.body && typeof req.body === 'object') return req.body;
  try { return typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {}); }
  catch { return {}; }
}

/**
 * POST /auth/signup  ➜ accounts UPSERT by email
 * 요청: { username?, email, phone?, password?, is_seller_requested?, profile?, webhookUrl? }
 * 응답: { ok:true, user:{id,email,display_name,role,webhook_url,created_at}, token, seller_request? }
 */
router.post('/auth/signup', async (req, res) => {
  const b = getJsonBody(req);
  const email = (b.email || '').trim();
  const displayName = (b.username || b.display_name || (email ? email.split('@')[0] : 'user')).trim();
  const wantSeller = !!b.is_seller_requested;
  // 가입 직후엔 항상 buyer로 기록(판매자 신청은 대기 표시만)
  const role = 'buyer';
  const webhookUrl = (b.webhookUrl || b.webhook_url || '').trim() || null;

  if (!email) return res.status(400).json({ ok:false, error:'EMAIL_REQUIRED' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // accounts UPSERT (email UNIQUE)
    const up = await client.query(
      `INSERT INTO public.accounts (email, display_name, role, webhook_url)
         VALUES ($1, $2, $3, $4)
       ON CONFLICT (email) DO UPDATE SET
         display_name = COALESCE(EXCLUDED.display_name, public.accounts.display_name),
         webhook_url  = COALESCE(EXCLUDED.webhook_url,  public.accounts.webhook_url)
       RETURNING id, email, display_name, role, webhook_url, created_at`,
      [email, displayName || null, role, webhookUrl]
    );

    const user = up.rows[0];
    await client.query('COMMIT');

    // 세션용 토큰(홈이 set-cookie로 변환)
    const token = issueToken({ sub: user.id, email: user.email, roles: [user.role] });

    return res.json({
      ok: true,
      user,
      token,
      seller_request: wantSeller ? 'pending' : null,
    });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[auth/signup] db error:', e);
    return res.status(500).json({ ok:false, error:'SIGNUP_DB_FAILED' });
  } finally {
    client.release();
  }
});

/**
 * POST /auth/login  ➜ accounts 조회 후 토큰 발급
 * 요청: { email? | username? | login?, password? }  (현재 데모: password 미검증)
 * 응답: { ok:true, user, token } | { ok:false, error }
 */
router.post('/auth/login', async (req, res) => {
  const b = getJsonBody(req);
  const idOrEmail = String(b.email || b.username || b.login || '').trim();
  if (!idOrEmail) return res.status(400).json({ ok:false, error:'LOGIN_REQUIRED' });

  try {
    // 이메일 우선, 아니면 display_name 기준
    const q = idOrEmail.includes('@')
      ? `SELECT id,email,display_name,role,webhook_url,created_at
           FROM public.accounts WHERE lower(email)=lower($1) LIMIT 1`
      : `SELECT id,email,display_name,role,webhook_url,created_at
           FROM public.accounts WHERE lower(display_name)=lower($1) LIMIT 1`;

    const { rows } = await pool.query(q, [idOrEmail]);
    if (!rows.length) return res.status(404).json({ ok:false, error:'NO_ACCOUNT' });

    const user = rows[0];
    const token = issueToken({ sub: user.id, email: user.email, roles: [user.role] });

    return res.json({ ok:true, user, token });
  } catch (e) {
    console.error('[auth/login] db error:', e);
    return res.status(500).json({ ok:false, error:'LOGIN_DB_FAILED' });
  }
});

export default router;
