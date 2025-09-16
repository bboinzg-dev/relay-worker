// relay-worker/src/routes/manager.js
import express from "express";
import { Pool } from "pg";
import jwt from "jsonwebtoken";
import argon2 from "argon2";
import crypto from "node:crypto";

const router = express.Router();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Cloud SQL/Neon/Supabase 등 대부분 호환. 필요시 PGSSLMODE=require 설정과 함께 사용.
  ssl: process.env.PGSSLMODE === "require" ? { rejectUnauthorized: false } : undefined,
});
const JWT_SECRET = process.env.JWT_SECRET || "dev-secret";

// --- helpers --------------------------------------------------
async function tableExists(client, fqName /* 'public.users' */) {
  const { rows } = await client.query(
    `select to_regclass($1) as reg`,
    [fqName]
  );
  return !!rows[0]?.reg;
}

async function ensureAuthTables(client) {
  // users / user_profiles 테이블이 없으면 생성 (운영 DB 권한이 없으면 스킵됨)
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.users (
      id BIGSERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      email TEXT UNIQUE,
      phone TEXT,
      password_hash TEXT NOT NULL,
      password_algo TEXT DEFAULT 'argon2id',
      is_seller BOOLEAN DEFAULT FALSE,
      is_seller_requested BOOLEAN DEFAULT FALSE,
      seller_status TEXT DEFAULT 'approved', -- pending|approved|rejected
      status TEXT DEFAULT 'active',
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now()
    );
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.user_profiles (
      user_id BIGINT PRIMARY KEY REFERENCES public.users(id) ON DELETE CASCADE,
      company_name TEXT,
      phone TEXT,
      role TEXT,
      categories TEXT[],
      website TEXT,
      address TEXT,
      updated_at timestamptz DEFAULT now()
    );
  `);
}

function getJsonBody(req) {
  if (req.is && req.is("application/json")) return req.body || {};
  try { return typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {}); }
  catch { return {}; }
}

// (간단 HS256) 필요 시 jsonwebtoken으로 교체 가능하지만 여기선 수동 구현
function issueToken(payload = {}) {
  const header = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
  const body   = Buffer.from(JSON.stringify({ ...payload, iat: Math.floor(Date.now() / 1000) })).toString("base64url");
  const sig    = crypto.createHmac("sha256", JWT_SECRET).update(`${header}.${body}`).digest("base64url");
  return `${header}.${body}.${sig}`;
}

// --- routes ---------------------------------------------------

/**
 * POST /auth/signup
 * homepage → /api/signup → (proxy) → worker /auth/signup
 * 성공 시 JSON { token, user } 반환(홈이 쿠키로 설정).
 */
router.post("/auth/signup", async (req, res) => {
  const b = getJsonBody(req);
  const { username, email, phone, password, is_seller_requested, profile } = b || {};
  if (!username || !password) return res.status(400).json({ error: "username & password required" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 테이블 보장(없으면 생성)
    await ensureAuthTables(client);

    // username 중복
    const dup = await client.query(
      "select 1 from public.users where lower(username)=lower($1)",
      [username]
    );
    if (dup.rowCount) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: "USERNAME_TAKEN" });
    }

    // users insert
    const hash = await argon2.hash(password);
    const { rows: urows } = await client.query(
      `insert into public.users
         (username,email,phone,password_hash,is_seller,is_seller_requested,seller_status,status)
       values ($1,$2,$3,$4,false,$5, case when $5 then 'pending' else 'approved' end, 'active')
       returning id, username, email, is_seller, is_seller_requested, seller_status, created_at`,
      [username, email || null, phone || null, hash, !!is_seller_requested]
    );
    const user = urows[0];
    const uid = user.id;

    // user_profiles upsert
    const p = profile || {};
    await client.query(
      `insert into public.user_profiles (user_id,company_name,phone,role,categories,website,address,updated_at)
       values ($1,$2,$3,$4,$5,$6,$7, now())
       on conflict (user_id) do update set
         company_name=excluded.company_name, phone=excluded.phone, role=excluded.role,
         categories=excluded.categories, website=excluded.website, address=excluded.address,
         updated_at=now()`,
      [uid, p.company_name || null, p.phone || null, p.role || null, p.categories || null, p.website || null, p.address || null]
    );

    // (옵션) accounts 테이블이 존재하면 미러링(시각적 확인용)
    if (await tableExists(client, "public.accounts")) {
      const accId = (crypto.randomUUID && crypto.randomUUID()) || null;
      const role = is_seller_requested ? "seller" : "buyer";
      await client.query(
        `insert into public.accounts (id, email, display_name, role, webhook_url, created_at)
         values ($1, $2, $3, $4, null, now())
         on conflict do nothing`,
        [accId, email || `${username}@example.com`, username, role]
      );
    }

    await client.query("COMMIT");

    const token = issueToken({ uid, username: user.username });
    return res.json({ ok: true, user, token }); // 홈에서 token을 쿠키(pp_session)로 변환
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("[auth/signup] error:", e);
    return res.status(500).json({ ok: false, error: "signup_failed" });
  } finally {
    client.release();
  }
});

/**
 * POST /auth/login
 * users 테이블이 있으면 비밀번호 검증(argon2). 없으면 개발편의상 email/username만으로 통과(임시).
 */
router.post("/auth/login", async (req, res) => {
  const b = getJsonBody(req);
  const login = b?.username || b?.email || b?.id_or_email || b?.login;
  const password = b?.password;
  if (!login) return res.status(400).json({ error: "MISSING_LOGIN" });

  try {
    const { rows: chk } = await pool.query(`select to_regclass('public.users') as reg`);
    if (!chk[0]?.reg) {
      // users 없음 → 임시 토큰(개발용)
      const token = issueToken({ uid: login, username: String(login) });
      return res.json({ ok: true, user: { id: login, username: String(login), email: b?.email || null }, token });
    }

    const { rows } = await pool.query(
      `select id,username,email,phone,password_hash,is_seller,is_seller_requested,seller_status
         from public.users
        where lower(username)=lower($1) or lower(email)=lower($1) or phone=$1
        limit 1`,
      [String(login)]
    );
    if (!rows.length) return res.status(401).json({ error: "INVALID_LOGIN" });
    const u = rows[0];

    if (!password) return res.status(400).json({ error: "PASSWORD_REQUIRED" });
    const ok = await argon2.verify(u.password_hash, password);
    if (!ok) return res.status(401).json({ error: "INVALID_LOGIN" });

    const token = issueToken({ uid: u.id, username: u.username });
    delete u.password_hash;
    return res.json({ ok: true, user: u, token });
  } catch (e) {
    console.error("[auth/login] error:", e);
    return res.status(500).json({ ok: false, error: "login_failed" });
  }
});

export default router;
