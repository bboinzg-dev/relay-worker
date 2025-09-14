import express from "express";
import { Pool } from "pg";
import jwt from "jsonwebtoken";
import argon2 from "argon2";

const router = express.Router();
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }, // Cloud SQL/Neon/Supabase 대부분 OK
});
const JWT_SECRET = process.env.JWT_SECRET || "dev-secret";

// POST /auth/signup
router.post("/auth/signup", async (req, res) => {
  const { username, email, phone, password, is_seller_requested, profile } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: "username & password required" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const exists = await client.query("select 1 from users where lower(username)=lower($1)", [username]);
    if (exists.rowCount) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: "USERNAME_TAKEN" });
    }
    const hash = await argon2.hash(password);
    const u = await client.query(
      `insert into users (username,email,phone,password_hash,is_seller_requested)
       values ($1,$2,$3,$4,$5)
       returning id,username,email,is_seller,is_seller_requested,seller_status`,
      [username, email || null, phone || null, hash, !!is_seller_requested]
    );
    const uid = u.rows[0].id;
    const p = profile || {};
    await client.query(
      `insert into user_profiles (user_id,company_name,phone,role,categories,website,address)
       values ($1,$2,$3,$4,$5,$6,$7)
       on conflict (user_id) do update set
         company_name=excluded.company_name, phone=excluded.phone, role=excluded.role,
         categories=excluded.categories, website=excluded.website, address=excluded.address`,
      [uid, p.company_name || null, p.phone || null, p.role || null, p.categories || null, p.website || null, p.address || null]
    );
    await client.query("COMMIT");

    const token = jwt.sign({ uid, username }, JWT_SECRET, { expiresIn: "7d" });
    res.json({ token, user: u.rows[0] });
  } catch (e) {
    await client.query("ROLLBACK");
    console.error(e);
    res.status(500).json({ error: "signup_failed" });
  } finally {
    client.release();
  }
});

// POST /auth/login
router.post("/auth/login", async (req, res) => {
  const { login, password } = req.body || {};
  if (!login || !password) return res.status(400).json({ error: "MISSING_CREDENTIALS" });
  try {
    const { rows } = await pool.query(
      `select id,username,email,phone,password_hash,is_seller,is_seller_requested,seller_status
         from users
        where lower(username)=lower($1) or lower(email)=lower($1) or phone=$1
        limit 1`,
      [login]
    );
    if (!rows.length) return res.status(401).json({ error: "INVALID_LOGIN" });
    const u = rows[0];
    const ok = await argon2.verify(u.password_hash, password);
    if (!ok) return res.status(401).json({ error: "INVALID_LOGIN" });
    const token = jwt.sign({ uid: u.id, username: u.username }, JWT_SECRET, { expiresIn: "7d" });
    delete u.password_hash;
    res.json({ token, user: u });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "login_failed" });
  }
});

// GET /account
router.get("/account", async (req, res) => {
  const token = (req.headers.authorization || "").replace(/^Bearer\s+/i, "") || req.headers["x-session-token"];
  if (!token) return res.status(401).json({ error: "NO_SESSION" });
  try {
    const { uid } = jwt.verify(token, JWT_SECRET);
    const { rows: u } = await pool.query(
      `select id,username,email,phone,is_seller,is_seller_requested,seller_status
         from users where id=$1`,
      [uid]
    );
    const { rows: p } = await pool.query(
      `select company_name,phone,role,categories,website,address
         from user_profiles where user_id=$1`,
      [uid]
    );
    res.json({ user: u[0], profile: p[0] || {} });
  } catch {
    res.status(401).json({ error: "BAD_SESSION" });
  }
});

// PUT /account
router.put("/account", async (req, res) => {
  const token = (req.headers.authorization || "").replace(/^Bearer\s+/i, "") || req.headers["x-session-token"];
  if (!token) return res.status(401).json({ error: "NO_SESSION" });
  try {
    const { uid } = jwt.verify(token, JWT_SECRET);
    const p = req.body || {};
    await pool.query(
      `insert into user_profiles (user_id,company_name,phone,role,categories,website,address)
       values ($1,$2,$3,$4,$5,$6,$7)
       on conflict (user_id) do update set
         company_name=excluded.company_name, phone=excluded.phone, role=excluded.role,
         categories=excluded.categories, website=excluded.website, address=excluded.address`,
      [uid, p.company_name || null, p.phone || null, p.role || null, p.categories || null, p.website || null, p.address || null]
    );
    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(401).json({ error: "BAD_SESSION" });
  }
});

export default router;
