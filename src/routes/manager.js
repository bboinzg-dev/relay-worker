// relay-worker/src/routes/manager.js (요약 복구안)
const express = require('express');
const { Pool } = require('pg');
const argon2 = require('argon2');
const jwt = require('jsonwebtoken');

const router = express.Router();
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.PGSSLMODE==='require' ? { rejectUnauthorized:false } : false });
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';

router.post('/auth/signup', async (req, res) => {
  const { username, email, phone, password, is_seller_requested, profile } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'username & password required' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const dup = await client.query('select 1 from users where lower(username)=lower($1)', [username]);
    if (dup.rowCount) { await client.query('ROLLBACK'); return res.status(409).json({ error: 'USERNAME_TAKEN' }); }

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

    await client.query('COMMIT');
    const token = jwt.sign({ uid, username }, JWT_SECRET, { expiresIn: '7d' });
    return res.json({ ok: true, user: u.rows[0], token });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[auth/signup] error:', e);
    return res.status(500).json({ ok:false, error: 'signup_failed' });
  } finally {
    client.release();
  }
});

module.exports = router;
module.exports.default = router;
