'use strict';

const express = require('express');
const { z } = require('zod');
const argon2 = require('argon2');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');

const router = express.Router();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : undefined,
});

const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';

const Signup = z.object({
  username: z.string().min(3).max(32),
  email: z.string().email(),
  phone: z.string().optional(),
  password: z.string().min(6),
  is_seller_requested: z.boolean().optional(),
});

router.get('/health', (_req, res) => {
  res.json({ ok: true, ready: true });
});

router.post('/signup', express.json({ limit: '2mb' }), async (req, res) => {
  let client;
  try {
    const body = Signup.parse(req.body || {});

    client = await pool.connect();
    await client.query('BEGIN');

    const duplicate = await client.query(
      `SELECT 1 FROM public.users WHERE username = $1 OR LOWER(email) = LOWER($2)`,
      [body.username, body.email.toLowerCase()]
    );

    if (duplicate.rowCount) {
      await client.query('ROLLBACK');
      return res.status(409).json({ ok: false, error: 'DUPLICATE_USERNAME_OR_EMAIL' });
    }

    const hash = await argon2.hash(body.password, { type: argon2.argon2id });
    const userInsert = await client.query(
      `INSERT INTO public.users (username, email, phone, password_hash, password_algo, is_seller, is_seller_requested, seller_status, status)
       VALUES ($1, $2, $3, $4, 'argon2id', false, $5, 'none', 'active')
       RETURNING id, username, email, is_seller`,
      [body.username, body.email, body.phone || null, hash, !!body.is_seller_requested]
    );

    const user = userInsert.rows[0];

    await client.query(
      `INSERT INTO public.user_profiles (user_id, role)
       VALUES ($1, $2)
       ON CONFLICT (user_id) DO NOTHING`,
      [user.id, body.is_seller_requested ? 'seller_candidate' : null]
    );

    await client.query('COMMIT');

    const payload = { uid: String(user.id), username: user.username, isSeller: !!user.is_seller };
    const token = jwt.sign(payload, JWT_SECRET, { expiresIn: '7d' });

    return res.json({ ok: true, user, token });
  } catch (err) {
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackErr) {
        console.error('[auth/signup] rollback failed', rollbackErr);
      }
    }
    console.error('[auth/signup] error', err);
    const isValidationError = err instanceof z.ZodError;
    const message = isValidationError
      ? err.issues?.[0]?.message || 'INVALID_REQUEST'
      : err?.message || String(err);
    const status = isValidationError ? 400 : 500;
    return res.status(status).json({ ok: false, error: message });
  } finally {
    if (client) client.release();
  }
});

const Login = z.object({
  idOrEmail: z.string().min(1),
  password: z.string().min(1),
});

router.post('/login', express.json({ limit: '2mb' }), async (req, res) => {
  try {
    const { idOrEmail, password } = Login.parse(req.body || {});

    const userQuery = await pool.query(
      `SELECT id, username, email, password_hash, is_seller
         FROM public.users
        WHERE LOWER(username) = LOWER($1) OR LOWER(email) = LOWER($1)
        LIMIT 1`,
      [idOrEmail.toLowerCase()]
    );

    if (!userQuery.rowCount) {
      return res.status(401).json({ ok: false, error: 'USER_NOT_FOUND' });
    }

    const user = userQuery.rows[0];
    const passwordOk = await argon2.verify(user.password_hash || '', password);

    if (!passwordOk) {
      return res.status(401).json({ ok: false, error: 'INVALID_PASSWORD' });
    }

    const payload = { uid: String(user.id), username: user.username, isSeller: !!user.is_seller };
    const token = jwt.sign(payload, JWT_SECRET, { expiresIn: '7d' });

    return res.json({
      ok: true,
      user: { id: user.id, username: user.username, email: user.email, is_seller: user.is_seller },
      token,
    });
  } catch (err) {
    console.error('[auth/login] error', err);
    const isValidationError = err instanceof z.ZodError;
    const message = isValidationError
      ? err.issues?.[0]?.message || 'INVALID_REQUEST'
      : err?.message || String(err);
    const status = isValidationError ? 400 : 500;
    return res.status(status).json({ ok: false, error: message });
  }
});

module.exports = router;
module.exports.default = router;