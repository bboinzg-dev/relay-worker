const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const db = require('./src/utils/db');
const { parseActor, requireRole } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));

async function ensureTenancyTables() {
  await db.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.tenants (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      name text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now()
    );
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.accounts (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      email text UNIQUE,
      display_name text,
      role text CHECK (role IN ('admin','seller','buyer')) NOT NULL DEFAULT 'buyer',
      webhook_url text,
      created_at timestamptz NOT NULL DEFAULT now()
    );
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.account_tenants (
      account_id uuid REFERENCES public.accounts(id) ON DELETE CASCADE,
      tenant_id uuid REFERENCES public.tenants(id) ON DELETE CASCADE,
      PRIMARY KEY (account_id, tenant_id)
    );
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.seller_subscriptions (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      seller_id uuid REFERENCES public.accounts(id) ON DELETE CASCADE,
      family_slug text,
      brand_norm text,
      code_norm text,
      created_at timestamptz NOT NULL DEFAULT now()
    );
  `);
}

// whoami
app.get('/me', async (req, res) => {
  const actor = parseActor(req);
  res.json({ actor });
});

// GET subscriptions for current seller
app.get('/api/subscriptions', async (req, res) => {
  await ensureTenancyTables();
  const actor = parseActor(req);
  if (!requireRole(actor, 'seller') && !requireRole(actor, 'admin')) {
    return res.status(403).json({ error: 'seller role required' });
  }
  const rows = await db.query(`SELECT * FROM public.seller_subscriptions WHERE seller_id = $1 ORDER BY created_at DESC`, [actor.id]);
  res.json({ items: rows.rows });
});

// POST subscription
app.post('/api/subscriptions', async (req, res) => {
  await ensureTenancyTables();
  const actor = parseActor(req);
  if (!requireRole(actor, 'seller') && !requireRole(actor, 'admin')) {
    return res.status(403).json({ error: 'seller role required' });
  }
  const { family_slug=null, brand=null, code=null } = req.body || {};
  const row = await db.query(
    `INSERT INTO public.seller_subscriptions (seller_id, family_slug, brand_norm, code_norm)
     VALUES ($1,$2, lower($3), lower($4)) RETURNING *`,
    [actor.id, family_slug, brand, code]
  );
  res.json(row.rows[0]);
});

// DELETE subscription
app.delete('/api/subscriptions/:id', async (req, res) => {
  await ensureTenancyTables();
  const actor = parseActor(req);
  if (!requireRole(actor, 'seller') && !requireRole(actor, 'admin')) {
    return res.status(403).json({ error: 'seller role required' });
  }
  const id = req.params.id;
  await db.query(`DELETE FROM public.seller_subscriptions WHERE id=$1 AND seller_id=$2`, [id, actor.id]);
  res.json({ ok: true });
});

module.exports = app;
