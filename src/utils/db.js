'use strict';

const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
  idleTimeoutMillis: 10_000,
  connectionTimeoutMillis: 5_000,
});

async function ensureSpecsTable(familySlug) {
  const flag = String(process.env.NO_SCHEMA_ENSURE || '0').toLowerCase();
  if (flag === '1' || flag === 'true' || flag === 'on') return;
  await pool.query('SELECT public.ensure_specs_table($1)', [familySlug]);
}

module.exports = { pool, ensureSpecsTable };
