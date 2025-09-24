const { Pool } = require('pg');
let _pool;

function getPool() {
  if (_pool) return _pool;
  const connectionString = process.env.DATABASE_URL || process.env.APP_DB_URL;
  if (!connectionString) throw new Error('DATABASE_URL missing');
  _pool = new Pool({
    connectionString,
    max: Number(process.env.PG_POOL_MAX || 10),
    idleTimeoutMillis: Number(process.env.PG_IDLE || 30000),
    ssl: process.env.PGSSL === 'disable' ? false : { rejectUnauthorized: false },
  });
  _pool.on('error', err => console.error('[pg] pool error', err));
  return _pool;
}

const db = { query: (text, params) => getPool().query(text, params) };

module.exports = { getPool, db };