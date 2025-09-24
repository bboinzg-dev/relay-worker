// db.js — 워커 공용 PG 커넥터 (싱글턴)
const { Pool } = require('pg');

let _pool;

function getPool() {
  if (_pool) return _pool;

  const connectionString =
    process.env.POSTGRES_URL ||
    process.env.DATABASE_URL ||
    process.env.APP_DB_URL ||
    process.env.PG_CONNECTION_STRING;

  _pool = new Pool({
    connectionString,
    // Cloud SQL / Supabase 등에서 흔히 필요
    ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : undefined,
    max: parseInt(process.env.PGPOOL_MAX || '10', 10),
    idleTimeoutMillis: 30000,
  });

  _pool.on('error', (err) => {
    console.error('[pg] unexpected error on idle client', err);
  });

  return _pool;
}

async function query(text, params) {
  return getPool().query(text, params);
}

module.exports = { getPool, query };