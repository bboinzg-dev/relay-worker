'use strict';
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
  idleTimeoutMillis: 10_000,
  connectionTimeoutMillis: 5_000,
  ssl: String(process.env.DB_SSL || '').toLowerCase() === 'true'
       ? { rejectUnauthorized: false }
       : undefined,
});

// 이미 있는 함수/export는 **그대로 유지**하고, 아래 한 줄만 추가
async function query(text, params) {
  // node-postgres 권장: 트랜잭션이 아닐 땐 pool.query를 그대로 사용. :contentReference[oaicite:1]{index=1}
  return pool.query(text, params);
}

module.exports = {
  pool,
  query,                      // ⬅⬅⬅ 이 줄이 핵심
  // ensureSpecsTable, withTransaction 등 기존 export 그대로 유지
};
