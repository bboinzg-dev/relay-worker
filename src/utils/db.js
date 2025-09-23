const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSLMODE === 'require' ? { rejectUnauthorized: false } : false,
  max: 10,
    // 연결/유휴 타임아웃(밀리초) — VPC/라우팅 순간 장애 시 분 단위 매달림 방지
  connectionTimeoutMillis: 8000,
  idleTimeoutMillis: 30000,
});

module.exports = {
  query: (text, params) => pool.query(text, params),
  pool,
};
