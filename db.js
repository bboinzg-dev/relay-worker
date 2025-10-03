// db.js — 워커 공용 PG 커넥터(싱글턴, 안전 TLS 지원)
'use strict';

const { Pool } = require('pg');
const fs = require('fs');
const { URL } = require('url');

let _pool;

function attachVerboseQueryLogging(pool) {
  if (process.env.VERBOSE_TRACE !== '1') return pool;

  const origQuery = pool.query.bind(pool);
  pool.query = async (text, params) => {
    try {
      const caller = new Error().stack.split('\n')[2]?.trim();
      const head = String(text).split('\n')[0].slice(0, 140);
      console.log(`[SQL] ${head} :: caller=${caller}`);
    } catch (_) {
      // best-effort logging only
    }
    return origQuery(text, params);
  };
  return pool;
}

/** 우선순위로 연결 문자열 선택 */
function resolveConnectionString() {
  const c =
    process.env.POSTGRES_URL ||
    process.env.DATABASE_URL ||
    process.env.APP_DB_URL ||
    process.env.PG_CONNECTION_STRING;

  if (!c) {
    throw new Error(
      '[db] No connection string. Set DATABASE_URL (or POSTGRES_URL / APP_DB_URL / PG_CONNECTION_STRING).'
    );
  }
  return c.trim();
}

/** SSL 설정 자동 구성
 *  - DB_TLS_INSECURE=1  → TLS 사용 + 인증서 검증 끔(rejectUnauthorized:false)
 *  - PGSSLROOTCERT가 존재 → TLS 사용 + CA 검증(rejectUnauthorized:true, ca:...)
 *  - 위 둘 다 아니면 기본적으로 TLS 사용 + 검증 끔(운영에서 CA 준비 전 임시)
 */
function resolveSslConfig(connStr) {
  if (/\bhost=\/cloudsql\//.test(connStr)) return false; // Cloud SQL unix-socket이면 TLS 끔
  const insecure = process.env.DB_TLS_INSECURE === '1';
  const caPath = process.env.PGSSLROOTCERT;

  // URL의 sslmode 파라미터도 참고(없어도 동작)
  let sslmode = '';
  try {
    const u = new URL(connStr);
    sslmode = (u.searchParams.get('sslmode') || '').toLowerCase();
  } catch {}

  if (insecure) {
    return { rejectUnauthorized: false };
  }

  if (caPath && fs.existsSync(caPath)) {
    return { ca: fs.readFileSync(caPath), rejectUnauthorized: true };
  }

  // AlloyDB는 TLS 필수(ENCRYPTED_ONLY). CA 없으면 기본적으로 검증만 끔.
  if (sslmode === 'disable') {
    // 비권장: AlloyDB에서는 실패할 수 있음
    return false;
  }
  return { rejectUnauthorized: false };
}

console.log('[db] file =', __filename);
console.log('[db] env  =', { DB_TLS_INSECURE: process.env.DB_TLS_INSECURE, PGSSLMODE: process.env.PGSSLMODE });

function buildPool() {
  const connectionString = resolveConnectionString();
  const ssl = resolveSslConfig(connectionString);

  const max = parseInt(process.env.PGPOOL_MAX || '10', 10);
  const idle = parseInt(process.env.PG_IDLE_TIMEOUT_MS || '30000', 10);
  const connTimeout = parseInt(process.env.PG_CONNECT_TIMEOUT_MS || '5000', 10);

  const pool = new Pool({
    connectionString,
    ssl,                                 // 위에서 결정
    max,
    idleTimeoutMillis: idle,
    connectionTimeoutMillis: connTimeout,
  });

  // 로그(민감정보 제외)
  try {
    const u = new URL(connectionString.replace(/:[^@]+@/, ':***@'));
    console.log('[db] pool created', {
      host: u.hostname,
      port: u.port || 5432,
      db: u.pathname.replace(/^\//, ''),
      ssl:
        ssl === false
          ? 'off'
          : ssl?.rejectUnauthorized === false
          ? 'tls-insecure'
          : 'tls-verify',
      max,
    });
  } catch {
    console.log('[db] pool created (sanitized)');
  }

  pool.on('error', (err) => {
    console.error('[db] unexpected error on idle client', err);
  });

  return attachVerboseQueryLogging(pool);
}

function getPool() {
  if (_pool) return _pool;
  _pool = buildPool();
  return _pool;
}

/** 단건 쿼리 */
async function query(text, params) {
  return getPool().query(text, params);
}

/** 클라이언트 단위로 작업 */
async function withClient(fn) {
  const client = await getPool().connect();
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}

/** 트랜잭션 유틸 */
async function withTransaction(fn) {
  return withClient(async (client) => {
    await client.query('BEGIN');
    try {
      const res = await fn(client);
      await client.query('COMMIT');
      return res;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    }
  });
}

/** 헬스체크용 빠른 핑(선택) */
async function ping(timeoutMs = 800) {
  return withClient(async (c) => {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      await c.query({ text: 'SELECT 1', signal: ctrl.signal });
      return true;
    } finally {
      clearTimeout(t);
    }
  });
}

const dbProxy = new Proxy(
  {},
  {
    get(_target, prop) {
      const pool = getPool();
      const value = pool[prop];
      if (typeof value === 'function') {
        return value.bind(pool);
      }
      return value;
    },
  },
);

const exportsObject = { getPool, query, withClient, withTransaction, ping, db: dbProxy };

Object.defineProperty(exportsObject, 'pool', {
  enumerable: true,
  get() {
    return getPool();
  },
});

module.exports = exportsObject;
