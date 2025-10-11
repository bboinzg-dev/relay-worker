// db.js — 워커 공용 PG 커넥터(싱글턴, 안전 TLS 지원)
'use strict';

const { Pool } = require('pg');
const fs = require('fs');
const { URL } = require('url');

let _pool;
let _poolInitError = null;

function createUnavailablePool(error) {
  const makeError = () => {
    const err = new Error('DB_UNAVAILABLE');
    if (error) {
      err.cause = error;
      err.message = error?.message
        ? `DB_UNAVAILABLE: ${error.message}`
        : 'DB_UNAVAILABLE';
    }
    return err;
  };

  const reject = async () => {
    throw makeError();
  };

  return {
    query: reject,
    connect: reject,
    end: async () => {},
    on: () => {},
  };
}

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

function parseEnvInt(name, defaultValue) {
  const raw = process.env[name];
  if (!raw) return defaultValue;

  const parsed = parseInt(raw, 10);
  if (!Number.isFinite(parsed)) {
    console.warn(`[db] invalid numeric value for ${name}:`, raw, `→ fallback ${defaultValue}`);
    return defaultValue;
  }
  return parsed;
}

function buildPool() {
  const connectionString = resolveConnectionString();
  const ssl = resolveSslConfig(connectionString);

  const max = parseEnvInt('PGPOOL_MAX', 10);
  const idle = parseEnvInt('PG_IDLE_TIMEOUT_MS', 600000);
  const connTimeout = parseEnvInt('PG_CONNECT_TIMEOUT_MS', 30000);
  const statementTimeout = parseEnvInt('PG_STATEMENT_TIMEOUT_MS', 30000);
  const queryTimeout = parseEnvInt('PG_QUERY_TIMEOUT_MS', 30000);

  const config = {
    connectionString,
    ssl,                                 // 위에서 결정
    max,
    // 긴 OCR/추출 대기 후에도 커넥션 유지/재연결 여유
    idleTimeoutMillis: idle,
    connectionTimeoutMillis: connTimeout,
  };

  if (Number.isFinite(statementTimeout)) config.statement_timeout = statementTimeout;
  if (Number.isFinite(queryTimeout)) config.query_timeout = queryTimeout;

  const pool = new Pool(config);

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
  try {
    _pool = buildPool();
  } catch (err) {
    _poolInitError = err;
    console.error('[db] buildPool failed:', err?.message || err);
    _pool = createUnavailablePool(err);
  }
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
