'use strict';
const { getPool } = require('../../lib/db');

const pool = getPool();

const origQuery = pool.query.bind(pool);
pool.query = async (text, params) => {
  if (process.env.VERBOSE_TRACE === '1') {
    const caller = new Error().stack.split('\n')[2]?.trim();
    const head = String(text).split('\n')[0].slice(0, 140);
    console.log(`[SQL] ${head} :: caller=${caller}`);
  }
  return origQuery(text, params);
};

async function query(text, params) {
  return pool.query(text, params);
}

module.exports = {
  pool,
  query,
  getPool,
};

module.exports.db = pool;