'use strict';
const { getPool, db } = require('../../lib/db');

const pool = getPool();

async function query(text, params) {
  return pool.query(text, params);
}

module.exports = {
  pool,
  query,
  getPool,
  db,
};
