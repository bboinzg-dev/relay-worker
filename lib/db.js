'use strict';

const { getPool } = require('../db');

function query(text, params) {
  return getPool().query(text, params);
}

const db = new Proxy({}, {
  get(_target, prop) {
    const pool = getPool();
    const value = pool[prop];
    if (typeof value === 'function') {
      return value.bind(pool);
    }
    return value;
  },
});

module.exports = { getPool, query, db };