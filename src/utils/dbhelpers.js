const db = require('../../db');

async function tableExists(schema, name) {
  const r = await db.query(`SELECT to_regclass($1) as reg`, [`${schema}.${name}`]);
  return !!r.rows[0]?.reg;
}

async function extInstalled(name) {
  try {
    const r = await db.query(`SELECT 1 FROM pg_extension WHERE extname=$1`, [name]);
    return !!r.rows.length;
  } catch { return false; }
}

module.exports = { tableExists, extInstalled };
