'use strict';
const db = require('./db');

/** Upsert by (brand_norm, code_norm) — updated_at는 마지막 1회만 now() */
async function upsertByBrandCode(tableName, values) {
  const safe = String(tableName).replace(/[^a-zA-Z0-9_.]/g, '');
  const cols = Object.keys(values).map(c => c.replace(/[^a-zA-Z0-9_]/g, ''));
  if (!cols.length) return null;

  const params  = cols.map((_, i) => `$${i+1}`);
  const updates = cols.filter(c => c !== 'updated_at').map(c => `${c}=EXCLUDED.${c}`);

  const sql = `
    INSERT INTO public.${safe} (${cols.join(',')})
    VALUES (${params.join(',')})
    ON CONFLICT (brand_norm, code_norm)
    DO UPDATE SET ${updates.join(', ')}${updates.length ? ', ' : ''}updated_at = now()
    RETURNING *`;

  const res = await db.query(sql, cols.map(c => values[c]));
  return res.rows[0] || null;
}

module.exports = { upsertByBrandCode };
