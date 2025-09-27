'use strict';
const { pool } = require('../utils/db');

async function saveExtractedSpecs(targetTable, familySlug, rows) {
  if (!rows?.length) return;

  const cols = new Set(['family_slug','brand','brand_norm','code','code_norm','mfr_full','datasheet_uri','verified_in_doc']);
  // 동적으로 스펙 컬럼 추가
  for (const r of rows) Object.keys(r).forEach(k => cols.add(k));

  const colList = Array.from(cols);
  const placeholders = colList.map((_, i) => `$${i+1}`).join(',');

  const sql = `
    INSERT INTO ${targetTable} (${colList.map(c => `"${c}"`).join(',')})
    VALUES (${placeholders})
    ON CONFLICT (brand_norm, code_norm, family_slug)
    DO UPDATE SET
      mfr_full = EXCLUDED.mfr_full,
      datasheet_uri = EXCLUDED.datasheet_uri,
      verified_in_doc = EXCLUDED.verified_in_doc
  `;

  const client = await pool.connect();
  try {
    for (const r of rows) {
      const rec = { ...r };
      rec.family_slug = familySlug;
      rec.brand_norm = (rec.brand || 'unknown').toLowerCase();
      rec.code_norm  = (rec.code  || '').toLowerCase();
      const vals = colList.map(c => rec[c]);
      await client.query(sql, vals);
    }
  } finally {
    client.release();
  }
}

module.exports = { saveExtractedSpecs };
