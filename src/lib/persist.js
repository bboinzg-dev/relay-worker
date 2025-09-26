'use strict';

const { pool } = require('../utils/db');

/** family별 스펙 테이블 존재/스키마 보장 */
async function ensureSpecsTableForFamily(family) {
  await pool.query('SELECT public.ensure_specs_table($1)', [family]); // DB 함수 사용
}

/** 공용 업서트: brand_norm+code_norm 유니크 키 기준 */
async function upsertSpecsRows(family, rows) {
  if (!rows?.length) return { inserted:0, updated:0 };

  await ensureSpecsTableForFamily(family);
  const table = `public.${family}_specs`;

  const colsFixed = [
    'family_slug','brand','brand_norm','code','code_norm',
    'mfr_full','datasheet_uri','verified_in_doc','series','series_code',
    'created_at','updated_at'
  ];

  const dynamicKeys = collectDynamicKeys(rows);
  const cols = [...colsFixed, ...dynamicKeys];
  const params = cols.map((_,i)=>`$${i+1}`).join(',');

  const textToNorm = s => (s||'').toString().trim();
  const normBrand = s => textToNorm(s).toLowerCase();
  const normCode  = s => textToNorm(s).toLowerCase();

  const now = new Date();
  let inserted = 0, updated = 0;

  for (const r of rows) {
    const series = r.series || (r.code?.match(/^[A-Z]+/)?.[0] ?? null);
    const rowValues = {
      family_slug: family,
      brand: r.brand || null,
      brand_norm: normBrand(r.brand),
      code: r.code,
      code_norm: normCode(r.code),
      mfr_full: r.mfr_full || null,
      datasheet_uri: r.datasheet_uri || null,
      verified_in_doc: !!r.verified_in_doc,
      series,
      series_code: series,
      created_at: now, updated_at: now,
    };

    for (const k of dynamicKeys) rowValues[k] = r[k] ?? null;
    const values = cols.map(c => rowValues[c]);

    const setList = cols
      .filter(c => !['family_slug','brand_norm','code_norm','created_at'].includes(c))
      .map((c,i)=> `"${c}" = EXCLUDED."${c}"`)
      .join(', ');

    const sql = `
      INSERT INTO ${table} (${cols.map(c=>`"${c}"`).join(',')})
      VALUES (${params})
      ON CONFLICT (brand_norm, code_norm)
      DO UPDATE SET ${setList}
      RETURNING (xmax = 0) AS inserted
    `;
    const { rows:ret } = await pool.query(sql, values);
    if (ret[0]?.inserted) inserted++; else updated++;
  }

  return { inserted, updated };
}

function collectDynamicKeys(rows) {
  const fixed = new Set([
    'family_slug','brand','brand_norm','code','code_norm',
    'mfr_full','datasheet_uri','verified_in_doc','series','series_code',
    'created_at','updated_at'
  ]);
  const dyn = new Set();
  for (const r of rows) for (const k of Object.keys(r)) if (!fixed.has(k)) dyn.add(k);
  return Array.from(dyn);
}

module.exports = { upsertSpecsRows };
