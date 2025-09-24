'use strict';

const { pool } = require('../utils/db');
const { getBlueprint } = require('../utils/blueprint');

function safeColumnName(name) {
  if (!/^[a-z0-9_]+$/i.test(name)) throw new Error('invalid column: ' + name);
  return `"${name}"`;
}

async function ensureSpecsTableForFamily(familySlug) {
  await pool.query('SELECT public.ensure_specs_table($1)', [familySlug]);
}

async function saveExtractedSpecs(familySlug, base, specs) {
  // 스키마 자동 보장(추가만 수행)
  await ensureSpecsTableForFamily(familySlug);

  const blueprint = await getBlueprint(pool, familySlug);
  const specsTable = blueprint?.specsTable;
  if (!specsTable) throw new Error(`specs table not found for family ${familySlug}`);
  const targetTable = specsTable.includes('.') ? specsTable : `public.${specsTable}`;

  const baseCols = ['family_slug','brand','brand_norm','code','code_norm','mfr_full','datasheet_uri','verified_in_doc'];
  const baseVals = [
    familySlug,
    base.brand, String(base.brand || '').toLowerCase(),
    base.code,  String(base.code  || '').toLowerCase(),
    base.mfr_full ?? null,
    base.datasheet_uri ?? null,
    true
  ];

  const specCols = Object.keys(specs || {});
  const allCols = baseCols.concat(specCols);
  const params = allCols.map((_, i) => `$${i + 1}`);
  const values = baseVals.concat(specCols.map(k => specs[k]));

  const colList = allCols.map(safeColumnName).join(',');
  const setList = specCols
    .map(k => `${safeColumnName(k)} = EXCLUDED.${safeColumnName(k)}`)
    .join(',');

  const sql = `
    INSERT INTO ${targetTable} (${colList})
    VALUES (${params.join(',')})
    ON CONFLICT (brand_norm, code_norm)
    DO UPDATE SET
      mfr_full = EXCLUDED.mfr_full,
      datasheet_uri = EXCLUDED.datasheet_uri,
      verified_in_doc = EXCLUDED.verified_in_doc
      ${setList ? ',' + setList : ''}
  `;

  await pool.query(sql, values);
}

module.exports = { saveExtractedSpecs };
