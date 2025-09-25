'use strict';

const { pool } = require('../utils/db');
const { getBlueprint, computeFastKeys } = require('../utils/blueprint');

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

  const RESERVED = new Set(['id','brand','code','brand_norm','code_norm','created_at','updated_at']);
  const FAST = String(process.env.INGEST_MODE || '').toUpperCase() === 'FAST' || process.env.FAST_INGEST === '1';
  const allowedKeys = Array.isArray(blueprint?.allowedKeys)
    ? blueprint.allowedKeys.map((k) => String(k || '').trim().toLowerCase()).filter(Boolean)
    : [];
    const fieldTypes = blueprint?.fields || {};
      function coerceNumeric(x) {
        if (x == null || x === '') return null;
        if (typeof x === 'number') return x;
        let s = String(x).toLowerCase();
        s = s.replace(/(?<=\d),(?=\d{3}\b)/g, '').replace(/\s+/g, ' ').trim();
        const m = s.match(/(-?\d+(?:\.\d+)?)(?:\s*([kmgmunpµ]))?/i);
        if (!m) return null;
        let n = parseFloat(m[1]);
        const mul = (m[2] || '').toLowerCase();
        const scale = { k:1e3, m:1e-3, 'µ':1e-6, u:1e-6, n:1e-9, p:1e-12, g:1e9 };
        if (mul && scale[mul] != null) n = n * scale[mul];
        return isFinite(n) ? n : null;
      }
      function coerceByType(key, val) {
        const t = String(fieldTypes[key] || 'text').toLowerCase();
        if (t === 'numeric') return coerceNumeric(val);
        if (t === 'int')     { const n = coerceNumeric(val); return (n==null?null:Math.round(n)); }
        if (t === 'bool')    {
          if (typeof val === 'boolean') return val;
          const s = String(val||'').toLowerCase().trim();
          if (!s) return null;
          return /^(true|yes|y|1|on|enable|enabled|pass)$/i.test(s);
        }
        return (val == null ? null : String(val));
      }
  const fastKeys = FAST
    ? computeFastKeys(blueprint).map((k) => String(k || '').trim().toLowerCase()).filter(Boolean)
    : allowedKeys;
  const allowedSet = new Set(allowedKeys);
  const fastSet = new Set(fastKeys);
  const specsNorm = {};
  for (const [rawKey, value] of Object.entries(specs || {})) {
    const key = String(rawKey || '')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_]/g, '');
    if (!key || RESERVED.has(key)) continue;
    if (allowedSet.size && !allowedSet.has(key)) continue;
    if (FAST && fastSet.size && !fastSet.has(key)) continue;
    if (!Object.prototype.hasOwnProperty.call(specsNorm, key)) {
      specsNorm[key] = coerceByType(key, value); // 타입 강제정규화
    }
  }

  const specCols = Object.keys(specsNorm);
  const allCols = baseCols.concat(specCols);
  const params = allCols.map((_, i) => `$${i + 1}`);
  const values = baseVals.concat(specCols.map((k) => specsNorm[k]));

  const colList = allCols.map(safeColumnName).join(',');
  const setList = specCols
    .map((k) => `${safeColumnName(k)} = EXCLUDED.${safeColumnName(k)}`)
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
