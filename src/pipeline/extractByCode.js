'use strict';
console.log(`[PATH] entered:${__filename}`);
const { toJsonSchema, callLLM } = require('../llm/structured');
const { safeJsonParse } = require('../utils/safe-json');
const { pool } = require('../../db'); // 공유 PG 풀
const { getBlueprint } = require('../utils/blueprint'); // DB에서 fields_json/specs_table 읽는 함수(앞서 만들어둔 버전)
const { ensureSpecsTable } = require('../utils/schema');

exports.extractAndUpsertOne = async function extractAndUpsertOne({ pdfBase64, family, brand, code }) {
  const bp = await getBlueprint(pool, family); // { fields, specsTable }
  // 스키마 보장(ADD COLUMN만)
  try {
    await pool.query('SELECT public.ensure_specs_table($1)', [family]);
  } catch (err) {
    console.warn('[schema] ensure_specs_table failed (extractByCode):', err?.message || err);
    if (bp?.specsTable) {
      try {
        await ensureSpecsTable(bp.specsTable);
      } catch (fallbackErr) {
        throw fallbackErr;
      }
    } else {
      throw err;
    }
  }

  const responseSchema = toJsonSchema(bp.fields);
  const prompt = `
family=${family}, code="${code}"의 스펙만 추출하세요.
허용 키만 채우고 없으면 null. 반드시 {"values":{...}} JSON만.
`;

  const raw = await callLLM({
    modelEnv:'GEMINI_MODEL_EXTRACT', fallback:'gemini-2.5-flash',
    prompt, pdfBase64, responseSchema, timeoutMs:30000
  });
  const j = safeJsonParse(raw) || {};
  const values = j.values || {};

  // UPSERT by (lower(brand), lower(pn))
  const resolvedValues = values && typeof values === 'object' ? { ...values } : {};
  const pnCandidate = resolvedValues.pn != null ? resolvedValues.pn : code;
  const pnValue = String(pnCandidate || code || '').trim();
  if (!pnValue) throw new Error('pn_missing');

  const skipKeys = new Set(['pn', 'brand', 'code', 'family_slug', 'verified_in_doc']);
  const neverInsert = new Set(['id', 'brand_norm', 'code_norm', 'pn_norm', 'created_at', 'updated_at']);
  const dynamicEntries = Object.entries(resolvedValues).filter(([key]) => {
    if (!key) return false;
    const lower = String(key).toLowerCase();
    if (skipKeys.has(lower)) return false;
    if (neverInsert.has(lower)) return false;
    return true;
  });

  const baseCols = ['family_slug', 'brand', 'code', 'pn', 'verified_in_doc'];
  const insertCols = baseCols.concat(dynamicEntries.map(([key]) => key));
  const placeholders = insertCols.map((_, idx) => `$${idx + 1}`);

  const updateCols = insertCols.filter((col) => !['pn', 'family_slug'].includes(String(col).toLowerCase()));
  const updateAssignments = updateCols.map((col) => `"${col}" = EXCLUDED."${col}"`);
  updateAssignments.push('"updated_at" = now()');

  const sql = `
    INSERT INTO public.${bp.specsTable}
    (${insertCols.map((c) => `"${c}"`).join(', ')})
    VALUES (${placeholders.join(', ')})
    ON CONFLICT ((lower(brand)), (lower(pn)))
    DO UPDATE SET ${updateAssignments.join(', ')}
  `;

  const args = [
    family,
    brand,
    code,
    pnValue,
    true,
    ...dynamicEntries.map(([, value]) => value),
  ];
  await pool.query(sql, args);
};
