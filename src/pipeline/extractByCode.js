'use strict';
console.log(`[PATH] entered:${__filename}`);
const { toJsonSchema, callLLM } = require('../llm/structured');
const { safeJsonParse } = require('../utils/safe-json');
const { pool } = require('../../db'); // 공유 PG 풀
const { getBlueprint } = require('../utils/blueprint'); // DB에서 fields_json/specs_table 읽는 함수(앞서 만들어둔 버전)

function norm(s){ return String(s||'').toLowerCase().replace(/\s+/g,''); } // 접미 보존!

exports.extractAndUpsertOne = async function extractAndUpsertOne({ pdfBase64, family, brand, code }) {
  const bp = await getBlueprint(pool, family); // { fields, specsTable }
  // 스키마 보장(ADD COLUMN만)
  await pool.query('SELECT public.ensure_specs_table($1)', [family]);

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

  // UPSERT (brand_norm, code_norm)
  const brand_norm = norm(brand);
  const code_norm  = norm(code);

  const cols = Object.keys(values);
  const colList = cols.map(c=>`"${c}"`).join(', ');
  const params = cols.map((_,i)=>`$${i+8}`).join(', ');

  const sql = `
    INSERT INTO public.${bp.specsTable}
    (family_slug, brand, brand_norm, code, code_norm, verified_in_doc${cols.length ? ','+colList : ''})
    VALUES ($1,$2,$3,$4,$5,true${cols.length ? ','+params : ''})
    ON CONFLICT (brand_norm, code_norm)
    DO UPDATE SET
      verified_in_doc = true
      ${cols.length ? ','+cols.map(c=>`"${c}"=EXCLUDED."${c}"`).join(',') : ''}
  `;
  const args = [family, brand, brand_norm, code, code_norm].concat(cols.map(c=>values[c]));
  await pool.query(sql, args);
};
