// relay-worker/src/utils/vertex.js
'use strict';

const { VertexAI } = require('@google-cloud/vertexai');

+ const env = require('../config/env');
+ const project = env.PROJECT_ID;
+ const location = env.VERTEX_LOCATION;

function getModel(systemText) {
  const systemInstruction = systemText
    ? { role: 'system', parts: [{ text: String(systemText) }] }
    : undefined;

  return vertex.getGenerativeModel({
    model: MODEL_ID,
    systemInstruction,
  });
}

/** 공통 JSON 호출 */
async function callModelJson(systemText, userText, genCfg = {}) {
  const model = getModel(systemText);
  const res = await model.generateContent({
    contents: [{ role: 'user', parts: [{ text: String(userText || '') }] }], // ✅ user만
    generationConfig: {
      temperature: genCfg.temperature ?? 0.2,
      maxOutputTokens: genCfg.maxOutputTokens ?? 2048,
      candidateCount: 1,
    },
  });

  const raw =
    res?.response?.candidates?.[0]?.content?.parts?.[0]?.text ||
    res?.response?.candidates?.[0]?.content?.parts?.[0]?.inlineData?.data || '';

  const trimmed = String(raw || '').trim();
  const fencedMatch = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
  const withoutFence = fencedMatch ? fencedMatch[1] : trimmed;
  const jsonSlice = (() => {
    const firstBrace = withoutFence.indexOf('{');
    const lastBrace = withoutFence.lastIndexOf('}');
    if (firstBrace === -1 || lastBrace === -1 || lastBrace < firstBrace) return withoutFence;
    return withoutFence.slice(firstBrace, lastBrace + 1);
  })();

  try { return JSON.parse(jsonSlice); }
  catch { throw new Error(`Vertex output is not JSON: ${String(raw).slice(0, 300)}`); } 
}

/** 가족/브랜드/코드 1차 감지(범용) */
async function identifyFamilyBrandCode(gcsUri, families = []) {
  const sys = [
    'You analyze electronic component catalogs.',
    'Return strict JSON with keys: family_slug, brand, code, series, display_name.',
    'family_slug must be one of: ' + families.join(', '),
    'Do not fabricate. Empty string if unknown.'
  ].join('\n');

  const usr = JSON.stringify({
    gcs_uri: gcsUri,
    hint: [
      'Use cover/first pages (title, TYPES, ORDERING) primarily.',
      'brand: manufacturer name; code: one concrete orderable part number (if present).'
    ]
  });

  const out = await callModelJson(sys, usr);
  return {
    family_slug: String(out?.family_slug || '').trim(),
    brand:       String(out?.brand       || '').trim(),
    code:        String(out?.code        || '').trim(),
    series:      String(out?.series      || '').trim(),
    display_name:String(out?.display_name|| '').trim(),
  };
}

/** 가족 분류(폐쇄 라벨) */
async function classifyFamily(corpus, allowedFamilies = []) {
  const sys = 'Classify a component family from text. Return JSON {"family_slug": "<one_of_allowed>"}';
  const usr = JSON.stringify({ allowed_families: allowedFamilies, text: String(corpus || '').slice(0, 9000) });
  const out = await callModelJson(sys, usr);
  return String(out?.family_slug || '').trim();
}

/** 블루프린트 필드 추출 */
async function extractByBlueprint(gcsUriOrText, fieldsJson = {}, promptTemplate = '') {
  const sys = [
    'Extract specifications from the catalog.',
    'Return JSON: {"values": {...}}; numbers must be plain numbers; omit fields not present.'
  ].join('\n');

  const usr = JSON.stringify({
    source: typeof gcsUriOrText === 'string' && /^gs:\/\//i.test(gcsUriOrText)
      ? { gcs_uri: gcsUriOrText }
      : { text: String(gcsUriOrText || '') },
    fields: fieldsJson,
    hint: promptTemplate || ''
  });

  const out = await callModelJson(sys, usr, { maxOutputTokens: 4096 });
  return { values: (out && out.values) ? out.values : {}, raw_text: out?.raw_text || '' };
}

/**
 * 타입 표 우선 품번 추출(범용)
 * - "TYPES"/"TYPE(S) TABLE"에 **명시적으로 나열된 품번**만 반환
 * - "ORDERING INFORMATION" 등 조합식만 있으면 **빈 배열**
 */
async function extractPartNumbersFromTypes(gcsUriOrText) {
  const sys = [
    'Find explicit part numbers listed in a catalog table named TYPES / TYPE / TYPES TABLE.',
    'Return strict JSON: {"parts": ["PN1","PN2",...], "table_hint": "<TYPES|ORDERING_INFO|>"}',
    'Only include part numbers that are explicitly enumerated in a table row/column.',
    'If only combinational rules (placeholders X/Y/Z) are shown, return an empty list.',
    'Do NOT expand combinations.'
  ].join('\n');

  const usr = JSON.stringify({
    source: typeof gcsUriOrText === 'string' && /^gs:\/\//i.test(gcsUriOrText)
      ? { gcs_uri: gcsUriOrText, prefer_pages: [1,2,3,4] }
      : { text: String(gcsUriOrText || '') },
    patterns: ["TYPES", "TYPE", "TYPES TABLE", "ORDERING INFORMATION", "ORDERING"]
  });

  const out = await callModelJson(sys, usr, { maxOutputTokens: 2048 });

  const list = Array.isArray(out?.parts) ? out.parts : [];
  const norm = list.map(x => String(x || '').trim().toUpperCase())
                   .filter(x => x && /^[A-Z0-9][A-Z0-9._/-]{2,}$/.test(x));
  const uniq = [...new Set(norm)];

  const hint = String(out?.table_hint || '').toUpperCase();
  return { parts: uniq.slice(0, 200), table_hint: hint };
}

module.exports = {
  LOCATION, MODEL_ID,
  callModelJson,
  identifyFamilyBrandCode,
  classifyFamily,
  extractByBlueprint,
  extractPartNumbersFromTypes,
};
