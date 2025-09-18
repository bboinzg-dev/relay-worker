// relay-worker/src/utils/vertex.js
'use strict';

/**
 * Vertex AI (Gemini) 유틸
 * - systemInstruction 사용 (❌ system role in contents)
 * - 공통 JSON 호출 헬퍼 + 감지/분류/추출 함수 제공
 *
 * ENV:
 *   VERTEX_LOCATION=asia-northeast3
 *   VERTEX_MODEL_ID=gemini-2.5-flash  (또는 운영 모델)
 */

const { VertexAI } = require('@google-cloud/vertexai');

const LOCATION = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID = process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash';

const vertex = new VertexAI({ location: LOCATION });

function getModel(systemText) {
  // ✅ systemInstruction 로 장착
  const systemInstruction = systemText
    ? { role: 'user', parts: [{ text: String(systemText) }] }
    : undefined;

  return vertex.getGenerativeModel({
    model: MODEL_ID,
    // generation config는 호출 시 지정 (필요시 여기에 기본값 둬도 됨)
    systemInstruction,
  });
}

/** 공통 JSON 호출 (systemText: 지시문, userText: 입력) */
async function callModelJson(systemText, userText, genCfg = {}) {
  const model = getModel(systemText);

  const res = await model.generateContent({
    contents: [
      // ❗ contents에는 user 만 넣는다 (system role 금지)
      { role: 'user', parts: [{ text: String(userText || '') }] },
    ],
    generationConfig: {
      temperature: genCfg.temperature ?? 0.2,
      maxOutputTokens: genCfg.maxOutputTokens ?? 2048,
      candidateCount: 1,
    },
  });

  // Vertex Node SDK 응답에서 텍스트 추출
  const txt =
    res?.response?.candidates?.[0]?.content?.parts?.[0]?.text ||
    res?.response?.candidates?.[0]?.content?.parts?.[0]?.inlineData?.data ||
    '';

  let out;
  try {
    out = JSON.parse(txt);
  } catch (e) {
    throw new Error(`Vertex output is not JSON: ${String(txt).slice(0, 300)}`);
  }
  return out;
}

/* ───────────────────────────── */
/*      감지 / 분류 / 추출       */
/* ───────────────────────────── */

/** 가족/브랜드/코드 1차 감지 */
async function identifyFamilyBrandCode(gcsUri, families = []) {
  const sys = [
    'You analyze electronic component catalogs.',
    'Return strict JSON with keys: family_slug, brand, code, series, display_name.',
    'family_slug must be one of: ' + families.join(', '),
    'Do not fabricate. Empty string if unknown.',
  ].join('\n');

  const usr = JSON.stringify({
    gcs_uri: gcsUri,
    instructions: [
      'Use the first pages (cover/ordering/types) as primary signal.',
      'brand: manufacturer (e.g., Panasonic, OMRON, TE Connectivity, ...)',
      'code: one concrete orderable part number (if many exist, choose the most representative).',
      'series/display_name: series name or catalog title if apparent.'
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

/** 가족 분류 (폐쇄 라벨 셋) */
async function classifyFamily(corpus, allowedFamilies = []) {
  const sys = [
    'Classify a component family from text.',
    'Return JSON {"family_slug": "<one_of_allowed>"}',
    'If unsure, return empty string.',
  ].join('\n');

  const usr = JSON.stringify({
    allowed_families: allowedFamilies,
    text: String(corpus || '').slice(0, 9000),
  });

  const out = await callModelJson(sys, usr);
  return String(out?.family_slug || '').trim();
}

/** 블루프린트 필드 추출 */
async function extractByBlueprint(gcsUriOrText, fieldsJson = {}, promptTemplate = '') {
  const sys = [
    'Extract specifications from the catalog.',
    'Return JSON: {"values": {...}} where keys match the provided fields.',
    'Numbers must be plain numbers; omit fields that are not present. No fabrication.',
  ].join('\n');

  // gcs_uri 를 주거나, 이미 전처리된 text 를 줄 수도 있게 유연하게 작성
  const usr = JSON.stringify({
    source: typeof gcsUriOrText === 'string' && /^gs:\/\//i.test(gcsUriOrText)
      ? { gcs_uri: gcsUriOrText }
      : { text: String(gcsUriOrText || '') },
    fields: fieldsJson,
    hint:   promptTemplate || ''
  });

  const out = await callModelJson(sys, usr, { maxOutputTokens: 4096 });
  return { values: (out && out.values) ? out.values : {}, raw_text: out?.raw_text || '' };
}

module.exports = {
  LOCATION,
  MODEL_ID,
  callModelJson,
  identifyFamilyBrandCode,
  classifyFamily,
  extractByBlueprint,
};
