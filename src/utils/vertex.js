'use strict';

const LOCATION = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID = process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash';

// Vertex 호출 공통 JSON 반환 헬퍼
async function callModelJson(systemText, userText, options = {}) {
  // 구현체는 기존 레포의 Vertex 호출 방식 그대로 사용하세요.
  // 아래는 의사 코드 형태(레포에 맞게 연결):
  //
  // const { VertexAI } = require('@google-cloud/vertexai');
  // const client = new VertexAI({ location: LOCATION, model: MODEL_ID });
  // const r = await client.generateContent({ contents: [...], ... });
  // const json = JSON.parse(r.candidates[0].outputText);
  // return json;

  // 레포에 이미 같은 함수가 있다면 이 파일을 그 구현으로 덮어쓰거나 재사용하세요.
  throw new Error('callModelJson not implemented – wire to your Vertex client.');
}

async function chooseBrandCode(corpus, brandCandidates = [], codeCandidates = []) {
  // 환각 방지: 후보 리스트만 허용
  const sys = [
    'You are a product catalog analyzer.',
    'Return strict JSON with shape {"brand": "...", "code": "...", "series": ""}.',
    'Choose brand only from provided candidates list; if none fits, return empty string.',
    'Choose code only from provided candidates list; prefer tokens that look like orderable part numbers.',
    'Do not fabricate values.'
  ].join('\n');

  const usr = JSON.stringify({
    text: corpus.slice(0, 9000),
    candidates: {
      brands: brandCandidates.slice(0, 30),
      codes: codeCandidates.slice(0, 60),
    }
  });

  const out = await callModelJson(sys, usr);
  return {
    brand: String(out?.brand || '').trim(),
    code:  String(out?.code  || '').trim(),
    series: String(out?.series || '').trim(),
  };
}

async function classifyFamily(corpus, allowedFamilies = []) {
  const sys = [
    'Classify the product family from the text.',
    'Return JSON {"family_slug": "<one_of_allowed>"}',
    'If unsure, return empty string.'
  ].join('\n');

  const usr = JSON.stringify({
    allowed_families: allowedFamilies,
    text: corpus.slice(0, 9000),
  });

  const out = await callModelJson(sys, usr);
  return String(out?.family_slug || '').trim();
}

async function extractByBlueprint(corpus, fieldsJson = {}, prompt = '') {
  const sys = [
    'Extract specifications.',
    'Return JSON: {"values": {...}} matching field names, with minimal normalization.',
    'Omit fields not present. Do not invent.'
  ].join('\n');
  const usr = JSON.stringify({ fields: fieldsJson, hint: prompt, text: corpus.slice(0, 12000) });
  const out = await callModelJson(sys, usr);
  return out?.values || {};
}

module.exports = {
  LOCATION, MODEL_ID,
  callModelJson,
  chooseBrandCode,
  classifyFamily,
  extractByBlueprint,
};
