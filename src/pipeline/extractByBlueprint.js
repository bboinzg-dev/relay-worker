'use strict';
console.log(`[PATH] entered:${__filename}`);
const { VertexAI } = require('@google-cloud/vertexai');
const { safeJsonParse } = require('../utils/safe-json');

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION   = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID   = process.env.GEMINI_MODEL_EXTRACT || 'gemini-2.5-flash';

// 성능/품질 튜닝용 ENV (없으면 기본값 사용)
const MAX_OUT_TOKENS   = Number(process.env.BLUEPRINT_MAX_TOKENS || 512);     // 응답 토큰 상한
const TEXT_LIMIT       = Number(process.env.BLUEPRINT_TEXT_LIMIT   || 32000); // 입력 텍스트 바이트/문자 상한(대략)
const MIN_TEXT_FOR_LLM = Number(process.env.BLUEPRINT_MIN_TEXT     || 800);   // 이보다 짧으면 LLM 호출 생략

const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });

// fieldsJson: {"coil_voltage_vdc":"numeric", "contact_form":"text", ...}
function buildSchema(fieldsJson = {}) {
  const schema = [];
  for (const [k, t] of Object.entries(fieldsJson)) {
    const type = String(t || 'text').toLowerCase();
    if (type.startsWith('num')) schema.push({ name: k, type: 'numeric' });
    else if (type.startsWith('bool')) schema.push({ name: k, type: 'boolean' });
    else schema.push({ name: k, type: 'text' });
  }
  // 자주 쓰는 치수 필드는 기본 보강
  const have = new Set(schema.map(s => s.name));
  for (const k of ['length_mm', 'width_mm', 'height_mm']) {
    if (!have.has(k)) schema.push({ name: k, type: 'numeric' });
  }
  return schema;
}

function normalizeList(v) {
  if (Array.isArray(v)) return v;
  const s = String(v || '').trim();
  if (!s) return [];
  if (/[;,\/]/.test(s)) return s.split(/[;,\/]/).map((x) => x.trim()).filter(Boolean);
  if (/\d/.test(s) && /(to|~|-)/i.test(s)) return [s];
  return [s];
}

function coerceValue(val, type) {
  if (val == null || val === '') return null;
  const t = String(type).toLowerCase();
  if (t.startsWith('num')) {
    const n = Number(String(val).replace(/[^0-9.+\-eE]/g, ''));
    return Number.isFinite(n) ? n : null;
  }
  if (t.startsWith('bool')) {
    const s = String(val).trim().toLowerCase();
    if (/^(true|yes|y|1|on)$/i.test(s)) return true;
    if (/^(false|no|n|0|off)$/i.test(s)) return false;
    return null;
  }
  // text
  let s = String(val).trim();
  // 흔한 노이즈 제거
  s = s.replace(/^```json|^```|```$/g, '').replace(/^["'`]+|["'`]+$/g, '');
  return s || null;
}

// --- 메인 ---
async function extractFields(rawText, code, fieldsJson) {
  const schema = buildSchema(fieldsJson);
  const wantKeys = schema.map(s => s.name);

  // 입력이 너무 짧으면 LLM 호출 생략 (그냥 빈 결과)
  if (!rawText || String(rawText).length < MIN_TEXT_FOR_LLM) {
    const empty = {};
    for (const k of wantKeys) empty[k] = null;
    return empty;
  }

  // 변경
  const full = String(rawText || '');
  const win = Math.min(TEXT_LIMIT, full.length);
  const half = Math.floor(win / 2);
  const head = full.slice(0, half);
  const tail = full.slice(Math.max(0, full.length - half));
  const twoSided = head + '\n---TAIL---\n' + tail;

  const prompt = [
    `You are an expert extracting exact fields from an electronic component datasheet.`,
    `Target model code: ${code || ''}`,
    `Return a STRICT JSON object with EXACTLY these keys (no extra keys, no markdown, no prose).`,
    `If a field is unknown, use null.`,
    `Numeric fields must be plain numbers (no units, no text).`,
    `Assume: dimensions in millimeters; voltage in volts; current in amperes.`,
    `Fields schema: ${JSON.stringify(schema)}`
  ].join('\n');

  const model = vertex.getGenerativeModel({
    model: MODEL_ID,
    generationConfig: {
      temperature: 0.2,
      topP: 0.8,
      maxOutputTokens: MAX_OUT_TOKENS,
      responseMimeType: 'application/json', // 구조화 출력 유지
    },
  });

  const req = {
    contents: [{
      role: 'user',
      parts: [{ text: prompt }, { text: twoSided }]
    }]
  };


  let text = '{}';
  try {
    const resp = await model.generateContent(req);
    // SDK 버전에 따라 text()가 있거나 없을 수 있어 둘 다 지원
    text = (typeof resp?.response?.text === 'function')
      ? resp.response.text()
      : (resp?.response?.candidates?.[0]?.content?.parts?.map(p => p?.text || '').join('') || '{}');
  } catch (e) {
    // 모델 호출 실패 시 빈 결과
    const empty = {};
    for (const k of wantKeys) empty[k] = null;
    return empty;
  }

  // 파싱 & 스키마에 맞춰 정돈
  let parsed = {};
  try { parsed = safeJsonParse(text) || {}; } catch { parsed = {}; }

  const out = {};
  for (const { name, type } of schema) {
    const val = coerceValue(parsed[name], type);
    if (val == null) { out[name] = null; continue; }
    if (type === 'text' && typeof val === 'string') {
      const arr = normalizeList(val);
      out[name] = arr.length > 1 ? arr : arr[0];
    } else {
      out[name] = val;
    }
  }
  return out;
}

module.exports = { extractFields };
