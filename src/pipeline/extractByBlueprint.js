'use strict';
console.log(`[PATH] entered:${__filename}`);

const { VertexAI } = require('@google-cloud/vertexai');
const { safeJsonParse } = require('../utils/safe-json');

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION   = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID   = process.env.GEMINI_MODEL_EXTRACT || 'gemini-2.5-flash';

// 성능/품질 튜닝용 ENV (없으면 기본값 사용)
const MAX_OUT_TOKENS   = Number(process.env.BLUEPRINT_MAX_TOKENS || 512);     // 응답 토큰 상한
const TEXT_LIMIT_RAW   = Number(process.env.BLUEPRINT_TEXT_LIMIT || 60000);
const TEXT_LIMIT       = Number.isFinite(TEXT_LIMIT_RAW) && TEXT_LIMIT_RAW > 0 ? TEXT_LIMIT_RAW : 60000;
const MIN_TEXT_FOR_LLM = Number(process.env.BLUEPRINT_MIN_TEXT     || 800);   // 이보다 짧으면 LLM 호출 생략
const WINDOW_SIZE_RAW  = Number(process.env.BLUEPRINT_WINDOW_SIZE || TEXT_LIMIT);
const WINDOW_SIZE      = Number.isFinite(WINDOW_SIZE_RAW) && WINDOW_SIZE_RAW > 0
  ? Math.min(WINDOW_SIZE_RAW, Math.max(2000, TEXT_LIMIT))
  : Math.min(TEXT_LIMIT, 60000);
const WINDOW_STRIDE_RAW = Number(process.env.BLUEPRINT_WINDOW_STRIDE || Math.floor(WINDOW_SIZE / 2));
const WINDOW_STRIDE     = Number.isFinite(WINDOW_STRIDE_RAW) && WINDOW_STRIDE_RAW > 0
  ? WINDOW_STRIDE_RAW
  : Math.max(1000, Math.floor(WINDOW_SIZE / 2));
const WINDOW_MAX_RAW    = Number(process.env.BLUEPRINT_MAX_WINDOWS || 5);
const WINDOW_MAX        = Number.isFinite(WINDOW_MAX_RAW) && WINDOW_MAX_RAW > 0 ? WINDOW_MAX_RAW : 5;

const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });

// fieldsJson: {"coil_voltage_vdc":"numeric", "contact_form":"text", ...}
function buildSchema(fieldsJson = {}) {
  const schema = [];
  for (const [k0, t0] of Object.entries(fieldsJson)) {
    let type = String(t0 || 'text').toLowerCase();
    if (/^(length_mm|width_mm|height_mm|dim_[lwh]_mm)$/i.test(k0)) {
      type = 'numeric';
    }
    if (type.startsWith('num')) schema.push({ name: k0, type: 'numeric' });
    else if (type.startsWith('bool')) schema.push({ name: k0, type: 'boolean' });
    else schema.push({ name: k0, type: 'text' });
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

function isEmptyResultValue(value) {
  if (value == null) return true;
  if (typeof value === 'string') return value.trim() === '';
  if (Array.isArray(value)) return value.length === 0;
  return false;
}

function pickBetterValue(existing, candidate) {
  if (isEmptyResultValue(candidate)) return existing;
  if (isEmptyResultValue(existing)) return candidate;
  if (Array.isArray(candidate)) {
    if (!Array.isArray(existing)) return candidate;
    if (candidate.length > existing.length) return candidate;
    return existing;
  }
  if (Array.isArray(existing)) return existing;
  if (typeof candidate === 'string' && typeof existing === 'string') {
    return candidate.length > existing.length ? candidate : existing;
  }
  return existing;
}

function buildTextWindows(fullText) {
  const text = String(fullText || '');
  const len = text.length;
  if (!len) return [];
  const windowLength = Math.max(1000, Math.min(WINDOW_SIZE, TEXT_LIMIT));
  if (len <= windowLength) {
    const segment = text.slice(0, windowLength);
    return segment ? [segment] : [];
  }

  const windows = [];
  const seen = new Set();
  const addSegment = (segment) => {
    if (!segment) return;
    const trimmed = segment.trim();
    if (!trimmed) return;
    const key = `${trimmed.slice(0, 512)}:${trimmed.length}`;
    if (seen.has(key)) return;
    windows.push(segment);
    seen.add(key);
  };

  addSegment(text.slice(0, windowLength));
  const safeStride = Math.max(1, WINDOW_STRIDE);
  for (let start = safeStride; start < len - windowLength && windows.length < WINDOW_MAX + 2; start += safeStride) {
    const end = Math.min(len, start + windowLength);
    addSegment(text.slice(start, end));
  }
  addSegment(text.slice(Math.max(0, len - windowLength)));

  if (windows.length <= WINDOW_MAX) return windows;

  const trimmed = [];
  if (WINDOW_MAX > 0 && windows.length) trimmed.push(windows[0]);
  if (WINDOW_MAX > 2) {
    const middle = windows.slice(1, -1);
    for (let i = 0; i < middle.length && trimmed.length < WINDOW_MAX - 1; i += 1) {
      trimmed.push(middle[i]);
    }
  }
  if (WINDOW_MAX > 1 && windows.length > 1) {
    trimmed.push(windows[windows.length - 1]);
  }
  return trimmed.slice(0, WINDOW_MAX);
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

  const full = String(rawText || '');
  const windows = buildTextWindows(full);
  if (!windows.length) {
    const empty = {};
    for (const k of wantKeys) empty[k] = null;
    return empty;
  }

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

  const aggregate = {};
  for (const key of wantKeys) aggregate[key] = null;

  let best = null;
  for (const segment of windows) {
    const req = {
      contents: [{
        role: 'user',
        parts: [{ text: prompt }, { text: segment }]
      }]
    };

    let text = '{}';
    try {
      const resp = await model.generateContent(req);
      text = (typeof resp?.response?.text === 'function')
        ? resp.response.text()
        : (resp?.response?.candidates?.[0]?.content?.parts?.map((p) => p?.text || '').join('') || '{}');
    } catch (e) {
      continue;
    }

    let parsed = {};
    try { parsed = safeJsonParse(text) || {}; } catch { parsed = {}; }

    const normalized = {};
    let filled = 0;
    for (const { name, type } of schema) {
      const coerced = coerceValue(parsed[name], type);
      if (coerced == null) {
        normalized[name] = null;
        continue;
      }
      if (type === 'text' && typeof coerced === 'string') {
        const arr = normalizeList(coerced);
        const value = arr.length > 1 ? arr : arr[0];
        normalized[name] = value;
        if (!isEmptyResultValue(value)) filled += 1;
      } else {
        normalized[name] = coerced;
        if (!isEmptyResultValue(coerced)) filled += 1;
      }
    }

    for (const key of wantKeys) {
      aggregate[key] = pickBetterValue(aggregate[key], normalized[key]);
    }

    if (!best || filled > best.filled) {
      best = { data: normalized, filled };
    }
  }

  if (!best) {
    return aggregate;
  }

  for (const key of wantKeys) {
    if (isEmptyResultValue(aggregate[key])) {
      aggregate[key] = best.data[key] ?? null;
    }
    if (aggregate[key] == null) aggregate[key] = null;
  }

  return aggregate;
}

module.exports = { extractFields };
