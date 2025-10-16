'use strict';

const { VertexAI } = require('@google-cloud/vertexai');
const db = require('../../db');

const PROJECT_ID = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const VERTEX_LOCATION = process.env.VERTEX_LOCATION || 'asia-northeast3';

const MODEL_CLASSIFY = process.env.GEMINI_MODEL_CLASSIFY || 'gemini-2.5-flash';
const MODEL_EXTRACT = process.env.GEMINI_MODEL_EXTRACT || 'gemini-2.5-flash';

function isNotFound(err) {
  const msg = (err?.message || err?.toString() || '').toLowerCase();
  return err?.code === 404 || msg.includes('not found');
}

async function callGemini({ modelName, contents, generationConfig, safetySettings }) {
  if (!PROJECT_ID) {
    throw new Error('GCP_PROJECT_ID (or GOOGLE_CLOUD_PROJECT) is required for VertexAI');
  }
  const vertex = new VertexAI({ project: PROJECT_ID, location: VERTEX_LOCATION });
  let gm = vertex.getGenerativeModel({
    model: modelName,
    generationConfig: {
      responseMimeType: 'application/json',
      response_mime_type: 'application/json',
    },
  });
  console.info('[vertex] model=%s region=%s', modelName, VERTEX_LOCATION);
  try {
    return await gm.generateContent({ contents, generationConfig, safetySettings });
  } catch (e) {
    if (isNotFound(e) && modelName !== 'gemini-1.5-flash-002') {
      console.warn('[vertex] 404 on', modelName, '→ fallback gemini-1.5-flash-002');
      gm = vertex.getGenerativeModel({
        model: 'gemini-1.5-flash-002',
        generationConfig: {
          responseMimeType: 'application/json',
          response_mime_type: 'application/json',
        },
      });
      return await gm.generateContent({ contents, generationConfig, safetySettings });
    }
    throw e;
  }
}

async function getFamilies() {
  const r = await db.query(
    `SELECT family_slug FROM public.component_registry ORDER BY family_slug`
  );
  return r.rows.map((row) => row.family_slug);
}

async function getFields(family) {
  const r = await db.query(
    `SELECT fields_json FROM public.component_spec_blueprint WHERE family_slug=$1`,
    [family],
  );
  if (!r.rows[0]) throw new Error(`Blueprint not found: ${family}`);
  return r.rows[0].fields_json;
}

function safeParseJson(text) {
  try {
    return JSON.parse(text);
  } catch (_) {}

  const stripped = String(text ?? '')
    .replace(/```(?:json)?/gi, '')
    .trim();

  try {
    return JSON.parse(stripped);
  } catch (_) {}

  let depth = 0;
  let start = -1;
  for (let i = 0; i < stripped.length; i += 1) {
    const ch = stripped[i];
    if (ch === '{') {
      if (depth === 0) start = i;
      depth += 1;
    } else if (ch === '}') {
      depth -= 1;
      if (depth === 0 && start >= 0) {
        const slice = stripped.slice(start, i + 1);
        try {
          return JSON.parse(slice);
        } catch (_) {}
        start = -1;
      }
    }
  }

  return null;
}

function extractCandidateJson(response) {
  const parts = Array.isArray(response?.response?.candidates?.[0]?.content?.parts)
    ? response.response.candidates[0].content.parts
    : [];
  const text = parts.map((part) => part?.text || '').join('');
  if (!text || !text.trim()) {
    return { text: '', data: null };
  }
  const data = safeParseJson(text);
  if (!data) {
    if (process.env.DEBUG_ORDERING === '1') {
      console.warn('[vertex:not-json]', text.slice(0, 300));
    }
    const err = new Error('VERTEX_NOT_JSON');
    err.sample = text.slice(0, 500);
    throw err;
  }
  return { text, data };
}

async function classifyByGcs(gcsUri, filename = 'datasheet.pdf') {
  const fams = await getFamilies();
  const prompt = [
    `PDF 전체를 읽고 {"family_slug","brand","code","series"} JSON만 반환. 파일명: ${filename}`,
    `- family_slug는 반드시 다음 중 하나: ${fams.map((f) => `"${f}"`).join(', ')}`,
  ].join('\n');
  const resp = await callGemini({
    modelName: MODEL_CLASSIFY,
    contents: [
      {
        role: 'user',
        parts: [
          { fileData: { fileUri: gcsUri, mimeType: 'application/pdf' } },
          { text: prompt },
        ],
      },
    ],
    generationConfig: { responseMimeType: 'application/json' },
  });
  const { text, data } = extractCandidateJson(resp);
  if (!text) return {};
  const parsed = data;
  if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
    return parsed;
  }
  if (text) {
    console.warn('[vertex] classify parse failed: invalid JSON payload');
  }
  return {};
}

async function extractValuesByGcs(gcsUri, family) {
  const fields = await getFields(family);
  const prompt = [
    `다음 PDF에서 ${family} 부품의 스펙을 추출합니다.`,
    '아래 DB 컬럼만 채우고 없으면 null.',
    '반드시 {"values":{ "<컬럼명>": 값 }} 만 출력.',
    `columns: ${JSON.stringify(fields)}`,
  ].join('\n');
  const resp = await callGemini({
    modelName: MODEL_EXTRACT,
    contents: [
      {
        role: 'user',
        parts: [
          { text: prompt },
          { fileData: { fileUri: gcsUri, mimeType: 'application/pdf' } },
        ],
      },
    ],
    generationConfig: { responseMimeType: 'application/json' },
  });
  const { text: raw, data } = extractCandidateJson(resp);
  if (!raw) return {};
  const parsed = data;
  if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
    return parsed.values || {};
  }
  if (raw) {
    console.warn('[vertex] extract parse failed: invalid JSON payload');
  }
  return {};
}

module.exports = { classifyByGcs, extractValuesByGcs, safeParseJson, extractCandidateJson };