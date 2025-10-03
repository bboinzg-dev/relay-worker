'use strict';

const { VertexAI } = require('@google-cloud/vertexai');
const db = require('../../db');

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

function model(name, def) {
  const project =
    process.env.GCP_PROJECT_ID ||
    process.env.GOOGLE_CLOUD_PROJECT ||
    process.env.PROJECT_ID;
  const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
  if (!project) {
    throw new Error('GCP_PROJECT_ID (or GOOGLE_CLOUD_PROJECT) is required for VertexAI');
  }
  const v = new VertexAI({ project, location });
  return v.getGenerativeModel({
    model: process.env[name] || def || 'gemini-2.5-flash',
  });
}

async function classifyByGcs(gcsUri, filename = 'datasheet.pdf') {
  const fams = await getFamilies();
  const mdl = model('GEMINI_MODEL_CLASSIFY', 'gemini-2.5-flash');
  const prompt = [
    `PDF 전체를 읽고 {"family_slug","brand","code","series"} JSON만 반환. 파일명: ${filename}`,
    `- family_slug는 반드시 다음 중 하나: ${fams.map((f) => `"${f}"`).join(', ')}`,
  ].join('\n');
  const resp = await mdl.generateContent({
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
  const parts = resp.response?.candidates?.[0]?.content?.parts ?? [];
  const text = parts.map((p) => p.text || '').join('');
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch (err) {
    console.warn('[vertex] classify parse failed:', err?.message || err);
    return {};
  }
}

async function extractValuesByGcs(gcsUri, family) {
  const fields = await getFields(family);
  const mdl = model('GEMINI_MODEL_EXTRACT', 'gemini-2.5-flash');
  const prompt = [
    `다음 PDF에서 ${family} 부품의 스펙을 추출합니다.`,
    '아래 DB 컬럼만 채우고 없으면 null.',
    '반드시 {"values":{ "<컬럼명>": 값 }} 만 출력.',
    `columns: ${JSON.stringify(fields)}`,
  ].join('\n');
  const resp = await mdl.generateContent({
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
  const parts = resp.response?.candidates?.[0]?.content?.parts ?? [];
  const raw = parts.map((p) => p.text || '').join('');
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    return parsed.values || {};
  } catch (err) {
    console.warn('[vertex] extract parse failed:', err?.message || err);
    return {};
  }
}

module.exports = { classifyByGcs, extractValuesByGcs };