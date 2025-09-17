const { VertexAI } = require('@google-cloud/vertexai');
const { readText } = require('./gcs');

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION = process.env.VERTEX_LOCATION || 'asia-northeast3'; // 또는 운영 리전에 맞춤
const MODEL_ID =
  process.env.GEMINI_MODEL_EXTRACT   // 홈페이지와 키 통일(있으면 우선)
  || process.env.VERTEX_MODEL_ID     // 기존 환경변수 호환
  || 'gemini-2.5-flash';
const EMBED_TEXT = process.env.VERTEX_EMBED_TEXT || 'text-embedding-004';
const EMBED_IMAGE = process.env.VERTEX_EMBED_IMAGE || 'multimodalembedding@001';

const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });

function fillPrompt(template, fields) {
  const keys = Object.keys(fields || {});
  return template.replace(/\{\{FIELDS\}\}/g, JSON.stringify(keys));
}

async function identifyFamilyBrandCode(gcsUri, families = []) {
  const gen = vertex.getGenerativeModel({ model: MODEL_ID });
  const prompt = [
    'You are a component classifier. Return a compact JSON with keys: family_slug, brand, code, series, display_name.',
    families.length ? `Families candidates: ${families.join(', ')}` : '',
    'If unsure, pick the closest family. Only JSON.',
  ].filter(Boolean).join('\n');

  const req = {
    contents: [{
      role: 'user',
      parts: [
        { text: prompt },
        { fileData: { fileUri: gcsUri, mimeType: 'application/pdf' } }
      ]
    }]
  };
  const resp = await gen.generateContent(req);
  const txt = resp?.response?.candidates?.[0]?.content?.parts?.map(p => p.text).join('') || '{}';
  try { return JSON.parse(txt); } catch { return {}; }
}

async function extractByBlueprintGemini(gcsUri, fieldsJson = {}, promptTemplate) {
  const gen = vertex.getGenerativeModel({ model: MODEL_ID });
  const prompt = fillPrompt(promptTemplate || 'Return ONLY JSON for keys: {{FIELDS}}', fieldsJson);
  const req = {
    contents: [{
      role: 'user',
      parts: [
        { text: prompt },
        { fileData: { fileUri: gcsUri, mimeType: 'application/pdf' } }
      ]
    }]
  };
  const resp = await gen.generateContent(req);
  const txt = resp?.response?.candidates?.[0]?.content?.parts?.map(p => p.text).join('') || '{}';
  let values = {};
  try { values = JSON.parse(txt); } catch (e) { values = {}; }
  return { fields: fieldsJson, values, raw_json: { model: MODEL_ID, output: txt } };
}

async function embedText(text) {
  const model = vertex.getEmbeddingModel({ model: EMBED_TEXT });
  const resp = await model.embedContent({ content: { parts: [{ text }] } });
  const vec = resp?.embeddings?.[0]?.values || [];
  return vec;
}

module.exports = { identifyFamilyBrandCode, extractByBlueprintGemini, embedText, fillPrompt };
