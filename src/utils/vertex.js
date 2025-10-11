'use strict';

const { VertexAI } = require('@google-cloud/vertexai');
const { safeJsonParse } = require('./safe-json');

let envConfig = null;
let envLoadError = null;
try {
  envConfig = require('../config/env');
} catch (err) {
  envLoadError = err;
  console.warn('[vertex] config env unavailable:', err?.message || err);
}

const PROJECT_ID = envConfig?.PROJECT_ID
  || process.env.GCP_PROJECT_ID
  || process.env.GOOGLE_CLOUD_PROJECT
  || null;
const LOCATION = envConfig?.VERTEX_LOCATION || process.env.VERTEX_LOCATION || 'asia-northeast3';
const DEFAULT_MODEL_ID = envConfig?.GEMINI_MODEL_EXTRACT
  || envConfig?.VERTEX_MODEL_ID
  || process.env.GEMINI_MODEL_EXTRACT
  || process.env.VERTEX_MODEL_ID
  || 'gemini-2.5-flash';

let vertexInstance;

function getVertex() {
    if (!PROJECT_ID) {
    const err = new Error('VERTEX_PROJECT_ID_MISSING');
    if (envLoadError) err.cause = envLoadError;
    throw err;
  }
  if (!vertexInstance) {
    vertexInstance = new VertexAI({ project: PROJECT_ID, location: LOCATION });
  return vertexInstance;
}

// Vertex는 "role: system" 메시지를 허용하지 않는다 → systemInstruction 사용
function getModel(systemText, modelId = DEFAULT_MODEL_ID) {
  const cfg = { model: modelId };
  if (systemText && String(systemText).trim()) {
    cfg.systemInstruction = { parts: [{ text: String(systemText) }] };
  }
  return getVertex().getGenerativeModel(cfg);
}

async function callModelJson(systemText, userText, { modelId, maxOutputTokens = 4096, temperature = 0.2, topP = 0.8 } = {}) {
  const model = getModel(systemText, modelId);
  const req = {
    contents: [{ role: 'user', parts: [{ text: String(userText || '') }]}],
    generationConfig: {
      responseMimeType: 'application/json',
      temperature,
      topP,
      maxOutputTokens,
    },
  };
  const resp = await model.generateContent(req);
  const txt = resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
  try {
    const parsed = safeJsonParse(txt);
    return parsed ?? {};
  } catch {
    throw new Error(`Vertex output is not JSON: ${String(txt).slice(0, 300)}`);
  }
}

module.exports = { getVertex, getModel, callModelJson };
