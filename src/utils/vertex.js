'use strict';

const { VertexAI } = require('@google-cloud/vertexai');

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT_ID;
const LOCATION   = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID   = process.env.GEMINI_MODEL_EXTRACT || process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash';

const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });

// Vertex는 "role: system" 메시지를 허용하지 않는다 → systemInstruction 사용
function getModel(systemText, modelId = MODEL_ID) {
  const cfg = { model: modelId };
  if (systemText && String(systemText).trim()) {
    cfg.systemInstruction = { parts: [{ text: String(systemText) }] };
  }
  return vertex.getGenerativeModel(cfg);
}

async function callModelJson(systemText, userText, { modelId, maxOutputTokens = 4096, temperature = 0.1 } = {}) {
  const model = getModel(systemText, modelId);
  const req = {
    contents: [{ role: 'user', parts: [{ text: String(userText || '') }]}],
    generationConfig: {
      responseMimeType: 'application/json',
      temperature,
      maxOutputTokens,
    },
  };
  const resp = await model.generateContent(req);
  const txt = resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
  try { return JSON.parse(txt); }
  catch { throw new Error(`Vertex output is not JSON: ${String(txt).slice(0, 300)}`); }
}

module.exports = { getModel, callModelJson };
