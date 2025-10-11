'use strict';

const { VertexAI } = require('@google-cloud/vertexai');
const { safeJsonParse } = require('./safe-json');
const env = require('../config/env');

const DEFAULT_MODEL_ID = env.GEMINI_MODEL_EXTRACT || env.VERTEX_MODEL_ID;


let vertexInstance;

function getVertex() {
  if (!vertexInstance) {
    vertexInstance = new VertexAI({ project: env.PROJECT_ID, location: env.VERTEX_LOCATION });
  }
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
