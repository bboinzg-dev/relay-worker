src/utils/ai.js
신규
+33
-0

'use strict';

// 최소 의존: Vertex AI. 없으면 null을 돌려 폴백 사용.
let client = null;
function getClient() {
  if (client) return client;
  try {
    const { VertexAI } = require('@google-cloud/vertexai');
    const project = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
    const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
    client = new VertexAI({ project, location });
    return client;
  } catch (e) {
    return null;
  }
}

async function generateJSON({ system, input, schema, model }) {
  const c = getClient();
  if (!c) return null;
  const m = c.getGenerativeModel({
    model: model || process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash',
    generationConfig: { temperature: 0.2, maxOutputTokens: 2048, responseMimeType: 'application/json' },
    systemInstruction: system ? { parts: [{ text: system }] } : undefined,
  });
  const res = await m.generateContent({
    contents: [{ role: 'user', parts: [{ text: JSON.stringify({ input, schema }, null, 2) }]}],
  });
  const text = res.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
  try { return JSON.parse(text); } catch { return null; }
}

module.exports = { generateJSON };