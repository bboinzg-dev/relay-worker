'use strict';

let client = null;

function getClient() {
  if (client) return client;
  try {
    const { VertexAI } = require('@google-cloud/vertexai');
    const project  = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
    const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
    client = new VertexAI({ project, location });
  } catch {
    client = null; // Vertex 미사용(또는 의존성 없음) 시 null 유지
  }
  return client;
}

/**
 * JSON 스키마 기반 생성. 실패 시 null 리턴(폴백 용이)
 */
async function generateJSON({ system, input, schema, model }) {
  const c = getClient();
  if (!c) return null;

  const m = c.getGenerativeModel({
    model: model || process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash',
    generationConfig: {
      temperature: 0.2,
      maxOutputTokens: 2048,
      responseMimeType: 'application/json',
    },
    ...(system ? { systemInstruction: { parts: [{ text: system }] } } : {}),
  });

  const res = await m.generateContent({
    contents: [{ role: 'user', parts: [{ text: JSON.stringify({ input, schema }) }]}],
  });

  const text =
    res?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}';

  try { return JSON.parse(text); } catch { return null; }
}

module.exports = { generateJSON };
