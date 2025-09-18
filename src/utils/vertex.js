// relay-worker/src/utils/vertex.js
'use strict';

const { VertexAI } = require('@google-cloud/vertexai');
const LOCATION = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID = process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash';

const vertex = new VertexAI({ location: LOCATION });
const model  = vertex.getGenerativeModel({ model: MODEL_ID });

async function callModelJson(systemText, userText) {
  const res = await model.generateContent({
    contents: [
      { role: 'system', parts: [{ text: systemText }] },
      { role: 'user',   parts: [{ text: userText   }] }
    ],
    generationConfig: { temperature: 0.2, maxOutputTokens: 2048 }
  });

  const txt = res?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '';
  try { return JSON.parse(txt); }
  catch { throw new Error(`Vertex output is not JSON: ${txt.slice(0,200)}`); }
}

module.exports = { LOCATION, MODEL_ID, callModelJson /* + 나머지 유틸이 있으면 export */ };
