'use strict';
const { VertexAI } = require('@google-cloud/vertexai');

function getModel(modelEnv, fallback) {
  const project  = process.env.GCP_PROJECT_ID;
  const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
  const v = new VertexAI({ project, location });
  return v.getGenerativeModel({ model: process.env[modelEnv] || fallback });
}

// DB fields_json → JSON Schema 변환 (Vertex Structured Output)
function toJsonSchema(fieldsJson) {
  const props = {};
  for (const [k, t] of Object.entries(fieldsJson || {})) {
    props[k] =
      t === 'text'        ? { type: 'string' } :
      t === 'numeric'     ? { type: 'number' } :
      t === 'int'         ? { type: 'integer' } :
      t === 'bool'        ? { type: 'boolean' } :
      t === 'jsonb'       ? { type: 'object' } :
      t === 'timestamptz' ? { type: 'string', format: 'date-time' } :
                            { type: 'string' };
  }
  return { type: 'object', properties: props, additionalProperties: false };
}

function stripFence(s) {
  let t = (s || '').trim();
  if (t.startsWith('```')) t = t.replace(/```json|```/g, '').trim();
  return t;
}

async function callLLM({ modelEnv, fallback, prompt, pdfBase64, responseSchema, timeoutMs = 30000 }) {
  const mdl = getModel(modelEnv, fallback);
  const ctrl = new AbortController();
  const killer = setTimeout(() => ctrl.abort('TIMEOUT'), timeoutMs);

  try {
    const generationConfig = {
      temperature: 0.2,
      topP: 0.8,
      responseMimeType: 'application/json',
    };
    if (responseSchema) generationConfig.responseSchema = responseSchema; // Vertex 구조화 출력 사용

    const resp = await mdl.generateContent({
      contents: [{
        role: 'user',
        parts: [
          { text: prompt },
          pdfBase64 ? { inlineData: { mimeType: 'application/pdf', data: pdfBase64 } } : null
        ].filter(Boolean),
      }],
      generationConfig,
    }, { signal: ctrl.signal });

    const parts = resp?.response?.candidates?.[0]?.content?.parts ?? [];
    return stripFence(parts.map(p => p?.text ?? '').join(''));
  } finally {
    clearTimeout(killer);
  }
}

module.exports = { toJsonSchema, callLLM };
