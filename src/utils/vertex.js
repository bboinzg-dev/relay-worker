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
  cfg.generationConfig = { responseMimeType: 'application/json' };
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

async function extractOrderingRecipe(gcsUriOrText) {
  const source = String(gcsUriOrText || '');
  if (!source) return { variant_domains: {}, pn_template: null };

  const sys = [
    'You analyze ORDERING INFORMATION / HOW TO ORDER sections from electronic component datasheets.',
    'Return strict JSON with shape: {"variant_domains": {"key": ["values"]}, "pn_template": "string or null"}.',
    'Keys must be concise machine-readable snake_case identifiers.',
    'If an option is blank/"Nil", represent it with an empty string "".',
    'Do not fabricate data. Leave arrays empty when unsure.',
  ].join('\n');

  const payload = /^gs:\/\//i.test(source)
    ? { gcs_uri: source }
    : { text: source };

  let out;
  try {
    out = await callModelJson(sys, JSON.stringify({ source: payload }), { maxOutputTokens: 2048 });
  } catch (err) {
    console.warn('[vertex] extractOrderingRecipe failed:', err?.message || err);
    return { variant_domains: {}, pn_template: null };
  }

  const domains = out && typeof out === 'object' && out.variant_domains && typeof out.variant_domains === 'object'
    ? out.variant_domains
    : {};
  const tpl = typeof out?.pn_template === 'string' && out.pn_template.trim()
    ? out.pn_template
    : null;

  return { variant_domains: domains, pn_template: tpl };
}

module.exports = { getVertex, getModel, callModelJson, extractOrderingRecipe };