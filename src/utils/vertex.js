'use strict';

const { VertexAI } = require('@google-cloud/vertexai');
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

// 안전 JSON 파서 (코드블록/설명 섞인 답도 복구)
function safeParseJson(text) {
  try { return JSON.parse(text); } catch (_) {}
  const stripped = String(text ?? '')
    .replace(/```(?:json)?/gi, '')
    .trim();
  try { return JSON.parse(stripped); } catch (_) {}
  let depth = 0;
  let start = -1;
  for (let i = 0; i < stripped.length; i += 1) {
    const ch = stripped[i];
    if (ch === '{') {
      if (depth === 0) start = i;
      depth += 1;
    } else if (ch === '}') {
      depth -= 1;
      if (depth === 0 && start >= 0) {
        const slice = stripped.slice(start, i + 1);
        try { return JSON.parse(slice); } catch (_) {}
        start = -1;
      }
    }
  }
  return null;
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
  const txt = resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '';
  const parsed = safeParseJson(txt);
  if (!parsed) {
    const sample = String(txt).slice(0, 300);
    if (process.env.DEBUG_ORDERING === '1') {
      console.warn('[vertex] callModelJson not-json sample=', sample);
    }
    const err = new Error('VERTEX_NOT_JSON');
    err.sample = sample;
    throw err;
  }
  return parsed;
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
    if (process.env.DEBUG_ORDERING === '1' && err?.sample) {
      console.warn('[vertex] extractOrderingRecipe sample=', err.sample);
    }
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

module.exports = { getVertex, getModel, callModelJson, extractOrderingRecipe, safeParseJson };