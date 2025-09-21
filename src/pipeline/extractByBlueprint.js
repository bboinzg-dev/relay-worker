'use strict';
const { VertexAI } = require('@google-cloud/vertexai');

const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_ID = process.env.GEMINI_MODEL_EXTRACT || 'gemini-2.5-flash';
const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });

// fieldsJson: {"coil_voltage_vdc":"numeric", "contact_form":"text", ...}
async function extractFields(rawText, code, fieldsJson) {
  const schema = Object.entries(fieldsJson || {}).map(([k,t]) => ({ name:k, type:String(t||'text') }));
  const have = new Set(schema.map(s=>s.name));
  for (const k of ['length_mm','width_mm','height_mm']) if (!have.has(k)) schema.push({ name:k, type:'numeric' });

  const prompt = [
    `You are an expert extracting exact fields from an electronic component datasheet.`,
    `Target model code: ${code}. Return a strict JSON object with these keys only.`,
    `If a field is unknown, return null. Numeric fields are plain numbers (no units).`,
    `Units assumption: dimensions in millimeters; voltage in volts; current in amperes.`,
    `Fields: ${JSON.stringify(schema)}`
  ].join('\n');

  const model = vertex.getGenerativeModel({ model: MODEL_ID });
  const resp = await model.generateContent({ contents: [{ role: 'user', parts: [{ text: prompt }, { text: rawText.slice(0, 32000) }]}] });
  const out = resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
  try {
    const obj = JSON.parse(out);
    // 숫자 캐스팅
    for (const {name, type} of schema) {
      if (String(type).toLowerCase().startsWith('num') && obj[name] != null) {
        const v = Number(String(obj[name]).replace(/[^0-9.+-eE]/g,''));
        obj[name] = Number.isFinite(v) ? v : null;
      }
    }
    return obj;
  } catch { return {}; }
}
module.exports = { extractFields };
