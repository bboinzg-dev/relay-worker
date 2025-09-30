const { VertexAI } = require('@google-cloud/vertexai');
const { safeJsonParse } = require('../src/utils/safe-json');
const env = require('../src/config/env');

function getModel(name) {
  const v = new VertexAI({
    project: env.PROJECT_ID,
    location: env.VERTEX_LOCATION,
  });
  return v.getGenerativeModel({ model: name });
}


function partsText(resp) {
  const parts = resp?.response?.candidates?.[0]?.content?.parts || [];
  return parts.map(p => p?.text || '').join('');
}

async function classifyFamily({ pdfBytes, allowedFamilies }) {
  const mdl = getModel(env.GEMINI_MODEL_CLASSIFY);  const prompt = `
PDF 전체를 보고 아래 JSON 한 객체로만 답하세요.
{"family_slug":"...", "brand": null|string, "code": null|string, "series": null|string}

- family_slug는 반드시 다음 중 정확히 하나: ${allowedFamilies.map(f => `"${f}"`).join(', ')}
- 허용 목록 외 값 금지. 불확실하면 가장 근접한 하나 선택.
`.trim();

  const resp = await mdl.generateContent({
    contents: [{
      role: 'user',
      parts: [
        { text: prompt },
        { inlineData: { mimeType: 'application/pdf', data: pdfBytes.toString('base64') } },
      ],
    }],
    generationConfig: { responseMimeType: 'application/json' },
  });

  const text = partsText(resp) || '{}';
   const parsed = safeJsonParse(text) || {};
  return parsed;
}

async function extractByBlueprint({ pdfBytes, family, fields }) {
  const mdl = getModel(env.GEMINI_MODEL_EXTRACT);  const keys = Object.keys(fields);
  const prompt = `
다음 PDF에서 ${family} 부품의 스펙을 추출합니다.
아래 컬럼들만 채우세요(없으면 null). 반드시 {"values": { "<컬럼>": <값> }} JSON 한 객체만 출력.
허용 컬럼: ${keys.join(', ')}
`.trim();

  const resp = await mdl.generateContent({
    contents: [{
      role: 'user',
      parts: [
        { text: prompt },
        { inlineData: { mimeType: 'application/pdf', data: pdfBytes.toString('base64') } },
      ],
    }],
    generationConfig: { responseMimeType: 'application/json' },
  });

  const text = partsText(resp) || '{}';
 const j = safeJsonParse(text) || {};
  return j?.values || {};
}

module.exports = { classifyFamily, extractByBlueprint };