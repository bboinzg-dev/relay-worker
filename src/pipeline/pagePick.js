'use strict';
const { callLLM } = require('../llm/structured');

// 필요한 페이지 번호만 추리기(최대 k개)
exports.pickPages = async function pickPages(pdfBase64, family, k = 4) {
  const schema = {
    type: 'object',
    properties: { pages: { type: 'array', items: { type: 'integer' }, maxItems: k } },
    required: ['pages'],
    additionalProperties: false
  };
  const prompt = `
PDF에서 family=${family}의 스펙/주문정보가 있을 법한 페이지 번호만 최대 ${k}개 뽑아주세요.
응답은 {"pages":[번호,...]} 하나만. 중복/설명 금지.
`;
  const raw = await callLLM({ modelEnv:'VERTEX_PAGE_PICK_MODEL', fallback:'gemini-2.5-flash', prompt, pdfBase64, responseSchema:schema, timeoutMs:15000 });
  const j = JSON.parse(raw || '{}');
  return Array.isArray(j.pages) ? Array.from(new Set(j.pages)).slice(0,k) : [];
};

// 카탈로그: 파트코드만 최대 N개
exports.listCodes = async function listCodes(pdfBase64, family, n = 20) {
  const schema = { type:'object', properties:{ codes:{ type:'array', items:{ type:'string' }, maxItems:n }}, required:['codes'], additionalProperties:false };
  const prompt = `
카탈로그형 PDF입니다. family=${family}.
실제 완성 품번만 최대 ${n}개, 대소문자/접미까지 원문 그대로 뽑아주세요.
응답은 {"codes":["...", "..."]} 하나만. (시리즈명/규칙 예시는 제외)
`;
  const raw = await callLLM({ modelEnv:'GEMINI_MODEL_CLASSIFY', fallback:'gemini-2.5-flash', prompt, pdfBase64, responseSchema:schema, timeoutMs:20000 });
  const j = JSON.parse(raw || '{}');
  return Array.isArray(j.codes) ? j.codes.slice(0,n) : [];
};
