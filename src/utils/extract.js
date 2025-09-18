'use strict';

const { getSignedUrl } = require('./gcs');

// 가능한 경우 Document AI 사용 (선택)
async function extractWithDocAI(gcsUri) {
  try {
    const processorId = process.env.DOCAI_PROCESSOR_ID;
    const loc = process.env.DOCAI_LOCATION || process.env.DOC_AI_LOCATION || 'us';
    if (!processorId) return null;

    // 레포가 @google-cloud/documentai 를 사용한다면 여기에 연결
    // 반환 형태: { text: '...', pages: [{text:'...'}, ...] }
    // 구현이 없다면 null을 반환하여 폴백으로 내려가게 하세요.
    return null;
  } catch (_) {
    return null;
  }
}

// LLM에 PDF 자체를 건네고 텍스트만 추출(범용 폴백)
const { callModelJson } = require('./vertex');
async function extractWithLLM(gcsUri) {
  const sys = [
    'You read a PDF and output the main text.',
    'Return JSON: {"text": "<flattened_text>", "pages": ["p1", "p2", ...]}',
    'Keep line breaks but remove headers/footers if repetitive.'
  ].join('\n');
  const usr = JSON.stringify({ gcs_uri: gcsUri, first_pages: 8 });
  const out = await callModelJson(sys, usr);
  const pages = Array.isArray(out?.pages) ? out.pages : [];
  const text = (out?.text || pages.join('\n\n') || '').toString();
  return { text, pages: pages.map(t => ({ text: t })) };
}

async function extractText(gcsUri) {
  // 1) DocAI
  const d = await extractWithDocAI(gcsUri);
  if (d && d.text) return d;

  // 2) LLM 폴백
  return await extractWithLLM(gcsUri);
}

module.exports = { extractText };
