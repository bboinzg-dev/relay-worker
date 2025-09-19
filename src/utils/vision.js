// relay-worker/src/utils/vision.js
'use strict';

const { Storage } = require('@google-cloud/storage');
const { v1: { DocumentProcessorServiceClient } } = require('@google-cloud/documentai');
const storage = new Storage();

const DOCAI_PROJECT_ID   = process.env.DOCAI_PROJECT_ID || process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const DOCAI_LOCATION     = process.env.DOCAI_LOCATION   || process.env.DOC_AI_LOCATION || 'us';
const DOCAI_PROCESSOR_ID = process.env.DOCAI_PROCESSOR_ID || '';
const MAX_INLINE_DEFAULT = Number(process.env.MAX_DOC_PAGES_INLINE || 15);

function isGs(u){ return /^gs:\/\//i.test(String(u||'')); }
function parseGs(u){
  const m=/^gs:\/\/([^/]+)\/(.+)$/.exec(String(u||'')); if(!m) throw new Error('INVALID_GCS_URI');
  return { bucket:m[1], object:m[2] };
}
const joinText = (pages=[]) => pages.map(p=>p.text||'').filter(Boolean).join('\n\n');

/** 앞쪽 n페이지 온라인 추출(텍스트/페이지) */
async function getPdfText(gcsUri, { limit = MAX_INLINE_DEFAULT } = {}) {
  if (!isGs(gcsUri)) throw new Error('gcsUri must be gs://…');
  if (!DOCAI_PROCESSOR_ID) {
    console.warn('[vision.getPdfText] DOCAI_PROCESSOR_ID not set');
    return { text:'', pages:[] };
  }

  const client = new DocumentProcessorServiceClient();
  const name = client.processorPath(DOCAI_PROJECT_ID, DOCAI_LOCATION, DOCAI_PROCESSOR_ID);

  const { bucket, object } = parseGs(gcsUri);
  const [buf] = await storage.bucket(bucket).file(object).download();

  const requestedPages = Math.max(1, Number(limit) || MAX_INLINE_DEFAULT);
  const perCallBudget = Math.max(1, Math.min(MAX_INLINE_DEFAULT, requestedPages));

  if (requestedPages > perCallBudget) {
    console.warn('[vision.getPdfText] requested pages exceed inline limit → clamped', {
      requested: requestedPages,
      limit: perCallBudget,
    });
  }

  const pagesReq = Array.from({ length: perCallBudget }, (_, i) => String(i + 1));
  let result;
  try {
    // v1에서 개별 페이지 선택이 지원되는 경우
    [result] = await client.processDocument({
      name,
      rawDocument: { content: buf, mimeType: 'application/pdf' },
      processOptions: { individualPageSelector: { pages: pagesReq } },
    });
  } catch (e) {
    console.warn('[vision.getPdfText] page selector failed → fallback whole doc:', e?.message || e);

      try {
          // 개별 페이지 선택이 거부되면 전체 문서를 한 번에 처리하되 imageless 모드로 시도
          [result] = await client.processDocument({
            name,
            rawDocument: { content: buf, mimeType: 'application/pdf' },
            processOptions: { ocrConfig: { enableNativePdfParsing: true } },
          });
        } catch (fallbackErr) {
          console.warn('[vision.getPdfText] imageless fallback failed:', fallbackErr?.message || fallbackErr);
          throw fallbackErr;
        }
  }

  const doc = result.document;
  const outPages = (doc?.pages||[]).map((p,i)=>({
    page: p.pageNumber || (i+1),
    text: p.layout?.textAnchor?.content || p.layout?.text || ''
  }));

  return { pages: outPages, text: joinText(outPages) };
}

/** 지정 페이지들만 DocAI 처리(raw JSON 반환) */
async function callDocAI(gcsUri, { pageNumbers = [] } = {}) {
  if (!isGs(gcsUri)) throw new Error('gcsUri must be gs://…');
  if (!DOCAI_PROCESSOR_ID) throw new Error('DOCAI_PROCESSOR_ID not set');

  const client = new DocumentProcessorServiceClient();
  const name = client.processorPath(DOCAI_PROJECT_ID, DOCAI_LOCATION, DOCAI_PROCESSOR_ID);

  const { bucket, object } = parseGs(gcsUri);
  const [buf] = await storage.bucket(bucket).file(object).download();

  let result;
  try {
    const req = {
      name,
      rawDocument: { content: buf, mimeType: 'application/pdf' },
      ...(Array.isArray(pageNumbers) && pageNumbers.length
        ? { processOptions: { individualPageSelector: { pages: pageNumbers.map(String) } } }
        : {})
    };
    [result] = await client.processDocument(req);
  } catch (e) {
    console.warn('[vision.callDocAI] page selector failed → fallback whole doc:', e?.message || e);

      try {
          [result] = await client.processDocument({
            name,
            rawDocument: { content: buf, mimeType: 'application/pdf' },
            processOptions: { ocrConfig: { enableNativePdfParsing: true } },
          });
        } catch (fallbackErr) {
          console.warn('[vision.callDocAI] imageless fallback failed:', fallbackErr?.message || fallbackErr);
          throw fallbackErr;
        }
  }
  return result.document || result;
}

/** 간단한 키워드 휴리스틱으로 “유력 페이지” 고르기 */
function pickPagesByVertex(meta, { target = ['ordering','type','selection','spec'] } = {}) {
  const pages = Array.isArray(meta?.pages) ? meta.pages : [];
  const keys = target.map(s => String(s).toLowerCase());
  const scored = pages.map(p => {
    const t = String(p.text||'').toLowerCase();
    const score = keys.reduce((acc,k)=> acc + (t.includes(k) ? 1 : 0), 0)
                + (t.match(/\bpart\s*(number|no|#)\b/g)?.length || 0);
    return { page: p.page, score };
  });
  scored.sort((a,b)=> b.score - a.score);
  const picked = scored.filter(x => x.score>0).slice(0, 8).map(x => x.page);
  return { pages: picked, reason: 'keyword_heuristic' };
}

module.exports = { getPdfText, callDocAI, pickPagesByVertex };
