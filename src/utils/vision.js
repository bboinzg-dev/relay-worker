// relay-worker/src/utils/vision.js
'use strict';

const path = require('path');
const { Storage } = require('@google-cloud/storage');
const { v1: { DocumentProcessorServiceClient } } = require('@google-cloud/documentai');

const storage = new Storage();

const DOCAI_PROJECT_ID   = process.env.DOCAI_PROJECT_ID || process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
const DOCAI_LOCATION     = process.env.DOCAI_LOCATION   || process.env.DOC_AI_LOCATION || 'us';
const DOCAI_PROCESSOR_ID = process.env.DOCAI_PROCESSOR_ID || '';
const MAX_INLINE_DEFAULT = Number(process.env.MAX_DOC_PAGES_INLINE || 15);

// --- helpers ---
function isGsUri(u){ return /^gs:\/\//i.test(String(u||'')); }
function gsParse(u){
  const m=/^gs:\/\/([^/]+)\/(.+)$/.exec(String(u||''));
  if(!m) throw new Error('INVALID_GCS_URI');
  return { bucket:m[1], object:m[2] };
}
const joinText = (pages=[]) => pages.map(p=>p.text||'').filter(Boolean).join('\n\n');

// --- Document AI wrappers ---

/**
 * 앞쪽 n페이지만 빠르게 텍스트 추출(온라인)
 * @returns {Promise<{text:string, pages:Array<{page:number,text:string}>}>}
 */
async function getPdfText(gcsUri, { limit = MAX_INLINE_DEFAULT } = {}) {
  if (!isGsUri(gcsUri)) throw new Error('gcsUri must be gs://…');
  if (!DOCAI_PROCESSOR_ID) {
    console.warn('[vision.getPdfText] DOCAI_PROCESSOR_ID not set');
    return { text:'', pages:[] };
  }

  const client = new DocumentProcessorServiceClient();
  const name = client.processorPath(DOCAI_PROJECT_ID, DOCAI_LOCATION, DOCAI_PROCESSOR_ID);

  const { bucket, object } = gsParse(gcsUri);
  const [buf] = await storage.bucket(bucket).file(object).download();

  // 1..limit 페이지만 선택
  const pagesReq = Array.from({length: Math.max(1, Number(limit)||MAX_INLINE_DEFAULT)}, (_,i)=> String(i+1));

  const [result] = await client.processDocument({
    name,
    rawDocument: { content: buf, mimeType: 'application/pdf' },
    processOptions: { individualPageSelector: { pages: pagesReq } },
  });

  const doc = result.document;
  const outPages = (doc?.pages||[]).map((p,i)=>({
    page: p.pageNumber || (i+1),
    text: p.layout?.textAnchor?.content || p.layout?.text || ''
  }));

  return { pages: outPages, text: joinText(outPages) };
}

/**
 * 특정 페이지들만 DocAI 처리(raw)
 */
async function callDocAI(gcsUri, { pageNumbers = [] } = {}) {
  if (!isGsUri(gcsUri)) throw new Error('gcsUri must be gs://…');
  if (!DOCAI_PROCESSOR_ID) throw new Error('DOCAI_PROCESSOR_ID not set');

  const client = new DocumentProcessorServiceClient();
  const name = client.processorPath(DOCAI_PROJECT_ID, DOCAI_LOCATION, DOCAI_PROCESSOR_ID);

  const { bucket, object } = gsParse(gcsUri);
  const [buf] = await storage.bucket(bucket).file(object).download();

  const req = {
    name,
    rawDocument: { content: buf, mimeType: 'application/pdf' },
  };
  if (Array.isArray(pageNumbers) && pageNumbers.length) {
    req.processOptions = { individualPageSelector: { pages: pageNumbers.map(String) } };
  }

  const [result] = await client.processDocument(req);
  return result.document || result; // 그대로 반환(표 파서는 별도 모듈)
}

/**
 * 페이지 후보 선택(안전한 키워드 휴리스틱)
 *  - ordering/type/selection/spec 키워드가 많은 페이지 우선
 *  - meta.pages[].text 기준
 */
function pickPagesByVertex(meta, { target = ['ordering','type','selection','specification'] } = {}) {
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
